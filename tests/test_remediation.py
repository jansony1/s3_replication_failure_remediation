"""
Tests for the S3 replication failure remediation stack.

Design constraint: the production code lives EMBEDDED inside allInOne_v2.yaml
(Lambda source as `ZipFile:` strings, the state machine as an ASL JSON in
`DefinitionString`). To avoid a drifting copy, these tests parse the YAML at
run time, extract the embedded code, and execute it with mocked boto3 clients.
The YAML is the single source of truth.
"""

import json
import os
import sys
import types
from unittest import mock

import pytest
import yaml

HERE = os.path.dirname(os.path.abspath(__file__))
TEMPLATE_PATH = os.path.join(HERE, "..", "allInOne_v2.yaml")


# ---------------------------------------------------------------------------
# YAML extraction helpers
# ---------------------------------------------------------------------------

class _CfnLoader(yaml.SafeLoader):
    """SafeLoader that tolerates CloudFormation short-form intrinsics (!Sub etc.).

    We only need the raw scalar/sequence content of the !Sub'd ASL and the
    plain ZipFile strings, so collapsing intrinsics to a marker string is fine.
    """


def _intrinsic(loader, tag_suffix, node):
    if isinstance(node, yaml.ScalarNode):
        return loader.construct_scalar(node)
    if isinstance(node, yaml.SequenceNode):
        return loader.construct_sequence(node)
    return loader.construct_mapping(node)


_CfnLoader.add_multi_constructor("!", _intrinsic)


def load_template():
    with open(TEMPLATE_PATH) as f:
        return yaml.load(f, Loader=_CfnLoader)


def get_lambda_code(template, resource_name):
    """Return the embedded ZipFile source string for a Lambda resource."""
    return template["Resources"][resource_name]["Properties"]["Code"]["ZipFile"]


def get_state_machine_definition(template):
    """Return the parsed ASL dict from the state machine's DefinitionString.

    DefinitionString uses !Sub; our loader collapses it to the raw scalar,
    which is the ASL JSON with ${...} placeholders. We replace the
    placeholders with dummy ARNs so it parses as JSON.
    """
    raw = template["Resources"]["MyStateMachine"]["Properties"]["DefinitionString"]
    import re
    cleaned = re.sub(r"\$\{[^}]+\}", "dummy-arn", raw)
    return json.loads(cleaned)


def exec_lambda_module(code, env, fake_boto3):
    """Execute extracted Lambda source as a module with mocked deps.

    Returns the module namespace (so callers can grab lambda_handler).
    """
    module = types.ModuleType("lambda_under_test")
    with mock.patch.dict(os.environ, env, clear=False), \
            mock.patch.dict(sys.modules, {"boto3": fake_boto3}):
        exec(compile(code, "<zipfile>", "exec"), module.__dict__)
    return module


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------

class FakeClientError(Exception):
    """Stand-in for botocore.exceptions.ClientError."""


def make_fake_boto3(*, s3=None, s3control=None, dynamodb_resource=None):
    """Build a fake `boto3` module exposing client()/resource()."""
    fake = types.ModuleType("boto3")

    def client(name, *args, **kwargs):
        if name == "s3":
            return s3
        if name == "s3control":
            return s3control
        raise AssertionError(f"unexpected client: {name}")

    def resource(name, *args, **kwargs):
        if name == "dynamodb":
            return dynamodb_resource
        raise AssertionError(f"unexpected resource: {name}")

    fake.client = client
    fake.resource = resource
    return fake


class FakeTable:
    """Minimal DynamoDB Table: returns a single page with one item."""

    def __init__(self):
        self.put_items = []

    def query(self, **kwargs):
        return {
            "Items": [
                {
                    "ReplicationRuleId": "rule-1",
                    "ObjectKeyVersionId": "some/key.txt#v123",
                }
            ]
        }

    def put_item(self, Item):
        self.put_items.append(Item)


class FakeDDBResource:
    def __init__(self, table):
        self._table = table

    def Table(self, name):
        return self._table


PROCESS_ENV = {
    "table_name": "t",
    "csv_bucket": "b",
    "account_id": "111111111111",
    "region": "us-west-2",
    "replication_role": "arn:aws:iam::111111111111:role/r",
}


# ---------------------------------------------------------------------------
# T3 / T5: structural tests (no code execution)
# ---------------------------------------------------------------------------

def test_every_task_state_has_catch():
    """T3: every Task state in the ASL must have an error-handling Catch.

    CheckCopyStatus currently lacks one, so a describe_job exception there
    crashes the whole execution with no cleanup. This test pins that gap.
    """
    asl = get_state_machine_definition(load_template())
    missing = [
        name
        for name, state in asl["States"].items()
        if state.get("Type") == "Task" and "Catch" not in state
    ]
    assert missing == [], f"Task states without Catch: {missing}"


# ---------------------------------------------------------------------------
# T1: create_job failure must not be silently swallowed
# ---------------------------------------------------------------------------

def test_create_job_failure_is_not_swallowed():
    """T1: when s3control.create_job fails, the handler must NOT return 200.

    Today error_occurred is passed by value into one_time_batch, so the
    function-local reassignment never reaches the caller; the handler returns
    a fake 'success' with job_id=None. The handler should instead surface the
    failure (raise, or return a non-200), letting the Step Functions Catch
    route to JobFailed.
    """
    template = load_template()
    code = get_lambda_code(template, "ProcessAndStartCopyFunction")

    s3 = mock.Mock()
    s3.put_object.return_value = {"ETag": '"etag123"'}

    s3control = mock.Mock()
    s3control.create_job.side_effect = FakeClientError("denied")

    fake_boto3 = make_fake_boto3(
        s3=s3, s3control=s3control,
        dynamodb_resource=FakeDDBResource(FakeTable()),
    )
    module = exec_lambda_module(code, PROCESS_ENV, fake_boto3)

    failed_cleanly = False
    result = None
    try:
        result = module.lambda_handler(
            {"ReplicationRuleId": "rule-1", "SourceBucket": "src"}, None
        )
    except Exception:
        failed_cleanly = True  # raising is an acceptable way to surface failure

    if not failed_cleanly:
        assert result is not None
        assert result.get("statusCode") != 200, (
            "create_job failed but handler returned success: %r" % result
        )
        assert result.get("job_id") is None or "job_id" not in result


# ---------------------------------------------------------------------------
# T2: put_object failure must fail cleanly (no NameError on etag)
# ---------------------------------------------------------------------------

def test_put_object_failure_does_not_raise_nameerror():
    """T2: if put_object fails, `etag` is never defined, yet one_time_batch is
    called before the error check -> NameError today. A NameError is a bug in
    error handling, not a clean failure. The handler must fail without it.
    """
    template = load_template()
    code = get_lambda_code(template, "ProcessAndStartCopyFunction")

    s3 = mock.Mock()
    s3.put_object.side_effect = FakeClientError("upload failed")

    s3control = mock.Mock()
    s3control.create_job.return_value = {"JobId": "job-1"}

    fake_boto3 = make_fake_boto3(
        s3=s3, s3control=s3control,
        dynamodb_resource=FakeDDBResource(FakeTable()),
    )
    module = exec_lambda_module(code, PROCESS_ENV, fake_boto3)

    try:
        result = module.lambda_handler(
            {"ReplicationRuleId": "rule-1", "SourceBucket": "src"}, None
        )
    except NameError as e:
        pytest.fail(f"handler raised NameError on upload failure: {e}")
    except Exception:
        return  # any other clean failure is acceptable

    # If it returned, it must signal failure rather than pretend success.
    assert result.get("statusCode") != 200


# ---------------------------------------------------------------------------
# T4: happy path regression guard
# ---------------------------------------------------------------------------

def test_happy_path_returns_job_id_for_downstream():
    """T4: when everything succeeds, the handler returns the fields the next
    state (CheckCopyStatus) needs: job_id, s3_bucket, account_id,
    table_name, s3_file_key_to_delete.
    """
    template = load_template()
    code = get_lambda_code(template, "ProcessAndStartCopyFunction")

    s3 = mock.Mock()
    s3.put_object.return_value = {"ETag": '"etag123"'}

    s3control = mock.Mock()
    s3control.create_job.return_value = {"JobId": "job-1"}

    fake_boto3 = make_fake_boto3(
        s3=s3, s3control=s3control,
        dynamodb_resource=FakeDDBResource(FakeTable()),
    )
    module = exec_lambda_module(code, PROCESS_ENV, fake_boto3)

    result = module.lambda_handler(
        {"ReplicationRuleId": "rule-1", "SourceBucket": "src"}, None
    )

    assert result["statusCode"] == 200
    assert result["job_id"] == "job-1"
    assert result["s3_bucket"] == "b"
    assert result["account_id"] == "111111111111"
    assert result["table_name"] == "t"
    assert result["s3_file_key_to_delete"] == "rule-1_delete.csv"


# ---------------------------------------------------------------------------
# T6 (#4): state machine must bound the poll loop with a timeout
# ---------------------------------------------------------------------------

def test_state_machine_has_timeout():
    """#4: the ASL must set a top-level TimeoutSeconds so the
    CheckCopyStatus <-> WaitAndCheckAgain poll loop cannot hang forever
    (README's known 'hangs with no error' failure mode).
    """
    asl = get_state_machine_definition(load_template())
    assert "TimeoutSeconds" in asl, "state machine has no TimeoutSeconds"
    assert isinstance(asl["TimeoutSeconds"], int) and asl["TimeoutSeconds"] > 0


# ---------------------------------------------------------------------------
# T7 (#5): the ETag passed to create_job must be stripped of quotes
# ---------------------------------------------------------------------------

def test_etag_quotes_stripped_before_create_job():
    """#5: put_object returns an ETag wrapped in double quotes ("abc").
    The manifest ETag passed to create_job should be the bare value, since
    S3 Control can reject the quoted form.
    """
    template = load_template()
    code = get_lambda_code(template, "ProcessAndStartCopyFunction")

    s3 = mock.Mock()
    s3.put_object.return_value = {"ETag": '"abc123"'}

    s3control = mock.Mock()
    s3control.create_job.return_value = {"JobId": "job-1"}

    fake_boto3 = make_fake_boto3(
        s3=s3, s3control=s3control,
        dynamodb_resource=FakeDDBResource(FakeTable()),
    )
    module = exec_lambda_module(code, PROCESS_ENV, fake_boto3)
    module.lambda_handler(
        {"ReplicationRuleId": "rule-1", "SourceBucket": "src"}, None
    )

    _, kwargs = s3control.create_job.call_args
    passed_etag = kwargs["Manifest"]["Location"]["ETag"]
    assert passed_etag == "abc123", f"ETag not stripped: {passed_etag!r}"


# ---------------------------------------------------------------------------
# T8 (#6): ProcessAndStartCopy must have a raised memory size
# ---------------------------------------------------------------------------

def test_process_lambda_memory_size():
    """#6: ProcessAndStartCopyFunction loads all failure items into memory
    before building the CSV, so it must request more than the 128MB default
    to handle large failure sets. Capacity is documented in the README.
    """
    template = load_template()
    props = template["Resources"]["ProcessAndStartCopyFunction"]["Properties"]
    assert "MemorySize" in props, "ProcessAndStartCopyFunction has no MemorySize"
    assert props["MemorySize"] >= 1024


# ---------------------------------------------------------------------------
# T9 (#7): the ingestion queue must have a dead-letter queue
# ---------------------------------------------------------------------------

def test_ingestion_queue_has_dlq():
    """#7: ReplicationQueue must have a RedrivePolicy pointing at a DLQ, so
    messages that repeatedly fail Lambda processing stop being redelivered
    forever and land somewhere inspectable.
    """
    template = load_template()
    queue = template["Resources"]["ReplicationQueue"]["Properties"]
    assert "RedrivePolicy" in queue, "ReplicationQueue has no RedrivePolicy"

    # A dedicated DLQ resource must exist.
    dlqs = [
        name
        for name, res in template["Resources"].items()
        if res.get("Type") == "AWS::SQS::Queue" and name != "ReplicationQueue"
    ]
    assert dlqs, "no dead-letter queue resource defined"


# ---------------------------------------------------------------------------
# T10 (#8): optional SourceBucket wiring via CloudFormation
# ---------------------------------------------------------------------------

def test_source_bucket_parameter_and_conditional_wiring():
    """#8: the template should accept an optional SourceBucket parameter and,
    when provided, wire the replication-failure notification automatically
    (via a custom resource) instead of requiring a manual CLI step. When the
    parameter is empty, the wiring is skipped (multi-bucket manual mode).
    """
    template = load_template()
    assert "SourceBucket" in template.get("Parameters", {}), \
        "no SourceBucket parameter"

    # A Condition gates the optional wiring.
    assert template.get("Conditions"), "no Conditions block for optional wiring"

    # A custom resource (or its backing Lambda) must exist to do the wiring.
    resources = template["Resources"]
    has_custom = any(
        res.get("Type", "").startswith("Custom::")
        or res.get("Type") == "AWS::CloudFormation::CustomResource"
        for res in resources.values()
    )
    assert has_custom, "no custom resource to configure bucket notification"


# ---------------------------------------------------------------------------
# T11: table schema migration to composite BucketRuleKey
# ---------------------------------------------------------------------------

def test_attribute_definitions_match_key_schema():
    """Table HASH key is BucketRuleKey; old ReplicationRuleId is gone from
    AttributeDefinitions (DynamoDB rejects unused attribute definitions)."""
    t = load_template()
    props = t["Resources"]["S3ReplicationFailureTable"]["Properties"]
    attr_names = {a["AttributeName"] for a in props["AttributeDefinitions"]}
    key_names = {k["AttributeName"] for k in props["KeySchema"]}
    hash_key = [k["AttributeName"] for k in props["KeySchema"]
                if k["KeyType"] == "HASH"][0]

    assert hash_key == "BucketRuleKey"
    assert "ReplicationRuleId" not in attr_names
    assert "BucketRuleKey" in attr_names
    assert attr_names == key_names


def test_ingestion_writes_composite_key():
    """Ingestion stores HASH key 'SRCBucket|ReplicationRuleId' and keeps
    SRCBucketName + ReplicationRuleId as attributes."""
    template = load_template()
    code = get_lambda_code(template, "FailureIngestionLambda")

    table = FakeTable()
    fake_boto3 = make_fake_boto3(dynamodb_resource=FakeDDBResource(table))

    s3_event = {
        "s3": {"bucket": {"name": "bucket-a"},
               "object": {"key": "path/obj.txt", "versionId": "v1",
                          "size": 10, "eTag": "etag"}},
        "replicationEventData": {"replicationRuleId": "rule-x",
                                 "failureReason": "DstPutObjectNotPermitted",
                                 "destinationBucket": "arn:aws:s3:::bucket-b"},
    }
    sqs_event = {"Records": [{"body": json.dumps({"Records": [s3_event]})}]}

    module = exec_lambda_module(code, {"table_name": "t"}, fake_boto3)
    module.lambda_handler(sqs_event, None)

    assert len(table.put_items) == 1
    item = table.put_items[0]
    assert item["BucketRuleKey"] == "bucket-a|rule-x"
    assert item["SRCBucketName"] == "bucket-a"
    assert item["ReplicationRuleId"] == "rule-x"
