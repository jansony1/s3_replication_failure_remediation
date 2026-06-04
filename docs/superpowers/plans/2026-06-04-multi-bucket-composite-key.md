# Multi-bucket Composite Key Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Change the DynamoDB partition key from `ReplicationRuleId` to a composite `SRCBucket|ReplicationRuleId` so one stack safely serves multiple source buckets / rules without cross-contamination.

**Architecture:** All production code is embedded in `allInOne_v2.yaml` (Lambda source as `ZipFile:` strings, table as a `AWS::DynamoDB::Table`). Tests in `tests/test_remediation.py` extract that embedded code from the YAML at runtime and execute it with mocked boto3 — the YAML is the single source of truth. We follow strict TDD: write failing test, watch it fail, make the minimal YAML change, watch it pass.

**Tech Stack:** CloudFormation YAML, Python 3.12 Lambda (boto3), pytest with `unittest.mock`, `cfn-lint`.

---

## Conventions for every test run

- Run pytest with the asyncio plugin disabled (it is broken in this env):
  `python3 -m pytest tests/test_remediation.py -p no:asyncio -v`
- The composite key joiner is `|` (pipe). The S3-filename-safe joiner is `__`
  (double underscore).
- `BucketRuleKey` is opaque — built by concatenation, never split.

## Reference: existing test harness helpers (already in `tests/test_remediation.py`)

These already exist and are reused — do NOT redefine them:
- `load_template()` → parsed YAML dict
- `get_lambda_code(template, "ResourceName")` → embedded ZipFile source string
- `exec_lambda_module(code, env, fake_boto3)` → module with `lambda_handler`
- `make_fake_boto3(s3=, s3control=, dynamodb_resource=)`
- `FakeClientError`, `FakeTable`, `FakeDDBResource`, `PROCESS_ENV`

`FakeTable` currently has `query()` returning one fixed item and a `put_items`
list capturing `put_item`. Some tasks extend it.

## File Structure

- Modify: `allInOne_v2.yaml`
  - `S3ReplicationFailureTable` (lines ~73-82): AttributeDefinitions + KeySchema
  - `FailureIngestionLambda` ZipFile (item dict ~138-144): write `BucketRuleKey`
  - `ProcessAndStartCopyFunction` ZipFile (~297-357): input validation, query key,
    projection, CSV source bucket, CSV object-key naming, to_delete CSV columns
  - `DeleteDynamoDBRecordsFunction` ZipFile (~566-575): delete by `BucketRuleKey`
- Modify: `tests/test_remediation.py` — add the tests below; update happy-path test

---

### Task 1: Table schema → composite partition key

**Files:**
- Test: `tests/test_remediation.py`
- Modify: `allInOne_v2.yaml` (S3ReplicationFailureTable, ~73-82)

- [ ] **Step 1: Write the failing test**

Add to `tests/test_remediation.py`:

```python
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
    # AttributeDefinitions must exactly match the set of key attributes.
    assert attr_names == key_names
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m pytest tests/test_remediation.py::test_attribute_definitions_match_key_schema -p no:asyncio -v`
Expected: FAIL — `hash_key == 'ReplicationRuleId'`, assertion on `BucketRuleKey` fails.

- [ ] **Step 3: Make the minimal YAML change**

In `allInOne_v2.yaml`, replace the `AttributeDefinitions` and `KeySchema` of
`S3ReplicationFailureTable`:

```yaml
      AttributeDefinitions:
        - AttributeName: 'BucketRuleKey'
          AttributeType: 'S'
        - AttributeName: 'ObjectKeyVersionId'
          AttributeType: 'S'
      KeySchema:
        - AttributeName: 'BucketRuleKey'
          KeyType: 'HASH'
        - AttributeName: 'ObjectKeyVersionId'
          KeyType: 'RANGE'
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m pytest tests/test_remediation.py::test_attribute_definitions_match_key_schema -p no:asyncio -v`
Expected: PASS

- [ ] **Step 5: Run cfn-lint**

Run: `cfn-lint allInOne_v2.yaml; echo $?`
Expected: `0`

- [ ] **Step 6: Commit**

```bash
git add allInOne_v2.yaml tests/test_remediation.py
git commit -m "feat: composite BucketRuleKey partition key on failure table"
```

---

### Task 2: Ingestion writes the composite key

**Files:**
- Test: `tests/test_remediation.py`
- Modify: `allInOne_v2.yaml` (FailureIngestionLambda item dict, ~138-144)

- [ ] **Step 1: Write the failing test**

The existing harness has no ingestion-side runner (the ingestion Lambda reads an
SQS-wrapped S3 event). Add this self-contained test:

```python
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
    assert "ReplicationRuleId" in item  # retained as attribute
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m pytest tests/test_remediation.py::test_ingestion_writes_composite_key -p no:asyncio -v`
Expected: FAIL — `KeyError: 'BucketRuleKey'` (item has no such key yet).

- [ ] **Step 3: Make the minimal YAML change**

In `FailureIngestionLambda`, change the item dict (currently starts with
`'ReplicationRuleId': replication_rule_id,`) to:

```python
                  item = {
                      'BucketRuleKey': src_bucket_name + '|' + replication_rule_id,
                      'ObjectKeyVersionId': object_key_with_version,
                      'SRCBucketName': src_bucket_name,
                      'ReplicationRuleId': replication_rule_id,
                      'DSTBucketName' : dst_bucket_name,
                      'FailureReason': failure_reason
                  }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m pytest tests/test_remediation.py::test_ingestion_writes_composite_key -p no:asyncio -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add allInOne_v2.yaml tests/test_remediation.py
git commit -m "feat: ingestion writes composite BucketRuleKey"
```

---

### Task 3: Remediation query + projection use composite key

**Files:**
- Test: `tests/test_remediation.py`
- Modify: `allInOne_v2.yaml` (ProcessAndStartCopyFunction, ~310-315)

This task needs `FakeTable.query` to record the kwargs it was called with so the
test can assert on them. First extend the fake.

- [ ] **Step 1: Extend FakeTable to capture query kwargs**

In `tests/test_remediation.py`, modify `FakeTable.query` to record its kwargs.
Replace the existing `query` method with:

```python
    def query(self, **kwargs):
        self.last_query_kwargs = kwargs
        return {
            "Items": [
                {
                    "BucketRuleKey": "src#bucket|rule-1",
                    "ObjectKeyVersionId": "some/key.txt#v123",
                    "SRCBucketName": "src-bucket",
                }
            ]
        }
```

Note: the returned item now carries `BucketRuleKey` and `SRCBucketName`, which
later tasks rely on. (Old code only needed `ObjectKeyVersionId`.)

- [ ] **Step 2: Write the failing tests**

```python
def test_query_uses_composite_key():
    """Remediation queries by BucketRuleKey = 'SourceBucket|ReplicationRuleId'."""
    template = load_template()
    code = get_lambda_code(template, "ProcessAndStartCopyFunction")
    table = FakeTable()
    s3 = mock.Mock(); s3.put_object.return_value = {"ETag": '"e"'}
    s3control = mock.Mock(); s3control.create_job.return_value = {"JobId": "j"}
    fake_boto3 = make_fake_boto3(s3=s3, s3control=s3control,
                                 dynamodb_resource=FakeDDBResource(table))
    module = exec_lambda_module(code, PROCESS_ENV, fake_boto3)
    module.lambda_handler({"ReplicationRuleId": "rule-1",
                           "SourceBucket": "src-bucket"}, None)

    kw = table.last_query_kwargs
    assert kw["KeyConditionExpression"] == "BucketRuleKey = :key"
    assert kw["ExpressionAttributeValues"][":key"] == "src-bucket|rule-1"


def test_projection_includes_required_fields():
    """Projection must include BucketRuleKey, ObjectKeyVersionId, SRCBucketName."""
    template = load_template()
    code = get_lambda_code(template, "ProcessAndStartCopyFunction")
    table = FakeTable()
    s3 = mock.Mock(); s3.put_object.return_value = {"ETag": '"e"'}
    s3control = mock.Mock(); s3control.create_job.return_value = {"JobId": "j"}
    fake_boto3 = make_fake_boto3(s3=s3, s3control=s3control,
                                 dynamodb_resource=FakeDDBResource(table))
    module = exec_lambda_module(code, PROCESS_ENV, fake_boto3)
    module.lambda_handler({"ReplicationRuleId": "rule-1",
                           "SourceBucket": "src-bucket"}, None)

    proj = table.last_query_kwargs["ProjectionExpression"]
    for field in ("BucketRuleKey", "ObjectKeyVersionId", "SRCBucketName"):
        assert field in proj
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `python3 -m pytest tests/test_remediation.py::test_query_uses_composite_key tests/test_remediation.py::test_projection_includes_required_fields -p no:asyncio -v`
Expected: FAIL — KeyConditionExpression is still `ReplicationRuleId = :id`,
projection lacks `BucketRuleKey`/`SRCBucketName`.

- [ ] **Step 4: Make the minimal YAML change**

In `ProcessAndStartCopyFunction`, replace the `kwargs` block:

```python
              # Query the table by composite key
              composite_key = src_bucket + '|' + replication_rule
              kwargs = {
                  'KeyConditionExpression': 'BucketRuleKey = :key',
                  'ExpressionAttributeValues': {':key': composite_key},
                  'ProjectionExpression': 'BucketRuleKey, ObjectKeyVersionId, SRCBucketName'
              }
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `python3 -m pytest tests/test_remediation.py::test_query_uses_composite_key tests/test_remediation.py::test_projection_includes_required_fields -p no:asyncio -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add allInOne_v2.yaml tests/test_remediation.py
git commit -m "feat: remediation queries by composite BucketRuleKey"
```

---

### Task 4: Data CSV uses stored SRCBucketName + collision-safe object keys

**Files:**
- Test: `tests/test_remediation.py`
- Modify: `allInOne_v2.yaml` (ProcessAndStartCopyFunction, ~353-357 and the
  `s3_file_key` / `s3_file_key_to_delete` assignments)

- [ ] **Step 1: Write the failing tests**

```python
def test_csv_uses_stored_source_bucket():
    """Data manifest rows are stamped with the record's SRCBucketName, not the
    execution-time SourceBucket."""
    template = load_template()
    code = get_lambda_code(template, "ProcessAndStartCopyFunction")
    table = FakeTable()
    s3 = mock.Mock(); s3.put_object.return_value = {"ETag": '"e"'}
    s3control = mock.Mock(); s3control.create_job.return_value = {"JobId": "j"}
    fake_boto3 = make_fake_boto3(s3=s3, s3control=s3control,
                                 dynamodb_resource=FakeDDBResource(table))
    module = exec_lambda_module(code, PROCESS_ENV, fake_boto3)
    # Pass a DIFFERENT execution SourceBucket than the record's SRCBucketName
    module.lambda_handler({"ReplicationRuleId": "rule-1",
                           "SourceBucket": "wrong-bucket"}, None)

    # First put_object is the data manifest CSV
    body = s3.put_object.call_args_list[0].kwargs["Body"]
    assert "src-bucket" in body         # the record's stored SRCBucketName
    assert "wrong-bucket" not in body   # NOT the execution input


def test_csv_object_keys_are_bucket_rule_scoped():
    """Manifest and delete-list S3 object keys include the bucket so two buckets
    sharing a rule ID don't overwrite each other."""
    template = load_template()
    code = get_lambda_code(template, "ProcessAndStartCopyFunction")
    table = FakeTable()
    s3 = mock.Mock(); s3.put_object.return_value = {"ETag": '"e"'}
    s3control = mock.Mock(); s3control.create_job.return_value = {"JobId": "j"}
    fake_boto3 = make_fake_boto3(s3=s3, s3control=s3control,
                                 dynamodb_resource=FakeDDBResource(table))
    module = exec_lambda_module(code, PROCESS_ENV, fake_boto3)
    module.lambda_handler({"ReplicationRuleId": "rule-1",
                           "SourceBucket": "src-bucket"}, None)

    keys = [c.kwargs["Key"] for c in s3.put_object.call_args_list]
    # both keys must contain the bucket name, not be named by rule alone
    assert all("src-bucket" in k for k in keys), keys
    assert "rule-1.csv" not in keys
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python3 -m pytest tests/test_remediation.py::test_csv_uses_stored_source_bucket tests/test_remediation.py::test_csv_object_keys_are_bucket_rule_scoped -p no:asyncio -v`
Expected: FAIL — rows use `wrong-bucket`; object keys are `rule-1.csv` /
`rule-1_delete.csv`.

- [ ] **Step 3: Make the minimal YAML changes**

3a. In the data-row loop, use the record's `SRCBucketName` for the manifest row
(keep the to_delete row change for Task 5). Replace the loop body:

```python
              for item in all_items:
                  object_key, version_id = item.get('ObjectKeyVersionId', '').split('#', 1)
                  row_bucket = item.get('SRCBucketName', '')
                  unique_rows_csv.add((row_bucket, object_key, version_id))
                  unique_rows_to_delete_csv.add((item.get('ReplicationRuleId', ''), item.get('ObjectKeyVersionId', '')))
```

3b. Change the S3 object-key names. Find:

```python
              s3_file_key = replication_rule + ".csv"
```
and the matching `s3_file_key_to_delete = replication_rule +"_delete"+".csv"`.
Replace both with composite-scoped, filename-safe keys:

```python
              # Filename-safe composite scope so two buckets sharing a rule id
              # do not overwrite each other's manifests.
              file_scope = src_bucket + "__" + replication_rule
              s3_file_key = file_scope + ".csv"
              s3_file_key_to_delete = file_scope + "_delete.csv"
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python3 -m pytest tests/test_remediation.py::test_csv_uses_stored_source_bucket tests/test_remediation.py::test_csv_object_keys_are_bucket_rule_scoped -p no:asyncio -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add allInOne_v2.yaml tests/test_remediation.py
git commit -m "feat: collision-safe CSV keys + stored source bucket in manifest"
```

---

### Task 5: to_delete CSV + DeleteDynamoDBRecords use composite key

**Files:**
- Test: `tests/test_remediation.py`
- Modify: `allInOne_v2.yaml` (ProcessAndStartCopyFunction to_delete row ~357;
  DeleteDynamoDBRecordsFunction item dict ~571-574)

- [ ] **Step 1: Write the failing tests**

```python
def test_to_delete_csv_uses_composite_key():
    """The to_delete CSV first column is BucketRuleKey, not ReplicationRuleId."""
    template = load_template()
    code = get_lambda_code(template, "ProcessAndStartCopyFunction")
    table = FakeTable()
    s3 = mock.Mock(); s3.put_object.return_value = {"ETag": '"e"'}
    s3control = mock.Mock(); s3control.create_job.return_value = {"JobId": "j"}
    fake_boto3 = make_fake_boto3(s3=s3, s3control=s3control,
                                 dynamodb_resource=FakeDDBResource(table))
    module = exec_lambda_module(code, PROCESS_ENV, fake_boto3)
    module.lambda_handler({"ReplicationRuleId": "rule-1",
                           "SourceBucket": "src-bucket"}, None)

    # Second put_object is the to_delete CSV
    to_delete_body = s3.put_object.call_args_list[1].kwargs["Body"]
    # FakeTable returns BucketRuleKey "src#bucket|rule-1"
    assert "src#bucket|rule-1" in to_delete_body


def test_delete_records_uses_composite_key():
    """DeleteDynamoDBRecords builds its delete Key with BucketRuleKey."""
    template = load_template()
    code = get_lambda_code(template, "DeleteDynamoDBRecordsFunction")

    captured = {}
    class DelTable:
        def batch_writer(self):
            table = self
            class W:
                def __enter__(self_): return self_
                def __exit__(self_, *a): return False
                def delete_item(self_, Key): captured.setdefault("keys", []).append(Key)
            return W()
    fake_boto3 = make_fake_boto3(dynamodb_resource=type("R", (), {"Table": lambda self, n: DelTable()})())
    # S3 returns a 2-column CSV: BucketRuleKey, ObjectKeyVersionId
    s3 = mock.Mock()
    s3.get_object.return_value = {
        "Body": type("B", (), {"read": lambda self: b"bucket-a|rule-1,key#v1\n"})()
    }
    fake_boto3.client = lambda name, *a, **k: s3 if name == "s3" else None

    module = exec_lambda_module(code, {}, fake_boto3)
    module.lambda_handler({"s3_bucket": "csv", "s3_file_key_to_delete": "f",
                           "table_name": "t"}, None)

    assert captured["keys"][0] == {"BucketRuleKey": "bucket-a|rule-1",
                                   "ObjectKeyVersionId": "key#v1"}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python3 -m pytest tests/test_remediation.py::test_to_delete_csv_uses_composite_key tests/test_remediation.py::test_delete_records_uses_composite_key -p no:asyncio -v`
Expected: FAIL — to_delete row still uses `ReplicationRuleId`; delete builds
`{'ReplicationRuleId': ...}`.

- [ ] **Step 3: Make the minimal YAML changes**

3a. In `ProcessAndStartCopyFunction`, change the to_delete row to use
`BucketRuleKey`:

```python
                  unique_rows_to_delete_csv.add((item.get('BucketRuleKey', ''), item.get('ObjectKeyVersionId', '')))
```

3b. In `DeleteDynamoDBRecordsFunction`, change the item dict built from each CSV
row:

```python
                      item = {
                          'BucketRuleKey': row[0],
                          'ObjectKeyVersionId': row[1]
                      }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python3 -m pytest tests/test_remediation.py::test_to_delete_csv_uses_composite_key tests/test_remediation.py::test_delete_records_uses_composite_key -p no:asyncio -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add allInOne_v2.yaml tests/test_remediation.py
git commit -m "feat: delete path addresses composite BucketRuleKey"
```

---

### Task 6: Fail loud on missing remediation inputs

**Files:**
- Test: `tests/test_remediation.py`
- Modify: `allInOne_v2.yaml` (ProcessAndStartCopyFunction, start of `lambda_handler` ~298)

- [ ] **Step 1: Write the failing test**

```python
def test_missing_source_bucket_fails_loud():
    """Missing/empty SourceBucket or ReplicationRuleId raises, rather than
    building a 'None|None' key and silently querying nothing."""
    template = load_template()
    code = get_lambda_code(template, "ProcessAndStartCopyFunction")
    table = FakeTable()
    s3 = mock.Mock(); s3.put_object.return_value = {"ETag": '"e"'}
    s3control = mock.Mock(); s3control.create_job.return_value = {"JobId": "j"}
    fake_boto3 = make_fake_boto3(s3=s3, s3control=s3control,
                                 dynamodb_resource=FakeDDBResource(table))
    module = exec_lambda_module(code, PROCESS_ENV, fake_boto3)

    import pytest as _pytest
    with _pytest.raises(Exception):
        module.lambda_handler({"ReplicationRuleId": "rule-1"}, None)  # no SourceBucket
    with _pytest.raises(Exception):
        module.lambda_handler({"SourceBucket": "src-bucket"}, None)   # no rule
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m pytest tests/test_remediation.py::test_missing_source_bucket_fails_loud -p no:asyncio -v`
Expected: FAIL — no validation; handler proceeds (does not raise).

- [ ] **Step 3: Make the minimal YAML change**

In `ProcessAndStartCopyFunction`, right after the two `event.get(...)` lines at
the top of `lambda_handler`, add validation:

```python
          def lambda_handler(event, context):
              replication_rule = event.get('ReplicationRuleId')
              src_bucket = event.get('SourceBucket')

              if not replication_rule or not src_bucket:
                  raise ValueError("ReplicationRuleId and SourceBucket are required")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m pytest tests/test_remediation.py::test_missing_source_bucket_fails_loud -p no:asyncio -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add allInOne_v2.yaml tests/test_remediation.py
git commit -m "feat: validate remediation inputs (fail loud)"
```

---

### Task 7: End-to-end no-cross-contamination + update happy path

**Files:**
- Test: `tests/test_remediation.py`
- Modify: `tests/test_remediation.py` (existing happy-path test only)

- [ ] **Step 1: Write the cross-contamination test**

This test proves the data model isolates buckets sharing a rule ID. Use a fake
table that holds two buckets' records under composite keys and only returns the
queried partition.

```python
def test_no_cross_bucket_contamination():
    """Two buckets share rule 'r'. Querying bucket A returns only A's objects."""
    template = load_template()
    code = get_lambda_code(template, "ProcessAndStartCopyFunction")

    class TwoBucketTable:
        DATA = {
            "bucket-a|r": [{"BucketRuleKey": "bucket-a|r",
                            "ObjectKeyVersionId": "a-obj#v1",
                            "SRCBucketName": "bucket-a"}],
            "bucket-b|r": [{"BucketRuleKey": "bucket-b|r",
                            "ObjectKeyVersionId": "b-obj#v1",
                            "SRCBucketName": "bucket-b"}],
        }
        def query(self, **kwargs):
            key = kwargs["ExpressionAttributeValues"][":key"]
            return {"Items": self.DATA.get(key, [])}

    s3 = mock.Mock(); s3.put_object.return_value = {"ETag": '"e"'}
    s3control = mock.Mock(); s3control.create_job.return_value = {"JobId": "j"}
    fake_boto3 = make_fake_boto3(s3=s3, s3control=s3control,
                                 dynamodb_resource=FakeDDBResource(TwoBucketTable()))
    module = exec_lambda_module(code, PROCESS_ENV, fake_boto3)
    module.lambda_handler({"ReplicationRuleId": "r",
                           "SourceBucket": "bucket-a"}, None)

    manifest_body = s3.put_object.call_args_list[0].kwargs["Body"]
    assert "a-obj" in manifest_body
    assert "b-obj" not in manifest_body          # B never leaks into A's run
    assert "bucket-b" not in manifest_body
```

- [ ] **Step 2: Run test to verify it fails or passes**

Run: `python3 -m pytest tests/test_remediation.py::test_no_cross_bucket_contamination -p no:asyncio -v`
Expected: PASS (the composite-key changes from Tasks 3-4 already make this hold).
If it FAILS, a prior task is incomplete — fix that task, do not weaken this test.

- [ ] **Step 3: Update the existing happy-path test for the new schema**

The existing `test_happy_path_returns_job_id_for_downstream` uses the old
`FakeTable`. After Task 3's `FakeTable.query` change it returns `BucketRuleKey` +
`SRCBucketName`, so the happy-path test should still pass unchanged. Run it:

Run: `python3 -m pytest tests/test_remediation.py::test_happy_path_returns_job_id_for_downstream -p no:asyncio -v`
Expected: PASS. If it fails because it asserts on `s3_file_key_to_delete` value,
update that assertion to the new composite name
(`"src-bucket__rule-1_delete.csv"` given `PROCESS_ENV` and inputs).

- [ ] **Step 4: Run the FULL suite + cfn-lint**

Run: `python3 -m pytest tests/test_remediation.py -p no:asyncio -v`
Expected: ALL PASS (the 5 original tests + all new composite-key tests).
Run: `cfn-lint allInOne_v2.yaml; echo $?`
Expected: `0`

- [ ] **Step 5: Commit**

```bash
git add tests/test_remediation.py
git commit -m "test: end-to-end no cross-bucket contamination + happy-path schema update"
```

---

### Task 8: Real-AWS verification (multi-bucket, shared rule id)

This proves the data model on live AWS, not just mocks. Per the verification
matrix rule: enumerate everything to check, run in one provisioning, tear down once.

- [ ] **Step 1: Provision** — create two source buckets (`mb-a`, `mb-b`) in
  us-west-2, each with versioning, a replication rule that **shares the same rule
  ID** (e.g. `shared-rule`) to its own destination, **with Replication metrics
  enabled** (hard prerequisite). Create a CSV bucket. Deploy one stack
  (`SourceBucket` left empty; wire both buckets' notifications manually to the
  one queue).

- [ ] **Step 2: Cause failures both buckets** — Deny the replication role on both
  destinations; upload an object to each of `mb-a` and `mb-b`.

- [ ] **Step 3: Verify ingestion isolation** — scan DDB; confirm two items with
  `BucketRuleKey` = `mb-a|shared-rule` and `mb-b|shared-rule` respectively.

- [ ] **Step 4: Remediate ONE bucket** — remove both Denies; run the Step Function
  with `{ReplicationRuleId: "shared-rule", SourceBucket: "mb-a"}`. Confirm the
  batch job manifest contains ONLY mb-a's object, mb-a's object becomes
  `COMPLETED`, and ONLY the `mb-a|shared-rule` DDB record is deleted
  (`mb-b|shared-rule` remains). This is the cross-contamination proof on live AWS.

- [ ] **Step 5: Tear down** — delete stack, both source buckets (with
  versions/markers), CSV bucket, replication role; restore any borrowed policies;
  confirm region clean.

- [ ] **Step 6: Document the result** in the PR / commit message with the observed
  DDB keys and which record was deleted.

---

## Final verification checklist (run before marking complete)

- [ ] `python3 -m pytest tests/test_remediation.py -p no:asyncio -v` → all pass
- [ ] `cfn-lint allInOne_v2.yaml` → exit 0
- [ ] No remaining `ReplicationRuleId` used as a *key* anywhere (grep: it should
  only appear as a stored attribute in the ingestion item dict and as an
  ExpressionAttributeValue name nowhere)
- [ ] Real-AWS Task 8 evidence recorded
