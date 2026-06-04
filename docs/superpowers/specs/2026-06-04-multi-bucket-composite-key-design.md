# Multi-bucket / multi-rule remediation via composite key — Design

Date: 2026-06-04
Status: Approved (pending written-spec review)

## Goal

Let a single deployed stack correctly serve **multiple source buckets and multiple
replication rules**, with remediation triggered precisely per **bucket + rule**.

Today a single stack can already *ingest* failures from many buckets (they all feed
one central SQS queue), but the remediation data model is unsafe when buckets share
a replication-rule ID. This design fixes that.

## Background: the defect being fixed

Two facts in the current code combine into a data-corruption risk:

1. The DynamoDB partition key is `ReplicationRuleId` **alone**
   (`S3ReplicationFailureTable`, ingestion writes it at the item's `ReplicationRuleId`).
2. The remediation CSV stamps every object with the `SourceBucket` passed at
   **execution time**, not the `SRCBucketName` stored on each record
   (`ProcessAndStartCopy`).

Consequence: if two different source buckets use the same replication-rule ID
(common when rule IDs are hand-named, e.g. `Rule1` / `replicate-all`), their
failures land in the **same partition**. A remediation run for that rule queries
both buckets' objects and stamps them all with one bucket name — so the batch job
looks for B's keys under A's bucket: missing objects or wrong-object replication.

## Decisions (confirmed with user)

- **Remediation granularity:** per `bucket + rule` (precise).
- **DDB partition key:** composite `SRCBucket | ReplicationRuleId`.
- **Ingestion side:** keep the single central SQS queue + DLQ (no throttling added).
- **Discovering what needs remediation:** operator queries DynamoDB manually (no list tool).
- **Backward compatibility:** none — no migration of existing single-key data.
- **Trigger input signature:** unchanged — still `{ReplicationRuleId, SourceBucket}`.

## Separator choice

The composite key uses `|` (pipe), **not** `#`. `#` is already used inside
`ObjectKeyVersionId` (`objectkey#versionId`) and reused by the delete path's
`split('#', 1)`.

Crucially, **`BucketRuleKey` is treated as an opaque string**: it is built by
concatenation on write and on query, and is **never parsed/split** back into its
parts. This is what makes the separator choice safe regardless of what characters
a replication rule ID may contain — we do not rely on any rule-ID charset
assumption. (`|` is merely a readable, S3-bucket-name-safe joiner; correctness
does not depend on it being absent from rule IDs, because we never split on it.)

## Changes (4 components + 1 collision fix)

All changes are inside the embedded Lambda code / table definition in
`allInOne_v2.yaml`. No structural change to the state machine, IAM, or the
trigger signature. The four key-touching sites are §1–§4; §3b is the related
S3-object-key collision fix found in review.

### 1. `S3ReplicationFailureTable` — partition key
- Partition (HASH) key attribute: `ReplicationRuleId` → `BucketRuleKey` (String).
- Sort (RANGE) key: `ObjectKeyVersionId` — unchanged.
- **`AttributeDefinitions` must drop the old `ReplicationRuleId` entry and add
  `BucketRuleKey`.** DynamoDB rejects an `AttributeDefinitions` that lists an
  attribute not used by a key or index, so leaving `ReplicationRuleId` there
  would fail stack creation. (`ReplicationRuleId` remains a stored *attribute*
  on items — it just must not appear in `AttributeDefinitions`.)

### 2. `FailureIngestionLambda` — write composite key
The source bucket name (`src_bucket_name`) is already extracted from the event,
so there is no extra data fetch. The written item becomes:
```python
item = {
    'BucketRuleKey': src_bucket_name + '|' + replication_rule_id,  # new HASH key
    'ObjectKeyVersionId': object_key_with_version,
    'SRCBucketName': src_bucket_name,        # retained — used by remediation CSV
    'ReplicationRuleId': replication_rule_id, # retained — readability / debugging
    'DSTBucketName': dst_bucket_name,
    'FailureReason': failure_reason,
    ...
}
```

### 3. `ProcessAndStartCopy` — query + both CSVs
- **Validate input first (fail loud):** if `SourceBucket` or `ReplicationRuleId`
  is missing/empty, raise immediately — otherwise we would build a `None|None`
  key and silently query nothing. The Step Functions `Catch` routes the raise to
  `JobFailed`.
- Build the key: `key = src_bucket + '|' + replication_rule` from the event input.
- `KeyConditionExpression`: `ReplicationRuleId = :id` → `BucketRuleKey = :key`.
- **Projection must include `BucketRuleKey, ObjectKeyVersionId, SRCBucketName`**
  (currently it projects `ReplicationRuleId, ObjectKeyVersionId`). Without
  `SRCBucketName` the data-CSV fix below cannot work; without `BucketRuleKey` the
  to_delete CSV cannot address the new primary key. `ReplicationRuleId` projection
  is now optional (debug only).
- **Data manifest CSV:** stamp each row with the record's stored `SRCBucketName`,
  not the execution-time `SourceBucket`. (For a single bucket+rule run these are
  identical, but using the stored value is correct by construction.)
- **to_delete CSV:** write `(BucketRuleKey, ObjectKeyVersionId)` instead of
  `(ReplicationRuleId, ObjectKeyVersionId)`, so the delete step can address the
  new primary key.

### 3b. `ProcessAndStartCopy` — S3 CSV object-key collision (BLOCKER)
The manifest and delete-list S3 object keys are currently named by rule alone:
`s3_file_key = replication_rule + ".csv"`,
`s3_file_key_to_delete = replication_rule + "_delete.csv"`. Two buckets sharing
a rule ID would write to the **same S3 object**, so concurrent/overlapping
remediation runs overwrite each other's manifest — the same single-rule-id root
cause as the DDB defect, recurring at the S3-filename layer.

Fix: name these S3 keys by the composite identity. Use a filename-safe form of
the composite key (replace `|` with a filename-safe separator, e.g.
`f"{src_bucket}__{replication_rule}.csv"` and `..._delete.csv"`). This keeps each
bucket+rule run isolated in S3.

### 4. `DeleteDynamoDBRecords` — delete by composite key
The delete key read from the to_delete CSV changes:
```python
item = {
    'BucketRuleKey': row[0],        # was 'ReplicationRuleId'
    'ObjectKeyVersionId': row[1]
}
```
This was the hidden dependency of the partition-key change: without it the
remediation succeeds but records are never deleted, and the table grows unbounded.

## Data flow (after change)

```
failure event (carries source bucket + rule) → FailureIngestionLambda
   → DDB item, HASH = "bucket|rule"
                                          ↓
trigger: start-execution {ReplicationRuleId, SourceBucket}
   → ProcessAndStartCopy: key = "SourceBucket|ReplicationRuleId"
       → Query that exact bucket+rule's failures
       → data CSV stamped with each record's SRCBucketName
       → to_delete CSV = (BucketRuleKey, ObjectKeyVersionId)
   → batch job → CheckCopyStatus (poll) → DeleteDynamoDBRecords (by composite key)
```

## Why this eliminates cross-contamination

- Two buckets with the same rule ID now hash to **different partitions**
  (`A|rule` vs `B|rule`).
- Remediation queries one composite key, so it can only retrieve the target
  bucket's objects.
- The data CSV uses each record's own `SRCBucketName`, so an object is never
  stamped with the wrong bucket.

## Testing (TDD, existing YAML-extraction harness)

New / updated tests in `tests/test_remediation.py`:
- `test_ingestion_writes_composite_key` — ingested item's HASH is `bucket|rule`.
- `test_query_uses_composite_key` — remediation Query uses `BucketRuleKey`.
- `test_projection_includes_required_fields` — Query projection contains
  `BucketRuleKey, ObjectKeyVersionId, SRCBucketName`.
- `test_csv_uses_stored_source_bucket` — data CSV source column comes from the
  record's `SRCBucketName`, not the execution input.
- `test_csv_object_keys_are_bucket_rule_scoped` — the S3 manifest / delete-list
  object keys include the bucket, so two buckets sharing a rule ID don't collide.
- `test_to_delete_csv_uses_composite_key` — the to_delete CSV first column is
  `BucketRuleKey`.
- `test_delete_records_uses_composite_key` — DeleteDynamoDBRecords builds its
  delete Key with `BucketRuleKey`, not `ReplicationRuleId`.
- `test_missing_source_bucket_fails_loud` — remediation with missing/empty
  `SourceBucket` or `ReplicationRuleId` raises rather than querying `None|None`.
- `test_no_cross_bucket_contamination` — two buckets sharing a rule ID: remediating
  bucket A never pulls bucket B's objects (end-to-end of the data model).
- `test_attribute_definitions_match_key_schema` — table `AttributeDefinitions`
  contains `BucketRuleKey` and not the dropped `ReplicationRuleId`.
- Existing happy-path test updated for the new table schema.

Plus `cfn-lint allInOne_v2.yaml` must stay exit 0.

## Out of scope (YAGNI)

- Ingestion-side throttling / per-bucket concurrency (single queue + DLQ kept).
- A "list pending bucket+rule combinations" tool (operator queries DDB directly).
- Backward compatibility / migration of old single-key records.
- GSI for bucket-dimension queries.
- Batch-job report `Prefix: 'report/'` is shared across runs (NIT). Reports are
  already separated into per-job subdirectories by S3, so this is cosmetic; not
  changed here.
- Heavy input validation on the ingestion side beyond the existing
  `if not event_record: continue` guard (only the remediation-trigger inputs are
  validated, per §3).
- IAM scoping changes, multi-region, multi-BU (tracked separately).
