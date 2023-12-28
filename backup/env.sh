ReplicationRuleId
ObjectKeyVersionId
s3_replication_records


account_id	269562551342
bucket_name	source-zhenyu
replication_role	arn:aws:iam::269562551342:role/s3_batch_replication
replication_rule	zy-replication-test
src_bucket	zhenyu-replication-source
table_name	s3_replication_records




# Mock replication failure by Deny replication role access

{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Deny",
            "Principal": {
                "AWS": "arn:aws:iam::269562551342:role/s3_replication"
            },
            "Action": "s3:*",
            "Resource": [
                "arn:aws:s3:::zhenyu-replication-dst",
                "arn:aws:s3:::zhenyu-replication-dst/*"
            ]
        }
    ]
}