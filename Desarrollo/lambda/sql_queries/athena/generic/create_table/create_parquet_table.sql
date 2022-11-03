CREATE TABLE IF NOT EXISTS "{{target_db}}"."{{table_name}}"
    WITH (
        format='parquet',
        external_location='{{s3_table_location}}',
        bucketed_by=ARRAY['block_id'],
        bucket_count=1
    ) AS
    {{sql_query}}