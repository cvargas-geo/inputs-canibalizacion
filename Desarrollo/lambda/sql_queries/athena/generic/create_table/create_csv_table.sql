CREATE TABLE IF NOT EXISTS "{{params.target_db}}"."{{params.table_name}}"
    WITH (
        format='TEXTFILE',
        field_delimiter = ';',
        external_location='{{params.s3_table_location}}',
        bucketed_by=ARRAY['geo_id'],
        bucket_count=1
    ) AS
    {{params.sql_query}}