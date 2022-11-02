CREATE EXTERNAL TABLE qa_table (
  foo string,
  bar string
)
PARTITIONED BY (dt string)
ROW FORMAT  serde 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://georesearch-datalake/QA/cannibalization/qa_table/'
-- s3://georesearch-datalake/PROD/athena_processing/prod_inputs_estudios/alsea_ar_ar_competencias_final_b2500/
TBLPROPERTIES ('has_encrypted_data'='true');