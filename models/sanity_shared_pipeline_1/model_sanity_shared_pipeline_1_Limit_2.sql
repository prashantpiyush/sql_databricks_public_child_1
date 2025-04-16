{{
  config({    
    "materialized": "table",
    "alias": "prophecy__temp_sanity_shared_pipeline_1_post_Limit_2_0",
    "database": "hive_metastore",
    "schema": "qa_db_warehouse"
  })
}}

WITH all_type_parquet AS (

  SELECT * 
  
  FROM {{ source('spark_catalog.qa_database', 'all_type_parquet') }}

),

Limit_2 AS (

  SELECT * 
  
  FROM all_type_parquet AS in0
  
  LIMIT 10

)

SELECT *

FROM Limit_2
