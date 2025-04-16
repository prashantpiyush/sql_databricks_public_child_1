{{
  config({    
    "materialized": "table",
    "alias": "prophecy__temp_sanity_shared_pipeline_1_post_SQLStatement_1_0",
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
  
  FROM {{ source('prophecy__temp_sanity_shared_pipeline_1_source', 'prophecy__temp_sanity_shared_pipeline_1_post_Limit_2_0') }}

),

SQLStatement_1 AS (

  SELECT *
  
  FROM all_type_parquet
  
  WHERE c_int != (
          (SELECT count(*)
          
          FROM Limit_2)
         )

)

SELECT *

FROM SQLStatement_1
