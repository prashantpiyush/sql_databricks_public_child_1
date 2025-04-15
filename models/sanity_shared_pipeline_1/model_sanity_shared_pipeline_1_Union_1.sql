{{
  config({    
    "materialized": "table",
    "alias": "prophecy__temp_sanity_shared_pipeline_1_pre_Intersect_1_0",
    "database": "hive_metastore",
    "schema": "qa_db_warehouse"
  })
}}

WITH all_type_parquet AS (

  SELECT * 
  
  FROM {{ source('spark_catalog.qa_database', 'all_type_parquet') }}

),

Join_1 AS (

  SELECT * 
  
  FROM {{ source('prophecy__temp_sanity_shared_pipeline_1_source', 'prophecy__temp_sanity_shared_pipeline_1_pre_Union_1_0') }}

),

Union_1 AS (

  SELECT * 
  
  FROM Join_1 AS in0
  
  UNION
  
  SELECT * 
  
  FROM all_type_parquet AS in1

)

SELECT *

FROM Union_1
