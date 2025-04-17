{{
  config({    
    "materialized": "table",
    "alias": "prophecy__temp_sanity_working_pipeline_shared_db_1_post_Limit_2_0",
    "database": "hive_metastore",
    "schema": "qa_db_warehouse"
  })
}}

WITH sanity_simple_model_1_1 AS (

  SELECT * 
  
  FROM {{ ref('sanity_simple_model_1')}}

),

all_type_parquet AS (

  SELECT * 
  
  FROM {{ source('spark_catalog.qa_database', 'all_type_parquet') }}

),

SQLStatement_1 AS (

  SELECT *
  
  FROM all_type_parquet
  
  WHERE c_int != (
          (SELECT count(*)
          
          FROM sanity_simple_model_1_1)
         )

),

Limit_2 AS (

  SELECT * 
  
  FROM SQLStatement_1 AS in0
  
  LIMIT 2

)

SELECT *

FROM Limit_2
