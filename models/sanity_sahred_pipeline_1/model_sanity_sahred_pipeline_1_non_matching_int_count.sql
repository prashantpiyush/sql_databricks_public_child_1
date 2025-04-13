{{
  config({    
    "materialized": "table",
    "alias": "prophecy__temp_sanity_sahred_pipeline_1_post_non_matching_int_count_0",
    "database": "hive_metastore",
    "schema": "qa_db_warehouse"
  })
}}

WITH shared_seed_basic AS (

  SELECT * 
  
  FROM {{ ref('shared_seed_basic')}}

),

industry_statistics_csv AS (

  SELECT * 
  
  FROM {{ source('prophecy__temp_sanity_sahred_pipeline_1_source', 'prophecy__temp_sanity_sahred_pipeline_1_pre_non_matching_int_count_1') }}

),

env_uitesting_shared_useallmodel_1_1 AS (

  SELECT * 
  
  FROM {{ ref('env_uitesting_shared_useallmodel_1')}}

),

S_MSSQLALL AS (

  SELECT * 
  
  FROM {{ source('prophecy__temp_sanity_sahred_pipeline_1_source', 'prophecy__temp_sanity_sahred_pipeline_1_pre_non_matching_int_count_0') }}

),

all_type_non_partitioned AS (

  SELECT * 
  
  FROM {{ source('hive_metastore.qa_db_warehouse', 'all_type_non_partitioned') }}

),

non_matching_int_count AS (

  SELECT *
  
  FROM all_type_non_partitioned
  
  WHERE c_int != (
          (
            SELECT count(*)
            
            FROM S_MSSQLALL
           )
          + (
              SELECT count(*)
              
              FROM industry_statistics_csv
             )
          + (
              SELECT count(*)
              
              FROM env_uitesting_shared_useallmodel_1_1
             )
          + (
              SELECT count(*)
              
              FROM shared_seed_basic
             )
        )

)

SELECT *

FROM non_matching_int_count
