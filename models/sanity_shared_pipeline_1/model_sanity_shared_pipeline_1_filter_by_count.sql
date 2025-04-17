{{
  config({    
    "materialized": "table",
    "alias": "prophecy__temp_sanity_shared_pipeline_1_post_filter_by_count_0",
    "database": "hive_metastore",
    "schema": "qa_db_warehouse"
  })
}}

WITH RestAPI_1 AS (

  SELECT * 
  
  FROM {{ source('prophecy__temp_sanity_shared_pipeline_1_source', 'prophecy__temp_sanity_shared_pipeline_1_pre_filter_by_count_3') }}

),

MultiColumnRename_1 AS (

  SELECT * 
  
  FROM {{ source('prophecy__temp_sanity_shared_pipeline_1_source', 'prophecy__temp_sanity_shared_pipeline_1_pre_filter_by_count_2') }}

),

DynamicSelect_1 AS (

  SELECT * 
  
  FROM {{ source('prophecy__temp_sanity_shared_pipeline_1_source', 'prophecy__temp_sanity_shared_pipeline_1_pre_filter_by_count_0') }}

),

DataCleansing_1 AS (

  SELECT * 
  
  FROM {{ source('prophecy__temp_sanity_shared_pipeline_1_source', 'prophecy__temp_sanity_shared_pipeline_1_pre_filter_by_count_4') }}

),

Join_1 AS (

  SELECT * 
  
  FROM {{ source('prophecy__temp_sanity_shared_pipeline_1_source', 'prophecy__temp_sanity_shared_pipeline_1_pre_filter_by_count_1') }}

),

filter_by_count AS (

  SELECT *
  
  FROM Join_1
  
  WHERE c_int != (
          (
            SELECT count(*)
            
            FROM DataCleansing_1
           )
          + (
              SELECT count(*)
              
              FROM DynamicSelect_1
             )
          + (
              SELECT count(*)
              
              FROM RestAPI_1
             )
          + (
              SELECT count(*)
              
              FROM MultiColumnRename_1
             )
        )

)

SELECT *

FROM filter_by_count
