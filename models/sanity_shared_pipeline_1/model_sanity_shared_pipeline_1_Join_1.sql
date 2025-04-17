{{
  config({    
    "materialized": "table",
    "alias": "prophecy__temp_sanity_shared_pipeline_1_pre_filter_by_count_1",
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

Join_1 AS (

  SELECT 
    in1.c_tinyint AS c_tinyint,
    in1.c_smallint AS c_smallint,
    in1.c_int AS c_int,
    in0.c_bigint AS c_bigint,
    in1.c_float AS c_float,
    in1.c_double AS c_double,
    in1.c_string AS c_string,
    in1.c_boolean AS c_boolean,
    in1.c_array AS c_array,
    in0.c_struct AS c_struct
  
  FROM sanity_simple_model_1_1 AS in0
  INNER JOIN all_type_parquet AS in1
     ON in0.c_string != in1.c_struct.state

)

SELECT *

FROM Join_1
