{{
  config({    
    "database":  var('PROJECT_DATABASE') ,
    "schema":  var('PROJECT_SCHEMA_DB_WAREHOUSE') ,
    "tags": ["tag1", "tag2"]
  })
}}

{% set MODEL_SCHEMA_DB_WAREHOUSE = "qa_db_warehouse" %}
{% set MODEL_SCHEMA = "qa_schema" %}
{% set MODEL_DATABASE = "hive_metastore" %}
{% set MODEL_ALIAS = "env_uitesting_shared_child_model_1" %}

WITH env_uitesting_shared_parent_model_1 AS (

  SELECT * 
  
  FROM {{ ref('env_uitesting_shared_parent_model_1')}}

),

Limit_1 AS (

  SELECT * 
  
  FROM env_uitesting_shared_parent_model_1 AS in0
  
  LIMIT 10

),

env_uitesting_shared_mid_model_1 AS (

  SELECT * 
  
  FROM {{ ref('env_uitesting_shared_mid_model_1')}}

),

Reformat_1 AS (

  SELECT * 
  
  FROM env_uitesting_shared_mid_model_1 AS in0

),

Limit_2 AS (

  SELECT * 
  
  FROM Reformat_1 AS in0
  
  LIMIT 10

),

Join_1 AS (

  SELECT 
    in0.c_tinyint AS c_tinyint,
    in0.c_smallint AS c_smallint,
    in0.c_int AS c_int,
    in0.c_bigint AS c_bigint,
    in0.c_float AS c_float,
    in0.c_double AS c_double,
    in0.c_string AS c_string,
    in0.c_boolean AS c_boolean,
    in0.c_array AS c_array,
    in0.c_struct AS c_struct
  
  FROM Limit_2 AS in0
  INNER JOIN Limit_1 AS in1
     ON in0.c_bigint = in1.c_bigint

)

SELECT 
  Join_1.c_tinyint,
  Join_1.c_smallint,
  Join_1.c_int,
  Join_1.c_bigint,
  Join_1.c_float,
  Join_1.c_double,
  Join_1.c_string,
  Join_1.c_boolean,
  Join_1.c_array,
  Join_1.c_struct,
  concat(
    Join_1.c_array[0], 
    Join_1.c_struct.city, 
    {{ SQL_DatabricksSharedBasic.qa_concat_function_main('c_string', 'c_boolean') }}) AS c_test

FROM Join_1

WHERE c_int != (
        (
          SELECT count(*)
          
          FROM Limit_2
         )
        + (
            SELECT count(*)
            
            FROM env_uitesting_shared_parent_model_1
           )
        + (
            SELECT count(*)
            
            FROM Limit_1
           )
      )
