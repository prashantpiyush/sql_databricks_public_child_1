{% snapshot snapshot_sanity_test_1 %}
{{
  config({    
    "check_cols": [],
    "strategy": 'timestamp',
    "target_schema": 'QA_SCHEMA',
    "unique_key": 'c_int',
    "updated_at": "c_float"
  })
}}

{% set v_snapshot_int = 11 %}

WITH sanity_simple_model_1 AS (

  SELECT *
  
  FROM {{ ref('sanity_simple_model_1')}}

),

Reformat_1 AS (

  SELECT 
    c_tinyint AS c_tinyint,
    c_smallint AS c_smallint,
    c_int AS c_int,
    c_bigint AS c_bigint,
    c_float AS c_float,
    c_double AS c_double,
    concat(c_string, {{v_snapshot_int}}, {{ var("v_project_int") }}) AS c_string,
    c_boolean AS c_boolean,
    c_array AS c_array,
    c_struct AS c_struct
  
  FROM sanity_simple_model_1 AS in0

),

OrderBy_1 AS (

  SELECT *
  
  FROM Reformat_1 AS in0
  
  ORDER BY c_int ASC, c_string DESC

)

SELECT *

FROM OrderBy_1

{% endsnapshot %}
