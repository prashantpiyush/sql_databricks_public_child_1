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

  SELECT *
  
  FROM sanity_simple_model_1 AS in0

),

OrderBy_1 AS (

  SELECT *
  
  FROM Reformat_1 AS in0

)

SELECT *

FROM OrderBy_1

{% endsnapshot %}
