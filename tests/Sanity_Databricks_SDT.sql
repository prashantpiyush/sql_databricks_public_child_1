{{
  config({    
    "error_if": ">=4000",
    "fail_calc": "count(*)",
    "limit": 3000,
    "severity": 'error',
    "warn_if": ">=3500"
  })
}}

WITH uitesting_expressions_1 AS (

  SELECT * 
  
  FROM {{ ref('uitesting_expressions_1')}}

),

uitesting_expressions_1_1 AS (

  SELECT * 
  
  FROM {{ ref('uitesting_expressions_1')}}

),

raw_customers AS (

  SELECT * 
  
  FROM {{ ref('raw_customers')}}

),

Limit_1 AS (

  SELECT * 
  
  FROM raw_customers AS in0
  
  LIMIT 10

),

snapshot_sanity_test_1 AS (

  SELECT * 
  
  FROM {{ ref('snapshot_sanity_test_1')}}

),

Limit_2 AS (

  SELECT * 
  
  FROM snapshot_sanity_test_1 AS in0
  
  LIMIT 10

),

Limit_4 AS (

  SELECT * 
  
  FROM uitesting_expressions_1_1 AS in0
  
  LIMIT 10

),

Limit_3 AS (

  SELECT * 
  
  FROM uitesting_expressions_1 AS in0
  
  LIMIT 10

),

Join_1 AS (

  SELECT 
    in0.id AS id,
    in0.first_name AS first_name,
    in0.last_name AS last_name,
    in3.c_tinyint AS c_tinyint,
    in3.c_smallint AS c_smallint,
    in2.c_bigint AS c_bigint,
    in1.c_string AS c_string,
    in1.c_boolean AS c_boolean
  
  FROM Limit_1 AS in0
  INNER JOIN Limit_2 AS in1
     ON in0.id != in1.c_tinyint
  INNER JOIN Limit_3 AS in2
     ON in1.c_tinyint = in2.c_tinyint
  INNER JOIN Limit_4 AS in3
     ON in2.c_tinyint = in3.c_tinyint

),

OrderBy_1 AS (

  SELECT * 
  
  FROM Join_1 AS in0
  
  ORDER BY first_name ASC, last_name ASC

),

Filter_1 AS (

  SELECT * 
  
  FROM OrderBy_1 AS in0
  
  WHERE c_boolean IS true

)

SELECT *

FROM Filter_1
