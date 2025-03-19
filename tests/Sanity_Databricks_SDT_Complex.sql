{{
  config({    
    "error_if": '>1000',
    "fail_calc": 'count(*)',
    "severity": 'warn',
    "warn_if": '>500'
  })
}}

WITH uitesting_scd2_diff_col_val AS (

  SELECT * 
  
  FROM {{ ref('uitesting_scd2_diff_col_val')}}

),

Aggregate_1 AS (

  SELECT 
    any_value(c_tinyint) AS c_tinyint,
    any_value(c_smallint) AS c_smallint,
    any_value(c_int) AS c_int,
    any_value(c_bigint) AS c_bigint,
    any_value(c_float) AS c_float,
    any_value(c_double) AS c_double,
    any_value(c_string) AS c_string,
    any_value(c_boolean) AS c_boolean,
    any_value(c_array) AS c_array,
    any_value(c_struct) AS c_struct,
    any_value(c_id) AS c_id
  
  FROM uitesting_scd2_diff_col_val AS in0
  
  GROUP BY 
    c_tinyint, c_smallint

),

Filter_1 AS (

  SELECT * 
  
  FROM Aggregate_1 AS in0
  
  WHERE c_int > -1

),

raw_customers AS (

  SELECT * 
  
  FROM {{ ref('raw_customers')}}

),

OrderBy_1 AS (

  SELECT * 
  
  FROM raw_customers AS in0
  
  ORDER BY id ASC NULLS FIRST, first_name DESC NULLS LAST, last_name ASC

),

Reformat_1 AS (

  SELECT 
    id AS id,
    first_name AS first_name,
    last_name AS last_name
  
  FROM OrderBy_1 AS in0

),

SetOperation_1 AS (

  SELECT * 
  
  FROM Reformat_1 AS in0
  
  UNION
  
  SELECT * 
  
  FROM OrderBy_1 AS in1

),

sanity_simple_model_1 AS (

  SELECT * 
  
  FROM {{ ref('sanity_simple_model_1')}}

),

Limit_2 AS (

  SELECT * 
  
  FROM sanity_simple_model_1 AS in0
  
  LIMIT 10

),

Join_1 AS (

  SELECT 
    in0.c_tinyint AS c_tinyint,
    in1.c_smallint AS c_smallint,
    in1.c_int AS c_int,
    in0.c_bigint AS c_bigint,
    in0.c_float AS c_float,
    in0.c_double AS c_double,
    in0.c_boolean AS c_boolean,
    in0.c_array AS c_array,
    in0.c_struct AS c_struct,
    in0.c_id AS c_id,
    in0.c_string AS c_string
  
  FROM uitesting_scd2_diff_col_val AS in0
  INNER JOIN Limit_2 AS in1
     ON in0.c_int != in1.c_float

),

Limit_3 AS (

  SELECT * 
  
  FROM Filter_1 AS in0
  
  LIMIT 10

),

Join_2 AS (

  SELECT 
    in0.c_tinyint AS c_tinyint,
    in1.c_smallint AS c_smallint,
    in0.c_int AS c_int,
    in0.c_bigint AS c_bigint,
    in0.c_float AS c_float,
    in0.c_double AS c_double,
    in0.c_string AS c_string,
    in0.c_boolean AS c_boolean,
    in0.c_struct AS c_struct,
    in0.c_id AS c_id
  
  FROM Limit_3 AS in0
  INNER JOIN Join_1 AS in1
     ON in0.c_bigint != in1.c_smallint

),

Limit_1 AS (

  SELECT * 
  
  FROM Join_2 AS in0
  
  LIMIT 10

),

SQLStatement_1 AS (

  SELECT *
  
  FROM SetOperation_1
  
  WHERE id BETWEEN -100 AND (
          SELECT max(Limit_1.c_bigint)
          
          FROM Limit_1
         )

)

SELECT count(*)

FROM SQLStatement_1
JOIN Limit_1
   ON SQLStatement_1.id != Limit_1.c_smallint

WHERE Limit_1.c_bigint != (
        SELECT count(*)
        
        FROM Reformat_1
       )
      and Limit_1.c_float != (
            SELECT count(*)
            
            FROM Limit_2
           )
