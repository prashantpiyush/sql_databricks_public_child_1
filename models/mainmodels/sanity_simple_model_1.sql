WITH all_type_parquet AS (

  SELECT * 
  
  FROM {{ source('spark_catalog.qa_database', 'all_type_parquet') }}

),

Reformat_1 AS (

  SELECT * 
  
  FROM all_type_parquet AS in0

),

Limit_1 AS (

  SELECT * 
  
  FROM Reformat_1 AS in0
  
  LIMIT 10

)

SELECT *

FROM Limit_1
