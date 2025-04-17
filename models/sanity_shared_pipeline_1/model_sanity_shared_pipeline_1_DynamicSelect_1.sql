{{
  config({    
    "materialized": "table",
    "alias": "prophecy__temp_sanity_shared_pipeline_1_pre_filter_by_count_0",
    "database": "hive_metastore",
    "schema": "qa_db_warehouse"
  })
}}

WITH env_uitesting_main_model_databricks_1_0 AS (

  SELECT * 
  
  FROM {{ ref('env_uitesting_main_model_databricks_1')}}

),

qa_all_not_null_base_1 AS (

  {{
    SQL_BaseGitDepProjectAllFinal.qa_all_not_null_base(
      model = 'env_uitesting_main_model_databricks_1_0', 
      column_name = 'c_string'
    )
  }}

),

Aggregate_1 AS (

  SELECT 
    any_value(c_int) AS c_int,
    any_value(c_bigint) AS c_bigint,
    any_value(c_smallint) AS c_smallint,
    any_value(c_float) AS c_float,
    any_value(c_boolean) AS c_boolean,
    any_value(c_struct) AS c_struct,
    any_value(c_id) AS c_id,
    any_value(p_string) AS p_string,
    any_value(p_int) AS p_int,
    any_value(c_string) AS c_string
  
  FROM qa_all_not_null_base_1 AS in0
  
  GROUP BY p_int
  
  HAVING p_int > -10

),

WindowFunction_1 AS (

  SELECT 
    *,
    row_number() OVER (PARTITION BY c_int, c_bigint, p_string, p_int ORDER BY c_smallint ASC NULLS FIRST, c_float DESC NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS c_boolean_0,
    row_number() OVER (PARTITION BY c_int, c_bigint, p_string, p_int ORDER BY c_smallint ASC NULLS FIRST, c_float DESC NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS c_float_0
  
  FROM Aggregate_1 AS in0

),

Transpose_1 AS (

  {{
    DatabricksSqlBasics.Transpose(
      'WindowFunction_1', 
      ['c_id', 'c_int', 'c_bigint', 'c_smallint', 'c_float'], 
      ['c_boolean', 'p_string', 'p_int', 'c_boolean_0', 'c_float_0'], 
      [
        'c_int', 
        'c_bigint', 
        'c_smallint', 
        'c_float', 
        'c_boolean', 
        'c_struct', 
        'c_id', 
        'p_string', 
        'p_int', 
        'c_string', 
        'c_boolean_0', 
        'c_float_0'
      ]
    )
  }}

),

DynamicSelect_1 AS (

  {{
    DatabricksSqlBasics.DynamicSelect(
      'Transpose_1', 
      [
        { "name": "c_id", "dataType": "Integer" }, 
        { "name": "c_int", "dataType": "Integer" }, 
        { "name": "c_bigint", "dataType": "Double" }, 
        { "name": "c_smallint", "dataType": "SmallInt" }, 
        { "name": "c_float", "dataType": "Float" }, 
        { "name": "Name", "dataType": "String" }, 
        { "name": "Value", "dataType": "String" }
      ], 
      [
        "Boolean", 
        "String", 
        "Integer", 
        "Short", 
        "Byte", 
        "Float", 
        "Double", 
        "Decimal", 
        "Binary", 
        "Date", 
        "Timestamp", 
        "Struct"
      ], 
      'SELECT_FIELD_TYPES', 
      ""
    )
  }}

)

SELECT *

FROM DynamicSelect_1
