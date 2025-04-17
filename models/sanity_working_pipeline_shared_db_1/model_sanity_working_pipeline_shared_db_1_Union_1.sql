{{
  config({    
    "materialized": "table",
    "alias": "prophecy__temp_sanity_working_pipeline_shared_db_1_post_Union_1_0",
    "database": "hive_metastore",
    "schema": "qa_db_warehouse"
  })
}}

WITH model_with_only_seed_base_1 AS (

  SELECT * 
  
  FROM {{ ref('model_with_only_seed_base')}}

),

qa_all_not_null_1 AS (

  {{
    SQL_DatabricksParentProjectMain.qa_all_not_null(
      model = 'model_with_only_seed_base_1', 
      column_name = 'code_1'
    )
  }}

),

Filter_1 AS (

  SELECT * 
  
  FROM qa_all_not_null_1 AS in0
  
  WHERE country_code IS NOT NULL and code_1 IS NOT NULL

),

OrderBy_1 AS (

  SELECT * 
  
  FROM Filter_1 AS in0
  
  ORDER BY country_code ASC NULLS FIRST, country_label DESC NULLS LAST

),

Reformat_1 AS (

  SELECT 
    country_code AS country_code,
    country_label AS country_label,
    code_1 AS code_1,
    service_label_1 AS service_label_1,
    c_macro2 AS c_macro2,
    named_struct('code', country_code, 'label', country_label) AS c_struct_country_details,
    ARRAY(country_code, country_label, code_1, service_label_1, c_macro2) AS c_array,
    monotonically_increasing_id() AS c_int_id
  
  FROM OrderBy_1 AS in0

),

Limit_1 AS (

  SELECT * 
  
  FROM Reformat_1 AS in0
  
  LIMIT 10

),

Deduplicate_1 AS (

  SELECT * 
  
  FROM Limit_1 AS in0
  
  QUALIFY COUNT(*) OVER (PARTITION BY country_code, country_label, code_1) = 1

),

FlattenSchema_1 AS (

  SELECT 
    c_array.col AS c_array,
    c_struct_country_details.code AS code,
    country_code AS country_code,
    country_label AS country_label,
    code_1 AS code_1,
    service_label_1 AS service_label_1,
    c_int_id AS c_int_id
  
  FROM Deduplicate_1 AS in0, 
  LATERAL explode_outer(c_array) AS c_array

),

env_uitesting_main_model_databricks_1_0 AS (

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

MultiColumnEdit_1 AS (

  {{
    DatabricksSqlBasics.MultiColumnEdit(
      'FlattenSchema_1', 
      "upper(column_name)", 
      ['c_array', 'code', 'country_code', 'country_label', 'code_1', 'service_label_1', 'c_int_id'], 
      ['code', 'country_code', 'country_label', 'code_1', 'service_label_1'], 
      true, 
      'Prefix', 
      'pre_'
    )
  }}

),

MultiColumnRename_1 AS (

  {{
    DatabricksSqlBasics.MultiColumnRename(
      'MultiColumnEdit_1', 
      ['code', 'country_code', 'pre_code', 'pre_country_code', 'pre_country_label', 'pre_code_1'], 
      'editPrefixSuffix', 
      [
        'c_array', 
        'code', 
        'country_code', 
        'country_label', 
        'code_1', 
        'service_label_1', 
        'c_int_id', 
        'pre_code', 
        'pre_country_code', 
        'pre_country_label', 
        'pre_code_1', 
        'pre_service_label_1'
      ], 
      'Suffix', 
      '_post', 
      ""
    )
  }}

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

),

Reformat_3 AS (

  SELECT 
    concat(Name, Value) AS service_label_1,
    CAST(CAST(c_int AS STRING) AS BIGINT) AS c_int
  
  FROM DynamicSelect_1 AS in0

),

RestAPI_1 AS (

  SELECT * 
  
  FROM {{ source('prophecy__temp_sanity_working_pipeline_shared_db_1_source', 'prophecy__temp_sanity_working_pipeline_shared_db_1_pre_Reformat_4_0') }}

),

DataCleansing_1 AS (

  {{
    DatabricksSqlBasics.DataCleansing(
      'MultiColumnRename_1', 
      [
        { "name": "c_array", "dataType": "String" }, 
        { "name": "code_post", "dataType": "String" }, 
        { "name": "country_code_post", "dataType": "String" }, 
        { "name": "country_label", "dataType": "String" }, 
        { "name": "code_1", "dataType": "String" }, 
        { "name": "service_label_1", "dataType": "String" }, 
        { "name": "c_int_id", "dataType": "Bigint" }, 
        { "name": "pre_code_post", "dataType": "String" }, 
        { "name": "pre_country_code_post", "dataType": "String" }, 
        { "name": "pre_country_label_post", "dataType": "String" }, 
        { "name": "pre_code_1_post", "dataType": "String" }, 
        { "name": "pre_service_label_1", "dataType": "String" }
      ], 
      'makeUppercase', 
      [
        'c_array', 
        'code', 
        'country_code', 
        'country_label', 
        'code_1', 
        'service_label_1', 
        'pre_code', 
        'pre_country_code', 
        'pre_country_label', 
        'pre_code_1', 
        'pre_service_label_1', 
        'c_int_id', 
        'pre_code_post'
      ], 
      true, 
      'NA', 
      true, 
      0, 
      true, 
      true, 
      true, 
      true, 
      true, 
      true, 
      true
    )
  }}

),

Reformat_4 AS (

  SELECT 
    concat(c_string, api_data) AS service_label_1,
    CAST(CAST(c_int AS STRING) AS BIGINT) AS c_int
  
  FROM RestAPI_1 AS in0

),

Reformat_2 AS (

  SELECT 
    service_label_1 AS service_label_1,
    c_int_id AS c_int
  
  FROM DataCleansing_1 AS in0

),

Union_1 AS (

  SELECT * 
  
  FROM Reformat_2 AS in0
  
  UNION
  
  SELECT * 
  
  FROM Reformat_3 AS in1
  
  UNION
  
  SELECT * 
  
  FROM Reformat_4 AS in2

)

SELECT *

FROM Union_1
