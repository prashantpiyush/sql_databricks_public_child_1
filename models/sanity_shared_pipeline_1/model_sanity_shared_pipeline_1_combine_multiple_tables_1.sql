{{
  config({    
    "materialized": "table",
    "alias": "prophecy__temp_sanity_shared_pipeline_1_post_combine_multiple_tables_1_0",
    "database": "hive_metastore",
    "schema": "qa_db_warehouse"
  })
}}

WITH RestAPI_1 AS (

  SELECT * 
  
  FROM {{ source('prophecy__temp_sanity_shared_pipeline_1_source', 'prophecy__temp_sanity_shared_pipeline_1_pre_child_deduplicate_custom_1_0') }}

),

model_with_only_seed_base_1 AS (

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
  
  WHERE country_code != NULL and code_1 != NULL

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

all_type_parquet AS (

  SELECT * 
  
  FROM {{ source('spark_catalog.qa_database', 'all_type_parquet') }}

),

Union_1 AS (

  SELECT * 
  
  FROM {{ source('prophecy__temp_sanity_shared_pipeline_1_source', 'prophecy__temp_sanity_shared_pipeline_1_pre_Intersect_1_0') }}

),

Intersect_1 AS (

  SELECT * 
  
  FROM Union_1 AS in0
  
  INTERSECT
  
  SELECT * 
  
  FROM all_type_parquet AS in1

),

Limit_2 AS (

  SELECT * 
  
  FROM {{ source('prophecy__temp_sanity_shared_pipeline_1_source', 'prophecy__temp_sanity_shared_pipeline_1_post_Limit_2_0') }}

),

Except_1 AS (

  SELECT * 
  
  FROM Intersect_1 AS in0
  
  EXCEPT
  
  SELECT * 
  
  FROM Limit_2 AS in1

),

UnionByName_1 AS (

  {{
    DatabricksSqlBasics.UnionByName(
      'Except_1,Limit_2', 
      [
        { "name": "c_tinyint", "dataType": "TinyInt" }, 
        { "name": "c_smallint", "dataType": "SmallInt" }, 
        { "name": "c_int", "dataType": "Integer" }, 
        { "name": "c_bigint", "dataType": "Bigint" }, 
        { "name": "c_float", "dataType": "Float" }, 
        { "name": "c_double", "dataType": "Double" }, 
        { "name": "c_string", "dataType": "String" }, 
        { "name": "c_boolean", "dataType": "Boolean" }, 
        { "name": "c_array", "dataType": "Array" }, 
        { "name": "c_struct", "dataType": "Struct" }
      ], 
      [
        { "name": "c_tinyint", "dataType": "TinyInt" }, 
        { "name": "c_smallint", "dataType": "SmallInt" }, 
        { "name": "c_int", "dataType": "Integer" }, 
        { "name": "c_bigint", "dataType": "Bigint" }, 
        { "name": "c_float", "dataType": "Float" }, 
        { "name": "c_double", "dataType": "Double" }, 
        { "name": "c_string", "dataType": "String" }, 
        { "name": "c_boolean", "dataType": "Boolean" }, 
        { "name": "c_array", "dataType": "Array" }, 
        { "name": "c_struct", "dataType": "Struct" }
      ], 
      'nameBasedUnionOperation'
    )
  }}

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
    any_value(p_int) AS p_int
  
  FROM qa_all_not_null_base_1 AS in0
  
  GROUP BY c_string
  
  HAVING c_string IS NOT NULL

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
        'c_boolean_0', 
        'c_float_0'
      ]
    )
  }}

),

child_deduplicate_custom_1 AS (

  {{ SQL_DatabricksSharedBasic.child_deduplicate_custom('''','''','''') }}

),

FuzzyMatch_1 AS (

  {{
    DatabricksSqlBasics.FuzzyMatch(
      'Transpose_1', 
      'PURGE', 
      '', 
      'c_int', 
      { 'equals': ['c_int'] }, 
      80, 
      true
    )
  }}

),

DynamicSelect_1 AS (

  {{
    DatabricksSqlBasics.DynamicSelect(
      'FuzzyMatch_1', 
      [
        { "name": "record_id1", "dataType": "String" }, 
        { "name": "record_id2", "dataType": "String" }, 
        { "name": "similarity_score", "dataType": "Double" }
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

combine_multiple_tables_1 AS (

  {{
    SQL_DatabricksSharedBasic.combine_multiple_tables(
      table_1 = 'in0', 
      table_2 = 'in1', 
      table_3 = 'in2', 
      table_4 = 'in3', 
      table_5 = 'in4', 
      col_table_1 = 'record_id1'
    )
  }}

)

SELECT *

FROM combine_multiple_tables_1
