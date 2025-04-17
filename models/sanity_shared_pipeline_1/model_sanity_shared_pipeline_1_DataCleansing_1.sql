{{
  config({    
    "materialized": "table",
    "alias": "prophecy__temp_sanity_shared_pipeline_1_pre_filter_by_count_4",
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

)

SELECT *

FROM DataCleansing_1
