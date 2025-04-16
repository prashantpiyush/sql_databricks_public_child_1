WITH all_type_non_partitioned AS (

  SELECT * 
  
  FROM {{ source('hive_metastore.qa_db_warehouse', 'all_type_non_partitioned') }}

),

field_data_types_1 AS (

  {{
    DatabricksSqlBasics.DynamicSelect(
      'all_type_non_partitioned', 
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
      ["Boolean", "String", "Integer", "Short", "Float", "Decimal", "Date", "Timestamp", "Struct"], 
      'SELECT_FIELD_TYPES', 
      ""
    )
  }}

),

MultiColumnEdit_1_1 AS (

  {{
    DatabricksSqlBasics.MultiColumnEdit(
      'field_data_types_1', 
      "concat(column_name, column_value)", 
      ['c_int', 'c_float', 'c_string', 'c_boolean', 'c_struct'], 
      ['c_int', 'c_float', 'c_string', 'c_boolean'], 
      false, 
      'Prefix', 
      'PRE_'
    )
  }}

),

MultiColumnRename_1_1 AS (

  {{
    DatabricksSqlBasics.MultiColumnRename(
      'MultiColumnEdit_1_1', 
      ['c_int', 'c_float', 'c_string', 'c_boolean', 'c_struct'], 
      'advancedRename', 
      ['c_int', 'c_float', 'c_string', 'c_boolean', 'c_struct'], 
      '', 
      '', 
      "concat(column_name,'_new')"
    )
  }}

),

DataCleansing_1_1 AS (

  {{
    DatabricksSqlBasics.DataCleansing(
      'MultiColumnRename_1_1', 
      [
        { "name": "c_int", "dataType": "String" }, 
        { "name": "c_float", "dataType": "String" }, 
        { "name": "c_string", "dataType": "String" }, 
        { "name": "c_boolean", "dataType": "String" }, 
        { "name": "c_struct", "dataType": "Struct" }
      ], 
      'makeLowercase', 
      ['c_int', 'c_float', 'c_string', 'c_boolean'], 
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

UnionByName_1_1 AS (

  {{
    DatabricksSqlBasics.UnionByName(
      'DataCleansing_1_1,DataCleansing_1_1', 
      [
        { "name": "c_int", "dataType": "String" }, 
        { "name": "c_float", "dataType": "String" }, 
        { "name": "c_string", "dataType": "String" }, 
        { "name": "c_boolean", "dataType": "String" }, 
        { "name": "c_struct", "dataType": "Struct" }
      ], 
      [
        { "name": "c_int", "dataType": "String" }, 
        { "name": "c_float", "dataType": "String" }, 
        { "name": "c_string", "dataType": "String" }, 
        { "name": "c_boolean", "dataType": "String" }, 
        { "name": "c_struct", "dataType": "Struct" }
      ], 
      'nameBasedUnionOperation'
    )
  }}

),

TextToColumns_1_1 AS (

  {{
    DatabricksSqlBasics.TextToColumns(
      'UnionByName_1_1', 
      'C_STRING', 
      "a", 
      'splitColumns', 
      2, 
      'Leave extra in last column', 
      'root', 
      'generated', 
      'generated_column'
    )
  }}

),

fuzzy_match_purge_1 AS (

  {{
    DatabricksSqlBasics.FuzzyMatch(
      'all_type_non_partitioned', 
      'PURGE', 
      '', 
      'c_smallint', 
      { 'custom': ['c_smallint'], 'exact': ['c_int', 'c_float'], 'equals': ['c_double'] }, 
      80, 
      false
    )
  }}

),

text_to_columns_filter AS (

  SELECT *
  
  FROM TextToColumns_1_1
  
  WHERE C_INT != (
          (SELECT count(*)
          
          FROM fuzzy_match_purge_1)
         )

)

SELECT *

FROM text_to_columns_filter
