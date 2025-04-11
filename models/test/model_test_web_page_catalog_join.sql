{{
  config({    
    "materialized": "table",
    "alias": "prophecy__temp_test_post_web_page_catalog_join_0",
    "database": "hive_metastore",
    "schema": "qa_db_warehouse"
  })
}}

WITH spark_catalog_qa_suggestion_database_web_page AS (

  SELECT * 
  
  FROM {{ source('spark_catalog.qa_suggestion_database', 'web_page') }}

),

spark_catalog_qa_suggestion_database_catalog_page AS (

  SELECT * 
  
  FROM {{ source('spark_catalog.qa_suggestion_database', 'catalog_page') }}

),

web_page_catalog_join AS (

  SELECT 
    spark_catalog_qa_suggestion_database_web_page.WP_WEB_PAGE_SK,
    spark_catalog_qa_suggestion_database_web_page.WP_WEB_PAGE_ID,
    spark_catalog_qa_suggestion_database_web_page.WP_REC_START_DATE,
    spark_catalog_qa_suggestion_database_web_page.WP_REC_END_DATE,
    spark_catalog_qa_suggestion_database_web_page.WP_CREATION_DATE_SK,
    spark_catalog_qa_suggestion_database_web_page.WP_ACCESS_DATE_SK,
    spark_catalog_qa_suggestion_database_web_page.WP_AUTOGEN_FLAG,
    spark_catalog_qa_suggestion_database_web_page.WP_CUSTOMER_SK,
    spark_catalog_qa_suggestion_database_web_page.WP_URL,
    spark_catalog_qa_suggestion_database_web_page.WP_TYPE,
    spark_catalog_qa_suggestion_database_web_page.WP_CHAR_COUNT,
    spark_catalog_qa_suggestion_database_web_page.WP_LINK_COUNT,
    spark_catalog_qa_suggestion_database_web_page.WP_IMAGE_COUNT,
    spark_catalog_qa_suggestion_database_web_page.WP_MAX_AD_COUNT,
    spark_catalog_qa_suggestion_database_catalog_page.CP_CATALOG_PAGE_SK,
    spark_catalog_qa_suggestion_database_catalog_page.CP_CATALOG_PAGE_ID,
    spark_catalog_qa_suggestion_database_catalog_page.CP_START_DATE_SK,
    spark_catalog_qa_suggestion_database_catalog_page.CP_END_DATE_SK,
    spark_catalog_qa_suggestion_database_catalog_page.CP_DEPARTMENT,
    spark_catalog_qa_suggestion_database_catalog_page.CP_CATALOG_NUMBER,
    spark_catalog_qa_suggestion_database_catalog_page.CP_CATALOG_PAGE_NUMBER,
    spark_catalog_qa_suggestion_database_catalog_page.CP_DESCRIPTION,
    spark_catalog_qa_suggestion_database_catalog_page.CP_TYPE
  
  FROM spark_catalog_qa_suggestion_database_web_page
  INNER JOIN spark_catalog_qa_suggestion_database_catalog_page
     ON spark_catalog_qa_suggestion_database_web_page.WP_WEB_PAGE_SK = spark_catalog_qa_suggestion_database_catalog_page.CP_CATALOG_PAGE_SK

)

SELECT *

FROM web_page_catalog_join
