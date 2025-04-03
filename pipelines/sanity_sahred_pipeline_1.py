Schedule = Schedule(cron = "* 0 2 * * * *", timezone = "GMT", emails = ["email@gmail.com"], enabled = False)

with DAG(Schedule = Schedule):
    S_MSSQLALL = SourceTask(
        task_id = "S_MSSQLALL", 
        component = "OrchestrationSource", 
        kind = "MSSQLSource", 
        connector = Connection(kind = "mssql", id = "mssql"), 
        format = MSSQLFormat(
          additionalProperties = {"copilot" : {"datasetDescriptionStatus" : "fromCopilot"}}, 
          description = "Comprehensive dataset capturing various data types for diverse business applications, supporting a wide range of analytical needs.", 
          schema = {
            "fields": [{
                          "dataType": {"type" : "binary"}, 
                          "description": "Unique identifier for each record", 
                          "name": "ID"
                        },                         {
                          "dataType": {"type" : "int32"}, 
                          "description": "Integer value representing a specific metric or count", 
                          "name": "IntCol"
                        },                         {
                          "dataType": {"type" : "int16"}, 
                          "description": "A small integer value representing a specific metric.", 
                          "name": "SmallIntCol"
                        },                         {
                          "dataType": {"type" : "int8"}, 
                          "description": "A tiny integer value used for storing small counts or flags.", 
                          "name": "TinyIntCol"
                        },                         {
                          "dataType": {"type" : "int64"}, 
                          "description": "A large integer value for significant data points.", 
                          "name": "BigIntCol"
                        },                         {
                          "dataType": {"type" : "float64"}, 
                          "description": "A precise decimal value for financial or measurement data.", 
                          "name": "DecimalCol"
                        },                         {
                          "dataType": {"type" : "float64"}, 
                          "description": "A precise numeric value for financial calculations", 
                          "name": "NumericCol"
                        },                         {
                          "dataType": {"type" : "float64"}, 
                          "description": "A floating-point number for representing approximate values", 
                          "name": "FloatCol"
                        },                         {
                          "dataType": {"type" : "float64"}, 
                          "description": "Real number value representing a measurement or calculation", 
                          "name": "RealCol"
                        },                         {
                          "dataType": {"type" : "float64"}, 
                          "description": "Monetary value associated with a transaction or account", 
                          "name": "MoneyCol"
                        },                         {
                          "dataType": {"type" : "float64"}, 
                          "description": "Represents a small monetary value", 
                          "name": "SmallMoneyCol"
                        },                         {
                          "dataType": {"type" : "bool"}, 
                          "description": "Indicates a true/false condition", 
                          "name": "BitCol"
                        },                         {
                          "dataType": {"type" : "utf8"}, 
                          "description": "Fixed-length character data for storing short text values", 
                          "name": "CharCol"
                        },                         {
                          "dataType": {"type" : "utf8"}, 
                          "description": "Variable-length character data for storing flexible text values", 
                          "name": "VarCharCol"
                        },                         {
                          "dataType": {"type" : "utf8"}, 
                          "description": "Fixed-length string for storing Unicode characters", 
                          "name": "NCharCol"
                        },                         {
                          "dataType": {"type" : "utf8"}, 
                          "description": "Variable-length string for storing Unicode characters", 
                          "name": "NVarCharCol"
                        },                         {
                          "dataType": {"type" : "utf8"}, 
                          "description": "Textual content that can store large amounts of data", 
                          "name": "TextCol"
                        },                         {
                          "dataType": {"type" : "utf8"}, 
                          "description": "Unicode textual content that can store large amounts of data", 
                          "name": "NTextCol"
                        },                         {
                          "dataType": {"type" : "timestamp"}, 
                          "description": "Timestamp indicating a specific date", 
                          "name": "DateCol"
                        },                         {
                          "dataType": {"type" : "timestamp"}, 
                          "description": "Timestamp indicating a specific time", 
                          "name": "TimeCol"
                        },                         {
                          "dataType": {"type" : "timestamp"}, 
                          "description": "Timestamp indicating the date and time of an event", 
                          "name": "DateTimeCol"
                        },                         {
                          "dataType": {"type" : "timestamp"}, 
                          "description": "Timestamp representing a smaller range of date and time", 
                          "name": "SmallDateTimeCol"
                        },                         {
                          "dataType": {"type" : "timestamp"}, 
                          "description": "Timestamp representing a specific date and time with higher precision.", 
                          "name": "DateTime2Col"
                        },                         {
                          "dataType": {"type" : "timestamp"}, 
                          "description": "Timestamp that includes the date, time, and the time zone offset.", 
                          "name": "DateTimeOffsetCol"
                        },                         {
                          "dataType": {"type" : "timestamp"}, 
                          "description": "Timestamp indicating when the record was created", 
                          "name": "CreatedAt"
                        }], 
            "providerType": "Arrow"
          }
        ), 
        tableFullName = {"database" : "qa_performance", "name" : "all_type_table", "schema" : "qa_schema"}
    )
    shared_seed_basic = Task(
        task_id = "shared_seed_basic", 
        component = "Dataset", 
        table = {"name" : "shared_seed_basic", "sourceType" : "Seed", "alias" : ""}
    )
    industry_statistics_csv = Task(
        task_id = "industry_statistics_csv", 
        component = "Dataset", 
        table = {
          "name": "prophecy__temp_sanity_sahred_pipeline_1_pre_non_matching_int_count_1", 
          "sourceType": "Source", 
          "sourceName": "prophecy__temp_sanity_sahred_pipeline_1_source", 
          "alias": ""
        }
    )
    model_sanity_sahred_pipeline_1_non_matching_int_count = Task(
        task_id = "model_sanity_sahred_pipeline_1_non_matching_int_count", 
        component = "Model", 
        modelName = "model_sanity_sahred_pipeline_1_non_matching_int_count"
    )
    industry_statistics_csv = SourceTask(
        task_id = "industry_statistics_csv", 
        component = "OrchestrationSource", 
        kind = "S3Source", 
        connector = Connection(kind = "s3", id = "s3"), 
        format = CSVFormat(
          description = "Industry statistics data capturing various variables over time, aiding in economic analysis and industry performance assessment.", 
          separator = ",", 
          schema = {
            "fields": [{
                          "dataType": {"type" : "int64"}, 
                          "description": "The year in which the data was collected.", 
                          "name": "Year"
                        },                         {
                          "dataType": {"type" : "utf8"}, 
                          "description": "The classification of industries based on the New Zealand Standard Industry Output Classification.", 
                          "name": "Industry_aggregation_NZSIOC"
                        },                         {
                          "dataType": {"type" : "utf8"}, 
                          "description": "Code representing the industry classification according to NZSIOC standards", 
                          "name": "Industry_code_NZSIOC"
                        },                         {
                          "dataType": {"type" : "utf8"}, 
                          "description": "Name of the industry as per the NZSIOC classification", 
                          "name": "Industry_name_NZSIOC"
                        },                         {
                          "dataType": {"type" : "utf8"}, 
                          "description": "The measurement units used for the data values", 
                          "name": "Units"
                        },                         {
                          "dataType": {"type" : "utf8"}, 
                          "description": "Code representing the specific variable being measured", 
                          "name": "Variable_code"
                        },                         {
                          "dataType": {"type" : "utf8"}, 
                          "description": "Name of the variable being measured in the dataset", 
                          "name": "Variable_name"
                        },                         {
                          "dataType": {"type" : "utf8"}, 
                          "description": "Category of the variable being measured", 
                          "name": "Variable_category"
                        },                         {
                          "dataType": {"type" : "utf8"}, 
                          "description": "The measured value associated with the industry data", 
                          "name": "Value"
                        },                         {
                          "dataType": {"type" : "utf8"}, 
                          "description": "The code representing the industry classification according to ANZSIC 2006 standards", 
                          "name": "Industry_code_ANZSIC06"
                        }], 
            "providerType": "Arrow"
          }, 
          additionalProperties = {"copilot" : {"datasetDescriptionStatus" : "fromCopilot"}}, 
          header = True
        ), 
        filePath = "/datasets/orchestration_datasets/csv/valid/9MB_annual-enterprise-survey-2023-financial-year-provisional.csv"
    )
    env_uitesting_shared_useallmodel_1_1 = Task(
        task_id = "env_uitesting_shared_useallmodel_1_1", 
        component = "Model", 
        modelName = "env_uitesting_shared_useallmodel_1"
    )
    S_MSSQLALL = Task(
        task_id = "S_MSSQLALL", 
        component = "Dataset", 
        table = {
          "name": "prophecy__temp_sanity_sahred_pipeline_1_pre_non_matching_int_count_0", 
          "sourceType": "Source", 
          "sourceName": "prophecy__temp_sanity_sahred_pipeline_1_source", 
          "alias": ""
        }
    )
    all_type_non_partitioned = Task(
        task_id = "all_type_non_partitioned", 
        component = "Dataset", 
        table = {
          "name": "all_type_non_partitioned", 
          "sourceType": "Source", 
          "sourceName": "hive_metastore.qa_db_warehouse", 
          "alias": ""
        }
    )
    notify_shared_sanity = Task(
        task_id = "notify_shared_sanity", 
        component = "Email", 
        body = "Shared sanity databricks", 
        subject = "Shared sanity databricks", 
        includeData = False, 
        to = ["abhisheks@prophecy.io"], 
        bcc = ["abhisheks+bcc@prophecy.io"], 
        cc = ["abhisheks+cc@prophecy.io"]
    )
    shared_seed_basic.out >> model_sanity_sahred_pipeline_1_non_matching_int_count.in_1
    industry_statistics_csv.output_port_1_1 >> model_sanity_sahred_pipeline_1_non_matching_int_count.in_1
    all_type_non_partitioned.out >> model_sanity_sahred_pipeline_1_non_matching_int_count.in_1
    env_uitesting_shared_useallmodel_1_1.out >> model_sanity_sahred_pipeline_1_non_matching_int_count.in_1
    model_sanity_sahred_pipeline_1_non_matching_int_count.out_1 >> notify_shared_sanity.in0
    S_MSSQLALL.out0 >> S_MSSQLALL.input_port_0_1
    industry_statistics_csv.out0 >> industry_statistics_csv.input_port_1_1
    S_MSSQLALL.output_port_0_1 >> model_sanity_sahred_pipeline_1_non_matching_int_count.in_1
