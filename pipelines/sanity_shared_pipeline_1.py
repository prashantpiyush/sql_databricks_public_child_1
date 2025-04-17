Schedule = Schedule(cron = "* 0 2 * * * *", timezone = "GMT", emails = ["email@gmail.com"], enabled = False)

with DAG(Schedule = Schedule):
    sanity_simple_model_1_1 = Task(
        task_id = "sanity_simple_model_1_1", 
        component = "Model", 
        modelName = "sanity_simple_model_1"
    )
    notify_pipeline_buddy = Task(
        task_id = "notify_pipeline_buddy", 
        component = "Email", 
        body = "Databricks Shared Sanity Project Pipeline Buddy", 
        subject = "Databricks Shared Sanity Project Pipeline Buddy", 
        includeData = True, 
        fileName = "DatabricksSharedSanityProjectPipelineBuddy", 
        to = ["abhisheks@prophecy.io"], 
        bcc = ["abhisheks@prophecy.io"], 
        cc = ["abhisheks@prophecy.io"], 
        fileFormat = "csv", 
        hasTemplate = False
    )
    RestAPI_1 = Task(
        task_id = "RestAPI_1", 
        component = "Dataset", 
        table = {
          "name": "prophecy__temp_sanity_shared_pipeline_1_pre_filter_by_count_3", 
          "sourceType": "Source", 
          "sourceName": "prophecy__temp_sanity_shared_pipeline_1_source", 
          "alias": ""
        }
    )
    env_uitesting_main_model_databricks_1_0 = Task(
        task_id = "env_uitesting_main_model_databricks_1_0", 
        component = "Model", 
        modelName = "env_uitesting_main_model_databricks_1"
    )
    model_sanity_shared_pipeline_1_DynamicSelect_1 = Task(
        task_id = "model_sanity_shared_pipeline_1_DynamicSelect_1", 
        component = "Model", 
        modelName = "model_sanity_shared_pipeline_1_DynamicSelect_1"
    )
    model_sanity_shared_pipeline_1_Limit_2 = Task(
        task_id = "model_sanity_shared_pipeline_1_Limit_2", 
        component = "Model", 
        modelName = "model_sanity_shared_pipeline_1_Limit_2"
    )
    all_type_parquet = Task(
        task_id = "all_type_parquet", 
        component = "Dataset", 
        writeOptions = {"writeMode" : "overwrite"}, 
        table = {"name" : "all_type_parquet", "sourceType" : "Table", "sourceName" : "spark_catalog.qa_database", "alias" : ""}
    )
    model_sanity_shared_pipeline_1_FuzzyMatch_1 = Task(
        task_id = "model_sanity_shared_pipeline_1_FuzzyMatch_1", 
        component = "Model", 
        modelName = "model_sanity_shared_pipeline_1_FuzzyMatch_1"
    )
    model_sanity_shared_pipeline_1_Join_1 = Task(
        task_id = "model_sanity_shared_pipeline_1_Join_1", 
        component = "Model", 
        modelName = "model_sanity_shared_pipeline_1_Join_1"
    )
    model_with_only_seed_base_1 = Task(
        task_id = "model_with_only_seed_base_1", 
        component = "Model", 
        modelName = "model_with_only_seed_base"
    )
    RestAPI_1 = Task(
        task_id = "RestAPI_1", 
        component = "RestAPI", 
        method = "POST", 
        body = "{    "name": {{c_string}},    "data": {       "year": 2019,       "price": 1849.99,       "CPU model": "Intel Core i9",       "Hard disk size": "1 TB"    } }", 
        url = "https://api.restful-api.dev/objects", 
        params = "", 
        headers = ""
    )
    model_sanity_shared_pipeline_1_filter_by_count = Task(
        task_id = "model_sanity_shared_pipeline_1_filter_by_count", 
        component = "Model", 
        modelName = "model_sanity_shared_pipeline_1_filter_by_count"
    )
    model_sanity_shared_pipeline_1_DataCleansing_1 = Task(
        task_id = "model_sanity_shared_pipeline_1_DataCleansing_1", 
        component = "Model", 
        modelName = "model_sanity_shared_pipeline_1_DataCleansing_1"
    )
    model_sanity_shared_pipeline_1_DynamicSelect_1.out_1 >> model_sanity_shared_pipeline_1_filter_by_count.in_1
    model_sanity_shared_pipeline_1_Limit_2.out_1 >> RestAPI_1.in0
    model_sanity_shared_pipeline_1_FuzzyMatch_1.out_1 >> model_sanity_shared_pipeline_1_filter_by_count.in_1
    model_sanity_shared_pipeline_1_filter_by_count.out_1 >> notify_pipeline_buddy.in0
    RestAPI_1.out0 >> RestAPI_1.input_port_4_1
    model_sanity_shared_pipeline_1_Join_1.out_1 >> model_sanity_shared_pipeline_1_filter_by_count.in_1
    (
        sanity_simple_model_1_1.out
        >> [model_sanity_shared_pipeline_1_Join_1.in_1, model_sanity_shared_pipeline_1_Limit_2.in_1]
    )
    model_sanity_shared_pipeline_1_DataCleansing_1.out_1 >> model_sanity_shared_pipeline_1_filter_by_count.in_1
    model_with_only_seed_base_1.out >> model_sanity_shared_pipeline_1_DataCleansing_1.in_1
    RestAPI_1.output_port_4_1 >> model_sanity_shared_pipeline_1_filter_by_count.in_1
    env_uitesting_main_model_databricks_1_0.out >> model_sanity_shared_pipeline_1_DynamicSelect_1.in_1
    all_type_parquet.out >> [model_sanity_shared_pipeline_1_Join_1.in_1, model_sanity_shared_pipeline_1_Limit_2.in_1]
