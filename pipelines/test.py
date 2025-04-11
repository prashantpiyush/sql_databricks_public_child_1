with DAG():
    model_test_web_page_catalog_join = Task(
        task_id = "model_test_web_page_catalog_join", 
        component = "Model", 
        modelName = "model_test_web_page_catalog_join"
    )
