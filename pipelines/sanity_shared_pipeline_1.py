{
  "id" : "sanity_shared_pipeline_1",
  "metainfo" : {
    "label" : "sanity_shared_pipeline_1",
    "autoLayout" : false,
    "version" : 1,
    "configuration" : {
      "schema" : {
        "type" : "record",
        "fields" : [ ]
      }
    },
    "schedule" : {
      "cron" : "* 0 2 * * * *",
      "timeZone" : "GMT",
      "emailOnSuccess" : false,
      "emailOnFailure" : false,
      "emailOnStart" : false,
      "emails" : [ "email@gmail.com" ],
      "enabled" : false
    }
  },
  "processes" : {
    "WjtJ3kS1VM6qaRT4v_N-j$$Sm1d7cP-2PxgZz6lq65iS" : {
      "id" : "WjtJ3kS1VM6qaRT4v_N-j$$Sm1d7cP-2PxgZz6lq65iS",
      "component" : "Model",
      "metadata" : {
        "label" : "model_sanity_shared_pipeline_1_SQLStatement_1",
        "phase" : 0
      },
      "properties" : {
        "modelName" : "model_sanity_shared_pipeline_1_SQLStatement_1"
      },
      "visualMetaInfo" : {
        "visualProcessIds" : [ "prophecy__temp_sanity_shared_pipeline_1_post_SQLStatement_1_0", "WjtJ3kS1VM6qaRT4v_N-j$$Sm1d7cP-2PxgZz6lq65iS", "KwXjRsjkX9PCfrCXg0hat", "prophecy__temp_sanity_shared_pipeline_1_post_Limit_2_0" ],
        "type" : "table",
        "isTemporaryTable" : true
      },
      "ports" : {
        "inputs" : [ {
          "id" : "model_sanity_shared_pipeline_1_SQLStatement_1_in_1",
          "slug" : "in_1"
        } ],
        "outputs" : [ {
          "id" : "model_sanity_shared_pipeline_1_SQLStatement_1_out_1",
          "slug" : "out_1"
        } ],
        "isCustomOutputSchema" : false
      }
    },
    "prophecy__temp_sanity_shared_pipeline_1_pre_child_deduplicate_custom_1_0" : {
      "id" : "prophecy__temp_sanity_shared_pipeline_1_pre_child_deduplicate_custom_1_0",
      "component" : "Dataset",
      "metadata" : {
        "label" : "RestAPI_1",
        "phase" : 0
      },
      "properties" : {
        "table" : {
          "name" : "prophecy__temp_sanity_shared_pipeline_1_pre_child_deduplicate_custom_1_0",
          "sourceType" : "Source",
          "sourceName" : "prophecy__temp_sanity_shared_pipeline_1_source",
          "alias" : ""
        }
      },
      "visualMetaInfo" : {
        "isTemporaryTable" : true
      },
      "ports" : {
        "inputs" : [ {
          "id" : "input_port_4_1",
          "slug" : "input_port_4_1"
        } ],
        "outputs" : [ {
          "id" : "output_port_4_1",
          "slug" : "output_port_4_1"
        } ],
        "isCustomOutputSchema" : false
      }
    },
    "O252X5-Ki4F7tDFfygq2i$$hkQCwqs0ijXUv58ZvUFW7" : {
      "id" : "O252X5-Ki4F7tDFfygq2i$$hkQCwqs0ijXUv58ZvUFW7",
      "component" : "Model",
      "metadata" : {
        "label" : "sanity_simple_model_1_1",
        "x" : 180,
        "y" : 360
      },
      "properties" : {
        "modelName" : "sanity_simple_model_1"
      },
      "ports" : {
        "inputs" : [ {
          "id" : "DIwvtqUqJ6ui2R07srBBP$$hG2Y_eVmxC7RP4g3T425K",
          "slug" : "in0"
        } ],
        "outputs" : [ {
          "id" : "xZXY0LTsgHGTsmJu7orGy$$ypSxKTAQXY33pJ_33uI2s",
          "schema" : {
            "entityType" : "",
            "providerType" : "Databricks",
            "fields" : [ {
              "name" : "c_tinyint",
              "dataType" : {
                "type" : "TinyInt"
              }
            }, {
              "name" : "c_smallint",
              "dataType" : {
                "type" : "SmallInt"
              }
            }, {
              "name" : "c_int",
              "dataType" : {
                "type" : "Integer"
              }
            }, {
              "name" : "c_bigint",
              "dataType" : {
                "type" : "Bigint"
              }
            }, {
              "name" : "c_float",
              "dataType" : {
                "type" : "Float"
              }
            }, {
              "name" : "c_double",
              "dataType" : {
                "type" : "Double"
              }
            }, {
              "name" : "c_string",
              "dataType" : {
                "type" : "String"
              }
            }, {
              "name" : "c_boolean",
              "dataType" : {
                "type" : "Boolean"
              }
            }, {
              "name" : "c_array",
              "dataType" : {
                "type" : "Array",
                "dataType" : {
                  "type" : "String"
                }
              }
            }, {
              "name" : "c_struct",
              "dataType" : {
                "type" : "Struct",
                "fields" : [ {
                  "name" : "city",
                  "dataType" : {
                    "type" : "String"
                  }
                }, {
                  "name" : "state",
                  "dataType" : {
                    "type" : "String"
                  }
                }, {
                  "name" : "pin",
                  "dataType" : {
                    "type" : "Bigint"
                  }
                } ]
              }
            } ]
          },
          "slug" : "out"
        } ],
        "isCustomOutputSchema" : true
      }
    },
    "gzNjNQKdKsgb4ncEObCUf$$t2BpyzOqz8r4I9Oy9PwIh" : {
      "id" : "gzNjNQKdKsgb4ncEObCUf$$t2BpyzOqz8r4I9Oy9PwIh",
      "component" : "Model",
      "metadata" : {
        "label" : "model_with_only_seed_base_1",
        "x" : -300,
        "y" : 200
      },
      "properties" : {
        "modelName" : "model_with_only_seed_base"
      },
      "ports" : {
        "inputs" : [ {
          "id" : "muiiOgsLk5K27G4ZRPW8I$$v5iHOCaOvQGUZMJnMbQTX",
          "slug" : "in0"
        } ],
        "outputs" : [ {
          "id" : "wIHJ3OFikwz3v1LwhN2Xw$$N8PTYELMWt2Aws28fLnfu",
          "schema" : {
            "entityType" : "",
            "providerType" : "Databricks",
            "fields" : [ {
              "name" : "country_code",
              "dataType" : {
                "type" : "String"
              }
            }, {
              "name" : "country_label",
              "dataType" : {
                "type" : "String"
              }
            }, {
              "name" : "code_1",
              "dataType" : {
                "type" : "String"
              }
            }, {
              "name" : "service_label_1",
              "dataType" : {
                "type" : "String"
              }
            }, {
              "name" : "c_macro2",
              "dataType" : {
                "type" : "String"
              }
            } ]
          },
          "slug" : "out"
        } ],
        "isCustomOutputSchema" : true
      }
    },
    "LijEZ5i9IBYdPgzL3ct58$$zbiRm-j50hYSdcS4gElJq" : {
      "id" : "LijEZ5i9IBYdPgzL3ct58$$zbiRm-j50hYSdcS4gElJq",
      "component" : "Model",
      "metadata" : {
        "label" : "env_uitesting_main_model_databricks_1_0",
        "x" : 20,
        "y" : 20
      },
      "properties" : {
        "modelName" : "env_uitesting_main_model_databricks_1"
      },
      "ports" : {
        "inputs" : [ {
          "id" : "rT_s5WrE7GV2DGOvmXG5H$$A8jm7GZDVzTFnLpMiy2h9",
          "slug" : "in0"
        } ],
        "outputs" : [ {
          "id" : "dlSxbtz9jD2_G4IspCMq-$$9EqEM1E3DmPOyRlFzrjqX",
          "schema" : {
            "entityType" : "",
            "providerType" : "Databricks",
            "fields" : [ {
              "name" : "p_int",
              "dataType" : {
                "type" : "Integer"
              }
            }, {
              "name" : "p_string",
              "dataType" : {
                "type" : "String"
              }
            }, {
              "name" : "c_string",
              "dataType" : {
                "type" : "String"
              }
            }, {
              "name" : "c_int",
              "dataType" : {
                "type" : "Integer"
              }
            }, {
              "name" : "c_bigint",
              "dataType" : {
                "type" : "Double"
              }
            }, {
              "name" : "c_smallint",
              "dataType" : {
                "type" : "SmallInt"
              }
            }, {
              "name" : "c_tinyint",
              "dataType" : {
                "type" : "TinyInt"
              }
            }, {
              "name" : "c_float",
              "dataType" : {
                "type" : "Float"
              }
            }, {
              "name" : "c_boolean",
              "dataType" : {
                "type" : "Boolean"
              }
            }, {
              "name" : "c_array",
              "dataType" : {
                "type" : "Array",
                "dataType" : {
                  "type" : "String"
                }
              }
            }, {
              "name" : "c_double",
              "dataType" : {
                "type" : "Double"
              }
            }, {
              "name" : "c_struct",
              "dataType" : {
                "type" : "Struct",
                "fields" : [ {
                  "name" : "city",
                  "dataType" : {
                    "type" : "String"
                  }
                }, {
                  "name" : "state",
                  "dataType" : {
                    "type" : "String"
                  }
                }, {
                  "name" : "pin",
                  "dataType" : {
                    "type" : "Bigint"
                  }
                } ]
              }
            }, {
              "name" : "c_id",
              "dataType" : {
                "type" : "Integer"
              }
            } ]
          },
          "slug" : "out"
        } ],
        "isCustomOutputSchema" : true
      }
    },
    "7Bz9vvW3NBIp2tGXBFhkT$$bfwMevlAlCaFaUhYezDl0" : {
      "id" : "7Bz9vvW3NBIp2tGXBFhkT$$bfwMevlAlCaFaUhYezDl0",
      "component" : "Model",
      "metadata" : {
        "label" : "model_sanity_shared_pipeline_1_Union_1",
        "phase" : 0
      },
      "properties" : {
        "modelName" : "model_sanity_shared_pipeline_1_Union_1"
      },
      "visualMetaInfo" : {
        "visualProcessIds" : [ "prophecy__temp_sanity_shared_pipeline_1_pre_Intersect_1_0", "7Bz9vvW3NBIp2tGXBFhkT$$bfwMevlAlCaFaUhYezDl0", "KwXjRsjkX9PCfrCXg0hat", "prophecy__temp_sanity_shared_pipeline_1_pre_Union_1_0" ],
        "type" : "table",
        "isTemporaryTable" : true
      },
      "ports" : {
        "inputs" : [ {
          "id" : "model_sanity_shared_pipeline_1_Union_1_in_1",
          "slug" : "in_1"
        } ],
        "outputs" : [ {
          "id" : "model_sanity_shared_pipeline_1_Union_1_out_1",
          "slug" : "out_1"
        } ],
        "isCustomOutputSchema" : false
      }
    },
    "KwXjRsjkX9PCfrCXg0hat" : {
      "id" : "KwXjRsjkX9PCfrCXg0hat",
      "component" : "Dataset",
      "metadata" : {
        "label" : "all_type_parquet",
        "x" : 180,
        "y" : 520,
        "phase" : 0
      },
      "properties" : {
        "writeOptions" : {
          "writeMode" : "overwrite"
        },
        "table" : {
          "name" : "all_type_parquet",
          "sourceType" : "Table",
          "sourceName" : "spark_catalog.qa_database",
          "alias" : ""
        }
      },
      "ports" : {
        "inputs" : [ {
          "id" : "nGVK_WnQuUf1LJarpt14d",
          "slug" : "in"
        } ],
        "outputs" : [ {
          "id" : "AVVIU7q7XGsbe1eYpFjRb",
          "schema" : {
            "entityType" : "",
            "providerType" : "databricks",
            "fields" : [ {
              "name" : "c_tinyint",
              "dataType" : {
                "type" : "TinyInt"
              }
            }, {
              "name" : "c_smallint",
              "dataType" : {
                "type" : "SmallInt"
              }
            }, {
              "name" : "c_int",
              "dataType" : {
                "type" : "Integer"
              }
            }, {
              "name" : "c_bigint",
              "dataType" : {
                "type" : "Bigint"
              }
            }, {
              "name" : "c_float",
              "dataType" : {
                "type" : "Float"
              }
            }, {
              "name" : "c_double",
              "dataType" : {
                "type" : "Double"
              }
            }, {
              "name" : "c_string",
              "dataType" : {
                "type" : "String"
              }
            }, {
              "name" : "c_boolean",
              "dataType" : {
                "type" : "Boolean"
              }
            }, {
              "name" : "c_array",
              "dataType" : {
                "type" : "Array",
                "dataType" : {
                  "type" : "String"
                }
              }
            }, {
              "name" : "c_struct",
              "dataType" : {
                "type" : "Struct",
                "fields" : [ {
                  "name" : "city",
                  "dataType" : {
                    "type" : "String"
                  }
                }, {
                  "name" : "state",
                  "dataType" : {
                    "type" : "String"
                  }
                }, {
                  "name" : "pin",
                  "dataType" : {
                    "type" : "Bigint"
                  }
                } ]
              }
            } ]
          },
          "slug" : "out"
        } ],
        "isCustomOutputSchema" : false
      }
    },
    "8jG3o48L9d_rvj-cka_Q1$$KQ4OE4SMpoDBGxDehgahQ" : {
      "id" : "8jG3o48L9d_rvj-cka_Q1$$KQ4OE4SMpoDBGxDehgahQ",
      "component" : "Email",
      "metadata" : {
        "label" : "notify_pipeline_buddy",
        "x" : 1760,
        "y" : 220,
        "phase" : 0
      },
      "properties" : {
        "body" : "Databricks Shared Sanity Project Pipeline Buddy",
        "subject" : "Databricks Shared Sanity Project Pipeline Buddy",
        "includeData" : true,
        "fileName" : "DatabricksSharedSanityProjectPipelineBuddy",
        "to" : [ "abhisheks@prophecy.io" ],
        "bcc" : [ "abhisheks@prophecy.io" ],
        "cc" : [ "abhisheks@prophecy.io" ],
        "fileFormat" : "csv",
        "hasTemplate" : false
      },
      "ports" : {
        "inputs" : [ {
          "id" : "5uDzNR7b0bONBGAUVpPp9$$T4Usuyi60k7bLHT6O2y5K",
          "slug" : "in0"
        } ],
        "outputs" : [ ],
        "isCustomOutputSchema" : false
      }
    },
    "HNpLbqiaSUYog1_UFDCkM$$EiF9W-HTUeJurxrXKV51b" : {
      "id" : "HNpLbqiaSUYog1_UFDCkM$$EiF9W-HTUeJurxrXKV51b",
      "component" : "RestAPI",
      "metadata" : {
        "label" : "RestAPI_1",
        "x" : 980,
        "y" : 640,
        "phase" : 0
      },
      "properties" : {
        "method" : "POST",
        "body" : "{    \"name\": {{c_string}},    \"data\": {       \"year\": 2019,       \"price\": 1849.99,       \"CPU model\": \"Intel Core i9\",       \"Hard disk size\": \"1 TB\"    } }",
        "url" : "https://api.restful-api.dev/objects",
        "params" : "",
        "headers" : ""
      },
      "ports" : {
        "inputs" : [ {
          "id" : "I6sVfPnYM-GUiAOHVIiDH$$bE4IJyJ9tn07FCYiGSz7j",
          "schema" : {
            "entityType" : "",
            "providerType" : "Databricks",
            "fields" : [ {
              "name" : "c_tinyint",
              "dataType" : {
                "type" : "TinyInt"
              }
            }, {
              "name" : "c_smallint",
              "dataType" : {
                "type" : "SmallInt"
              }
            }, {
              "name" : "c_int",
              "dataType" : {
                "type" : "Integer"
              }
            }, {
              "name" : "c_bigint",
              "dataType" : {
                "type" : "Bigint"
              }
            }, {
              "name" : "c_float",
              "dataType" : {
                "type" : "Float"
              }
            }, {
              "name" : "c_double",
              "dataType" : {
                "type" : "Double"
              }
            }, {
              "name" : "c_string",
              "dataType" : {
                "type" : "String"
              }
            }, {
              "name" : "c_boolean",
              "dataType" : {
                "type" : "Boolean"
              }
            }, {
              "name" : "c_array",
              "dataType" : {
                "type" : "Array",
                "dataType" : {
                  "type" : "String"
                }
              }
            }, {
              "name" : "c_struct",
              "dataType" : {
                "type" : "Struct",
                "fields" : [ {
                  "name" : "city",
                  "dataType" : {
                    "type" : "String"
                  }
                }, {
                  "name" : "state",
                  "dataType" : {
                    "type" : "String"
                  }
                }, {
                  "name" : "pin",
                  "dataType" : {
                    "type" : "Bigint"
                  }
                } ]
              }
            } ]
          },
          "slug" : "in0"
        } ],
        "outputs" : [ {
          "id" : "LZZ3hiH_fz-hQVa4-EBll$$PMVOTlKFXu1pwgp07iVMH",
          "schema" : {
            "entityType" : "",
            "providerType" : "Databricks",
            "fields" : [ {
              "name" : "c_tinyint",
              "dataType" : {
                "type" : "TinyInt"
              }
            }, {
              "name" : "c_smallint",
              "dataType" : {
                "type" : "SmallInt"
              }
            }, {
              "name" : "c_int",
              "dataType" : {
                "type" : "Integer"
              }
            }, {
              "name" : "c_bigint",
              "dataType" : {
                "type" : "Bigint"
              }
            }, {
              "name" : "c_float",
              "dataType" : {
                "type" : "Float"
              }
            }, {
              "name" : "c_double",
              "dataType" : {
                "type" : "Double"
              }
            }, {
              "name" : "c_string",
              "dataType" : {
                "type" : "String"
              }
            }, {
              "name" : "c_boolean",
              "dataType" : {
                "type" : "Boolean"
              }
            }, {
              "name" : "c_array",
              "dataType" : {
                "type" : "Array",
                "dataType" : {
                  "type" : "String"
                }
              }
            }, {
              "name" : "c_struct",
              "dataType" : {
                "type" : "Struct",
                "fields" : [ {
                  "name" : "city",
                  "dataType" : {
                    "type" : "String"
                  }
                }, {
                  "name" : "state",
                  "dataType" : {
                    "type" : "String"
                  }
                }, {
                  "name" : "pin",
                  "dataType" : {
                    "type" : "Bigint"
                  }
                } ]
              }
            }, {
              "name" : "api_data",
              "dataType" : {
                "type" : "String"
              }
            } ]
          },
          "slug" : "out0"
        } ]
      }
    },
    "i0JRbr5BFKhDyKiW7RgEk$$EfEkwDjCuksI53yWaA1w8" : {
      "id" : "i0JRbr5BFKhDyKiW7RgEk$$EfEkwDjCuksI53yWaA1w8",
      "component" : "Model",
      "metadata" : {
        "label" : "model_sanity_shared_pipeline_1_combine_multiple_tables_1",
        "phase" : 0
      },
      "properties" : {
        "modelName" : "model_sanity_shared_pipeline_1_combine_multiple_tables_1"
      },
      "visualMetaInfo" : {
        "visualProcessIds" : [ "prophecy__temp_sanity_shared_pipeline_1_post_combine_multiple_tables_1_0", "i0JRbr5BFKhDyKiW7RgEk$$EfEkwDjCuksI53yWaA1w8", "kvGBPPt7F21QjBmKYfzrN$$a5ktq8iFOG8rRvylr-JW0", "IAjlagnDtN91jNls7fgU_$$13bkf2SdP0usZBIh9-4K0", "B4u3Uj3LM0wYqdz_hKAC5$$KqMfETUZ1TDod0pGdMg-s", "prophecy__temp_sanity_shared_pipeline_1_post_Limit_2_0", "tlOac_HgvgEjX1o2fl8qM$$snEmDRv8dS6OgHQNu7bHl", "prophecy__temp_sanity_shared_pipeline_1_pre_child_deduplicate_custom_1_0", "PVmh9soV7SXaIuFIjkdDY$$JrCqAN_zJ1MQK4aUTG9Sh", "_yYnkXQxq_wxe09OrHsBD$$upwrCMcRkBSs-CEWE65Gq", "5O8Ko1DuRGKM7Vo0lLuO1$$aMbvdiq3xaEZpyFhlhK2w", "prophecy__temp_sanity_shared_pipeline_1_post_Limit_2_0", "3ZlanR7M1r6rQGAfAb59q$$CVhSf5_gjGwMq-JRsksWM", "Nf3o73DjzK8hdjU3XM4fQ$$8BDyIMrKxkJnz6ba23Lb4", "A_HtC6XVWzjoYIi-gJZhj$$NAC7Pq0W9ogLKDs1iodLh", "KwXjRsjkX9PCfrCXg0hat", "prophecy__temp_sanity_shared_pipeline_1_pre_Intersect_1_0", "XkI9hJZKYFyw5zMzjNW03$$BIYj4pIEmBr-8OP-tjVvt", "aLHlU-uhMU-o5Zl7nzo--$$5RmVRJ0ppyp_9JZLsMX9E", "mxR703jY0miwDrlOAI41f$$IZdQXNFJGnNdO4lcjhqq5", "4TUNeu4IKEgbyYxd03nsP$$OgqqEI18U_2RWlbuWtt5B", "iu2l3skFal0AY4HXPcuei$$XewFSSlxx5N3Rd8buRbLS", "LijEZ5i9IBYdPgzL3ct58$$zbiRm-j50hYSdcS4gElJq", "6FztLSNPMJZ5_0qUJghOG$$uJlCpOlJgjGuz6wcat_0S", "G3HsUZvqYUbhLNsHSfsec$$5oLcZ4wAT-57Dc7KsS26d", "3Z9Us-wY0wOGU6DpHOCE6$$m-NvMLPeRbd4M8vjDTd-K", "W38i6Kf8pJa35O-CkZXw_$$B7MYcLEx_0Uk9V-KvX0AX", "_6yz8AXBuRgXVPMbJsKXQ$$_-BY9QU-cV4J9YdsTSXHY", "gzNjNQKdKsgb4ncEObCUf$$t2BpyzOqz8r4I9Oy9PwIh" ],
        "type" : "table",
        "isTemporaryTable" : true
      },
      "ports" : {
        "inputs" : [ {
          "id" : "model_sanity_shared_pipeline_1_combine_multiple_tables_1_in_1",
          "slug" : "in_1"
        } ],
        "outputs" : [ {
          "id" : "model_sanity_shared_pipeline_1_combine_multiple_tables_1_out_1",
          "slug" : "out_1"
        } ],
        "isCustomOutputSchema" : false
      }
    },
    "AP54Ot4hBzO3roY-e5KkE$$nhyETPgPRCwdNFPfG71L4" : {
      "id" : "AP54Ot4hBzO3roY-e5KkE$$nhyETPgPRCwdNFPfG71L4",
      "component" : "Model",
      "metadata" : {
        "label" : "model_sanity_shared_pipeline_1_Limit_2",
        "phase" : 0
      },
      "properties" : {
        "modelName" : "model_sanity_shared_pipeline_1_Limit_2"
      },
      "visualMetaInfo" : {
        "visualProcessIds" : [ "prophecy__temp_sanity_shared_pipeline_1_post_Limit_2_0", "AP54Ot4hBzO3roY-e5KkE$$nhyETPgPRCwdNFPfG71L4", "KwXjRsjkX9PCfrCXg0hat" ],
        "type" : "table",
        "isTemporaryTable" : true
      },
      "ports" : {
        "inputs" : [ {
          "id" : "model_sanity_shared_pipeline_1_Limit_2_in_1",
          "slug" : "in_1"
        } ],
        "outputs" : [ {
          "id" : "model_sanity_shared_pipeline_1_Limit_2_out_1",
          "slug" : "out_1"
        } ],
        "isCustomOutputSchema" : false
      }
    },
    "f5py-YZ0owj5gwBvEQ_z5$$rAcakbwQhHHRhg5xrmjYh" : {
      "id" : "f5py-YZ0owj5gwBvEQ_z5$$rAcakbwQhHHRhg5xrmjYh",
      "component" : "Model",
      "metadata" : {
        "label" : "model_sanity_shared_pipeline_1_Join_1",
        "phase" : 0
      },
      "properties" : {
        "modelName" : "model_sanity_shared_pipeline_1_Join_1"
      },
      "visualMetaInfo" : {
        "visualProcessIds" : [ "prophecy__temp_sanity_shared_pipeline_1_pre_Union_1_0", "f5py-YZ0owj5gwBvEQ_z5$$rAcakbwQhHHRhg5xrmjYh", "O252X5-Ki4F7tDFfygq2i$$hkQCwqs0ijXUv58ZvUFW7", "KwXjRsjkX9PCfrCXg0hat" ],
        "type" : "table",
        "isTemporaryTable" : true
      },
      "ports" : {
        "inputs" : [ {
          "id" : "model_sanity_shared_pipeline_1_Join_1_in_1",
          "slug" : "in_1"
        } ],
        "outputs" : [ {
          "id" : "model_sanity_shared_pipeline_1_Join_1_out_1",
          "slug" : "out_1"
        } ],
        "isCustomOutputSchema" : false
      }
    }
  },
  "connections" : [ {
    "id" : "xwmMZIlGwh5kylHzR30fs",
    "source" : "HNpLbqiaSUYog1_UFDCkM$$EiF9W-HTUeJurxrXKV51b",
    "sourcePort" : "LZZ3hiH_fz-hQVa4-EBll$$PMVOTlKFXu1pwgp07iVMH",
    "target" : "prophecy__temp_sanity_shared_pipeline_1_pre_child_deduplicate_custom_1_0",
    "targetPort" : "input_port_4_1"
  }, {
    "id" : "7NBD_ShCpg28o3g8MBSdV$$xkPvtltw-MubzeTiBCs9f",
    "source" : "LijEZ5i9IBYdPgzL3ct58$$zbiRm-j50hYSdcS4gElJq",
    "sourcePort" : "dlSxbtz9jD2_G4IspCMq-$$9EqEM1E3DmPOyRlFzrjqX",
    "target" : "i0JRbr5BFKhDyKiW7RgEk$$EfEkwDjCuksI53yWaA1w8",
    "targetPort" : "model_sanity_shared_pipeline_1_combine_multiple_tables_1_in_1"
  }, {
    "id" : "GD7jfUbSGGozYyAk-hhfN$$zOIrIz4BHtjp6yY-WHtAg",
    "source" : "gzNjNQKdKsgb4ncEObCUf$$t2BpyzOqz8r4I9Oy9PwIh",
    "sourcePort" : "wIHJ3OFikwz3v1LwhN2Xw$$N8PTYELMWt2Aws28fLnfu",
    "target" : "i0JRbr5BFKhDyKiW7RgEk$$EfEkwDjCuksI53yWaA1w8",
    "targetPort" : "model_sanity_shared_pipeline_1_combine_multiple_tables_1_in_1"
  }, {
    "id" : "RYh-LBiWAVICDNIvg1U52",
    "source" : "KwXjRsjkX9PCfrCXg0hat",
    "sourcePort" : "AVVIU7q7XGsbe1eYpFjRb",
    "target" : "i0JRbr5BFKhDyKiW7RgEk$$EfEkwDjCuksI53yWaA1w8",
    "targetPort" : "model_sanity_shared_pipeline_1_combine_multiple_tables_1_in_1"
  }, {
    "id" : "-L3P7icLLSDmXVM6INbVY$$o0iCcMl81JO1C6xBKoiaw",
    "source" : "A_HtC6XVWzjoYIi-gJZhj$$NAC7Pq0W9ogLKDs1iodLh",
    "sourcePort" : "MfD9iJL6jK5aiKCBElIeO$$jl0X9omb90dXUca7sD6YI",
    "target" : "i0JRbr5BFKhDyKiW7RgEk$$EfEkwDjCuksI53yWaA1w8",
    "targetPort" : "model_sanity_shared_pipeline_1_combine_multiple_tables_1_in_1"
  }, {
    "id" : "ZhXq8d8ajcypVQfydLFC5",
    "source" : "5O8Ko1DuRGKM7Vo0lLuO1$$aMbvdiq3xaEZpyFhlhK2w",
    "sourcePort" : "sJH1sTtJbXIaGtM7Enqq8$$HYg_HGhsKr-WLYaEXc9Dd",
    "target" : "i0JRbr5BFKhDyKiW7RgEk$$EfEkwDjCuksI53yWaA1w8",
    "targetPort" : "model_sanity_shared_pipeline_1_combine_multiple_tables_1_in_1"
  }, {
    "id" : "conn_prophecy__temp_sanity_shared_pipeline_1_pre_child_deduplicate_custom_1_0_tlOac_HgvgEjX1o2fl8qM$$snEmDRv8dS6OgHQNu7bHl_Ak67GVUecPXk9iY-5cp8x$$ThHpZijbWCV2yIjZiwgVi",
    "source" : "prophecy__temp_sanity_shared_pipeline_1_pre_child_deduplicate_custom_1_0",
    "sourcePort" : "output_port_4_1",
    "target" : "i0JRbr5BFKhDyKiW7RgEk$$EfEkwDjCuksI53yWaA1w8",
    "targetPort" : "model_sanity_shared_pipeline_1_combine_multiple_tables_1_in_1"
  }, {
    "id" : "nbjUG9__wpWLBwkeWX0fk",
    "source" : "KwXjRsjkX9PCfrCXg0hat",
    "sourcePort" : "AVVIU7q7XGsbe1eYpFjRb",
    "target" : "7Bz9vvW3NBIp2tGXBFhkT$$bfwMevlAlCaFaUhYezDl0",
    "targetPort" : "model_sanity_shared_pipeline_1_Union_1_in_1"
  }, {
    "id" : "iJ4G6esxIaSVUtgKBn_yS",
    "source" : "KwXjRsjkX9PCfrCXg0hat",
    "sourcePort" : "AVVIU7q7XGsbe1eYpFjRb",
    "target" : "WjtJ3kS1VM6qaRT4v_N-j$$Sm1d7cP-2PxgZz6lq65iS",
    "targetPort" : "model_sanity_shared_pipeline_1_SQLStatement_1_in_1"
  }, {
    "id" : "tiIyPkT-d5Xm8GJniNGJr$$Ld4Zep_G7rHl6Hz7rF-8i",
    "source" : "O252X5-Ki4F7tDFfygq2i$$hkQCwqs0ijXUv58ZvUFW7",
    "sourcePort" : "xZXY0LTsgHGTsmJu7orGy$$ypSxKTAQXY33pJ_33uI2s",
    "target" : "f5py-YZ0owj5gwBvEQ_z5$$rAcakbwQhHHRhg5xrmjYh",
    "targetPort" : "model_sanity_shared_pipeline_1_Join_1_in_1"
  }, {
    "id" : "FPTbtWQ2A6K755DFkuNWb",
    "source" : "KwXjRsjkX9PCfrCXg0hat",
    "sourcePort" : "AVVIU7q7XGsbe1eYpFjRb",
    "target" : "f5py-YZ0owj5gwBvEQ_z5$$rAcakbwQhHHRhg5xrmjYh",
    "targetPort" : "model_sanity_shared_pipeline_1_Join_1_in_1"
  }, {
    "id" : "T7ZdKwEYaV4rljCPcT1QZ",
    "source" : "KwXjRsjkX9PCfrCXg0hat",
    "sourcePort" : "AVVIU7q7XGsbe1eYpFjRb",
    "target" : "AP54Ot4hBzO3roY-e5KkE$$nhyETPgPRCwdNFPfG71L4",
    "targetPort" : "model_sanity_shared_pipeline_1_Limit_2_in_1"
  }, {
    "id" : "conn_WjtJ3kS1VM6qaRT4v_N-j$$Sm1d7cP-2PxgZz6lq65iS_HNpLbqiaSUYog1_UFDCkM$$EiF9W-HTUeJurxrXKV51b",
    "source" : "WjtJ3kS1VM6qaRT4v_N-j$$Sm1d7cP-2PxgZz6lq65iS",
    "sourcePort" : "model_sanity_shared_pipeline_1_SQLStatement_1_out_1",
    "target" : "HNpLbqiaSUYog1_UFDCkM$$EiF9W-HTUeJurxrXKV51b",
    "targetPort" : "I6sVfPnYM-GUiAOHVIiDH$$bE4IJyJ9tn07FCYiGSz7j"
  }, {
    "id" : "conn_7Bz9vvW3NBIp2tGXBFhkT$$bfwMevlAlCaFaUhYezDl0_i0JRbr5BFKhDyKiW7RgEk$$EfEkwDjCuksI53yWaA1w8",
    "source" : "7Bz9vvW3NBIp2tGXBFhkT$$bfwMevlAlCaFaUhYezDl0",
    "sourcePort" : "model_sanity_shared_pipeline_1_Union_1_out_1",
    "target" : "i0JRbr5BFKhDyKiW7RgEk$$EfEkwDjCuksI53yWaA1w8",
    "targetPort" : "model_sanity_shared_pipeline_1_combine_multiple_tables_1_in_1"
  }, {
    "id" : "conn_i0JRbr5BFKhDyKiW7RgEk$$EfEkwDjCuksI53yWaA1w8_8jG3o48L9d_rvj-cka_Q1$$KQ4OE4SMpoDBGxDehgahQ",
    "source" : "i0JRbr5BFKhDyKiW7RgEk$$EfEkwDjCuksI53yWaA1w8",
    "sourcePort" : "model_sanity_shared_pipeline_1_combine_multiple_tables_1_out_1",
    "target" : "8jG3o48L9d_rvj-cka_Q1$$KQ4OE4SMpoDBGxDehgahQ",
    "targetPort" : "5uDzNR7b0bONBGAUVpPp9$$T4Usuyi60k7bLHT6O2y5K"
  }, {
    "id" : "conn_AP54Ot4hBzO3roY-e5KkE$$nhyETPgPRCwdNFPfG71L4_i0JRbr5BFKhDyKiW7RgEk$$EfEkwDjCuksI53yWaA1w8",
    "source" : "AP54Ot4hBzO3roY-e5KkE$$nhyETPgPRCwdNFPfG71L4",
    "sourcePort" : "model_sanity_shared_pipeline_1_Limit_2_out_1",
    "target" : "i0JRbr5BFKhDyKiW7RgEk$$EfEkwDjCuksI53yWaA1w8",
    "targetPort" : "model_sanity_shared_pipeline_1_combine_multiple_tables_1_in_1"
  }, {
    "id" : "conn_AP54Ot4hBzO3roY-e5KkE$$nhyETPgPRCwdNFPfG71L4_i0JRbr5BFKhDyKiW7RgEk$$EfEkwDjCuksI53yWaA1w8",
    "source" : "AP54Ot4hBzO3roY-e5KkE$$nhyETPgPRCwdNFPfG71L4",
    "sourcePort" : "model_sanity_shared_pipeline_1_Limit_2_out_1",
    "target" : "i0JRbr5BFKhDyKiW7RgEk$$EfEkwDjCuksI53yWaA1w8",
    "targetPort" : "model_sanity_shared_pipeline_1_combine_multiple_tables_1_in_1"
  }, {
    "id" : "conn_AP54Ot4hBzO3roY-e5KkE$$nhyETPgPRCwdNFPfG71L4_WjtJ3kS1VM6qaRT4v_N-j$$Sm1d7cP-2PxgZz6lq65iS",
    "source" : "AP54Ot4hBzO3roY-e5KkE$$nhyETPgPRCwdNFPfG71L4",
    "sourcePort" : "model_sanity_shared_pipeline_1_Limit_2_out_1",
    "target" : "WjtJ3kS1VM6qaRT4v_N-j$$Sm1d7cP-2PxgZz6lq65iS",
    "targetPort" : "model_sanity_shared_pipeline_1_SQLStatement_1_in_1"
  }, {
    "id" : "conn_f5py-YZ0owj5gwBvEQ_z5$$rAcakbwQhHHRhg5xrmjYh_7Bz9vvW3NBIp2tGXBFhkT$$bfwMevlAlCaFaUhYezDl0",
    "source" : "f5py-YZ0owj5gwBvEQ_z5$$rAcakbwQhHHRhg5xrmjYh",
    "sourcePort" : "model_sanity_shared_pipeline_1_Join_1_out_1",
    "target" : "7Bz9vvW3NBIp2tGXBFhkT$$bfwMevlAlCaFaUhYezDl0",
    "targetPort" : "model_sanity_shared_pipeline_1_Union_1_in_1"
  } ],
  "component" : "Pipeline"
}