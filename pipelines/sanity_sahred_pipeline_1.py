Schedule = Schedule(cron = "* 0 2 * * * *", timezone = "GMT", emails = ["email@gmail.com"], enabled = False)

with DAG(Schedule = Schedule):
    S3Source_1 = SourceTask(
        task_id = "S3Source_1", 
        component = "OrchestrationSource", 
        kind = "S3Source", 
        connector = Connection(kind = "s3", id = "s3"), 
        format = JSONFormat(
          inferenceDataSamplingLimit = 0, 
          multiDoc = False, 
          schema = {
            "fields": [{"dataType" : {"type" : "utf8"}, "description" : "The name of the individual", "name" : "name"},                         {
                          "dataType": {"type" : "bool"}, 
                          "description": "Indicates whether the individual is a student", 
                          "name": "is_student"
                        },                         {
                          "dataType": {
                            "fields": [{
                                          "dataType": {"type" : "utf8"}, 
                                          "description": "City where the individual resides", 
                                          "name": "city"
                                        },                                         {
                                          "dataType": {"type" : "utf8"}, 
                                          "description": "ZIP code of the individual's address", 
                                          "name": "zip"
                                        },                                         {
                                          "dataType": {"type" : "utf8"}, 
                                          "description": "Street address of the individual", 
                                          "name": "street"
                                        }], 
                            "type": "Struct"
                          }, 
                          "description": "The address details of the individual", 
                          "name": "address"
                        },                         {
                          "dataType": {
                            "dataType": {
                              "fields": [                                          {
                                            "dataType": {
                                              "dataType": {
                                                "fields": [                                                            {
                                                              "dataType": {
                                                                "dataType": {
                                                                  "fields": [                                                                              {
                                                                                "dataType": {
                                                                                  "dataType": {
                                                                                    "fields": [                                                                                                {
                                                                                                  "dataType": {
                                                                                                    "dataType": {
                                                                                                      "fields": [{
                                                                                                                    "dataType": {
                                                                                                                      "dataType": {
                                                                                                                        "fields": [{
                                                                                                                                      "dataType": {
                                                                                                                                        "type": "utf8"
                                                                                                                                      }, 
                                                                                                                                      "description": "Name of the associated drug in the second class of diabetes medications", 
                                                                                                                                      "name": "name"
                                                                                                                                    },                                                                                                                                     {
                                                                                                                                      "dataType": {
                                                                                                                                        "type": "float64"
                                                                                                                                      }, 
                                                                                                                                      "description": "Dosage of the associated drug in the second class of diabetes medications", 
                                                                                                                                      "name": "dose"
                                                                                                                                    },                                                                                                                                     {
                                                                                                                                      "dataType": {
                                                                                                                                        "type": "float64"
                                                                                                                                      }, 
                                                                                                                                      "description": "Indicates if the associated drug in the second class of diabetes medications is used", 
                                                                                                                                      "name": "cf-used"
                                                                                                                                    },                                                                                                                                     {
                                                                                                                                      "dataType": {
                                                                                                                                        "type": "bool"
                                                                                                                                      }, 
                                                                                                                                      "description": "Indicates whether the associated drug should be taken as prescribed.", 
                                                                                                                                      "name": "take-it_on"
                                                                                                                                    },                                                                                                                                     {
                                                                                                                                      "dataType": {
                                                                                                                                        "type": "utf8"
                                                                                                                                      }, 
                                                                                                                                      "description": "Strength of the associated drug for diabetes management.", 
                                                                                                                                      "name": "strength"
                                                                                                                                    }], 
                                                                                                                        "type": "Struct"
                                                                                                                      }, 
                                                                                                                      "type": "Array"
                                                                                                                    }, 
                                                                                                                    "description": "List of drugs associated with the second class of diabetes medications", 
                                                                                                                    "name": "associated-Drug"
                                                                                                                  },                                                                                                                   {
                                                                                                                    "dataType": {
                                                                                                                      "dataType": {
                                                                                                                        "fields": [{
                                                                                                                                      "dataType": {
                                                                                                                                        "type": "utf8"
                                                                                                                                      }, 
                                                                                                                                      "description": "Name of the second associated drug for diabetes management.", 
                                                                                                                                      "name": "name"
                                                                                                                                    },                                                                                                                                     {
                                                                                                                                      "dataType": {
                                                                                                                                        "type": "float64"
                                                                                                                                      }, 
                                                                                                                                      "description": "Dose of the second associated drug for diabetes treatment.", 
                                                                                                                                      "name": "dose"
                                                                                                                                    },                                                                                                                                     {
                                                                                                                                      "dataType": {
                                                                                                                                        "type": "float64"
                                                                                                                                      }, 
                                                                                                                                      "description": "Indicates the frequency of use for the second associated drug in the diabetes medication class.", 
                                                                                                                                      "name": "cf-used"
                                                                                                                                    },                                                                                                                                     {
                                                                                                                                      "dataType": {
                                                                                                                                        "type": "bool"
                                                                                                                                      }, 
                                                                                                                                      "description": "Flag indicating whether the second associated drug in the diabetes medication class should be taken.", 
                                                                                                                                      "name": "take-it_on"
                                                                                                                                    },                                                                                                                                     {
                                                                                                                                      "dataType": {
                                                                                                                                        "type": "utf8"
                                                                                                                                      }, 
                                                                                                                                      "description": "Strength of the second associated drug in the diabetes medication class.", 
                                                                                                                                      "name": "strength"
                                                                                                                                    }], 
                                                                                                                        "type": "Struct"
                                                                                                                      }, 
                                                                                                                      "type": "Array"
                                                                                                                    }, 
                                                                                                                    "description": "Second associated drug for diabetes treatment.", 
                                                                                                                    "name": "associated-Drug#2"
                                                                                                                  }], 
                                                                                                      "type": "Struct"
                                                                                                    }, 
                                                                                                    "type": "Array"
                                                                                                  }, 
                                                                                                  "description": "Class name for the second category of diabetes medications", 
                                                                                                  "name": "className_2"
                                                                                                },                                                                                                 {
                                                                                                  "dataType": {
                                                                                                    "dataType": {
                                                                                                      "fields": [{
                                                                                                                    "dataType": {
                                                                                                                      "dataType": {
                                                                                                                        "fields": [{
                                                                                                                                      "dataType": {
                                                                                                                                        "type": "float64"
                                                                                                                                      }, 
                                                                                                                                      "description": "The dose of the associated drug for diabetes treatment.", 
                                                                                                                                      "name": "dose"
                                                                                                                                    },                                                                                                                                     {
                                                                                                                                      "dataType": {
                                                                                                                                        "type": "float64"
                                                                                                                                      }, 
                                                                                                                                      "description": "The frequency of use for the associated drug in diabetes management.", 
                                                                                                                                      "name": "cf-used"
                                                                                                                                    },                                                                                                                                     {
                                                                                                                                      "dataType": {
                                                                                                                                        "type": "bool"
                                                                                                                                      }, 
                                                                                                                                      "description": "Indicates whether the associated drug should be taken on a specific schedule.", 
                                                                                                                                      "name": "take-it_on"
                                                                                                                                    },                                                                                                                                     {
                                                                                                                                      "dataType": {
                                                                                                                                        "type": "utf8"
                                                                                                                                      }, 
                                                                                                                                      "description": "The strength of the associated drug prescribed for diabetes.", 
                                                                                                                                      "name": "strength"
                                                                                                                                    },                                                                                                                                     {
                                                                                                                                      "dataType": {
                                                                                                                                        "type": "utf8"
                                                                                                                                      }, 
                                                                                                                                      "description": "The name of the associated drug used in diabetes treatment.", 
                                                                                                                                      "name": "name"
                                                                                                                                    }], 
                                                                                                                        "type": "Struct"
                                                                                                                      }, 
                                                                                                                      "type": "Array"
                                                                                                                    }, 
                                                                                                                    "description": "List of associated drugs within the first class of diabetes medications.", 
                                                                                                                    "name": "associated-Drug"
                                                                                                                  },                                                                                                                   {
                                                                                                                    "dataType": {
                                                                                                                      "dataType": {
                                                                                                                        "fields": [{
                                                                                                                                      "dataType": {
                                                                                                                                        "type": "utf8"
                                                                                                                                      }, 
                                                                                                                                      "description": "Name of the second associated drug for the first medication class related to diabetes.", 
                                                                                                                                      "name": "name"
                                                                                                                                    },                                                                                                                                     {
                                                                                                                                      "dataType": {
                                                                                                                                        "type": "float64"
                                                                                                                                      }, 
                                                                                                                                      "description": "Dose of the second associated drug for the first medication class related to diabetes.", 
                                                                                                                                      "name": "dose"
                                                                                                                                    },                                                                                                                                     {
                                                                                                                                      "dataType": {
                                                                                                                                        "type": "float64"
                                                                                                                                      }, 
                                                                                                                                      "description": "Indicates if the second associated drug for the first medication class related to diabetes is used.", 
                                                                                                                                      "name": "cf-used"
                                                                                                                                    },                                                                                                                                     {
                                                                                                                                      "dataType": {
                                                                                                                                        "type": "bool"
                                                                                                                                      }, 
                                                                                                                                      "description": "Indicates if the second associated drug for the first medication class related to diabetes should be taken.", 
                                                                                                                                      "name": "take-it_on"
                                                                                                                                    },                                                                                                                                     {
                                                                                                                                      "dataType": {
                                                                                                                                        "type": "utf8"
                                                                                                                                      }, 
                                                                                                                                      "description": "Strength of the second associated drug for diabetes medications", 
                                                                                                                                      "name": "strength"
                                                                                                                                    }], 
                                                                                                                        "type": "Struct"
                                                                                                                      }, 
                                                                                                                      "type": "Array"
                                                                                                                    }, 
                                                                                                                    "description": "List of associated drugs for the first medication class related to diabetes.", 
                                                                                                                    "name": "associated-Drug#2"
                                                                                                                  }], 
                                                                                                      "type": "Struct"
                                                                                                    }, 
                                                                                                    "type": "Array"
                                                                                                  }, 
                                                                                                  "description": "First class of medications used for diabetes treatment.", 
                                                                                                  "name": "className_1"
                                                                                                }], 
                                                                                    "type": "Struct"
                                                                                  }, 
                                                                                  "type": "Array"
                                                                                }, 
                                                                                "description": "Classes of medications associated with diabetes treatments", 
                                                                                "name": "medicationsClasses"
                                                                              }], 
                                                                  "type": "Struct"
                                                                }, 
                                                                "type": "Array"
                                                              }, 
                                                              "description": "Detailed list of diabetes medications", 
                                                              "name": "medications"
                                                            },                                                             {
                                                              "dataType": {
                                                                "dataType": {
                                                                  "fields": [{
                                                                                "dataType": {"type" : "utf8"}, 
                                                                                "description": "Field indicating missing information in diabetes lab results", 
                                                                                "name": "missing_field"
                                                                              }], 
                                                                  "type": "Struct"
                                                                }, 
                                                                "type": "Array"
                                                              }, 
                                                              "description": "Laboratory tests associated with diabetes", 
                                                              "name": "labs"
                                                            }], 
                                                "type": "Struct"
                                              }, 
                                              "type": "Array"
                                            }, 
                                            "description": "Medications specifically related to diabetes management", 
                                            "name": "Diabetes"
                                          },                                           {
                                            "dataType": {
                                              "dataType": {
                                                "fields": [{
                                                              "dataType": {
                                                                "dataType": {
                                                                  "fields": [{
                                                                                "dataType": {"type" : "utf8"}, 
                                                                                "description": "Field indicating missing lab information related to asthma medications", 
                                                                                "name": "missing_field"
                                                                              }], 
                                                                  "type": "Struct"
                                                                }, 
                                                                "type": "Array"
                                                              }, 
                                                              "description": "Laboratory tests associated with asthma", 
                                                              "name": "labs"
                                                            },                                                             {
                                                              "dataType": {
                                                                "dataType": {
                                                                  "fields": [                                                                              {
                                                                                "dataType": {
                                                                                  "dataType": {
                                                                                    "fields": [                                                                                                {
                                                                                                  "dataType": {
                                                                                                    "dataType": {
                                                                                                      "fields": [{
                                                                                                                    "dataType": {
                                                                                                                      "dataType": {
                                                                                                                        "fields": [{
                                                                                                                                      "dataType": {
                                                                                                                                        "type": "utf8"
                                                                                                                                      }, 
                                                                                                                                      "description": "Strength of the associated drug for asthma treatment", 
                                                                                                                                      "name": "strength"
                                                                                                                                    },                                                                                                                                     {
                                                                                                                                      "dataType": {
                                                                                                                                        "type": "utf8"
                                                                                                                                      }, 
                                                                                                                                      "description": "Name of the associated drug used for asthma", 
                                                                                                                                      "name": "name"
                                                                                                                                    },                                                                                                                                     {
                                                                                                                                      "dataType": {
                                                                                                                                        "type": "float64"
                                                                                                                                      }, 
                                                                                                                                      "description": "Dosage of the associated drug for asthma management", 
                                                                                                                                      "name": "dose"
                                                                                                                                    },                                                                                                                                     {
                                                                                                                                      "dataType": {
                                                                                                                                        "type": "float64"
                                                                                                                                      }, 
                                                                                                                                      "description": "Indicates if the associated drug is currently used", 
                                                                                                                                      "name": "cf-used"
                                                                                                                                    },                                                                                                                                     {
                                                                                                                                      "dataType": {
                                                                                                                                        "type": "bool"
                                                                                                                                      }, 
                                                                                                                                      "description": "Indicates if the associated drug should be taken regularly", 
                                                                                                                                      "name": "take-it_on"
                                                                                                                                    }], 
                                                                                                                        "type": "Struct"
                                                                                                                      }, 
                                                                                                                      "type": "Array"
                                                                                                                    }, 
                                                                                                                    "description": "Drugs associated with the second class of asthma medications", 
                                                                                                                    "name": "associated-Drug"
                                                                                                                  },                                                                                                                   {
                                                                                                                    "dataType": {
                                                                                                                      "dataType": {
                                                                                                                        "fields": [{
                                                                                                                                      "dataType": {
                                                                                                                                        "type": "utf8"
                                                                                                                                      }, 
                                                                                                                                      "description": "Strength of the second associated drug in the asthma medication class.", 
                                                                                                                                      "name": "strength"
                                                                                                                                    },                                                                                                                                     {
                                                                                                                                      "dataType": {
                                                                                                                                        "type": "utf8"
                                                                                                                                      }, 
                                                                                                                                      "description": "Name of the second associated drug in the asthma medication class.", 
                                                                                                                                      "name": "name"
                                                                                                                                    },                                                                                                                                     {
                                                                                                                                      "dataType": {
                                                                                                                                        "type": "float64"
                                                                                                                                      }, 
                                                                                                                                      "description": "Dose of the second associated drug in the asthma medication class.", 
                                                                                                                                      "name": "dose"
                                                                                                                                    },                                                                                                                                     {
                                                                                                                                      "dataType": {
                                                                                                                                        "type": "float64"
                                                                                                                                      }, 
                                                                                                                                      "description": "Indicates if the second associated drug in the asthma medication class is used.", 
                                                                                                                                      "name": "cf-used"
                                                                                                                                    },                                                                                                                                     {
                                                                                                                                      "dataType": {
                                                                                                                                        "type": "bool"
                                                                                                                                      }, 
                                                                                                                                      "description": "Indicates whether the second associated drug for className_2 in asthma medications should be taken.", 
                                                                                                                                      "name": "take-it_on"
                                                                                                                                    }], 
                                                                                                                        "type": "Struct"
                                                                                                                      }, 
                                                                                                                      "type": "Array"
                                                                                                                    }, 
                                                                                                                    "description": "List of associated drugs for the second class of asthma medications.", 
                                                                                                                    "name": "associated-Drug#2"
                                                                                                                  }], 
                                                                                                      "type": "Struct"
                                                                                                    }, 
                                                                                                    "type": "Array"
                                                                                                  }, 
                                                                                                  "description": "Second class of medications associated with asthma treatment", 
                                                                                                  "name": "className_2"
                                                                                                },                                                                                                 {
                                                                                                  "dataType": {
                                                                                                    "dataType": {
                                                                                                      "fields": [{
                                                                                                                    "dataType": {
                                                                                                                      "dataType": {
                                                                                                                        "fields": [{
                                                                                                                                      "dataType": {
                                                                                                                                        "type": "utf8"
                                                                                                                                      }, 
                                                                                                                                      "description": "Name of the associated drug in the first class of asthma medications.", 
                                                                                                                                      "name": "name"
                                                                                                                                    },                                                                                                                                     {
                                                                                                                                      "dataType": {
                                                                                                                                        "type": "float64"
                                                                                                                                      }, 
                                                                                                                                      "description": "Dose of the associated drug in the first class of asthma medications.", 
                                                                                                                                      "name": "dose"
                                                                                                                                    },                                                                                                                                     {
                                                                                                                                      "dataType": {
                                                                                                                                        "type": "float64"
                                                                                                                                      }, 
                                                                                                                                      "description": "Indicates whether the associated drug has been used", 
                                                                                                                                      "name": "cf-used"
                                                                                                                                    },                                                                                                                                     {
                                                                                                                                      "dataType": {
                                                                                                                                        "type": "bool"
                                                                                                                                      }, 
                                                                                                                                      "description": "Indicates if the associated drug should be taken", 
                                                                                                                                      "name": "take-it_on"
                                                                                                                                    },                                                                                                                                     {
                                                                                                                                      "dataType": {
                                                                                                                                        "type": "utf8"
                                                                                                                                      }, 
                                                                                                                                      "description": "Strength of the associated drug", 
                                                                                                                                      "name": "strength"
                                                                                                                                    }], 
                                                                                                                        "type": "Struct"
                                                                                                                      }, 
                                                                                                                      "type": "Array"
                                                                                                                    }, 
                                                                                                                    "description": "List of associated drugs under the first class of asthma medications.", 
                                                                                                                    "name": "associated-Drug"
                                                                                                                  },                                                                                                                   {
                                                                                                                    "dataType": {
                                                                                                                      "dataType": {
                                                                                                                        "fields": [{
                                                                                                                                      "dataType": {
                                                                                                                                        "type": "float64"
                                                                                                                                      }, 
                                                                                                                                      "description": "Dose of the second associated drug", 
                                                                                                                                      "name": "dose"
                                                                                                                                    },                                                                                                                                     {
                                                                                                                                      "dataType": {
                                                                                                                                        "type": "float64"
                                                                                                                                      }, 
                                                                                                                                      "description": "Indicates the frequency of use for the associated drug in asthma treatment.", 
                                                                                                                                      "name": "cf-used"
                                                                                                                                    },                                                                                                                                     {
                                                                                                                                      "dataType": {
                                                                                                                                        "type": "bool"
                                                                                                                                      }, 
                                                                                                                                      "description": "Flag indicating whether the associated drug should be taken.", 
                                                                                                                                      "name": "take-it_on"
                                                                                                                                    },                                                                                                                                     {
                                                                                                                                      "dataType": {
                                                                                                                                        "type": "utf8"
                                                                                                                                      }, 
                                                                                                                                      "description": "Strength of the associated drug used for asthma management.", 
                                                                                                                                      "name": "strength"
                                                                                                                                    },                                                                                                                                     {
                                                                                                                                      "dataType": {
                                                                                                                                        "type": "utf8"
                                                                                                                                      }, 
                                                                                                                                      "description": "Name of the associated drug prescribed for asthma.", 
                                                                                                                                      "name": "name"
                                                                                                                                    }], 
                                                                                                                        "type": "Struct"
                                                                                                                      }, 
                                                                                                                      "type": "Array"
                                                                                                                    }, 
                                                                                                                    "description": "Second associated drug in the medication class", 
                                                                                                                    "name": "associated-Drug#2"
                                                                                                                  }], 
                                                                                                      "type": "Struct"
                                                                                                    }, 
                                                                                                    "type": "Array"
                                                                                                  }, 
                                                                                                  "description": "Details of the first class of medications related to asthma.", 
                                                                                                  "name": "className_1"
                                                                                                }], 
                                                                                    "type": "Struct"
                                                                                  }, 
                                                                                  "type": "Array"
                                                                                }, 
                                                                                "description": "Classes of medications used for asthma treatment", 
                                                                                "name": "medicationsClasses"
                                                                              }], 
                                                                  "type": "Struct"
                                                                }, 
                                                                "type": "Array"
                                                              }, 
                                                              "description": "List of medications prescribed for asthma", 
                                                              "name": "medications"
                                                            }], 
                                                "type": "Struct"
                                              }, 
                                              "type": "Array"
                                            }, 
                                            "description": "Medications related to asthma treatment", 
                                            "name": "Asthma"
                                          }], 
                              "type": "Struct"
                            }, 
                            "type": "Array"
                          }, 
                          "description": "List of medications prescribed to the individual", 
                          "name": "medications"
                        },                         {"dataType" : {"type" : "float64"}, "description" : "Age of the individual.", "name" : "age"},                         {"dataType" : {"type" : "float64"}, "description" : "Height of the individual", "name" : "height"},                         {
                          "dataType": {"dataType" : {"type" : "utf8"}, "type" : "Array"}, 
                          "description": "List of hobbies the individual enjoys", 
                          "name": "hobbies"
                        },                         {
                          "dataType": {
                            "dataType": {
                              "fields": [{
                                            "dataType": {"type" : "utf8"}, 
                                            "description": "Subject name for which the score is recorded", 
                                            "name": "subject"
                                          },                                           {
                                            "dataType": {"type" : "float64"}, 
                                            "description": "Score achieved in the respective subject", 
                                            "name": "score"
                                          }], 
                              "type": "Struct"
                            }, 
                            "type": "Array"
                          }, 
                          "description": "Collection of subjects and scores achieved by the individual", 
                          "name": "grades"
                        },                         {
                          "dataType": {"type" : "utf8"}, 
                          "description": "The date and time when the individual registered.", 
                          "name": "registered_at"
                        }], 
            "providerType": "Arrow"
          }
        ), 
        filePath = "/datasets/orchestration_datasets/json/dict/performance/json_file_500MB.json"
    )
    S_MSSQLALL = SourceTask(
        task_id = "S_MSSQLALL", 
        component = "OrchestrationSource", 
        kind = "MSSQLSource", 
        connector = Connection(kind = "mssql", id = "mssql"), 
        format = MSSQLFormat(), 
        tableFullName = {"database" : "qa_performance", "name" : "all_type_table", "schema" : "qa_schema"}
    )
    shared_seed_basic = Task(
        task_id = "shared_seed_basic", 
        component = "Dataset", 
        table = {"name" : "shared_seed_basic", "sourceType" : "Seed", "alias" : ""}
    )
    S_MSSQLALL = Task(
        task_id = "S_MSSQLALL", 
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
    env_uitesting_shared_useallmodel_1_1 = Task(
        task_id = "env_uitesting_shared_useallmodel_1_1", 
        component = "Model", 
        modelName = "env_uitesting_shared_useallmodel_1"
    )
    S3Source_1 = Task(
        task_id = "S3Source_1", 
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
    S3Source_1.out0 >> S3Source_1.input_port_0_1
    S_MSSQLALL.output_port_1_1 >> model_sanity_sahred_pipeline_1_non_matching_int_count.in_1
    all_type_non_partitioned.out >> model_sanity_sahred_pipeline_1_non_matching_int_count.in_1
    env_uitesting_shared_useallmodel_1_1.out >> model_sanity_sahred_pipeline_1_non_matching_int_count.in_1
    model_sanity_sahred_pipeline_1_non_matching_int_count.out_1 >> notify_shared_sanity.in0
    S_MSSQLALL.out0 >> S_MSSQLALL.input_port_1_1
    S3Source_1.output_port_0_1 >> model_sanity_sahred_pipeline_1_non_matching_int_count.in_1
