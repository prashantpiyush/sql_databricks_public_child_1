WITH qa_complex_array_data AS (

  SELECT * 
  
  FROM {{ source('hive_metastore.qa_database', 'qa_complex_array_data') }}

),

exploded_complex_data AS (

  {#Breaks down complex data structures into individual components for detailed analysis.#}
  SELECT 
    array_of_integers.col AS array_of_integers,
    array_of_strings.col AS array_of_strings,
    id AS id,
    name AS name,
    nested_array_of_strings.col AS nested_array_of_strings,
    array_of_structs.col.description AS description,
    array_of_mixed_structs.col.nested.flag AS flag,
    array_of_mixed_structs.col AS array_of_mixed_structs,
    array_of_mixed_structs.col.details AS details
  
  FROM qa_complex_array_data AS in0, 
  LATERAL explode_outer(array_of_integers) AS array_of_integers, 
  LATERAL explode_outer(array_of_strings) AS array_of_strings, 
  LATERAL explode_outer(nested_array_of_strings) AS nested_array_of_strings, 
  LATERAL explode_outer(array_of_structs) AS array_of_structs, 
  LATERAL explode_outer(array_of_mixed_structs) AS array_of_mixed_structs

),

win_row AS (

  {#Organizes complex data into ranked entries for better analysis and reporting.#}
  SELECT 
    *,
    row_number() OVER (PARTITION BY id, name, array_of_mixed_structs.nested.value ORDER BY array_of_mixed_structs.nested.flag ASC NULLS LAST, description DESC NULLS FIRST, details ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS id1,
    row_number() OVER (PARTITION BY id, name, array_of_mixed_structs.nested.value ORDER BY array_of_mixed_structs.nested.flag ASC NULLS LAST, description DESC NULLS FIRST, details ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS name1
  
  FROM exploded_complex_data AS in0

),

win_range AS (

  {#Calculates maximum IDs and average values for grouped data, enhancing insights into trends.#}
  SELECT 
    *,
    max(id) OVER (PARTITION BY array_of_mixed_structs.nested.value ORDER BY id ASC, id1 ASC, name1 ASC RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS id2,
    avg(array_of_mixed_structs.nested.value) OVER (PARTITION BY array_of_mixed_structs.nested.value ORDER BY id ASC, id1 ASC, name1 ASC RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS name2
  
  FROM win_row AS in0

),

very_complex_table AS (

  SELECT * 
  
  FROM {{ source('hive_metastore.qa_database', 'very_complex_table') }}

),

distinct_asthma_medications AS (

  {#Identifies unique classes of asthma medications for better treatment options.#}
  SELECT DISTINCT `c_complex-array`.Asthma.medications.medicationsClasses.className_1.`associated-Drug`
  
  FROM very_complex_table AS in0

)

SELECT *

FROM distinct_asthma_medications
