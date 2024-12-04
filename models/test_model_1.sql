WITH very_complex_table AS (

  SELECT * 
  
  FROM {{ source('hive_metastore.qa_database', 'very_complex_table') }}

),

distinct_asthma_medications AS (

  {#Identifies unique classes of asthma medications for better treatment options.#}
  SELECT DISTINCT `c_complex-array`.Asthma.medications.medicationsClasses.className_1.`associated-Drug`
  
  FROM very_complex_table AS in0

),

IS_INCREMENTAL AS (

  SELECT * 
  
  FROM {{ source('qa-team.qa_database', 'table_keywords') }}

),

PIVOT AS (

  {#Retrieves a comprehensive list of SQL keywords for reference or documentation purposes.#}
  SELECT 
    LIMIT AS LIMIT,
    JOIN AS JOIN,
    ASC AS ASC,
    DESC AS DESC,
    INNER AS INNER,
    FULL AS FULL,
    CROSS AS CROSS,
    SEMI AS SEMI,
    ANTI AS ANTI,
    ESCAPE AS ESCAPE,
    INTERVAL AS INTERVAL,
    LEFT AS LEFT,
    RIGHT AS RIGHT,
    OUTER AS OUTER,
    TIMESTAMP AS TIMESTAMP,
    DATETIME AS DATETIME,
    END AS END,
    OVER AS OVER,
    RANGE AS RANGE,
    PRECEDING AS PRECEDING,
    FORMAT AS FORMAT,
    ARRAY AS ARRAY,
    PARTITION AS PARTITION,
    UNBOUNDED AS UNBOUNDED,
    JSON AS JSON,
    TYPE AS TYPE,
    IGNORE AS IGNORE,
    RESPECT AS RESPECT,
    VERSION AS VERSION,
    FILTER AS FILTER,
    CLUSTER AS CLUSTER,
    DISTRIBUTE AS DISTRIBUTE,
    ROLLUP AS ROLLUP,
    CUBE AS CUBE,
    GROUPING AS GROUPING,
    SETS AS SETS,
    LATERAL AS LATERAL,
    OFFSET AS OFFSET,
    SORT AS SORT,
    WINDOW AS WINDOW,
    FETCH AS FETCH,
    catch AS catch,
    finally AS finally,
    object AS object,
    protected AS protected,
    return AS return,
    final AS final,
    new AS new,
    while AS while,
    yield AS yield,
    true AS true,
    false AS false,
    trait AS trait,
    except AS except,
    do AS do,
    extends AS extends,
    assert AS assert,
    global AS global,
    import AS import
  
  FROM IS_INCREMENTAL AS in0

),

UNPIVOT AS (

  SELECT * 
  
  FROM PIVOT AS in0
  
  LIMIT 10

),

array_contains AS (

  SELECT * 
  
  FROM UNPIVOT AS in0
  
  WHERE true

),

map_contains_key AS (

  {#Sorts filtered results based on specified criteria for better visibility.#}
  SELECT * 
  
  FROM array_contains AS in0
  
  ORDER BY LIMIT ASC, JOIN DESC

)

SELECT *

FROM distinct_asthma_medications
