

{%
  set v_complex_dict = {
    "problems": [
      {
        "Diabetes": [
          {
            "medications": [
              {
                "medicationsClasses": [
                  {
                    "className": [
                      {
                        "associatedDrug": [{ "name": "asprin", "dose": "", "strength": "500 mg" }], 
                        "associatedDrug#2": [{ "name": "somethingElse", "dose": "", "strength": "500 mg" }]
                      }
                    ], 
                    "className2": [
                      {
                        "associatedDrug": [{ "name": "asprin", "dose": "", "strength": "500 mg" }], 
                        "associatedDrug#2": [{ "name": "somethingElse", "dose": "", "strength": "500 mg" }]
                      }
                    ]
                  }
                ]
              }
            ], 
            "labs": [{ "missing_field": "missing_value" }]
          }
        ], 
        "Asthma": [{  }]
      }
    ]
  }
%}
{% set v_float = 10.12 %}
{% set v_bool = True %}
{% set v_array = [1, 2, 3, 4] %}
{% set v_dict = { 'a': 10, 'b': 20 } %}
{% set v_int = 10 %}

WITH env_uitesting_shared_parent_model_1 AS (

  SELECT * 
  
  FROM {{ ref('env_uitesting_shared_parent_model_1')}}

),

SQLStatement_1 AS (

  SELECT * 
  
  FROM env_uitesting_shared_parent_model_1 AS in0
  
  WHERE c_tinyint > -1

),

AllStunningOne AS (

  SELECT 
    (1 != 2)
    or (true != NULL)
    or (NULL != NULL)
    or (1 < 2)
    or (2 <= 2)
    or (2 <=> 2)
    or ((2 % 1.8) == 1)
    or (to_date('2009-07-30 04:17:52') < to_date('2009-07-30 04:17:52'))
    or (add_months('2016-08-31', 1) < add_months('2017-08-31', 3))
    or (true and false)
    or array_contains(array_distinct(ARRAY(1, 2, 3)), 2)
    or array_contains(array_except(ARRAY(1, 2, 3), ARRAY(1, 3, 5)), 2)
    or array_contains(array_intersect(ARRAY(1, 2, 3), ARRAY(1, 3, 5)), 10)
    or (array_join(ARRAY('hello', NULL, 'world'), ' ', ',') LIKE '%hello%')
    or (array_max(ARRAY(1, 20, NULL, 3)) > 10)
    or (array_min(ARRAY(1, 20, NULL, 3)) > 20)
    or array_contains(array_remove(ARRAY(1, 2, 3, NULL, 3), 3), 3)
    or array_contains(array_repeat(5, 2), 6)
    or array_contains(array_union(ARRAY(1, 2, 3), ARRAY(1, 3, 5)), 10)
    or arrays_overlap(ARRAY(1, 2, 3), ARRAY(3, 4, 5))
    or (10 BETWEEN 2 AND 20)
    or contains('Spark SQL', 'Spark')
    or endswith('Spark SQL', 'SQL')
    or (
         EXISTS(
           ARRAY(1, 2, 3), 
           x -> x % 2 == 0)
       )
    or array_contains(filter(
         ARRAY(1, 2, 3), 
         x -> x % 2 == 1), 5)
    or array_contains(flatten(ARRAY(ARRAY(1, 2), ARRAY(3, 4))), 10)
    or forall(
         ARRAY(1, 2, 3), 
         x -> x % 2 == 0)
    or ilike('Spark', '_Park')
    or (1 IN (2, 3, 4))
    or (isnan(CAST('NaN' AS double)))
    or isnotnull(1)
    or isnull(1)
    or array_contains(json_object_keys('{"key": "value"}'), 'key1')
    or like('Spark', '_park')
    or map_contains_key(MAP(1, 'a', 2, 'b'), 1)
    or map_contains_key(map_concat(MAP(1, 'a', 2, 'b'), MAP(3, 'c')), 4)
    or map_contains_key(map_filter(
         MAP(1, 0, 2, 2, 3, -1), 
         (k, v) -> k > v), 3)
    or map_contains_key(map_from_arrays(ARRAY(1.0, 3.0), ARRAY('2', '4')), 2)
    or map_contains_key(map_from_entries(ARRAY(STRUCT(1, 'a'), STRUCT(2, 'b'))), 1)
    or array_contains(map_keys(MAP(1, 'a', 2, 'b')), 2)
    or array_contains(map_values(MAP(1, 'a', 2, 'b')), 'a')
    or map_contains_key(map_zip_with(
         MAP(1, 'a', 2, 'b'), 
         MAP(1, 'x', 2, 'y'), 
         (k, v1, v2) -> concat(v1, v2)), 1)
    or (named_struct('a', 1, 'b', 2) IN (named_struct('a', 1, 'b', 1), named_struct('a', 1, 'b', 3)))
    or (NOT true)
    or regexp('%SystemDrive%\\Users\\John', '%SystemDrive%\\\\Users.*')
    or array_contains(regexp_extract_all('100-200, 300-400', '(\\d+)-(\\d+)', 1), '100')
    or (rlike('%SystemDrive%\\Users\\John', '%SystemDrive%\\\\Users.*'))
    or array_contains(sequence(1, 5), 4)
    or array_contains(shuffle(ARRAY(1, 20, 3, 5)), 10)
    or array_contains(slice(ARRAY(1, 2, 3, 4), 2, 2), 4)
    or array_contains(sort_array(ARRAY('b', 'd', NULL, 'c', 'a'), true), 'b')
    or array_contains(split('oneAtwoBthreeC', '[ABC]'), 'one')
    or startswith('Spark SQL', 'Spark')
    or map_contains_key(str_to_map('a:1,b:2,c:3', ',', ':'), 'a')
    or array_contains(transform(
         ARRAY(1, 2, 3), 
         x -> x + 1), 1)
    or map_contains_key(transform_keys(
         map_from_arrays(ARRAY(1, 2, 3), ARRAY(1, 2, 3)), 
         (k, v) -> k + 1), 1)
    or map_contains_key(transform_values(
         map_from_arrays(ARRAY(1, 2, 3), ARRAY(1, 2, 3)), 
         (k, v) -> v + 1), 2)
    or array_contains(xpath('<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>', 'a/b/text()'), 'a')
    or xpath_boolean('<a><b>1</b></a>', 'a/b')
    or array_contains(zip_with(
         ARRAY(1, 2), 
         ARRAY(3, 4), 
         (x, y) -> x + y), 1) AS c_bool_expr,
    concat(
      aes_decrypt(unhex('83F16B2AA704794132802D248E6BFD4E380078182D1544813898AC97E709B28A94'), '0000111122223333'), 
      base64(aes_encrypt('Spark SQL', '1234567890abcdef', 'ECB', 'PKCS')), 
      bin(13), 
      btrim('    SparkSQL   '), 
      char(65), 
      chr(65), 
      concat('Spark', 'SQL'), 
      concat_ws(' ', 'Spark', 'SQL'), 
      crc32('Spark'), 
      current_catalog(), 
      current_database(), 
      current_date(), 
      current_timestamp(), 
      current_timezone(), 
      current_user(), 
      date_add('2016-07-30', 1), 
      date_sub('2016-07-30', 1), 
      date_format('2016-04-08', 'y'), 
      date_from_unix_date(1), 
      date_part('YEAR', TIMESTAMP'2019-08-12 01:00:00.123456'), 
      date_part('MONTH', INTERVAL '2021-11' YEAR TO MONTH), 
      date_part('MINUTE', INTERVAL '123 23:55:59.002001' DAY TO SECOND), 
      date_trunc('HOUR', '2015-03-05T09:32:05.359'), 
      date_trunc('DD', '2015-03-05T09:32:05.359'), 
      datediff('2009-07-31', '2009-07-30'), 
      decode(encode('abc', 'utf-8'), 'utf-8'), 
      e(), 
      elt(1, 'scala', 'java'), 
      format_number(12332.123456, '##################.###'), 
      format_string('Hello World %d %s', 100, 'days'), 
      CAST(from_csv('1, 0.8', 'a INT, b DOUBLE') AS STRING), 
      CAST(from_json(
        '{"teacher": "Alice", "student": [{"name": "Bob", "rank": 1}, {"name": "Charlie", "rank": 2}]}', 
        'STRUCT<teacher: STRING, student: ARRAY<STRUCT<name: STRING, rank: INT>>>') AS STRING), 
      CAST(from_unixtime(0, 'yyyy-MM-dd HH:mm:ss') AS STRING), 
      CAST(from_utc_timestamp('2016-08-31', 'Asia/Seoul') AS STRING), 
      CAST(get_json_object('{"a":"b"}', '$.a') AS STRING), 
      hash('Spark', ARRAY(123), 2), 
      hex(17), 
      CAST(hour('2009-07-30 12:58:59') AS STRING), 
      CAST(hypot(3, 4) AS STRING), 
      CAST(ilike('Spark', '_Park') AS STRING), 
      CAST(initcap('sPark sql') AS STRING), 
      CAST(last_day('2009-01-12') AS STRING), 
      CAST(lcase('SparkSql') AS STRING), 
      CAST(if(
        1 < 2, 
        'a', 
        'b') AS STRING), 
      CAST(ifnull(NULL, ARRAY('2')) AS STRING), 
      LEFT('Spark SQL', 3), 
      lower('SparkSql'), 
      lpad('hi', 5, '??'), 
      ltrim('    SparkSQL   '), 
      CAST(make_date(2013, 7, 15) AS STRING), 
      CAST(make_dt_interval(1, 12, 30, 1.001001) AS STRING), 
      CAST(make_interval(100, 11, 1, 1, 12, 30, 1.001001) AS STRING), 
      CAST(make_timestamp(2019, 6, 30, 23, 59, 60) AS STRING), 
      CAST(make_ym_interval(1, 2) AS STRING), 
      md5('Spark'), 
      next_day('2015-01-14', 'TU'), 
      now(), 
      nullif(2, 2), 
      nvl(NULL, 'hello'), 
      CAST(overlay('Spark SQL' PLACING '_' FROM 6) AS STRING), 
      CAST(parse_url('http://spark.apache.org/path?query=1', 'HOST') AS STRING), 
      printf('Hello World %d %s', 100, 'days'), 
      CAST(regexp_extract('100-200', '(\\d+)-(\\d+)', 1) AS STRING), 
      CAST(regexp_replace('100-200', '(\\d+)', 'num') AS STRING), 
      repeat('123', 2), 
      replace('ABCabc', 'abc', 'DEF'), 
      reverse('Spark SQL'), 
      RIGHT('Spark SQL', 3), 
      rpad('hi', 5, '??'), 
      rtrim('    SparkSQL   '), 
      CAST(schema_of_json('[{"col":0}]') AS STRING), 
      sha('Spark'), 
      sha1('Spark'), 
      sha2('Spark', 256), 
      concat(space(2), '1'), 
      split_part('11.12.13', '.', 3), 
      substr('Spark SQL', 5), 
      substring('Spark SQL', 5), 
      substring_index('www.apache.org', '.', 2), 
      timestamp_micros(1230219000123123), 
      timestamp_millis(1230219000123), 
      timestamp_seconds(1.230219000123E9), 
      to_csv(named_struct('a', 1, 'b', 2)), 
      to_date('2009-07-30 04:17:52'), 
      to_timestamp('2016-12-31 00:12:00'), 
      to_unix_timestamp('2016-04-08', 'yyyy-MM-dd'), 
      to_utc_timestamp('2016-08-31', 'Asia/Seoul'), 
      translate('AaBbCc', 'abc', '123'), 
      trunc('2019-08-04', 'week'), 
      try_to_binary('abc', 'utf-8'), 
      try_to_number('454', '999'), 
      typeof(1), 
      ucase('SparkSql'), 
      unbase64('U3BhcmsgU1FM'), 
      decode(unhex('537061726B2053514C'), 'UTF-8'), 
      unix_date(DATE("1970-01-02")), 
      unix_micros(TIMESTAMP('1970-01-01 00:00:01Z')), 
      unix_millis(TIMESTAMP('1970-01-01 00:00:01Z')), 
      unix_seconds(TIMESTAMP('1970-01-01 00:00:01Z')), 
      unix_timestamp('2016-04-08', 'yyyy-MM-dd'), 
      upper('SparkSql'), 
      uuid(), 
      xpath_string('<a><b>b</b><c>cc</c></a>', 'a/c'), 
      xxhash64('Spark', ARRAY(123), 2), 
      YEAR ('2016-07-30')) AS c_concat_expr,
    (2 % 1.8)
    + (MOD(2, 1.8))
    + (3 & 5)
    + (2 * 3)
    + (5 + 10)
    - (100 + 45)
    + (3 / 2)
    + (3 ^ 5)
    + abs(-1)
    + acos(1)
    + acosh(1)
    + array_position(ARRAY(3, 2, 1), 1)
    + array_size(ARRAY('b', 'd', 'c', 'a'))
    + ascii(2)
    + asin(0)
    + asinh(0)
    + atan(0)
    + atan2(0, 0)
    + atanh(0)
    + bit_count(0)
    + bit_get(11, 0)
    + bit_length('Spark SQL')
    + bround(25, -1)
    + cardinality(ARRAY('b', 'd', 'c', 'a'))
    + cardinality(MAP('a', 1, 'b', 2))
    + CAST('10' AS INT)
    + cbrt(27.0)
    + ceil(3.1411, 3)
    + ceiling(3.1411, 3)
    + char_length('Spark SQL ')
    + coalesce(NULL, 1, NULL)
    + conv('100', 2, 10)
    + cos(0)
    + cosh(0)
    + cot(1)
    + csc(1)
    + day('2009-07-30')
    + dayofmonth('2009-07-30')
    + dayofweek('2009-07-30')
    + dayofyear('2016-04-09')
    + degrees(3.141592653589793)
    + element_at(ARRAY(1, 2, 3), 2)
    + exp(0)
    + expm1(0)
    + EXTRACT(SECONDS FROM TIMESTAMP'2019-10-01 00:00:01.000001')
    + EXTRACT(MINUTE FROM INTERVAL '123 23:55:59.002001' DAY TO SECOND)
    + factorial(2)
    + find_in_set('ab', 'abc,b,ab,c,def')
    + floor(-0.1)
    + getbit(11, 0)
    + greatest(10, 9, 2, 4, 3)
    + instr('SparkSQL', 'SQL')
    + json_array_length('[1,2,3,{"f1":1,"f2":[5,6]},4]')
    + least(10, 9, 2, 4, 3)
    + length('Spark SQL ')
    + levenshtein('kitten', 'sitting')
    + ln(10)
    + locate('bar', 'foobarbar')
    + log(10, 100)
    + log10(10)
    + log1p(0)
    + log2(2)
    + minute('2009-07-30 12:58:59')
    + (2 % 1.8)
    + month('2016-07-30')
    + months_between('1997-02-28 10:30:00', '1996-10-30', false)
    + nanvl(CAST('NaN' AS double), 123)
    + negative(100)
    + nvl2(NULL, 2, 1)
    + octet_length('Spark SQL')
    + pi()
    + pmod(10, 3)
    + position('bar', 'foobarbar')
    + positive(1)
    + pow(2, 3)
    + power(2, 3)
    + quarter('2016-08-31')
    + radians(180)
    + rand()
    + randn()
    + random()
    + rint(12.3456)
    + round(2.5, 0)
    + sec(0)
    + second('2009-07-30 12:58:59')
    + shiftleft(2, 1)
    + shiftright(4, 1)
    + shiftrightunsigned(4, 1)
    + sign(40)
    + signum(40)
    + sin(0)
    + sinh(0)
    + size(ARRAY('b', 'd', 'c', 'a'))
    + sqrt(4)
    + tan(0)
    + tanh(0)
    + to_number('454.00', '000.00')
    + try_add(1, 2)
    + try_divide(2L, 2L)
    + try_element_at(ARRAY(1, 2, 3), 2)
    + try_multiply(2, 3)
    + try_subtract(2, 1)
    + weekday('2009-07-30')
    + weekofyear('2008-02-20')
    + (
        CASE
          WHEN 1 > 0
            THEN 1
          WHEN 2 > 0
            THEN 2.0
          ELSE 1.2
        END
      )
    + width_bucket(5.3, 0.2, 10.6, 5)
    + xpath_double('<a><b>1</b><b>2</b></a>', 'sum(a/b)')
    + xpath_int('<a><b>1</b><b>2</b></a>', 'sum(a/b)')
    + xpath_long('<a><b>1</b><b>2</b></a>', 'sum(a/b)')
    + xpath_number('<a><b>1</b><b>2</b></a>', 'sum(a/b)')
    + xpath_short('<a><b>1</b><b>2</b></a>', 'sum(a/b)')
    + (~ 0) AS c_add_expr,
    c_tinyint AS c_tinyint,
    c_smallint AS c_smallint,
    c_int AS c_int,
    c_bigint AS c_bigint,
    c_float AS c_float,
    c_double AS c_double,
    c_string AS c_string,
    c_boolean AS c_boolean,
    c_array AS c_array,
    c_struct AS c_struct,
    {{ SQL_DatabricksSharedBasic.qa_concat_function_main('c_string', 'c_boolean') }} AS c_macro,
    {% if v_int > 0 or var('v_project_dict') ['a'] > 10 %}
      concat(c_string, c_float) AS c_if,
    {% elif v_dict['a'] > 10 or var('v_project_dict') ['b'] == 'hello' %}
      concat(c_string, c_int) AS c_if,
    {% else %}
      concat(c_string, c_bigint) AS c_if,
    {% endif %}
    {% for c_i in range(0, 5) %}
      concat(c_string, {{c_i}}) AS cfor_col_{{c_i}}
    {% if not loop.last %} , {% endif %}
    {% endfor %}
  
  FROM env_uitesting_shared_parent_model_1 AS in0

),

Limit_1 AS (

  SELECT * 
  
  FROM AllStunningOne AS in0
  
  LIMIT 10

),

Join_1 AS (

  SELECT 
    in0.c_tinyint AS c_tinyint,
    in1.c_smallint AS c_smallint,
    in0.c_int AS c_int,
    in1.c_bigint AS c_bigint,
    in0.c_float AS c_float,
    in0.c_double AS c_double,
    in0.c_string AS c_string,
    in0.c_boolean AS c_boolean,
    in0.c_array AS c_array,
    in0.c_struct AS c_struct
  
  FROM SQLStatement_1 AS in0
  INNER JOIN Limit_1 AS in1
     ON in0.c_tinyint = in1.c_tinyint

)

{#Utilizes combined customer and asset data for testing purposes.#}
SELECT *

FROM Join_1
