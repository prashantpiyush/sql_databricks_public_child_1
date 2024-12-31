WITH ALL_TYPE_TABLE AS (

  SELECT * 
  
  FROM {{ source('spark_catalog.qa_database', 'all_type_parquet') }}

),

SQLStatementTest34 AS (

  SELECT 
    (xpath_double('<a><b>1</b><b>2</b></a>', 'sum(a/b)') != NULL),
    (xpath_float('<a><b>1</b><b>2</b></a>', 'sum(a/b)') != NULL),
    (xpath_int('<a><b>1</b><b>2</b></a>', 'sum(a/b)') == 2) AS col117,
    (xpath_long('<a><b>1</b><b>2</b></a>', 'sum(a/b)') == 3),
    (xpath_number('<a><b>1</b><b>2</b></a>', 'sum(a/b)') == 2),
    (xpath_int('<a><b>1</b><b>2</b></a>', 'sum(a/b)') == 2),
    (xpath_string('<a><b>b</b><c>cc</c></a>', 'a/c') != NULL),
    (
      assert_true(
        0 < 1) == NULL
    ) AS col10,
    (
      (
        CASE
          WHEN 1 > 0
            THEN 1
          WHEN 2 > 0
            THEN 2.0
          ELSE 1.2
        END
      ) == 1
    ) AS col9,
    (
      (
        CASE 3
          WHEN 1
            THEN 'A'
          WHEN 2
            THEN 'B'
          WHEN 3
            THEN 'C'
        END
      ) != NULL
    ) AS col8,
    random() % 2 = 0 AS c_test_col
  
  FROM ALL_TYPE_TABLE
  
  WHERE c_string != NULL

),

SQLStatementTest2 AS (

  SELECT 
    isnull(51),
    ('t' IS NOT true) AS c46,
    (1 <=> '1'),
    (1 <> CAST(c_int AS INT)),
    (10 - 2 + 10 == 8),
    (NOT true),
    (false or true),
    (2 % 0 == 0),
    ('Spark' || 'SQL' == 'SparkSQL'),
    (r'%SystemDrive%\Users\John' LIKE r'%System23Drive%\\Users%'),
    random() % 2 = 0 AS c_test_col
  
  FROM ALL_TYPE_TABLE

),

SQLStatementTest10 AS (

  SELECT 
    (substr('Spark SQL', 5, 1) != NULL),
    (substring_index('www.apache.org', '.', 2) != NULL),
    (CAST(to_binary('537061726B') AS STRING) != NULL),
    (CAST(to_binary('537061726B', 'hex') AS STRING) != NULL),
    (CAST(try_to_binary('U3Bhxcms=', 'base64') AS STRING) != NULL) AS col50,
    (translate('AaBbCc', 'abc', '123') != NULL),
    (CAST(try_to_binary('U3Bhxcms=', 'base64') AS STRING) != NULL),
    (ucase('SparkSql') != NULL),
    (CAST(unbase64('U3BhcmsgU1FM') AS STRING) != NULL),
    (decode(unhex('537061726B2053514C'), 'UTF-8') != NULL),
    random() % 2 = 0 AS c_test_col
  
  FROM ALL_TYPE_TABLE

),

SQLStatementTest7 AS (

  SELECT 
    (md5('Spark') != NULL),
    (octet_length('Spark SQL') != NULL),
    (parse_url('http://spark.apache.org/path?query=1', 'HOST') != NULL),
    (position('bar', 'abcbarbar') > 10),
    (position('bar', 'abcbarbar', 5) > 5),
    (printf('Hello World %d %s', 100, 'days') != NULL),
    (r'%SystemDarive%\Users\John' RLIKE r'%SystemDrive%\\Users.*'),
    (r'%System1Drive%\Users\John' RLIKE '%SystemDrive%\\\\Users.*'),
    (regexp_like('%SystemDa1rive%\\Users\\John', '%SystemDrive%\\\\Users.*')),
    (regexp_extract('100-200', '(\\d+)-(\\d+)', 1) > 10),
    random() % 2 = 0 AS c_test_col
  
  FROM ALL_TYPE_TABLE

),

SQLStatementTest11 AS (

  SELECT 
    (upper('SparkSql')),
    (10 / 2 == 5) AS d9,
    ((3 | 5) == 2),
    ((DATE'2021-03-20' - INTERVAL '2' MONTH)),
    (10 - 2 + 2 == 4),
    (2 % 1.8 == 2),
    (3 ^ 5 == 3),
    (3 & 5 == 2),
    (3 * 2 == 2),
    ((INTERVAL '3' YEAR * 3)),
    random() % 2 = 0 AS c_test_col
  
  FROM ALL_TYPE_TABLE

),

SQLStatementTest22 AS (

  SELECT 
    (reverse(ARRAY(2, 1, 4, 3)) != NULL),
    (sequence(5, 1) != NULL),
    (cardinality(ARRAY('b', 'd', 'c', 'a')) == 10),
    (slice(ARRAY(1, 2, 3, 4), 2, 2) != NULL),
    (sort_array(ARRAY('b', 'd', NULL, 'c', 'a'), true) != NULL),
    (
      transform(
        ARRAY(1, 2, 3), 
        x -> x + 1) != NULL
    ) AS col16,
    (try_element_at(ARRAY(1, 2, 3), 2) == 2),
    (
      zip_with(
        ARRAY('a', 'b', 'c'), 
        ARRAY('d', 'e', 'f'), 
        (x, y) -> concat(x, y)) != NULL
    ) AS col15,
    (MAP(1, 'Hello', 2, 'World')[1] != NULL),
    (cardinality(MAP('a', 1, 'b', 2)) == 2) AS col67,
    random() % 2 = 0 AS c_test_col
  
  FROM ALL_TYPE_TABLE
  
  WHERE c_string != NULL

),

SQLStatementTest17 AS (

  SELECT 
    (rint(12.3456) == 1),
    (round(2.5, 0) == 3),
    (sec(pi()) == -1),
    (sin(0) == 0),
    (shiftleft(2, 1) == 2),
    (shiftright(4, 1) == 2),
    (shiftrightunsigned(4, 1) == 2),
    (sign(40) == 1),
    (signum(40) == 1),
    (sinh(0) == 1),
    random() % 2 = 0 AS c_test_col
  
  FROM ALL_TYPE_TABLE
  
  WHERE c_string != NULL

),

SQLStatementTest4 AS (

  SELECT 
    (btrim('abcaabaSparkSQLabcaaba', 'abc') != NULL),
    (char(65) != NULL),
    (char_length('Spark SQL ') == 10),
    (character_length('Spark SQL ') == 10),
    (concat(c_int, 'hello') != NULL),
    (concat_ws(',', 'Spark', ARRAY('S', 'Q', NULL, 'L'), NULL) != NULL),
    contains('SparkSQL', 'Spork'),
    (crc32('Spark') > 0),
    chr(65) != NULL,
    (3 ^ 5 == 6),
    random() % 2 = 0 AS c_test_col
  
  FROM ALL_TYPE_TABLE

),

SQLStatementTest37 AS (

  SELECT 
    (uuid() != NULL),
    (xxhash64('Spark', ARRAY(123), 2) != NULL),
    ('20'::INTEGER == 20),
    like('Spark', '_park') AS d866,
    ('Spark' LIKE SOME('_park', '_ock')) AS d889,
    (bitmap_count(x'00') == 10),
    (charindex('bar', 'abcbarbar') == 1),
    (decode(x'FEFF0053007000610072006B002000530051004C', 'UTF-16') != NULL),
    (like('Spark', '_park')) AS d8123,
    (len('Spark SQL ') > 10),
    random() % 2 = 0 AS c_test_col
  
  FROM ALL_TYPE_TABLE
  
  WHERE c_string != NULL

),

SQLStatementTest14 AS (

  SELECT 
    (cosh(0) == 1),
    (cot(1) == 1),
    (csc(pi() / 2) == 2),
    (decimal('5.2') == 5) AS d10,
    (degrees(3.141592653589793) == 10),
    (double('5.2') == 4),
    (e() == 2),
    (exp(1) == 2),
    (expm1(0) == 1),
    (factorial(2) == 2),
    random() % 2 = 0 AS c_test_col
  
  FROM ALL_TYPE_TABLE
  
  WHERE c_string != NULL

),

SQLStatementTest41 AS (

  SELECT 
    ((TIMESTAMP'2021-03-20 12:15:29' + INTERVAL '3' SECOND) != NULL) AS col156781254,
    (bit_reverse(-1) == -1),
    (bitmap_bit_position(-32768) == 1),
    (bitmap_bucket_number(-32768) == 1),
    (MOD(2, 1.8) == 2),
    (array_append(ARRAY(1, 2, 3), 0) == NULL),
    (array_compact(ARRAY(1, 2, NULL, 3, NULL, 3)) == NULL),
    (array_insert(ARRAY('a', 'b', 'c'), 1, 'z') != NULL),
    (array_prepend(ARRAY(1, 2, 3), 0) != NULL),
    (get(ARRAY(1, 2, 3), 2) != NULL),
    random() % 2 = 0 AS c_test_col
  
  FROM ALL_TYPE_TABLE

),

SQLStatementTest45 AS (

  SELECT 
    (schema_of_xml('<p><a attr="2">1</a><a>3</a></p>', MAP('excludeAttribute', 'true')) IS NOT NULL),
    (current_metastore() != NULL),
    (current_schema() != NULL),
    (current_version() != NULL),
    (equal_null(2, 2)),
    (
      iff(
        1 < 2, 
        'a', 
        'b') == 'a'
    ) AS col4,
    (is_account_group_member('admins')),
    (is_member('admins')) AS cold1d1,
    (luhn_check('12345') == NULL) AS cold13412asd,
    (user() != NULL) AS cold13423423,
    random() % 2 = 0 AS c_test_col
  
  FROM ALL_TYPE_TABLE

),

SQLStatementTest0 AS (

  SELECT 
    CAST(c_int AS INT) > 5,
    CAST(c_int AS INT) != 0,
    c_string LIKE '%A%',
    ((1 & 1) == 1),
    ((2 | 2) == 2),
    10 * CAST(c_int AS INT) == 20,
    'test' != c_string,
    (CAST(c_int AS INT) BETWEEN 10 AND 20),
    (ARRAY(10, 20, 30)[2] == 30),
    (MAP(1, 'Hello', 2, 'World')[1] == 'Hello'),
    random() % 2 = 0 AS c_test_col
  
  FROM ALL_TYPE_TABLE

),

SQLStatementTest44 AS (

  SELECT 
    (make_interval(100, 11) IS NOT NULL),
    (make_timestamp(2014, 12, 28, 6, 30, 45.887) IS NOT NULL),
    (STRING(5) != NULL),
    (to_char(454, '000.00') != NULL),
    (to_varchar(454, '999') IS NOT NULL),
    (from_json('{"a":1, "b":0.8}', 'a INT, b DOUBLE') IS NOT NULL),
    (get_json_object('{"a":"b"}', '$.a') IS NOT NULL),
    (to_csv(named_struct('time', to_timestamp('2015-08-26', 'yyyy-MM-dd')), MAP('timestampFormat', 'dd/MM/yyyy')) IS NOT NULL),
    (to_csv(named_struct('a', 1, 'b', 2)) IS NOT NULL),
    (from_xml('<p><time>26/08/2015</time></p>', 'time Timestamp', MAP('timestampFormat', 'dd/MM/yyyy')) != NULL),
    random() % 2 = 0 AS c_test_col
  
  FROM ALL_TYPE_TABLE
  
  WHERE c_string != NULL

),

SQLStatementTest39 AS (

  SELECT 
    (regexp_instr('Mary had a little lamb', NULL) != NULL),
    (regexp_substr(NULL, 'Ste(v|ph)en') != NULL),
    (rtrim('ab', 'SparkSQLabcaaba') != NULL),
    (STRING(4) != NULL),
    (substr('Spark SQL', -3) != NULL),
    (substr('Spark SQL' FROM 5 FOR 1) != NULL),
    (substr('Spark SQL' FROM -3) != NULL),
    (to_char(DATE'2016-04-08', 'y') != NULL) AS col11112,
    (to_char(encode('abc', 'utf-8'), 'utf-8') != NULL),
    (to_varchar(454, '999') != NULL),
    random() % 2 = 0 AS c_test_col
  
  FROM ALL_TYPE_TABLE

),

SQLStatementTest30 AS (

  SELECT 
    (unix_micros(TIMESTAMP('1970-01-01 00:00:01Z'))),
    (unix_millis(TIMESTAMP('1970-01-01 00:00:01Z'))),
    (unix_seconds(TIMESTAMP('1970-01-01 00:00:01Z'))),
    (unix_timestamp('2016-04-08', 'yyyy-MM-dd')) AS col1123423,
    (weekday(DATE'2009-07-30') == 1),
    (EXTRACT(DAYOFWEEK_ISO FROM DATE'2009-07-30') == 4),
    (weekofyear('2008-02-20') == 8),
    (year('2016-07-30') == 2016) AS col122121rtyr,
    (ARRAY(1, 2, 3)),
    (BIGINT('5') == 5) AS d3,
    random() % 2 = 0 AS c_test_col
  
  FROM ALL_TYPE_TABLE

),

SQLStatementTest25 AS (

  SELECT 
    ((INTERVAL '3' YEAR * 3)) AS col100,
    (add_months('2016-08-31', -6) != NULL),
    (current_date() != NULL),
    (current_timestamp()),
    (current_timezone()),
    (DATE('2021-03-21') != NULL) AS col112,
    (date_add('2016-07-30', 1)) AS col122121d,
    (date_format('2016-04-08', 'y') == 2016) AS col122121,
    (date_from_unix_date(1)),
    (date_sub('2016-07-30', 1)) AS col122121gh,
    random() % 2 = 0 AS c_test_col
  
  FROM ALL_TYPE_TABLE

),

SQLStatementTest23 AS (

  SELECT 
    (element_at(MAP(1, 'a', 2, 'b'), 3) == NULL),
    (MAP(1.0, '2', 3.0, '4')),
    (map_concat(MAP(1, 'a', 2, 'b'), MAP(3, 'c'))),
    (map_contains_key(MAP(1, 'a', 2, 'b'), 2)) AS col122,
    (map_entries(MAP(1, 'a', 2, 'b'))),
    (
      map_filter(
        MAP(1, 0, 2, 2, 3, -1), 
        (k, v) -> k > v) IS NOT NULL
    ) AS col14,
    (map_from_arrays(ARRAY(1.0, 3.0), ARRAY('2', '4')) IS NOT NULL),
    (map_from_entries(ARRAY(STRUCT(1, 'a'), STRUCT(2, 'b')))),
    (map_keys(MAP(1, 'a', 2, 'b'))),
    (map_values(MAP(1, 'a', 2, 'b'))),
    random() % 2 = 0 AS c_test_col
  
  FROM ALL_TYPE_TABLE

),

SQLStatementTest19 AS (

  SELECT 
    (try_subtract(TIMESTAMP'2021-03-20 12:15:29', INTERVAL '3' SECOND) != NULL) AS col111,
    (try_subtract(-128Y, 1Y) != NULL),
    (width_bucket(5.3, 0.2, 10.6, 5) + width_bucket(-0.9, 5.2, 0.5, 2) == 3),
    (width_bucket(INTERVAL '1' DAY, INTERVAL '0' DAY, INTERVAL '10' DAY, 11) == 10),
    (ARRAY(10, 20, 30) != NULL),
    (
      (
        aggregate(
          ARRAY(1, 2, 3), 
          0, 
          (acc, x) -> acc + x, 
          acc -> acc * 10)
      ) == 1
    ) AS col21,
    (array_contains(ARRAY(1, 2, 3), 2)),
    (array_distinct(ARRAY(1, 2, 3, NULL, 3)) != NULL),
    (array_except(ARRAY(1, 2, 2, 3), ARRAY(1, 1, 3, 5)) != NULL),
    (array_intersect(ARRAY(1, 2, 3), ARRAY(1, 3, 3, 5)) != NULL),
    random() % 2 = 0 AS c_test_col
  
  FROM ALL_TYPE_TABLE
  
  WHERE c_string != NULL

),

SQLStatementTest32 AS (

  SELECT 
    (make_dt_interval(100, 13) != NULL),
    (make_ym_interval(100, 5) == NULL),
    (MAP(1.0, '2', 3.0, '4') IS NOT NULL) AS col113,
    (named_struct('a', 1, 'b', 2, 'c', 3) IS NOT NULL),
    (smallint('5') == 5) AS d5,
    (STRUCT(1, 2, 3) IS NOT NULL),
    (tinyint('12') == 12),
    (TIMESTAMP('2020-04-30 12:25:13.45') != NULL) AS col114,
    (to_date('2016-12-31', 'yyyy-MM-dd') != NULL) AS col115,
    (to_timestamp('2016-12-31 00:12:00') != NULL),
    random() % 2 = 0 AS c_test_col
  
  FROM ALL_TYPE_TABLE
  
  WHERE c_string != NULL

),

SQLStatementTest3 AS (

  SELECT 
    (r'%SystemDr12ive%\Users\John' RLIKE '%System23Drive%\\\\Users.*'),
    (regexp_like('%Syst2emDrive%\\Users\\John', '%SystemD545rive%\\\\Users.*')),
    (2L / 2L == 10),
    (~ 0 == -1),
    (base64(aes_encrypt('Spark', 'abcdefghijklmnop')) == NULL),
    (CAST(aes_decrypt(unbase64('4A5jOAh9FNGwoMeuJukfllrLdHEZxA2DyuSQAWz77dfn'), 'abcdefghijklmnop') AS STRING) == NULL),
    (ascii('234') == 234),
    (base64('Spark SQL') == NULL),
    (bin(13) == NULL),
    (binary('Spark SQL') == NULL),
    random() % 2 = 0 AS c_test_col
  
  FROM ALL_TYPE_TABLE
  
  WHERE c_string != NULL

),

SQLStatementTest43 AS (

  SELECT 
    (dateadd(MICROSECOND, 5, TIMESTAMP'2022-02-28 00:00:00')),
    (datediff(MONTH, TIMESTAMP'2021-02-28 12:00:00', TIMESTAMP'2021-03-28 11:59:59')),
    (make_interval(0, 0, 1, 1, 12, 30, 1.001001) IS NOT NULL),
    (make_timestamp(2014, 12, 28, 6, 30, 45.887, 'CET') IS NOT NULL),
    (make_timestamp(NULL, 7, 22, 15, 30, 0) IS NOT NULL),
    (now()),
    (timediff(MONTH, TIMESTAMP'2021-02-28 12:00:00', TIMESTAMP'2021-03-28 12:00:00') == 1) AS c45fg6789hgf1,
    (timestampdiff(MONTH, TIMESTAMP'2021-02-28 12:00:00', TIMESTAMP'2021-03-28 12:00:00') == 1) AS c45fg6789hgf,
    (to_unix_timestamp('2016-04-08', 'yyyy-MM-dd') == 100) AS c45fg6789hgf2,
    (try_to_timestamp('2016-12-31', 'yyyy-MM-dd') != NULL),
    random() % 2 = 0 AS c_test_col
  
  FROM ALL_TYPE_TABLE

),

SQLStatementTest24 AS (

  SELECT 
    (
      map_zip_with(
        MAP(1, 'a', 2, 'b'), 
        MAP(1, 'x', 2, 'y'), 
        (k, v1, v2) -> concat(v1, v2)) IS NOT NULL
    ) AS col13,
    (cardinality(MAP('a', 1, 'b', 2)) == 2),
    (str_to_map('a:1,b:2,c:3', ',', ':') IS NOT NULL),
    (
      transform_keys(
        map_from_arrays(ARRAY(1, 2, 3), ARRAY(1, 2, 3)), 
        (k, v) -> k + 1) IS NOT NULL
    ) AS col12,
    (
      transform_values(
        map_from_arrays(ARRAY(1, 2, 3), ARRAY(1, 2, 3)), 
        (k, v) -> k + v) IS NOT NULL
    ) AS col11,
    (try_element_at(MAP(1, 'a', 2, 'b'), 2) IS NOT NULL),
    ((INTERVAL '3:15' HOUR TO MINUTE / 3) IS NOT NULL),
    ((DATE'2021-03-31' - INTERVAL '1' MONTH) != NULL) AS col123,
    (typeof(current_timestamp - (current_date + INTERVAL '1' DAY)) != NULL),
    ((DATE'2021-03-31' + INTERVAL '1' MONTH) != NULL) AS col124,
    random() % 2 = 0 AS c_test_col
  
  FROM ALL_TYPE_TABLE
  
  WHERE c_string != NULL

),

SQLStatementTest38 AS (

  SELECT 
    (levenshtein('kitten', 'sitting', 4) > 10),
    (('+' || ltrim('abc', 'acbabSparkSQL   ') || '+') != NULL),
    (mask('AaBb123-&^ASDXYZ921312asd', 'Z', 'z', '9', 'X') != NULL),
    (mask('AaBb123-&^ASDXYZ921312asd', lowerChar => 'z', upperChar => 'X') != NULL),
    (mask('AaBb123-&ASDXYZ921312asd', NULL, NULL, NULL, NULL) != NULL),
    (overlay('Spark SQL' PLACING '_' FROM 6) != NULL),
    (overlay('Spark SQL' PLACING 'tructured' FROM 2 FOR 4) != NULL),
    (overlay(encode('Spark SQL', 'utf-8') PLACING encode('_', 'utf-8') FROM 6) != NULL),
    (position('bar' IN 'abcbarbar') > 2),
    (regexp_count('Steven Jones and Stephen Smith are the best players', 'Ste(v|ph)en') > 2),
    random() % 2 = 0 AS c_test_col
  
  FROM ALL_TYPE_TABLE
  
  WHERE c_string != NULL

),

SQLStatementTest1 AS (

  SELECT 
    (10 ^ 20 == 50) AS d7,
    (MAP('three', 3).four == NULL),
    (named_struct('a', 5, 'b', 'Spark').a == 5) AS d8,
    (1 = 2 and 1 == 2),
    (1 >= 2 and 1 <= 2 and 1 != 3 or 2 > 4 or 4 < 5),
    (
      EXISTS(
        ARRAY(1, NULL, 3), 
        x -> x % 2 == 0)
    ) AS col22,
    ilike('Spark', '_PARK') AS d81,
    (named_struct('a', 1, 'b', 2) IN (named_struct('a', 1, 'b', 1), named_struct('a', 1, 'b', 3))),
    (1 IS DISTINCT FROM 5),
    ('invalid' IS false),
    random() % 2 = 0 AS c_test_col
  
  FROM ALL_TYPE_TABLE

),

SQLStatementTest9 AS (

  SELECT 
    (sha('Spark') != NULL),
    (sha1('Spark') != NULL),
    (sha2('Spark', 256) != NULL),
    (soundex('Miller') != NULL),
    (concat('1', space(2), '1') != NULL),
    (('->' || split_part('Hello,world,!', ',', 1) || '<-') != NULL),
    (('->' || split_part('', ',', 1) || '<-') != NULL),
    (('->' || split_part('Hello,World,!', ',', 0) || '<-') != NULL),
    (startswith('SparkSQL', 'Spark')),
    (startswith(NULL, 'Spark')),
    random() % 2 = 0 AS c_test_col
  
  FROM ALL_TYPE_TABLE
  
  WHERE c_string != NULL

),

SQLStatementTest46 AS (

  SELECT 
    (h3_coverash3('POLYGON((-122.4194 37.7749,-118.2437 34.0522,-74.0060 40.7128,-122.4194 37.7749))', 0) != NULL) AS cold1f4,
    (h3_coverash3string('{"type":"LineString","coordinates":[[-122.4194,37.7749],[-118.2437,34.0522],[-74.0060,40.7128]]}', 1) != NULL) AS cold1asd,
    (h3_longlatash3(-122.4783, 37.8199, 13) > 0) AS cold1asd12,
    (h3_longlatash3string(-122.4783, 37.8199, 13) != NULL) AS cold1das122,
    (h3_pointash3('POINT(-122.4783 37.8199)', 13) > 0) AS cold1,
    (h3_pointash3string('{"type":"Point","coordinates":[]}', 15) == NULL) AS cold1test1,
    (h3_pointash3string('POINT(-122.4783 37.8199)', 13) != NULL) AS cold1test2,
    (
      h3_polyfillash3(
        unhex(
          '0103000000010000000400000050fc1873d79a5ec0d0d556ec2fe342404182e2c7988f5dc0f46c567dae064140aaf1d24d628052c05e4bc8073d5b444050fc1873d79a5ec0d0d556ec2fe34240'), 
        2) != NULL
    ) AS col3455,
    (h3_polyfillash3('POLYGON((-122.4194 37.7749,-118.2437 34.0522,-74.0060 40.7128,-122.4194 37.7749))', 2) != NULL) AS cold1asd342346,
    (h3_polyfillash3string('POLYGON((-122.4194 37.7749,-118.2437 34.0522,-74.0060 40.7128,-122.4194 37.7749))', 2) != NULL) AS cold1j,
    random() % 2 = 0 AS c_test_col
  
  FROM ALL_TYPE_TABLE
  
  WHERE c_string != NULL

),

SQLStatementTest6 AS (

  SELECT 
    (lcase('LowerCase') != NULL),
    (LEFT('Spark SQL', 3) != NULL),
    (length('Spark SQL ') > 20),
    (levenshtein('kitten', 'sitting') > 10),
    (locate('bar', 'abcbarbar') > 2) AS col1201,
    (locate('bar', 'abcbarbar', 5) > 2) AS col1202,
    (lower('LowerCase') != NULL),
    (lpad('hi', 1, '??') != NULL),
    (hex(lpad(x'1020', 5, x'05')) != NULL),
    (('+' || ltrim('abc', 'acbabSparkSQL   ') || '+') != NULL) AS col120,
    random() % 2 = 0 AS c_test_col
  
  FROM ALL_TYPE_TABLE
  
  WHERE c_string != NULL

),

SQLStatementTest33 AS (

  SELECT 
    (from_csv('1, 0.8', 'a INT, b DOUBLE') != NULL),
    (schema_of_csv('1,abc') != NULL),
    (json_array_length('[1,2,3,{"f1":1,"f2":[5,6]},4]') == 5),
    (json_object_keys('{"f1":"abc","f2":{"f3":"a", "f4":"b"}}') != NULL),
    (schema_of_json('[{"col":01}]', MAP('allowNumericLeadingZeros', 'true')) != NULL),
    (to_json(named_struct('a', 1, 'b', 2)) != NULL),
    (to_json(MAP(named_struct('a', 1), named_struct('b', 2))) != NULL),
    (to_json(ARRAY((MAP('a', 1)))) != NULL),
    (xpath('<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>', 'a/b/text()') != NULL),
    (xpath_boolean('<a><b>1</b></a>', 'a/b')),
    random() % 2 = 0 AS c_test_col
  
  FROM ALL_TYPE_TABLE

),

SQLStatementTest16 AS (

  SELECT 
    (log2(2) == 1),
    (log10(10) == 1),
    (nanvl(CAST('NaN' AS DOUBLE), 123) == 2),
    (negative(1) == -1),
    (pmod(-10, 3) == 2),
    (positive(-1) == -1),
    (pow(2, 3) * power(2, 3) == 8),
    (radians(180) == 10),
    (rand(0) * random(0) == 1),
    (randn(0) == 1),
    random() % 2 = 0 AS c_test_col
  
  FROM ALL_TYPE_TABLE

),

SQLStatementTest42 AS (

  SELECT 
    (
      reduce(
        ARRAY(1, 2, 3), 
        0, 
        (acc, x) -> acc + x) == 2
    ) AS col5,
    (shuffle(ARRAY(1, 20, 3, 5)) != NULL),
    (map_contains_key(MAP(1, 'a', 2, 'b'), 2)),
    ((DATE'2021-03-31' - INTERVAL '1' MONTH) != NULL),
    ((DATE'2021-03-31' + INTERVAL '1' MONTH) != NULL),
    ((TIMESTAMP'2021-03-20 12:15:29' - INTERVAL '3' SECOND) != NULL),
    (date_diff(MONTH, TIMESTAMP'2021-02-28 12:00:00', TIMESTAMP'2021-03-28 12:00:00') == 1),
    (date_part('SECONDS', TIMESTAMP'2019-10-01 00:00:01.000001') == 1),
    (date_part('Week', TIMESTAMP'2019-08-12 01:00:00.123456') == 33),
    (dateadd('2016-07-30', 1) != NULL) AS col122121a1,
    random() % 2 = 0 AS c_test_col
  
  FROM ALL_TYPE_TABLE

),

SQLStatementTest13 AS (

  SELECT 
    (bit_get(23Y, 1) == 1),
    (bround(13.5, -1) == 1),
    (round(13.5, -1) == 1),
    (cbrt(27.0) == 3),
    (ceil(3345.1, -2) == 1),
    (ceiling(5.4) == 6),
    (ceiling(3345.1, -2) == 1),
    (conv('100', 2, 10) == 4),
    (conv('FFFFFFFFFFFFFFFF', 16, 10) == 1),
    (cos(pi()) == -1),
    random() % 2 = 0 AS c_test_col
  
  FROM ALL_TYPE_TABLE
  
  WHERE c_string != NULL

),

SQLStatementTest35 AS (

  SELECT 
    (coalesce(2, 5 / 0) == 2),
    (current_catalog() != NULL),
    (current_database() != NULL) AS c21543123d,
    (current_user() != NULL),
    (decode(5, 6, 'Spark', 5, 'SQL', 4, 'rocks') != NULL),
    (elt(1, 'scala', 'java') != NULL),
    (greatest(10, 9, 2, 4, 3) == 10),
    (hash('Spark', ARRAY(123), 2)),
    (
      if(
        1 < 2, 
        'a', 
        'b') != NULL
    ) AS col7,
    (ifnull(NULL, ARRAY('2'))),
    random() % 2 = 0 AS c_test_col
  
  FROM ALL_TYPE_TABLE

),

SQLStatementTest31 AS (

  SELECT 
    (binary('Spark SQL') != NULL),
    (BOOLEAN(1)),
    (CAST(5.6 AS DECIMAL (2, 0)) != NULL),
    (CAST(INTERVAL '1-2' YEAR TO MONTH AS INTEGER) == 12),
    (DATE('2021-03-21') != NULL),
    (decimal('5.2') == 5) AS d1,
    (double('5.2') / 2 == 5) AS d2,
    (FLOAT('5.2') / 2 == 2),
    (INT('5') == 5) AS d4,
    (make_date(2013, 7, 15) != NULL),
    random() % 2 = 0 AS c_test_col
  
  FROM ALL_TYPE_TABLE

),

SQLStatementTest29 AS (

  SELECT 
    (to_utc_timestamp('2017-07-14 02:40:00.0', 'GMT+1') != NULL),
    (trunc('2015-10-27', 'YEAR') != NULL),
    (trunc('2019-08-04', 'quarter') != NULL),
    (try_add(TIMESTAMP'2021-03-20 12:15:29', INTERVAL '3' SECOND) != NULL),
    (try_add(DATE'2021-03-31', INTERVAL '1' MONTH) != NULL),
    (try_divide(INTERVAL '3:15' HOUR TO MINUTE, 3) != NULL),
    ((INTERVAL '3' YEAR * 3) != NULL) AS col101,
    (try_subtract(DATE'2021-03-20', INTERVAL '2' MONTH) != NULL),
    (try_subtract(TIMESTAMP'2021-03-20 12:15:29', INTERVAL '3' SECOND) != NULL),
    (unix_date(DATE('1970-01-02')) == 1),
    random() % 2 = 0 AS c_test_col
  
  FROM ALL_TYPE_TABLE

),

SQLStatementTest40 AS (

  SELECT 
    (to_varchar(DATE'2016-04-08', 'y') != NULL) AS col1567812,
    (to_varchar(x'537061726b2053514c', 'hex') != NULL),
    (TRIM( 'SL' FROM 'SSparkSQLS') != NULL) AS col15678121,
    (TRIM(BOTH 'SL' FROM 'SSparkSQLS') != NULL) AS col15678123,
    (TRIM(LEADING 'SL' FROM 'SSparkSQLS') != NULL) AS col15678125,
    (TRIM(TRAILING 'SL' FROM 'SSparkSQLS') != NULL) AS col15678127,
    (
      CAST(try_aes_decrypt(
        unbase64('MTIzNDU2Nzg5MDEyMdXvR41sJqwZ6hnTU8FRTTtXbL8yeChIZA=='), 
        '1234567890abcdef', 
        'GCM', 
        'DEFAULT', 
        'Some AAD') AS STRING) != NULL
    ) AS col6,
    (url_decode('http%3A%2F%2Fspark.apache.org%2Fpath%3Fquery%3D1') != NULL),
    (url_encode('http://spark.apache.org/path?query=1') != NULL),
    ((TIMESTAMP'2021-03-20 12:15:29' - INTERVAL '3' SECOND) != NULL) AS col125,
    random() % 2 = 0 AS c_test_col
  
  FROM ALL_TYPE_TABLE
  
  WHERE c_string != NULL

),

SQLStatementTest18 AS (

  SELECT 
    (smallint(-5.6) == 5),
    (sqrt(4) == 2),
    (tan(0) == 1),
    (tanh(0) == 1),
    (tinyint('12') * tinyint(5.4) == 1),
    (try_add(DATE'2021-03-20', INTERVAL '2' MONTH) != NULL),
    (try_add(1, 2) == 3),
    (try_divide(3, 2) == 3),
    (try_divide(INTERVAL '3:15' HOUR TO MINUTE, 3) != NULL) AS col110,
    (try_subtract(1, 2) == 2),
    random() % 2 = 0 AS c_test_col
  
  FROM ALL_TYPE_TABLE
  
  WHERE c_string != NULL

),

SQLStatementTest27 AS (

  SELECT 
    (from_utc_timestamp('2016-08-31', 'Asia/Seoul') != NULL),
    (hour('2009-07-30 12:58:59') == 10),
    (last_day('2009-01-12') IS NOT NULL),
    (make_date(2013, 7, 15) IS NOT NULL),
    (make_dt_interval(0, 0, 1, -0.1) IS NOT NULL),
    (make_ym_interval(100, 5) IS NOT NULL),
    (minute('2009-07-30 12:58:59') == 58) AS d15,
    (month('2016-07-30') == 7) AS col122121hjh,
    (months_between('1997-02-28 10:30:00', '1996-10-30') == 3),
    (months_between('1997-02-28 10:30:00', '1996-10-30', false) == 2),
    random() % 2 = 0 AS c_test_col
  
  FROM ALL_TYPE_TABLE

),

SQLStatementTest12 AS (

  SELECT 
    (abs(-1) == 1),
    (acos(1) == 1),
    (acosh(1) == 1),
    (asin(0) == 1),
    (asinh(0) == 1),
    (atan(0) == 1),
    (atan2(0, 0) == 1),
    (atanh(0) == 1),
    (BIGINT(current_timestamp) > 1),
    (bit_count(-1) > 1),
    random() % 2 = 0 AS c_test_col
  
  FROM ALL_TYPE_TABLE

),

SQLStatementTest47 AS (

  SELECT 
    (h3_try_polyfillash3('Not-a-valid-rep', 2) == NULL) AS cold1jk,
    (h3_try_polyfillash3('POLYGON((-122.4194 37.7749,-118.2437 34.0522,-74.0060 40.7128,-122.4194 37.7749))', 2) != NULL) AS cold1jk1,
    (h3_try_polyfillash3string('POLYGON((-122.4194 37.7749,-118.2437 34.0522,-74.0060 40.7128,-122.4194 37.7749))', 2) != NULL) AS cold1ghj,
    (h3_boundaryasgeojson('8009fffffffffff') != NULL) AS cold1qwe,
    (h3_boundaryasgeojson(599686042433355775) != NULL) AS cold1qweqwe,
    (hex(h3_boundaryaswkb(599686042433355775)) != NULL) AS cold1qweq123,
    (h3_boundaryaswkt(599686042433355775) != NULL) AS cold112sas,
    (h3_centerasgeojson(599686042433355775) != NULL) AS cold1qwe234,
    (hex(h3_centeraswkb('8009fffffffffff')) == NULL) AS cold1dasd3456456,
    (h3_centeraswkt('8009fffffffffff') != NULL) AS cold1sdf56456,
    random() % 2 = 0 AS c_test_col
  
  FROM ALL_TYPE_TABLE

),

SQLStatementTest36 AS (

  SELECT 
    (input_file_block_length() == -1),
    (input_file_block_start() == -1),
    (isnull(1)),
    (isnotnull(1)),
    (least(10, 9, 2, 4, 3) == 2),
    (monotonically_increasing_id() > 10),
    (nullif(2, 2) == NULL),
    (nvl(NULL, 2) == 2),
    (nvl2(NULL, 2, 1) == 1),
    (typeof(1) != NULL),
    random() % 2 = 0 AS c_test_col
  
  FROM ALL_TYPE_TABLE

),

SQLStatementTest21 AS (

  SELECT 
    (arrays_overlap(ARRAY(1, 2, NULL, 3), ARRAY(NULL, 4, 5)) == NULL),
    (arrays_zip(ARRAY(1, 2), ARRAY('shoe', 'string', 'budget')) == NULL),
    (cardinality(ARRAY('b', 'd', 'c', 'a')) == 2),
    (concat(ARRAY(1, 2, 3), ARRAY(4, 5), ARRAY(6)) != NULL),
    (element_at(MAP(1, 'a', 2, 'b'), 2) == NULL),
    (
      EXISTS(
        ARRAY(1, 2, 3), 
        x -> x % 2 == 0)
    ) AS col20,
    (
      EXISTS(
        ARRAY(0, NULL, 2, 3, NULL), 
        x -> x IS NULL)
    ) AS col19,
    (
      filter(
        ARRAY(1, 2, 3), 
        x -> x % 2 == 1) != NULL
    ) AS col18,
    (flatten(ARRAY(ARRAY(1, 2), ARRAY(3, 4))) != NULL),
    (
      forall(
        ARRAY(1, 2, 3), 
        x -> x % 2 == 0) == NULL
    ) AS col17,
    random() % 2 = 0 AS c_test_col
  
  FROM ALL_TYPE_TABLE
  
  WHERE c_string != NULL

),

SQLStatementTest20 AS (

  SELECT 
    (array_join(ARRAY('hello', 'world'), ',') != NULL),
    (array_join(ARRAY('hello', NULL, 'world'), ',', '*') == NULL),
    (array_max(ARRAY(1, 20, NULL, 3)) > 10),
    (array_min(ARRAY(1, 20, NULL, 3)) == 1),
    (array_position(ARRAY(3, 2, 1, 4, 1), 1) == 2),
    (array_remove(ARRAY(1, 2, 3, NULL, 3, 2), 3) != NULL),
    (array_repeat('123', 2) != NULL),
    (array_size(ARRAY(1, NULL, 3, NULL)) == 2),
    (array_sort(ARRAY('bc', 'ab', 'dc')) != NULL),
    (array_union(ARRAY(1, 2, 2, 3), ARRAY(1, 3, 5)) != NULL),
    random() % 2 = 0 AS c_test_col
  
  FROM ALL_TYPE_TABLE
  
  WHERE c_string != NULL

),

SQLStatementTest15 AS (

  SELECT 
    (FLOAT('5.2') == 5) AS d11,
    (floor(-0.1) == -1),
    (floor(3345.1, -2) == 20),
    (getbit(23Y, 0) == 1),
    (hypot(3, 4) == 5) AS d12,
    (INT(-5.6) == 5) AS d13,
    (isnan(CAST('NaN' AS double))),
    (ln(1) == 1),
    (log(10, 100) == 2),
    (log1p(0) == 1),
    random() % 2 = 0 AS c_test_col
  
  FROM ALL_TYPE_TABLE

),

SQLStatementTest26 AS (

  SELECT 
    (date_trunc('YEAR', '2015-03-05T09:32:05.359') != NULL),
    (datediff('2009-07-31', '2009-07-30') == 1),
    (day('2009-07-30') == 30),
    (dayofmonth('2009-07-30') == 30),
    (dayofweek('2009-07-30') == 5) AS d14,
    (dayofyear('2016-04-09') == 100) AS col16121,
    (EXTRACT(SECONDS FROM INTERVAL '5:00:30.001' HOUR TO SECOND) == 30),
    (EXTRACT(WEEK FROM TIMESTAMP'2019-08-12 01:00:00.123456') == 33),
    (from_unixtime(0, 'yyyy-MM-dd HH:mm:ss') != NULL),
    (from_utc_timestamp('2017-07-14 02:40:00.0', 'GMT+1') != NULL),
    random() % 2 = 0 AS c_test_col
  
  FROM ALL_TYPE_TABLE

),

SQLStatementTest8 AS (

  SELECT 
    (regexp_extract_all('100-200, 300-400', '(\\d+)-(\\d+)', 1) != NULL),
    (regexp_replace('100-200', '(\\d+)', 'num') != NULL),
    (repeat('123', 2) != NULL),
    (replace('ABCabc', 'abc', 'DEF') != NULL),
    (reverse('Spark SQL') != NULL),
    (RIGHT('Spark SQL', 3) != NULL),
    (rpad('hi', 5, 'ab') != NULL),
    (hex(rpad(x'1020', 5, x'05')) != NULL),
    (rtrim('ab', 'SparkSQLabcaaba') != NULL) AS col121,
    (sentences('Hi there! Good morning.', 'en', 'US') != NULL),
    random() % 2 = 0 AS c_test_col
  
  FROM ALL_TYPE_TABLE
  
  WHERE c_string != NULL

),

SQLStatementTest28 AS (

  SELECT 
    (next_day('2015-01-14', 'TU') != NULL),
    (quarter('2016-08-31') == 3),
    (second('2009-07-30 12:58:59') == 59) AS d16,
    (TIMESTAMP(123) != NULL),
    (TIMESTAMP('2020-04-30 12:25:13.45') != NULL),
    (timestamp_micros(1230219000123123) != NULL),
    (timestamp_millis(1230219000123) != NULL),
    (timestamp_seconds(1230219000) != NULL),
    (to_date('2016-12-31', 'yyyy-MM-dd') != NULL),
    (to_timestamp('2016-12-31', 'yyyy-MM-dd') != NULL),
    random() % 2 = 0 AS c_test_col
  
  FROM ALL_TYPE_TABLE

),

SQLStatementTest48 AS (

  SELECT 
    (h3_h3tostring(599686042433355775) != NULL) AS cold1sdf43534523,
    (h3_stringtoh3('85283473fffffff') == NULL) AS cold1dasd56346435234,
    (h3_ischildof('88283471b9fffff', '85283473fffffff')) AS cold1dasd23414123,
    (h3_ispentagon(590112357393367039)) AS cold1adsasd123412312,
    (h3_isvalid('85283473fffffff')) AS cold1cold1adsasd123412312,
    (h3_try_validate(590112357393367039) != NULL) AS cold1cold12,
    (h3_validate(590112357393367039) != NULL) AS cold1cold15,
    (h3_hexring(599686042433355775, 1) != NULL) AS cold1cold17,
    (h3_kring(599686042433355775, 1) != NULL) AS cold1cold1as,
    (h3_kringdistances(599686042433355775, 1) != NULL) AS cold1cold1121,
    random() % 2 = 0 AS c_test_col
  
  FROM ALL_TYPE_TABLE

),

SQLStatementTest5 AS (

  SELECT 
    (hex(encode('Spark SQL', 'US-ASCII')) != NULL),
    endswith('SparkSQL', 'SQL'),
    (find_in_set('ab', 'abc,b,ab,c,def') == 2),
    (format_number(12332.123456, 4) == 2),
    (format_number(12332.123456, '#.###') == 10),
    (format_string('Hello World %d %s', 100, 'days') != NULL),
    (hex('Spark SQL') != NULL),
    (r'%SystemDrive%\Users\John' LIKE '%SystemDrive%\\\\Users%'),
    (initcap('sPark sql') != NULL),
    (instr('SparkSQL', 'R') == 2),
    random() % 2 = 0 AS c_test_col
  
  FROM ALL_TYPE_TABLE

),

SQLStatement_2 AS (

  SELECT *
  
  FROM ALL_TYPE_TABLE AS t_main
  
  WHERE t_main.c_int != (
          (
            SELECT count(*)
            
            FROM SQLStatementTest0
           )
          + (
              SELECT count(*)
              
              FROM SQLStatementTest1
             )
          + (
              SELECT count(*)
              
              FROM SQLStatementTest2
             )
          + (
              SELECT count(*)
              
              FROM SQLStatementTest3
             )
          + (
              SELECT count(*)
              
              FROM SQLStatementTest4
             )
          + (
              SELECT count(*)
              
              FROM SQLStatementTest5
             )
          + (
              SELECT count(*)
              
              FROM SQLStatementTest6
             )
          + (
              SELECT count(*)
              
              FROM SQLStatementTest7
             )
          + (
              SELECT count(*)
              
              FROM SQLStatementTest8
             )
          + (
              SELECT count(*)
              
              FROM SQLStatementTest9
             )
          + (
              SELECT count(*)
              
              FROM SQLStatementTest10
             )
          + (
              SELECT count(*)
              
              FROM SQLStatementTest11
             )
          + (
              SELECT count(*)
              
              FROM SQLStatementTest12
             )
          + (
              SELECT count(*)
              
              FROM SQLStatementTest13
             )
          + (
              SELECT count(*)
              
              FROM SQLStatementTest14
             )
          + (
              SELECT count(*)
              
              FROM SQLStatementTest15
             )
          + (
              SELECT count(*)
              
              FROM SQLStatementTest16
             )
          + (
              SELECT count(*)
              
              FROM SQLStatementTest17
             )
          + (
              SELECT count(*)
              
              FROM SQLStatementTest18
             )
          + (
              SELECT count(*)
              
              FROM SQLStatementTest19
             )
          + (
              SELECT count(*)
              
              FROM SQLStatementTest20
             )
          + (
              SELECT count(*)
              
              FROM SQLStatementTest21
             )
          + (
              SELECT count(*)
              
              FROM SQLStatementTest22
             )
          + (
              SELECT count(*)
              
              FROM SQLStatementTest23
             )
          + (
              SELECT count(*)
              
              FROM SQLStatementTest24
             )
          + (
              SELECT count(*)
              
              FROM SQLStatementTest25
             )
          + (
              SELECT count(*)
              
              FROM SQLStatementTest26
             )
          + (
              SELECT count(*)
              
              FROM SQLStatementTest27
             )
          + (
              SELECT count(*)
              
              FROM SQLStatementTest28
             )
          + (
              SELECT count(*)
              
              FROM SQLStatementTest29
             )
          + (
              SELECT count(*)
              
              FROM SQLStatementTest30
             )
          + (
              SELECT count(*)
              
              FROM SQLStatementTest31
             )
          + (
              SELECT count(*)
              
              FROM SQLStatementTest32
             )
          + (
              SELECT count(*)
              
              FROM SQLStatementTest33
             )
          + (
              SELECT count(*)
              
              FROM SQLStatementTest34
             )
          + (
              SELECT count(*)
              
              FROM SQLStatementTest35
             )
          + (
              SELECT count(*)
              
              FROM SQLStatementTest36
             )
          + (
              SELECT count(*)
              
              FROM SQLStatementTest37
             )
          + (
              SELECT count(*)
              
              FROM SQLStatementTest38
             )
          + (
              SELECT count(*)
              
              FROM SQLStatementTest39
             )
          + (
              SELECT count(*)
              
              FROM SQLStatementTest40
             )
          + (
              SELECT count(*)
              
              FROM SQLStatementTest41
             )
          + (
              SELECT count(*)
              
              FROM SQLStatementTest42
             )
          + (
              SELECT count(*)
              
              FROM SQLStatementTest43
             )
          + (
              SELECT count(*)
              
              FROM SQLStatementTest44
             )
          + (
              SELECT count(*)
              
              FROM SQLStatementTest45
             )
          + (
              SELECT count(*)
              
              FROM SQLStatementTest46
             )
          + (
              SELECT count(*)
              
              FROM SQLStatementTest47
             )
          + (
              SELECT count(*)
              
              FROM SQLStatementTest48
             )
        )

)

SELECT *

FROM SQLStatement_2
