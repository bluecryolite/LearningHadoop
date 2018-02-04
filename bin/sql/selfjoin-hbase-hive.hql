CREATE DATABASE IF NOT EXISTS learning_hadoop LOCATION '/hive_meta/learning_hadoop';

USE learning_hadoop;

CREATE TABLE IF NOT EXISTS selfjoin_hive (
childname STRING,
parentname STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

TRUNCATE TABLE selfjoin_hive;

LOAD DATA INPATH '/selfjoin_hbase_hive/input/file01' INTO TABLE selfjoin_hive;

CREATE EXTERNAL TABLE IF NOT EXISTS selfjoin_hbase (
  row_key STRING,
  childname STRING,
  parentname STRING
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES('hbase.columns.mapping' = ":key, names:childname, names:parentname")
TBLPROPERTIES('hbase.table.name'='selfjoin');

INSERT INTO selfjoin_hbase
SELECT CONCAT(childname, ":", parentname), childname, parentname
FROM selfjoin_hive;

CREATE EXTERNAL TABLE IF NOT EXISTS selfjoin_hbase_result (
  row_key STRING,
  childname STRING,
  parentname STRING
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES('hbase.columns.mapping' = ":key, names:childname, names:parentname")
TBLPROPERTIES('hbase.table.name'='selfjoin_result');

INSERT INTO selfjoin_hbase_result
SELECT CONCAT(a.childname, ":", b.parentname),  a.childname, b.parentname
FROM selfjoin_hbase a
INNER JOIN selfjoin_hbase b
  ON a.parentname=b.childname;
