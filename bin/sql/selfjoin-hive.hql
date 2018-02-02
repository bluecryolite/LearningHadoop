CREATE DATABASE IF NOT EXISTS learning_hadoop LOCATION '/hive_meta/learning_hadoop';

USE learning_hadoop;

CREATE TABLE IF NOT EXISTS selfjoin_hive (
childname STRING,
parentname STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

TRUNCATE TABLE selfjoin_hive;

LOAD DATA INPATH '/selfjoin_hive/input/file01' INTO TABLE selfjoin_hive;

INSERT OVERWRITE DIRECTORY '/selfjoin_hive/output'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ' '
SELECT a.childname, b.parentname
FROM selfjoin_hive a
INNER JOIN selfjoin_hive b
  ON a.parentname=b.childname;
