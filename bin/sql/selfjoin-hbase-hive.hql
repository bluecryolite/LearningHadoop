use learning_hadoop;

CREATE EXTERNAL TABLE IF NOT EXISTS selfjoin_hbase (
  row_key STRING,
  childname STRING,
  parentname STRING
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES('hbase.columns.mapping' = ":key, names:childname, names:parentname")
TBLPROPERTIES('hbase.table.name'='selfjoin');

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
