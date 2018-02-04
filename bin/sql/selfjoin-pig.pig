a = LOAD 'hbase://selfjoin'
    USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('names:childname, names:parentname', '-loadKey true')
    AS (keystring:chararray, child:chararray, parent:chararray);

b = LOAD 'hbase://selfjoin'
    USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('names:childname, names:parentname', '-loadKey true')
    AS (keystring:chararray, child:chararray, parent:chararray);

c = JOIN a BY child, b BY parent;

d = FOREACH c GENERATE CONCAT(b::child, ':', a::parent), b::child, a::parent;

e = STORE d INTO 'hbase://selfjoin_result'
    USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('names:childname, names:parentname');
