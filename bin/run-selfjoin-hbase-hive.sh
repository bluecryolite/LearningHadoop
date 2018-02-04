solution=selfjoin_hbase_hive
sample=selfjoin

cd /usr/lib/hadoop/bin

./hadoop fs -test -e /$solution
if [ ! $? -eq 0 ]; then
  ./hadoop fs -mkdir /$solution
fi

./hadoop fs -test -e /$solution/input
if [ ! $? -eq 0 ]; then
  ./hadoop fs -mkdir /$solution/input
fi

sourcedir=~/git/LearningHadoop
./hadoop fs -put -f $sourcedir/samples/$sample/file* /$solution/input

cd /usr/lib/hbase/bin

for tablename in 'selfjoin' 'selfjoin_result' ; do
  if ! echo -e "exists '$tablename'" | hbase shell 2>&1 | grep -q "does exist" 2>/dev/null ; then
    echo -e "create '$tablename', 'names'" | hbase shell
  else
    echo -e "truncate '$tablename'" | hbase shell
  fi
done

cd /usr/lib/hive/bin
./hive -f $sourcedir/bin/sql/selfjoin-hbase-hive.hql
