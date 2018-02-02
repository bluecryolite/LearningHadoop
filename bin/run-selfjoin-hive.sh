solution=selfjoin_hive
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

./hadoop fs -rm /$solution/output/*
./hadoop fs -rmdir /$solution/output

sourcedir=~/git/LearningHadoop
./hadoop fs -put -f $sourcedir/samples/$sample/file* /$solution/input

cd /usr/lib/hive/bin
./hive -f $sourcedir/bin/sql/selfjoin-hive.hql
