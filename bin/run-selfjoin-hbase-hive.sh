init-selfjoin-hbase.sh

sourcedir=~/git/LearningHadoop
cd /usr/lib/hive/bin
./hive -f $sourcedir/bin/sql/selfjoin-hbase-hive.hql
