init-selfjoin-hbase.sh

sourcedir=~/git/LearningHadoop
cd /usr/lib/pig/bin
./pig -x mapreduce $sourcedir/bin/sql/selfjoin-pig.pig
