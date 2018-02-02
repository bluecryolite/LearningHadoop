if [ ! -d "../obj" ]; then
  mkdir ../obj
fi

if [ ! -d "../obj/wordcount" ]; then
  mkdir ../obj/wordcount
fi

/usr/lib/hadoop/bin/hadoop com.sun.tools.javac.Main ../src/WordCount.java -d ../obj/wordcount
cd ../obj/wordcount
jar cf ../../bin/wordcount.jar *.class

cd /usr/lib/hadoop/bin

./hadoop fs -test -e /wordcount
if [ ! $? -eq 0 ]; then
  ./hadoop fs -mkdir /wordcount
fi

./hadoop fs -test -e /wordcount/input
if [ ! $? -eq 0 ]; then
  ./hadoop fs -mkdir /wordcount/input
fi

./hadoop fs -rm /wordcount/output/*
./hadoop fs -rmdir /wordcount/output

sourcedir=~/git/LearningHadoop/WordCounter
./hadoop fs -put -f $sourcedir/samples/wordcount/file* /wordcount/input

./hadoop jar $sourcedir/bin/wordcount.jar WordCount /wordcount/input /wordcount/output

