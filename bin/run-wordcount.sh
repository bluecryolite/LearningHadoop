solution=wordcount
sample=$solution

if [ ! -d "../obj" ]; then
  mkdir ../obj
fi

if [ ! -d "../obj/$solution" ]; then
  mkdir ../obj/$solution
fi

/usr/lib/hadoop/bin/hadoop com.sun.tools.javac.Main ../src/WordCount.java -d ../obj/$solution
cd ../obj/$solution
jar cf ../../bin/wordcount.jar *.class

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

./hadoop jar $sourcedir/bin/wordcount.jar WordCount /$solution/input /$solution/output

