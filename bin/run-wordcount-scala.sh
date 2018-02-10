#!/bin/sh
solution=wordcount_scala
sample=wordcount

cd /usr/lib/hadoop/bin

./hadoop fs -test -e /$solution
if [ ! $? -eq 0 ]; then
  ./hadoop fs -mkdir /$solution
fi

./hadoop fs -test -e /$solution/input
if [ ! $? -eq 0 ]; then
  ./hadoop fs -mkdir /$solution/input
fi

./hadoop fs -test -e /$solution/output
if [ ! $? -eq 0 ]; then
  ./hadoop fs -mkdir /$solution/output
fi

sourcedir=~/git/LearningHadoop
./hadoop fs -put -f $sourcedir/samples/$sample/file* /$solution/input

cd $sourcedir/bin
exec scala -classpath "$SPARK_HOME/jars/*:$HADOOP_HOME/share/common/*:$JAVA_HOME/lib/*:$HADOOP_HOME/etc/hadoop/*" -savecompiled "$0" "hdfs://localhost:9000/$solution/input/*" "hdfs://localhost:9000/$solution/output"
!#

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object WordCount {
  def FILE_NAME: String = "/word_count_results_";
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Word Count in Spark").setMaster("local[1]");
    conf.set("spark.testing", "true");  //for memory limit
//    conf.set("spark.hadoop.yarn.resourcemanager.hostname", "localhost");
//    conf.set("spark.hadoop.yarn.resourcemanager.address", "localhost:8032");
    val sc = new SparkContext(conf);
    val textFile = sc.textFile(args(0));
    var wordCounts = textFile.flatMap(ln => ln.split(" ")).map(word => (word, 1))
      .reduceByKey((a, b) => a + b, 1);
    wordCounts.saveAsTextFile(args(1) + FILE_NAME + System.currentTimeMillis());
    println("Word count sucessfully.");
  }
}

WordCount.main(args);
