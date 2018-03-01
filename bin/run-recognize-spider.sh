#!/bin/sh
solution=recognize_spider
sample=$solution

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

cd $sourcedir/bin

exec scala -classpath "$SPARK_HOME/jars/*:$HADOOP_HOME/share/common/*:$JAVA_HOME/lib/*:$HADOOP_HOME/etc/hadoop/*:$HBASE_HOME/lib/*" -savecompiled "$0" "hdfs://localhost:9000/$solution/input/"
!#

import java.text.SimpleDateFormat
import java.util.UUID
import java.util.Calendar
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.Seconds
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.clustering.KMeans

object RecognizeSpider {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("Recognize Spider")
      .setMaster("local[1]")
      .set("spark.testing", "true");  //for memory limit

    val sc = new StreamingContext(conf, Seconds(10));

    sc.textFileStream(args(0)) //when copy large file from local to HDFS, the file named *._COPYING_ may be catched and an path not exist exception would be thrown. 
    //the solution is: copy file with name start with ".", then rm to specified name.
    .foreachRDD(rdd => {
      if (rdd.count > 0) {
        val dateFormat = new SimpleDateFormat("yyyy-MM-dd H:mm:ss");
        val calendar = Calendar.getInstance();

        val logDatas = rdd.filter(ln => !ln.startsWith("#"))
          .map(ln => ln.split(" "))
          .map(arr => {
             calendar.setTime(dateFormat.parse(arr(0).concat(" ").concat(arr(1))));
             calendar.add(Calendar.HOUR, 8);
             (arr(9), Array(arr(9)
             , dateFormat.format(calendar.getTime())
             , arr(11), arr(4), arr(5), arr(6)));
          })  //0:ip; 1:time; 2:status; 3:method; 4:url; 5:query
        .repartition(2)  //repartition by ip
        .cache();

        val hbaseConf = HBaseConfiguration.create();
        hbaseConf.set("hbase.zookeeper.property.clientPort", "2181");
        hbaseConf.set("hbase.zookeeper.quorum", "localhost");

        val jobConf = new JobConf(hbaseConf, this.getClass())
        jobConf.setOutputFormat(classOf[TableOutputFormat]);
        jobConf.set(TableOutputFormat.OUTPUT_TABLE, "recognize_fact");

        val clusterBytes = Bytes.toBytes("info");
        val visitTimeBytes = Bytes.toBytes("visitTime");
        val methodBytes = Bytes.toBytes("mehtod");
        val urlBytes = Bytes.toBytes("url");
        val queryBytes = Bytes.toBytes("query");
        val statusBytes = Bytes.toBytes("status");
        val ipBytes = Bytes.toBytes("ip");
 
        logDatas
          .map(v => {
            val put = new Put(Bytes.toBytes(v._2(0).concat(v._2(1)).concat(v._2(4))));
            put.add(clusterBytes, visitTimeBytes, Bytes.toBytes(v._2(1)));
            put.add(clusterBytes, methodBytes, Bytes.toBytes(v._2(3)));
            put.add(clusterBytes, urlBytes, Bytes.toBytes(v._2(4)));
            put.add(clusterBytes, queryBytes, Bytes.toBytes(v._2(5)));
            put.add(clusterBytes, statusBytes, Bytes.toBytes(v._2(2)));
            put.add(clusterBytes, ipBytes, Bytes.toBytes(v._2(0)));
            (new ImmutableBytesWritable, put);
          })
          .saveAsHadoopDataset(jobConf);

        val pageDatas = logDatas.filter(v => v._2(4).matches("(?i).*[.]aspx")).cache();
        val resDatas = logDatas.filter(v => !v._2(4).matches("(?i).*[.]aspx"))
          .mapValues(v => 1)
          .reduceByKey((a, b) => 1); //rdd(ip1, count1), (ip2, count2))

        //distinct pages / per ip
        val pageDatasPerIP = pageDatas
          .mapValues(v => v(4))
          .distinct()
          .cache();
        val countPagesPerIP = pageDatasPerIP
          .mapValues(v => 1).reduceByKey((a, b) => a + b);  //rdd((ip1, count3), (ip2, count4))

        //count of distinct pages
        val countPages = sc.sparkContext.broadcast(pageDatasPerIP.map(v => v._2)
          .distinct().count().toDouble);

        //distinct hours / per ip
        val hoursDatasPerIP = pageDatas
          .mapValues(v => {
              calendar.setTime(dateFormat.parse(v(1)));
              calendar.get(Calendar.HOUR_OF_DAY);
          })
          .distinct()
          .mapValues(v => 1)
          .reduceByKey((a, b) => a + b);  //rdd((ip1, count5), (ip2, count6))

        val inputDatas = countPagesPerIP.leftOuterJoin(resDatas) //rdd(ip1, (count3, count1))
          .leftOuterJoin(hoursDatasPerIP) //rdd(ip1, ((count3, count1), count5))
          .map(v => Array(v._2._1._1.toDouble / countPages.value, v._2._1._2.getOrElse(0).toDouble, v._2._2.getOrElse(0).toDouble))
          .map(v => Vectors.dense(v))
          .cache();

        val countInput = sc.sparkContext.broadcast(inputDatas.count);
        val kMeansModel = KMeans.train(inputDatas, 2, 50);
        jobConf.set(TableOutputFormat.OUTPUT_TABLE, "recognize_spider");

        val cost = Bytes.toBytes(kMeansModel.computeCost(inputDatas).toString());
        val predict0 = Bytes.toBytes("0");
        val predict1 = Bytes.toBytes("1");
        val predictBytes = Bytes.toBytes("predict");
        val resBytes = Bytes.toBytes("res");
        val urlCountBytes = Bytes.toBytes("urlCount");
        val timeCountBytes = Bytes.toBytes("timeCount");

        inputDatas
          .map(arr => {
            val put = new Put(Bytes.toBytes(UUID.randomUUID().toString()));
            val predict = if (kMeansModel.predict(arr) == 0) predict0 else predict1;

            put.add(clusterBytes, predictBytes, predict);
            put.add(clusterBytes, urlCountBytes, Bytes.toBytes(arr(0)));
            put.add(clusterBytes, resBytes, Bytes.toBytes(arr(1)));
            put.add(clusterBytes, timeCountBytes, Bytes.toBytes(arr(2)));
            (new ImmutableBytesWritable, put);
          })
          .saveAsHadoopDataset(jobConf);
       }
    });

    sc.start();
    sc.awaitTermination();
  }
}

RecognizeSpider.main(args);
