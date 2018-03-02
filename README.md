# Learning Hadoop
1. Platform:  
Ubunut 16.04  
Hadoop 2.9.0  
Spark 2.2.1  
Hbase 1.4.0  
Hive 2.1.1  
Pig 0.17.0  

2. Project of Recognize Spider:
Recognize Spirder from IIS Logs with the percent of visit pages per IP, visit resource files or not, percent of visit time per IP.  
Analysis it on Spark by scala.  
files:  
       /bin/run-recognize-spider.sh  
       /bin/init-recognize-hbase.sh

3. Projects of Hello World:

3.1 WordCount on MapReduce by JAVA.  
files:  
       /bin/run-wordcount.sh  
       /src/WordCount.java

3.2 WordCount on Spark by Scala.  
files: /bin/run-wordcount-scala.sh

3.3 One table join itself on MapReduce  
Stored into Derby by Hive  
files: /bin/run-selfjoin-hive.sh

Stored into Hbase by Hive  
files: /bin/run-selfjoin-hbase-hive.sh

Stored into Hbase by Pig  
files: /bin/run-selfjoin-pig.sh

