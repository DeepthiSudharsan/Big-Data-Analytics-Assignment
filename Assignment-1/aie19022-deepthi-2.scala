val wikiRdd = sc.textFile("wikisample.txt")
// wikiRdd: org.apache.spark.rdd.RDD[String] = wikisample.txt MapPartitionsRDD[58] at textFile at <console>:24
println(" QUESTION 2(a) ")
// QUESTION 2(a)
val wordRdd = wikiRdd.map(_.replaceAll("[^A-Za-z0-9\\s]*","")).flatMap(_.split("\\s+")).map(l => l.toLowerCase)
// wordRdd: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[61] at map at <console>:25
println("Total no of words : "+ wordRdd.count)
// Total no of words : 6380358
val uniquewordRdd = wordRdd.map((_,1)).reduceByKey(_+_)
// uniquewordRdd: org.apache.spark.rdd.RDD[(String, Int)] = ShuffledRDD[63] at reduceByKey at <console>:25
println("No of unique words : "+ uniquewordRdd.count)
// No of unique words : 196082
println(" QUESTION 2(b) ")
// QUESTION 2(b)
uniquewordRdd.filter{case (k,v) => (k == "high")}.toDF("Word","Count").show
// +----+-----+
// |Word|Count|
// +----+-----+
// |high|39470|
// +----+-----+
val bigramRdd = wikiRdd.map(_.replaceAll("[^A-Za-z0-9\\s]*","").toLowerCase).map(_.split("\\s+")).flatMap(_.sliding(2)).map{_.mkString(" ")}.map((_,1)).reduceByKey(_+_)
// bigramRdd: org.apache.spark.rdd.RDD[(String, Int)] = ShuffledRDD[73] at reduceByKey at <console>:25
bigramRdd.filter{case (k,v) => (k == "high profile")}.toDF("Bigram","Count").show
// +------------+-----+
// |      Bigram|Count|
// +------------+-----+
// |high profile|  158|
// +------------+-----+
println("Bigram probability of high profile : " + (bigramRdd.filter{case (k,v) => (k == "high profile")}.values.collect()(0).toFloat/bigramRdd.count.toFloat))
// Bigram probability of high profile : 7.476494E-5
bigramRdd.filter{case (k,v) => (k == "high school")}.toDF("Bigram","Count").show
// +-----------+-----+
// |     Bigram|Count|
// +-----------+-----+
// |high school|12603|
// +-----------+-----+
println("Bigram probability of high school : " + (bigramRdd.filter{case (k,v) => (k == "high school")}.values.collect()(0).toFloat/bigramRdd.count.toFloat))
// Bigram probability of high school : 0.005963687
println(" QUESTION 2(c) ")
// QUESTION 2(c)
val sentwordRdd = wikiRdd.map(_.replaceAll("[^A-Za-z0-9.\\s]*","").toLowerCase)
// sentwordRdd: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[148] at map at <console>:25
val sentenceRdd = sentwordRdd.flatMap{_.split('.')}.map(_.split("\\s+"))
// sentenceRdd: org.apache.spark.rdd.RDD[Array[String]] = MapPartitionsRDD[150] at map at <console>:25
println("No of sentences with the word mountain and god : "+ sentenceRdd.map(l => (l.contains("mountain") && l.contains("god"))).filter(_ == true).count)
// No of sentences with the word mountain and god : 3
uniquewordRdd.filter{case (k,v) => (k == "mountain")}.toDF("Word","Count").show
// +--------+-----+
// |    Word|Count|
// +--------+-----+
// |mountain| 2072|
// +--------+-----+
println(" QUESTION 2(d) ")
// QUESTION 2(d)
uniquewordRdd.sortBy(_._2).toDF("Word","Occurences").show(10)
// +---------------+----------+
// |           Word|Occurences|
// +---------------+----------+
// |            mjs|         1|
// |       bodongpa|         1|
// |   sundayschool|         1|
// |          sympy|         1|
// |     strippable|         1|
// |australianbuilt|         1|
// |          3000c|         1|
// |           bagn|         1|
// |          kwong|         1|
// |        ibestad|         1|
// +---------------+----------+
// only showing top 10 rows
uniquewordRdd.sortBy(_._2,false).toDF("Word","Occurences").show(10)
// +----+----------+
// |Word|Occurences|
// +----+----------+
// | the|    477818|
// |  of|    228398|
// | and|    195340|
// |  in|    173612|
// |  to|    140312|
// |   a|    137489|
// |  is|     72324|
// | was|     60984|
// |  as|     56400|
// | for|     52400|
// +----+----------+
// only showing top 10 rows
println(" QUESTION 2(e) ")
// QUESTION 2(e)
println("No of lines exited without numerical values : "+ sentwordRdd.flatMap{_.split('.')}.map(_.replaceAll("[^0-9]*","")).map(_.length>0).filter(_ == true).count)
// No of lines exited without numerical values : 117507
