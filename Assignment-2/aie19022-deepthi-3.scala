val sampleRdd = sc.textFile("weblogsample.csv").mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
// sampleRdd: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[168] at mapPartitionsWithIndex at aie19022-deepthi-3.scala:24
println(" QUESTION 3(a) ")
// QUESTION 3(a)
val dataRdd = sampleRdd.map{l => val tupd = l.split(',')
    val(ip,dt,tm,zn,cik,acc,ext) = (tupd(0),tupd(1),tupd(2),tupd(3),tupd(4),tupd(5),tupd(6))
    val(cod,sz,idx,nrefer,nagent,fd,crw) = (tupd(7),tupd(8),tupd(9),tupd(10),tupd(11),tupd(12),tupd(13))
    (ip,dt,tm,zn,cik,acc,ext,cod,sz,idx,nrefer,nagent,fd,crw)}
// dataRdd: org.apache.spark.rdd.RDD[(String, String, String, String, String, String, String, String, String, String, String, String, String, String)] = MapPartitionsRDD[170] at map at aie19022-deepthi-3.scala:25
dataRdd.map{s => (s._1,(s._2,s._3,s._4,s._5,s._6,s._7,s._8,s._9,s._10,s._11,s._12,s._13,s._14))}.take(4).foreach(println)
// (101.81.76.dii,(2016-03-31,00:00:00,0.0,1283497.0,0001209191-16-111028,-index.htm,200.0,14926.0,1.0,0.0,0.0,10.0,0.0))
// (104.40.128.jig,(2016-03-31,00:00:00,0.0,1094392.0,0001407682-16-000270,.txt,200.0,5161.0,0.0,0.0,0.0,10.0,0.0))
// (104.40.128.jig,(2016-03-31,00:00:00,0.0,278130.0,0001183887-16-000354,.txt,200.0,5105.0,0.0,0.0,0.0,10.0,0.0))
// (104.40.128.jig,(2016-03-31,00:00:00,0.0,1389680.0,0001407682-16-000270,.txt,301.0,237.0,0.0,0.0,1.0,10.0,0.0))
println(" QUESTION 3(b) ")
// QUESTION 3(b)
dataRdd.sortBy(_._3).toDF("ip","date","time","zone","cik","accession","extention","code","size","idx","norefer","noagent","find","crawler").show(10)
// +---------------+----------+--------+----+---------+--------------------+----------+-----+--------+---+-------+-------+----+-------+
// |             ip|      date|    time|zone|      cik|           accession| extention| code|    size|idx|norefer|noagent|find|crawler|
// +---------------+----------+--------+----+---------+--------------------+----------+-----+--------+---+-------+-------+----+-------+
// |  101.81.76.dii|2016-03-31|00:00:00| 0.0|1283497.0|0001209191-16-111028|-index.htm|200.0| 14926.0|1.0|    0.0|    0.0|10.0|    0.0|
// | 104.40.128.jig|2016-03-31|00:00:00| 0.0|1094392.0|0001407682-16-000270|      .txt|200.0|  5161.0|0.0|    0.0|    0.0|10.0|    0.0|
// | 104.40.128.jig|2016-03-31|00:00:00| 0.0| 278130.0|0001183887-16-000354|      .txt|200.0|  5105.0|0.0|    0.0|    0.0|10.0|    0.0|
// | 104.40.128.jig|2016-03-31|00:00:00| 0.0|1389680.0|0001407682-16-000270|      .txt|301.0|   237.0|0.0|    0.0|    1.0|10.0|    0.0|
// | 104.40.128.jig|2016-03-31|00:00:00| 0.0|1439019.0|0001183887-16-000354|      .txt|301.0|   237.0|0.0|    0.0|    1.0|10.0|    0.0|
// | 104.40.128.jig|2016-03-31|00:00:00| 0.0|1094392.0|0001407682-16-000270|      .txt|301.0|   237.0|0.0|    0.0|    1.0|10.0|    0.0|
// | 104.40.128.jig|2016-03-31|00:00:00| 0.0|1389680.0|0001407682-16-000270|      .txt|200.0|  5161.0|0.0|    0.0|    0.0|10.0|    0.0|
// | 104.40.128.jig|2016-03-31|00:00:00| 0.0|1439019.0|0001183887-16-000354|      .txt|200.0|  5105.0|0.0|    0.0|    0.0|10.0|    0.0|
// | 104.40.128.jig|2016-03-31|00:00:00| 0.0| 278130.0|0001183887-16-000354|      .txt|301.0|   236.0|0.0|    0.0|    1.0|10.0|    0.0|
// |107.178.195.cej|2016-03-31|00:00:00| 0.0|1402737.0|0001594062-16-000417| -xbrl.zip|200.0|155422.0|0.0|    0.0|    0.0|10.0|    0.0|
// +---------------+----------+--------+----+---------+--------------------+----------+-----+--------+---+-------+-------+----+-------+
// only showing top 10 rows
println(" QUESTION 3(c) ")
// QUESTION 3(c)
dataRdd.map(s => (s._7.split('.')(s._7.split('.').length-1),1)).reduceByKey(_+_).toDF("File types","Count").show(false)
// +----------------------------------------+-----+
// |File types                              |Count|
// +----------------------------------------+-----+
// |paper                                   |13   |
// |x                                       |1    |
// |pfe-%20%20%20%20%20%20%2012312015x10kshe|1    |
// |pdf                                     |57   |
// |xslForm13F_X01/Greenlight_13FXML_1231201|1    |
// |txt                                     |12591|
// |fil                                     |9    |
// |js                                      |3    |
// |xml                                     |5673 |
// |htm?URL=http%                           |26   |
// |css                                     |3    |
// |sgml                                    |135  |
// |xm                                      |4    |
// |htm?utm_campaign=Feed%3A+lo             |1    |
// |xls                                     |5    |
// |htm                                     |30330|
// |xlsx                                    |207  |
// |html                                    |407  |
// |zip                                     |477  |
// |xsd                                     |55   |
// +----------------------------------------+-----+
println(" QUESTION 3(d) ")
// QUESTION 3(d)
println("No of times .xml type was denied : " + dataRdd.map(s => (s._7.split('.'),s._8)).filter{case (k,v) => (v == "404.0")&&(k.contains("xml"))}.count)
// No of times .xml type was denied : 273
println(" QUESTION 3(e) ")
// QUESTION 3(e)
dataRdd.map(s => (s._8,1)).reduceByKey(_+_).filter{case (k,v) => (k == "404.0")}.toDF("Denial","Count").show
// +------+-----+
// |Denial|Count|
// +------+-----+
// | 404.0|  502|
// +------+-----+
println(" QUESTION 3(f) ")
// QUESTION 3(f)
dataRdd.sortBy(_._7).toDF("ip","date","time","zone","cik","accession","extention","code","size","idx","norefer","noagent","find","crawler").show(10)
// +---------------+----------+--------+----+---------+--------------------+---------+-----+--------+---+-------+-------+----+-------+
// |             ip|      date|    time|zone|      cik|           accession|extention| code|    size|idx|norefer|noagent|find|crawler|
// +---------------+----------+--------+----+---------+--------------------+---------+-----+--------+---+-------+-------+----+-------+
// |209.136.244.jdd|2016-03-31|00:00:30| 0.0| 907561.0|0000891092-00-001056|-0001.htm|200.0|218503.0|0.0|    0.0|    0.0|10.0|    0.0|
// |  66.249.66.ahd|2016-03-31|00:00:19| 0.0| 791914.0|0000935069-01-000219|-0001.txt|304.0|   293.0|0.0|    0.0|    0.0|10.0|    0.0|
// | 68.180.229.dea|2016-03-31|00:00:38| 0.0|1103720.0|0001077357-00-000288|-0001.txt|200.0|  2506.0|0.0|    0.0|    0.0|10.0|    0.0|
// |220.181.108.jca|2016-03-31|00:01:04| 0.0| 798523.0|0000225375-01-000009|-0001.txt|200.0|  2985.0|0.0|    0.0|    0.0|10.0|    0.0|
// | 96.227.207.fhg|2016-03-31|00:02:09| 0.0| 713676.0|0000950132-01-000174|-0001.txt|200.0| 53170.0|0.0|    0.0|    0.0| 9.0|    0.0|
// | 68.180.229.dea|2016-03-31|00:04:51| 0.0|1098380.0|0000931731-00-000394|-0001.txt|200.0|  5939.0|0.0|    0.0|    0.0|10.0|    0.0|
// |   72.79.55.haf|2016-03-31|00:00:19| 0.0|1050370.0|0000910680-01-000218|-0002.txt|200.0|  5283.0|0.0|    0.0|    0.0|10.0|    0.0|
// | 68.180.229.dea|2016-03-31|00:02:12| 0.0| 949348.0|0000949349-00-000008|-0002.txt|200.0| 12836.0|0.0|    0.0|    0.0|10.0|    0.0|
// |  66.249.66.ahd|2016-03-31|00:03:33| 0.0| 946820.0|0000929624-00-001462|-0002.txt|304.0|   293.0|0.0|    0.0|    0.0|10.0|    0.0|
// |  66.249.66.ahd|2016-03-31|00:04:03| 0.0|1094058.0|0000950170-00-001548|-0002.txt|304.0|   293.0|0.0|    0.0|    0.0|10.0|    0.0|
// +---------------+----------+--------+----+---------+--------------------+---------+-----+--------+---+-------+-------+----+-------+
// only showing top 10 rows
println(" QUESTION 3(g) ")
// QUESTION 3(g)
dataRdd.toDF("ip","date","time","zone","cik","accession","extention","code","size","idx","norefer","noagent","find","crawler").show
// +---------------+----------+--------+----+---------+--------------------+---------------+-----+--------+---+-------+-------+----+-------+
// |             ip|      date|    time|zone|      cik|           accession|      extention| code|    size|idx|norefer|noagent|find|crawler|
// +---------------+----------+--------+----+---------+--------------------+---------------+-----+--------+---+-------+-------+----+-------+
// |  101.81.76.dii|2016-03-31|00:00:00| 0.0|1283497.0|0001209191-16-111028|     -index.htm|200.0| 14926.0|1.0|    0.0|    0.0|10.0|    0.0|
// | 104.40.128.jig|2016-03-31|00:00:00| 0.0|1094392.0|0001407682-16-000270|           .txt|200.0|  5161.0|0.0|    0.0|    0.0|10.0|    0.0|
// | 104.40.128.jig|2016-03-31|00:00:00| 0.0| 278130.0|0001183887-16-000354|           .txt|200.0|  5105.0|0.0|    0.0|    0.0|10.0|    0.0|
// | 104.40.128.jig|2016-03-31|00:00:00| 0.0|1389680.0|0001407682-16-000270|           .txt|301.0|   237.0|0.0|    0.0|    1.0|10.0|    0.0|
// | 104.40.128.jig|2016-03-31|00:00:00| 0.0|1439019.0|0001183887-16-000354|           .txt|301.0|   237.0|0.0|    0.0|    1.0|10.0|    0.0|
// | 104.40.128.jig|2016-03-31|00:00:00| 0.0|1094392.0|0001407682-16-000270|           .txt|301.0|   237.0|0.0|    0.0|    1.0|10.0|    0.0|
// | 104.40.128.jig|2016-03-31|00:00:00| 0.0|1389680.0|0001407682-16-000270|           .txt|200.0|  5161.0|0.0|    0.0|    0.0|10.0|    0.0|
// | 104.40.128.jig|2016-03-31|00:00:00| 0.0|1439019.0|0001183887-16-000354|           .txt|200.0|  5105.0|0.0|    0.0|    0.0|10.0|    0.0|
// | 104.40.128.jig|2016-03-31|00:00:00| 0.0| 278130.0|0001183887-16-000354|           .txt|301.0|   236.0|0.0|    0.0|    1.0|10.0|    0.0|
// |107.178.195.cej|2016-03-31|00:00:00| 0.0|1402737.0|0001594062-16-000417|      -xbrl.zip|200.0|155422.0|0.0|    0.0|    0.0|10.0|    0.0|
// |107.178.195.cej|2016-03-31|00:00:00| 0.0|1063104.0|0001019687-16-005675|      -xbrl.zip|200.0| 51009.0|0.0|    0.0|    0.0|10.0|    0.0|
// |107.178.195.fgg|2016-03-31|00:00:00| 0.0|1174672.0|0000721748-16-001126|      -xbrl.zip|200.0| 75571.0|0.0|    0.0|    0.0|10.0|    0.0|
// | 121.200.54.aah|2016-03-31|00:00:00| 0.0|1372514.0|0001144204-16-091485|v434534_10k.htm|200.0|137569.0|0.0|    0.0|    0.0|10.0|    0.0|
// | 123.125.64.abf|2016-03-31|00:00:00| 0.0| 728387.0|0001144204-15-066965|     -index.htm|200.0|  5497.0|1.0|    0.0|    0.0|10.0|    0.0|
// |128.118.209.fbd|2016-03-31|00:00:00| 0.0| 706688.0|0001127602-09-003221|           .txt|200.0|  2262.0|0.0|    0.0|    0.0|10.0|    0.0|
// |128.118.209.fbd|2016-03-31|00:00:00| 0.0| 822416.0|0001127602-09-003229|           .txt|200.0|  2393.0|0.0|    0.0|    0.0|10.0|    0.0|
// |128.118.209.fbd|2016-03-31|00:00:00| 0.0| 822416.0|0001127602-09-003231|           .txt|200.0|  2127.0|0.0|    0.0|    0.0|10.0|    0.0|
// |128.118.209.fbd|2016-03-31|00:00:00| 0.0|1168054.0|0001127602-09-003224|           .txt|200.0|  2613.0|0.0|    0.0|    0.0|10.0|    0.0|
// |128.118.209.fbd|2016-03-31|00:00:00| 0.0| 822416.0|0001127602-09-003230|           .txt|200.0|  2328.0|0.0|    0.0|    0.0|10.0|    0.0|
// |128.118.209.fbd|2016-03-31|00:00:00| 0.0|1020062.0|0001127602-09-003228|           .txt|200.0|  3892.0|0.0|    0.0|    0.0|10.0|    0.0|
// +---------------+----------+--------+----+---------+--------------------+---------------+-----+--------+---+-------+-------+----+-------+
// only showing top 20 rows