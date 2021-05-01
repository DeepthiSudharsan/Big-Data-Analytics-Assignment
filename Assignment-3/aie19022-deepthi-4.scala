val nseRdd = sc.textFile("nsesample.txt")
// nseRdd: org.apache.spark.rdd.RDD[String] = nsesample.txt MapPartitionsRDD[59] at textFile at aie19022-deepthi-4.scala:24
val nseSchemeRdd = nseRdd.map{l => val str0 = l.split('|')
    val(ds,ts,id,stck) = (str0(0).toInt,str0(1),str0(2).toInt,str0(3))
    val (pr,vol) = (str0(4).toFloat, str0(5).toFloat)
    (ds,ts,id,stck,pr,vol)}
// nseSchemeRdd: org.apache.spark.rdd.RDD[(Int, String, Int, String, Float, Float)] = MapPartitionsRDD[60] at map at aie19022-deepthi-4.scala:25
println(" QUESTION 4(a) ")
// QUESTION 4(a)
nseSchemeRdd.map(s => (s._1,1)).reduceByKey(_+_).filter{case (k,v) => (k == 20140701)}.toDF("Date","Trades Count").show
// +--------+------------+
// |    Date|Trades Count|
// +--------+------------+
// |20140701|        7703|
// +--------+------------+
println(" QUESTION 4(b) ")
// QUESTION 4(b)
println("No of companies that traded that day : " + nseSchemeRdd.map(s => (s._1,s._4)).filter{case (k,v) => (k == 20140701)}.map((_,1)).reduceByKey(_+_).count)
// No of companies that traded that day : 20
println(" QUESTION 4(c) ")
// QUESTION 4(c)
println("No of trades b/w 9:00 to 10:00 : "+ nseSchemeRdd.map(s => (s._1,s._2)).filter{case (k,v) => (v < "10:00")&&(v > "09:00")}.count)
// No of trades b/w 9:00 to 10:00 : 2760
println(" QUESTION 4(d) ")
// QUESTION 4(d)
nseSchemeRdd.map{case (ds,ts,id,stck,pr,vol) => ((stck),(pr,vol))}.reduceByKey{
    case ((pr1,vol1),(pr2,vol2)) => (pr1+pr2,vol1+vol2)}.mapValues{
    case (pr,vol) => (pr/vol)}.map{case ((company),(perstock)) => (company,perstock)}.toDF("Company","Value Per Stock").show
// +----------+---------------+
// |   Company|Value Per Stock|
// +----------+---------------+
// |       TCS|      1.6935452|
// |      INFY|      1.6958706|
// |        LT|     0.46985382|
// |    MARUTI|      1.0162028|
// |  HINDALCO|    0.004922691|
// |TATAMOTORS|    0.025674148|
// |      SBIN|     0.76065844|
// |       ITC|     0.01744589|
// |   HCLTECH|      0.5966683|
// | ICICIBANK|     0.21275887|
// | POWERGRID|    0.006293068|
// |      ONGC|    0.029927634|
// |  AXISBANK|      0.8672583|
// |      HDFC|     0.22919473|
// |      BHEL|    0.015354721|
// |  RELIANCE|      0.1583563|
// |BHARTIARTL|     0.04836545|
// |   YESBANK|     0.05937187|
// |      IDEA|    0.006425382|
// | SUNPHARMA|     0.12365219|
// +----------+---------------+
println(" QUESTION 4(e) ")
// QUESTION 4(e)
nseSchemeRdd.map(s => (s._4,s._5)).reduceByKey(_+_).sortBy(_._1).toDF("Companies","Total Price").show
// +----------+-----------+
// | Companies|Total Price|
// +----------+-----------+
// |  AXISBANK|  1907931.9|
// |BHARTIARTL|   338077.3|
// |      BHEL|  258294.53|
// |   HCLTECH|  1476414.0|
// |      HDFC|  988278.06|
// |  HINDALCO|  176608.84|
// | ICICIBANK|  1442298.8|
// |      IDEA|   135554.6|
// |      INFY|  3204208.5|
// |       ITC|  329033.12|
// |        LT|  1745166.2|
// |    MARUTI|  2589063.2|
// |      ONGC|   428127.1|
// | POWERGRID|  140423.28|
// |  RELIANCE| 1029724.75|
// |      SBIN|  2717597.5|
// | SUNPHARMA|  692314.25|
// |TATAMOTORS|  450260.78|
// |       TCS|  2394012.5|
// |   YESBANK|   560320.0|
// +----------+-----------+
nseSchemeRdd.map(s => (s._4,s._5)).sortBy(_._1).toDF("Companies","Prices").show
// +---------+-------+
// |Companies| Prices|
// +---------+-------+
// | AXISBANK| 1928.0|
// | AXISBANK| 1922.2|
// | AXISBANK| 1925.4|
// | AXISBANK| 1928.0|
// | AXISBANK| 1927.5|
// | AXISBANK| 1925.0|
// | AXISBANK|1927.25|
// | AXISBANK|1927.85|
// | AXISBANK| 1929.0|
// | AXISBANK| 1927.0|
// | AXISBANK|1926.75|
// | AXISBANK| 1925.7|
// | AXISBANK|1925.65|
// | AXISBANK|1925.75|
// | AXISBANK| 1926.0|
// | AXISBANK| 1925.2|
// | AXISBANK| 1924.2|
// | AXISBANK| 1923.5|
// | AXISBANK| 1925.2|
// | AXISBANK| 1928.0|
// +---------+-------+
// only showing top 20 rows
// println(" QUESTION 4(f) ")
// QUESTION 4(f)
val amalgRdd = nseSchemeRdd.map{case (ds,ts,id,stck,pr,vol) => ((ds,ts.split(':')(0)),(pr,vol,1))}.reduceByKey{
    case ((pr1,vol1,count1),(pr2,vol2,count2)) => (pr1+pr2,vol1+vol2,count1+count2)}.mapValues{
    case (pr,vol,count) => (pr/count,vol)}.map{case ((ds,hour),(avgpr,vol)) => (ds,hour,avgpr,vol)}.sortBy(_._2).sortBy(_._1)
// amalgRdd: org.apache.spark.rdd.RDD[(Int, String, Float, Float)] = MapPartitionsRDD[112] at sortBy at aie19022-deepthi-4.scala:27
amalgRdd.toDF("Date","Hour","Average prices","Total Volumes").show
// +--------+----+--------------+-------------+
// |    Date|Hour|Average prices|Total Volumes|
// +--------+----+--------------+-------------+
// |20140701|  09|     1141.9519|  2.0161176E7|
// |20140701|  10|     1142.1617|  1.1876386E7|
// |20140701|  11|     1150.6204|    8235269.0|
// |20140701|  12|     1145.8376|    8264323.0|
// |20140701|  13|     1145.2273|    8388763.0|
// |20140701|  14|     1146.7056|  1.1461912E7|
// |20140701|  15|     1111.5363|  1.2031596E7|
// |20140702|  09|     1152.0974|  1.6008942E7|
// |20140702|  10|     1155.4465|  1.2055746E7|
// |20140702|  11|     1160.5166|    8317887.0|
// |20140702|  12|     1154.8484|    8471638.0|
// |20140702|  13|        1156.2|    9923698.0|
// |20140702|  14|     1156.1263|  1.1082862E7|
// |20140702|  15|     1130.4727|  1.5314167E7|
// |20140703|  09|     1158.9916|  1.4552694E7|
// |20140703|  10|     1158.3129|    8909720.0|
// |20140703|  11|     1158.6658|  1.0794019E7|
// |20140703|  12|     1158.3597|    7877229.0|
// |20140703|  13|      1146.532|     516233.0|
+--------+----+--------------+-------------+
amalgRdd.saveAsTextFile("Amalgamate_hourly_data")