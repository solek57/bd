import org.apache.spark.{SparkConf, SparkContext}

object Main {
//  @SuppressWarnings("unchecked")
  def main(args: Array[String]): Unit = {
    val cfg = new SparkConf().setAppName("Test").setMaster("local[2]")
      System.setProperty("hadoop.home.dir", "C:\\opt\\mapr\\hadoop\\hadoop-2.7.0")
        val sc = new SparkContext(cfg)
       // val textFile = sc.textFile("file:///C:/Users/stud/Documents/lab1/warandsociety.txt")
        val tripData = sc.textFile("file:///C:/Users/stud/Documents/lab1/trips.csv")
    // запомним заголовок, чтобы затем его исключить
    val tripsHeader = tripData.first
    val trips = tripData.filter(row=>row!=tripsHeader)
      .map(row=>row.split(",",-1))

    val stationData = sc.textFile("file:///C:/Users/stud/Documents/lab1/stations.csv")
    val stationsHeader = stationData.first
    val stations = stationData.filter(row=>row!=stationsHeader)
      .map(row=>row.split(",",-1))
      println(tripsHeader)
    println(stationsHeader)
    println(trips)
  trips.first().foreach(elem => print(elem + " ,"))
       // textFile.foreach(println)
       val stationsIndexed = stations.map(row=>row(0).toInt)
    val  stationsStartTerminalIndexes = stations.map(row => row(1))
    val  stationsEndTerminalIndexes = stations.map(row => row(5))

//        stationsStartTerminalIndexes.foreach(println)
//    stationsEndTerminalIndexes.foreach(println)

    val startTrips =
      stationsIndexed.join(stationsStartTerminalIndexes)
    val endTrips =
      stationsIndexed.join(stationsEndTerminalIndexes)
    sc.stop()
  }
}