
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.split
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

object SparkMapReduce {

  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def runMapReduce(spark: SparkSession, pointPath: String, rectanglePath: String): DataFrame = 
  {
    var pointDf = spark.read.format("csv").option("delimiter",",").option("header","false").load(pointPath);
    pointDf = pointDf.toDF()
    pointDf.createOrReplaceTempView("points")

    pointDf = spark.sql("select ST_Point(cast(points._c0 as Decimal(24,20)),cast(points._c1 as Decimal(24,20))) as point from points")
    pointDf.createOrReplaceTempView("pointsDf")
    pointDf.show()

    var rectangleDf = spark.read.format("csv").option("delimiter",",").option("header","false").load(rectanglePath);
    rectangleDf = rectangleDf.toDF()
    rectangleDf.createOrReplaceTempView("rectangles")

    rectangleDf = spark.sql("select ST_PolygonFromEnvelope(cast(rectangles._c0 as Decimal(24,20)),cast(rectangles._c1 as Decimal(24,20)), cast(rectangles._c2 as Decimal(24,20)), cast(rectangles._c3 as Decimal(24,20))) as rectangle from rectangles")
    rectangleDf.createOrReplaceTempView("rectanglesDf")
    rectangleDf.show()

    val joinDf = spark.sql("select rectanglesDf.rectangle as rectangle, pointsDf.point as point from rectanglesDf, pointsDf where ST_Contains(rectanglesDf.rectangle, pointsDf.point)")
    joinDf.createOrReplaceTempView("joinDf")
    joinDf.show()
    // println(joinDf.count())

    import spark.implicits._
    val joinRdd = joinDf.rdd

    // You need to complete this part

    // below link really useful to know more about spark
    // https://medium.com/expedia-group-tech/start-your-journey-with-apache-spark-part-1-3575b20ee088

    var rectangle_point_pairs = joinRdd.map(x => (x(0), 1))

    // this is like counting all the
    var reduce = rectangle_point_pairs.reduceByKey((x, y) => (x + y))
    // reduce.collect().foreach(x => println(x))

    // filter out the number of points and sorting them
    var result = reduce.sortBy(x => x._2).map(x => x._2)

    return result.toDF() // You need to change this part
  }

}
