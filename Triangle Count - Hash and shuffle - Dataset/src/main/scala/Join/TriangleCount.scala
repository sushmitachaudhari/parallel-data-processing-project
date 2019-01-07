package Join

import org.apache.spark.SparkConf
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import java.io.{BufferedWriter, File, FileWriter, PrintWriter}



object TriangleCount {

  case class Twitter (from:Int , to:Int )


  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nwc.FollowerCountMain <input dir> <output dir>")
      System.exit(1)
    }


    val conf = new SparkConf().setAppName("Triangle Count")

    val sc = SparkSession.builder().config(conf).getOrCreate()

    import sc.implicits._

    val maxValue = 5000

    val schemaString = "from to"

    val fields = schemaString.split(" ").map(field => StructField(field, IntegerType, nullable = false))

    val input = sc.read.schema(StructType(fields)).csv(args(0))

    val filtered      = input.filter($"from" < maxValue && $"to" < maxValue)

    val join1 = filtered.as("s1").join(filtered.as("s2"))
                     .where($"s1.to" === $"s2.from")

    val join = join1.toDF("from", "mid1", "mid2", "to")
                    .filter($"from" =!= $"to")

    val select = join.select("from", "to")

    val triangle = select.join(filtered, select("from") === filtered("to") && select("to") === filtered("from"))

    val triangleCnt = triangle.count()

    val rr = triangleCnt.toInt

    val file = new File("something.txt")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(rr)
    bw.close()

    print("the resutl")
    println(triangleCnt.toInt)

    logger.info("the number of triangles are ")
    logger.info(triangleCnt)

  }
}