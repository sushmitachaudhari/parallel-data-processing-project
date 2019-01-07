package Join

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager

object TriangleCount {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nJoin.TriangleCount <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Triangle Count")
    val sc = new SparkContext(conf)
    val maxValue = 70000

    val textFile      = sc.textFile(args(0)).map(line => line.split(','))

    val filtered      = textFile
                        .filter({case (k) => Integer.parseInt(k(0))<maxValue && Integer.parseInt(k(1))<maxValue})

    val forwardEdge   = filtered.map(edge => (edge(0) , edge(1)))

    val reverseEdge   = filtered.map(edge => (edge(1) , edge(0)))

    val chevron       = forwardEdge.join(reverseEdge)
                        .filter( { case( k ,(v,w)) => v!=w })
                        .map({ case (k ,(v,w)) => (v ,w)})

    val triangleCount = chevron.join(forwardEdge)
                        .filter({ case( k ,(v,w)) => v == w })
                        .count()

    val resultFinal   = triangleCount/3

    print("the Number of Triangles are:")
    println(resultFinal)


  }
}