package Join

import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object TriangleCount {

  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger

    // Checking the length of the arguments
    if (args.length != 2) {
      logger.error("Usage:\nJoin.TriangleCount <input nodes dir> <input edges dir>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Twitter Triangle Count")

    val sc = new SparkContext(conf)

    val max = 70000

    // Creating Pair RDD from edges file
    val edgesFile = sc.textFile(args(0))

    val forwardEdges = edgesFile.map(line => {
      val split = line.split(",")
      (split(0), split(1))
    }).filter(x => x._1.toInt < max && x._2.toInt < max)

    val reverseEdges = edgesFile.map(line => {
      val split = line.split(",")
      (split(1), split(0))
    }).filter(x => x._1.toInt < max && x._2.toInt < max)


    //  Setting up the HashMap
    val emptySet = mutable.HashSet.empty[String]
    val addToSet = (s: mutable.HashSet[String], v: String) => s += v
    val mergeSets = (p1: mutable.HashSet[String], p2: mutable.HashSet[String]) => p1 ++= p2

    //  BroadCasting the Hashmap
    val broadcastRDD = forwardEdges.aggregateByKey(emptySet)(addToSet, mergeSets)
    val broadcast = sc.broadcast(broadcastRDD.collectAsMap())


    //    Calculating the chevron
    val chevron = reverseEdges.mapPartitions(edge => {
      edge.flatMap {
        case (k, e1) =>
          broadcast.value.get(k) match {
            case None => Seq.empty[(String, String)]
            case Some(e2) => {
              var emptySeq = Seq.empty[(String, String)]
              e2.foreach(e3 => emptySeq = emptySeq :+ (e3, e1))
              emptySeq
            }
          }
      }
    }, preservesPartitioning = true)


    val triangle = chevron.mapPartitions(edge => {
      edge.flatMap {
        case (k, e1) =>
          broadcast.value.get(k) match {
            case None => Seq.empty[(String, String)]
            case Some(e2) => if (e2.contains(e1)) Seq((e1, e1)) else Seq.empty[(String, String)]
          }
      }
    }, preservesPartitioning = true)

    println(triangle.count() / 3)

  }
}

