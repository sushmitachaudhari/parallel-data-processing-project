package fullpagerank

import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object FullPageRank {

  def main(args: Array[String]): Unit = {
    //val logger: org.apache.log4j.Logger = LogManager.getRootLogger

    if (args.length != 2) {
      //logger.error("Usage:\npageRank.PageRank <input dir> <output dir>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Full Page Rank")
    val sc = new SparkContext(conf)

    //Reading the input
    val inputFile = sc.textFile(args(0))

    //Pre-processing the input
    val inputData = inputFile.map(line => {
      (line.split(",")(0), line.split(",")(1))
    })

    //collect 'from' nodes in one set
    //and 'to' nodes in one set
    val toList = inputData.map(x => x._1)
    val fromList = inputData.map(x => x._2)

    //nodes that don't have any incoming edges with pr 0
    val sourceNodesPR = toList.subtract(fromList).map(ele => (ele, 0.0)).distinct()

    //sourceNodesPR.foreach(println)

    //Dangling nodes with empty List as their adjacency lists
    val danglingNodes = fromList.subtract(toList).map(node => (node, List[String]())).distinct()

    //Each node with its adjacency list
    val graph = inputData.groupByKey().mapValues(_.toList)

    //creating rdd for all the nodes(including dangling nodes) with their adjacency lists
    val adjacencyListData = graph.union(danglingNodes)
    adjacencyListData.persist()

    //count the number of vertices
    val V = adjacencyListData.count()

    //ranks of each node
    var ranks = adjacencyListData.map(x => (x._1, 1.0/V))
    val iters = 15
    for(i <- 1 to iters){

      //node, (adjacency list, rank) e.g => (9, (List(8), 0.11))
      val joinRDD: RDD[(String, (List[String], Double))] = adjacencyListData.join(ranks)
      joinRDD.persist()

      //ranks of the node, without the adjacency list, e.g (9, 0.11)
      ranks = joinRDD.map(data => (data._1, data._2._2))

      //delta -- the sum of the pr of all the nodes whose adjacency list are empty -- dangling nodes
      val delta = joinRDD.filter(x => x._2._1.isEmpty).map(x => x._2._2).sum()

      //calculate the outgoing pr
      val outGoingContributions: RDD[(String, Double)] = joinRDD.flatMap(
        {
          case (nodeId, data) =>
            //if adjacency list is not empty
            if(data._1.nonEmpty) {

              //calculate the size of the adjacency list
              val adjListSize = data._1.size

              //map each pr to new pr
              data._1.map(el => (el, data._2/adjListSize))
            } else {
              List((nodeId, data._2))
            }
        }
      )
      val newPR = outGoingContributions.reduceByKey(_+_)
      ranks = newPR.union(sourceNodesPR)
      ranks.persist()
    }
    val sortedUsers = sc.parallelize(ranks.take(100))
    sortedUsers.saveAsTextFile(args(1))
    println(ranks.values.sum())
  }}