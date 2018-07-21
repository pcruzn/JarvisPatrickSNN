package org.daa.jpsnn
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import org.apache.log4j._

/**
 * Please, note that:
 * -val k indicates the k nearest neighbors
 * -val E defines the number of executors; if omitted, Spark will estimate approx. two executors by nodes
 * -similarity is tested against an integer
 */

object JarvisPatrickSNN {
  def main(args: Array[String]) = {
    // only errors will be shown (i.e., no verbose)
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // vertices ids are required to be Long
    type VertexId = Long
    val E = 2
    
    // initial configuration: 2 nodes, local mode, each executor with 1 gb of ram
    val sparkConf = new SparkConf().setMaster("local[2]").set("spark.executor.memory", "1g");
    val sparkSession = SparkSession.builder().appName("JPAlgorithm").config(sparkConf).getOrCreate()
    val sparkContext = sparkSession.sparkContext
    
    var verticesArrayIndex = 0
    var edgesArrayIndex = 0
   
    // size of arrays or vertices and edges must be increased if data set is longer!!
    // remember that we must support at least two times the relationships
    var vertices:Array[(VertexId, String)] = new Array[(VertexId, String)](24186)
    var edges:Array[Edge[Int]] = new Array[Edge[Int]](24186*2)
    
    
    val df = sparkSession.read.csv(path = "bitcoinalpha.csv")
    df.createOrReplaceTempView(viewName = "datasetView")
    val nodes = sparkSession.sql("SELECT DISTINCT _c0  FROM datasetView ORDER BY _c0 ASC").collect()
    
    // we select the K nearest neighbors
    // then we prepare the data as vertices and edges (remember, two edges per relationship)
    val k = 3
    for (node <- nodes) {
      vertices(verticesArrayIndex) = (node(0).toString().toInt.toLong, node(0).toString())
      verticesArrayIndex += 1
      val kNodes = sparkSession.sql("SELECT * FROM datasetView WHERE _c0 = " + node(0) + " ORDER BY _c2 ASC LIMIT " + k).collect()
      for (kNode <- kNodes) {
        edges(edgesArrayIndex) = Edge(kNode(0).toString().toInt.toLong, kNode(1).toString().toInt.toLong, kNode(2).toString().toInt)
        edgesArrayIndex += 1
        edges(edgesArrayIndex) = Edge(kNode(1).toString().toInt.toLong, kNode(0).toString().toInt.toLong, kNode(2).toString().toInt)
        edgesArrayIndex += 1
      }
    }
    
            
    // create an RDD for the vertices and edges
    // the lambda expression disregarding null values is due to the fact that size is not the same as capacity! 
    val verticesRDD: RDD[(VertexId, String)] = sparkContext.parallelize(vertices, E).filter(x => x != null)
    val edgesRDD: RDD[Edge[Int]] =  sparkContext.parallelize(edges, E).filter(x => x != null)

    // constructing the graph... 
    val graph	=	Graph(verticesRDD, edgesRDD)
    // collect gives the driver the power on all the edges
    val allEdges = graph.edges.collect()

    for (tempEdge <- allEdges) {
      // edgesFromFirst and Second Point are the lists of neighbors
      val edgesFromFirstPoint = graph.edges.filter(e => e.srcId == tempEdge.srcId).map(e => (e.dstId))
      val edgesFromSecondPoint = graph.edges.filter(e => e.srcId == tempEdge.dstId).map(e => (e.dstId))
      
      // similarity is the intesection between a map
      // here we request from the executors the count of the intersected points
      val similarity = edgesFromFirstPoint.intersection(edgesFromSecondPoint).count()
   
      // in the future, this might be replaced with some functionality to group points visually
      if (similarity >= 2) 
        println ("(" + tempEdge.srcId + ", " + tempEdge.dstId + ")")
     
    }
      
    // stop the context; cannot be running another one at the same time in the same jvm in local mode
    sparkContext.stop()
  }
}