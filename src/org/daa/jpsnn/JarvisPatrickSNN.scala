package org.daa.jpsnn
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import scala.math.random
import org.apache.log4j._


object JarvisPatrickSNN {
  def main(args: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    type	VertexId	=	Long
    
    val sparkConf = new SparkConf().setAppName("Spark Test").setMaster("local[2]").set("spark.executor.memory", "1g");
    //val sparkContext = new SparkContext(sparkConf)
    //val minThreshold = 5
    var verticesArrayIndex = 0
    var edgesArrayIndex = 0
   
    var vertices:Array[(VertexId, String)] = new Array[(VertexId, String)](24186)
    var edges:Array[Edge[Int]] = new Array[Edge[Int]](24186*2)
    
    val sparkSession = SparkSession.builder().appName("JPAlgorithm").config(sparkConf).getOrCreate()
    val sparkContext = sparkSession.sparkContext
    
    val df = sparkSession.read.csv(path = "bitcoinalpha.csv")
    df.createOrReplaceTempView(viewName = "data")
    val nodes = sparkSession.sql("SELECT DISTINCT _c0  FROM data ORDER BY _c0 ASC").collect()
    
        
    for (node <- nodes) {
      vertices(verticesArrayIndex) = (node(0).toString().toInt.toLong, node(0).toString())
      verticesArrayIndex += 1
      val newNodes = sparkSession.sql("SELECT * FROM data WHERE _c0 = " + node(0) + " ORDER BY _c2 ASC LIMIT 3").collect()
      for (newNode <- newNodes) {
        edges(edgesArrayIndex) = Edge(newNode(0).toString().toInt.toLong, newNode(1).toString().toInt.toLong, newNode(2).toString().toInt)
        edgesArrayIndex += 1
        edges(edgesArrayIndex) = Edge(newNode(1).toString().toInt.toLong, newNode(0).toString().toInt.toLong, newNode(2).toString().toInt)
        edgesArrayIndex += 1
      }
    }
    
            
    // Create an RDD for the vertices
    val verticesRDD: RDD[(VertexId, String)] = sparkContext.parallelize(vertices, 2).filter(x => x != null)

    // There is no such thing as 'undirected graph' in graphx
    // We need to create both ends of an edge (-> and <-)
                         
    val edgesRDD: RDD[Edge[Int]] =  sparkContext.parallelize(edges, 2).filter(x => x != null)

    val graph	=	Graph(verticesRDD, edgesRDD)
    // we will always have two edges per relationship (
    // val numberOfRelationships = graph.edges.count() / 2
    
      
    // allEdges is the 
    //val filteredEdges = graph.edges.map(e=> (e.srcId,e.dstId, e.attr)).intersection(graph.edges.map(e => (e.dstId, e.srcId, e.attr))).collect()
    val allEdges = graph.edges.collect()
      
    
    var clusterElement = 1
    for (tempEdge <- allEdges) {
      val edgesFromFirstPoint = graph.edges.filter(e => e.srcId == tempEdge.srcId).map(e => (e.dstId))
      val edgesFromSecondPoint = graph.edges.filter(e => e.srcId == tempEdge.dstId).map(e => (e.dstId))
      
      val similarity = edgesFromFirstPoint.intersection(edgesFromSecondPoint).count()
   
      if (similarity >= 2) {
        println ("(" + tempEdge.srcId + ", " + tempEdge.dstId + ")")
        clusterElement.+(1);
      }
     
    }
      
    
    // stop the context; cannot be running another one at the same time in the same jvm
    sparkContext.stop()
  }
}