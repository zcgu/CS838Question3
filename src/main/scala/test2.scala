/**
 * Created by zhichenggu on 11/4/15.
 */

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

class test2 {
  def main(args: Array[String]): Unit = {

    //read
    val length = Source.fromFile("QuestionA2_countdata_1").getLines().length
    val linesarray = new Array[String](length)
    Source.fromFile("QuestionA2_countdata_1").getLines().copyToArray(linesarray)

    //split
    val splitarray = new Array[Array[String]](length)
    for (i <- 0 to length-1)
      splitarray(i) = linesarray(i).split(" ")

    //create VD array
    val VDarray = new Array[(VertexId,Array[String])](length)
    for (i <- 0 to length-1)
      VDarray.update(i,(i,splitarray(i)))

    //create ED array
    val EDarraybuffer = new ArrayBuffer[Edge[Int]]()
    var flag = 0
    for (vd1 <- 0 to length-1)
      for (vd2 <- 0 to length-1) {
        if(vd1 != vd2) {
        flag=0
        for (i <- 0 to VDarray(vd1)._2.length-1)
          for (j <- 0 to VDarray(vd2)._2.length-1)
            if(VDarray(vd1)._2(i).equals(VDarray(vd2)._2(j)))
              flag = 1
        if (flag == 1) {
          EDarraybuffer += new Edge(VDarray(vd1)._1, VDarray(vd2)._1, -1)
        }
        }
      }
    val EDarray = new Array[Edge[Int]](EDarraybuffer.length)
    EDarraybuffer.copyToArray(EDarray)

    //init
    val words: RDD[(VertexId, Array[String])] = sc.parallelize(VDarray)
    val connect: RDD[Edge[Int]] = sc.parallelize(EDarray)

    val graph = Graph(words, connect)

    //Q1
    val Q1num = graph.triplets.map(triplet => (triplet.srcAttr.length,triplet.dstAttr.length)).filter{case(a,b) => a>b}.count

    //Q2
    val Q2total: VertexRDD[(Int,Int)] = graph.aggregateMessages[(Int,Int)](
      triplet => triplet.sendToSrc(1,triplet.srcAttr.length),
      (a, b) => (a._1+b._1 , b._2)
      )

    def max(a: (VertexId,(Int,Int)), b: (VertexId,(Int,Int))) :(VertexId,(Int,Int)) ={
      if(a._2._1 > b._2._1 || (a._2._1 == b._2._1 && a._2._2>b._2._2)) a else b
    }

    Q2total.reduce(max)

    //Q3
    val Q3total: VertexRDD[(Int,Double)] = graph.aggregateMessages[(Int,Double)](
      triplet => triplet.sendToSrc(1,1.0*triplet.dstAttr.length),
      (a, b) => (a._1+b._1 , a._2+b._2)
      )
    val Q3res: VertexRDD[Double] = Q3total.mapValues( (id, value) => value match { case (count, total) => total / count } )
    Q3res.collect.foreach(println)

    //Q4



  }
}
