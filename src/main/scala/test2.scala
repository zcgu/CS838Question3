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
        flag=0
        for (i <- 0 to VDarray(vd1)._2.length-1)
          for (j <- 0 to VDarray(vd2)._2.length-1)
            if(VDarray(vd1)._2(i).equals(VDarray(vd2)._2(j)))
              flag = 1
        if (flag == 1){
          EDarraybuffer+=new Edge(VDarray(vd1)._1,VDarray(vd2)._1,-1)
        }
      }
    val EDarray = new Array[Edge[Int]](EDarraybuffer.length)
    EDarraybuffer.copyToArray(EDarray)

    //init
    val words: RDD[(VertexId, Array[String])] = sc.parallelize(VDarray)
    val connect: RDD[Edge[Int]] = sc.parallelize(EDarray)

    val graph = Graph(words, connect)

    //Q1
    val num: RDD[(Int,Int)] = graph.triplets.map(triplet => (triplet.srcAttr.length,triplet.dstAttr.length))
    num.filter{case(a,b) => a>b}.count

    //Q2


  }
}
