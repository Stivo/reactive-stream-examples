package actors

import java.io.{File, FileOutputStream}

import akka.Done
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Broadcast, FileIO, GraphDSL, Merge, RunnableGraph, Sink}
import akka.stream.{ActorMaterializer, ClosedShape, OverflowStrategy}
import test.utils.{Compressors, Parameters, TimeMeasure}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

object AkkaStreamScala {
  var name: String = Parameters.fileToBackup

  import scala.concurrent.ExecutionContext.Implicits.global

  def main(args: Array[String]): Unit = TimeMeasure.measure("Akka Stream with Graph", () => {
    val system = ActorSystem.create("compression")
    implicit val mat = ActorMaterializer.create(system)

    val inputFile = new File(name).toPath
    val graph = RunnableGraph.fromGraph(GraphDSL.create(Sink.ignore) { implicit builder =>
      sink =>
        import GraphDSL.Implicits._

        val broadcast = builder.add(Broadcast[(Array[Byte], Long)](2))
        val merge = builder.add(Merge[Done](2))

        FileIO.fromPath(inputFile, Parameters.blockSize)
          .zipWithIndex
          .buffer(20, OverflowStrategy.backpressure)
          .mapAsync(8) { case (bs, index) =>
            Future {
              val bytes = bs.toArray
              val bytes1 = Compressors.compressLzma(bytes, bytes.length)
              (bytes1, index)
            }
          }.buffer(20, OverflowStrategy.backpressure) ~> broadcast

        val dataSaved = broadcast
          .fold(new FileOutputStream("data")) { case (fos, (bytes, _)) =>
            fos.write(bytes)
            fos
          }.map{ fos => fos.close(); Done }

        val metadataSaved = broadcast
          .fold(new FileOutputStream("metadata")) { case (fos, (bytes, index)) =>
            fos.write(s"$index ${bytes.length}\n".getBytes)
            fos
          }.map{ fos => fos.close(); Done }

        dataSaved ~> merge.in(0)
        metadataSaved ~> merge.in(1)

        merge.out ~> sink
        ClosedShape
    })
    Await.result(graph.run(), 1.minutes)
    mat.shutdown()
    system.terminate
  })
}