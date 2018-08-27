package actors

import java.io.{FileInputStream, FileOutputStream}

import akka.actor.{Actor, ActorRef, Props}
import akka.pattern.ask
import akka.util.Timeout
import test.utils.{Compressors, Parameters, TimeMeasure}

import scala.collection.immutable.TreeMap
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}


case class Block(content: Array[Byte], length: Int = -1) {
  def contentLength: Int = if (length < 0) content.length else length
}

case class CompressedBlock(content: Array[Byte])


class Storage extends Actor {
  private val data = new FileOutputStream("data")
  override def receive: Receive = {
    case "Finish?" => {
      data.close()
      sender() ! true
    }
    case b: CompressedBlock =>
      data.write(b.content)
    case _ => ???
  }
}


class Compressor extends Actor {
  import scala.concurrent.ExecutionContext.Implicits.global
  private var destination: TreeMap[Int, ActorRef] = TreeMap.empty
  private var doneBlocks: TreeMap[Int, CompressedBlock] = TreeMap.empty
  private var nextToSend: Int = 0
  private var futuresEnqueued: Int = 0

  private def emptyIfReady(): Unit = {
    while (doneBlocks.contains(nextToSend)) {
      destination(nextToSend) ! doneBlocks(nextToSend)
      doneBlocks -= nextToSend
      destination -= nextToSend
      nextToSend += 1
    }
    updateOpenFutures()
  }

  override def receive: Receive = {
    case (int: Int, cb: CompressedBlock) =>
      doneBlocks += int -> cb
      emptyIfReady()

    case (b: Block, dest: ActorRef) =>
      val compressorSelf = self
      destination += futuresEnqueued -> dest
      val thisFuture = futuresEnqueued
      Future {
        val compressed = Actors.compress(b)
        compressorSelf ! (thisFuture, compressed)
      }
      futuresEnqueued += 1
      updateOpenFutures()

    case "Finish?" => {
      println(s"Received question about finishing, $nextToSend == $futuresEnqueued?")
      sender() ! (nextToSend == futuresEnqueued)
    }

    case _ => ???
  }

  private def updateOpenFutures(): Unit = {
    Actors.openFutures = futuresEnqueued - nextToSend
  }

}


object Actors {
  implicit val timeout = Timeout(5.minutes)

  @volatile var openFutures = 0

  def main(args: Array[String]): Unit = {
    TimeMeasure.measure("Akka Actors", () => {
      import akka.actor.ActorSystem

      val system = ActorSystem("mySystem")
      val storage = system.actorOf(Props[Storage], "storage")
      val compressor = system.actorOf(Props[Compressor], "compressor")
      val name: String = Parameters.fileToBackup

      readFile(name, (bytes, read) => {
        compressor ! (Block(bytes, read), storage)
        while (openFutures > 20) {
          Thread.sleep(50)
        }
      })
      waitForActorToFinish(compressor)
      waitForActorToFinish(storage)

      system.terminate()
    })
  }

  def compress(block: Block) = {
    CompressedBlock(Compressors.compressLzma(block.content, block.contentLength))
  }

  def readFile(name: String, function: (Array[Byte], Int) => Unit) = {
    val fileInputStream = new FileInputStream(name)
    while (fileInputStream.available > 0) {
      val buffer = new Array[Byte](Parameters.blockSize)
      val read = fileInputStream.readNBytes(buffer, 0, buffer.length)
      function(buffer, read)
    }
    fileInputStream.close()
  }

  def waitForActorToFinish(compressor: ActorRef): Unit = {
    var keepWaiting = true
    while (keepWaiting) {
      val result = compressor ? "Finish?"
      Await.result(result, 5.seconds) match {
        case true => keepWaiting = false
        case _ => Thread.sleep(50)
      }
    }
  }


}
