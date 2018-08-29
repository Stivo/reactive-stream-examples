package actors

import java.io.{File, FileInputStream, FileOutputStream}
import java.nio.Buffer
import java.nio.file.Files

import akka.actor.{Actor, ActorRef, Props}
import akka.pattern.ask
import akka.util.Timeout
import org.apache.commons.io.FileUtils
import test.utils.{Compressors, Parameters, TimeMeasure}

import scala.collection.immutable.TreeMap
import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}


case class Block(content: Array[Byte], length: Int = -1) {
  def contentLength: Int = if (length < 0) content.length else length
}

case class CompressedBlock(content: Array[Byte])


class Storage extends Actor {
  private val data = new FileOutputStream("data")
  private val metadata: mutable.Buffer[String] = mutable.Buffer.empty
  private var counter = 0
  override def receive: Receive = {
    case b: CompressedBlock =>
      data.write(b.content)
      metadata += s"$counter ${b.content.length}"
      counter += 1
    case Actors.`askIfFinished` => {
      data.close()
      FileUtils.writeLines(new File("metadata"), metadata.asJava)
      sender() ! true
    }
    case x => throw new IllegalArgumentException("Unexpected message " + x)
  }
}

object Actors {
  val askIfFinished = "Finished?"
  val queryForOpenFutures = "openFutures?"

  implicit val timeout = Timeout(5.minutes)

  @volatile var openFutures = 0

  def main(args: Array[String]): Unit = {
    TimeMeasure.measure("AkkaStream Actors", () => {
      import akka.actor.ActorSystem

      val system = ActorSystem("mySystem")
      val storage = system.actorOf(Props[Storage], "storage")
      val compressor = system.actorOf(Props[Compressor], "compressor")
      val name: String = Parameters.fileToBackup

      readFile(name, (bytes, read) => {
        compressor ! (Block(bytes, read), storage)
        while (openFutures > 20) {
          println(s"Currently have $openFutures open futures, so waiting before resubmitting")
          Thread.sleep(500)
        }
      })
      waitForActorToFinish(compressor)
      waitForActorToFinish(storage)

      system.terminate()
    })
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
      val result = compressor ? askIfFinished
      Await.result(result, 5.seconds) match {
        case true => keepWaiting = false
        case _ => Thread.sleep(50)
      }
    }
  }

}


class Compressor extends Actor {
  import scala.concurrent.ExecutionContext.Implicits.global
  private var destination: TreeMap[Int, ActorRef] = TreeMap.empty
  private var doneBlocks: TreeMap[Int, CompressedBlock] = TreeMap.empty
  private var nextToSend: Int = 0
  private var futuresEnqueued: Int = 0

  val compressorSelf = self

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
    case (b: Block, dest: ActorRef) =>
      val thisFuture = futuresEnqueued
      destination += thisFuture -> dest
      Future {
        val compressed = compress(b)
        compressorSelf ! (thisFuture, compressed)
      }
      futuresEnqueued += 1
      updateOpenFutures()

    case (int: Int, cb: CompressedBlock) =>
      doneBlocks += int -> cb
      emptyIfReady()

    case Actors.queryForOpenFutures =>
      sender() ! futuresEnqueued - nextToSend

    case Actors.`askIfFinished` => {
      println(s"Received question about finishing, $nextToSend == $futuresEnqueued?")
      sender() ! (nextToSend == futuresEnqueued)
    }

    case x => throw new IllegalArgumentException("Unexpected message " + x)
  }

  private def compress(block: Block) = {
    CompressedBlock(Compressors.compressLzma(block.content, block.contentLength))
  }

  private def updateOpenFutures(): Unit = {
    Actors.openFutures = futuresEnqueued - nextToSend
  }

}


//var keepWaiting = true
//while (keepWaiting) {
//  val answer = Await.result(compressor ? queryForOpenFutures, 1.minute)
//  answer match {
//  case x: Int if x > 20 =>
//  println(s"Currently have $x open futures, so waiting before resubmitting")
//  Thread.sleep(500)
//  case _ => keepWaiting = false
//}
//}
