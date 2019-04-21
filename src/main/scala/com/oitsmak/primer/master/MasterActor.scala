package com.oitsmak.primer
package master

import java.io._
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedDeque}

import scala.collection.concurrent
import scala.concurrent.duration._
import collection.JavaConverters._
import akka.actor._
import akka.event.Logging
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}
import org.roaringbitmap.RoaringBitmap

import scala.concurrent.{Await, Future}

/**
  * Remote actor which listens on port 5150
  */
class MasterActor(config: Config) extends Actor {

  import MasterActor._
  import worker.WorkerActor._

  val storageDir: File = checkDir(config.getString("primer.storageDir"))
  val log = Logging(context.system, this)
  var checkpoint: BigInt = 0
  val jobQueue = new ConcurrentLinkedDeque[BigInt]()
  val workers: concurrent.Map[ActorRef, BigInt] =
    new ConcurrentHashMap[ActorRef, BigInt]().asScala
  var assigned: Set[BigInt] = Set[BigInt]()


  def checkDir(path: String): File = {
    val dir = new File(path)
    assert(dir.exists() && dir.isDirectory)
    dir
  }

  def getFile(name: String, checkExists: Boolean = false,
              checkNotExists: Boolean = false): File = {
    val file = new File(storageDir, name)
    if (checkExists)
      require(file.exists() && file.isFile)
    if (checkNotExists)
      require(!file.exists())
    file
  }

  def chunkDone(idx: BigInt): Boolean = {
    new File(storageDir, chunkPrefix + idx.toString).exists()
  }

  def rmIfExists(name: String): File = {
    val file: File = new File(storageDir, name)
    if (file.exists()) file.delete()
    file
  }

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    checkFirstChunk()
    checkCheckpointFile()
    log.info("Master is ready.")
  }

  def checkFirstChunk(): Unit = {
    try {
      val file: File = getFile(MasterActor.chunkPrefix + BigInt(1).toString(), checkExists = true)
    } catch {
      case ex: IllegalArgumentException =>
        createFirstChunk()
    }
  }

  def createFirstChunk(): Unit = {
    log.info("Creating First chunk.")
    val bm = new RoaringBitmap()
    bm.add(2L, MasterActor.chunkLen)
    for (idx <- 2 to MasterActor.chunkLen) {
      if (bm.contains(idx)) {
        var j = 2 * idx
        while (j < MasterActor.chunkLen) {
          bm.remove(j)
          j += idx
        }
      }
    }
    bm.runOptimize()
    saveChunk(bm, BigInt(1))
  }

  def checkCheckpointFile(): Unit = {
    try {
      val file: File = getFile(checkpointFileName, checkExists = true)
      val ois = new ObjectInputStream(new FileInputStream(file))
      checkpoint = ois.readObject().asInstanceOf[BigInt]
    } catch {
      case ex: IllegalArgumentException =>
        saveCheckpointFile(BigInt(1))
    }
  }

  def saveCheckpointFile(checkPoint: BigInt): Unit = {
    val file: File = getFile(checkpointFileName + ".TEMP", checkNotExists = true)
    val fos = new FileOutputStream(file)
    val oos = new ObjectOutputStream(fos)
    oos.writeObject(checkPoint)
    fos.close()
    getFile(checkpointFileName, checkNotExists = true)
    file.renameTo(getFile(checkpointFileName))
    this.checkpoint = checkPoint
  }

  def saveChunk(bm: RoaringBitmap, idx: BigInt): Unit = {
    val filename = MasterActor.chunkPrefix + idx.toString()
    log.info("Saving chunk {}", filename)
    val file = getFile(filename, checkNotExists = true)
    val out = new DataOutputStream(new FileOutputStream(file))
    bm.serialize(out)
    out.close()
    log.info("Saved chunk {}", filename)
  }

  def getChunk(idx: BigInt): RoaringBitmap = {
    val fileName = MasterActor.chunkPrefix + idx.toString()
    val file = getFile(fileName, checkExists = true)
    val ret = new RoaringBitmap()
    ret.deserialize(new DataInputStream(new FileInputStream(file)))
    ret
  }

  def updateJobQueue(idx: BigInt = 0): Unit = {
    if (idx != 0) {
      jobQueue.remove(idx)
    }
    var addable = checkpoint
    while (jobQueue.size() < 1.5 * workers.size) {
      if (!chunkDone(addable)
        && assigned.contains(addable)
        && !isOngoing(addable))
        jobQueue.add(addable)
      addable += 1
    }
  }

  def isOngoing(idx: BigInt): Boolean = {
    workers.foreach(w => {
      if (w._2 == idx && isOk(w._1))
        return true
    })
    false
  }

  def isOk(ref: ActorRef): Boolean = {
    implicit val timeout: Timeout = Timeout(3 seconds)
    Await.result(ref ? AskOk, 3 seconds).asInstanceOf[IsOk].is
  }

  override def receive: Receive = {
    case Connect =>
      workers += ((sender, null))
      updateJobQueue()
    case JobRequest => {
      if (jobQueue.isEmpty) updateJobQueue() // just to be safe
      val nextJob = JobDef(jobQueue.removeFirst(), chunkLen)
      sender ! nextJob
      assigned += nextJob.idx
      updateJobQueue()
    }
    case GetChunk(idx) =>
      try {
        val bm = getChunk(idx)
        sender() ! GiveChunk(bm, idx)
      } catch {
        case e: Exception => sender() ! akka.actor.Status.Failure(e)
      }
    case PutChunk(bm, idx) => {
      saveChunk(bm, idx)
      assigned -= idx
      workers += ((sender, null))
      updateJobQueue(idx)
    }
    case _ => println("Received unknown msg!")
  }
}

object MasterActor {
  def props(config: Config): Props = Props(new MasterActor(config))

  final case class GetChunk(idx: BigInt)

  final case class PutChunk(bm: RoaringBitmap, idx: BigInt)

  final case class IsOk(is: Boolean)

  case object JobRequest

  case object Connect

  val chunkPrefix: String = "chunk"
  val checkpointFileName: String = "checkpoint.bin"
  val chunkLen: Int = 1 << 26

  def main(args: Array[String]) {
    val config = ConfigFactory.parseResources("master.conf")
    val system = ActorSystem("MasterSystem", config)
    system.actorOf(MasterActor.props(config), name = "master")
  }
}

