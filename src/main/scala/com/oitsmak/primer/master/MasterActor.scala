package com.oitsmak.primer
package master

import java.io._

import akka.actor._
import akka.event.Logging
import com.typesafe.config.{Config, ConfigFactory}
import org.roaringbitmap.RoaringBitmap
import org.roaringbitmap.buffer.ImmutableRoaringBitmap

/**
  * Remote actor which listens on port 5150
  */
class MasterActor(config: Config) extends Actor {
  val storageDir: File = checkDir(config.getString("primer.storageDir"))
  val log = Logging(context.system, this)
  var checkpoint: BigInt = 0

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
      val file: File = getFile(MasterActor.checkpointFileName, checkExists = true)
      val ois = new ObjectInputStream(new FileInputStream(file))
      checkpoint = ois.readObject().asInstanceOf[BigInt]
    } catch {
      case ex: IllegalArgumentException =>
        saveCheckpointFile(BigInt(1))
    }
  }

  def saveCheckpointFile(checkPoint: BigInt): Unit = {
    val file: File = getFile(MasterActor.checkpointFileName + ".TEMP", checkNotExists = true)
    val fos = new FileOutputStream(file)
    val oos = new ObjectOutputStream(fos)
    oos.writeObject(checkPoint)
    fos.close()
    getFile(MasterActor.checkpointFileName, checkNotExists = true)
    file.renameTo(getFile(MasterActor.checkpointFileName))
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

  override def receive: Receive = {
    case msg: String => {
      println("Message received " + msg + " from " + sender)
      sender ! "hi"
    }
    case _ => println("Received unknown msg ")
  }
}

object MasterActor {
  def props(config: Config): Props = Props(new MasterActor(config))

  final case class chunkRequest(idx: BigInt)

  final case class saveChunk(bm: RoaringBitmap, idx: BigInt)

  case object jobRequest

  val chunkPrefix: String = "chunk"
  val checkpointFileName: String = "checkpoint.bin"
  val chunkLen: Int = 1 << 26

  def main(args: Array[String]) {
    val config = ConfigFactory.parseResources("master.conf")
    val system = ActorSystem("MasterSystem", config)
    system.actorOf(MasterActor.props(config), name = "master")
  }
}

