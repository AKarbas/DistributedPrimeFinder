package com.oitsmak.primer
package worker

import akka.actor.{Actor, ActorSelection, ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import org.roaringbitmap.RoaringBitmap

/**
  * Local actor which listens on any free port
  */
class WorkerActor extends Actor {

  import master.MasterActor._
  import WorkerActor._

  var masterActor: ActorSelection = _

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    masterActor = context.actorSelection("akka.tcp://MasterSystem@127.0.0.1:5150/user/master")
    println("That 's master:" + masterActor)
    masterActor ! Connect
  }

  override def receive: Receive = {

    case msg: String => {
      println("got message from master" + msg + "; Sender is: " + sender)
    }
  }
}


object WorkerActor {

  final case class GiveChunk(bm: RoaringBitmap, idx: BigInt)

  final case class JobDef(idx: BigInt, length: Int)

  final case object AskOk


  def main(args: Array[String]) {

    val config = ConfigFactory.parseResources("worker.conf")
    val system = ActorSystem("ClientSystem", config)
    val localActor = system.actorOf(Props[WorkerActor], name = "client")
  }


}