package com.oitsmak.primer
package worker

import akka.actor.{Props, Actor, ActorSystem}
import com.typesafe.config.ConfigFactory

/**
  * Local actor which listens on any free port
  */
class WorkerActor extends Actor {
  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    val masterActor = context.actorSelection("akka.tcp://MasterSystem@127.0.0.1:5150/user/master")
    println("That 's master:" + masterActor)
    masterActor ! "Hey"
  }

  override def receive: Receive = {

    case msg: String => {
      println("got message from master" + msg + "; Sender is: " + sender)
    }
  }
}


object WorkerActor {

  def main(args: Array[String]) {

    val config = ConfigFactory.parseResources("worker.conf")
    val system = ActorSystem("ClientSystem", config)
    val localActor = system.actorOf(Props[WorkerActor], name = "client")
  }


}