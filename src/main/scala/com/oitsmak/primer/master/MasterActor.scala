package com.oitsmak.primer
package master

import akka.actor._
import com.typesafe.config.ConfigFactory

/**
  * Remote actor which listens on port 5150
  */
class MasterActor extends Actor {
  override def receive: Receive = {
    case msg: String => {
      println("Message received " + msg + " from " + sender)
      sender ! "hi"
    }
    case _ => println("Received unknown msg ")
  }
}

object MasterActor {
  def main(args: Array[String]) {
    val config = ConfigFactory.parseResources("master.conf")
    val system = ActorSystem("MasterSystem", config)
    val remote = system.actorOf(Props[MasterActor], name = "master")
    println("Master is ready")
  }
}

