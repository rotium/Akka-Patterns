/*
 * Copyright 2014 Rotium Ltd.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.example

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.ClassTag
import com.rotium.akka.CreateWorkers
import com.rotium.akka.Master
import com.rotium.akka.WorkPullingPattern.Epic
import com.rotium.akka.Worker
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.PoisonPill
import com.rotium.akka.NotifyWorkDone
import com.rotium.akka.NotifyWorkStart

class StringMaster extends Actor with Master[String] with NotifyWorkDone {
  override def createWorker[A: ClassTag, T <: Actor with Worker[A]: ClassTag]() = {
    context.actorOf(Props(new StringWorker(context.self)))
  }
  override def unregisterWorker(worker: ActorRef) = {
    if (workers.isEmpty) {
      context.system.shutdown() 
    }
  }
  override def epicDone(epic: Epic[String], requester: ActorRef) = {
    super.epicDone(epic, requester)
    if (epics.isEmpty)
      workers foreach { _ ! PoisonPill }
  }
}
class StringWorker(val master: ActorRef)(implicit val tag: ClassTag[String]) extends Actor with Worker[String] {

  def doWork(work: String): Future[_] = {
    Future {
      log.error("Data=" + work)
      Thread.sleep(1000)
    }
  }

}

object WorkPullingPatternDemo extends App {
  
  val system = ActorSystem("MyActorSystem")
  val master = system.actorOf(Props[StringMaster], "StringMaster")
  master ! CreateWorkers[String, StringWorker](1)
  
  import akka.pattern.ask
  import akka.util.Timeout
  import scala.concurrent.duration._
  implicit val timeout = Timeout(5.seconds)
  
  val ans = master ? Epic(List("A1","A2","A3"))
  ans.onComplete { case a => println(a) }
  
//   Thread.sleep(1000)
  master ? Epic(List("B1","B2","B3"))
   ans.onComplete { case a => println(a) }
//  pingActor ! PingActor.Initialize
  // This example app will ping pong 3 times and thereafter terminate the ActorSystem - 
  // see counter logic in PingActor
//  system.awaitTermination()

}