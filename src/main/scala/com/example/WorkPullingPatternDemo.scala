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
import com.rotium.akka.Master
import com.rotium.akka.WorkPullingPattern.Job
import com.rotium.akka.Worker
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.PoisonPill
import com.rotium.akka.NotifyWorkDone
import com.rotium.akka.NotifyWorkStart
import com.rotium.akka.WorkPullingPattern.WorkDone
import com.rotium.akka.NotifyWorkStart
import com.rotium.akka.WorkPullingPattern.CreateWorkers
import akka.actor.ActorPath
import akka.actor.SupervisorStrategy.Stop
import akka.actor.OneForOneStrategy
import com.rotium.akka.NotifyWorkStart

class StringMaster extends Actor with Master[String] with NotifyWorkDone with NotifyWorkStart {
  override def createWorker[A: ClassTag, T <: Actor with Worker[A]: ClassTag]() = {
    context.actorOf(Props(new StringWorker(context.self.path)))
  }
  override def unregisterWorker(worker: ActorRef) = {
    if (workers.isEmpty) {
      context.system.shutdown()
    }
  }
  override def jobDone(job: Job[String], requester: ActorRef) = {
    super.jobDone(job, requester)
    if (jobs.isEmpty) {
//      Thread.sleep(1000)
      println("Kill workers")
      workers foreach { _ ! PoisonPill }
    }
  }

  override def handleTaskRecover(job: Job[String], task: String, requester: ActorRef): Unit = {
    log.error(s"Failure task: $task from ($job)")
  }
}
class StringWorker(val masterLocation: ActorPath)(implicit val tag: ClassTag[String]) extends Actor with Worker[String] {

  var c = 0;
  def doWork(task: String, job: Job[String], requester: ActorRef): Future[_] = {
    log.error("doWork=" + task)
    c += 1
    Future {
      if (c % 4 == 0) {
        println(s"! Error in Data($c)=$task")
        throw new Exception("Error in Data=" + task)
      } else {
        log.error(s"Data($c)=$task")
        Thread.sleep(100)
      }
    }
  }
}

case class Dispatch(job: Job[String])
class JobDispatcher extends Actor {
  val master = context.system.actorOf(Props[StringMaster], "StringMaster")
  master ! CreateWorkers[String, StringWorker](1)

  override def receive: Actor.Receive = {
    case Dispatch(job) ⇒ master ! job
    case a             ⇒ println(a)
  }
}

object WorkPullingPatternDemo extends App {

  val system = ActorSystem("MyActorSystem")

    val jobDispatcher = system.actorOf(Props[JobDispatcher], "JobDispatcher")
    jobDispatcher ! Dispatch(Job(List("A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8", "A9")))
    jobDispatcher ! Dispatch(Job(List("B1", "B2", "B3", "B4", "B5", "B6", "B7", "B8", "B9")))

//  import akka.pattern.ask
//  import akka.util.Timeout
//  import scala.concurrent.duration._
//  implicit val timeout = Timeout(10.seconds)
//  val master = system.actorOf(Props[StringMaster], "StringMaster2")
//   master ! CreateWorkers[String, StringWorker](1)
//  val res = master ? Job(List("A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8", "A9"))
//  res.onComplete { println }
}