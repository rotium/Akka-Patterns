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

package com.rotium.akka

import scala.collection.mutable
import akka.actor.Actor
import akka.actor.ActorRef
import com.rotium.akka.WorkPullingPattern._
import akka.actor.Terminated
import akka.actor.ActorLogging
import scala.reflect.ClassTag
import akka.actor.Props
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy

trait WorkStart {
  type MessageType
  protected def jobStarted(job: Job[MessageType], requester: ActorRef)
}
trait NotifyWorkStart extends WorkStart {
  override abstract protected def jobStarted(job: Job[MessageType], requester: ActorRef) = {
    requester ! WorkStarted(job)
    super.jobStarted(job, requester)
  }
}
trait WorkDone {
  type MessageType
  protected def jobDone(job: Job[MessageType], requester: ActorRef): Unit
}
trait NotifyWorkDone extends WorkDone {
  override abstract protected def jobDone(job: Job[MessageType], requester: ActorRef): Unit = {
    requester ! WorkDone(job)
    super.jobDone(job, requester)
  }
}

trait Master[T] extends WorkStart with WorkDone with ActorLogging {
  selfActor: Actor ⇒
  type MessageType = T

  val workers = mutable.Set.empty[ActorRef]
  val workersWithJob = mutable.Map.empty[ActorRef, Work[T]]
  val jobs: mutable.Queue[(Job[T], ActorRef)] = mutable.Queue.empty
  var currentjob: Option[(Job[T], Iterator[T], ActorRef)] = None

  // Stop the CounterService child if it throws ServiceUnavailable
//  override val supervisorStrategy = OneForOneStrategy() {
//    case _: Exception => println("Restart"); SupervisorStrategy.Restart
//  }
  // override default to kill all children during restart
  override def preRestart(cause: Throwable, msg: Option[Any]) {}
  
  override def receive: Actor.Receive = {
    case job: Job[T] ⇒
      jobReceived(job)
      jobs += ((job, sender))
      workers filterNot (workersWithJob contains) foreach { w=>
      println(s"notify workers: $w for now job: $job")
      w ! WorkAvailable }

    case RegisterWorker(worker) ⇒
    println(s"RegisterWorker($worker)")
      context.watch(worker)
      workers += worker
      registerWorker(worker)
      if (!jobs.isEmpty) worker ! WorkAvailable

    case CreateWorkers(n) ⇒
      createWorkers(n)

    case Terminated(worker) ⇒
      workers.remove(worker)
      val lostJob = workersWithJob.remove(worker)
      lostJob.foreach { case Work(t, job, sender) ⇒ handleTaskRecover(job, t, sender) }
      unregisterWorker(worker)

    case WorkerReady ⇒
      workersWithJob.remove(sender)
      workerReady(sender)

    case e ⇒
      log.warning("Unknown message " + e)
  }

  private def workerReady(sender: ActorRef): Unit = {
    currentjob match {
      case None ⇒
        val nextjob = jobs.dequeueFirst(_ ⇒ true)
        currentjob = nextjob map {
          case (job, jobRequester) ⇒
            jobStarted(job, jobRequester)
            (job, job.t.toIterator, jobRequester)
        }
        currentjob foreach (_ ⇒ workerReady(sender))
      case Some((job, jobItr, jobRequester)) ⇒
        if (jobItr.hasNext) {
          val task = jobItr.next
          val w = Work(task, job, jobRequester)
          workersWithJob += (sender -> w)
          sender ! w
        } else {
          jobDone(job, jobRequester)
          currentjob = None
          //          jobRequester ! WorkDone(job)
          workerReady(sender)
        }
    }
  }
  
  protected def createWorkers[A: ClassTag, T <: Actor with Worker[A]: ClassTag](n: Int): Unit = {
    for (i ← 1 to n) {
      val a = createWorker
      context.self ! RegisterWorker(a)
    }
  }

  protected def createWorker[A: ClassTag, T <: Actor with Worker[A]: ClassTag]() = {
    context.actorOf(Props[T])
  }

  protected def jobReceived(job: Job[T]) = {}
  override protected def jobStarted(job: Job[T], requester: ActorRef) = {}
  override protected def jobDone(job: Job[T], requester: ActorRef) = {}
  protected def registerWorker(worker: ActorRef) = {}
  protected def unregisterWorker(worker: ActorRef) = {}
  protected def handleTaskRecover(ob: Job[T], task: T, requester: ActorRef): Unit = {}

}
