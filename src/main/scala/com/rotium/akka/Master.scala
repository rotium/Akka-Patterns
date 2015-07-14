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

import scala.annotation.tailrec
import scala.collection.mutable
import akka.actor._
import com.rotium.akka.WorkPullingPattern._
import scala.language.postfixOps
import scala.reflect.ClassTag

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
trait NotifyWorkComplete extends WorkDone {
  override abstract protected def jobDone(job: Job[MessageType], requester: ActorRef): Unit = {
    requester ! WorkDone(job)
    super.jobDone(job, requester)
  }
}

//noinspection EmptyParenMethodAccessedAsParameterless,SideEffectsInMonadicTransformation
trait Master[T] extends WorkStart with WorkDone with ActorLogging {
  selfActor: Actor ⇒
  type MessageType = T

  val workers = mutable.Set.empty[ActorRef]
  val workersWithJob = mutable.Map.empty[ActorRef, Work[T,_]]
  val jobs: mutable.Queue[(Job[T], ActorRef)] = mutable.Queue.empty
  var currentjob: Option[(Job[T], Iterator[T], ActorRef)] = None

  // Stop the CounterService child if it throws ServiceUnavailable
//  override val supervisorStrategy = OneForOneStrategy() {
//    case _: Exception => println("Restart"); SupervisorStrategy.Restart
//  }
  // override default to kill all children during restart
  override def preRestart(cause: Throwable, msg: Option[Any]): Unit = {}
  
  override def receive: Actor.Receive = {
    case job: Job[T] ⇒
      jobReceived(job)
      jobs += ((job, sender))
      log.info(s"notify workers for now job: $job")
      workers filterNot (worker => workersWithJob contains worker) foreach { w=>
        log.debug(s"notify worker: $w for now job: $job")
        w ! WorkAvailable
      }

    case RegisterWorker(worker) ⇒
      log.info(s"Register Worker ($worker)")
      context.watch(worker)
      workers += worker
      registerWorker(worker)
      //if (jobs.nonEmpty) worker ! WorkAvailable

    case CreateWorkers(n) ⇒
      createWorkers(n)

    case Terminated(worker) ⇒
      onTerminatedWorker(worker)

    case WorkerReady ⇒
      workersWithJob.remove(sender)
      workerReady(sender)

    case Shutdown ⇒
      log.warning("Shutdown myself ")
      if (workersWithJob.isEmpty)
        terminateAll()
      if (workers.isEmpty) {
        log.warning("no more workers - kill myself 1")
        self ! PoisonPill
      }
      context.become {
        case Terminated(worker) ⇒
          log.warning("worker terminated: " + worker)
          onTerminatedWorker(worker)
          if (workers.isEmpty) {
            log.warning("no more workers - kill myself 2")
            self ! PoisonPill
          }

        case WorkerReady ⇒
          log.warning("worker ready: " + sender)
          workersWithJob.remove(sender)
          if (workersWithJob.isEmpty)
            terminateAll()
      }



    case e ⇒
      log.warning("Unknown message " + e)
  }


  private def terminateAll() = {
    workers foreach { _ ! PoisonPill }
  }

  private def onTerminatedWorker(worker: ActorRef) = {
    log.info(s"Unregister Worker ($worker)")
    workers.remove(worker)
    val lostJob = workersWithJob.remove(worker)
    lostJob.foreach { case Work(t, job, sender) ⇒ handleTaskRecover(job, t, sender) }
    unregisterWorker(worker)
  }

  @tailrec
  private def workerReady(sender: ActorRef): Unit = {
    currentjob match {
      case None ⇒
        val nextjob = if (jobs.nonEmpty) Some(jobs.dequeue) else None //First(_ ⇒ true)
        currentjob = nextjob map {
          case (job, jobRequester) ⇒
            jobStarted(job, jobRequester)
            (job, job.t.toIterator, jobRequester)
        }
//        currentjob foreach (_ ⇒ workerReady(sender))
        if (currentjob.isDefined) workerReady(sender)
      case Some((job, jobItr, jobRequester)) ⇒
        if (jobItr.hasNext) {
          val task = jobItr.next()
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
  
  protected def createWorkers[W <: Actor with Worker[T, _]: ClassTag](n: Int): Unit = {
    for (i ← 1 to n) {
      val a = createWorker(i)
      context.self ! RegisterWorker(a)
    }
  }

  protected def createWorker[W <: Actor with Worker[T, _]: ClassTag](id: Int) = {
    context.actorOf(Props[W],"Worker-"+id)
  }

  protected def jobReceived(job: Job[T]) = {}
  override protected def jobStarted(job: Job[T], requester: ActorRef) = {}
  override protected def jobDone(job: Job[T], requester: ActorRef) = {}
  protected def registerWorker(worker: ActorRef) = {}
  protected def unregisterWorker(worker: ActorRef) = {}
  protected def handleTaskRecover(ob: Job[T], task: T, requester: ActorRef): Unit = {}

}
