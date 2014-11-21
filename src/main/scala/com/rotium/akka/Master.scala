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

case class CreateWorkers[A: ClassTag, T <: Actor with Worker[A]: ClassTag](n: Int)

trait WorkStart {
  type MessageType
  protected def epicStarted(epic: Epic[MessageType], requester: ActorRef)
}
trait NotifyWorkStart extends WorkStart {
  override abstract protected def epicStarted(epic: Epic[MessageType], requester: ActorRef) = {
    requester ! WorkStarted(epic)
    super.epicStarted(epic, requester)
  }
}
trait WorkDone {
  type MessageType
  protected def epicDone(epic: Epic[MessageType], requester: ActorRef): Unit
}
trait NotifyWorkDone extends WorkDone {
  override abstract protected def epicDone(epic: Epic[MessageType], requester: ActorRef): Unit = {
    requester ! WorkDone(epic)
    super.epicDone(epic, requester)
  }
}

trait Master[T] extends WorkStart with WorkDone with ActorLogging {
  selfActor: Actor ⇒
  type MessageType = T
  
  val workers = mutable.Set.empty[ActorRef]
  val epics: mutable.Queue[(Epic[T], ActorRef)] = mutable.Queue.empty
  var currentEpic: Option[(Epic[T], ActorRef)] = None
  var currentEpicItr: Option[Iterator[T]] = None

  override def receive: Actor.Receive = {
    case epic: Epic[T] ⇒
      epicReceived(epic)
      epics += ((epic, sender))
      workers foreach { _ ! WorkAvailable }

    case RegisterWorker(worker) ⇒
      context.watch(worker)
      workers += worker
      registerWorker(worker)
      worker ! WorkAvailable

    case c @ CreateWorkers(n) ⇒
      createWorkers(n)

    case Terminated(worker) ⇒
      workers.remove(worker)
      unregisterWorker(worker)

    case WorkerReady ⇒
      workerReady(sender)

    case e ⇒
      log.warning("Unknown message " + e)
  }

  private def workerReady(sender: ActorRef): Unit = {
    currentEpicItr match {
      case None ⇒
        currentEpic = epics.dequeueFirst(_ ⇒ true)
        currentEpicItr = currentEpic.map(_._1.t.toIterator)
        currentEpicItr foreach { e ⇒
          epicStarted(currentEpic.get._1, currentEpic.get._2)
          //          currentEpic.get._2 ! WorkStarted(currentEpic.get._1)
          workerReady(sender)
        }

      case Some(iter) ⇒
        if (iter.hasNext) {
          val i = iter.next
          val w = Work(i)
          sender ! w
        } else {
          epicDone(currentEpic.get._1, currentEpic.get._2)
          //          currentEpic.get._2 ! WorkDone(currentEpic.get._1)
          currentEpicItr = None
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

  def createWorker[A: ClassTag, T <: Actor with Worker[A]: ClassTag]() = {
    context.actorOf(Props[T])
  }

  protected def epicReceived(epic: Epic[T]) = {}
  override protected def epicStarted(epic: Epic[T], requester: ActorRef) = {}
  override protected def epicDone(epic: Epic[T], requester: ActorRef) = {}
  protected def registerWorker(worker: ActorRef) = {}
  protected def unregisterWorker(worker: ActorRef) = {}

}
