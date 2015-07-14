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

import akka.actor.Actor
import akka.actor.ActorRef
import com.rotium.akka.WorkPullingPattern._
import scala.concurrent.Future
import scala.reflect.ClassTag
import akka.actor.ActorLogging
import scala.util.Success
import scala.util.Failure
import akka.actor.ActorPath

trait Worker[T, U] extends ActorLogging {
  selfActor: Actor ⇒
  implicit val tag: ClassTag[T]

  def masterLocation: ActorPath
  val master = context.actorSelection(masterLocation)

  override def preStart() = {
    master ! WorkerReady
  }

  override def receive: Actor.Receive = {
    case WorkAvailable ⇒
      master ! WorkerReady
    case work: Work[T, U] ⇒
      workRecieved(work)
      implicit val executor = context.dispatcher
      doWork(work.task, work.job, work.requester).andThen {
        case Success(result) ⇒ workCompleteSuccess(work, result)
        case Failure(e) ⇒ workCompleteFailure(work, e)
      }.andThen { case _ =>
        master ! WorkerReady
      }
    case e ⇒ log.warning("Unknown message " + e)
  }

  def doWork(task: T, job: Job[T], requester: ActorRef): Future[U]

  protected def workRecieved(work: Work[T, U]) = {}
  protected def workCompleteSuccess(work: Work[T, U], result: U) = {}
  protected def workCompleteFailure(work: Work[T, U], e: Throwable) = {}
}