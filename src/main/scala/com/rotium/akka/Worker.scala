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

trait Worker[T] extends ActorLogging {
  selfActor: Actor ⇒
  implicit val tag: ClassTag[T]
  def master: ActorRef

  override def preStart {
    master ! WorkerReady
  }

  override def receive: Actor.Receive = {
    case WorkAvailable ⇒
      master ! WorkerReady
    case Work(work: T) ⇒
      doWork(work).onComplete { case _ ⇒ master ! WorkerReady }(context.dispatcher)
    case e ⇒ log.warning("Unknown message " + e)
  }

  def doWork(work: T): Future[_]
}