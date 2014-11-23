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

import akka.actor.ActorRef
import scala.reflect.ClassTag
import akka.actor.Actor

/**
 * @author rotem
 *
 */
object WorkPullingPattern {
  sealed trait Message
  case class Job[T](t: Traversable[T])
  
  case object WorkerReady extends Message
  case object WorkAvailable extends Message
  case class WorkStarted[T](job: Job[T]) extends Message
  case class WorkDone[T](job: Job[T]) extends Message
  case class RegisterWorker(worker: ActorRef) extends Message
  case class Work[T,U](task: T, job: Job[T], requester: ActorRef) extends Message
  
  case class CreateWorkers[A: ClassTag, U, T <: Actor with Worker[A, U]: ClassTag](n: Int)
}
