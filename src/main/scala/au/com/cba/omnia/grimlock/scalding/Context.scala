// Copyright 2016 Commonwealth Bank of Australia
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package au.com.cba.omnia.grimlock.scalding.environment

import au.com.cba.omnia.grimlock.framework.environment.{
  Context => FwContext,
  DistributedData => FwDistributedData,
  Environment => FwEnvironment,
  UserData => FwUserData
}

import cascading.flow.FlowDef

import com.twitter.scalding.{ Config, Mode }
import com.twitter.scalding.typed.{ TypedPipe, ValuePipe }

/**
 * Scalding operating context state.
 *
 * @param flow   The job `FlowDef`.
 * @param mode   The job `Mode`.
 * @param config The job `Config`.
 */
case class Context(flow: FlowDef, mode: Mode, config: Config) extends FwContext {
  implicit val f = flow
  implicit val m = mode
  implicit val c = config
}

/** Companion object to `Context` with additional constructors. */
object Context {
  def apply()(implicit config: Config, flow: FlowDef, mode: Mode): Context = Context(flow, mode, config)
}

trait DistributedData extends FwDistributedData {
  type U[X] = TypedPipe[X]
}

trait UserData extends FwUserData {
  type E[X] = ValuePipe[X]
}

trait Environment extends FwEnvironment {
  type C = Context
}

