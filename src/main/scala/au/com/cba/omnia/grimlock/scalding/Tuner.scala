// Copyright 2015,2016 Commonwealth Bank of Australia
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

package au.com.cba.omnia.grimlock.scalding

import au.com.cba.omnia.grimlock.framework.TunerParameters

import au.com.cba.omnia.grimlock.scalding.environment._

/** Trait for tuned jobs involving a `Execution`. */
trait Execution extends TunerParameters {
  type P = Execution

  /** The Scalding operating contex. */
  val context: Context
}

/** Companion object to `Execution` trait, defines apply/unapply methods. */
object Execution {
  def apply()(implicit ctx: Context): Execution = new Execution { val context = ctx }

  def unapply(e: Execution): Option[(Context)] = Some(e.context)
}

