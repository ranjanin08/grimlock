// Copyright 2015 Commonwealth Bank of Australia
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

package au.com.cba.omnia.grimlock.framework

sealed trait TunerParameters extends java.io.Serializable {
  type P <: TunerParameters
  def |->[Q <: TunerParameters](parameters: Q): Sequence2[P, Q] = Sequence2(this.asInstanceOf[P], parameters)
}

case object NoParameters extends TunerParameters {
  type P = NoParameters.type
}

case class Reducers(reducers: Int) extends TunerParameters {
  type P = Reducers
}

case class Sequence2[F <: TunerParameters, S <: TunerParameters](first: F, second: S) extends TunerParameters {
  type P = Sequence2[F, S]
}

sealed trait Tuner extends java.io.Serializable {
  type P <: TunerParameters
  val parameters: P
}

case class InMemory[Q <: TunerParameters](parameters: Q = NoParameters) extends Tuner { type P = Q }

object InMemory {
  def apply[F <: TunerParameters, S <: TunerParameters](f: F, s: S): InMemory[Sequence2[F, S]] = {
    InMemory(Sequence2(f, s))
  }
}

case class Default[Q <: TunerParameters](parameters: Q = NoParameters) extends Tuner { type P = Q }

object Default {
  def apply[F <: TunerParameters, S <: TunerParameters](f: F, s: S): Default[Sequence2[F, S]] = {
    Default(Sequence2(f, s))
  }
}

case class Unbalanced[Q <: TunerParameters](parameters: Q) extends Tuner { type P = Q }

object Unbalanced {
  def apply[F <: TunerParameters, S <: TunerParameters](f: F, s: S): Unbalanced[Sequence2[F, S]] = {
    Unbalanced(Sequence2(f, s))
  }
}

