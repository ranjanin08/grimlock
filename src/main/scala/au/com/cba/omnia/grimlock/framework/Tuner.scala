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

/** Base trait for tuner parameters. */
trait TunerParameters extends java.io.Serializable {
  /** Type of these parameters. */
  type P <: TunerParameters

  /**
   * Create a sequence of tune operations.
   *
   * @param parameters The operations (parameters) to perform after this.
   *
   * @return A sequence of `this` followed by `parameters`.
   */
  def |->[Q <: TunerParameters](parameters: Q): Sequence2[P, Q] = Sequence2(this.asInstanceOf[P], parameters)
}

/** Indicates that the data should be collated. */
case object Collate extends TunerParameters {
  type P = Collate.type
}

/** Indicates that no special operations are to be performed. */
case object NoParameters extends TunerParameters {
  type P = NoParameters.type
}

/**
 * Tune the number of reducers.
 *
 * @param reducers The number of reducers to use.
 */
case class Reducers(reducers: Int) extends TunerParameters {
  type P = Reducers
}

/**
 * Redistribute the data across a number of partitions.
 *
 * @param partitions The number of partitions to redistribute among.
 */
case class Redistribute(partitions: Int) extends TunerParameters {
  type P = Redistribute
}

/**
 * Create a sequence of two tuner parameters
 *
 * @param first  The first parameters of the sequence.
 * @param second The second parameters of the sequence.
 */
case class Sequence2[F <: TunerParameters, S <: TunerParameters](first: F, second: S) extends TunerParameters {
  type P = Sequence2[F, S]
}

/** Base trait that indicates size/shape of the data for tuning. */
sealed trait Tuner extends java.io.Serializable {
  /** Type of the parameters used with this tuner. */
  type P <: TunerParameters

  /** The parameters used for tuning. */
  val parameters: P
}

/** Indicates that some of the data can fit in memory (permits map-side only operations). */
case class InMemory[Q <: TunerParameters](parameters: Q = NoParameters) extends Tuner { type P = Q }

/** Indicates that the data is (reasonably) evenly distributed. */
case class Default[Q <: TunerParameters](parameters: Q = NoParameters) extends Tuner { type P = Q }

/** Companion object to `Default`. */
object Default {
  /**
   * Create a tuner with a sequence of two tuner parameters.
   *
   * @param first  The first parameters of the sequence.
   * @param second The second parameters of the sequence.
   */
  def apply[F <: TunerParameters, S <: TunerParameters](first: F, second: S): Default[Sequence2[F, S]] = {
    Default(Sequence2(first, second))
  }
}

/** Indicates that the data is (heavily) skewed. */
case class Unbalanced[Q <: TunerParameters](parameters: Q) extends Tuner { type P = Q }

/** Companion object to `Unbalanced`. */
object Unbalanced {
  /**
   * Create an unbalanced tuner with a sequence of two tuner parameters.
   *
   * @param first  The first parameters of the sequence.
   * @param second The second parameters of the sequence.
   */
  def apply[F <: TunerParameters, S <: TunerParameters](first: F, second: S): Unbalanced[Sequence2[F, S]] = {
    Unbalanced(Sequence2(first, second))
  }
}

