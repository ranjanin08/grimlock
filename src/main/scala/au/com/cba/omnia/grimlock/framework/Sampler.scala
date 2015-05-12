// Copyright 2014-2015 Commonwealth Bank of Australia
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

package au.com.cba.omnia.grimlock.framework.sample

import au.com.cba.omnia.grimlock.framework._
import au.com.cba.omnia.grimlock.framework.position._

/** Base trait for sampling. */
trait Sampler

/** Base trait for selecting samples. */
trait Select extends SelectWithValue { self: Sampler =>
  type V = Any

  def select[P <: Position](cell: Cell[P], ext: V): Boolean = select(cell)

  /**
   * Indicate if the cell is selected as part of the sample.
   *
   * @param cell The cell.
   */
  def select[P <: Position](cell: Cell[P]): Boolean

  /**
   * Operator for chaining sampling.
   *
   * @param that The sampling to perform after `this`.
   *
   * @return A sampler that runs `this` and then `that`.
   */
  def andThen(that: Sampler with Select): AndThenSampler = AndThenSampler(this, that)
}

/** Base trait for selecting samples with a user provided value. */
trait SelectWithValue { self: Sampler =>
  /** Type of the external value. */
  type V

  /**
   * Indicate if the cell is selected as part of the sample.
   *
   * @param cell The cell.
   * @param ext  The user define the value.
   */
  def select[P <: Position](cell: Cell[P], ext: V): Boolean

  /**
   * Operator for chaining sampling.
   *
   * @param that The sampling to perform after `this`.
   *
   * @return A sampler that runs `this` and then `that`.
   */
  def andThen[W <: V](that: Sampler with SelectWithValue { type V = W }): AndThenSamplerWithValue[W] = {
    AndThenSamplerWithValue[W](this, that)
  }
}

/**
 * Sampler that is a composition of two samplers with `Select`.
 *
 * @param first  The first sampling to appy.
 * @param second The second sampling to appy.
 *
 * @note This need not be called in an application. The `andThen` method will create it.
 */
case class AndThenSampler(first: Sampler with Select, second: Sampler with Select) extends Sampler with Select {
  def select[P <: Position](cell: Cell[P]): Boolean = first.select(cell) && second.select(cell)
}

/**
 * Sampler that is a composition of two samplers with `SelectWithValue`.
 *
 * @param first  The first sampling to appy.
 * @param second The second sampling to appy.
 *
 * @note This need not be called in an application. The `andThen` method will create it.
 */
case class AndThenSamplerWithValue[W](first: Sampler with SelectWithValue { type V >: W },
  second: Sampler with SelectWithValue { type V >: W }) extends Sampler with SelectWithValue {
  type V = W
  def select[P <: Position](cell: Cell[P], ext: V): Boolean = first.select(cell, ext) && second.select(cell, ext)
}

/**
 * Sampler that is a combination of one or more samplers with `Select`.
 *
 * @param singles `List` of samplers that are combined together.
 *
 * @note This need not be called in an application. The `Sampleable` type class will convert any `List[Sampler]`
 *       automatically to one of these.
 */
// TODO: Test this
case class CombinationSampler[T <: Sampler with Select](singles: List[T]) extends Sampler with Select {
  def select[P <: Position](cell: Cell[P]): Boolean = singles.map { case s => s.select(cell) }.reduce(_ || _)
}

/**
 * Sampler that is a combination of one or more samplers with `SelectWithValue`.
 *
 * @param singles `List` of samplers that are combined together.
 *
 * @note This need not be called in an application. The `SampleableWithValue` type class will convert any
 *       `List[Sampler]` automatically to one of these.
 */
// TODO: Test this
case class CombinationSamplerWithValue[T <: Sampler with SelectWithValue { type V >: W }, W](singles: List[T])
  extends Sampler with SelectWithValue {
  type V = W
  def select[P <: Position](cell: Cell[P], ext: V): Boolean = {
    singles.map { case s => s.select(cell, ext) }.reduce(_ || _)
  }
}

/** Type class for transforming a type `T` to a `Sampler with Select`. */
trait Sampleable[T] {
  /**
   * Returns a `Sampler with Select` for type `T`.
   *
   * @param t Object that can be converted to a `Sampler with Select`.
   */
  def convert(t: T): Sampler with Select
}

/** Companion object for the `Sampleable` type class. */
object Sampleable {
  /**
   * Converts a `List[Sampler with Select]` to a single `Sampler with Select` using `CombinationSampler`.
   */
  implicit def LS2S[T <: Sampler with Select]: Sampleable[List[T]] = {
    new Sampleable[List[T]] { def convert(t: List[T]): Sampler with Select = CombinationSampler(t) }
  }

  /** Converts a `Sampler with Select` to a `Sampler with Select`; that is, it is a pass through. */
  implicit def S2S[T <: Sampler with Select]: Sampleable[T] = {
    new Sampleable[T] { def convert(t: T): Sampler with Select = t }
  }
}

/** Type class for transforming a type `T` to a `Sampler with SelectWithValue`. */
trait SampleableWithValue[T, W] {
  /**
   * Returns a `Sampler with SelectWithValue` for type `T`.
   *
   * @param t Object that can be converted to a `Sampler with SelectWithValue`.
   */
  def convert(t: T): Sampler with SelectWithValue { type V >: W }
}

/** Companion object for the `SampleableWithValue` type class. */
object SampleableWithValue {
  /**
   * Converts a `List[Sampler with SelectWithValue]` to a single `Sampler with SelectWithValue` using
   * `CombinationSamplerWithValue`.
   */
  implicit def LS2SWV[T <: Sampler with SelectWithValue { type V >: W }, W]: SampleableWithValue[List[T], W] = {
    new SampleableWithValue[List[T], W] {
      def convert(t: List[T]): Sampler with SelectWithValue { type V >: W } = {
        CombinationSamplerWithValue[T, W](t)
      }
    }
  }

  /**
   * Converts a `Sampler with SelectWithValue` to a `Sampler with SelectWithValue`; that is, it is a pass
   * through.
   */
  implicit def S2SWV[T <: Sampler with SelectWithValue { type V >: W }, W]: SampleableWithValue[T, W] = {
    new SampleableWithValue[T, W] { def convert(t: T): Sampler with SelectWithValue { type V >: W } = t }
  }
}

