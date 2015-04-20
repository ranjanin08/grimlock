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

import au.com.cba.omnia.grimlock.framework.position._

/** Base trait for sampling. */
trait Sampler

/** Base trait for selecting samples. */
trait Select extends SelectWithValue { self: Sampler =>
  type V = Any

  def select[P <: Position](pos: P, ext: V): Boolean = select(pos)

  /**
   * Indicate if the cell is selected as part of the sample.
   *
   * @param pos The position of the cell.
   */
  def select[P <: Position](pos: P): Boolean

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
   * @param pos The position of the cell.
   * @param ext The user define the value.
   */
  def select[P <: Position](pos: P, ext: V): Boolean

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
  def select[P <: Position](pos: P): Boolean = first.select(pos) && second.select(pos)
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
  def select[P <: Position](pos: P, ext: V): Boolean = first.select(pos, ext) && second.select(pos, ext)
}

