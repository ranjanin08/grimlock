// Copyright 2014,2015,2016 Commonwealth Bank of Australia
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

package commbank.grimlock.framework.sample

import commbank.grimlock.framework._

import shapeless.Nat

/** Base trait for sampling. */
trait Sampler[P <: Nat] extends SamplerWithValue[P] { self =>
  type V = Any

  def selectWithValue(cell: Cell[P], ext: V): Boolean = select(cell)

  /**
   * Indicate if the cell is selected as part of the sample.
   *
   * @param cell The cell.
   */
  def select(cell: Cell[P]): Boolean

  /**
   * Operator for chaining sampling.
   *
   * @param that The sampling to perform after `this`.
   *
   * @return A sampler that runs `this` and then `that`.
   */
  def andThen(that: Sampler[P]) = new Sampler[P] {
    def select(cell: Cell[P]): Boolean = self.select(cell) && that.select(cell)
  }
}

/** Companion object for the `Sampler` trait. */
object Sampler {
  /** Converts a `(Cell[P]) => Boolean` to a `Sampler[P]`. */
  implicit def funcToSampler[P <: Nat](func: (Cell[P]) => Boolean) = new Sampler[P] {
    def select(cell: Cell[P]): Boolean = func(cell)
  }

  /** Converts a `List[Sampler[P]]` to a `Sampler[P]`. */
  implicit def listToSamples[P <: Nat](samplers: List[Sampler[P]]) = new Sampler[P] {
    def select(cell: Cell[P]): Boolean = samplers.map(_.select(cell)).reduce(_ || _)
  }
}

/** Base trait for selecting samples with a user provided value. */
trait SamplerWithValue[P <: Nat] extends java.io.Serializable { self =>
  /** Type of the external value. */
  type V

  /**
   * Indicate if the cell is selected as part of the sample.
   *
   * @param cell The cell.
   * @param ext  The user define the value.
   */
  def selectWithValue(cell: Cell[P], ext: V): Boolean

  /**
   * Operator for chaining sampling.
   *
   * @param that The sampling to perform after `this`.
   *
   * @return A sampler that runs `this` and then `that`.
   */
  def andThenWithValue(that: SamplerWithValue[P] { type V >: self.V }) = new SamplerWithValue[P] {
    type V = self.V

    def selectWithValue(cell: Cell[P], ext: V): Boolean =
      self.selectWithValue(cell, ext) && that.selectWithValue(cell, ext)
  }
}

/** Companion object for the `SamplerWithValue` trait. */
object SamplerWithValue {
  /** Converts a `(Cell[P], W) => Boolean` to a `SamplerWithValue[P]`. */
  implicit def funcToSamplerWithValue[P <: Nat, W](func: (Cell[P], W) => Boolean) = new SamplerWithValue[P] {
    type V = W

    def selectWithValue(cell: Cell[P], ext: V): Boolean = func(cell, ext)
  }

  /** Converts a `List[SamplerWithValue[P] { type V >: W }]` to a `SamplerWithValue[P] { type V >: W }`. */
  implicit def listToSamplerWithValue[
    P <: Nat,
    W
  ](
    samplers: List[SamplerWithValue[P] { type V >: W }]
  ) = new SamplerWithValue[P] {
    type V = W

    def selectWithValue(cell: Cell[P], ext: V): Boolean = samplers.map(_.selectWithValue(cell, ext)).reduce(_ || _)
  }
}

