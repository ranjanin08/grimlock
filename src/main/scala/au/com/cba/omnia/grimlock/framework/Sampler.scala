// Copyright 2014,2015 Commonwealth Bank of Australia
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
trait Sampler[P <: Position] extends SamplerWithValue[P] { self =>
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
  def andThen(that: Sampler[P]) = {
    new Sampler[P] { def select(cell: Cell[P]): Boolean = self.select(cell) && that.select(cell) }
  }
}

/** Base trait for selecting samples with a user provided value. */
trait SamplerWithValue[P <: Position] extends java.io.Serializable { self =>
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
  def andThenWithValue(that: SamplerWithValue[P] { type V >: self.V }) = {
    new SamplerWithValue[P] {
      type V = self.V

      def selectWithValue(cell: Cell[P], ext: V): Boolean = {
        self.selectWithValue(cell, ext) && that.selectWithValue(cell, ext)
      }
    }
  }
}

/** Trait for transforming a type `T` to a `Sampler[P]`. */
trait Sampleable[P <: Position] extends java.io.Serializable {
  /** Returns a `Sampler[P]` for this type `T`. */
  def apply(): Sampler[P]
}

/** Companion object for the `Sampleable` trait. */
object Sampleable {
  /** Converts a `(Cell[P]) => Boolean` to a `Sampler[P]`. */
  implicit def C2S[P <: Position](t: (Cell[P]) => Boolean): Sampleable[P] = {
    new Sampleable[P] { def apply(): Sampler[P] = new Sampler[P] { def select(cell: Cell[P]): Boolean = t(cell) } }
  }

  /** Converts a `Sampler[P]` to a `Sampler[P]`; that is, it is a pass through. */
  implicit def S2S[P <: Position](t: Sampler[P]): Sampleable[P] = {
    new Sampleable[P] { def apply(): Sampler[P] = t }
  }

  /** Converts a `List[Sampler[P]]` to a `Sampler[P]`. */
  implicit def LS2S[P <: Position](t: List[Sampler[P]]): Sampleable[P] = {
    new Sampleable[P] {
      def apply(): Sampler[P] = {
        new Sampler[P] { def select(cell: Cell[P]): Boolean = t.map { case s => s.select(cell) }.reduce(_ || _) }
      }
    }
  }
}

/** Trait for transforming a type `T` to a `SamplerWithValue[P]`. */
trait SampleableWithValue[P <: Position, W] extends java.io.Serializable {
  /** Returns a `SamplerWithValue[P]` for this type `T`. */
  def apply(): SamplerWithValue[P] { type V >: W }
}

/** Companion object for the `SampleableWithValue` trait. */
object SampleableWithValue {
  /** Converts a `(Cell[P], W) => Boolean` to a `SamplerWithValue[P]`. */
  implicit def CW2SWV[P <: Position, W](t: (Cell[P], W) => Boolean): SampleableWithValue[P, W] = {
    new SampleableWithValue[P, W] {
      def apply(): SamplerWithValue[P] { type V >: W } = {
        new SamplerWithValue[P] {
          type V = W

          def selectWithValue(cell: Cell[P], ext: V): Boolean = t(cell, ext)
        }
      }
    }
  }

  /** Converts a `SamplerWithValue[P]` to a `SamplerWithValue[P]`; that is, it is a pass through. */
  implicit def SWV2SWV[P <: Position, W](t: SamplerWithValue[P] { type V >: W }): SampleableWithValue[P, W] = {
    new SampleableWithValue[P, W] { def apply(): SamplerWithValue[P] { type V >: W } = t }
  }

  /** Converts a `List[SamplerWithValue[P] { type V >: W }]` to a `SamplerWithValue[P] { type V >: W }`. */
  implicit def LSWV2SWV[P <: Position, W](t: List[SamplerWithValue[P] { type V >: W }]): SampleableWithValue[P, W] = {
    new SampleableWithValue[P, W] {
      def apply(): SamplerWithValue[P] { type V >: W } = {
        new SamplerWithValue[P] {
          type V = W

          def selectWithValue(cell: Cell[P], ext: V): Boolean = {
            t.map { case s => s.selectWithValue(cell, ext) }.reduce(_ || _)
          }
        }
      }
    }
  }
}

