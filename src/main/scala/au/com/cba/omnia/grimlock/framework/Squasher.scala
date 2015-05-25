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

package au.com.cba.omnia.grimlock.framework.squash

import au.com.cba.omnia.grimlock.framework._
import au.com.cba.omnia.grimlock.framework.position._

/** Base trait for squashing a dimension. */
trait Squasher[P <: Position] extends SquasherWithValue[P] {
  type V = Any

  def reduceWithValue(dim: Dimension, x: Cell[P], y: Cell[P], ext: V): Cell[P] = reduce(dim, x, y)

  /**
   * Reduce two cells.
   *
   * @param dim The dimension along which to squash.
   * @param x   The first cell to reduce.
   * @param y   The second cell to reduce.
   */
  def reduce(dim: Dimension, x: Cell[P], y: Cell[P]): Cell[P]
}

/** Base trait for squashing a dimension with a user provided value. */
trait SquasherWithValue[P <: Position] extends java.io.Serializable {
  /** Type of the external value. */
  type V

  /**
   * Reduce two cells with a user supplied value.
   *
   * @param dim The dimension along which to squash.
   * @param x   The first cell to reduce.
   * @param y   The second cell to reduce.
   * @param ext The user define the value.
   */
  def reduceWithValue(dim: Dimension, x: Cell[P], y: Cell[P], ext: V): Cell[P]
}

/** Type class for transforming a type `T` to a `Squasher[P]`. */
trait Squashable[T, P <: Position] {
  /**
   * Returns a `Squasher[P]` for type `T`.
   *
   * @param t Object that can be converted to a `Squasher[P]`.
   */
  def convert(t: T): Squasher[P]
}

/** Companion object for the `Squashable` type class. */
object Squashable {
  /** Converts a `(Dimension, Cell[P], Cell[P]) => Cell[P]` to a `Squasher[P]`. */
  implicit def DCC2S[P <: Position]: Squashable[(Dimension, Cell[P], Cell[P]) => Cell[P], P] = {
    new Squashable[(Dimension, Cell[P], Cell[P]) => Cell[P], P] {
      def convert(t: (Dimension, Cell[P], Cell[P]) => Cell[P]): Squasher[P] = {
        new Squasher[P] { def reduce(dim: Dimension, x: Cell[P], y: Cell[P]): Cell[P] = t(dim, x, y) }
      }
    }
  }

  /** Converts a `Squasher[P]` to a `Squasher[P]`; that is, it is a pass through. */
  implicit def S2S[P <: Position, T <: Squasher[P]]: Squashable[T, P] = {
    new Squashable[T, P] { def convert(t: T): Squasher[P] = t }
  }
}

/** Type class for transforming a type `T` to a `SquasherWithValue[P]`. */
trait SquashableWithValue[T, P <: Position, W] {
  /**
   * Returns a `SquasherWithValue[P]` for type `T`.
   *
   * @param t Object that can be converted to a `SquasherWithValue[P]`.
   */
  def convert(t: T): SquasherWithValue[P] { type V >: W }
}

/** Companion object for the `SquashableWithValue` type class. */
object SquashableWithValue {
  /** Converts a `(Dimension, Cell[P], Cell[P], W) => Cell[P]` to a `SquasherWithValue[P]`. */
  implicit def DCCW2SWV[P <: Position, W]: SquashableWithValue[(Dimension, Cell[P], Cell[P], W) => Cell[P], P, W] = {
    new SquashableWithValue[(Dimension, Cell[P], Cell[P], W) => Cell[P], P, W] {
      def convert(t: (Dimension, Cell[P], Cell[P], W) => Cell[P]): SquasherWithValue[P] { type V >: W } = {
        new SquasherWithValue[P] {
          type V = W

          def reduceWithValue(dim: Dimension, x: Cell[P], y: Cell[P], ext: W): Cell[P] = t(dim, x, y, ext)
        }
      }
    }
  }

  /** Converts a `SquasherWithValue[P]` to a `SquasherWithValue[P]`; that is, it is a pass through. */
  implicit def S2SWV[P <: Position, T <: SquasherWithValue[P] { type V >: W }, W]: SquashableWithValue[T, P, W] = {
    new SquashableWithValue[T, P, W] { def convert(t: T): SquasherWithValue[P] { type V >: W } = t }
  }
}

