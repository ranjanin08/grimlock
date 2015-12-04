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

package au.com.cba.omnia.grimlock.framework.squash

import au.com.cba.omnia.grimlock.framework._
import au.com.cba.omnia.grimlock.framework.content._
import au.com.cba.omnia.grimlock.framework.position._

/** Base trait for squashing a dimension. */
trait Squasher[P <: Position] extends SquasherWithValue[P] {
  type V = Any

  def prepareWithValue(cell: Cell[P], dim: Dimension, ext: V): T = prepare(cell, dim)
  def presentWithValue(t: T, ext: V): Option[Content] = present(t)

  /**
   * Prepare for squashing.
   *
   * @param cell Cell which is to be squashed.
   * @param dim  The dimension being squashed.
   *
   * @return State to reduce.
   */
  def prepare(cell: Cell[P], dim: Dimension): T

  /**
   * Present the squashed content.
   *
   * @param t The reduced state.
   *
   * @return The squashed content.
   */
  def present(t: T): Option[Content]
}

/** Base trait for squashing a dimension with a user provided value. */
trait SquasherWithValue[P <: Position] extends java.io.Serializable {
  /** Type of the state being squashed. */
  type T

  /** Type of the external value. */
  type V

  /**
   * Prepare for squashing.
   *
   * @param cell Cell which is to be squashed.
   * @param dim  The dimension being squashed.
   * @param ext  User provided data required for preparation.
   *
   * @return State to reduce.
   */
  def prepareWithValue(cell: Cell[P], dim: Dimension, ext: V): T

  /**
   * Standard reduce method.
   *
   * @param lt The left state to reduce.
   * @param rt The right state to reduce.
   */
  def reduce(lt: T, rt: T): T

  /**
   * Present the squashed content.
   *
   * @param t   The reduced state.
   * @param ext User provided data required for presentation.
   *
   * @return The squashed content.
   */
  def presentWithValue(t: T, ext: V): Option[Content]
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
  /** Converts a `SquasherWithValue[P]` to a `SquasherWithValue[P]`; that is, it is a pass through. */
  implicit def S2SWV[P <: Position, T <: SquasherWithValue[P] { type V >: W }, W]: SquashableWithValue[T, P, W] = {
    new SquashableWithValue[T, P, W] { def convert(t: T): SquasherWithValue[P] { type V >: W } = t }
  }
}

