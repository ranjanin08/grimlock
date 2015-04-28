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

package au.com.cba.omnia.grimlock.framework.window

import au.com.cba.omnia.grimlock.framework._
import au.com.cba.omnia.grimlock.framework.content._
import au.com.cba.omnia.grimlock.framework.position._
import au.com.cba.omnia.grimlock.framework.utility._

/**
 * Base trait for generating windowed data.
 *
 * Windowed data is derived from two or more values, for example deltas or gradients. To generate this, the process is
 * as follows. First the matrix is grouped according to a slice. The data in each group is then sorted by the remaining
 * coordinates. The first cell of each group is passed to the prepare method. This allows a windower to initialise it's
 * running state. All subsequent cells are passed to the present method (together with the running state). The present
 * method can update the running state, and optionally return one or more cells with derived data. Note that the
 * running state can be used to create derived features of different window sizes.
 */
trait Windower {
  /** Type of the state. */
  type T
}

/** Base trait for initialising a windower. */
trait Initialise extends InitialiseWithValue { self: Windower =>
  type V = Any

  def initialise[P <: Position, D <: Dimension](slice: Slice[P, D], ext: V)(cell: Cell[slice.S], rem: slice.R): T = {
    initialise(slice)(cell, rem)
  }

  def present[P <: Position, D <: Dimension](slice: Slice[P, D], ext: V)(cell: Cell[slice.S], rem: slice.R,
    t: T): (T, Collection[Cell[slice.S#M]]) = present(slice)(cell, rem, t)

  /**
   * Initialise the state using the first cell (ordered according to its position).
   *
   * @param slice Encapsulates the dimension(s) along which to derive.
   * @param cell  The cell to initialise with.
   * @param rem   The remaining coordinates of the cell.
   *
   * @return The state for this object.
   */
  def initialise[P <: Position, D <: Dimension](slice: Slice[P, D])(cell: Cell[slice.S], rem: slice.R): T

  /**
   * Update state with the current cell and, optionally, output derived data.
   *
   * @param slice Encapsulates the dimension(s) along which to derive.
   * @param cell  The selected cell to derive from.
   * @param rem   The remaining coordinates of the cell.
   * @param t     The state.
   *
   * @return A tuple consisting of updated state together with optional derived data.
   */
  def present[P <: Position, D <: Dimension](slice: Slice[P, D])(cell: Cell[slice.S], rem: slice.R,
    t: T): (T, Collection[Cell[slice.S#M]])
}

/** Base trait for initialising a windower with a user supplied value. */
trait InitialiseWithValue { self: Windower =>
  /** Type of the external value. */
  type V

  /**
   * Initialise the state using the first cell (ordered according to its position).
   *
   * @param slice Encapsulates the dimension(s) along which to derive.
   * @param ext   The user define the value.
   * @param cell  The cell to initialise with.
   * @param rem   The remaining coordinates of the cell.
   *
   * @return The state for this object.
   */
  def initialise[P <: Position, D <: Dimension](slice: Slice[P, D], ext: V)(cell: Cell[slice.S], rem: slice.R): T

  /**
   * Update state with the current cell and, optionally, output derived data.
   *
   * @param slice Encapsulates the dimension(s) along which to derive.
   * @param ext   The user define the value.
   * @param cell  The selected cell to derive from.
   * @param rem   The remaining coordinates of the cell.
   * @param t     The state.
   *
   * @return A tuple consisting of updated state together with optional derived data.
   */
  def present[P <: Position, D <: Dimension](slice: Slice[P, D], ext: V)(cell: Cell[slice.S], rem: slice.R,
    t: T): (T, Collection[Cell[slice.S#M]])
}

/**
 * Windower that is a combination of one or more windowers with `Initialise`.
 *
 * @param singles `List` of windowers that are combined together.
 *
 * @note This need not be called in an application. The `Windowable` type class will convert any `List[Windower]`
 *       automatically to one of these.
 */
case class CombinationWindower[T <: Windower with Initialise](singles: List[T]) extends Windower with Initialise {
  type T = List[Any]

  def initialise[P <: Position, D <: Dimension](slice: Slice[P, D])(cell: Cell[slice.S], rem: slice.R): T = {
    singles.map { case windower => windower.initialise(slice)(cell, rem) }
  }

  def present[P <: Position, D <: Dimension](slice: Slice[P, D])(cell: Cell[slice.S], rem: slice.R,
    t: T): (T, Collection[Cell[slice.S#M]]) = {
    val state = (singles, t)
      .zipped
      .map { case (windower, s) => windower.present(slice)(cell, rem, s.asInstanceOf[windower.T]) }

    (state.map { case (t, c) => t }, Collection(state.flatMap { case (t, c) => c.toList }))
  }
}

/**
 * Windower that is a combination of one or more windowers with `InitialiseWithValue`.
 *
 * @param singles `List` of windowers that are combined together.
 *
 * @note This need not be called in an application. The `WindowableWithValue` type class will convert any
 *       `List[Windower]` automatically to one of these.
 */
case class CombinationWindowerWithValue[T <: Windower with InitialiseWithValue { type V >: W }, W](singles: List[T])
  extends Windower with InitialiseWithValue {
  type T = List[Any]
  type V = W

  def initialise[P <: Position, D <: Dimension](slice: Slice[P, D], ext: V)(cell: Cell[slice.S], rem: slice.R): T = {
    singles.map { case windower => windower.initialise(slice, ext)(cell, rem) }
  }

  def present[P <: Position, D <: Dimension](slice: Slice[P, D], ext: V)(cell: Cell[slice.S], rem: slice.R,
    t: T): (T, Collection[Cell[slice.S#M]]) = {
    val state = (singles, t)
      .zipped
      .map { case (windower, s) => windower.present(slice, ext)(cell, rem, s.asInstanceOf[windower.T]) }

    (state.map { case (t, c) => t }, Collection(state.flatMap { case (t, c) => c.toList }))
  }
}

/** Type class for transforming a type `T` to a `Windower with Initialise`. */
trait Windowable[T] {
  /**
   * Returns a `Windower with Initialise` for type `T`.
   *
   * @param t Object that can be converted to a `Windower with Initialise`.
   */
  def convert(t: T): Windower with Initialise
}

/** Companion object for the `Windowable` type class. */
object Windowable {
  /** Converts a `List[Windower with Initialise]` to a single `Windower with Initialise` using `CombinationWindower`. */
  implicit def LD2D[T <: Windower with Initialise]: Windowable[List[T]] = {
    new Windowable[List[T]] { def convert(t: List[T]): Windower with Initialise = CombinationWindower(t) }
  }
  /** Converts a `Windower with Initialise` to a `Windower with Initialise`; that is, it is a pass through. */
  implicit def D2D[T <: Windower with Initialise]: Windowable[T] = {
    new Windowable[T] { def convert(t: T): Windower with Initialise = t }
  }
}

/** Type class for transforming a type `T` to a `Windower with InitialiseWithValue`. */
trait WindowableWithValue[T, W] {
  /**
   * Returns a `Windower with InitialiseWithValue` for type `T`.
   *
   * @param t Object that can be converted to a `Windower with InitialiseWithValue`.
   */
  def convert(t: T): Windower with InitialiseWithValue { type V >: W }
}

/** Companion object for the `WindowableWithValue` type class. */
object WindowableWithValue {
  /**
   * Converts a `List[Windower with InitialiseWithValue]` to a single `Windower with InitialiseWithValue` using
   * `CombinationWindowerWithValue`.
   */
  implicit def DT2DWV[T <: Windower with InitialiseWithValue { type V >: W }, W]: WindowableWithValue[List[T], W] = {
    new WindowableWithValue[List[T], W] {
      def convert(t: List[T]): Windower with InitialiseWithValue { type V >: W } = CombinationWindowerWithValue[T, W](t)
    }
  }
  /**
   * Converts a `Windower with InitialiseWithValue` to a `Windower with InitialiseWithValue`; that is, it is a pass
   * through.
   */
  implicit def D2DWV[T <: Windower with InitialiseWithValue { type V >: W }, W]: WindowableWithValue[T, W] = {
    new WindowableWithValue[T, W] { def convert(t: T): Windower with InitialiseWithValue { type V >: W } = t }
  }
}

