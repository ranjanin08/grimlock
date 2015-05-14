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
import au.com.cba.omnia.grimlock.framework.position._
import au.com.cba.omnia.grimlock.framework.utility._

/**
 * Base trait for generating windowed data.
 *
 * Windowed data is derived from two or more values, for example deltas or gradients. To generate this, the process is
 * as follows. First the matrix is grouped according to a slice. The data in each group is then sorted by the remaining
 * coordinates. The first cell of each group is passed to the prepare method. This allows a windowed to initialise it's
 * running state. All subsequent cells are passed to the present method (together with the running state). The present
 * method can update the running state, and optionally return one or more cells with derived data. Note that the
 * running state can be used to create derived features of different window sizes.
 */
trait Windowed {
  /** Type of the state. */
  type T
}

/** Base trait for initialising a windowed. */
trait Initialise extends InitialiseWithValue { self: Windowed =>
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

/** Base trait for initialising a windowed with a user supplied value. */
trait InitialiseWithValue { self: Windowed =>
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
 * Windowed that is a combination of one or more `Windowed with Initialise`.
 *
 * @param singles `List` of windowed that are combined together.
 *
 * @note This need not be called in an application. The `Windowable` type class will convert any `List[Windowed]`
 *       automatically to one of these.
 */
case class CombinationWindowed[T <: Windowed with Initialise](singles: List[T]) extends Windowed with Initialise {
  type T = List[Any]

  def initialise[P <: Position, D <: Dimension](slice: Slice[P, D])(cell: Cell[slice.S], rem: slice.R): T = {
    singles.map { case windowed => windowed.initialise(slice)(cell, rem) }
  }

  def present[P <: Position, D <: Dimension](slice: Slice[P, D])(cell: Cell[slice.S], rem: slice.R,
    t: T): (T, Collection[Cell[slice.S#M]]) = {
    val state = (singles, t)
      .zipped
      .map { case (windowed, s) => windowed.present(slice)(cell, rem, s.asInstanceOf[windowed.T]) }

    (state.map { case (t, c) => t }, Collection(state.flatMap { case (t, c) => c.toList }))
  }
}

/**
 * Windowed that is a combination of one or more `Windowed with InitialiseWithValue`.
 *
 * @param singles `List` of windowed that are combined together.
 *
 * @note This need not be called in an application. The `WindowableWithValue` type class will convert any
 *       `List[Windowed]` automatically to one of these.
 */
case class CombinationWindowedWithValue[T <: Windowed with InitialiseWithValue { type V >: W }, W](singles: List[T])
  extends Windowed with InitialiseWithValue {
  type T = List[Any]
  type V = W

  def initialise[P <: Position, D <: Dimension](slice: Slice[P, D], ext: V)(cell: Cell[slice.S], rem: slice.R): T = {
    singles.map { case windowed => windowed.initialise(slice, ext)(cell, rem) }
  }

  def present[P <: Position, D <: Dimension](slice: Slice[P, D], ext: V)(cell: Cell[slice.S], rem: slice.R,
    t: T): (T, Collection[Cell[slice.S#M]]) = {
    val state = (singles, t)
      .zipped
      .map { case (windowed, s) => windowed.present(slice, ext)(cell, rem, s.asInstanceOf[windowed.T]) }

    (state.map { case (t, c) => t }, Collection(state.flatMap { case (t, c) => c.toList }))
  }
}

/** Type class for transforming a type `T` to a `Windowed with Initialise`. */
trait Windowable[T] {
  /**
   * Returns a `Windowed with Initialise` for type `T`.
   *
   * @param t Object that can be converted to a `Windowed with Initialise`.
   */
  def convert(t: T): Windowed with Initialise
}

/** Companion object for the `Windowable` type class. */
object Windowable {
  /** Converts a `List[Windowed with Initialise]` to a single `Windowed with Initialise` using `CombinationWindowed`. */
  implicit def LD2D[T <: Windowed with Initialise]: Windowable[List[T]] = {
    new Windowable[List[T]] { def convert(t: List[T]): Windowed with Initialise = CombinationWindowed(t) }
  }
  /** Converts a `Windowed with Initialise` to a `Windowed with Initialise`; that is, it is a pass through. */
  implicit def D2D[T <: Windowed with Initialise]: Windowable[T] = {
    new Windowable[T] { def convert(t: T): Windowed with Initialise = t }
  }
}

/** Type class for transforming a type `T` to a `Windowed with InitialiseWithValue`. */
trait WindowableWithValue[T, W] {
  /**
   * Returns a `Windowed with InitialiseWithValue` for type `T`.
   *
   * @param t Object that can be converted to a `Windowed with InitialiseWithValue`.
   */
  def convert(t: T): Windowed with InitialiseWithValue { type V >: W }
}

/** Companion object for the `WindowableWithValue` type class. */
object WindowableWithValue {
  /**
   * Converts a `List[Windowed with InitialiseWithValue]` to a single `Windowed with InitialiseWithValue` using
   * `CombinationWindowedWithValue`.
   */
  implicit def DT2DWV[T <: Windowed with InitialiseWithValue { type V >: W }, W]: WindowableWithValue[List[T], W] = {
    new WindowableWithValue[List[T], W] {
      def convert(t: List[T]): Windowed with InitialiseWithValue { type V >: W } = CombinationWindowedWithValue[T, W](t)
    }
  }
  /**
   * Converts a `Windowed with InitialiseWithValue` to a `Windowed with InitialiseWithValue`; that is, it is a pass
   * through.
   */
  implicit def D2DWV[T <: Windowed with InitialiseWithValue { type V >: W }, W]: WindowableWithValue[T, W] = {
    new WindowableWithValue[T, W] { def convert(t: T): Windowed with InitialiseWithValue { type V >: W } = t }
  }
}

