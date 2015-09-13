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

package au.com.cba.omnia.grimlock.framework.window

import au.com.cba.omnia.grimlock.framework._
import au.com.cba.omnia.grimlock.framework.position._

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
trait Window[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position]
  extends WindowWithValue[S, R, Q] { self =>
  type V = Any

  def initialiseWithValue(cell: Cell[S], rem: R, ext: V): (T, TraversableOnce[Cell[Q]]) = initialise(cell, rem)

  def presentWithValue(cell: Cell[S], rem: R, ext: V, t: T): (T, TraversableOnce[Cell[Q]]) = present(cell, rem, t)

  /**
   * Initialise the state using the first cell (ordered according to its position).
   *
   * @param cell The cell to initialise with.
   * @param rem  The remaining coordinates of the cell.
   *
   * @return The state for this object.
   */
  def initialise(cell: Cell[S], rem: R): (T, TraversableOnce[Cell[Q]])

  /**
   * Update state with the current cell and, optionally, output derived data.
   *
   * @param cell The selected cell to derive from.
   * @param rem  The remaining coordinates of the cell.
   * @param t    The state.
   *
   * @return A tuple consisting of updated state together with optional derived data.
   */
  def present(cell: Cell[S], rem: R, t: T): (T, TraversableOnce[Cell[Q]])

  /**
   * Operator for generating derived data and then renaming dimensions.
   *
   * @param rename The rename to apply after the derived data has been generated.
   *
   * @return A window that runs `this` and then renames the resulting dimension(s).
   */
  def andThenRename(rename: (Cell[S], R, Cell[Q]) => Q) = {
    new Window[S, R, Q] {
      type T = self.T

      def initialise(cell: Cell[S], rem: R): (T, TraversableOnce[Cell[Q]]) = {
        val state = self.initialise(cell, rem)

        (state._1, state._2.map { case c => Cell(rename(cell, rem, c), c.content) })
      }

      def present(cell: Cell[S], rem: R, t: T): (T, TraversableOnce[Cell[Q]]) = {
        val state = self.present(cell, rem, t)

        (state._1, state._2.map { case c => Cell(rename(cell, rem, c), c.content) })
      }
    }
  }

  /**
   * Operator for generating derived data and then expanding dimensions.
   *
   * @param expand The expansion to apply after the derived data has been generated.
   *
   * @return A window that runs `this` and then expands the resulting dimensions.
   */
  def andThenExpand[U <: Position](expand: (Cell[S], R, Cell[Q]) => U)(implicit ev: PosExpDep[Q, U]) = {
    new Window[S, R, U] {
      type T = self.T

      def initialise(cell: Cell[S], rem: R): (T, TraversableOnce[Cell[U]]) = {
        val state = self.initialise(cell, rem)

        (state._1, state._2.map { case c => Cell(expand(cell, rem, c), c.content) })
      }

      def present(cell: Cell[S], rem: R, t: T): (T, TraversableOnce[Cell[U]]) = {
        val state = self.present(cell, rem, t)

        (state._1, state._2.map { case c => Cell(expand(cell, rem, c), c.content) })
      }
    }
  }
}

/**
 * Base trait for initialising a windowed with a user supplied value.
 *
 * Windowed data is derived from two or more values, for example deltas or gradients. To generate this, the process is
 * as follows. First the matrix is grouped according to a slice. The data in each group is then sorted by the remaining
 * coordinates. The first cell of each group is passed to the prepare method. This allows a windowed to initialise it's
 * running state. All subsequent cells are passed to the present method (together with the running state). The present
 * method can update the running state, and optionally return one or more cells with derived data. Note that the
 * running state can be used to create derived features of different window sizes.
 */
trait WindowWithValue[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position]
  extends java.io.Serializable { self =>
  /** Type of the external value. */
  type V

  /** Type of the state. */
  type T

  /**
   * Initialise the state using the first cell (ordered according to its position).
   *
   * @param cell The cell to initialise with.
   * @param rem  The remaining coordinates of the cell.
   * @param ext  The user define the value.
   *
   * @return The state for this object.
   */
  def initialiseWithValue(cell: Cell[S], rem: R, ext: V): (T, TraversableOnce[Cell[Q]])

  /**
   * Update state with the current cell and, optionally, output derived data.
   *
   * @param cell The selected cell to derive from.
   * @param rem  The remaining coordinates of the cell.
   * @param ext  The user define the value.
   * @param t    The state.
   *
   * @return A tuple consisting of updated state together with optional derived data.
   */
  def presentWithValue(cell: Cell[S], rem: R, ext: V, t: T): (T, TraversableOnce[Cell[Q]])

  /**
   * Operator for generating derived data and then renaming dimensions.
   *
   * @param rename The rename to apply after the derived data has been generated.
   *
   * @return A window that runs `this` and then renames the resulting dimension(s).
   */
  def andThenRenameWithValue(rename: (Cell[S], R, V, Cell[Q]) => Q) = {
    new WindowWithValue[S, R, Q] {
      type V = self.V
      type T = self.T

      def initialiseWithValue(cell: Cell[S], rem: R, ext: V): (T, TraversableOnce[Cell[Q]]) = {
        val state = self.initialiseWithValue(cell, rem, ext)

        (state._1, state._2.map { case c => Cell(rename(cell, rem, ext, c), c.content) })
      }

      def presentWithValue(cell: Cell[S], rem: R, ext: V, t: T): (T, TraversableOnce[Cell[Q]]) = {
        val state = self.presentWithValue(cell, rem, ext, t)

        (state._1, state._2.map { case c => Cell(rename(cell, rem, ext, c), c.content) })
      }
    }
  }

  /**
   * Operator for generating derived data and then expanding dimensions.
   *
   * @param expand The expansion to apply after the derived data has been generated.
   *
   * @return A window that runs `this` and then expands the resulting dimensions.
   */
  def andThenExpandWithValue[U <: Position](expand: (Cell[S], R, V, Cell[Q]) => U)(implicit ev: PosExpDep[Q, U]) = {
    new WindowWithValue[S, R, U] {
      type V = self.V
      type T = self.T

      def initialiseWithValue(cell: Cell[S], rem: R, ext: V): (T, TraversableOnce[Cell[U]]) = {
        val state = self.initialiseWithValue(cell, rem, ext)

        (state._1, state._2.map { case c => Cell(expand(cell, rem, ext, c), c.content) })
      }

      def presentWithValue(cell: Cell[S], rem: R, ext: V, t: T): (T, TraversableOnce[Cell[U]]) = {
        val state = self.presentWithValue(cell, rem, ext, t)

        (state._1, state._2.map { case c => Cell(expand(cell, rem, ext, c), c.content) })
      }
    }
  }
}

/** Type class for transforming a type `T` to a `Window[S, R, Q]`. */
trait Windowable[T, S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position] {
  /**
   * Returns a `Window[S, R, Q]` for type `T`.
   *
   * @param t Object that can be converted to a `Window[S, R, Q]`.
   */
  def convert(t: T): Window[S, R, Q]
}

/** Companion object for the `Windowable` type class. */
object Windowable {
  /** Converts a `Window[S, R, S#M]` to a `Window[S, R, S#M]`; that is, it is a pass through. */
  implicit def WSRSM2W[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, T <: Window[S, R, S#M]]: Windowable[T, S, R, S#M] = W2W[S, R, S#M, T]

  /** Converts a `Window[S, R, Q]` to a `Window[S, R, Q]`; that is, it is a pass through. */
  implicit def WSRQ2W[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position, T <: Window[S, R, Q]](implicit ev: PosExpDep[S, Q]): Windowable[T, S, R, Q] = W2W[S, R, Q, T]

  private def W2W[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position, T <: Window[S, R, Q]]: Windowable[T, S, R, Q] = {
    new Windowable[T, S, R, Q] { def convert(t: T): Window[S, R, Q] = t }
  }

  /** Converts a `List[Window[S, R, S#M]]` to a single `Window[S, R, S#M]`. */
  implicit def LWSRSM2W[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, T <: Window[S, R, S#M]]: Windowable[List[T], S, R, S#M] = LW2W[S, R, S#M, T]

  /** Converts a `List[Window[S, R, Q]]` to a single `Window[S, R, Q]`. */
  implicit def LWSRQ2W[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position, T <: Window[S, R, Q]](implicit ev: PosExpDep[S, Q]): Windowable[List[T], S, R, Q] = LW2W[S, R, Q, T]

  private def LW2W[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position, T <: Window[S, R, Q]]: Windowable[List[T], S, R, Q] = {
    new Windowable[List[T], S, R, Q] {
      def convert(windows: List[T]): Window[S, R, Q] = {
        new Window[S, R, Q] {
          type T = List[Any]

          def initialise(cell: Cell[S], rem: R): (T, TraversableOnce[Cell[Q]]) = {
            val state = windows.map { case window => window.initialise(cell, rem) }

            (state.map { case (t, c) => t }, state.flatMap { case (t, c) => c })
          }

          def present(cell: Cell[S], rem: R, t: T): (T, TraversableOnce[Cell[Q]]) = {
            val state = (windows, t)
              .zipped
              .map { case (window, s) => window.present(cell, rem, s.asInstanceOf[window.T]) }

            (state.map { case (t, c) => t }, state.flatMap { case (t, c) => c })
          }
        }
      }
    }
  }
}

/** Type class for transforming a type `T` to a `WindowWithValue[S, R, Q]`. */
trait WindowableWithValue[T, S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position, W] {
  /**
   * Returns a `WindowWithValue[S, R, Q]` for type `T`.
   *
   * @param t Object that can be converted to a `WindowWithValue[S, R, Q]`.
   */
  def convert(t: T): WindowWithValue[S, R, Q] { type V >: W }
}

/** Companion object for the `WindowableWithValue` type class. */
object WindowableWithValue {
  /** Converts a `WindowWithValue[S, R, S#M]` to a `WindowWithValue[S, R, S#M]`; that is, it is a pass through. */
  implicit def WSRSMW2WWV[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, T <: WindowWithValue[S, R, S#M] { type V >: W }, W]: WindowableWithValue[T, S, R, S#M, W] = W2WWV[S, R, S#M, T, W]

  /** Converts a `WindowWithValue[S, R, Q]` to a `WindowWithValue[S, R, Q]`; that is, it is a pass through. */
  implicit def WSRQW2WWV[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position, T <: WindowWithValue[S, R, Q] { type V >: W }, W](implicit ev: PosExpDep[S, Q]): WindowableWithValue[T, S, R, Q, W] = W2WWV[S, R, Q, T, W]

  private def W2WWV[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position, T <: WindowWithValue[S, R, Q] { type V >: W }, W]: WindowableWithValue[T, S, R, Q, W] = {
    new WindowableWithValue[T, S, R, Q, W] { def convert(t: T): WindowWithValue[S, R, Q] { type V >: W } = t }
  }

  /** Converts a `List[WindowWithValue[S, R, S#M]]` to a single `WindowWithValue[S, R, S#M]`. */
  implicit def LWSRSMW2WWV[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, T <: WindowWithValue[S, R, S#M] { type V >: W }, W]: WindowableWithValue[List[T], S, R, S#M, W] = LW2WWV[S, R, S#M, T, W]

  /** Converts a `List[WindowWithValue[S, R, Q]]` to a single `WindowWithValue[S, R, Q]`. */
  implicit def LWSRQW2WWV[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position, T <: WindowWithValue[S, R, Q] { type V >: W }, W](implicit ev: PosExpDep[S, Q]): WindowableWithValue[List[T], S, R, Q, W] = LW2WWV[S, R, Q, T, W]

  private def LW2WWV[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position, T <: WindowWithValue[S, R, Q] { type V >: W }, W]: WindowableWithValue[List[T], S, R, Q, W] = {
    new WindowableWithValue[List[T], S, R, Q, W] {
      def convert(windows: List[T]): WindowWithValue[S, R, Q] { type V >: W } = {
        new WindowWithValue[S, R, Q] {
          type T = List[Any]
          type V = W

          def initialiseWithValue(cell: Cell[S], rem: R, ext: V): (T, TraversableOnce[Cell[Q]]) = {
            val state = windows.map { case window => window.initialiseWithValue(cell, rem, ext) }

            (state.map { case (t, c) => t }, state.flatMap { case (t, c) => c })
          }

          def presentWithValue(cell: Cell[S], rem: R, ext: V, t: T): (T, TraversableOnce[Cell[Q]]) = {
            val state = (windows, t)
              .zipped
              .map { case (window, s) => window.presentWithValue(cell, rem, ext, s.asInstanceOf[window.T]) }

            (state.map { case (t, c) => t }, state.flatMap { case (t, c) => c })
          }
        }
      }
    }
  }
}

