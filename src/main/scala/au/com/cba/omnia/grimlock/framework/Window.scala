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
import au.com.cba.omnia.grimlock.framework.content._
import au.com.cba.omnia.grimlock.framework.position._

/**
 * Base trait for generating windowed data.
 *
 * Windowed data is derived from two or more values, for example deltas or gradients. To generate this, the process is
 * as follows. First each cell is prepared for windowed operations. This involves return the input data to slide over.
 * Next the input data is grouped according to a `slice`. The data in each group is then sorted by the remaining
 * coordinates. The first cell's data of each group is passed to the initialise method. This allows a window to
 * initialise it's running state. All subsequent cells' data are passed to the update method (together with the running
 * state). The update method can update the running state, and optionally return one or more output values. Finally,
 * the present method returns cells for the output values. Note that the running state can be used to create derived
 * features of different window sizes.
 */
trait Window[P <: Position, S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position]
  extends WindowWithValue[P, S, R, Q] { self =>
  type V = Any

  def prepareWithValue(cell: Cell[P], ext: V): I = prepare(cell)

  def presentWithValue(pos: S, out: O, ext: V): TraversableOnce[Cell[Q]] = present(pos, out)

  /**
   * Prepare a sliding window operations.
   *
   * @param cell The cell from which to generate the input data.
   *
   * @return The input data over which the sliding window operates.
   */
  def prepare(cell: Cell[P]): I

  /**
   * Present zero or more cells for the output data.
   *
   * @param pos Selected position from which to generate the derived data.
   * @param out The output data from which to generate the cell.
   *
   * @return Zero or more cells.
   */
  def present(pos: S, out: O): TraversableOnce[Cell[Q]]

  /**
   * Operator for preparing content prior to generating derived data.
   *
   * @param prep The function to apply prior to generating derived data.
   *
   * @return A windowed function that prepares the content and then runs `this`.
   */
  override def withPrepare(prep: (Cell[P]) => Content) = {
    new Window[P, S, R, Q] {
      type I = self.I
      type T = self.T
      type O = self.O

      def prepare(cell: Cell[P]): I = self.prepare(Cell(cell.position, prep(cell)))

      def initialise(rem: R, in: I): (T, TraversableOnce[O]) = self.initialise(rem, in)

      def update(rem: R, in: I, t: T): (T, TraversableOnce[O]) = self.update(rem, in, t)

      def present(pos: S, out: O): TraversableOnce[Cell[Q]] = self.present(pos, out)
    }
  }

  /**
   * Operator for generating derived data and then updating the contents.
   *
   * @param mutate The mutation to apply after generating derived data.
   *
   * @return A windowed function that runs `this` and then updates the resulting contents.
   */
  override def andThenMutate(mutate: (Cell[Q]) => Content) = {
    new Window[P, S, R, Q] {
      type I = self.I
      type T = self.T
      type O = self.O

      def prepare(cell: Cell[P]): I = self.prepare(cell)

      def initialise(rem: R, in: I): (T, TraversableOnce[O]) = self.initialise(rem, in)

      def update(rem: R, in: I, t: T): (T, TraversableOnce[O]) = self.update(rem, in, t)

      def present(pos: S, out: O): TraversableOnce[Cell[Q]] = {
        self.present(pos, out).map { case c => Cell(c.position, mutate(c)) }
      }
    }
  }

  /**
   * Operator for generating derived data and then relocating the contents.
   *
   * @param locate The relocation to apply after generating derived data.
   *
   * @return A windowed function that runs `this` and then relocates the contents.
   */
  override def andThenRelocate[U <: Position](locate: Locate.FromCell[Q, U])(implicit ev: PosIncDep[Q, U]) = {
    new Window[P, S, R, U] {
      type I = self.I
      type T = self.T
      type O = self.O

      def prepare(cell: Cell[P]): I = self.prepare(cell)

      def initialise(rem: R, in: I): (T, TraversableOnce[O]) = self.initialise(rem, in)

      def update(rem: R, in: I, t: T): (T, TraversableOnce[O]) = self.update(rem, in, t)

      def present(pos: S, out: O): TraversableOnce[Cell[U]] = {
        self.present(pos, out).map { case c => Cell(locate(c), c.content) }
      }
    }
  }
}

/**
 * Base trait for initialising a windowed with a user supplied value.
 *
 * Windowed data is derived from two or more values, for example deltas or gradients. To generate this, the process is
 * as follows. First each cell is prepared for windowed operations. This involves return the input data to slide over.
 * Next the input data is grouped according to a `slice`. The data in each group is then sorted by the remaining
 * coordinates. The first cell's data of each group is passed to the initialise method. This allows a window to
 * initialise it's running state. All subsequent cells' data are passed to the update method (together with the running
 * state). The update method can update the running state, and optionally return one or more output values. Finally,
 * the present method returns cells for the output values. Note that the running state can be used to create derived
 * features of different window sizes.
 */
trait WindowWithValue[P <: Position, S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position]
  extends java.io.Serializable { self =>
  /** Type of the external value. */
  type V

  /** Type of the input data. */
  type I

  /** Type of the running state. */
  type T

  /** Type of the output data. */
  type O

  /**
   * Prepare a sliding window operations.
   *
   * @param cell The cell from which to generate the input data.
   * @param ext  User provided data required for preparation.
   *
   * @return The input data over which the sliding window operates.
   */
  def prepareWithValue(cell: Cell[P], ext: V): I

  /**
   * Initialise the running state using the first cell (ordered according to its position).
   *
   * @param rem  The remaining coordinates of the cell.
   * @param in   The input data.
   *
   * @return The running state for this object.
   */
  def initialise(rem: R, in: I): (T, TraversableOnce[O])

  /**
   * Update running state with the state and, optionally, return output data.
   *
   * @param rem  The remaining coordinates of the cell.
   * @param in   The input data.
   * @param t    The running state.
   *
   * @return A tuple consisting of updated running state together with optional output data.
   */
  def update(rem: R, in: I, t: T): (T, TraversableOnce[O])

  /**
   * Present zero or more cells for the output data.
   *
   * @param pos Selected position from which to generate the derived data.
   * @param out The output data from which to generate the cell.
   * @param ext User provided data required for preparation.
   *
   * @return Zero or more cells.
   */
  def presentWithValue(pos: S, out: O, ext: V): TraversableOnce[Cell[Q]]

  /**
   * Operator for preparing content prior to generating derived data.
   *
   * @param prep The function to apply prior to generating derived data.
   *
   * @return A windowed function that prepares the content and then runs `this`.
   */
  def withPrepare(prep: (Cell[P]) => Content) = {
    new WindowWithValue[P, S, R, Q] {
      type V = self.V
      type I = self.I
      type T = self.T
      type O = self.O

      def prepareWithValue(cell: Cell[P], ext: V): I = self.prepareWithValue(Cell(cell.position, prep(cell)), ext)

      def initialise(rem: R, in: I): (T, TraversableOnce[O]) = self.initialise(rem, in)

      def update(rem: R, in: I, t: T): (T, TraversableOnce[O]) = self.update(rem, in, t)

      def presentWithValue(pos: S, out: O, ext: V): TraversableOnce[Cell[Q]] = self.presentWithValue(pos, out, ext)
    }
  }

  /**
   * Operator for generating derived data and then updating the contents.
   *
   * @param mutate The mutation to apply after generating derived data.
   *
   * @return A windowed function that runs `this` and then updates the resulting contents.
   */
  def andThenMutate(mutate: (Cell[Q]) => Content) = {
    new WindowWithValue[P, S, R, Q] {
      type V = self.V
      type I = self.I
      type T = self.T
      type O = self.O

      def prepareWithValue(cell: Cell[P], ext: V): I = self.prepareWithValue(cell, ext)

      def initialise(rem: R, in: I): (T, TraversableOnce[O]) = self.initialise(rem, in)

      def update(rem: R, in: I, t: T): (T, TraversableOnce[O]) = self.update(rem, in, t)

      def presentWithValue(pos: S, out: O, ext: V): TraversableOnce[Cell[Q]] = {
        self.presentWithValue(pos, out, ext).map { case c => Cell(c.position, mutate(c)) }
      }
    }
  }

  /**
   * Operator for generating derived data and then relocating the contents.
   *
   * @param locate The relocation to apply after generating derived data.
   *
   * @return A windowed function that runs `this` and then relocates the contents.
   */
  def andThenRelocate[U <: Position](locate: Locate.FromCell[Q, U])(implicit ev: PosIncDep[Q, U]) = {
    new WindowWithValue[P, S, R, U] {
      type V = self.V
      type I = self.I
      type T = self.T
      type O = self.O

      def prepareWithValue(cell: Cell[P], ext: V): I = self.prepareWithValue(cell, ext)

      def initialise(rem: R, in: I): (T, TraversableOnce[O]) = self.initialise(rem, in)

      def update(rem: R, in: I, t: T): (T, TraversableOnce[O]) = self.update(rem, in, t)

      def presentWithValue(pos: S, out: O, ext: V): TraversableOnce[Cell[U]] = {
        self.presentWithValue(pos, out, ext).map { case c => Cell(locate(c), c.content) }
      }
    }
  }

  /**
   * Operator for preparing content prior to generating derived data.
   *
   * @param prep The function to apply prior to generating derived data.
   *
   * @return A windowed function that prepares the content and then runs `this`.
   */
  def withPrepareWithValue(prep: (Cell[P], V) => Content) = {
    new WindowWithValue[P, S, R, Q] {
      type V = self.V
      type I = self.I
      type T = self.T
      type O = self.O

      def prepareWithValue(cell: Cell[P], ext: V): I = self.prepareWithValue(Cell(cell.position, prep(cell, ext)), ext)

      def initialise(rem: R, in: I): (T, TraversableOnce[O]) = self.initialise(rem, in)

      def update(rem: R, in: I, t: T): (T, TraversableOnce[O]) = self.update(rem, in, t)

      def presentWithValue(pos: S, out: O, ext: V): TraversableOnce[Cell[Q]] = self.presentWithValue(pos, out, ext)
    }
  }

  /**
   * Operator for generating derived data and then updating the contents.
   *
   * @param mutate The mutation to apply after generating derived data.
   *
   * @return A windowed function that runs `this` and then updates the resulting contents.
   */
  def andThenMutateWithValue(mutate: (Cell[Q], V) => Content) = {
    new WindowWithValue[P, S, R, Q] {
      type V = self.V
      type I = self.I
      type T = self.T
      type O = self.O

      def prepareWithValue(cell: Cell[P], ext: V): I = self.prepareWithValue(cell, ext)

      def initialise(rem: R, in: I): (T, TraversableOnce[O]) = self.initialise(rem, in)

      def update(rem: R, in: I, t: T): (T, TraversableOnce[O]) = self.update(rem, in, t)

      def presentWithValue(pos: S, out: O, ext: V): TraversableOnce[Cell[Q]] = {
        self.presentWithValue(pos, out, ext).map { case c => Cell(c.position, mutate(c, ext)) }
      }
    }
  }

  /**
   * Operator for generating derived data and then relocating the contents.
   *
   * @param locate The relocation to apply after generating derived data.
   *
   * @return A windowed function that runs `this` and then relocates the contents.
   */
  def andThenRelocateWithValue[U <: Position](locate: Locate.FromCellWithValue[Q, U, V])(
    implicit ev: PosIncDep[Q, U]) = {
    new WindowWithValue[P, S, R, U] {
      type V = self.V
      type I = self.I
      type T = self.T
      type O = self.O

      def prepareWithValue(cell: Cell[P], ext: V): I = self.prepareWithValue(cell, ext)

      def initialise(rem: R, in: I): (T, TraversableOnce[O]) = self.initialise(rem, in)

      def update(rem: R, in: I, t: T): (T, TraversableOnce[O]) = self.update(rem, in, t)

      def presentWithValue(pos: S, out: O, ext: V): TraversableOnce[Cell[U]] = {
        self.presentWithValue(pos, out, ext).map { case c => Cell(locate(c, ext), c.content) }
      }
    }
  }
}

/** Trait for transforming a type `T` to a `Window[S, R, Q]`. */
trait Windowable[P <: Position, S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position]
  extends java.io.Serializable {
  /** Returns a `Window[S, R, Q]` for this type `T`. */
  def apply(): Window[P, S, R, Q]
}

/** Companion object for the `Windowable` trait. */
object Windowable {
  /** Converts a `Window[P, S, R, Q]` to a `Window[P, S, R, Q]`; that is, it is a pass through. */
  implicit def W2W[P <: Position, S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position](
    t: Window[P, S, R, Q]): Windowable[P, S, R, Q] = {
    new Windowable[P, S, R, Q] { def apply(): Window[P, S, R, Q] = t }
  }

  /** Converts a `List[Window[P, S, R, Q]]` to a single `Window[P, S, R, Q]`. */
  implicit def LW2W[P <: Position, S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position](
    t: List[Window[P, S, R, Q]]): Windowable[P, S, R, Q] = {
    new Windowable[P, S, R, Q] {
      def apply(): Window[P, S, R, Q] = {
        new Window[P, S, R, Q] {
          type I = List[Any]
          type T = List[Any]
          type O = List[TraversableOnce[Any]]

          def prepare(cell: Cell[P]): I = t.map { case window => window.prepare(cell) }

          def initialise(rem: R, in: I): (T, TraversableOnce[O]) = {
            val state = (t, in)
              .zipped
              .map { case (window, j) => window.initialise(rem, j.asInstanceOf[window.I]) }

            (state.map(_._1), Some(state.map(_._2)))
          }

          def update(rem: R, in: I, s: T): (T, TraversableOnce[O]) = {
            val state = (t, in, s)
              .zipped
              .map { case (window, j, u) => window.update(rem, j.asInstanceOf[window.I], u.asInstanceOf[window.T]) }

            (state.map(_._1), Some(state.map(_._2)))
          }

          def present(pos: S, out: O): TraversableOnce[Cell[Q]] = {
            (t, out)
              .zipped
              .flatMap {
                case (window, s) => s.flatMap { case t => window.present(pos, t.asInstanceOf[window.O]) }
              }
          }
        }
      }
    }
  }
}

/** Trait for transforming a type `T` to a `WindowWithValue[P, S, R, Q]`. */
trait WindowableWithValue[P <: Position, S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position, W]
  extends java.io.Serializable {
  /** Returns a `WindowWithValue[P, S, R, Q]` for this type `T`. */
  def apply(): WindowWithValue[P, S, R, Q] { type V >: W }
}

/** Companion object for the `WindowableWithValue` trait. */
object WindowableWithValue {
  /** Converts a `WindowWithValue[P, S, R, Q]` to a `WindowWithValue[P, S, R, Q]`; that is, it is a pass through. */
  implicit def WWV2WWV[P <: Position, S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position, W](
    t: WindowWithValue[P, S, R, Q] { type V >: W }): WindowableWithValue[P, S, R, Q, W] = {
    new WindowableWithValue[P, S, R, Q, W] { def apply(): WindowWithValue[P, S, R, Q] { type V >: W } = t }
  }

  /** Converts a `List[WindowWithValue[S, R, Q]]` to a single `WindowWithValue[S, R, Q]`. */
  implicit def LWWV2WWV[P <: Position, S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position, W](
    t: List[WindowWithValue[P, S, R, Q] { type V >: W }]): WindowableWithValue[P, S, R, Q, W] = {
    new WindowableWithValue[P, S, R, Q, W] {
      def apply(): WindowWithValue[P, S, R, Q] { type V >: W } = {
        new WindowWithValue[P, S, R, Q] {
          type V = W
          type I = List[Any]
          type T = List[Any]
          type O = List[TraversableOnce[Any]]

          def prepareWithValue(cell: Cell[P], ext: V): I = t.map { case window => window.prepareWithValue(cell, ext) }

          def initialise(rem: R, in: I): (T, TraversableOnce[O]) = {
            val state = (t, in)
              .zipped
              .map { case (window, j) => window.initialise(rem, j.asInstanceOf[window.I]) }

            (state.map(_._1), Some(state.map(_._2)))
          }

          def update(rem: R, in: I, s: T): (T, TraversableOnce[O]) = {
            val state = (t, in, s)
              .zipped
              .map { case (window, j, u) => window.update(rem, j.asInstanceOf[window.I], u.asInstanceOf[window.T]) }

            (state.map(_._1), Some(state.map(_._2)))
          }

          def presentWithValue(pos: S, out: O, ext: V): TraversableOnce[Cell[Q]] = {
            (t, out)
              .zipped
              .flatMap {
                case (window, s) => s.flatMap { case t => window.presentWithValue(pos, t.asInstanceOf[window.O], ext) }
              }
          }
        }
      }
    }
  }
}

