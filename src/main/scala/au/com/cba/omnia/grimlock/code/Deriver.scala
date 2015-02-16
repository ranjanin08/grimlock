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

package au.com.cba.omnia.grimlock.derive

import au.com.cba.omnia.grimlock._
import au.com.cba.omnia.grimlock.content._
import au.com.cba.omnia.grimlock.Matrix.Cell
import au.com.cba.omnia.grimlock.position._
import au.com.cba.omnia.grimlock.utility._

/**
 * Base trait for generating derived data.
 *
 * Derived data is derived from two or more values, for example deltas or gradients. To generate this, the process is
 * as follows. First the matrix is grouped according to a slice. The data in each group is then sorted by the remaining
 * coordinates. The first cell of each group is passed to the prepare method. This allows a deriver to initialise it's
 * running state. All subsequent cells are passed to the present method (together with the running state). The present
 * method can update the running state, and optionally return one or more cells with derived data. Note that the
 * running state can be used to create derived features of different window sizes.
 */
trait Deriver {
  /** Type of the state. */
  type T

  /**
   * Update state with the current cell and, optionally, output derived data.
   *
   * @param sel The selected coordinates of the current cell.
   * @param rem The remaining coordinates of the current cell.
   * @param con The content of the current cell.
   * @param t   The state.
   *
   * @return A tuple consisting of updated state together with optional derived data.
   */
  def present[P <: Position, D <: Dimension](sel: Slice[P, D]#S, rem: Slice[P, D]#R, con: Content,
    t: T): (T, Collection[Cell[sel.M]])
}

/** Base trait for initialising a deriver. */
trait Initialise extends InitialiseWithValue { self: Deriver =>
  type V = Any

  def initialise[P <: Position, D <: Dimension](sel: Slice[P, D]#S, rem: Slice[P, D]#R, con: Content, ext: V): T = {
    initialise(sel, rem, con)
  }

  /**
   * Initialise the state using the first cell (ordered according to its position).
   *
   * @param sel The selected coordinates of the current cell.
   * @param rem The remaining coordinates of the current cell.
   * @param con The content of the first cell.
   *
   * @return The state for this object.
   */
  def initialise[P <: Position, D <: Dimension](sel: Slice[P, D]#S, rem: Slice[P, D]#R, con: Content): T
}

/** Base trait for initialising a deriver with a user supplied value. */
trait InitialiseWithValue { self: Deriver =>
  /** Type of the external value. */
  type V

  /**
   * Initialise the state using the first cell (ordered according to its position).
   *
   * @param sel The selected coordinates of the current cell.
   * @param rem The remaining coordinates of the current cell.
   * @param con The content of the first cell.
   * @param ext The user define the value.
   *
   * @return The state for this object.
   */
  def initialise[P <: Position, D <: Dimension](sel: Slice[P, D]#S, rem: Slice[P, D]#R, con: Content, ext: V): T
}

/** Base trait for combination derivers. */
trait CombinationDerivable[U <: Deriver] { self: Deriver =>
  /** Type of the state. */
  type T = List[Any]

  /** List of derivers that are combined. */
  val singles: List[U]

  /**
   * Update state with the current cell and, optionally, output derived data.
   *
   * @param sel The selected coordinates of the current cell.
   * @param rem The remaining coordinates of the current cell.
   * @param con The content of the current cell.
   * @param t   The state.
   *
   * @return A tuple consisting of updated state together with optional derived data.
   */
  def present[P <: Position, D <: Dimension](sel: Slice[P, D]#S, rem: Slice[P, D]#R, con: Content,
    t: T): (T, Collection[Cell[sel.M]]) = {
    val state = (singles, t)
      .zipped
      .map { case (deriver, s) => deriver.present(sel, rem, con, s.asInstanceOf[deriver.T]) }

    (state.map { case (t, c) => t }, Collection(state.flatMap { case (t, c) => c.toList }))
  }
}

/**
 * Deriver that is a combination of one or more derivers with `Initialise`.
 *
 * @param singles `List` of derivers that are combined together.
 *
 * @note This need not be called in an application. The `Derivable` type class will convert any `List[Deriver]`
 *       automatically to one of these.
 */
case class CombinationDeriver[T <: Deriver with Initialise](singles: List[T]) extends Deriver with Initialise
  with CombinationDerivable[T] {
  def initialise[P <: Position, D <: Dimension](sel: Slice[P, D]#S, rem: Slice[P, D]#R, con: Content): T = {
    singles.map { case deriver => deriver.initialise(sel, rem, con) }
  }
}

/**
 * Deriver that is a combination of one or more derivers with `InitialiseWithValue`.
 *
 * @param singles `List` of derivers that are combined together.
 *
 * @note This need not be called in an application. The `DerivableWithValue` type class will convert any
 *       `List[Deriver]` automatically to one of these.
 */
case class CombinationDeriverWithValue[T <: Deriver with InitialiseWithValue, W](singles: List[T])
  extends Deriver with InitialiseWithValue with CombinationDerivable[T] {
  type V = W

  def initialise[P <: Position, D <: Dimension](sel: Slice[P, D]#S, rem: Slice[P, D]#R, con: Content, ext: V): T = {
    singles.map { case deriver => deriver.initialise(sel, rem, con, ext.asInstanceOf[deriver.V]) }
  }
}

/** Type class for transforming a type `T` to a `Deriver with Initialise`. */
trait Derivable[T] {
  /**
   * Returns a `Deriver with Initialise` for type `T`.
   *
   * @param t Object that can be converted to a `Deriver with Initialise`.
   */
  def convert(t: T): Deriver with Initialise
}

/** Companion object for the `Derivable` type class. */
object Derivable {
  /** Converts a `List[Deriver with Initialise]` to a single `Deriver with Initialise` using `CombinationDeriver`. */
  implicit def LD2D[T <: Deriver with Initialise]: Derivable[List[T]] = {
    new Derivable[List[T]] { def convert(t: List[T]): Deriver with Initialise = CombinationDeriver(t) }
  }
  /** Converts a `Deriver with Initialise` to a `Deriver with Initialise`; that is, it is a pass through. */
  implicit def D2D[T <: Deriver with Initialise]: Derivable[T] = {
    new Derivable[T] { def convert(t: T): Deriver with Initialise = t }
  }
}

/** Type class for transforming a type `T` to a `Deriver with InitialiseWithValue`. */
trait DerivableWithValue[T, W] {
  /**
   * Returns a `Deriver with InitialiseWithValue` for type `T`.
   *
   * @param t Object that can be converted to a `Deriver with InitialiseWithValue`.
   */
  def convert(t: T): Deriver with InitialiseWithValue
}

/** Companion object for the `DerivableWithValue` type class. */
object DerivableWithValue {
  /**
   * Converts a `List[Deriver with InitialiseWithValue]` to a single `Deriver with InitialiseWithValue` using
   * `CombinationDeriverWithValue`.
   */
  implicit def DT2DWV[T <: Deriver with InitialiseWithValue { type V >: W }, W]: DerivableWithValue[List[T], W] = {
    new DerivableWithValue[List[T], W] {
      def convert(t: List[T]): Deriver with InitialiseWithValue = CombinationDeriverWithValue[T, W](t)
    }
  }
  /**
   * Converts a `Deriver with InitialiseWithValue` to a `Deriver with InitialiseWithValue`; that is, it is a pass
   * through.
   */
  implicit def D2DWV[T <: Deriver with InitialiseWithValue { type V >: W }, W]: DerivableWithValue[T, W] = {
    new DerivableWithValue[T, W] { def convert(t: T): Deriver with InitialiseWithValue = t }
  }
}

