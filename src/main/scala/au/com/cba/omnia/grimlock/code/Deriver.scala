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
import au.com.cba.omnia.grimlock.Matrix.CellCollection
import au.com.cba.omnia.grimlock.position._

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
    t: T): (T, CellCollection[sel.M])
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

