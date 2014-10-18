// Copyright 2014 Commonwealth Bank of Australia
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

import au.com.cba.omnia.grimlock.content._
import au.com.cba.omnia.grimlock.Matrix.Cell
import au.com.cba.omnia.grimlock.position._

/**
 * Base trait for generating derived data.
 *
 * Derived data is derived from two or more values, for example deltas or
 * gradients. To generate this, the process is as follows. First the matrix is
 * grouped according to a slice. The data in each group is then sorted by the
 * remaining coordinates. The first cell of each group is passed to the
 * prepare method. This allows a deriver to initialise it's running state. All
 * subsequent cells are passed to the present method (together with the running
 * state). The present method can update the running state, and optionally
 * return one or more cells with derived data. Note that the running state can
 * be used to create derived features of different window sizes.
 */
trait Deriver {
  /** Type of the state. */
  type T

  /**
   * Update state with the current cell and, optionally, output derived data.
   *
   * @param curr The current cell.
   * @param t    The state.
   *
   * @return A tuple consisting of updated state together with optional
   *         derived data.
   *
   * @note An `Option` is used for the derived data to allow derivers to
   *       be selective in when derived data is returned. An `Either` is
   *       used to allow a deriver to return more than one derived data.
   */
  def present[P <: Position with ModifyablePosition](curr: Cell[P],
    t: T): (T, Option[Either[Cell[P#S], List[Cell[P#S]]]])
}

/** Base trait for initialising a deriver. */
trait Initialise { self: Deriver =>
  /**
   * Initialise the state using the first cell (ordered according to its
   * position).
   *
   * @param curr The current cell.
   *
   * @return The state for this object.
   */
  def initialise[P <: Position](curr: Cell[P]): T
}

/** Base trait for initialising a deriver with a user supplied value. */
trait InitialiseWithValue { self: Deriver =>
  /** Type of the external value. */
  type V

  /**
   * Initialise the state using the first cell (ordered according to its
   * position).
   *
   * @param curr The current cell.
   * @param ext  The user define the value.
   *
   * @return The state for this object.
   */
  def initialise[P <: Position](curr: Cell[P], ext: V): T
}

/**
 * Convenience trait for derivers that initialise with or without using a
 * user supplied value.
 */
trait InitialiseAndWithValue extends Initialise
  with InitialiseWithValue { self: Deriver =>
  type V = Any

  def initialise[P <: Position](curr: Cell[P], ext: V): T = initialise(curr)
}

