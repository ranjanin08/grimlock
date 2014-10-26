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

package au.com.cba.omnia.grimlock.pairwise

import au.com.cba.omnia.grimlock._
import au.com.cba.omnia.grimlock.Matrix.Cell
import au.com.cba.omnia.grimlock.position._

/** Base trait for pairwise operations. */
trait Operator

/** Base trait for computing pairwise values. */
trait Compute extends ComputeWithValue { self: Operator =>
  type V = Any

  def compute[P <: Position with ModifyablePosition, D <: Dimension](
    slice: Slice[P, D], left: Cell[P], right: Cell[P], ext: V) = {
    compute(slice, left, right)
  }

  /**
   * Indicate if the cell is selected as part of the sample.
   *
   * @param slice Encapsulates the dimension(s) along which to compute.
   * @param left  The left cell to compute with.
   * @param right The right cell to compute with.
   *
   * @note The return value is an `Option` to allow, for example, upper
   *       or lower triangular matrices to be returned (this can be done by
   *       comparing the approriate coordinates)
   */
  def compute[P <: Position with ModifyablePosition, D <: Dimension](
    slice: Slice[P, D], left: Cell[P], right: Cell[P]): Option[Cell[P#S]]
}

/** Base trait for computing pairwise values with a user provided value. */
trait ComputeWithValue { self: Operator =>
  /** Type of the external value. */
  type V

  /**
   * Indicate if the cell is selected as part of the sample.
   *
   * @param slice Encapsulates the dimension(s) along which to compute.
   * @param left  The left cell to compute with.
   * @param right The right cell to compute with.
   * @param ext   The user define the value.
   *
   * @note The return value is an `Option` to allow, for example, upper
   *       or lower triangular matrices to be returned (this can be done by
   *       comparing the approriate coordinates)
   */
  def compute[P <: Position with ModifyablePosition, D <: Dimension](
    slice: Slice[P, D], left: Cell[P], right: Cell[P],
    ext: V): Option[Cell[P#S]]
}

