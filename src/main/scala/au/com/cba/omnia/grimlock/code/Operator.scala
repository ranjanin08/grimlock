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
import au.com.cba.omnia.grimlock.content._
import au.com.cba.omnia.grimlock.Matrix.Cell
import au.com.cba.omnia.grimlock.position._

/**
 * Base trait for comparing two positions to determine is pairwise operation
 * is to be applied.
 */
trait Comparer {
  /**
   * Check, based on left and right positions, if pairwise operation should
   * be computed.
   *
   * @param l Left position.
   * @param r Right position.
   */
  def check(l: Position, r: Position): Boolean
}

/** Case object for computing all pairwise combinations. */
case object All extends Comparer {
  def check(l: Position, r: Position): Boolean = true
}

/** Case object for computing diagonal pairwise combinations (i.e. l == r). */
case object Diagonal extends Comparer {
  def check(l: Position, r: Position): Boolean = l.compare(r) == 0
}

/**
 * Case object for computing upper triangular pairwise combinations (i.e.
 * r > l).
 */
case object Upper extends Comparer {
  def check(l: Position, r: Position): Boolean = r.compare(l) > 0
}

/**
 * Case object for computing upper triangular or diagonal pairwise combinations
 * (i.e. r >= l).
 */
case object UpperDiagonal extends Comparer {
  def check(l: Position, r: Position): Boolean = r.compare(l) >= 0
}

/**
 * Case object for computing lower triangular pairwise combinations (i.e.
 * l > r).
 */
case object Lower extends Comparer {
  def check(l: Position, r: Position): Boolean = l.compare(r) > 0
}

/**
 * Case object for computing lower triangular or diagonal pairwise combinations
 * (i.e. l >= r).
 */
case object LowerDiagonal extends Comparer {
  def check(l: Position, r: Position): Boolean = l.compare(r) >= 0
}

/** Base trait for pairwise operations. */
trait Operator

/** Base trait for computing pairwise values. */
trait Compute extends ComputeWithValue { self: Operator =>
  type V = Any

  def compute[P <: Position, D <: Dimension](slice: Slice[P, D],
    leftPos: Slice[P, D]#S, leftCon: Content, rightPos: Slice[P, D]#S,
      rightCon: Content, rem: Slice[P, D]#R, ext: V): Option[Cell[rem.M]] = {
    compute(slice, leftPos, leftCon, rightPos, rightCon, rem)
  }

  /**
   * Indicate if the cell is selected as part of the sample.
   *
   * @param slice    Encapsulates the dimension(s) along which to compute.
   * @param leftPos  The selected left cell position to compute for.
   * @param leftCon  The contents of the left cell position to compute with.
   * @param rightPos The selected right cell position to compute for.
   * @param rightCon The contents of the right cell position to compute with.
   * @param rem      The remaining coordinates.
   *
   * @note The return value is an `Option` to allow, for example, upper
   *       or lower triangular matrices to be returned (this can be done by
   *       comparing the selected coordinates)
   */
  def compute[P <: Position, D <: Dimension](slice: Slice[P, D],
    leftPos: Slice[P, D]#S, leftCon: Content, rightPos: Slice[P, D]#S,
      rightCon: Content, rem: Slice[P, D]#R): Option[Cell[rem.M]]
}

/** Base trait for computing pairwise values with a user provided value. */
trait ComputeWithValue { self: Operator =>
  /** Type of the external value. */
  type V

  /**
   * Indicate if the cell is selected as part of the sample.
   *
   * @param slice    Encapsulates the dimension(s) along which to compute.
   * @param leftPos  The selected left cell position to compute for.
   * @param leftCon  The contents of the left cell position to compute with.
   * @param rightPos The selected right cell position to compute for.
   * @param rightCon The contents of the right cell position to compute with.
   * @param rem      The remaining coordinates.
   * @param ext      The user define the value.
   *
   * @note The return value is an `Option` to allow, for example, upper
   *       or lower triangular matrices to be returned (this can be done by
   *       comparing the selected coordinates)
   */
  def compute[P <: Position, D <: Dimension](slice: Slice[P, D],
    leftPos: Slice[P, D]#S, leftCon: Content, rightPos: Slice[P, D]#S,
      rightCon: Content, rem: Slice[P, D]#R, ext: V): Option[Cell[rem.M]]
}

// TODO: Add listable versions

