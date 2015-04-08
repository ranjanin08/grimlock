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

package au.com.cba.omnia.grimlock.pairwise

import au.com.cba.omnia.grimlock._
import au.com.cba.omnia.grimlock.content._
import au.com.cba.omnia.grimlock.position._
import au.com.cba.omnia.grimlock.utility._

/** Base trait for comparing two positions to determine is pairwise operation is to be applied. */
trait Comparer {
  /**
   * Check, based on left and right positions, if pairwise operation should be computed.
   *
   * @param l Left position.
   * @param r Right position.
   */
  def check(l: Position, r: Position): Boolean
}

/** Case object for computing all pairwise combinations. */
case object All extends Comparer { def check(l: Position, r: Position): Boolean = true }

/** Case object for computing diagonal pairwise combinations (i.e. l == r). */
case object Diagonal extends Comparer { def check(l: Position, r: Position): Boolean = l.compare(r) == 0 }

/** Case object for computing upper triangular pairwise combinations (i.e. r > l). */
case object Upper extends Comparer { def check(l: Position, r: Position): Boolean = r.compare(l) > 0 }

/** Case object for computing upper triangular or diagonal pairwise combinations (i.e. r >= l). */
case object UpperDiagonal extends Comparer { def check(l: Position, r: Position): Boolean = r.compare(l) >= 0 }

/** Case object for computing lower triangular pairwise combinations (i.e. l > r). */
case object Lower extends Comparer { def check(l: Position, r: Position): Boolean = l.compare(r) > 0 }

/** Case object for computing lower triangular or diagonal pairwise combinations (i.e. l >= r). */
case object LowerDiagonal extends Comparer { def check(l: Position, r: Position): Boolean = l.compare(r) >= 0 }

/** Base trait for pairwise operations. */
trait Operator

/** Base trait for computing pairwise values. */
trait Compute extends ComputeWithValue { self: Operator =>
  type V = Any

  def compute[P <: Position, D <: Dimension](slice: Slice[P, D], ext: V)(left: Cell[slice.S], right: Cell[slice.S],
    rem: slice.R): Collection[Cell[slice.R#M]] = compute(slice)(left, right, rem)

  /**
   * Indicate if the cell is selected as part of the sample.
   *
   * @param slice Encapsulates the dimension(s) along which to compute.
   * @param left  The selected left cell to compute with.
   * @param right The selected right cell to compute with.
   * @param rem   The remaining coordinates.
   *
   * @note The return value is a `Collection` to allow, for example, upper or lower triangular matrices to be returned
   *       (this can be done by comparing the selected coordinates)
   */
  def compute[P <: Position, D <: Dimension](slice: Slice[P, D])(left: Cell[slice.S], right: Cell[slice.S],
    rem: slice.R): Collection[Cell[slice.R#M]]
}

/** Base trait for computing pairwise values with a user provided value. */
trait ComputeWithValue { self: Operator =>
  /** Type of the external value. */
  type V

  /**
   * Indicate if the cell is selected as part of the sample.
   *
   * @param slice Encapsulates the dimension(s) along which to compute.
   * @param ext   The user define the value.
   * @param left  The selected left cell to compute with.
   * @param right The selected right cell to compute with.
   * @param rem   The remaining coordinates.
   *
   * @note The return value is a `Collection` to allow, for example, upper or lower triangular matrices to be returned
   *       (this can be done by comparing the selected coordinates)
   */
  def compute[P <: Position, D <: Dimension](slice: Slice[P, D], ext: V)(left: Cell[slice.S], right: Cell[slice.S],
    rem: slice.R): Collection[Cell[slice.R#M]]
}

/**
 * Operator that is a combination of one or more operators with `Compute`.
 *
 * @param singles `List` of operators that are combined together.
 *
 * @note This need not be called in an application. The `Operable` type class will convert any `List[Operator]`
 *       automatically to one of these.
 */
case class CombinationOperator[T <: Operator with Compute](singles: List[T]) extends Operator with Compute {
  def compute[P <: Position, D <: Dimension](slice: Slice[P, D])(left: Cell[slice.S], right: Cell[slice.S],
    rem: slice.R): Collection[Cell[slice.R#M]] = {
    Collection(singles.flatMap { case operator => operator.compute(slice)(left, right, rem).toList })
  }
}

/**
 * Operator that is a combination of one or more operators with `ComputeWithValue`.
 *
 * @param singles `List` of operators that are combined together.
 *
 * @note This need not be called in an application. The `OperableWithValue` type class will convert any
 *       `List[Operator]` automatically to one of these.
 */
case class CombinationOperatorWithValue[T <: Operator with ComputeWithValue { type V >: W }, W](
  singles: List[T]) extends Operator with ComputeWithValue {
  type V = W

  def compute[P <: Position, D <: Dimension](slice: Slice[P, D], ext: V)(left: Cell[slice.S], right: Cell[slice.S],
    rem: slice.R): Collection[Cell[slice.R#M]] = {
    Collection(singles.flatMap { case operator => operator.compute(slice, ext)(left, right, rem).toList })
  }
}

/** Type class for transforming a type `T` to a `Operator with Compute`. */
trait Operable[T] {
  /**
   * Returns a `Operator with Compute` for type `T`.
   *
   * @param t Object that can be converted to a `Operator with Compute`.
   */
  def convert(t: T): Operator with Compute
}

/** Companion object for the `Operable` type class. */
object Operable {
  /** Converts a `List[Operator with Compute]` to a single `Operator with Compute` using `CombinationOperator`. */
  implicit def LO2O[T <: Operator with Compute]: Operable[List[T]] = {
    new Operable[List[T]] { def convert(t: List[T]): Operator with Compute = CombinationOperator(t) }
  }
  /** Converts a `Operator with Compute` to a `Operator with Compute`; that is, it is a pass through. */
  implicit def O2O[T <: Operator with Compute]: Operable[T] = {
    new Operable[T] { def convert(t: T): Operator with Compute = t }
  }
}

/** Type class for transforming a type `T` to a `Operator with ComputeWithValue`. */
trait OperableWithValue[T, W] {
  /**
   * Returns a `Operator with ComputeWithValue` for type `T`.
   *
   * @param t Object that can be converted to a `Operator with ComputeWithValue`.
   */
  def convert(t: T): Operator with ComputeWithValue { type V >: W }
}

/** Companion object for the `OperableWithValue` type class. */
object OperableWithValue {
  /**
   * Converts a `List[Operator with ComputeWithValue]` to a single `Operator with ComputeWithValue` using
   * `CombinationOperatorWithValue`.
   */
  implicit def OT2OWV[T <: Operator with ComputeWithValue { type V >: W }, W]: OperableWithValue[List[T], W] = {
    new OperableWithValue[List[T], W] {
      def convert(t: List[T]): Operator with ComputeWithValue { type V >: W } = CombinationOperatorWithValue[T, W](t)
    }
  }
  /**
   * Converts a `Operator with ComputeWithValue` to a `Operator with ComputeWithValue`; that is, it is a pass through.
   */
  implicit def O2OWV[T <: Operator with ComputeWithValue { type V >: W }, W]: OperableWithValue[T, W] = {
    new OperableWithValue[T, W] { def convert(t: T): Operator with ComputeWithValue { type V >: W } = t }
  }
}

