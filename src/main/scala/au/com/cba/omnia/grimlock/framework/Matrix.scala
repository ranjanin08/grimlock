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

package au.com.cba.omnia.grimlock

import au.com.cba.omnia.grimlock.content._
import au.com.cba.omnia.grimlock.content.metadata._
import au.com.cba.omnia.grimlock.derive._
import au.com.cba.omnia.grimlock.encoding._
import au.com.cba.omnia.grimlock.pairwise._
import au.com.cba.omnia.grimlock.partition._
import au.com.cba.omnia.grimlock.position._
import au.com.cba.omnia.grimlock.reduce._
import au.com.cba.omnia.grimlock.sample._
import au.com.cba.omnia.grimlock.squash._
import au.com.cba.omnia.grimlock.transform._
import au.com.cba.omnia.grimlock.utility._

import scala.reflect._

/**
 * Cell in a matrix.
 *
 * @param position The position of the cell in the matri.
 * @param content  The contents of the cell.
 */
case class Cell[P <: Position](position: P, content: Content) {
  /**
   * Return string representation of a cell.
   *
   * @param separator   The separator to use between various fields.
   * @param descriptive Indicator if descriptive string is required or not.
   */
  def toString(separator: String, descriptive: Boolean): String = {
    descriptive match {
      case true => position.toString + separator + content.toString
      case false => position.toShortString(separator) + separator + content.toShortString(separator)
    }
  }
}

/** Base trait for matrix operations. */
trait Matrix[P <: Position] extends Persist[Cell[P]] {
  /** Type of the underlying data structure. */
  type U[_]

  /** Type of 'wrapper' around user defined data. */
  type E[_]

  /** Self-type of a specific implementation of this API. */
  type S <: Matrix[P]

  /** Predicate used in, for example, the `which` methods of a matrix for finding content. */
  type Predicate = Cell[P] => Boolean

  /**
   * Change the variable type of `positions` in a matrix.
   *
   * @param slice     Encapsulates the dimension(s) to change.
   * @param positions The position(s) within the dimension(s) to change.
   * @param schema    The schema to change to.
   *
   * @return A `U[Cell[P]]' with the changed contents.
   */
  def change[T, D <: Dimension](slice: Slice[P, D], positions: T, schema: Schema)(implicit ev1: PosDimDep[P, D],
    ev2: Nameable[T, P, slice.S, D, U], ev3: ClassTag[slice.S]): U[Cell[P]]

  /**
   * Create window based derived data.
   *
   * @param slice    Encapsulates the dimension(s) to derive over.
   * @param derivers The derivers to apply to the content.
   *
   * @return A `U[Cell[slice.S#M]]` with the derived data.
   */
  def derive[D <: Dimension, T](slice: Slice[P, D], derivers: T)(implicit ev1: PosDimDep[P, D], ev2: Derivable[T],
    ev3: slice.R =!= Position0D, ev4: ClassTag[slice.S]): U[Cell[slice.S#M]]

  /**
   * Create window based derived data with a user supplied value.
   *
   * @param slice    Encapsulates the dimension(s) to derive over.
   * @param derivers The derivers to apply to the content.
   * @param value    A `E` holding a user supplied value.
   *
   * @return A `U[Cell[slice.S#M]]` with the derived data.
   */
  def deriveWithValue[D <: Dimension, T, W](slice: Slice[P, D], derivers: T, value: E[W])(implicit ev1: PosDimDep[P, D],
    ev2: DerivableWithValue[T, W], ev3: slice.R =!= Position0D): U[Cell[slice.S#M]]

  /** Return all possible positions of a matrix. */
  def domain(): U[P]

  /**
   * Return contents of a matrix at `positions`.
   *
   * @param positions The positions for which to get the contents.
   *
   * @return A `U[Cell[P]]' of the `positions` together with their content.
   */
  def get[T](positions: T)(implicit ev1: PositionDistributable[T, P, U], ev2: ClassTag[P]): U[Cell[P]]

  /**
   * Join two matrices.
   *
   * @param slice Encapsulates the dimension(s) along which to join.
   * @param that  The matrix to join with.
   *
   * @return A `U[Cell[P]]` consisting of the inner-join of the two matrices.
   */
  // TODO: Add inner/left/right/outer join functionality?
  def join[D <: Dimension](slice: Slice[P, D], that: S)(implicit ev1: PosDimDep[P, D], ev2: P =!= Position1D,
    ev3: ClassTag[slice.S]): U[Cell[P]]

  /**
   * Returns the distinct position(s) (or names) for a given `slice`.
   *
   * @param slice Encapsulates the dimension(s) for which the names are to be returned.
   *
   * @return A `U[(Slice.S, Long)]` of the distinct position(s) together with a unique index.
   *
   * @note The position(s) are returned with an index so the return value can be used in various `persist` methods. The
   *       index itself is unique for each position but no ordering is defined.
   *
   * @see [[Names]]
   */
  def names[D <: Dimension](slice: Slice[P, D])(implicit ev1: PosDimDep[P, D], ev2: slice.S =!= Position0D,
    ev3: ClassTag[slice.S]): U[(slice.S, Long)]

  /**
   * Compute pairwise values between all pairs of values given a slice.
   *
   * @param slice     Encapsulates the dimension(s) along which to compute values.
   * @param operators The pairwise operators to apply.
   *
   * @return A `U[Cell[slice.R#M]]` where the content contains the pairwise values.
   */
  def pairwise[D <: Dimension, T](slice: Slice[P, D], operators: T)(implicit ev1: PosDimDep[P, D], ev2: Operable[T],
    ev3: slice.S =!= Position0D, ev4: ClassTag[slice.S], ev5: ClassTag[slice.R]): U[Cell[slice.R#M]]

  /**
   * Compute pairwise values between all pairs of values given a slice with a user supplied value.
   *
   * @param slice     Encapsulates the dimension(s) along which to compute values.
   * @param operators The pairwise operators to apply.
   * @param value     The user supplied value.
   *
   * @return A `U[Cell[slice.R#M]]` where the content contains the pairwise values.
   */
  def pairwiseWithValue[D <: Dimension, T, W](slice: Slice[P, D], operators: T, value: E[W])(
    implicit ev1: PosDimDep[P, D], ev2: OperableWithValue[T, W], ev3: slice.S =!= Position0D, ev4: ClassTag[slice.S],
      ev5: ClassTag[slice.R]): U[Cell[slice.R#M]]

  /**
   * Compute pairwise values between all values of this and that given a slice.
   *
   * @param slice     Encapsulates the dimension(s) along which to compute values.
   * @param that      Other matrix to compute pairwise values with.
   * @param operators The pairwise operators to apply.
   *
   * @return A `U[Cell[slice.R#M]]` where the content contains the pairwise values.
   */
  def pairwiseBetween[D <: Dimension, T](slice: Slice[P, D], that: S, operators: T)(implicit ev1: PosDimDep[P, D],
    ev2: Operable[T], ev3: slice.S =!= Position0D, ev4: ClassTag[slice.S], ev5: ClassTag[slice.R]): U[Cell[slice.R#M]]

  /**
   * Compute pairwise values between all values of this and that given a slice with a user supplied value.
   *
   * @param slice     Encapsulates the dimension(s) along which to compute values.
   * @param that      Other matrix to compute pairwise values with.
   * @param operators The pairwise operators to apply.
   * @param value     The user supplied value.
   *
   * @return A `U[Cell[slice.R#M]]` where the content contains the pairwise values.
   */
  def pairwiseBetweenWithValue[D <: Dimension, T, W](slice: Slice[P, D], that: S, operators: T, value: E[W])(
    implicit ev1: PosDimDep[P, D], ev2: OperableWithValue[T, W], ev3: slice.S =!= Position0D, ev4: ClassTag[slice.S],
      ev5: ClassTag[slice.R]): U[Cell[slice.R#M]]

  /**
   * Partition a matrix according to `partitioner`.
   *
   * @param partitioner Assigns each position to zero, one or more partitions.
   *
   * @return A `U[(S, Cell[P])]` where `T` is the partition for the corresponding tuple.
   */
  def partition[S: Ordering](partitioner: Partitioner with Assign { type T = S }): U[(S, Cell[P])]

  /**
   * Partition a matrix according to `partitioner` using a user supplied value.
   *
   * @param partitioner Assigns each position to zero, one or more partitions.
   * @param value       A `ValuePipe` holding a user supplied value.
   *
   * @return A `U[(S, Cell[P])]` where `T` is the partition for the corresponding tuple.
   */
  def partitionWithValue[S: Ordering, W](partitioner: Partitioner with AssignWithValue { type V >: W; type T = S },
    value: E[W]): U[(S, Cell[P])]

  /**
   * Reduce a matrix and return the reductions with an expanded position.
   *
   * @param slice    Encapsulates the dimension(s) along which to reduce.
   * @param reducers The reducer(s) to apply to the data.
   *
   * @return A `U[Cell[slice.S#M]]` with the aggregates.
   *
   * @note If the `slice` is an `Over` then the returned position will be a `Position2D` since `Slice.S` for `Over` is
   *       a `Position1D` and that expands to `Position2D`. Analogously, if the `slice` is an `Along` then the returned
   *       position will be equal to `P`.
   */
  def reduceAndExpand[T, D <: Dimension](slice: Slice[P, D], reducers: T)(implicit ev1: PosDimDep[P, D],
    ev2: ReducibleMultiple[T], ev3: ClassTag[slice.S]): U[Cell[slice.S#M]]

  /**
   * Reduce a matrix, using a user supplied value, and return the reductions with an expanded position.
   *
   * @param slice    Encapsulates the dimension(s) along which to reduce.
   * @param reducers The reducer(s) to apply to the data.
   * @param value    A `E` holding a user supplied value.
   *
   * @return A `U[Cell[slice.S#M]]` with the aggregates.
   *
   * @note If the `slice` is an `Over` then the returned position will be a `Position2D` since `Slice.S` for `Over` is
   *       a `Position1D` and that expands to `Position2D`. Analogously, if the `slice` is an `Along` then the returned
   *       position will be equal to `P`.
   */
  def reduceAndExpandWithValue[T, D <: Dimension, V](slice: Slice[P, D], reducers: T, value: E[V])(
    implicit ev1: PosDimDep[P, D], ev2: ReducibleMultipleWithValue[T, V], ev3: ClassTag[slice.S]): U[Cell[slice.S#M]]

  /**
   * Refine (filter) a matrix according to some function `f`. It keeps only those cells for which `f` returns true.
   *
   * @param f Filtering function.
   *
   * @return A `U[Cell[P]]` with the filtered cells.
   */
  def refine(f: Cell[P] => Boolean): U[Cell[P]]

  /**
   * Refine (filter) a matrix according to some function `f` using a user supplied value. It keeps only those cells for
   * which `f` returns true.
   *
   * @param f     Filtering function.
   * @param value A `E` holding a user supplied value.
   *
   * @return A `U[Cell[P]]` with the filtered cells.
   */
  def refineWithValue[V](f: (Cell[P], V) => Boolean, value: E[V]): U[Cell[P]]

  /**
   * Rename the coordinates of a dimension.
   *
   * @param dim     The dimension to rename.
   * @param renamer Function that renames coordinates.
   *
   * @return A `U[Cell[P]]` where the dimension `dim` has been renamed.
   */
  def rename[D <: Dimension](dim: D, renamer: (Dimension, Cell[P]) => P)(implicit ev: PosDimDep[P, D]): U[Cell[P]]

  /**
   * Rename the coordinates of a dimension using user a suplied value.
   *
   * @param dim     The dimension to rename.
   * @param renamer Function that renames coordinates.
   * @param value   A `E` holding a user supplied value.
   *
   * @return A `U[Cell[P]]` where the dimension `dim` has been renamed.
   */
  def renameWithValue[D <: Dimension, V](dim: D, renamer: (Dimension, Cell[P], V) => P, value: E[V])(
    implicit ev: PosDimDep[P, D]): U[Cell[P]]

  /**
   * Sample a matrix according to some `sampler`. It keeps only those cells for which `sampler` returns true.
   *
   * @param sampler Sampling function.
   *
   * @return A `U[Cell[P]]` with the sampled cells.
   */
  def sample(sampler: Sampler with Select): U[Cell[P]]

  /**
   * Sample a matrix according to some `sampler` using a user supplied value. It keeps only those cells for which
   * `sampler` returns true.
   *
   * @param sampler Sampling function.
   * @param value   A `E` holding a user supplied value.
   *
   * @return A `U[Cell[P]]` with the sampled cells.
   */
  def sampleWithValue[W](sampler: Sampler with SelectWithValue { type V >: W }, value: E[W]): U[Cell[P]]

  /**
   * Set `value` as the content for all `positions` in a matrix.
   *
   * @param positions The positions for which to set the contents.
   * @param value     The value to set.
   *
   * @return A `U[Cell[P]]' where the `positions` have `value` as their content.
   */
  def set[T](positions: T, value: Content)(implicit ev1: PositionDistributable[T, P, U], ev2: ClassTag[P]): U[Cell[P]]

  /**
   * Set the `values` in a matrix.
   *
   * @param values The values to set.
   *
   * @return A `U[Cell[P]]' with the `values` set.
   */
  def set[T](values: T)(implicit ev1: Matrixable[T, P, U], ev2: ClassTag[P]): U[Cell[P]]

  /**
   * Returns the shape of the matrix.
   *
   * @return A `U[Cell[Position1D]]`. The position consists of a string value with the name of the dimension
   *         (`dim.toString`). The content has the actual size in it as a discrete variable.
   */
  def shape(): U[Cell[Position1D]]

  /**
   * Returns the size of the matrix in dimension `dim`.
   *
   * @param dim      The dimension for which to get the size.
   * @param distinct Indicates if each coordinate in dimension `dim` occurs only once. If this is the case, then
   *                 enabling this flag has better run-time performance.
   *
   * @return A `U[Cell[Position1D]]`. The position consists of a string value with the name of the dimension
   *         (`dim.toString`). The content has the actual size in it as a discrete variable.
   */
  def size[D <: Dimension](dim: D, distinct: Boolean = false)(implicit ev: PosDimDep[P, D]): U[Cell[Position1D]]

  /**
   * Slice a matrix.
   *
   * @param slice     Encapsulates the dimension(s) to slice.
   * @param positions The position(s) within the dimension(s) to slice.
   * @param keep      Indicates if the `positions` should be kept or removed.
   *
   * @return A `U[Cell[P]]' of the remaining content.
   */
  def slice[T, D <: Dimension](slice: Slice[P, D], positions: T, keep: Boolean)(implicit ev1: PosDimDep[P, D],
    ev2: Nameable[T, P, slice.S, D, U], ev3: ClassTag[slice.S]): U[Cell[P]]

  /**
   * Convert a matrix to an in-memory `Map`.
   *
   * @param slice Encapsulates the dimension(s) along which to convert.
   *
   * @return A `E[Map[Slice.S, Slice.C]]` containing the Map representation of this matrix.
   *
   * @note Avoid using this for very large matrices.
   */
  def toMap[D <: Dimension](slice: Slice[P, D])(implicit ev1: PosDimDep[P, D], ev2: slice.S =!= Position0D,
    ev3: ClassTag[slice.S]): E[Map[slice.S, slice.C]]

  /**
   * Transform the content of a matrix.
   *
   * @param transformers The transformer(s) to apply to the content.
   *
   * @return A `U[Cell[P]]` with the transformed cells.
   */
  def transform[T](transformers: T)(implicit ev: Transformable[T]): U[Cell[P]]

  /**
   * Transform the content of a matrix using a user supplied value.
   *
   * @param transformers The transformer(s) to apply to the content.
   * @param value        A `E` holding a user supplied value.
   *
   * @return A `U[Cell[P]]` with the transformed cells.
   */
  def transformWithValue[T, V](transformers: T, value: E[V])(implicit ev: TransformableWithValue[T, V]): U[Cell[P]]

  /**
   * Returns the variable type of the content(s) for a given `slice`.
   *
   * @param slice    Encapsulates the dimension(s) for this the types are to be returned.
   * @param specific Indicates if the most specific type should be returned, or it's generalisation (default).
   *
   * @return A `U[(Slice.S, Type)]` of the distinct position(s) together with their type.
   *
   * @see [[Types]]
   */
  def types[D <: Dimension](slice: Slice[P, D], specific: Boolean = false)(implicit ev1: PosDimDep[P, D],
    ev2: slice.S =!= Position0D, ev3: ClassTag[slice.S]): U[(slice.S, Type)]

  /**
   * Return the unique (distinct) contents of an entire matrix.
   *
   * @note Comparison is performed based on the string representation of the `Content`.
   */
  def unique(): U[Content]

  /**
   * Return the unique (distinct) contents along a dimension.
   *
   * @param slice Encapsulates the dimension(s) along which to join.
   *
   * @return A `U[Cell[slice.S]]` consisting of the unique values.
   *
   * @note Comparison is performed based on the string representation of the `Cell[slice.S]`.
   */
  // TODO: Should this return a Cell? Coordinates are no longer unique in the matrix.
  def unique[D <: Dimension](slice: Slice[P, D])(implicit ev: slice.S =!= Position0D): U[Cell[slice.S]]

  /**
   * Query the contents of a matrix and return the positions of those that match the predicate.
   *
   * @param predicate The predicate used to filter the contents.
   *
   * @return A `U[P]' of the positions for which the content matches `predicate`.
   */
  def which(predicate: Predicate)(implicit ev: ClassTag[P]): U[P]

  /**
   * Query the contents of the `positions` of a matrix and return the positions of those that match the predicate.
   *
   * @param slice     Encapsulates the dimension(s) to query.
   * @param positions The position(s) within the dimension(s) to query.
   * @param predicate The predicate used to filter the contents.
   *
   * @return A `U[P]' of the positions for which the content matches `predicate`.
   */
  def which[T, D <: Dimension](slice: Slice[P, D], positions: T, predicate: Predicate)(implicit ev1: PosDimDep[P, D],
    ev2: Nameable[T, P, slice.S, D, U], ev3: ClassTag[slice.S], ev4: ClassTag[P]): U[P]

  /**
   * Query the contents of one of more positions of a matrix and return the positions of those that match the
   * corresponding predicates.
   *
   * @param slice   Encapsulates the dimension(s) to query.
   * @param pospred The list of position(s) within the dimension(s) to query together with the predicates used to
   *                filter the contents.
   *
   * @return A `U[P]' of the positions for which the content matches predicates.
   */
  def which[T, D <: Dimension](slice: Slice[P, D], pospred: List[(T, Predicate)])(implicit ev1: PosDimDep[P, D],
    ev2: Nameable[T, P, slice.S, D, U], ev3: ClassTag[slice.S], ev4: ClassTag[P]): U[P]

  protected def toString(t: Cell[P], separator: String, descriptive: Boolean): String = {
    t.toString(separator, descriptive)
  }

  protected implicit def PositionOrdering[T <: Position] = Position.Ordering[T]

  // TODO: Add more compile-time type checking
  // TODO: Add label join operations
  // TODO: Add read/write[CSV|Hive|VW|LibSVM] operations
  // TODO: Add statistics/dictionary into memory (from HDFS) operations
  // TODO: Is there a way not to use asInstanceOf[] as much?
  // TODO: Add machine learning operations (SVD/finding cliques/etc.) - use Spark instead?
}

/** Base trait for methods that reduce the number of dimensions or that can be filled. */
trait ReduceableMatrix[P <: Position with ReduceablePosition] { self: Matrix[P] =>
  /**
   * Fill a matrix with `values` for a given `slice`.
   *
   * @param slice  Encapsulates the dimension(s) on which to fill.
   * @param values The content to fill a matrix with.
   *
   * @return A `U[Cell[P]]` where all missing values have been filled in.
   *
   * @note This joins `values` onto this matrix, as such it can be used for imputing missing values.
   */
  // TODO: Is it possible to do this without currying `values`?
  def fillHetrogenous[D <: Dimension](slice: Slice[P, D])(values: U[Cell[slice.S]])(
    implicit ev: PosDimDep[P, D]): U[Cell[P]]

  /**
   * Fill a matrix with `value`.
   *
   * @param value The content to fill a matrix with.
   *
   * @return A `U[Cell[P]]` where all missing values have been filled in.
   */
  def fillHomogenous(value: Content): U[Cell[P]]

  /**
   * Melt one dimension of a matrix into another.
   *
   * @param dim       The dimension to melt
   * @param into      The dimension to melt into
   * @param separator The separator to use in the melt dimension
   *
   * @return A `U[Cell[P#L]]` with one fewer dimension.
   *
   * @note A melt coordinate is always a string value constructed from the string representation of the `dim` and
   *       `into` coordinates.
   */
  def melt[D <: Dimension, E <: Dimension](dim: D, into: E, separator: String = ".")(implicit ev1: PosDimDep[P, D],
    ev2: PosDimDep[P, E], ne: D =!= E): U[Cell[P#L]]

  /**
   * Reduce a matrix.
   *
   * @param slice   Encapsulates the dimension(s) along which to reduce.
   * @param reducer The reducer to apply to the data.
   *
   * @return A `U[Cell[slice.S]]` with the aggregates.
   */
  def reduce[D <: Dimension](slice: Slice[P, D], reducer: Reducer with Prepare with PresentSingle)(
    implicit ev: PosDimDep[P, D]): U[Cell[slice.S]]

  /**
   * Reduce a matrix, using a user supplied value.
   *
   * @param slice   Encapsulates the dimension(s) along which to reduce.
   * @param reducer The reducer to apply to the data.
   * @param value   A `E` holding a user supplied value.
   *
   * @return A `U[Cell[slice.S]]` with the aggregates.
   */
  def reduceWithValue[D <: Dimension, W](slice: Slice[P, D],
    reducer: Reducer with PrepareWithValue with PresentSingle { type V >: W }, value: E[W])(
      implicit ev: PosDimDep[P, D]): U[Cell[slice.S]]

  /**
   * Squash a dimension of a matrix.
   *
   * @param dim      The dimension to squash.
   * @param squasher The squasher that reduces two cells.
   *
   * @return A `U[Cell[P#L]]` with the dimension `dim` removed.
   */
  def squash[D <: Dimension](dim: D, squasher: Squasher with Reduce)(implicit ev: PosDimDep[P, D]): U[Cell[P#L]]

  /**
   * Squash a dimension of a matrix with a user supplied value.
   *
   * @param dim      The dimension to squash.
   * @param squasher The squasher that reduces two cells.
   * @param value    The user supplied value.
   *
   * @return A `U[Cell[P#L]]` with the dimension `dim` removed.
   */
  def squashWithValue[D <: Dimension, W](dim: D, squasher: Squasher with ReduceWithValue { type V >: W },
    value: E[W])(implicit ev: PosDimDep[P, D]): U[Cell[P#L]]
}

/** Base trait for methods that expand the number of dimension of a matrix. */
trait ExpandableMatrix[P <: Position with ExpandablePosition] { self: Matrix[P] =>
  /**
   * Expand a matrix with an extra dimension.
   *
   * @param expander A function that expands each position with 1 dimension.
   *
   * @return A `U[Cell[P#M]]` with a dimension added.
   */
  def expand(expander: Cell[P] => P#M): U[Cell[P#M]]

  /**
   * Expand a matrix with an extra dimension using a user supplied value.
   *
   * @param expander A function that expands each position with 1 dimension.
   * @param value    A `E` holding a user supplied value.
   *
   * @return A `U[Cell[P#M]]` with a dimension added.
   */
  def expandWithValue[V](expander: (Cell[P], V) => P#M, value: E[V]): U[Cell[P#M]]

  /**
   * Transform the content of a matrix and return the transformations with an expanded position.
   *
   * @param transformers The transformer(s) to apply to the content.
   *
   * @return A `U[Cell[P#M]]` with the transformed cells.
   */
  def transformAndExpand[T](transformers: T)(implicit ev: TransformableExpanded[T]): U[Cell[P#M]]

  /**
   * Transform the content of a matrix using a user supplied value, and return the transformations with an expanded
   * position.
   *
   * @param transformers The transformer(s) to apply to the content.
   * @param value        A `E` holding a user supplied value.
   *
   * @return A `U[Cell[P#M]]` with the transformed cells.
   */
  def transformAndExpandWithValue[T, V](transformers: T, value: E[V])(
    implicit ev: TransformableExpandedWithValue[T, V]): U[Cell[P#M]]
}

/** Type class for transforming a type `T` into a `U[Cell[P]]`. */
trait Matrixable[T, P <: Position, U[_]] {
  /**
   * Returns a `U[Cell[P]]` for type `T`.
   *
   * @param t Object that can be converted to a `U[Cell[P]]`.
   */
  def convert(t: T): U[Cell[P]]
}

