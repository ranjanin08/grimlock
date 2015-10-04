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

package au.com.cba.omnia.grimlock.framework

import au.com.cba.omnia.grimlock.framework.aggregate._
import au.com.cba.omnia.grimlock.framework.content._
import au.com.cba.omnia.grimlock.framework.content.metadata._
import au.com.cba.omnia.grimlock.framework.pairwise._
import au.com.cba.omnia.grimlock.framework.partition._
import au.com.cba.omnia.grimlock.framework.position._
import au.com.cba.omnia.grimlock.framework.sample._
import au.com.cba.omnia.grimlock.framework.squash._
import au.com.cba.omnia.grimlock.framework.transform._
import au.com.cba.omnia.grimlock.framework.utility._
import au.com.cba.omnia.grimlock.framework.utility.OneOf._
import au.com.cba.omnia.grimlock.framework.window._

import java.util.regex.Pattern

import scala.reflect.ClassTag

/** Base trait for matrix operations. */
trait Matrix[P <: Position] {
  /** Type of the underlying data structure. */
  type U[_]

  /** Type of 'wrapper' around user defined data. */
  type E[_]

  /** Self-type of a specific implementation of this API. */
  type S <: Matrix[P]

  /** Specifies tuners permitted on a call to `change`. */
  type ChangeTuners <: OneOf

  /**
   * Change the variable type of `positions` in a matrix.
   *
   * @param slice     Encapsulates the dimension(s) to change.
   * @param positions The position(s) within the dimension(s) to change.
   * @param schema    The schema to change to.
   * @param tuner     The tuner for the job.
   *
   * @return A `U[Cell[P]]' with the changed contents.
   */
  def change[I, T <: Tuner](slice: Slice[P], positions: I, schema: Schema, tuner: T)(
    implicit ev1: PositionDistributable[I, slice.S, U], ev2: ClassTag[slice.S], ev3: ChangeTuners#V[T]): U[Cell[P]]

  /** Specifies tuners permitted on a call to `domain`. */
  type DomainTuners <: OneOf

  /**
   * Return all possible positions of a matrix.
   *
   * @param tuner The tuner for the job.
   */
  def domain[T <: Tuner](tuner: T)(implicit ev: DomainTuners#V[T]): U[P]

  /** Specifies tuners permitted on a call to `get`. */
  type GetTuners <: OneOf

  /**
   * Return contents of a matrix at `positions`.
   *
   * @param positions The positions for which to get the contents.
   * @param tuner     The tuner for the job.
   *
   * @return A `U[Cell[P]]' of the `positions` together with their content.
   */
  def get[I, T <: Tuner](positions: I, tuner: T)(implicit ev1: PositionDistributable[I, P, U], ev2: ClassTag[P],
    ev3: GetTuners#V[T]): U[Cell[P]]

  /** Specifies tuners permitted on a call to `join`. */
  type JoinTuners <: OneOf

  /**
   * Join two matrices.
   *
   * @param slice Encapsulates the dimension(s) along which to join.
   * @param that  The matrix to join with.
   * @param tuner The tuner for the job.
   *
   * @return A `U[Cell[P]]` consisting of the inner-join of the two matrices.
   */
  // TODO: Add inner/left/right/outer join functionality?
  def join[T <: Tuner](slice: Slice[P], that: S, tuner: T)(implicit ev1: P =!= Position1D, ev2: ClassTag[slice.S],
    ev3: JoinTuners#V[T]): U[Cell[P]]

  /** Specifies tuners permitted on a call to `names`. */
  type NamesTuners <: OneOf

  /**
   * Returns the distinct position(s) (or names) for a given `slice`.
   *
   * @param slice Encapsulates the dimension(s) for which the names are to be returned.
   * @param tuner The tuner for the job.
   *
   * @return A `U[(slice.S, Long)]` of the distinct position(s) together with a unique index.
   */
  def names[T <: Tuner](slice: Slice[P], tuner: T)(implicit ev1: slice.S =!= Position0D, ev2: ClassTag[slice.S],
    ev3: NamesTuners#V[T]): U[slice.S]

  /** Specifies tuners permitted on a call to `pairwise` functions. */
  type PairwiseTuners <: OneOf

  /**
   * Compute pairwise values between all pairs of values given a slice.
   *
   * @param slice     Encapsulates the dimension(s) along which to compute values.
   * @param comparer  Defines which element the pairwise operations should apply to.
   * @param operators The pairwise operators to apply.
   * @param tuner     The tuner for the job.
   *
   * @return A `U[Cell[slice.R#M]]` where the content contains the pairwise values.
   */
  def pairwise[Q <: Position, F, T <: Tuner](slice: Slice[P], comparer: Comparer, operators: F, tuner: T)(
    implicit ev1: Operable[F, slice.S, slice.R, Q], ev2: slice.S =!= Position0D, ev3: ClassTag[slice.S],
      ev4: ClassTag[slice.R], ev5: PairwiseTuners#V[T]): U[Cell[Q]]

  /**
   * Compute pairwise values between all pairs of values given a slice with a user supplied value.
   *
   * @param slice     Encapsulates the dimension(s) along which to compute values.
   * @param comparer  Defines which element the pairwise operations should apply to.
   * @param operators The pairwise operators to apply.
   * @param value     The user supplied value.
   * @param tuner     The tuner for the job.
   *
   * @return A `U[Cell[slice.R#M]]` where the content contains the pairwise values.
   */
  def pairwiseWithValue[Q <: Position, F, W, T <: Tuner](slice: Slice[P], comparer: Comparer, operators: F,
    value: E[W], tuner: T)(implicit ev1: OperableWithValue[F, slice.S, slice.R, Q, W], ev2: slice.S =!= Position0D,
      ev3: ClassTag[slice.S], ev4: ClassTag[slice.R], ev5: PairwiseTuners#V[T]): U[Cell[Q]]

  /**
   * Compute pairwise values between all values of this and that given a slice.
   *
   * @param slice     Encapsulates the dimension(s) along which to compute values.
   * @param comparer  Defines which element the pairwise operations should apply to.
   * @param that      Other matrix to compute pairwise values with.
   * @param operators The pairwise operators to apply.
   * @param tuner     The tuner for the job.
   *
   * @return A `U[Cell[slice.R#M]]` where the content contains the pairwise values.
   */
  def pairwiseBetween[Q <: Position, F, T <: Tuner](slice: Slice[P], comparer: Comparer, that: S, operators: F,
    tuner: T)(implicit ev1: Operable[F, slice.S, slice.R, Q], ev2: slice.S =!= Position0D, ev3: ClassTag[slice.S],
      ev4: ClassTag[slice.R], ev5: PairwiseTuners#V[T]): U[Cell[Q]]

  /**
   * Compute pairwise values between all values of this and that given a slice with a user supplied value.
   *
   * @param slice     Encapsulates the dimension(s) along which to compute values.
   * @param comparer  Defines which element the pairwise operations should apply to.
   * @param that      Other matrix to compute pairwise values with.
   * @param operators The pairwise operators to apply.
   * @param value     The user supplied value.
   * @param tuner     The tuner for the job.
   *
   * @return A `U[Cell[slice.R#M]]` where the content contains the pairwise values.
   */
  def pairwiseBetweenWithValue[Q <: Position, F, W, T <: Tuner](slice: Slice[P], comparer: Comparer, that: S,
    operators: F, value: E[W], tuner: T)(implicit ev1: OperableWithValue[F, slice.S, slice.R, Q, W],
      ev2: slice.S =!= Position0D, ev3: ClassTag[slice.S], ev4: ClassTag[slice.R], ev5: PairwiseTuners#V[T]): U[Cell[Q]]

  /**
   * Rename the coordinates of a dimension.
   *
   * @param renamer Function that renames coordinates.
   *
   * @return A `U[Cell[P]]` where the position has been renamed.
   */
  def rename(renamer: (Cell[P]) => P): U[Cell[P]]

  /**
   * Rename the coordinates of a dimension using user a suplied value.
   *
   * @param renamer Function that renames coordinates.
   * @param value   A `E` holding a user supplied value.
   *
   * @return A `U[Cell[P]]` where the position has been renamed.
   */
  def renameWithValue[W](renamer: (Cell[P], W) => P, value: E[W]): U[Cell[P]]

  /**
   * Sample a matrix according to some `sampler`. It keeps only those cells for which `sampler` returns true.
   *
   * @param samplers Sampling function(s).
   *
   * @return A `U[Cell[P]]` with the sampled cells.
   */
  def sample[F](samplers: F)(implicit ev: Sampleable[F, P]): U[Cell[P]]

  /**
   * Sample a matrix according to some `sampler` using a user supplied value. It keeps only those cells for which
   * `sampler` returns true.
   *
   * @param samplers Sampling function(s).
   * @param value    A `E` holding a user supplied value.
   *
   * @return A `U[Cell[P]]` with the sampled cells.
   */
  def sampleWithValue[F, W](samplers: F, value: E[W])(implicit ev: SampleableWithValue[F, P, W]): U[Cell[P]]

  /** Specifies tuners permitted on a call to `set` functions. */
  type SetTuners <: OneOf

  /**
   * Set the `values` in a matrix.
   *
   * @param values The values to set.
   * @param tuner  The tuner for the job.
   *
   * @return A `U[Cell[P]]' with the `values` set.
   */
  def set[M, T <: Tuner](values: M, tuner: T)(implicit ev1: Matrixable[M, P, U], ev2: ClassTag[P],
    ev3: SetTuners#V[T]): U[Cell[P]]

  /** Specifies tuners permitted on a call to `shape`. */
  type ShapeTuners <: OneOf

  /**
   * Returns the shape of the matrix.
   *
   * @param tuner The tuner for the job.
   *
   * @return A `U[Cell[Position1D]]`. The position consists of a string value with the name of the dimension
   *         (`dim.toString`). The content has the actual size in it as a discrete variable.
   */
  def shape[T <: Tuner](tuner: T)(implicit ev: ShapeTuners#V[T]): U[Cell[Position1D]]

  /** Specifies tuners permitted on a call to `shape`. */
  type SizeTuners <: OneOf

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
  def size[D <: Dimension, T <: Tuner](dim: D, distinct: Boolean = false, tuner: T)(implicit ev1: PosDimDep[P, D],
    ev2: SizeTuners#V[T]): U[Cell[Position1D]]

  /** Specifies tuners permitted on a call to `slice`. */
  type SliceTuners <: OneOf

  /**
   * Slice a matrix.
   *
   * @param slice     Encapsulates the dimension(s) to slice.
   * @param positions The position(s) within the dimension(s) to slice.
   * @param keep      Indicates if the `positions` should be kept or removed.
   * @param tuner     The tuner for the job.
   *
   * @return A `U[Cell[P]]' of the remaining content.
   */
  def slice[I, T <: Tuner](slice: Slice[P], positions: I, keep: Boolean, tuner: T)(
    implicit ev1: PositionDistributable[I, slice.S, U], ev2: ClassTag[slice.S], ev3: SliceTuners#V[T]): U[Cell[P]]

  /** Specifies tuners permitted on a call to `slide` functions. */
  type SlideTuners <: OneOf

  /**
   * Create window based derived data.
   *
   * @param slice     Encapsulates the dimension(s) to slide over.
   * @param windows   The window functions to apply to the content.
   * @param ascending Indicator if the data should be sorted ascending or descending.
   * @param tuner     The tuner for the job.
   *
   * @return A `U[Cell[slice.S#M]]` with the derived data.
   */
  def slide[Q <: Position, F, T <: Tuner](slice: Slice[P], windows: F, ascending: Boolean = true, tuner: T)(
    implicit ev1: Windowable[F, slice.S, slice.R, Q], ev2: slice.R =!= Position0D, ev3: ClassTag[slice.S],
      ev4: ClassTag[slice.R], ev5: SlideTuners#V[T]): U[Cell[Q]]

  /**
   * Create window based derived data with a user supplied value.
   *
   * @param slice     Encapsulates the dimension(s) to slide over.
   * @param windows   The window functions to apply to the content.
   * @param value     A `E` holding a user supplied value.
   * @param ascending Indicator if the data should be sorted ascending or descending.
   * @param tuner     The tuner for the job.
   *
   * @return A `U[Cell[slice.S#M]]` with the derived data.
   */
  def slideWithValue[Q <: Position, F, W, T <: Tuner](slice: Slice[P], windows: F, value: E[W],
    ascendig: Boolean = true, tuner: T)(implicit ev1: WindowableWithValue[F, slice.S, slice.R, Q, W],
      ev2: slice.R =!= Position0D, ev3: ClassTag[slice.S], ev4: ClassTag[slice.R], ev5: SlideTuners#V[T]): U[Cell[Q]]

  /**
   * Partition a matrix according to `partitioner`.
   *
   * @param partitioners Assigns each position to zero, one or more partition(s).
   *
   * @return A `U[(Q, Cell[P])]` where `Q` is the partition for the corresponding tuple.
   */
  def split[Q, F](partitioners: F)(implicit ev: Partitionable[F, P, Q]): U[(Q, Cell[P])]

  /**
   * Partition a matrix according to `partitioner` using a user supplied value.
   *
   * @param partitioners Assigns each position to zero, one or more partition(s).
   * @param value        A `ValuePipe` holding a user supplied value.
   *
   * @return A `U[(Q, Cell[P])]` where `Q` is the partition for the corresponding tuple.
   */
  def splitWithValue[Q, F, W](partitioners: F, value: E[W])(
    implicit ev: PartitionableWithValue[F, P, Q, W]): U[(Q, Cell[P])]

  /**
   * Stream this matrix through `command` and apply `script`.
   *
   * @param command   The command to stream (pipe) the data through.
   * @param script    The script to apply to the data.
   * @param separator The separator to convert a cell to string.
   * @param parser    Function that parses the resulting string back to a cell.
   *
   * @return A `U[Cell[Q]]` with the new data as well as a `U[String]` with any parse errors.
   *
   * @note The `command` must be installed on each node of the cluster. Also, `script` must be a single self
   *       contained script. Lastly, `parser` functions are provided on the `Cell` object.
   */
  def stream[Q <: Position](command: String, script: String, separator: String,
    parser: String => TraversableOnce[Either[Cell[Q], String]]): (U[Cell[Q]], U[String])

  /** Specifies tuners permitted on a call to `summarise` functions. */
  type SummariseTuners <: OneOf

  /**
   * Summarise a matrix and return the aggregates.
   *
   * @param slice       Encapsulates the dimension(s) along which to aggregate.
   * @param aggregators The aggregator(s) to apply to the data.
   * @param tuner       The tuner for the job.
   *
   * @return A `U[Cell[Q]]` with the aggregates.
   */
  def summarise[Q <: Position, F, T <: Tuner](slice: Slice[P], aggregators: F, tuner: T)(
    implicit ev1: Aggregatable[F, P, slice.S, Q], ev2: ClassTag[slice.S], ev3: SummariseTuners#V[T]): U[Cell[Q]]

  /**
   * Summarise a matrix, using a user supplied value, and return the aggregates.
   *
   * @param slice       Encapsulates the dimension(s) along which to aggregate.
   * @param aggregators The aggregator(s) to apply to the data.
   * @param value       A `E` holding a user supplied value.
   * @param tuner       The tuner for the job.
   *
   * @return A `U[Cell[Q]]` with the aggregates.
   */
  def summariseWithValue[Q <: Position, F, W, T <: Tuner](slice: Slice[P], aggregators: F, value: E[W], tuner: T)(
    implicit ev1: AggregatableWithValue[F, P, slice.S, Q, W], ev2: ClassTag[slice.S],
      ev3: SummariseTuners#V[T]): U[Cell[Q]]

  /** Specifies tuners permitted on a call to `toMap` functions. */
  type ToMapTuners <: OneOf

  /**
   * Convert a matrix to an in-memory `Map`.
   *
   * @return A `E[Map[P, Content]]` containing the Map representation of this matrix.
   *
   * @note Avoid using this for very large matrices.
   */
  def toMap()(implicit ev: ClassTag[P]): E[Map[P, Content]]

  /**
   * Convert a matrix to an in-memory `Map`.
   *
   * @param slice Encapsulates the dimension(s) along which to convert.
   * @param tuner The tuner for the job.
   *
   * @return A `E[Map[slice.S, Slice.C]]` containing the Map representation of this matrix.
   *
   * @note Avoid using this for very large matrices.
   */
  def toMap[T <: Tuner](slice: Slice[P], tuner: T)(implicit ev1: slice.S =!= Position0D, ev2: ClassTag[slice.S],
    ev3: ToMapTuners#V[T]): E[Map[slice.S, slice.C]]

  /**
   * Transform the content of a matrix.
   *
   * @param transformers The transformer(s) to apply to the content.
   *
   * @return A `U[Cell[Q]]` with the transformed cells.
   */
  def transform[Q <: Position, F](transformers: F)(implicit ev: Transformable[F, P, Q]): U[Cell[Q]]

  /**
   * Transform the content of a matrix using a user supplied value.
   *
   * @param transformers The transformer(s) to apply to the content.
   * @param value        A `E` holding a user supplied value.
   *
   * @return A `U[Cell[P]]` with the transformed cells.
   */
  def transformWithValue[Q <: Position, F, W](transformers: F, value: E[W])(
    implicit ev: TransformableWithValue[F, P, Q, W]): U[Cell[Q]]

  /** Specifies tuners permitted on a call to `types`. */
  type TypesTuners <: OneOf

  /**
   * Returns the variable type of the content(s) for a given `slice`.
   *
   * @param slice    Encapsulates the dimension(s) for this the types are to be returned.
   * @param specific Indicates if the most specific type should be returned, or it's generalisation (default).
   * @param tuner    The tuner for the job.
   *
   * @return A `U[(slice.S, Type)]` of the distinct position(s) together with their type.
   *
   * @see [[Types]]
   */
  def types[T <: Tuner](slice: Slice[P], specific: Boolean = false, tuner: T)(implicit ev1: slice.S =!= Position0D,
    ev2: ClassTag[slice.S], ev3: TypesTuners#V[T]): U[(slice.S, Type)]

  /** Specifies tuners permitted on a call to `unique` functions. */
  type UniqueTuners <: OneOf

  /**
   * Return the unique (distinct) contents of an entire matrix.
   *
   * @param tuner The tuner for the job.
   *
   * @note Comparison is performed based on the string representation of the `Content`.
   */
  def unique[T <: Tuner](tuner: T)(implicit ev: UniqueTuners#V[T]): U[Content]

  /**
   * Return the unique (distinct) contents along a dimension.
   *
   * @param slice Encapsulates the dimension(s) along which to find unique contents.
   * @param tuner The tuner for the job.
   *
   * @return A `U[Cell[slice.S]]` consisting of the unique values.
   *
   * @note Comparison is performed based on the string representation of the `slice.S` and `Content`.
   */
  def uniqueByPositions[T <: Tuner](slice: Slice[P], tuner: T)(implicit ev1: slice.S =!= Position0D,
    ev2: UniqueTuners#V[T]): U[(slice.S, Content)]

  /** Specifies tuners permitted on a call to `which` functions. */
  type WhichTuners <: OneOf

  /**
   * Query the contents of a matrix and return the positions of those that match the predicate.
   *
   * @param predicate The predicate used to filter the contents.
   *
   * @return A `U[P]` of the positions for which the content matches `predicate`.
   */
  def which(predicate: Matrix.Predicate[P])(implicit ev: ClassTag[P]): U[P]

  /**
   * Query the contents of one of more positions of a matrix and return the positions of those that match the
   * corresponding predicates.
   *
   * @param slice      Encapsulates the dimension(s) to query.
   * @param predicates The position(s) within the dimension(s) to query together with the predicates used to
   *                   filter the contents.
   * @param tuner      The tuner for the job.
   *
   * @return A `U[P]` of the positions for which the content matches predicates.
   *
   * @note `predicates` can be anything that can be converted to a `List[(U[slice.S], Matrix.Predicate[P])]`.
   *       Supported conversions include (see `Predicateable` type classes for full list):
   *       `(Valueable[T], Matrix.Predicate[P])`, `(List[Valueable[T]], Matrix.Predicate[P])`,
   *       `(U[slice.S], Matrix.Predicate[P])`, `List[(Valueable[T], Matrix.Predicate[P])]`,
   *       `List[(List[Valueable[T]], Matrix.Predicate[P])]`, `List[(U[slice.S], Matrix.Predicate[P])]`.
   */
  def whichByPositions[I, T <: Tuner](slice: Slice[P], predicates: I, tuner: T)(
    implicit ev1: Predicateable[I, P, slice.S, U], ev2: ClassTag[slice.S], ev3: ClassTag[P],
      ev4: WhichTuners#V[T]): U[P]

  protected type TP1 = OneOf1[Default[NoParameters.type]]
  protected type TP2 = OneOf2[Default[NoParameters.type], Default[Reducers]]
  protected type TP3 = OneOf3[Default[NoParameters.type], Default[Reducers], Default[Sequence2[Reducers, Reducers]]]

  protected implicit def PositionOrdering[T <: Position] = Position.Ordering[T]()

  // TODO: Add more compile-time type checking
  // TODO: Add label join operations
  // TODO: Add read/write[CSV|Hive|HBase|VW|LibSVM] operations
  // TODO: Add statistics/dictionary into memory (from HDFS) operations
  // TODO: Is there a way not to use asInstanceOf[] as much?
  // TODO: Add machine learning operations (SVD/finding cliques/etc.) - use Spark instead?
}

/** Companion object to `Matrix` trait. */
object Matrix {
  /** Predicate used in, for example, the `which` methods of a matrix for finding content. */
  type Predicate[P <: Position] = Cell[P] => Boolean
}

/** Base trait for methods that reduce the number of dimensions or that can be filled. */
trait ReduceableMatrix[P <: Position with ReduceablePosition] { self: Matrix[P] =>
  /** Specifies tuners permitted on a call to `fill` with hetrogeneous data. */
  type FillHeterogeneousTuners <: OneOf

  /**
   * Fill a matrix with `values` for a given `slice`.
   *
   * @param slice  Encapsulates the dimension(s) on which to fill.
   * @param values The content to fill a matrix with.
   * @param tuner  The tuner for the job.
   *
   * @return A `U[Cell[P]]` where all missing values have been filled in.
   *
   * @note This joins `values` onto this matrix, as such it can be used for imputing missing values. As
   *       the join is an inner join, any positions in the matrix that aren't in `values` are filtered
   *       from the resulting matrix.
   */
  def fillHeterogeneous[Q <: Position, T <: Tuner](slice: Slice[P], values: U[Cell[Q]], tuner: T)(
    implicit ev1: ClassTag[P], ev2: ClassTag[slice.S], ev3: slice.S =:= Q,
      ev4: FillHeterogeneousTuners#V[T]): U[Cell[P]]

  /** Specifies tuners permitted on a call to `fill` with homogeneous data. */
  type FillHomogeneousTuners <: OneOf

  /**
   * Fill a matrix with `value`.
   *
   * @param value The content to fill a matrix with.
   * @param tuner The tuner for the job.
   *
   * @return A `U[Cell[P]]` where all missing values have been filled in.
   */
  def fillHomogeneous[T <: Tuner](value: Content, tuner: T)(implicit ev1: ClassTag[P],
    ev2: FillHomogeneousTuners#V[T]): U[Cell[P]]

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
  def melt[D <: Dimension, G <: Dimension](dim: D, into: G, separator: String = ".")(implicit ev1: PosDimDep[P, D],
    ev2: PosDimDep[P, G], ne: D =!= G): U[Cell[P#L]]

  /** Specifies tuners permitted on a call to `squash` functions. */
  type SquashTuners <: OneOf

  /**
   * Squash a dimension of a matrix.
   *
   * @param dim      The dimension to squash.
   * @param squasher The squasher that reduces two cells.
   * @param tuner    The tuner for the job.
   *
   * @return A `U[Cell[P#L]]` with the dimension `dim` removed.
   */
  def squash[D <: Dimension, F, T <: Tuner](dim: D, squasher: F, tuner: T)(implicit ev1: PosDimDep[P, D],
    ev2: Squashable[F, P], ev3: SquashTuners#V[T]): U[Cell[P#L]]

  /**
   * Squash a dimension of a matrix with a user supplied value.
   *
   * @param dim      The dimension to squash.
   * @param squasher The squasher that reduces two cells.
   * @param value    The user supplied value.
   * @param tuner    The tuner for the job.
   *
   * @return A `U[Cell[P#L]]` with the dimension `dim` removed.
   */
  def squashWithValue[D <: Dimension, F, W, T <: Tuner](dim: D, squasher: F, value: E[W], tuner: T)(
    implicit ev1: PosDimDep[P, D], ev2: SquashableWithValue[F, P, W], ev3: SquashTuners#V[T]): U[Cell[P#L]]

  /**
   * Merge all dimensions into a single.
   *
   * @param separator The separator to use when merging the coordinates.
   *
   * @return A `U[CellPosition1D]]` where all coordinates have been merged into a single string.
   */
  def toVector(separator: String = "|"): U[Cell[Position1D]]
}

/** Base trait for methods that expands the number of dimension of a matrix. */
trait ExpandableMatrix[P <: Position with ExpandablePosition] { self: Matrix[P] =>
  /**
   * Expand a matrix with extra dimension(s).
   *
   * @param expander A function that expands each position with extra dimension(s).
   *
   * @return A `U[Cell[Q]]` with extra dimension(s) added.
   */
  def expand[Q <: Position](expander: Cell[P] => Q)(implicit ev: PosExpDep[P, Q]): U[Cell[Q]]

  /**
   * Expand a matrix with extra dimension(s) using a user supplied value.
   *
   * @param expander A function that expands each position with extra dimension(s).
   * @param value    A `E` holding a user supplied value.
   *
   * @return A `U[Cell[Q]]` with extra dimension(s) added.
   */
  def expandWithValue[Q <: Position, W](expander: (Cell[P], W) => Q, value: E[W])(
    implicit ev: PosExpDep[P, Q]): U[Cell[Q]]
}

/**
 * Convenience type for access results from `load` methods that return the data and any parse errors.
 *
 * @param data   The parsed matrix.
 * @param errors Any parse errors.
 */
case class MatrixWithParseErrors[P <: Position, U[_]](data: U[Cell[P]], errors: U[String])

/** Type class for transforming a type `T` into a `U[Cell[P]]`. */
trait Matrixable[T, P <: Position, U[_]] {
  /**
   * Returns a `U[Cell[P]]` for type `T`.
   *
   * @param t Object that can be converted to a `U[Cell[P]]`.
   */
  def convert(t: T): U[Cell[P]]
}

/** Type class for transforming a type `T` to a `List[(U[S], Matrix.Predicate[P])]`. */
trait Predicateable[T, P <: Position, S <: Position, U[_]] {
  /**
   * Returns a `List[(U[S], Matrix.Predicate[P])]` for type `T`.
   *
   * @param t Object that can be converted to a `List[(U[S], Matrix.Predicate[P])]`.
   */
  def convert(t: T): List[(U[S], Matrix.Predicate[P])]
}

