// Copyright 2014,2015,2016 Commonwealth Bank of Australia
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
import au.com.cba.omnia.grimlock.framework.distribution._
import au.com.cba.omnia.grimlock.framework.encoding._
import au.com.cba.omnia.grimlock.framework.environment._
import au.com.cba.omnia.grimlock.framework.pairwise._
import au.com.cba.omnia.grimlock.framework.partition._
import au.com.cba.omnia.grimlock.framework.position._
import au.com.cba.omnia.grimlock.framework.sample._
import au.com.cba.omnia.grimlock.framework.squash._
import au.com.cba.omnia.grimlock.framework.transform._
import au.com.cba.omnia.grimlock.framework.utility._
import au.com.cba.omnia.grimlock.framework.window._

import org.apache.hadoop.io.Writable

import scala.reflect.ClassTag

/** Base trait for matrix operations. */
trait Matrix[P <: Position] extends Persist[Cell[P]] with UserData with DefaultTuners with PositionOrdering {

  /** Self-type of a specific implementation of this API. */
  type M <: Matrix[P]

  /** Specifies tuners permitted on a call to `change`. */
  type ChangeTuners[_]

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
  def change[I, T <: Tuner : ChangeTuners](slice: Slice[P], positions: I, schema: Content.Parser, tuner: T)(
    implicit ev1: PositionDistributable[I, slice.S, U], ev2: ClassTag[slice.S]): U[Cell[P]]

  /** Specifies tuners permitted on a call to `compact` functions. */
  type CompactTuners[_]

  /**
   * Compacts a matrix to a `Map`.
   *
   * @return A `E[Map[P, Content]]` containing the Map representation of this matrix.
   *
   * @note Avoid using this for very large matrices.
   */
  def compact()(implicit ev: ClassTag[P]): E[Map[P, Content]]

  /**
   * Compact a matrix to a `Map`.
   *
   * @param slice Encapsulates the dimension(s) along which to convert.
   * @param tuner The tuner for the job.
   *
   * @return A `E[Map[slice.S, Slice.C]]` containing the Map representation of this matrix.
   *
   * @note Avoid using this for very large matrices.
   */
  def compact[T <: Tuner : CompactTuners](slice: Slice[P], tuner: T)
                                         (implicit
                                          ev1: slice.S =!= Position0D,
                                          ev2: ClassTag[slice.S]
                                         ): E[Map[slice.S, slice.C]]

  /** Specifies tuners permitted on a call to `domain`. */
  type DomainTuners[T]

  /**
   * Return all possible positions of a matrix.
   *
   * @param tuner The tuner for the job.
   */
  def domain[T <: Tuner : DomainTuners](tuner: T): U[P]

  /** Specifies tuners permitted on a call to `fill` with hetrogeneous data. */
  type FillHeterogeneousTuners[_]

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
  def fillHeterogeneous[S <: Position, T <: Tuner : FillHeterogeneousTuners](slice: Slice[P], values: U[Cell[S]], tuner: T)(
    implicit ev1: ClassTag[P], ev2: ClassTag[slice.S], ev3: slice.S =:= S): U[Cell[P]]

  /** Specifies tuners permitted on a call to `fill` with homogeneous data. */
  type FillHomogeneousTuners[_]

  /**
   * Fill a matrix with `value`.
   *
   * @param value The content to fill a matrix with.
   * @param tuner The tuner for the job.
   *
   * @return A `U[Cell[P]]` where all missing values have been filled in.
   */
  def fillHomogeneous[T <: Tuner : FillHomogeneousTuners](value: Content, tuner: T)
                                                         (implicit ev1: ClassTag[P]): U[Cell[P]]

  /** Specifies tuners permitted on a call to `get`. */
  type GetTuners[_]

  /**
   * Return contents of a matrix at `positions`.
   *
   * @param positions The positions for which to get the contents.
   * @param tuner     The tuner for the job.
   *
   * @return A `U[Cell[P]]' of the `positions` together with their content.
   */
  def get[I, T <: Tuner : GetTuners](positions: I, tuner: T)(implicit ev1: PositionDistributable[I, P, U], ev2: ClassTag[P]): U[Cell[P]]

  /** Specifies tuners permitted on a call to `join`. */
  type JoinTuners[_]

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
  def join[T <: Tuner : JoinTuners](slice: Slice[P], that: M, tuner: T)(implicit ev1: P =!= Position1D, ev2: ClassTag[slice.S]): U[Cell[P]]

  /** Specifies tuners permitted on a call to `materialise`. */
  type MaterialiseTuners[_]

  /**
   * Returns the matrix as in in-memory list of cells.
   *
   * @param tuner The tuner for the job.
   *
   * @return A `L[Cell[P]]` of the cells.
   *
   * @note Avoid using this for very large matrices.
   */
  def materialise[T <: Tuner : MaterialiseTuners](tuner: T): List[Cell[P]]

  /** Specifies tuners permitted on a call to `names`. */
  type NamesTuners[_]

  /**
   * Returns the distinct position(s) (or names) for a given `slice`.
   *
   * @param slice Encapsulates the dimension(s) for which the names are to be returned.
   * @param tuner The tuner for the job.
   *
   * @return A `U[slice.S]` of the distinct position(s).
   */
  def names[T <: Tuner : NamesTuners](slice: Slice[P], tuner: T)
                       (implicit
                        ev1: slice.S =!= Position0D,
                        ev2: ClassTag[slice.S]): U[slice.S]

  /** Specifies tuners permitted on a call to `pairwise` functions. */
  type PairwiseTuners[_]

  /**
   * Compute pairwise values between all pairs of values given a slice.
   *
   * @param slice     Encapsulates the dimension(s) along which to compute values.
   * @param comparer  Defines which element the pairwise operations should apply to.
   * @param operators The pairwise operators to apply.
   * @param tuner     The tuner for the job.
   *
   * @return A `U[Cell[Q]]` where the content contains the pairwise values.
   */
  def pairwise[Q <: Position, T <: Tuner : PairwiseTuners]
  (slice: Slice[P], comparer: Comparer, operators: Operable[P, Q], tuner: T)
  (implicit ev1: slice.S =!= Position0D, ev2: PosExpDep[slice.R, Q], ev3: ClassTag[slice.S], ev4: ClassTag[slice.R]): U[Cell[Q]]

  /**
   * Compute pairwise values between all pairs of values given a slice with a user supplied value.
   *
   * @param slice     Encapsulates the dimension(s) along which to compute values.
   * @param comparer  Defines which element the pairwise operations should apply to.
   * @param operators The pairwise operators to apply.
   * @param value     The user supplied value.
   * @param tuner     The tuner for the job.
   *
   * @return A `U[Cell[Q]]` where the content contains the pairwise values.
   */
  def pairwiseWithValue[Q <: Position, W, T <: Tuner : PairwiseTuners](slice: Slice[P], comparer: Comparer,
    operators: OperableWithValue[P, Q, W], value: E[W], tuner: T)(implicit ev1: slice.S =!= Position0D,
      ev2: PosExpDep[slice.R, Q], ev3: ClassTag[slice.S], ev4: ClassTag[slice.R]): U[Cell[Q]]

  /**
   * Compute pairwise values between all values of this and that given a slice.
   *
   * @param slice     Encapsulates the dimension(s) along which to compute values.
   * @param comparer  Defines which element the pairwise operations should apply to.
   * @param that      Other matrix to compute pairwise values with.
   * @param operators The pairwise operators to apply.
   * @param tuner     The tuner for the job.
   *
   * @return A `U[Cell[Q]]` where the content contains the pairwise values.
   */
  def pairwiseBetween[Q <: Position, T <: Tuner : PairwiseTuners](slice: Slice[P], comparer: Comparer, that: M,
    operators: Operable[P, Q], tuner: T)(implicit ev1: slice.S =!= Position0D, ev2: PosExpDep[slice.R, Q],
      ev3: ClassTag[slice.S], ev4: ClassTag[slice.R]): U[Cell[Q]]

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
   * @return A `U[Cell[Q]]` where the content contains the pairwise values.
   */
  def pairwiseBetweenWithValue[Q <: Position, W, T <: Tuner : PairwiseTuners](slice: Slice[P], comparer: Comparer, that: M,
    operators: OperableWithValue[P, Q, W], value: E[W], tuner: T)(implicit ev1: slice.S =!= Position0D,
      ev2: PosExpDep[slice.R, Q], ev3: ClassTag[slice.S], ev4: ClassTag[slice.R]): U[Cell[Q]]

  /**
   * Relocate the coordinates of the cells.
   *
   * @param locate Function that relocates coordinates.
   *
   * @return A `U[Cell[Q]]` where the cells have been relocated.
   */
  def relocate[Q <: Position](locate: Locate.FromCell[P, Q])(implicit ev: PosIncDep[P, Q]): U[Cell[Q]]

  /**
   * Relocate the coordinates of the cells using user a suplied value.
   *
   * @param locate Function that relocates coordinates.
   * @param value  A `E` holding a user supplied value.
   *
   * @return A `U[Cell[Q]]` where the cells have been relocated.
   */
  def relocateWithValue[Q <: Position, W](locate: Locate.FromCellWithValue[P, Q, W], value: E[W])(
    implicit ev: PosIncDep[P, Q]): U[Cell[Q]]

  /**
   * Persist as a sparse matrix file (index, value).
   *
   * @param file       File to write to.
   * @param dictionary Pattern for the dictionary file name.
   * @param separator  Column separator to use in dictionary file.
   *
   * @return A `U[Cell[P]]`; that is it returns `data`.
   */
  def saveAsIV(file: String, dictionary: String = "%1$s.dict.%2$d", separator: String = "|")(
    implicit ctx: C): U[Cell[P]]

  /**
   * Persist to disk.
   *
   * @param file   Name of the output file.
   * @param writer Writer that converts `Cell[P]` to string.
   *
   * @return A `U[Cell[P]]`; that is it returns `data`.
   */
  def saveAsText(file: String, writer: TextWriter = Cell.toString())(implicit ctx: C): U[Cell[P]]

  /** Specifies tuners permitted on a call to `set` functions. */
  type SetTuners[_]

  /**
   * Set the `values` in a matrix.
   *
   * @param values The values to set.
   * @param tuner  The tuner for the job.
   *
   * @return A `U[Cell[P]]' with the `values` set.
   */
  def set[T <: Tuner : SetTuners](values: Matrixable[P, U], tuner: T)(implicit ev1: ClassTag[P]): U[Cell[P]]

  /** Specifies tuners permitted on a call to `shape`. */
  type ShapeTuners[_]

  /**
   * Returns the shape of the matrix.
   *
   * @param tuner The tuner for the job.
   *
   * @return A `U[Cell[Position1D]]`. The position consists of a string value with the name of the dimension
   *         (`dim.toString`). The content has the actual size in it as a discrete variable.
   */
  def shape[T <: Tuner : ShapeTuners](tuner: T): U[Cell[Position1D]]

  /** Specifies tuners permitted on a call to `shape`. */
  type SizeTuners[_]

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
  def size[T <: Tuner : SizeTuners](dim: Dimension, distinct: Boolean = false, tuner: T)
                                   (implicit ev1: PosDimDep[P, dim.D]): U[Cell[Position1D]]

  /** Specifies tuners permitted on a call to `slice`. */
  type SliceTuners[_]

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
  def slice[I, T <: Tuner : SliceTuners](slice: Slice[P], positions: I, keep: Boolean, tuner: T)(
    implicit ev1: PositionDistributable[I, slice.S, U], ev2: ClassTag[slice.S]): U[Cell[P]]

  /** Specifies tuners permitted on a call to `slide` functions. */
  type SlideTuners[_]

  /**
   * Create window based derived data.
   *
   * @param slice     Encapsulates the dimension(s) to slide over.
   * @param windows   The window functions to apply to the content.
   * @param ascending Indicator if the data should be sorted ascending or descending.
   * @param tuner     The tuner for the job.
   *
   * @return A `U[Cell[Q]]` with the derived data.
   */
  def slide[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position, T <: Tuner: SlideTuners](
    slice: Slice[P], windows: Windowable[P, S, R, Q], ascending: Boolean = true, tuner: T)(
      implicit ev1: slice.S =:= S, ev2: slice.R =:= R, ev3: slice.R =!= Position0D, ev4: PosExpDep[S, Q],
        ev5: ClassTag[slice.S], ev6: ClassTag[slice.R]): U[Cell[Q]]

  /**
   * Create window based derived data with a user supplied value.
   *
   * @param slice     Encapsulates the dimension(s) to slide over.
   * @param windows   The window functions to apply to the content.
   * @param value     A `E` holding a user supplied value.
   * @param ascending Indicator if the data should be sorted ascending or descending.
   * @param tuner     The tuner for the job.
   *
   * @return A `U[Cell[Q]]` with the derived data.
   */
  def slideWithValue[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position, W, T <: Tuner : SlideTuners](
    slice: Slice[P], windows: WindowableWithValue[P, S, R, Q, W], value: E[W], ascendig: Boolean = true, tuner: T)(
      implicit ev1: slice.S =:= S, ev2: slice.R =:= R, ev3: slice.R =!= Position0D, ev4: PosExpDep[S, Q],
        ev5: ClassTag[slice.S], ev6: ClassTag[slice.R]): U[Cell[Q]]

  /**
   * Partition a matrix according to `partitioner`.
   *
   * @param partitioners Assigns each position to zero, one or more partition(s).
   *
   * @return A `U[(I, Cell[P])]` where `I` is the partition for the corresponding tuple.
   */
  def split[I](partitioners: Partitionable[P, I]): U[(I, Cell[P])]

  /**
   * Partition a matrix according to `partitioner` using a user supplied value.
   *
   * @param partitioners Assigns each position to zero, one or more partition(s).
   * @param value        A `E` holding a user supplied value.
   *
   * @return A `U[(I, Cell[P])]` where `I` is the partition for the corresponding tuple.
   */
  def splitWithValue[I, W](partitioners: PartitionableWithValue[P, I, W], value: E[W]): U[(I, Cell[P])]

  /**
   * Stream this matrix through `command` and apply `script`.
   *
   * @param command   The command to stream (pipe) the data through.
   * @param files     A list of text files that will be available to `command`. Note that all files must be
   *                  located in the same directory as which the job is started.
   * @param writer    Function that converts a cell to a string (prior to streaming it through `command`).
   * @param parser    Function that parses the resulting string back to a cell.
   *
   * @return A `U[Cell[Q]]` with the new data as well as a `U[String]` with any parse errors.
   *
   * @note The `command` must be installed on each node of the cluster.
   */
  def stream[Q <: Position](command: String, files: List[String], writer: TextWriter,
    parser: Cell.TextParser[Q]): (U[Cell[Q]], U[String])

  /**
   * Sample a matrix according to some `sampler`. It keeps only those cells for which `sampler` returns true.
   *
   * @param samplers Sampling function(s).
   *
   * @return A `U[Cell[P]]` with the sampled cells.
   */
  def subset(samplers: Sampleable[P]): U[Cell[P]]

  /**
   * Sample a matrix according to some `sampler` using a user supplied value. It keeps only those cells for which
   * `sampler` returns true.
   *
   * @param samplers Sampling function(s).
   * @param value    A `E` holding a user supplied value.
   *
   * @return A `U[Cell[P]]` with the sampled cells.
   */
  def subsetWithValue[W](samplers: SampleableWithValue[P, W], value: E[W]): U[Cell[P]]

  /** Specifies tuners permitted on a call to `summarise` functions. */
  type SummariseTuners[_]

  /**
   * Summarise a matrix and return the aggregates.
   *
   * @param slice       Encapsulates the dimension(s) along which to aggregate.
   * @param aggregators The aggregator(s) to apply to the data.
   * @param tuner       The tuner for the job.
   *
   * @return A `U[Cell[Q]]` with the aggregates.
   */
  def summarise[S <: Position with ExpandablePosition, Q <: Position, T <: Tuner : SummariseTuners](slice: Slice[P],
    aggregators: Aggregatable[P, S, Q], tuner: T)(implicit ev1: slice.S =:= S, ev2: PosIncDep[S, Q],
      ev3: ClassTag[slice.S]): U[Cell[Q]]

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
  def summariseWithValue[S <: Position with ExpandablePosition, Q <: Position, W, T <: Tuner : SummariseTuners](slice: Slice[P],
    aggregators: AggregatableWithValue[P, S, Q, W], value: E[W], tuner: T)(implicit ev1: slice.S =:= S,
      ev2: PosIncDep[S, Q], ev3: ClassTag[slice.S]): U[Cell[Q]]

  /**
   * Convert all cells to key value tuples.
   *
   * @param writer The writer to convert a cell to key value tuple.
   *
   * @return A `U[(K, V)]` with all cells as key value tuples.
   */
  def toSequence[K <: Writable, V <: Writable](writer: SequenceWriter[K, V]): U[(K, V)]

  /**
   * Convert all cells to strings.
   *
   * @param writer The writer to convert a cell to string.
   *
   * @return A `U[String]` with all cells as string.
   */
  def toText(writer: TextWriter): U[String]

  /**
   * Merge all dimensions into a single.
   *
   * @param separator The separator to use when merging the coordinates.
   *
   * @return A `U[CellPosition1D]]` where all coordinates have been merged into a single string.
   */
  def toVector(separator: String = "|"): U[Cell[Position1D]]

  /**
   * Transform the content of a matrix.
   *
   * @param transformers The transformer(s) to apply to the content.
   *
   * @return A `U[Cell[Q]]` with the transformed cells.
   */
  def transform[Q <: Position](transformers: Transformable[P, Q])(implicit ev: PosIncDep[P, Q]): U[Cell[Q]]

  /**
   * Transform the content of a matrix using a user supplied value.
   *
   * @param transformers The transformer(s) to apply to the content.
   * @param value        A `E` holding a user supplied value.
   *
   * @return A `U[Cell[Q]]` with the transformed cells.
   */
  def transformWithValue[Q <: Position, W](transformers: TransformableWithValue[P, Q, W], value: E[W])(
    implicit ev: PosIncDep[P, Q]): U[Cell[Q]]

  /** Specifies tuners permitted on a call to `types`. */
  type TypesTuners[_]

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
  def types[T <: Tuner : TypesTuners](slice: Slice[P], specific: Boolean = false, tuner: T)(implicit ev1: slice.S =!= Position0D,
    ev2: ClassTag[slice.S]): U[(slice.S, Type)]

  /** Specifies tuners permitted on a call to `unique` functions. */
  type UniqueTuners[_]

  /**
   * Return the unique (distinct) contents of an entire matrix.
   *
   * @param tuner The tuner for the job.
   *
   * @note Comparison is performed based on the string representation of the `Content`.
   */
  def unique[T <: Tuner : UniqueTuners](tuner: T): U[Content]

  /**
   * Return the unique (distinct) contents along a dimension.
   *
   * @param slice Encapsulates the dimension(s) along which to find unique contents.
   * @param tuner The tuner for the job.
   *
   * @return A `U[(slice.S, Content)]` consisting of the unique values for each selected position.
   *
   * @note Comparison is performed based on the string representation of the `slice.S` and `Content`.
   */
  def uniqueByPositions[T <: Tuner : UniqueTuners](slice: Slice[P], tuner: T)
                                                  (implicit ev1: slice.S =!= Position0D): U[(slice.S, Content)]

  /** Specifies tuners permitted on a call to `which` functions. */
  type WhichTuners[_]

  /**
   * Query the contents of a matrix and return the positions of those that match the predicate.
   *
   * @param predicate The predicate used to filter the contents.
   *
   * @return A `U[P]` of the positions for which the content matches `predicate`.
   */
  def which(predicate: Cell.Predicate[P])(implicit ev: ClassTag[P]): U[P]

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
   */
  def whichByPositions[I, T <: Tuner : WhichTuners](slice: Slice[P], predicates: I, tuner: T)(
    implicit ev1: Predicateable[I, P, slice.S, U], ev2: ClassTag[slice.S], ev3: ClassTag[P]): U[P]

  // TODO: Add more compile-time type checking
  // TODO: Add label join operations
  // TODO: Add read/write[CSV|Hive|HBase|VW|LibSVM] operations
  // TODO: Is there a way not to use asInstanceOf[] as much?
  // TODO: Add machine learning operations (SVD/finding cliques/etc.) - use Spark instead?
}

/** Base trait for loading data into a matrix. */
trait Consume extends DistributedData with Environment {
  /**
   * Read column oriented, pipe separated matrix text data into a `U[Cell[P]]`.
   *
   * @param file   The text file to read from.
   * @param parser The parser that converts a single line to a cell.
   */
  def loadText[P <: Position](file: String, parser: Cell.TextParser[P])(implicit ctx: C): (U[Cell[P]], U[String])

  /**
   * Read binary key-value (sequence) matrix data into a `U[Cell[P]]`.
   *
   * @param file   The text file to read from.
   * @param parser The parser that converts a single key-value to a cell.
   */
  def loadSequence[K <: Writable, V <: Writable, P <: Position](file: String, parser: Cell.SequenceParser[K, V, P])(
    implicit ctx: C, ev1: Manifest[K], ev2: Manifest[V]): (U[Cell[P]], U[String])
}

/** Base trait for methods that reduce the number of dimensions or that can be filled. */
trait ReduceableMatrix[P <: Position with ReduceablePosition] { self: Matrix[P] =>
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
  def melt(dim: Dimension, into: Dimension, separator: String = ".")(implicit ev1: PosDimDep[P, dim.D],
    ev2: PosDimDep[P, into.D], ne: dim.D =!= into.D): U[Cell[P#L]]

  /** Specifies tuners permitted on a call to `squash` functions. */
  type SquashTuners[_]

  /**
   * Squash a dimension of a matrix.
   *
   * @param dim      The dimension to squash.
   * @param squasher The squasher that reduces two cells.
   * @param tuner    The tuner for the job.
   *
   * @return A `U[Cell[P#L]]` with the dimension `dim` removed.
   */
  def squash[T <: Tuner : SquashTuners](dim: Dimension, squasher: Squashable[P], tuner: T)(implicit ev1: PosDimDep[P, dim.D],
    ev2: ClassTag[P#L]): U[Cell[P#L]]

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
  def squashWithValue[W, T <: Tuner : SquashTuners](dim: Dimension, squasher: SquashableWithValue[P, W], value: E[W],
    tuner: T)(implicit ev1: PosDimDep[P, dim.D], ev2: ClassTag[P#L]): U[Cell[P#L]]
}

/** Base trait for methods that reshapes the number of dimension of a matrix. */
trait ReshapeableMatrix[P <: Position with ExpandablePosition with ReduceablePosition] { self: Matrix[P] =>
  type ReshapeTuners[_]

  /**
   * Reshape a coordinate into it's own dimension.
   *
   * @param dim        The dimension to reshape.
   * @param coordinate The coordinate (in `dim`) to reshape into its own dimension.
   * @param locate     A locator that defines the coordinate for the new dimension.
   * @param tuner      The tuner for the job.
   *
   * @return A `U[Cell[Q]]` with reshaped dimensions.
   */
  def reshape[Q <: Position, T <: Tuner : ReshapeTuners](dim: Dimension, coordinate: Valueable,
    locate: Locate.FromCellAndOptionalValue[P, Q], tuner: T)(implicit ev1: PosDimDep[P, dim.D], ev2: PosExpDep[P, Q],
      ev3: ClassTag[P#L]): U[Cell[Q]]
}

/** Base trait for 1D specific operations. */
trait Matrix1D extends Matrix[Position1D] with ApproximateDistribution[Position1D] { }

/** Base trait for 2D specific operations. */
trait Matrix2D extends Matrix[Position2D] with ReduceableMatrix[Position2D] with ReshapeableMatrix[Position2D]
  with ApproximateDistribution[Position2D] {
  /**
   * Permute the order of the coordinates in a position.
   *
   * @param dim1 Dimension to use for the first coordinate.
   * @param dim2 Dimension to use for the second coordinate.
   */
  def permute(dim1: Dimension, dim2: Dimension)(implicit ev1: PosDimDep[Position2D, dim1.D],
    ev2: PosDimDep[Position2D, dim2.D], ev3: dim1.D =!= dim2.D): U[Cell[Position2D]]

  /**
   * Persist as a CSV file.
   *
   * @param slice       Encapsulates the dimension that makes up the columns.
   * @param file        File to write to.
   * @param separator   Column separator to use.
   * @param escapee     The method for escaping the separator character.
   * @param writeHeader Indicator of the header should be written to a separate file.
   * @param header      Postfix for the header file name.
   * @param writeRowId  Indicator if row names should be written.
   * @param rowId       Column name of row names.
   *
   * @return A `TypedPipe[Cell[Position2D]]`; that is it returns `data`.
   */
  def saveAsCSV(slice: Slice[Position2D], file: String, separator: String = "|", escapee: Escape = Quote("|"),
    writeHeader: Boolean = true, header: String = "%s.header", writeRowId: Boolean = true, rowId: String = "id")(
      implicit ctx: C, ct: ClassTag[slice.S]): U[Cell[Position2D]]

  /**
   * Persist a `Matrix2D` as a Vowpal Wabbit file.
   *
   * @param slice      Encapsulates the dimension that makes up the columns.
   * @param file       File to write to.
   * @param dictionary Pattern for the dictionary file name, use `%``s` for the file name.
   * @param tag        Indicator if the selected position should be added as a tag.
   * @param separator  Separator to use in dictionary.
   *
   * @return A `TypedPipe[Cell[Position2D]]`; that is it returns `data`.
   */
  def saveAsVW(slice: Slice[Position2D], file: String, dictionary: String = "%s.dict", tag: Boolean = false,
    separator: String = "|")(implicit ctx: C, ct: ClassTag[slice.S]): U[Cell[Position2D]]

  /**
   * Persist a `Matrix2D` as a Vowpal Wabbit file with the provided labels.
   *
   * @param slice      Encapsulates the dimension that makes up the columns.
   * @param file       File to write to.
   * @param labels     The labels.
   * @param dictionary Pattern for the dictionary file name, use `%``s` for the file name.
   * @param tag        Indicator if the selected position should be added as a tag.
   * @param separator  Separator to use in dictionary.
   *
   * @return A `TypedPipe[Cell[Position2D]]`; that is it returns `data`.
   *
   * @note The labels are joined to the data keeping only those examples for which data and a label are available.
   */
  def saveAsVWWithLabels(slice: Slice[Position2D], file: String, labels: U[Cell[Position1D]],
    dictionary: String = "%s.dict", tag: Boolean = false, separator: String = "|")(implicit ctx: C,
      ct: ClassTag[slice.S]): U[Cell[Position2D]]

  /**
   * Persist a `Matrix2D` as a Vowpal Wabbit file with the provided importance weights.
   *
   * @param slice      Encapsulates the dimension that makes up the columns.
   * @param file       File to write to.
   * @param importance The importance weights.
   * @param dictionary Pattern for the dictionary file name, use `%``s` for the file name.
   * @param tag        Indicator if the selected position should be added as a tag.
   * @param separator  Separator to use in dictionary.
   *
   * @return A `TypedPipe[Cell[Position2D]]`; that is it returns `data`.
   *
   * @note The weights are joined to the data keeping only those examples for which data and a weight are available.
   */
  def saveAsVWWithImportance(slice: Slice[Position2D], file: String, importance: U[Cell[Position1D]],
    dictionary: String = "%s.dict", tag: Boolean = false, separator: String = "|")(implicit ctx: C,
      ct: ClassTag[slice.S]): U[Cell[Position2D]]

  /**
   * Persist a `Matrix2D` as a Vowpal Wabbit file with the provided labels and importance weights.
   *
   * @param slice      Encapsulates the dimension that makes up the columns.
   * @param file       File to write to.
   * @param labels     The labels.
   * @param importance The importance weights.
   * @param dictionary Pattern for the dictionary file name, use `%``s` for the file name.
   * @param tag        Indicator if the selected position should be added as a tag.
   * @param separator  Separator to use in dictionary.
   *
   * @return A `TypedPipe[Cell[Position2D]]`; that is it returns `data`.
   *
   * @note The labels and weights are joined to the data keeping only those examples for which data and a label
   *       and weight are available.
   */
  def saveAsVWWithLabelsAndImportance(slice: Slice[Position2D], file: String, labels: U[Cell[Position1D]],
    importance: U[Cell[Position1D]], dictionary: String = "%s.dict", tag: Boolean = false,
      separator: String = "|")(implicit ctx: C, ct: ClassTag[slice.S]): U[Cell[Position2D]]
}

/** Base trait for 3D specific operations. */
trait Matrix3D extends Matrix[Position3D] with ReduceableMatrix[Position3D] with ReshapeableMatrix[Position3D]
  with ApproximateDistribution[Position3D] {
  /**
   * Permute the order of the coordinates in a position.
   *
   * @param dim1 Dimension to use for the first coordinate.
   * @param dim2 Dimension to use for the second coordinate.
   * @param dim3 Dimension to use for the third coordinate.
   */
  def permute(dim1: Dimension, dim2: Dimension, dim3: Dimension)(implicit ev1: PosDimDep[Position3D, dim1.D],
    ev2: PosDimDep[Position3D, dim2.D], ev3: PosDimDep[Position3D, dim3.D],
      ev4: Distinct[(dim1.D, dim2.D, dim3.D)]): U[Cell[Position3D]]
}

/** Base trait for 4D specific operations. */
trait Matrix4D extends Matrix[Position4D] with ReduceableMatrix[Position4D] with ReshapeableMatrix[Position4D]
  with ApproximateDistribution[Position4D] {
  /**
   * Permute the order of the coordinates in a position.
   *
   * @param dim1 Dimension to use for the first coordinate.
   * @param dim2 Dimension to use for the second coordinate.
   * @param dim3 Dimension to use for the third coordinate.
   * @param dim4 Dimension to use for the fourth coordinate.
   */
 def permute(dim1: Dimension, dim2: Dimension, dim3: Dimension, dim4: Dimension)(
   implicit ev1: PosDimDep[Position4D, dim1.D], ev2: PosDimDep[Position4D, dim2.D],
     ev3: PosDimDep[Position4D, dim3.D], ev4: PosDimDep[Position4D, dim4.D],
       ev5: Distinct[(dim1.D, dim2.D, dim3.D, dim4.D)]): U[Cell[Position4D]]
}

/** Base trait for 5D specific operations. */
trait Matrix5D extends Matrix[Position5D] with ReduceableMatrix[Position5D]
  with ReshapeableMatrix[Position5D] with ApproximateDistribution[Position5D] {
  /**
   * Permute the order of the coordinates in a position.
   *
   * @param dim1 Dimension to use for the first coordinate.
   * @param dim2 Dimension to use for the second coordinate.
   * @param dim3 Dimension to use for the third coordinate.
   * @param dim4 Dimension to use for the fourth coordinate.
   * @param dim5 Dimension to use for the fifth coordinate.
   */
  def permute(dim1: Dimension, dim2: Dimension, dim3: Dimension, dim4: Dimension, dim5: Dimension)(
    implicit ev1: PosDimDep[Position5D, dim1.D], ev2: PosDimDep[Position5D, dim2.D],
      ev3: PosDimDep[Position5D, dim3.D], ev4: PosDimDep[Position5D, dim4.D], ev5: PosDimDep[Position5D, dim5.D],
        ev6: Distinct[(dim1.D, dim2.D, dim3.D, dim4.D, dim5.D)]): U[Cell[Position5D]]
}

/** Base trait for 6D specific operations. */
trait Matrix6D extends Matrix[Position6D] with ReduceableMatrix[Position6D] with ReshapeableMatrix[Position6D]
  with ApproximateDistribution[Position6D] {
  /**
   * Permute the order of the coordinates in a position.
   *
   * @param dim1 Dimension to use for the first coordinate.
   * @param dim2 Dimension to use for the second coordinate.
   * @param dim3 Dimension to use for the third coordinate.
   * @param dim4 Dimension to use for the fourth coordinate.
   * @param dim5 Dimension to use for the fifth coordinate.
   * @param dim6 Dimension to use for the sixth coordinate.
   */
  def permute(dim1: Dimension, dim2: Dimension, dim3: Dimension, dim4: Dimension, dim5: Dimension, dim6: Dimension)(
    implicit ev1: PosDimDep[Position6D, dim1.D], ev2: PosDimDep[Position6D, dim2.D],
      ev3: PosDimDep[Position6D, dim3.D], ev4: PosDimDep[Position6D, dim4.D],
        ev5: PosDimDep[Position6D, dim5.D], ev6: PosDimDep[Position6D, dim6.D],
          ev7: Distinct[(dim1.D, dim2.D, dim3.D, dim4.D, dim5.D, dim6.D)]): U[Cell[Position6D]]
}

/** Base trait for 7D specific operations. */
trait Matrix7D extends Matrix[Position7D] with ReduceableMatrix[Position7D] with ReshapeableMatrix[Position7D]
  with ApproximateDistribution[Position7D] {
  /**
   * Permute the order of the coordinates in a position.
   *
   * @param dim1 Dimension to use for the first coordinate.
   * @param dim2 Dimension to use for the second coordinate.
   * @param dim3 Dimension to use for the third coordinate.
   * @param dim4 Dimension to use for the fourth coordinate.
   * @param dim5 Dimension to use for the fifth coordinate.
   * @param dim6 Dimension to use for the sixth coordinate.
   * @param dim7 Dimension to use for the seventh coordinate.
   */
  def permute(dim1: Dimension, dim2: Dimension, dim3: Dimension, dim4: Dimension, dim5: Dimension, dim6: Dimension,
    dim7: Dimension)(implicit ev1: PosDimDep[Position7D, dim1.D], ev2: PosDimDep[Position7D, dim2.D],
      ev3: PosDimDep[Position7D, dim3.D], ev4: PosDimDep[Position7D, dim4.D], ev5: PosDimDep[Position7D, dim5.D],
        ev6: PosDimDep[Position7D, dim6.D], ev7: PosDimDep[Position7D, dim7.D],
          ev8: Distinct[(dim1.D, dim2.D, dim3.D, dim4.D, dim5.D, dim6.D, dim7.D)]): U[Cell[Position7D]]
}

/** Base trait for 8D specific operations. */
trait  Matrix8D extends Matrix[Position8D] with ReduceableMatrix[Position8D] with ReshapeableMatrix[Position8D]
  with ApproximateDistribution[Position8D] {
  /**
   * Permute the order of the coordinates in a position.
   *
   * @param dim1 Dimension to use for the first coordinate.
   * @param dim2 Dimension to use for the second coordinate.
   * @param dim3 Dimension to use for the third coordinate.
   * @param dim4 Dimension to use for the fourth coordinate.
   * @param dim5 Dimension to use for the fifth coordinate.
   * @param dim6 Dimension to use for the sixth coordinate.
   * @param dim7 Dimension to use for the seventh coordinate.
   * @param dim8 Dimension to use for the eighth coordinate.
   */
  def permute(dim1: Dimension, dim2: Dimension, dim3: Dimension, dim4: Dimension, dim5: Dimension, dim6: Dimension,
    dim7: Dimension, dim8: Dimension)(implicit ev1: PosDimDep[Position8D, dim1.D], ev2: PosDimDep[Position8D, dim2.D],
      ev3: PosDimDep[Position8D, dim3.D], ev4: PosDimDep[Position8D, dim4.D], ev5: PosDimDep[Position8D, dim5.D],
        ev6: PosDimDep[Position8D, dim6.D], ev7: PosDimDep[Position8D, dim7.D], ev8: PosDimDep[Position8D, dim8.D],
          ev9: Distinct[(dim1.D, dim2.D, dim3.D, dim4.D, dim5.D, dim6.D, dim7.D, dim8.D)]): U[Cell[Position8D]]
}

/** Base trait for 9D specific operations. */
trait Matrix9D extends Matrix[Position9D] with ReduceableMatrix[Position9D] with ApproximateDistribution[Position9D] {
  /**
   * Permute the order of the coordinates in a position.
   *
   * @param dim1 Dimension to use for the first coordinate.
   * @param dim2 Dimension to use for the second coordinate.
   * @param dim3 Dimension to use for the third coordinate.
   * @param dim4 Dimension to use for the fourth coordinate.
   * @param dim5 Dimension to use for the fifth coordinate.
   * @param dim6 Dimension to use for the sixth coordinate.
   * @param dim7 Dimension to use for the seventh coordinate.
   * @param dim8 Dimension to use for the eighth coordinate.
   * @param dim9 Dimension to use for the ninth coordinate.
   */
  def permute(dim1: Dimension, dim2: Dimension, dim3: Dimension, dim4: Dimension, dim5: Dimension, dim6: Dimension,
    dim7: Dimension, dim8: Dimension, dim9: Dimension)(implicit ev1: PosDimDep[Position9D, dim1.D],
      ev2: PosDimDep[Position9D, dim2.D], ev3: PosDimDep[Position9D, dim3.D], ev4: PosDimDep[Position9D, dim4.D],
        ev5: PosDimDep[Position9D, dim5.D], ev6: PosDimDep[Position9D, dim6.D], ev7: PosDimDep[Position9D, dim7.D],
          ev8: PosDimDep[Position9D, dim8.D], ev9: PosDimDep[Position9D, dim9.D],
            ev10: Distinct[(dim1.D, dim2.D, dim3.D, dim4.D, dim5.D, dim6.D, dim7.D, dim8.D, dim9.D)]): U[Cell[Position9D]]
}

/**
 * Convenience type for access results from `load` methods that return the data and any parse errors.
 *
 * @param data   The parsed matrix.
 * @param errors Any parse errors.
 */
case class MatrixWithParseErrors[P <: Position, U[_]](data: U[Cell[P]], errors: U[String])

/** Type class for transforming a type `T` into a `U[Cell[P]]`. */
trait Matrixable[P <: Position, U[_]] extends java.io.Serializable {
  /** Returns a `U[Cell[P]]` for this type `T`. */
  def apply(): U[Cell[P]]
}

/** Type class for transforming a type `T` to a `List[(U[S], Cell.Predicate[P])]`. */
trait Predicateable[T, P <: Position, S <: Position, U[_]] {
  /**
   * Returns a `List[(U[S], Cell.Predicate[P])]` for type `T`.
   *
   * @param t Object that can be converted to a `List[(U[S], Cell.Predicate[P])]`.
   */
  def convert(t: T): List[(U[S], Cell.Predicate[P])]
}

