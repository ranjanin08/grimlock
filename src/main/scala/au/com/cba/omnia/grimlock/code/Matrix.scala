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
import au.com.cba.omnia.grimlock.Matrix._
import au.com.cba.omnia.grimlock.pairwise._
import au.com.cba.omnia.grimlock.partition._
import au.com.cba.omnia.grimlock.position._
import au.com.cba.omnia.grimlock.reduce._
import au.com.cba.omnia.grimlock.sample._
import au.com.cba.omnia.grimlock.squash._
import au.com.cba.omnia.grimlock.transform._
import au.com.cba.omnia.grimlock.utility.{ =!=, Miscellaneous => Misc }

import cascading.flow.FlowDef
import com.twitter.scalding._
import com.twitter.scalding.TDsl._, Dsl._
import com.twitter.scalding.typed.{ IterablePipe, TypedSink }

/**
 * Rich wrapper around a `TypedPipe[Cell[P]]`.
 *
 * @param data `TypedPipe[Cell[P]]`.
 */
trait Matrix[P <: Position] extends Persist[Cell[P]] {
  protected val data: TypedPipe[Cell[P]]

  protected implicit def PositionOrdering[T <: Position] = new Ordering[T] { def compare(l: T, r: T) = l.compare(r) }

  /**
   * Returns the distinct position(s) (or names) for a given `slice`.
   *
   * @param slice Encapsulates the dimension(s) for which the names are to be returned.
   *
   * @return A Scalding `TypedPipe[(Slice.S, Long)]` of the distinct position(s) together with a unique index.
   *
   * @note The position(s) are returned with an index so the return value can be used in various `persist` methods. The
   *       index itself is unique for each position but no ordering is defined.
   *
   * @see [[Names]]
   */
  def names[D <: Dimension](slice: Slice[P, D])(implicit ev: PosDimDep[P, D]): TypedPipe[(slice.S, Long)] = {
    Names.number(data.map { case (p, c) => slice.selected(p) }.distinct)
  }

  /**
   * Returns the variable type of the content(s) for a given `slice`.
   *
   * @param slice    Encapsulates the dimension(s) for this the types are to be returned.
   * @param specific Indicates if the most specific type should be returned, or it's generalisation (default).
   *
   * @return A Scalding `TypedPipe[(Slice.S, Type)]` of the distinct position(s) together with their type.
   *
   * @see [[Types]]
   */
  def types[D <: Dimension](slice: Slice[P, D], specific: Boolean = false)(
    implicit ev: PosDimDep[P, D]): TypedPipe[(slice.S, Type)] = {
    data
      .map { case (p, c) => (slice.selected(p), c.schema.kind) }
      .groupBy { case (p, t) => p }
      .reduce[(slice.S, Type)] {
        case ((lp, lt), (rp, rt)) =>
          (lp,
            if (lt == rt) {
              lt
            } else if (!specific && lt.isSpecialisationOf(rt)) {
              rt
            } else if (!specific && rt.isSpecialisationOf(lt)) {
              lt
            } else {
              Type.Mixed
            })
      }
      .values
      .map { case (p, t) => (p, if (specific) t else t.getGeneralisation()) }
  }

  /**
   * Returns the size of the matrix in dimension `dim`.
   *
   * @param dim      The dimension for which to get the size.
   * @param distinct Indicates if the coordinates in dimension `dim` are unique. If this is the case, then enabling
   *                 this flag has better run-time performance.
   *
   * @return A Scalding `TypedPipe[Cell[Position1D]]`. The position consists of a string value with the name of the
   *         dimension (`dim.toString`). The content has the actual size in it as a discrete variable.
   */
  def size[D <: Dimension](dim: D, distinct: Boolean = false)(
    implicit ev: PosDimDep[P, D]): TypedPipe[Cell[Position1D]] = {
    val coords = data.map { case (p, c) => p.get(dim) }
    val dist = distinct match {
      case true => coords
      case false => coords.distinct(Value.Ordering)
    }

    dist
      .map { case _ => 1L }
      .sum
      .map { case sum => (Position1D(dim.toString), Content(DiscreteSchema[Codex.LongCodex](), sum)) }
  }

  /**
   * Returns the shape of the matrix.
   *
   * @return A Scalding `TypedPipe[Cell[Position1D]]`. The position consists of a string value with the name of
   *         the dimension (`dim.toString`). The content has the actual size in it as a discrete variable.
   */
  def shape(): TypedPipe[Cell[Position1D]] = {
    data
      .flatMap { case (p, c) => p.coordinates.map(_.toString).zipWithIndex }
      .distinct
      .groupBy { case (s, i) => i }
      .size
      .map { case (i, s) => (Position1D(Dimension.All(i).toString), Content(DiscreteSchema[Codex.LongCodex](), s)) }
  }

  /**
   * Slice a matrix.
   *
   * @param slice     Encapsulates the dimension(s) to slice.
   * @param positions The position(s) within the dimension(s) to slice.
   * @param keep      Indicates if the `positions` should be kept or removed.
   *
   * @return A Scalding `TypedPipe[Cell[P]]' of the remaining content.
   */
  def slice[T, D <: Dimension](slice: Slice[P, D], positions: T, keep: Boolean)(
    implicit ev1: Nameable[T, P, slice.S, D], ev2: PosDimDep[P, D]): TypedPipe[Cell[P]] = {
    val pos = ev1.convert(this, slice, positions)
    val wanted = keep match {
      case true => pos
      case false =>
        names(slice)
          .groupBy { case (p, i) => p }
          .leftJoin(pos.groupBy { case (p, i) => p })
          .flatMap {
            case (p, (lpi, rpi)) => rpi match {
              case Some(_) => None
              case None => Some(lpi)
            }
          }
    }

    data
      .groupBy { case (p, c) => slice.selected(p) }
      .join(wanted.groupBy { case (p, i) => p })
      .map { case (_, ((p, c), _)) => (p, c) }
  }

  /** Predicate used in, for example, the `which` methods of a matrix for finding content. */
  type Predicate = (P, Content) => Boolean

  /**
   * Query the contents of a matrix and return the positions of those that match the predicate.
   *
   * @param predicate The predicate used to filter the contents.
   *
   * @return A Scalding `TypedPipe[P]' of the positions for which the content matches `predicate`.
   */
  def which(predicate: Predicate): TypedPipe[P] = data.collect { case (p, c) if predicate(p, c) => p }

  /**
   * Query the contents of the `positions` of a matrix and return the positions of those that match the predicate.
   *
   * @param slice     Encapsulates the dimension(s) to query.
   * @param positions The position(s) within the dimension(s) to query.
   * @param predicate The predicate used to filter the contents.
   *
   * @return A Scalding `TypedPipe[P]' of the positions for which the content matches `predicate`.
   */
  def which[T, D <: Dimension](slice: Slice[P, D], positions: T, predicate: Predicate)(
    implicit ev1: Nameable[T, P, slice.S, D], ev2: PosDimDep[P, D]): TypedPipe[P] = {
    which(slice, List((positions, predicate)))
  }

  /**
   * Query the contents of one of more positions of a matrix and return the positions of those that match the
   * corresponding predicates.
   *
   * @param slice   Encapsulates the dimension(s) to query.
   * @param pospred The list of position(s) within the dimension(s) to query together with the predicates used to
   *                filter the contents.
   *
   * @return A Scalding `TypedPipe[P]' of the positions for which the content matches predicates.
   */
  def which[T, D <: Dimension](slice: Slice[P, D], pospred: List[(T, Predicate)])(
    implicit ev1: Nameable[T, P, slice.S, D], ev2: PosDimDep[P, D]): TypedPipe[P] = {

    val nampred = pospred.map { case (pos, pred) => ev1.convert(this, slice, pos).map { case (p, i) => (p, pred) } }
    val pipe = nampred.tail.foldLeft(nampred.head)((b, a) => b ++ a)

    data
      .groupBy { case (p, c) => slice.selected(p) }
      .join(pipe.groupBy { case (p, pred) => p })
      .collect { case (s, ((p, c), (_, predicate))) if predicate(p, c) => p }
  }

  /**
   * Return contents of a matrix at `positions`.
   *
   * @param positions The positions for which to get the contents.
   *
   * @return A Scalding `TypedPipe[Cell[P]]' of the `positions` together with their content.
   */
  def get[T](positions: T)(implicit ev: PositionPipeable[T, P]): TypedPipe[Cell[P]] = {
    data
      .groupBy { case (p, c) => p }
      .leftJoin(ev.convert(positions).groupBy { case p => p })
      .flatMap {
        case (_, ((p, c), po)) => po match {
          case Some(_) => Some((p, c))
          case None => None
        }
      }
  }

  /**
   * Convert a matrix to an in-memory `Map`.
   *
   * @param slice Encapsulates the dimension(s) along which to convert.
   *
   * @return A Scalding `ValuePipe[Map[Slice.S, Slice.C]]` containing the Map representation of this matrix.
   *
   * @note Avoid using this for very large matrices.
   */
  def toMap[D <: Dimension](slice: Slice[P, D])(implicit ev: PosDimDep[P, D]): ValuePipe[Map[slice.S, slice.C]] = {
    data
      .map { case (p, c) => (p, slice.toMap(p, c)) }
      .groupBy { case (p, m) => slice.selected(p) }
      .reduce[(P, Map[slice.S, slice.C])] { case ((lp, lm), (rp, rm)) => (lp, slice.combineMaps(lp, lm, rm)) }
      .map { case (_, (_, m)) => m }
      .sum(new com.twitter.algebird.Semigroup[Map[slice.S, slice.C]] {
        def plus(l: Map[slice.S, slice.C], r: Map[slice.S, slice.C]): Map[slice.S, slice.C] = l ++ r
      })
  }

  /**
   * Reduce a matrix and return the reductions with an expanded position.
   *
   * @param slice    Encapsulates the dimension(s) along which to reduce.
   * @param reducers The reducer(s) to apply to the data.
   *
   * @return A Scalding `TypedPipe[Cell[slice.S#M]]`.
   *
   * @note If the `slice` is an `Over` then the returned position will be a `Position2D` since `Slice.S` for `Over` is
   *       a `Position1D` and that expands to `Position2D`. Analogously, if the `slice` is an `Along` then the returned
   *       position will be equal to `P`.
   */
  def reduceAndExpand[T, D <: Dimension](slice: Slice[P, D], reducers: T)(implicit ev1: ReducerableMultiple[T],
    ev2: PosDimDep[P, D]): TypedPipe[Cell[slice.S#M]] = {
    val reducer = ev1.convert(reducers)

    data
      .map { case (p, c) => (slice.selected(p), reducer.prepare(slice, p, c)) }
      .groupBy { case (p, t) => p }
      .reduce[(slice.S, reducer.T)] { case ((lp, lt), (rp, rt)) => (lp, reducer.reduce(lt, rt)) }
      .values
      .flatMap { case (p, t) => Misc.mapFlatten(reducer.presentMultiple(p, t)) }
  }

  /**
   * Reduce a matrix, using a user supplied value, and return the reductions with an expanded position.
   *
   * @param slice    Encapsulates the dimension(s) along which to reduce.
   * @param reducers The reducer(s) to apply to the data.
   * @param value    A `ValuePipe` holding a user supplied value.
   *
   * @return A Scalding `TypedPipe[Cell[slice.S#M]]`.
   *
   * @note If the `slice` is an `Over` then the returned position will be a `Position2D` since `Slice.S` for `Over` is
   *       a `Position1D` and that expands to `Position2D`. Analogously, if the `slice` is an `Along` then the returned
   *       position will be equal to `P`.
   */
  def reduceAndExpandWithValue[T, D <: Dimension, V](slice: Slice[P, D], reducers: T, value: ValuePipe[V])(
    implicit ev1: ReducerableMultipleWithValue[T, V], ev2: PosDimDep[P, D]): TypedPipe[Cell[slice.S#M]] = {
    val reducer = ev1.convert(reducers)

    data
      .leftCross(value)
      .map { case ((p, c), vo) => (slice.selected(p), reducer.prepare(slice, p, c, vo.get.asInstanceOf[reducer.V])) }
      .groupBy { case (p, t) => p }
      .reduce[(slice.S, reducer.T)] { case ((lp, lt), (rp, rt)) => (lp, reducer.reduce(lt, rt)) }
      .values
      .flatMap { case (p, t) => Misc.mapFlatten(reducer.presentMultiple(p, t)) }
  }

  /**
   * Partition a matrix according to `partitioner`.
   *
   * @param partitioner Assigns each position to zero, one or more partitions.
   *
   * @return A Scalding `TypedPipe[(S, Cell[P])]` where `T` is the partition for the corresponding tuple.
   */
  def partition[S: Ordering](partitioner: Partitioner with Assign { type T = S }): TypedPipe[(S, Cell[P])] = {
    data.flatMap { case (p, c) => Misc.mapFlatten(partitioner.assign(p), (p, c)) }
  }

  /**
   * Partition a matrix according to `partitioner` using a user supplied value.
   *
   * @param partitioner Assigns each position to zero, one or more partitions.
   * @param value       A `ValuePipe` holding a user supplied value.
   *
   * @return A Scalding `TypedPipe[(S, Cell[P])]` where `T` is the partition for the corresponding tuple.
   */
  def partitionWithValue[S: Ordering, W](partitioner: Partitioner with AssignWithValue { type V >: W; type T = S },
    value: ValuePipe[W]): TypedPipe[(S, Cell[P])] = {
    data.flatMapWithValue(value) { case ((p, c), vo) => Misc.mapFlatten(partitioner.assign(p, vo.get), (p, c)) }
  }

  /**
   * Refine (filter) a matrix according to some function `f`. It keeps only those cells for which `f` returns true.
   *
   * @param f Filtering function.
   *
   * @return A Scalding `TypedPipe[Cell[P]]`.
   */
  def refine(f: (P, Content) => Boolean): TypedPipe[Cell[P]] = data.filter { case (p, c) => f(p, c) }

  /**
   * Refine (filter) a matrix according to some function `f` using a user supplied value. It keeps only those cells for
   * which `f` returns true.
   *
   * @param f     Filtering function.
   * @param value A `ValuePipe` holding a user supplied value.
   *
   * @return A Scalding `TypedPipe[Cell[P]]`.
   */
  def refineWithValue[V](f: (P, Content, V) => Boolean, value: ValuePipe[V]): TypedPipe[Cell[P]] = {
    data.filterWithValue(value) { case ((p, c), vo) => f(p, c, vo.get) }
  }

  /**
   * Sample a matrix according to some function `f`. It keeps only those cells for which `f` returns true.
   *
   * @param sampler Sampling function.
   *
   * @return A Scalding `TypedPipe[Cell[P]]`.
   */
  def sample(sampler: Sampler with Select): TypedPipe[Cell[P]] = data.filter { case (p, c) => sampler.select(p) }

  /**
   * Sample a matrix according to some function `f` using a user supplied value. It keeps only those cells for which
   * `f` returns true.
   *
   * @param sampler Sampling function.
   * @param value A `ValuePipe` holding a user supplied value.
   *
   * @return A Scalding `TypedPipe[Cell[P]]`.
   */
  def sampleWithValue[W](sampler: Sampler with SelectWithValue { type V >: W },
    value: ValuePipe[W]): TypedPipe[Cell[P]] = {
    data.filterWithValue(value) { case ((p, c), vo) => sampler.select(p, vo.get) }
  }

  /**
   * Return all possible positions of a matrix.
   *
   * @return A Scalding `TypedPipe[P]`
   */
  def domain(): TypedPipe[P]

  /**
   * Join two matrices.
   *
   * @param slice Encapsulates the dimension(s) along which to join.
   * @param that  The matrix to join with.
   *
   * @return A Scalding `TypedPipe[Cell[P]]` consisting of the inner-join of the two matrices.
   */
  def join[D <: Dimension](slice: Slice[P, D], that: Matrix[P])(implicit ev: PosDimDep[P, D]): TypedPipe[Cell[P]] = {
    val keep = names(slice)
      .groupBy { case (p, i) => p }
      .join(that.names(slice).groupBy { case (p, i) => p })

    data
      .groupBy { case (p, c) => slice.selected(p) }
      .join(keep)
      .values
      .map { case ((p, c), pi) => (p, c) } ++
      that
      .data
      .groupBy { case (p, c) => slice.selected(p) }
      .join(keep)
      .values
      .map { case ((p, c), pi) => (p, c) }
  }

  /** Return the unique (distinct) contents of an entire matrix. */
  def unique(): TypedPipe[Content] = {
    implicit def ContentOrdering: Ordering[Content] = new Ordering[Content] {
      def compare(l: Content, r: Content) = l.toString.compare(r.toString)
    }

    data
      .map { case (p, c) => c }
      .distinct
  }

  /**
   * Return the unique (distinct) contents along a dimension
   *
   * @param slice Encapsulates the dimension(s) along which to join.
   *
   * @return A Scalding `TypedPipe[Cell[slice.S]]` consisting of the unique values.
   */
  def unique[D <: Dimension](slice: Slice[P, D]): TypedPipe[Cell[slice.S]] = {
    implicit def CellOrdering: Ordering[Cell[slice.S]] = new Ordering[Cell[slice.S]] {
      def compare(l: Cell[slice.S], r: Cell[slice.S]) = l.toString.compare(r.toString)
    }

    data
      .map { case (p, c) => (slice.selected(p), c) }
      .distinct
  }

  protected def toString(t: Cell[P], separator: String, descriptive: Boolean): String = {
    descriptive match {
      case true => t._1.toString + separator + t._2.toString
      case false => t._1.toShortString(separator) + separator + t._2.toShortString(separator)
    }
  }

  private def pairwise[D <: Dimension](slice: Slice[P, D])(implicit ev: PosDimDep[P, D]) = {
    val wanted = names(slice).map { case (p, i) => p }
    val values = data.groupBy { case (p, c) => (slice.selected(p), slice.remainder(p)) }

    wanted
      .cross(wanted)
      .cross(names(slice.inverse).asInstanceOf[TypedPipe[(slice.R, Long)]])
      .map { case ((l, r), (o, i)) => (l, r, o) }
      .groupBy { case (l, r, o) => (l, o) }
      .join(values)
      .groupBy { case (_, ((l, r, o), x)) => (r, o) }
      .join(values)
      .map { case (_, ((_, ((lp, rp, r), (_, lc))), (_, rc))) => ((lp, lc), (rp, rc), r) }
  }

  /**
   * Compute pairwise values between all pairs of values given a slice.
   *
   * @param slice    Encapsulates the dimension(s) along which to compute values.
   * @param operator The pairwise operator to apply.
   *
   * @return A Scalding `TypedPipe[Cell[slice.R#M]]` where the content contains the pairwise value.
   */
  def pairwise[D <: Dimension](slice: Slice[P, D], operator: Operator with Compute)(
    implicit ev: PosDimDep[P, D]): TypedPipe[Cell[slice.R#M]] = {
    pairwise(slice).flatMap { case ((lp, lc), (rp, rc), r) => operator.compute(slice, lp, lc, rp, rc, r) }
  }

  /**
   * Compute pairwise values between all pairs of values given a slice with a user supplied value.
   *
   * @param slice    Encapsulates the dimension(s) along which to compute values.
   * @param operator The pairwise operator to apply.
   * @param value    The user supplied value.
   *
   * @return A Scalding `TypedPipe[Cell[slice.R#M]]` where the content contains the pairwise value.
   */
  def pairwiseWithValue[D <: Dimension, W](slice: Slice[P, D], operator: Operator with ComputeWithValue { type V >: W },
    value: ValuePipe[W])(implicit ev: PosDimDep[P, D]): TypedPipe[Cell[slice.R#M]] = {
    pairwise(slice)
      .flatMapWithValue(value) {
        case (((lp, lc), (rp, rc), r), vo) => operator.compute(slice, lp, lc, rp, rc, r, vo.get)
      }
  }

  private def pairwiseBetween[D <: Dimension](slice: Slice[P, D], that: Matrix[P])(implicit ev: PosDimDep[P, D]) = {
    val thisWanted = names(slice).map { case (p, i) => p }
    val thisValues = data.groupBy { case (p, c) => (slice.selected(p), slice.remainder(p)) }

    val thatWanted = that.names(slice).map { case (p, i) => p }
    val thatValues = that.data.groupBy { case (p, c) => (slice.selected(p), slice.remainder(p)) }

    thisWanted
      .cross(thatWanted)
      .cross(names(slice.inverse).asInstanceOf[TypedPipe[(slice.R, Long)]])
      .map { case ((l, r), (o, i)) => (l, r, o) }
      .groupBy { case (l, r, o) => (l, o) }
      .join(thisValues)
      .groupBy { case (_, ((l, r, o), x)) => (r, o) }
      .join(thatValues)
      .map { case (_, ((_, ((lp, rp, r), (_, lc))), (_, rc))) => ((lp, lc), (rp, rc), r) }
  }

  /**
   * Compute pairwise values between all values of this and that given a slice.
   *
   * @param slice    Encapsulates the dimension(s) along which to compute values.
   * @param that     Other matrix to compute pairwise values with.
   * @param operator The pairwise operator to apply.
   *
   * @return A Scalding `TypedPipe[Cell[slice.R#M]]` where the content contains the pairwise value.
   */
  def pairwiseBetween[D <: Dimension](slice: Slice[P, D], that: Matrix[P], operator: Operator with Compute)(
    implicit ev: PosDimDep[P, D]): TypedPipe[Cell[slice.R#M]] = {
    pairwiseBetween(slice, that).flatMap { case ((lp, lc), (rp, rc), r) => operator.compute(slice, lp, lc, rp, rc, r) }
  }

  /**
   * Compute pairwise values between all values of this and that given a slice with a user supplied value.
   *
   * @param slice    Encapsulates the dimension(s) along which to compute values.
   * @param that     Other matrix to compute pairwise values with.
   * @param operator The pairwise operator to apply.
   * @param value    The user supplied value.
   *
   * @return A Scalding `TypedPipe[Cell[slice.R#M]]` where the content contains the pairwise value.
   */
  def pairwiseBetweenWithValue[D <: Dimension, W](slice: Slice[P, D], that: Matrix[P],
    operator: Operator with ComputeWithValue { type V >: W }, value: ValuePipe[W])(
      implicit ev: PosDimDep[P, D]): TypedPipe[Cell[slice.R#M]] = {
    pairwiseBetween(slice, that)
      .flatMapWithValue(value) {
        case (((lp, lc), (rp, rc), r), vo) => operator.compute(slice, lp, lc, rp, rc, r, vo.get)
      }
  }

  protected def persistDictionary(names: TypedPipe[(Position1D, Long)], file: String, dictionary: String,
    separator: String)(implicit flow: FlowDef, mode: Mode) = {
    val dict = names.groupBy { case (p, i) => p }

    dict
      .map { case (_, (p, i)) => p.toShortString(separator) + separator + i }
      .write(TypedSink(TextLine(dictionary.format(file))))

    dict
  }

  protected def persistDictionary(names: TypedPipe[(Position1D, Long)], file: String, dictionary: String,
    separator: String, dim: Dimension)(implicit flow: FlowDef, mode: Mode) = {
    val dict = names.groupBy { case (p, j) => p }

    dict
      .map { case (_, (p, i)) => p.toShortString(separator) + separator + i }
      .write(TypedSink(TextLine(dictionary.format(file, dim.index))))

    dict
  }

  // TODO: Add more compile-time type checking
  // TODO: Add label join operations
  // TODO: Add read/write[CSV|Hive|VW|LibSVM] operations
  // TODO: Add statistics/dictionary into memory (from HDFS) operations
  // TODO: Is there a way not to use asInstanceOf[]?
  // TODO: Add machine learning operations (SVD/finding cliques/etc.) - use Spark instead?
}

/** Define operations that modify a matrix. */
trait ModifiableMatrix[P <: Position with ModifiablePosition] { self: Matrix[P] =>
  /**
   * Change the variable type of `positions` in a matrix.
   *
   * @param slice     Encapsulates the dimension(s) to change.
   * @param positions The position(s) within the dimension(s) to change.
   * @param schema    The schema to change to.
   *
   * @return A Scalding `TypedPipe[Cell[P]]' of the changed contents.
   */
  def change[T, D <: Dimension](slice: Slice[P, D], positions: T, schema: Schema)(
    implicit ev1: Nameable[T, P, slice.S, D], ev2: PosDimDep[P, D]): TypedPipe[Cell[P]] = {
    data
      .groupBy { case (p, c) => slice.selected(p) }
      .leftJoin(ev1.convert(this, slice, positions).groupBy { case (p, i) => p })
      .values
      .flatMap {
        case ((p, c), po) => po match {
          case Some(_) => schema.decode(c.value.toShortString).map { case con => (p, con) }
          case None => Some((p, c))
        }
      }
  }

  /**
   * Set `value` as the content for all `positions` in a matrix.
   *
   * @param positions The positions for which to set the contents.
   * @param value     The value to set.
   *
   * @return A Scalding `TypedPipe[Cell[P]]' where the `positions` have `value` as their content.
   */
  def set[T](positions: T, value: Content)(implicit ev: PositionPipeable[T, P]): TypedPipe[Cell[P]] = {
    set((positions, value))
  }

  /**
   * Set the `values` in a matrix.
   *
   * @param values The values to set.
   *
   * @return A Scalding `TypedPipe[Cell[P]]' with the `values` set.
   */
  def set[T](values: T)(implicit ev: Matrixable[T, P]): TypedPipe[Cell[P]] = {
    data
      .groupBy { case (p, c) => p }
      .leftJoin(ev.convert(values).groupBy { case (p, c) => p })
      .map {
        case (_, ((p, c), co)) => co match {
          case Some((sp, sc)) => (sp, sc)
          case None => (p, c)
        }
      }
  }

  /**
   * Transform the content of a matrix.
   *
   * @param transformers The transformer(s) to apply to the content.
   *
   * @return A Scalding `TypedPipe[Cell[P#S]]`
   */
  def transform[T](transformers: T)(implicit ev: Transformable[T]): TypedPipe[Cell[P#S]] = {
    val t = ev.convert(transformers)

    data.flatMap { case (p, c) => Misc.mapFlatten(t.present(p, c)) }
  }

  /**
   * Transform the content of a matrix using a user supplied value.
   *
   * @param transformers The transformer(s) to apply to the content.
   * @param value        A `ValuePipe` holding a user supplied value.
   *
   * @return A Scalding `TypedPipe[Cell[P#S]]`
   */
  def transformWithValue[T, V](transformers: T, value: ValuePipe[V])(
    implicit ev: TransformableWithValue[T, V]): TypedPipe[Cell[P#S]] = {
    val t = ev.convert(transformers)

    data.flatMapWithValue(value) { case ((p, c), vo) => Misc.mapFlatten(t.present(p, c, vo.get.asInstanceOf[t.V])) }
  }

  /**
   * Create window based derived data.
   *
   * @param slice   Encapsulates the dimension(s) to derive over.
   * @param deriver The deriver to apply to the content.
   *
   * @return A Scalding `TypedPipe[Cell[slice.S#M]]` with the derived data.
   */
  def derive[D <: Dimension](slice: Slice[P, D], deriver: Deriver with Initialise)(
    implicit ev: PosDimDep[P, D]): TypedPipe[Cell[slice.S#M]] = {
    data
      .map { case (p, c) => (slice.selected(p), slice.remainder(p), c) }
      .groupBy { case (s, r, c) => s }
      .sortBy { case (s, r, c) => r }
      .scanLeft(Option.empty[(deriver.T, CellCollection[slice.S#M])]) {
        case (None, (s, r, c)) => Some((deriver.initialise(s, r, c), None))
        case (Some((t, _)), (s, r, c)) => Some(deriver.present(s, r, c, t))
      }
      .flatMap {
        case (p, Some((t, oe @ Some(_)))) => Misc.mapFlatten(oe)
        case _ => List()
      }
  }

  /**
   * Create window based derived data with a user supplied value.
   *
   * @param slice   Encapsulates the dimension(s) to derive over.
   * @param deriver The deriver to apply to the content.
   * @param value   A `ValuePipe` holding a user supplied value.
   *
   * @return A Scalding `TypedPipe[Cell[slice.S#M]]` with the derived data.
   */
  def deriveWithValue[D <: Dimension, W](slice: Slice[P, D], deriver: Deriver with InitialiseWithValue { type V >: W },
    value: ValuePipe[W])(implicit ev: PosDimDep[P, D]): TypedPipe[Cell[slice.S#M]] = {
    data
      .leftCross(value)
      .map { case ((p, c), vo) => (slice.selected(p), slice.remainder(p), c, vo) }
      .groupBy { case (s, r, c, vo) => s }
      .sortBy { case (s, r, c, vo) => r }
      .scanLeft(Option.empty[(deriver.T, CellCollection[slice.S#M])]) {
        case (None, (s, r, c, vo)) => Some((deriver.initialise(s, r, c, vo.get.asInstanceOf[deriver.V]), None))
        case (Some((t, _)), (s, r, c, vo)) => Some(deriver.present(s, r, c, t))
      }
      .flatMap {
        case (p, Some((t, oe @ Some(_)))) => Misc.mapFlatten(oe)
        case _ => List()
      }
  }

  /**
   * Fill a matrix with `value`.
   *
   * @param value The content to fill a matrix with.
   *
   * @return A Scalding `TypedPipe[Cell[P]]` where all missing values have been filled in.
   */
  def fill(value: Content): TypedPipe[Cell[P]] = {
    domain
      .groupBy { case p => p }
      .leftJoin(data.groupBy { case (p, c) => p })
      .map {
        case (p, (_, co)) => co match {
          case Some((_, c)) => (p, c)
          case None => (p, value)
        }
      }
  }

  /**
   * Fill a matrix with `values` for a given `slice`.
   *
   * @param slice  Encapsulates the dimension(s) on which to fill.
   * @param values The content to fill a matrix with.
   *
   * @return A Scalding `TypedPipe[Cell[P]]` where all missing values have been filled in.
   *
   * @note This joins `values` onto this matrix, as such it can be used for imputing missing values.
   */
  def fill[D <: Dimension](slice: Slice[P, D], values: TypedPipe[Cell[P]])(
    implicit ev: PosDimDep[P, D]): TypedPipe[Cell[P]] = {
    val dense = domain
      .groupBy { case p => slice.selected(p) }
      .join(values.groupBy { case (p, c) => slice.selected(p) })
      .map { case (_, (p, (_, c))) => (p, c) }

    dense
      .groupBy { case (p, c) => p }
      .leftJoin(data.groupBy { case (p, c) => p })
      .map {
        case (p, ((_, fc), co)) => co match {
          case Some((_, c)) => (p, c)
          case None => (p, fc)
        }
      }
  }

  /**
   * Rename the coordinates of a dimension.
   *
   * @param dim     The dimension to rename.
   * @param renamer Function that renames coordinates.
   *
   * @return A Scalding `TypedPipe[Cell[P#S]]` where the dimension `dim` has been renamed.
   */
  def rename[D <: Dimension](dim: D, renamer: (Dimension, P, Content) => P#S)(
    implicit ev: PosDimDep[P, D]): TypedPipe[Cell[P#S]] = data.map { case (p, c) => (renamer(dim, p, c), c) }

  /**
   * Rename the coordinates of a dimension using user a suplied value.
   *
   * @param dim     The dimension to rename.
   * @param renamer Function that renames coordinates.
   * @param value   A `ValuePipe` holding a user supplied value.
   *
   * @return A Scalding `TypedPipe[Cell[P#S]]` where the dimension `dim` has been renamed.
   */
  def renameWithValue[D <: Dimension, V](dim: D, renamer: (Dimension, P, Content, V) => P#S, value: ValuePipe[V])(
    implicit ev: PosDimDep[P, D]): TypedPipe[Cell[P#S]] = {
    data.mapWithValue(value) { case ((p, c), vo) => (renamer(dim, p, c, vo.get), c) }
  }
}

/** Define operations that reduce a matrix's dimensions. */
trait ReduceableMatrix[P <: Position with ReduceablePosition] { self: Matrix[P] =>
  /**
   * Squash a dimension of a matrix.
   *
   * @param dim      The dimension to squash.
   * @param squasher The squasher that reduces two cells.
   *
   * @return A Scalding `TypedPipe[Cell[P#L]]`.
   */
  def squash[D <: Dimension](dim: D, squasher: Squasher with Reduce)(
    implicit ev: PosDimDep[P, D]): TypedPipe[Cell[P#L]] = {
    data
      .groupBy { case (p, c) => p.remove(dim) }
      .reduce[Cell[P]] { case ((xp, xc), (yp, yc)) => squasher.reduce(dim, xp, xc, yp, yc) }
      .map { case (p, (_, c)) => (p, c) }
  }

  /**
   * Squash a dimension of a matrix with a user supplied value.
   *
   * @param dim      The dimension to squash.
   * @param squasher The squasher that reduces two cells.
   * @param value    The user supplied value.
   *
   * @return A Scalding `TypedPipe[Cell[P#L]]`.
   */
  def squashWithValue[D <: Dimension, V](dim: D, squasher: Squasher with ReduceWithValue, value: ValuePipe[V])(
    implicit ev: PosDimDep[P, D]): TypedPipe[Cell[P#L]] = {
    data
      .leftCross(value)
      .groupBy { case ((p, c), vo) => p.remove(dim) }
      .reduce[(Cell[P], Option[V])] {
        case (((xp, xc), xvo), ((yp, yc), yvo)) => (squasher.reduce(dim, xp, xc, yp, yc,
          xvo.get.asInstanceOf[squasher.V]), xvo)
      }
      .map { case (p, ((_, c), _)) => (p, c) }
  }

  /**
   * Melt one dimension of a matrix into another.
   *
   * @param dim       The dimension to melt
   * @param into      The dimension to melt into
   * @param separator The separator to use in the melt dimension
   *
   * @return A Scalding `TypedPipe[Cell[P#L]]`.
   *
   * @note A melt coordinate is always a string value constructed from the string representation of the `dim` and
   *       `into` coordinates.
   */
  def melt[D <: Dimension, E <: Dimension](dim: D, into: E, separator: String = ".")(implicit ev1: PosDimDep[P, D],
    ev2: PosDimDep[P, E], ne: D =!= E): TypedPipe[Cell[P#L]] = {
    data.map { case (p, c) => (p.melt(dim, into, separator), c) }
  }

  /**
   * Reduce a matrix.
   *
   * @param slice   Encapsulates the dimension(s) along which to reduce.
   * @param reducer The reducer to apply to the data.
   *
   * @return A Scalding `TypedPipe[Cell[slice.S]]`.
   */
  def reduce[D <: Dimension](slice: Slice[P, D], reducer: Reducer with Prepare with PresentSingle)(
    implicit ev: PosDimDep[P, D]): TypedPipe[Cell[slice.S]] = {
    data
      .map { case (p, c) => (slice.selected(p), reducer.prepare(slice, p, c)) }
      .groupBy { case (p, t) => p }
      .reduce[(slice.S, reducer.T)] { case ((lp, lt), (rp, rt)) => (lp, reducer.reduce(lt, rt)) }
      .values
      .flatMap { case (p, t) => reducer.presentSingle(p, t) }
  }

  /**
   * Reduce a matrix, using a user supplied value.
   *
   * @param slice   Encapsulates the dimension(s) along which to reduce.
   * @param reducer The reducer to apply to the data.
   * @param value   A `ValuePipe` holding a user supplied value.
   *
   * @return A Scalding `TypedPipe[Cell[slice.S]]`.
   */
  def reduceWithValue[D <: Dimension, W](slice: Slice[P, D],
    reducer: Reducer with PrepareWithValue with PresentSingle { type V >: W }, value: ValuePipe[W])(
      implicit ev: PosDimDep[P, D]): TypedPipe[Cell[slice.S]] = {
    data
      .leftCross(value)
      .map { case ((p, c), vo) => (slice.selected(p), reducer.prepare(slice, p, c, vo.get)) }
      .groupBy { case (p, t) => p }
      .reduce[(slice.S, reducer.T)] { case ((lp, lt), (rp, rt)) => (lp, reducer.reduce(lt, rt)) }
      .values
      .flatMap { case (p, t) => reducer.presentSingle(p, t) }
  }
}

/** Define operations that expand a matrix's dimensions. */
trait ExpandableMatrix[P <: Position with ExpandablePosition] { self: Matrix[P] =>
  /**
   * Transform the content of a matrix and return the transformations with an expanded position.
   *
   * @param transformers The transformer(s) to apply to the content.
   *
   * @return A Scalding `TypedPipe[Cell[P#M]]`
   */
  def transformAndExpand[T](transformers: T)(implicit ev: TransformableExpanded[T]): TypedPipe[Cell[P#M]] = {
    val t = ev.convert(transformers)

    data.flatMap { case (p, c) => Misc.mapFlatten(t.present(p, c)) }
  }

  /**
   * Transform the content of a matrix using a user supplied value, and return the transformations with an expanded
   * position.
   *
   * @param transformers The transformer(s) to apply to the content.
   * @param value        A `ValuePipe` holding a user supplied value.
   *
   * @return A Scalding `TypedPipe[Cell[P#M]]`
   */
  def transformAndExpandWithValue[T, V](transformers: T, value: ValuePipe[V])(
    implicit ev: TransformableExpandedWithValue[T, V]): TypedPipe[Cell[P#M]] = {
    val t = ev.convert(transformers)

    data.flatMapWithValue(value) { case ((p, c), vo) => Misc.mapFlatten(t.present(p, c, vo.get.asInstanceOf[t.V])) }
  }

  /**
   * Expand a matrix with an extra dimension.
   *
   * @param expander A function that expands each position with 1 dimension.
   *
   * @return A Scalding `TypedPipe[Cell[P#M]]`
   */
  def expand(expander: (P, Content) => P#M): TypedPipe[Cell[P#M]] = data.map { case (p, c) => (expander(p, c), c) }

  /**
   * Expand a matrix with an extra dimension using a user supplied value.
   *
   * @param expander A function that expands each position with 1 dimension.
   * @param value    A `ValuePipe` holding a user supplied value.
   *
   * @return A Scalding `TypedPipe[Cell[P#M]]`
   */
  def expandWithValue[V](expander: (P, Content, V) => P#M, value: ValuePipe[V]): TypedPipe[Cell[P#M]] = {
    data.mapWithValue(value) { case ((p, c), vo) => (expander(p, c, vo.get), c) }
  }
}

object Matrix {
  /** Type of cell in a Matrix. */
  type Cell[P <: Position] = (P, Content)

  /** Type of a collection of cells. */
  type CellCollection[P <: Position] = Misc.Collection[Cell[P]]

  /**
   * Read column oriented, pipe separated matrix data into a `TypedPipe[Cell[Position1D]]`.
   *
   * @param file      The file to read from.
   * @param separator The column separator.
   * @param first     The codex for decoding the first dimension.
   */
  def read1DFile(file: String, separator: String = "|", first: Codex = StringCodex): TypedPipe[Cell[Position1D]] = {
    read1D(FixedPathTypedDelimited[(String, String, String, String)](file, separator), first)
  }

  /**
   * Read source of `(String, String, String, String)` data into a `TypedPipe[Cell[Position1D]]`.
   *
   * @param source The source to read from.
   * @param first  The codex for decoding the first dimension.
   */
  def read1D(source: TypedSource[(String, String, String, String)],
    first: Codex = StringCodex): TypedPipe[Cell[Position1D]] = {
    source
      .flatMap {
        case (r, t, e, v) =>
          Schema.fromString(e, t).flatMap {
            case s => (s.decode(v), first.decode(r)) match {
              case (Some(con), Some(c1)) => Some((Position1D(c1), con))
              case _ => None
            }
          }
      }
  }

  /**
   * Read column oriented, pipe separated data into a `TypedPipe[Cell[Position1D]]`.
   *
   * @param file      The file to read from.
   * @param schema    The schema for decoding the data.
   * @param separator The column separator.
   * @param first     The codex for decoding the first dimension.
   */
  def read1DFileWithSchema(file: String, schema: Schema, separator: String = "|",
    first: Codex = StringCodex): TypedPipe[Cell[Position1D]] = {
    read1DWithSchema(FixedPathTypedDelimited[(String, String)](file, separator), schema, first)
  }

  /**
   * Read source of `(String, String)` data into a `TypedPipe[Cell[Position1D]]`.
   *
   * @param source The source to read from.
   * @param schema The schema for decoding the data.
   * @param first  The codex for decoding the first dimension.
   */
  def read1DWithSchema(source: TypedSource[(String, String)], schema: Schema,
    first: Codex = StringCodex): TypedPipe[Cell[Position1D]] = {
    source
      .flatMap {
        case (e, v) =>
          (schema.decode(v), first.decode(e)) match {
            case (Some(con), Some(c1)) => Some((Position1D(c1), con))
            case _ => None
          }
      }
  }

  /**
   * Read column oriented, pipe separated data into a `TypedPipe[Cell[Position1D]]`.
   *
   * @param file      The file to read from.
   * @param dict      The dictionary describing the features in the data.
   * @param separator The column separator.
   * @param first     The codex for decoding the first dimension.
   */
  def read1DFileWithDictionary(file: String, dict: Map[String, Schema], separator: String = "|",
    first: Codex = StringCodex): TypedPipe[Cell[Position1D]] = {
    read1DWithDictionary(FixedPathTypedDelimited[(String, String)](file, separator), dict, first)
  }

  /**
   * Read source of `(String, String)` data into a `TypedPipe[Cell[Position1D]]`.
   *
   * @param source The source to read from.
   * @param dict   The dictionary describing the features in the data.
   * @param first  The codex for decoding the first dimension.
   */
  def read1DWithDictionary(source: TypedSource[(String, String)], dict: Map[String, Schema],
    first: Codex = StringCodex): TypedPipe[Cell[Position1D]] = {
    source
      .flatMap {
        case (e, v) =>
          (dict(e).decode(v), first.decode(e)) match {
            case (Some(con), Some(c1)) => Some((Position1D(c1), con))
            case _ => None
          }
      }
  }

  /**
   * Read column oriented, pipe separated matrix data into a `TypedPipe[Cell[Position2D]]`.
   *
   * @param file      The file to read from.
   * @param separator The column separator.
   * @param first     The codex for decoding the first dimension.
   * @param second    The codex for decoding the second dimension.
   */
  def read2DFile(file: String, separator: String = "|", first: Codex = StringCodex,
    second: Codex = StringCodex): TypedPipe[Cell[Position2D]] = {
    read2D(FixedPathTypedDelimited[(String, String, String, String, String)](file, separator), first, second)
  }

  /**
   * Read source of `(String, String, String, String, String)` data into a `TypedPipe[Cell[Position2D]]`.
   *
   * @param source The source to read from.
   * @param first  The codex for decoding the first dimension.
   * @param second The codex for decoding the second dimension.
   */
  def read2D(source: TypedSource[(String, String, String, String, String)], first: Codex = StringCodex,
    second: Codex = StringCodex): TypedPipe[Cell[Position2D]] = {
    source
      .flatMap {
        case (r, c, t, e, v) =>
          Schema.fromString(e, t).flatMap {
            case s => (s.decode(v), first.decode(r), second.decode(c)) match {
              case (Some(con), Some(c1), Some(c2)) => Some((Position2D(c1, c2), con))
              case _ => None
            }
          }
      }
  }

  /**
   * Read column oriented, pipe separated data into a `TypedPipe[Cell[Position2D]]`.
   *
   * @param file      The file to read from.
   * @param schema    The schema for decoding the data.
   * @param separator The column separator.
   * @param first     The codex for decoding the first dimension.
   * @param second    The codex for decoding the second dimension.
   */
  def read2DFileWithSchema(file: String, schema: Schema, separator: String = "|", first: Codex = StringCodex,
    second: Codex = StringCodex): TypedPipe[Cell[Position2D]] = {
    read2DWithSchema(FixedPathTypedDelimited[(String, String, String)](file, separator), schema, first, second)
  }

  /**
   * Read source of `(String, String, String)` data into a `TypedPipe[Cell[Position2D]]`.
   *
   * @param source The source to read from.
   * @param schema The schema for decoding the data.
   * @param first  The codex for decoding the first dimension.
   * @param second The codex for decoding the second dimension.
   */
  def read2DWithSchema(source: TypedSource[(String, String, String)], schema: Schema, first: Codex = StringCodex,
    second: Codex = StringCodex): TypedPipe[Cell[Position2D]] = {
    source
      .flatMap {
        case (e, a, v) =>
          (schema.decode(v), first.decode(e), second.decode(a)) match {
            case (Some(con), Some(c1), Some(c2)) => Some((Position2D(c1, c2), con))
            case _ => None
          }
      }
  }

  /**
   * Read column oriented, pipe separated data into a `TypedPipe[Cell[Position2D]]`.
   *
   * @param file      The file to read from.
   * @param dict      The dictionary describing the features in the data.
   * @param dim       The dimension on which to apply the dictionary.
   * @param separator The column separator.
   * @param first     The codex for decoding the first dimension.
   * @param second    The codex for decoding the second dimension.
   */
  def read2DFileWithDictionary[D <: Dimension](file: String, dict: Map[String, Schema], dim: D = Second,
    separator: String = "|", first: Codex = StringCodex, second: Codex = StringCodex)(
      implicit ev: PosDimDep[Position2D, D]): TypedPipe[Cell[Position2D]] = {
    read2DWithDictionary(FixedPathTypedDelimited[(String, String, String)](file, separator), dict, dim, first, second)
  }

  /**
   * Read source of `(String, String, String)` data into a `TypedPipe[Cell[Position2D]]`.
   *
   * @param source The source to read from.
   * @param dict   The dictionary describing the features in the data.
   * @param dim    The dimension on which to apply the dictionary.
   * @param first  The codex for decoding the first dimension.
   * @param second The codex for decoding the second dimension.
   */
  def read2DWithDictionary[D <: Dimension](source: TypedSource[(String, String, String)], dict: Map[String, Schema],
    dim: D = Second, first: Codex = StringCodex, second: Codex = StringCodex)(
      implicit ev: PosDimDep[Position2D, D]): TypedPipe[Cell[Position2D]] = {
    source
      .flatMap {
        case (e, a, v) =>
          val s = dim match {
            case First => dict(e)
            case Second => dict(a)
          }

          (s.decode(v), first.decode(e), second.decode(a)) match {
            case (Some(con), Some(c1), Some(c2)) => Some((Position2D(c1, c2), con))
            case _ => None
          }
      }
  }

  /**
   * Read column oriented, pipe separated matrix data into a `TypedPipe[Cell[Position3D]]`.
   *
   * @param file      The file to read from.
   * @param separator The column separator.
   * @param first     The codex for decoding the first dimension.
   * @param second    The codex for decoding the second dimension.
   * @param third     The codex for decoding the third dimension.
   */
  def read3DFile(file: String, separator: String = "|", first: Codex = StringCodex, second: Codex = StringCodex,
    third: Codex = StringCodex): TypedPipe[Cell[Position3D]] = {
    read3D(FixedPathTypedDelimited[(String, String, String, String, String, String)](file, separator), first, second,
      third)
  }

  /**
   * Read source of `(String, String, String, String, String, String)` data into a `TypedPipe[Cell[Position3D]]`.
   *
   * @param source The source to read from.
   * @param first  The codex for decoding the first dimension.
   * @param second The codex for decoding the second dimension.
   * @param third  The codex for decoding the third dimension.
   */
  def read3D(source: TypedSource[(String, String, String, String, String, String)], first: Codex = StringCodex,
    second: Codex = StringCodex, third: Codex = StringCodex): TypedPipe[Cell[Position3D]] = {
    source
      .flatMap {
        case (r, c, d, t, e, v) =>
          Schema.fromString(e, t).flatMap {
            case s => (s.decode(v), first.decode(r), second.decode(c), third.decode(d)) match {
              case (Some(con), Some(c1), Some(c2), Some(c3)) => Some((Position3D(c1, c2, c3), con))
              case _ => None
            }
          }
      }
  }

  /**
   * Read column oriented, pipe separated data into a `TypedPipe[Cell[Position3D]]`.
   *
   * @param file      The file to read from.
   * @param schema    The schema for decoding the data.
   * @param separator The column separator.
   * @param first     The codex for decoding the first dimension.
   * @param second    The codex for decoding the second dimension.
   * @param third     The codex for decoding the third dimension.
   */
  def read3DFileWithSchema(file: String, schema: Schema, separator: String = "|", first: Codex = StringCodex,
    second: Codex = StringCodex, third: Codex = DateCodex): TypedPipe[Cell[Position3D]] = {
    read3DWithSchema(FixedPathTypedDelimited[(String, String, String, String)](file, separator), schema, first, second,
      third)
  }

  /**
   * Read source of `(String, String, String, String)` data into a `TypedPipe[Cell[Position3D]]`.
   *
   * @param source The source to read from.
   * @param schema The schema for decoding the data.
   * @param first  The codex for decoding the first dimension.
   * @param second The codex for decoding the second dimension.
   * @param third  The codex for decoding the third dimension.
   */
  def read3DWithSchema(source: TypedSource[(String, String, String, String)], schema: Schema,
    first: Codex = StringCodex, second: Codex = StringCodex, third: Codex = DateCodex): TypedPipe[Cell[Position3D]] = {
    source
      .flatMap {
        case (e, a, t, v) =>
          (schema.decode(v), first.decode(e), second.decode(a), third.decode(t)) match {
            case (Some(con), Some(c1), Some(c2), Some(c3)) => Some((Position3D(c1, c2, c3), con))
            case _ => None
          }
      }
  }

  /**
   * Read column oriented, pipe separated data into a `TypedPipe[Cell[Position3D]]`.
   *
   * @param file      The file to read from.
   * @param dict      The dictionary describing the features in the data.
   * @param dim       The dimension on which to apply the dictionary.
   * @param separator The column separator.
   * @param first     The codex for decoding the first dimension.
   * @param second    The codex for decoding the second dimension.
   * @param third     The codex for decoding the third dimension.
   */
  def read3DFileWithDictionary[D <: Dimension](file: String, dict: Map[String, Schema], dim: D = Second,
    separator: String = "|", first: Codex = StringCodex, second: Codex = StringCodex, third: Codex = DateCodex)(
      implicit ev: PosDimDep[Position3D, D]): TypedPipe[Cell[Position3D]] = {
    read3DWithDictionary(FixedPathTypedDelimited[(String, String, String, String)](file, separator), dict, dim, first,
      second, third)
  }

  /**
   * Read source of `(String, String, String, String)` data into a `TypedPipe[Cell[Position3D]]`.
   *
   * @param source The source to read from.
   * @param dict   The dictionary describing the features in the data.
   * @param dim    The dimension on which to apply the dictionary.
   * @param first  The codex for decoding the first dimension.
   * @param second The codex for decoding the second dimension.
   * @param third  The codex for decoding the third dimension.
   */
  def read3DWithDictionary[D <: Dimension](source: TypedSource[(String, String, String, String)],
    dict: Map[String, Schema], dim: D = Second, first: Codex = StringCodex, second: Codex = StringCodex,
      third: Codex = DateCodex)(implicit ev: PosDimDep[Position3D, D]): TypedPipe[Cell[Position3D]] = {
    source
      .flatMap {
        case (e, a, t, v) =>
          val s = dim match {
            case First => dict(e)
            case Second => dict(a)
            case Third => dict(t)
          }

          (s.decode(v), first.decode(e), second.decode(a), third.decode(t)) match {
            case (Some(con), Some(c1), Some(c2), Some(c3)) => Some((Position3D(c1, c2, c3), con))
            case _ => None
          }
      }
  }

  /**
   * Read tabled data into a `TypedPipe[Cell[Position2D]]`.
   *
   * @param table     The file (table) to read from.
   * @param columns   `List[(String, Schema)]` describing each column in the table.
   * @param pkeyIndex Index (into `columns`) describing which column is the primary key.
   * @param separator The column separator.
   *
   * @note The returned `Position2D` consists of 2 string values. The first string value is the contents of the primary
   *       key column. The second string value is the name of the column.
   */
  def readTable(table: String, columns: List[(String, Schema)], pkeyIndex: Int = 0,
    separator: String = "\01"): TypedPipe[Cell[Position2D]] = {
    TypedPipe.from(TextLine(table))
      .flatMap {
        case line =>
          val parts = line.trim.split(separator, columns.length).toList
          val pkey = parts(pkeyIndex)

          columns.zipWithIndex.flatMap {
            case ((name, schema), idx) if (idx != pkeyIndex) =>
              schema.decode(parts(idx).trim).map { case c => (Position2D(pkey, name), c) }
            case _ => None
          }
      }
  }

  /** Conversion from `TypedPipe[Cell[Position1D]]` to a `Matrix1D`. */
  implicit def typedPipePosition1DContent(data: TypedPipe[Cell[Position1D]]): Matrix1D = new Matrix1D(data)
  /** Conversion from `TypedPipe[Cell[Position2D]]` to a `Matrix2D`. */
  implicit def typedPipePosition2DContent(data: TypedPipe[Cell[Position2D]]): Matrix2D = new Matrix2D(data)
  /** Conversion from `TypedPipe[Cell[Position3D]]` to a `Matrix3D`. */
  implicit def typedPipePosition3DContent(data: TypedPipe[Cell[Position3D]]): Matrix3D = new Matrix3D(data)
  /** Conversion from `TypedPipe[Cell[Position4D]]` to a `Matrix4D`. */
  implicit def typedPipePosition4DContent(data: TypedPipe[Cell[Position4D]]): Matrix4D = new Matrix4D(data)
  /** Conversion from `TypedPipe[Cell[Position5D]]` to a `Matrix5D`. */
  implicit def typedPipePosition5DContent(data: TypedPipe[Cell[Position5D]]): Matrix5D = new Matrix5D(data)

  /** Conversion from `List[(Valueable, Content)]` to a `Matrix1D`. */
  implicit def tuple2List[V: Valueable](list: List[(V, Content)]): Matrix1D = {
    new Matrix1D(new IterablePipe(list.map { case (v, c) => (Position1D(v), c) }))
  }
  /** Conversion from `List[(Valueable, Valueable, Content)]` to a `Matrix2D`. */
  implicit def tuple3List[V: Valueable, W: Valueable](list: List[(V, W, Content)]): Matrix2D = {
    new Matrix2D(new IterablePipe(list.map { case (v, w, c) => (Position2D(v, w), c) }))
  }
  /** Conversion from `List[(Valueable, Valueable, Valueable, Content)]` to a `Matrix3D`. */
  implicit def tuple4List[V: Valueable, W: Valueable, X: Valueable](list: List[(V, W, X, Content)]): Matrix3D = {
    new Matrix3D(new IterablePipe(list.map { case (v, w, x, c) => (Position3D(v, w, x), c) }))
  }
  /** Conversion from `List[(Valueable, Valueable, Valueable, Valueable, Content)]` to a `Matrix4D`. */
  implicit def tuple5List[V: Valueable, W: Valueable, X: Valueable, Y: Valueable](
    list: List[(V, W, X, Y, Content)]): Matrix4D = {
    new Matrix4D(new IterablePipe(list.map { case (v, w, x, y, c) => (Position4D(v, w, x, y), c) }))
  }
  /** Conversion from `List[(Valueable, Valueable, Valueable, Valueable, Valueable, Content)]` to a `Matrix5D`. */
  implicit def tuple6List[V: Valueable, W: Valueable, X: Valueable, Y: Valueable, Z: Valueable](
    list: List[(V, W, X, Y, Z, Content)]): Matrix5D = {
    new Matrix5D(new IterablePipe(list.map { case (v, w, x, y, z, c) => (Position5D(v, w, x, y, z), c) }))
  }
}

/**
 * Rich wrapper around a `TypedPipe[Cell[Position1D]]`.
 *
 * @param data `TypedPipe[Cell[Position1D]]`.
 */
class Matrix1D(val data: TypedPipe[Cell[Position1D]]) extends Matrix[Position1D] with ModifiableMatrix[Position1D]
  with ExpandableMatrix[Position1D] {
  def domain(): TypedPipe[Position1D] = names(Over(First)).map { case (p, i) => p }

  /**
   * Persist a `Matrix1D` as sparse matrix file (index, value).
   *
   * @param file       File to write to.
   * @param dictionary Pattern for the dictionary file name.
   * @param separator  Column separator to use in dictionary file.
   *
   * @return A `TypedPipe[Cell[Position1D]]`; that is it returns `data`.
   */
  def persistIVFile(file: String, dictionary: String = "%1$s.dict.%2$d", separator: String = "|")(
    implicit flow: FlowDef, mode: Mode): TypedPipe[Cell[Position1D]] = {
    persistIVFileWithNames(file, names(Over(First)), dictionary, separator)
  }

  /**
   * Persist a `Matrix1D` as sparse matrix file (index, value).
   *
   * @param file       File to write to.
   * @param names      The names to use for the first dimension (according to their ordering).
   * @param dictionary Pattern for the dictionary file name.
   * @param separator  Column separator to use in dictionary file.
   *
   * @return A `TypedPipe[Cell[Position1D]]`; that is it returns `data`.
   *
   * @note If `names` contains a subset of the columns, then only those columns get persisted to file.
   */
  def persistIVFileWithNames(file: String, names: TypedPipe[(Position1D, Long)], dictionary: String = "%1$s.dict.%2$d",
    separator: String = "|")(implicit flow: FlowDef, mode: Mode): TypedPipe[Cell[Position1D]] = {
    data
      .groupBy { case (p, c) => p }
      .join(persistDictionary(names, file, dictionary, separator, First))
      .map { case (_, ((_, c), (_, i))) => i + separator + c.value.toShortString }
      .write(TypedSink(TextLine(file)))

    data
  }
}

/**
 * Rich wrapper around a `TypedPipe[Cell[Position2D]]`.
 *
 * @param data `TypedPipe[Cell[Position2D]]`.
 */
class Matrix2D(val data: TypedPipe[Cell[Position2D]]) extends Matrix[Position2D] with ModifiableMatrix[Position2D]
  with ReduceableMatrix[Position2D] with ExpandableMatrix[Position2D] {
  def domain(): TypedPipe[Position2D] = {
    names(Over(First))
      .map { case (Position1D(c), i) => c }
      .cross(names(Over(Second)).map { case (Position1D(c), i) => c })
      .map { case (c1, c2) => Position2D(c1, c2) }
  }

  /**
   * Permute the order of the coordinates in a position.
   *
   * @param first  Dimension used for the first coordinate.
   * @param second Dimension used for the second coordinate.
   */
  def permute[D <: Dimension, E <: Dimension](first: D, second: E)(implicit ev1: PosDimDep[Position2D, D],
    ev2: PosDimDep[Position2D, E], ne: D =!= E): TypedPipe[Cell[Position2D]] = {
    data.map { case (p, c) => (p.permute(List(first, second)), c) }
  }

  // TODO: Make this work on more than 2D matrices
  def mutualInformation[D <: Dimension](slice: Slice[Position2D, D])(
    implicit ev: PosDimDep[Position2D, D]): TypedPipe[Cell[Position1D]] = {
    val marginal = data
      .reduceAndExpand(slice, Entropy("marginal"))
      .pairwise(Over(First), Plus(name = "%s,%s", comparer = Upper))

    val joint = data
      .pairwise(slice, Concatenate(name = "%s,%s", comparer = Upper))
      .reduceAndExpand(Over(First), Entropy("joint", strict = true, nan = true, negate = true))

    (marginal ++ joint)
      .reduce(Over(First), Sum())
  }

  // TODO: Make this work on more than 2D matrices
  def correlation[D <: Dimension](slice: Slice[Position2D, D])(
    implicit ev: PosDimDep[Position2D, D]): TypedPipe[Cell[Position1D]] = {
    val mean = data
      .reduce(slice, Mean())
      .toMap(Over(First))

    val centered = data
      .transformWithValue(Subtract(slice.dimension), mean)

    val denom = centered
      .transform(Power(slice.dimension, 2))
      .reduce(slice, Sum())
      .pairwise(Over(First), Times())
      .transform(SquareRoot(First))
      .toMap(Over(First))

    centered
      .pairwise(slice, Times())
      .reduce(Over(First), Sum())
      .transformWithValue(Fraction(First), denom)
  }

  /**
   * Persist a `Matrix2D` as a CSV file.
   *
   * @param slice       Encapsulates the dimension that makes up the columns.
   * @param file        File to write to.
   * @param separator   Column separator to use.
   * @param writeHeader Indicator of the header should be written to a separate file.
   * @param header      Postfix for the header file name.
   * @param writeRowId  Indicator if row names should be written.
   * @param rowId       Column name of row names.
   *
   * @return A `TypedPipe[Cell[Position2D]]`; that is it returns `data`.
   */
  def persistCSVFile[D <: Dimension](slice: Slice[Position2D, D], file: String, separator: String = "|",
    writeHeader: Boolean = true, header: String = "%s.header", writeRowId: Boolean = true, rowId: String = "id")(
      implicit ev: PosDimDep[Position2D, D], flow: FlowDef, mode: Mode): TypedPipe[Cell[Position2D]] = {
    persistCSVFileWithNames(slice, file, names(slice), separator, writeHeader, header, writeRowId, rowId)
  }

  /**
   * Persist a `Matrix2D` as a CSV file.
   *
   * @param slice       Encapsulates the dimension that makes up the columns.
   * @param file        File to write to.
   * @param names       The names to use for the columns (according to their ordering).
   * @param separator   Column separator to use.
   * @param writeHeader Indicator of the header should be written to a separate file.
   * @param header      Postfix for the header file name.
   * @param writeRowId  Indicator if row names should be written.
   * @param rowId       Column name of row names.
   *
   * @return A `TypedPipe[Cell[Position2D]]`; that is it returns `data`.
   *
   * @note If `names` contains a subset of the columns, then only those columns get persisted to file.
   */
  def persistCSVFileWithNames[T, D <: Dimension](slice: Slice[Position2D, D], file: String, names: T,
    separator: String = "|", writeHeader: Boolean = true, header: String = "%s.header", writeRowId: Boolean = true,
    rowId: String = "id")(implicit ev1: Nameable[T, Position2D, slice.S, D], ev2: PosDimDep[Position2D, D],
      flow: FlowDef, mode: Mode): TypedPipe[Cell[Position2D]] = {
    // Note: Usage of .toShortString should be safe as data is written
    //       as string anyways. It does assume that all indices have
    //       unique short string representations.
    val columns = ev1.convert(this, slice, names)
      .map { List(_) }
      .sum
      .map { _.sortBy(_._2).map(_._1.toShortString("")) }

    if (writeHeader) {
      columns
        .map { case lst => if (writeRowId) rowId + separator + lst.mkString(separator) else lst.mkString(separator) }
        .write(TypedSink(TextLine(header.format(file))))
    }

    // TODO: Escape separator in values
    data
      .groupBy { case (p, c) => slice.remainder(p).toShortString("") }
      .mapValues { case (p, c) => Map(slice.selected(p).toShortString("") -> c.value.toShortString) }
      .sum
      .flatMapWithValue(columns) {
        case ((key, values), optCols) => optCols.map {
          case cols => (key, cols.map { case c => values.getOrElse(c, "") })
        }
      }
      .map { case (i, lst) => if (writeRowId) i + separator + lst.mkString(separator) else lst.mkString(separator) }
      .write(TypedSink(TextLine(file)))

    data
  }

  def persistVWFile[D <: Dimension](slice: Slice[Position2D, D], labels: TypedPipe[Cell[Position1D]], file: String,
    dictionary: String = "%s.dict", separator: String = ":")(implicit ev: PosDimDep[Position2D, D], flow: FlowDef,
      mode: Mode): TypedPipe[Cell[Position2D]] = {
    persistVWFileWithNames(slice, labels, file, names(Along(slice.dimension)), dictionary, separator)
  }

  def persistVWFileWithNames[D <: Dimension](slice: Slice[Position2D, D], labels: TypedPipe[Cell[Position1D]],
    file: String, names: TypedPipe[(Position1D, Long)], dictionary: String = "%s.dict", separator: String = ":")(
      implicit ev: PosDimDep[Position2D, D], flow: FlowDef, mode: Mode): TypedPipe[Cell[Position2D]] = {
    data
      .groupBy { case (p, c) => slice.remainder(p).asInstanceOf[Position1D] }
      .join(persistDictionary(names, file, dictionary, separator))
      .map { case (_, ((p, c), (_, i))) => (p, " " + i + ":" + c.value.toShortString) }
      .groupBy { case (p, ics) => slice.selected(p).asInstanceOf[Position1D] }
      .reduce[(Position2D, String)] { case ((p, ls), (_, rs)) => (p, ls + rs) }
      .join(labels.groupBy { case (p, c) => p })
      .map { case (p, ((_, ics), (_, l))) => l.value.toShortString + " " + p.toShortString(separator) + "|" + ics }
      .write(TypedSink(TextLine(file)))

    data
  }

  /**
   * Persist a `Matrix2D` as a LDA file.
   *
   * @param slice      Encapsulates the dimension that makes up the columns.
   * @param file       File to write to.
   * @param dictionary Pattern for the dictionary file name.
   * @param separator  Column separator to use in dictionary file.
   * @param addId      Indicator if each line should start with the row id followed by `separator`.
   *
   * @return A `TypedPipe[Cell[Position2D]]`; that is it returns `data`.
   */
  def persistLDAFile[D <: Dimension](slice: Slice[Position2D, D], file: String, dictionary: String = "%s.dict",
    separator: String = "|", addId: Boolean = false)(implicit ev: PosDimDep[Position2D, D], flow: FlowDef,
      mode: Mode): TypedPipe[Cell[Position2D]] = {
    persistLDAFileWithNames(slice, file, names(Along(slice.dimension)), dictionary, separator, addId)
  }

  /**
   * Persist a `Matrix2D` as a LDA file.
   *
   * @param slice      Encapsulates the dimension that makes up the columns.
   * @param file       File to write to.
   * @param names      The names to use for the columns (according to their ordering).
   * @param dictionary Pattern for the dictionary file name.
   * @param separator  Column separator to use in dictionary file.
   * @param addId      Indicator if each line should start with the row id followed by `separator`.
   *
   * @return A `TypedPipe[Cell[Position2D]]`; that is it returns `data`.
   *
   * @note If `names` contains a subset of the columns, then only those columns get persisted to file.
   */
  def persistLDAFileWithNames[D <: Dimension](slice: Slice[Position2D, D], file: String,
    names: TypedPipe[(Position1D, Long)], dictionary: String = "%s.dict", separator: String = "|",
    addId: Boolean = false)(implicit ev: PosDimDep[Position2D, D], flow: FlowDef,
      mode: Mode): TypedPipe[Cell[Position2D]] = {
    data
      .groupBy { case (p, c) => slice.remainder(p).asInstanceOf[Position1D] }
      .join(persistDictionary(names, file, dictionary, separator))
      .map { case (_, ((p, c), (_, i))) => (p, " " + i + ":" + c.value.toShortString, 1L) }
      .groupBy { case (p, ics, m) => slice.selected(p) }
      .reduce[(Position2D, String, Long)] { case ((p, ls, lm), (_, rs, rm)) => (p, ls + rs, lm + rm) }
      .map { case (p, (_, ics, m)) => if (addId) p.toShortString(separator) + separator + m + ics else m + ics }
      .write(TypedSink(TextLine(file)))

    data
  }

  /**
   * Persist a `Matrix2D` as sparse matrix file (index, index, value).
   *
   * @param file       File to write to.
   * @param dictionary Pattern for the dictionary file name.
   * @param separator  Column separator to use in dictionary file.
   *
   * @return A `TypedPipe[Cell[Position2D]]`; that is it returns `data`.
   *
   * @note R's slam package has a simple triplet matrx format (which in turn is used by the tm package). This format
   *       should be compatible.
   */
  def persistIVFile(file: String, dictionary: String = "%1$s.dict.%2$d", separator: String = "|")(
    implicit flow: FlowDef, mode: Mode): TypedPipe[Cell[Position2D]] = {
    persistIVFileWithNames(file, names(Over(First)), names(Over(Second)), dictionary, separator)
  }

  /**
   * Persist a `Matrix2D` as sparse matrix file (index, index, value).
   *
   * @param file       File to write to.
   * @param namesI     The names to use for the first dimension (according to their ordering).
   * @param namesJ     The names to use for the second dimension (according to their ordering).
   * @param dictionary Pattern for the dictionary file name.
   * @param separator  Column separator to use in dictionary file.
   *
   * @return A `TypedPipe[Cell[Position2D]]`; that is it returns `data`.
   *
   * @note If `names` contains a subset of the columns, then only those columns get persisted to file.
   * @note R's slam package has a simple triplet matrx format (which in turn is used by the tm package). This format
   *       should be compatible.
   */
  def persistIVFileWithNames(file: String, namesI: TypedPipe[(Position1D, Long)], namesJ: TypedPipe[(Position1D, Long)],
    dictionary: String = "%1$s.dict.%2$d", separator: String = "|")(implicit flow: FlowDef,
      mode: Mode): TypedPipe[Cell[Position2D]] = {
    data
      .groupBy { case (p, c) => Position1D(p.get(First)) }
      .join(persistDictionary(namesI, file, dictionary, separator, First))
      .values
      .groupBy { case ((p, c), pi) => Position1D(p.get(Second)) }
      .join(persistDictionary(namesJ, file, dictionary, separator, Second))
      .map { case (_, (((_, c), (_, i)), (_, j))) => i + separator + j + separator + c.value.toShortString }
      .write(TypedSink(TextLine(file)))

    data
  }

  protected implicit def typedPipeSlicePosition2DSContent[D <: Dimension](
    data: TypedPipe[Cell[Slice[Position2D, D]#S]]): Matrix1D = {
    new Matrix1D(data.asInstanceOf[TypedPipe[Cell[Position1D]]])
  }
  protected implicit def typedPipeSlicePosition2DSMContent[D <: Dimension](
    data: TypedPipe[Cell[Slice[Position2D, D]#S#M]]): Matrix2D = {
    new Matrix2D(data.asInstanceOf[TypedPipe[Cell[Position2D]]])
  }
  protected implicit def typedPipeSlicePosition2DRMContent[D <: Dimension](
    data: TypedPipe[Cell[Slice[Position2D, D]#R#M]]): Matrix2D = {
    new Matrix2D(data.asInstanceOf[TypedPipe[Cell[Position2D]]])
  }
}

/**
 * Rich wrapper around a `TypedPipe[Cell[Position3D]]`.
 *
 * @param data `TypedPipe[Cell[Position3D]]`.
 */
class Matrix3D(val data: TypedPipe[Cell[Position3D]]) extends Matrix[Position3D] with ModifiableMatrix[Position3D]
  with ReduceableMatrix[Position3D] with ExpandableMatrix[Position3D] {
  def domain(): TypedPipe[Position3D] = {
    names(Over(First))
      .map { case (Position1D(c), i) => c }
      .cross(names(Over(Second)).map { case (Position1D(c), i) => c })
      .cross(names(Over(Third)).map { case (Position1D(c), i) => c })
      .map { case ((c1, c2), c3) => Position3D(c1, c2, c3) }
  }

  /**
   * Permute the order of the coordinates in a position.
   *
   * @param first  Dimension used for the first coordinate.
   * @param second Dimension used for the second coordinate.
   * @param third  Dimension used for the third coordinate.
   */
  def permute[D <: Dimension, E <: Dimension, F <: Dimension](first: D, second: E, third: F)(
    implicit ev1: PosDimDep[Position3D, D], ev2: PosDimDep[Position3D, E], ev3: PosDimDep[Position3D, F], ne1: D =!= E,
    ne2: D =!= F, ne3: E =!= F): TypedPipe[Cell[Position3D]] = {
    data.map { case (p, c) => (p.permute(List(first, second, third)), c) }
  }

  /**
   * Persist a `Matrix3D` as sparse matrix file (index, index, index, value).
   *
   * @param file       File to write to.
   * @param dictionary Pattern for the dictionary file name.
   * @param separator  Column separator to use in dictionary file.
   *
   * @return A `TypedPipe[Cell[Position3D]]`; that is it returns `data`.
   */
  def persistIVFile(file: String, dictionary: String = "%1$s.dict.%2$d", separator: String = "|")(
    implicit flow: FlowDef, mode: Mode): TypedPipe[Cell[Position3D]] = {
    persistIVFileWithNames(file, names(Over(First)), names(Over(Second)), names(Over(Third)), dictionary, separator)
  }

  /**
   * Persist a `Matrix3D` as sparse matrix file (index, index, index, value).
   *
   * @param file       File to write to.
   * @param namesI     The names to use for the first dimension (according to their ordering).
   * @param namesJ     The names to use for the second dimension (according to their ordering).
   * @param namesK     The names to use for the third dimension (according to their ordering).
   * @param dictionary Pattern for the dictionary file name.
   * @param separator  Column separator to use in dictionary file.
   *
   * @return A `TypedPipe[Cell[Position3D]]`; that is it returns `data`.
   *
   * @note If `names` contains a subset of the columns, then only those columns get persisted to file.
   */
  def persistIVFileWithNames(file: String, namesI: TypedPipe[(Position1D, Long)], namesJ: TypedPipe[(Position1D, Long)],
    namesK: TypedPipe[(Position1D, Long)], dictionary: String = "%1$s.dict.%2$d", separator: String = "|")(
      implicit flow: FlowDef, mode: Mode): TypedPipe[Cell[Position3D]] = {
    data
      .groupBy { case (p, c) => Position1D(p.get(First)) }
      .join(persistDictionary(namesI, file, dictionary, separator, First))
      .values
      .groupBy { case ((p, c), pi) => Position1D(p.get(Second)) }
      .join(persistDictionary(namesJ, file, dictionary, separator, Second))
      .map { case (_, ((pc, pi), pj)) => (pc, pi, pj) }
      .groupBy { case ((p, c), pi, pj) => Position1D(p.get(Third)) }
      .join(persistDictionary(namesK, file, dictionary, separator, Third))
      .map {
        case (_, (((_, c), (_, i), (_, j)), (_, k))) =>
          i + separator + j + separator + k + separator + c.value.toShortString
      }
      .write(TypedSink(TextLine(file)))

    data
  }
}

/**
 * Rich wrapper around a `TypedPipe[Cell[Position4D]]`.
 *
 * @param data `TypedPipe[Cell[Position4D]]`.
 */
class Matrix4D(val data: TypedPipe[Cell[Position4D]]) extends Matrix[Position4D] with ModifiableMatrix[Position4D]
  with ReduceableMatrix[Position4D] with ExpandableMatrix[Position4D] {
  def domain(): TypedPipe[Position4D] = {
    names(Over(First))
      .map { case (Position1D(c), i) => c }
      .cross(names(Over(Second)).map { case (Position1D(c), i) => c })
      .cross(names(Over(Third)).map { case (Position1D(c), i) => c })
      .cross(names(Over(Fourth)).map { case (Position1D(c), i) => c })
      .map { case (((c1, c2), c3), c4) => Position4D(c1, c2, c3, c4) }
  }

  /**
   * Permute the order of the coordinates in a position.
   *
   * @param first  Dimension used for the first coordinate.
   * @param second Dimension used for the second coordinate.
   * @param third  Dimension used for the third coordinate.
   * @param fourth Dimension used for the fourth coordinate.
   */
  def permute[D <: Dimension, E <: Dimension, F <: Dimension, G <: Dimension](first: D, second: E, third: F,
    fourth: G)(implicit ev1: PosDimDep[Position4D, D], ev2: PosDimDep[Position4D, E], ev3: PosDimDep[Position4D, F],
      ev4: PosDimDep[Position4D, G], ne1: D =!= E, ne2: D =!= F, ne3: D =!= G, ne4: E =!= F, ne5: E =!= G,
      ne6: F =!= G): TypedPipe[Cell[Position4D]] = {
    data.map { case (p, c) => (p.permute(List(first, second, third, fourth)), c) }
  }

  /**
   * Persist a `Matrix4D` as sparse matrix file (index, index, index, index, value).
   *
   * @param file       File to write to.
   * @param dictionary Pattern for the dictionary file name.
   * @param separator  Column separator to use in dictionary file.
   *
   * @return A `TypedPipe[Cell[Position4D]]`; that is it returns `data`.
   */
  def persistIVFile(file: String, dictionary: String = "%1$s.dict.%2$d", separator: String = "|")(
    implicit flow: FlowDef, mode: Mode): TypedPipe[Cell[Position4D]] = {
    persistIVFileWithNames(file, names(Over(First)), names(Over(Second)), names(Over(Third)), names(Over(Fourth)),
      dictionary, separator)
  }

  /**
   * Persist a `Matrix4D` as sparse matrix file (index, index, index, index, value).
   *
   * @param file       File to write to.
   * @param namesI     The names to use for the first dimension (according to their ordering).
   * @param namesJ     The names to use for the second dimension (according to their ordering).
   * @param namesK     The names to use for the third dimension (according to their ordering).
   * @param namesL     The names to use for the fourth dimension (according to their ordering).
   * @param dictionary Pattern for the dictionary file name.
   * @param separator  Column separator to use in dictionary file.
   *
   * @return A `TypedPipe[Cell[Position4D]]`; that is it returns `data`.
   *
   * @note If `names` contains a subset of the columns, then only those columns get persisted to file.
   */
  def persistIVFileWithNames(file: String, namesI: TypedPipe[(Position1D, Long)], namesJ: TypedPipe[(Position1D, Long)],
    namesK: TypedPipe[(Position1D, Long)], namesL: TypedPipe[(Position1D, Long)],
    dictionary: String = "%1$s.dict.%2$d", separator: String = "|")(implicit flow: FlowDef,
      mode: Mode): TypedPipe[Cell[Position4D]] = {
    data
      .groupBy { case (p, c) => Position1D(p.get(First)) }
      .join(persistDictionary(namesI, file, dictionary, separator, First))
      .values
      .groupBy { case ((p, c), pi) => Position1D(p.get(Second)) }
      .join(persistDictionary(namesJ, file, dictionary, separator, Second))
      .map { case (_, ((pc, pi), pj)) => (pc, pi, pj) }
      .groupBy { case ((p, c), pi, pj) => Position1D(p.get(Third)) }
      .join(persistDictionary(namesK, file, dictionary, separator, Third))
      .map { case (_, ((pc, pi, pj), pk)) => (pc, pi, pj, pk) }
      .groupBy { case ((p, c), pi, pj, pk) => Position1D(p.get(Fourth)) }
      .join(persistDictionary(namesL, file, dictionary, separator, Fourth))
      .map {
        case (_, (((_, c), (_, i), (_, j), (_, k)), (_, l))) =>
          i + separator + j + separator + k + separator + l + separator + c.value.toShortString
      }
      .write(TypedSink(TextLine(file)))

    data
  }
}

/**
 * Rich wrapper around a `TypedPipe[Cell[Position5D]]`.
 *
 * @param data `TypedPipe[Cell[Position5D]]`.
 */
class Matrix5D(val data: TypedPipe[Cell[Position5D]]) extends Matrix[Position5D] with ModifiableMatrix[Position5D]
  with ReduceableMatrix[Position5D] {
  def domain(): TypedPipe[Position5D] = {
    names(Over(First))
      .map { case (Position1D(c), i) => c }
      .cross(names(Over(Second)).map { case (Position1D(c), i) => c })
      .cross(names(Over(Third)).map { case (Position1D(c), i) => c })
      .cross(names(Over(Fourth)).map { case (Position1D(c), i) => c })
      .cross(names(Over(Fifth)).map { case (Position1D(c), i) => c })
      .map { case ((((c1, c2), c3), c4), c5) => Position5D(c1, c2, c3, c4, c5) }
  }

  /**
   * Permute the order of the coordinates in a position.
   *
   * @param first  Dimension used for the first coordinate.
   * @param second Dimension used for the second coordinate.
   * @param third  Dimension used for the third coordinate.
   * @param fourth Dimension used for the fourth coordinate.
   * @param fifth  Dimension used for the fifth coordinate.
   */
  def permute[D <: Dimension, E <: Dimension, F <: Dimension, G <: Dimension, H <: Dimension](first: D, second: E,
    third: F, fourth: G, fifth: H)(implicit ev1: PosDimDep[Position5D, D], ev2: PosDimDep[Position5D, E],
      ev3: PosDimDep[Position5D, F], ev4: PosDimDep[Position5D, G], ev5: PosDimDep[Position5D, H], ne1: D =!= E,
      ne2: D =!= F, ne3: D =!= G, ne4: D =!= H, ne5: E =!= F, ne6: E =!= G, ne7: E =!= H, ne8: F =!= G, ne9: F =!= H,
      ne10: G =!= H): TypedPipe[Cell[Position5D]] = {
    data.map { case (p, c) => (p.permute(List(first, second, third, fourth, fifth)), c) }
  }

  /**
   * Persist a `Matrix5D` as sparse matrix file (index, index, index, index, index, value).
   *
   * @param file       File to write to.
   * @param dictionary Pattern for the dictionary file name.
   * @param separator  Column separator to use in dictionary file.
   *
   * @return A `TypedPipe[Cell[Position5D]]`; that is it returns `data`.
   */
  def persistIVFile(file: String, dictionary: String = "%1$s.dict.%2$d", separator: String = "|")(
    implicit flow: FlowDef, mode: Mode): TypedPipe[Cell[Position5D]] = {
    persistIVFileWithNames(file, names(Over(First)), names(Over(Second)), names(Over(Third)), names(Over(Fourth)),
      names(Over(Fifth)), dictionary, separator)
  }

  /**
   * Persist a `Matrix5D` as sparse matrix file (index, index, index, index, index, value).
   *
   * @param file       File to write to.
   * @param namesI     The names to use for the first dimension (according to their ordering).
   * @param namesJ     The names to use for the second dimension (according to their ordering).
   * @param namesK     The names to use for the third dimension (according to their ordering).
   * @param namesL     The names to use for the fourth dimension (according to their ordering).
   * @param namesM     The names to use for the fifth dimension (according to their ordering).
   * @param dictionary Pattern for the dictionary file name.
   * @param separator  Column separator to use in dictionary file.
   *
   * @return A `TypedPipe[Cell[Position5D]]`; that is it returns `data`.
   *
   * @note If `names` contains a subset of the columns, then only those columns get persisted to file.
   */
  def persistIVFileWithNames(file: String, namesI: TypedPipe[(Position1D, Long)], namesJ: TypedPipe[(Position1D, Long)],
    namesK: TypedPipe[(Position1D, Long)], namesL: TypedPipe[(Position1D, Long)],
    namesM: TypedPipe[(Position1D, Long)], dictionary: String = "%1$s.dict.%2$d", separator: String = "|")(
      implicit flow: FlowDef, mode: Mode): TypedPipe[Cell[Position5D]] = {
    data
      .groupBy { case (p, c) => Position1D(p.get(First)) }
      .join(persistDictionary(namesI, file, dictionary, separator, First))
      .values
      .groupBy { case ((p, c), pi) => Position1D(p.get(Second)) }
      .join(persistDictionary(namesJ, file, dictionary, separator, Second))
      .map { case (_, ((pc, pi), pj)) => (pc, pi, pj) }
      .groupBy { case ((p, c), pi, pj) => Position1D(p.get(Third)) }
      .join(persistDictionary(namesK, file, dictionary, separator, Third))
      .map { case (_, ((pc, pi, pj), pk)) => (pc, pi, pj, pk) }
      .groupBy { case ((p, c), pi, pj, pk) => Position1D(p.get(Fourth)) }
      .join(persistDictionary(namesL, file, dictionary, separator, Fourth))
      .map { case (_, ((pc, pi, pj, pk), pl)) => (pc, pi, pj, pk, pl) }
      .groupBy { case ((p, c), pi, pj, pk, pl) => Position1D(p.get(Fifth)) }
      .join(persistDictionary(namesM, file, dictionary, separator, Fifth))
      .map {
        case (_, (((_, c), (_, i), (_, j), (_, k), (_, l)), (_, m))) =>
          i + separator + j + separator + k + separator + l + separator + m + separator + c.value.toShortString
      }
      .write(TypedSink(TextLine(file)))

    data
  }
}

/** Type class for transforming a type `T` into a `TypedPipe[Cell[P]]`. */
trait Matrixable[T, P <: Position] {
  /**
   * Returns a `TypedPipe[Cell[P]]` for type `T`.
   *
   * @param t Object that can be converted to a `TypedPipe[Cell[P]]`.
   */
  def convert(t: T): TypedPipe[Cell[P]]
}

/** Companion object for the `Matrixable` type class. */
object Matrixable {
  /** Converts a `TypedPipe[Cell[P]]` into a `TypedPipe[Cell[P]]`; that is, it is a  pass through. */
  implicit def MatrixMatrixable[P <: Position]: Matrixable[TypedPipe[Cell[P]], P] = {
    new Matrixable[TypedPipe[Cell[P]], P] { def convert(t: TypedPipe[Cell[P]]): TypedPipe[Cell[P]] = t }
  }
  /** Converts a `(PositionPipeable, Content)` tuple into a `TypedPipe[Cell[P]]`. */
  implicit def PositionPipeableContentTupleMatrixable[T, P <: Position](
    implicit ev: PositionPipeable[T, P]): Matrixable[(T, Content), P] = {
    new Matrixable[(T, Content), P] {
      def convert(t: (T, Content)): TypedPipe[Cell[P]] = ev.convert(t._1).map { case p => (p, t._2) }
    }
  }
  /** Converts a `List[Cell[P]]` into a `TypedPipe[Cell[P]]`. */
  implicit def ListCellMatrixable[P <: Position]: Matrixable[List[Cell[P]], P] = {
    new Matrixable[List[Cell[P]], P] { def convert(t: List[Cell[P]]): TypedPipe[Cell[P]] = new IterablePipe(t) }
  }
  /** Converts a `Cell[P]` tuple into a `TypedPipe[Cell[P]]`. */
  implicit def CellMatrixable[P <: Position]: Matrixable[Cell[P], P] = {
    new Matrixable[Cell[P], P] { def convert(t: Cell[P]): TypedPipe[Cell[P]] = new IterablePipe(List(t)) }
  }
}

