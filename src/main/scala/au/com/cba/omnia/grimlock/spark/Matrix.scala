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

package au.com.cba.omnia.grimlock.spark

import au.com.cba.omnia.grimlock.framework.{
  ExpandableMatrix => BaseExpandableMatrix,
  Matrix => BaseMatrix,
  Matrixable => BaseMatrixable,
  Nameable => BaseNameable,
  Persist => BasePersist,
  ReduceableMatrix => BaseReduceableMatrix,
  _
}
import au.com.cba.omnia.grimlock.framework.content._
import au.com.cba.omnia.grimlock.framework.content.metadata._
import au.com.cba.omnia.grimlock.framework.derive._
import au.com.cba.omnia.grimlock.framework.encoding._
import au.com.cba.omnia.grimlock.framework.pairwise._
import au.com.cba.omnia.grimlock.framework.partition._
import au.com.cba.omnia.grimlock.framework.position._
import au.com.cba.omnia.grimlock.framework.reduce._
import au.com.cba.omnia.grimlock.framework.sample._
import au.com.cba.omnia.grimlock.framework.squash._
import au.com.cba.omnia.grimlock.framework.transform._
import au.com.cba.omnia.grimlock.framework.utility._

import au.com.cba.omnia.grimlock.spark.Matrix._
import au.com.cba.omnia.grimlock.spark.Matrixable._

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/** Base trait for matrix operations using a `RDD[Cell[P]]`. */
trait Matrix[P <: Position] extends BaseMatrix[P] with Persist[Cell[P]] {
  type U[A] = RDD[A]
  type E[B] = B
  type S = Matrix[P]

  def change[T, D <: Dimension](slice: Slice[P, D], positions: T, schema: Schema)(implicit ev1: PosDimDep[P, D],
    ev2: BaseNameable[T, P, slice.S, D, RDD], ev3: ClassTag[slice.S]): RDD[Cell[P]] = {
    data
      .keyBy { case c => slice.selected(c.position) }
      .leftOuterJoin(ev2.convert(this, slice, positions).keyBy { case (p, i) => p })
      .flatMap {
        case (_, (c, po)) => po match {
          case Some(_) => schema.decode(c.content.value.toShortString).map { case con => Cell(c.position, con) }
          case None => Some(c)
        }
      }
  }

  def derive[D <: Dimension, T](slice: Slice[P, D], derivers: T)(implicit ev1: PosDimDep[P, D],
    ev2: Derivable[T], ev3: slice.R =!= Position0D, ev4: ClassTag[slice.S]): RDD[Cell[slice.S#M]] = ??? /*{
    val d = ev2.convert(derivers)

    data
      .map { case Cell(p, c) => (Cell(slice.selected(p), c), slice.remainder(p)) }
      .groupBy { case (c, r) => c.position }
      .sortBy { case (c, r) => r }
      .scanLeft(Option.empty[(d.T, Collection[Cell[slice.S#M]])]) {
        case (None, (c, r)) => Some((d.initialise(slice)(c, r), Collection[Cell[slice.S#M]]()))
        case (Some((t, _)), (c, r)) => Some(d.present(slice)(c, r, t))
      }
      .flatMap {
        case (p, Some((t, c))) => c.toList
        case _ => List()
      }
  }*/

  def deriveWithValue[D <: Dimension, T, W](slice: Slice[P, D], derivers: T, value: W)(implicit ev1: PosDimDep[P, D],
    ev2: DerivableWithValue[T, W], ev3: slice.R =!= Position0D): RDD[Cell[slice.S#M]] = ??? /*{
    val d = ev2.convert(derivers)

    data
      .leftCross(value)
      .map { case (Cell(p, c), vo) => (Cell(slice.selected(p), c), slice.remainder(p), vo.get) }
      .groupBy { case (c, r, v) => c.position }
      .sortBy { case (c, r, v) => r }
      .scanLeft(Option.empty[(d.T, Collection[Cell[slice.S#M]])]) {
        case (None, (c, r, v)) => Some((d.initialise(slice, v)(c, r), Collection[Cell[slice.S#M]]()))
        case (Some((t, _)), (c, r, v)) => Some(d.present(slice, v)(c, r, t))
      }
      .flatMap {
        case (p, Some((t, c))) => c.toList
        case _ => List()
      }
  }*/

  def get[T](positions: T)(implicit ev1: PositionDistributable[T, P, RDD], ev2: ClassTag[P]): RDD[Cell[P]] = {
    data
      .keyBy { case c => c.position }
      .join(ev1.convert(positions).keyBy { case p => p })
      .map { case (_, (c, _)) => c }
  }

  def join[D <: Dimension](slice: Slice[P, D], that: Matrix[P])(implicit ev1: PosDimDep[P, D], ev2: P =!= Position1D,
    ev3: ClassTag[slice.S]): RDD[Cell[P]] = {
    val keep = names(slice)
      .keyBy { case (p, i) => p }
      .join(that.names(slice).keyBy { case (p, i) => p })

    data
      .keyBy { case c => slice.selected(c.position) }
      .join(keep)
      .map { case (_, (c, _)) => c } ++
      that
      .data
      .keyBy { case c => slice.selected(c.position) }
      .join(keep)
      .map { case (_, (c, _)) => c }
  }

  def names[D <: Dimension](slice: Slice[P, D])(implicit ev1: PosDimDep[P, D], ev2: slice.S =!= Position0D,
    ev3: ClassTag[slice.S]): RDD[(slice.S, Long)] = {
    Names.number(data.map { case c => slice.selected(c.position) }.distinct)
  }

  def pairwise[D <: Dimension, T](slice: Slice[P, D], operators: T)(implicit ev1: PosDimDep[P, D], ev2: Operable[T],
    ev3: slice.S =!= Position0D, ev4: ClassTag[slice.S], ev5: ClassTag[slice.R]): RDD[Cell[slice.R#M]] = {
    val o = ev2.convert(operators)

    pairwise(slice).flatMap { case (lc, rc, r) => o.compute(slice)(lc, rc, r).toList }
  }

  def pairwiseWithValue[D <: Dimension, T, W](slice: Slice[P, D], operators: T, value: W)(implicit ev1: PosDimDep[P, D],
    ev2: OperableWithValue[T, W], ev3: slice.S =!= Position0D, ev4: ClassTag[slice.S],
      ev5: ClassTag[slice.R]): RDD[Cell[slice.R#M]] = {
    val o = ev2.convert(operators)

    pairwise(slice).flatMap { case (lc, rc, r) => o.compute(slice, value)(lc, rc, r).toList }
  }

  def pairwiseBetween[D <: Dimension, T](slice: Slice[P, D], that: Matrix[P], operators: T)(
    implicit ev1: PosDimDep[P, D], ev2: Operable[T], ev3: slice.S =!= Position0D, ev4: ClassTag[slice.S],
      ev5: ClassTag[slice.R]): RDD[Cell[slice.R#M]] = {
    val o = ev2.convert(operators)

    pairwiseBetween(slice, that).flatMap { case (lc, rc, r) => o.compute(slice)(lc, rc, r).toList }
  }

  def pairwiseBetweenWithValue[D <: Dimension, T, W](slice: Slice[P, D], that: Matrix[P], operators: T, value: W)(
    implicit ev1: PosDimDep[P, D], ev2: OperableWithValue[T, W], ev3: slice.S =!= Position0D, ev4: ClassTag[slice.S],
      ev5: ClassTag[slice.R]): RDD[Cell[slice.R#M]] = {
    val o = ev2.convert(operators)

    pairwiseBetween(slice, that).flatMap { case (lc, rc, r) => o.compute(slice, value)(lc, rc, r).toList }
  }

  def partition[S: Ordering](partitioner: Partitioner with Assign { type T = S }): RDD[(S, Cell[P])] = {
    data.flatMap { case c => partitioner.assign(c.position).toList(c) }
  }

  def partitionWithValue[S: Ordering, W](partitioner: Partitioner with AssignWithValue { type V >: W; type T = S },
    value: W): RDD[(S, Cell[P])] = {
    data.flatMap { case c => partitioner.assign(c.position, value).toList(c) }
  }

  def reduceAndExpand[T, D <: Dimension](slice: Slice[P, D], reducers: T)(implicit ev1: PosDimDep[P, D],
    ev2: ReducibleMultiple[T], ev3: ClassTag[slice.S]): RDD[Cell[slice.S#M]] = {
    val reducer = ev2.convert(reducers)
    implicit val ct = reducer.ct

    data
      .map { case c => (slice.selected(c.position), reducer.prepare(slice, c)) }
      .reduceByKey { case (lt, rt) => reducer.reduce(lt, rt) }
      .flatMap { case (p, t) => reducer.presentMultiple(p, t).toList }
  }

  def reduceAndExpandWithValue[T, D <: Dimension, V](slice: Slice[P, D], reducers: T, value: V)(
    implicit ev1: PosDimDep[P, D], ev2: ReducibleMultipleWithValue[T, V],
      ev3: ClassTag[slice.S]): RDD[Cell[slice.S#M]] = {
    val reducer = ev2.convert(reducers)
    implicit val ct = reducer.ct

    data
      .map { case c => (slice.selected(c.position), reducer.prepare(slice, c, value)) }
      .reduceByKey { case (lt, rt) => reducer.reduce(lt, rt) }
      .flatMap { case (p, t) => reducer.presentMultiple(p, t).toList }
  }

  def refine(f: Cell[P] => Boolean): RDD[Cell[P]] = data.filter { case c => f(c) }

  def refineWithValue[V](f: (Cell[P], V) => Boolean, value: V): RDD[Cell[P]] = data.filter { case c => f(c, value) }

  def rename[D <: Dimension](dim: D, renamer: (Dimension, Cell[P]) => P)(
    implicit ev: PosDimDep[P, D]): RDD[Cell[P]] = data.map { case c => Cell(renamer(dim, c), c.content) }

  def renameWithValue[D <: Dimension, V](dim: D, renamer: (Dimension, Cell[P], V) => P, value: V)(
    implicit ev: PosDimDep[P, D]): RDD[Cell[P]] = data.map { case c => Cell(renamer(dim, c, value), c.content) }

  def sample(sampler: Sampler with Select): RDD[Cell[P]] = data.filter { case c => sampler.select(c.position) }

  def sampleWithValue[W](sampler: Sampler with SelectWithValue { type V >: W }, value: W): RDD[Cell[P]] = {
    data.filter { case c => sampler.select(c.position, value) }
  }

  def set[T](positions: T, value: Content)(implicit ev1: PositionDistributable[T, P, RDD],
    ev2: ClassTag[P]): RDD[Cell[P]] = set(ev1.convert(positions).map { case p => Cell(p, value) })

  def set[T](values: T)(implicit ev1: BaseMatrixable[T, P, RDD], ev2: ClassTag[P]): RDD[Cell[P]] = {
    data
      .keyBy { case c => c.position }
      .fullOuterJoin(ev1.convert(values).keyBy { case c => c.position })
      .map { case (_, (c, cn)) => cn.getOrElse(c.get) }
  }

  def shape(): RDD[Cell[Position1D]] = {
    data
      .flatMap { case c => c.position.coordinates.map(_.toString).zipWithIndex }
      .distinct
      .groupBy { case (s, i) => i }
      .map {
        case (i, s) => Cell(Position1D(Dimension.All(i).toString), Content(DiscreteSchema[Codex.LongCodex](), s.size))
      }
  }

  def size[D <: Dimension](dim: D, distinct: Boolean = false)(implicit ev: PosDimDep[P, D]): RDD[Cell[Position1D]] = {
    val coords = data.map { case c => c.position(dim) }
    val dist = if (distinct) { coords } else { coords.distinct() }

    dist
      .context
      .parallelize(List(Cell(Position1D(dim.toString), Content(DiscreteSchema[Codex.LongCodex](), dist.count))))
  }

  def slice[T, D <: Dimension](slice: Slice[P, D], positions: T, keep: Boolean)(implicit ev1: PosDimDep[P, D],
    ev2: BaseNameable[T, P, slice.S, D, RDD], ev3: ClassTag[slice.S]): RDD[Cell[P]] = {
    val pos = ev2.convert(this, slice, positions)
    val wanted = keep match {
      case true => pos
      case false =>
        names(slice)
          .keyBy { case (p, i) => p }
          .leftOuterJoin(pos.keyBy { case (p, i) => p })
          .flatMap {
            case (p, (lpi, rpi)) => rpi match {
              case Some(_) => None
              case None => Some(lpi)
            }
          }
    }

    data
      .keyBy { case c => slice.selected(c.position) }
      .join(wanted.keyBy { case (p, i) => p })
      .map { case (_, (c, _)) => c }
  }

  def toMap[D <: Dimension](slice: Slice[P, D])(implicit ev1: PosDimDep[P, D], ev2: slice.S =!= Position0D,
    ev3: ClassTag[slice.S]): Map[slice.S, slice.C] = {
    data
      .map { case c => (c.position, slice.toMap(c)) }
      .keyBy { case (p, m) => slice.selected(p) }
      .reduceByKey { case ((lp, lm), (rp, rm)) => (lp, slice.combineMaps(lp, lm, rm)) }
      .map { case (_, (_, m)) => m }
      .reduce { case (lm, rm) => lm ++ rm }
  }

  def transform[T](transformers: T)(implicit ev: Transformable[T]): RDD[Cell[P]] = {
    val t = ev.convert(transformers)

    data.flatMap { case c => t.present(c).toList }
  }

  def transformWithValue[T, V](transformers: T, value: V)(implicit ev: TransformableWithValue[T, V]): RDD[Cell[P]] = {
    val t = ev.convert(transformers)

    data.flatMap { case c => t.present(c, value).toList }
  }

  def types[D <: Dimension](slice: Slice[P, D], specific: Boolean = false)(implicit ev1: PosDimDep[P, D],
    ev2: slice.S =!= Position0D, ev3: ClassTag[slice.S]): RDD[(slice.S, Type)] = {
    data
      .map { case Cell(p, c) => (slice.selected(p), c.schema.kind) }
      .reduceByKey { case (lt, rt) => Type.getCommonType(lt, rt) }
      .map { case (p, t) => (p, if (specific) t else t.getGeneralisation()) }
  }

  def unique(): RDD[Content] = {
    data
      .map { case c => c.content }
      .distinct()
  }

  def unique[D <: Dimension](slice: Slice[P, D])(implicit ev: slice.S =!= Position0D): RDD[Cell[slice.S]] = {
    data
      .map { case Cell(p, c) => Cell(slice.selected(p), c) }
      .distinct()
  }

  def which(predicate: Predicate)(implicit ev: ClassTag[P]): RDD[P] = {
    data.collect { case c if predicate(c) => c.position }
  }

  def which[T, D <: Dimension](slice: Slice[P, D], positions: T, predicate: Predicate)(implicit ev1: PosDimDep[P, D],
    ev2: BaseNameable[T, P, slice.S, D, RDD], ev3: ClassTag[slice.S], ev4: ClassTag[P]): RDD[P] = {
    which(slice, List((positions, predicate)))
  }

  def which[T, D <: Dimension](slice: Slice[P, D], pospred: List[(T, Predicate)])(implicit ev1: PosDimDep[P, D],
    ev2: BaseNameable[T, P, slice.S, D, RDD], ev3: ClassTag[slice.S], ev4: ClassTag[P]): RDD[P] = {
    val nampred = pospred.map { case (pos, pred) => ev2.convert(this, slice, pos).map { case (p, i) => (p, pred) } }
    val pipe = nampred.tail.foldLeft(nampred.head)((b, a) => b ++ a)

    data
      .keyBy { case c => slice.selected(c.position) }
      .join(pipe.keyBy { case (p, pred) => p })
      .collect { case (_, (c, (_, predicate))) if predicate(c) => c.position }
  }

  val data: RDD[Cell[P]]

  protected def persistDictionary(names: RDD[(Position1D, Long)], file: String, dictionary: String,
    separator: String) = {
    names
      .map { case (p, i) => p.toShortString(separator) + separator + i }
      .saveAsTextFile(dictionary.format(file))

    names
  }

  protected def persistDictionary(names: RDD[(Position1D, Long)], file: String, dictionary: String,
    separator: String, dim: Dimension) = {
    names
      .map { case (p, i) => p.toShortString(separator) + separator + i }
      .saveAsTextFile(dictionary.format(file, dim.index))

    names
  }

  private def pairwise[D <: Dimension](slice: Slice[P, D])(implicit ev1: PosDimDep[P, D], ev2: ClassTag[slice.S],
    ev3: ClassTag[slice.R]): RDD[(Cell[slice.S], Cell[slice.S], slice.R)] = {
    val wanted = names(slice).map { case (p, i) => p }
    val values = data.keyBy { case Cell(p, _) => (slice.selected(p), slice.remainder(p)) }
    val other = data.map { case c => slice.remainder(c.position) }.distinct

    wanted
      .cartesian(wanted)
      .cartesian(other)
      .map { case ((l, r), o) => (l, r, o) }
      .keyBy { case (l, _, o) => (l, o) }
      .join(values)
      .keyBy { case (_, ((_, r, o), _)) => (r, o) }
      .join(values)
      .map { case (_, ((_, ((lp, rp, r), lc)), rc)) => (Cell(lp, lc.content), Cell(rp, rc.content), r) }
  }

  private def pairwiseBetween[D <: Dimension](slice: Slice[P, D], that: Matrix[P])(implicit ev1: PosDimDep[P, D],
    ev2: ClassTag[slice.S], ev3: ClassTag[slice.R]): RDD[(Cell[slice.S], Cell[slice.S], slice.R)] = {
    val thisWanted = names(slice).map { case (p, i) => p }
    val thisValues = data.keyBy { case Cell(p, _) => (slice.selected(p), slice.remainder(p)) }

    val thatWanted = that.names(slice).map { case (p, i) => p }
    val thatValues = that.data.keyBy { case Cell(p, _) => (slice.selected(p), slice.remainder(p)) }

    val other = data.map { case c => slice.remainder(c.position) }.distinct

    thisWanted
      .cartesian(thatWanted)
      .cartesian(other)
      .map { case ((l, r), o) => (l, r, o) }
      .keyBy { case (l, _, o) => (l, o) }
      .join(thisValues)
      .keyBy { case (_, ((_, r, o), _)) => (r, o) }
      .join(thatValues)
      .map { case (_, ((_, ((lp, rp, r), lc)), rc)) => (Cell(lp, lc.content), Cell(rp, rc.content), r) }
  }
}

/** Base trait for methods that reduce the number of dimensions or that can be filled using a `RDD[Cell[P]]`. */
trait ReduceableMatrix[P <: Position with ReduceablePosition] extends BaseReduceableMatrix[P] { self: Matrix[P] =>

  def fillHetrogenous[D <: Dimension, Q <: Position](slice: Slice[P, D], values: RDD[Cell[Q]])(
    implicit ev1: PosDimDep[P, D], ev2: ClassTag[P], ev3: ClassTag[slice.S], ev4: slice.S =:= Q): RDD[Cell[P]] = {
    val dense = domain
      .keyBy { case p => slice.selected(p) }
      .join(values.keyBy { case c => c.position.asInstanceOf[slice.S] })
      .map { case (_, (p, c)) => Cell(p, c.content) }

    dense
      .keyBy { case c => c.position }
      .leftOuterJoin(data.keyBy { case c => c.position })
      .map { case (p, (fc, co)) => co.getOrElse(fc) }
  }

  def fillHomogenous(value: Content)(implicit ev: ClassTag[P]): RDD[Cell[P]] = {
    domain
      .keyBy { case p => p }
      .leftOuterJoin(data.keyBy { case c => c.position })
      .map { case (p, (_, co)) => co.getOrElse(Cell(p, value)) }
  }

  def melt[D <: Dimension, E <: Dimension](dim: D, into: E, separator: String = ".")(implicit ev1: PosDimDep[P, D],
    ev2: PosDimDep[P, E], ne: D =!= E): RDD[Cell[P#L]] = {
    data.map { case Cell(p, c) => Cell(p.melt(dim, into, separator), c) }
  }

  def reduce[D <: Dimension](slice: Slice[P, D], reducer: Reducer with Prepare with PresentSingle)(
    implicit ev1: PosDimDep[P, D], ev2: ClassTag[slice.S]): RDD[Cell[slice.S]] = {
    implicit val ct = reducer.ct

    data
      .map { case c => (slice.selected(c.position), reducer.prepare(slice, c)) }
      .reduceByKey { case (lt, rt) => reducer.reduce(lt, rt) }
      .flatMap { case (p, t) => reducer.presentSingle(p, t) }
  }

  def reduceWithValue[D <: Dimension, W](slice: Slice[P, D],
    reducer: Reducer with PrepareWithValue with PresentSingle { type V >: W }, value: W)(implicit ev1: PosDimDep[P, D],
      ev2: ClassTag[slice.S]): RDD[Cell[slice.S]] = {
    implicit val ct = reducer.ct

    data
      .map { case c => (slice.selected(c.position), reducer.prepare(slice, c, value)) }
      .reduceByKey { case (lt, rt) => reducer.reduce(lt, rt) }
      .flatMap { case (p, t) => reducer.presentSingle(p, t) }
  }

  def squash[D <: Dimension](dim: D, squasher: Squasher with Reduce)(implicit ev: PosDimDep[P, D]): RDD[Cell[P#L]] = {
    data
      .keyBy { case c => c.position.remove(dim) }
      .reduceByKey { case (x, y) => squasher.reduce(dim, x, y) }
      .map { case (p, c) => Cell(p, c.content) }
  }

  def squashWithValue[D <: Dimension, W](dim: D, squasher: Squasher with ReduceWithValue { type V >: W },
    value: W)(implicit ev: PosDimDep[P, D]): RDD[Cell[P#L]] = {
    data
      .keyBy { case c => c.position.remove(dim) }
      .reduceByKey { case (x, y) => squasher.reduce(dim, x, y, value) }
      .map { case (p, c) => Cell(p, c.content) }
  }
}

/** Base trait for methods that expand the number of dimension of a matrix using a `RDD[Cell[P]]`. */
trait ExpandableMatrix[P <: Position with ExpandablePosition] extends BaseExpandableMatrix[P] { self: Matrix[P] =>

  def expand(expander: Cell[P] => P#M): RDD[Cell[P#M]] = data.map { case c => Cell(expander(c), c.content) }

  def expandWithValue[V](expander: (Cell[P], V) => P#M, value: V): RDD[Cell[P#M]] = {
    data.map { case c => Cell(expander(c, value), c.content) }
  }

  def transformAndExpand[T](transformers: T)(implicit ev: TransformableExpanded[T]): RDD[Cell[P#M]] = {
    val t = ev.convert(transformers)

    data.flatMap { case c => t.present(c).toList }
  }

  def transformAndExpandWithValue[T, V](transformers: T, value: V)(
    implicit ev: TransformableExpandedWithValue[T, V]): RDD[Cell[P#M]] = {
    val t = ev.convert(transformers)

    data.flatMap { case c => t.present(c, value).toList }
  }
}

object Matrix {
  /**
   * Read column oriented, pipe separated matrix data into a `RDD[Cell[Position1D]]`.
   *
   * @param file      The file to read from.
   * @param separator The column separator.
   * @param first     The codex for decoding the first dimension.
   */
  def read1D(file: String, separator: String = "|", first: Codex = StringCodex)(
    implicit sc: SparkContext): RDD[Cell[Position1D]] = {
    sc.textFile(file).flatMap { Cell.parse1D(_, separator, first) }
  }

  /**
   * Read column oriented, pipe separated data into a `RDD[Cell[Position1D]]`.
   *
   * @param file      The file to read from.
   * @param dict      The dictionary describing the features in the data.
   * @param separator The column separator.
   * @param first     The codex for decoding the first dimension.
   */
  def read1DWithDictionary(file: String, dict: Map[String, Schema], separator: String = "|",
    first: Codex = StringCodex)(implicit sc: SparkContext): RDD[Cell[Position1D]] = {
    sc.textFile(file).flatMap { Cell.parse1DWithDictionary(_, dict, separator, first) }
  }

  /**
   * Read column oriented, pipe separated data into a `RDD[Cell[Position1D]]`.
   *
   * @param file      The file to read from.
   * @param schema    The schema for decoding the data.
   * @param separator The column separator.
   * @param first     The codex for decoding the first dimension.
   */
  def read1DWithSchema(file: String, schema: Schema, separator: String = "|", first: Codex = StringCodex)(
    implicit sc: SparkContext): RDD[Cell[Position1D]] = {
    sc.textFile(file).flatMap { Cell.parse1DWithSchema(_, schema, separator, first) }
  }

  /**
   * Read column oriented, pipe separated matrix data into a `RDD[Cell[Position2D]]`.
   *
   * @param file      The file to read from.
   * @param separator The column separator.
   * @param first     The codex for decoding the first dimension.
   * @param second    The codex for decoding the second dimension.
   */
  def read2D(file: String, separator: String = "|", first: Codex = StringCodex, second: Codex = StringCodex)(
    implicit sc: SparkContext): RDD[Cell[Position2D]] = {
    sc.textFile(file).flatMap { Cell.parse2D(_, separator, first, second) }
  }

  /**
   * Read column oriented, pipe separated data into a `RDD[Cell[Position2D]]`.
   *
   * @param file      The file to read from.
   * @param dict      The dictionary describing the features in the data.
   * @param dim       The dimension on which to apply the dictionary.
   * @param separator The column separator.
   * @param first     The codex for decoding the first dimension.
   * @param second    The codex for decoding the second dimension.
   */
  def read2DWithDictionary[D <: Dimension](file: String, dict: Map[String, Schema], dim: D = Second,
    separator: String = "|", first: Codex = StringCodex, second: Codex = StringCodex)(
      implicit ev: PosDimDep[Position2D, D], sc: SparkContext): RDD[Cell[Position2D]] = {
    sc.textFile(file).flatMap { Cell.parse2DWithDictionary(_, dict, dim, separator, first, second) }
  }

  /**
   * Read column oriented, pipe separated data into a `RDD[Cell[Position2D]]`.
   *
   * @param file      The file to read from.
   * @param schema    The schema for decoding the data.
   * @param separator The column separator.
   * @param first     The codex for decoding the first dimension.
   * @param second    The codex for decoding the second dimension.
   */
  def read2DWithSchema(file: String, schema: Schema, separator: String = "|", first: Codex = StringCodex,
    second: Codex = StringCodex)(implicit sc: SparkContext): RDD[Cell[Position2D]] = {
    sc.textFile(file).flatMap { Cell.parse2DWithSchema(_, schema, separator, first, second) }
  }

  /**
   * Read column oriented, pipe separated matrix data into a `RDD[Cell[Position3D]]`.
   *
   * @param file      The file to read from.
   * @param separator The column separator.
   * @param first     The codex for decoding the first dimension.
   * @param second    The codex for decoding the second dimension.
   * @param third     The codex for decoding the third dimension.
   */
  def read3D(file: String, separator: String = "|", first: Codex = StringCodex, second: Codex = StringCodex,
    third: Codex = StringCodex)(implicit sc: SparkContext): RDD[Cell[Position3D]] = {
    sc.textFile(file).flatMap { Cell.parse3D(_, separator, first, second, third) }
  }

  /**
   * Read column oriented, pipe separated data into a `RDD[Cell[Position3D]]`.
   *
   * @param file      The file to read from.
   * @param dict      The dictionary describing the features in the data.
   * @param dim       The dimension on which to apply the dictionary.
   * @param separator The column separator.
   * @param first     The codex for decoding the first dimension.
   * @param second    The codex for decoding the second dimension.
   * @param third     The codex for decoding the third dimension.
   */
  def read3DWithDictionary[D <: Dimension](file: String, dict: Map[String, Schema], dim: D = Second,
    separator: String = "|", first: Codex = StringCodex, second: Codex = StringCodex, third: Codex = DateCodex)(
      implicit ev: PosDimDep[Position3D, D], sc: SparkContext): RDD[Cell[Position3D]] = {
    sc.textFile(file).flatMap { Cell.parse3DWithDictionary(_, dict, dim, separator, first, second, third) }
  }

  /**
   * Read column oriented, pipe separated data into a `RDD[Cell[Position3D]]`.
   *
   * @param file      The file to read from.
   * @param schema    The schema for decoding the data.
   * @param separator The column separator.
   * @param first     The codex for decoding the first dimension.
   * @param second    The codex for decoding the second dimension.
   * @param third     The codex for decoding the third dimension.
   */
  def read3DWithSchema(file: String, schema: Schema, separator: String = "|", first: Codex = StringCodex,
    second: Codex = StringCodex, third: Codex = DateCodex)(implicit sc: SparkContext): RDD[Cell[Position3D]] = {
    sc.textFile(file).flatMap { Cell.parse3DWithSchema(_, schema, separator, first, second, third) }
  }

  /**
   * Read tabled data into a `RDD[Cell[Position2D]]`.
   *
   * @param table     The file (table) to read from.
   * @param columns   `List[(String, Schema)]` describing each column in the table.
   * @param pkeyIndex Index (into `columns`) describing which column is the primary key.
   * @param separator The column separator.
   *
   * @note The returned `Position2D` consists of 2 string values. The first string value is the contents of the primary
   *       key column. The second string value is the name of the column.
   */
  def readTable(table: String, columns: List[(String, Schema)], pkeyIndex: Int = 0, separator: String = "\01")(
    implicit sc: SparkContext): RDD[Cell[Position2D]] = {
    sc.textFile(table).flatMap { Cell.parseTable(_, columns, pkeyIndex, separator) }
  }

  /** Conversion from `RDD[Cell[Position1D]]` to a Spark `Matrix1D`. */
  implicit def RDDP1DC2RDDM1D(data: RDD[Cell[Position1D]]): Matrix1D = new Matrix1D(data)
  /** Conversion from `RDD[Cell[Position2D]]` to a Spark `Matrix2D`. */
  implicit def RDDP2DC2RDDM2D(data: RDD[Cell[Position2D]]): Matrix2D = new Matrix2D(data)
  /** Conversion from `RDD[Cell[Position3D]]` to a Spark `Matrix3D`. */
  implicit def RDDP3DC2RDDM3D(data: RDD[Cell[Position3D]]): Matrix3D = new Matrix3D(data)
  /** Conversion from `RDD[Cell[Position4D]]` to a Spark `Matrix4D`. */
  implicit def RDDP4DC2RDDM4D(data: RDD[Cell[Position4D]]): Matrix4D = new Matrix4D(data)
  /** Conversion from `RDD[Cell[Position5D]]` to a Spark `Matrix5D`. */
  implicit def RDDP5DC2RDDM5D(data: RDD[Cell[Position5D]]): Matrix5D = new Matrix5D(data)

  /** Conversion from `List[(Valueable, Content)]` to a Spark `Matrix1D`. */
  implicit def LVCT2RDDM1D[V: Valueable](list: List[(V, Content)])(implicit sc: SparkContext): Matrix1D = {
    new Matrix1D(sc.parallelize(list.map { case (v, c) => Cell(Position1D(v), c) }))
  }
  /** Conversion from `List[(Valueable, Valueable, Content)]` to a Spark `Matrix2D`. */
  implicit def LVVCT2RDDM2D[V: Valueable, W: Valueable](list: List[(V, W, Content)])(
    implicit sc: SparkContext): Matrix2D = {
    new Matrix2D(sc.parallelize(list.map { case (v, w, c) => Cell(Position2D(v, w), c) }))
  }
  /** Conversion from `List[(Valueable, Valueable, Valueable, Content)]` to a Spark `Matrix3D`. */
  implicit def LVVVCT2RDDM3D[V: Valueable, W: Valueable, X: Valueable](
    list: List[(V, W, X, Content)])(implicit sc: SparkContext): Matrix3D = {
    new Matrix3D(sc.parallelize(list.map { case (v, w, x, c) => Cell(Position3D(v, w, x), c) }))
  }
  /** Conversion from `List[(Valueable, Valueable, Valueable, Valueable, Content)]` to a Spark `Matrix4D`. */
  implicit def LVVVVCT2RDDM4D[V: Valueable, W: Valueable, X: Valueable, Y: Valueable](
    list: List[(V, W, X, Y, Content)])(implicit sc: SparkContext): Matrix4D = {
    new Matrix4D(sc.parallelize(list.map { case (v, w, x, y, c) => Cell(Position4D(v, w, x, y), c) }))
  }
  /**
   * Conversion from `List[(Valueable, Valueable, Valueable, Valueable, Valueable, Content)]` to a Spark `Matrix5D`.
   */
  implicit def LVVVVVCT2RDDM5D[V: Valueable, W: Valueable, X: Valueable, Y: Valueable, Z: Valueable](
    list: List[(V, W, X, Y, Z, Content)])(implicit sc: SparkContext): Matrix5D = {
    new Matrix5D(sc.parallelize(list.map { case (v, w, x, y, z, c) => Cell(Position5D(v, w, x, y, z), c) }))
  }
}

/**
 * Rich wrapper around a `RDD[Cell[Position1D]]`.
 *
 * @param data `RDD[Cell[Position1D]]`.
 */
class Matrix1D(val data: RDD[Cell[Position1D]]) extends Matrix[Position1D] with ExpandableMatrix[Position1D] {
  def domain(): RDD[Position1D] = names(Over(First)).map { case (p, i) => p }

  /**
   * Persist a `Matrix1D` as sparse matrix file (index, value).
   *
   * @param file       File to write to.
   * @param dictionary Pattern for the dictionary file name.
   * @param separator  Column separator to use in dictionary file.
   *
   * @return A `RDD[Cell[Position1D]]`; that is it returns `data`.
   */
  def persistAsIV(file: String, dictionary: String = "%1$s.dict.%2$d",
    separator: String = "|"): RDD[Cell[Position1D]] = {
    persistAsIVWithNames(file, names(Over(First)), dictionary, separator)
  }

  /**
   * Persist a `Matrix1D` as sparse matrix file (index, value).
   *
   * @param file       File to write to.
   * @param names      The names to use for the first dimension (according to their ordering).
   * @param dictionary Pattern for the dictionary file name.
   * @param separator  Column separator to use in dictionary file.
   *
   * @return A `RDD[Cell[Position1D]]`; that is it returns `data`.
   *
   * @note If `names` contains a subset of the columns, then only those columns get persisted to file.
   */
  def persistAsIVWithNames(file: String, names: RDD[(Position1D, Long)], dictionary: String = "%1$s.dict.%2$d",
    separator: String = "|"): RDD[Cell[Position1D]] = {
    data
      .keyBy { case c => c.position }
      .join(persistDictionary(names, file, dictionary, separator, First))
      .map { case (_, (c, i)) => i + separator + c.content.value.toShortString }
      .saveAsTextFile(file)

    data
  }
}

/**
 * Rich wrapper around a `RDD[Cell[Position2D]]`.
 *
 * @param data `RDD[Cell[Position2D]]`.
 */
class Matrix2D(val data: RDD[Cell[Position2D]]) extends Matrix[Position2D] with ReduceableMatrix[Position2D]
  with ExpandableMatrix[Position2D] {
  def domain(): RDD[Position2D] = {
    names(Over(First))
      .map { case (Position1D(c), i) => c }
      .cartesian(names(Over(Second)).map { case (Position1D(c), i) => c })
      .map { case (c1, c2) => Position2D(c1, c2) }
  }

  /**
   * Permute the order of the coordinates in a position.
   *
   * @param first  Dimension used for the first coordinate.
   * @param second Dimension used for the second coordinate.
   */
  def permute[D <: Dimension, E <: Dimension](first: D, second: E)(implicit ev1: PosDimDep[Position2D, D],
    ev2: PosDimDep[Position2D, E], ne: D =!= E): RDD[Cell[Position2D]] = {
    data.map { case Cell(p, c) => Cell(p.permute(List(first, second)), c) }
  }

  /**
   * Persist a `Matrix2D` as a CSV file.
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
   * @return A `RDD[Cell[Position2D]]`; that is it returns `data`.
   */
  def persistAsCSV[D <: Dimension](slice: Slice[Position2D, D], file: String, separator: String = "|",
    escapee: Escape = Quote(), writeHeader: Boolean = true, header: String = "%s.header", writeRowId: Boolean = true,
    rowId: String = "id")(implicit ev1: BaseNameable[RDD[(slice.S, Long)], Position2D, slice.S, D, RDD],
      ev2: PosDimDep[Position2D, D], ev3: ClassTag[slice.S]): RDD[Cell[Position2D]] = {
    persistAsCSVWithNames(slice, file, names(slice), separator, escapee, writeHeader, header, writeRowId, rowId)
  }

  /**
   * Persist a `Matrix2D` as a CSV file.
   *
   * @param slice       Encapsulates the dimension that makes up the columns.
   * @param file        File to write to.
   * @param names       The names to use for the columns (according to their ordering).
   * @param separator   Column separator to use.
   * @param escapee     The method for escaping the separator character.
   * @param writeHeader Indicator of the header should be written to a separate file.
   * @param header      Postfix for the header file name.
   * @param writeRowId  Indicator if row names should be written.
   * @param rowId       Column name of row names.
   *
   * @return A `RDD[Cell[Position2D]]`; that is it returns `data`.
   *
   * @note If `names` contains a subset of the columns, then only those columns get persisted to file.
   */
  def persistAsCSVWithNames[T, D <: Dimension](slice: Slice[Position2D, D], file: String, names: T,
    separator: String = "|", escapee: Escape = Quote(), writeHeader: Boolean = true, header: String = "%s.header",
    writeRowId: Boolean = true, rowId: String = "id")(implicit ev1: BaseNameable[T, Position2D, slice.S, D, RDD],
      ev2: PosDimDep[Position2D, D], ev3: ClassTag[slice.S]): RDD[Cell[Position2D]] = ??? /*{
    // Note: Usage of .toShortString should be safe as data is written as string anyways. It does assume that all
    //       indices have unique short string representations.
    val columns = ev1.convert(this, slice, names)
      .map { List(_) }
      .sum
      .map { _.sortBy(_._2).map { case (p, i) => escapee.escape(p.toShortString(""), separator) } }

    if (writeHeader) {
      columns
        .map {
          case lst => (if (writeRowId) escapee.escape(rowId, separator) + separator else "") + lst.mkString(separator)
        }
        .write(TypedSink(TextLine(header.format(file))))
    }

    data
      .groupBy { case c => slice.remainder(c.position).toShortString("") }
      .mapValues {
        case Cell(p, c) => Map(escapee.escape(slice.selected(p).toShortString(""), separator) ->
          escapee.escape(c.value.toShortString, separator))
      }
      .sum
      .flatMapWithValue(columns) {
        case ((key, values), optCols) => optCols.map {
          case cols => (key, cols.map { case c => values.getOrElse(c, "") })
        }
      }
      .map {
        case (i, lst) => (if (writeRowId) escapee.escape(i, separator) + separator else "") + lst.mkString(separator)
      }
      .write(TypedSink(TextLine(file)))

    data
  }*/

  /**
   * Persist a `Matrix2D` as sparse matrix file (index, index, value).
   *
   * @param file       File to write to.
   * @param dictionary Pattern for the dictionary file name.
   * @param separator  Column separator to use in dictionary file.
   *
   * @return A `RDD[Cell[Position2D]]`; that is it returns `data`.
   *
   * @note R's slam package has a simple triplet matrix format (which in turn is used by the tm package). This format
   *       should be compatible.
   */
  def persistAsIV(file: String, dictionary: String = "%1$s.dict.%2$d",
    separator: String = "|"): RDD[Cell[Position2D]] = {
    persistAsIVWithNames(file, names(Over(First)), names(Over(Second)), dictionary, separator)
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
   * @return A `RDD[Cell[Position2D]]`; that is it returns `data`.
   *
   * @note If `names` contains a subset of the columns, then only those columns get persisted to file.
   * @note R's slam package has a simple triplet matrix format (which in turn is used by the tm package). This format
   *       should be compatible.
   */
  def persistAsIVWithNames(file: String, namesI: RDD[(Position1D, Long)], namesJ: RDD[(Position1D, Long)],
    dictionary: String = "%1$s.dict.%2$d", separator: String = "|"): RDD[Cell[Position2D]] = {
    data
      .keyBy { case c => Position1D(c.position(First)) }
      .join(persistDictionary(namesI, file, dictionary, separator, First))
      .values
      .keyBy { case (c, i) => Position1D(c.position(Second)) }
      .join(persistDictionary(namesJ, file, dictionary, separator, Second))
      .map { case (_, ((c, i), j)) => i + separator + j + separator + c.content.value.toShortString }
      .saveAsTextFile(file)

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
   * @return A `RDD[Cell[Position2D]]`; that is it returns `data`.
   */
  def persistAsLDA[D <: Dimension](slice: Slice[Position2D, D], file: String, dictionary: String = "%s.dict",
    separator: String = "|", addId: Boolean = false)(implicit ev1: PosDimDep[Position2D, D],
      ev2: ClassTag[slice.S]): RDD[Cell[Position2D]] = {
    persistAsLDAWithNames(slice, file, names(Along(slice.dimension)), dictionary, separator, addId)
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
   * @return A `RDD[Cell[Position2D]]`; that is it returns `data`.
   *
   * @note If `names` contains a subset of the columns, then only those columns get persisted to file.
   */
  def persistAsLDAWithNames[D <: Dimension](slice: Slice[Position2D, D], file: String, names: RDD[(Position1D, Long)],
    dictionary: String = "%s.dict", separator: String = "|", addId: Boolean = false)(
      implicit ev1: PosDimDep[Position2D, D], ev2: ClassTag[slice.S]): RDD[Cell[Position2D]] = {
    data
      .keyBy { case c => slice.remainder(c.position).asInstanceOf[Position1D] }
      .join(persistDictionary(names, file, dictionary, separator))
      .map { case (_, (Cell(p, c), i)) => (p, " " + i + ":" + c.value.toShortString, 1L) }
      .keyBy { case (p, ics, m) => slice.selected(p) }
      .reduceByKey { case ((p, ls, lm), (_, rs, rm)) => (p, ls + rs, lm + rm) }
      .map { case (p, (_, ics, m)) => if (addId) p.toShortString(separator) + separator + m + ics else m + ics }
      .saveAsTextFile(file)

    data
  }

  /**
   * Persist a `Matrix2D` as a Vowpal Wabbit file.
   *
   * @param slice      Encapsulates the dimension that makes up the columns.
   * @param labels     The labels to write with.
   * @param file       File to write to.
   * @param dictionary Pattern for the dictionary file name.
   * @param separator  Column separator to use in dictionary file.
   *
   * @return A `RDD[Cell[Position2D]]`; that is it returns `data`.
   */
  def persistAsVW[D <: Dimension](slice: Slice[Position2D, D], labels: RDD[Cell[Position1D]], file: String,
    dictionary: String = "%s.dict", separator: String = ":")(
      implicit ev: PosDimDep[Position2D, D]): RDD[Cell[Position2D]] = {
    persistAsVWWithNames(slice, labels, file, names(Along(slice.dimension)), dictionary, separator)
  }

  /**
   * Persist a `Matrix2D` as a Vowpal Wabbit file.
   *
   * @param slice      Encapsulates the dimension that makes up the columns.
   * @param labels     The labels to write with.
   * @param file       File to write to.
   * @param names      The names to use for the columns (according to their ordering).
   * @param dictionary Pattern for the dictionary file name.
   * @param separator  Column separator to use in dictionary file.
   *
   * @return A `RDD[Cell[Position2D]]`; that is it returns `data`.
   *
   * @note If `names` contains a subset of the columns, then only those columns get persisted to file.
   */
  def persistAsVWWithNames[D <: Dimension](slice: Slice[Position2D, D], labels: RDD[Cell[Position1D]],
    file: String, names: RDD[(Position1D, Long)], dictionary: String = "%s.dict", separator: String = ":")(
      implicit ev: PosDimDep[Position2D, D]): RDD[Cell[Position2D]] = {
    data
      .keyBy { case c => slice.remainder(c.position).asInstanceOf[Position1D] }
      .join(persistDictionary(names, file, dictionary, separator))
      .map { case (_, (Cell(p, c), i)) => (p, " " + i + ":" + c.value.toShortString) }
      .keyBy { case (p, ics) => slice.selected(p).asInstanceOf[Position1D] }
      .reduceByKey { case ((p, ls), (_, rs)) => (p, ls + rs) }
      .join(labels.keyBy { case c => c.position })
      .map { case (p, ((_, ics), c)) => c.content.value.toShortString + " " + p.toShortString(separator) + "|" + ics }
      .saveAsTextFile(file)

    data
  }
}

/**
 * Rich wrapper around a `RDD[Cell[Position3D]]`.
 *
 * @param data `RDD[Cell[Position3D]]`.
 */
class Matrix3D(val data: RDD[Cell[Position3D]]) extends Matrix[Position3D] with ReduceableMatrix[Position3D]
  with ExpandableMatrix[Position3D] {
  def domain(): RDD[Position3D] = {
    names(Over(First))
      .map { case (Position1D(c), i) => c }
      .cartesian(names(Over(Second)).map { case (Position1D(c), i) => c })
      .cartesian(names(Over(Third)).map { case (Position1D(c), i) => c })
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
    ne2: D =!= F, ne3: E =!= F): RDD[Cell[Position3D]] = {
    data.map { case Cell(p, c) => Cell(p.permute(List(first, second, third)), c) }
  }

  /**
   * Persist a `Matrix3D` as sparse matrix file (index, index, index, value).
   *
   * @param file       File to write to.
   * @param dictionary Pattern for the dictionary file name.
   * @param separator  Column separator to use in dictionary file.
   *
   * @return A `RDD[Cell[Position3D]]`; that is it returns `data`.
   */
  def persistAsIV(file: String, dictionary: String = "%1$s.dict.%2$d",
    separator: String = "|"): RDD[Cell[Position3D]] = {
    persistAsIVWithNames(file, names(Over(First)), names(Over(Second)), names(Over(Third)), dictionary, separator)
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
   * @return A `RDD[Cell[Position3D]]`; that is it returns `data`.
   *
   * @note If `names` contains a subset of the columns, then only those columns get persisted to file.
   */
  def persistAsIVWithNames(file: String, namesI: RDD[(Position1D, Long)], namesJ: RDD[(Position1D, Long)],
    namesK: RDD[(Position1D, Long)], dictionary: String = "%1$s.dict.%2$d",
      separator: String = "|"): RDD[Cell[Position3D]] = {
    data
      .keyBy { case c => Position1D(c.position(First)) }
      .join(persistDictionary(namesI, file, dictionary, separator, First))
      .values
      .keyBy { case (c, i) => Position1D(c.position(Second)) }
      .join(persistDictionary(namesJ, file, dictionary, separator, Second))
      .map { case (_, ((c, i), j)) => (c, i, j) }
      .keyBy { case (c, i, j) => Position1D(c.position(Third)) }
      .join(persistDictionary(namesK, file, dictionary, separator, Third))
      .map { case (_, ((c, i, j), k)) => i + separator + j + separator + k + separator + c.content.value.toShortString }
      .saveAsTextFile(file)

    data
  }
}

/**
 * Rich wrapper around a `RDD[Cell[Position4D]]`.
 *
 * @param data `RDD[Cell[Position4D]]`.
 */
class Matrix4D(val data: RDD[Cell[Position4D]]) extends Matrix[Position4D] with ReduceableMatrix[Position4D]
  with ExpandableMatrix[Position4D] {
  def domain(): RDD[Position4D] = {
    names(Over(First))
      .map { case (Position1D(c), i) => c }
      .cartesian(names(Over(Second)).map { case (Position1D(c), i) => c })
      .cartesian(names(Over(Third)).map { case (Position1D(c), i) => c })
      .cartesian(names(Over(Fourth)).map { case (Position1D(c), i) => c })
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
      ne6: F =!= G): RDD[Cell[Position4D]] = {
    data.map { case Cell(p, c) => Cell(p.permute(List(first, second, third, fourth)), c) }
  }

  /**
   * Persist a `Matrix4D` as sparse matrix file (index, index, index, index, value).
   *
   * @param file       File to write to.
   * @param dictionary Pattern for the dictionary file name.
   * @param separator  Column separator to use in dictionary file.
   *
   * @return A `RDD[Cell[Position4D]]`; that is it returns `data`.
   */
  def persistAsIV(file: String, dictionary: String = "%1$s.dict.%2$d",
    separator: String = "|"): RDD[Cell[Position4D]] = {
    persistAsIVWithNames(file, names(Over(First)), names(Over(Second)), names(Over(Third)), names(Over(Fourth)),
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
   * @return A `RDD[Cell[Position4D]]`; that is it returns `data`.
   *
   * @note If `names` contains a subset of the columns, then only those columns get persisted to file.
   */
  def persistAsIVWithNames(file: String, namesI: RDD[(Position1D, Long)], namesJ: RDD[(Position1D, Long)],
    namesK: RDD[(Position1D, Long)], namesL: RDD[(Position1D, Long)], dictionary: String = "%1$s.dict.%2$d",
      separator: String = "|"): RDD[Cell[Position4D]] = {
    data
      .keyBy { case c => Position1D(c.position(First)) }
      .join(persistDictionary(namesI, file, dictionary, separator, First))
      .values
      .keyBy { case (c, i) => Position1D(c.position(Second)) }
      .join(persistDictionary(namesJ, file, dictionary, separator, Second))
      .map { case (_, ((c, i), j)) => (c, i, j) }
      .keyBy { case (c, i, j) => Position1D(c.position(Third)) }
      .join(persistDictionary(namesK, file, dictionary, separator, Third))
      .map { case (_, ((c, i, j), k)) => (c, i, j, k) }
      .keyBy { case (c, i, j, p) => Position1D(c.position(Fourth)) }
      .join(persistDictionary(namesL, file, dictionary, separator, Fourth))
      .map {
        case (_, ((c, i, j, k), l)) =>
          i + separator + j + separator + k + separator + l + separator + c.content.value.toShortString
      }
      .saveAsTextFile(file)

    data
  }
}

/**
 * Rich wrapper around a `RDD[Cell[Position5D]]`.
 *
 * @param data `RDD[Cell[Position5D]]`.
 */
class Matrix5D(val data: RDD[Cell[Position5D]]) extends Matrix[Position5D] with ReduceableMatrix[Position5D] {
  def domain(): RDD[Position5D] = {
    names(Over(First))
      .map { case (Position1D(c), i) => c }
      .cartesian(names(Over(Second)).map { case (Position1D(c), i) => c })
      .cartesian(names(Over(Third)).map { case (Position1D(c), i) => c })
      .cartesian(names(Over(Fourth)).map { case (Position1D(c), i) => c })
      .cartesian(names(Over(Fifth)).map { case (Position1D(c), i) => c })
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
      ne10: G =!= H): RDD[Cell[Position5D]] = {
    data.map { case Cell(p, c) => Cell(p.permute(List(first, second, third, fourth, fifth)), c) }
  }

  /**
   * Persist a `Matrix5D` as sparse matrix file (index, index, index, index, index, value).
   *
   * @param file       File to write to.
   * @param dictionary Pattern for the dictionary file name.
   * @param separator  Column separator to use in dictionary file.
   *
   * @return A `RDD[Cell[Position5D]]`; that is it returns `data`.
   */
  def persistAsIV(file: String, dictionary: String = "%1$s.dict.%2$d",
    separator: String = "|"): RDD[Cell[Position5D]] = {
    persistAsIVWithNames(file, names(Over(First)), names(Over(Second)), names(Over(Third)), names(Over(Fourth)),
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
   * @return A `RDD[Cell[Position5D]]`; that is it returns `data`.
   *
   * @note If `names` contains a subset of the columns, then only those columns get persisted to file.
   */
  def persistAsIVWithNames(file: String, namesI: RDD[(Position1D, Long)], namesJ: RDD[(Position1D, Long)],
    namesK: RDD[(Position1D, Long)], namesL: RDD[(Position1D, Long)], namesM: RDD[(Position1D, Long)],
    dictionary: String = "%1$s.dict.%2$d", separator: String = "|"): RDD[Cell[Position5D]] = {
    data
      .keyBy { case c => Position1D(c.position(First)) }
      .join(persistDictionary(namesI, file, dictionary, separator, First))
      .values
      .keyBy { case (c, i) => Position1D(c.position(Second)) }
      .join(persistDictionary(namesJ, file, dictionary, separator, Second))
      .map { case (_, ((c, i), j)) => (c, i, j) }
      .keyBy { case (c, pi, pj) => Position1D(c.position(Third)) }
      .join(persistDictionary(namesK, file, dictionary, separator, Third))
      .map { case (_, ((c, i, j), k)) => (c, i, j, k) }
      .keyBy { case (c, i, j, k) => Position1D(c.position(Fourth)) }
      .join(persistDictionary(namesL, file, dictionary, separator, Fourth))
      .map { case (_, ((c, i, j, k), l)) => (c, i, j, k, l) }
      .keyBy { case (c, i, j, k, l) => Position1D(c.position(Fifth)) }
      .join(persistDictionary(namesM, file, dictionary, separator, Fifth))
      .map {
        case (_, ((c, i, j, k, l), m)) =>
          i + separator + j + separator + k + separator + l + separator + m + separator + c.content.value.toShortString
      }
      .saveAsTextFile(file)

    data
  }
}

/** Spark Companion object for the `Matrixable` type class. */
object Matrixable {
  /** Converts a `RDD[Cell[P]]` into a `RDD[Cell[P]]`; that is, it is a  pass through. */
  implicit def RDDC2RDDM[P <: Position]: BaseMatrixable[RDD[Cell[P]], P, RDD] = {
    new BaseMatrixable[RDD[Cell[P]], P, RDD] { def convert(t: RDD[Cell[P]]): RDD[Cell[P]] = t }
  }

  /** Converts a `List[Cell[P]]` into a `RDD[Cell[P]]`. */
  implicit def LC2RDDM[P <: Position](implicit sc: SparkContext,
    ct: ClassTag[P]): BaseMatrixable[List[Cell[P]], P, RDD] = {
    new BaseMatrixable[List[Cell[P]], P, RDD] { def convert(t: List[Cell[P]]): RDD[Cell[P]] = sc.parallelize(t) }
  }

  /** Converts a `Cell[P]` into a `RDD[Cell[P]]`. */
  implicit def C2RDDM[P <: Position](implicit sc: SparkContext, ct: ClassTag[P]): BaseMatrixable[Cell[P], P, RDD] = {
    new BaseMatrixable[Cell[P], P, RDD] { def convert(t: Cell[P]): RDD[Cell[P]] = sc.parallelize(List(t)) }
  }
}

