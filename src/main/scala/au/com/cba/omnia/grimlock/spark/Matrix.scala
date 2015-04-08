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

import au.com.cba.omnia.grimlock.SparkMatrixable._

import org.apache.spark.SparkContext
import org.apache.spark.rdd._

import scala.reflect._

/** Base trait for matrix operations using a `RDD[Cell[P]]`. */
trait SparkMatrix[P <: Position] extends Matrix[P] with SparkPersist[Cell[P]] {
  type U[A] = RDD[A]
  type E[B] = B
  type S = SparkMatrix[P]

  def change[T, D <: Dimension](slice: Slice[P, D], positions: T, schema: Schema)(implicit ev1: PosDimDep[P, D],
    ev2: Nameable[T, P, slice.S, D, RDD], ev3: ClassTag[slice.S]): RDD[Cell[P]] = {
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

  def join[D <: Dimension](slice: Slice[P, D], that: SparkMatrix[P])(implicit ev1: PosDimDep[P, D],
    ev2: P =!= Position1D, ev3: ClassTag[slice.S]): RDD[Cell[P]] = {
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
    SparkNames.number(data.map { case c => slice.selected(c.position) }.distinct)
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

  def pairwiseBetween[D <: Dimension, T](slice: Slice[P, D], that: SparkMatrix[P], operators: T)(
    implicit ev1: PosDimDep[P, D], ev2: Operable[T], ev3: slice.S =!= Position0D, ev4: ClassTag[slice.S],
      ev5: ClassTag[slice.R]): RDD[Cell[slice.R#M]] = {
    val o = ev2.convert(operators)

    pairwiseBetween(slice, that).flatMap { case (lc, rc, r) => o.compute(slice)(lc, rc, r).toList }
  }

  def pairwiseBetweenWithValue[D <: Dimension, T, W](slice: Slice[P, D], that: SparkMatrix[P], operators: T,
    value: W)(implicit ev1: PosDimDep[P, D], ev2: OperableWithValue[T, W], ev3: slice.S =!= Position0D,
      ev4: ClassTag[slice.S], ev5: ClassTag[slice.R]): RDD[Cell[slice.R#M]] = {
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
    ev2: ReducibleMultiple[T], ev3: ClassTag[slice.S]): RDD[Cell[slice.S#M]] = ??? /*{
    val reducer = ev2.convert(reducers)

    data
      .map { case c => (slice.selected(c.position), reducer.prepare(slice, c)) }
      .reduceByKey { case (lt, rt) => reducer.reduce(lt, rt) }
      .flatMap { case (p, t) => reducer.presentMultiple(p, t).toList }
  }*/

  def reduceAndExpandWithValue[T, D <: Dimension, V](slice: Slice[P, D], reducers: T, value: V)(
    implicit ev1: PosDimDep[P, D], ev2: ReducibleMultipleWithValue[T, V],
      ev3: ClassTag[slice.S]): RDD[Cell[slice.S#M]] = ??? /*{
    val reducer = ev2.convert(reducers)

    data
      .map { case c => (slice.selected(c.position), reducer.prepare(slice, c, value)) }
      .reduceByKey { case (lt, rt) => reducer.reduce(lt, rt) }
      .flatMap { case (p, t) => reducer.presentMultiple(p, t).toList }
  }*/

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

  def set[T](values: T)(implicit ev1: Matrixable[T, P, RDD], ev2: ClassTag[P]): RDD[Cell[P]] = {
    data
      .keyBy { case c => c.position }
      .leftOuterJoin(ev1.convert(values).keyBy { case c => c.position })
      .map { case (_, (c, cn)) => cn.getOrElse(c) }
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

  def size[D <: Dimension](dim: D, distinct: Boolean = false)(implicit ev: PosDimDep[P, D]): RDD[Cell[Position1D]] = ???
  /*{
    val coords = data.map { case c => c.position(dim) }
    val dist = if (distinct) { coords } else { coords.distinct() }

    dist
      .map { case _ => 1L }
      .sum
      .map { case sum => Cell(Position1D(dim.toString), Content(DiscreteSchema[Codex.LongCodex](), sum)) }
  }*/

  def slice[T, D <: Dimension](slice: Slice[P, D], positions: T, keep: Boolean)(implicit ev1: PosDimDep[P, D],
    ev2: Nameable[T, P, slice.S, D, RDD], ev3: ClassTag[slice.S]): RDD[Cell[P]] = ??? /*{
    val pos = ev2.convert(this, slice, positions)
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
      .groupBy { case c => slice.selected(c.position) }
      .join(wanted.groupBy { case (p, i) => p })
      .map { case (_, (c, _)) => c }
  }*/

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
      .reduceByKey {
        case (lt, rt) =>
          if (lt == rt) { lt }
          else if (lt.isSpecialisationOf(rt)) { rt }
          else if (rt.isSpecialisationOf(lt)) { lt }
          else if (lt.isSpecialisationOf(rt.getGeneralisation())) { rt.getGeneralisation() }
          else if (rt.isSpecialisationOf(lt.getGeneralisation())) { lt.getGeneralisation() }
          else { Type.Mixed }
      }
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
    ev2: Nameable[T, P, slice.S, D, RDD], ev3: ClassTag[slice.S], ev4: ClassTag[P]): RDD[P] = {
    which(slice, List((positions, predicate)))
  }

  def which[T, D <: Dimension](slice: Slice[P, D], pospred: List[(T, Predicate)])(implicit ev1: PosDimDep[P, D],
    ev2: Nameable[T, P, slice.S, D, RDD], ev3: ClassTag[slice.S], ev4: ClassTag[P]): RDD[P] = {
    val nampred = pospred.map { case (pos, pred) => ev2.convert(this, slice, pos).map { case (p, i) => (p, pred) } }
    val pipe = nampred.tail.foldLeft(nampred.head)((b, a) => b ++ a)

    data
      .keyBy { case c => slice.selected(c.position) }
      .join(pipe.keyBy { case (p, pred) => p })
      .collect { case (_, (c, (_, predicate))) if predicate(c) => c.position }
  }

  val data: RDD[Cell[P]]

  protected def persistDictionary(names: RDD[(Position1D, Long)], file: String, dictionary: String,
    separator: String) = ??? /*{
    val dict = names.groupBy { case (p, i) => p }

    dict
      .map { case (_, (p, i)) => p.toShortString(separator) + separator + i }
      .saveAsTextFile(dictionary.format(file))

    dict
  }*/

  protected def persistDictionary(names: RDD[(Position1D, Long)], file: String, dictionary: String,
    separator: String, dim: Dimension) = ??? /*{
    val dict = names.groupBy { case (p, j) => p }

    dict
      .map { case (_, (p, i)) => p.toShortString(separator) + separator + i }
      .saveAsTextFile(dictionary.format(file, dim.index))

    dict
  }*/

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

  private def pairwiseBetween[D <: Dimension](slice: Slice[P, D], that: SparkMatrix[P])(implicit ev1: PosDimDep[P, D],
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

/** Spark Companion object for the `Matrixable` type class. */
object SparkMatrixable {
  /** Converts a `RDD[Cell[P]]` into a `RDD[Cell[P]]`; that is, it is a  pass through. */
  implicit def TPC2M[P <: Position]: Matrixable[RDD[Cell[P]], P, RDD] = {
    new Matrixable[RDD[Cell[P]], P, RDD] { def convert(t: RDD[Cell[P]]): RDD[Cell[P]] = t }
  }
  /** Converts a `List[Cell[P]]` into a `RDD[Cell[P]]`. */
  implicit def LC2M[P <: Position](implicit sc: SparkContext, ct: ClassTag[P]): Matrixable[List[Cell[P]], P, RDD] = {
    new Matrixable[List[Cell[P]], P, RDD] { def convert(t: List[Cell[P]]): RDD[Cell[P]] = sc.parallelize(t) }
  }
  /** Converts a `Cell[P]` into a `RDD[Cell[P]]`. */
  implicit def C2M[P <: Position](implicit sc: SparkContext, ct: ClassTag[P]): Matrixable[Cell[P], P, RDD] = {
    new Matrixable[Cell[P], P, RDD] { def convert(t: Cell[P]): RDD[Cell[P]] = sc.parallelize(List(t)) }
  }
}

