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

import au.com.cba.omnia.grimlock.ScaldingMatrix._
import au.com.cba.omnia.grimlock.ScaldingMatrixable._

import cascading.flow.FlowDef
import com.twitter.scalding._
import com.twitter.scalding.TDsl._, Dsl._
import com.twitter.scalding.typed.{ IterablePipe, TypedSink }

import scala.reflect._

/** Base trait for matrix operations using a `TypedPipe[Cell[P]]`. */
trait ScaldingMatrix[P <: Position] extends Matrix[P] with ScaldingPersist[Cell[P]] {
  type U[A] = TypedPipe[A]
  type E[B] = ValuePipe[B]
  type S = ScaldingMatrix[P]

  def change[T, D <: Dimension](slice: Slice[P, D], positions: T, schema: Schema)(implicit ev1: PosDimDep[P, D],
    ev2: Nameable[T, P, slice.S, D, TypedPipe], ev3: ClassTag[slice.S]): TypedPipe[Cell[P]] = {
    data
      .groupBy { case c => slice.selected(c.position) }
      .leftJoin(ev2.convert(this, slice, positions).groupBy { case (p, i) => p })
      .flatMap {
        case (_, (c, po)) => po match {
          case Some(_) => schema.decode(c.content.value.toShortString).map { case con => Cell(c.position, con) }
          case None => Some(c)
        }
      }
  }

  def derive[D <: Dimension, T](slice: Slice[P, D], derivers: T)(implicit ev1: PosDimDep[P, D],
    ev2: Derivable[T], ev3: slice.R =!= Position0D, ev4: ClassTag[slice.S]): TypedPipe[Cell[slice.S#M]] = {
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
  }

  def deriveWithValue[D <: Dimension, T, W](slice: Slice[P, D], derivers: T, value: ValuePipe[W])(
    implicit ev1: PosDimDep[P, D], ev2: DerivableWithValue[T, W],
    ev3: slice.R =!= Position0D): TypedPipe[Cell[slice.S#M]] = {
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
  }

  def get[T](positions: T)(implicit ev1: PositionDistributable[T, P, TypedPipe],
    ev2: ClassTag[P]): TypedPipe[Cell[P]] = {
    data
      .groupBy { case c => c.position }
      .join(ev1.convert(positions).groupBy { case p => p })
      .map { case (_, (c, p)) => c }
  }

  def join[D <: Dimension](slice: Slice[P, D], that: ScaldingMatrix[P])(implicit ev1: PosDimDep[P, D],
    ev2: P =!= Position1D, ev3: ClassTag[slice.S]): TypedPipe[Cell[P]] = {
    val keep = names(slice)
      .groupBy { case (p, i) => p }
      .join(that.names(slice).groupBy { case (p, i) => p })

    data
      .groupBy { case c => slice.selected(c.position) }
      .join(keep)
      .map { case (_, (c, _)) => c } ++
      that
      .data
      .groupBy { case c => slice.selected(c.position) }
      .join(keep)
      .map { case (_, (c, _)) => c }
  }

  def names[D <: Dimension](slice: Slice[P, D])(implicit ev1: PosDimDep[P, D], ev2: slice.S =!= Position0D,
    ev3: ClassTag[slice.S]): TypedPipe[(slice.S, Long)] = {
    ScaldingNames.number(data.map { case c => slice.selected(c.position) }.distinct)
  }

  def pairwise[D <: Dimension, T](slice: Slice[P, D], operators: T)(implicit ev1: PosDimDep[P, D], ev2: Operable[T],
    ev3: slice.S =!= Position0D, ev4: ClassTag[slice.S], ev5: ClassTag[slice.R]): TypedPipe[Cell[slice.R#M]] = {
    val o = ev2.convert(operators)

    pairwise(slice).flatMap { case (lc, rc, r) => o.compute(slice)(lc, rc, r).toList }
  }

  def pairwiseWithValue[D <: Dimension, T, W](slice: Slice[P, D], operators: T, value: ValuePipe[W])(
    implicit ev1: PosDimDep[P, D], ev2: OperableWithValue[T, W], ev3: slice.S =!= Position0D, ev4: ClassTag[slice.S],
      ev5: ClassTag[slice.R]): TypedPipe[Cell[slice.R#M]] = {
    val o = ev2.convert(operators)

    pairwise(slice)
      .flatMapWithValue(value) { case ((lc, rc, r), vo) => o.compute(slice, vo.get)(lc, rc, r).toList }
  }

  def pairwiseBetween[D <: Dimension, T](slice: Slice[P, D], that: ScaldingMatrix[P], operators: T)(
    implicit ev1: PosDimDep[P, D], ev2: Operable[T], ev3: slice.S =!= Position0D, ev4: ClassTag[slice.S],
      ev5: ClassTag[slice.R]): TypedPipe[Cell[slice.R#M]] = {
    val o = ev2.convert(operators)

    pairwiseBetween(slice, that).flatMap { case (lc, rc, r) => o.compute(slice)(lc, rc, r).toList }
  }

  def pairwiseBetweenWithValue[D <: Dimension, T, W](slice: Slice[P, D], that: ScaldingMatrix[P], operators: T,
    value: ValuePipe[W])(implicit ev1: PosDimDep[P, D], ev2: OperableWithValue[T, W], ev3: slice.S =!= Position0D,
      ev4: ClassTag[slice.S], ev5: ClassTag[slice.R]): TypedPipe[Cell[slice.R#M]] = {
    val o = ev2.convert(operators)

    pairwiseBetween(slice, that)
      .flatMapWithValue(value) { case ((lc, rc, r), vo) => o.compute(slice, vo.get)(lc, rc, r).toList }
  }

  def partition[S: Ordering](partitioner: Partitioner with Assign { type T = S }): TypedPipe[(S, Cell[P])] = {
    data.flatMap { case c => partitioner.assign(c.position).toList(c) }
  }

  def partitionWithValue[S: Ordering, W](partitioner: Partitioner with AssignWithValue { type V >: W; type T = S },
    value: ValuePipe[W]): TypedPipe[(S, Cell[P])] = {
    data.flatMapWithValue(value) { case (c, vo) => partitioner.assign(c.position, vo.get).toList(c) }
  }

  def reduceAndExpand[T, D <: Dimension](slice: Slice[P, D], reducers: T)(implicit ev1: PosDimDep[P, D],
    ev2: ReducibleMultiple[T], ev3: ClassTag[slice.S]): TypedPipe[Cell[slice.S#M]] = {
    val reducer = ev2.convert(reducers)

    data
      .map { case c => (slice.selected(c.position), reducer.prepare(slice, c)) }
      .groupBy { case (p, t) => p }
      .reduce[(slice.S, reducer.T)] { case ((lp, lt), (rp, rt)) => (lp, reducer.reduce(lt, rt)) }
      .flatMap { case (_, (p, t)) => reducer.presentMultiple(p, t).toList }
  }

  def reduceAndExpandWithValue[T, D <: Dimension, V](slice: Slice[P, D], reducers: T, value: ValuePipe[V])(
    implicit ev1: PosDimDep[P, D], ev2: ReducibleMultipleWithValue[T, V],
      ev3: ClassTag[slice.S]): TypedPipe[Cell[slice.S#M]] = {
    val reducer = ev2.convert(reducers)

    data
      .leftCross(value)
      .map { case (c, vo) => (slice.selected(c.position), reducer.prepare(slice, c, vo.get)) }
      .groupBy { case (p, t) => p }
      .reduce[(slice.S, reducer.T)] { case ((lp, lt), (rp, rt)) => (lp, reducer.reduce(lt, rt)) }
      .flatMap { case (_, (p, t)) => reducer.presentMultiple(p, t).toList }
  }

  def refine(f: Cell[P] => Boolean): TypedPipe[Cell[P]] = data.filter { case c => f(c) }

  def refineWithValue[V](f: (Cell[P], V) => Boolean, value: ValuePipe[V]): TypedPipe[Cell[P]] = {
    data.filterWithValue(value) { case (c, vo) => f(c, vo.get) }
  }

  def rename[D <: Dimension](dim: D, renamer: (Dimension, Cell[P]) => P)(
    implicit ev: PosDimDep[P, D]): TypedPipe[Cell[P]] = data.map { case c => Cell(renamer(dim, c), c.content) }

  def renameWithValue[D <: Dimension, V](dim: D, renamer: (Dimension, Cell[P], V) => P, value: ValuePipe[V])(
    implicit ev: PosDimDep[P, D]): TypedPipe[Cell[P]] = {
    data.mapWithValue(value) { case (c, vo) => Cell(renamer(dim, c, vo.get), c.content) }
  }

  def sample(sampler: Sampler with Select): TypedPipe[Cell[P]] = data.filter { case c => sampler.select(c.position) }

  def sampleWithValue[W](sampler: Sampler with SelectWithValue { type V >: W },
    value: ValuePipe[W]): TypedPipe[Cell[P]] = {
    data.filterWithValue(value) { case (c, vo) => sampler.select(c.position, vo.get) }
  }

  def set[T](positions: T, value: Content)(implicit ev1: PositionDistributable[T, P, TypedPipe],
    ev2: ClassTag[P]): TypedPipe[Cell[P]] = {
    set(ev1.convert(positions).map { case p => Cell(p, value) })
  }

  def set[T](values: T)(implicit ev1: Matrixable[T, P, TypedPipe], ev2: ClassTag[P]): TypedPipe[Cell[P]] = {
    data
      .groupBy { case c => c.position }
      .outerJoin(ev1.convert(values).groupBy { case c => c.position })
      .map { case (_, (co, cn)) => cn.getOrElse(co.get) }
  }

  def shape(): TypedPipe[Cell[Position1D]] = {
    data
      .flatMap { case c => c.position.coordinates.map(_.toString).zipWithIndex }
      .distinct
      .groupBy { case (s, i) => i }
      .size
      .map {
        case (i, s) => Cell(Position1D(Dimension.All(i).toString), Content(DiscreteSchema[Codex.LongCodex](), s))
      }
  }

  def size[D <: Dimension](dim: D, distinct: Boolean = false)(
    implicit ev: PosDimDep[P, D]): TypedPipe[Cell[Position1D]] = {
    val coords = data.map { case c => c.position(dim) }
    val dist = if (distinct) { coords } else { coords.distinct(Value.Ordering) }

    dist
      .map { case _ => 1L }
      .sum
      .map { case sum => Cell(Position1D(dim.toString), Content(DiscreteSchema[Codex.LongCodex](), sum)) }
  }

  def slice[T, D <: Dimension](slice: Slice[P, D], positions: T, keep: Boolean)(implicit ev1: PosDimDep[P, D],
    ev2: Nameable[T, P, slice.S, D, TypedPipe], ev3: ClassTag[slice.S]): TypedPipe[Cell[P]] = {
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
  }

  def toMap[D <: Dimension](slice: Slice[P, D])(implicit ev1: PosDimDep[P, D], ev2: slice.S =!= Position0D,
    ev3: ClassTag[slice.S]): ValuePipe[Map[slice.S, slice.C]] = {
    data
      .map { case c => (c.position, slice.toMap(c)) }
      .groupBy { case (p, m) => slice.selected(p) }
      .reduce[(P, Map[slice.S, slice.C])] { case ((lp, lm), (rp, rm)) => (lp, slice.combineMaps(lp, lm, rm)) }
      .map { case (_, (_, m)) => m }
      .sum(new com.twitter.algebird.Semigroup[Map[slice.S, slice.C]] {
        def plus(l: Map[slice.S, slice.C], r: Map[slice.S, slice.C]): Map[slice.S, slice.C] = l ++ r
      })
  }

  def transform[T](transformers: T)(implicit ev: Transformable[T]): TypedPipe[Cell[P]] = {
    val t = ev.convert(transformers)

    data.flatMap { case c => t.present(c).toList }
  }

  def transformWithValue[T, V](transformers: T, value: ValuePipe[V])(
    implicit ev: TransformableWithValue[T, V]): TypedPipe[Cell[P]] = {
    val t = ev.convert(transformers)

    data.flatMapWithValue(value) { case (c, vo) => t.present(c, vo.get).toList }
  }

  def types[D <: Dimension](slice: Slice[P, D], specific: Boolean = false)(implicit ev1: PosDimDep[P, D],
    ev2: slice.S =!= Position0D, ev3: ClassTag[slice.S]): TypedPipe[(slice.S, Type)] = {
    data
      .map { case Cell(p, c) => (slice.selected(p), c.schema.kind) }
      .groupBy { case (p, t) => p }
      .reduce[(slice.S, Type)] {
        case ((lp, lt), (rp, rt)) =>
          (lp,
            if (lt == rt) { lt }
            else if (lt.isSpecialisationOf(rt)) { rt }
            else if (rt.isSpecialisationOf(lt)) { lt }
            else if (lt.isSpecialisationOf(rt.getGeneralisation())) { rt.getGeneralisation() }
            else if (rt.isSpecialisationOf(lt.getGeneralisation())) { lt.getGeneralisation() }
            else { Type.Mixed })
      }
      .map { case (_, (p, t)) => (p, if (specific) t else t.getGeneralisation()) }
  }

  def unique(): TypedPipe[Content] = {
    data
      .map { case c => c.content }
      .distinct(new Ordering[Content] { def compare(l: Content, r: Content) = l.toString.compare(r.toString) })
  }

  def unique[D <: Dimension](slice: Slice[P, D])(implicit ev: slice.S =!= Position0D): TypedPipe[Cell[slice.S]] = {
    data
      .map { case Cell(p, c) => Cell(slice.selected(p), c) }
      .distinct(new Ordering[Cell[slice.S]] {
        def compare(l: Cell[slice.S], r: Cell[slice.S]) = l.toString.compare(r.toString)
      })
  }

  def which(predicate: Predicate)(implicit ev: ClassTag[P]): TypedPipe[P] = {
    data.collect { case c if predicate(c) => c.position }
  }

  def which[T, D <: Dimension](slice: Slice[P, D], positions: T, predicate: Predicate)(implicit ev1: PosDimDep[P, D],
    ev2: Nameable[T, P, slice.S, D, TypedPipe], ev3: ClassTag[slice.S], ev4: ClassTag[P]): TypedPipe[P] = {
    which(slice, List((positions, predicate)))
  }

  def which[T, D <: Dimension](slice: Slice[P, D], pospred: List[(T, Predicate)])(implicit ev1: PosDimDep[P, D],
    ev2: Nameable[T, P, slice.S, D, TypedPipe], ev3: ClassTag[slice.S], ev4: ClassTag[P]): TypedPipe[P] = {
    val nampred = pospred.map { case (pos, pred) => ev2.convert(this, slice, pos).map { case (p, i) => (p, pred) } }
    val pipe = nampred.tail.foldLeft(nampred.head)((b, a) => b ++ a)

    data
      .groupBy { case c => slice.selected(c.position) }
      .join(pipe.groupBy { case (p, pred) => p })
      .collect { case (_, (c, (_, predicate))) if predicate(c) => c.position }
  }

  val data: TypedPipe[Cell[P]]

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

  private def pairwise[D <: Dimension](slice: Slice[P, D])(implicit ev1: PosDimDep[P, D],
    ev2: ClassTag[slice.S]): TypedPipe[(Cell[slice.S], Cell[slice.S], slice.R)] = {
    val wanted = names(slice).map { case (p, i) => p }
    val values = data.groupBy { case Cell(p, _) => (slice.selected(p), slice.remainder(p)) }
    val other = data.map { case c => slice.remainder(c.position) }.distinct

    wanted
      .cross(wanted)
      .cross(other)
      .map { case ((l, r), o) => (l, r, o) }
      .groupBy { case (l, _, o) => (l, o) }
      .join(values)
      .groupBy { case (_, ((_, r, o), _)) => (r, o) }
      .join(values)
      .map { case (_, ((_, ((lp, rp, r), lc)), rc)) => (Cell(lp, lc.content), Cell(rp, rc.content), r) }
  }

  private def pairwiseBetween[D <: Dimension](slice: Slice[P, D], that: ScaldingMatrix[P])(
    implicit ev1: PosDimDep[P, D], ev2: ClassTag[slice.S]): TypedPipe[(Cell[slice.S], Cell[slice.S], slice.R)] = {
    val thisWanted = names(slice).map { case (p, i) => p }
    val thisValues = data.groupBy { case Cell(p, _) => (slice.selected(p), slice.remainder(p)) }

    val thatWanted = that.names(slice).map { case (p, i) => p }
    val thatValues = that.data.groupBy { case Cell(p, _) => (slice.selected(p), slice.remainder(p)) }

    val other = data.map { case c => slice.remainder(c.position) }.distinct

    thisWanted
      .cross(thatWanted)
      .cross(other)
      .map { case ((l, r), o) => (l, r, o) }
      .groupBy { case (l, _, o) => (l, o) }
      .join(thisValues)
      .groupBy { case (_, ((_, r, o), _)) => (r, o) }
      .join(thatValues)
      .map { case (_, ((_, ((lp, rp, r), lc)), rc)) => (Cell(lp, lc.content), Cell(rp, rc.content), r) }
  }
}

/** Base trait for methods that reduce the number of dimensions or that can be filled using a `TypedPipe[Cell[P]]`. */
trait ScaldingReduceableMatrix[P <: Position with ReduceablePosition] extends ReduceableMatrix[P] {
  self: ScaldingMatrix[P] =>

  def fillHetrogenous[D <: Dimension](slice: Slice[P, D])(values: TypedPipe[Cell[slice.S]])(
    implicit ev: PosDimDep[P, D]): TypedPipe[Cell[P]] = {
    val dense = domain
      .groupBy[Slice[P, D]#S] { case p => slice.selected(p) }
      .join(values.groupBy { case c => c.position })
      .map { case (_, (p, c)) => Cell(p, c.content) }

    dense
      .groupBy { case c => c.position }
      .leftJoin(data.groupBy { case c => c.position })
      .map { case (p, (fc, co)) => co.getOrElse(fc) }
  }

  def fillHomogenous(value: Content): TypedPipe[Cell[P]] = {
    domain
      .groupBy { case p => p }
      .leftJoin(data.groupBy { case c => c.position })
      .map { case (p, (_, co)) => co.getOrElse(Cell(p, value)) }
  }

  def melt[D <: Dimension, E <: Dimension](dim: D, into: E, separator: String = ".")(implicit ev1: PosDimDep[P, D],
    ev2: PosDimDep[P, E], ne: D =!= E): TypedPipe[Cell[P#L]] = {
    data.map { case Cell(p, c) => Cell(p.melt(dim, into, separator), c) }
  }

  def reduce[D <: Dimension](slice: Slice[P, D], reducer: Reducer with Prepare with PresentSingle)(
    implicit ev: PosDimDep[P, D]): TypedPipe[Cell[slice.S]] = {
    data
      .map { case c => (slice.selected(c.position), reducer.prepare(slice, c)) }
      .groupBy { case (p, t) => p }
      .reduce[(slice.S, reducer.T)] { case ((lp, lt), (rp, rt)) => (lp, reducer.reduce(lt, rt)) }
      .flatMap { case (_, (p, t)) => reducer.presentSingle(p, t) }
  }

  def reduceWithValue[D <: Dimension, W](slice: Slice[P, D],
    reducer: Reducer with PrepareWithValue with PresentSingle { type V >: W }, value: ValuePipe[W])(
      implicit ev: PosDimDep[P, D]): TypedPipe[Cell[slice.S]] = {
    data
      .leftCross(value)
      .map { case (c, vo) => (slice.selected(c.position), reducer.prepare(slice, c, vo.get)) }
      .groupBy { case (p, t) => p }
      .reduce[(slice.S, reducer.T)] { case ((lp, lt), (rp, rt)) => (lp, reducer.reduce(lt, rt)) }
      .flatMap { case (_, (p, t)) => reducer.presentSingle(p, t) }
  }

  def squash[D <: Dimension](dim: D, squasher: Squasher with Reduce)(
    implicit ev: PosDimDep[P, D]): TypedPipe[Cell[P#L]] = {
    data
      .groupBy { case c => c.position.remove(dim) }
      .reduce[Cell[P]] { case (x, y) => squasher.reduce(dim, x, y) }
      .map { case (p, c) => Cell(p, c.content) }
  }

  def squashWithValue[D <: Dimension, W](dim: D, squasher: Squasher with ReduceWithValue { type V >: W },
    value: ValuePipe[W])(implicit ev: PosDimDep[P, D]): TypedPipe[Cell[P#L]] = {
    data
      .leftCross(value)
      .groupBy { case (c, vo) => c.position.remove(dim) }
      .reduce[(Cell[P], Option[W])] { case ((x, xvo), (y, yvo)) => (squasher.reduce(dim, x, y, xvo.get), xvo) }
      .map { case (p, (c, _)) => Cell(p, c.content) }
  }
}

/** Base trait for methods that expand the number of dimension of a matrix using a `TypedPipe[Cell[P]]`. */
trait ScaldingExpandableMatrix[P <: Position with ExpandablePosition] extends ExpandableMatrix[P] {
  self: ScaldingMatrix[P] =>

  def expand(expander: Cell[P] => P#M): TypedPipe[Cell[P#M]] = data.map { case c => Cell(expander(c), c.content) }

  def expandWithValue[V](expander: (Cell[P], V) => P#M, value: ValuePipe[V]): TypedPipe[Cell[P#M]] = {
    data.mapWithValue(value) { case (c, vo) => Cell(expander(c, vo.get), c.content) }
  }

  def transformAndExpand[T](transformers: T)(implicit ev: TransformableExpanded[T]): TypedPipe[Cell[P#M]] = {
    val t = ev.convert(transformers)

    data.flatMap { case c => t.present(c).toList }
  }

  def transformAndExpandWithValue[T, V](transformers: T, value: ValuePipe[V])(
    implicit ev: TransformableExpandedWithValue[T, V]): TypedPipe[Cell[P#M]] = {
    val t = ev.convert(transformers)

    data.flatMapWithValue(value) { case (c, vo) => t.present(c, vo.get).toList }
  }
}

object ScaldingMatrix {
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
              case (Some(con), Some(c1)) => Some(Cell(Position1D(c1), con))
              case _ => None
            }
          }
      }
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
            case (Some(con), Some(c1)) => Some(Cell(Position1D(c1), con))
            case _ => None
          }
      }
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
            case (Some(con), Some(c1)) => Some(Cell(Position1D(c1), con))
            case _ => None
          }
      }
  }

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
              case (Some(con), Some(c1), Some(c2)) => Some(Cell(Position2D(c1, c2), con))
              case _ => None
            }
          }
      }
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
            case (Some(con), Some(c1), Some(c2)) => Some(Cell(Position2D(c1, c2), con))
            case _ => None
          }
      }
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
            case (Some(con), Some(c1), Some(c2)) => Some(Cell(Position2D(c1, c2), con))
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
              case (Some(con), Some(c1), Some(c2), Some(c3)) => Some(Cell(Position3D(c1, c2, c3), con))
              case _ => None
            }
          }
      }
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
            case (Some(con), Some(c1), Some(c2), Some(c3)) => Some(Cell(Position3D(c1, c2, c3), con))
            case _ => None
          }
      }
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
            case (Some(con), Some(c1), Some(c2), Some(c3)) => Some(Cell(Position3D(c1, c2, c3), con))
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
              schema.decode(parts(idx).trim).map { case c => Cell(Position2D(pkey, name), c) }
            case _ => None
          }
      }
  }

  /** Conversion from `TypedPipe[Cell[Position1D]]` to a `ScaldingMatrix1D`. */
  implicit def TPP1DC2M1D(data: TypedPipe[Cell[Position1D]]): ScaldingMatrix1D = new ScaldingMatrix1D(data)
  /** Conversion from `TypedPipe[Cell[Position2D]]` to a `ScaldingMatrix2D`. */
  implicit def TPP2DC2M2D(data: TypedPipe[Cell[Position2D]]): ScaldingMatrix2D = new ScaldingMatrix2D(data)
  /** Conversion from `TypedPipe[Cell[Position3D]]` to a `ScaldingMatrix3D`. */
  implicit def TPP3DC2M3D(data: TypedPipe[Cell[Position3D]]): ScaldingMatrix3D = new ScaldingMatrix3D(data)
  /** Conversion from `TypedPipe[Cell[Position4D]]` to a `ScaldingMatrix4D`. */
  implicit def TPP4DC2M4D(data: TypedPipe[Cell[Position4D]]): ScaldingMatrix4D = new ScaldingMatrix4D(data)
  /** Conversion from `TypedPipe[Cell[Position5D]]` to a `ScaldingMatrix5D`. */
  implicit def TPP5DC2M5D(data: TypedPipe[Cell[Position5D]]): ScaldingMatrix5D = new ScaldingMatrix5D(data)

  /** Conversion from `List[(Valueable, Content)]` to a `ScaldingMatrix1D`. */
  implicit def LVCT2M1D[V: Valueable](list: List[(V, Content)]): ScaldingMatrix1D = {
    new ScaldingMatrix1D(new IterablePipe(list.map { case (v, c) => Cell(Position1D(v), c) }))
  }
  /** Conversion from `List[(Valueable, Valueable, Content)]` to a `ScaldingMatrix2D`. */
  implicit def LVVCT2M2D[V: Valueable, W: Valueable](list: List[(V, W, Content)]): ScaldingMatrix2D = {
    new ScaldingMatrix2D(new IterablePipe(list.map { case (v, w, c) => Cell(Position2D(v, w), c) }))
  }
  /** Conversion from `List[(Valueable, Valueable, Valueable, Content)]` to a `ScaldingMatrix3D`. */
  implicit def LVVVCT2M3D[V: Valueable, W: Valueable, X: Valueable](
    list: List[(V, W, X, Content)]): ScaldingMatrix3D = {
    new ScaldingMatrix3D(new IterablePipe(list.map { case (v, w, x, c) => Cell(Position3D(v, w, x), c) }))
  }
  /** Conversion from `List[(Valueable, Valueable, Valueable, Valueable, Content)]` to a `ScaldingMatrix4D`. */
  implicit def LVVVVCT2M4D[V: Valueable, W: Valueable, X: Valueable, Y: Valueable](
    list: List[(V, W, X, Y, Content)]): ScaldingMatrix4D = {
    new ScaldingMatrix4D(new IterablePipe(list.map { case (v, w, x, y, c) => Cell(Position4D(v, w, x, y), c) }))
  }
  /**
   * Conversion from `List[(Valueable, Valueable, Valueable, Valueable, Valueable, Content)]` to a `ScaldingMatrix5D`.
   */
  implicit def LVVVVVCT2M5D[V: Valueable, W: Valueable, X: Valueable, Y: Valueable, Z: Valueable](
    list: List[(V, W, X, Y, Z, Content)]): ScaldingMatrix5D = {
    new ScaldingMatrix5D(new IterablePipe(list.map { case (v, w, x, y, z, c) => Cell(Position5D(v, w, x, y, z), c) }))
  }
}

/**
 * Rich wrapper around a `TypedPipe[Cell[Position1D]]`.
 *
 * @param data `TypedPipe[Cell[Position1D]]`.
 */
class ScaldingMatrix1D(val data: TypedPipe[Cell[Position1D]]) extends ScaldingMatrix[Position1D]
  with ScaldingExpandableMatrix[Position1D] {
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
      .groupBy { case c => c.position }
      .join(persistDictionary(names, file, dictionary, separator, First))
      .map { case (_, (c, (_, i))) => i + separator + c.content.value.toShortString }
      .write(TypedSink(TextLine(file)))

    data
  }
}

/**
 * Rich wrapper around a `TypedPipe[Cell[Position2D]]`.
 *
 * @param data `TypedPipe[Cell[Position2D]]`.
 */
class ScaldingMatrix2D(val data: TypedPipe[Cell[Position2D]]) extends ScaldingMatrix[Position2D]
  with ScaldingReduceableMatrix[Position2D] with ScaldingExpandableMatrix[Position2D] {
  // TODO: Make this work on more than 2D matrices
  def correlation[D <: Dimension](slice: Slice[Position2D, D])(implicit ev1: PosDimDep[Position2D, D],
    ev2: ClassTag[slice.S], ev3: ClassTag[slice.R]): TypedPipe[Cell[Position1D]] = {
    implicit def TPP2DSC2M1D(data: TypedPipe[Cell[slice.S]]): ScaldingMatrix1D = {
      new ScaldingMatrix1D(data.asInstanceOf[TypedPipe[Cell[Position1D]]])
    }
    implicit def TPP2DRMC2M2D(data: TypedPipe[Cell[slice.R#M]]): ScaldingMatrix2D = {
      new ScaldingMatrix2D(data.asInstanceOf[TypedPipe[Cell[Position2D]]])
    }

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

  def domain(): TypedPipe[Position2D] = {
    names(Over(First))
      .map { case (Position1D(c), i) => c }
      .cross(names(Over(Second)).map { case (Position1D(c), i) => c })
      .map { case (c1, c2) => Position2D(c1, c2) }
  }

  // TODO: Make this work on more than 2D matrices
  def mutualInformation[D <: Dimension](slice: Slice[Position2D, D])(implicit ev1: PosDimDep[Position2D, D],
    ev2: ClassTag[slice.S], ev3: ClassTag[slice.R]): TypedPipe[Cell[Position1D]] = {
    implicit def TPP2DSMC2M2D(data: TypedPipe[Cell[slice.S#M]]): ScaldingMatrix2D = {
      new ScaldingMatrix2D(data.asInstanceOf[TypedPipe[Cell[Position2D]]])
    }
    implicit def TPP2DRMC2M2D(data: TypedPipe[Cell[slice.R#M]]): ScaldingMatrix2D = {
      new ScaldingMatrix2D(data.asInstanceOf[TypedPipe[Cell[Position2D]]])
    }

    val marginal = data
      .reduceAndExpand(slice, Entropy("marginal"))
      .pairwise(Over(First), Plus(name = "%s,%s", comparer = Upper))

    val joint = data
      .pairwise(slice, Concatenate(name = "%s,%s", comparer = Upper))
      .reduceAndExpand(Over(First), Entropy("joint", strict = true, nan = true, all = false, negate = true))

    (marginal ++ joint)
      .reduce(Over(First), Sum())
  }

  /**
   * Permute the order of the coordinates in a position.
   *
   * @param first  Dimension used for the first coordinate.
   * @param second Dimension used for the second coordinate.
   */
  def permute[D <: Dimension, E <: Dimension](first: D, second: E)(implicit ev1: PosDimDep[Position2D, D],
    ev2: PosDimDep[Position2D, E], ne: D =!= E): TypedPipe[Cell[Position2D]] = {
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
   * @return A `TypedPipe[Cell[Position2D]]`; that is it returns `data`.
   */
  def persistCSVFile[D <: Dimension](slice: Slice[Position2D, D], file: String, separator: String = "|",
    escapee: Escape = Quote(), writeHeader: Boolean = true, header: String = "%s.header", writeRowId: Boolean = true,
    rowId: String = "id")(implicit ev1: Nameable[TypedPipe[(slice.S, Long)], Position2D, slice.S, D, TypedPipe],
      ev2: PosDimDep[Position2D, D], ev3: ClassTag[slice.S], flow: FlowDef, mode: Mode): TypedPipe[Cell[Position2D]] = {
    persistCSVFileWithNames(slice, file, names(slice), separator, escapee, writeHeader, header, writeRowId, rowId)
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
   * @return A `TypedPipe[Cell[Position2D]]`; that is it returns `data`.
   *
   * @note If `names` contains a subset of the columns, then only those columns get persisted to file.
   */
  def persistCSVFileWithNames[T, D <: Dimension](slice: Slice[Position2D, D], file: String, names: T,
    separator: String = "|", escapee: Escape = Quote(), writeHeader: Boolean = true, header: String = "%s.header",
    writeRowId: Boolean = true, rowId: String = "id")(implicit ev1: Nameable[T, Position2D, slice.S, D, TypedPipe],
      ev2: PosDimDep[Position2D, D], ev3: ClassTag[slice.S], flow: FlowDef, mode: Mode): TypedPipe[Cell[Position2D]] = {
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
      .groupBy { case c => Position1D(c.position(First)) }
      .join(persistDictionary(namesI, file, dictionary, separator, First))
      .values
      .groupBy { case (c, pi) => Position1D(c.position(Second)) }
      .join(persistDictionary(namesJ, file, dictionary, separator, Second))
      .map { case (_, ((c, (_, i)), (_, j))) => i + separator + j + separator + c.content.value.toShortString }
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
      .groupBy { case c => slice.remainder(c.position).asInstanceOf[Position1D] }
      .join(persistDictionary(names, file, dictionary, separator))
      .map { case (_, (Cell(p, c), (_, i))) => (p, " " + i + ":" + c.value.toShortString, 1L) }
      .groupBy { case (p, ics, m) => slice.selected(p) }
      .reduce[(Position2D, String, Long)] { case ((p, ls, lm), (_, rs, rm)) => (p, ls + rs, lm + rm) }
      .map { case (p, (_, ics, m)) => if (addId) p.toShortString(separator) + separator + m + ics else m + ics }
      .write(TypedSink(TextLine(file)))

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
   * @return A `TypedPipe[Cell[Position2D]]`; that is it returns `data`.
   */
  def persistVWFile[D <: Dimension](slice: Slice[Position2D, D], labels: TypedPipe[Cell[Position1D]], file: String,
    dictionary: String = "%s.dict", separator: String = ":")(implicit ev: PosDimDep[Position2D, D], flow: FlowDef,
      mode: Mode): TypedPipe[Cell[Position2D]] = {
    persistVWFileWithNames(slice, labels, file, names(Along(slice.dimension)), dictionary, separator)
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
   * @return A `TypedPipe[Cell[Position2D]]`; that is it returns `data`.
   *
   * @note If `names` contains a subset of the columns, then only those columns get persisted to file.
   */
  def persistVWFileWithNames[D <: Dimension](slice: Slice[Position2D, D], labels: TypedPipe[Cell[Position1D]],
    file: String, names: TypedPipe[(Position1D, Long)], dictionary: String = "%s.dict", separator: String = ":")(
      implicit ev: PosDimDep[Position2D, D], flow: FlowDef, mode: Mode): TypedPipe[Cell[Position2D]] = {
    data
      .groupBy { case c => slice.remainder(c.position).asInstanceOf[Position1D] }
      .join(persistDictionary(names, file, dictionary, separator))
      .map { case (_, (Cell(p, c), (_, i))) => (p, " " + i + ":" + c.value.toShortString) }
      .groupBy { case (p, ics) => slice.selected(p).asInstanceOf[Position1D] }
      .reduce[(Position2D, String)] { case ((p, ls), (_, rs)) => (p, ls + rs) }
      .join(labels.groupBy { case c => c.position })
      .map { case (p, ((_, ics), c)) => c.content.value.toShortString + " " + p.toShortString(separator) + "|" + ics }
      .write(TypedSink(TextLine(file)))

    data
  }
}

/**
 * Rich wrapper around a `TypedPipe[Cell[Position3D]]`.
 *
 * @param data `TypedPipe[Cell[Position3D]]`.
 */
class ScaldingMatrix3D(val data: TypedPipe[Cell[Position3D]]) extends ScaldingMatrix[Position3D]
  with ScaldingReduceableMatrix[Position3D] with ScaldingExpandableMatrix[Position3D] {
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
    data.map { case Cell(p, c) => Cell(p.permute(List(first, second, third)), c) }
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
      .groupBy { case c => Position1D(c.position(First)) }
      .join(persistDictionary(namesI, file, dictionary, separator, First))
      .values
      .groupBy { case (c, pi) => Position1D(c.position(Second)) }
      .join(persistDictionary(namesJ, file, dictionary, separator, Second))
      .map { case (_, ((pc, pi), pj)) => (pc, pi, pj) }
      .groupBy { case (c, pi, pj) => Position1D(c.position(Third)) }
      .join(persistDictionary(namesK, file, dictionary, separator, Third))
      .map {
        case (_, ((c, (_, i), (_, j)), (_, k))) =>
          i + separator + j + separator + k + separator + c.content.value.toShortString
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
class ScaldingMatrix4D(val data: TypedPipe[Cell[Position4D]]) extends ScaldingMatrix[Position4D]
  with ScaldingReduceableMatrix[Position4D] with ScaldingExpandableMatrix[Position4D] {
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
    data.map { case Cell(p, c) => Cell(p.permute(List(first, second, third, fourth)), c) }
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
      .groupBy { case c => Position1D(c.position(First)) }
      .join(persistDictionary(namesI, file, dictionary, separator, First))
      .values
      .groupBy { case (c, pi) => Position1D(c.position(Second)) }
      .join(persistDictionary(namesJ, file, dictionary, separator, Second))
      .map { case (_, ((pc, pi), pj)) => (pc, pi, pj) }
      .groupBy { case (c, pi, pj) => Position1D(c.position(Third)) }
      .join(persistDictionary(namesK, file, dictionary, separator, Third))
      .map { case (_, ((pc, pi, pj), pk)) => (pc, pi, pj, pk) }
      .groupBy { case (c, pi, pj, pk) => Position1D(c.position(Fourth)) }
      .join(persistDictionary(namesL, file, dictionary, separator, Fourth))
      .map {
        case (_, ((c, (_, i), (_, j), (_, k)), (_, l))) =>
          i + separator + j + separator + k + separator + l + separator + c.content.value.toShortString
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
class ScaldingMatrix5D(val data: TypedPipe[Cell[Position5D]]) extends ScaldingMatrix[Position5D]
  with ScaldingReduceableMatrix[Position5D] {
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
    data.map { case Cell(p, c) => Cell(p.permute(List(first, second, third, fourth, fifth)), c) }
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
      .groupBy { case c => Position1D(c.position(First)) }
      .join(persistDictionary(namesI, file, dictionary, separator, First))
      .values
      .groupBy { case (c, pi) => Position1D(c.position(Second)) }
      .join(persistDictionary(namesJ, file, dictionary, separator, Second))
      .map { case (_, ((pc, pi), pj)) => (pc, pi, pj) }
      .groupBy { case (c, pi, pj) => Position1D(c.position(Third)) }
      .join(persistDictionary(namesK, file, dictionary, separator, Third))
      .map { case (_, ((pc, pi, pj), pk)) => (pc, pi, pj, pk) }
      .groupBy { case (c, pi, pj, pk) => Position1D(c.position(Fourth)) }
      .join(persistDictionary(namesL, file, dictionary, separator, Fourth))
      .map { case (_, ((pc, pi, pj, pk), pl)) => (pc, pi, pj, pk, pl) }
      .groupBy { case (c, pi, pj, pk, pl) => Position1D(c.position(Fifth)) }
      .join(persistDictionary(namesM, file, dictionary, separator, Fifth))
      .map {
        case (_, ((c, (_, i), (_, j), (_, k), (_, l)), (_, m))) =>
          i + separator + j + separator + k + separator + l + separator + m + separator + c.content.value.toShortString
      }
      .write(TypedSink(TextLine(file)))

    data
  }
}

/** Scalding Companion object for the `Matrixable` type class. */
object ScaldingMatrixable {
  /** Converts a `TypedPipe[Cell[P]]` into a `TypedPipe[Cell[P]]`; that is, it is a  pass through. */
  implicit def TPC2M[P <: Position]: Matrixable[TypedPipe[Cell[P]], P, TypedPipe] = {
    new Matrixable[TypedPipe[Cell[P]], P, TypedPipe] { def convert(t: TypedPipe[Cell[P]]): TypedPipe[Cell[P]] = t }
  }
  /** Converts a `List[Cell[P]]` into a `TypedPipe[Cell[P]]`. */
  implicit def LC2M[P <: Position]: Matrixable[List[Cell[P]], P, TypedPipe] = {
    new Matrixable[List[Cell[P]], P, TypedPipe] {
      def convert(t: List[Cell[P]]): TypedPipe[Cell[P]] = new IterablePipe(t)
    }
  }
  /** Converts a `Cell[P]` into a `TypedPipe[Cell[P]]`. */
  implicit def C2M[P <: Position]: Matrixable[Cell[P], P, TypedPipe] = {
    new Matrixable[Cell[P], P, TypedPipe] { def convert(t: Cell[P]): TypedPipe[Cell[P]] = new IterablePipe(List(t)) }
  }
}

