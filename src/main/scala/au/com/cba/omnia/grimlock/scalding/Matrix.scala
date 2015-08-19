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

package au.com.cba.omnia.grimlock.scalding

import au.com.cba.omnia.grimlock.framework.{
  Cell,
  Default,
  ExpandableMatrix => BaseExpandableMatrix,
  ExtractWithDimension,
  ExtractWithKey,
  InMemory,
  Locate,
  Matrix => BaseMatrix,
  Matrixable => BaseMatrixable,
  Nameable => BaseNameable,
  NoParameters,
  Redistribute,
  ReduceableMatrix => BaseReduceableMatrix,
  Reducers,
  Sequence2,
  Tuner,
  TunerParameters,
  Type,
  Unbalanced
}
import au.com.cba.omnia.grimlock.framework.aggregate._
import au.com.cba.omnia.grimlock.framework.content._
import au.com.cba.omnia.grimlock.framework.content.metadata._
import au.com.cba.omnia.grimlock.framework.encoding._
import au.com.cba.omnia.grimlock.framework.pairwise._
import au.com.cba.omnia.grimlock.framework.partition._
import au.com.cba.omnia.grimlock.framework.position._
import au.com.cba.omnia.grimlock.framework.sample._
import au.com.cba.omnia.grimlock.framework.squash._
import au.com.cba.omnia.grimlock.framework.transform._
import au.com.cba.omnia.grimlock.framework.utility._
import au.com.cba.omnia.grimlock.framework.utility.OneOf._
import au.com.cba.omnia.grimlock.framework.window._

import au.com.cba.omnia.grimlock.scalding.Matrix._
import au.com.cba.omnia.grimlock.scalding.Matrixable._
import au.com.cba.omnia.grimlock.scalding.Nameable._

import cascading.flow.FlowDef
import com.twitter.scalding.{ Mode, TextLine }
import com.twitter.scalding.typed.{ IterablePipe, Grouped, TypedPipe, TypedSink, ValuePipe }

import java.io.{ File, PrintWriter }
import java.lang.{ ProcessBuilder, Thread }
import java.nio.file.Paths

import scala.collection.immutable.HashSet
import scala.io.Source
import scala.reflect.ClassTag

private[scalding] object ScaldingImplicits {

  implicit def hashSetSemigroup[A] = new com.twitter.algebird.Semigroup[HashSet[A]] {
    def plus(l: HashSet[A], r: HashSet[A]): HashSet[A] = l ++ r
  }

  implicit def mapSemigroup[K, V] = new com.twitter.algebird.Semigroup[Map[K, V]] {
    def plus(l: Map[K, V], r: Map[K, V]): Map[K, V] = l ++ r
  }

  implicit def mapListSemigroup[K, V] = new com.twitter.algebird.Semigroup[Map[K, List[V]]] {
    def plus(l: Map[K, List[V]], r: Map[K, List[V]]): Map[K, List[V]] = {
      (l.toSeq ++ r.toSeq).groupBy(_._1).mapValues(_.map(_._2).toList.flatten)
    }
  }

  implicit def cellOrdering[P <: Position] = new Ordering[Cell[P]] {
    def compare(l: Cell[P], r: Cell[P]) = l.toString().compare(r.toString)
  }

  implicit def contentOrdering = new Ordering[Content] {
    def compare(l: Content, r: Content) = l.toString.compare(r.toString)
  }

  implicit def serialisePosition[T <: Position](key: T): Array[Byte] = {
    key.toShortString("|").toCharArray.map(_.toByte)
  }

  implicit class GroupedTuner[K <: Position, V](grouped: Grouped[K, V]) {
    def redistribute(parameters: TunerParameters): TypedPipe[(K, V)] = {
      parameters match {
        case Redistribute(reducers) => grouped.withReducers(reducers).forceToReducers
        case _ => grouped
      }
    }

    def tuneReducers(parameters: TunerParameters): Grouped[K, V] = {
      parameters match {
        case Reducers(reducers) => grouped.withReducers(reducers)
        case _ => grouped
      }
    }

    def tunedJoin[W](tuner: Tuner, parameters: TunerParameters, smaller: TypedPipe[(K, W)])(
      implicit ev: Ordering[K]): TypedPipe[(K, (V, W))] = {
      (tuner, parameters) match {
        case (Default(_), Reducers(reducers)) => grouped.withReducers(reducers).join(Grouped(smaller))
        case (Unbalanced(_), Reducers(reducers)) => grouped.sketch(reducers).join(smaller)
        case _ => grouped.join(Grouped(smaller))
      }
    }

    def tunedLeftJoin[W](tuner: Tuner, parameters: TunerParameters, smaller: TypedPipe[(K, W)])(
      implicit ev: Ordering[K]): TypedPipe[(K, (V, Option[W]))] = {
      (tuner, parameters) match {
        case (Default(_), Reducers(reducers)) => grouped.withReducers(reducers).leftJoin(Grouped(smaller))
        case (Unbalanced(_), Reducers(reducers)) => grouped.sketch(reducers).leftJoin(smaller)
        case _ => grouped.leftJoin(Grouped(smaller))
      }
    }
  }

  implicit class PipeTuner[P](pipe: TypedPipe[P]) {
    def mapSideJoin[V, Q](value: ValuePipe[V], f: (P, V) => TraversableOnce[Q]): TypedPipe[Q] = {
      pipe.flatMapWithValue(value) { case (c, vo) => f(c, vo.get) }
    }

    def toHashSetValue[Q](f: (P) => Q): ValuePipe[HashSet[Q]] = pipe.map { case p => HashSet(f(p)) }.sum
    def toListValue[Q](f: (P) => Q): ValuePipe[List[Q]] = pipe.map { case p => List(f(p)) }.sum
  }
}

/** Base trait for matrix operations using a `TypedPipe[Cell[P]]`. */
trait Matrix[P <: Position] extends BaseMatrix[P] with Persist[Cell[P]] {
  type U[A] = TypedPipe[A]
  type E[B] = ValuePipe[B]
  type S = Matrix[P]

  import ScaldingImplicits._

  protected type TP4 = OneOf4[InMemory[NoParameters.type],
                              Default[NoParameters.type], Default[Reducers],
                              Unbalanced[Reducers]]

  type ChangeTuners = TP4
  def change[D <: Dimension, I, T <: Tuner](slice: Slice[P, D], positions: I, schema: Schema, tuner: T = InMemory())(
    implicit ev1: PosDimDep[P, D], ev2: PositionDistributable[I, slice.S, TypedPipe], ev3: ClassTag[slice.S],
      ev4: ChangeTuners#V[T]): U[Cell[P]] = {
    val pos = ev2.convert(positions)
    val update = (change: Boolean, cell: Cell[P]) => change match {
      case true => schema.decode(cell.content.value.toShortString).map { case con => Cell(cell.position, con) }
      case false => Some(cell)
    }

    tuner match {
      case InMemory(_) =>
        data
          .mapSideJoin(pos.toHashSetValue((p: slice.S) => p), (c: Cell[P], v: HashSet[slice.S]) =>
            update(v.contains(slice.selected(c.position)), c))
      case _ =>
        data
          .groupBy { case c => slice.selected(c.position) }
          .tunedLeftJoin(tuner, tuner.parameters, pos.map { case p => (p, ()) })
          .flatMap { case (_, (c, o)) => update(!o.isEmpty, c) }
    }
  }

  type DomainTuners = TP1

  type GetTuners = TP4
  def get[I, T <: Tuner](positions: I, tuner: T = InMemory())(implicit ev1: PositionDistributable[I, P, TypedPipe],
    ev2: ClassTag[P], ev3: GetTuners#V[T]): U[Cell[P]] = {
    val pos = ev1.convert(positions)

    tuner match {
      case InMemory(_) =>
        data
          .mapSideJoin(pos.toHashSetValue((p: P) => p), (c: Cell[P], v: HashSet[P]) =>
            if (v.contains(c.position)) { Some(c) } else { None })
      case _ =>
        data
          .groupBy { case c => c.position }
          .tunedJoin(tuner, tuner.parameters, pos.map { case p => (p, ()) })
          .map { case (_, (c, _)) => c }
    }
  }

  type JoinTuners = OneOf7[InMemory[NoParameters.type], InMemory[Reducers],
                           Default[NoParameters.type], Default[Reducers], Default[Sequence2[Reducers, Reducers]],
                           Unbalanced[Reducers], Unbalanced[Sequence2[Reducers, Reducers]]]
  def join[D <: Dimension, T <: Tuner](slice: Slice[P, D], that: S, tuner: T = Default())(implicit ev1: PosDimDep[P, D],
    ev2: P =!= Position1D, ev3: ClassTag[slice.S], ev4: JoinTuners#V[T]): U[Cell[P]] = {
    val (p1, p2) = (tuner, tuner.parameters) match {
      case (_, Sequence2(f, s)) => (f, s)
      case (InMemory(_), p) => (p, NoParameters)
      case (_, p) => (NoParameters, p)
    }
    val keep = Grouped(names(slice).map { case p => (p, ()) })
      .tuneReducers(p1)
      .join(Grouped(that.names(slice).map { case p => (p, ()) }))
      .map { case (p, _) => (p, ()) } // TODO: Does this need a forceToDisk?

    tuner match {
      case InMemory(_) =>
        (data ++ that.data)
          .mapSideJoin(keep.toHashSetValue[slice.S](_._1), (c: Cell[P], v: HashSet[slice.S]) =>
            if (v.contains(slice.selected(c.position))) { Some(c) } else { None })
      case _ =>
        (data ++ that.data)
          .groupBy { case c => slice.selected(c.position) }
          .tunedJoin(tuner, p2, keep)
          .map { case (_, (c, _)) => c }
    }
  }

  type NamesTuners = TP1
  def names[D <: Dimension, T <: Tuner](slice: Slice[P, D], tuner: T = Default())(implicit ev1: PosDimDep[P, D],
    ev2: slice.S =!= Position0D, ev3: ClassTag[slice.S], ev4: NamesTuners#V[T]): U[slice.S] = {
    data.map { case c => slice.selected(c.position) }.distinct
  }

  type PairwiseTuners = OneOf14[InMemory[NoParameters.type],
                                Default[NoParameters.type],
                                Default[Reducers],
                                Default[Sequence2[Redistribute, Redistribute]],
                                Default[Sequence2[Redistribute, Reducers]],
                                Default[Sequence2[Redistribute, Sequence2[Redistribute, Reducers]]],
                                Default[Sequence2[Reducers, Redistribute]],
                                Default[Sequence2[Reducers, Reducers]],
                                Default[Sequence2[Reducers, Sequence2[Redistribute, Reducers]]],
                                Default[Sequence2[Sequence2[Redistribute, Reducers], Redistribute]],
                                Default[Sequence2[Sequence2[Redistribute, Reducers], Reducers]],
                                Default[Sequence2[Sequence2[Redistribute, Reducers],
                                                  Sequence2[Redistribute, Reducers]]],
                                Unbalanced[Sequence2[Reducers, Reducers]],
                                Unbalanced[Sequence2[Sequence2[Redistribute, Reducers],
                                                     Sequence2[Redistribute, Reducers]]]]
  def pairwise[D <: Dimension, Q <: Position, F, T <: Tuner](slice: Slice[P, D], comparer: Comparer, operators: F,
    tuner: T = Default())(implicit ev1: PosDimDep[P, D], ev2: Operable[F, slice.S, slice.R, Q],
      ev3: slice.S =!= Position0D, ev4: ClassTag[slice.S], ev5: ClassTag[slice.R],
        ev6: PairwiseTuners#V[T]): U[Cell[Q]] = {
    val operator = ev2.convert(operators)

    pairwiseTuples(slice, comparer, tuner)(data, data)
      .flatMap { case ((lc, lr), (rc, rr)) => operator.compute(lc, lr, rc, rr) }
  }

  def pairwiseWithValue[D <: Dimension, Q <: Position, F, W, T <: Tuner](slice: Slice[P, D], comparer: Comparer,
    operators: F, value: E[W], tuner: T = Default())(implicit ev1: PosDimDep[P, D],
      ev2: OperableWithValue[F, slice.S, slice.R, Q, W], ev3: slice.S =!= Position0D, ev4: ClassTag[slice.S],
        ev5: ClassTag[slice.R], ev6: PairwiseTuners#V[T]): U[Cell[Q]] = {
    val operator = ev2.convert(operators)

    pairwiseTuples(slice, comparer, tuner)(data, data)
      .flatMapWithValue(value) { case (((lc, lr), (rc, rr)), vo) => operator.computeWithValue(lc, lr, rc, rr, vo.get) }
  }

  def pairwiseBetween[D <: Dimension, Q <: Position, F, T <: Tuner](slice: Slice[P, D], comparer: Comparer, that: S,
    operators: F, tuner: T = Default())(implicit ev1: PosDimDep[P, D], ev2: Operable[F, slice.S, slice.R, Q],
      ev3: slice.S =!= Position0D, ev4: ClassTag[slice.S], ev5: ClassTag[slice.R],
        ev6: PairwiseTuners#V[T]): U[Cell[Q]] = {
    val operator = ev2.convert(operators)

    pairwiseTuples(slice, comparer, tuner)(data, that.data)
      .flatMap { case ((lc, lr), (rc, rr)) => operator.compute(lc, lr, rc, rr) }
  }

  def pairwiseBetweenWithValue[D <: Dimension, Q <: Position, F, W, T <: Tuner](slice: Slice[P, D], comparer: Comparer,
    that: S, operators: F, value: E[W], tuner: T = Default())(implicit ev1: PosDimDep[P, D],
      ev2: OperableWithValue[F, slice.S, slice.R, Q, W], ev3: slice.S =!= Position0D, ev4: ClassTag[slice.S],
        ev5: ClassTag[slice.R], ev6: PairwiseTuners#V[T]): U[Cell[Q]] = {
    val operator = ev2.convert(operators)

    pairwiseTuples(slice, comparer, tuner)(data, that.data)
      .flatMapWithValue(value) { case (((lc, lr), (rc, rr)), vo) => operator.computeWithValue(lc, lr, rc, rr, vo.get) }
  }

  def rename(renamer: (Cell[P]) => P): U[Cell[P]] = data.map { case c => Cell(renamer(c), c.content) }

  def renameWithValue[W](renamer: (Cell[P], W) => P, value: E[W]): U[Cell[P]] = {
    data.mapWithValue(value) { case (c, vo) => Cell(renamer(c, vo.get), c.content) }
  }

  def sample[F](samplers: F)(implicit ev: Sampleable[F, P]): U[Cell[P]] = {
    val sampler = ev.convert(samplers)

    data.filter { case c => sampler.select(c) }
  }

  def sampleWithValue[F, W](samplers: F, value: E[W])(implicit ev: SampleableWithValue[F, P, W]): U[Cell[P]] = {
    val sampler = ev.convert(samplers)

    data.filterWithValue(value) { case (c, vo) => sampler.selectWithValue(c, vo.get) }
  }

  type SetTuners = TP2
  def set[I, T <: Tuner](positions: I, value: Content, tuner: T = Default())(
    implicit ev1: PositionDistributable[I, P, TypedPipe], ev2: ClassTag[P], ev3: SetTuners#V[T]): U[Cell[P]] = {
    set(ev1.convert(positions).map { case p => Cell(p, value) }, tuner)
  }

  def set[M, T <: Tuner](values: M, tuner: T = Default())(implicit ev1: BaseMatrixable[M, P, TypedPipe],
    ev2: ClassTag[P], ev3: SetTuners#V[T]): U[Cell[P]] = {
    data
      .groupBy { case c => c.position }
      .tuneReducers(tuner.parameters)
      .outerJoin(ev1.convert(values).groupBy { case c => c.position })
      .map { case (_, (co, cn)) => cn.getOrElse(co.get) }
  }

  type ShapeTuners = TP1
  def shape[T <: Tuner](tuner: T = Default())(implicit ev: ShapeTuners#V[T]): U[Cell[Position1D]] = {
    Grouped(data.flatMap { case c => c.position.coordinates.map(_.toString).zipWithIndex.map(_.swap) }.distinct)
      .size
      .map { case (i, s) => Cell(Position1D(Dimension.All(i).toString), Content(DiscreteSchema(LongCodex), s)) }
  }

  type SizeTuners = TP1
  def size[D <: Dimension, T <: Tuner](dim: D, distinct: Boolean, tuner: T = Default())(implicit ev1: PosDimDep[P, D],
    ev2: SizeTuners#V[T]): U[Cell[Position1D]] = {
    val coords = data.map { case c => c.position(dim) }
    val dist = if (distinct) { coords } else { coords.distinct(Value.Ordering) }

    dist
      .map { case _ => 1L }
      .sum
      .map { case sum => Cell(Position1D(dim.toString), Content(DiscreteSchema(LongCodex), sum)) }
  }

  type SliceTuners = TP4
  def slice[D <: Dimension, I, T <: Tuner](slice: Slice[P, D], positions: I, keep: Boolean, tuner: T = InMemory())(
    implicit ev1: PosDimDep[P, D], ev2: PositionDistributable[I, slice.S, TypedPipe], ev3: ClassTag[slice.S],
      ev4: SliceTuners#V[T]): U[Cell[P]] = {
    val pos = ev2.convert(positions)

    tuner match {
      case InMemory(_) =>
        data
          .mapSideJoin(pos.toHashSetValue((p: slice.S) => p), (c: Cell[P], v: HashSet[slice.S]) =>
            if (v.contains(slice.selected(c.position)) == keep) { Some(c) } else { None })
      case _ =>
        data
          .groupBy { case c => slice.selected(c.position) }
          .tunedLeftJoin(tuner, tuner.parameters, pos.map { case p => (p, ()) })
          .collect { case (_, (c, o)) if (o.isEmpty != keep) => c }
    }
  }

  type SlideTuners = TP2
  def slide[D <: Dimension, Q <: Position, F, T <: Tuner](slice: Slice[P, D], windows: F, ascending: Boolean,
    tuner: T = Default())(implicit ev1: PosDimDep[P, D], ev2: Windowable[F, slice.S, slice.R, Q],
      ev3: slice.R =!= Position0D, ev4: ClassTag[slice.S], ev5: ClassTag[slice.R],
        ev6: SlideTuners#V[T]): U[Cell[Q]] = {
    val window = ev2.convert(windows)

    data
      .map { case Cell(p, c) => (Cell(slice.selected(p), c), slice.remainder(p)) }
      .groupBy { case (c, r) => c.position }
      .tuneReducers(tuner.parameters)
      .sortBy { case (c, r) => r }(Position.Ordering(ascending))
      .scanLeft(Option.empty[(window.T, TraversableOnce[Cell[Q]])]) {
        case (None, (c, r)) => Some(window.initialise(c, r))
        case (Some((t, _)), (c, r)) => Some(window.present(c, r, t))
      }
      .flatMap {
        case (p, Some((t, c))) => c
        case _ => None
      }
  }

  def slideWithValue[D <: Dimension, Q <: Position, F, W, T <: Tuner](slice: Slice[P, D], windows: F, value: E[W],
    ascending: Boolean, tuner: T = Default())(implicit ev1: PosDimDep[P, D],
      ev2: WindowableWithValue[F, slice.S, slice.R, Q, W], ev3: slice.R =!= Position0D, ev4: ClassTag[slice.S],
        ev5: ClassTag[slice.R], ev6: SlideTuners#V[T]): U[Cell[Q]] = {
    val window = ev2.convert(windows)

    data
      .mapWithValue(value) { case (Cell(p, c), vo) => (Cell(slice.selected(p), c), slice.remainder(p), vo.get) }
      .groupBy { case (c, r, v) => c.position }
      .tuneReducers(tuner.parameters)
      .sortBy { case (c, r, v) => r }(Position.Ordering(ascending))
      .scanLeft(Option.empty[(window.T, TraversableOnce[Cell[Q]])]) {
        case (None, (c, r, v)) => Some(window.initialiseWithValue(c, r, v))
        case (Some((t, _)), (c, r, v)) => Some(window.presentWithValue(c, r, v, t))
      }
      .flatMap {
        case (p, Some((t, c))) => c
        case _ => None
      }
  }

  def split[Q, F](partitioners: F)(implicit ev: Partitionable[F, P, Q]): U[(Q, Cell[P])] = {
    val partitioner = ev.convert(partitioners)

    data.flatMap { case c => partitioner.assign(c).map { case q => (q, c) } }
  }

  def splitWithValue[Q, F, W](partitioners: F, value: E[W])(
    implicit ev: PartitionableWithValue[F, P, Q, W]): U[(Q, Cell[P])] = {
    val partitioner = ev.convert(partitioners)

    data.flatMapWithValue(value) { case (c, vo) => partitioner.assignWithValue(c, vo.get).map { case q => (q, c) } }
  }

  def stream[Q <: Position](command: String, script: String, separator: String,
    parser: String => TraversableOnce[Cell[Q]]): U[Cell[Q]] = {
    val lines = Source.fromFile(script).getLines.toList
    val smfn = (k: Unit, itr: Iterator[String]) => {
      val tmp = File.createTempFile("grimlock-", "-" + Paths.get(script).getFileName().toString())
      val name = tmp.getAbsolutePath
      tmp.deleteOnExit()

      val writer = new PrintWriter(name, "UTF-8")
      for (line <- lines) {
        writer.println(line)
      }
      writer.close()

      val process = new ProcessBuilder(command, name).start()

      new Thread() {
        override def run() {
          val out = new PrintWriter(process.getOutputStream)
          for (cell <- itr) {
            out.println(cell)
          }
          out.close()
        }
      }.start()

      val result = Source.fromInputStream(process.getInputStream).getLines()

      new Iterator[String] {
        def next(): String = result.next()
        def hasNext: Boolean = {
          if (result.hasNext) {
            true
          } else {
            val status = process.waitFor()
            if (status != 0) {
              throw new Exception(s"Subprocess '${command} ${script}' exited with status ${status}")
            }
            false
          }
        }
      }
    }

    data
      .map(_.toString(separator, false))
      .groupAll
      .mapGroup(smfn)
      .values
      .flatMap(parser(_))
  }

  type SummariseTuners = TP2
  def summarise[D <: Dimension, Q <: Position, F, T <: Tuner](slice: Slice[P, D], aggregators: F,
    tuner: T = Default())(implicit ev1: PosDimDep[P, D], ev2: Aggregatable[F, P, slice.S, Q], ev3: ClassTag[slice.S],
      ev4: SummariseTuners#V[T]): U[Cell[Q]] = {
    val aggregator = ev2.convert(aggregators)

    Grouped(data.map { case c => (slice.selected(c.position), aggregator.map { case a => a.prepare(c) }) })
      .tuneReducers(tuner.parameters)
      .reduce[List[Any]] {
        case (lt, rt) => (aggregator, lt, rt).zipped.map {
          case (a, l, r) => a.reduce(l.asInstanceOf[a.T], r.asInstanceOf[a.T])
        }
      }
      .flatMap {
        case (p, t) => (aggregator, t).zipped.flatMap { case (a, s) => a.present(p, s.asInstanceOf[a.T]) }
      }
  }

  def summariseWithValue[D <: Dimension, Q <: Position, F, W, T <: Tuner](slice: Slice[P, D], aggregators: F,
    value: E[W], tuner: T = Default())(implicit ev1: PosDimDep[P, D], ev2: AggregatableWithValue[F, P, slice.S, Q, W],
      ev3: ClassTag[slice.S], ev4: SummariseTuners#V[T]): U[Cell[Q]] = {
    val aggregator = ev2.convert(aggregators)

    Grouped(data.mapWithValue(value) {
      case (c, vo) => (slice.selected(c.position), aggregator.map { case a => a.prepareWithValue(c, vo.get) })
    })
      .tuneReducers(tuner.parameters)
      .reduce[List[Any]] {
        case (lt, rt) => (aggregator, lt, rt).zipped.map {
          case (a, l, r) => a.reduce(l.asInstanceOf[a.T], r.asInstanceOf[a.T])
        }
      }
      .flatMapWithValue(value) {
        case ((p, t), vo) => (aggregator, t).zipped.flatMap {
          case (a, s) => a.presentWithValue(p, s.asInstanceOf[a.T], vo.get)
        }
      }
  }

  type ToMapTuners = TP2
  def toMap()(implicit ev: ClassTag[P]): E[Map[P, Content]] = {
    data
      .map { case c => Map(c.position -> c.content) }
      .sum
  }

  def toMap[D <: Dimension, T <: Tuner](slice: Slice[P, D], tuner: T = Default())(implicit ev1: PosDimDep[P, D],
    ev2: slice.S =!= Position0D, ev3: ClassTag[slice.S], ev4: ToMapTuners#V[T]): E[Map[slice.S, slice.C]] = {
    data
      .map { case c => (c.position, slice.toMap(c)) }
      .groupBy { case (p, m) => slice.selected(p) }
      .tuneReducers(tuner.parameters)
      .reduce[(P, Map[slice.S, slice.C])] { case ((lp, lm), (rp, rm)) => (lp, slice.combineMaps(lp, lm, rm)) }
      .map { case (_, (_, m)) => m }
      .sum
  }

  def transform[Q <: Position, F](transformers: F)(implicit ev: Transformable[F, P, Q]): U[Cell[Q]] = {
    val transformer = ev.convert(transformers)

    data.flatMap { case c => transformer.present(c) }
  }

  def transformWithValue[Q <: Position, F, W](transformers: F, value: E[W])(
    implicit ev: TransformableWithValue[F, P, Q, W]): U[Cell[Q]] = {
    val transformer = ev.convert(transformers)

    data.flatMapWithValue(value) { case (c, vo) => transformer.presentWithValue(c, vo.get) }
  }

  type TypesTuners = TP2
  def types[D <: Dimension, T <: Tuner](slice: Slice[P, D], specific: Boolean, tuner: T = Default())(
    implicit ev1: PosDimDep[P, D], ev2: slice.S =!= Position0D, ev3: ClassTag[slice.S],
      ev4: TypesTuners#V[T]): U[(slice.S, Type)] = {
    Grouped(data.map { case Cell(p, c) => (slice.selected(p), c.schema.kind) })
      .tuneReducers(tuner.parameters)
      .reduce[Type] { case (lt, rt) => Type.getCommonType(lt, rt) }
      .map { case (p, t) => (p, if (specific) t else t.getGeneralisation()) }
  }

  type UniqueTuners = TP1
  /** @note Comparison is performed based on the string representation of the `Content`. */
  def unique[T <: Tuner](tuner: T = Default())(implicit ev: UniqueTuners#V[T]): U[Content] = {
    data
      .map { case c => c.content }
      .distinct
  }

  /** @note Comparison is performed based on the string representation of the `Content`. */
  def unique[D <: Dimension, T <: Tuner](slice: Slice[P, D], tuner: T = Default())(
    implicit ev1: slice.S =!= Position0D, ev2: UniqueTuners#V[T]): U[Cell[slice.S]] = {
    data
      .map { case Cell(p, c) => Cell(slice.selected(p), c) }
      .distinct
  }

  type WhichTuners = TP4
  def which(predicate: Predicate)(implicit ev: ClassTag[P]): U[P] = {
    data.collect { case c if predicate(c) => c.position }
  }

  def which[D <: Dimension, I, T <: Tuner](slice: Slice[P, D], positions: I, predicate: Predicate,
    tuner: T = InMemory())(implicit ev1: PosDimDep[P, D], ev2: PositionDistributable[I, slice.S, TypedPipe],
      ev3: ClassTag[slice.S], ev4: ClassTag[P], ev5: WhichTuners#V[T]): U[P] = {
    which(slice, List((positions, predicate)), tuner)
  }

  def which[D <: Dimension, I, T <: Tuner](slice: Slice[P, D], pospred: List[(I, Predicate)], tuner: T = InMemory())(
    implicit ev1: PosDimDep[P, D], ev2: PositionDistributable[I, slice.S, TypedPipe], ev3: ClassTag[slice.S],
      ev4: ClassTag[P], ev5: WhichTuners#V[T]): U[P] = {
    val pp = pospred
      .map { case (pos, pred) => ev2.convert(pos).map { case p => (p, pred) } }
      .reduce((l, r) => l ++ r)

    tuner match {
      case InMemory(_) =>
        data
          .mapSideJoin(pp.map { case (pos, pred) => Map(pos -> List(pred)) }.sum,
            (c: Cell[P], v: Map[slice.S, List[Predicate]]) => v.get(slice.selected(c.position)).flatMap {
              case lst => if (lst.exists((pred) => pred(c))) { Some(c.position) } else { None }
            })
      case _ =>
        data
          .groupBy { case c => slice.selected(c.position) }
          .tunedJoin(tuner, tuner.parameters, pp)
          .collect { case (_, (c, predicate)) if predicate(c) => c.position }
    }
  }

  val data: U[Cell[P]]

  protected def saveDictionary(names: U[(Position1D, Long)], file: String, dictionary: String, separator: String)(
    implicit flow: FlowDef, mode: Mode) = {
    names
      .map { case (p, i) => p.toShortString(separator) + separator + i }
      .write(TypedSink(TextLine(dictionary.format(file))))

    Grouped(names)
  }

  protected def saveDictionary(names: U[(Position1D, Long)], file: String, dictionary: String, separator: String,
    dim: Dimension)(implicit flow: FlowDef, mode: Mode) = {
    names
      .map { case (p, i) => p.toShortString(separator) + separator + i }
      .write(TypedSink(TextLine(dictionary.format(file, dim.index))))

    Grouped(names)
  }

  private def pairwiseTuples[D <: Dimension, T <: Tuner](slice: Slice[P, D], comparer: Comparer,
    tuner: T)(ldata: U[Cell[P]], rdata: U[Cell[P]])(implicit ev1: PosDimDep[P, D], ev2: ClassTag[slice.S],
      ev3: PairwiseTuners#V[T]): U[((Cell[slice.S], slice.R), (Cell[slice.S], slice.R))] = {
    val toTuple = (c: Cell[P]) => (Cell(slice.selected(c.position), c.content), slice.remainder(c.position))

    tuner match {
      case InMemory(_) =>
        ldata
          .mapSideJoin(rdata.toListValue((c: Cell[P]) => c),
            (lc: Cell[P], v: List[Cell[P]]) => v.collect {
              case rc if comparer.keep(slice.selected(lc.position), slice.selected(rc.position)) =>
                (toTuple(lc), toTuple(rc))
            })
      case _ =>
        val (rr, rj, lr, lj) = tuner.parameters match {
          case Sequence2(Sequence2(rr, rj), Sequence2(lr, lj)) => (rr, rj, lr, lj)
          case Sequence2(rj @ Reducers(_), Sequence2(lr, lj)) => (NoParameters, rj, lr, lj)
          case Sequence2(rr @ Redistribute(_), Sequence2(lr, lj)) => (rr, NoParameters, lr, lj)
          case Sequence2(Sequence2(rr, rj), lj @ Reducers(_)) => (rr, rj, NoParameters, lj)
          case Sequence2(Sequence2(rr, rj), lr @ Redistribute(_)) => (rr, rj, lr, NoParameters)
          case Sequence2(rj @ Reducers(_), lj @ Reducers(_)) => (NoParameters, rj, NoParameters, lj)
          case Sequence2(rr @ Redistribute(_), lj @ Reducers(_)) => (rr, NoParameters, NoParameters, lj)
          case Sequence2(rj @ Reducers(_), lr @ Redistribute(_)) => (NoParameters, rj, lr, NoParameters)
          case Sequence2(rr @ Redistribute(_), lr @ Redistribute(_)) => (rr, NoParameters, lr, NoParameters)
          case lj @ Reducers(_) => (NoParameters, NoParameters, NoParameters, lj)
          case _ => (NoParameters, NoParameters, NoParameters, NoParameters)
        }
        val right = Grouped(rdata.map { case Cell(p, _) => (slice.selected(p), ()) }.distinct)
          .redistribute(rr)
          .map { case (p, _) => List(p) }
          .sum
        val keys = Grouped(ldata.map { case Cell(p, _) => (slice.selected(p), ()) }.distinct)
          .redistribute(lr)
          .flatMapWithValue(right) { case ((l, _), vo) => vo.get.collect { case r if comparer.keep(l, r) => (r, l) } }
          .forceToDisk // TODO: Should this be configurable?

        ldata
          .groupBy { case Cell(p, _) => slice.selected(p) }
          .tunedJoin(tuner, lj, rdata
            .groupBy { case Cell(p, _) => slice.selected(p) }
            .tunedJoin(tuner, rj, keys)
            .map { case (r, (c, l)) => (l, c) })
          .map {
            case (_, (lc, rc)) => (toTuple(lc), toTuple(rc))
          }
    }
  }
}

/** Base trait for methods that reduce the number of dimensions or that can be filled using a `TypedPipe[Cell[P]]`. */
trait ReduceableMatrix[P <: Position with ReduceablePosition] extends BaseReduceableMatrix[P] { self: Matrix[P] =>

  import ScaldingImplicits._

  type FillHetrogeneousTuners = OneOf4[InMemory[NoParameters.type], InMemory[Reducers],
                                       Default[NoParameters.type], Default[Reducers]]
  def fill[D <: Dimension, Q <: Position, T <: Tuner](slice: Slice[P, D], values: U[Cell[Q]], tuner: T = Default())(
    implicit ev1: PosDimDep[P, D], ev2: ClassTag[P], ev3: ClassTag[slice.S], ev4: slice.S =:= Q,
      ev5: FillHetrogeneousTuners#V[T]): U[Cell[P]] = {
    val vals = values.groupBy { case c => c.position.asInstanceOf[slice.S] }
    val dense = tuner match {
      case InMemory(_) =>
        domain(Default())
          .mapSideJoin(vals.map { case (p, c) => Map(p -> c.content) }.sum, (p: P, v: Map[slice.S, Content]) =>
            v.get(slice.selected(p)).map { case c => Cell(p, c) })
      case _ =>
        domain(Default())
          .groupBy { case p => slice.selected(p) }
          .tunedJoin(tuner, tuner.parameters, vals)
          .map { case (_, (p, c)) => Cell(p, c.content) }
    }

    dense
      .groupBy { case c => c.position }
      .tuneReducers(tuner.parameters)
      .leftJoin(data.groupBy { case c => c.position })
      .map { case (p, (fc, co)) => co.getOrElse(fc) }
  }

  type FillHomogeneousTuners = TP2
  def fill[T <: Tuner](value: Content, tuner: T = Default())(implicit ev1: ClassTag[P],
    ev2: FillHomogeneousTuners#V[T]): U[Cell[P]] = {
    domain(Default())
      .asKeys
      .tuneReducers(tuner.parameters)
      .leftJoin(data.groupBy { case c => c.position })
      .map { case (p, (_, co)) => co.getOrElse(Cell(p, value)) }
  }

  def melt[D <: Dimension, G <: Dimension](dim: D, into: G, separator: String = ".")(implicit ev1: PosDimDep[P, D],
    ev2: PosDimDep[P, G], ne: D =!= G): U[Cell[P#L]] = {
    data.map { case Cell(p, c) => Cell(p.melt(dim, into, separator), c) }
  }

  type SquashTuners = TP2
  def squash[D <: Dimension, F, T <: Tuner](dim: D, squasher: F, tuner: T = Default())(implicit ev1: PosDimDep[P, D],
    ev2: Squashable[F, P], ev3: SquashTuners#V[T]): U[Cell[P#L]] = {
    val squash = ev2.convert(squasher)

    data
      .groupBy { case c => c.position.remove(dim) }
      .tuneReducers(tuner.parameters)
      .reduce[Cell[P]] { case (x, y) => squash.reduce(dim, x, y) }
      .map { case (p, c) => Cell(p, c.content) }
  }

  def squashWithValue[D <: Dimension, F, W, T <: Tuner](dim: D, squasher: F, value: E[W], tuner: T = Default())(
    implicit ev1: PosDimDep[P, D], ev2: SquashableWithValue[F, P, W], ev3: SquashTuners#V[T]): U[Cell[P#L]] = {
    val squash = ev2.convert(squasher)

    data
      .leftCross(value)
      .groupBy { case (c, vo) => c.position.remove(dim) }
      .tuneReducers(tuner.parameters)
      .reduce[(Cell[P], Option[W])] { case ((x, xvo), (y, yvo)) => (squash.reduceWithValue(dim, x, y, xvo.get), xvo) }
      .map { case (p, (c, _)) => Cell(p, c.content) }
  }
}

/** Base trait for methods that expand the number of dimension of a matrix using a `TypedPipe[Cell[P]]`. */
trait ExpandableMatrix[P <: Position with ExpandablePosition] extends BaseExpandableMatrix[P] { self: Matrix[P] =>

  def expand[Q <: Position](expander: Cell[P] => Q)(implicit ev: PosExpDep[P, Q]): TypedPipe[Cell[Q]] = {
    data.map { case c => Cell(expander(c), c.content) }
  }

  def expandWithValue[Q <: Position, W](expander: (Cell[P], W) => Q, value: ValuePipe[W])(
    implicit ev: PosExpDep[P, Q]): TypedPipe[Cell[Q]] = {
    data.mapWithValue(value) { case (c, vo) => Cell(expander(c, vo.get), c.content) }
  }
}

// TODO: Make this work on more than 2D matrices and share with Spark
trait MatrixDistance { self: Matrix[Position2D] with ReduceableMatrix[Position2D] =>

  import au.com.cba.omnia.grimlock.library.aggregate._
  import au.com.cba.omnia.grimlock.library.pairwise._
  import au.com.cba.omnia.grimlock.library.transform._
  import au.com.cba.omnia.grimlock.library.window._

  /**
   * Compute correlations.
   *
   * @param slice  Encapsulates the dimension for which to compute correlations.
   * @param stuner The sumamrise tuner for the job.
   * @param ptuner The pairwise tuner for the job.
   *
   * @return A `U[Cell[Position1D]]` with all pairwise correlations.
   */
  def correlation[D <: Dimension, ST <: Tuner, PT <: Tuner](slice: Slice[Position2D, D], stuner: ST = Default(),
    ptuner: PT = Default())(implicit ev1: PosDimDep[Position2D, D], ev2: ClassTag[slice.S], ev3: ClassTag[slice.R],
      ev4: SummariseTuners#V[ST], ev5: PairwiseTuners#V[PT]): U[Cell[Position1D]] = {
    implicit def UP2DSC2M1D(data: U[Cell[slice.S]]): Matrix1D = new Matrix1D(data.asInstanceOf[U[Cell[Position1D]]])
    implicit def UP2DRMC2M2D(data: U[Cell[slice.R#M]]): Matrix2D = new Matrix2D(data.asInstanceOf[U[Cell[Position2D]]])

    val mean = data
      .summarise(slice, Mean[Position2D, slice.S](), stuner)
      .toMap(Over(First))

    val centered = data
      .transformWithValue(Subtract(ExtractWithDimension(slice.dimension)
        .andThenPresent((con: Content) => con.value.asDouble)), mean)

    val denom = centered
      .transform(Power[Position2D](2))
      .summarise(slice, Sum[Position2D, slice.S](), stuner)
      .pairwise(Over(First), Lower, Times(Locate.OperatorString[Position1D, Position0D]("(%1$s*%2$s)")), ptuner)
      .transform(SquareRoot[Position1D]())
      .toMap(Over(First))

    centered
      .pairwise(slice, Lower, Times(Locate.OperatorString[slice.S, slice.R]("(%1$s*%2$s)")), ptuner)
      .summarise(Over(First), Sum[Position2D, Position1D](), stuner)
      .transformWithValue(Fraction(ExtractWithDimension[Dimension.First, Position1D, Content](First)
        .andThenPresent(_.value.asDouble)), denom)
  }

  /**
   * Compute mutual information.
   *
   * @param slice  Encapsulates the dimension for which to compute mutual information.
   * @param stuner The summarise tuner for the job.
   * @param ptuner The pairwise tuner for the job.
   *
   * @return A `U[Cell[Position1D]]` with all pairwise mutual information.
   */
  def mutualInformation[D <: Dimension, ST <: Tuner, PT <: Tuner](slice: Slice[Position2D, D], stuner: ST = Default(),
    ptuner: PT = Default())(implicit ev1: PosDimDep[Position2D, D], ev2: ClassTag[slice.S], ev3: ClassTag[slice.R],
      ev4: SummariseTuners#V[ST], ev5: PairwiseTuners#V[PT]): U[Cell[Position1D]] = {
    implicit def UP2DRMC2M2D(data: U[Cell[slice.R#M]]): Matrix2D = new Matrix2D(data.asInstanceOf[U[Cell[Position2D]]])

    val dim = slice match {
      case Over(First) => Second
      case Over(Second) => First
      case Along(d) => d
      case _ => throw new Exception("unexpected dimension")
    }

    implicit object P3D extends PosDimDep[Position3D, dim.type]

    type W = Map[Position1D, Content]

    val extractor = ExtractWithDimension[Dimension.First, Position2D, Content](First)
      .andThenPresent(_.value.asDouble)

    val mhist = new Matrix2D(data)
      .expand((c: Cell[Position2D]) => c.position.append(c.content.value.toShortString))
      .summarise(Along[Position3D, dim.type](dim), Count[Position3D, Position2D](), stuner)

    val mcount = mhist
      .summarise(Over(First), Sum[Position2D, Position1D](), stuner)
      .toMap()

    val marginal = mhist
      .summariseWithValue(Over(First), Entropy[Position2D, Position1D, W](extractor)
        .andThenExpandWithValue((cell, _) => cell.position.append("marginal")), mcount, stuner)
      .pairwise(Over(First), Upper, Plus(Locate.OperatorString[Position1D, Position1D]("%s,%s")), ptuner)

    val jhist = new Matrix2D(data)
      .pairwise(slice, Upper, Concatenate(Locate.OperatorString[slice.S, slice.R]("%s,%s")), ptuner)
      .expand((c: Cell[Position2D]) => c.position.append(c.content.value.toShortString))
      .summarise(Along(Second), Count[Position3D, Position2D](), stuner)

    val jcount = jhist
      .summarise(Over(First), Sum[Position2D, Position1D](), stuner)
      .toMap()

    val joint = jhist
      .summariseWithValue(Over(First), Entropy[Position2D, Position1D, W](extractor, negate = true)
        .andThenExpandWithValue((cell, _) => cell.position.append("joint")), jcount, stuner)

    (marginal ++ joint)
      .summarise(Over(First), Sum[Position2D, Position1D](), stuner)
  }

  /**
   * Compute Gini index.
   *
   * @param slice  Encapsulates the dimension for which to compute the Gini index.
   * @param stuner The summarise tuner for the job.
   * @param wtuner The window tuner for the job.
   * @param ptuner The pairwise tuner for the job.
   *
   * @return A `U[Cell[Position1D]]` with all pairwise Gini indices.
   */
  def gini[D <: Dimension, ST <: Tuner, WT <: Tuner, PT <: Tuner](slice: Slice[Position2D, D], stuner: ST = Default(),
    wtuner: WT = Default(), ptuner: PT = Default())(implicit ev1: PosDimDep[Position2D, D], ev2: ClassTag[slice.S],
      ev3: ClassTag[slice.R], ev4: SummariseTuners#V[ST], ev5: SlideTuners#V[WT],
        ev6: PairwiseTuners#V[PT]): U[Cell[Position1D]] = {
    implicit def UP2DSC2M1D(data: U[Cell[slice.S]]): Matrix1D = new Matrix1D(data.asInstanceOf[U[Cell[Position1D]]])
    implicit def UP2DSMC2M2D(data: U[Cell[slice.S#M]]): Matrix2D = new Matrix2D(data.asInstanceOf[U[Cell[Position2D]]])

    def isPositive = (cell: Cell[Position2D]) => cell.content.value.asDouble.map(_ > 0).getOrElse(false)
    def isNegative = (cell: Cell[Position2D]) => cell.content.value.asDouble.map(_ <= 0).getOrElse(false)

    val extractor = ExtractWithDimension[Dimension.First, Position2D, Content](First)
      .andThenPresent(_.value.asDouble)

    val pos = data
      .transform(Compare[Position2D](isPositive))
      .summarise(slice, Sum[Position2D, slice.S](), stuner)
      .toMap(Over(First))

    val neg = data
      .transform(Compare[Position2D](isNegative))
      .summarise(slice, Sum[Position2D, slice.S](), stuner)
      .toMap(Over(First))

    val tpr = data
      .transform(Compare[Position2D](isPositive))
      .slide(slice, CumulativeSum(Locate.WindowString[slice.S, slice.R]()), true, wtuner)
      .transformWithValue(Fraction(extractor), pos)
      .slide(Over(First), BinOp((l: Double, r: Double) => r + l,
        Locate.WindowPairwiseString[Position1D, Position1D]("%2$s.%1$s")), true, wtuner)

    val fpr = data
      .transform(Compare[Position2D](isNegative))
      .slide(slice, CumulativeSum(Locate.WindowString[slice.S, slice.R]()), true, wtuner)
      .transformWithValue(Fraction(extractor), neg)
      .slide(Over(First), BinOp((l: Double, r: Double) => r - l,
        Locate.WindowPairwiseString[Position1D, Position1D]("%2$s.%1$s")), true, wtuner)

    tpr
      .pairwiseBetween(Along(First), Diagonal, fpr,
        Times(Locate.OperatorString[Position1D, Position1D]("(%1$s*%2$s)")), ptuner)
      .summarise(Along(First), Sum[Position2D, Position1D](), stuner)
      .transformWithValue(Subtract(ExtractWithKey[Position1D, String, Double]("one"), true),
        ValuePipe(Map(Position1D("one") -> 1.0)))
  }
}

object Matrix {
  /**
   * Read column oriented, pipe separated matrix text data into a `TypedPipe[Cell[P]]`.
   *
   * @param file   The text file to read from.
   * @param parser The parser that converts a single line to a cell.
   */
  def loadText[P <: Position](file: String, parser: (String) => TraversableOnce[Cell[P]]): TypedPipe[Cell[P]] = {
    TypedPipe.from(TextLine(file)).flatMap { parser(_) }
  }

  /** Conversion from `TypedPipe[Cell[Position1D]]` to a Scalding `Matrix1D`. */
  implicit def TP2M1(data: TypedPipe[Cell[Position1D]]): Matrix1D = new Matrix1D(data)
  /** Conversion from `TypedPipe[Cell[Position2D]]` to a Scalding `Matrix2D`. */
  implicit def TP2M2(data: TypedPipe[Cell[Position2D]]): Matrix2D = new Matrix2D(data)
  /** Conversion from `TypedPipe[Cell[Position3D]]` to a Scalding `Matrix3D`. */
  implicit def TP2M3(data: TypedPipe[Cell[Position3D]]): Matrix3D = new Matrix3D(data)
  /** Conversion from `TypedPipe[Cell[Position4D]]` to a Scalding `Matrix4D`. */
  implicit def TP2TM4(data: TypedPipe[Cell[Position4D]]): Matrix4D = new Matrix4D(data)
  /** Conversion from `TypedPipe[Cell[Position5D]]` to a Scalding `Matrix5D`. */
  implicit def TP2M5(data: TypedPipe[Cell[Position5D]]): Matrix5D = new Matrix5D(data)
  /** Conversion from `TypedPipe[Cell[Position6D]]` to a Scalding `Matrix6D`. */
  implicit def TP2M6(data: TypedPipe[Cell[Position6D]]): Matrix6D = new Matrix6D(data)
  /** Conversion from `TypedPipe[Cell[Position7D]]` to a Scalding `Matrix7D`. */
  implicit def TP2M7(data: TypedPipe[Cell[Position7D]]): Matrix7D = new Matrix7D(data)
  /** Conversion from `TypedPipe[Cell[Position8D]]` to a Scalding `Matrix8D`. */
  implicit def TP2M8(data: TypedPipe[Cell[Position8D]]): Matrix8D = new Matrix8D(data)
  /** Conversion from `TypedPipe[Cell[Position9D]]` to a Scalding `Matrix9D`. */
  implicit def TP2M9(data: TypedPipe[Cell[Position9D]]): Matrix9D = new Matrix9D(data)

  /** Conversion from `List[(Valueable, Content)]` to a Scalding `Matrix1D`. */
  implicit def LV1C2M1[V: Valueable](list: List[(V, Content)]): Matrix1D = {
    new Matrix1D(new IterablePipe(list.map { case (v, c) => Cell(Position1D(v), c) }))
  }
  /** Conversion from `List[(Valueable, Valueable, Content)]` to a Scalding `Matrix2D`. */
  implicit def LV2C2M2[V: Valueable, W: Valueable](list: List[(V, W, Content)]): Matrix2D = {
    new Matrix2D(new IterablePipe(list.map { case (v, w, c) => Cell(Position2D(v, w), c) }))
  }
  /** Conversion from `List[(Valueable, Valueable, Valueable, Content)]` to a Scalding `Matrix3D`. */
  implicit def LV3C2M3[V: Valueable, W: Valueable, X: Valueable](list: List[(V, W, X, Content)]): Matrix3D = {
    new Matrix3D(new IterablePipe(list.map { case (v, w, x, c) => Cell(Position3D(v, w, x), c) }))
  }
  /** Conversion from `List[(Valueable, Valueable, Valueable, Valueable, Content)]` to a Scalding `Matrix4D`. */
  implicit def LV4C2M4[V: Valueable, W: Valueable, X: Valueable, Y: Valueable](
    list: List[(V, W, X, Y, Content)]): Matrix4D = {
    new Matrix4D(new IterablePipe(list.map { case (v, w, x, y, c) => Cell(Position4D(v, w, x, y), c) }))
  }
  /**
   * Conversion from `List[(Valueable, Valueable, Valueable, Valueable, Valueable, Content)]` to a Scalding `Matrix5D`.
   */
  implicit def LV5C2M5[V: Valueable, W: Valueable, X: Valueable, Y: Valueable, Z: Valueable](
    list: List[(V, W, X, Y, Z, Content)]): Matrix5D = {
    new Matrix5D(new IterablePipe(list.map { case (v, w, x, y, z, c) => Cell(Position5D(v, w, x, y, z), c) }))
  }
  /**
   * Conversion from `List[(Valueable, Valueable, Valueable, Valueable, Valueable, Valueable, Content)]` to a
   * Scalding `Matrix6D`.
   */
  implicit def LV6C2M6[U: Valueable, V: Valueable, W: Valueable, X: Valueable, Y: Valueable, Z: Valueable](
    list: List[(U, V, W, X, Y, Z, Content)]): Matrix6D = {
    new Matrix6D(new IterablePipe(list.map { case (u, v, w, x, y, z, c) => Cell(Position6D(u, v, w, x, y, z), c) }))
  }
  /**
   * Conversion from `List[(Valueable, Valueable, Valueable, Valueable, Valueable, Valueable, Valueable, Content)]`
   * to a Scalding `Matrix7D`.
   */
  implicit def LV7C2M7[T: Valueable, U: Valueable, V: Valueable, W: Valueable, X: Valueable, Y: Valueable, Z: Valueable](
    list: List[(T, U, V, W, X, Y, Z, Content)]): Matrix7D = {
    new Matrix7D(new IterablePipe(list.map {
      case (t, u, v, w, x, y, z, c) => Cell(Position7D(t, u, v, w, x, y, z), c)
    }))
  }
  /**
   * Conversion from `List[(Valueable, Valueable, Valueable, Valueable, Valueable, Valueable, Valueable, Valueable,
   * Content)]` to a Scalding `Matrix8D`.
   */
  implicit def LV8C2M8[S: Valueable, T: Valueable, U: Valueable, V: Valueable, W: Valueable, X: Valueable, Y: Valueable, Z: Valueable](
    list: List[(S, T, U, V, W, X, Y, Z, Content)]): Matrix8D = {
    new Matrix8D(new IterablePipe(list.map {
      case (s, t, u, v, w, x, y, z, c) => Cell(Position8D(s, t, u, v, w, x, y, z), c)
    }))
  }
  /**
   * Conversion from `List[(Valueable, Valueable, Valueable, Valueable, Valueable, Valueable, Valueable, Valueable,
   * Valueable, Content)]` to a Scalding `Matrix9D`.
   */
  implicit def LV9C2M9[R: Valueable, S: Valueable, T: Valueable, U: Valueable, V: Valueable, W: Valueable, X: Valueable, Y: Valueable, Z: Valueable](
    list: List[(R, S, T, U, V, W, X, Y, Z, Content)]): Matrix9D = {
    new Matrix9D(new IterablePipe(list.map {
      case (r, s, t, u, v, w, x, y, z, c) => Cell(Position9D(r, s, t, u, v, w, x, y, z), c)
    }))
  }
}

/**
 * Rich wrapper around a `TypedPipe[Cell[Position1D]]`.
 *
 * @param data `TypedPipe[Cell[Position1D]]`.
 */
class Matrix1D(val data: TypedPipe[Cell[Position1D]]) extends Matrix[Position1D] with ExpandableMatrix[Position1D] {
  def domain[T <: Tuner](tuner: T = Default())(implicit ev: DomainTuners#V[T]): U[Position1D] = names(Over(First))

  /**
   * Persist a `Matrix1D` as sparse matrix file (index, value).
   *
   * @param file       File to write to.
   * @param dictionary Pattern for the dictionary file name.
   * @param separator  Column separator to use in dictionary file.
   *
   * @return A `TypedPipe[Cell[Position1D]]`; that is it returns `data`.
   */
  def saveAsIV(file: String, dictionary: String = "%1$s.dict.%2$d", separator: String = "|")(implicit flow: FlowDef,
    mode: Mode): U[Cell[Position1D]] = {
    saveAsIVWithNames(file, names(Over(First)), dictionary, separator)
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
  def saveAsIVWithNames[I](file: String, names: I, dictionary: String = "%1$s.dict.%2$d", separator: String = "|")(
    implicit ev: BaseNameable[I, Position1D, Position1D, Dimension.First, TypedPipe], flow: FlowDef,
      mode: Mode): U[Cell[Position1D]] = {
    data
      .groupBy { case c => c.position }
      .join(saveDictionary(ev.convert(this, Over(First), names), file, dictionary, separator, First))
      .map { case (_, (c, i)) => i + separator + c.content.value.toShortString }
      .write(TypedSink(TextLine(file)))

    data
  }
}

/**
 * Rich wrapper around a `TypedPipe[Cell[Position2D]]`.
 *
 * @param data `TypedPipe[Cell[Position2D]]`.
 */
class Matrix2D(val data: TypedPipe[Cell[Position2D]]) extends Matrix[Position2D] with ReduceableMatrix[Position2D]
  with ExpandableMatrix[Position2D] with MatrixDistance {
  def domain[T <: Tuner](tuner: T = Default())(implicit ev: DomainTuners#V[T]): U[Position2D] = {
    names(Over(First))
      .map { case Position1D(c) => c }
      .cross(names(Over(Second)).map { case Position1D(c) => c })
      .map { case (c1, c2) => Position2D(c1, c2) }
  }

  /**
   * Permute the order of the coordinates in a position.
   *
   * @param first  Dimension used for the first coordinate.
   * @param second Dimension used for the second coordinate.
   */
  def permute[D <: Dimension, F <: Dimension](first: D, second: F)(implicit ev1: PosDimDep[Position2D, D],
    ev2: PosDimDep[Position2D, F], ev3: D =!= F): U[Cell[Position2D]] = {
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
  def saveAsCSV[D <: Dimension](slice: Slice[Position2D, D], file: String, separator: String = "|",
    escapee: Escape = Quote(), writeHeader: Boolean = true, header: String = "%s.header", writeRowId: Boolean = true,
      rowId: String = "id")(implicit ev2: PosDimDep[Position2D, D], ev3: ClassTag[slice.S], flow: FlowDef,
        mode: Mode): U[Cell[Position2D]] = {
    saveAsCSVWithNames(slice, file, names(slice), separator, escapee, writeHeader, header, writeRowId, rowId)
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
  def saveAsCSVWithNames[D <: Dimension, I](slice: Slice[Position2D, D], file: String, names: I,
    separator: String = "|", escapee: Escape = Quote(), writeHeader: Boolean = true, header: String = "%s.header",
    writeRowId: Boolean = true, rowId: String = "id")(implicit ev1: BaseNameable[I, Position2D, slice.S, D, TypedPipe],
      ev2: PosDimDep[Position2D, D], ev3: ClassTag[slice.S], flow: FlowDef, mode: Mode): U[Cell[Position2D]] = {
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
   * @note R's slam package has a simple triplet matrix format (which in turn is used by the tm package). This format
   *       should be compatible.
   */
  def saveAsIV(file: String, dictionary: String = "%1$s.dict.%2$d", separator: String = "|")(implicit flow: FlowDef,
    mode: Mode): U[Cell[Position2D]] = {
    saveAsIVWithNames(file, names(Over(First)), names(Over(Second)), dictionary, separator)
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
   * @note R's slam package has a simple triplet matrix format (which in turn is used by the tm package). This format
   *       should be compatible.
   */
  def saveAsIVWithNames[I, J](file: String, namesI: I, namesJ: J, dictionary: String = "%1$s.dict.%2$d",
    separator: String = "|")(implicit ev1: BaseNameable[I, Position2D, Position1D, Dimension.First, TypedPipe],
      ev2: BaseNameable[J, Position2D, Position1D, Dimension.Second, TypedPipe], flow: FlowDef,
        mode: Mode): U[Cell[Position2D]] = {
    data
      .groupBy { case c => Position1D(c.position(First)) }
      .join(saveDictionary(ev1.convert(this, Over(First), namesI), file, dictionary, separator, First))
      .values
      .groupBy { case (c, i) => Position1D(c.position(Second)) }
      .join(saveDictionary(ev2.convert(this, Over(Second), namesJ), file, dictionary, separator, Second))
      .map { case (_, ((c, i), j)) => i + separator + j + separator + c.content.value.toShortString }
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
  def saveAsLDA[D <: Dimension](slice: Slice[Position2D, D], file: String, dictionary: String = "%s.dict",
    separator: String = "|", addId: Boolean = false)(implicit ev: PosDimDep[Position2D, D], flow: FlowDef,
      mode: Mode): U[Cell[Position2D]] = {
    saveAsLDAWithNames(slice, file, names(Along(slice.dimension)), dictionary, separator, addId)
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
  def saveAsLDAWithNames[D <: Dimension, I](slice: Slice[Position2D, D], file: String, names: I,
    dictionary: String = "%s.dict", separator: String = "|", addId: Boolean = false)(
      implicit ev1: PosDimDep[Position2D, D], ev2: BaseNameable[I, Position2D, Position1D, D, TypedPipe],
        flow: FlowDef, mode: Mode): U[Cell[Position2D]] = {
    data
      .groupBy { case c => slice.remainder(c.position).asInstanceOf[Position1D] }
      .join(saveDictionary(ev2.convert(this, Along(slice.dimension), names), file, dictionary, separator))
      .map { case (_, (Cell(p, c), i)) => (p, " " + i + ":" + c.value.toShortString, 1L) }
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
  def saveAsVW[D <: Dimension](slice: Slice[Position2D, D], labels: U[Cell[Position1D]], file: String,
    dictionary: String = "%s.dict", separator: String = ":")(implicit ev: PosDimDep[Position2D, D], flow: FlowDef,
      mode: Mode): U[Cell[Position2D]] = {
    saveAsVWWithNames(slice, labels, file, names(Along(slice.dimension)), dictionary, separator)
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
  def saveAsVWWithNames[D <: Dimension, I](slice: Slice[Position2D, D], labels: U[Cell[Position1D]], file: String,
    names: I, dictionary: String = "%s.dict", separator: String = ":")(implicit ev: PosDimDep[Position2D, D],
      ev2: BaseNameable[I, Position2D, Position1D, D, TypedPipe], flow: FlowDef, mode: Mode): U[Cell[Position2D]] = {
    data
      .groupBy { case c => slice.remainder(c.position).asInstanceOf[Position1D] }
      .join(saveDictionary(ev2.convert(this, Along(slice.dimension), names), file, dictionary, separator))
      .map { case (_, (Cell(p, c), i)) => (p, " " + i + ":" + c.value.toShortString) }
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
class Matrix3D(val data: TypedPipe[Cell[Position3D]]) extends Matrix[Position3D] with ReduceableMatrix[Position3D]
  with ExpandableMatrix[Position3D] {
  def domain[T <: Tuner](tuner: T = Default())(implicit ev: DomainTuners#V[T]): U[Position3D] = {
    names(Over(First))
      .map { case Position1D(c) => c }
      .cross(names(Over(Second)).map { case Position1D(c) => c })
      .cross(names(Over(Third)).map { case Position1D(c) => c })
      .map { case ((c1, c2), c3) => Position3D(c1, c2, c3) }
  }

  /**
   * Permute the order of the coordinates in a position.
   *
   * @param first  Dimension used for the first coordinate.
   * @param second Dimension used for the second coordinate.
   * @param third  Dimension used for the third coordinate.
   */
  def permute[D <: Dimension, F <: Dimension, G <: Dimension](first: D, second: F, third: G)(
    implicit ev1: PosDimDep[Position3D, D], ev2: PosDimDep[Position3D, F], ev3: PosDimDep[Position3D, G],
      ev4: Distinct3[D, F, G]): U[Cell[Position3D]] = {
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
  def saveAsIV(file: String, dictionary: String = "%1$s.dict.%2$d", separator: String = "|")(implicit flow: FlowDef,
    mode: Mode): U[Cell[Position3D]] = {
    saveAsIVWithNames(file, names(Over(First)), names(Over(Second)), names(Over(Third)), dictionary, separator)
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
  def saveAsIVWithNames[I, J, K](file: String, namesI: I, namesJ: J, namesK: K, dictionary: String = "%1$s.dict.%2$d",
    separator: String = "|")(implicit ev1: BaseNameable[I, Position3D, Position1D, Dimension.First, TypedPipe],
      ev2: BaseNameable[J, Position3D, Position1D, Dimension.Second, TypedPipe],
        ev3: BaseNameable[K, Position3D, Position1D, Dimension.Third, TypedPipe], flow: FlowDef,
          mode: Mode): U[Cell[Position3D]] = {
    data
      .groupBy { case c => Position1D(c.position(First)) }
      .join(saveDictionary(ev1.convert(this, Over(First), namesI), file, dictionary, separator, First))
      .values
      .groupBy { case (c, i) => Position1D(c.position(Second)) }
      .join(saveDictionary(ev2.convert(this, Over(Second), namesJ), file, dictionary, separator, Second))
      .map { case (_, ((c, i), j)) => (c, i, j) }
      .groupBy { case (c, i, j) => Position1D(c.position(Third)) }
      .join(saveDictionary(ev3.convert(this, Over(Third), namesK), file, dictionary, separator, Third))
      .map { case (_, ((c, i, j), k)) => i + separator + j + separator + k + separator + c.content.value.toShortString }
      .write(TypedSink(TextLine(file)))

    data
  }
}

/**
 * Rich wrapper around a `TypedPipe[Cell[Position4D]]`.
 *
 * @param data `TypedPipe[Cell[Position4D]]`.
 */
class Matrix4D(val data: TypedPipe[Cell[Position4D]]) extends Matrix[Position4D] with ReduceableMatrix[Position4D]
  with ExpandableMatrix[Position4D] {
  def domain[T <: Tuner](tuner: T = Default())(implicit ev: DomainTuners#V[T]): U[Position4D] = {
    names(Over(First))
      .map { case Position1D(c) => c }
      .cross(names(Over(Second)).map { case Position1D(c) => c })
      .cross(names(Over(Third)).map { case Position1D(c) => c })
      .cross(names(Over(Fourth)).map { case Position1D(c) => c })
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
  def permute[D <: Dimension, F <: Dimension, G <: Dimension, H <: Dimension](first: D, second: F, third: G,
    fourth: H)(implicit ev1: PosDimDep[Position4D, D], ev2: PosDimDep[Position4D, F], ev3: PosDimDep[Position4D, G],
      ev4: PosDimDep[Position4D, H], ev5: Distinct4[D, F, G, H]): U[Cell[Position4D]] = {
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
  def saveAsIV(file: String, dictionary: String = "%1$s.dict.%2$d", separator: String = "|")(implicit flow: FlowDef,
    mode: Mode): U[Cell[Position4D]] = {
    saveAsIVWithNames(file, names(Over(First)), names(Over(Second)), names(Over(Third)), names(Over(Fourth)),
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
  def saveAsIVWithNames[I, J, K, L](file: String, namesI: I, namesJ: J, namesK: K, namesL: L,
    dictionary: String = "%1$s.dict.%2$d", separator: String = "|")(
      implicit ev1: BaseNameable[I, Position4D, Position1D, Dimension.First, TypedPipe],
        ev2: BaseNameable[J, Position4D, Position1D, Dimension.Second, TypedPipe],
          ev3: BaseNameable[K, Position4D, Position1D, Dimension.Third, TypedPipe],
            ev4: BaseNameable[L, Position4D, Position1D, Dimension.Fourth, TypedPipe], flow: FlowDef,
              mode: Mode): U[Cell[Position4D]] = {
    data
      .groupBy { case c => Position1D(c.position(First)) }
      .join(saveDictionary(ev1.convert(this, Over(First), namesI), file, dictionary, separator, First))
      .values
      .groupBy { case (c, i) => Position1D(c.position(Second)) }
      .join(saveDictionary(ev2.convert(this, Over(Second), namesJ), file, dictionary, separator, Second))
      .map { case (_, ((c, i), j)) => (c, i, j) }
      .groupBy { case (c, i, j) => Position1D(c.position(Third)) }
      .join(saveDictionary(ev3.convert(this, Over(Third), namesK), file, dictionary, separator, Third))
      .map { case (_, ((c, i, j), k)) => (c, i, j, k) }
      .groupBy { case (c, i, j, k) => Position1D(c.position(Fourth)) }
      .join(saveDictionary(ev4.convert(this, Over(Fourth), namesL), file, dictionary, separator, Fourth))
      .map {
        case (_, ((c, i, j, k), l)) =>
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
class Matrix5D(val data: TypedPipe[Cell[Position5D]]) extends Matrix[Position5D] with ReduceableMatrix[Position5D]
  with ExpandableMatrix[Position5D] {
  def domain[T <: Tuner](tuner: T = Default())(implicit ev: DomainTuners#V[T]): U[Position5D] = {
    names(Over(First))
      .map { case Position1D(c) => c }
      .cross(names(Over(Second)).map { case Position1D(c) => c })
      .cross(names(Over(Third)).map { case Position1D(c) => c })
      .cross(names(Over(Fourth)).map { case Position1D(c) => c })
      .cross(names(Over(Fifth)).map { case Position1D(c) => c })
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
  def permute[D <: Dimension, F <: Dimension, G <: Dimension, H <: Dimension, I <: Dimension](first: D, second: F,
    third: G, fourth: H, fifth: I)(implicit ev1: PosDimDep[Position5D, D], ev2: PosDimDep[Position5D, F],
      ev3: PosDimDep[Position5D, G], ev4: PosDimDep[Position5D, H], ev5: PosDimDep[Position5D, I],
        ev6: Distinct5[D, F, G, H, I]): U[Cell[Position5D]] = {
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
  def saveAsIV(file: String, dictionary: String = "%1$s.dict.%2$d", separator: String = "|")(implicit flow: FlowDef,
    mode: Mode): U[Cell[Position5D]] = {
    saveAsIVWithNames(file, names(Over(First)), names(Over(Second)), names(Over(Third)), names(Over(Fourth)),
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
  def saveAsIVWithNames[I, J, K, L, M](file: String, namesI: I, namesJ: J, namesK: K, namesL: L, namesM: M,
    dictionary: String = "%1$s.dict.%2$d", separator: String = "|")(
      implicit ev1: BaseNameable[I, Position5D, Position1D, Dimension.First, TypedPipe],
        ev2: BaseNameable[J, Position5D, Position1D, Dimension.Second, TypedPipe],
          ev3: BaseNameable[K, Position5D, Position1D, Dimension.Third, TypedPipe],
            ev4: BaseNameable[L, Position5D, Position1D, Dimension.Fourth, TypedPipe],
              ev5: BaseNameable[M, Position5D, Position1D, Dimension.Fifth, TypedPipe], flow: FlowDef,
                mode: Mode): U[Cell[Position5D]] = {
    data
      .groupBy { case c => Position1D(c.position(First)) }
      .join(saveDictionary(ev1.convert(this, Over(First), namesI), file, dictionary, separator, First))
      .values
      .groupBy { case (c, i) => Position1D(c.position(Second)) }
      .join(saveDictionary(ev2.convert(this, Over(Second), namesJ), file, dictionary, separator, Second))
      .map { case (_, ((c, i), j)) => (c, i, j) }
      .groupBy { case (c, i, j) => Position1D(c.position(Third)) }
      .join(saveDictionary(ev3.convert(this, Over(Third), namesK), file, dictionary, separator, Third))
      .map { case (_, ((c, i, j), k)) => (c, i, j, k) }
      .groupBy { case (c, i, j, k) => Position1D(c.position(Fourth)) }
      .join(saveDictionary(ev4.convert(this, Over(Fourth), namesL), file, dictionary, separator, Fourth))
      .map { case (_, ((c, i, j, k), l)) => (c, i, j, k, l) }
      .groupBy { case (c, i, j, k, l) => Position1D(c.position(Fifth)) }
      .join(saveDictionary(ev5.convert(this, Over(Fifth), namesM), file, dictionary, separator, Fifth))
      .map {
        case (_, ((c, i, j, k, l), m)) =>
          i + separator + j + separator + k + separator + l + separator + m + separator + c.content.value.toShortString
      }
      .write(TypedSink(TextLine(file)))

    data
  }
}

/**
 * Rich wrapper around a `TypedPipe[Cell[Position6D]]`.
 *
 * @param data `TypedPipe[Cell[Position6D]]`.
 */
class Matrix6D(val data: TypedPipe[Cell[Position6D]]) extends Matrix[Position6D] with ReduceableMatrix[Position6D]
  with ExpandableMatrix[Position6D] {
  def domain[T <: Tuner](tuner: T = Default())(implicit ev: DomainTuners#V[T]): U[Position6D] = {
    names(Over(First))
      .map { case Position1D(c) => c }
      .cross(names(Over(Second)).map { case Position1D(c) => c })
      .cross(names(Over(Third)).map { case Position1D(c) => c })
      .cross(names(Over(Fourth)).map { case Position1D(c) => c })
      .cross(names(Over(Fifth)).map { case Position1D(c) => c })
      .cross(names(Over(Sixth)).map { case Position1D(c) => c })
      .map { case (((((c1, c2), c3), c4), c5), c6) => Position6D(c1, c2, c3, c4, c5, c6) }
  }

  /**
   * Permute the order of the coordinates in a position.
   *
   * @param first  Dimension used for the first coordinate.
   * @param second Dimension used for the second coordinate.
   * @param third  Dimension used for the third coordinate.
   * @param fourth Dimension used for the fourth coordinate.
   * @param fifth  Dimension used for the fifth coordinate.
   * @param sixth  Dimension used for the sixth coordinate.
   */
  def permute[D <: Dimension, F <: Dimension, G <: Dimension, H <: Dimension, I <: Dimension, J <: Dimension](
    first: D, second: F, third: G, fourth: H, fifth: I, sixth: J)(implicit ev1: PosDimDep[Position6D, D],
      ev2: PosDimDep[Position6D, F], ev3: PosDimDep[Position6D, G], ev4: PosDimDep[Position6D, H],
        ev5: PosDimDep[Position6D, I], ev6: PosDimDep[Position6D, J],
          ev7: Distinct6[D, F, G, H, I, J]): U[Cell[Position6D]] = {
    data.map { case Cell(p, c) => Cell(p.permute(List(first, second, third, fourth, fifth, sixth)), c) }
  }

  /**
   * Persist a `Matrix6D` as sparse matrix file (index, index, index, index, index, index, value).
   *
   * @param file       File to write to.
   * @param dictionary Pattern for the dictionary file name.
   * @param separator  Column separator to use in dictionary file.
   *
   * @return A `TypedPipe[Cell[Position6D]]`; that is it returns `data`.
   */
  def saveAsIV(file: String, dictionary: String = "%1$s.dict.%2$d", separator: String = "|")(implicit flow: FlowDef,
    mode: Mode): U[Cell[Position6D]] = {
    saveAsIVWithNames(file, names(Over(First)), names(Over(Second)), names(Over(Third)), names(Over(Fourth)),
      names(Over(Fifth)), names(Over(Sixth)), dictionary, separator)
  }

  /**
   * Persist a `Matrix6D` as sparse matrix file (index, index, index, index, index, index, value).
   *
   * @param file       File to write to.
   * @param namesI     The names to use for the first dimension (according to their ordering).
   * @param namesJ     The names to use for the second dimension (according to their ordering).
   * @param namesK     The names to use for the third dimension (according to their ordering).
   * @param namesL     The names to use for the fourth dimension (according to their ordering).
   * @param namesM     The names to use for the fifth dimension (according to their ordering).
   * @param namesN     The names to use for the sixth dimension (according to their ordering).
   * @param dictionary Pattern for the dictionary file name.
   * @param separator  Column separator to use in dictionary file.
   *
   * @return A `TypedPipe[Cell[Position6D]]`; that is it returns `data`.
   *
   * @note If `names` contains a subset of the columns, then only those columns get persisted to file.
   */
  def saveAsIVWithNames[I, J, K, L, M, N](file: String, namesI: I, namesJ: J, namesK: K, namesL: L, namesM: M,
    namesN: N, dictionary: String = "%1$s.dict.%2$d", separator: String = "|")(
      implicit ev1: BaseNameable[I, Position6D, Position1D, Dimension.First, TypedPipe],
        ev2: BaseNameable[J, Position6D, Position1D, Dimension.Second, TypedPipe],
          ev3: BaseNameable[K, Position6D, Position1D, Dimension.Third, TypedPipe],
            ev4: BaseNameable[L, Position6D, Position1D, Dimension.Fourth, TypedPipe],
              ev5: BaseNameable[M, Position6D, Position1D, Dimension.Fifth, TypedPipe],
                ev6: BaseNameable[N, Position6D, Position1D, Dimension.Sixth, TypedPipe], flow: FlowDef,
                  mode: Mode): U[Cell[Position6D]] = {
    data
      .groupBy { case c => Position1D(c.position(First)) }
      .join(saveDictionary(ev1.convert(this, Over(First), namesI), file, dictionary, separator, First))
      .values
      .groupBy { case (c, i) => Position1D(c.position(Second)) }
      .join(saveDictionary(ev2.convert(this, Over(Second), namesJ), file, dictionary, separator, Second))
      .map { case (_, ((c, i), j)) => (c, i, j) }
      .groupBy { case (c, i, j) => Position1D(c.position(Third)) }
      .join(saveDictionary(ev3.convert(this, Over(Third), namesK), file, dictionary, separator, Third))
      .map { case (_, ((c, i, j), k)) => (c, i, j, k) }
      .groupBy { case (c, i, j, k) => Position1D(c.position(Fourth)) }
      .join(saveDictionary(ev4.convert(this, Over(Fourth), namesL), file, dictionary, separator, Fourth))
      .map { case (_, ((c, i, j, k), l)) => (c, i, j, k, l) }
      .groupBy { case (c, i, j, k, l) => Position1D(c.position(Fifth)) }
      .join(saveDictionary(ev5.convert(this, Over(Fifth), namesM), file, dictionary, separator, Fifth))
      .map { case (_, ((c, i, j, k, l), m)) => (c, i, j, k, l, m) }
      .groupBy { case (c, i, j, k, l, m) => Position1D(c.position(Sixth)) }
      .join(saveDictionary(ev6.convert(this, Over(Sixth), namesN), file, dictionary, separator, Sixth))
      .map {
        case (_, ((c, i, j, k, l, m), n)) =>
          i + separator + j + separator + k + separator + l + separator + m + separator +
            n + separator + c.content.value.toShortString
      }
      .write(TypedSink(TextLine(file)))

    data
  }
}

/**
 * Rich wrapper around a `TypedPipe[Cell[Position7D]]`.
 *
 * @param data `TypedPipe[Cell[Position7D]]`.
 */
class Matrix7D(val data: TypedPipe[Cell[Position7D]]) extends Matrix[Position7D] with ReduceableMatrix[Position7D]
  with ExpandableMatrix[Position7D] {
  def domain[T <: Tuner](tuner: T = Default())(implicit ev: DomainTuners#V[T]): U[Position7D] = {
    names(Over(First))
      .map { case Position1D(c) => c }
      .cross(names(Over(Second)).map { case Position1D(c) => c })
      .cross(names(Over(Third)).map { case Position1D(c) => c })
      .cross(names(Over(Fourth)).map { case Position1D(c) => c })
      .cross(names(Over(Fifth)).map { case Position1D(c) => c })
      .cross(names(Over(Sixth)).map { case Position1D(c) => c })
      .cross(names(Over(Seventh)).map { case Position1D(c) => c })
      .map { case ((((((c1, c2), c3), c4), c5), c6), c7) => Position7D(c1, c2, c3, c4, c5, c6, c7) }
  }

  /**
   * Permute the order of the coordinates in a position.
   *
   * @param first   Dimension used for the first coordinate.
   * @param second  Dimension used for the second coordinate.
   * @param third   Dimension used for the third coordinate.
   * @param fourth  Dimension used for the fourth coordinate.
   * @param fifth   Dimension used for the fifth coordinate.
   * @param sixth   Dimension used for the sixth coordinate.
   * @param seventh Dimension used for the seventh coordinate.
   */
  def permute[D <: Dimension, F <: Dimension, G <: Dimension, H <: Dimension, I <: Dimension, J <: Dimension, K <: Dimension](
    first: D, second: F, third: G, fourth: H, fifth: I, sixth: J, seventh: K)(implicit ev1: PosDimDep[Position7D, D],
      ev2: PosDimDep[Position7D, F], ev3: PosDimDep[Position7D, G], ev4: PosDimDep[Position7D, H],
        ev5: PosDimDep[Position7D, I], ev6: PosDimDep[Position7D, J], ev7: PosDimDep[Position7D, K],
          ev8: Distinct7[D, F, G, H, I, J, K]): U[Cell[Position7D]] = {
    data.map { case Cell(p, c) => Cell(p.permute(List(first, second, third, fourth, fifth, sixth, seventh)), c) }
  }

  /**
   * Persist a `Matrix7D` as sparse matrix file (index, index, index, index, index, index, index, value).
   *
   * @param file       File to write to.
   * @param dictionary Pattern for the dictionary file name.
   * @param separator  Column separator to use in dictionary file.
   *
   * @return A `TypedPipe[Cell[Position7D]]`; that is it returns `data`.
   */
  def saveAsIV(file: String, dictionary: String = "%1$s.dict.%2$d", separator: String = "|")(implicit flow: FlowDef,
    mode: Mode): U[Cell[Position7D]] = {
    saveAsIVWithNames(file, names(Over(First)), names(Over(Second)), names(Over(Third)), names(Over(Fourth)),
      names(Over(Fifth)), names(Over(Sixth)), names(Over(Seventh)), dictionary, separator)
  }

  /**
   * Persist a `Matrix7D` as sparse matrix file (index, index, index, index, index, index, index, value).
   *
   * @param file       File to write to.
   * @param namesI     The names to use for the first dimension (according to their ordering).
   * @param namesJ     The names to use for the second dimension (according to their ordering).
   * @param namesK     The names to use for the third dimension (according to their ordering).
   * @param namesL     The names to use for the fourth dimension (according to their ordering).
   * @param namesM     The names to use for the fifth dimension (according to their ordering).
   * @param namesN     The names to use for the sixth dimension (according to their ordering).
   * @param namesO     The names to use for the seventh dimension (according to their ordering).
   * @param dictionary Pattern for the dictionary file name.
   * @param separator  Column separator to use in dictionary file.
   *
   * @return A `TypedPipe[Cell[Position7D]]`; that is it returns `data`.
   *
   * @note If `names` contains a subset of the columns, then only those columns get persisted to file.
   */
  def saveAsIVWithNames[I, J, K, L, M, N, O](file: String, namesI: I, namesJ: J, namesK: K, namesL: L, namesM: M,
    namesN: N, namesO: O, dictionary: String = "%1$s.dict.%2$d", separator: String = "|")(
      implicit ev1: BaseNameable[I, Position7D, Position1D, Dimension.First, TypedPipe],
        ev2: BaseNameable[J, Position7D, Position1D, Dimension.Second, TypedPipe],
          ev3: BaseNameable[K, Position7D, Position1D, Dimension.Third, TypedPipe],
            ev4: BaseNameable[L, Position7D, Position1D, Dimension.Fourth, TypedPipe],
              ev5: BaseNameable[M, Position7D, Position1D, Dimension.Fifth, TypedPipe],
                ev6: BaseNameable[N, Position7D, Position1D, Dimension.Sixth, TypedPipe],
                  ev7: BaseNameable[O, Position7D, Position1D, Dimension.Seventh, TypedPipe], flow: FlowDef,
                    mode: Mode): U[Cell[Position7D]] = {
    data
      .groupBy { case c => Position1D(c.position(First)) }
      .join(saveDictionary(ev1.convert(this, Over(First), namesI), file, dictionary, separator, First))
      .values
      .groupBy { case (c, i) => Position1D(c.position(Second)) }
      .join(saveDictionary(ev2.convert(this, Over(Second), namesJ), file, dictionary, separator, Second))
      .map { case (_, ((c, i), j)) => (c, i, j) }
      .groupBy { case (c, i, j) => Position1D(c.position(Third)) }
      .join(saveDictionary(ev3.convert(this, Over(Third), namesK), file, dictionary, separator, Third))
      .map { case (_, ((c, i, j), k)) => (c, i, j, k) }
      .groupBy { case (c, i, j, k) => Position1D(c.position(Fourth)) }
      .join(saveDictionary(ev4.convert(this, Over(Fourth), namesL), file, dictionary, separator, Fourth))
      .map { case (_, ((c, i, j, k), l)) => (c, i, j, k, l) }
      .groupBy { case (c, i, j, k, l) => Position1D(c.position(Fifth)) }
      .join(saveDictionary(ev5.convert(this, Over(Fifth), namesM), file, dictionary, separator, Fifth))
      .map { case (_, ((c, i, j, k, l), m)) => (c, i, j, k, l, m) }
      .groupBy { case (c, i, j, k, l, m) => Position1D(c.position(Sixth)) }
      .join(saveDictionary(ev6.convert(this, Over(Sixth), namesN), file, dictionary, separator, Sixth))
      .map { case (_, ((c, i, j, k, l, m), n)) => (c, i, j, k, l, m, n) }
      .groupBy { case (c, i, j, k, l, m, n) => Position1D(c.position(Seventh)) }
      .join(saveDictionary(ev7.convert(this, Over(Seventh), namesO), file, dictionary, separator, Seventh))
      .map {
        case (_, ((c, i, j, k, l, m, n), o)) =>
          i + separator + j + separator + k + separator + l + separator + m + separator +
            n + separator + o + separator + c.content.value.toShortString
      }
      .write(TypedSink(TextLine(file)))

    data
  }
}

/**
 * Rich wrapper around a `TypedPipe[Cell[Position8D]]`.
 *
 * @param data `TypedPipe[Cell[Position8D]]`.
 */
class Matrix8D(val data: TypedPipe[Cell[Position8D]]) extends Matrix[Position8D] with ReduceableMatrix[Position8D]
  with ExpandableMatrix[Position8D] {
  def domain[T <: Tuner](tuner: T = Default())(implicit ev: DomainTuners#V[T]): U[Position8D] = {
    names(Over(First))
      .map { case Position1D(c) => c }
      .cross(names(Over(Second)).map { case Position1D(c) => c })
      .cross(names(Over(Third)).map { case Position1D(c) => c })
      .cross(names(Over(Fourth)).map { case Position1D(c) => c })
      .cross(names(Over(Fifth)).map { case Position1D(c) => c })
      .cross(names(Over(Sixth)).map { case Position1D(c) => c })
      .cross(names(Over(Seventh)).map { case Position1D(c) => c })
      .cross(names(Over(Eighth)).map { case Position1D(c) => c })
      .map { case (((((((c1, c2), c3), c4), c5), c6), c7), c8) => Position8D(c1, c2, c3, c4, c5, c6, c7, c8) }
  }

  /**
   * Permute the order of the coordinates in a position.
   *
   * @param first   Dimension used for the first coordinate.
   * @param second  Dimension used for the second coordinate.
   * @param third   Dimension used for the third coordinate.
   * @param fourth  Dimension used for the fourth coordinate.
   * @param fifth   Dimension used for the fifth coordinate.
   * @param sixth   Dimension used for the sixth coordinate.
   * @param seventh Dimension used for the seventh coordinate.
   * @param eighth  Dimension used for the eighth coordinate.
   */
  def permute[D <: Dimension, F <: Dimension, G <: Dimension, H <: Dimension, I <: Dimension, J <: Dimension, K <: Dimension, L <: Dimension](
    first: D, second: F, third: G, fourth: H, fifth: I, sixth: J, seventh: K, eighth: L)(
      implicit ev1: PosDimDep[Position8D, D], ev2: PosDimDep[Position8D, F], ev3: PosDimDep[Position8D, G],
      ev4: PosDimDep[Position8D, H], ev5: PosDimDep[Position8D, I], ev6: PosDimDep[Position8D, J],
        ev7: PosDimDep[Position8D, K], ev8: PosDimDep[Position8D, L],
          ev9: Distinct8[D, F, G, H, I, J, K, L]): U[Cell[Position8D]] = {
    data.map {
      case Cell(p, c) => Cell(p.permute(List(first, second, third, fourth, fifth, sixth, seventh, eighth)), c)
    }
  }

  /**
   * Persist a `Matrix8D` as sparse matrix file (index, index, index, index, index, index, index, index, value).
   *
   * @param file       File to write to.
   * @param dictionary Pattern for the dictionary file name.
   * @param separator  Column separator to use in dictionary file.
   *
   * @return A `TypedPipe[Cell[Position8D]]`; that is it returns `data`.
   */
  def saveAsIV(file: String, dictionary: String = "%1$s.dict.%2$d", separator: String = "|")(implicit flow: FlowDef,
    mode: Mode): U[Cell[Position8D]] = {
    saveAsIVWithNames(file, names(Over(First)), names(Over(Second)), names(Over(Third)), names(Over(Fourth)),
      names(Over(Fifth)), names(Over(Sixth)), names(Over(Seventh)), names(Over(Eighth)), dictionary, separator)
  }

  /**
   * Persist a `Matrix8D` as sparse matrix file (index, index, index, index, index, index, index, index, value).
   *
   * @param file       File to write to.
   * @param namesI     The names to use for the first dimension (according to their ordering).
   * @param namesJ     The names to use for the second dimension (according to their ordering).
   * @param namesK     The names to use for the third dimension (according to their ordering).
   * @param namesL     The names to use for the fourth dimension (according to their ordering).
   * @param namesM     The names to use for the fifth dimension (according to their ordering).
   * @param namesN     The names to use for the sixth dimension (according to their ordering).
   * @param namesO     The names to use for the seventh dimension (according to their ordering).
   * @param namesQ     The names to use for the eighth dimension (according to their ordering).
   * @param dictionary Pattern for the dictionary file name.
   * @param separator  Column separator to use in dictionary file.
   *
   * @return A `TypedPipe[Cell[Position8D]]`; that is it returns `data`.
   *
   * @note If `names` contains a subset of the columns, then only those columns get persisted to file.
   */
  def saveAsIVWithNames[I, J, K, L, M, N, O, Q](file: String, namesI: I, namesJ: J, namesK: K, namesL: L, namesM: M,
    namesN: N, namesO: O, namesQ: Q, dictionary: String = "%1$s.dict.%2$d", separator: String = "|")(
      implicit ev1: BaseNameable[I, Position8D, Position1D, Dimension.First, TypedPipe],
        ev2: BaseNameable[J, Position8D, Position1D, Dimension.Second, TypedPipe],
          ev3: BaseNameable[K, Position8D, Position1D, Dimension.Third, TypedPipe],
            ev4: BaseNameable[L, Position8D, Position1D, Dimension.Fourth, TypedPipe],
              ev5: BaseNameable[M, Position8D, Position1D, Dimension.Fifth, TypedPipe],
                ev6: BaseNameable[N, Position8D, Position1D, Dimension.Sixth, TypedPipe],
                  ev7: BaseNameable[O, Position8D, Position1D, Dimension.Seventh, TypedPipe],
                    ev8: BaseNameable[Q, Position8D, Position1D, Dimension.Eighth, TypedPipe], flow: FlowDef,
                      mode: Mode): U[Cell[Position8D]] = {
    data
      .groupBy { case c => Position1D(c.position(First)) }
      .join(saveDictionary(ev1.convert(this, Over(First), namesI), file, dictionary, separator, First))
      .values
      .groupBy { case (c, i) => Position1D(c.position(Second)) }
      .join(saveDictionary(ev2.convert(this, Over(Second), namesJ), file, dictionary, separator, Second))
      .map { case (_, ((c, i), j)) => (c, i, j) }
      .groupBy { case (c, i, j) => Position1D(c.position(Third)) }
      .join(saveDictionary(ev3.convert(this, Over(Third), namesK), file, dictionary, separator, Third))
      .map { case (_, ((c, i, j), k)) => (c, i, j, k) }
      .groupBy { case (c, i, j, k) => Position1D(c.position(Fourth)) }
      .join(saveDictionary(ev4.convert(this, Over(Fourth), namesL), file, dictionary, separator, Fourth))
      .map { case (_, ((c, i, j, k), l)) => (c, i, j, k, l) }
      .groupBy { case (c, i, j, k, l) => Position1D(c.position(Fifth)) }
      .join(saveDictionary(ev5.convert(this, Over(Fifth), namesM), file, dictionary, separator, Fifth))
      .map { case (_, ((c, i, j, k, l), m)) => (c, i, j, k, l, m) }
      .groupBy { case (c, i, j, k, l, m) => Position1D(c.position(Sixth)) }
      .join(saveDictionary(ev6.convert(this, Over(Sixth), namesN), file, dictionary, separator, Sixth))
      .map { case (_, ((c, i, j, k, l, m), n)) => (c, i, j, k, l, m, n) }
      .groupBy { case (c, i, j, k, l, m, n) => Position1D(c.position(Seventh)) }
      .join(saveDictionary(ev7.convert(this, Over(Seventh), namesO), file, dictionary, separator, Seventh))
      .map { case (_, ((c, i, j, k, l, m, n), o)) => (c, i, j, k, l, m, n, o) }
      .groupBy { case (c, i, j, k, l, m, n, o) => Position1D(c.position(Eighth)) }
      .join(saveDictionary(ev8.convert(this, Over(Eighth), namesQ), file, dictionary, separator, Eighth))
      .map {
        case (_, ((c, i, j, k, l, m, n, o), p)) =>
          i + separator + j + separator + k + separator + l + separator + m + separator +
            n + separator + o + separator + p + separator + c.content.value.toShortString
      }
      .write(TypedSink(TextLine(file)))

    data
  }
}

/**
 * Rich wrapper around a `TypedPipe[Cell[Position9D]]`.
 *
 * @param data `TypedPipe[Cell[Position9D]]`.
 */
class Matrix9D(val data: TypedPipe[Cell[Position9D]]) extends Matrix[Position9D] with ReduceableMatrix[Position9D] {
  def domain[T <: Tuner](tuner: T = Default())(implicit ev: DomainTuners#V[T]): U[Position9D] = {
    names(Over(First))
      .map { case Position1D(c) => c }
      .cross(names(Over(Second)).map { case Position1D(c) => c })
      .cross(names(Over(Third)).map { case Position1D(c) => c })
      .cross(names(Over(Fourth)).map { case Position1D(c) => c })
      .cross(names(Over(Fifth)).map { case Position1D(c) => c })
      .cross(names(Over(Sixth)).map { case Position1D(c) => c })
      .cross(names(Over(Seventh)).map { case Position1D(c) => c })
      .cross(names(Over(Eighth)).map { case Position1D(c) => c })
      .cross(names(Over(Ninth)).map { case Position1D(c) => c })
      .map { case ((((((((c1, c2), c3), c4), c5), c6), c7), c8), c9) => Position9D(c1, c2, c3, c4, c5, c6, c7, c8, c9) }
  }

  /**
   * Permute the order of the coordinates in a position.
   *
   * @param first   Dimension used for the first coordinate.
   * @param second  Dimension used for the second coordinate.
   * @param third   Dimension used for the third coordinate.
   * @param fourth  Dimension used for the fourth coordinate.
   * @param fifth   Dimension used for the fifth coordinate.
   * @param sixth   Dimension used for the sixth coordinate.
   * @param seventh Dimension used for the seventh coordinate.
   * @param eighth  Dimension used for the eighth coordinate.
   * @param ninth   Dimension used for the ninth coordinate.
   */
  def permute[D <: Dimension, F <: Dimension, G <: Dimension, H <: Dimension, I <: Dimension, J <: Dimension, K <: Dimension, L <: Dimension, M <: Dimension](
    first: D, second: F, third: G, fourth: H, fifth: I, sixth: J, seventh: K, eighth: L, ninth: M)(
      implicit ev1: PosDimDep[Position9D, D], ev2: PosDimDep[Position9D, F], ev3: PosDimDep[Position9D, G],
        ev4: PosDimDep[Position9D, H], ev5: PosDimDep[Position9D, I], ev6: PosDimDep[Position9D, J],
          ev7: PosDimDep[Position9D, K], ev8: PosDimDep[Position9D, L], ev9: PosDimDep[Position9D, M],
            ev10: Distinct9[D, F, G, H, I, J, K, L, M]): U[Cell[Position9D]] = {
    data.map {
      case Cell(p, c) => Cell(p.permute(List(first, second, third, fourth, fifth, sixth, seventh, eighth, ninth)), c)
    }
  }

  /**
   * Persist a `Matrix9D` as sparse matrix file (index, index, index, index, index, index, index, index, index, value).
   *
   * @param file       File to write to.
   * @param dictionary Pattern for the dictionary file name.
   * @param separator  Column separator to use in dictionary file.
   *
   * @return A `TypedPipe[Cell[Position9D]]`; that is it returns `data`.
   */
  def saveAsIV(file: String, dictionary: String = "%1$s.dict.%2$d", separator: String = "|")(implicit flow: FlowDef,
    mode: Mode): U[Cell[Position9D]] = {
    saveAsIVWithNames(file, names(Over(First)), names(Over(Second)), names(Over(Third)), names(Over(Fourth)),
      names(Over(Fifth)), names(Over(Sixth)), names(Over(Seventh)), names(Over(Eighth)), names(Over(Ninth)),
      dictionary, separator)
  }

  /**
   * Persist a `Matrix9D` as sparse matrix file (index, index, index, index, index, index, index, index, index, value).
   *
   * @param file       File to write to.
   * @param namesI     The names to use for the first dimension (according to their ordering).
   * @param namesJ     The names to use for the second dimension (according to their ordering).
   * @param namesK     The names to use for the third dimension (according to their ordering).
   * @param namesL     The names to use for the fourth dimension (according to their ordering).
   * @param namesM     The names to use for the fifth dimension (according to their ordering).
   * @param namesN     The names to use for the sixth dimension (according to their ordering).
   * @param namesO     The names to use for the seventh dimension (according to their ordering).
   * @param namesQ     The names to use for the eighth dimension (according to their ordering).
   * @param namesR     The names to use for the ninth dimension (according to their ordering).
   * @param dictionary Pattern for the dictionary file name.
   * @param separator  Column separator to use in dictionary file.
   *
   * @return A `TypedPipe[Cell[Position9D]]`; that is it returns `data`.
   *
   * @note If `names` contains a subset of the columns, then only those columns get persisted to file.
   */
  def saveAsIVWithNames[I, J, K, L, M, N, O, Q, R](file: String, namesI: I, namesJ: J, namesK: K, namesL: L, namesM: M,
    namesN: N, namesO: O, namesQ: Q, namesR: R, dictionary: String = "%1$s.dict.%2$d", separator: String = "|")(
      implicit ev1: BaseNameable[I, Position9D, Position1D, Dimension.First, TypedPipe],
        ev2: BaseNameable[J, Position9D, Position1D, Dimension.Second, TypedPipe],
          ev3: BaseNameable[K, Position9D, Position1D, Dimension.Third, TypedPipe],
            ev4: BaseNameable[L, Position9D, Position1D, Dimension.Fourth, TypedPipe],
              ev5: BaseNameable[M, Position9D, Position1D, Dimension.Fifth, TypedPipe],
                ev6: BaseNameable[N, Position9D, Position1D, Dimension.Sixth, TypedPipe],
                  ev7: BaseNameable[O, Position9D, Position1D, Dimension.Seventh, TypedPipe],
                    ev8: BaseNameable[Q, Position9D, Position1D, Dimension.Eighth, TypedPipe],
                      ev9: BaseNameable[R, Position9D, Position1D, Dimension.Ninth, TypedPipe], flow: FlowDef,
                        mode: Mode): U[Cell[Position9D]] = {
    data
      .groupBy { case c => Position1D(c.position(First)) }
      .join(saveDictionary(ev1.convert(this, Over(First), namesI), file, dictionary, separator, First))
      .values
      .groupBy { case (c, i) => Position1D(c.position(Second)) }
      .join(saveDictionary(ev2.convert(this, Over(Second), namesJ), file, dictionary, separator, Second))
      .map { case (_, ((c, i), j)) => (c, i, j) }
      .groupBy { case (c, i, j) => Position1D(c.position(Third)) }
      .join(saveDictionary(ev3.convert(this, Over(Third), namesK), file, dictionary, separator, Third))
      .map { case (_, ((c, i, j), k)) => (c, i, j, k) }
      .groupBy { case (c, i, j, k) => Position1D(c.position(Fourth)) }
      .join(saveDictionary(ev4.convert(this, Over(Fourth), namesL), file, dictionary, separator, Fourth))
      .map { case (_, ((c, i, j, k), l)) => (c, i, j, k, l) }
      .groupBy { case (c, i, j, k, l) => Position1D(c.position(Fifth)) }
      .join(saveDictionary(ev5.convert(this, Over(Fifth), namesM), file, dictionary, separator, Fifth))
      .map { case (_, ((c, i, j, k, l), m)) => (c, i, j, k, l, m) }
      .groupBy { case (c, i, j, k, l, m) => Position1D(c.position(Sixth)) }
      .join(saveDictionary(ev6.convert(this, Over(Sixth), namesN), file, dictionary, separator, Sixth))
      .map { case (_, ((c, i, j, k, l, m), n)) => (c, i, j, k, l, m, n) }
      .groupBy { case (c, i, j, k, l, m, n) => Position1D(c.position(Seventh)) }
      .join(saveDictionary(ev7.convert(this, Over(Seventh), namesO), file, dictionary, separator, Seventh))
      .map { case (_, ((c, i, j, k, l, m, n), o)) => (c, i, j, k, l, m, n, o) }
      .groupBy { case (c, i, j, k, l, m, n, o) => Position1D(c.position(Eighth)) }
      .join(saveDictionary(ev8.convert(this, Over(Eighth), namesQ), file, dictionary, separator, Eighth))
      .map { case (_, ((c, i, j, k, l, m, n, o), p)) => (c, i, j, k, l, m, n, o, p) }
      .groupBy { case (c, i, j, k, l, m, n, o, p) => Position1D(c.position(Ninth)) }
      .join(saveDictionary(ev9.convert(this, Over(Ninth), namesR), file, dictionary, separator, Ninth))
      .map {
        case (_, ((c, i, j, k, l, m, n, o, p), q)) =>
          i + separator + j + separator + k + separator + l + separator + m + separator +
            n + separator + o + separator + p + separator + q + separator + c.content.value.toShortString
      }
      .write(TypedSink(TextLine(file)))

    data
  }
}

/** Scalding Companion object for the `Matrixable` type class. */
object Matrixable {
  /** Converts a `TypedPipe[Cell[P]]` into a `TypedPipe[Cell[P]]`; that is, it is a  pass through. */
  implicit def TPC2TPM[P <: Position]: BaseMatrixable[TypedPipe[Cell[P]], P, TypedPipe] = {
    new BaseMatrixable[TypedPipe[Cell[P]], P, TypedPipe] { def convert(t: TypedPipe[Cell[P]]): TypedPipe[Cell[P]] = t }
  }

  /** Converts a `List[Cell[P]]` into a `TypedPipe[Cell[P]]`. */
  implicit def LC2TPM[P <: Position]: BaseMatrixable[List[Cell[P]], P, TypedPipe] = {
    new BaseMatrixable[List[Cell[P]], P, TypedPipe] {
      def convert(t: List[Cell[P]]): TypedPipe[Cell[P]] = new IterablePipe(t)
    }
  }

  /** Converts a `Cell[P]` into a `TypedPipe[Cell[P]]`. */
  implicit def C2TPM[P <: Position]: BaseMatrixable[Cell[P], P, TypedPipe] = {
    new BaseMatrixable[Cell[P], P, TypedPipe] {
      def convert(t: Cell[P]): TypedPipe[Cell[P]] = new IterablePipe(List(t))
    }
  }
}

