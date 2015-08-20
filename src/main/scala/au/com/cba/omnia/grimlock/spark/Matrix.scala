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

package au.com.cba.omnia.grimlock.spark

import au.com.cba.omnia.grimlock.framework.{
  Cell,
  Default,
  ExpandableMatrix => BaseExpandableMatrix,
  ExtractWithDimension,
  ExtractWithKey,
  Locate,
  Matrix => BaseMatrix,
  Matrixable => BaseMatrixable,
  Nameable => BaseNameable,
  NoParameters,
  ReduceableMatrix => BaseReduceableMatrix,
  Reducers,
  Sequence2,
  Tuner,
  TunerParameters,
  Type
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

import au.com.cba.omnia.grimlock.spark.Matrix._
import au.com.cba.omnia.grimlock.spark.Matrixable._
import au.com.cba.omnia.grimlock.spark.Nameable._

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

private[spark] object SparkImplicits {
  implicit class RDDTuner[T](rdd: RDD[T]) {
    def tunedDistinct(parameters: TunerParameters)(implicit ev: Ordering[T]): RDD[T] = {
      parameters match {
        case Reducers(reducers) => rdd.distinct(reducers)(ev)
        case _ => rdd.distinct()
      }
    }
  }

  implicit class PairRDDTuner[K <: Position, V](rdd: RDD[(K, V)])(implicit kt: ClassTag[K], vt: ClassTag[V]) {
    def tunedJoin[W](parameters: TunerParameters, other: RDD[(K, W)]): RDD[(K, (V, W))] = {
      parameters match {
        case Reducers(reducers) => rdd.join(other, reducers)
        case _ => rdd.join(other)
      }
    }

    def tunedLeftJoin[W](parameters: TunerParameters, other: RDD[(K, W)]): RDD[(K, (V, Option[W]))] = {
      parameters match {
        case Reducers(reducers) => rdd.leftOuterJoin(other, reducers)
        case _ => rdd.leftOuterJoin(other)
      }
    }

    def tunedOuterJoin[W](parameters: TunerParameters, other: RDD[(K, W)]): RDD[(K, (Option[V], Option[W]))] = {
      parameters match {
        case Reducers(reducers) => rdd.fullOuterJoin(other, reducers)
        case _ => rdd.fullOuterJoin(other)
      }
    }

    def tunedReduce(parameters: TunerParameters, reduction: (V, V) => V): RDD[(K, V)] = {
      parameters match {
        case Reducers(reducers) => rdd.reduceByKey(reduction, reducers)
        case _ => rdd.reduceByKey(reduction)
      }
    }
  }
}

/** Base trait for matrix operations using a `RDD[Cell[P]]`. */
trait Matrix[P <: Position] extends BaseMatrix[P] with Persist[Cell[P]] {
  type U[A] = RDD[A]
  type E[B] = B
  type S = Matrix[P]

  import SparkImplicits._

  type ChangeTuners = TP2
  def change[D <: Dimension, I, T <: Tuner](slice: Slice[P, D], positions: I, schema: Schema, tuner: T = Default())(
    implicit ev1: PosDimDep[P, D], ev2: PositionDistributable[I, slice.S, RDD], ev3: ClassTag[slice.S],
      ev4: ChangeTuners#V[T]): U[Cell[P]] = {
    data
      .keyBy { case c => slice.selected(c.position) }
      .tunedLeftJoin(tuner.parameters, ev2.convert(positions).keyBy { case p => p })
      .flatMap {
        case (_, (c, po)) => po match {
          case Some(_) => schema.decode(c.content.value.toShortString).map { case con => Cell(c.position, con) }
          case None => Some(c)
        }
      }
  }

  type DomainTuners = TP1

  type GetTuners = TP2
  def get[I, T <: Tuner](positions: I, tuner: T = Default())(implicit ev1: PositionDistributable[I, P, RDD],
    ev2: ClassTag[P], ev3: GetTuners#V[T]): U[Cell[P]] = {
    data
      .keyBy { case c => c.position }
      .tunedJoin(tuner.parameters, ev1.convert(positions).keyBy { case p => p })
      .map { case (_, (c, _)) => c }
  }

  type JoinTuners = TP3
  def join[D <: Dimension, T <: Tuner](slice: Slice[P, D], that: S, tuner: T = Default())(implicit ev1: PosDimDep[P, D],
    ev2: P =!= Position1D, ev3: ClassTag[slice.S], ev4: JoinTuners#V[T]): U[Cell[P]] = {
    val (p1, p2) = tuner.parameters match {
      case Sequence2(f, s) => (f, s)
      case p => (NoParameters, p)
    }

    (data ++ that.data)
      .keyBy { case c => slice.selected(c.position) }
      .tunedJoin(p2, names(slice).map { case p => (p, ()) }.tunedJoin(p1, that.names(slice).map { case p => (p, ()) }))
      .map { case (_, (c, _)) => c }
  }

  type NamesTuners = TP2
  def names[D <: Dimension, T <: Tuner](slice: Slice[P, D], tuner: T = Default())(implicit ev1: PosDimDep[P, D],
    ev2: slice.S =!= Position0D, ev3: ClassTag[slice.S], ev4: NamesTuners#V[T]): U[slice.S] = {
    data.map { case c => slice.selected(c.position) }.tunedDistinct(tuner.parameters)(Position.Ordering[slice.S]())
  }

  type PairwiseTuners = OneOf6[Default[NoParameters.type],
                               Default[Reducers],
                               Default[Sequence2[Reducers, Reducers]],
                               Default[Sequence2[Reducers, Sequence2[Reducers, Reducers]]],
                               Default[Sequence2[Sequence2[Reducers, Reducers], Reducers]],
                               Default[Sequence2[Sequence2[Reducers, Reducers], Sequence2[Reducers, Reducers]]]]
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
      .flatMap { case ((lc, lr), (rc, rr)) => operator.computeWithValue(lc, lr, rc, rr, value) }
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
      .flatMap { case ((lc, lr), (rc, rr)) => operator.computeWithValue(lc, lr, rc, rr, value) }
  }

  def rename(renamer: (Cell[P]) => P): U[Cell[P]] = data.map { case c => Cell(renamer(c), c.content) }

  def renameWithValue[W](renamer: (Cell[P], W) => P, value: E[W]): U[Cell[P]] = {
    data.map { case c => Cell(renamer(c, value), c.content) }
  }

  def sample[F](samplers: F)(implicit ev: Sampleable[F, P]): U[Cell[P]] = {
    val sampler = ev.convert(samplers)

    data.filter { case c => sampler.select(c) }
  }

  def sampleWithValue[F, W](samplers: F, value: E[W])(implicit ev: SampleableWithValue[F, P, W]): U[Cell[P]] = {
    val sampler = ev.convert(samplers)

    data.filter { case c => sampler.selectWithValue(c, value) }
  }

  type SetTuners = TP2
  def set[I, T <: Tuner](positions: I, value: Content, tuner: T = Default())(
    implicit ev1: PositionDistributable[I, P, RDD], ev2: ClassTag[P], ev3: SetTuners#V[T]): U[Cell[P]] = {
    set(ev1.convert(positions).map { case p => Cell(p, value) }, tuner)
  }

  def set[M, T <: Tuner](values: M, tuner: T = Default())(implicit ev1: BaseMatrixable[M, P, RDD], ev2: ClassTag[P],
    ev3: SetTuners#V[T]): U[Cell[P]] = {
    data
      .keyBy { case c => c.position }
      .tunedOuterJoin(tuner.parameters, ev1.convert(values).keyBy { case c => c.position })
      .map { case (_, (co, cn)) => cn.getOrElse(co.get) }
  }

  type ShapeTuners = TP2
  def shape[T <: Tuner](tuner: T = Default())(implicit ev: ShapeTuners#V[T]): U[Cell[Position1D]] = {
    data
      .flatMap { case c => c.position.coordinates.map(_.toString).zipWithIndex }
      .tunedDistinct(tuner.parameters)
      .groupBy { case (s, i) => i }
      .map {
        case (i, s) => Cell(Position1D(Dimension.All(i).toString), Content(DiscreteSchema(LongCodex), s.size))
      }
  }

  type SizeTuners = TP2
  def size[D <: Dimension, T <: Tuner](dim: D, distinct: Boolean, tuner: T = Default())(implicit ev1: PosDimDep[P, D],
    ev2: SizeTuners#V[T]): U[Cell[Position1D]] = {
    val coords = data.map { case c => c.position(dim) }
    val dist = if (distinct) { coords } else { coords.tunedDistinct(tuner.parameters)(Value.Ordering) }

    dist
      .context
      .parallelize(List(Cell(Position1D(dim.toString), Content(DiscreteSchema(LongCodex), dist.count))))
  }

  type SliceTuners = TP2
  def slice[D <: Dimension, I, T <: Tuner](slice: Slice[P, D], positions: I, keep: Boolean, tuner: T = Default())(
    implicit ev1: PosDimDep[P, D], ev2: PositionDistributable[I, slice.S, RDD], ev3: ClassTag[slice.S],
      ev4: SliceTuners#V[T]): U[Cell[P]] = {
    data
      .keyBy { case c => slice.selected(c.position) }
      .tunedLeftJoin(tuner.parameters, ev2.convert(positions).map { case p => (p, ()) })
      .collect { case (_, (c, o)) if (o.isEmpty != keep) => c }
  }

  type SlideTuners = TP1
  def slide[D <: Dimension, Q <: Position, F, T <: Tuner](slice: Slice[P, D], windows: F, ascending: Boolean,
    tuner: T = Default())(implicit ev1: PosDimDep[P, D], ev2: Windowable[F, slice.S, slice.R, Q],
      ev3: slice.R =!= Position0D, ev4: ClassTag[slice.S], ev5: ClassTag[slice.R],
        ev6: SlideTuners#V[T]): U[Cell[Q]] = {
    val window = ev2.convert(windows)

    data
      .map { case Cell(p, c) => (Cell(slice.selected(p), c), slice.remainder(p)) }
      .groupBy { case (c, r) => c.position }
      .flatMap {
        case (_, itr) => itr
          .toList
          .sortBy { case (c, r) => r }(Position.Ordering(ascending))
          .scanLeft(Option.empty[(window.T, TraversableOnce[Cell[Q]])]) {
            case (None, (c, r)) => Some(window.initialise(c, r))
            case (Some((t, _)), (c, r)) => Some(window.present(c, r, t))
          }
          .flatMap {
            case Some((t, c)) => c
            case _ => None
          }
      }
  }

  def slideWithValue[D <: Dimension, Q <: Position, F, W, T <: Tuner](slice: Slice[P, D], windows: F, value: E[W],
    ascending: Boolean, tuner: T = Default())(implicit ev1: PosDimDep[P, D],
      ev2: WindowableWithValue[F, slice.S, slice.R, Q, W], ev3: slice.R =!= Position0D, ev4: ClassTag[slice.S],
        ev5: ClassTag[slice.R], ev6: SlideTuners#V[T]): U[Cell[Q]] = {
    val window = ev2.convert(windows)

    data
      .map { case Cell(p, c) => (Cell(slice.selected(p), c), slice.remainder(p)) }
      .groupBy { case (c, r) => c.position }
      .flatMap {
        case (_, itr) => itr
          .toList
          .sortBy { case (c, r) => r }(Position.Ordering(ascending))
          .scanLeft(Option.empty[(window.T, TraversableOnce[Cell[Q]])]) {
            case (None, (c, r)) => Some(window.initialiseWithValue(c, r, value))
            case (Some((t, _)), (c, r)) => Some(window.presentWithValue(c, r, value, t))
          }
          .flatMap {
            case Some((t, c)) => c
            case _ => None
          }
      }
  }

  def split[Q, F](partitioners: F)(implicit ev: Partitionable[F, P, Q]): U[(Q, Cell[P])] = {
    val partitioner = ev.convert(partitioners)

    data.flatMap { case c => partitioner.assign(c).map { case q => (q, c) } }
  }

  def splitWithValue[Q, F, W](partitioners: F, value: E[W])(
    implicit ev: PartitionableWithValue[F, P, Q, W]): U[(Q, Cell[P])] = {
    val partitioner = ev.convert(partitioners)

    data.flatMap { case c => partitioner.assignWithValue(c, value).map { case q => (q, c) } }
  }

  def stream[Q <: Position](command: String, script: String, separator: String,
    parser: String => TraversableOnce[Cell[Q]]): U[Cell[Q]] = {
    data
      .map(_.toString(separator, false))
      .pipe(command + " " + script)
      .flatMap(parser(_))
  }

  type SummariseTuners = TP2
  def summarise[D <: Dimension, Q <: Position, F, T <: Tuner](slice: Slice[P, D], aggregators: F,
    tuner: T = Default())(implicit ev1: PosDimDep[P, D], ev2: Aggregatable[F, P, slice.S, Q], ev3: ClassTag[slice.S],
      ev4: SummariseTuners#V[T]): U[Cell[Q]] = {
    val aggregator = ev2.convert(aggregators)

    data
      .map { case c => (slice.selected(c.position), aggregator.map { case a => a.prepare(c) }) }
      .tunedReduce(tuner.parameters, (lt: List[Any], rt: List[Any]) =>
        (aggregator, lt, rt).zipped.map { case (a, l, r) => a.reduce(l.asInstanceOf[a.T], r.asInstanceOf[a.T]) })
      .flatMap {
        case (p, t) => (aggregator, t).zipped.flatMap { case (a, s) => a.present(p, s.asInstanceOf[a.T]) }
      }
  }

  def summariseWithValue[D <: Dimension, Q <: Position, F, W, T <: Tuner](slice: Slice[P, D], aggregators: F,
    value: E[W], tuner: T = Default())(implicit ev1: PosDimDep[P, D], ev2: AggregatableWithValue[F, P, slice.S, Q, W],
      ev3: ClassTag[slice.S], ev4: SummariseTuners#V[T]): U[Cell[Q]] = {
    val aggregator = ev2.convert(aggregators)

    data
      .map { case c => (slice.selected(c.position), aggregator.map { case a => a.prepareWithValue(c, value) }) }
      .tunedReduce(tuner.parameters, (lt: List[Any], rt: List[Any]) =>
        (aggregator, lt, rt).zipped.map { case (a, l, r) => a.reduce(l.asInstanceOf[a.T], r.asInstanceOf[a.T]) })
      .flatMap {
        case (p, t) => (aggregator, t).zipped.flatMap {
          case (a, s) => a.presentWithValue(p, s.asInstanceOf[a.T], value)
        }
      }
  }

  type ToMapTuners = TP2
  def toMap()(implicit ev: ClassTag[P]): E[Map[P, Content]] = {
    data
      .map { case c => (c.position, c.content) }
      .collectAsMap
      .toMap
  }

  def toMap[D <: Dimension, T <: Tuner](slice: Slice[P, D], tuner: T = Default())(implicit ev1: PosDimDep[P, D],
    ev2: slice.S =!= Position0D, ev3: ClassTag[slice.S], ev4: ToMapTuners#V[T]): E[Map[slice.S, slice.C]] = {
    data
      .map { case c => (c.position, slice.toMap(c)) }
      .keyBy { case (p, m) => slice.selected(p) }
      .tunedReduce(tuner.parameters, (l: (P, Map[slice.S, slice.C]), r: (P, Map[slice.S, slice.C])) =>
        (l._1, slice.combineMaps(l._1, l._2, r._2)))
      .map { case (_, (_, m)) => m }
      .reduce { case (lm, rm) => lm ++ rm }
  }

  def transform[Q <: Position, F](transformers: F)(implicit ev: Transformable[F, P, Q]): U[Cell[Q]] = {
    val transformer = ev.convert(transformers)

    data.flatMap { case c => transformer.present(c) }
  }

  def transformWithValue[Q <: Position, F, W](transformers: F, value: E[W])(
    implicit ev: TransformableWithValue[F, P, Q, W]): U[Cell[Q]] = {
    val transformer = ev.convert(transformers)

    data.flatMap { case c => transformer.presentWithValue(c, value) }
  }

  type TypesTuners = TP2
  def types[D <: Dimension, T <: Tuner](slice: Slice[P, D], specific: Boolean, tuner: T = Default())(
    implicit ev1: PosDimDep[P, D], ev2: slice.S =!= Position0D, ev3: ClassTag[slice.S],
      ev4: TypesTuners#V[T]): U[(slice.S, Type)] = {
    data
      .map { case Cell(p, c) => (slice.selected(p), c.schema.kind) }
      .tunedReduce(tuner.parameters, (lt: Type, rt: Type) => Type.getCommonType(lt, rt))
      .map { case (p, t) => (p, if (specific) t else t.getGeneralisation()) }
  }

  type UniqueTuners = TP2
  def unique[T <: Tuner](tuner: T = Default())(implicit ev: UniqueTuners#V[T]): U[Content] = {
    val ordering = new Ordering[Content] { def compare(l: Content, r: Content) = l.toString.compare(r.toString) }

    data
      .map { case c => c.content }
      .tunedDistinct(tuner.parameters)(ordering)
  }

  def unique[D <: Dimension, T <: Tuner](slice: Slice[P, D], tuner: T = Default())(
    implicit ev1: slice.S =!= Position0D, ev2: UniqueTuners#V[T]): U[(slice.S, Content)] = {
    val ordering = new Ordering[Cell[slice.S]] {
      def compare(l: Cell[slice.S], r: Cell[slice.S]) = l.toString().compare(r.toString)
    }

    data
      .map { case Cell(p, c) => Cell(slice.selected(p), c) }
      .tunedDistinct(tuner.parameters)(ordering)
      .map { case Cell(p, c) => (p, c) }
  }

  type WhichTuners = TP2
  def which(predicate: Predicate)(implicit ev: ClassTag[P]): U[P] = {
    data.collect { case c if predicate(c) => c.position }
  }

  def which[D <: Dimension, I, T <: Tuner](slice: Slice[P, D], positions: I, predicate: Predicate,
    tuner: T = Default())(implicit ev1: PosDimDep[P, D], ev2: PositionDistributable[I, slice.S, RDD],
      ev3: ClassTag[slice.S], ev4: ClassTag[P], ev5: WhichTuners#V[T]): U[P] = {
    which(slice, List((positions, predicate)), tuner)
  }

  def which[D <: Dimension, I, T <: Tuner](slice: Slice[P, D], pospred: List[(I, Predicate)], tuner: T = Default())(
    implicit ev1: PosDimDep[P, D], ev2: PositionDistributable[I, slice.S, RDD], ev3: ClassTag[slice.S],
      ev4: ClassTag[P], ev5: WhichTuners#V[T]): U[P] = {
    val pp = pospred
      .map { case (pos, pred) => ev2.convert(pos).map { case p => (p, pred) } }
      .reduce((l, r) => l ++ r)

    data
      .keyBy { case c => slice.selected(c.position) }
      .tunedJoin(tuner.parameters, pp.keyBy { case (p, pred) => p })
      .collect { case (_, (c, (_, predicate))) if predicate(c) => c.position }
  }

  val data: U[Cell[P]]

  protected def saveDictionary(names: U[(Position1D, Long)], file: String, dictionary: String, separator: String) = {
    names
      .map { case (p, i) => p.toShortString(separator) + separator + i }
      .saveAsTextFile(dictionary.format(file))

    names
  }

  protected def saveDictionary(names: U[(Position1D, Long)], file: String, dictionary: String, separator: String,
    dim: Dimension) = {
    names
      .map { case (p, i) => p.toShortString(separator) + separator + i }
      .saveAsTextFile(dictionary.format(file, dim.index))

    names
  }

  private def pairwiseTuples[D <: Dimension, T <: Tuner](slice: Slice[P, D], comparer: Comparer, tuner: T)(
    ldata: U[Cell[P]], rdata: U[Cell[P]])(implicit ev1: PosDimDep[P, D], ev2: ClassTag[slice.S],
      ev3: ClassTag[slice.R]): U[((Cell[slice.S], slice.R), (Cell[slice.S], slice.R))] = {
    val toTuple = (c: Cell[P]) => (Cell(slice.selected(c.position), c.content), slice.remainder(c.position))
    val (rr, rj, lr, lj) = tuner.parameters match {
      case Sequence2(Sequence2(rr, rj), Sequence2(lr, lj)) => (rr, rj, lr, lj)
      case Sequence2(rj @ Reducers(_), Sequence2(lr, lj)) => (NoParameters, rj, lr, lj)
      case Sequence2(Sequence2(rr, rj), lj @ Reducers(_)) => (rr, rj, NoParameters, lj)
      case Sequence2(rj @ Reducers(_), lj @ Reducers(_)) => (NoParameters, rj, NoParameters, lj)
      case lj @ Reducers(_) => (NoParameters, NoParameters, NoParameters, lj)
      case _ => (NoParameters, NoParameters, NoParameters, NoParameters)
    }
    val ordering = Position.Ordering[slice.S]()

    ldata.map { case c => slice.selected(c.position) }.tunedDistinct(lr)(ordering)
      .cartesian(rdata.map { case c => slice.selected(c.position) }.tunedDistinct(rr)(ordering))
      .collect { case (l, r) if comparer.keep(l, r) => (l, r) }
      .keyBy { case (l, _) => l }
      .tunedJoin(lj, ldata.keyBy { case Cell(p, _) => slice.selected(p) })
      .keyBy { case (_, ((_, r),  _)) => r }
      .tunedJoin(rj, rdata.keyBy { case Cell(p, _) => slice.selected(p) })
      .map { case (_, ((_, (_, lc)), rc)) => (toTuple(lc), toTuple(rc)) }
  }
}

/** Base trait for methods that reduce the number of dimensions or that can be filled using a `RDD[Cell[P]]`. */
trait ReduceableMatrix[P <: Position with ReduceablePosition] extends BaseReduceableMatrix[P] { self: Matrix[P] =>

  import SparkImplicits._

  type FillHeterogeneousTuners = TP3
  def fill[D <: Dimension, Q <: Position, T <: Tuner](slice: Slice[P, D], values: U[Cell[Q]], tuner: T = Default())(
    implicit ev1: PosDimDep[P, D], ev2: ClassTag[P], ev3: ClassTag[slice.S], ev4: slice.S =:= Q,
      ev5: FillHeterogeneousTuners#V[T]): U[Cell[P]] = {
    val (p1, p2) = tuner.parameters match {
      case Sequence2(f, s) => (f, s)
      case p => (NoParameters, p)
    }

    domain(Default())
      .keyBy { case p => slice.selected(p) }
      .tunedJoin(p1, values.keyBy { case c => c.position.asInstanceOf[slice.S] })
      .map { case (_, (p, c)) => (p, Cell(p, c.content)) }
      .tunedLeftJoin(p2, data.keyBy { case c => c.position })
      .map { case (_, (c, co)) => co.getOrElse(c) }
  }

  type FillHomogeneousTuners = TP2
  def fill[T <: Tuner](value: Content, tuner: T = Default())(implicit ev1: ClassTag[P],
    ev2: FillHomogeneousTuners#V[T]): U[Cell[P]] = {
    domain(Default())
      .keyBy { case p => p }
      .tunedLeftJoin(tuner.parameters, data.keyBy { case c => c.position })
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
      .keyBy { case c => c.position.remove(dim) }
      .tunedReduce(tuner.parameters, (x: Cell[P], y: Cell[P]) => squash.reduce(dim, x, y))
      .map { case (p, c) => Cell(p, c.content) }
  }

  def squashWithValue[D <: Dimension, F, W, T <: Tuner](dim: D, squasher: F, value: E[W], tuner: T = Default())(
    implicit ev1: PosDimDep[P, D], ev2: SquashableWithValue[F, P, W], ev3: SquashTuners#V[T]): U[Cell[P#L]] = {
    val squash = ev2.convert(squasher)

    data
      .keyBy { case c => c.position.remove(dim) }
      .tunedReduce(tuner.parameters, (x: Cell[P], y: Cell[P]) => squash.reduceWithValue(dim, x, y, value))
      .map { case (p, c) => Cell(p, c.content) }
  }
}

/** Base trait for methods that expand the number of dimension of a matrix using a `RDD[Cell[P]]`. */
trait ExpandableMatrix[P <: Position with ExpandablePosition] extends BaseExpandableMatrix[P] { self: Matrix[P] =>

  def expand[Q <: Position](expander: Cell[P] => Q)(implicit ev: PosExpDep[P, Q]): RDD[Cell[Q]] = {
    data.map { case c => Cell(expander(c), c.content) }
  }

  def expandWithValue[Q <: Position, W](expander: (Cell[P], W) => Q, value: W)(
    implicit ev: PosExpDep[P, Q]): RDD[Cell[Q]] = data.map { case c => Cell(expander(c, value), c.content) }
}

// TODO: Make this work on more than 2D matrices and share with Scalding
trait MatrixDistance { self: Matrix[Position2D] with ReduceableMatrix[Position2D] =>

  import au.com.cba.omnia.grimlock.library.aggregate._
  import au.com.cba.omnia.grimlock.library.pairwise._
  import au.com.cba.omnia.grimlock.library.transform._
  import au.com.cba.omnia.grimlock.library.window._

  /**
   * Compute correlations.
   *
   * @param slice  Encapsulates the dimension for which to compute correlations.
   * @param stuner The summarise tuner for the job.
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
        Map(Position1D("one") -> 1.0))
  }
}

object Matrix {
  /**
   * Read column oriented, pipe separated matrix text data into a `RDD[Cell[P]]`.
   *
   * @param file   The text file to read from.
   * @param parser The parser that converts a single line to a cell.
   */
  def loadText[P <: Position](file: String, parser: (String) => TraversableOnce[Cell[P]])(
    implicit sc: SparkContext): RDD[Cell[P]] = sc.textFile(file).flatMap { parser(_) }

  /** Conversion from `RDD[Cell[Position1D]]` to a Spark `Matrix1D`. */
  implicit def RDD2M1(data: RDD[Cell[Position1D]]): Matrix1D = new Matrix1D(data)
  /** Conversion from `RDD[Cell[Position2D]]` to a Spark `Matrix2D`. */
  implicit def RDD2M2(data: RDD[Cell[Position2D]]): Matrix2D = new Matrix2D(data)
  /** Conversion from `RDD[Cell[Position3D]]` to a Spark `Matrix3D`. */
  implicit def RDD2M3(data: RDD[Cell[Position3D]]): Matrix3D = new Matrix3D(data)
  /** Conversion from `RDD[Cell[Position4D]]` to a Spark `Matrix4D`. */
  implicit def RDD2M4(data: RDD[Cell[Position4D]]): Matrix4D = new Matrix4D(data)
  /** Conversion from `RDD[Cell[Position5D]]` to a Spark `Matrix5D`. */
  implicit def RDD2M5(data: RDD[Cell[Position5D]]): Matrix5D = new Matrix5D(data)
  /** Conversion from `RDD[Cell[Position6D]]` to a Spark `Matrix6D`. */
  implicit def RDD2M6(data: RDD[Cell[Position6D]]): Matrix6D = new Matrix6D(data)
  /** Conversion from `RDD[Cell[Position7D]]` to a Spark `Matrix7D`. */
  implicit def RDD2M7(data: RDD[Cell[Position7D]]): Matrix7D = new Matrix7D(data)
  /** Conversion from `RDD[Cell[Position8D]]` to a Spark `Matrix8D`. */
  implicit def RDD2M8(data: RDD[Cell[Position8D]]): Matrix8D = new Matrix8D(data)
  /** Conversion from `RDD[Cell[Position9D]]` to a Spark `Matrix9D`. */
  implicit def RDD2M9(data: RDD[Cell[Position9D]]): Matrix9D = new Matrix9D(data)

  /** Conversion from `List[(Valueable, Content)]` to a Spark `Matrix1D`. */
  implicit def LV1C2M1[V: Valueable](list: List[(V, Content)])(implicit sc: SparkContext): Matrix1D = {
    new Matrix1D(sc.parallelize(list.map { case (v, c) => Cell(Position1D(v), c) }))
  }
  /** Conversion from `List[(Valueable, Valueable, Content)]` to a Spark `Matrix2D`. */
  implicit def LV2C2M2[V: Valueable, W: Valueable](list: List[(V, W, Content)])(
    implicit sc: SparkContext): Matrix2D = {
    new Matrix2D(sc.parallelize(list.map { case (v, w, c) => Cell(Position2D(v, w), c) }))
  }
  /** Conversion from `List[(Valueable, Valueable, Valueable, Content)]` to a Spark `Matrix3D`. */
  implicit def LV3C2M3[V: Valueable, W: Valueable, X: Valueable](
    list: List[(V, W, X, Content)])(implicit sc: SparkContext): Matrix3D = {
    new Matrix3D(sc.parallelize(list.map { case (v, w, x, c) => Cell(Position3D(v, w, x), c) }))
  }
  /** Conversion from `List[(Valueable, Valueable, Valueable, Valueable, Content)]` to a Spark `Matrix4D`. */
  implicit def LV4C2M4[V: Valueable, W: Valueable, X: Valueable, Y: Valueable](
    list: List[(V, W, X, Y, Content)])(implicit sc: SparkContext): Matrix4D = {
    new Matrix4D(sc.parallelize(list.map { case (v, w, x, y, c) => Cell(Position4D(v, w, x, y), c) }))
  }
  /**
   * Conversion from `List[(Valueable, Valueable, Valueable, Valueable, Valueable, Content)]` to a Spark `Matrix5D`.
   */
  implicit def LV5C2M5[V: Valueable, W: Valueable, X: Valueable, Y: Valueable, Z: Valueable](
    list: List[(V, W, X, Y, Z, Content)])(implicit sc: SparkContext): Matrix5D = {
    new Matrix5D(sc.parallelize(list.map { case (v, w, x, y, z, c) => Cell(Position5D(v, w, x, y, z), c) }))
  }
  /**
   * Conversion from `List[(Valueable, Valueable, Valueable, Valueable, Valueable, Valueable, Content)]` to a
   * Spark `Matrix6D`.
   */
  implicit def LV6C2M6[U: Valueable, V: Valueable, W: Valueable, X: Valueable, Y: Valueable, Z: Valueable](
    list: List[(U, V, W, X, Y, Z, Content)])(implicit sc: SparkContext): Matrix6D = {
    new Matrix6D(sc.parallelize(list.map { case (u, v, w, x, y, z, c) => Cell(Position6D(u, v, w, x, y, z), c) }))
  }
  /**
   * Conversion from `List[(Valueable, Valueable, Valueable, Valueable, Valueable, Valueable, Valueable, Content)]` to a
   * Spark `Matrix7D`.
   */
  implicit def LV7C2M7[T: Valueable, U: Valueable, V: Valueable, W: Valueable, X: Valueable, Y: Valueable, Z: Valueable](
    list: List[(T, U, V, W, X, Y, Z, Content)])(implicit sc: SparkContext): Matrix7D = {
    new Matrix7D(sc.parallelize(list.map { case (t, u, v, w, x, y, z, c) => Cell(Position7D(t, u, v, w, x, y, z), c) }))
  }
  /**
   * Conversion from `List[(Valueable, Valueable, Valueable, Valueable, Valueable, Valueable, Valueable, Valueable,
   * Content)]` to a Spark `Matrix8D`.
   */
  implicit def LV8C2M8[S: Valueable, T: Valueable, U: Valueable, V: Valueable, W: Valueable, X: Valueable, Y: Valueable, Z: Valueable](
    list: List[(S, T, U, V, W, X, Y, Z, Content)])(implicit sc: SparkContext): Matrix8D = {
    new Matrix8D(sc.parallelize(list.map {
      case (s, t, u, v, w, x, y, z, c) => Cell(Position8D(s, t, u, v, w, x, y, z), c)
    }))
  }
  /**
   * Conversion from `List[(Valueable, Valueable, Valueable, Valueable, Valueable, Valueable, Valueable, Valueable,
   * Valueable, Content)]` to a Spark `Matrix9D`.
   */
  implicit def LV9C2M9[R: Valueable, S: Valueable, T: Valueable, U: Valueable, V: Valueable, W: Valueable, X: Valueable, Y: Valueable, Z: Valueable](
    list: List[(R, S, T, U, V, W, X, Y, Z, Content)])(implicit sc: SparkContext): Matrix9D = {
    new Matrix9D(sc.parallelize(list.map {
      case (r, s, t, u, v, w, x, y, z, c) => Cell(Position9D(r, s, t, u, v, w, x, y, z), c)
    }))
  }
}

/**
 * Rich wrapper around a `RDD[Cell[Position1D]]`.
 *
 * @param data `RDD[Cell[Position1D]]`.
 */
class Matrix1D(val data: RDD[Cell[Position1D]]) extends Matrix[Position1D] with ExpandableMatrix[Position1D] {
  def domain[T <: Tuner](tuner: T = Default())(implicit ev: DomainTuners#V[T]): U[Position1D] = names(Over(First))

  /**
   * Persist a `Matrix1D` as sparse matrix file (index, value).
   *
   * @param file       File to write to.
   * @param dictionary Pattern for the dictionary file name.
   * @param separator  Column separator to use in dictionary file.
   *
   * @return A `RDD[Cell[Position1D]]`; that is it returns `data`.
   */
  def saveAsIV(file: String, dictionary: String = "%1$s.dict.%2$d", separator: String = "|"): U[Cell[Position1D]] = {
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
   * @return A `RDD[Cell[Position1D]]`; that is it returns `data`.
   *
   * @note If `names` contains a subset of the columns, then only those columns get persisted to file.
   */
  def saveAsIVWithNames[I](file: String, names: I, dictionary: String = "%1$s.dict.%2$d", separator: String = "|")(
    implicit ev: BaseNameable[I, Position1D, Position1D, Dimension.First, RDD]): U[Cell[Position1D]] = {
    data
      .keyBy { case c => c.position }
      .join(saveDictionary(ev.convert(this, Over(First), names), file, dictionary, separator, First))
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
  with ExpandableMatrix[Position2D] with MatrixDistance {
  def domain[T <: Tuner](tuner: T = Default())(implicit ev: DomainTuners#V[T]): U[Position2D] = {
    names(Over(First))
      .map { case Position1D(c) => c }
      .cartesian(names(Over(Second)).map { case Position1D(c) => c })
      .map { case (c1, c2) => Position2D(c1, c2) }
  }

  /**
   * Permute the order of the coordinates in a position.
   *
   * @param first  Dimension used for the first coordinate.
   * @param second Dimension used for the second coordinate.
   */
  def permute[D <: Dimension, F <: Dimension](first: D, second: F)(implicit ev1: PosDimDep[Position2D, D],
    ev2: PosDimDep[Position2D, F], ne: D =!= F): U[Cell[Position2D]] = {
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
  def saveAsCSV[D <: Dimension](slice: Slice[Position2D, D], file: String, separator: String = "|",
    escapee: Escape = Quote(), writeHeader: Boolean = true, header: String = "%s.header", writeRowId: Boolean = true,
      rowId: String = "id")(implicit ev2: PosDimDep[Position2D, D], ev3: ClassTag[slice.S]): U[Cell[Position2D]] = {
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
   * @return A `RDD[Cell[Position2D]]`; that is it returns `data`.
   *
   * @note If `names` contains a subset of the columns, then only those columns get persisted to file.
   */
  def saveAsCSVWithNames[D <: Dimension, I](slice: Slice[Position2D, D], file: String, names: I,
    separator: String = "|", escapee: Escape = Quote(), writeHeader: Boolean = true, header: String = "%s.header",
      writeRowId: Boolean = true, rowId: String = "id")(implicit ev1: BaseNameable[I, Position2D, slice.S, D, RDD],
        ev2: PosDimDep[Position2D, D], ev3: ClassTag[slice.S]): U[Cell[Position2D]] = {
    // Note: Usage of .toShortString should be safe as data is written as string anyways. It does assume that all
    //       indices have unique short string representations.
    val columns = ev1.convert(this, slice, names)
      .map { case (p, i) => (0, List((escapee.escape(p.toShortString(""), separator), i))) }
      .reduceByKey(_ ++ _)
      .map { case (_, l) => l.sortBy(_._2).map(_._1) }

    if (writeHeader) {
      columns
        .map {
          case lst => (if (writeRowId) escapee.escape(rowId, separator) + separator else "") + lst.mkString(separator)
        }
        .saveAsTextFile(header.format(file))
    }

    val cols = columns
      .first

    data
      .map {
        case Cell(p, c) => (slice.remainder(p).toShortString(""),
          Map(escapee.escape(slice.selected(p).toShortString(""), separator) ->
            escapee.escape(c.value.toShortString, separator)))
      }
      .reduceByKey(_ ++ _)
      .map {
        case (key, values) => (if (writeRowId) escapee.escape(key, separator) + separator else "") +
          cols.map { case c => values.getOrElse(c, "") }.mkString(separator)
      }
      .saveAsTextFile(file)

    data
  }

  /**
   * Persist a `Matrix2D` as sparse matrix file (index, index, value).
   *
   * @param file       File to write to.
   * @param dictionary Pattern for the dictionary file name.
   * @param separator  Column separator to use in dictionary file.
   *
   * @return A `RDD[Cell[Position2D]]`; that is it returns `data`.
   */
  def saveAsIV(file: String, dictionary: String = "%1$s.dict.%2$d", separator: String = "|"): U[Cell[Position2D]] = {
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
   * @return A `RDD[Cell[Position2D]]`; that is it returns `data`.
   *
   * @note If `names` contains a subset of the columns, then only those columns get persisted to file.
   */
  def saveAsIVWithNames[I, J](file: String, namesI: I, namesJ: J, dictionary: String = "%1$s.dict.%2$d",
    separator: String = "|")(implicit ev1: BaseNameable[I, Position2D, Position1D, Dimension.First, RDD],
      ev2: BaseNameable[J, Position2D, Position1D, Dimension.Second, RDD]) : U[Cell[Position2D]] = {
    data
      .keyBy { case c => Position1D(c.position(First)) }
      .join(saveDictionary(ev1.convert(this, Over(First), namesI), file, dictionary, separator, First))
      .values
      .keyBy { case (c, i) => Position1D(c.position(Second)) }
      .join(saveDictionary(ev2.convert(this, Over(Second), namesJ), file, dictionary, separator, Second))
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
  def saveAsLDA[D <: Dimension](slice: Slice[Position2D, D], file: String, dictionary: String = "%s.dict",
    separator: String = "|", addId: Boolean = false)(implicit ev1: PosDimDep[Position2D, D],
      ev2: ClassTag[slice.S]): U[Cell[Position2D]] = {
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
   * @return A `RDD[Cell[Position2D]]`; that is it returns `data`.
   *
   * @note If `names` contains a subset of the columns, then only those columns get persisted to file.
   */
  def saveAsLDAWithNames[D <: Dimension, I](slice: Slice[Position2D, D], file: String, names: I,
    dictionary: String = "%s.dict", separator: String = "|", addId: Boolean = false)(
      implicit ev1: PosDimDep[Position2D, D], ev2: ClassTag[slice.S],
        ev3: BaseNameable[I, Position2D, Position1D, D, RDD]): U[Cell[Position2D]] = {
    data
      .keyBy { case c => slice.remainder(c.position).asInstanceOf[Position1D] }
      .join(saveDictionary(ev3.convert(this, Along(slice.dimension), names), file, dictionary, separator))
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
  def saveAsVW[D <: Dimension](slice: Slice[Position2D, D], labels: U[Cell[Position1D]], file: String,
    dictionary: String = "%s.dict", separator: String = ":")(
      implicit ev: PosDimDep[Position2D, D]): U[Cell[Position2D]] = {
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
   * @return A `RDD[Cell[Position2D]]`; that is it returns `data`.
   *
   * @note If `names` contains a subset of the columns, then only those columns get persisted to file.
   */
  def saveAsVWWithNames[D <: Dimension, I](slice: Slice[Position2D, D], labels: U[Cell[Position1D]], file: String,
    names: I, dictionary: String = "%s.dict", separator: String = ":")(implicit ev1: PosDimDep[Position2D, D],
      ev2: BaseNameable[I, Position2D, Position1D, D, RDD]): U[Cell[Position2D]] = {
    data
      .keyBy { case c => slice.remainder(c.position).asInstanceOf[Position1D] }
      .join(saveDictionary(ev2.convert(this, Along(slice.dimension), names), file, dictionary, separator))
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
  def domain[T <: Tuner](tuner: T = Default())(implicit ev: DomainTuners#V[T]): U[Position3D] = {
    names(Over(First))
      .map { case Position1D(c) => c }
      .cartesian(names(Over(Second)).map { case Position1D(c) => c })
      .cartesian(names(Over(Third)).map { case Position1D(c) => c })
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
   * @return A `RDD[Cell[Position3D]]`; that is it returns `data`.
   */
  def saveAsIV(file: String, dictionary: String = "%1$s.dict.%2$d", separator: String = "|"): U[Cell[Position3D]] = {
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
   * @return A `RDD[Cell[Position3D]]`; that is it returns `data`.
   *
   * @note If `names` contains a subset of the columns, then only those columns get persisted to file.
   */
  def saveAsIVWithNames[I, J, K](file: String, namesI: I, namesJ: J, namesK: K, dictionary: String = "%1$s.dict.%2$d",
    separator: String = "|")(implicit ev1: BaseNameable[I, Position3D, Position1D, Dimension.First, RDD],
      ev2: BaseNameable[J, Position3D, Position1D, Dimension.Second, RDD],
        ev3: BaseNameable[K, Position3D, Position1D, Dimension.Third, RDD]): U[Cell[Position3D]] = {
    data
      .keyBy { case c => Position1D(c.position(First)) }
      .join(saveDictionary(ev1.convert(this, Over(First), namesI), file, dictionary, separator, First))
      .values
      .keyBy { case (c, i) => Position1D(c.position(Second)) }
      .join(saveDictionary(ev2.convert(this, Over(Second), namesJ), file, dictionary, separator, Second))
      .map { case (_, ((c, i), j)) => (c, i, j) }
      .keyBy { case (c, i, j) => Position1D(c.position(Third)) }
      .join(saveDictionary(ev3.convert(this, Over(Third), namesK), file, dictionary, separator, Third))
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
  def domain[T <: Tuner](tuner: T = Default())(implicit ev: DomainTuners#V[T]): U[Position4D] = {
    names(Over(First))
      .map { case Position1D(c) => c }
      .cartesian(names(Over(Second)).map { case Position1D(c) => c })
      .cartesian(names(Over(Third)).map { case Position1D(c) => c })
      .cartesian(names(Over(Fourth)).map { case Position1D(c) => c })
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
   * @return A `RDD[Cell[Position4D]]`; that is it returns `data`.
   */
  def saveAsIV(file: String, dictionary: String = "%1$s.dict.%2$d", separator: String = "|"): U[Cell[Position4D]] = {
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
   * @return A `RDD[Cell[Position4D]]`; that is it returns `data`.
   *
   * @note If `names` contains a subset of the columns, then only those columns get persisted to file.
   */
  def saveAsIVWithNames[I, J, K, L](file: String, namesI: I, namesJ: J, namesK: K, namesL: L,
    dictionary: String = "%1$s.dict.%2$d", separator: String = "|")(
      implicit ev1: BaseNameable[I, Position4D, Position1D, Dimension.First, RDD],
        ev2: BaseNameable[J, Position4D, Position1D, Dimension.Second, RDD],
          ev3: BaseNameable[K, Position4D, Position1D, Dimension.Third, RDD],
            ev4: BaseNameable[L, Position4D, Position1D, Dimension.Fourth, RDD]): U[Cell[Position4D]] = {
    data
      .keyBy { case c => Position1D(c.position(First)) }
      .join(saveDictionary(ev1.convert(this, Over(First), namesI), file, dictionary, separator, First))
      .values
      .keyBy { case (c, i) => Position1D(c.position(Second)) }
      .join(saveDictionary(ev2.convert(this, Over(Second), namesJ), file, dictionary, separator, Second))
      .map { case (_, ((c, i), j)) => (c, i, j) }
      .keyBy { case (c, i, j) => Position1D(c.position(Third)) }
      .join(saveDictionary(ev3.convert(this, Over(Third), namesK), file, dictionary, separator, Third))
      .map { case (_, ((c, i, j), k)) => (c, i, j, k) }
      .keyBy { case (c, i, j, p) => Position1D(c.position(Fourth)) }
      .join(saveDictionary(ev4.convert(this, Over(Fourth), namesL), file, dictionary, separator, Fourth))
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
class Matrix5D(val data: RDD[Cell[Position5D]]) extends Matrix[Position5D] with ReduceableMatrix[Position5D]
  with ExpandableMatrix[Position5D] {
  def domain[T <: Tuner](tuner: T = Default())(implicit ev: DomainTuners#V[T]): U[Position5D] = {
    names(Over(First))
      .map { case Position1D(c) => c }
      .cartesian(names(Over(Second)).map { case Position1D(c) => c })
      .cartesian(names(Over(Third)).map { case Position1D(c) => c })
      .cartesian(names(Over(Fourth)).map { case Position1D(c) => c })
      .cartesian(names(Over(Fifth)).map { case Position1D(c) => c })
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
   * @return A `RDD[Cell[Position5D]]`; that is it returns `data`.
   */
  def saveAsIV(file: String, dictionary: String = "%1$s.dict.%2$d", separator: String = "|"): U[Cell[Position5D]] = {
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
   * @return A `RDD[Cell[Position5D]]`; that is it returns `data`.
   *
   * @note If `names` contains a subset of the columns, then only those columns get persisted to file.
   */
  def saveAsIVWithNames[I, J, K, L, M](file: String, namesI: I, namesJ: J, namesK: K, namesL: L, namesM: M,
    dictionary: String = "%1$s.dict.%2$d", separator: String = "|")(
      implicit ev1: BaseNameable[I, Position5D, Position1D, Dimension.First, RDD],
        ev2: BaseNameable[J, Position5D, Position1D, Dimension.Second, RDD],
          ev3: BaseNameable[K, Position5D, Position1D, Dimension.Third, RDD],
            ev4: BaseNameable[L, Position5D, Position1D, Dimension.Fourth, RDD],
              ev5: BaseNameable[M, Position5D, Position1D, Dimension.Fifth, RDD]): U[Cell[Position5D]] = {
    data
      .keyBy { case c => Position1D(c.position(First)) }
      .join(saveDictionary(ev1.convert(this, Over(First), namesI), file, dictionary, separator, First))
      .values
      .keyBy { case (c, i) => Position1D(c.position(Second)) }
      .join(saveDictionary(ev2.convert(this, Over(Second), namesJ), file, dictionary, separator, Second))
      .map { case (_, ((c, i), j)) => (c, i, j) }
      .keyBy { case (c, pi, pj) => Position1D(c.position(Third)) }
      .join(saveDictionary(ev3.convert(this, Over(Third), namesK), file, dictionary, separator, Third))
      .map { case (_, ((c, i, j), k)) => (c, i, j, k) }
      .keyBy { case (c, i, j, k) => Position1D(c.position(Fourth)) }
      .join(saveDictionary(ev4.convert(this, Over(Fourth), namesL), file, dictionary, separator, Fourth))
      .map { case (_, ((c, i, j, k), l)) => (c, i, j, k, l) }
      .keyBy { case (c, i, j, k, l) => Position1D(c.position(Fifth)) }
      .join(saveDictionary(ev5.convert(this, Over(Fifth), namesM), file, dictionary, separator, Fifth))
      .map {
        case (_, ((c, i, j, k, l), m)) =>
          i + separator + j + separator + k + separator + l + separator + m + separator + c.content.value.toShortString
      }
      .saveAsTextFile(file)

    data
  }
}

/**
 * Rich wrapper around a `RDD[Cell[Position6D]]`.
 *
 * @param data `RDD[Cell[Position6D]]`.
 */
class Matrix6D(val data: RDD[Cell[Position6D]]) extends Matrix[Position6D] with ReduceableMatrix[Position6D]
  with ExpandableMatrix[Position6D] {
  def domain[T <: Tuner](tuner: T = Default())(implicit ev: DomainTuners#V[T]): U[Position6D] = {
    names(Over(First))
      .map { case Position1D(c) => c }
      .cartesian(names(Over(Second)).map { case Position1D(c) => c })
      .cartesian(names(Over(Third)).map { case Position1D(c) => c })
      .cartesian(names(Over(Fourth)).map { case Position1D(c) => c })
      .cartesian(names(Over(Fifth)).map { case Position1D(c) => c })
      .cartesian(names(Over(Sixth)).map { case Position1D(c) => c })
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
  def permute[D <: Dimension, F <: Dimension, G <: Dimension, H <: Dimension, I <: Dimension, J <: Dimension](first: D,
    second: F, third: G, fourth: H, fifth: I, sixth: J)(implicit ev1: PosDimDep[Position6D, D],
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
   * @return A `RDD[Cell[Position6D]]`; that is it returns `data`.
   */
  def saveAsIV(file: String, dictionary: String = "%1$s.dict.%2$d", separator: String = "|"): U[Cell[Position6D]] = {
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
   * @return A `RDD[Cell[Position6D]]`; that is it returns `data`.
   *
   * @note If `names` contains a subset of the columns, then only those columns get persisted to file.
   */
  def saveAsIVWithNames[I, J, K, L, M, N](file: String, namesI: I, namesJ: J, namesK: K, namesL: L, namesM: M,
    namesN: N, dictionary: String = "%1$s.dict.%2$d", separator: String = "|")(
      implicit ev1: BaseNameable[I, Position6D, Position1D, Dimension.First, RDD],
        ev2: BaseNameable[J, Position6D, Position1D, Dimension.Second, RDD],
          ev3: BaseNameable[K, Position6D, Position1D, Dimension.Third, RDD],
            ev4: BaseNameable[L, Position6D, Position1D, Dimension.Fourth, RDD],
              ev5: BaseNameable[M, Position6D, Position1D, Dimension.Fifth, RDD],
                ev6: BaseNameable[N, Position6D, Position1D, Dimension.Sixth, RDD]): U[Cell[Position6D]] = {
    data
      .keyBy { case c => Position1D(c.position(First)) }
      .join(saveDictionary(ev1.convert(this, Over(First), namesI), file, dictionary, separator, First))
      .values
      .keyBy { case (c, i) => Position1D(c.position(Second)) }
      .join(saveDictionary(ev2.convert(this, Over(Second), namesJ), file, dictionary, separator, Second))
      .map { case (_, ((c, i), j)) => (c, i, j) }
      .keyBy { case (c, pi, pj) => Position1D(c.position(Third)) }
      .join(saveDictionary(ev3.convert(this, Over(Third), namesK), file, dictionary, separator, Third))
      .map { case (_, ((c, i, j), k)) => (c, i, j, k) }
      .keyBy { case (c, i, j, k) => Position1D(c.position(Fourth)) }
      .join(saveDictionary(ev4.convert(this, Over(Fourth), namesL), file, dictionary, separator, Fourth))
      .map { case (_, ((c, i, j, k), l)) => (c, i, j, k, l) }
      .keyBy { case (c, i, j, k, l) => Position1D(c.position(Fifth)) }
      .join(saveDictionary(ev5.convert(this, Over(Fifth), namesM), file, dictionary, separator, Fifth))
      .map { case (_, ((c, i, j, k, l), m)) => (c, i, j, k, l, m) }
      .keyBy { case (c, i, j, k, l, m) => Position1D(c.position(Sixth)) }
      .join(saveDictionary(ev6.convert(this, Over(Sixth), namesN), file, dictionary, separator, Sixth))
      .map {
        case (_, ((c, i, j, k, l, m), n)) =>
          i + separator + j + separator + k + separator + l + separator + m + separator +
            n + separator + c.content.value.toShortString
      }
      .saveAsTextFile(file)

    data
  }
}

/**
 * Rich wrapper around a `RDD[Cell[Position7D]]`.
 *
 * @param data `RDD[Cell[Position7D]]`.
 */
class Matrix7D(val data: RDD[Cell[Position7D]]) extends Matrix[Position7D] with ReduceableMatrix[Position7D]
  with ExpandableMatrix[Position7D] {
  def domain[T <: Tuner](tuner: T = Default())(implicit ev: DomainTuners#V[T]): U[Position7D] = {
    names(Over(First))
      .map { case Position1D(c) => c }
      .cartesian(names(Over(Second)).map { case Position1D(c) => c })
      .cartesian(names(Over(Third)).map { case Position1D(c) => c })
      .cartesian(names(Over(Fourth)).map { case Position1D(c) => c })
      .cartesian(names(Over(Fifth)).map { case Position1D(c) => c })
      .cartesian(names(Over(Sixth)).map { case Position1D(c) => c })
      .cartesian(names(Over(Seventh)).map { case Position1D(c) => c })
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
   * @return A `RDD[Cell[Position7D]]`; that is it returns `data`.
   */
  def saveAsIV(file: String, dictionary: String = "%1$s.dict.%2$d", separator: String = "|"): U[Cell[Position7D]] = {
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
   * @return A `RDD[Cell[Position7D]]`; that is it returns `data`.
   *
   * @note If `names` contains a subset of the columns, then only those columns get persisted to file.
   */
  def saveAsIVWithNames[I, J, K, L, M, N, O](file: String, namesI: I, namesJ: J, namesK: K, namesL: L, namesM: M,
    namesN: N, namesO: O, dictionary: String = "%1$s.dict.%2$d", separator: String = "|")(
      implicit ev1: BaseNameable[I, Position7D, Position1D, Dimension.First, RDD],
        ev2: BaseNameable[J, Position7D, Position1D, Dimension.Second, RDD],
          ev3: BaseNameable[K, Position7D, Position1D, Dimension.Third, RDD],
            ev4: BaseNameable[L, Position7D, Position1D, Dimension.Fourth, RDD],
              ev5: BaseNameable[M, Position7D, Position1D, Dimension.Fifth, RDD],
                ev6: BaseNameable[N, Position7D, Position1D, Dimension.Sixth, RDD],
                  ev7: BaseNameable[O, Position7D, Position1D, Dimension.Seventh, RDD]): U[Cell[Position7D]] = {
    data
      .keyBy { case c => Position1D(c.position(First)) }
      .join(saveDictionary(ev1.convert(this, Over(First), namesI), file, dictionary, separator, First))
      .values
      .keyBy { case (c, i) => Position1D(c.position(Second)) }
      .join(saveDictionary(ev2.convert(this, Over(Second), namesJ), file, dictionary, separator, Second))
      .map { case (_, ((c, i), j)) => (c, i, j) }
      .keyBy { case (c, pi, pj) => Position1D(c.position(Third)) }
      .join(saveDictionary(ev3.convert(this, Over(Third), namesK), file, dictionary, separator, Third))
      .map { case (_, ((c, i, j), k)) => (c, i, j, k) }
      .keyBy { case (c, i, j, k) => Position1D(c.position(Fourth)) }
      .join(saveDictionary(ev4.convert(this, Over(Fourth), namesL), file, dictionary, separator, Fourth))
      .map { case (_, ((c, i, j, k), l)) => (c, i, j, k, l) }
      .keyBy { case (c, i, j, k, l) => Position1D(c.position(Fifth)) }
      .join(saveDictionary(ev5.convert(this, Over(Fifth), namesM), file, dictionary, separator, Fifth))
      .map { case (_, ((c, i, j, k, l), m)) => (c, i, j, k, l, m) }
      .keyBy { case (c, i, j, k, l, m) => Position1D(c.position(Sixth)) }
      .join(saveDictionary(ev6.convert(this, Over(Sixth), namesN), file, dictionary, separator, Sixth))
      .map { case (_, ((c, i, j, k, l, m), n)) => (c, i, j, k, l, m, n) }
      .keyBy { case (c, i, j, k, l, m, n) => Position1D(c.position(Seventh)) }
      .join(saveDictionary(ev7.convert(this, Over(Seventh), namesO), file, dictionary, separator, Seventh))
      .map {
        case (_, ((c, i, j, k, l, m, n), o)) =>
          i + separator + j + separator + k + separator + l + separator + m + separator +
            n + separator + o + separator + c.content.value.toShortString
      }
      .saveAsTextFile(file)

    data
  }
}

/**
 * Rich wrapper around a `RDD[Cell[Position8D]]`.
 *
 * @param data `RDD[Cell[Position8D]]`.
 */
class Matrix8D(val data: RDD[Cell[Position8D]]) extends Matrix[Position8D] with ReduceableMatrix[Position8D]
  with ExpandableMatrix[Position8D] {
  def domain[T <: Tuner](tuner: T = Default())(implicit ev: DomainTuners#V[T]): U[Position8D] = {
    names(Over(First))
      .map { case Position1D(c) => c }
      .cartesian(names(Over(Second)).map { case Position1D(c) => c })
      .cartesian(names(Over(Third)).map { case Position1D(c) => c })
      .cartesian(names(Over(Fourth)).map { case Position1D(c) => c })
      .cartesian(names(Over(Fifth)).map { case Position1D(c) => c })
      .cartesian(names(Over(Sixth)).map { case Position1D(c) => c })
      .cartesian(names(Over(Seventh)).map { case Position1D(c) => c })
      .cartesian(names(Over(Eighth)).map { case Position1D(c) => c })
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
   * @return A `RDD[Cell[Position8D]]`; that is it returns `data`.
   */
  def saveAsIV(file: String, dictionary: String = "%1$s.dict.%2$d", separator: String = "|"): U[Cell[Position8D]] = {
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
   * @return A `RDD[Cell[Position8D]]`; that is it returns `data`.
   *
   * @note If `names` contains a subset of the columns, then only those columns get persisted to file.
   */
  def saveAsIVWithNames[I, J, K, L, M, N, O, Q](file: String, namesI: I, namesJ: J, namesK: K, namesL: L, namesM: M,
    namesN: N, namesO: O, namesQ: Q, dictionary: String = "%1$s.dict.%2$d", separator: String = "|")(
      implicit ev1: BaseNameable[I, Position8D, Position1D, Dimension.First, RDD],
        ev2: BaseNameable[J, Position8D, Position1D, Dimension.Second, RDD],
          ev3: BaseNameable[K, Position8D, Position1D, Dimension.Third, RDD],
            ev4: BaseNameable[L, Position8D, Position1D, Dimension.Fourth, RDD],
              ev5: BaseNameable[M, Position8D, Position1D, Dimension.Fifth, RDD],
                ev6: BaseNameable[N, Position8D, Position1D, Dimension.Sixth, RDD],
                  ev7: BaseNameable[O, Position8D, Position1D, Dimension.Seventh, RDD],
                    ev8: BaseNameable[Q, Position8D, Position1D, Dimension.Eighth, RDD]): U[Cell[Position8D]] = {
    data
      .keyBy { case c => Position1D(c.position(First)) }
      .join(saveDictionary(ev1.convert(this, Over(First), namesI), file, dictionary, separator, First))
      .values
      .keyBy { case (c, i) => Position1D(c.position(Second)) }
      .join(saveDictionary(ev2.convert(this, Over(Second), namesJ), file, dictionary, separator, Second))
      .map { case (_, ((c, i), j)) => (c, i, j) }
      .keyBy { case (c, pi, pj) => Position1D(c.position(Third)) }
      .join(saveDictionary(ev3.convert(this, Over(Third), namesK), file, dictionary, separator, Third))
      .map { case (_, ((c, i, j), k)) => (c, i, j, k) }
      .keyBy { case (c, i, j, k) => Position1D(c.position(Fourth)) }
      .join(saveDictionary(ev4.convert(this, Over(Fourth), namesL), file, dictionary, separator, Fourth))
      .map { case (_, ((c, i, j, k), l)) => (c, i, j, k, l) }
      .keyBy { case (c, i, j, k, l) => Position1D(c.position(Fifth)) }
      .join(saveDictionary(ev5.convert(this, Over(Fifth), namesM), file, dictionary, separator, Fifth))
      .map { case (_, ((c, i, j, k, l), m)) => (c, i, j, k, l, m) }
      .keyBy { case (c, i, j, k, l, m) => Position1D(c.position(Sixth)) }
      .join(saveDictionary(ev6.convert(this, Over(Sixth), namesN), file, dictionary, separator, Sixth))
      .map { case (_, ((c, i, j, k, l, m), n)) => (c, i, j, k, l, m, n) }
      .keyBy { case (c, i, j, k, l, m, n) => Position1D(c.position(Seventh)) }
      .join(saveDictionary(ev7.convert(this, Over(Seventh), namesO), file, dictionary, separator, Seventh))
      .map { case (_, ((c, i, j, k, l, m, n), o)) => (c, i, j, k, l, m, n, o) }
      .keyBy { case (c, i, j, k, l, m, n, o) => Position1D(c.position(Eighth)) }
      .join(saveDictionary(ev8.convert(this, Over(Eighth), namesQ), file, dictionary, separator, Eighth))
      .map {
        case (_, ((c, i, j, k, l, m, n, o), p)) =>
          i + separator + j + separator + k + separator + l + separator + m + separator +
            n + separator + o + separator + p + separator + c.content.value.toShortString
      }
      .saveAsTextFile(file)

    data
  }
}

/**
 * Rich wrapper around a `RDD[Cell[Position9D]]`.
 *
 * @param data `RDD[Cell[Position9D]]`.
 */
class Matrix9D(val data: RDD[Cell[Position9D]]) extends Matrix[Position9D] with ReduceableMatrix[Position9D] {
  def domain[T <: Tuner](tuner: T = Default())(implicit ev: DomainTuners#V[T]): U[Position9D] = {
    names(Over(First))
      .map { case Position1D(c) => c }
      .cartesian(names(Over(Second)).map { case Position1D(c) => c })
      .cartesian(names(Over(Third)).map { case Position1D(c) => c })
      .cartesian(names(Over(Fourth)).map { case Position1D(c) => c })
      .cartesian(names(Over(Fifth)).map { case Position1D(c) => c })
      .cartesian(names(Over(Sixth)).map { case Position1D(c) => c })
      .cartesian(names(Over(Seventh)).map { case Position1D(c) => c })
      .cartesian(names(Over(Eighth)).map { case Position1D(c) => c })
      .cartesian(names(Over(Ninth)).map { case Position1D(c) => c })
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
   * @return A `RDD[Cell[Position9D]]`; that is it returns `data`.
   */
  def saveAsIV(file: String, dictionary: String = "%1$s.dict.%2$d", separator: String = "|"): U[Cell[Position9D]] = {
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
   * @return A `RDD[Cell[Position9D]]`; that is it returns `data`.
   *
   * @note If `names` contains a subset of the columns, then only those columns get persisted to file.
   */
  def saveAsIVWithNames[I, J, K, L, M, N, O, Q, R](file: String, namesI: I, namesJ: J, namesK: K, namesL: L, namesM: M,
    namesN: N, namesO: O, namesQ: Q, namesR: R, dictionary: String = "%1$s.dict.%2$d", separator: String = "|")(
      implicit ev1: BaseNameable[I, Position9D, Position1D, Dimension.First, RDD],
        ev2: BaseNameable[J, Position9D, Position1D, Dimension.Second, RDD],
          ev3: BaseNameable[K, Position9D, Position1D, Dimension.Third, RDD],
            ev4: BaseNameable[L, Position9D, Position1D, Dimension.Fourth, RDD],
              ev5: BaseNameable[M, Position9D, Position1D, Dimension.Fifth, RDD],
                ev6: BaseNameable[N, Position9D, Position1D, Dimension.Sixth, RDD],
                  ev7: BaseNameable[O, Position9D, Position1D, Dimension.Seventh, RDD],
                    ev8: BaseNameable[Q, Position9D, Position1D, Dimension.Eighth, RDD],
                      ev9: BaseNameable[R, Position9D, Position1D, Dimension.Ninth, RDD]): U[Cell[Position9D]] = {
    data
      .keyBy { case c => Position1D(c.position(First)) }
      .join(saveDictionary(ev1.convert(this, Over(First), namesI), file, dictionary, separator, First))
      .values
      .keyBy { case (c, i) => Position1D(c.position(Second)) }
      .join(saveDictionary(ev2.convert(this, Over(Second), namesJ), file, dictionary, separator, Second))
      .map { case (_, ((c, i), j)) => (c, i, j) }
      .keyBy { case (c, pi, pj) => Position1D(c.position(Third)) }
      .join(saveDictionary(ev3.convert(this, Over(Third), namesK), file, dictionary, separator, Third))
      .map { case (_, ((c, i, j), k)) => (c, i, j, k) }
      .keyBy { case (c, i, j, k) => Position1D(c.position(Fourth)) }
      .join(saveDictionary(ev4.convert(this, Over(Fourth), namesL), file, dictionary, separator, Fourth))
      .map { case (_, ((c, i, j, k), l)) => (c, i, j, k, l) }
      .keyBy { case (c, i, j, k, l) => Position1D(c.position(Fifth)) }
      .join(saveDictionary(ev5.convert(this, Over(Fifth), namesM), file, dictionary, separator, Fifth))
      .map { case (_, ((c, i, j, k, l), m)) => (c, i, j, k, l, m) }
      .keyBy { case (c, i, j, k, l, m) => Position1D(c.position(Sixth)) }
      .join(saveDictionary(ev6.convert(this, Over(Sixth), namesN), file, dictionary, separator, Sixth))
      .map { case (_, ((c, i, j, k, l, m), n)) => (c, i, j, k, l, m, n) }
      .keyBy { case (c, i, j, k, l, m, n) => Position1D(c.position(Seventh)) }
      .join(saveDictionary(ev7.convert(this, Over(Seventh), namesO), file, dictionary, separator, Seventh))
      .map { case (_, ((c, i, j, k, l, m, n), o)) => (c, i, j, k, l, m, n, o) }
      .keyBy { case (c, i, j, k, l, m, n, o) => Position1D(c.position(Eighth)) }
      .join(saveDictionary(ev8.convert(this, Over(Eighth), namesQ), file, dictionary, separator, Eighth))
      .map { case (_, ((c, i, j, k, l, m, n, o), p)) => (c, i, j, k, l, m, n, o, p) }
      .keyBy { case (c, i, j, k, l, m, n, o, p) => Position1D(c.position(Ninth)) }
      .join(saveDictionary(ev9.convert(this, Over(Ninth), namesR), file, dictionary, separator, Ninth))
      .map {
        case (_, ((c, i, j, k, l, m, n, o, p), q)) =>
          i + separator + j + separator + k + separator + l + separator + m + separator +
            n + separator + o + separator + p + separator + q + separator + c.content.value.toShortString
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

