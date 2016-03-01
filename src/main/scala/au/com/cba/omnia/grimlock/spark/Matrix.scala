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

package au.com.cba.omnia.grimlock.spark

import au.com.cba.omnia.grimlock.framework.{
  Cell,
  Consume,
  Default,
  ReshapeableMatrix => FwReshapeableMatrix,
  ExtractWithDimension,
  ExtractWithKey,
  Locate,
  Matrix => FwMatrix,
  Matrix1D => FwMatrix1D,
  Matrix2D => FwMatrix2D,
  Matrix3D => FwMatrix3D,
  Matrix4D => FwMatrix4D,
  Matrix5D => FwMatrix5D,
  Matrix6D => FwMatrix6D,
  Matrix7D => FwMatrix7D,
  Matrix8D => FwMatrix8D,
  Matrix9D => FwMatrix9D,
  Matrixable => FwMatrixable,
  MatrixWithParseErrors,
  NoParameters,
  Predicateable => FwPredicateable,
  ReduceableMatrix => FwReduceableMatrix,
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

import au.com.cba.omnia.grimlock.spark.distribution._
import au.com.cba.omnia.grimlock.spark.environment._

import java.io.File
import java.nio.file.{ Files, Paths }

import org.apache.hadoop.io.Writable

import org.apache.spark.rdd.RDD

import scala.collection.immutable.HashSet
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
trait Matrix[P <: Position] extends FwMatrix[P] with Persist[Cell[P]] with UserData {
  type M = Matrix[P]

  import SparkImplicits._

  type ChangeTuners = TP2
  def change[I, T <: Tuner](slice: Slice[P], positions: I, schema: Content.Parser, tuner: T = Default())(
    implicit ev1: PositionDistributable[I, slice.S, RDD], ev2: ClassTag[slice.S],
      ev3: ChangeTuners#V[T]): U[Cell[P]] = {
    data
      .keyBy { case c => slice.selected(c.position) }
      .tunedLeftJoin(tuner.parameters, ev1.convert(positions).keyBy { case p => p })
      .flatMap {
        case (_, (c, po)) => po match {
          case Some(_) => schema(c.content.value.toShortString).map { case con => Cell(c.position, con) }
          case None => Some(c)
        }
      }
  }

  type CompactTuners = TP2
  def compact()(implicit ev: ClassTag[P]): E[Map[P, Content]] = {
    data
      .map { case c => (c.position, c.content) }
      .collectAsMap
      .toMap
  }

  def compact[T <: Tuner](slice: Slice[P], tuner: T = Default())(implicit ev1: slice.S =!= Position0D,
    ev2: ClassTag[slice.S], ev3: CompactTuners#V[T]): E[Map[slice.S, slice.C]] = {
    data
      .map { case c => (c.position, slice.toMap(c)) }
      .keyBy { case (p, m) => slice.selected(p) }
      .tunedReduce(tuner.parameters, (l: (P, Map[slice.S, slice.C]), r: (P, Map[slice.S, slice.C])) =>
        (l._1, slice.combineMaps(l._1, l._2, r._2)))
      .map { case (_, (_, m)) => m }
      .reduce { case (lm, rm) => lm ++ rm }
  }

  //type DomainTuners = TP1
  type DomainTuners[T] = TP1[T]

  type FillHeterogeneousTuners[T] = Aux3[T]
  def fillHeterogeneous[S <: Position, T <: Tuner : FillHeterogeneousTuners](slice: Slice[P], values: U[Cell[S]], tuner: T = Default())(
    implicit ev1: ClassTag[P], ev2: ClassTag[slice.S], ev3: slice.S =:= S): U[Cell[P]] = {
    val (p1, p2) = tuner.parameters match {
      case Sequence2(f, s) => (f, s)
      case p => (NoParameters(), p)
    }

    domain(Default())
      .keyBy { case p => slice.selected(p) }
      .tunedJoin(p1, values.keyBy { case c => c.position.asInstanceOf[slice.S] })
      .map { case (_, (p, c)) => (p, Cell(p, c.content)) }
      .tunedLeftJoin(p2, data.keyBy { case c => c.position })
      .map { case (_, (c, co)) => co.getOrElse(c) }
  }

  type FillHomogeneousTuners = TP2
  def fillHomogeneous[T <: Tuner](value: Content, tuner: T = Default())(implicit ev1: ClassTag[P],
    ev2: FillHomogeneousTuners#V[T]): U[Cell[P]] = {
    domain(Default())
      .keyBy { case p => p }
      .tunedLeftJoin(tuner.parameters, data.keyBy { case c => c.position })
      .map { case (p, (_, co)) => co.getOrElse(Cell(p, value)) }
  }

  type GetTuners = TP2
  def get[I, T <: Tuner](positions: I, tuner: T = Default())(implicit ev1: PositionDistributable[I, P, RDD],
    ev2: ClassTag[P], ev3: GetTuners#V[T]): U[Cell[P]] = {
    data
      .keyBy { case c => c.position }
      .tunedJoin(tuner.parameters, ev1.convert(positions).keyBy { case p => p })
      .map { case (_, (c, _)) => c }
  }

  type JoinTuners[T] = Aux3[T]
  def join[T <: Tuner : JoinTuners](slice: Slice[P], that: M, tuner: T = Default())(implicit ev1: P =!= Position1D,
    ev2: ClassTag[slice.S]): U[Cell[P]] = {
    val (p1, p2) = tuner.parameters match {
      case Sequence2(f, s) => (f, s)
      case p => (NoParameters(), p)
    }

    (data ++ that.data)
      .keyBy { case c => slice.selected(c.position) }
      .tunedJoin(p2, names(slice).map { case p => (p, ()) }.tunedJoin(p1, that.names(slice).map { case p => (p, ()) }))
      .map { case (_, (c, _)) => c }
  }

  type MaterialiseTuners[T] = TP1[T]
  def materialise[T <: Tuner : MaterialiseTuners](tuner: T = Default()): List[Cell[P]] = {
    data.collect.toList
  }

  type NamesTuners = TP2
  def names[T <: Tuner](slice: Slice[P], tuner: T = Default())(implicit ev1: slice.S =!= Position0D,
    ev2: ClassTag[slice.S], ev3: NamesTuners#V[T]): U[slice.S] = {
    data.map { case c => slice.selected(c.position) }.tunedDistinct(tuner.parameters)(Position.Ordering[slice.S]())
  }

  type PairwiseTuners = OneOf6[Default[NoParameters],
                               Default[Reducers],
                               Default[Sequence2[Reducers, Reducers]],
                               Default[Sequence2[Reducers, Sequence2[Reducers, Reducers]]],
                               Default[Sequence2[Sequence2[Reducers, Reducers], Reducers]],
                               Default[Sequence2[Sequence2[Reducers, Reducers], Sequence2[Reducers, Reducers]]]]
  def pairwise[Q <: Position, T <: Tuner](slice: Slice[P], comparer: Comparer, operators: Operable[P, Q],
    tuner: T = Default())(implicit ev1: slice.S =!= Position0D, ev2: PosExpDep[slice.R, Q], ev3: ClassTag[slice.S],
      ev4: ClassTag[slice.R], ev5: PairwiseTuners#V[T]): U[Cell[Q]] = {
    val operator = operators()

    pairwiseTuples(slice, comparer, tuner)(data, data).flatMap { case (lc, rc) => operator.compute(lc, rc) }
  }

  def pairwiseWithValue[Q <: Position, W, T <: Tuner](slice: Slice[P], comparer: Comparer,
    operators: OperableWithValue[P, Q, W], value: E[W], tuner: T = Default())(implicit ev1: slice.S =!= Position0D,
      ev2: PosExpDep[slice.R, Q], ev3: ClassTag[slice.S], ev4: ClassTag[slice.R],
        ev5: PairwiseTuners#V[T]): U[Cell[Q]] = {
    val operator = operators()

    pairwiseTuples(slice, comparer, tuner)(data, data)
      .flatMap { case (lc, rc) => operator.computeWithValue(lc, rc, value) }
  }

  def pairwiseBetween[Q <: Position, T <: Tuner](slice: Slice[P], comparer: Comparer, that: M,
    operators: Operable[P, Q], tuner: T = Default())(implicit ev1: slice.S =!= Position0D, ev2: PosExpDep[slice.R, Q],
      ev3: ClassTag[slice.S], ev4: ClassTag[slice.R], ev5: PairwiseTuners#V[T]): U[Cell[Q]] = {
    val operator = operators()

    pairwiseTuples(slice, comparer, tuner)(data, that.data).flatMap { case (lc, rc) => operator.compute(lc, rc) }
  }

  def pairwiseBetweenWithValue[Q <: Position, W, T <: Tuner](slice: Slice[P], comparer: Comparer, that: M,
    operators: OperableWithValue[P, Q, W], value: E[W], tuner: T = Default())(implicit ev1: slice.S =!= Position0D,
      ev2: PosExpDep[slice.R, Q], ev3: ClassTag[slice.S], ev4: ClassTag[slice.R],
        ev5: PairwiseTuners#V[T]): U[Cell[Q]] = {
    val operator = operators()

    pairwiseTuples(slice, comparer, tuner)(data, that.data)
      .flatMap { case (lc, rc) => operator.computeWithValue(lc, rc, value) }
  }

  def relocate[Q <: Position](locate: Locate.FromCell[P, Q])(implicit ev: PosIncDep[P, Q]): U[Cell[Q]] = {
    data.flatMap { case c => locate(c).map(Cell(_, c.content)) }
  }

  def relocateWithValue[Q <: Position, W](locate: Locate.FromCellWithValue[P, Q, W], value: E[W])(
    implicit ev: PosIncDep[P, Q]): U[Cell[Q]] = {
    data.flatMap { case c => locate(c, value).map(Cell(_, c.content)) }
  }

  def saveAsText(file: String, writer: TextWriter)(implicit ctx: C): U[Cell[P]] = saveText(file, writer)

  type SetTuners = TP2
  def set[T <: Tuner](values: FwMatrixable[P, RDD], tuner: T = Default())(implicit ev1: ClassTag[P],
    ev2: SetTuners#V[T]): U[Cell[P]] = {
    data
      .keyBy { case c => c.position }
      .tunedOuterJoin(tuner.parameters, values().keyBy { case c => c.position })
      .map { case (_, (co, cn)) => cn.getOrElse(co.get) }
  }

  type ShapeTuners = TP2
  def shape[T <: Tuner](tuner: T = Default())(implicit ev: ShapeTuners#V[T]): U[Cell[Position1D]] = {
    data
      .flatMap { case c => c.position.coordinates.map(_.toString).zipWithIndex }
      .tunedDistinct(tuner.parameters)
      .groupBy { case (s, i) => i }
      .map {
        case (i, s) => Cell(Position1D(Dimension.All(i).toString), Content(DiscreteSchema[Long](), s.size))
      }
  }

  type SizeTuners = TP2
  def size[T <: Tuner](dim: Dimension, distinct: Boolean, tuner: T = Default())(implicit ev1: PosDimDep[P, dim.D],
    ev2: SizeTuners#V[T]): U[Cell[Position1D]] = {
    val coords = data.map { case c => c.position(dim) }
    val dist = if (distinct) { coords } else { coords.tunedDistinct(tuner.parameters)(Value.Ordering) }

    dist
      .context
      .parallelize(List(Cell(Position1D(dim.toString), Content(DiscreteSchema[Long](), dist.count))))
  }

  type SliceTuners = TP2
  def slice[I, T <: Tuner](slice: Slice[P], positions: I, keep: Boolean, tuner: T = Default())(
    implicit ev1: PositionDistributable[I, slice.S, RDD], ev2: ClassTag[slice.S],
      ev3: SliceTuners#V[T]): U[Cell[P]] = {
    data
      .keyBy { case c => slice.selected(c.position) }
      .tunedLeftJoin(tuner.parameters, ev1.convert(positions).map { case p => (p, ()) })
      .collect { case (_, (c, o)) if (o.isEmpty != keep) => c }
  }

  type SlideTuners[T] = TP1[T]
  def slide[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position, T <: Tuner : SlideTuners](
    slice: Slice[P], windows: Windowable[P, S, R, Q], ascending: Boolean = true, tuner: T = Default())(
      implicit ev1: slice.S =:= S, ev2: slice.R =:= R, ev3: slice.R =!= Position0D, ev4: PosExpDep[S, Q],
        ev5: ClassTag[slice.S], ev6: ClassTag[slice.R]): U[Cell[Q]] ={
    val window = windows()

    data
      .map { case c => (slice.selected(c.position), (slice.remainder(c.position), window.prepare(c))) }
      .groupByKey
      .flatMap {
        case (p, itr) => itr
          .toList
          .sortBy { case (r, _) => r }(Position.Ordering(ascending))
          .scanLeft(Option.empty[(window.T, TraversableOnce[window.O])]) {
            case (None, (r, i)) => Some(window.initialise(r, i))
            case (Some((t, _)), (r, i)) => Some(window.update(r, i, t))
          }
          .flatMap {
            case Some((_, o)) => o.flatMap(window.present(p, _))
            case _ => None
          }
      }
  }

  def slideWithValue[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position, W, T <: Tuner : SlideTuners](
    slice: Slice[P], windows: WindowableWithValue[P, S, R, Q, W], value: E[W], ascending: Boolean = true,
      tuner: T = Default())(implicit ev1: slice.S =:= S, ev2: slice.R =:= R, ev3: slice.R =!= Position0D,
        ev4: PosExpDep[S, Q], ev5: ClassTag[slice.S], ev6: ClassTag[slice.R]): U[Cell[Q]] = {
    val window = windows()

    data
      .map { case c => (slice.selected(c.position), (slice.remainder(c.position), window.prepareWithValue(c, value))) }
      .groupByKey
      .flatMap {
        case (p, itr) => itr
          .toList
          .sortBy { case (r, _) => r }(Position.Ordering(ascending))
          .scanLeft(Option.empty[(window.T, TraversableOnce[window.O])]) {
            case (None, (r, i)) => Some(window.initialise(r, i))
            case (Some((t, _)), (r, i)) => Some(window.update(r, i, t))
          }
          .flatMap {
            case Some((_, o)) => o.flatMap(window.presentWithValue(p, _, value))
            case _ => None
          }
      }
  }

  def split[I](partitioners: Partitionable[P, I]): U[(I, Cell[P])] = {
    val partitioner = partitioners()

    data.flatMap { case c => partitioner.assign(c).map { case q => (q, c) } }
  }

  def splitWithValue[I, W](partitioners: PartitionableWithValue[P, I, W], value: E[W]): U[(I, Cell[P])] = {
    val partitioner = partitioners()

    data.flatMap { case c => partitioner.assignWithValue(c, value).map { case q => (q, c) } }
  }

  def stream[Q <: Position](command: String, files: List[String], writer: TextWriter,
    parser: Cell.TextParser[Q]): (U[Cell[Q]], U[String]) = {
    val result = data
      .flatMap(writer(_))
      .pipe(command)
      .flatMap(parser(_))

    (result.collect { case Right(c) => c }, result.collect { case Left(e) => e })
  }

  def subset(samplers: Sampleable[P]): U[Cell[P]] = {
    val sampler = samplers()

    data.filter { case c => sampler.select(c) }
  }

  def subsetWithValue[W](samplers: SampleableWithValue[P, W], value: E[W]): U[Cell[P]] = {
    val sampler = samplers()

    data.filter { case c => sampler.selectWithValue(c, value) }
  }

  type SummariseTuners = TP2
  def summarise[S <: Position with ExpandablePosition, Q <: Position, T <: Tuner](slice: Slice[P],
    aggregators: Aggregatable[P, S, Q], tuner: T = Default())(implicit ev1: slice.S =:= S, ev2: PosIncDep[S, Q],
      ev3: ClassTag[slice.S], ev4: SummariseTuners#V[T]): U[Cell[Q]] = {
    val aggregator = aggregators()

    data
      .map[(slice.S, List[Option[Any]])] {
        case c => (slice.selected(c.position), aggregator.map { case a => a.prepare(c) })
      }
      .tunedReduce(tuner.parameters, (lt: List[Option[Any]], rt: List[Option[Any]]) =>
        (aggregator, lt, rt).zipped.map {
          case (a, Some(l), Some(r)) => Some(a.reduce(l.asInstanceOf[a.T], r.asInstanceOf[a.T]))
          case (_, l, r) => if (l.isEmpty) { r } else { l }
        })
      .flatMap {
        case (p, t) => (aggregator, t).zipped.flatMap {
          case (a, Some(s)) => a.present(p, s.asInstanceOf[a.T])
          case _ => None
        }
      }
  }

  def summariseWithValue[S <: Position with ExpandablePosition, Q <: Position, W, T <: Tuner](slice: Slice[P],
    aggregators: AggregatableWithValue[P, S, Q, W], value: E[W], tuner: T = Default())(implicit ev1: slice.S =:= S,
      ev2: PosIncDep[S, Q], ev3: ClassTag[slice.S], ev4: SummariseTuners#V[T]): U[Cell[Q]] = {
    val aggregator = aggregators()

    data
      .map[(slice.S, List[Option[Any]])] {
        case c => (slice.selected(c.position), aggregator.map { case a => a.prepareWithValue(c, value) })
      }
      .tunedReduce(tuner.parameters, (lt: List[Option[Any]], rt: List[Option[Any]]) =>
        (aggregator, lt, rt).zipped.map {
          case (a, Some(l), Some(r)) => Some(a.reduce(l.asInstanceOf[a.T], r.asInstanceOf[a.T]))
          case (_, l, r) => if (l.isEmpty) { r } else { l }
        })
      .flatMap {
        case (p, t) => (aggregator, t).zipped.flatMap {
          case (a, Some(s)) => a.presentWithValue(p, s.asInstanceOf[a.T], value)
          case _ => None
        }
      }
  }

  def toSequence[K <: Writable, V <: Writable](writer: SequenceWriter[K, V]): U[(K, V)] = data.flatMap(writer(_))

  def toText(writer: TextWriter): U[String] = data.flatMap(writer(_))

  def toVector(separator: String): U[Cell[Position1D]] = {
    data.map { case Cell(p, c) => Cell(Position1D(p.coordinates.map(_.toShortString).mkString(separator)), c) }
  }

  def transform[Q <: Position](transformers: Transformable[P, Q])(implicit ev: PosIncDep[P, Q]): U[Cell[Q]] = {
    val transformer = transformers()

    data.flatMap { case c => transformer.present(c) }
  }

  def transformWithValue[Q <: Position, W](transformers: TransformableWithValue[P, Q, W], value: E[W])(
    implicit ev: PosIncDep[P, Q]): U[Cell[Q]] = {
    val transformer = transformers()

    data.flatMap { case c => transformer.presentWithValue(c, value) }
  }

  type TypesTuners = TP2
  def types[T <: Tuner](slice: Slice[P], specific: Boolean, tuner: T = Default())(implicit ev1: slice.S =!= Position0D,
    ev2: ClassTag[slice.S], ev3: TypesTuners#V[T]): U[(slice.S, Type)] = {
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

  def uniqueByPositions[T <: Tuner](slice: Slice[P], tuner: T = Default())(implicit ev1: slice.S =!= Position0D,
    ev2: UniqueTuners#V[T]): U[(slice.S, Content)] = {
    val ordering = new Ordering[Cell[slice.S]] {
      def compare(l: Cell[slice.S], r: Cell[slice.S]) = l.toString().compare(r.toString)
    }

    data
      .map { case Cell(p, c) => Cell(slice.selected(p), c) }
      .tunedDistinct(tuner.parameters)(ordering)
      .map { case Cell(p, c) => (p, c) }
  }

  type WhichTuners = TP2
  def which(predicate: Cell.Predicate[P])(implicit ev: ClassTag[P]): U[P] = {
    data.collect { case c if predicate(c) => c.position }
  }

  def whichByPositions[I, T <: Tuner](slice: Slice[P], predicates: I, tuner: T = Default())(
    implicit ev1: FwPredicateable[I, P, slice.S, RDD], ev2: ClassTag[slice.S], ev3: ClassTag[P],
      ev4: WhichTuners#V[T]): U[P] = {
    val pp = ev1.convert(predicates)
      .map { case (pos, pred) => pos.map { case p => (p, pred) } }
      .reduce((l, r) => l ++ r)

    data
      .keyBy { case c => slice.selected(c.position) }
      .tunedJoin(tuner.parameters, pp.keyBy { case (p, pred) => p })
      .collect { case (_, (c, (_, predicate))) if predicate(c) => c.position }
  }

  protected def saveDictionary(slice: Slice[P], file: String, dictionary: String, separator: String)(
    implicit ev: ClassTag[slice.S]) = {
    val numbered = names(slice)
      .zipWithIndex

    numbered
      .map { case (p, i) => p.toShortString(separator) + separator + i }
      .saveAsTextFile(dictionary.format(file, slice.dimension.index))

    numbered
  }

  private def pairwiseTuples[T <: Tuner](slice: Slice[P], comparer: Comparer, tuner: T)(ldata: U[Cell[P]],
    rdata: U[Cell[P]])(implicit ev1: ClassTag[slice.S], ev2: ClassTag[slice.R]): U[(Cell[P], Cell[P])] = {
    val (rr, rj, lr, lj) = tuner.parameters match {
      case Sequence2(Sequence2(rr, rj), Sequence2(lr, lj)) => (rr, rj, lr, lj)
      case Sequence2(rj @ Reducers(_), Sequence2(lr, lj)) => (NoParameters(), rj, lr, lj)
      case Sequence2(Sequence2(rr, rj), lj @ Reducers(_)) => (rr, rj, NoParameters(), lj)
      case Sequence2(rj @ Reducers(_), lj @ Reducers(_)) => (NoParameters(), rj, NoParameters(), lj)
      case lj @ Reducers(_) => (NoParameters(), NoParameters(), NoParameters(), lj)
      case _ => (NoParameters(), NoParameters(), NoParameters(), NoParameters())
    }
    val ordering = Position.Ordering[slice.S]()

    ldata.map { case c => slice.selected(c.position) }.tunedDistinct(lr)(ordering)
      .cartesian(rdata.map { case c => slice.selected(c.position) }.tunedDistinct(rr)(ordering))
      .collect { case (l, r) if comparer.keep(l, r) => (l, r) }
      .keyBy { case (l, _) => l }
      .tunedJoin(lj, ldata.keyBy { case Cell(p, _) => slice.selected(p) })
      .keyBy { case (_, ((_, r),  _)) => r }
      .tunedJoin(rj, rdata.keyBy { case Cell(p, _) => slice.selected(p) })
      .map { case (_, ((_, (_, lc)), rc)) => (lc, rc) }
  }
}

/** Base trait for methods that reduce the number of dimensions or that can be filled using a `RDD[Cell[P]]`. */
trait ReduceableMatrix[P <: Position with ReduceablePosition] extends FwReduceableMatrix[P] { self: Matrix[P] =>

  import SparkImplicits._

  def melt(dim: Dimension, into: Dimension, separator: String = ".")(implicit ev1: PosDimDep[P, dim.D],
    ev2: PosDimDep[P, into.D], ne: dim.D =!= into.D): U[Cell[P#L]] = {
    data.map { case Cell(p, c) => Cell(p.melt(dim, into, separator), c) }
  }

  type SquashTuners = TP2
  def squash[T <: Tuner](dim: Dimension, squasher: Squashable[P], tuner: T = Default())(
    implicit ev1: PosDimDep[P, dim.D], ev2: ClassTag[P#L], ev3: SquashTuners#V[T]): U[Cell[P#L]] = {
    val squash = squasher()

    data
      .map[(P#L, Any)] { case c => (c.position.remove(dim), squash.prepare(c, dim)) }
      .tunedReduce(tuner.parameters, (lt, rt) => squash.reduce(lt.asInstanceOf[squash.T], rt.asInstanceOf[squash.T]))
      .flatMap { case (p, t) => squash.present(t.asInstanceOf[squash.T]).map { case c => Cell(p, c) } }
  }

  def squashWithValue[W, T <: Tuner](dim: Dimension, squasher: SquashableWithValue[P, W], value: E[W],
    tuner: T = Default())(implicit ev1: PosDimDep[P, dim.D], ev2: ClassTag[P#L],
      ev3: SquashTuners#V[T]): U[Cell[P#L]] = {
    val squash = squasher()

    data
      .map[(P#L, Any)] { case c => (c.position.remove(dim), squash.prepareWithValue(c, dim, value)) }
      .tunedReduce(tuner.parameters, (lt, rt) => squash.reduce(lt.asInstanceOf[squash.T], rt.asInstanceOf[squash.T]))
      .flatMap { case (p, t) => squash.presentWithValue(t.asInstanceOf[squash.T], value).map { case c => Cell(p, c) } }
  }
}

/** Base trait for methods that reshapes the number of dimension of a matrix using a `RDD[Cell[P]]`. */
trait ReshapeableMatrix[P <: Position with ExpandablePosition with ReduceablePosition]
  extends FwReshapeableMatrix[P] { self: Matrix[P] =>

  import SparkImplicits._

  type ReshapeTuners = TP2
  def reshape[Q <: Position, T <: Tuner](dim: Dimension, coordinate: Valueable,
    locate: Locate.FromCellAndOptionalValue[P, Q], tuner: T = Default())(implicit ev1: PosDimDep[P, dim.D],
      ev2: PosExpDep[P, Q], ev3: ClassTag[P#L], ev4: ReshapeTuners#V[T]): U[Cell[Q]] = {
    val keys = data
      .collect[(P#L, Value)] { case c if (c.position(dim) equ coordinate) => (c.position.remove(dim), c.content.value) }

    data
      .collect[(P#L, Cell[P])] { case c if (c.position(dim) neq coordinate) => (c.position.remove(dim), c) }
      .tunedLeftJoin(tuner.parameters, keys)
      .flatMap { case (_, (c, v)) => locate(c, v).map(Cell(_, c.content)) }
  }
}

// TODO: Make this work on more than 2D matrices and share with Scalding
trait MatrixDistance { self: Matrix[Position2D] with ReduceableMatrix[Position2D] =>

  import au.com.cba.omnia.grimlock.library.aggregate._
  import au.com.cba.omnia.grimlock.library.pairwise._
  import au.com.cba.omnia.grimlock.library.transform._
  import au.com.cba.omnia.grimlock.library.window._

  import au.com.cba.omnia.grimlock.spark.Matrix._

  /**
   * Compute correlations.
   *
   * @param slice  Encapsulates the dimension for which to compute correlations.
   * @param stuner The summarise tuner for the job.
   * @param ptuner The pairwise tuner for the job.
   *
   * @return A `U[Cell[Position1D]]` with all pairwise correlations.
   */
  def correlation[ST <: Tuner, PT <: Tuner](slice: Slice[Position2D], stuner: ST = Default(), ptuner: PT = Default())(
    implicit ev1: ClassTag[slice.S], ev2: ClassTag[slice.R], ev3: SummariseTuners#V[ST],
      ev4: PairwiseTuners#V[PT]): U[Cell[Position1D]] = {
    implicit def UP2DSC2M1D(data: U[Cell[slice.S]]): Matrix1D = Matrix1D(data.asInstanceOf[U[Cell[Position1D]]])
    implicit def UP2DRMC2M2D(data: U[Cell[slice.R#M]]): Matrix2D = Matrix2D(data.asInstanceOf[U[Cell[Position2D]]])

    val mean = data
      .summarise(slice, Mean[Position2D, slice.S](), stuner)
      .compact(Over(First))

    implicit object P2D extends PosDimDep[Position2D, slice.dimension.type]

    val centered = data
      .transformWithValue(Subtract(ExtractWithDimension[Position2D, Content](slice.dimension)
        .andThenPresent(_.value.asDouble)), mean)

    val denom = centered
      .transform(Power[Position2D](2))
      .summarise(slice, Sum[Position2D, slice.S](), stuner)
      .pairwise(Over(First), Lower,
        Times(Locate.PrependPairwiseSelectedStringToRemainder[Position1D](Over(First), "(%1$s*%2$s)")), ptuner)
      .transform(SquareRoot[Position1D]())
      .compact(Over(First))

    centered
      .pairwise(slice, Lower,
        Times(Locate.PrependPairwiseSelectedStringToRemainder[Position2D](slice, "(%1$s*%2$s)")), ptuner)
      .summarise(Over(First), Sum[Position2D, Position1D](), stuner)
      .transformWithValue(Fraction(ExtractWithDimension[Position1D, Content](First)
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
  def mutualInformation[ST <: Tuner, PT <: Tuner](slice: Slice[Position2D], stuner: ST = Default(),
    ptuner: PT = Default())(implicit ev1: ClassTag[slice.S], ev2: ClassTag[slice.R], ev3: SummariseTuners#V[ST],
      ev4: PairwiseTuners#V[PT]): U[Cell[Position1D]] = {
    implicit def UP2DRMC2M2D(data: U[Cell[slice.R#M]]): Matrix2D = Matrix2D(data.asInstanceOf[U[Cell[Position2D]]])

    val dim = slice match {
      case Over(First) => Second
      case Over(Second) => First
      case Along(d) => d
      case _ => throw new Exception("unexpected dimension")
    }

    implicit object P3D extends PosDimDep[Position3D, dim.type]

    type W = Map[Position1D, Content]

    val extractor = ExtractWithDimension[Position2D, Content](First).andThenPresent(_.value.asDouble)

    val mhist = data
      .relocate(c => Some(c.position.append(c.content.value.toShortString)))
      .summarise(Along(dim), Count[Position3D, Position2D](), stuner)

    val mcount = mhist
      .summarise(Over(First), Sum[Position2D, Position1D](), stuner)
      .compact()

    val marginal = mhist
      .summariseWithValue(Over(First), Entropy[Position2D, Position1D, W](extractor)
        .andThenRelocate(_.position.append("marginal").toOption), mcount, stuner)
      .pairwise(Over(First), Upper,
        Plus(Locate.PrependPairwiseSelectedStringToRemainder[Position2D](Over(First), "%s,%s")), ptuner)

    val jhist = data
      .pairwise(slice, Upper,
        Concatenate(Locate.PrependPairwiseSelectedStringToRemainder[Position2D](slice, "%s,%s")), ptuner)
      .relocate(c => Some(c.position.append(c.content.value.toShortString)))
      .summarise(Along(Second), Count[Position3D, Position2D](), stuner)

    val jcount = jhist
      .summarise(Over(First), Sum[Position2D, Position1D](), stuner)
      .compact()

    val joint = jhist
      .summariseWithValue(Over(First), Entropy[Position2D, Position1D, W](extractor, negate = true)
        .andThenRelocate(_.position.append("joint").toOption), jcount, stuner)

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
  def gini[ST <: Tuner, WT <: Tuner : SlideTuners, PT <: Tuner](slice: Slice[Position2D], stuner: ST = Default(),
    wtuner: WT = Default(), ptuner: PT = Default())(implicit ev1: ClassTag[slice.S], ev2: ClassTag[slice.R],
      ev3: SummariseTuners#V[ST], ev5: PairwiseTuners#V[PT]): U[Cell[Position1D]] = {
    implicit def UP2DSC2M1D(data: U[Cell[slice.S]]): Matrix1D = Matrix1D(data.asInstanceOf[U[Cell[Position1D]]])
    implicit def UP2DSMC2M2D(data: U[Cell[slice.S#M]]): Matrix2D = Matrix2D(data.asInstanceOf[U[Cell[Position2D]]])

    def isPositive = (cell: Cell[Position2D]) => cell.content.value.asDouble.map(_ > 0).getOrElse(false)
    def isNegative = (cell: Cell[Position2D]) => cell.content.value.asDouble.map(_ <= 0).getOrElse(false)

    val extractor = ExtractWithDimension[Position2D, Content](First).andThenPresent(_.value.asDouble)

    val pos = data
      .transform(Compare[Position2D](isPositive))
      .summarise(slice, Sum[Position2D, slice.S](false), stuner)
      .compact(Over(First))

    val neg = data
      .transform(Compare[Position2D](isNegative))
      .summarise(slice, Sum[Position2D, slice.S](false), stuner)
      .compact(Over(First))

    val tpr = data
      .transform(Compare[Position2D](isPositive))
      .slide(slice, CumulativeSum[Position2D, slice.S, slice.R, slice.S#M](Locate.AppendRemainderString()),
        true, wtuner)
      .transformWithValue(Fraction(extractor), pos)
      .slide(Over(First), BinOp[Position2D, Position1D, Position1D, Position2D]((l: Double, r: Double) => r + l,
        Locate.AppendPairwiseString[Position1D, Position1D]("%2$s.%1$s")), true, wtuner)

    val fpr = data
      .transform(Compare[Position2D](isNegative))
      .slide(slice, CumulativeSum[Position2D, slice.S, slice.R, slice.S#M](Locate.AppendRemainderString()),
        true, wtuner)
      .transformWithValue(Fraction(extractor), neg)
      .slide(Over(First), BinOp[Position2D, Position1D, Position1D, Position2D]((l: Double, r: Double) => r - l,
        Locate.AppendPairwiseString[Position1D, Position1D]("%2$s.%1$s")), true, wtuner)

    tpr
      .pairwiseBetween(Along(First), Diagonal, fpr,
        Times(Locate.PrependPairwiseSelectedStringToRemainder[Position2D](Along(First), "(%1$s*%2$s)")), ptuner)
      .summarise(Along(First), Sum[Position2D, Position1D](), stuner)
      .transformWithValue(Subtract(ExtractWithKey[Position1D, Double]("one"), true),
        Map(Position1D("one") -> 1.0))
  }
}

object Matrix extends Consume with DistributedData with Environment {
  def loadText[P <: Position](file: String, parser: Cell.TextParser[P])(
    implicit ctx: C): (U[Cell[P]], U[String]) = {
    val rdd = ctx.context.textFile(file).flatMap { parser(_) }

    (rdd.collect { case Right(c) => c }, rdd.collect { case Left(e) => e })
  }

  def loadSequence[K <: Writable, V <: Writable, P <: Position](file: String, parser: Cell.SequenceParser[K, V, P])(
    implicit ctx: C, ev1: Manifest[K], ev2: Manifest[V]): (U[Cell[P]], U[String]) = {
    val pipe = ctx.context.sequenceFile[K, V](file).flatMap { case (k, v) => parser(k, v) }

    (pipe.collect { case Right(c) => c }, pipe.collect { case Left(e) => e })
  }

  /** Conversion from `U[Cell[Position1D]]` to a Spark `Matrix1D`. */
  implicit def RDD2M1(data: U[Cell[Position1D]]): Matrix1D = Matrix1D(data)
  /** Conversion from `U[Cell[Position2D]]` to a Spark `Matrix2D`. */
  implicit def RDD2M2(data: U[Cell[Position2D]]): Matrix2D = Matrix2D(data)
  /** Conversion from `U[Cell[Position3D]]` to a Spark `Matrix3D`. */
  implicit def RDD2M3(data: U[Cell[Position3D]]): Matrix3D = Matrix3D(data)
  /** Conversion from `U[Cell[Position4D]]` to a Spark `Matrix4D`. */
  implicit def RDD2M4(data: U[Cell[Position4D]]): Matrix4D = Matrix4D(data)
  /** Conversion from `U[Cell[Position5D]]` to a Spark `Matrix5D`. */
  implicit def RDD2M5(data: U[Cell[Position5D]]): Matrix5D = Matrix5D(data)
  /** Conversion from `U[Cell[Position6D]]` to a Spark `Matrix6D`. */
  implicit def RDD2M6(data: U[Cell[Position6D]]): Matrix6D = Matrix6D(data)
  /** Conversion from `U[Cell[Position7D]]` to a Spark `Matrix7D`. */
  implicit def RDD2M7(data: U[Cell[Position7D]]): Matrix7D = Matrix7D(data)
  /** Conversion from `U[Cell[Position8D]]` to a Spark `Matrix8D`. */
  implicit def RDD2M8(data: U[Cell[Position8D]]): Matrix8D = Matrix8D(data)
  /** Conversion from `U[Cell[Position9D]]` to a Spark `Matrix9D`. */
  implicit def RDD2M9(data: U[Cell[Position9D]]): Matrix9D = Matrix9D(data)

  /** Conversion from `List[Cell[Position1D]]` to a Spark `Matrix1D`. */
  implicit def L2RDDM1(data: List[Cell[Position1D]])(implicit ctx: C): Matrix1D = {
    Matrix1D(ctx.context.parallelize(data))
  }
  /** Conversion from `List[Cell[Position2D]]` to a Spark `Matrix2D`. */
  implicit def L2RDDM2(data: List[Cell[Position2D]])(implicit ctx: C): Matrix2D = {
    Matrix2D(ctx.context.parallelize(data))
  }
  /** Conversion from `List[Cell[Position3D]]` to a Spark `Matrix3D`. */
  implicit def L2RDDM3(data: List[Cell[Position3D]])(implicit ctx: C): Matrix3D = {
    Matrix3D(ctx.context.parallelize(data))
  }
  /** Conversion from `List[Cell[Position4D]]` to a Spark `Matrix4D`. */
  implicit def L2RDDM4(data: List[Cell[Position4D]])(implicit ctx: C): Matrix4D = {
    Matrix4D(ctx.context.parallelize(data))
  }
  /** Conversion from `List[Cell[Position5D]]` to a Spark `Matrix5D`. */
  implicit def L2RDDM5(data: List[Cell[Position5D]])(implicit ctx: C): Matrix5D = {
    Matrix5D(ctx.context.parallelize(data))
  }
  /** Conversion from `List[Cell[Position6D]]` to a Spark `Matrix6D`. */
  implicit def L2RDDM6(data: List[Cell[Position6D]])(implicit ctx: C): Matrix6D = {
    Matrix6D(ctx.context.parallelize(data))
  }
  /** Conversion from `List[Cell[Position7D]]` to a Spark `Matrix7D`. */
  implicit def L2RDDM7(data: List[Cell[Position7D]])(implicit ctx: C): Matrix7D = {
    Matrix7D(ctx.context.parallelize(data))
  }
  /** Conversion from `List[Cell[Position8D]]` to a Spark `Matrix8D`. */
  implicit def L2RDDM8(data: List[Cell[Position8D]])(implicit ctx: C): Matrix8D = {
    Matrix8D(ctx.context.parallelize(data))
  }
  /** Conversion from `List[Cell[Position9D]]` to a Spark `Matrix9D`. */
  implicit def L2RDDM9(data: List[Cell[Position9D]])(implicit ctx: C): Matrix9D = {
    Matrix9D(ctx.context.parallelize(data))
  }

  /** Conversion from `List[(Valueable, Content)]` to a Spark `Matrix1D`. */
  implicit def LV1C2RDDM1[V <% Valueable](list: List[(V, Content)])(implicit ctx: C): Matrix1D = {
    Matrix1D(ctx.context.parallelize(list.map { case (v, c) => Cell(Position1D(v), c) }))
  }
  /** Conversion from `List[(Valueable, Valueable, Content)]` to a Spark `Matrix2D`. */
  implicit def LV2C2RDDM2[V <% Valueable, W <% Valueable](list: List[(V, W, Content)])(implicit ctx: C): Matrix2D = {
    Matrix2D(ctx.context.parallelize(list.map { case (v, w, c) => Cell(Position2D(v, w), c) }))
  }
  /** Conversion from `List[(Valueable, Valueable, Valueable, Content)]` to a Spark `Matrix3D`. */
  implicit def LV3C2RDDM3[V <% Valueable, W <% Valueable, X <% Valueable](
    list: List[(V, W, X, Content)])(implicit ctx: C): Matrix3D = {
    Matrix3D(ctx.context.parallelize(list.map { case (v, w, x, c) => Cell(Position3D(v, w, x), c) }))
  }
  /** Conversion from `List[(Valueable, Valueable, Valueable, Valueable, Content)]` to a Spark `Matrix4D`. */
  implicit def LV4C2RDDM4[V <% Valueable, W <% Valueable, X <% Valueable, Y <% Valueable](
    list: List[(V, W, X, Y, Content)])(implicit ctx: C): Matrix4D = {
    Matrix4D(ctx.context.parallelize(list.map { case (v, w, x, y, c) => Cell(Position4D(v, w, x, y), c) }))
  }
  /**
   * Conversion from `List[(Valueable, Valueable, Valueable, Valueable, Valueable, Content)]` to a Spark `Matrix5D`.
   */
  implicit def LV5C2RDDM5[V <% Valueable, W <% Valueable, X <% Valueable, Y <% Valueable, Z <% Valueable](
    list: List[(V, W, X, Y, Z, Content)])(implicit ctx: C): Matrix5D = {
    Matrix5D(ctx.context.parallelize(list.map { case (v, w, x, y, z, c) => Cell(Position5D(v, w, x, y, z), c) }))
  }
  /**
   * Conversion from `List[(Valueable, Valueable, Valueable, Valueable, Valueable, Valueable, Content)]` to a
   * Spark `Matrix6D`.
   */
  implicit def LV6C2RDDM6[T <% Valueable, V <% Valueable, W <% Valueable, X <% Valueable, Y <% Valueable, Z <% Valueable](
    list: List[(T, V, W, X, Y, Z, Content)])(implicit ctx: C): Matrix6D = {
    Matrix6D(ctx.context.parallelize(list.map { case (t, v, w, x, y, z, c) => Cell(Position6D(t, v, w, x, y, z), c) }))
  }
  /**
   * Conversion from `List[(Valueable, Valueable, Valueable, Valueable, Valueable, Valueable, Valueable, Content)]` to a
   * Spark `Matrix7D`.
   */
  implicit def LV7C2RDDM7[S <% Valueable, T <% Valueable, V <% Valueable, W <% Valueable, X <% Valueable, Y <% Valueable, Z <% Valueable](
    list: List[(S, T, V, W, X, Y, Z, Content)])(implicit ctx: C): Matrix7D = {
    Matrix7D(ctx.context.parallelize(list.map {
      case (s, t, v, w, x, y, z, c) => Cell(Position7D(s, t, v, w, x, y, z), c)
    }))
  }
  /**
   * Conversion from `List[(Valueable, Valueable, Valueable, Valueable, Valueable, Valueable, Valueable, Valueable,
   * Content)]` to a Spark `Matrix8D`.
   */
  implicit def LV8C2RDDM8[R <% Valueable, S <% Valueable, T <% Valueable, V <% Valueable, W <% Valueable, X <% Valueable, Y <% Valueable, Z <% Valueable](
    list: List[(R, S, T, V, W, X, Y, Z, Content)])(implicit ctx: C): Matrix8D = {
    Matrix8D(ctx.context.parallelize(list.map {
      case (r, s, t, v, w, x, y, z, c) => Cell(Position8D(r, s, t, v, w, x, y, z), c)
    }))
  }
  /**
   * Conversion from `List[(Valueable, Valueable, Valueable, Valueable, Valueable, Valueable, Valueable, Valueable,
   * Valueable, Content)]` to a Spark `Matrix9D`.
   */
  implicit def LV9C2RDDM9[Q <% Valueable, R <% Valueable, S <% Valueable, T <% Valueable, V <% Valueable, W <% Valueable, X <% Valueable, Y <% Valueable, Z <% Valueable](
    list: List[(Q, R, S, T, V, W, X, Y, Z, Content)])(implicit ctx: C): Matrix9D = {
    Matrix9D(ctx.context.parallelize(list.map {
      case (q, r, s, t, v, w, x, y, z, c) => Cell(Position9D(q, r, s, t, v, w, x, y, z), c)
    }))
  }

  /** Conversion from matrix with errors tuple to `MatrixWithParseErrors`. */
  implicit def RDD2MWPE[P <: Position](t: (U[Cell[P]], U[String])): MatrixWithParseErrors[P, RDD] = {
    MatrixWithParseErrors(t._1, t._2)
  }
}

/**
 * Rich wrapper around a `RDD[Cell[Position1D]]`.
 *
 * @param data `RDD[Cell[Position1D]]`.
 */
case class Matrix1D(data: RDD[Cell[Position1D]]) extends FwMatrix1D with Matrix[Position1D]
  with ApproximateDistribution[Position1D] {
  def domain[T <: Tuner : DomainTuners](tuner: T = Default()): U[Position1D] = names(Over(First))

  def saveAsIV(file: String, dictionary: String, separator: String)(implicit ctx: C): U[Cell[Position1D]] = {
    data
      .keyBy { case c => c.position }
      .join(saveDictionary(Over(First), file, dictionary, separator))
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
case class Matrix2D(data: RDD[Cell[Position2D]]) extends FwMatrix2D with Matrix[Position2D]
  with ReduceableMatrix[Position2D] with ReshapeableMatrix[Position2D] with MatrixDistance
    with ApproximateDistribution[Position2D] {
  def domain[T <: Tuner : DomainTuners](tuner: T = Default()): U[Position2D] = {
    names(Over(First))
      .map { case Position1D(c) => c }
      .cartesian(names(Over(Second)).map { case Position1D(c) => c })
      .map { case (c1, c2) => Position2D(c1, c2) }
  }

  def permute(dim1: Dimension, dim2: Dimension)(implicit ev1: PosDimDep[Position2D, dim1.D],
    ev2: PosDimDep[Position2D, dim2.D], ne: dim1.D =!= dim2.D): U[Cell[Position2D]] = {
    data.map { case Cell(p, c) => Cell(p.permute(List(dim1, dim2)), c) }
  }

  def saveAsCSV(slice: Slice[Position2D], file: String, separator: String, escapee: Escape, writeHeader: Boolean,
    header: String, writeRowId: Boolean, rowId: String)(implicit ctx: C, ct: ClassTag[slice.S]): U[Cell[Position2D]] = {
    val escape = (str: String) => escapee.escape(str)
    val columns = data
      .map { case c => ((), HashSet(escape(slice.remainder(c.position)(First).toShortString))) }
      .reduceByKey(_ ++ _)
      .map { case (_, set) => set.toList.sorted }

    if (writeHeader) {
      columns
        .map { case lst => (if (writeRowId) escape(rowId) + separator else "") + lst.mkString(separator) }
        .saveAsTextFile(header.format(file))
    }

    val cols = columns
      .first

    data
      .map {
        case Cell(p, c) => (slice.selected(p)(First).toShortString,
          Map(escape(slice.remainder(p)(First).toShortString) -> escape(c.value.toShortString)))
      }
      .reduceByKey(_ ++ _)
      .map {
        case (key, values) => (if (writeRowId) escape(key) + separator else "") +
          cols.map { case c => values.getOrElse(c, "") }.mkString(separator)
      }
      .saveAsTextFile(file)

    data
  }

  def saveAsIV(file: String, dictionary: String, separator: String)(implicit ctx: C): U[Cell[Position2D]] = {
    data
      .keyBy { case c => Position1D(c.position(First)) }
      .join(saveDictionary(Over(First), file, dictionary, separator))
      .values
      .keyBy { case (c, i) => Position1D(c.position(Second)) }
      .join(saveDictionary(Over(Second), file, dictionary, separator))
      .map { case (_, ((c, i), j)) => i + separator + j + separator + c.content.value.toShortString }
      .saveAsTextFile(file)

    data
  }

  def saveAsVW(slice: Slice[Position2D], file: String, dictionary: String, tag: Boolean, separator: String)(
    implicit ctx: C, ct: ClassTag[slice.S]): U[Cell[Position2D]] = {
    saveAsVW(slice, file, None, None, tag, dictionary, separator)
  }

  def saveAsVWWithLabels(slice: Slice[Position2D], file: String, labels: U[Cell[Position1D]], dictionary: String,
    tag: Boolean, separator: String)(implicit ctx: C, ct: ClassTag[slice.S]): U[Cell[Position2D]] = {
    saveAsVW(slice, file, Some(labels), None, tag, dictionary, separator)
  }

  def saveAsVWWithImportance(slice: Slice[Position2D], file: String, importance: U[Cell[Position1D]],
    dictionary: String, tag: Boolean, separator: String)(implicit ctx: C,
      ct: ClassTag[slice.S]): U[Cell[Position2D]] = {
    saveAsVW(slice, file, None, Some(importance), tag, dictionary, separator)
  }

  def saveAsVWWithLabelsAndImportance(slice: Slice[Position2D], file: String, labels: U[Cell[Position1D]],
    importance: U[Cell[Position1D]], dictionary: String, tag: Boolean, separator: String)(implicit ctx: C,
      ct: ClassTag[slice.S]): U[Cell[Position2D]] = {
    saveAsVW(slice, file, Some(labels), Some(importance), tag, dictionary, separator)
  }

  private def saveAsVW(slice: Slice[Position2D], file: String, labels: Option[U[Cell[Position1D]]],
    importance: Option[U[Cell[Position1D]]], tag: Boolean, dictionary: String, separator: String)(
      implicit ev: ClassTag[slice.S]): U[Cell[Position2D]] = {
    val dict = data
      .map { c => slice.remainder(c.position)(First).toShortString }
      .distinct
      .zipWithIndex

    dict
      .map { case (s, i) => s + separator + i }
      .saveAsTextFile(dictionary.format(file))

    val features = data
      .keyBy { c => slice.remainder(c.position)(First).toShortString }
      .join(dict)
      .flatMap {
        case (_, (c, i)) => c.content.value.asDouble.map { case v => (slice.selected(c.position), (i, v)) }
      }
      .groupByKey
      .map {
        case (p, itr) => (p, itr
          .toList
          .sortBy { case (i, _) => i }
          .foldLeft("|") { case (b, (i, v)) => b + " " + i + ":" + v })
      }

    val tagged = tag match {
      case true => features.map { case (p, s) => (p, p(First).toShortString + s) }
      case false => features
    }

    val weighted = importance match {
      case Some(imp) => tagged
        .join(imp.keyBy { case c => c.position.asInstanceOf[slice.S] })
        .flatMap { case (p, (s, c)) => c.content.value.asDouble.map { case i => (p, i + " " + s) } }
      case None => tagged
    }

    val examples = labels match {
      case Some(lab) => weighted
        .join(lab.keyBy { case c => c.position.asInstanceOf[slice.S] })
        .flatMap { case (p, (s, c)) => c.content.value.asDouble.map { case l => (p, l + " " + s) } }
      case None => weighted
    }

    examples
      .map { case (p, s) => s }
      .saveAsTextFile(file)

    data
  }
}

/**
 * Rich wrapper around a `RDD[Cell[Position3D]]`.
 *
 * @param data `RDD[Cell[Position3D]]`.
 */
case class Matrix3D(data: RDD[Cell[Position3D]]) extends FwMatrix3D with Matrix[Position3D]
  with ReduceableMatrix[Position3D] with ReshapeableMatrix[Position3D] with ApproximateDistribution[Position3D] {
  def domain[T <: Tuner : DomainTuners](tuner: T = Default()): U[Position3D] = {
    names(Over(First))
      .map { case Position1D(c) => c }
      .cartesian(names(Over(Second)).map { case Position1D(c) => c })
      .cartesian(names(Over(Third)).map { case Position1D(c) => c })
      .map { case ((c1, c2), c3) => Position3D(c1, c2, c3) }
  }

  def permute(dim1: Dimension, dim2: Dimension, dim3: Dimension)(implicit ev1: PosDimDep[Position3D, dim1.D],
    ev2: PosDimDep[Position3D, dim2.D], ev3: PosDimDep[Position3D, dim3.D],
      ev4: Distinct3[dim1.D, dim2.D, dim3.D]): U[Cell[Position3D]] = {
    data.map { case Cell(p, c) => Cell(p.permute(List(dim1, dim2, dim3)), c) }
  }

  def saveAsIV(file: String, dictionary: String, separator: String)(implicit ctx: C): U[Cell[Position3D]] = {
    data
      .keyBy { case c => Position1D(c.position(First)) }
      .join(saveDictionary(Over(First), file, dictionary, separator))
      .values
      .keyBy { case (c, i) => Position1D(c.position(Second)) }
      .join(saveDictionary(Over(Second), file, dictionary, separator))
      .map { case (_, ((c, i), j)) => (c, i, j) }
      .keyBy { case (c, i, j) => Position1D(c.position(Third)) }
      .join(saveDictionary(Over(Third), file, dictionary, separator))
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
case class Matrix4D(data: RDD[Cell[Position4D]]) extends FwMatrix4D with Matrix[Position4D]
  with ReduceableMatrix[Position4D] with ReshapeableMatrix[Position4D] with ApproximateDistribution[Position4D] {
  def domain[T <: Tuner : DomainTuners](tuner: T = Default()): U[Position4D] = {
    names(Over(First))
      .map { case Position1D(c) => c }
      .cartesian(names(Over(Second)).map { case Position1D(c) => c })
      .cartesian(names(Over(Third)).map { case Position1D(c) => c })
      .cartesian(names(Over(Fourth)).map { case Position1D(c) => c })
      .map { case (((c1, c2), c3), c4) => Position4D(c1, c2, c3, c4) }
  }

  def permute(dim1: Dimension, dim2: Dimension, dim3: Dimension, dim4: Dimension)(
    implicit ev1: PosDimDep[Position4D, dim1.D], ev2: PosDimDep[Position4D, dim2.D],
      ev3: PosDimDep[Position4D, dim3.D], ev4: PosDimDep[Position4D, dim4.D],
        ev5: Distinct4[dim1.D, dim2.D, dim3.D, dim4.D]): U[Cell[Position4D]] = {
    data.map { case Cell(p, c) => Cell(p.permute(List(dim1, dim2, dim3, dim4)), c) }
  }

  def saveAsIV(file: String, dictionary: String, separator: String)(implicit ctx: C): U[Cell[Position4D]] = {
    data
      .keyBy { case c => Position1D(c.position(First)) }
      .join(saveDictionary(Over(First), file, dictionary, separator))
      .values
      .keyBy { case (c, i) => Position1D(c.position(Second)) }
      .join(saveDictionary(Over(Second), file, dictionary, separator))
      .map { case (_, ((c, i), j)) => (c, i, j) }
      .keyBy { case (c, i, j) => Position1D(c.position(Third)) }
      .join(saveDictionary(Over(Third), file, dictionary, separator))
      .map { case (_, ((c, i, j), k)) => (c, i, j, k) }
      .keyBy { case (c, i, j, p) => Position1D(c.position(Fourth)) }
      .join(saveDictionary(Over(Fourth), file, dictionary, separator))
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
case class Matrix5D(data: RDD[Cell[Position5D]]) extends FwMatrix5D with Matrix[Position5D]
  with ReduceableMatrix[Position5D] with ReshapeableMatrix[Position5D] with ApproximateDistribution[Position5D] {
  def domain[T <: Tuner : DomainTuners](tuner: T = Default()): U[Position5D] = {
    names(Over(First))
      .map { case Position1D(c) => c }
      .cartesian(names(Over(Second)).map { case Position1D(c) => c })
      .cartesian(names(Over(Third)).map { case Position1D(c) => c })
      .cartesian(names(Over(Fourth)).map { case Position1D(c) => c })
      .cartesian(names(Over(Fifth)).map { case Position1D(c) => c })
      .map { case ((((c1, c2), c3), c4), c5) => Position5D(c1, c2, c3, c4, c5) }
  }

  def permute(dim1: Dimension, dim2: Dimension, dim3: Dimension, dim4: Dimension, dim5: Dimension)(
    implicit ev1: PosDimDep[Position5D, dim1.D], ev2: PosDimDep[Position5D, dim2.D],
      ev3: PosDimDep[Position5D, dim3.D], ev4: PosDimDep[Position5D, dim4.D], ev5: PosDimDep[Position5D, dim5.D],
        ev6: Distinct5[dim1.D, dim2.D, dim3.D, dim4.D, dim5.D]): U[Cell[Position5D]] = {
    data.map { case Cell(p, c) => Cell(p.permute(List(dim1, dim2, dim3, dim4, dim5)), c) }
  }

  def saveAsIV(file: String, dictionary: String, separator: String)(implicit ctx: C): U[Cell[Position5D]] = {
    data
      .keyBy { case c => Position1D(c.position(First)) }
      .join(saveDictionary(Over(First), file, dictionary, separator))
      .values
      .keyBy { case (c, i) => Position1D(c.position(Second)) }
      .join(saveDictionary(Over(Second), file, dictionary, separator))
      .map { case (_, ((c, i), j)) => (c, i, j) }
      .keyBy { case (c, pi, pj) => Position1D(c.position(Third)) }
      .join(saveDictionary(Over(Third), file, dictionary, separator))
      .map { case (_, ((c, i, j), k)) => (c, i, j, k) }
      .keyBy { case (c, i, j, k) => Position1D(c.position(Fourth)) }
      .join(saveDictionary(Over(Fourth), file, dictionary, separator))
      .map { case (_, ((c, i, j, k), l)) => (c, i, j, k, l) }
      .keyBy { case (c, i, j, k, l) => Position1D(c.position(Fifth)) }
      .join(saveDictionary(Over(Fifth), file, dictionary, separator))
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
case class Matrix6D(data: RDD[Cell[Position6D]]) extends FwMatrix6D with Matrix[Position6D]
  with ReduceableMatrix[Position6D] with ReshapeableMatrix[Position6D] with ApproximateDistribution[Position6D] {
  def domain[T <: Tuner : DomainTuners](tuner: T = Default()): U[Position6D] = {
    names(Over(First))
      .map { case Position1D(c) => c }
      .cartesian(names(Over(Second)).map { case Position1D(c) => c })
      .cartesian(names(Over(Third)).map { case Position1D(c) => c })
      .cartesian(names(Over(Fourth)).map { case Position1D(c) => c })
      .cartesian(names(Over(Fifth)).map { case Position1D(c) => c })
      .cartesian(names(Over(Sixth)).map { case Position1D(c) => c })
      .map { case (((((c1, c2), c3), c4), c5), c6) => Position6D(c1, c2, c3, c4, c5, c6) }
  }


  def permute(dim1: Dimension, dim2: Dimension, dim3: Dimension, dim4: Dimension, dim5: Dimension, dim6: Dimension)(
    implicit ev1: PosDimDep[Position6D, dim1.D], ev2: PosDimDep[Position6D, dim2.D],
      ev3: PosDimDep[Position6D, dim3.D], ev4: PosDimDep[Position6D, dim4.D],
        ev5: PosDimDep[Position6D, dim5.D], ev6: PosDimDep[Position6D, dim6.D],
          ev7: Distinct6[dim1.D, dim2.D, dim3.D, dim4.D, dim5.D, dim6.D]): U[Cell[Position6D]] = {
    data.map { case Cell(p, c) => Cell(p.permute(List(dim1, dim2, dim3, dim4, dim5, dim6)), c) }
  }

  def saveAsIV(file: String, dictionary: String, separator: String)(implicit ctx: C): U[Cell[Position6D]] = {
    data
      .keyBy { case c => Position1D(c.position(First)) }
      .join(saveDictionary(Over(First), file, dictionary, separator))
      .values
      .keyBy { case (c, i) => Position1D(c.position(Second)) }
      .join(saveDictionary(Over(Second), file, dictionary, separator))
      .map { case (_, ((c, i), j)) => (c, i, j) }
      .keyBy { case (c, pi, pj) => Position1D(c.position(Third)) }
      .join(saveDictionary(Over(Third), file, dictionary, separator))
      .map { case (_, ((c, i, j), k)) => (c, i, j, k) }
      .keyBy { case (c, i, j, k) => Position1D(c.position(Fourth)) }
      .join(saveDictionary(Over(Fourth), file, dictionary, separator))
      .map { case (_, ((c, i, j, k), l)) => (c, i, j, k, l) }
      .keyBy { case (c, i, j, k, l) => Position1D(c.position(Fifth)) }
      .join(saveDictionary(Over(Fifth), file, dictionary, separator))
      .map { case (_, ((c, i, j, k, l), m)) => (c, i, j, k, l, m) }
      .keyBy { case (c, i, j, k, l, m) => Position1D(c.position(Sixth)) }
      .join(saveDictionary(Over(Sixth), file, dictionary, separator))
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
case class Matrix7D(data: RDD[Cell[Position7D]]) extends FwMatrix7D with Matrix[Position7D]
  with ReduceableMatrix[Position7D] with ReshapeableMatrix[Position7D] with ApproximateDistribution[Position7D] {
  def domain[T <: Tuner : DomainTuners](tuner: T = Default()): U[Position7D] = {
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

  def permute(dim1: Dimension, dim2: Dimension, dim3: Dimension, dim4: Dimension, dim5: Dimension, dim6: Dimension,
    dim7: Dimension)(implicit ev1: PosDimDep[Position7D, dim1.D], ev2: PosDimDep[Position7D, dim2.D],
      ev3: PosDimDep[Position7D, dim3.D], ev4: PosDimDep[Position7D, dim4.D], ev5: PosDimDep[Position7D, dim5.D],
        ev6: PosDimDep[Position7D, dim6.D], ev7: PosDimDep[Position7D, dim7.D],
          ev8: Distinct7[dim1.D, dim2.D, dim3.D, dim4.D, dim5.D, dim6.D, dim7.D]): U[Cell[Position7D]] = {
    data.map { case Cell(p, c) => Cell(p.permute(List(dim1, dim2, dim3, dim4, dim5, dim6, dim7)), c) }
  }

  def saveAsIV(file: String, dictionary: String, separator: String)(implicit ctx: C): U[Cell[Position7D]] = {
    data
      .keyBy { case c => Position1D(c.position(First)) }
      .join(saveDictionary(Over(First), file, dictionary, separator))
      .values
      .keyBy { case (c, i) => Position1D(c.position(Second)) }
      .join(saveDictionary(Over(Second), file, dictionary, separator))
      .map { case (_, ((c, i), j)) => (c, i, j) }
      .keyBy { case (c, pi, pj) => Position1D(c.position(Third)) }
      .join(saveDictionary(Over(Third), file, dictionary, separator))
      .map { case (_, ((c, i, j), k)) => (c, i, j, k) }
      .keyBy { case (c, i, j, k) => Position1D(c.position(Fourth)) }
      .join(saveDictionary(Over(Fourth), file, dictionary, separator))
      .map { case (_, ((c, i, j, k), l)) => (c, i, j, k, l) }
      .keyBy { case (c, i, j, k, l) => Position1D(c.position(Fifth)) }
      .join(saveDictionary(Over(Fifth), file, dictionary, separator))
      .map { case (_, ((c, i, j, k, l), m)) => (c, i, j, k, l, m) }
      .keyBy { case (c, i, j, k, l, m) => Position1D(c.position(Sixth)) }
      .join(saveDictionary(Over(Sixth), file, dictionary, separator))
      .map { case (_, ((c, i, j, k, l, m), n)) => (c, i, j, k, l, m, n) }
      .keyBy { case (c, i, j, k, l, m, n) => Position1D(c.position(Seventh)) }
      .join(saveDictionary(Over(Seventh), file, dictionary, separator))
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
case class Matrix8D(data: RDD[Cell[Position8D]]) extends FwMatrix8D with Matrix[Position8D]
  with ReduceableMatrix[Position8D] with ReshapeableMatrix[Position8D] with ApproximateDistribution[Position8D] {
  def domain[T <: Tuner : DomainTuners](tuner: T = Default()): U[Position8D] = {
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

  def permute(dim1: Dimension, dim2: Dimension, dim3: Dimension, dim4: Dimension, dim5: Dimension, dim6: Dimension,
    dim7: Dimension, dim8: Dimension)(implicit ev1: PosDimDep[Position8D, dim1.D], ev2: PosDimDep[Position8D, dim2.D],
      ev3: PosDimDep[Position8D, dim3.D], ev4: PosDimDep[Position8D, dim4.D], ev5: PosDimDep[Position8D, dim5.D],
        ev6: PosDimDep[Position8D, dim6.D], ev7: PosDimDep[Position8D, dim7.D], ev8: PosDimDep[Position8D, dim8.D],
            ev9: Distinct8[dim1.D, dim2.D, dim3.D, dim4.D, dim5.D, dim6.D, dim7.D, dim8.D]): U[Cell[Position8D]] = {
    data.map { case Cell(p, c) => Cell(p.permute(List(dim1, dim2, dim3, dim4, dim5, dim6, dim7, dim8)), c) }
  }

  def saveAsIV(file: String, dictionary: String, separator: String)(implicit ctx: C): U[Cell[Position8D]] = {
    data
      .keyBy { case c => Position1D(c.position(First)) }
      .join(saveDictionary(Over(First), file, dictionary, separator))
      .values
      .keyBy { case (c, i) => Position1D(c.position(Second)) }
      .join(saveDictionary(Over(Second), file, dictionary, separator))
      .map { case (_, ((c, i), j)) => (c, i, j) }
      .keyBy { case (c, pi, pj) => Position1D(c.position(Third)) }
      .join(saveDictionary(Over(Third), file, dictionary, separator))
      .map { case (_, ((c, i, j), k)) => (c, i, j, k) }
      .keyBy { case (c, i, j, k) => Position1D(c.position(Fourth)) }
      .join(saveDictionary(Over(Fourth), file, dictionary, separator))
      .map { case (_, ((c, i, j, k), l)) => (c, i, j, k, l) }
      .keyBy { case (c, i, j, k, l) => Position1D(c.position(Fifth)) }
      .join(saveDictionary(Over(Fifth), file, dictionary, separator))
      .map { case (_, ((c, i, j, k, l), m)) => (c, i, j, k, l, m) }
      .keyBy { case (c, i, j, k, l, m) => Position1D(c.position(Sixth)) }
      .join(saveDictionary(Over(Sixth), file, dictionary, separator))
      .map { case (_, ((c, i, j, k, l, m), n)) => (c, i, j, k, l, m, n) }
      .keyBy { case (c, i, j, k, l, m, n) => Position1D(c.position(Seventh)) }
      .join(saveDictionary(Over(Seventh), file, dictionary, separator))
      .map { case (_, ((c, i, j, k, l, m, n), o)) => (c, i, j, k, l, m, n, o) }
      .keyBy { case (c, i, j, k, l, m, n, o) => Position1D(c.position(Eighth)) }
      .join(saveDictionary(Over(Eighth), file, dictionary, separator))
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
case class Matrix9D(data: RDD[Cell[Position9D]]) extends FwMatrix9D with Matrix[Position9D]
  with ReduceableMatrix[Position9D] with ApproximateDistribution[Position9D] {
  def domain[T <: Tuner : DomainTuners](tuner: T = Default()): U[Position9D] = {
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

  def permute(dim1: Dimension, dim2: Dimension, dim3: Dimension, dim4: Dimension, dim5: Dimension, dim6: Dimension,
    dim7: Dimension, dim8: Dimension, dim9: Dimension)(implicit ev1: PosDimDep[Position9D, dim1.D],
      ev2: PosDimDep[Position9D, dim2.D], ev3: PosDimDep[Position9D, dim3.D], ev4: PosDimDep[Position9D, dim4.D],
        ev5: PosDimDep[Position9D, dim5.D], ev6: PosDimDep[Position9D, dim6.D], ev7: PosDimDep[Position9D, dim7.D],
          ev8: PosDimDep[Position9D, dim8.D], ev9: PosDimDep[Position9D, dim9.D],
            ev10: Distinct9[dim1.D, dim2.D, dim3.D, dim4.D, dim5.D, dim6.D, dim7.D, dim8.D, dim9.D]): U[Cell[Position9D]] = {
    data.map { case Cell(p, c) => Cell(p.permute(List(dim1, dim2, dim3, dim4, dim5, dim6, dim7, dim8, dim9)), c) }
  }

  def saveAsIV(file: String, dictionary: String, separator: String)(implicit ctx: C): U[Cell[Position9D]] = {
    data
      .keyBy { case c => Position1D(c.position(First)) }
      .join(saveDictionary(Over(First), file, dictionary, separator))
      .values
      .keyBy { case (c, i) => Position1D(c.position(Second)) }
      .join(saveDictionary(Over(Second), file, dictionary, separator))
      .map { case (_, ((c, i), j)) => (c, i, j) }
      .keyBy { case (c, pi, pj) => Position1D(c.position(Third)) }
      .join(saveDictionary(Over(Third), file, dictionary, separator))
      .map { case (_, ((c, i, j), k)) => (c, i, j, k) }
      .keyBy { case (c, i, j, k) => Position1D(c.position(Fourth)) }
      .join(saveDictionary(Over(Fourth), file, dictionary, separator))
      .map { case (_, ((c, i, j, k), l)) => (c, i, j, k, l) }
      .keyBy { case (c, i, j, k, l) => Position1D(c.position(Fifth)) }
      .join(saveDictionary(Over(Fifth), file, dictionary, separator))
      .map { case (_, ((c, i, j, k, l), m)) => (c, i, j, k, l, m) }
      .keyBy { case (c, i, j, k, l, m) => Position1D(c.position(Sixth)) }
      .join(saveDictionary(Over(Sixth), file, dictionary, separator))
      .map { case (_, ((c, i, j, k, l, m), n)) => (c, i, j, k, l, m, n) }
      .keyBy { case (c, i, j, k, l, m, n) => Position1D(c.position(Seventh)) }
      .join(saveDictionary(Over(Seventh), file, dictionary, separator))
      .map { case (_, ((c, i, j, k, l, m, n), o)) => (c, i, j, k, l, m, n, o) }
      .keyBy { case (c, i, j, k, l, m, n, o) => Position1D(c.position(Eighth)) }
      .join(saveDictionary(Over(Eighth), file, dictionary, separator))
      .map { case (_, ((c, i, j, k, l, m, n, o), p)) => (c, i, j, k, l, m, n, o, p) }
      .keyBy { case (c, i, j, k, l, m, n, o, p) => Position1D(c.position(Ninth)) }
      .join(saveDictionary(Over(Ninth), file, dictionary, separator))
      .map {
        case (_, ((c, i, j, k, l, m, n, o, p), q)) =>
          i + separator + j + separator + k + separator + l + separator + m + separator +
            n + separator + o + separator + p + separator + q + separator + c.content.value.toShortString
      }
      .saveAsTextFile(file)

    data
  }
}

/** Spark companion object for the `Matrixable` trait. */
object Matrixable {
  /** Converts a `RDD[Cell[P]]` into a `RDD[Cell[P]]`; that is, it is a  pass through. */
  implicit def RDDC2RDDM[P <: Position](t: RDD[Cell[P]]): FwMatrixable[P, RDD] = {
    new FwMatrixable[P, RDD] { def apply(): RDD[Cell[P]] = t }
  }

  /** Converts a `List[Cell[P]]` into a `RDD[Cell[P]]`. */
  implicit def LC2RDDM[P <: Position](t: List[Cell[P]])(implicit ctx: Context,
    ct: ClassTag[P]): FwMatrixable[P, RDD] = {
    new FwMatrixable[P, RDD] { def apply(): RDD[Cell[P]] = ctx.context.parallelize(t) }
  }

  /** Converts a `Cell[P]` into a `RDD[Cell[P]]`. */
  implicit def C2RDDM[P <: Position](t: Cell[P])(implicit ctx: Context, ct: ClassTag[P]): FwMatrixable[P, RDD] = {
    new FwMatrixable[P, RDD] { def apply(): RDD[Cell[P]] = ctx.context.parallelize(List(t)) }
  }
}

/** Spark companion object for the `Predicateable` type class. */
object Predicateable {
  /**
   * Converts a `List[(PositionDistributable[I, S, U], Cell.Predicate[P])]` to a `List[(U[S], Cell.Predicate[P])]`.
   */
  implicit def PDPT2LTPP[I, P <: Position, S <: Position](
    implicit ev: PositionDistributable[I, S, RDD]): FwPredicateable[(I, Cell.Predicate[P]), P, S, RDD] = {
    new FwPredicateable[(I, Cell.Predicate[P]), P, S, RDD] {
      def convert(t: (I, Cell.Predicate[P])): List[(RDD[S], Cell.Predicate[P])] = List((ev.convert(t._1), t._2))
    }
  }

  /**
   * Converts a `(PositionDistributable[I, S, U], Cell.Predicate[P])` to a `List[(U[S], Cell.Predicate[P])]`.
   */
  implicit def LPDP2LTPP[I, P <: Position, S <: Position](
    implicit ev: PositionDistributable[I, S, RDD]): FwPredicateable[List[(I, Cell.Predicate[P])], P, S, RDD] = {
    new FwPredicateable[List[(I, Cell.Predicate[P])], P, S, RDD] {
      def convert(t: List[(I, Cell.Predicate[P])]): List[(RDD[S], Cell.Predicate[P])] = {
        t.map { case (i, p) => (ev.convert(i), p) }
      }
    }
  }
}

