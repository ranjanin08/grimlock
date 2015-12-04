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
  Collate,
  Default,
  ExpandableMatrix => BaseExpandableMatrix,
  ExtractWithDimension,
  ExtractWithKey,
  InMemory,
  Locate,
  Matrix => BaseMatrix,
  Matrixable => BaseMatrixable,
  MatrixWithParseErrors,
  NoParameters,
  Predicateable => BasePredicateable,
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

import cascading.flow.FlowDef
import com.twitter.scalding.{ Mode, TextLine, WritableSequenceFile }
import com.twitter.scalding.typed.{ IterablePipe, Grouped, TypedPipe, TypedSink, ValuePipe }

import java.io.{ File, OutputStreamWriter, PrintWriter }
import java.lang.{ ProcessBuilder, Thread }
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.attribute.PosixFilePermissions
import java.nio.file.{ Files, Paths }

import org.apache.hadoop.io.Writable

import scala.collection.immutable.HashSet
import scala.collection.JavaConverters._
import scala.io.Source
import scala.reflect.ClassTag

private[scalding] object ScaldingImplicits {
  implicit class GroupedTuner[K <: Position, V](grouped: Grouped[K, V]) {
    private implicit def serialisePosition[T <: Position](key: T): Array[Byte] = {
      key.toShortString("|").toCharArray.map(_.toByte)
    }

    def redistribute(parameters: TunerParameters): TypedPipe[(K, V)] = {
      parameters match {
        case Redistribute(reducers) => grouped.withReducers(reducers).forceToReducers
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

    def tuneReducers(parameters: TunerParameters): Grouped[K, V] = {
      parameters match {
        case Reducers(reducers) => grouped.withReducers(reducers)
        case _ => grouped
      }
    }
  }

  implicit class PipeTuner[P](pipe: TypedPipe[P]) {
    def collate(parameters: TunerParameters): TypedPipe[P] = {
      parameters match {
        case Collate => pipe.forceToDisk
        case _ => pipe
      }
    }

    def mapSideJoin[V, Q](value: ValuePipe[V], f: (P, V) => TraversableOnce[Q], empty: V): TypedPipe[Q] = {
      pipe.flatMapWithValue(value) {
        case (c, Some(v)) => f(c, v)
        case (c, None) => f(c, empty)
      }
    }

    def redistribute(parameters: TunerParameters): TypedPipe[P] = {
      parameters match {
        case Redistribute(reducers) =>
          Grouped(pipe.map { case p => (scala.util.Random.nextInt(reducers), p) })
            .withReducers(reducers)
            .forceToReducers
            .values
        case _ => pipe
      }
    }

    def toHashSetValue[Q](f: (P) => Q): ValuePipe[HashSet[Q]] = {
      val semigroup = new com.twitter.algebird.Semigroup[HashSet[Q]] {
        def plus(l: HashSet[Q], r: HashSet[Q]): HashSet[Q] = l ++ r
      }

      pipe.map { case p => HashSet(f(p)) }.sum(semigroup)
    }

    def toListValue[Q](f: (P) => Q): ValuePipe[List[Q]] = pipe.map { case p => List(f(p)) }.sum

    def tunedDistinct(parameters: TunerParameters)(implicit ev: Ordering[P]): TypedPipe[P] = {
      parameters match {
        case Reducers(reducers) => pipe.asKeys.withReducers(reducers).sum.keys
        case _ => pipe.asKeys.sum.keys
      }
    }
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
  def change[I, T <: Tuner](slice: Slice[P], positions: I, schema: Schema, tuner: T = InMemory())(
    implicit ev1: PositionDistributable[I, slice.S, TypedPipe], ev2: ClassTag[slice.S],
      ev3: ChangeTuners#V[T]): U[Cell[P]] = {
    val pos = ev1.convert(positions)
    val update = (change: Boolean, cell: Cell[P]) => change match {
      case true => schema.decode(cell.content.value.toShortString).map { case con => Cell(cell.position, con) }
      case false => Some(cell)
    }

    tuner match {
      case InMemory(_) =>
        data
          .mapSideJoin(pos.toHashSetValue((p: slice.S) => p), (c: Cell[P], v: HashSet[slice.S]) =>
            update(v.contains(slice.selected(c.position)), c), HashSet.empty[slice.S])
      case _ =>
        data
          .groupBy { case c => slice.selected(c.position) }
          .tunedLeftJoin(tuner, tuner.parameters, pos.map { case p => (p, ()) })
          .flatMap { case (_, (c, o)) => update(!o.isEmpty, c) }
    }
  }

  type CompactTuners = TP2
  def compact()(implicit ev: ClassTag[P]): E[Map[P, Content]] = {
    val semigroup = new com.twitter.algebird.Semigroup[Map[P, Content]] {
      def plus(l: Map[P, Content], r: Map[P, Content]): Map[P, Content] = l ++ r
    }

    data
      .map { case c => Map(c.position -> c.content) }
      .sum(semigroup)
  }

  def compact[T <: Tuner](slice: Slice[P], tuner: T = Default())(implicit ev1: slice.S =!= Position0D,
    ev2: ClassTag[slice.S], ev3: CompactTuners#V[T]): E[Map[slice.S, slice.C]] = {
    val semigroup = new com.twitter.algebird.Semigroup[Map[slice.S, slice.C]] {
      def plus(l: Map[slice.S, slice.C], r: Map[slice.S, slice.C]): Map[slice.S, slice.C] = l ++ r
    }

    data
      .map { case c => (c.position, slice.toMap(c)) }
      .groupBy { case (p, m) => slice.selected(p) }
      .tuneReducers(tuner.parameters)
      .reduce[(P, Map[slice.S, slice.C])] { case ((lp, lm), (rp, rm)) => (lp, slice.combineMaps(lp, lm, rm)) }
      .map { case (_, (_, m)) => m }
      .sum(semigroup)
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
            if (v.contains(c.position)) { Some(c) } else { None }, HashSet.empty[P])
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
  def join[T <: Tuner](slice: Slice[P], that: S, tuner: T = Default())(implicit ev1: P =!= Position1D,
    ev2: ClassTag[slice.S], ev3: JoinTuners#V[T]): U[Cell[P]] = {
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
            if (v.contains(slice.selected(c.position))) { Some(c) } else { None }, HashSet.empty[slice.S])
      case _ =>
        (data ++ that.data)
          .groupBy { case c => slice.selected(c.position) }
          .tunedJoin(tuner, p2, keep)
          .map { case (_, (c, _)) => c }
    }
  }

  type MaterialiseTuners = OneOf1[Default[Execution]]
  def materialise[T <: Tuner](tuner: T)(implicit ev: MaterialiseTuners#V[T]): List[Cell[P]] = {
    val (config, mode) = tuner.parameters match {
      case Execution(cfg, md) => (cfg, md)
    }

    data
      .toIterableExecution
      .waitFor(config, mode) match {
        case scala.util.Success(itr) => itr.toList
        case _ => List.empty
      }
  }

  type NamesTuners = TP2
  def names[T <: Tuner](slice: Slice[P], tuner: T = Default())(implicit ev1: slice.S =!= Position0D,
    ev2: ClassTag[slice.S], ev3: NamesTuners#V[T]): U[slice.S] = {
    data
      .map { case c => slice.selected(c.position) }
      .tunedDistinct(tuner.parameters)
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
      .flatMapWithValue(value) { case ((lc, rc), vo) => operator.computeWithValue(lc, rc, vo.get) }
  }

  def pairwiseBetween[Q <: Position, T <: Tuner](slice: Slice[P], comparer: Comparer, that: S,
    operators: Operable[P, Q], tuner: T = Default())(implicit ev1: slice.S =!= Position0D, ev2: PosExpDep[slice.R, Q],
      ev3: ClassTag[slice.S], ev4: ClassTag[slice.R], ev5: PairwiseTuners#V[T]): U[Cell[Q]] = {
    val operator = operators()

    pairwiseTuples(slice, comparer, tuner)(data, that.data).flatMap { case (lc, rc) => operator.compute(lc, rc) }
  }

  def pairwiseBetweenWithValue[Q <: Position, W, T <: Tuner](slice: Slice[P], comparer: Comparer, that: S,
    operators: OperableWithValue[P, Q, W], value: E[W], tuner: T = Default())(implicit ev1: slice.S =!= Position0D,
      ev2: PosExpDep[slice.R, Q], ev3: ClassTag[slice.S], ev4: ClassTag[slice.R],
        ev5: PairwiseTuners#V[T]): U[Cell[Q]] = {
    val operator = operators()

    pairwiseTuples(slice, comparer, tuner)(data, that.data)
      .flatMapWithValue(value) { case ((lc, rc), vo) => operator.computeWithValue(lc, rc, vo.get) }
  }

  def rename(renamer: (Cell[P]) => Option[P]): U[Cell[P]] = {
    data.flatMap { case c => renamer(c).map { Cell(_, c.content) } }
  }

  def renameWithValue[W](renamer: (Cell[P], W) => Option[P], value: E[W]): U[Cell[P]] = {
    data.flatMapWithValue(value) { case (c, vo) => renamer(c, vo.get).map { Cell(_, c.content) } }
  }

  def saveAsText(file: String, writer: TextWriter = Cell.toString())(implicit flow: FlowDef,
    mode: Mode): U[Cell[P]] = saveText(file, writer)

  type SetTuners = TP2
  def set[M, T <: Tuner](values: M, tuner: T = Default())(implicit ev1: BaseMatrixable[M, P, TypedPipe],
    ev2: ClassTag[P], ev3: SetTuners#V[T]): U[Cell[P]] = {
    data
      .groupBy { case c => c.position }
      .tuneReducers(tuner.parameters)
      .outerJoin(ev1.convert(values).groupBy { case c => c.position })
      .map { case (_, (co, cn)) => cn.getOrElse(co.get) }
  }

  type ShapeTuners = TP2
  def shape[T <: Tuner](tuner: T = Default())(implicit ev: ShapeTuners#V[T]): U[Cell[Position1D]] = {
    Grouped(data
      .flatMap { case c => c.position.coordinates.map(_.toString).zipWithIndex.map(_.swap) }
      .tunedDistinct(tuner.parameters))
      .size
      .map { case (i, s) => Cell(Position1D(Dimension.All(i).toString), Content(DiscreteSchema(LongCodex), s)) }
  }

  type SizeTuners = TP2
  def size[D <: Dimension, T <: Tuner](dim: D, distinct: Boolean, tuner: T = Default())(implicit ev1: PosDimDep[P, D],
    ev2: SizeTuners#V[T]): U[Cell[Position1D]] = {
    val coords = data.map { case c => c.position(dim) }
    val dist = if (distinct) { coords } else { coords.tunedDistinct(tuner.parameters)(Value.Ordering) }

    dist
      .map { case _ => 1L }
      .sum
      .map { case sum => Cell(Position1D(dim.toString), Content(DiscreteSchema(LongCodex), sum)) }
  }

  type SliceTuners = TP4
  def slice[I, T <: Tuner](slice: Slice[P], positions: I, keep: Boolean, tuner: T = InMemory())(
    implicit ev1: PositionDistributable[I, slice.S, TypedPipe], ev2: ClassTag[slice.S],
      ev3: SliceTuners#V[T]): U[Cell[P]] = {
    val pos = ev1.convert(positions)

    tuner match {
      case InMemory(_) =>
        data
          .mapSideJoin(pos.toHashSetValue((p: slice.S) => p), (c: Cell[P], v: HashSet[slice.S]) =>
            if (v.contains(slice.selected(c.position)) == keep) { Some(c) } else { None }, HashSet.empty[slice.S])
      case _ =>
        data
          .groupBy { case c => slice.selected(c.position) }
          .tunedLeftJoin(tuner, tuner.parameters, pos.map { case p => (p, ()) })
          .collect { case (_, (c, o)) if (o.isEmpty != keep) => c }
    }
  }

  type SlideTuners = TP2
  def slide[Q <: Position, F, T <: Tuner](slice: Slice[P], windows: F, ascending: Boolean, tuner: T = Default())(
    implicit ev1: Windowable[F, slice.S, slice.R, Q], ev2: slice.R =!= Position0D, ev3: ClassTag[slice.S],
      ev4: ClassTag[slice.R], ev5: SlideTuners#V[T]): U[Cell[Q]] = {
    val window = ev1.convert(windows)

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

  def slideWithValue[Q <: Position, F, W, T <: Tuner](slice: Slice[P], windows: F, value: E[W], ascending: Boolean,
    tuner: T = Default())(implicit ev1: WindowableWithValue[F, slice.S, slice.R, Q, W], ev2: slice.R =!= Position0D,
      ev3: ClassTag[slice.S], ev4: ClassTag[slice.R], ev5: SlideTuners#V[T]): U[Cell[Q]] = {
    val window = ev1.convert(windows)

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

  def stream[Q <: Position](command: String, files: List[String], writer: TextWriter,
    parser: BaseMatrix.TextParser[Q]): (U[Cell[Q]], U[String]) = {
    val contents = files.map { case f => (f, Files.readAllBytes(Paths.get(f))) }
    val smfn = (k: Unit, itr: Iterator[String]) => {
      val tmp = Files.createTempDirectory("grimlock-")
      tmp.toFile.deleteOnExit

      contents.foreach {
        case (file, content) =>
          val path = Paths.get(tmp.toString, file)

          Files.write(path, content)
          Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("rwxr-x---"))
      }

      val process = new ProcessBuilder(command.split(' ').toList.asJava).directory(tmp.toFile).start

      new Thread() {
        override def run() {
          Source.fromInputStream(process.getErrorStream, "ISO-8859-1").getLines.foreach(System.err.println)
        }
      }.start

      new Thread() {
        override def run() {
          val out = new PrintWriter(new OutputStreamWriter(process.getOutputStream, UTF_8))

          itr.foreach(out.println)
          out.close
        }
      }.start

      val result = Source.fromInputStream(process.getInputStream, "UTF-8").getLines

      new Iterator[String] {
        def next(): String = result.next

        def hasNext: Boolean = {
          if (result.hasNext) {
            true
          } else {
            val status = process.waitFor

            if (status != 0) {
              throw new Exception(s"Subprocess '${command}' exited with status ${status}")
            }

            false
          }
        }
      }
    }

    val result = data
      .flatMap(writer(_))
      .groupAll
      .mapGroup(smfn)
      .values
      .flatMap(parser(_))

    (result.collect { case Left(c) => c }, result.collect { case Right(e) => e })
  }

  def subset(samplers: Sampleable[P]): U[Cell[P]] = {
    val sampler = samplers()

    data.filter { case c => sampler.select(c) }
  }

  def subsetWithValue[W](samplers: SampleableWithValue[P, W], value: E[W]): U[Cell[P]] = {
    val sampler = samplers()

    data.filterWithValue(value) { case (c, vo) => sampler.selectWithValue(c, vo.get) }
  }

  type SummariseTuners = TP2
  def summarise[Q <: Position, F, T <: Tuner](slice: Slice[P], aggregators: F, tuner: T = Default())(
    implicit ev1: Aggregatable[F, P, slice.S, Q], ev2: ClassTag[slice.S], ev3: SummariseTuners#V[T]): U[Cell[Q]] = {
    val aggregator = ev1.convert(aggregators)

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

  def summariseWithValue[Q <: Position, F, W, T <: Tuner](slice: Slice[P], aggregators: F, value: E[W],
    tuner: T = Default())(implicit ev1: AggregatableWithValue[F, P, slice.S, Q, W], ev2: ClassTag[slice.S],
      ev3: SummariseTuners#V[T]): U[Cell[Q]] = {
    val aggregator = ev1.convert(aggregators)

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

  def toSequence[K <: Writable, V <: Writable](writer: SequenceWriter[K, V]): U[(K, V)] = data.flatMap(writer(_))

  def toText(writer: TextWriter): U[String] = data.flatMap(writer(_))

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
  def types[T <: Tuner](slice: Slice[P], specific: Boolean, tuner: T = Default())(implicit ev1: slice.S =!= Position0D,
    ev2: ClassTag[slice.S], ev3: TypesTuners#V[T]): U[(slice.S, Type)] = {
    Grouped(data.map { case Cell(p, c) => (slice.selected(p), c.schema.kind) })
      .tuneReducers(tuner.parameters)
      .reduce[Type] { case (lt, rt) => Type.getCommonType(lt, rt) }
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
    ev2:UniqueTuners#V[T]): U[(slice.S, Content)] = {
    val ordering = new Ordering[Cell[slice.S]] {
      def compare(l: Cell[slice.S], r: Cell[slice.S]) = l.toString().compare(r.toString)
    }

    data
      .map { case Cell(p, c) => Cell(slice.selected(p), c) }
      .tunedDistinct(tuner.parameters)(ordering)
      .map { case Cell(p, c) => (p, c) }
  }

  type WhichTuners = TP4
  def which(predicate: BaseMatrix.Predicate[P])(implicit ev: ClassTag[P]): U[P] = {
    data.collect { case c if predicate(c) => c.position }
  }

  def whichByPositions[I, T <: Tuner](slice: Slice[P], predicates: I, tuner: T = InMemory())(
    implicit ev1: BasePredicateable[I, P, slice.S, TypedPipe], ev2: ClassTag[slice.S], ev3: ClassTag[P],
      ev4: WhichTuners#V[T]): U[P] = {
    val pp = ev1.convert(predicates)
      .map { case (pos, pred) => pos.map { case p => (p, pred) } }
      .reduce((l, r) => l ++ r)

    tuner match {
      case InMemory(_) =>
        type M = Map[slice.S, List[BaseMatrix.Predicate[P]]]
        val semigroup = new com.twitter.algebird.Semigroup[M] {
          def plus(l: M, r: M): M = { (l.toSeq ++ r.toSeq).groupBy(_._1).mapValues(_.map(_._2).toList.flatten) }
        }

        data
          .mapSideJoin(pp.map { case (pos, pred) => Map(pos -> List(pred)) }.sum(semigroup),
            (c: Cell[P], v: M) => v.get(slice.selected(c.position)).flatMap {
              case lst => if (lst.exists((pred) => pred(c))) { Some(c.position) } else { None }
            }, Map.empty[slice.S, List[BaseMatrix.Predicate[P]]])
      case _ =>
        data
          .groupBy { case c => slice.selected(c.position) }
          .tunedJoin(tuner, tuner.parameters, pp)
          .collect { case (_, (c, predicate)) if predicate(c) => c.position }
    }
  }

  val data: U[Cell[P]]

  protected def saveDictionary(slice: Slice[P], file: String, dictionary: String, separator: String)(
    implicit ev1: ClassTag[slice.S], flow: FlowDef, mode: Mode) = {
    val numbered = names(slice)
      .groupAll
      .mapGroup { case (_, itr) => itr.zipWithIndex }
      .map { case (_, (p, i)) => (p, i) }

    numbered
      .map { case (Position1D(c), i) => c.toShortString + separator + i }
      .write(TypedSink(TextLine(dictionary.format(file, slice.dimension.index))))

    Grouped(numbered)
  }

  private def pairwiseTuples[T <: Tuner](slice: Slice[P], comparer: Comparer, tuner: T)(ldata: U[Cell[P]],
    rdata: U[Cell[P]])(implicit ev1: ClassTag[slice.S], ev2: PairwiseTuners#V[T]): U[(Cell[P], Cell[P])] = {
    tuner match {
      case InMemory(_) =>
        ldata
          .mapSideJoin(rdata.toListValue((c: Cell[P]) => c),
            (lc: Cell[P], v: List[Cell[P]]) => v.collect {
              case rc if comparer.keep(slice.selected(lc.position), slice.selected(rc.position)) => (lc, rc)
            }, List.empty[Cell[P]])
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
        val ordering = Position.Ordering[slice.S]()
        val right = rdata
          .map { case Cell(p, _) => slice.selected(p) }
          .distinct(ordering)
          .asKeys
          .redistribute(rr)
          .map { case (p, _) => List(p) }
          .sum
        val keys = ldata
          .map { case Cell(p, _) => slice.selected(p) }
          .distinct(ordering)
          .asKeys
          .redistribute(lr)
          .flatMapWithValue(right) {
            case ((l, _), Some(v)) => v.collect { case r if comparer.keep(l, r) => (r, l) }
            case _ => None
          }
          .forceToDisk // TODO: Should this be configurable?

        ldata
          .groupBy { case Cell(p, _) => slice.selected(p) }
          .tunedJoin(tuner, lj, rdata
            .groupBy { case Cell(p, _) => slice.selected(p) }
            .tunedJoin(tuner, rj, keys)
            .map { case (r, (c, l)) => (l, c) })
          .values
    }
  }
}

/** Base trait for methods that reduce the number of dimensions or that can be filled using a `TypedPipe[Cell[P]]`. */
trait ReduceableMatrix[P <: Position with ReduceablePosition] extends BaseReduceableMatrix[P] { self: Matrix[P] =>

  import ScaldingImplicits._

  type FillHeterogeneousTuners = OneOf4[InMemory[NoParameters.type], InMemory[Reducers],
                                        Default[NoParameters.type], Default[Reducers]]
  def fillHeterogeneous[Q <: Position, T <: Tuner](slice: Slice[P], values: U[Cell[Q]], tuner: T = Default())(
    implicit ev1: ClassTag[P], ev2: ClassTag[slice.S], ev3: slice.S =:= Q,
      ev4: FillHeterogeneousTuners#V[T]): U[Cell[P]] = {
    val vals = values.groupBy { case c => c.position.asInstanceOf[slice.S] }
    val dense = tuner match {
      case InMemory(_) =>
        val semigroup = new com.twitter.algebird.Semigroup[Map[slice.S, Content]] {
          def plus(l: Map[slice.S, Content], r: Map[slice.S, Content]): Map[slice.S, Content] = l ++ r
        }

        domain(Default())
          .mapSideJoin(vals.map { case (p, c) => Map(p -> c.content) }.sum(semigroup),
            (p: P, v: Map[slice.S, Content]) => v.get(slice.selected(p)).map { case c => Cell(p, c) },
              Map.empty[slice.S, Content])
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
  def fillHomogeneous[T <: Tuner](value: Content, tuner: T = Default())(implicit ev1: ClassTag[P],
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
  def squash[D <: Dimension, T <: Tuner](dim: D, squasher: Squashable[P], tuner: T = Default())(
    implicit ev1: PosDimDep[P, D], ev2: ClassTag[P#L], ev3: SquashTuners#V[T]): U[Cell[P#L]] = {
    val squash = squasher()

    data
      .map { case c => (c.position.remove(dim), squash.prepare(c, dim)) }
      .group[P#L, squash.T]
      .tuneReducers(tuner.parameters)
      .reduce[squash.T] { case (lt, rt) => squash.reduce(lt, rt) }
      .flatMap { case (p, t) => squash.present(t).map { case c => Cell(p, c) } }
  }

  def squashWithValue[D <: Dimension, W, T <: Tuner](dim: D, squasher: SquashableWithValue[P, W], value: E[W],
    tuner: T = Default())(implicit ev1: PosDimDep[P, D], ev2: ClassTag[P#L], ev3: SquashTuners#V[T]): U[Cell[P#L]] = {
    val squash = squasher()

    data
      .mapWithValue(value) { case (c, vo) => (c.position.remove(dim), squash.prepareWithValue(c, dim, vo.get)) }
      .group[P#L, squash.T]
      .tuneReducers(tuner.parameters)
      .reduce[squash.T] { case (lt, rt) => squash.reduce(lt, rt) }
      .flatMapWithValue(value) { case ((p, t), vo) => squash.presentWithValue(t, vo.get).map { case c => Cell(p, c) } }
  }

  def toVector(separator: String): U[Cell[Position1D]] = {
    data.map { case Cell(p, c) => Cell(Position1D(p.coordinates.map(_.toShortString).mkString(separator)), c) }
  }
}

/** Base trait for methods that expand the number of dimension of a matrix using a `TypedPipe[Cell[P]]`. */
trait ExpandableMatrix[P <: Position with ExpandablePosition] extends BaseExpandableMatrix[P] { self: Matrix[P] =>

  def expand[Q <: Position](expander: (Cell[P]) => TraversableOnce[Q])(
    implicit ev: PosExpDep[P, Q]): TypedPipe[Cell[Q]] = {
    data.flatMap { case c => expander(c).map { Cell(_, c.content) } }
  }

  def expandWithValue[Q <: Position, W](expander: (Cell[P], W) => TraversableOnce[Q], value: ValuePipe[W])(
    implicit ev: PosExpDep[P, Q]): TypedPipe[Cell[Q]] = {
    data.flatMapWithValue(value) { case (c, vo) => expander(c, vo.get).map { Cell(_, c.content) } }
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
  def correlation[ST <: Tuner, PT <: Tuner](slice: Slice[Position2D], stuner: ST = Default(), ptuner: PT = Default())(
    implicit ev1: ClassTag[slice.S], ev2: ClassTag[slice.R], ev3: SummariseTuners#V[ST],
      ev4: PairwiseTuners#V[PT]): U[Cell[Position1D]] = {
    implicit def UP2DSC2M1D(data: U[Cell[slice.S]]): Matrix1D = new Matrix1D(data.asInstanceOf[U[Cell[Position1D]]])
    implicit def UP2DRMC2M2D(data: U[Cell[slice.R#M]]): Matrix2D = new Matrix2D(data.asInstanceOf[U[Cell[Position2D]]])

    val mean = data
      .summarise(slice, Mean[Position2D, slice.S](), stuner)
      .compact(Over(First))

    implicit object P2D extends PosDimDep[Position2D, slice.dimension.type]

    val centered = data
      .transformWithValue(Subtract(ExtractWithDimension[slice.dimension.type, Position2D, Content](slice.dimension)
        .andThenPresent(_.value.asDouble)), mean)

    val denom = centered
      .transform(Power[Position2D](2))
      .summarise(slice, Sum[Position2D, slice.S](), stuner)
      .pairwise(Over(First), Lower, Times(Locate.OperatorString[Position1D](Over(First), "(%1$s*%2$s)")), ptuner)
      .transform(SquareRoot[Position1D]())
      .compact(Over(First))

    centered
      .pairwise(slice, Lower, Times(Locate.OperatorString[Position2D](slice, "(%1$s*%2$s)")), ptuner)
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
  def mutualInformation[ST <: Tuner, PT <: Tuner](slice: Slice[Position2D], stuner: ST = Default(),
    ptuner: PT = Default())(implicit ev1: ClassTag[slice.S], ev2: ClassTag[slice.R], ev3: SummariseTuners#V[ST],
      ev4: PairwiseTuners#V[PT]): U[Cell[Position1D]] = {
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

    val mhist = data
      .expand(c => Some(c.position.append(c.content.value.toShortString)))
      .summarise(Along[Position3D, dim.type](dim), Count[Position3D, Position2D](), stuner)

    val mcount = mhist
      .summarise(Over(First), Sum[Position2D, Position1D](), stuner)
      .compact()

    val marginal = mhist
      .summariseWithValue(Over(First), Entropy[Position2D, Position1D, W](extractor)
        .andThenExpandWithValue((cell, _) => cell.position.append("marginal")), mcount, stuner)
      .pairwise(Over(First), Upper, Plus(Locate.OperatorString[Position2D](Over(First), "%s,%s")), ptuner)

    val jhist = data
      .pairwise(slice, Upper, Concatenate(Locate.OperatorString[Position2D](slice, "%s,%s")), ptuner)
      .expand(c => Some(c.position.append(c.content.value.toShortString)))
      .summarise(Along(Second), Count[Position3D, Position2D](), stuner)

    val jcount = jhist
      .summarise(Over(First), Sum[Position2D, Position1D](), stuner)
      .compact()

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
  def gini[ST <: Tuner, WT <: Tuner, PT <: Tuner](slice: Slice[Position2D], stuner: ST = Default(),
    wtuner: WT = Default(), ptuner: PT = Default())(implicit ev1: ClassTag[slice.S], ev2: ClassTag[slice.R],
      ev3: SummariseTuners#V[ST], ev4: SlideTuners#V[WT], ev5: PairwiseTuners#V[PT]): U[Cell[Position1D]] = {
    implicit def UP2DSC2M1D(data: U[Cell[slice.S]]): Matrix1D = new Matrix1D(data.asInstanceOf[U[Cell[Position1D]]])
    implicit def UP2DSMC2M2D(data: U[Cell[slice.S#M]]): Matrix2D = new Matrix2D(data.asInstanceOf[U[Cell[Position2D]]])

    def isPositive = (cell: Cell[Position2D]) => cell.content.value.asDouble.map(_ > 0).getOrElse(false)
    def isNegative = (cell: Cell[Position2D]) => cell.content.value.asDouble.map(_ <= 0).getOrElse(false)

    val extractor = ExtractWithDimension[Dimension.First, Position2D, Content](First)
      .andThenPresent(_.value.asDouble)

    val pos = data
      .transform(Compare[Position2D](isPositive))
      .summarise(slice, Sum[Position2D, slice.S](), stuner)
      .compact(Over(First))

    val neg = data
      .transform(Compare[Position2D](isNegative))
      .summarise(slice, Sum[Position2D, slice.S](), stuner)
      .compact(Over(First))

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
        Times(Locate.OperatorString[Position2D](Along(First), "(%1$s*%2$s)")), ptuner)
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
  def loadText[P <: Position](file: String,
    parser: BaseMatrix.TextParser[P]): (TypedPipe[Cell[P]], TypedPipe[String]) = {
    val pipe = TypedPipe.from(TextLine(file)).flatMap { parser(_) }

    (pipe.collect { case Left(c) => c }, pipe.collect { case Right(e) => e })
  }

  /**
   * Read binary key-value (sequence) matrix data into a `TypedPipe[Cell[P]]`.
   *
   * @param file   The text file to read from.
   * @param parser The parser that converts a single key-value to a cell.
   */
  def loadSequence[K <: Writable, V <: Writable, P <: Position](file: String,
    parser: BaseMatrix.SequenceParser[K, V, P])(implicit ev1: Manifest[K],
      ev2: Manifest[V]): (TypedPipe[Cell[P]], TypedPipe[String]) = {
    val pipe = TypedPipe.from(WritableSequenceFile[K, V](file)).flatMap { case (k, v) => parser(k, v) }

    (pipe.collect { case Left(c) => c }, pipe.collect { case Right(e) => e })
  }

  /** Conversion from `TypedPipe[Cell[Position1D]]` to a Scalding `Matrix1D`. */
  implicit def TP2M1(data: TypedPipe[Cell[Position1D]]): Matrix1D = new Matrix1D(data)
  /** Conversion from `TypedPipe[Cell[Position2D]]` to a Scalding `Matrix2D`. */
  implicit def TP2M2(data: TypedPipe[Cell[Position2D]]): Matrix2D = new Matrix2D(data)
  /** Conversion from `TypedPipe[Cell[Position3D]]` to a Scalding `Matrix3D`. */
  implicit def TP2M3(data: TypedPipe[Cell[Position3D]]): Matrix3D = new Matrix3D(data)
  /** Conversion from `TypedPipe[Cell[Position4D]]` to a Scalding `Matrix4D`. */
  implicit def TP2M4(data: TypedPipe[Cell[Position4D]]): Matrix4D = new Matrix4D(data)
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

  /** Conversion from `List[Cell[Position1D]]` to a Scalding `Matrix1D`. */
  implicit def L2TPM1(data: List[Cell[Position1D]]): Matrix1D = new Matrix1D(new IterablePipe(data))
  /** Conversion from `List[Cell[Position2D]]` to a Scalding `Matrix2D`. */
  implicit def L2TPM2(data: List[Cell[Position2D]]): Matrix2D = new Matrix2D(new IterablePipe(data))
  /** Conversion from `List[Cell[Position3D]]` to a Scalding `Matrix3D`. */
  implicit def L2TPM3(data: List[Cell[Position3D]]): Matrix3D = new Matrix3D(new IterablePipe(data))
  /** Conversion from `List[Cell[Position4D]]` to a Scalding `Matrix4D`. */
  implicit def L2TPM4(data: List[Cell[Position4D]]): Matrix4D = new Matrix4D(new IterablePipe(data))
  /** Conversion from `List[Cell[Position5D]]` to a Scalding `Matrix5D`. */
  implicit def L2TPM5(data: List[Cell[Position5D]]): Matrix5D = new Matrix5D(new IterablePipe(data))
  /** Conversion from `List[Cell[Position6D]]` to a Scalding `Matrix6D`. */
  implicit def L2TPM6(data: List[Cell[Position6D]]): Matrix6D = new Matrix6D(new IterablePipe(data))
  /** Conversion from `List[Cell[Position7D]]` to a Scalding `Matrix7D`. */
  implicit def L2TPM7(data: List[Cell[Position7D]]): Matrix7D = new Matrix7D(new IterablePipe(data))
  /** Conversion from `List[Cell[Position8D]]` to a Scalding `Matrix8D`. */
  implicit def L2TPM8(data: List[Cell[Position8D]]): Matrix8D = new Matrix8D(new IterablePipe(data))
  /** Conversion from `List[Cell[Position9D]]` to a Scalding `Matrix9D`. */
  implicit def L2TPM9(data: List[Cell[Position9D]]): Matrix9D = new Matrix9D(new IterablePipe(data))

  /** Conversion from `List[(Valueable, Content)]` to a Scalding `Matrix1D`. */
  implicit def LV1C2TPM1[V: Valueable](list: List[(V, Content)]): Matrix1D = {
    new Matrix1D(new IterablePipe(list.map { case (v, c) => Cell(Position1D(v), c) }))
  }
  /** Conversion from `List[(Valueable, Valueable, Content)]` to a Scalding `Matrix2D`. */
  implicit def LV2C2TPM2[V: Valueable, W: Valueable](list: List[(V, W, Content)]): Matrix2D = {
    new Matrix2D(new IterablePipe(list.map { case (v, w, c) => Cell(Position2D(v, w), c) }))
  }
  /** Conversion from `List[(Valueable, Valueable, Valueable, Content)]` to a Scalding `Matrix3D`. */
  implicit def LV3C2TPM3[V: Valueable, W: Valueable, X: Valueable](list: List[(V, W, X, Content)]): Matrix3D = {
    new Matrix3D(new IterablePipe(list.map { case (v, w, x, c) => Cell(Position3D(v, w, x), c) }))
  }
  /** Conversion from `List[(Valueable, Valueable, Valueable, Valueable, Content)]` to a Scalding `Matrix4D`. */
  implicit def LV4C2TPM4[V: Valueable, W: Valueable, X: Valueable, Y: Valueable](
    list: List[(V, W, X, Y, Content)]): Matrix4D = {
    new Matrix4D(new IterablePipe(list.map { case (v, w, x, y, c) => Cell(Position4D(v, w, x, y), c) }))
  }
  /**
   * Conversion from `List[(Valueable, Valueable, Valueable, Valueable, Valueable, Content)]` to a Scalding `Matrix5D`.
   */
  implicit def LV5C2TPM5[V: Valueable, W: Valueable, X: Valueable, Y: Valueable, Z: Valueable](
    list: List[(V, W, X, Y, Z, Content)]): Matrix5D = {
    new Matrix5D(new IterablePipe(list.map { case (v, w, x, y, z, c) => Cell(Position5D(v, w, x, y, z), c) }))
  }
  /**
   * Conversion from `List[(Valueable, Valueable, Valueable, Valueable, Valueable, Valueable, Content)]` to a
   * Scalding `Matrix6D`.
   */
  implicit def LV6C2TPM6[U: Valueable, V: Valueable, W: Valueable, X: Valueable, Y: Valueable, Z: Valueable](
    list: List[(U, V, W, X, Y, Z, Content)]): Matrix6D = {
    new Matrix6D(new IterablePipe(list.map { case (u, v, w, x, y, z, c) => Cell(Position6D(u, v, w, x, y, z), c) }))
  }
  /**
   * Conversion from `List[(Valueable, Valueable, Valueable, Valueable, Valueable, Valueable, Valueable, Content)]`
   * to a Scalding `Matrix7D`.
   */
  implicit def LV7C2TPM7[T: Valueable, U: Valueable, V: Valueable, W: Valueable, X: Valueable, Y: Valueable, Z: Valueable](
    list: List[(T, U, V, W, X, Y, Z, Content)]): Matrix7D = {
    new Matrix7D(new IterablePipe(list.map {
      case (t, u, v, w, x, y, z, c) => Cell(Position7D(t, u, v, w, x, y, z), c)
    }))
  }
  /**
   * Conversion from `List[(Valueable, Valueable, Valueable, Valueable, Valueable, Valueable, Valueable, Valueable,
   * Content)]` to a Scalding `Matrix8D`.
   */
  implicit def LV8C2TPM8[S: Valueable, T: Valueable, U: Valueable, V: Valueable, W: Valueable, X: Valueable, Y: Valueable, Z: Valueable](
    list: List[(S, T, U, V, W, X, Y, Z, Content)]): Matrix8D = {
    new Matrix8D(new IterablePipe(list.map {
      case (s, t, u, v, w, x, y, z, c) => Cell(Position8D(s, t, u, v, w, x, y, z), c)
    }))
  }
  /**
   * Conversion from `List[(Valueable, Valueable, Valueable, Valueable, Valueable, Valueable, Valueable, Valueable,
   * Valueable, Content)]` to a Scalding `Matrix9D`.
   */
  implicit def LV9C2TPM9[R: Valueable, S: Valueable, T: Valueable, U: Valueable, V: Valueable, W: Valueable, X: Valueable, Y: Valueable, Z: Valueable](
    list: List[(R, S, T, U, V, W, X, Y, Z, Content)]): Matrix9D = {
    new Matrix9D(new IterablePipe(list.map {
      case (r, s, t, u, v, w, x, y, z, c) => Cell(Position9D(r, s, t, u, v, w, x, y, z), c)
    }))
  }

  /** Conversion from matrix with errors tuple to `MatrixWithParseErrors`. */
  implicit def TP2MWPE[P <: Position](
    t: (TypedPipe[Cell[P]], TypedPipe[String])): MatrixWithParseErrors[P, TypedPipe] = {
    MatrixWithParseErrors(t._1, t._2)
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
    data
      .groupBy { case c => c.position }
      .join(saveDictionary(Over(First), file, dictionary, separator))
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
  def saveAsCSV(slice: Slice[Position2D], file: String, separator: String = "|", escapee: Escape = Quote("|"),
    writeHeader: Boolean = true, header: String = "%s.header", writeRowId: Boolean = true, rowId: String = "id")(
      implicit ev1: ClassTag[slice.S], flow: FlowDef, mode: Mode): U[Cell[Position2D]] = {
    val escape = (str: String) => escapee.escape(str)
    val semigroup = new com.twitter.algebird.Semigroup[HashSet[String]] {
      def plus(l: HashSet[String], r: HashSet[String]): HashSet[String] = l ++ r
    }
    val columns = data
      .map { case c => HashSet(escape(slice.remainder(c.position)(First).toShortString)) }
      .sum(semigroup)
      .map { _.toList.sorted }

    if (writeHeader) {
      columns
        .map { case lst => (if (writeRowId) escape(rowId) + separator else "") + lst.mkString(separator) }
        .write(TypedSink(TextLine(header.format(file))))
    }

    data
      .groupBy { case c => slice.selected(c.position)(First).toShortString }
      .mapValues {
        case Cell(p, c) => Map(escape(slice.remainder(p)(First).toShortString) -> escape(c.value.toShortString))
      }
      .sum
      .flatMapWithValue(columns) {
        case ((key, values), optCols) => optCols.map {
          case cols => (key, cols.map { case c => values.getOrElse(c, "") })
        }
      }
      .map { case (i, lst) => (if (writeRowId) escape(i) + separator else "") + lst.mkString(separator) }
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
   */
  def saveAsIV(file: String, dictionary: String = "%1$s.dict.%2$d", separator: String = "|")(implicit flow: FlowDef,
    mode: Mode): U[Cell[Position2D]] = {
    data
      .groupBy { case c => Position1D(c.position(First)) }
      .join(saveDictionary(Over(First), file, dictionary, separator))
      .values
      .groupBy { case (c, i) => Position1D(c.position(Second)) }
      .join(saveDictionary(Over(Second), file, dictionary, separator))
      .map { case (_, ((c, i), j)) => i + separator + j + separator + c.content.value.toShortString }
      .write(TypedSink(TextLine(file)))

    data
  }

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
    separator: String = "|")(implicit flow: FlowDef, mode: Mode): U[Cell[Position2D]] = {
    saveAsVW(slice, file, None, None, tag, dictionary, separator)
  }

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
    dictionary: String = "%s.dict", tag: Boolean = false, separator: String = "|")(implicit flow: FlowDef,
      mode: Mode): U[Cell[Position2D]] = saveAsVW(slice, file, Some(labels), None, tag, dictionary, separator)

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
    dictionary: String = "%s.dict", tag: Boolean = false, separator: String = "|")(implicit flow: FlowDef,
      mode: Mode): U[Cell[Position2D]] = saveAsVW(slice, file, None, Some(importance), tag, dictionary, separator)

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
      separator: String = "|")(implicit flow: FlowDef, mode: Mode): U[Cell[Position2D]] = {
    saveAsVW(slice, file, Some(labels), Some(importance), tag, dictionary, separator)
  }

  private def saveAsVW(slice: Slice[Position2D], file: String, labels: Option[U[Cell[Position1D]]],
    importance: Option[U[Cell[Position1D]]], tag: Boolean, dictionary: String, separator: String)(
      implicit flow: FlowDef, mode: Mode): U[Cell[Position2D]] = {
    val dict = data
      .map { c => (slice.remainder(c.position)(First).toShortString, ()) }
      .group
      .sum
      .map(_.swap)
      .group
      .mapGroup { case (_, itr) => itr.zipWithIndex }
      .map { case (_, si) => si }

    dict
      .map { case (s, i) => s + separator + i }
      .write(TypedSink(TextLine(dictionary.format(file))))

    val features = data
      .groupBy { c => slice.remainder(c.position)(First).toShortString }
      .join(dict)
      .flatMap {
        case (_, (c, i)) => c.content.value.asDouble.map { case v => (slice.selected(c.position), (i, v)) }
      }
      .group
      .sortBy { case (i, _) => i }
      .foldLeft("|") { case (b, (i, v)) => b + " " + i + ":" + v }

    val tagged = tag match {
      case true => features.map { case (p, s) => (p, p(First).toShortString + s) }
      case false => features.toTypedPipe
    }

    val weighted = importance match {
      case Some(imp) => tagged
        .join(imp.groupBy { case c => c.position.asInstanceOf[slice.S] })
        .flatMap { case (p, (s, c)) => c.content.value.asDouble.map { case i => (p, i + " " + s) } }
      case None => tagged
    }

    val examples = labels match {
      case Some(lab) => weighted
        .join(lab.groupBy { case c => c.position.asInstanceOf[slice.S] })
        .flatMap { case (p, (s, c)) => c.content.value.asDouble.map { case l => (p, l + " " + s) } }
      case None => weighted
    }

    examples
      .map { case (p, s) => s }
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
    data
      .groupBy { case c => Position1D(c.position(First)) }
      .join(saveDictionary(Over(First), file, dictionary, separator))
      .values
      .groupBy { case (c, i) => Position1D(c.position(Second)) }
      .join(saveDictionary(Over(Second), file, dictionary, separator))
      .map { case (_, ((c, i), j)) => (c, i, j) }
      .groupBy { case (c, i, j) => Position1D(c.position(Third)) }
      .join(saveDictionary(Over(Third), file, dictionary, separator))
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
    data
      .groupBy { case c => Position1D(c.position(First)) }
      .join(saveDictionary(Over(First), file, dictionary, separator))
      .values
      .groupBy { case (c, i) => Position1D(c.position(Second)) }
      .join(saveDictionary(Over(Second), file, dictionary, separator))
      .map { case (_, ((c, i), j)) => (c, i, j) }
      .groupBy { case (c, i, j) => Position1D(c.position(Third)) }
      .join(saveDictionary(Over(Third), file, dictionary, separator))
      .map { case (_, ((c, i, j), k)) => (c, i, j, k) }
      .groupBy { case (c, i, j, k) => Position1D(c.position(Fourth)) }
      .join(saveDictionary(Over(Fourth), file, dictionary, separator))
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
    data
      .groupBy { case c => Position1D(c.position(First)) }
      .join(saveDictionary(Over(First), file, dictionary, separator))
      .values
      .groupBy { case (c, i) => Position1D(c.position(Second)) }
      .join(saveDictionary(Over(Second), file, dictionary, separator))
      .map { case (_, ((c, i), j)) => (c, i, j) }
      .groupBy { case (c, i, j) => Position1D(c.position(Third)) }
      .join(saveDictionary(Over(Third), file, dictionary, separator))
      .map { case (_, ((c, i, j), k)) => (c, i, j, k) }
      .groupBy { case (c, i, j, k) => Position1D(c.position(Fourth)) }
      .join(saveDictionary(Over(Fourth), file, dictionary, separator))
      .map { case (_, ((c, i, j, k), l)) => (c, i, j, k, l) }
      .groupBy { case (c, i, j, k, l) => Position1D(c.position(Fifth)) }
      .join(saveDictionary(Over(Fifth), file, dictionary, separator))
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
    data
      .groupBy { case c => Position1D(c.position(First)) }
      .join(saveDictionary(Over(First), file, dictionary, separator))
      .values
      .groupBy { case (c, i) => Position1D(c.position(Second)) }
      .join(saveDictionary(Over(Second), file, dictionary, separator))
      .map { case (_, ((c, i), j)) => (c, i, j) }
      .groupBy { case (c, i, j) => Position1D(c.position(Third)) }
      .join(saveDictionary(Over(Third), file, dictionary, separator))
      .map { case (_, ((c, i, j), k)) => (c, i, j, k) }
      .groupBy { case (c, i, j, k) => Position1D(c.position(Fourth)) }
      .join(saveDictionary(Over(Fourth), file, dictionary, separator))
      .map { case (_, ((c, i, j, k), l)) => (c, i, j, k, l) }
      .groupBy { case (c, i, j, k, l) => Position1D(c.position(Fifth)) }
      .join(saveDictionary(Over(Fifth), file, dictionary, separator))
      .map { case (_, ((c, i, j, k, l), m)) => (c, i, j, k, l, m) }
      .groupBy { case (c, i, j, k, l, m) => Position1D(c.position(Sixth)) }
      .join(saveDictionary(Over(Sixth), file, dictionary, separator))
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
    data
      .groupBy { case c => Position1D(c.position(First)) }
      .join(saveDictionary(Over(First), file, dictionary, separator))
      .values
      .groupBy { case (c, i) => Position1D(c.position(Second)) }
      .join(saveDictionary(Over(Second), file, dictionary, separator))
      .map { case (_, ((c, i), j)) => (c, i, j) }
      .groupBy { case (c, i, j) => Position1D(c.position(Third)) }
      .join(saveDictionary(Over(Third), file, dictionary, separator))
      .map { case (_, ((c, i, j), k)) => (c, i, j, k) }
      .groupBy { case (c, i, j, k) => Position1D(c.position(Fourth)) }
      .join(saveDictionary(Over(Fourth), file, dictionary, separator))
      .map { case (_, ((c, i, j, k), l)) => (c, i, j, k, l) }
      .groupBy { case (c, i, j, k, l) => Position1D(c.position(Fifth)) }
      .join(saveDictionary(Over(Fifth), file, dictionary, separator))
      .map { case (_, ((c, i, j, k, l), m)) => (c, i, j, k, l, m) }
      .groupBy { case (c, i, j, k, l, m) => Position1D(c.position(Sixth)) }
      .join(saveDictionary(Over(Sixth), file, dictionary, separator))
      .map { case (_, ((c, i, j, k, l, m), n)) => (c, i, j, k, l, m, n) }
      .groupBy { case (c, i, j, k, l, m, n) => Position1D(c.position(Seventh)) }
      .join(saveDictionary(Over(Seventh), file, dictionary, separator))
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
    data
      .groupBy { case c => Position1D(c.position(First)) }
      .join(saveDictionary(Over(First), file, dictionary, separator))
      .values
      .groupBy { case (c, i) => Position1D(c.position(Second)) }
      .join(saveDictionary(Over(Second), file, dictionary, separator))
      .map { case (_, ((c, i), j)) => (c, i, j) }
      .groupBy { case (c, i, j) => Position1D(c.position(Third)) }
      .join(saveDictionary(Over(Third), file, dictionary, separator))
      .map { case (_, ((c, i, j), k)) => (c, i, j, k) }
      .groupBy { case (c, i, j, k) => Position1D(c.position(Fourth)) }
      .join(saveDictionary(Over(Fourth), file, dictionary, separator))
      .map { case (_, ((c, i, j, k), l)) => (c, i, j, k, l) }
      .groupBy { case (c, i, j, k, l) => Position1D(c.position(Fifth)) }
      .join(saveDictionary(Over(Fifth), file, dictionary, separator))
      .map { case (_, ((c, i, j, k, l), m)) => (c, i, j, k, l, m) }
      .groupBy { case (c, i, j, k, l, m) => Position1D(c.position(Sixth)) }
      .join(saveDictionary(Over(Sixth), file, dictionary, separator))
      .map { case (_, ((c, i, j, k, l, m), n)) => (c, i, j, k, l, m, n) }
      .groupBy { case (c, i, j, k, l, m, n) => Position1D(c.position(Seventh)) }
      .join(saveDictionary(Over(Seventh), file, dictionary, separator))
      .map { case (_, ((c, i, j, k, l, m, n), o)) => (c, i, j, k, l, m, n, o) }
      .groupBy { case (c, i, j, k, l, m, n, o) => Position1D(c.position(Eighth)) }
      .join(saveDictionary(Over(Eighth), file, dictionary, separator))
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
    data
      .groupBy { case c => Position1D(c.position(First)) }
      .join(saveDictionary(Over(First), file, dictionary, separator))
      .values
      .groupBy { case (c, i) => Position1D(c.position(Second)) }
      .join(saveDictionary(Over(Second), file, dictionary, separator))
      .map { case (_, ((c, i), j)) => (c, i, j) }
      .groupBy { case (c, i, j) => Position1D(c.position(Third)) }
      .join(saveDictionary(Over(Third), file, dictionary, separator))
      .map { case (_, ((c, i, j), k)) => (c, i, j, k) }
      .groupBy { case (c, i, j, k) => Position1D(c.position(Fourth)) }
      .join(saveDictionary(Over(Fourth), file, dictionary, separator))
      .map { case (_, ((c, i, j, k), l)) => (c, i, j, k, l) }
      .groupBy { case (c, i, j, k, l) => Position1D(c.position(Fifth)) }
      .join(saveDictionary(Over(Fifth), file, dictionary, separator))
      .map { case (_, ((c, i, j, k, l), m)) => (c, i, j, k, l, m) }
      .groupBy { case (c, i, j, k, l, m) => Position1D(c.position(Sixth)) }
      .join(saveDictionary(Over(Sixth), file, dictionary, separator))
      .map { case (_, ((c, i, j, k, l, m), n)) => (c, i, j, k, l, m, n) }
      .groupBy { case (c, i, j, k, l, m, n) => Position1D(c.position(Seventh)) }
      .join(saveDictionary(Over(Seventh), file, dictionary, separator))
      .map { case (_, ((c, i, j, k, l, m, n), o)) => (c, i, j, k, l, m, n, o) }
      .groupBy { case (c, i, j, k, l, m, n, o) => Position1D(c.position(Eighth)) }
      .join(saveDictionary(Over(Eighth), file, dictionary, separator))
      .map { case (_, ((c, i, j, k, l, m, n, o), p)) => (c, i, j, k, l, m, n, o, p) }
      .groupBy { case (c, i, j, k, l, m, n, o, p) => Position1D(c.position(Ninth)) }
      .join(saveDictionary(Over(Ninth), file, dictionary, separator))
      .map {
        case (_, ((c, i, j, k, l, m, n, o, p), q)) =>
          i + separator + j + separator + k + separator + l + separator + m + separator +
            n + separator + o + separator + p + separator + q + separator + c.content.value.toShortString
      }
      .write(TypedSink(TextLine(file)))

    data
  }
}

/** Scalding companion object for the `Matrixable` type class. */
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

/** Scalding companion object for the `Predicateable` type class. */
object Predicateable {
  /**
   * Converts a `List[(PositionDistributable[I, S, U], Matrix.Predicate[P])]` to a
   * `List[(U[S], BaseMatrix.Predicate[P])]`.
   */
  implicit def PDPT2LTPP[I, P <: Position, S <: Position](implicit ev: PositionDistributable[I, S, TypedPipe]): BasePredicateable[(I, BaseMatrix.Predicate[P]), P, S, TypedPipe] = {
    new BasePredicateable[(I, BaseMatrix.Predicate[P]), P, S, TypedPipe] {
      def convert(t: (I, BaseMatrix.Predicate[P])): List[(TypedPipe[S], BaseMatrix.Predicate[P])] = {
        List((ev.convert(t._1), t._2))
      }
    }
  }

  /**
   * Converts a `(PositionDistributable[I, S, U], Matrix.Predicate[P])` to a `List[(U[S], BaseMatrix.Predicate[P])]`.
   */
  implicit def LPDP2LTPP[I, P <: Position, S <: Position](implicit ev: PositionDistributable[I, S, TypedPipe]): BasePredicateable[List[(I, BaseMatrix.Predicate[P])], P, S, TypedPipe] = {
    new BasePredicateable[List[(I, BaseMatrix.Predicate[P])], P, S, TypedPipe] {
      def convert(t: List[(I, BaseMatrix.Predicate[P])]): List[(TypedPipe[S], BaseMatrix.Predicate[P])] = {
        t.map { case (i, p) => (ev.convert(i), p) }
      }
    }
  }
}

