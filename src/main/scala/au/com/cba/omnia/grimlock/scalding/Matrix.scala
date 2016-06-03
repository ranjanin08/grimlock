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

package au.com.cba.omnia.grimlock.scalding

import au.com.cba.omnia.ebenezer.scrooge.ParquetScroogeSource
import au.com.cba.omnia.grimlock.framework.{
  Cell,
  Collate,
  Consume,
  Compactable,
  Default,
  ReshapeableMatrix => FwReshapeableMatrix,
  ExtractWithDimension,
  ExtractWithKey,
  InMemory,
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
  Redistribute,
  ReduceableMatrix => FwReduceableMatrix,
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
import au.com.cba.omnia.grimlock.framework.window._
import au.com.cba.omnia.grimlock.framework.utility.UnionTypes._

import au.com.cba.omnia.grimlock.scalding.distribution._
import au.com.cba.omnia.grimlock.scalding.environment._

import com.twitter.scalding.{ TextLine, WritableSequenceFile }
import com.twitter.scalding.typed.{ IterablePipe, Grouped, TypedPipe, TypedSink, ValuePipe }

import java.io.{ File, OutputStreamWriter, PrintWriter }
import java.lang.{ ProcessBuilder, Thread }
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.attribute.PosixFilePermissions
import java.nio.file.{ Files, Paths }

import com.twitter.scrooge.ThriftStruct
import org.apache.hadoop.io.Writable

import scala.collection.immutable.HashSet
import scala.collection.JavaConverters._
import scala.io.Source
import scala.reflect.ClassTag

import shapeless.=:!=

private[scalding] object ScaldingImplicits {
  implicit class GroupedTuner[K <: Position, V](grouped: Grouped[K, V]) {
    private implicit def serialisePosition[T <: Position](key: T): Array[Byte] = {
      key.toShortString("|").toCharArray.map(_.toByte)
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
        case Collate() => pipe.forceToDisk
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
        case Redistribute(reducers) => pipe.shard(reducers)
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
trait Matrix[P <: Position with CompactablePosition with ReduceablePosition] extends FwMatrix[P] with Persist[Cell[P]]
  with UserData {
  type M = Matrix[P]

  import ScaldingImplicits._

  type ChangeTuners[T] = TP4[T]
  def change[I, T <: Tuner : ChangeTuners](slice: Slice[P], positions: I, schema: Content.Parser,
    tuner: T = InMemory())(implicit ev1: PositionDistributable[I, slice.S, TypedPipe],
      ev2: ClassTag[slice.S]): U[Cell[P]] = {
    val pos = ev1.convert(positions)
    val update = (change: Boolean, cell: Cell[P]) => change match {
      case true => schema(cell.content.value.toShortString).map { case con => Cell(cell.position, con) }
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

  type CompactTuners[T] = TP2[T]
  def compact()(implicit ev: ClassTag[P]): E[Map[P, Content]] = {
    val semigroup = new com.twitter.algebird.Semigroup[Map[P, Content]] {
      def plus(l: Map[P, Content], r: Map[P, Content]): Map[P, Content] = l ++ r
    }

    data
      .map { case c => Map(c.position -> c.content) }
      .sum(semigroup)
  }

  def compact[T <: Tuner : CompactTuners](slice: Slice[P], tuner: T = Default())(implicit ev1: slice.S =:!= Position0D,
    ev2: ClassTag[slice.S], ev3: Compactable[P]): E[Map[slice.S, P#C[slice.R]]] = {
    val semigroup = new com.twitter.algebird.Semigroup[Map[slice.S, P#C[slice.R]]] {
      def plus(l: Map[slice.S, P#C[slice.R]], r: Map[slice.S, P#C[slice.R]]): Map[slice.S, P#C[slice.R]] = l ++ r
    }

    data
      .map { case c => (slice.selected(c.position), ev3.toMap(slice, c)) }
      .group
      .tuneReducers(tuner.parameters)
      .reduce[Map[slice.S, P#C[slice.R]]] { case (lm, rm) => ev3.combineMaps(lm, rm) }
      .values
      .sum(semigroup)
  }

  type DomainTuners[T] = TP1[T]

  type FillHeterogeneousTuners[T] = T In OneOf[InMemory[NoParameters]]#
    Or[InMemory[Reducers]]#
    Or[Default[NoParameters]]#
    Or[Default[Reducers]]
  def fillHeterogeneous[S <: Position, T <: Tuner : FillHeterogeneousTuners](slice: Slice[P], values: U[Cell[S]],
    tuner: T = Default())(implicit ev1: ClassTag[P], ev2: ClassTag[slice.S], ev3: slice.S =:= S): U[Cell[P]] = {
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

  type FillHomogeneousTuners[T] = TP2[T]
  def fillHomogeneous[T <: Tuner : FillHomogeneousTuners](value: Content, tuner: T = Default())(
    implicit ev1: ClassTag[P]): U[Cell[P]] = {
    domain(Default())
      .asKeys
      .tuneReducers(tuner.parameters)
      .leftJoin(data.groupBy { case c => c.position })
      .map { case (p, (_, co)) => co.getOrElse(Cell(p, value)) }
  }

  type GetTuners[T] = TP4[T]
  def get[I, T <: Tuner : GetTuners](positions: I, tuner: T = InMemory())(
    implicit ev1: PositionDistributable[I, P, TypedPipe], ev2: ClassTag[P]): U[Cell[P]] = {
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

  type JoinTuners[T] = T In OneOf[InMemory[NoParameters]]#
    Or[InMemory[Reducers]]#
    Or[Default[NoParameters]]#
    Or[Default[Reducers]]#
    Or[Default[Sequence2[Reducers, Reducers]]]#
    Or[Unbalanced[Reducers]]#
    Or[Unbalanced[Sequence2[Reducers, Reducers]]]
  def join[T <: Tuner : JoinTuners](slice: Slice[P], that: M, tuner: T = Default())(implicit ev1: P =:!= Position1D,
    ev2: ClassTag[slice.S]): U[Cell[P]] = {
    val (p1, p2) = (tuner, tuner.parameters) match {
      case (_, Sequence2(f, s)) => (f, s)
      case (InMemory(_), p) => (p, NoParameters())
      case (_, p) => (NoParameters(), p)
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

  type MaterialiseTuners[T] = T Is Default[Execution]
  def materialise[T <: Tuner : MaterialiseTuners](tuner: T): List[Cell[P]] = {
    val context = tuner.parameters match {
      case Execution(ctx) => ctx
    }

    data
      .toIterableExecution
      .waitFor(context.config, context.mode) match {
        case scala.util.Success(itr) => itr.toList
        case _ => List.empty
      }
  }

  type NamesTuners[T] = TP2[T]
  def names[T <: Tuner : NamesTuners](slice: Slice[P], tuner: T = Default())(implicit ev1: slice.S =:!= Position0D,
    ev2: ClassTag[slice.S]): U[slice.S] = {
    data
      .map { case c => slice.selected(c.position) }
      .tunedDistinct(tuner.parameters)
  }

  type PairwiseTuners[T] = T In OneOf[InMemory[NoParameters]]#
    Or[Default[NoParameters]]#
    Or[Default[Reducers]]#
    Or[Default[Sequence2[Redistribute, Redistribute]]]#
    Or[Default[Sequence2[Redistribute, Reducers]]]#
    Or[Default[Sequence2[Redistribute, Sequence2[Redistribute, Reducers]]]]#
    Or[Default[Sequence2[Reducers, Redistribute]]]#
    Or[Default[Sequence2[Reducers, Reducers]]]#
    Or[Default[Sequence2[Reducers, Sequence2[Redistribute, Reducers]]]]#
    Or[Default[Sequence2[Sequence2[Redistribute, Reducers], Redistribute]]]#
    Or[Default[Sequence2[Sequence2[Redistribute, Reducers], Reducers]]]#
    Or[Default[Sequence2[Sequence2[Redistribute, Reducers],Sequence2[Redistribute, Reducers]]]]#
    Or[Unbalanced[Sequence2[Reducers, Reducers]]]#
    Or[Unbalanced[Sequence2[Sequence2[Redistribute, Reducers], Sequence2[Redistribute, Reducers]]]]
  def pairwise[Q <: Position, T <: Tuner : PairwiseTuners](slice: Slice[P], comparer: Comparer,
    operators: Operable[P, Q], tuner: T = Default())(implicit ev1: slice.S =:!= Position0D, ev2: PosExpDep[slice.R, Q],
      ev3: ClassTag[slice.S], ev4: ClassTag[slice.R]): U[Cell[Q]] = {
    val operator = operators()

    pairwiseTuples(slice, comparer, tuner)(data, data).flatMap { case (lc, rc) => operator.compute(lc, rc) }
  }

  def pairwiseWithValue[Q <: Position, W, T <: Tuner : PairwiseTuners](slice: Slice[P], comparer: Comparer,
    operators: OperableWithValue[P, Q, W], value: E[W], tuner: T = Default())(implicit ev1: slice.S =:!= Position0D,
      ev2: PosExpDep[slice.R, Q], ev3: ClassTag[slice.S], ev4: ClassTag[slice.R]): U[Cell[Q]] = {
    val operator = operators()

    pairwiseTuples(slice, comparer, tuner)(data, data)
      .flatMapWithValue(value) { case ((lc, rc), vo) => operator.computeWithValue(lc, rc, vo.get) }
  }

  def pairwiseBetween[Q <: Position, T <: Tuner : PairwiseTuners](slice: Slice[P], comparer: Comparer, that: M,
    operators: Operable[P, Q], tuner: T = Default())(implicit ev1: slice.S =:!= Position0D, ev2: PosExpDep[slice.R, Q],
      ev3: ClassTag[slice.S], ev4: ClassTag[slice.R]): U[Cell[Q]] = {
    val operator = operators()

    pairwiseTuples(slice, comparer, tuner)(data, that.data).flatMap { case (lc, rc) => operator.compute(lc, rc) }
  }

  def pairwiseBetweenWithValue[Q <: Position, W, T <: Tuner : PairwiseTuners](slice: Slice[P], comparer: Comparer,
    that: M, operators: OperableWithValue[P, Q, W], value: E[W], tuner: T = Default())(
      implicit ev1: slice.S =:!= Position0D, ev2: PosExpDep[slice.R, Q], ev3: ClassTag[slice.S],
        ev4: ClassTag[slice.R]): U[Cell[Q]] = {
    val operator = operators()

    pairwiseTuples(slice, comparer, tuner)(data, that.data)
      .flatMapWithValue(value) { case ((lc, rc), vo) => operator.computeWithValue(lc, rc, vo.get) }
  }

  def relocate[Q <: Position](locate: Locate.FromCell[P, Q])(implicit ev: PosIncDep[P, Q]): U[Cell[Q]] = {
    data.flatMap { case c => locate(c).map(Cell(_, c.content)) }
  }

  def relocateWithValue[Q <: Position, W](locate: Locate.FromCellWithValue[P, Q, W], value: E[W])(
    implicit ev: PosIncDep[P, Q]): U[Cell[Q]] = {
    data.flatMapWithValue(value) { case (c, vo) => locate(c, vo.get).map(Cell(_, c.content)) }
  }

  def saveAsText(file: String, writer: TextWriter)(implicit ctx: C): U[Cell[P]] = saveText(file, writer)

  type SetTuners[T] = TP2[T]
  def set[T <: Tuner : SetTuners](values: FwMatrixable[P, TypedPipe], tuner: T = Default())(
    implicit ev1: ClassTag[P]): U[Cell[P]] = {
    data
      .groupBy { case c => c.position }
      .tuneReducers(tuner.parameters)
      .outerJoin(values().groupBy { case c => c.position })
      .map { case (_, (co, cn)) => cn.getOrElse(co.get) }
  }

  type ShapeTuners[T] = TP2[T]
  def shape[T <: Tuner : ShapeTuners](tuner: T = Default()): U[Cell[Position1D]] = {
    data
      .flatMap { case c => c.position.coordinates.map(_.toString).zipWithIndex.map(_.swap) }
      .tunedDistinct(tuner.parameters)
      .group
      .size
      .map { case (i, s) => Cell(Position1D(Dimension.All(i).toString), Content(DiscreteSchema[Long](), s)) }
  }

  type SizeTuners[T] = TP2[T]
  def size[T <: Tuner : SizeTuners](dim: Dimension, distinct: Boolean, tuner: T = Default())(
    implicit ev1: PosDimDep[P, dim.D]): U[Cell[Position1D]] = {
    val coords = data.map { case c => c.position(dim) }
    val dist = if (distinct) { coords } else { coords.tunedDistinct(tuner.parameters)(Value.Ordering) }

    dist
      .map { case _ => 1L }
      .sum
      .map { case sum => Cell(Position1D(dim.toString), Content(DiscreteSchema[Long](), sum)) }
  }

  type SliceTuners[T] = TP4[T]
  def slice[I, T <: Tuner : SliceTuners](slice: Slice[P], positions: I, keep: Boolean, tuner: T = InMemory())(
    implicit ev1: PositionDistributable[I, slice.S, TypedPipe], ev2: ClassTag[slice.S]): U[Cell[P]] = {
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

  type SlideTuners[T] = T In OneOf[Default[NoParameters]]#
    Or[Default[Redistribute]]#
    Or[Default[Reducers]]#
    Or[Default[Sequence2[Redistribute, Reducers]]]
  def slide[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position, T <: Tuner : SlideTuners](
    slice: Slice[P], windows: Windowable[P, S, R, Q], ascending: Boolean = true, tuner: T = Default())(
      implicit ev1: slice.S =:= S, ev2: slice.R =:= R, ev3: slice.R =:!= Position0D, ev4: PosExpDep[S, Q],
        ev5: ClassTag[slice.S], ev6: ClassTag[slice.R]): U[Cell[Q]] = {
    val window = windows()
    val (partitions, reducers) = tuner.parameters match {
      case Sequence2(rp @ Redistribute(_), rr @ Reducers(_)) => (rp, rr)
      case rp @ Redistribute(_) => (rp, NoParameters())
      case rr @ Reducers(_) => (NoParameters(), rr)
      case _ => (NoParameters(), NoParameters())
    }

    data
      .map { case c => (slice.selected(c.position), (slice.remainder(c.position), window.prepare(c))) }
      .redistribute(partitions)
      .group
      .tuneReducers(reducers)
      .sortBy { case (r, _) => r }(Position.Ordering(ascending))
      .scanLeft(Option.empty[(window.T, TraversableOnce[window.O])]) {
        case (None, (r, i)) => Some(window.initialise(r, i))
        case (Some((t, _)), (r, i)) => Some(window.update(r, i, t))
      }
      .flatMap {
        case (p, Some((_, o))) => o.flatMap(window.present(p, _))
        case _ => None
      }
  }

  def slideWithValue[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position, W, T <: Tuner : SlideTuners](
    slice: Slice[P], windows: WindowableWithValue[P, S, R, Q, W], value: E[W], ascending: Boolean = true,
      tuner: T = Default())(implicit ev1: slice.S =:= S, ev2: slice.R =:= R, ev3: slice.R =:!= Position0D,
        ev4: PosExpDep[S, Q], ev5: ClassTag[slice.S], ev6: ClassTag[slice.R]): U[Cell[Q]] = {
    val window = windows()
    val (partitions, reducers) = tuner.parameters match {
      case Sequence2(rp @ Redistribute(_), rr @ Reducers(_)) => (rp, rr)
      case rp @ Redistribute(_) => (rp, NoParameters())
      case rr @ Reducers(_) => (NoParameters(), rr)
      case _ => (NoParameters(), NoParameters())
    }

    data
      .mapWithValue(value) {
        case (c, vo) => (slice.selected(c.position), (slice.remainder(c.position), window.prepareWithValue(c, vo.get)))
      }
      .redistribute(partitions)
      .group
      .tuneReducers(reducers)
      .sortBy { case (r, _) => r }(Position.Ordering(ascending))
      .scanLeft(Option.empty[(window.T, TraversableOnce[window.O])]) {
        case (None, (r, i)) => Some(window.initialise(r, i))
        case (Some((t, _)), (r, i)) => Some(window.update(r, i, t))
      }
      .flatMapWithValue(value) {
        case ((p, Some((_, o))), vo) => o.flatMap(window.presentWithValue(p, _, vo.get))
        case _ => None
      }
  }

  def split[I](partitioners: Partitionable[P, I]): U[(I, Cell[P])] = {
    val partitioner = partitioners()

    data.flatMap { case c => partitioner.assign(c).map { case q => (q, c) } }
  }

  def splitWithValue[I, W](partitioners: PartitionableWithValue[P, I, W], value: E[W]): U[(I, Cell[P])] = {
    val partitioner = partitioners()

    data.flatMapWithValue(value) { case (c, vo) => partitioner.assignWithValue(c, vo.get).map { case q => (q, c) } }
  }

  def stream[Q <: Position](command: String, files: List[String], writer: TextWriter,
    parser: Cell.TextParser[Q]): (U[Cell[Q]], U[String]) = {
    val contents = files.map { case f => (new File(f).getName, Files.readAllBytes(Paths.get(f))) }
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

    (result.collect { case Right(c) => c }, result.collect { case Left(e) => e })
  }

  def subset(samplers: Sampleable[P]): U[Cell[P]] = {
    val sampler = samplers()

    data.filter { case c => sampler.select(c) }
  }

  def subsetWithValue[W](samplers: SampleableWithValue[P, W], value: E[W]): U[Cell[P]] = {
    val sampler = samplers()

    data.filterWithValue(value) { case (c, vo) => sampler.selectWithValue(c, vo.get) }
  }

  type SummariseTuners[T] = TP2[T]
  def summarise[S <: Position with ExpandablePosition, Q <: Position, T <: Tuner : SummariseTuners](slice: Slice[P],
    aggregators: Aggregatable[P, S, Q], tuner: T = Default())(implicit ev1: slice.S =:= S, ev2: PosIncDep[S, Q],
      ev3: ClassTag[slice.S]): U[Cell[Q]] = {
    val aggregator = aggregators()

    Grouped(data.map { case c => (slice.selected(c.position), aggregator.map { case a => a.prepare(c) }) })
      .tuneReducers(tuner.parameters)
      .reduce[List[Option[Any]]] {
        case (lt, rt) => (aggregator, lt, rt).zipped.map {
          case (a, Some(l), Some(r)) => Some(a.reduce(l.asInstanceOf[a.T], r.asInstanceOf[a.T]))
          case (_, l, r) => if (l.isEmpty) { r } else { l }
        }
      }
      .flatMap {
        case (p, t) => (aggregator, t).zipped.flatMap {
          case (a, Some(s)) => a.present(p, s.asInstanceOf[a.T]).toTraversableOnce
          case _ => None
        }
      }
  }

  def summariseWithValue[S <: Position with ExpandablePosition, Q <: Position, W, T <: Tuner : SummariseTuners](
    slice: Slice[P], aggregators: AggregatableWithValue[P, S, Q, W], value: E[W], tuner: T = Default())(
      implicit ev1: slice.S =:= S, ev2: PosIncDep[S, Q], ev3: ClassTag[slice.S]): U[Cell[Q]] = {
    val aggregator = aggregators()

    data
      .mapWithValue(value) {
        case (c, vo) => (slice.selected(c.position), aggregator.map { case a => a.prepareWithValue(c, vo.get) })
      }
      .group
      .tuneReducers(tuner.parameters)
      .reduce[List[Option[Any]]] {
        case (lt, rt) => (aggregator, lt, rt).zipped.map {
          case (a, Some(l), Some(r)) => Some(a.reduce(l.asInstanceOf[a.T], r.asInstanceOf[a.T]))
          case (_, l, r) => if (l.isEmpty) { r } else { l }
        }
      }
      .flatMapWithValue(value) {
        case ((p, t), vo) => (aggregator, t).zipped.flatMap {
          case (a, Some(s)) => a.presentWithValue(p, s.asInstanceOf[a.T], vo.get).toTraversableOnce
          case _ => None
        }
      }
  }

  def toSequence[K <: Writable, V <: Writable](writer: SequenceWriter[K, V]): U[(K, V)] = data.flatMap(writer(_))

  def toText(writer: TextWriter): U[String] = data.flatMap(writer(_))

  def toVector(melt: (List[Value]) => Valueable): U[Cell[Position1D]] = {
    data.map { case Cell(p, c) => Cell(Position1D(melt(p.coordinates)), c) }
  }

  def transform[Q <: Position](transformers: Transformable[P, Q])(implicit ev: PosIncDep[P, Q]): U[Cell[Q]] = {
    val transformer = transformers()

    data.flatMap { case c => transformer.present(c) }
  }

  def transformWithValue[Q <: Position, W](transformers: TransformableWithValue[P, Q, W], value: E[W])(
    implicit ev: PosIncDep[P, Q]): U[Cell[Q]] = {
    val transformer = transformers()

    data.flatMapWithValue(value) { case (c, vo) => transformer.presentWithValue(c, vo.get) }
  }

  type TypesTuners[T] = TP2[T]
  def types[T <: Tuner : TypesTuners](slice: Slice[P], specific: Boolean, tuner: T = Default())(
    implicit ev1: slice.S =:!= Position0D, ev2: ClassTag[slice.S]): U[(slice.S, Type)] = {
    Grouped(data.map { case Cell(p, c) => (slice.selected(p), c.schema.kind) })
      .tuneReducers(tuner.parameters)
      .reduce[Type] { case (lt, rt) => Type.getCommonType(lt, rt) }
      .map { case (p, t) => (p, if (specific) t else t.getGeneralisation()) }
  }

  type UniqueTuners[T] = TP2[T]
  def unique[T <: Tuner : UniqueTuners](tuner: T = Default()): U[Content] = {
    val ordering = new Ordering[Content] { def compare(l: Content, r: Content) = l.toString.compare(r.toString) }

    data
      .map { case c => c.content }
      .tunedDistinct(tuner.parameters)(ordering)
  }

  def uniqueByPosition[T <: Tuner : UniqueTuners](slice: Slice[P], tuner: T = Default())(
    implicit ev1: slice.S =:!= Position0D): U[(slice.S, Content)] = {
    val ordering = new Ordering[Cell[slice.S]] {
      def compare(l: Cell[slice.S], r: Cell[slice.S]) = l.toString().compare(r.toString)
    }

    data
      .map { case Cell(p, c) => Cell(slice.selected(p), c) }
      .tunedDistinct(tuner.parameters)(ordering)
      .map { case Cell(p, c) => (p, c) }
  }

  type WhichTuners[T] = TP4[T]
  def which(predicate: Cell.Predicate[P])(implicit ev: ClassTag[P]): U[P] = {
    data.collect { case c if predicate(c) => c.position }
  }

  def whichByPosition[I, T <: Tuner : WhichTuners](slice: Slice[P], predicates: I, tuner: T = InMemory())(
    implicit ev1: FwPredicateable[I, P, slice.S, TypedPipe], ev2: ClassTag[slice.S], ev3: ClassTag[P]): U[P] = {
    val pp = ev1.convert(predicates)
      .map { case (pos, pred) => pos.map { case p => (p, pred) } }
      .reduce((l, r) => l ++ r)

    tuner match {
      case InMemory(_) =>
        type M = Map[slice.S, List[Cell.Predicate[P]]]
        val semigroup = new com.twitter.algebird.Semigroup[M] {
          def plus(l: M, r: M): M = { (l.toSeq ++ r.toSeq).groupBy(_._1).mapValues(_.map(_._2).toList.flatten) }
        }

        data
          .mapSideJoin(pp.map { case (pos, pred) => Map(pos -> List(pred)) }.sum(semigroup),
            (c: Cell[P], v: M) => v.get(slice.selected(c.position)).flatMap {
              case lst => if (lst.exists((pred) => pred(c))) { Some(c.position) } else { None }
            }, Map.empty[slice.S, List[Cell.Predicate[P]]])
      case _ =>
        data
          .groupBy { case c => slice.selected(c.position) }
          .tunedJoin(tuner, tuner.parameters, pp)
          .collect { case (_, (c, predicate)) if predicate(c) => c.position }
    }
  }

  protected def saveDictionary(slice: Slice[P], file: String, dictionary: String, separator: String)(
    implicit ctx: C, ct: ClassTag[slice.S]) = {
    import ctx._

    val numbered = names(slice)
      .groupAll
      .mapGroup { case (_, itr) => itr.zipWithIndex }
      .map { case (_, (p, i)) => (p, i) }

    numbered
      .map { case (Position1D(c), i) => c.toShortString + separator + i }
      .write(TypedSink(TextLine(dictionary.format(file, slice.dimension.index))))

    Grouped(numbered)
  }

  private def pairwiseTuples[T <: Tuner : PairwiseTuners](slice: Slice[P], comparer: Comparer, tuner: T)(
    ldata: U[Cell[P]], rdata: U[Cell[P]])(implicit ev1: ClassTag[slice.S]): U[(Cell[P], Cell[P])] = {
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
          case Sequence2(rj @ Reducers(_), Sequence2(lr, lj)) => (NoParameters(), rj, lr, lj)
          case Sequence2(rr @ Redistribute(_), Sequence2(lr, lj)) => (rr, NoParameters(), lr, lj)
          case Sequence2(Sequence2(rr, rj), lj @ Reducers(_)) => (rr, rj, NoParameters(), lj)
          case Sequence2(Sequence2(rr, rj), lr @ Redistribute(_)) => (rr, rj, lr, NoParameters())
          case Sequence2(rj @ Reducers(_), lj @ Reducers(_)) => (NoParameters(), rj, NoParameters(), lj)
          case Sequence2(rr @ Redistribute(_), lj @ Reducers(_)) => (rr, NoParameters(), NoParameters(), lj)
          case Sequence2(rj @ Reducers(_), lr @ Redistribute(_)) => (NoParameters(), rj, lr, NoParameters())
          case Sequence2(rr @ Redistribute(_), lr @ Redistribute(_)) => (rr, NoParameters(), lr, NoParameters())
          case lj @ Reducers(_) => (NoParameters(), NoParameters(), NoParameters(), lj)
          case _ => (NoParameters(), NoParameters(), NoParameters(), NoParameters())
        }
        val ordering = Position.Ordering[slice.S]()
        val right = rdata
          .map { case Cell(p, _) => slice.selected(p) }
          .redistribute(rr)
          .distinct(ordering)
          .map { case p => List(p) }
          .sum
        val keys = ldata
          .map { case Cell(p, _) => slice.selected(p) }
          .redistribute(lr)
          .distinct(ordering)
          .flatMapWithValue(right) {
            case (l, Some(v)) => v.collect { case r if comparer.keep(l, r) => (r, l) }
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
trait ReduceableMatrix[P <: Position with CompactablePosition with ReduceablePosition] extends FwReduceableMatrix[P] {
  self: Matrix[P] =>

  import ScaldingImplicits._

  def melt(dim: Dimension, into: Dimension, merge: (Value, Value) => Valueable)(implicit ev1: PosDimDep[P, dim.D],
    ev2: PosDimDep[P, into.D], ne: dim.D =:!= into.D): U[Cell[P#L]] = {
    data.map { case Cell(p, c) => Cell(p.melt(dim, into, merge), c) }
  }

  type SquashTuners[T] = TP2[T]
  def squash[T <: Tuner : SquashTuners](dim: Dimension, squasher: Squashable[P], tuner: T = Default())(
    implicit ev1: PosDimDep[P, dim.D], ev2: ClassTag[P#L]): U[Cell[P#L]] = {
    val squash = squasher()

    data
      .map { case c => (c.position.remove(dim), squash.prepare(c, dim)) }
      .group[P#L, squash.T]
      .tuneReducers(tuner.parameters)
      .reduce[squash.T] { case (lt, rt) => squash.reduce(lt, rt) }
      .flatMap { case (p, t) => squash.present(t).map { case c => Cell(p, c) } }
  }

  def squashWithValue[W, T <: Tuner : SquashTuners](dim: Dimension, squasher: SquashableWithValue[P, W], value: E[W],
    tuner: T = Default())(implicit ev1: PosDimDep[P, dim.D], ev2: ClassTag[P#L]): U[Cell[P#L]] = {
    val squash = squasher()

    data
      .mapWithValue(value) { case (c, vo) => (c.position.remove(dim), squash.prepareWithValue(c, dim, vo.get)) }
      .group[P#L, squash.T]
      .tuneReducers(tuner.parameters)
      .reduce[squash.T] { case (lt, rt) => squash.reduce(lt, rt) }
      .flatMapWithValue(value) { case ((p, t), vo) => squash.presentWithValue(t, vo.get).map { case c => Cell(p, c) } }
  }
}

/** Base trait for methods that reshapes the number of dimension of a matrix using a `TypedPipe[Cell[P]]`. */
trait ReshapeableMatrix[P <: Position with CompactablePosition with ExpandablePosition with ReduceablePosition]
  extends FwReshapeableMatrix[P] { self: Matrix[P] =>

  import ScaldingImplicits._

  type ReshapeTuners[T] = TP4[T]
  def reshape[Q <: Position, T <: Tuner : ReshapeTuners](dim: Dimension, coordinate: Valueable,
    locate: Locate.FromCellAndOptionalValue[P, Q], tuner: T = Default())(implicit ev1: PosDimDep[P, dim.D],
      ev2: PosExpDep[P, Q], ev3: ClassTag[P#L]): U[Cell[Q]] = {
    val keys = data
      .collect[(P#L, Value)] { case c if (c.position(dim) equ coordinate) => (c.position.remove(dim), c.content.value) }
    val values = data
      .collect[(P#L, Cell[P])] { case c if (c.position(dim) neq coordinate) => (c.position.remove(dim), c) }
    val append = (c: Cell[P], v: Option[Value]) => locate(c, v).map(Cell(_, c.content))

    tuner match {
      case InMemory(_) =>
        values
          .mapSideJoin(keys.toListValue((t: (P#L, Value)) => t), (t: (P#L, Cell[P]), v: List[(P#L, Value)]) =>
            append(t._2, v.find(_._1 == t._1).map(_._2)), List.empty[(P#L, Value)])
      case _ =>
        values
          .group
          .tunedLeftJoin(tuner, tuner.parameters, keys)
          .flatMap { case (_, (c, v)) => append(c, v) }
    }
  }
}

// TODO: Make this work on more than 2D matrices and share with Spark
trait MatrixDistance { self: Matrix[Position2D] with ReduceableMatrix[Position2D] =>

  import au.com.cba.omnia.grimlock.library.aggregate._
  import au.com.cba.omnia.grimlock.library.pairwise._
  import au.com.cba.omnia.grimlock.library.transform._
  import au.com.cba.omnia.grimlock.library.window._

  import au.com.cba.omnia.grimlock.scalding.Matrix._

  /**
   * Compute correlations.
   *
   * @param slice  Encapsulates the dimension for which to compute correlations.
   * @param stuner The sumamrise tuner for the job.
   * @param ptuner The pairwise tuner for the job.
   *
   * @return A `U[Cell[Position1D]]` with all pairwise correlations.
   */
  def correlation[ST <: Tuner : SummariseTuners, PT <: Tuner : PairwiseTuners](slice: Slice[Position2D],
    stuner: ST = Default(), ptuner: PT = Default())(implicit ev1: ClassTag[slice.S],
      ev2: ClassTag[slice.R]): U[Cell[Position1D]] = {
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
  def mutualInformation[ST <: Tuner : SummariseTuners, PT <: Tuner : PairwiseTuners](slice: Slice[Position2D],
    stuner: ST = Default(), ptuner: PT = Default())(implicit ev1: ClassTag[slice.S],
      ev2: ClassTag[slice.R]): U[Cell[Position1D]] = {
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
  def gini[ST <: Tuner : SummariseTuners, WT <: Tuner : SlideTuners, PT <: Tuner : PairwiseTuners](
    slice: Slice[Position2D], stuner: ST = Default(), wtuner: WT = Default(), ptuner: PT = Default())(
      implicit ev1: ClassTag[slice.S], ev2: ClassTag[slice.R]): U[Cell[Position1D]] = {
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
        ValuePipe(Map(Position1D("one") -> 1.0)))
  }
}

object Matrix extends Consume with DistributedData with Environment {
  def loadText[P <: Position](file: String, parser: Cell.TextParser[P])(
    implicit ctx: C): (U[Cell[P]], U[String]) = {
    val pipe = TypedPipe.from(TextLine(file)).flatMap { parser(_) }

    (pipe.collect { case Right(c) => c }, pipe.collect { case Left(e) => e })
  }

  def loadSequence[K <: Writable, V <: Writable, P <: Position](file: String, parser: Cell.SequenceParser[K, V, P])(
    implicit ctx: C, ev1: Manifest[K], ev2: Manifest[V]): (U[Cell[P]], U[String]) = {
    val pipe = TypedPipe.from(WritableSequenceFile[K, V](file)).flatMap { case (k, v) => parser(k, v) }

    (pipe.collect { case Right(c) => c }, pipe.collect { case Left(e) => e })
  }

  def loadParquet[T <: ThriftStruct : Manifest, P <: Position](file: String,
    parser: Cell.ParquetParser[T, P])(implicit ctx: C): (U[Cell[P]], U[String]) = {
    val pipe = TypedPipe.from(ParquetScroogeSource[T](file)).flatMap { parser(_) }

    (pipe.collect { case Right(c) => c }, pipe.collect { case Left(e) => e })
  }

  /** Conversion from `U[Cell[Position1D]]` to a Scalding `Matrix1D`. */
  implicit def TP2M1(data: U[Cell[Position1D]]): Matrix1D = Matrix1D(data)
  /** Conversion from `U[Cell[Position2D]]` to a Scalding `Matrix2D`. */
  implicit def TP2M2(data: U[Cell[Position2D]]): Matrix2D = Matrix2D(data)
  /** Conversion from `U[Cell[Position3D]]` to a Scalding `Matrix3D`. */
  implicit def TP2M3(data: U[Cell[Position3D]]): Matrix3D = Matrix3D(data)
  /** Conversion from `U[Cell[Position4D]]` to a Scalding `Matrix4D`. */
  implicit def TP2M4(data: U[Cell[Position4D]]): Matrix4D = Matrix4D(data)
  /** Conversion from `U[Cell[Position5D]]` to a Scalding `Matrix5D`. */
  implicit def TP2M5(data: U[Cell[Position5D]]): Matrix5D = Matrix5D(data)
  /** Conversion from `U[Cell[Position6D]]` to a Scalding `Matrix6D`. */
  implicit def TP2M6(data: U[Cell[Position6D]]): Matrix6D = Matrix6D(data)
  /** Conversion from `U[Cell[Position7D]]` to a Scalding `Matrix7D`. */
  implicit def TP2M7(data: U[Cell[Position7D]]): Matrix7D = Matrix7D(data)
  /** Conversion from `U[Cell[Position8D]]` to a Scalding `Matrix8D`. */
  implicit def TP2M8(data: U[Cell[Position8D]]): Matrix8D = Matrix8D(data)
  /** Conversion from `U[Cell[Position9D]]` to a Scalding `Matrix9D`. */
  implicit def TP2M9(data: U[Cell[Position9D]]): Matrix9D = Matrix9D(data)

  /** Conversion from `List[Cell[Position1D]]` to a Scalding `Matrix1D`. */
  implicit def L2TPM1(data: List[Cell[Position1D]]): Matrix1D = Matrix1D(IterablePipe(data))
  /** Conversion from `List[Cell[Position2D]]` to a Scalding `Matrix2D`. */
  implicit def L2TPM2(data: List[Cell[Position2D]]): Matrix2D = Matrix2D(IterablePipe(data))
  /** Conversion from `List[Cell[Position3D]]` to a Scalding `Matrix3D`. */
  implicit def L2TPM3(data: List[Cell[Position3D]]): Matrix3D = Matrix3D(IterablePipe(data))
  /** Conversion from `List[Cell[Position4D]]` to a Scalding `Matrix4D`. */
  implicit def L2TPM4(data: List[Cell[Position4D]]): Matrix4D = Matrix4D(IterablePipe(data))
  /** Conversion from `List[Cell[Position5D]]` to a Scalding `Matrix5D`. */
  implicit def L2TPM5(data: List[Cell[Position5D]]): Matrix5D = Matrix5D(IterablePipe(data))
  /** Conversion from `List[Cell[Position6D]]` to a Scalding `Matrix6D`. */
  implicit def L2TPM6(data: List[Cell[Position6D]]): Matrix6D = Matrix6D(IterablePipe(data))
  /** Conversion from `List[Cell[Position7D]]` to a Scalding `Matrix7D`. */
  implicit def L2TPM7(data: List[Cell[Position7D]]): Matrix7D = Matrix7D(IterablePipe(data))
  /** Conversion from `List[Cell[Position8D]]` to a Scalding `Matrix8D`. */
  implicit def L2TPM8(data: List[Cell[Position8D]]): Matrix8D = Matrix8D(IterablePipe(data))
  /** Conversion from `List[Cell[Position9D]]` to a Scalding `Matrix9D`. */
  implicit def L2TPM9(data: List[Cell[Position9D]]): Matrix9D = Matrix9D(IterablePipe(data))

  /** Conversion from `List[(Valueable, Content)]` to a Scalding `Matrix1D`. */
  implicit def LV1C2TPM1[V <% Valueable](list: List[(V, Content)]): Matrix1D = {
    Matrix1D(IterablePipe(list.map { case (v, c) => Cell(Position1D(v), c) }))
  }
  /** Conversion from `List[(Valueable, Valueable, Content)]` to a Scalding `Matrix2D`. */
  implicit def LV2C2TPM2[V <% Valueable, W <% Valueable](list: List[(V, W, Content)]): Matrix2D = {
    Matrix2D(IterablePipe(list.map { case (v, w, c) => Cell(Position2D(v, w), c) }))
  }
  /** Conversion from `List[(Valueable, Valueable, Valueable, Content)]` to a Scalding `Matrix3D`. */
  implicit def LV3C2TPM3[V <% Valueable, W <% Valueable, X <% Valueable](list: List[(V, W, X, Content)]): Matrix3D = {
    Matrix3D(IterablePipe(list.map { case (v, w, x, c) => Cell(Position3D(v, w, x), c) }))
  }
  /** Conversion from `List[(Valueable, Valueable, Valueable, Valueable, Content)]` to a Scalding `Matrix4D`. */
  implicit def LV4C2TPM4[V <% Valueable, W <% Valueable, X <% Valueable, Y <% Valueable](
    list: List[(V, W, X, Y, Content)]): Matrix4D = {
    Matrix4D(IterablePipe(list.map { case (v, w, x, y, c) => Cell(Position4D(v, w, x, y), c) }))
  }
  /**
   * Conversion from `List[(Valueable, Valueable, Valueable, Valueable, Valueable, Content)]` to a Scalding `Matrix5D`.
   */
  implicit def LV5C2TPM5[V <% Valueable, W <% Valueable, X <% Valueable, Y <% Valueable, Z <% Valueable](
    list: List[(V, W, X, Y, Z, Content)]): Matrix5D = {
    Matrix5D(IterablePipe(list.map { case (v, w, x, y, z, c) => Cell(Position5D(v, w, x, y, z), c) }))
  }
  /**
   * Conversion from `List[(Valueable, Valueable, Valueable, Valueable, Valueable, Valueable, Content)]` to a
   * Scalding `Matrix6D`.
   */
  implicit def LV6C2TPM6[T <% Valueable, V <% Valueable, W <% Valueable, X <% Valueable, Y <% Valueable, Z <% Valueable](
    list: List[(T, V, W, X, Y, Z, Content)]): Matrix6D = {
    Matrix6D(IterablePipe(list.map { case (t, v, w, x, y, z, c) => Cell(Position6D(t, v, w, x, y, z), c) }))
  }
  /**
   * Conversion from `List[(Valueable, Valueable, Valueable, Valueable, Valueable, Valueable, Valueable, Content)]`
   * to a Scalding `Matrix7D`.
   */
  implicit def LV7C2TPM7[S <% Valueable, T <% Valueable, V <% Valueable, W <% Valueable, X <% Valueable, Y <% Valueable, Z <% Valueable](
    list: List[(S, T, V, W, X, Y, Z, Content)]): Matrix7D = {
    Matrix7D(IterablePipe(list.map { case (s, t, v, w, x, y, z, c) => Cell(Position7D(s, t, v, w, x, y, z), c) }))
  }
  /**
   * Conversion from `List[(Valueable, Valueable, Valueable, Valueable, Valueable, Valueable, Valueable, Valueable,
   * Content)]` to a Scalding `Matrix8D`.
   */
  implicit def LV8C2TPM8[R <% Valueable, S <% Valueable, T <% Valueable, V <% Valueable, W <% Valueable, X <% Valueable, Y <% Valueable, Z <% Valueable](
    list: List[(R, S, T, V, W, X, Y, Z, Content)]): Matrix8D = {
    Matrix8D(IterablePipe(list.map { case (r, s, t, v, w, x, y, z, c) => Cell(Position8D(r, s, t, v, w, x, y, z), c) }))
  }
  /**
   * Conversion from `List[(Valueable, Valueable, Valueable, Valueable, Valueable, Valueable, Valueable, Valueable,
   * Valueable, Content)]` to a Scalding `Matrix9D`.
   */
  implicit def LV9C2TPM9[Q <% Valueable, R <% Valueable, S <% Valueable, T <% Valueable, V <% Valueable, W <% Valueable, X <% Valueable, Y <% Valueable, Z <% Valueable](
    list: List[(Q, R, S, T, V, W, X, Y, Z, Content)]): Matrix9D = {
    Matrix9D(IterablePipe(list.map {
      case (q, r, s, t, v, w, x, y, z, c) => Cell(Position9D(q, r, s, t, v, w, x, y, z), c)
    }))
  }

  /** Conversion from matrix with errors tuple to `MatrixWithParseErrors`. */
  implicit def TP2MWPE[P <: Position](t: (U[Cell[P]], U[String])): MatrixWithParseErrors[P, TypedPipe] = {
    MatrixWithParseErrors(t._1, t._2)
  }
}

/**
 * Rich wrapper around a `TypedPipe[Cell[Position1D]]`.
 *
 * @param data `TypedPipe[Cell[Position1D]]`.
 */
case class Matrix1D(data: TypedPipe[Cell[Position1D]]) extends FwMatrix1D with Matrix[Position1D]
  with ApproximateDistribution[Position1D] {
  def domain[T <: Tuner : DomainTuners](tuner: T = Default()): U[Position1D] = names(Over(First))

  def saveAsIV(file: String, dictionary: String, separator: String)(implicit ctx: C): U[Cell[Position1D]] = {
    import ctx._

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
case class Matrix2D(data: TypedPipe[Cell[Position2D]]) extends FwMatrix2D with Matrix[Position2D]
  with ReduceableMatrix[Position2D] with ReshapeableMatrix[Position2D] with MatrixDistance
    with ApproximateDistribution[Position2D] {
  def domain[T <: Tuner : DomainTuners](tuner: T = Default()): U[Position2D] = {
    names(Over(First))
      .map { case Position1D(c) => c }
      .cross(names(Over(Second)).map { case Position1D(c) => c })
      .map { case (c1, c2) => Position2D(c1, c2) }
  }

  def permute(dim1: Dimension, dim2: Dimension)(implicit ev1: PosDimDep[Position2D, dim1.D],
    ev2: PosDimDep[Position2D, dim2.D], ev3: dim1.D =:!= dim2.D): U[Cell[Position2D]] = {
    data.map { case Cell(p, c) => Cell(p.permute(List(dim1, dim2)), c) }
  }

  def saveAsCSV(slice: Slice[Position2D], file: String, separator: String, escapee: Escape, writeHeader: Boolean,
    header: String, writeRowId: Boolean, rowId: String)(implicit ctx: C, ct: ClassTag[slice.S]): U[Cell[Position2D]] = {
    import ctx._

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

  def saveAsIV(file: String, dictionary: String, separator: String)(implicit ctx: C): U[Cell[Position2D]] = {
    import ctx._

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
      implicit ctx: C): U[Cell[Position2D]] = {
    import ctx._

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
case class Matrix3D(data: TypedPipe[Cell[Position3D]]) extends FwMatrix3D with Matrix[Position3D]
  with ReduceableMatrix[Position3D] with ReshapeableMatrix[Position3D] with ApproximateDistribution[Position3D] {
  def domain[T <: Tuner : DomainTuners](tuner: T = Default()): U[Position3D] = {
    names(Over(First))
      .map { case Position1D(c) => c }
      .cross(names(Over(Second)).map { case Position1D(c) => c })
      .cross(names(Over(Third)).map { case Position1D(c) => c })
      .map { case ((c1, c2), c3) => Position3D(c1, c2, c3) }
  }

  def permute(dim1: Dimension, dim2: Dimension, dim3: Dimension)(implicit ev1: PosDimDep[Position3D, dim1.D],
    ev2: PosDimDep[Position3D, dim2.D], ev3: PosDimDep[Position3D, dim3.D],
      ev4: Distinct[(dim1.D, dim2.D, dim3.D)]): U[Cell[Position3D]] = {
    data.map { case Cell(p, c) => Cell(p.permute(List(dim1, dim2, dim3)), c) }
  }

  def saveAsIV(file: String, dictionary: String, separator: String)(implicit ctx: C): U[Cell[Position3D]] = {
    import ctx._

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
case class Matrix4D(data: TypedPipe[Cell[Position4D]]) extends FwMatrix4D with Matrix[Position4D]
  with ReduceableMatrix[Position4D] with ReshapeableMatrix[Position4D] with ApproximateDistribution[Position4D] {
  def domain[T <: Tuner : DomainTuners](tuner: T = Default()): U[Position4D] = {
    names(Over(First))
      .map { case Position1D(c) => c }
      .cross(names(Over(Second)).map { case Position1D(c) => c })
      .cross(names(Over(Third)).map { case Position1D(c) => c })
      .cross(names(Over(Fourth)).map { case Position1D(c) => c })
      .map { case (((c1, c2), c3), c4) => Position4D(c1, c2, c3, c4) }
  }

  def permute(dim1: Dimension, dim2: Dimension, dim3: Dimension, dim4: Dimension)(
    implicit ev1: PosDimDep[Position4D, dim1.D], ev2: PosDimDep[Position4D, dim2.D],
      ev3: PosDimDep[Position4D, dim3.D], ev4: PosDimDep[Position4D, dim4.D],
        ev5: Distinct[(dim1.D, dim2.D, dim3.D, dim4.D)]): U[Cell[Position4D]] = {
    data.map { case Cell(p, c) => Cell(p.permute(List(dim1, dim2, dim3, dim4)), c) }
  }

  def saveAsIV(file: String, dictionary: String, separator: String)(implicit ctx: C): U[Cell[Position4D]] = {
    import ctx._

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
case class Matrix5D(data: TypedPipe[Cell[Position5D]]) extends FwMatrix5D with Matrix[Position5D]
  with ReduceableMatrix[Position5D] with ReshapeableMatrix[Position5D] with ApproximateDistribution[Position5D] {
  def domain[T <: Tuner : DomainTuners](tuner: T = Default()): U[Position5D] = {
    names(Over(First))
      .map { case Position1D(c) => c }
      .cross(names(Over(Second)).map { case Position1D(c) => c })
      .cross(names(Over(Third)).map { case Position1D(c) => c })
      .cross(names(Over(Fourth)).map { case Position1D(c) => c })
      .cross(names(Over(Fifth)).map { case Position1D(c) => c })
      .map { case ((((c1, c2), c3), c4), c5) => Position5D(c1, c2, c3, c4, c5) }
  }

  def permute(dim1: Dimension, dim2: Dimension, dim3: Dimension, dim4: Dimension, dim5: Dimension)(
    implicit ev1: PosDimDep[Position5D, dim1.D], ev2: PosDimDep[Position5D, dim2.D],
      ev3: PosDimDep[Position5D, dim3.D], ev4: PosDimDep[Position5D, dim4.D], ev5: PosDimDep[Position5D, dim5.D],
          ev6: Distinct[(dim1.D, dim2.D, dim3.D, dim4.D, dim5.D)]): U[Cell[Position5D]] = {
    data.map { case Cell(p, c) => Cell(p.permute(List(dim1, dim2, dim3, dim4, dim5)), c) }
  }

  def saveAsIV(file: String, dictionary: String, separator: String)(implicit ctx: C): U[Cell[Position5D]] = {
    import ctx._

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
case class Matrix6D(data: TypedPipe[Cell[Position6D]]) extends FwMatrix6D with Matrix[Position6D]
  with ReduceableMatrix[Position6D] with ReshapeableMatrix[Position6D] with ApproximateDistribution[Position6D] {
  def domain[T <: Tuner : DomainTuners](tuner: T = Default()): U[Position6D] = {
    names(Over(First))
      .map { case Position1D(c) => c }
      .cross(names(Over(Second)).map { case Position1D(c) => c })
      .cross(names(Over(Third)).map { case Position1D(c) => c })
      .cross(names(Over(Fourth)).map { case Position1D(c) => c })
      .cross(names(Over(Fifth)).map { case Position1D(c) => c })
      .cross(names(Over(Sixth)).map { case Position1D(c) => c })
      .map { case (((((c1, c2), c3), c4), c5), c6) => Position6D(c1, c2, c3, c4, c5, c6) }
  }

  def permute(dim1: Dimension, dim2: Dimension, dim3: Dimension, dim4: Dimension, dim5: Dimension, dim6: Dimension)(
    implicit ev1: PosDimDep[Position6D, dim1.D], ev2: PosDimDep[Position6D, dim2.D],
      ev3: PosDimDep[Position6D, dim3.D], ev4: PosDimDep[Position6D, dim4.D],
        ev5: PosDimDep[Position6D, dim5.D], ev6: PosDimDep[Position6D, dim6.D],
          ev7: Distinct[(dim1.D, dim2.D, dim3.D, dim4.D, dim5.D, dim6.D)]): U[Cell[Position6D]] = {
    data.map { case Cell(p, c) => Cell(p.permute(List(dim1, dim2, dim3, dim4, dim5, dim6)), c) }
  }

  def saveAsIV(file: String, dictionary: String, separator: String)(implicit ctx: C): U[Cell[Position6D]] = {
    import ctx._

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
case class Matrix7D(data: TypedPipe[Cell[Position7D]]) extends FwMatrix7D with Matrix[Position7D]
  with ReduceableMatrix[Position7D] with ReshapeableMatrix[Position7D] with ApproximateDistribution[Position7D] {
  def domain[T <: Tuner : DomainTuners](tuner: T = Default()): U[Position7D] = {
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

  def permute(dim1: Dimension, dim2: Dimension, dim3: Dimension, dim4: Dimension, dim5: Dimension, dim6: Dimension,
    dim7: Dimension)(implicit ev1: PosDimDep[Position7D, dim1.D], ev2: PosDimDep[Position7D, dim2.D],
      ev3: PosDimDep[Position7D, dim3.D], ev4: PosDimDep[Position7D, dim4.D], ev5: PosDimDep[Position7D, dim5.D],
        ev6: PosDimDep[Position7D, dim6.D], ev7: PosDimDep[Position7D, dim7.D],
          ev8: Distinct[(dim1.D, dim2.D, dim3.D, dim4.D, dim5.D, dim6.D, dim7.D)]): U[Cell[Position7D]] = {
    data.map { case Cell(p, c) => Cell(p.permute(List(dim1, dim2, dim3, dim4, dim5, dim6, dim7)), c) }
  }

  def saveAsIV(file: String, dictionary: String, separator: String)(implicit ctx: C): U[Cell[Position7D]] = {
    import ctx._

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
case class Matrix8D(data: TypedPipe[Cell[Position8D]]) extends FwMatrix8D with Matrix[Position8D]
  with ReduceableMatrix[Position8D] with ReshapeableMatrix[Position8D] with ApproximateDistribution[Position8D] {
  def domain[T <: Tuner : DomainTuners](tuner: T = Default()): U[Position8D] = {
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

  def permute(dim1: Dimension, dim2: Dimension, dim3: Dimension, dim4: Dimension, dim5: Dimension, dim6: Dimension,
    dim7: Dimension, dim8: Dimension)(implicit ev1: PosDimDep[Position8D, dim1.D], ev2: PosDimDep[Position8D, dim2.D],
      ev3: PosDimDep[Position8D, dim3.D], ev4: PosDimDep[Position8D, dim4.D], ev5: PosDimDep[Position8D, dim5.D],
        ev6: PosDimDep[Position8D, dim6.D], ev7: PosDimDep[Position8D, dim7.D], ev8: PosDimDep[Position8D, dim8.D],
            ev9: Distinct[(dim1.D, dim2.D, dim3.D, dim4.D, dim5.D, dim6.D, dim7.D, dim8.D)]): U[Cell[Position8D]] = {
    data.map { case Cell(p, c) => Cell(p.permute(List(dim1, dim2, dim3, dim4, dim5, dim6, dim7, dim8)), c) }
  }

  def saveAsIV(file: String, dictionary: String, separator: String)(implicit ctx: C): U[Cell[Position8D]] = {
    import ctx._

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
case class Matrix9D(data: TypedPipe[Cell[Position9D]]) extends FwMatrix9D with Matrix[Position9D]
  with ReduceableMatrix[Position9D] with ApproximateDistribution[Position9D] {
  def domain[T <: Tuner : DomainTuners](tuner: T = Default()): U[Position9D] = {
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

  def permute(dim1: Dimension, dim2: Dimension, dim3: Dimension, dim4: Dimension, dim5: Dimension, dim6: Dimension,
    dim7: Dimension, dim8: Dimension, dim9: Dimension)(implicit ev1: PosDimDep[Position9D, dim1.D],
      ev2: PosDimDep[Position9D, dim2.D], ev3: PosDimDep[Position9D, dim3.D], ev4: PosDimDep[Position9D, dim4.D],
        ev5: PosDimDep[Position9D, dim5.D], ev6: PosDimDep[Position9D, dim6.D], ev7: PosDimDep[Position9D, dim7.D],
          ev8: PosDimDep[Position9D, dim8.D], ev9: PosDimDep[Position9D, dim9.D],
            ev10: Distinct[(dim1.D, dim2.D, dim3.D, dim4.D, dim5.D, dim6.D, dim7.D, dim8.D, dim9.D)]): U[Cell[Position9D]] = {
    data.map { case Cell(p, c) => Cell(p.permute(List(dim1, dim2, dim3, dim4, dim5, dim6, dim7, dim8, dim9)), c) }
  }

  def saveAsIV(file: String, dictionary: String, separator: String)(implicit ctx: C): U[Cell[Position9D]] = {
    import ctx._

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

/** Scalding companion object for the `Matrixable` trait. */
object Matrixable {
  /** Converts a `TypedPipe[Cell[P]]` into a `TypedPipe[Cell[P]]`; that is, it is a  pass through. */
  implicit def TPC2TPM[P <: Position](t: TypedPipe[Cell[P]]): FwMatrixable[P, TypedPipe] = {
    new FwMatrixable[P, TypedPipe] { def apply(): TypedPipe[Cell[P]] = t }
  }

  /** Converts a `List[Cell[P]]` into a `TypedPipe[Cell[P]]`. */
  implicit def LC2TPM[P <: Position](t: List[Cell[P]]): FwMatrixable[P, TypedPipe] = {
    new FwMatrixable[P, TypedPipe] { def apply(): TypedPipe[Cell[P]] = IterablePipe(t) }
  }

  /** Converts a `Cell[P]` into a `TypedPipe[Cell[P]]`. */
  implicit def C2TPM[P <: Position](t: Cell[P]): FwMatrixable[P, TypedPipe] = {
    new FwMatrixable[P, TypedPipe] { def apply(): TypedPipe[Cell[P]] = IterablePipe(List(t)) }
  }
}

/** Scalding companion object for the `Predicateable` type class. */
object Predicateable {
  /**
   * Converts a `List[(PositionDistributable[I, S, U], Cell.Predicate[P])]` to a `List[(U[S], Cell.Predicate[P])]`.
   */
  implicit def PDPT2LTPP[I, P <: Position, S <: Position](
    implicit ev: PositionDistributable[I, S, TypedPipe]): FwPredicateable[(I, Cell.Predicate[P]), P, S, TypedPipe] = {
    new FwPredicateable[(I, Cell.Predicate[P]), P, S, TypedPipe] {
      def convert(t: (I, Cell.Predicate[P])): List[(TypedPipe[S], Cell.Predicate[P])] = List((ev.convert(t._1), t._2))
    }
  }

  /**
   * Converts a `(PositionDistributable[I, S, U], Cell.Predicate[P])` to a `List[(U[S], Cell.Predicate[P])]`.
   */
  implicit def LPDP2LTPP[I, P <: Position, S <: Position](implicit ev: PositionDistributable[I, S, TypedPipe]): FwPredicateable[List[(I, Cell.Predicate[P])], P, S, TypedPipe] = {
    new FwPredicateable[List[(I, Cell.Predicate[P])], P, S, TypedPipe] {
      def convert(t: List[(I, Cell.Predicate[P])]): List[(TypedPipe[S], Cell.Predicate[P])] = {
        t.map { case (i, p) => (ev.convert(i), p) }
      }
    }
  }
}

