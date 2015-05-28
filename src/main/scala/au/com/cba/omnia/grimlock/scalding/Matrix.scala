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

package au.com.cba.omnia.grimlock.scalding

import au.com.cba.omnia.grimlock.framework.{
  Along,
  Cell,
  ExpandableMatrix => BaseExpandableMatrix,
  ExpPosDep,
  ExtractWithDimension,
  ExtractWithKey,
  Matrix => BaseMatrix,
  Matrixable => BaseMatrixable,
  Nameable => BaseNameable,
  Over,
  ReduceableMatrix => BaseReduceableMatrix,
  Slice,
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
import au.com.cba.omnia.grimlock.framework.window._

import au.com.cba.omnia.grimlock.scalding.Matrix._
import au.com.cba.omnia.grimlock.scalding.Matrixable._

import cascading.flow.FlowDef
import com.twitter.scalding.{ Mode, TextLine }
import com.twitter.scalding.typed.{ IterablePipe, Grouped, TypedPipe, TypedSink, ValuePipe }

import java.io.{ File, PrintWriter }
import java.lang.{ ProcessBuilder, Thread }
import java.nio.file.Paths

import scala.io.Source
import scala.reflect.ClassTag

/** Base trait for matrix operations using a `TypedPipe[Cell[P]]`. */
trait Matrix[P <: Position] extends BaseMatrix[P] with Persist[Cell[P]] {
  type U[A] = TypedPipe[A]
  type E[B] = ValuePipe[B]
  type S = Matrix[P]

  def change[T, D <: Dimension](slice: Slice[P, D], positions: T, schema: Schema)(implicit ev1: PosDimDep[P, D],
    ev2: BaseNameable[T, P, slice.S, D, TypedPipe], ev3: ClassTag[slice.S]): U[Cell[P]] = {
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

  def get[T](positions: T)(implicit ev1: PositionDistributable[T, P, TypedPipe], ev2: ClassTag[P]): U[Cell[P]] = {
    data
      .groupBy { case c => c.position }
      .join(ev1.convert(positions).groupBy { case p => p })
      .map { case (_, (c, p)) => c }
  }

  def join[D <: Dimension](slice: Slice[P, D], that: S)(implicit ev1: PosDimDep[P, D], ev2: P =!= Position1D,
    ev3: ClassTag[slice.S]): U[Cell[P]] = {
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
    ev3: ClassTag[slice.S]): U[(slice.S, Long)] = {
    Names.number(data.map { case c => slice.selected(c.position) }.distinct)
  }

  def pairwise[D <: Dimension, T](slice: Slice[P, D], operators: T)(implicit ev1: PosDimDep[P, D], ev2: Operable[T],
    ev3: slice.S =!= Position0D, ev4: ClassTag[slice.S], ev5: ClassTag[slice.R]): U[Cell[slice.R#M]] = {
    val o = ev2.convert(operators)

    pairwise(slice).flatMap { case (lc, rc, r) => o.compute(slice)(lc, rc, r).toList }
  }

  def pairwiseWithValue[D <: Dimension, T, W](slice: Slice[P, D], operators: T, value: E[W])(
    implicit ev1: PosDimDep[P, D], ev2: OperableWithValue[T, W], ev3: slice.S =!= Position0D, ev4: ClassTag[slice.S],
    ev5: ClassTag[slice.R]): U[Cell[slice.R#M]] = {
    val o = ev2.convert(operators)

    pairwise(slice)
      .flatMapWithValue(value) { case ((lc, rc, r), vo) => o.compute(slice, vo.get)(lc, rc, r).toList }
  }

  def pairwiseBetween[D <: Dimension, T](slice: Slice[P, D], that: S, operators: T)(implicit ev1: PosDimDep[P, D],
    ev2: Operable[T], ev3: slice.S =!= Position0D, ev4: ClassTag[slice.S],
    ev5: ClassTag[slice.R]): U[Cell[slice.R#M]] = {
    val o = ev2.convert(operators)

    pairwiseBetween(slice, that).flatMap { case (lc, rc, r) => o.compute(slice)(lc, rc, r).toList }
  }

  def pairwiseBetweenWithValue[D <: Dimension, T, W](slice: Slice[P, D], that: S, operators: T, value: E[W])(
    implicit ev1: PosDimDep[P, D], ev2: OperableWithValue[T, W], ev3: slice.S =!= Position0D, ev4: ClassTag[slice.S],
    ev5: ClassTag[slice.R]): U[Cell[slice.R#M]] = {
    val o = ev2.convert(operators)

    pairwiseBetween(slice, that)
      .flatMapWithValue(value) { case ((lc, rc, r), vo) => o.compute(slice, vo.get)(lc, rc, r).toList }
  }

  def rename(renamer: (Cell[P]) => P): U[Cell[P]] = data.map { case c => Cell(renamer(c), c.content) }

  def renameWithValue[W](renamer: (Cell[P], W) => P, value: E[W]): U[Cell[P]] = {
    data.mapWithValue(value) { case (c, vo) => Cell(renamer(c, vo.get), c.content) }
  }

  def sample[T](samplers: T)(implicit ev: Sampleable[T, P]): U[Cell[P]] = {
    val sampler = ev.convert(samplers)

    data.filter { case c => sampler.select(c) }
  }

  def sampleWithValue[T, W](samplers: T, value: E[W])(implicit ev: SampleableWithValue[T, P, W]): U[Cell[P]] = {
    val sampler = ev.convert(samplers)

    data.filterWithValue(value) { case (c, vo) => sampler.selectWithValue(c, vo.get) }
  }

  def set[T](positions: T, value: Content)(implicit ev1: PositionDistributable[T, P, TypedPipe],
    ev2: ClassTag[P]): U[Cell[P]] = {
    set(ev1.convert(positions).map { case p => Cell(p, value) })
  }

  def set[T](values: T)(implicit ev1: BaseMatrixable[T, P, TypedPipe], ev2: ClassTag[P]): U[Cell[P]] = {
    data
      .groupBy { case c => c.position }
      .outerJoin(ev1.convert(values).groupBy { case c => c.position })
      .map { case (_, (co, cn)) => cn.getOrElse(co.get) }
  }

  def shape(): U[Cell[Position1D]] = {
    data
      .flatMap { case c => c.position.coordinates.map(_.toString).zipWithIndex }
      .distinct
      .groupBy { case (s, i) => i }
      .size
      .map {
        case (i, s) => Cell(Position1D(Dimension.All(i).toString), Content(DiscreteSchema[Codex.LongCodex](), s))
      }
  }

  def size[D <: Dimension](dim: D, distinct: Boolean = false)(implicit ev: PosDimDep[P, D]): U[Cell[Position1D]] = {
    val coords = data.map { case c => c.position(dim) }
    val dist = if (distinct) { coords } else { coords.distinct(Value.Ordering) }

    dist
      .map { case _ => 1L }
      .sum
      .map { case sum => Cell(Position1D(dim.toString), Content(DiscreteSchema[Codex.LongCodex](), sum)) }
  }

  def slice[T, D <: Dimension](slice: Slice[P, D], positions: T, keep: Boolean)(implicit ev1: PosDimDep[P, D],
    ev2: BaseNameable[T, P, slice.S, D, TypedPipe], ev3: ClassTag[slice.S]): U[Cell[P]] = {
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

  def slide[D <: Dimension, T](slice: Slice[P, D], windows: T)(implicit ev1: PosDimDep[P, D], ev2: Windowable[T],
    ev3: slice.R =!= Position0D, ev4: ClassTag[slice.S], ev5: ClassTag[slice.R]): U[Cell[slice.S#M]] = {
    val w = ev2.convert(windows)

    data
      .map { case Cell(p, c) => (Cell(slice.selected(p), c), slice.remainder(p)) }
      .groupBy { case (c, r) => c.position }
      .sortBy { case (c, r) => r }
      .scanLeft(Option.empty[(w.T, Collection[Cell[slice.S#M]])]) {
        case (None, (c, r)) => Some((w.initialise(slice)(c, r), Collection[Cell[slice.S#M]]()))
        case (Some((t, _)), (c, r)) => Some(w.present(slice)(c, r, t))
      }
      .flatMap {
        case (p, Some((t, c))) => c.toList
        case _ => List()
      }
  }

  def slideWithValue[D <: Dimension, T, W](slice: Slice[P, D], windows: T, value: E[W])(
    implicit ev1: PosDimDep[P, D], ev2: WindowableWithValue[T, W], ev3: slice.R =!= Position0D,
    ev4: ClassTag[slice.S], ev5: ClassTag[slice.R]): U[Cell[slice.S#M]] = {
    val w = ev2.convert(windows)

    data
      .mapWithValue(value) { case (Cell(p, c), vo) => (Cell(slice.selected(p), c), slice.remainder(p), vo.get) }
      .groupBy { case (c, r, v) => c.position }
      .sortBy { case (c, r, v) => r }
      .scanLeft(Option.empty[(w.T, Collection[Cell[slice.S#M]])]) {
        case (None, (c, r, v)) => Some((w.initialise(slice, v)(c, r), Collection[Cell[slice.S#M]]()))
        case (Some((t, _)), (c, r, v)) => Some(w.present(slice, v)(c, r, t))
      }
      .flatMap {
        case (p, Some((t, c))) => c.toList
        case _ => List()
      }
  }

  def split[I, T](partitioners: T)(implicit ev: Partitionable[T, P, I]): U[(I, Cell[P])] = {
    val partitioner = ev.convert(partitioners)

    data.flatMap { case c => partitioner.assign(c).toList(c) }
  }

  def splitWithValue[I: Ordering, T, W](partitioners: T, value: E[W])(
    implicit ev: PartitionableWithValue[T, P, I, W]): U[(I, Cell[P])] = {
    val partitioner = ev.convert(partitioners)

    data.flatMapWithValue(value) { case (c, vo) => partitioner.assignWithValue(c, vo.get).toList(c) }
  }

  def stream[Q <: Position](command: String, script: String, separator: String,
    parser: String => Option[Cell[Q]]): U[Cell[Q]] = {
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

  def summariseAndExpand[T, D <: Dimension](slice: Slice[P, D], aggregators: T)(implicit ev1: PosDimDep[P, D],
    ev2: AggregatableMultiple[T], ev3: ClassTag[slice.S]): U[Cell[slice.S#M]] = {
    val a = ev2.convert(aggregators)

    data
      .map { case c => (slice.selected(c.position), a.prepare(slice, c)) }
      .groupBy { case (p, t) => p }
      .reduce[(slice.S, a.T)] { case ((lp, lt), (rp, rt)) => (lp, a.reduce(lt, rt)) }
      .flatMap { case (_, (p, t)) => a.presentMultiple(p, t).toList }
  }

  def summariseAndExpandWithValue[T, D <: Dimension, W](slice: Slice[P, D], aggregators: T, value: E[W])(
    implicit ev1: PosDimDep[P, D], ev2: AggregatableMultipleWithValue[T, W],
    ev3: ClassTag[slice.S]): U[Cell[slice.S#M]] = {
    val a = ev2.convert(aggregators)

    data
      .mapWithValue(value) { case (c, vo) => (slice.selected(c.position), a.prepareWithValue(slice, c, vo.get)) }
      .groupBy { case (p, t) => p }
      .reduce[(slice.S, a.T)] { case ((lp, lt), (rp, rt)) => (lp, a.reduce(lt, rt)) }
      .flatMapWithValue(value) { case ((_, (p, t)), vo) => a.presentMultipleWithValue(p, t, vo.get).toList }
  }

  def toMap()(implicit ev: ClassTag[P]): E[Map[P, Content]] = {
    data
      .map { case c => Map(c.position -> c.content) }
      .sum(new com.twitter.algebird.Semigroup[Map[P, Content]] {
        def plus(l: Map[P, Content], r: Map[P, Content]): Map[P, Content] = l ++ r
      })
  }

  def toMap[D <: Dimension](slice: Slice[P, D])(implicit ev1: PosDimDep[P, D], ev2: slice.S =!= Position0D,
    ev3: ClassTag[slice.S]): E[Map[slice.S, slice.C]] = {
    data
      .map { case c => (c.position, slice.toMap(c)) }
      .groupBy { case (p, m) => slice.selected(p) }
      .reduce[(P, Map[slice.S, slice.C])] { case ((lp, lm), (rp, rm)) => (lp, slice.combineMaps(lp, lm, rm)) }
      .map { case (_, (_, m)) => m }
      .sum(new com.twitter.algebird.Semigroup[Map[slice.S, slice.C]] {
        def plus(l: Map[slice.S, slice.C], r: Map[slice.S, slice.C]): Map[slice.S, slice.C] = l ++ r
      })
  }

  def transform[Q <: Position, T](transformers: T)(implicit ev: Transformable[T, P, Q]): U[Cell[Q]] = {
    val transformer = ev.convert(transformers)

    data.flatMap { case c => transformer.present(c).toList }
  }

  def transformWithValue[Q <: Position, T, W](transformers: T, value: E[W])(
    implicit ev: TransformableWithValue[T, P, Q, W]): U[Cell[Q]] = {
    val transformer = ev.convert(transformers)

    data.flatMapWithValue(value) { case (c, vo) => transformer.presentWithValue(c, vo.get).toList }
  }

  def types[D <: Dimension](slice: Slice[P, D], specific: Boolean = false)(implicit ev1: PosDimDep[P, D],
    ev2: slice.S =!= Position0D, ev3: ClassTag[slice.S]): U[(slice.S, Type)] = {
    data
      .map { case Cell(p, c) => (slice.selected(p), c.schema.kind) }
      .groupBy { case (p, t) => p }
      .reduce[(slice.S, Type)] { case ((lp, lt), (rp, rt)) => (lp, Type.getCommonType(lt, rt)) }
      .map { case (_, (p, t)) => (p, if (specific) t else t.getGeneralisation()) }
  }

  /** @note Comparison is performed based on the string representation of the `Content`. */
  def unique(): U[Content] = {
    data
      .map { case c => c.content }
      .distinct(new Ordering[Content] { def compare(l: Content, r: Content) = l.toString.compare(r.toString) })
  }

  /** @note Comparison is performed based on the string representation of the `Content`. */
  def unique[D <: Dimension](slice: Slice[P, D])(implicit ev: slice.S =!= Position0D): U[Cell[slice.S]] = {
    data
      .map { case Cell(p, c) => Cell(slice.selected(p), c) }
      .distinct(new Ordering[Cell[slice.S]] {
        def compare(l: Cell[slice.S], r: Cell[slice.S]) = l.toString.compare(r.toString)
      })
  }

  def which(predicate: Predicate)(implicit ev: ClassTag[P]): U[P] = {
    data.collect { case c if predicate(c) => c.position }
  }

  def which[T, D <: Dimension](slice: Slice[P, D], positions: T, predicate: Predicate)(implicit ev1: PosDimDep[P, D],
    ev2: BaseNameable[T, P, slice.S, D, TypedPipe], ev3: ClassTag[slice.S], ev4: ClassTag[P]): U[P] = {
    which(slice, List((positions, predicate)))
  }

  def which[T, D <: Dimension](slice: Slice[P, D], pospred: List[(T, Predicate)])(implicit ev1: PosDimDep[P, D],
    ev2: BaseNameable[T, P, slice.S, D, TypedPipe], ev3: ClassTag[slice.S], ev4: ClassTag[P]): U[P] = {
    val nampred = pospred.map { case (pos, pred) => ev2.convert(this, slice, pos).map { case (p, i) => (p, pred) } }
    val pipe = nampred.tail.foldLeft(nampred.head)((b, a) => b ++ a)

    data
      .groupBy { case c => slice.selected(c.position) }
      .join(pipe.groupBy { case (p, pred) => p })
      .collect { case (_, (c, (_, predicate))) if predicate(c) => c.position }
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

  private def pairwise[D <: Dimension](slice: Slice[P, D])(implicit ev1: PosDimDep[P, D],
    ev2: ClassTag[slice.S]): U[(Cell[slice.S], Cell[slice.S], slice.R)] = {
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

  private def pairwiseBetween[D <: Dimension](slice: Slice[P, D], that: S)(implicit ev1: PosDimDep[P, D],
    ev2: ClassTag[slice.S]): U[(Cell[slice.S], Cell[slice.S], slice.R)] = {
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
trait ReduceableMatrix[P <: Position with ReduceablePosition] extends BaseReduceableMatrix[P] { self: Matrix[P] =>

  def fill[D <: Dimension, Q <: Position](slice: Slice[P, D], values: U[Cell[Q]])(implicit ev1: PosDimDep[P, D],
    ev2: ClassTag[P], ev3: ClassTag[slice.S], ev4: slice.S =:= Q): U[Cell[P]] = {
    val dense = domain
      .groupBy[Slice[P, D]#S] { case p => slice.selected(p) }
      .join(values.groupBy { case c => c.position.asInstanceOf[slice.S] })
      .map { case (_, (p, c)) => Cell(p, c.content) }

    dense
      .groupBy { case c => c.position }
      .leftJoin(data.groupBy { case c => c.position })
      .map { case (p, (fc, co)) => co.getOrElse(fc) }
  }

  def fill(value: Content)(implicit ev: ClassTag[P]): U[Cell[P]] = {
    domain
      .groupBy { case p => p }
      .leftJoin(data.groupBy { case c => c.position })
      .map { case (p, (_, co)) => co.getOrElse(Cell(p, value)) }
  }

  def melt[D <: Dimension, F <: Dimension](dim: D, into: F, separator: String = ".")(implicit ev1: PosDimDep[P, D],
    ev2: PosDimDep[P, F], ne: D =!= F): U[Cell[P#L]] = {
    data.map { case Cell(p, c) => Cell(p.melt(dim, into, separator), c) }
  }

  def summarise[D <: Dimension](slice: Slice[P, D], aggregator: Aggregator with Prepare with PresentSingle)(
    implicit ev1: PosDimDep[P, D], ev2: ClassTag[slice.S]): U[Cell[slice.S]] = {
    data
      .map { case c => (slice.selected(c.position), aggregator.prepare(slice, c)) }
      .groupBy { case (p, t) => p }
      .reduce[(slice.S, aggregator.T)] { case ((lp, lt), (rp, rt)) => (lp, aggregator.reduce(lt, rt)) }
      .flatMap { case (_, (p, t)) => aggregator.presentSingle(p, t) }
  }

  def summariseWithValue[D <: Dimension, W](slice: Slice[P, D],
    aggregator: Aggregator with PrepareWithValue with PresentSingleWithValue { type V >: W }, value: E[W])(
      implicit ev1: PosDimDep[P, D], ev2: ClassTag[slice.S]): U[Cell[slice.S]] = {
    data
      .mapWithValue(value) {
        case (c, vo) => (slice.selected(c.position), aggregator.prepareWithValue(slice, c, vo.get))
      }
      .groupBy { case (p, t) => p }
      .reduce[(slice.S, aggregator.T)] { case ((lp, lt), (rp, rt)) => (lp, aggregator.reduce(lt, rt)) }
      .flatMapWithValue(value) { case ((_, (p, t)), vo) => aggregator.presentSingleWithValue(p, t, vo.get) }
  }

  def squash[D <: Dimension, T](dim: D, squasher: T)(implicit ev1: PosDimDep[P, D],
    ev2: Squashable[T, P]): U[Cell[P#L]] = {
    val squash = ev2.convert(squasher)

    data
      .groupBy { case c => c.position.remove(dim) }
      .reduce[Cell[P]] { case (x, y) => squash.reduce(dim, x, y) }
      .map { case (p, c) => Cell(p, c.content) }
  }

  def squashWithValue[D <: Dimension, T, W](dim: D, squasher: T, value: E[W])(implicit ev1: PosDimDep[P, D],
    ev2: SquashableWithValue[T, P, W]): U[Cell[P#L]] = {
    val squash = ev2.convert(squasher)

    data
      .leftCross(value)
      .groupBy { case (c, vo) => c.position.remove(dim) }
      .reduce[(Cell[P], Option[W])] { case ((x, xvo), (y, yvo)) => (squash.reduceWithValue(dim, x, y, xvo.get), xvo) }
      .map { case (p, (c, _)) => Cell(p, c.content) }
  }
}

/** Base trait for methods that expand the number of dimension of a matrix using a `TypedPipe[Cell[P]]`. */
trait ExpandableMatrix[P <: Position with ExpandablePosition] extends BaseExpandableMatrix[P] { self: Matrix[P] =>

  def expand[Q <: Position](expander: Cell[P] => Q)(implicit ev: ExpPosDep[P, Q]): TypedPipe[Cell[Q]] = {
    data.map { case c => Cell(expander(c), c.content) }
  }

  def expandWithValue[Q <: Position, W](expander: (Cell[P], W) => Q, value: ValuePipe[W])(
    implicit ev: ExpPosDep[P, Q]): TypedPipe[Cell[Q]] = {
    data.mapWithValue(value) { case (c, vo) => Cell(expander(c, vo.get), c.content) }
  }
}

// TODO: Make this work on more than 2D matrices and share with Spark
trait MatrixDistance { self: Matrix[Position2D] with ReduceableMatrix[Position2D] =>

  import au.com.cba.omnia.grimlock.library.aggregate._
  import au.com.cba.omnia.grimlock.library.pairwise._
  import au.com.cba.omnia.grimlock.library.transform._
  import au.com.cba.omnia.grimlock.library.window._

  def correlation[D <: Dimension](slice: Slice[Position2D, D])(implicit ev1: PosDimDep[Position2D, D],
    ev2: ClassTag[slice.S], ev3: ClassTag[slice.R]): U[Cell[Position1D]] = {
    implicit def UP2DSC2M1D(data: U[Cell[slice.S]]): Matrix1D = new Matrix1D(data.asInstanceOf[U[Cell[Position1D]]])
    implicit def UP2DRMC2M2D(data: U[Cell[slice.R#M]]): Matrix2D = new Matrix2D(data.asInstanceOf[U[Cell[Position2D]]])

    type V = Map[Position1D, Content]

    val mean = data
      .summarise(slice, Mean())
      .toMap(Over(First))

    val centered = data
      .transformWithValue[Position2D, Subtract[Position2D, V], V](Subtract(
        ExtractWithDimension(slice.dimension).andThenPresent((con: Content) => con.value.asDouble)), mean)

    val denom = centered
      .transform[Position2D, Power[Position2D]](Power(2))
      .summarise(slice, Sum())
      .pairwise(Over(First), Times())
      .transform[Position1D, SquareRoot[Position1D]](SquareRoot())
      .toMap(Over(First))

    centered
      .pairwise(slice, Times())
      .summarise(Over(First), Sum())
      .transformWithValue(Fraction(
        ExtractWithDimension[Dimension.First, Position1D, Content](First).andThenPresent(_.value.asDouble)), denom)
  }

  def mutualInformation[D <: Dimension](slice: Slice[Position2D, D])(implicit ev1: PosDimDep[Position2D, D],
    ev2: ClassTag[slice.S], ev3: ClassTag[slice.R]): U[Cell[Position1D]] = {
    implicit def UP2DSMC2M2D(data: U[Cell[slice.S#M]]): Matrix2D = new Matrix2D(data.asInstanceOf[U[Cell[Position2D]]])
    implicit def UP2DRMC2M2D(data: U[Cell[slice.R#M]]): Matrix2D = new Matrix2D(data.asInstanceOf[U[Cell[Position2D]]])

    val marginal = data
      .summariseAndExpand(slice, Entropy("marginal"))
      .pairwise(Over(First), Plus(name = "%s,%s", comparer = Upper))

    val joint = data
      .pairwise(slice, Concatenate(name = "%s,%s", comparer = Upper))
      .summariseAndExpand(Over(First), Entropy("joint", strict = true, nan = true, all = false, negate = true))

    (marginal ++ joint)
      .summarise(Over(First), Sum())
  }

  def gini[D <: Dimension](slice: Slice[Position2D, D])(implicit ev1: PosDimDep[Position2D, D],
    ev2: ClassTag[slice.S], ev3: ClassTag[slice.R]): U[Cell[Position1D]] = {
    implicit def UP2DSC2M1D(data: U[Cell[slice.S]]): Matrix1D = new Matrix1D(data.asInstanceOf[U[Cell[Position1D]]])
    implicit def UP2DSMC2M2D(data: U[Cell[slice.S#M]]): Matrix2D = new Matrix2D(data.asInstanceOf[U[Cell[Position2D]]])

    type V = Map[Position1D, Content]

    def isPositive(d: Double) = d > 0

    val extractor = ExtractWithDimension[Dimension.First, Position2D, Content](First).andThenPresent(_.value.asDouble)

    val pos = data
      .transform[Position2D, Compare[Position2D]](Compare(isPositive(_)))
      .summarise(slice, Sum())
      .toMap(Over(First))

    val neg = data
      .transform[Position2D, Compare[Position2D]](Compare(!isPositive(_)))
      .summarise(slice, Sum())
      .toMap(Over(First))

    val tpr = data
      .transform[Position2D, Compare[Position2D]](Compare(isPositive(_)))
      .slide(slice, CumulativeSum())
      .transformWithValue[Position2D, Fraction[Position2D, V], V](Fraction(extractor), pos)
      .slide(Over(First), Sliding((l: Double, r: Double) => r + l, name = "%2$s.%1$s"))

    val fpr = data
      .transform[Position2D, Compare[Position2D]](Compare(!isPositive(_)))
      .slide(slice, CumulativeSum())
      .transformWithValue[Position2D, Fraction[Position2D, V], V](Fraction(extractor), neg)
      .slide(Over(First), Sliding((l: Double, r: Double) => r - l, name = "%2$s.%1$s"))

    tpr
      .pairwiseBetween(Along(First), fpr, Times(comparer = Diagonal))
      .summarise(Along(First), Sum())
      .transformWithValue[Position1D, Subtract[Position1D, Map[Position1D, Double]], Map[Position1D, Double]](
        Subtract(ExtractWithKey("one"), true), ValuePipe(Map(Position1D("one") -> 1.0)))
  }
}

object Matrix {
  /**
   * Read column oriented, pipe separated matrix data into a `TypedPipe[Cell[Position1D]]`.
   *
   * @param file      The file to read from.
   * @param separator The column separator.
   * @param first     The codex for decoding the first dimension.
   */
  def load1D(file: String, separator: String = "|", first: Codex = StringCodex): TypedPipe[Cell[Position1D]] = {
    TypedPipe.from(TextLine(file)).flatMap { Cell.parse1D(separator, first)(_) }
  }

  /**
   * Read column oriented, pipe separated data into a `TypedPipe[Cell[Position1D]]`.
   *
   * @param file      The file to read from.
   * @param dict      The dictionary describing the features in the data.
   * @param separator The column separator.
   * @param first     The codex for decoding the first dimension.
   */
  def load1DWithDictionary(file: String, dict: Map[String, Schema], separator: String = "|",
    first: Codex = StringCodex): TypedPipe[Cell[Position1D]] = {
    TypedPipe.from(TextLine(file)).flatMap { Cell.parse1DWithDictionary(dict, separator, first)(_) }
  }

  /**
   * Read column oriented, pipe separated data into a `TypedPipe[Cell[Position1D]]`.
   *
   * @param file      The file to read from.
   * @param schema    The schema for decoding the data.
   * @param separator The column separator.
   * @param first     The codex for decoding the first dimension.
   */
  def load1DWithSchema(file: String, schema: Schema, separator: String = "|",
    first: Codex = StringCodex): TypedPipe[Cell[Position1D]] = {
    TypedPipe.from(TextLine(file)).flatMap { Cell.parse1DWithSchema(schema, separator, first)(_) }
  }

  /**
   * Read column oriented, pipe separated matrix data into a `TypedPipe[Cell[Position2D]]`.
   *
   * @param file      The file to read from.
   * @param separator The column separator.
   * @param first     The codex for decoding the first dimension.
   * @param second    The codex for decoding the second dimension.
   */
  def load2D(file: String, separator: String = "|", first: Codex = StringCodex,
    second: Codex = StringCodex): TypedPipe[Cell[Position2D]] = {
    TypedPipe.from(TextLine(file)).flatMap { Cell.parse2D(separator, first, second)(_) }
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
  def load2DWithDictionary[D <: Dimension](file: String, dict: Map[String, Schema], dim: D = Second,
    separator: String = "|", first: Codex = StringCodex, second: Codex = StringCodex)(
      implicit ev: PosDimDep[Position2D, D]): TypedPipe[Cell[Position2D]] = {
    TypedPipe.from(TextLine(file)).flatMap { Cell.parse2DWithDictionary(dict, dim, separator, first, second)(_) }
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
  def load2DWithSchema(file: String, schema: Schema, separator: String = "|", first: Codex = StringCodex,
    second: Codex = StringCodex): TypedPipe[Cell[Position2D]] = {
    TypedPipe.from(TextLine(file)).flatMap { Cell.parse2DWithSchema(schema, separator, first, second)(_) }
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
  def load3D(file: String, separator: String = "|", first: Codex = StringCodex, second: Codex = StringCodex,
    third: Codex = StringCodex): TypedPipe[Cell[Position3D]] = {
    TypedPipe.from(TextLine(file)).flatMap { Cell.parse3D(separator, first, second, third)(_) }
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
  def load3DWithDictionary[D <: Dimension](file: String, dict: Map[String, Schema], dim: D = Second,
    separator: String = "|", first: Codex = StringCodex, second: Codex = StringCodex, third: Codex = DateCodex)(
      implicit ev: PosDimDep[Position3D, D]): TypedPipe[Cell[Position3D]] = {
    TypedPipe.from(TextLine(file)).flatMap { Cell.parse3DWithDictionary(dict, dim, separator, first, second, third)(_) }
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
  def load3DWithSchema(file: String, schema: Schema, separator: String = "|", first: Codex = StringCodex,
    second: Codex = StringCodex, third: Codex = DateCodex): TypedPipe[Cell[Position3D]] = {
    TypedPipe.from(TextLine(file)).flatMap { Cell.parse3DWithSchema(schema, separator, first, second, third)(_) }
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
  def loadTable(table: String, columns: List[(String, Schema)], pkeyIndex: Int = 0,
    separator: String = "\u0001"): TypedPipe[Cell[Position2D]] = {
    TypedPipe.from(TextLine(table)).flatMap { Cell.parseTable(columns, pkeyIndex, separator)(_) }
  }

  /** Conversion from `TypedPipe[Cell[Position1D]]` to a Scalding `Matrix1D`. */
  implicit def TPP1DC2TPM1D(data: TypedPipe[Cell[Position1D]]): Matrix1D = new Matrix1D(data)
  /** Conversion from `TypedPipe[Cell[Position2D]]` to a Scalding `Matrix2D`. */
  implicit def TPP2DC2TPM2D(data: TypedPipe[Cell[Position2D]]): Matrix2D = new Matrix2D(data)
  /** Conversion from `TypedPipe[Cell[Position3D]]` to a Scalding `Matrix3D`. */
  implicit def TPP3DC2TPM3D(data: TypedPipe[Cell[Position3D]]): Matrix3D = new Matrix3D(data)
  /** Conversion from `TypedPipe[Cell[Position4D]]` to a Scalding `Matrix4D`. */
  implicit def TPP4DC2TPM4D(data: TypedPipe[Cell[Position4D]]): Matrix4D = new Matrix4D(data)
  /** Conversion from `TypedPipe[Cell[Position5D]]` to a Scalding `Matrix5D`. */
  implicit def TPP5DC2TPM5D(data: TypedPipe[Cell[Position5D]]): Matrix5D = new Matrix5D(data)

  /** Conversion from `List[(Valueable, Content)]` to a Scalding `Matrix1D`. */
  implicit def LVCT2TPM1D[V: Valueable](list: List[(V, Content)]): Matrix1D = {
    new Matrix1D(new IterablePipe(list.map { case (v, c) => Cell(Position1D(v), c) }))
  }
  /** Conversion from `List[(Valueable, Valueable, Content)]` to a Scalding `Matrix2D`. */
  implicit def LVVCT2TPM2D[V: Valueable, W: Valueable](list: List[(V, W, Content)]): Matrix2D = {
    new Matrix2D(new IterablePipe(list.map { case (v, w, c) => Cell(Position2D(v, w), c) }))
  }
  /** Conversion from `List[(Valueable, Valueable, Valueable, Content)]` to a Scalding `Matrix3D`. */
  implicit def LVVVCT2TPM3D[V: Valueable, W: Valueable, X: Valueable](
    list: List[(V, W, X, Content)]): Matrix3D = {
    new Matrix3D(new IterablePipe(list.map { case (v, w, x, c) => Cell(Position3D(v, w, x), c) }))
  }
  /** Conversion from `List[(Valueable, Valueable, Valueable, Valueable, Content)]` to a Scalding `Matrix4D`. */
  implicit def LVVVVCT2TPM4D[V: Valueable, W: Valueable, X: Valueable, Y: Valueable](
    list: List[(V, W, X, Y, Content)]): Matrix4D = {
    new Matrix4D(new IterablePipe(list.map { case (v, w, x, y, c) => Cell(Position4D(v, w, x, y), c) }))
  }
  /**
   * Conversion from `List[(Valueable, Valueable, Valueable, Valueable, Valueable, Content)]` to a Scalding `Matrix5D`.
   */
  implicit def LVVVVVCT2TPM5D[V: Valueable, W: Valueable, X: Valueable, Y: Valueable, Z: Valueable](
    list: List[(V, W, X, Y, Z, Content)]): Matrix5D = {
    new Matrix5D(new IterablePipe(list.map { case (v, w, x, y, z, c) => Cell(Position5D(v, w, x, y, z), c) }))
  }
}

/**
 * Rich wrapper around a `TypedPipe[Cell[Position1D]]`.
 *
 * @param data `TypedPipe[Cell[Position1D]]`.
 */
class Matrix1D(val data: TypedPipe[Cell[Position1D]]) extends Matrix[Position1D] with ExpandableMatrix[Position1D] {
  def domain(): U[Position1D] = names(Over(First)).map { case (p, i) => p }

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
  def saveAsIVWithNames(file: String, names: U[(Position1D, Long)], dictionary: String = "%1$s.dict.%2$d",
    separator: String = "|")(implicit flow: FlowDef, mode: Mode): U[Cell[Position1D]] = {
    data
      .groupBy { case c => c.position }
      .join(saveDictionary(names, file, dictionary, separator, First))
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
  def domain(): U[Position2D] = {
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
   * @return A `TypedPipe[Cell[Position2D]]`; that is it returns `data`.
   */
  def saveAsCSV[D <: Dimension](slice: Slice[Position2D, D], file: String, separator: String = "|",
    escapee: Escape = Quote(), writeHeader: Boolean = true, header: String = "%s.header", writeRowId: Boolean = true,
    rowId: String = "id")(implicit ev1: BaseNameable[TypedPipe[(slice.S, Long)], Position2D, slice.S, D, TypedPipe],
      ev2: PosDimDep[Position2D, D], ev3: ClassTag[slice.S], flow: FlowDef, mode: Mode): U[Cell[Position2D]] = {
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
  def saveAsCSVWithNames[T, D <: Dimension](slice: Slice[Position2D, D], file: String, names: T,
    separator: String = "|", escapee: Escape = Quote(), writeHeader: Boolean = true, header: String = "%s.header",
    writeRowId: Boolean = true, rowId: String = "id")(implicit ev1: BaseNameable[T, Position2D, slice.S, D, TypedPipe],
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
  def saveAsIVWithNames(file: String, namesI: U[(Position1D, Long)], namesJ: U[(Position1D, Long)],
    dictionary: String = "%1$s.dict.%2$d", separator: String = "|")(implicit flow: FlowDef,
      mode: Mode): U[Cell[Position2D]] = {
    data
      .groupBy { case c => Position1D(c.position(First)) }
      .join(saveDictionary(namesI, file, dictionary, separator, First))
      .values
      .groupBy { case (c, i) => Position1D(c.position(Second)) }
      .join(saveDictionary(namesJ, file, dictionary, separator, Second))
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
  def saveAsLDAWithNames[D <: Dimension](slice: Slice[Position2D, D], file: String, names: U[(Position1D, Long)],
    dictionary: String = "%s.dict", separator: String = "|", addId: Boolean = false)(
      implicit ev: PosDimDep[Position2D, D], flow: FlowDef, mode: Mode): U[Cell[Position2D]] = {
    data
      .groupBy { case c => slice.remainder(c.position).asInstanceOf[Position1D] }
      .join(saveDictionary(names, file, dictionary, separator))
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
  def saveAsVWWithNames[D <: Dimension](slice: Slice[Position2D, D], labels: U[Cell[Position1D]], file: String,
    names: U[(Position1D, Long)], dictionary: String = "%s.dict", separator: String = ":")(
      implicit ev: PosDimDep[Position2D, D], flow: FlowDef, mode: Mode): U[Cell[Position2D]] = {
    data
      .groupBy { case c => slice.remainder(c.position).asInstanceOf[Position1D] }
      .join(saveDictionary(names, file, dictionary, separator))
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
  def domain(): U[Position3D] = {
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
  def permute[D <: Dimension, F <: Dimension, G <: Dimension](first: D, second: F, third: G)(
    implicit ev1: PosDimDep[Position3D, D], ev2: PosDimDep[Position3D, F], ev3: PosDimDep[Position3D, G], ne1: D =!= F,
    ne2: D =!= G, ne3: F =!= G): U[Cell[Position3D]] = {
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
  def saveAsIVWithNames(file: String, namesI: U[(Position1D, Long)], namesJ: U[(Position1D, Long)],
    namesK: U[(Position1D, Long)], dictionary: String = "%1$s.dict.%2$d", separator: String = "|")(
      implicit flow: FlowDef, mode: Mode): U[Cell[Position3D]] = {
    data
      .groupBy { case c => Position1D(c.position(First)) }
      .join(saveDictionary(namesI, file, dictionary, separator, First))
      .values
      .groupBy { case (c, i) => Position1D(c.position(Second)) }
      .join(saveDictionary(namesJ, file, dictionary, separator, Second))
      .map { case (_, ((c, i), j)) => (c, i, j) }
      .groupBy { case (c, i, j) => Position1D(c.position(Third)) }
      .join(saveDictionary(namesK, file, dictionary, separator, Third))
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
  def domain(): U[Position4D] = {
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
  def permute[D <: Dimension, F <: Dimension, G <: Dimension, H <: Dimension](first: D, second: F, third: G,
    fourth: H)(implicit ev1: PosDimDep[Position4D, D], ev2: PosDimDep[Position4D, F], ev3: PosDimDep[Position4D, G],
      ev4: PosDimDep[Position4D, H], ne1: D =!= F, ne2: D =!= G, ne3: D =!= H, ne4: F =!= G, ne5: F =!= H,
      ne6: G =!= H): U[Cell[Position4D]] = {
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
  def saveAsIVWithNames(file: String, namesI: U[(Position1D, Long)], namesJ: U[(Position1D, Long)],
    namesK: U[(Position1D, Long)], namesL: U[(Position1D, Long)], dictionary: String = "%1$s.dict.%2$d",
    separator: String = "|")(implicit flow: FlowDef, mode: Mode): U[Cell[Position4D]] = {
    data
      .groupBy { case c => Position1D(c.position(First)) }
      .join(saveDictionary(namesI, file, dictionary, separator, First))
      .values
      .groupBy { case (c, i) => Position1D(c.position(Second)) }
      .join(saveDictionary(namesJ, file, dictionary, separator, Second))
      .map { case (_, ((c, i), j)) => (c, i, j) }
      .groupBy { case (c, i, j) => Position1D(c.position(Third)) }
      .join(saveDictionary(namesK, file, dictionary, separator, Third))
      .map { case (_, ((c, i, j), k)) => (c, i, j, k) }
      .groupBy { case (c, i, j, k) => Position1D(c.position(Fourth)) }
      .join(saveDictionary(namesL, file, dictionary, separator, Fourth))
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
class Matrix5D(val data: TypedPipe[Cell[Position5D]]) extends Matrix[Position5D] with ReduceableMatrix[Position5D] {
  def domain(): U[Position5D] = {
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
  def permute[D <: Dimension, F <: Dimension, G <: Dimension, H <: Dimension, I <: Dimension](first: D, second: F,
    third: G, fourth: H, fifth: I)(implicit ev1: PosDimDep[Position5D, D], ev2: PosDimDep[Position5D, F],
      ev3: PosDimDep[Position5D, G], ev4: PosDimDep[Position5D, H], ev5: PosDimDep[Position5D, I], ne1: D =!= F,
      ne2: D =!= G, ne3: D =!= H, ne4: D =!= I, ne5: F =!= G, ne6: F =!= H, ne7: F =!= I, ne8: G =!= H, ne9: G =!= I,
      ne10: H =!= I): U[Cell[Position5D]] = {
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
  def saveAsIVWithNames(file: String, namesI: U[(Position1D, Long)], namesJ: U[(Position1D, Long)],
    namesK: U[(Position1D, Long)], namesL: U[(Position1D, Long)], namesM: U[(Position1D, Long)],
    dictionary: String = "%1$s.dict.%2$d", separator: String = "|")(implicit flow: FlowDef,
      mode: Mode): U[Cell[Position5D]] = {
    data
      .groupBy { case c => Position1D(c.position(First)) }
      .join(saveDictionary(namesI, file, dictionary, separator, First))
      .values
      .groupBy { case (c, i) => Position1D(c.position(Second)) }
      .join(saveDictionary(namesJ, file, dictionary, separator, Second))
      .map { case (_, ((c, i), j)) => (c, i, j) }
      .groupBy { case (c, i, j) => Position1D(c.position(Third)) }
      .join(saveDictionary(namesK, file, dictionary, separator, Third))
      .map { case (_, ((c, i, j), k)) => (c, i, j, k) }
      .groupBy { case (c, i, j, k) => Position1D(c.position(Fourth)) }
      .join(saveDictionary(namesL, file, dictionary, separator, Fourth))
      .map { case (_, ((c, i, j, k), l)) => (c, i, j, k, l) }
      .groupBy { case (c, i, j, k, l) => Position1D(c.position(Fifth)) }
      .join(saveDictionary(namesM, file, dictionary, separator, Fifth))
      .map {
        case (_, ((c, i, j, k, l), m)) =>
          i + separator + j + separator + k + separator + l + separator + m + separator + c.content.value.toShortString
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

