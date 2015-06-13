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
  Along,
  Cell,
  ExpandableMatrix => BaseExpandableMatrix,
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

  def change[D <: Dimension, T](slice: Slice[P, D], positions: T, schema: Schema)(implicit ev1: PosDimDep[P, D],
    ev2: BaseNameable[T, P, slice.S, D, RDD], ev3: ClassTag[slice.S]): U[Cell[P]] = {
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

  def get[T](positions: T)(implicit ev1: PositionDistributable[T, P, RDD], ev2: ClassTag[P]): U[Cell[P]] = {
    data
      .keyBy { case c => c.position }
      .join(ev1.convert(positions).keyBy { case p => p })
      .map { case (_, (c, _)) => c }
  }

  def join[D <: Dimension](slice: Slice[P, D], that: S)(implicit ev1: PosDimDep[P, D], ev2: P =!= Position1D,
    ev3: ClassTag[slice.S]): U[Cell[P]] = {
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
    ev3: ClassTag[slice.S]): U[(slice.S, Long)] = {
    Names.number(data.map { case c => slice.selected(c.position) }.distinct)
  }

  def pairwise[D <: Dimension, Q <: Position, T](slice: Slice[P, D], comparer: Comparer, operators: T)(
    implicit ev1: PosDimDep[P, D], ev2: PosExpDep[slice.R#M, Q], ev3: Operable[T, slice.S, slice.R, Q],
    ev4: slice.S =!= Position0D, ev5: ClassTag[slice.S], ev6: ClassTag[slice.R]): U[Cell[Q]] = {
    val o = ev3.convert(operators)

    pairwiseTuples(slice, comparer)(names(slice), data, names(slice), data)
      .flatMap { case (lc, rc, r) => o.compute(lc, rc, r).toList }
  }

  def pairwiseWithValue[D <: Dimension, Q <: Position, T, W](slice: Slice[P, D], comparer: Comparer, operators: T,
    value: E[W])( implicit ev1: PosDimDep[P, D], ev2: PosExpDep[slice.R#M, Q],
      ev3: OperableWithValue[T, slice.S, slice.R, Q, W], ev4: slice.S =!= Position0D, ev5: ClassTag[slice.S],
      ev6: ClassTag[slice.R]): U[Cell[Q]] = {
    val o = ev3.convert(operators)

    pairwiseTuples(slice, comparer)(names(slice), data, names(slice), data)
      .flatMap { case (lc, rc, r) => o.computeWithValue(lc, rc, r, value).toList }
  }

  def pairwiseBetween[D <: Dimension, Q <: Position, T](slice: Slice[P, D], comparer: Comparer, that: S, operators: T)(
    implicit ev1: PosDimDep[P, D], ev2: PosExpDep[slice.R#M, Q], ev3: Operable[T, slice.S, slice.R, Q],
    ev4: slice.S =!= Position0D, ev5: ClassTag[slice.S], ev6: ClassTag[slice.R]): U[Cell[Q]] = {
    val o = ev3.convert(operators)

    pairwiseTuples(slice, comparer)(names(slice), data, that.names(slice), that.data)
      .flatMap { case (lc, rc, r) => o.compute(lc, rc, r).toList }
  }

  def pairwiseBetweenWithValue[D <: Dimension, Q <: Position, T, W](slice: Slice[P, D], comparer: Comparer, that: S,
    operators: T, value: E[W])(implicit ev1: PosDimDep[P, D], ev2: PosExpDep[slice.R#M, Q],
      ev3: OperableWithValue[T, slice.S, slice.R, Q, W], ev4: slice.S =!= Position0D, ev5: ClassTag[slice.S],
      ev6: ClassTag[slice.R]): U[Cell[Q]] = {
    val o = ev3.convert(operators)

    pairwiseTuples(slice, comparer)(names(slice), data, that.names(slice), that.data)
      .flatMap { case (lc, rc, r) => o.computeWithValue(lc, rc, r, value).toList }
  }

  def rename(renamer: (Cell[P]) => P): U[Cell[P]] = data.map { case c => Cell(renamer(c), c.content) }

  def renameWithValue[W](renamer: (Cell[P], W) => P, value: E[W]): U[Cell[P]] = {
    data.map { case c => Cell(renamer(c, value), c.content) }
  }

  def sample[T](samplers: T)(implicit ev: Sampleable[T, P]): U[Cell[P]] = {
    val sampler = ev.convert(samplers)

    data.filter { case c => sampler.select(c) }
  }

  def sampleWithValue[T, W](samplers: T, value: E[W])(implicit ev: SampleableWithValue[T, P, W]): U[Cell[P]] = {
    val sampler = ev.convert(samplers)

    data.filter { case c => sampler.selectWithValue(c, value) }
  }

  def set[T](positions: T, value: Content)(implicit ev1: PositionDistributable[T, P, RDD],
    ev2: ClassTag[P]): U[Cell[P]] = set(ev1.convert(positions).map { case p => Cell(p, value) })

  def set[T](values: T)(implicit ev1: BaseMatrixable[T, P, RDD], ev2: ClassTag[P]): U[Cell[P]] = {
    data
      .keyBy { case c => c.position }
      .fullOuterJoin(ev1.convert(values).keyBy { case c => c.position })
      .map { case (_, (c, cn)) => cn.getOrElse(c.get) }
  }

  def shape(): U[Cell[Position1D]] = {
    data
      .flatMap { case c => c.position.coordinates.map(_.toString).zipWithIndex }
      .distinct
      .groupBy { case (s, i) => i }
      .map {
        case (i, s) => Cell(Position1D(Dimension.All(i).toString), Content(DiscreteSchema[Codex.LongCodex](), s.size))
      }
  }

  def size[D <: Dimension](dim: D, distinct: Boolean = false)(implicit ev: PosDimDep[P, D]): U[Cell[Position1D]] = {
    val coords = data.map { case c => c.position(dim) }
    val dist = if (distinct) { coords } else { coords.distinct() }

    dist
      .context
      .parallelize(List(Cell(Position1D(dim.toString), Content(DiscreteSchema[Codex.LongCodex](), dist.count))))
  }

  def slice[D <: Dimension, T](slice: Slice[P, D], positions: T, keep: Boolean)(implicit ev1: PosDimDep[P, D],
    ev2: BaseNameable[T, P, slice.S, D, RDD], ev3: ClassTag[slice.S]): U[Cell[P]] = {
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

  def slide[D <: Dimension, Q <: Position, T](slice: Slice[P, D], windows: T)(implicit ev1: PosDimDep[P, D],
    ev2: PosExpDep[slice.S#M, Q], ev3: Windowable[T, slice.S, slice.R, Q], ev4: slice.R =!= Position0D,
    ev5: ClassTag[slice.S], ev6: ClassTag[slice.R]): U[Cell[Q]] = {
    val w = ev3.convert(windows)

    data
      .map { case Cell(p, c) => (Cell(slice.selected(p), c), slice.remainder(p)) }
      .groupBy { case (c, r) => c.position }
      .flatMap {
        case (_, itr) => itr
          .toList
          .sortBy { case (c, r) => r }
          .scanLeft(Option.empty[(w.T, Collection[Cell[Q]])]) {
            case (None, (c, r)) => Some(w.initialise(c, r))
            case (Some((t, _)), (c, r)) => Some(w.present(c, r, t))
          }
          .flatMap {
            case Some((t, c)) => c.toList
            case _ => List()
          }
      }
  }

  def slideWithValue[D <: Dimension, Q <: Position, T, W](slice: Slice[P, D], windows: T, value: E[W])(
    implicit ev1: PosDimDep[P, D], ev2: PosExpDep[slice.S#M, Q], ev3: WindowableWithValue[T, slice.S, slice.R, Q, W],
    ev4: slice.R =!= Position0D, ev5: ClassTag[slice.S], ev6: ClassTag[slice.R]): U[Cell[Q]] = {
    val w = ev3.convert(windows)

    data
      .map { case Cell(p, c) => (Cell(slice.selected(p), c), slice.remainder(p)) }
      .groupBy { case (c, r) => c.position }
      .flatMap {
        case (_, itr) => itr
          .toList
          .sortBy { case (c, r) => r }
          .scanLeft(Option.empty[(w.T, Collection[Cell[Q]])]) {
            case (None, (c, r)) => Some(w.initialiseWithValue(c, r, value))
            case (Some((t, _)), (c, r)) => Some(w.presentWithValue(c, r, value, t))
          }
          .flatMap {
            case Some((t, c)) => c.toList
            case _ => List()
          }
      }
  }

  def split[Q, T](partitioners: T)(implicit ev: Partitionable[T, P, Q]): U[(Q, Cell[P])] = {
    val partitioner = ev.convert(partitioners)

    data.flatMap { case c => partitioner.assign(c).toList(c) }
  }

  def splitWithValue[Q, T, W](partitioners: T, value: E[W])(
    implicit ev: PartitionableWithValue[T, P, Q, W]): U[(Q, Cell[P])] = {
    val partitioner = ev.convert(partitioners)

    data.flatMap { case c => partitioner.assignWithValue(c, value).toList(c) }
  }

  def stream[Q <: Position](command: String, script: String, separator: String,
    parser: String => Option[Cell[Q]]): U[Cell[Q]] = {
    data
      .map(_.toString(separator, false))
      .pipe(command + " " + script)
      .flatMap(parser(_))
  }

  def summariseAndExpand[D <: Dimension, T](slice: Slice[P, D], aggregators: T)(implicit ev1: PosDimDep[P, D],
    ev2: AggregatableMultiple[T], ev3: ClassTag[slice.S]): U[Cell[slice.S#M]] = {
    val a = ev2.convert(aggregators)
    implicit val ct = a.ct

    data
      .map { case c => (slice.selected(c.position), a.prepare(slice, c)) }
      .reduceByKey { case (lt, rt) => a.reduce(lt, rt) }
      .flatMap { case (p, t) => a.presentMultiple(p, t).toList }
  }

  def summariseAndExpandWithValue[D <: Dimension, T, W](slice: Slice[P, D], aggregators: T, value: E[W])(
    implicit ev1: PosDimDep[P, D], ev2: AggregatableMultipleWithValue[T, W],
    ev3: ClassTag[slice.S]): U[Cell[slice.S#M]] = {
    val a = ev2.convert(aggregators)
    implicit val ct = a.ct

    data
      .map { case c => (slice.selected(c.position), a.prepareWithValue(slice, c, value)) }
      .reduceByKey { case (lt, rt) => a.reduce(lt, rt) }
      .flatMap { case (p, t) => a.presentMultipleWithValue(p, t, value).toList }
  }

  def toMap()(implicit ev: ClassTag[P]): E[Map[P, Content]] = {
    data
      .map { case c => (c.position, c.content) }
      .collectAsMap
      .toMap
  }

  def toMap[D <: Dimension](slice: Slice[P, D])(implicit ev1: PosDimDep[P, D], ev2: slice.S =!= Position0D,
    ev3: ClassTag[slice.S]): E[Map[slice.S, slice.C]] = {
    data
      .map { case c => (c.position, slice.toMap(c)) }
      .keyBy { case (p, m) => slice.selected(p) }
      .reduceByKey { case ((lp, lm), (rp, rm)) => (lp, slice.combineMaps(lp, lm, rm)) }
      .map { case (_, (_, m)) => m }
      .reduce { case (lm, rm) => lm ++ rm }
  }

  def transform[Q <: Position, T](transformers: T)(implicit ev1: Transformable[T, P, Q],
    ev2: PosExpDep[P, Q]): U[Cell[Q]] = {
    val transformer = ev1.convert(transformers)

    data.flatMap { case c => transformer.present(c).toList }
  }

  def transformWithValue[Q <: Position, T, W](transformers: T, value: E[W])(
    implicit ev1: TransformableWithValue[T, P, Q, W], ev2: PosExpDep[P, Q]): U[Cell[Q]] = {
    val transformer = ev1.convert(transformers)

    data.flatMap { case c => transformer.presentWithValue(c, value).toList }
  }

  def types[D <: Dimension](slice: Slice[P, D], specific: Boolean = false)(implicit ev1: PosDimDep[P, D],
    ev2: slice.S =!= Position0D, ev3: ClassTag[slice.S]): U[(slice.S, Type)] = {
    data
      .map { case Cell(p, c) => (slice.selected(p), c.schema.kind) }
      .reduceByKey { case (lt, rt) => Type.getCommonType(lt, rt) }
      .map { case (p, t) => (p, if (specific) t else t.getGeneralisation()) }
  }

  def unique(): U[Content] = {
    data
      .map { case c => c.content }
      .distinct()
  }

  def unique[D <: Dimension](slice: Slice[P, D])(implicit ev: slice.S =!= Position0D): U[Cell[slice.S]] = {
    data
      .map { case Cell(p, c) => Cell(slice.selected(p), c) }
      .distinct()
  }

  def which(predicate: Predicate)(implicit ev: ClassTag[P]): U[P] = {
    data.collect { case c if predicate(c) => c.position }
  }

  def which[D <: Dimension, T](slice: Slice[P, D], positions: T, predicate: Predicate)(implicit ev1: PosDimDep[P, D],
    ev2: BaseNameable[T, P, slice.S, D, RDD], ev3: ClassTag[slice.S], ev4: ClassTag[P]): U[P] = {
    which(slice, List((positions, predicate)))
  }

  def which[D <: Dimension, T](slice: Slice[P, D], pospred: List[(T, Predicate)])(implicit ev1: PosDimDep[P, D],
    ev2: BaseNameable[T, P, slice.S, D, RDD], ev3: ClassTag[slice.S], ev4: ClassTag[P]): U[P] = {
    val nampred = pospred.map { case (pos, pred) => ev2.convert(this, slice, pos).map { case (p, i) => (p, pred) } }
    val pipe = nampred.tail.foldLeft(nampred.head)((b, a) => b ++ a)

    data
      .keyBy { case c => slice.selected(c.position) }
      .join(pipe.keyBy { case (p, pred) => p })
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

  private def pairwiseTuples[D <: Dimension](slice: Slice[P, D], comparer: Comparer)(lnames: U[(slice.S, Long)],
    ldata: U[Cell[P]], rnames: U[(slice.S, Long)], rdata: U[Cell[P]])(implicit ev1: PosDimDep[P, D],
    ev2: ClassTag[slice.S], ev3: ClassTag[slice.R]): U[(Cell[slice.S], Cell[slice.S], slice.R)] = {
    lnames
      .cartesian(rnames)
      .collect { case ((l, _), (r, _)) if comparer.keep(l, r) => (l, r) }
      .keyBy { case (l, _) => l }
      .join(ldata.keyBy { case Cell(p, _) => slice.selected(p) })
      .keyBy { case (_, ((_, r), Cell(p, _))) => (r, slice.remainder(p)) }
      .join(rdata.keyBy { case Cell(p, _) => (slice.selected(p), slice.remainder(p)) })
      .map { case ((_, o), ((_, ((lp, rp), lc)), rc)) => (Cell(lp, lc.content), Cell(rp, rc.content), o) }
  }
}

/** Base trait for methods that reduce the number of dimensions or that can be filled using a `RDD[Cell[P]]`. */
trait ReduceableMatrix[P <: Position with ReduceablePosition] extends BaseReduceableMatrix[P] { self: Matrix[P] =>

  def fill[D <: Dimension, Q <: Position](slice: Slice[P, D], values: U[Cell[Q]])(implicit ev1: PosDimDep[P, D],
    ev2: ClassTag[P], ev3: ClassTag[slice.S], ev4: slice.S =:= Q): U[Cell[P]] = {
    val dense = domain
      .keyBy { case p => slice.selected(p) }
      .join(values.keyBy { case c => c.position.asInstanceOf[slice.S] })
      .map { case (_, (p, c)) => Cell(p, c.content) }

    dense
      .keyBy { case c => c.position }
      .leftOuterJoin(data.keyBy { case c => c.position })
      .map { case (p, (fc, co)) => co.getOrElse(fc) }
  }

  def fill(value: Content)(implicit ev: ClassTag[P]): U[Cell[P]] = {
    domain
      .keyBy { case p => p }
      .leftOuterJoin(data.keyBy { case c => c.position })
      .map { case (p, (_, co)) => co.getOrElse(Cell(p, value)) }
  }

  def melt[D <: Dimension, F <: Dimension](dim: D, into: F, separator: String = ".")(implicit ev1: PosDimDep[P, D],
    ev2: PosDimDep[P, F], ne: D =!= F): U[Cell[P#L]] = {
    data.map { case Cell(p, c) => Cell(p.melt(dim, into, separator), c) }
  }

  def summarise[D <: Dimension](slice: Slice[P, D], aggregator: Aggregator with Prepare with PresentSingle)(
    implicit ev1: PosDimDep[P, D], ev2: ClassTag[slice.S]): U[Cell[slice.S]] = {
    implicit val ct = aggregator.ct

    data
      .map { case c => (slice.selected(c.position), aggregator.prepare(slice, c)) }
      .reduceByKey { case (lt, rt) => aggregator.reduce(lt, rt) }
      .flatMap { case (p, t) => aggregator.presentSingle(p, t) }
  }

  def summariseWithValue[D <: Dimension, W](slice: Slice[P, D],
    aggregator: Aggregator with PrepareWithValue with PresentSingleWithValue { type V >: W }, value: E[W])(
      implicit ev1: PosDimDep[P, D], ev2: ClassTag[slice.S]): U[Cell[slice.S]] = {
    implicit val ct = aggregator.ct

    data
      .map { case c => (slice.selected(c.position), aggregator.prepareWithValue(slice, c, value)) }
      .reduceByKey { case (lt, rt) => aggregator.reduce(lt, rt) }
      .flatMap { case (p, t) => aggregator.presentSingleWithValue(p, t, value) }
  }

  def squash[D <: Dimension, T](dim: D, squasher: T)(implicit ev1: PosDimDep[P, D],
    ev2: Squashable[T, P]): U[Cell[P#L]] = {
    val squash = ev2.convert(squasher)

    data
      .keyBy { case c => c.position.remove(dim) }
      .reduceByKey { case (x, y) => squash.reduce(dim, x, y) }
      .map { case (p, c) => Cell(p, c.content) }
  }

  def squashWithValue[D <: Dimension, T, W](dim: D, squasher: T, value: E[W])(implicit ev1: PosDimDep[P, D],
    ev2: SquashableWithValue[T, P, W]): U[Cell[P#L]] = {
    val squash = ev2.convert(squasher)

    data
      .keyBy { case c => c.position.remove(dim) }
      .reduceByKey { case (x, y) => squash.reduceWithValue(dim, x, y, value) }
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
      .pairwise[Dimension.First, Position1D, Times[Position1D, Position0D]](Over(First), Lower, Times())
      .transform[Position1D, SquareRoot[Position1D]](SquareRoot())
      .toMap(Over(First))

    centered
      .pairwise[D, slice.R#M, Times[slice.S, slice.R]](slice, Lower, Times())
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
      .pairwise[Dimension.First, Position2D, Plus[Position1D, Position1D]](Over(First), Upper, Plus(name = "%s,%s"))

    val joint = data
      .pairwise[D, slice.R#M, Concatenate[slice.S, slice.R]](slice, Upper, Concatenate(name = "%s,%s"))
      .summariseAndExpand(Over(First), Entropy("joint", strict = true, nan = true, all = false, negate = true))

    (marginal ++ joint)
      .summarise(Over(First), Sum())
  }

  def gini[D <: Dimension](slice: Slice[Position2D, D])(implicit ev1: PosDimDep[Position2D, D],
    ev2: ClassTag[slice.S], ev3: ClassTag[slice.R]): U[Cell[Position1D]] = {
    implicit def UP2DSC2M1D(data: U[Cell[slice.S]]): Matrix1D = new Matrix1D(data.asInstanceOf[U[Cell[Position1D]]])
    implicit def UP2DSMC2M2D(data: U[Cell[slice.S#M]]): Matrix2D = new Matrix2D(data.asInstanceOf[U[Cell[Position2D]]])

    def isPositive(d: Double) = d > 0

    type V = Map[Position1D, Content]

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
      .slide[D, slice.S#M, CumulativeSum[slice.S, slice.R]](slice, CumulativeSum())
      .transformWithValue[Position2D, Fraction[Position2D, V], V](Fraction(extractor), pos)
      .slide[Dimension.First, Position2D, Sliding[Position1D, Position1D]](Over(First),
        Sliding((l: Double, r: Double) => r + l, name = "%2$s.%1$s"))

    val fpr = data
      .transform[Position2D, Compare[Position2D]](Compare(!isPositive(_)))
      .slide[D, slice.S#M, CumulativeSum[slice.S, slice.R]](slice, CumulativeSum())
      .transformWithValue[Position2D, Fraction[Position2D, V], V](Fraction(extractor), neg)
      .slide[Dimension.First, Position2D, Sliding[Position1D, Position1D]](Over(First),
        Sliding((l: Double, r: Double) => r - l, name = "%2$s.%1$s"))

    tpr
      .pairwiseBetween[Dimension.First, Position2D, Times[Position1D, Position1D]](Along(First), Diagonal, fpr, Times())
      .summarise(Along(First), Sum())
      .transformWithValue[Position1D, Subtract[Position1D, Map[Position1D, Double]], Map[Position1D, Double]](
        Subtract(ExtractWithKey("one"), true), Map(Position1D("one") -> 1.0))
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
  def load1D(file: String, separator: String = "|", first: Codex = StringCodex)(
    implicit sc: SparkContext): RDD[Cell[Position1D]] = {
    sc.textFile(file).flatMap { Cell.parse1D(separator, first)(_) }
  }

  /**
   * Read column oriented, pipe separated data into a `RDD[Cell[Position1D]]`.
   *
   * @param file      The file to read from.
   * @param dict      The dictionary describing the features in the data.
   * @param separator The column separator.
   * @param first     The codex for decoding the first dimension.
   */
  def load1DWithDictionary(file: String, dict: Map[String, Schema], separator: String = "|",
    first: Codex = StringCodex)(implicit sc: SparkContext): RDD[Cell[Position1D]] = {
    sc.textFile(file).flatMap { Cell.parse1DWithDictionary(dict, separator, first)(_) }
  }

  /**
   * Read column oriented, pipe separated data into a `RDD[Cell[Position1D]]`.
   *
   * @param file      The file to read from.
   * @param schema    The schema for decoding the data.
   * @param separator The column separator.
   * @param first     The codex for decoding the first dimension.
   */
  def load1DWithSchema(file: String, schema: Schema, separator: String = "|", first: Codex = StringCodex)(
    implicit sc: SparkContext): RDD[Cell[Position1D]] = {
    sc.textFile(file).flatMap { Cell.parse1DWithSchema(schema, separator, first)(_) }
  }

  /**
   * Read column oriented, pipe separated matrix data into a `RDD[Cell[Position2D]]`.
   *
   * @param file      The file to read from.
   * @param separator The column separator.
   * @param first     The codex for decoding the first dimension.
   * @param second    The codex for decoding the second dimension.
   */
  def load2D(file: String, separator: String = "|", first: Codex = StringCodex, second: Codex = StringCodex)(
    implicit sc: SparkContext): RDD[Cell[Position2D]] = {
    sc.textFile(file).flatMap { Cell.parse2D(separator, first, second)(_) }
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
  def load2DWithDictionary[D <: Dimension](file: String, dict: Map[String, Schema], dim: D = Second,
    separator: String = "|", first: Codex = StringCodex, second: Codex = StringCodex)(
      implicit ev: PosDimDep[Position2D, D], sc: SparkContext): RDD[Cell[Position2D]] = {
    sc.textFile(file).flatMap { Cell.parse2DWithDictionary(dict, dim, separator, first, second)(_) }
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
  def load2DWithSchema(file: String, schema: Schema, separator: String = "|", first: Codex = StringCodex,
    second: Codex = StringCodex)(implicit sc: SparkContext): RDD[Cell[Position2D]] = {
    sc.textFile(file).flatMap { Cell.parse2DWithSchema(schema, separator, first, second)(_) }
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
  def load3D(file: String, separator: String = "|", first: Codex = StringCodex, second: Codex = StringCodex,
    third: Codex = StringCodex)(implicit sc: SparkContext): RDD[Cell[Position3D]] = {
    sc.textFile(file).flatMap { Cell.parse3D(separator, first, second, third)(_) }
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
  def load3DWithDictionary[D <: Dimension](file: String, dict: Map[String, Schema], dim: D = Second,
    separator: String = "|", first: Codex = StringCodex, second: Codex = StringCodex, third: Codex = DateCodex)(
      implicit ev: PosDimDep[Position3D, D], sc: SparkContext): RDD[Cell[Position3D]] = {
    sc.textFile(file).flatMap { Cell.parse3DWithDictionary(dict, dim, separator, first, second, third)(_) }
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
  def load3DWithSchema(file: String, schema: Schema, separator: String = "|", first: Codex = StringCodex,
    second: Codex = StringCodex, third: Codex = DateCodex)(implicit sc: SparkContext): RDD[Cell[Position3D]] = {
    sc.textFile(file).flatMap { Cell.parse3DWithSchema(schema, separator, first, second, third)(_) }
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
  def loadTable(table: String, columns: List[(String, Schema)], pkeyIndex: Int = 0, separator: String = "\u0001")(
    implicit sc: SparkContext): RDD[Cell[Position2D]] = {
    sc.textFile(table).flatMap { Cell.parseTable(columns, pkeyIndex, separator)(_) }
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
  def domain(): U[Position1D] = names(Over(First)).map { case (p, i) => p }

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
  def saveAsIVWithNames(file: String, names: U[(Position1D, Long)], dictionary: String = "%1$s.dict.%2$d",
    separator: String = "|"): U[Cell[Position1D]] = {
    data
      .keyBy { case c => c.position }
      .join(saveDictionary(names, file, dictionary, separator, First))
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
  def domain(): U[Position2D] = {
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
    rowId: String = "id")(implicit ev1: BaseNameable[RDD[(slice.S, Long)], Position2D, slice.S, D, RDD],
      ev2: PosDimDep[Position2D, D], ev3: ClassTag[slice.S]): U[Cell[Position2D]] = {
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
  def saveAsCSVWithNames[T, D <: Dimension](slice: Slice[Position2D, D], file: String, names: T,
    separator: String = "|", escapee: Escape = Quote(), writeHeader: Boolean = true, header: String = "%s.header",
    writeRowId: Boolean = true, rowId: String = "id")(implicit ev1: BaseNameable[T, Position2D, slice.S, D, RDD],
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
   *
   * @note R's slam package has a simple triplet matrix format (which in turn is used by the tm package). This format
   *       should be compatible.
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
   * @note R's slam package has a simple triplet matrix format (which in turn is used by the tm package). This format
   *       should be compatible.
   */
  def saveAsIVWithNames(file: String, namesI: U[(Position1D, Long)], namesJ: U[(Position1D, Long)],
    dictionary: String = "%1$s.dict.%2$d", separator: String = "|"): U[Cell[Position2D]] = {
    data
      .keyBy { case c => Position1D(c.position(First)) }
      .join(saveDictionary(namesI, file, dictionary, separator, First))
      .values
      .keyBy { case (c, i) => Position1D(c.position(Second)) }
      .join(saveDictionary(namesJ, file, dictionary, separator, Second))
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
  def saveAsLDAWithNames[D <: Dimension](slice: Slice[Position2D, D], file: String, names: U[(Position1D, Long)],
    dictionary: String = "%s.dict", separator: String = "|", addId: Boolean = false)(
      implicit ev1: PosDimDep[Position2D, D], ev2: ClassTag[slice.S]): U[Cell[Position2D]] = {
    data
      .keyBy { case c => slice.remainder(c.position).asInstanceOf[Position1D] }
      .join(saveDictionary(names, file, dictionary, separator))
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
  def saveAsVWWithNames[D <: Dimension](slice: Slice[Position2D, D], labels: U[Cell[Position1D]], file: String,
    names: U[(Position1D, Long)], dictionary: String = "%s.dict", separator: String = ":")(
      implicit ev: PosDimDep[Position2D, D]): U[Cell[Position2D]] = {
    data
      .keyBy { case c => slice.remainder(c.position).asInstanceOf[Position1D] }
      .join(saveDictionary(names, file, dictionary, separator))
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
  def domain(): U[Position3D] = {
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
  def saveAsIVWithNames(file: String, namesI: U[(Position1D, Long)], namesJ: U[(Position1D, Long)],
    namesK: U[(Position1D, Long)], dictionary: String = "%1$s.dict.%2$d",
      separator: String = "|"): U[Cell[Position3D]] = {
    data
      .keyBy { case c => Position1D(c.position(First)) }
      .join(saveDictionary(namesI, file, dictionary, separator, First))
      .values
      .keyBy { case (c, i) => Position1D(c.position(Second)) }
      .join(saveDictionary(namesJ, file, dictionary, separator, Second))
      .map { case (_, ((c, i), j)) => (c, i, j) }
      .keyBy { case (c, i, j) => Position1D(c.position(Third)) }
      .join(saveDictionary(namesK, file, dictionary, separator, Third))
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
  def domain(): U[Position4D] = {
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
  def saveAsIVWithNames(file: String, namesI: U[(Position1D, Long)], namesJ: U[(Position1D, Long)],
    namesK: U[(Position1D, Long)], namesL: U[(Position1D, Long)], dictionary: String = "%1$s.dict.%2$d",
    separator: String = "|"): U[Cell[Position4D]] = {
    data
      .keyBy { case c => Position1D(c.position(First)) }
      .join(saveDictionary(namesI, file, dictionary, separator, First))
      .values
      .keyBy { case (c, i) => Position1D(c.position(Second)) }
      .join(saveDictionary(namesJ, file, dictionary, separator, Second))
      .map { case (_, ((c, i), j)) => (c, i, j) }
      .keyBy { case (c, i, j) => Position1D(c.position(Third)) }
      .join(saveDictionary(namesK, file, dictionary, separator, Third))
      .map { case (_, ((c, i, j), k)) => (c, i, j, k) }
      .keyBy { case (c, i, j, p) => Position1D(c.position(Fourth)) }
      .join(saveDictionary(namesL, file, dictionary, separator, Fourth))
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
  def domain(): U[Position5D] = {
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
  def saveAsIVWithNames(file: String, namesI: U[(Position1D, Long)], namesJ: U[(Position1D, Long)],
    namesK: U[(Position1D, Long)], namesL: U[(Position1D, Long)], namesM: U[(Position1D, Long)],
      dictionary: String = "%1$s.dict.%2$d", separator: String = "|"): U[Cell[Position5D]] = {
    data
      .keyBy { case c => Position1D(c.position(First)) }
      .join(saveDictionary(namesI, file, dictionary, separator, First))
      .values
      .keyBy { case (c, i) => Position1D(c.position(Second)) }
      .join(saveDictionary(namesJ, file, dictionary, separator, Second))
      .map { case (_, ((c, i), j)) => (c, i, j) }
      .keyBy { case (c, pi, pj) => Position1D(c.position(Third)) }
      .join(saveDictionary(namesK, file, dictionary, separator, Third))
      .map { case (_, ((c, i, j), k)) => (c, i, j, k) }
      .keyBy { case (c, i, j, k) => Position1D(c.position(Fourth)) }
      .join(saveDictionary(namesL, file, dictionary, separator, Fourth))
      .map { case (_, ((c, i, j, k), l)) => (c, i, j, k, l) }
      .keyBy { case (c, i, j, k, l) => Position1D(c.position(Fifth)) }
      .join(saveDictionary(namesM, file, dictionary, separator, Fifth))
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

