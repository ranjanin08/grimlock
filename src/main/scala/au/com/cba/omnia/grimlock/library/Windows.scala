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

package au.com.cba.omnia.grimlock.library.window

import au.com.cba.omnia.grimlock.framework._
import au.com.cba.omnia.grimlock.framework.content._
import au.com.cba.omnia.grimlock.framework.content.metadata._
import au.com.cba.omnia.grimlock.framework.position._
import au.com.cba.omnia.grimlock.framework.window._

/** Base trait for computing a moving average. */
trait MovingAverage[
  P <: Position[P],
  S <: Position[S] with ExpandablePosition[S, _],
  R <: Position[R] with ExpandablePosition[R, _],
  Q <: Position[Q]
] extends Window[P, S, R, Q] {
  type I = Double
  type O = (R, Double)

  /** Function to extract result position. */
  val position: Locate.FromSelectedAndRemainder[S, R, Q]

  def prepare(cell: Cell[P]): I = cell.content.value.asDouble.getOrElse(Double.NaN)

  def present(pos: S, out: O): TraversableOnce[Cell[Q]] = {
    position(pos, out._1).map(Cell(_, Content(ContinuousSchema[Double](), out._2)))
  }
}

/**
 * Trait for computing moving average in batch mode; that is, keep the last N values and compute the moving average
 * from it.
 */
trait BatchMovingAverage[
  P <: Position[P],
  S <: Position[S] with ExpandablePosition[S, _],
  R <: Position[R] with ExpandablePosition[R, _],
  Q <: Position[Q]
] extends MovingAverage[P, S, R, Q] {
  type T = List[(R, Double)]

  /** Size of the window. */
  val window: Int

  /** Indicates if averages should be output when a full window isn't available yet. */
  val all: Boolean

  protected val idx: Int

  def initialise(rem: R, in: I): (T, TraversableOnce[O]) = {
    (List((rem, in)), if (all) { Some((rem, in)) } else { None })
  }

  def update(rem: R, in: I, t: T): (T, TraversableOnce[O]) = {
    val lst = (if (t.size == window) { t.tail } else { t }) :+ ((rem, in))
    val out = (all || lst.size == window) match {
      case true => Some((lst(math.min(idx, lst.size - 1))._1, compute(lst)))
      case false => None
    }

    (lst, out)
  }

  protected def compute(lst: T): Double
}

/** Compute simple moving average over last `window` values. */
case class SimpleMovingAverage[
  P <: Position[P],
  S <: Position[S] with ExpandablePosition[S, _],
  R <: Position[R] with ExpandablePosition[R, _],
  Q <: Position[Q]
](
  window: Int,
  position: Locate.FromSelectedAndRemainder[S, R, Q],
  all: Boolean = false
) extends BatchMovingAverage[P, S, R, Q] {
  protected val idx = window - 1

  protected def compute(lst: T): Double = lst.foldLeft(0.0)((c, p) => p._2 + c) / lst.size
}

/** Compute centered moving average over last `2 * width + 1` values. */
case class CenteredMovingAverage[
  P <: Position[P],
  S <: Position[S] with ExpandablePosition[S, _],
  R <: Position[R] with ExpandablePosition[R, _],
  Q <: Position[Q]
](
  width: Int,
  position: Locate.FromSelectedAndRemainder[S, R, Q]
) extends BatchMovingAverage[P, S, R, Q] {
  val window = 2 * width + 1
  val all = false
  protected val idx = width

  protected def compute(lst: T): Double = lst.foldLeft(0.0)((c, p) => p._2 + c) / lst.size
}

/** Compute weighted moving average over last `window` values. */
case class WeightedMovingAverage[
  P <: Position[P],
  S <: Position[S] with ExpandablePosition[S, _],
  R <: Position[R] with ExpandablePosition[R, _],
  Q <: Position[Q]
](
  window: Int,
  position: Locate.FromSelectedAndRemainder[S, R, Q],
  all: Boolean = false
) extends BatchMovingAverage[P, S, R, Q] {
  protected val idx = window - 1

  protected def compute(lst: T): Double = {
    val curr = lst.zipWithIndex.foldLeft((0.0, 0))((c, p) => ((p._2 + 1) * p._1._2 + c._1, c._2 + p._2 + 1))

    curr._1 / curr._2
  }
}

/** Trait for computing moving average in online mode. */
trait OnlineMovingAverage[
  P <: Position[P],
  S <: Position[S] with ExpandablePosition[S, _],
  R <: Position[R] with ExpandablePosition[R, _],
  Q <: Position[Q]
] extends MovingAverage[P, S, R, Q] {
  type T = (Double, Long)

  def initialise(rem: R, in: I): (T, TraversableOnce[O]) = ((in, 1), Some((rem, in)))

  def update(rem: R, in: I, t: T): (T, TraversableOnce[O]) = {
    val curr = compute(in, t)

    ((curr, t._2 + 1), Some((rem, curr)))
  }

  protected def compute(curr: Double, t: T): Double
}

/** Compute cumulatve moving average. */
case class CumulativeMovingAverage[
  P <: Position[P],
  S <: Position[S] with ExpandablePosition[S, _],
  R <: Position[R] with ExpandablePosition[R, _],
  Q <: Position[Q]
](
  position: Locate.FromSelectedAndRemainder[S, R, Q]
) extends OnlineMovingAverage[P, S, R, Q] {
  protected def compute(curr: Double, t: T): Double = (curr + t._2 * t._1) / (t._2 + 1)
}

/** Compute exponential moving average. */
case class ExponentialMovingAverage[
  P <: Position[P],
  S <: Position[S] with ExpandablePosition[S, _],
  R <: Position[R] with ExpandablePosition[R, _],
  Q <: Position[Q]
](
  alpha: Double,
  position: Locate.FromSelectedAndRemainder[S, R, Q]
) extends OnlineMovingAverage[P, S, R, Q] {
  protected def compute(curr: Double, t: T): Double = alpha * curr + (1 - alpha) * t._1
}

/**
 * Compute cumulative sum.
 *
 * @param position Function to extract result position.
 * @param strict   Indicates is non-numeric values should result in NaN.
 */
case class CumulativeSum[
  P <: Position[P],
  S <: Position[S] with ExpandablePosition[S, _],
  R <: Position[R] with ExpandablePosition[R, _],
  Q <: Position[Q]
](
  position: Locate.FromSelectedAndRemainder[S, R, Q],
  strict: Boolean = true
) extends Window[P, S, R, Q] {
  type I = Option[Double]
  type T = Option[Double]
  type O = (R, Double)

  val schema = ContinuousSchema[Double]()

  def prepare(cell: Cell[P]): I = {
    (strict, cell.content.value.asDouble) match {
      case (true, None) => Some(Double.NaN)
      case (_, v) => v
    }
  }

  def initialise(rem: R, in: I): (T, TraversableOnce[O]) = (in, in.map { case d => (rem, d) })

  def update(rem: R, in: I, t: T): (T, TraversableOnce[O]) = {
    (strict, t, in) match {
      case (true, _, None) => (Some(Double.NaN), Some((rem, Double.NaN)))
      case (false, p, None) => (p, None)
      case (_, None, Some(d)) => (Some(d), Some((rem, d)))
      case (_, Some(p), Some(d)) => (Some(p + d), Some((rem, p + d)))
    }
  }

  def present(pos: S, out: O): TraversableOnce[Cell[Q]] = position(pos, out._1).map(Cell(_, Content(schema, out._2)))
}

/**
 * Compute sliding binary operator on sequential numeric cells.
 *
 * @param binop    The binary operator to apply to two sequential numeric cells.
 * @param position Function to extract result position.
 * @param strict   Indicates is non-numeric values should result in NaN.
 */
case class BinOp[
  P <: Position[P],
  S <: Position[S] with ExpandablePosition[S, _],
  R <: Position[R] with ExpandablePosition[R, _],
  Q <: Position[Q]
](
  binop: (Double, Double) => Double,
  position: Locate.FromSelectedAndPairwiseRemainder[S, R, Q],
  strict: Boolean = true
) extends Window[P, S, R, Q] {
  type I = Option[Double]
  type T = (Option[Double], R)
  type O = (Double, R, R)

  def prepare(cell: Cell[P]): I = {
    (strict, cell.content.value.asDouble) match {
      case (true, None) => Some(Double.NaN)
      case (_, v) => v
    }
  }

  def initialise(rem: R, in: I): (T, TraversableOnce[O]) = ((in, rem), None)

  def update(rem: R, in: I, t: T): (T, TraversableOnce[O]) = {
    (strict, t, in) match {
      case (true, (_, c), None) => getResult(rem, Double.NaN, Double.NaN, c)
      case (false, p, None) => (p, None)
      case (_, (None, _), Some(d)) => ((Some(d), rem), None)
      case (_, (Some(p), c), Some(d)) => getResult(rem, if (p.isNaN) p else d, binop(p, d), c)
    }
  }

  def present(pos: S, out: O): TraversableOnce[Cell[Q]] = {
    position(pos, out._3, out._2).map(Cell(_, Content(ContinuousSchema[Double](), out._1)))
  }

  private def getResult(rem: R, value: Double, result: Double, prev: R): (T, TraversableOnce[O]) = {
    ((Some(value), rem), Some((result, prev, rem)))
  }
}

