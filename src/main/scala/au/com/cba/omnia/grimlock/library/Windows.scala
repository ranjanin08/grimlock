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

package au.com.cba.omnia.grimlock.library.window

import au.com.cba.omnia.grimlock.framework._
import au.com.cba.omnia.grimlock.framework.content._
import au.com.cba.omnia.grimlock.framework.content.metadata._
import au.com.cba.omnia.grimlock.framework.encoding._
import au.com.cba.omnia.grimlock.framework.position._
import au.com.cba.omnia.grimlock.framework.window._

/** Base trait for computing a moving average. */
trait MovingAverage[P <: Position, S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position] extends Window[P, S, R, Q] {
  type I = Double
  type O = (R, Double)

  /** Function to extract result position. */
  val position: Locate.WindowSize1[S, R, Q]

  def prepare(cell: Cell[P]): I = cell.content.value.asDouble.getOrElse(Double.NaN)

  def present(pos: S, out: O): TraversableOnce[Cell[Q]] = {
    Some(Cell(position(pos, out._1), Content(ContinuousSchema(DoubleCodex), out._2)))
  }
}

/**
 * Trait for computing moving average in batch mode; that is, keep the last N values and compute the moving average
 * from it.
 */
trait BatchMovingAverage[P <: Position, S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position] extends MovingAverage[P, S, R, Q] {
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
case class SimpleMovingAverage[P <: Position, S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position](
  window: Int, position: Locate.WindowSize1[S, R, Q], all: Boolean = false) extends BatchMovingAverage[P, S, R, Q] {
  protected val idx = window - 1

  protected def compute(lst: T): Double = lst.foldLeft(0.0)((c, p) => p._2 + c) / lst.size
}

/** Compute centered moving average over last `2 * width + 1` values. */
case class CenteredMovingAverage[P <: Position, S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position](
  width: Int, position: Locate.WindowSize1[S, R, Q]) extends BatchMovingAverage[P, S, R, Q] {
  val window = 2 * width + 1
  val all = false
  protected val idx = width

  protected def compute(lst: T): Double = lst.foldLeft(0.0)((c, p) => p._2 + c) / lst.size
}

/** Compute weighted moving average over last `window` values. */
case class WeightedMovingAverage[P <: Position, S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position](
  window: Int, position: Locate.WindowSize1[S, R, Q], all: Boolean = false) extends BatchMovingAverage[P, S, R, Q] {
  protected val idx = window - 1

  protected def compute(lst: T): Double = {
    val curr = lst.zipWithIndex.foldLeft((0.0, 0))((c, p) => ((p._2 + 1) * p._1._2 + c._1, c._2 + p._2 + 1))

    curr._1 / curr._2
  }
}

/** Trait for computing moving average in online mode. */
trait OnlineMovingAverage[P <: Position, S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position] extends MovingAverage[P, S, R, Q] {
  type T = (Double, Long)

  def initialise(rem: R, in: I): (T, TraversableOnce[O]) = ((in, 1), Some((rem, in)))

  def update(rem: R, in: I, t: T): (T, TraversableOnce[O]) = {
    val curr = compute(in, t)

    ((curr, t._2 + 1), Some((rem, curr)))
  }

  protected def compute(curr: Double, t: T): Double
}

/** Compute cumulatve moving average. */
case class CumulativeMovingAverage[P <: Position, S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position](
  position: Locate.WindowSize1[S, R, Q]) extends OnlineMovingAverage[P, S, R, Q] {
  protected def compute(curr: Double, t: T): Double = (curr + t._2 * t._1) / (t._2 + 1)
}

/** Compute exponential moving average. */
case class ExponentialMovingAverage[P <: Position, S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position](
  alpha: Double, position: Locate.WindowSize1[S, R, Q]) extends OnlineMovingAverage[P, S, R, Q] {
  protected def compute(curr: Double, t: T): Double = alpha * curr + (1 - alpha) * t._1
}

/**
 * Compute cumulative sum.
 *
 * @param position Function to extract result position.
 * @param strict   Indicates is non-numeric values should result in NaN.
 */
case class CumulativeSum[P <: Position, S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position](
  position: Locate.WindowSize1[S, R, Q], strict: Boolean = true) extends Window[P, S, R, Q] {
  type I = Option[Double]
  type T = Option[Double]
  type O = (R, Double)

  val schema = ContinuousSchema(DoubleCodex)

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

  def present(pos: S, out: O): TraversableOnce[Cell[Q]] = Some(Cell(position(pos, out._1), Content(schema, out._2)))
}

/**
 * Compute sliding binary operator on sequential numeric cells.
 *
 * @param binop    The binary operator to apply to two sequential numeric cells.
 * @param position Function to extract result position.
 * @param strict   Indicates is non-numeric values should result in NaN.
 */
case class BinOp[P <: Position, S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position](
  binop: (Double, Double) => Double, position: Locate.WindowSize2[S, R, Q],
    strict: Boolean = true) extends Window[P, S, R, Q] {
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
    Some(Cell(position(pos, out._3, out._2), Content(ContinuousSchema(DoubleCodex), out._1)))
  }

  private def getResult(rem: R, value: Double, result: Double, prev: R): (T, TraversableOnce[O]) = {
    ((Some(value), rem), Some((result, prev, rem)))
  }
}

/**
 * Compute sample quantiles.
 *
 * @param probs     List of probabilities; values must lie in (0, 1).
 * @param count     Function that extracts the count value statistics from the user provided value.
 * @param quantiser Function that determines the quantile indices into the order statistics.
 * @param name      Pattern for the name. Use `%1$``s` for the probability correpsponding to the quantile.
 *
 * @note Non-numeric result in `NaN` quantiles.
 */
case class Quantile[P <: Position, S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, W](
  probs: List[Double], count: Extract[P, W, Long], quantiser: Quantile.Quantiser,
    name: Locate.FromSelectedAndOutput[S, Double, S#M]) extends WindowWithValue[P, S, R, S#M] {
  type V = W
  type I = (Double, Option[Long])
  type T = (Double, Long, List[(Long, Double, Double)])
  type O = (Double, Double)

  def prepareWithValue(cell: Cell[P], ext: V): I = {
    (cell.content.value.asDouble.getOrElse(Double.NaN), count.extract(cell, ext))
  }

  def initialise(rem: R, in: I): (T, TraversableOnce[O]) = {
    val count = 0
    val state = in._2
      .map {
        case n => probs
          .map { case p => (quantiser(p, n), p) }
          .map { case ((j, g), p) => (j, g, p) }
      }
      .getOrElse(List())
    val curr = in._1
    val out = None

    ((curr, count, state), out)
  }

  def update(rem: R, in: I, t: T): (T, TraversableOnce[O]) = {
    val prev = t._1
    val count = t._2 + 1
    val state = t._3
    val curr = if (state.isEmpty || prev.isNaN) { Double.NaN } else { in._1 }
    val out = state.find(_._1 == t._2) match {
      case Some((_, g, p)) => Some((p, (1 - g) * prev + g * curr))
      case None => None
    }

    ((curr, count, state), out)
  }

  def presentWithValue(pos: S, out: O, ext: V): TraversableOnce[Cell[S#M]] = {
    Some(Cell[S#M](name(pos, out._1), Content(ContinuousSchema(DoubleCodex), out._2)))
  }
}

/** Companion object to Quantile class. */
object Quantile {
  /** Type of quantiser function. */
  type Quantiser = (Double, Long) => (Long, Double)

  /**
   * Compute quantile indices and gamma coefficient according to R's Type 1 rule; inverse of empirical
   * distribution function.
   *
   * @see https://stat.ethz.ch/R-manual/R-devel/library/stats/html/quantile.html
   */
  val Type1: Quantiser = (p: Double, n: Long) => {
    val (j, g) = TypeX(p, n, 0)

    (j, if (g == 0) 0 else 1)
  }

  /**
   * Compute quantile indices and gamma coefficient according to R's Type 2 rule; similar to type 1 but
   * with averaging at discontinuities.
   *
   * @see https://stat.ethz.ch/R-manual/R-devel/library/stats/html/quantile.html
   */
  val Type2: Quantiser = (p: Double, n: Long) => {
    val (j, g) = TypeX(p, n, 0)

    (j, if (g == 0) 0.5 else 1)
  }

  /**
   * Compute quantile indices and gamma coefficient according to R's Type 3 rule; nearest even order statistic.
   *
   * @see https://stat.ethz.ch/R-manual/R-devel/library/stats/html/quantile.html
   */
  val Type3: Quantiser = (p: Double, n: Long) => {
    val (j, g) = TypeX(p, n, -0.5)

    (j, if (g == 0 && j % 2 == 0) 0 else 1)
  }

  /**
   * Compute quantile indices and gamma coefficient according to R's Type 4 rule; linear interpolation of
   * the empirical cdf.
   *
   * @see https://stat.ethz.ch/R-manual/R-devel/library/stats/html/quantile.html
   */
  val Type4: Quantiser = (p: Double, n: Long) => { TypeX(p, n, 0) }

  /**
   * Compute quantile indices and gamma coefficient according to R's Type 5 rule; a piecewise linear function
   * where the knots are the values midway through the steps of the empirical cdf.
   *
   * @see https://stat.ethz.ch/R-manual/R-devel/library/stats/html/quantile.html
   */
  val Type5: Quantiser = (p: Double, n: Long) => { TypeX(p, n, 0.5) }

  /**
   * Compute quantile indices and gamma coefficient according to R's Type 6 rule; p[k] = E[F(x[k])].
   *
   * @see https://stat.ethz.ch/R-manual/R-devel/library/stats/html/quantile.html
   */
  val Type6: Quantiser = (p: Double, n: Long) => { TypeX(p, n, p) }

  /**
   * Compute quantile indices and gamma coefficient according to R's Type 7 rule; p[k] = mode[F(x[k])].
   *
   * @see https://stat.ethz.ch/R-manual/R-devel/library/stats/html/quantile.html
   */
  val Type7: Quantiser = (p: Double, n: Long) => { TypeX(p, n, 1 - p) }

  /**
   * Compute quantile indices and gamma coefficient according to R's Type 8 rule; p[k] =~ median[F(x[k])]. The
   * resulting quantile estimates are approximately median-unbiased regardless of the distribution of x.
   *
   * @see https://stat.ethz.ch/R-manual/R-devel/library/stats/html/quantile.html
   */
  val Type8: Quantiser = (p: Double, n: Long) => { TypeX(p, n, (p + 1) / 3) }

  /**
   * Compute quantile indices and gamma coefficient according to R's Type 9 rule; quantile estimates are
   * approximately unbiased for the expected order statistics if x is normally distributed.
   *
   * @see https://stat.ethz.ch/R-manual/R-devel/library/stats/html/quantile.html
   */
  val Type9: Quantiser = (p: Double, n: Long) => { TypeX(p, n, (p / 4) + 3 / 8) }

  private val TypeX = (p: Double, n: Long, m: Double) => {
    val npm = n * p + m
    val j = math.floor(npm).toLong

    (j, npm - j)
  }
}

