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

package au.com.cba.omnia.grimlock.library.window

import au.com.cba.omnia.grimlock.framework._
import au.com.cba.omnia.grimlock.framework.content._
import au.com.cba.omnia.grimlock.framework.content.metadata._
import au.com.cba.omnia.grimlock.framework.encoding._
import au.com.cba.omnia.grimlock.framework.position._
import au.com.cba.omnia.grimlock.framework.utility._
import au.com.cba.omnia.grimlock.framework.window._

/** Base trait for computing a moving average. */
trait MovingAverage[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position]
  extends Window[S, R, Q] {

  /** Function to extract result position. */
  val pos: Locate.WindowSize1[S, R, Q]

  protected def getCollection(cell: Cell[S], rem: R, value: Double): Collection[Cell[Q]] = {
    Collection(Cell[Q](pos(cell, rem), Content(ContinuousSchema[Codex.DoubleCodex](), value)))
  }

  protected def getDouble(con: Content): Double = con.value.asDouble.getOrElse(Double.NaN)

  protected def getCurrent(rem: R, con: Content): (R, Double) = (rem, getDouble(con))
}

/**
 * Trait for computing moving average in batch mode; that is, keep the last N values and compute the moving average
 * from it.
 */
trait BatchMovingAverage[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position]
  extends MovingAverage[S, R, Q] {
  type T = List[(R, Double)]

  /** Size of the window. */
  val window: Int

  /** Indicates if averages should be output when a full window isn't available yet. */
  val all: Boolean

  protected val idx: Int

  def initialise(cell: Cell[S], rem: R): (T, Collection[Cell[Q]]) = {
    val curr = getCurrent(rem, cell.content)

    (List(curr), if (all) { getCollection(cell, curr._1, curr._2) } else { Collection() })
  }

  def present(cell: Cell[S], rem: R, t: T): (T, Collection[Cell[Q]]) = {
    val lst = updateList(rem, cell.content, t)
    val out = (all || lst.size == window) match {
      case true => getCollection(cell, lst(math.min(idx, lst.size - 1))._1, compute(lst))
      case false => Collection[Cell[Q]]()
    }

    (lst, out)
  }

  private def updateList(rem: R, con: Content, lst: List[(R, Double)]): List[(R, Double)] = {
    (if (lst.size == window) { lst.tail } else { lst }) :+ getCurrent(rem, con)
  }

  protected def compute(lst: List[(R, Double)]): Double
}

/** Compute simple moving average over last `window` values. */
case class SimpleMovingAverage[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position](
  window: Int, pos: Locate.WindowSize1[S, R, Q], all: Boolean = false) extends BatchMovingAverage[S, R, Q] {
  protected val idx = window - 1

  protected def compute(lst: List[(R, Double)]): Double = lst.foldLeft(0.0)((c, p) => p._2 + c) / lst.size
}

/** Compute centered moving average over last `2 * width + 1` values. */
case class CenteredMovingAverage[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position](
  width: Int, pos: Locate.WindowSize1[S, R, Q]) extends BatchMovingAverage[S, R, Q] {
  val window = 2 * width + 1
  val all = false
  protected val idx = width

  protected def compute(lst: List[(R, Double)]): Double = lst.foldLeft(0.0)((c, p) => p._2 + c) / lst.size
}

/** Compute weighted moving average over last `window` values. */
case class WeightedMovingAverage[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position](
  window: Int, pos: Locate.WindowSize1[S, R, Q], all: Boolean = false) extends BatchMovingAverage[S, R, Q] {
  protected val idx = window - 1

  protected def compute(lst: List[(R, Double)]): Double = {
    val curr = lst.zipWithIndex.foldLeft((0.0, 0))((c, p) => ((p._2 + 1) * p._1._2 + c._1, c._2 + p._2 + 1))

    curr._1 / curr._2
  }
}

/** Trait for computing moving average in online mode. */
trait OnlineMovingAverage[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position]
  extends MovingAverage[S, R, Q] {
  type T = (Double, Long)

  def initialise(cell: Cell[S], rem: R): (T, Collection[Cell[Q]]) = {
    val curr = getCurrent(rem, cell.content)

    ((curr._2, 1), getCollection(cell, curr._1, curr._2))
  }

  def present(cell: Cell[S], rem: R, t: T): (T, Collection[Cell[Q]]) = {
    val curr = compute(getDouble(cell.content), t)

    ((curr, t._2 + 1), getCollection(cell, rem, curr))
  }

  protected def compute(curr: Double, t: T): Double
}

/** Compute cumulatve moving average. */
case class CumulativeMovingAverage[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position](
  pos: Locate.WindowSize1[S, R, Q]) extends OnlineMovingAverage[S, R, Q] {
  protected def compute(curr: Double, t: T): Double = (curr + t._2 * t._1) / (t._2 + 1)
}

/** Compute exponential moving average. */
case class ExponentialMovingAverage[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position](
  alpha: Double, pos: Locate.WindowSize1[S, R, Q]) extends OnlineMovingAverage[S, R, Q] {
  protected def compute(curr: Double, t: T): Double = alpha * curr + (1 - alpha) * t._1
}

/**
 * Compute cumulative sum.
 *
 * @param pos    Function to extract result position.
 * @param strict Indicates is non-numeric values should result in NaN.
 */
case class CumulativeSum[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position](
  pos: Locate.WindowSize1[S, R, Q], strict: Boolean = true) extends Window[S, R, Q] {
  type T = Option[Double]

  val schema = ContinuousSchema[Codex.DoubleCodex]()

  def initialise(cell: Cell[S], rem: R): (T, Collection[Cell[Q]]) = {
    val value = (strict, cell.content.value.asDouble) match {
      case (true, None) => Some(Double.NaN)
      case (_, v) => v
    }

    (value, value match {
      case Some(d) => Collection(pos(cell, rem), Content(schema, d))
      case None => Collection()
    })
  }

  def present(cell: Cell[S], rem: R, t: T): (T, Collection[Cell[Q]]) = {
    val position = pos(cell, rem)

    (strict, t, cell.content.value.asDouble) match {
      case (true, _, None) => (Some(Double.NaN), Collection(position, Content(schema, Double.NaN)))
      case (false, p, None) => (p, Collection())
      case (_, None, Some(d)) => (Some(d), Collection(position, Content(schema, d)))
      case (_, Some(p), Some(d)) => (Some(p + d), Collection(position, Content(schema, p + d)))
    }
  }
}

/**
 * Compute sliding binary operator on sequential numeric cells.
 *
 * @param binop  The binary operator to apply to two sequential numeric cells.
 * @param pos    Function to extract result position.
 * @param strict Indicates is non-numeric values should result in NaN.
 */
case class BinOp[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position](
  binop: (Double, Double) => Double, pos: Locate.WindowSize2[S, R, Q], strict: Boolean = true) extends Window[S, R, Q] {
  type T = (Option[Double], R)

  def initialise(cell: Cell[S], rem: R): (T, Collection[Cell[Q]]) = {
    val value = (strict, cell.content.value.asDouble) match {
      case (true, None) => Some(Double.NaN)
      case (_, v) => v
    }

    ((value, rem), Collection())
  }

  def present(cell: Cell[S], rem: R, t: T): (T, Collection[Cell[Q]]) = {
    (strict, t, cell.content.value.asDouble) match {
      case (true, (_, c), None) => getResult(cell, rem, Double.NaN, Double.NaN, c)
      case (false, p, None) => (p, Collection())
      case (_, (None, _), Some(d)) => ((Some(d), rem), Collection())
      case (_, (Some(p), c), Some(d)) => getResult(cell, rem, if (p.isNaN) p else d, binop(p, d), c)
    }
  }

  private def getResult(cell: Cell[S], rem: R, value: Double, result: Double, prev: R): (T, Collection[Cell[Q]]) = {
    ((Some(value), rem), Collection(pos(cell, rem, prev), Content(ContinuousSchema[Codex.DoubleCodex](), result)))
  }
}

/**
 * Compute sample quantiles.
 *
 * @param probs     List of probabilities; values must lie in (0, 1).
 * @param count     Function that extracts the count value statistics from the user provided value.
 * @param min       Function that extracts the minimum value statistics from the user provided value.
 * @param max       Function that extracts the maximum value statistics from the user provided value.
 * @param quantiser Function that determines the quantile indices into the order statistics.
 * @param name      Pattern for the name. Use `%1$``s` for the probability correpsponding to the quantile.
 *
 * @note Non-numeric result in `NaN` quantiles.
 */
case class Quantile[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, W](
  probs: List[Double], count: Extract[S, W, Long], min: Extract[S, W, Double], max: Extract[S, W, Double],
  quantiser: Quantile.Quantiser, name: String = "%1$f%%") extends WindowWithValue[S, R, S#M] {
  type T = (Double, Long, List[(Long, Double, String)])
  type V = W

  def initialiseWithValue(cell: Cell[S], rem: R, ext: V): (T, Collection[Cell[S#M]]) = {
    val state = count
      .extract(cell, ext)
      .map {
        case n => probs
          .map { case p => (quantiser(p, n), name.format(p * 100)) }
          .map { case ((j, g), c) => (j, g, c) }
      }
      .getOrElse(List())
    val curr = (state.isEmpty, cell.content.value.asDouble) match {
      case (false, Some(c)) => c
      case _ => Double.NaN
    }
    val col = Collection(List(boundary(cell, ext, min, name, 0, state),
      boundary(cell, ext, max, name, 100, state)).flatten)

    ((curr, 0, state), col)
  }

  def presentWithValue(cell: Cell[S], rem: R, ext: V, t: T): (T, Collection[Cell[S#M]]) = {
    val state = t._3
    val curr = (t._1.isNaN || state.isEmpty, cell.content.value.asDouble) match {
      case (false, Some(c)) => c
      case _ => Double.NaN
    }
    val col = Collection(state.find(_._1 == t._2) match {
      case Some((_, g, n)) => List(Cell[S#M](cell.position.append(n),
        Content(ContinuousSchema[Codex.DoubleCodex](), (1 - g) * t._1 + g * curr)))
      case None => List()
    })

    ((curr, t._2 + 1, state), col)
  }

  private def boundary(cell: Cell[S], ext: W, extractor: Extract[S, W, Double], name: String, value: Double,
    state: List[(Long, Double, String)]): List[Cell[S#M]] = {
    extractor
      .extract(cell, ext)
      .map {
        case v => Cell[S#M](cell.position.append(name.format(value)),
          Content(ContinuousSchema[Codex.DoubleCodex](), if (state.isEmpty) Double.NaN else v))
      }
      .toList
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

