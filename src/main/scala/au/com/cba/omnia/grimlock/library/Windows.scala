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
trait MovingAverage[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition]
  extends Window[S, R, S#M] {

  /** The dimension in `rem` from which to get the coordinate to append to `sel`. */
  val dim: Dimension

  protected def getCollection(sel: S, coord: Value, value: Double): Collection[Cell[S#M]] = {
    Collection(Cell[S#M](sel.append(coord), Content(ContinuousSchema[Codex.DoubleCodex](), value)))
  }

  protected def getDouble(con: Content): Double = con.value.asDouble.getOrElse(Double.NaN)

  protected def getCurrent(rem: R, con: Content): (Value, Double) = (rem(dim), getDouble(con))
}

/**
 * Trait for computing moving average in batch mode; that is, keep the last N values and compute the moving average
 * from it.
 */
trait BatchMovingAverage[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition]
  extends MovingAverage[S, R] {
  type T = List[(Value, Double)]

  /** Size of the window. */
  val window: Int

  /** Indicates if averages should be output when a full window isn't available yet. */
  val all: Boolean

  protected val idx: Int

  def initialise(cell: Cell[S], rem: R): (T, Collection[Cell[S#M]]) = {
    val curr = getCurrent(rem, cell.content)

    (List(curr), if (all) { getCollection(cell.position, curr._1, curr._2) } else { Collection() })
  }

  def present(cell: Cell[S], rem: R, t: T): (T, Collection[Cell[S#M]]) = {
    val lst = updateList(rem, cell.content, t)
    val out = (all || lst.size == window) match {
      case true => getCollection(cell.position, lst(math.min(idx, lst.size - 1))._1, compute(lst))
      case false => Collection[Cell[S#M]]()
    }

    (lst, out)
  }

  private def updateList(rem: R, con: Content, lst: List[(Value, Double)]): List[(Value, Double)] = {
    (if (lst.size == window) { lst.tail } else { lst }) :+ getCurrent(rem, con)
  }

  protected def compute(lst: List[(Value, Double)]): Double
}

/**
 * Compute simple moving average over last `window` values.
 *
 * @param window Size of the window.
 * @param dim    The dimension in `rem` from which to get the coordinate to append to `sel`.
 * @param all    Indicates if averages should be output when a full window isn't available yet.
 */
case class SimpleMovingAverage[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition](
  window: Int, dim: Dimension = First, all: Boolean = false) extends BatchMovingAverage[S, R] {
  protected val idx = window - 1

  protected def compute(lst: List[(Value, Double)]): Double = lst.foldLeft(0.0)((c, p) => p._2 + c) / lst.size
}

/**
 * Compute centered moving average over last `2 * width + 1` values.
 *
 * @param width Number of values before and after a given value to use when computing the moving average.
 * @param dim   The dimension in `rem` from which to get the coordinate to append to `sel`.
 */
case class CenteredMovingAverage[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition](
  width: Int, dim: Dimension = First) extends BatchMovingAverage[S, R] {
  val window = 2 * width + 1
  val all = false
  protected val idx = width

  protected def compute(lst: List[(Value, Double)]): Double = lst.foldLeft(0.0)((c, p) => p._2 + c) / lst.size
}

/**
 * Compute weighted moving average over last `window` values.
 *
 * @param window Size of the window.
 * @param dim    The dimension in `rem` from which to get the coordinate to append to `sel`.
 * @param all    Indicates if averages should be output when a full window isn't available yet.
 */
case class WeightedMovingAverage[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition](
  window: Int, dim: Dimension = First, all: Boolean = false) extends BatchMovingAverage[S, R] {
  protected val idx = window - 1

  protected def compute(lst: List[(Value, Double)]): Double = {
    val curr = lst.zipWithIndex.foldLeft((0.0, 0))((c, p) => ((p._2 + 1) * p._1._2 + c._1, c._2 + p._2 + 1))

    curr._1 / curr._2
  }
}

/** Trait for computing moving average in online mode. */
trait OnlineMovingAverage[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition]
  extends MovingAverage[S, R] {
  type T = (Double, Long)

  def initialise(cell: Cell[S], rem: R): (T, Collection[Cell[S#M]]) = {
    val curr = getCurrent(rem, cell.content)

    ((curr._2, 1), getCollection(cell.position, curr._1, curr._2))
  }

  def present(cell: Cell[S], rem: R, t: T): (T, Collection[Cell[S#M]]) = {
    val curr = compute(getDouble(cell.content), t)

    ((curr, t._2 + 1), getCollection(cell.position, rem(dim), curr))
  }

  protected def compute(curr: Double, t: T): Double
}

/**
 * Compute cumulatve moving average.
 *
 * @param dim  The dimension in `rem` from which to get the coordinate to append to `sel`.
 */
case class CumulativeMovingAverage[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition](
  dim: Dimension = First) extends OnlineMovingAverage[S, R] {
  protected def compute(curr: Double, t: T): Double = (curr + t._2 * t._1) / (t._2 + 1)
}

/**
 * Compute exponential moving average.
 *
 * @param alpha Degree of weighting coefficient.
 * @param dim   The dimension in `rem` from which to get the coordinate to append to `sel`.
 */
case class ExponentialMovingAverage[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition](
  alpha: Double, dim: Dimension = First) extends OnlineMovingAverage[S, R] {
  protected def compute(curr: Double, t: T): Double = alpha * curr + (1 - alpha) * t._1
}

/**
 * Compute cumulative sum.
 *
 * @param separator The separator to use for the appended coordinate.
 *
 * @note Non-numeric values are silently ignored.
 */
// TODO: Test this
case class CumulativeSum[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition](
  separator: String = "|") extends Window[S, R, S#M] {
  type T = Option[Double]

  val schema = ContinuousSchema[Codex.DoubleCodex]()

  def initialise(cell: Cell[S], rem: R): (T, Collection[Cell[S#M]]) = {
    val value = cell.content.value.asDouble

    (value, value match {
      case Some(d) => Collection(cell.position.append(rem.toShortString(separator)), Content(schema, d))
      case None => Collection()
    })
  }

  def present(cell: Cell[S], rem: R, t: T): (T, Collection[Cell[S#M]]) = {
    val coord = rem.toShortString(separator)

    (t, cell.content.value.asDouble) match {
      case (None, None) => (None, Collection())
      case (None, Some(d)) => (Some(d), Collection(cell.position.append(coord), Content(schema, d)))
      case (Some(p), None) => (Some(p), Collection())
      case (Some(p), Some(d)) => (Some(p + d), Collection(cell.position.append(coord), Content(schema, p + d)))
    }
  }
}

/**
 * Compute sliding binary operator on sequential numeric cells.
 *
 * @param binop     The binary operator to apply to two sequential numeric cells.
 * @param name      The name of the appended (result) coordinate. Use `%[12]$``s` for the string representations of
 *                  left (first) and right (second) coordinates.
 * @param separator The separator to use for the appended coordinate.
 *
 * @note Non-numeric values are silently ignored.
 */
// TODO: Test this
case class BinOp[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition](
  binop: (Double, Double) => Double, name: String = "f(%1$s, %2$s)",
  separator: String = "|") extends Window[S, R, S#M] {
  type T = (Option[Double], String)

  def initialise(cell: Cell[S], rem: R): (T, Collection[Cell[S#M]]) = {
    ((cell.content.value.asDouble, rem.toShortString(separator)), Collection())
  }

  def present(cell: Cell[S], rem: R, t: T): (T, Collection[Cell[S#M]]) = {
    val coord = rem.toShortString(separator)
    val schema = ContinuousSchema[Codex.DoubleCodex]()

    (t, cell.content.value.asDouble) match {
      case ((None, _), None) =>
        ((None, coord), Collection())
      case ((None, _), Some(d)) =>
        ((Some(d), coord), Collection())
      case ((Some(p), _), None) =>
        ((None, coord), Collection())
      case ((Some(p), c), Some(d)) =>
        ((Some(d), coord), Collection(cell.position.append(name.format(c, coord)), Content(schema, binop(p, d))))
    }
  }
}

/**
 * Compute sample quantiles.
 *
 * @param probs     List of probabilities; values must lie in [0 1].
 * @param count     Function that extracts the count value statistics from the user provided value.
 * @param min       Function that extracts the minimum value statistics from the user provided value.
 * @param max       Function that extracts the maximum value statistics from the user provided value.
 * @param quantiser Function that determines the quantile indices into the order statistics.
 * @param name      Pattern for the name. Use `%1$``s` for the probability correpsponding to the quantile.
 *
 * @note Non-numeric result in `NaN` quantiles.
 */
// TODO: Test this
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
          .map {
            case p =>
              val (j, g) = quantiser(p, n)

              (j, g, name.format(p * 100))
          }
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
    val curr = cell.content.value.asDouble.getOrElse(Double.NaN)
    val col = Collection(state.find(_._1 == t._2) match {
      case Some((_, g, n))  => List(Cell[S#M](cell.position.append(n),
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

