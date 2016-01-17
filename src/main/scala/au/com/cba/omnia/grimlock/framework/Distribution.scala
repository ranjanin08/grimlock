// Copyright 2015 Commonwealth Bank of Australia
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

package au.com.cba.omnia.grimlock.framework.distribution

import au.com.cba.omnia.grimlock.framework._
import au.com.cba.omnia.grimlock.framework.content._
import au.com.cba.omnia.grimlock.framework.content.metadata._
import au.com.cba.omnia.grimlock.framework.encoding._
import au.com.cba.omnia.grimlock.framework.position._
import au.com.cba.omnia.grimlock.framework.utility._

import scala.math.BigDecimal
import scala.reflect.ClassTag

/** Trait for computing approximate distributions from a matrix. */
trait ApproximateDistribution[P <: Position] { self: Matrix[P] =>

  /** Specifies tuners permitted on a call to `histogram`. */
  type HistogramTuners <: OneOf

  /**
   * Compute histogram.
   *
   * @param slice     Encapsulates the dimension(s) to compute histogram on.
   * @param position  Function for extracting the position of the histogram.
   * @param filter    Indicator if numerical values shoud be filtered or not.
   * @param tuner     The tuner for the job.
   *
   * @return A `U[Cell[Q]]' with the histogram.
   *
   * @note The histogram is computed on the positions returned by `position`.
   */
  def histogram[S <: Position with ExpandablePosition, Q <: Position, T <: Tuner](slice: Slice[P],
    position: Locate.FromSelectedAndContent[S, Q], filter: Boolean = true, tuner: T)(
      implicit ev1: PosExpDep[slice.S, Q], ev2: slice.S =:= S, ev3: ClassTag[Q], ev4: HistogramTuners#V[T]): U[Cell[Q]]

  /** Specifies tuners permitted on a call to `quantile`. */
  type QuantileTuners <: OneOf

  /**
   * Compute sample quantiles.
   *
   * @param slice     Encapsulates the dimension(s) to compute quantiles on.
   * @param probs     List of probabilities; values must lie in (0, 1).
   * @param quantiser Function that determines the quantile indices into the order statistics.
   * @param position  Function for extracting the position of the quantile.
   * @param count     Function that extracts the count value statistics from the user provided value.
   * @param value     Value holding the counts.
   * @param filter    Indicator if categorical values should be filtered or not.
   * @param nan       Indicator if NaN quantiles should be output or not.
   * @param tuner     The tuner for the job.
   *
   * @return A `U[Cell[Q]]' with the quantiles.
   *
   * @note Non numeric values result in `NaN` quantiles, while missing counts result in no quantiles.
   */
  def quantile[S <: Position with ExpandablePosition, Q <: Position, W, T <: Tuner](slice: Slice[P],
    probs: List[Double], quantiser: Quantile.Quantiser, position: Locate.FromSelectedAndOutput[S, Double, Q],
      count: Extract[P, W, Long], value: E[W], filter: Boolean = true, nan: Boolean = false, tuner: T)(
        implicit ev1: slice.S =:= S, ev2: PosExpDep[slice.S, Q], ev3: slice.R =!= Position0D, ev4: ClassTag[slice.S],
          ev5: QuantileTuners#V[T]): U[Cell[Q]]
}

private[grimlock] case class Quantile[P <: Position, S <: Position with ExpandablePosition, Q <: Position, W](
  probs: List[Double], count: Extract[P, W, Long], quantiser: Quantile.Quantiser,
    position: Locate.FromSelectedAndOutput[S, Double, Q], nan: Boolean) {
  type V = W
  type C = (Long, List[(Long, Double, Double)])
  type T = (Double, Long, Long)
  type O = (Double, Double)

  def prepare(cell: Cell[P], ext: V): (Double, Long) = {
    (cell.content.value.asDouble.getOrElse(Double.NaN), count.extract(cell, ext).getOrElse(0L))
  }

  def initialise(curr: Double, count: Long): (T, C, List[O]) = {
    val index = 0
    val bounds = boundaries(count)
    val target = next(bounds, index)

    val first = if (count == 0) {
      bounds.map { case (_, _, p) => (p, Double.NaN) }
    } else if (count == 1) {
      bounds.map { case (_, _, p) => (p, curr) }
    } else {
      bounds.collect { case (j, _, p) if (j == index && count != 0) => (p, curr) }
    }

    ((curr, index, target), (count, bounds), first)
  }

  def update(curr: Double, t: T, state: C): (T, List[O]) = {
    val index = t._2 + 1
    val prev = t._1
    val target = t._3
    val count = state._1
    val bounds = state._2

    val last = if (index == (count - 1)) {
      bounds.collect { case (j, _, p) if (j == count) => (p, curr) }
    } else {
      List()
    }

    if (index == target) {
      ((curr, index, next(bounds, index)),
        last ++ bounds.collect { case (j, g, p) if (j == index) => (p, (1 - g) * prev + g * curr) })
    } else {
      ((curr, index, target), last)
    }
  }

  def present(pos: S, out: O): Option[Cell[Q]] = {
    if (!out._2.isNaN || nan) {
      position(pos, out._1).map(Cell(_, Content(ContinuousSchema(DoubleCodex), round(out._2))))
    } else {
      None
    }
  }

  private def boundaries(count: Long): List[(Long, Double, Double)] = {
    probs
      .sorted
      .map { case p => (quantiser(p, count), p) }
      .map { case ((j, g), p) => (j, g, p) }
  }

  private def next(boundaries: List[(Long, Double, Double)], index: Long): Long = {
    boundaries
      .collectFirst { case (j, _, _) if j > index => j }
      .getOrElse(0)
  }

  private def round(value: Double, places: Int = 6): Double = {
    if (value.isNaN) { value } else { BigDecimal(value).setScale(places, BigDecimal.RoundingMode.HALF_UP).toDouble }
  }
}

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
  val Type9: Quantiser = (p: Double, n: Long) => { TypeX(p, n, (p / 4) + (3.0 / 8)) }

  private val TypeX = (p: Double, n: Long, m: Double) => {
    val npm = n * p + m
    val j = math.floor(npm).toLong

    (j, npm - j)
  }
}

