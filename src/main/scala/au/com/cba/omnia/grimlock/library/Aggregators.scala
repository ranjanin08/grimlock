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

package au.com.cba.omnia.grimlock.library.aggregate

import au.com.cba.omnia.grimlock.framework._
import au.com.cba.omnia.grimlock.framework.aggregate._
import au.com.cba.omnia.grimlock.framework.content._
import au.com.cba.omnia.grimlock.framework.content.metadata._
import au.com.cba.omnia.grimlock.framework.distribution._
import au.com.cba.omnia.grimlock.framework.encoding._
import au.com.cba.omnia.grimlock.framework.position._

import com.twitter.algebird.{ Moments => AlgeMoments, Monoid }

/** Trait for aggregators that can be filter based on the type. */
private[aggregate] trait PrepareDouble[P <: Position] {

  /** Indicates if filtering data is required. If so then any non-numeric value is filtered. */
  val filter: Boolean

  def prepareDouble(cell: Cell[P]): Option[Double] = {
    (filter && !cell.content.schema.kind.isSpecialisationOf(Type.Numerical)) match {
      case true => None
      case false => Some(cell.content.value.asDouble.getOrElse(Double.NaN))
    }
  }
}

/** Trait for aggregators that can be lenient or strict when it comes to invalid (or unexpected) values. */
private[aggregate] trait StrictReduce[P <: Position, S <: Position with ExpandablePosition, Q <: Position] {
  self: AggregatorWithValue[P, S, Q] =>

  /**
   * Indicates if strict data handling is required. If so then any invalid value fails the reduction. If not, then
   * invalid values are silently ignored.
   */
  val strict: Boolean

  /**
   * Standard reduce method.
   *
   * @param lt Left state to reduce.
   * @param rt Right state to reduce.
   *
   * @return Reduced state
   */
  def reduce(lt: T, rt: T): T = {
    if (invalid(lt)) { if (strict) { lt } else { rt } }
    else if (invalid(rt)) { if (strict) { rt } else { lt } }
    else { reduction(lt, rt) }
  }

  protected def invalid(t: T): Boolean
  protected def reduction(lt: T, rt: T): T
}

/** Trait for aggregators that can be lenient or strict when it comes to invalid (or unexpected) values. */
private[aggregate] trait PresentDouble[P <: Position, S <: Position with ExpandablePosition]
  extends StrictReduce[P, S, S] { self: Aggregator[P, S, S] =>
  type O[A] = Single[A]

  /** Indicator if 'NaN' value should be output if the reduction failed (for example due to non-numeric data). */
  val nan: Boolean

  /**
   * Present the reduced content.
   *
   * @param pos The reduced position. That is, the position returned by `Slice.selected`.
   * @param t   The reduced state.
   *
   * @return Optional cell where the position is `pos` and the content is derived from `t`.
   *
   * @note An `Option` is used in the return type to allow aggregators to be selective in what content they apply to.
   *       For example, computing the mean is undefined for categorical variables. The aggregator now has the option to
   *       return `None`. This in turn permits an external API, for simple cases, where the user need not know about
   *       the types of variables of their data.
   */
  def present(pos: S, t: T): O[Cell[S]] = {
    if (missing(t) || (invalid(t) && !nan)) {
      Single()
    } else if (invalid(t)) {
      Single(Cell(pos, Content(ContinuousSchema[Double](), Double.NaN)))
    } else {
      Single(Cell(pos, Content(ContinuousSchema[Double](), asDouble(t))))
    }
  }

  protected def missing(t: T): Boolean
  protected def asDouble(t: T): Double
}

/** Base trait for aggregator that return a `Double` value. */
private[aggregate] trait DoubleAggregator[P <: Position, S <: Position with ExpandablePosition]
  extends PrepareDouble[P] with PresentDouble[P, S] { self: Aggregator[P, S, S] =>
  /** Type of the state being aggregated. */
  type T = Double

  /**
   * Prepare for reduction.
   *
   * @param cell Cell which is to be aggregated. Note that its position is prior to `slice.selected` being applied.
   *
   * @return State to reduce.
   */
  def prepare(cell: Cell[P]): Option[T] = prepareDouble(cell)

  protected def invalid(t: T): Boolean = t.isNaN

  protected def missing(t: T): Boolean = false
  protected def asDouble(t: T): Double = t
}

/** Trait for preparing and reducing algebird Moments. */
private[aggregate] trait MomentsPrepareReduce[P <: Position, S <: Position with ExpandablePosition, Q <: Position]
  extends PrepareDouble[P] with StrictReduce[P, S, Q] { self: Aggregator[P, S, Q] =>
  /** Type of the state being aggregated. */
  type T = AlgeMoments

  /**
   * Prepare for reduction.
   *
   * @param cell Cell which is to be aggregated. Note that its position is prior to `slice.selected` being applied.
   *
   * @return State to reduce.
   */
  def prepare(cell: Cell[P]): Option[T] = prepareDouble(cell).map(AlgeMoments(_))

  protected def invalid(t: T): Boolean = t.mean.isNaN
  protected def reduction(lt: T, rt: T): T = Monoid.plus(lt, rt)
}

/** Trait for presenting algebird Moments. */
private[aggregate] trait MomentsPresent[P <: Position, S <: Position with ExpandablePosition]
  extends MomentsPrepareReduce[P, S, S] with PresentDouble[P, S] { self: Aggregator[P, S, S] =>
  protected def missing(t: T): Boolean = false
}

/** Companion object to `MomentsPresent`. */
private[aggregate] object MomentsPresent {
  /**
   * Return the standard deviation.
   *
   * @param t      Algebird moments object to get standard deviation from.
   * @param biased Indicates if the biased estimate should be return or not.
   */
  def sd(t: AlgeMoments, biased: Boolean): Double = {
    if (biased) t.stddev else if (t.count > 1) t.stddev * math.sqrt(t.count / (t.count - 1.0)) else Double.NaN
  }

  /**
   * Return the kurtosis.
   *
   * @param t      Algebird moments object to get kurtosis from.
   * @param excess Indicates if the excess kurtosis should be return or not.
   */
  def kurtosis(t: AlgeMoments, excess: Boolean): Double = if (excess) t.kurtosis else t.kurtosis + 3
}

/** Count reductions. */
case class Count[P <: Position, S <: Position with ExpandablePosition]() extends Aggregator[P, S, S] {
  type T = Long
  type O[A] = Single[A]

  def prepare(cell: Cell[P]): Option[T] = Some(1)

  def reduce(lt: T, rt: T): T = lt + rt

  def present(pos: S, t: T): O[Cell[S]] = Single(Cell(pos, Content(DiscreteSchema[Long](), t)))
}

/** Distinct count reductions. */
case class DistinctCount[P <: Position, S <: Position with ExpandablePosition]() extends Aggregator[P, S, S] {
  type T = Set[Value]
  type O[A] = Single[A]

  def prepare(cell: Cell[P]): Option[T] = Some(Set(cell.content.value))

  def reduce(lt: T, rt: T): T = lt ++ rt

  def present(pos: S, t: T): O[Cell[S]] = Single(Cell(pos, Content(DiscreteSchema[Long](), t.size)))
}

/**
 * Compute counts of values matching a predicate.
 *
 * @param predicte Function to be applied to content.
 */
case class PredicateCount[P <: Position, S <: Position with ExpandablePosition](
  predicate: (Content) => Boolean) extends Aggregator[P, S, S] {
  type T = Long
  type O[A] = Single[A]

  def prepare(cell: Cell[P]): Option[T] = Some(if (predicate(cell.content)) { 1 } else { 0 })

  def reduce(lt: T, rt: T): T = lt + rt

  def present(pos: S, t: T): O[Cell[S]] = Single(Cell(pos, Content(DiscreteSchema[Long](), t)))
}

/**
 * Moments of a distribution.
 *
 * @param mean     The name for the mean of the distibution
 * @param sd       The name for the standard deviation of the distibution
 * @param skewness The name for the skewness of the distibution
 * @param kurtosis The name for the kurtosis of the distibution
 * @param biased   Indicates if the biased estimate should be returned.
 * @param excess   Indicates if the kurtosis or excess kurtosis should be returned.
 * @param filter   Indicates if only numerical types should be aggregated. Is set then all categorical values are
 *                 filtered prior to aggregation.
 * @param strict   Indicates if strict data handling is required. If so then any non-numeric value fails the reduction.
 *                 If not then non-numeric values are silently ignored.
 * @param nan      Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
 *                 data).
 */
case class Moments[P <: Position, S <: Position with ExpandablePosition](mean: String, sd: String, skewness: String,
  kurtosis: String, biased: Boolean = false, excess: Boolean = false, filter: Boolean = true, strict: Boolean = true,
  nan: Boolean = false) extends Aggregator[P, S, S#M] with MomentsPrepareReduce[P, S, S#M] {
  type O[A] = Multiple[A]

  def present(pos: S, t: T): O[Cell[S#M]] = {
    if (invalid(t) && !nan) {
      Multiple()
    } else if (invalid(t)) {
      Multiple(List(Cell(pos.append(mean), Content(ContinuousSchema[Double](), Double.NaN)),
        Cell(pos.append(sd), Content(ContinuousSchema[Double](), Double.NaN)),
        Cell(pos.append(skewness), Content(ContinuousSchema[Double](), Double.NaN)),
        Cell(pos.append(kurtosis), Content(ContinuousSchema[Double](), Double.NaN))))
    } else {
      Multiple(List(Cell(pos.append(mean), Content(ContinuousSchema[Double](), t.mean)),
        Cell(pos.append(sd), Content(ContinuousSchema[Double](), MomentsPresent.sd(t, biased))),
        Cell(pos.append(skewness), Content(ContinuousSchema[Double](), t.skewness)),
        Cell(pos.append(kurtosis), Content(ContinuousSchema[Double](), MomentsPresent.kurtosis(t, excess)))))
    }
  }
}

/**
 * Mean of a distribution.
 *
 * @param filter Indicates if only numerical types should be aggregated. Is set then all categorical values are
 *               filtered prior to aggregation.
 * @param strict Indicates if strict data handling is required. If so then any non-numeric value fails the reduction.
 *               If not then non-numeric values are silently ignored.
 * @param nan    Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
 *               data).
 */
case class Mean[P <: Position, S <: Position with ExpandablePosition](filter: Boolean = true, strict: Boolean = true,
  nan: Boolean = false) extends Aggregator[P, S, S] with MomentsPrepareReduce[P, S, S] with MomentsPresent[P, S] {
  protected def asDouble(t: T): Double = t.mean
}

/**
 * Standard deviation of a distribution.
 *
 * @param biased Indicates if the biased estimate should be returned.
 * @param filter Indicates if only numerical types should be aggregated. Is set then all categorical values are
 *               filtered prior to aggregation.
 * @param strict Indicates if strict data handling is required. If so then any non-numeric value fails the reduction.
 *               If not then non-numeric values are silently ignored.
 * @param nan    Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
 *               data).
 */
case class StandardDeviation[P <: Position, S <: Position with ExpandablePosition](biased: Boolean = false,
  filter: Boolean = true, strict: Boolean = true, nan: Boolean = false) extends Aggregator[P, S, S]
    with MomentsPrepareReduce[P, S, S] with MomentsPresent[P, S] {
  protected def asDouble(t: T): Double = MomentsPresent.sd(t, biased)
}

/**
 * Skewness of a distribution.
 *
 * @param filter Indicates if only numerical types should be aggregated. Is set then all categorical values are
 *               filtered prior to aggregation.
 * @param strict Indicates if strict data handling is required. If so then any non-numeric value fails the reduction.
 *               If not then non-numeric values are silently ignored.
 * @param nan    Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
 *               data).
 */
case class Skewness[P <: Position, S <: Position with ExpandablePosition](filter: Boolean = true,
  strict: Boolean = true, nan: Boolean = false) extends Aggregator[P, S, S] with MomentsPrepareReduce[P, S, S]
    with MomentsPresent[P, S] {
  protected def asDouble(t: T): Double = t.skewness
}

/**
 * Kurtosis of a distribution.
 *
 * @param excess Indicates if the kurtosis or excess kurtosis should be returned.
 * @param filter Indicates if only numerical types should be aggregated. Is set then all categorical values are
 *               filtered prior to aggregation.
 * @param strict Indicates if strict data handling is required. If so then any non-numeric value fails the reduction.
 *               If not then non-numeric values are silently ignored.
 * @param nan    Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
 *               data).
 */
case class Kurtosis[P <: Position, S <: Position with ExpandablePosition](excess: Boolean = false,
  filter: Boolean = true, strict: Boolean = true, nan: Boolean = false) extends Aggregator[P, S, S]
    with MomentsPrepareReduce[P, S, S] with MomentsPresent[P, S] {
  protected def asDouble(t: T): Double = MomentsPresent.kurtosis(t, excess)
}

/**
 * Limits (minimum/maximum value) reduction.
 *
 * @param min    The name for the minimum value.
 * @param max    The name for the maximum value.
 * @param filter Indicates if only numerical types should be aggregated. Is set then all categorical values are
 *               filtered prior to aggregation.
 * @param strict Indicates if strict data handling is required. If so then any non-numeric value fails the reduction.
 *               If not then non-numeric values are silently ignored.
 * @param nan    Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
 *               data).
 */
case class Limits[P <: Position, S <: Position with ExpandablePosition](min: String, max: String,
  filter: Boolean = true, strict: Boolean = true, nan: Boolean = false) extends Aggregator[P, S, S#M]
    with PrepareDouble[P] with StrictReduce[P, S, S#M] {
  type T = (Double, Double)
  type O[A] = Multiple[A]

  def prepare(cell: Cell[P]): Option[T] = prepareDouble(cell).map(d => (d, d))

  def present(pos: S, t: T): O[Cell[S#M]] = {
    if (invalid(t) && !nan) {
      Multiple()
    } else if (invalid(t)) {
      Multiple(List(Cell(pos.append(min), Content(ContinuousSchema[Double](), Double.NaN)),
        Cell(pos.append(max), Content(ContinuousSchema[Double](), Double.NaN))))
    } else {
      Multiple(List(Cell(pos.append(min), Content(ContinuousSchema[Double](), t._1)),
        Cell(pos.append(max), Content(ContinuousSchema[Double](), t._2))))
    }
  }

  protected def invalid(t: T): Boolean = t._1.isNaN || t._2.isNaN
  protected def reduction(lt: T, rt: T): T = (math.min(lt._1, rt._1), math.max(lt._2, rt._2))
}

/**
 * Minimum value reduction.
 *
 * @param filter Indicates if only numerical types should be aggregated. Is set then all categorical values are
 *               filtered prior to aggregation.
 * @param strict Indicates if strict data handling is required. If so then any non-numeric value fails the reduction.
 *               If not then non-numeric values are silently ignored.
 * @param nan    Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
 *               data).
 */
case class Min[P <: Position, S <: Position with ExpandablePosition](filter: Boolean = true, strict: Boolean = true,
  nan: Boolean = false) extends Aggregator[P, S, S] with DoubleAggregator[P, S] {
  protected def reduction(lt: T, rt: T): T = math.min(lt, rt)
}

/**
 * Maximum value reduction.
 *
 * @param filter Indicates if only numerical types should be aggregated. Is set then all categorical values are
 *               filtered prior to aggregation.
 * @param strict Indicates if strict data handling is required. If so then any non-numeric value fails the reduction.
 *               If not then non-numeric values are silently ignored.
 * @param nan    Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
 *               data).
 */
case class Max[P <: Position, S <: Position with ExpandablePosition](filter: Boolean = true, strict: Boolean = true,
  nan: Boolean = false) extends Aggregator[P, S, S] with DoubleAggregator[P, S] {
  protected def reduction(lt: T, rt: T): T = math.max(lt, rt)
}

/**
 * Maximum absolute value reduction.
 *
 * @param filter Indicates if only numerical types should be aggregated. Is set then all categorical values are
 *               filtered prior to aggregation.
 * @param strict Indicates if strict data handling is required. If so then any non-numeric value fails the reduction.
 *               If not then non-numeric values are silently ignored.
 * @param nan    Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
 *               data).
 */
case class MaxAbs[P <: Position, S <: Position with ExpandablePosition](filter: Boolean = true, strict: Boolean = true,
  nan: Boolean = false) extends Aggregator[P, S, S] with DoubleAggregator[P, S] {
  protected def reduction(lt: T, rt: T): T = math.max(math.abs(lt), math.abs(rt))
}

/**
 * Sum value reduction.
 *
 * @param filter Indicates if only numerical types should be aggregated. Is set then all categorical values are
 *               filtered prior to aggregation.
 * @param strict Indicates if strict data handling is required. If so then any non-numeric value fails the reduction.
 *               If not then non-numeric values are silently ignored.
 * @param nan    Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
 *               data).
 */
case class Sum[P <: Position, S <: Position with ExpandablePosition](filter: Boolean = true, strict: Boolean = true,
  nan: Boolean = false) extends Aggregator[P, S, S] with DoubleAggregator[P, S] {
  protected def reduction(lt: T, rt: T): T = lt + rt
}

/**
 * Weighted sum reduction. This is particularly useful for scoring linear models.
 *
 * @param weight Object that will extract, for `cell`, its corresponding weight.
 * @param filter Indicates if only numerical types should be aggregated. Is set then all categorical values are
 *               filtered prior to aggregation.
 * @param strict Indicates if strict data handling is required. If so then any non-numeric value fails the reduction.
 *               If not then non-numeric values are silently ignored.
 * @param nan    Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
 *               data).
 */
case class WeightedSum[P <: Position, S <: Position with ExpandablePosition, W](weight: Extract[P, W, Double],
  filter: Boolean = true, strict: Boolean = true, nan: Boolean = false) extends AggregatorWithValue[P, S, S]
    with PrepareDouble[P] with StrictReduce[P, S, S] {
  type T = Double
  type V = W
  type O[A] = Single[A]

  def prepareWithValue(cell: Cell[P], ext: V): Option[T] = {
    prepareDouble(cell).map(_ * weight.extract(cell, ext).getOrElse(0.0))
  }

  def presentWithValue(pos: S, t: T, ext: V): O[Cell[S]] = {
    if (t.isNaN && !nan) { Single() } else { Single(Cell(pos, Content(ContinuousSchema[Double](), t))) }
  }

  protected def invalid(t: T): Boolean = t.isNaN
  protected def reduction(lt: T, rt: T): T = lt + rt
}

/**
 * Compute entropy.
 *
 * @param count  Object that will extract, for `cell`, its corresponding count.
 * @param filter Indicates if only numerical types should be aggregated. Is set then all categorical values are
 *               filtered prior to aggregation.
 * @param strict Indicates if strict data handling is required. If so then any non-numeric value fails the reduction.
 *               If not then non-numeric values are silently ignored.
 * @param nan    Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
 *               data).
 * @param negate Indicator if negative entropy should be returned.
 * @param log    The log function to use.
 */
case class Entropy[P <: Position, S <: Position with ExpandablePosition, W](count: Extract[P, W, Double],
  filter: Boolean = true, strict: Boolean = true, nan: Boolean = false, negate: Boolean = false,
    log: (Double) => Double = (x: Double) => math.log(x) / math.log(2)) extends AggregatorWithValue[P, S, S]
      with PrepareDouble[P] {
  type T = (Long, Double)
  type V = W
  type O[A] = Single[A]

  def prepareWithValue(cell: Cell[P], ext: V): Option[T] = {
    prepareDouble(cell).map {
      case v => (1, count.extract(cell, ext) match {
        case Some(c) => (v / c) * log(v / c)
        case None => Double.NaN
      })
    }
  }

  def reduce(lt: T, rt: T): T = {
    (lt._1 + rt._1,
      if (lt._2.isNaN) { if (strict) { lt._2 } else { rt._2 } }
      else if (rt._2.isNaN) { if (strict) { rt._2 } else { lt._2 } }
      else { lt._2 + rt._2 })
  }

  def presentWithValue(pos: S, t: T, ext: V): O[Cell[S]] = {
    (t._1 == 1 || (t._2.isNaN && !nan)) match {
      case true => Single()
      case false => Single(Cell(pos, Content(ContinuousSchema[Double](), if (negate) t._2 else -t._2)))
    }
  }
}

/**
 * Compute frequency ratio.
 *
 * @param filter Indicates if only numerical types should be aggregated. Is set then all categorical values are
 *               filtered prior to aggregation.
 * @param strict Indicates if strict data handling is required. If so then any non-numeric value fails the reduction.
 *               If not then non-numeric values are silently ignored.
 * @param nan    Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
 *               data).
 */
case class FrequencyRatio[P <: Position, S <: Position with ExpandablePosition](filter: Boolean = true,
  strict: Boolean = true, nan: Boolean = false) extends Aggregator[P, S, S] with PrepareDouble[P]
    with PresentDouble[P, S] {
  type T = (Long, Double, Double)

  def prepare(cell: Cell[P]): Option[T] = prepareDouble(cell).map { case d => (1, d, d) }

  protected def invalid(t: T): Boolean = t._2.isNaN
  protected def reduction(lt: T, rt: T): T = {
    val high = math.max(lt._2, rt._2)
    val low = (math.max(lt._3, rt._3) == high) match {
      case true => math.min(lt._3, rt._3)
      case false => math.max(lt._3, rt._3)
    }

    (lt._1 + rt._1, high, low)
  }

  protected def missing(t: T): Boolean = t._1 == 1
  protected def asDouble(t: T): Double = t._2 / t._3
}

/**
 * Compute approximate quantiles using a t-digest.
 *
 * @param probs       The quantile probabilities to compute.
 * @param compression The t-digest compression parameter.
 * @param name        Names each quantile output.
 * @param filter      Indicates if only numerical types should be aggregated. Is set then all categorical values are
 *                    filtered prior to aggregation.
 * @param nan         Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
 *                    data).
 *
 * @see https://github.com/tdunning/t-digest
 */
case class TDigestQuantiles[P <: Position, S <: Position with ExpandablePosition, Q <: Position](probs: List[Double],
  compression: Double, name: Locate.FromSelectedAndOutput[S, Double, Q], filter: Boolean = true, nan: Boolean = false)(
    implicit ev: PosExpDep[S, Q]) extends Aggregator[P, S, Q] with PrepareDouble[P] {
  type T = TDigest.T
  type O[A] = Multiple[A]

  def prepare(cell: Cell[P]): Option[T] = prepareDouble(cell).flatMap(TDigest.from(_, compression))

  def reduce(lt: T, rt: T): T = TDigest.reduce(lt, rt)

  def present(pos: S, t: T): O[Cell[Q]] = Multiple(TDigest.toCells(t, probs, pos, name, nan))
}

/**
 * Compute quantiles using a count map.
 *
 * @param probs     The quantile probabilities to compute.
 * @param quantiser Function that determines the quantile indices into the order statistics.
 * @param name      Names each quantile output.
 * @param filter    Indicates if only numerical types should be aggregated. Is set then all categorical values are
 *                  filtered prior to aggregation.
 * @param nan       Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
 *                  data).
 *
 * @note Only use this if all distinct values and their counts fit in memory.
 */
case class CountMapQuantiles[P <: Position, S <: Position with ExpandablePosition, Q <: Position](probs: List[Double],
  quantiser: Quantile.Quantiser, name: Locate.FromSelectedAndOutput[S, Double, Q], filter: Boolean = true,
    nan: Boolean = false)(implicit ev: PosExpDep[S, Q]) extends Aggregator[P, S, Q] with PrepareDouble[P] {
  type T = CountMap.T
  type O[A] = Multiple[A]

  def prepare(cell: Cell[P]): Option[T] = prepareDouble(cell).map(CountMap.from(_))

  def reduce(lt: T, rt: T): T = CountMap.reduce(lt, rt)

  def present(pos: S, t: T): O[Cell[Q]] = Multiple(CountMap.toCells(t, probs, pos, quantiser, name, nan))
}

/**
 * Compute `count` uniformly spaced approximate quantiles using an online streaming parallel histogram.
 *
 * @param count  The number of quantiles to compute.
 * @param name   Names each quantile output.
 * @param filter Indicates if only numerical types should be aggregated. Is set then all categorical values are
 *               filtered prior to aggregation.
 * @param nan    Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
 *               data).
 *
 * @see http://www.jmlr.org/papers/volume11/ben-haim10a/ben-haim10a.pdf
 */
sealed case class UniformQuantiles[P <: Position, S <: Position with ExpandablePosition, Q <: Position](count: Long,
  name: Locate.FromSelectedAndOutput[S, Double, Q], filter: Boolean = true, nan: Boolean = false)(
    implicit ev: PosExpDep[S, Q]) extends Aggregator[P, S, Q] with PrepareDouble[P] {
  type T = StreamingHistogram.T
  type O[A] = Multiple[A]

  def prepare(cell: Cell[P]): Option[T] = prepareDouble(cell).map(d => StreamingHistogram.from(d, count))

  def reduce(lt: T, rt: T): T = StreamingHistogram.reduce(lt, rt)

  def present(pos: S, t: T): O[Cell[Q]] = Multiple(StreamingHistogram.toCells(t, count, pos, name, nan))
}

