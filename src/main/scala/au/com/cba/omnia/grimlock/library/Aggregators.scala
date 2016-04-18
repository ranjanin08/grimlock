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
import au.com.cba.omnia.grimlock.framework.encoding._
import au.com.cba.omnia.grimlock.framework.position._

/** Count reductions. */
case class Count[P <: Position, S <: Position with ExpandablePosition]() extends Aggregator[P, S, S] {
  type T = Long

  def prepare(cell: Cell[P]): Option[T] = Some(1)

  def reduce(lt: T, rt: T): T = lt + rt

  def present(pos: S, t: T): Option[Cell[S]] = Some(Cell(pos, Content(DiscreteSchema[Long](), t)))
}

/** Distinct count reductions. */
case class DistinctCount[P <: Position, S <: Position with ExpandablePosition]() extends Aggregator[P, S, S] {
  type T = Set[Value]

  def prepare(cell: Cell[P]): Option[T] = Some(Set(cell.content.value))

  def reduce(lt: T, rt: T): T = lt ++ rt

  def present(pos: S, t: T): Option[Cell[S]] = Some(Cell(pos, Content(DiscreteSchema[Long](), t.size)))
}

/**
 * Compute counts of values matching a predicate.
 *
 * @param predicte Function to be applied to content.
 */
case class PredicateCount[P <: Position, S <: Position with ExpandablePosition](
  predicate: (Content) => Boolean) extends Aggregator[P, S, S] {
  type T = Long

  def prepare(cell: Cell[P]): Option[T] = Some(if (predicate(cell.content)) { 1 } else { 0 })

  def reduce(lt: T, rt: T): T = lt + rt

  def present(pos: S, t: T): Option[Cell[S]] = Some(Cell(pos, Content(DiscreteSchema[Long](), t)))
}

/** Trait for aggregators that can be filter based on the type. */
private[aggregate] trait FilterAggregator[P <: Position] {

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
private[aggregate] trait StrictAggregator[P <: Position, S <: Position with ExpandablePosition] {
  self: Aggregator[P, S, S] =>

  /**
   * Indicates if strict data handling is required. If so then any invalid value fails the reduction. If not, then
   * invalid values are silently ignored.
   */
  val strict: Boolean

  /** Indicator if 'NaN' value should be output if the reduction failed (for example due to non-numeric data). */
  val nan: Boolean

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
  def present(pos: S, t: T): Option[Cell[S]] = {
    if (invalid(t) && !nan) {
      None
    } else if (invalid(t)) {
      Some(Cell(pos, Content(ContinuousSchema[Double](), Double.NaN)))
    } else {
      Some(Cell(pos, Content(ContinuousSchema[Double](), asDouble(t))))
    }
  }

  protected def invalid(t: T): Boolean
  protected def reduction(lt: T, rt: T): T
  protected def asDouble(t: T): Double
}

private[aggregate] trait MomentsAggregator[P <: Position, S <: Position with ExpandablePosition]
  extends FilterAggregator[P] with StrictAggregator[P, S] { self: Aggregator[P, S, S] =>
  /** Type of the state being aggregated. */
  type T = com.twitter.algebird.Moments

  /**
   * Prepare for reduction.
   *
   * @param cell Cell which is to be aggregated. Note that its position is prior to `slice.selected` being applied.
   *
   * @return State to reduce.
   */
  def prepare(cell: Cell[P]): Option[T] = prepareDouble(cell).map(com.twitter.algebird.Moments(_))

  protected def invalid(t: T): Boolean = t.mean.isNaN
  protected def reduction(lt: T, rt: T): T = com.twitter.algebird.Monoid.plus(lt, rt)
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
  nan: Boolean = false) extends Aggregator[P, S, S] with MomentsAggregator[P, S] {
  protected def asDouble(t: T): Double = t.mean
}

/**
 * Standard deviation of a distribution.
 *
 * @param filter Indicates if only numerical types should be aggregated. Is set then all categorical values are
 *               filtered prior to aggregation.
 * @param strict Indicates if strict data handling is required. If so then any non-numeric value fails the reduction.
 *               If not then non-numeric values are silently ignored.
 * @param nan    Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
 *               data).
 */
case class StandardDeviation[P <: Position, S <: Position with ExpandablePosition](filter: Boolean = true,
  strict: Boolean = true, nan: Boolean = false) extends Aggregator[P, S, S] with MomentsAggregator[P, S] {
  protected def asDouble(t: T): Double = t.stddev * math.sqrt(t.count / (t.count - 1))
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
  strict: Boolean = true, nan: Boolean = false) extends Aggregator[P, S, S] with MomentsAggregator[P, S] {
  protected def asDouble(t: T): Double = t.skewness
}

/**
 * Kurtosis of a distribution.
 *
 * @param filter Indicates if only numerical types should be aggregated. Is set then all categorical values are
 *               filtered prior to aggregation.
 * @param strict Indicates if strict data handling is required. If so then any non-numeric value fails the reduction.
 *               If not then non-numeric values are silently ignored.
 * @param nan    Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
 *               data).
 */
case class Kurtosis[P <: Position, S <: Position with ExpandablePosition](filter: Boolean = true,
  strict: Boolean = true, nan: Boolean = false) extends Aggregator[P, S, S] with MomentsAggregator[P, S] {
  protected def asDouble(t: T): Double = t.kurtosis + 3
}

/** Base trait for aggregator that return a `Double` value. */
private[aggregate] trait DoubleAggregator[P <: Position, S <: Position with ExpandablePosition]
  extends FilterAggregator[P] with StrictAggregator[P, S] { self: Aggregator[P, S, S] =>
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
  protected def asDouble(t: T): Double = t
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
    with FilterAggregator[P] {
  type T = Double

  type V = W

  def prepareWithValue(cell: Cell[P], ext: V): Option[T] = {
    prepareDouble(cell).map {
      case v => weight.extract(cell, ext) match {
        case None => v * 0
        case Some(w) => v * w
      }
    }
  }

  def reduce(lt: T, rt: T): T = {
    if (lt.isNaN) { if (strict) { lt } else { rt } }
    else if (rt.isNaN) { if (strict) { rt } else { lt } }
    else { lt + rt }
  }

  def presentWithValue(pos: S, t: T, ext: V): Option[Cell[S]] = {
    (t.isNaN && !nan) match {
      case true => None
      case false => Some(Cell(pos, Content(ContinuousSchema[Double](), t)))
    }
  }
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
      with FilterAggregator[P] {
  type T = (Long, Double)

  type V = W

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

  def presentWithValue(pos: S, t: T, ext: V): Option[Cell[S]] = {
    (t._1 == 1 || (t._2.isNaN && !nan)) match {
      case true => None
      case false => Some(Cell(pos, Content(ContinuousSchema[Double](), if (negate) t._2 else -t._2)))
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
  strict: Boolean = true, nan: Boolean = false) extends Aggregator[P, S, S] with FilterAggregator[P] {
  type T = (Long, Double, Double)

  def prepare(cell: Cell[P]): Option[T] = prepareDouble(cell).map { case d => (1, d, d) }

  def reduce(lt: T, rt: T): T = {
    if (lt._2.isNaN) { if (strict) { lt } else { rt } }
    else if (rt._2.isNaN) { if (strict) { rt } else { lt } }
    else {
      val high = math.max(lt._2, rt._2)
      val low = (math.max(lt._3, rt._3) == high) match {
        case true => math.min(lt._3, rt._3)
        case false => math.max(lt._3, rt._3)
      }

      (lt._1 + rt._1, high, low)
    }
  }

  def present(pos: S, t: T): Option[Cell[S]] = {
    if (t._1 == 1 || (t._2.isNaN && !nan)) {
      None
    } else if (t._2.isNaN) {
      Some(Cell(pos, Content(ContinuousSchema[Double](), Double.NaN)))
    } else {
      Some(Cell(pos, Content(ContinuousSchema[Double](), t._2 / t._3)))
    }
  }
}

