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

package au.com.cba.omnia.grimlock.reduce

import au.com.cba.omnia.grimlock._
import au.com.cba.omnia.grimlock.content._
import au.com.cba.omnia.grimlock.content.metadata._
import au.com.cba.omnia.grimlock.encoding._
import au.com.cba.omnia.grimlock.position._
import au.com.cba.omnia.grimlock.Type._
import au.com.cba.omnia.grimlock.utility._

import scala.reflect.ClassTag

/** Trait for reducers that can be lenient or strict when it comes to invalid (or unexpected) values. */
trait StrictReduce { self: Reducer =>
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

/** Trait with default values for various reducer arguments. */
trait DefaultReducerValues {
  /** Default indicator for strict handling or invalid values. */
  val DefaultStrict: Boolean = true

  /** Default indicator for presenting invalid (NaN) reduced values or None. */
  val DefaultNaN: Boolean = false
}

/**
 * Count reductions.
 *
 * @param name Optional coordinate name for the count value. Name must be provided when presenting `PresentMultiple`.
 */
case class Count private (name: Option[Value]) extends Reducer with Prepare with PresentSingleAndMultiple {
  type T = Long

  val ct = ClassTag[Long](Long.getClass)

  def prepare[P <: Position, D <: Dimension](slice: Slice[P, D], cell: Cell[P]): T = 1

  def reduce(lt: T, rt: T): T = lt + rt

  protected def content(t: T): Option[Content] = Some(Content(DiscreteSchema[Codex.LongCodex](), t))
}

/** Companion object to `Count` reducer class. */
object Count {
  /** Count reductions. */
  def apply(): Reducer with Prepare with PresentSingle = Count(None)

  /**
   * Count reductions.
   *
   * @param name Coordinate name for the count values.
   */
  def apply[V](name: V)(implicit ev: Valueable[V]): Reducer with Prepare with PresentMultiple = {
    Count(Some(ev.convert(name)))
  }
}

/** Trait for moments of a distribution. */
sealed trait Moment {
  /** Index of the moment (starting at 0). */
  val index: Int
}

/** Object for computing the mean. */
case object Mean extends Moment {
  val index = 1

  /** Mean of a distribution. */
  def apply(): Reducer with Prepare with PresentSingle = Moments(Mean)

  /**
   * Mean of a distribution.
   *
   * @param strict Indicates if strict data handling is required. If so then any non-numeric value fails the reduction.
   *               If not then non-numeric values are silently ignored.
   * @param nan    Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
   *               data).
   */
  def apply(strict: Boolean, nan: Boolean): Reducer with Prepare with PresentSingle = Moments(Mean, strict, nan)

  /**
   * Mean of a distribution.
   *
   * @param name Coordinate name of the computed mean.
   */
  def apply[V](name: V)(implicit ev: Valueable[V]): Reducer with Prepare with PresentMultiple = Moments((Mean, name))

  /**
   * Mean of a distribution.
   *
   * @param name   Coordinate name of the computed mean.
   * @param strict Indicates if strict data handling is required. If so then any non-numeric value fails the reduction.
   *               If not then non-numeric values are silently ignored.
   * @param nan    Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
   *               data).
   */
  def apply[V](name: V, strict: Boolean, nan: Boolean)(
    implicit ev: Valueable[V]): Reducer with Prepare with PresentMultiple = Moments((Mean, name), strict, nan)
}

/** Object for computing the standard deviation. */
object StandardDeviation extends Moment {
  val index = 2

  /** Standard deviation of a distribution. */
  def apply(): Reducer with Prepare with PresentSingle = Moments(StandardDeviation)

  /**
   * Standard deviation of a distribution.
   *
   * @param strict Indicates if strict data handling is required. If so then any non-numeric value fails the reduction.
   *               If not then non-numeric values are silently ignored.
   * @param nan    Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
   *               data).
   */
  def apply(strict: Boolean, nan: Boolean): Reducer with Prepare with PresentSingle = {
    Moments(StandardDeviation, strict, nan)
  }

  /**
   * Standard deviation of a distribution.
   *
   * @param name Coordinate name of the computed standard deviation.
   */
  def apply[V](name: V)(implicit ev: Valueable[V]): Reducer with Prepare with PresentMultiple = {
    Moments((StandardDeviation, name))
  }

  /**
   * Standard deviation of a distribution.
   *
   * @param name   Coordinate name of the computed standard deviation.
   * @param strict Indicates if strict data handling is required. If so then any non-numeric value fails the reduction.
   *               If not then non-numeric values are silently ignored.
   * @param nan    Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
   *               data).
   */
  def apply[V](name: V, strict: Boolean, nan: Boolean)(
    implicit ev: Valueable[V]): Reducer with Prepare with PresentMultiple = {
    Moments((StandardDeviation, name), strict, nan)
  }
}

/** Object for computing the skewness. */
object Skewness extends Moment {
  val index = 3

  /** Skewness of a distribution. */
  def apply(): Reducer with Prepare with PresentSingle = Moments(Skewness)

  /**
   * Skewness of a distribution.
   *
   * @param strict Indicates if strict data handling is required. If so then any non-numeric value fails the reduction.
   *               If not then non-numeric values are silently ignored.
   * @param nan    Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
   *               data).
   */
  def apply(strict: Boolean, nan: Boolean): Reducer with Prepare with PresentSingle = Moments(Skewness, strict, nan)

  /**
   * Skewness of a distribution.
   *
   * @param name Coordinate name of the computed skewness.
   */
  def apply[V](name: V)(implicit ev: Valueable[V]): Reducer with Prepare with PresentMultiple = {
    Moments((Skewness, name))
  }

  /**
   * Skewness of a distribution.
   *
   * @param name   Coordinate name of the computed skewness.
   * @param strict Indicates if strict data handling is required. If so then any non-numeric value fails the reduction.
   *               If not then non-numeric values are silently ignored.
   * @param nan    Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
   *               data).
   */
  def apply[V](name: V, strict: Boolean, nan: Boolean)(
    implicit ev: Valueable[V]): Reducer with Prepare with PresentMultiple = Moments((Skewness, name), strict, nan)
}

/** Object for computing the kurtosis. */
object Kurtosis extends Moment {
  val index = 4

  /** Kurtosis of a distribution. */
  def apply(): Reducer with Prepare with PresentSingle = Moments(Kurtosis)

  /**
   * Kurtosis of a distribution.
   *
   * @param strict Indicates if strict data handling is required. If so then any non-numeric value fails the reduction.
   *               If not then non-numeric values are silently ignored.
   * @param nan    Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
   *               data).
   */
  def apply(strict: Boolean, nan: Boolean): Reducer with Prepare with PresentSingle = Moments(Kurtosis, strict, nan)

  /**
   * Kurtosis of a distribution.
   *
   * @param name Coordinate name of the computed kurtosis.
   */
  def apply[V](name: V)(implicit ev: Valueable[V]): Reducer with Prepare with PresentMultiple = {
    Moments((Kurtosis, name))
  }

  /**
   * Kurtosis of a distribution.
   *
   * @param name   Coordinate name of the computed kurtosis.
   * @param strict Indicates if strict data handling is required. If so then any non-numeric value fails the reduction.
   *               If not then non-numeric values are silently ignored.
   * @param nan    Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
   *               data).
   */
  def apply[V](name: V, strict: Boolean, nan: Boolean)(
    implicit ev: Valueable[V]): Reducer with Prepare with PresentMultiple = Moments((Kurtosis, name), strict, nan)
}

/**
 * Moments of a distribution.
 *
 * @param strict  Indicates if strict data handling is required. If so then any non-numeric value fails the reduction.
 *                If not then non-numeric values are silently ignored.
 * @param nan     Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
 *                data).
 * @param moments Subset of moments (1 to 4) to compute together with coordinate name. Names must be provided when
 *                presenting `PresentMultiple`.
 */
case class Moments private (strict: Boolean, nan: Boolean, moments: List[(Int, Option[Value])]) extends Reducer
  with Prepare with PresentSingle with PresentMultiple with StrictReduce {
  type T = com.twitter.algebird.Moments

  val ct = ClassTag[com.twitter.algebird.Moments](com.twitter.algebird.Moments.getClass)

  def prepare[P <: Position, D <: Dimension](slice: Slice[P, D], cell: Cell[P]): T = {
    com.twitter.algebird.Moments(cell.content.value.asDouble.getOrElse(Double.NaN))
  }

  def presentSingle[P <: Position](pos: P, t: T): Option[Cell[P]] = {
    content(t).map { case cl => Cell(pos, cl(moments(0)._1 - 1)) }
  }

  def presentMultiple[P <: Position with ExpandablePosition](pos: P, t: T): Collection[Cell[P#M]] = {
    Collection(content(t).map {
      case cl => Right(moments.map {
        case (i, Some(n)) => Cell[P#M](pos.append(n), cl(i - 1))
        case _ => throw new Exception("Missing name")
      })
    })
  }

  protected def invalid(t: T): Boolean = t.mean.isNaN

  protected def reduction(lt: T, rt: T): T = com.twitter.algebird.Monoid.plus(lt, rt)

  protected def content(t: T): Option[List[Content]] = {
    if (t.mean.isNaN && !nan) {
      None
    } else if (t.mean.isNaN) {
      Some(List.fill(4)(Content(ContinuousSchema[Codex.DoubleCodex](), Double.NaN)))
    } else {
      Some(List(
        Content(ContinuousSchema[Codex.DoubleCodex](), t.mean),
        Content(ContinuousSchema[Codex.DoubleCodex](), t.stddev),
        Content(ContinuousSchema[Codex.DoubleCodex](), t.skewness),
        Content(ContinuousSchema[Codex.DoubleCodex](), t.kurtosis)))
    }
  }
}

/** Companion object to `Moments` reducer class. */
object Moments extends DefaultReducerValues {
  /**
   * Moment of a distribution.
   *
   * @param moment Moment to compute.
   */
  def apply(moment: Moment): Reducer with Prepare with PresentSingle = {
    Moments(DefaultStrict, DefaultNaN, List((moment.index, None)))
  }

  /**
   * Moment of a distribution.
   *
   * @param moment Moment to compute.
   * @param strict Indicates if strict data handling is required. If so then any non-numeric value fails the reduction.
   *               If not then non-numeric values are silently ignored.
   * @param nan    Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
   *               data).
   */
  def apply(moment: Moment, strict: Boolean, nan: Boolean): Reducer with Prepare with PresentSingle = {
    Moments(strict, nan, List((moment.index, None)))
  }

  /**
   * Moment of a distribution.
   *
   * @param moment1 Tuple of moment to compute togeter with the name of the coordinate.
   */
  def apply[M <: Moment, T](moment1: (M, T))(implicit ev1: Valueable[T]): Reducer with Prepare with PresentMultiple = {
    Moments(DefaultStrict, DefaultNaN, List((moment1._1.index, Some(ev1.convert(moment1._2)))))
  }

  /**
   * Moments of a distribution.
   *
   * @param moment1 First tuple of moment to compute togeter with the name of the coordinate.
   * @param moment2 Second tuple of moment to compute togeter with the name of the coordinate.
   */
  def apply[M <: Moment, N <: Moment, T, U](moment1: (M, T), moment2: (N, U))(implicit ev1: Valueable[T],
    ev2: Valueable[U], ne1: M =!= N): Reducer with Prepare with PresentMultiple = {
    Moments(DefaultStrict, DefaultNaN, List((moment1._1.index, Some(ev1.convert(moment1._2))),
      (moment2._1.index, Some(ev2.convert(moment2._2)))))
  }

  /**
   * Moments of a distribution.
   *
   * @param moment1 First tuple of moment to compute togeter with the name of the coordinate.
   * @param moment2 Second tuple of moment to compute togeter with the name of the coordinate.
   * @param moment3 Third tuple of moment to compute togeter with the name of the coordinate.
   */
  def apply[M <: Moment, N <: Moment, O <: Moment, T, U, V](moment1: (M, T), moment2: (N, U), moment3: (O, V))(
    implicit ev1: Valueable[T], ev2: Valueable[U], ev3: Valueable[V], ne1: M =!= N, ne2: M =!= O,
    ne3: N =!= O): Reducer with Prepare with PresentMultiple = {
    Moments(DefaultStrict, DefaultNaN, List((moment1._1.index, Some(ev1.convert(moment1._2))),
      (moment2._1.index, Some(ev2.convert(moment2._2))), (moment3._1.index, Some(ev3.convert(moment3._2)))))
  }

  /**
   * Moments of a distribution.
   *
   * @param moment1 First tuple of moment to compute togeter with the name of the coordinate.
   * @param moment2 Second tuple of moment to compute togeter with the name of the coordinate.
   * @param moment3 Third tuple of moment to compute togeter with the name of the coordinate.
   * @param moment4 Fourth tuple of moment to compute togeter with the name of the coordinate.
   */
  def apply[M <: Moment, N <: Moment, O <: Moment, P <: Moment, T, U, V, S](moment1: (M, T), moment2: (N, U),
    moment3: (O, V), moment4: (P, S))(implicit ev1: Valueable[T], ev2: Valueable[U], ev3: Valueable[V],
      ev4: Valueable[S], ne1: M =!= N, ne2: M =!= O, ne3: M =!= P, ne4: N =!= O, ne5: N =!= P,
      ne6: O =!= P): Reducer with Prepare with PresentMultiple = {
    Moments(DefaultStrict, DefaultNaN, List((moment1._1.index, Some(ev1.convert(moment1._2))),
      (moment2._1.index, Some(ev2.convert(moment2._2))), (moment3._1.index, Some(ev3.convert(moment3._2))),
      (moment4._1.index, Some(ev4.convert(moment4._2)))))
  }

  /**
   * Moments of a distribution.
   *
   * @param moment1 Tuple of moment to compute togeter with the name of the coordinate.
   * @param strict  Indicates if strict data handling is required. If so then any non-numeric value fails the
   *                reduction. If not then non-numeric values are silently ignored.
   * @param nan     Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
   *                data).
   */
  def apply[M <: Moment, T](moment1: (M, T), strict: Boolean, nan: Boolean)(
    implicit ev1: Valueable[T]): Reducer with Prepare with PresentMultiple = {
    Moments(strict, nan, List((moment1._1.index, Some(ev1.convert(moment1._2)))))
  }

  /**
   * Moments of a distribution.
   *
   * @param moment1 First tuple of moment to compute togeter with the name of the coordinate.
   * @param moment2 Second tuple of moment to compute togeter with the name of the coordinate.
   * @param strict  Indicates if strict data handling is required. If so then any non-numeric value fails the
   *                reduction. If not then non-numeric values are silently ignored.
   * @param nan     Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
   *                data).
   */
  def apply[M <: Moment, N <: Moment, T, U](moment1: (M, T), moment2: (N, U), strict: Boolean, nan: Boolean)(
    implicit ev1: Valueable[T], ev2: Valueable[U], ne1: M =!= N): Reducer with Prepare with PresentMultiple = {
    Moments(strict, nan, List((moment1._1.index, Some(ev1.convert(moment1._2))),
      (moment2._1.index, Some(ev2.convert(moment2._2)))))
  }

  /**
   * Moments of a distribution.
   *
   * @param moment1 First tuple of moment to compute togeter with the name of the coordinate.
   * @param moment2 Second tuple of moment to compute togeter with the name of the coordinate.
   * @param moment3 Third tuple of moment to compute togeter with the name of the coordinate.
   * @param strict  Indicates if strict data handling is required. If so then any non-numeric value fails the
   *                reduction. If not then non-numeric values are silently ignored.
   * @param nan     Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
   *                data).
   */
  def apply[M <: Moment, N <: Moment, O <: Moment, T, U, V](moment1: (M, T), moment2: (N, U), moment3: (O, V),
    strict: Boolean, nan: Boolean)(implicit ev1: Valueable[T], ev2: Valueable[U], ev3: Valueable[V], ne1: M =!= N,
      ne2: M =!= O, ne3: N =!= O): Reducer with Prepare with PresentMultiple = {
    Moments(strict, nan, List((moment1._1.index, Some(ev1.convert(moment1._2))),
      (moment2._1.index, Some(ev2.convert(moment2._2))), (moment3._1.index, Some(ev3.convert(moment3._2)))))
  }

  /**
   * Moments of a distribution.
   *
   * @param moment1 First tuple of moment to compute togeter with the name of the coordinate.
   * @param moment2 Second tuple of moment to compute togeter with the name of the coordinate.
   * @param moment3 Third tuple of moment to compute togeter with the name of the coordinate.
   * @param moment4 Fourth tuple of moment to compute togeter with the name of the coordinate.
   * @param strict  Indicates if strict data handling is required. If so then any non-numeric value fails the
   *                reduction. If not then non-numeric values are silently ignored.
   * @param nan     Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
   *                data).
   */
  def apply[M <: Moment, N <: Moment, O <: Moment, P <: Moment, T, U, V, S](moment1: (M, T), moment2: (N, U),
    moment3: (O, V), moment4: (P, S), strict: Boolean, nan: Boolean)(implicit ev1: Valueable[T], ev2: Valueable[U],
      ev3: Valueable[V], ev4: Valueable[S], ne1: M =!= N, ne2: M =!= O, ne3: M =!= P, ne4: N =!= O, ne5: N =!= P,
      ne6: O =!= P): Reducer with Prepare with PresentMultiple = {
    Moments(strict, nan, List((moment1._1.index, Some(ev1.convert(moment1._2))),
      (moment2._1.index, Some(ev2.convert(moment2._2))), (moment3._1.index, Some(ev3.convert(moment3._2))),
      (moment4._1.index, Some(ev4.convert(moment4._2)))))
  }

  /**
   * Moments of a distribution.
   *
   * @param mean Name of the coordinate of the mean.
   */
  def apply[T](mean: T)(implicit ev1: Valueable[T]): Reducer with Prepare with PresentMultiple = {
    Moments(DefaultStrict, DefaultNaN, List((Mean.index, Some(ev1.convert(mean)))))
  }

  /**
   * Moments of a distribution.
   *
   * @param mean Name of the coordinate of the mean.
   * @param sd   Name of the coordinate of the standard deviation.
   */
  def apply[T, U](mean: T, sd: U)(implicit ev1: Valueable[T],
    ev2: Valueable[U]): Reducer with Prepare with PresentMultiple = {
    Moments(DefaultStrict, DefaultNaN, List((Mean.index, Some(ev1.convert(mean))),
      (StandardDeviation.index, Some(ev2.convert(sd)))))
  }

  /**
   * Moments of a distribution.
   *
   * @param mean     Name of the coordinate of the mean.
   * @param sd       Name of the coordinate of the standard deviation.
   * @param skewness Name of the coordinate of the skewness.
   */
  def apply[T, U, V](mean: T, sd: U, skewness: V)(implicit ev1: Valueable[T], ev2: Valueable[U],
    ev3: Valueable[V]): Reducer with Prepare with PresentMultiple = {
    Moments(DefaultStrict, DefaultNaN, List((Mean.index, Some(ev1.convert(mean))),
      (StandardDeviation.index, Some(ev2.convert(sd))), (Skewness.index, Some(ev3.convert(skewness)))))
  }

  /**
   * Moments of a distribution.
   *
   * @param mean     Name of the coordinate of the mean.
   * @param sd       Name of the coordinate of the standard deviation.
   * @param skewness Name of the coordinate of the skewness.
   * @param kurtosis Name of the coordinate of the kurtosis.
   */
  def apply[T, U, V, W](mean: T, sd: U, skewness: V, kurtosis: W)(implicit ev1: Valueable[T], ev2: Valueable[U],
    ev3: Valueable[V], ev4: Valueable[W]): Reducer with Prepare with PresentMultiple = {
    Moments(DefaultStrict, DefaultNaN, List((Mean.index, Some(ev1.convert(mean))),
      (StandardDeviation.index, Some(ev2.convert(sd))), (Skewness.index, Some(ev3.convert(skewness))),
      (Kurtosis.index, Some(ev4.convert(kurtosis)))))
  }

  /**
   * Moments of a distribution.
   *
   * @param mean   Name of the coordinate of the mean.
   * @param strict Indicates if strict data handling is required. If so then any non-numeric value fails the reduction.
   *               If not then non-numeric values are silently ignored.
   * @param nan    Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
   *               data).
   */
  def apply[T](mean: T, strict: Boolean, nan: Boolean)(
    implicit ev1: Valueable[T]): Reducer with Prepare with PresentMultiple = {
    Moments(strict, nan, List((Mean.index, Some(ev1.convert(mean)))))
  }

  /**
   * Moments of a distribution.
   *
   * @param mean   Name of the coordinate of the mean.
   * @param sd     Name of the coordinate of the standard deviation.
   * @param strict Indicates if strict data handling is required. If so then any non-numeric value fails the reduction.
   *               If not then non-numeric values are silently ignored.
   * @param nan    Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
   *               data).
   */
  def apply[T, U](mean: T, sd: U, strict: Boolean, nan: Boolean)(implicit ev1: Valueable[T],
    ev2: Valueable[U]): Reducer with Prepare with PresentMultiple = {
    Moments(strict, nan, List((Mean.index, Some(ev1.convert(mean))), (StandardDeviation.index, Some(ev2.convert(sd)))))
  }

  /**
   * Moments of a distribution.
   *
   * @param mean     Name of the coordinate of the mean.
   * @param sd       Name of the coordinate of the standard deviation.
   * @param skewness Name of the coordinate of the skewness.
   * @param strict   Indicates if strict data handling is required. If so then any non-numeric value fails the
   *                 reduction. If not then non-numeric values are silently ignored.
   * @param nan      Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
   *                 data).
   */
  def apply[T, U, V](mean: T, sd: U, skewness: V, strict: Boolean, nan: Boolean)(implicit ev1: Valueable[T],
    ev2: Valueable[U], ev3: Valueable[V]): Reducer with Prepare with PresentMultiple = {
    Moments(strict, nan, List((Mean.index, Some(ev1.convert(mean))), (StandardDeviation.index, Some(ev2.convert(sd))),
      (Skewness.index, Some(ev3.convert(skewness)))))
  }

  /**
   * Moments of a distribution.
   *
   * @param mean     Name of the coordinate of the mean.
   * @param sd       Name of the coordinate of the standard deviation.
   * @param skewness Name of the coordinate of the skewness.
   * @param kurtosis Name of the coordinate of the kurtosis.
   * @param strict   Indicates if strict data handling is required. If so then any non-numeric value fails the
   *                 reduction. If not then non-numeric values are silently ignored.
   * @param nan      Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
   *                 data).
   */
  def apply[T, U, V, W](mean: T, sd: U, skewness: V, kurtosis: W, strict: Boolean, nan: Boolean)(
    implicit ev1: Valueable[T], ev2: Valueable[U], ev3: Valueable[V],
    ev4: Valueable[W]): Reducer with Prepare with PresentMultiple = {
    Moments(strict, nan, List((Mean.index, Some(ev1.convert(mean))), (StandardDeviation.index, Some(ev2.convert(sd))),
      (Skewness.index, Some(ev3.convert(skewness))), (Kurtosis.index, Some(ev4.convert(kurtosis)))))
  }
}

/** Base trait for reducers that return a `Double` value. */
trait DoubleReducer extends Reducer with Prepare with PresentSingleAndMultiple with StrictReduce {
  type T = Double

  val ct = ClassTag[Double](Double.getClass)

  def prepare[P <: Position, D <: Dimension](slice: Slice[P, D], cell: Cell[P]): T = {
    cell.content.value.asDouble.getOrElse(Double.NaN)
  }

  /** Indicator if 'NaN' value should be output if the reduction failed (for example due to non-numeric data). */
  val nan: Boolean

  protected def invalid(t: T): Boolean = t.isNaN

  protected def content(t: T): Option[Content] = {
    if (t.isNaN && !nan) { None } else { Some(Content(ContinuousSchema[Codex.DoubleCodex](), t)) }
  }
}

/** Minimum value reduction. */
case class Min private (strict: Boolean, nan: Boolean, name: Option[Value]) extends DoubleReducer {
  protected def reduction(lt: T, rt: T): T = math.min(lt, rt)
}

/** Companion object to `Min` reducer class. */
object Min extends DefaultReducerValues {
  /** Minimum value reduction. */
  def apply(): Reducer with Prepare with PresentSingle = Min(DefaultStrict, DefaultNaN, None)

  /**
   * Minimum value reduction.
   *
   * @param strict Indicates if strict data handling is required. If so then any non-numeric value fails the reduction.
   *               If not then non-numeric values are silently ignored.
   * @param nan    Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
   *               data).
   */
  def apply(strict: Boolean, nan: Boolean): Reducer with Prepare with PresentSingle = Min(strict, nan, None)

  /**
   * Minimum value reduction.
   *
   * @param name Coordinate name of the computed minimum.
   */
  def apply[V](name: V)(implicit ev: Valueable[V]): Reducer with Prepare with PresentMultiple = {
    Min(DefaultStrict, DefaultNaN, Some(ev.convert(name)))
  }

  /**
   * Minimum value reduction.
   *
   * @param name   Coordinate name of the computed minimum.
   * @param strict Indicates if strict data handling is required. If so then any non-numeric value fails the reduction.
   *               If not then non-numeric values are silently ignored.
   * @param nan    Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
   *               data).
   */
  def apply[V](name: V, strict: Boolean, nan: Boolean)(
    implicit ev: Valueable[V]): Reducer with Prepare with PresentMultiple = Min(strict, nan, Some(ev.convert(name)))
}

/** Maximum value reduction. */
case class Max private (strict: Boolean, nan: Boolean, name: Option[Value]) extends DoubleReducer {
  protected def reduction(lt: T, rt: T): T = math.max(lt, rt)
}

/** Companion object to `Max` reducer class. */
object Max extends DefaultReducerValues {
  /** Maximum value reduction. */
  def apply(): Reducer with Prepare with PresentSingle = Max(DefaultStrict, DefaultNaN, None)

  /**
   * Maximum value reduction.
   *
   * @param strict Indicates if strict data handling is required. If so then any non-numeric value fails the reduction.
   *               If not then non-numeric values are silently ignored.
   * @param nan    Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
   *               data).
   */
  def apply(strict: Boolean, nan: Boolean): Reducer with Prepare with PresentSingle = Max(strict, nan, None)

  /**
   * Maximum value reduction.
   *
   * @param name Coordinate name of the computed maximum.
   */
  def apply[V](name: V)(implicit ev: Valueable[V]): Reducer with Prepare with PresentMultiple = {
    Max(DefaultStrict, DefaultNaN, Some(ev.convert(name)))
  }

  /**
   * Maximum value reduction.
   *
   * @param name   Coordinate name of the computed maximum.
   * @param strict Indicates if strict data handling is required. If so then any non-numeric value fails the reduction.
   *               If not then non-numeric values are silently ignored.
   * @param nan    Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
   *               data).
   */
  def apply[V](name: V, strict: Boolean, nan: Boolean)(
    implicit ev: Valueable[V]): Reducer with Prepare with PresentMultiple = Max(strict, nan, Some(ev.convert(name)))
}

/** Maximum absolute value reduction. */
case class MaxAbs private (strict: Boolean, nan: Boolean, name: Option[Value]) extends DoubleReducer {
  protected def reduction(lt: T, rt: T): T = math.max(math.abs(lt), math.abs(rt))
}

/** Companion object to `MaxAbs` reducer class. */
object MaxAbs extends DefaultReducerValues {
  /** Maximum absolute value reduction. */
  def apply(): Reducer with Prepare with PresentSingle = MaxAbs(DefaultStrict, DefaultNaN, None)

  /**
   * Maximum absolute value reduction.
   *
   * @param strict Indicates if strict data handling is required. If so then any non-numeric value fails the reduction.
   *               If not then non-numeric values are silently ignored.
   * @param nan    Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
   *               data).
   */
  def apply(strict: Boolean, nan: Boolean): Reducer with Prepare with PresentSingle = MaxAbs(strict, nan, None)

  /**
   * Maximum absolute value reduction.
   *
   * @param name Coordinate name of the computed absolute maximum.
   */
  def apply[V](name: V)(implicit ev: Valueable[V]): Reducer with Prepare with PresentMultiple = {
    MaxAbs(DefaultStrict, DefaultNaN, Some(ev.convert(name)))
  }

  /**
   * Maximum absolute value reduction.
   *
   * @param name   Coordinate name of the computed absolute maximum.
   * @param strict Indicates if strict data handling is required. If so then any non-numeric value fails the reduction.
   *               If not then non-numeric values are silently ignored.
   * @param nan    Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
   *               data).
   */
  def apply[V](name: V, strict: Boolean, nan: Boolean)(
    implicit ev: Valueable[V]): Reducer with Prepare with PresentMultiple = MaxAbs(strict, nan, Some(ev.convert(name)))
}

/** Sum value reduction. */
case class Sum private (strict: Boolean, nan: Boolean, name: Option[Value]) extends DoubleReducer {
  protected def reduction(lt: T, rt: T): T = lt + rt
}

/** Companion object to `Sum` reducer class. */
object Sum extends DefaultReducerValues {
  /** Sum value reduction. */
  def apply(): Reducer with Prepare with PresentSingle = Sum(DefaultStrict, DefaultNaN, None)

  /**
   * Sum value reduction.
   *
   * @param strict Indicates if strict data handling is required. If so then any non-numeric value fails the reduction.
   *               If not then non-numeric values are silently ignored.
   * @param nan    Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
   *               data).
   */
  def apply(strict: Boolean, nan: Boolean): Reducer with Prepare with PresentSingle = Sum(strict, nan, None)

  /**
   * Sum value reduction.
   *
   * @param name Coordinate name of the computed sum.
   */
  def apply[V](name: V)(implicit ev: Valueable[V]): Reducer with Prepare with PresentMultiple = {
    Sum(DefaultStrict, DefaultNaN, Some(ev.convert(name)))
  }

  /**
   * Sum value reduction.
   *
   * @param name   Coordinate name of the computed sum.
   * @param strict Indicates if strict data handling is required. If so then any non-numeric value fails the reduction.
   *               If not then non-numeric values are silently ignored.
   * @param nan    Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
   *               data).
   */
  def apply[V](name: V, strict: Boolean, nan: Boolean)(
    implicit ev: Valueable[V]): Reducer with Prepare with PresentMultiple = Sum(strict, nan, Some(ev.convert(name)))
}

/** Trait for reducers that require counts of all unique elements. */
trait ElementCounts { self: Reducer with Prepare with StrictReduce =>
  /** Type of the state being reduced (aggregated). */
  type T = Option[Map[String, Long]]

  /** Serialisation ClassTag for state `T`. */
  val ct = ClassTag[Option[Map[String, Long]]](Option.getClass)

  /** Optional variable type that the content must adhere to. */
  val all: Option[Type]

  /**
   * Prepare for reduction.
   *
   * @param slice Encapsulates the dimension(s) over with to reduce.
   * @param cell  Cell which is to be reduced. Note that its position is prior to `slice.selected` being applied.
   *
   * @return State to reduce.
   */
  def prepare[P <: Position, D <: Dimension](slice: Slice[P, D], cell: Cell[P]): T = {
    (all.isEmpty || cell.content.schema.kind.isSpecialisationOf(all.get)) match {
      case true => Some(Map(cell.content.value.toShortString -> 1))
      case false => None
    }
  }

  protected def invalid(t: T): Boolean = t.isEmpty

  protected def reduction(lt: T, rt: T): T = {
    (lt, rt) match {
      case (Some(lm), Some(rm)) => Some(lm ++ rm.map { case (k, v) => k -> (v + lm.getOrElse(k, 0L)) })
      case _ => throw new Exception("None where Some expected")
    }
  }
}

/**
 * Compute histogram.
 *
 * @param strict     Indicates if strict data handling is required. If so then any non-numeric value fails the
 *                   reduction. If not then non-numeric values are silently ignored.
 * @param all        Optional variable type that the content must adhere to.
 * @param frequency  Indicator if categories should be returned as frequency or as distribution.
 * @param statistics List of statistics to compute on the histogram.
 * @param name       Name pattern for the histogram bin names. Use `%[12]$``s` for the string representations of the
 *                   position, and the content.
 * @param separator  The separator used in `pos.toShortString`.
 */
// TODO: Add option to limit maximum number of categories
case class Histogram private (strict: Boolean, all: Option[Type], frequency: Boolean,
  statistics: List[Histogram.Statistic], name: String, separator: String) extends Reducer with Prepare
  with PresentMultiple with StrictReduce with ElementCounts {
  def presentMultiple[P <: Position with ExpandablePosition](pos: P, t: T): Collection[Cell[P#M]] = {
    Collection(t.map {
      case m =>
        val counts = m.values.toList.sorted
        val stats = statistics.map { case s => s.compute(pos, counts) }.flatten
        val vals = (m.map {
          case (k, v) =>
            Cell[P#M](pos.append(name.format(pos.toShortString(separator), k)), frequency match {
              case true => Content(DiscreteSchema[Codex.LongCodex](), v)
              case false => Content(ContinuousSchema[Codex.DoubleCodex](), v.toDouble / counts.sum)
            })
        }).toList

        Right(stats ++ vals)
    })
  }
}

/** Companion object to `Histogram` reducer class. */
object Histogram extends DefaultReducerValues {
  /** Default value for the variable type the content must adhere to. */
  val DefaultAll: Option[Type] = Some(Categorical)

  /** Default separator to use in `pos.toShortString`. */
  val DefaultSeparator: String = "|"

  /** Default value for indicator whether to return frequency or density. */
  val DefaultFrequency: Boolean = true

  /** Trait for computing statistics on a histogram */
  trait Statistic {
    /**
     * Compute statistics on histogram.
     *
     * @param pos    The is the position of the cell.
     * @param counts The the (ordered) counts in each bin.
     *
     * @return An optional cell containing the statistic.
     */
    def compute[P <: Position with ExpandablePosition](pos: P, counts: List[Long]): Option[Cell[P#M]]
  }

  /**
   * Compute histogram.
   *
   * @param name Name pattern for the histogram bin names. Use `%[12]$``s` for the string representations of the
   *             position, and the content.
   */
  def apply(name: String): Reducer with Prepare with PresentMultiple = {
    Histogram(DefaultStrict, DefaultAll, DefaultFrequency, List(), name, DefaultSeparator)
  }

  /**
   * Compute histogram.
   *
   * @param name      Name pattern for the histogram bin names. Use `%[12]$``s` for the string representations of the
   *                  position, and the content.
   * @param separator The separator used in `pos.toShortString`.
   */
  def apply(name: String, separator: String): Reducer with Prepare with PresentMultiple = {
    Histogram(DefaultStrict, DefaultAll, DefaultFrequency, List(), name, separator)
  }

  /**
   * Compute histogram.
   *
   * @param name      Name pattern for the histogram bin names. Use `%[12]$``s` for the string representations of the
   *                  position, and the content.
   * @param strict    Indicates if strict data handling is required. If so then any non-numeric value fails the
   *                  reduction. If not then non-numeric values are silently ignored.
   * @param all       Indicator if histogram should apply to all data, or only to categorical variables.
   * @param frequency Indicator if categories should be returned as frequency or as distribution.
   */
  def apply(name: String, strict: Boolean, all: Boolean,
    frequency: Boolean): Reducer with Prepare with PresentMultiple = {
    Histogram(strict, if (all) None else DefaultAll, frequency, List(), name, DefaultSeparator)
  }

  /**
   * Compute histogram.
   *
   * @param name      Name pattern for the histogram bin names. Use `%[12]$``s` for the string representations of the
   *                  position, and the content.
   * @param strict    Indicates if strict data handling is required. If so then any non-numeric value fails the
   *                  reduction. If not then non-numeric values are silently ignored.
   * @param all       Indicator if histogram should apply to all data, or only to categorical variables.
   * @param frequency Indicator if categories should be returned as frequency or as distribution.
   * @param separator The separator used in `pos.toShortString`.
   */
  def apply(name: String, strict: Boolean, all: Boolean, frequency: Boolean,
    separator: String): Reducer with Prepare with PresentMultiple = {
    Histogram(strict, if (all) None else DefaultAll, frequency, List(), name, separator)
  }

  /**
   * Compute histogram.
   *
   * @param name       Name pattern for the histogram bin names. Use `%[12]$``s` for the string representations of the
   *                   position, and the content.
   * @param statistics List of statistics to compute on the histogram.
   */
  def apply(name: String, statistics: List[Statistic]): Reducer with Prepare with PresentMultiple = {
    Histogram(DefaultStrict, DefaultAll, DefaultFrequency, statistics, name, DefaultSeparator)
  }

  /**
   * Compute histogram.
   *
   * @param name       Name pattern for the histogram bin names. Use `%[12]$``s` for the string representations of the
   *                   position, and the content.
   * @param statistics List of statistics to compute on the histogram.
   * @param separator  The separator used in `pos.toShortString`.
   */
  def apply(name: String, statistics: List[Statistic], separator: String): Reducer with Prepare with PresentMultiple = {
    Histogram(DefaultStrict, DefaultAll, DefaultFrequency, statistics, name, separator)
  }

  /**
   * Compute histogram.
   *
   * @param name       Name pattern for the histogram bin names. Use `%[12]$``s` for the string representations of the
   *                   position, and the content.
   * @param statistics List of statistics to compute on the histogram.
   * @param strict     Indicates if strict data handling is required. If so then any non-numeric value fails the
   *                   reduction. If not then non-numeric values are silently ignored.
   * @param all        Indicator if histogram should apply to all data, or only to categorical variables.
   * @param frequency  Indicator if categories should be returned as frequency or as distribution.
   */
  def apply(name: String, statistics: List[Statistic], strict: Boolean, all: Boolean,
    frequency: Boolean): Reducer with Prepare with PresentMultiple = {
    Histogram(strict, if (all) None else DefaultAll, frequency, statistics, name, DefaultSeparator)
  }

  /**
   * Compute histogram.
   *
   * @param name       Name pattern for the histogram bin names. Use `%[12]$``s` for the string representations of the
   *                   position, and the content.
   * @param statistics List of statistics to compute on the histogram.
   * @param strict     Indicates if strict data handling is required. If so then any non-numeric value fails the
   *                   reduction. If not then non-numeric values are silently ignored.
   * @param all        Indicator if histogram should apply to all data, or only to categorical variables.
   * @param frequency  Indicator if categories should be returned as frequency or as distribution.
   * @param separator  The separator used in `pos.toShortString`.
   */
  def apply(name: String, statistics: List[Statistic], strict: Boolean, all: Boolean, frequency: Boolean,
    separator: String): Reducer with Prepare with PresentMultiple = {
    Histogram(strict, if (all) None else DefaultAll, frequency, statistics, name, separator)
  }

  /**
   * Returns the statistic for computing the number of categories (bins) of a histogram.
   *
   * @param name Name to use for the coordinate of the statistic.
   */
  def numberOfCategories[V](name: V)(implicit ev: Valueable[V]): Statistic = {
    val n = ev.convert(name)

    new Statistic {
      def compute[P <: Position with ExpandablePosition](pos: P, counts: List[Long]) = {
        Some(Cell(pos.append(n), Content(DiscreteSchema[Codex.LongCodex](), counts.size)))
      }
    }
  }

  /**
   * Returns the statistic for computing the entropy of categories (bins) of a histogram.
   *
   * @param name Name to use for the coordinate of the statistic.
   * @param nan  Indicator if 'NaN' string should be output if the reduction failed (for example if only 1 bins is
   *             present).
   */
  def entropy[V](name: V, nan: Boolean = false)(implicit ev: Valueable[V]): Statistic = {
    val n = ev.convert(name)

    new Statistic {
      def compute[P <: Position with ExpandablePosition](pos: P, counts: List[Long]) = {
        Entropy.compute(counts, nan, false, Entropy.DefaultLog()).map { case con => Cell(pos.append(n), con) }
      }
    }
  }

  /**
   * Returns the statistic for computing the frequency ratio (ratio of highest bin count over second highest) of
   * categories (bins) of a histogram. This gives an indication on the skewness of the underlying distribution.
   *
   * @param name Name to use for the coordinate of the statistic.
   * @param nan  Indicator if 'NaN' string should be output if the reduction failed (for example if only 1 bins is
   *             present).
   */
  def frequencyRatio[V](name: V, nan: Boolean = false)(implicit ev: Valueable[V]): Statistic = {
    val n = ev.convert(name)

    new Statistic {
      def compute[P <: Position with ExpandablePosition](pos: P, counts: List[Long]) = {
        ((counts.size > 1, nan) match {
          case (true, _) => Some(Content(ContinuousSchema[Codex.DoubleCodex](),
            counts.last.toDouble / counts(counts.length - 2)))
          case (false, true) => Some(Content(ContinuousSchema[Codex.DoubleCodex](), Double.NaN))
          case (false, false) => None
        }).map { case con => Cell(pos.append(n), con) }
      }
    }
  }
}

/**
 * Compute counts of values less-or-equal or greater than some `threshold`.
 *
 * @param strict    Indicates if strict data handling is required. If so then any non-numeric value fails the
 *                  reduction. If not then non-numeric values are silently ignored.
 * @param nan       Indicator if 'NaN' value (-1) should be output if the reduction failed (for example due to
 *                  non-numeric data).
 * @param threshold The threshold value.
 * @param names     Coordinate names of the counts. Names must be provided when presenting `PresentMultiple`.
 */
case class ThresholdCount private (strict: Boolean, nan: Boolean, threshold: Double, names: List[Value])
  extends Reducer with Prepare with PresentMultiple with StrictReduce {
  type T = (Long, Long) // (leq, gtr)

  val ct = ClassTag[(Long, Long)]((Long, Long).getClass)

  def prepare[P <: Position, D <: Dimension](slice: Slice[P, D], cell: Cell[P]): T = {
    cell.content.value.asDouble match {
      case Some(v) => if (v > threshold) (0, 1) else (1, 0)
      case _ => (-1, -1)
    }
  }

  def presentMultiple[P <: Position with ExpandablePosition](pos: P, t: T): Collection[Cell[P#M]] = {
    Collection(content(t).map { case cl => Right(names.zip(cl).map { case (n, c) => Cell[P#M](pos.append(n), c) }) })
  }

  protected def invalid(t: T): Boolean = t._1 < 0

  protected def reduction(lt: T, rt: T): T = (lt._1 + rt._1, lt._2 + rt._2)

  private def content(t: T): Option[List[Content]] = {
    (t._1 < 0 && !nan) match {
      case true => None
      case false => Some(List(
        Content(DiscreteSchema[Codex.LongCodex](), t._1),
        Content(DiscreteSchema[Codex.LongCodex](), t._2)))
    }
  }
}

/** Companion object to `ThresholdCount` reducer class. */
object ThresholdCount extends DefaultReducerValues {
  /** Default threshold value. */
  val DefaultThreshold: Double = 0

  /**
   * Compute counts of values less-or-equal or greater-than some `threshold`.
   *
   * @param leq Name for the coordinate of the less-or-equal value.
   * @param gtr Name for the coordinate of the greater-than value.
   */
  def apply[T, U](leq: T, gtr: U)(implicit ev1: Valueable[T],
    ev2: Valueable[U]): Reducer with Prepare with PresentMultiple = {
    ThresholdCount(DefaultStrict, DefaultNaN, DefaultThreshold, List(ev1.convert(leq), ev2.convert(gtr)))
  }

  /**
   * Compute counts of values less-or-equal or greater-than some `threshold`.
   *
   * @param leq    Name for the coordinate of the less-or-equal value.
   * @param gtr    Name for the coordinate of the greater-than value.
   * @param strict Indicates if strict data handling is required. If so then any non-numeric value fails the reduction.
   *               If not then non-numeric values are silently ignored.
   * @param nan    Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
   *               data).
   */
  def apply[T, U](leq: T, gtr: U, strict: Boolean, nan: Boolean)(implicit ev1: Valueable[T],
    ev2: Valueable[U]): Reducer with Prepare with PresentMultiple = {
    ThresholdCount(strict, nan, DefaultThreshold, List(ev1.convert(leq), ev2.convert(gtr)))
  }

  /**
   * Compute counts of values less-or-equal or greater-than some `threshold`.
   *
   * @param leq       Name for the coordinate of the less-or-equal value.
   * @param gtr       Name for the coordinate of the greater-than value.
   * @param threshold The threshold value.
   */
  def apply[T, U](leq: T, gtr: U, threshold: Double)(implicit ev1: Valueable[T],
    ev2: Valueable[U]): Reducer with Prepare with PresentMultiple = {
    ThresholdCount(DefaultStrict, DefaultNaN, threshold, List(ev1.convert(leq), ev2.convert(gtr)))
  }

  /**
   * Compute counts of values less-or-equal or greater-than some `threshold`.
   *
   * @param leq       Name for the coordinate of the less-or-equal value.
   * @param gtr       Name for the coordinate of the greater-than value.
   * @param strict    Indicates if strict data handling is required. If so then any non-numeric value fails the
   *                  reduction. If not then non-numeric values are silently ignored.
   * @param nan       Indicator if 'NaN' string should be output if the reduction failed (for example due to
   *                  non-numeric data).
   * @param threshold The threshold value.
   */
  def apply[T, U](leq: T, gtr: U, strict: Boolean, nan: Boolean, threshold: Double)(implicit ev1: Valueable[T],
    ev2: Valueable[U]): Reducer with Prepare with PresentMultiple = {
    ThresholdCount(strict, nan, threshold, List(ev1.convert(leq), ev2.convert(gtr)))
  }
}

/**
 * Weighted sum reduction. This is particularly useful for scoring linear models.
 *
 * @param dim    Dimension for for which to create weigthed variables.
 * @param strict Indicates if strict data handling is required. If so then any non-numeric value fails the reduction.
 *               If not then non-numeric values are silently ignored.
 * @param nan    Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
 *               data).
 * @param format Format for converting dimension to string, this allows multiple models to be scored simultaneuously.
 *               Use `%1$``s` for the string representation of the coordinate.
 * @param name   Coordinate name of the computed weighted sum.
 *
 * @note The `Position1D` in the `type V` must all be `StringValue` if a format is used.
 */
case class WeightedSum private (dim: Dimension, strict: Boolean, nan: Boolean, format: Option[String],
  name: Option[Value]) extends Reducer
  with PrepareWithValue with PresentSingleAndMultiple with StrictReduce {
  type T = Double

  val ct = ClassTag[Double](Double.getClass)

  type V = Map[Position1D, Content]

  def prepare[P <: Position, D <: Dimension](slice: Slice[P, D], cell: Cell[P], ext: V): T = {
    val key = format match {
      case Some(fmt) => Position1D(fmt.format(cell.position(dim).toShortString))
      case None => Position1D(cell.position(dim))
    }

    (cell.content.schema.kind.isSpecialisationOf(Numerical), cell.content.value.asDouble,
      ext.get(key).flatMap(_.value.asDouble)) match {
        case (false, _, _) => Double.NaN
        case (true, None, _) => Double.NaN
        case (true, Some(_), None) => 0
        case (true, Some(v), Some(w)) => v * w
      }
  }

  protected def invalid(t: T): Boolean = t.isNaN

  protected def reduction(lt: T, rt: T): T = lt + rt

  protected def content(t: T): Option[Content] = {
    if (t.isNaN && !nan) { None } else { Some(Content(ContinuousSchema[Codex.DoubleCodex](), t)) }
  }
}

/** Companion object to `WeightedSum` reducer class. */
object WeightedSum extends DefaultReducerValues {
  /**
   * Weighted sum reduction. This is particularly useful for scoring linear models.
   *
   * @param dim Dimension for for which to create weigthed variables.
   */
  def apply(dim: Dimension): Reducer with PrepareWithValue with PresentSingle { type V = WeightedSum#V } = {
    WeightedSum(dim, DefaultStrict, DefaultNaN, None, None)
  }

  /**
   * Weighted sum reduction. This is particularly useful for scoring linear models.
   *
   * @param dim    Dimension for for which to create weigthed variables.
   * @param strict Indicates if strict data handling is required. If so then any non-numeric value fails the reduction.
   *               If not then non-numeric values are silently ignored.
   * @param nan    Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
   *               data).
   */
  def apply(dim: Dimension, strict: Boolean,
    nan: Boolean): Reducer with PrepareWithValue with PresentSingle { type V = WeightedSum#V } = {
    WeightedSum(dim, strict, nan, None, None)
  }

  /**
   * Weighted sum reduction. This is particularly useful for scoring linear models.
   *
   * @param dim  Dimension for for which to create weigthed variables.
   * @param name Coordinate name of the computed weighted sum.
   */
  def apply[V](dim: Dimension, name: V)(
    implicit ev: Valueable[V]): Reducer with PrepareWithValue with PresentMultiple { type V = WeightedSum#V } = {
    WeightedSum(dim, DefaultStrict, DefaultNaN, None, Some(ev.convert(name)))
  }

  /**
   * Weighted sum reduction. This is particularly useful for scoring linear models.
   *
   * @param dim    Dimension for for which to create weigthed variables.
   * @param name   Coordinate name of the computed weighted sum.
   * @param strict Indicates if strict data handling is required. If so then any non-numeric value fails the reduction.
   *               If not then non-numeric values are silently ignored.
   * @param nan    Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
   *               data).
   */
  def apply[V](dim: Dimension, name: V, strict: Boolean, nan: Boolean)(
    implicit ev: Valueable[V]): Reducer with PrepareWithValue with PresentMultiple { type V = WeightedSum#V } = {
    WeightedSum(dim, strict, nan, None, Some(ev.convert(name)))
  }

  /**
   * Weighted sum reduction. This is particularly useful for scoring linear models.
   *
   * @param dim    Dimension for for which to create weigthed variables.
   * @param name   Coordinate name of the computed weighted sum.
   * @param format Format for converting dimension to string, this allows multiple models to be scored simultaneuously.
   *               Use `%1$``s` for the string representation of the coordinate.
   *
   * @note The `Position1D` in the `type V` must all be `StringValue`.
   */
  def apply[V](dim: Dimension, name: V, format: String)(
    implicit ev: Valueable[V]): Reducer with PrepareWithValue with PresentMultiple { type V = WeightedSum#V } = {
    WeightedSum(dim, DefaultStrict, DefaultNaN, Some(format), Some(ev.convert(name)))
  }

  /**
   * Weighted sum reduction. This is particularly useful for scoring linear models.
   *
   * @param dim    Dimension for for which to create weigthed variables.
   * @param name   Coordinate name of the computed weighted sum.
   * @param format Format for converting dimension to string, this allows multiple models to be scored simultaneuously.
   *               Use `%1$``s` for the string representation of the coordinate.
   * @param strict Indicates if strict data handling is required. If so then any non-numeric value fails the reduction.
   *               If not then non-numeric values are silently ignored.
   * @param nan    Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
   *               data).
   *
   * @note The `Position1D` in the `type V` must all be `StringValue`.
   */
  def apply[V](dim: Dimension, name: V, format: String, strict: Boolean, nan: Boolean)(
    implicit ev: Valueable[V]): Reducer with PrepareWithValue with PresentMultiple { type V = WeightedSum#V } = {
    WeightedSum(dim, strict, nan, Some(format), Some(ev.convert(name)))
  }
}

/**
 * Distinct count reductions.
 *
 * @param name Optional coordinate name for the count value. Name must be provided when presenting `PresentMultiple`.
 */
case class DistinctCount private (name: Option[Value]) extends Reducer with Prepare with PresentSingleAndMultiple {
  type T = Set[Value]

  val ct = ClassTag[Set[Value]](Set.getClass)

  def prepare[P <: Position, D <: Dimension](slice: Slice[P, D], cell: Cell[P]): T = Set(cell.content.value)

  def reduce(lt: T, rt: T): T = lt ++ rt

  protected def content(t: T): Option[Content] = Some(Content(DiscreteSchema[Codex.LongCodex](), t.size))
}

/** Companion object to `DistinctCount` reducer class. */
object DistinctCount {
  /** Distinct count reductions. */
  def apply(): Reducer with Prepare with PresentSingle = DistinctCount(None)

  /**
   * Distinct count reductions.
   *
   * @param name Coordinate name for the count values.
   */
  def apply[V](name: V)(implicit ev: Valueable[V]): Reducer with Prepare with PresentMultiple = {
    DistinctCount(Some(ev.convert(name)))
  }
}

/**
 * Compute quantiles using nearest rank method.
 *
 * @param quantiles Number of quantiles to compute.
 * @param strict    Indicates if strict data handling is required. If so then any non-numeric value fails the
 *                  reduction. If not then non-numeric values are silently ignored.
 * @param name      Name format for the quantiles. Usage of a `%1$``d` in the name will be substituded with percentile
 *                  index, while `%2$``f` will be substituted by the percentage.
 */
case class Quantiles(quantiles: Int, strict: Boolean = true, name: String = "quantile.%1$d=%2$2.3f") extends Reducer
  with Prepare with PresentMultiple with StrictReduce {
  type T = Option[Map[Double, Long]]

  val ct = ClassTag[Option[Map[Double, Long]]](Option.getClass)

  def prepare[P <: Position, D <: Dimension](slice: Slice[P, D], cell: Cell[P]): T = {
    cell.content.schema.kind.isSpecialisationOf(Numerical) match {
      case true =>
        Some(new scala.collection.immutable.TreeMap[Double, Long]() + (cell.content.value.asDouble.get -> 1))
      case false => None
    }
  }

  def presentMultiple[P <: Position with ExpandablePosition](pos: P, t: T): Collection[Cell[P#M]] = {
    Collection[Cell[P#M]](t match {
      case Some(m) =>
        val keys = m.keys.toList
        val cumsum = m.values.tail.scanLeft(m.values.head)(_ + _).toList
        val N = cumsum.last.toDouble
        val boundaries = for (cnt <- (0.0 until N by (N / quantiles)).tail) yield {
          (cnt / N, keys(cumsum.indexWhere(_ >= cnt)))
        }

        Some(Right((boundaries.zipWithIndex.map {
          case ((perc, quant), idx) =>
            Cell[P#M](pos.append(name.format(idx + 1, perc)), Content(ContinuousSchema[Codex.DoubleCodex](), quant))
        }).toList))
      case None => Option.empty[Either[Cell[P#M], List[Cell[P#M]]]]
    })
  }

  protected def invalid(t: T): Boolean = t.isEmpty

  protected def reduction(lt: T, rt: T): T = {
    (lt, rt) match {
      case (Some(lm), Some(rm)) => Some(lm ++ rm.map { case (k, v) => k -> (v + lm.getOrElse(k, 0L)) })
      case _ => throw new Exception("None where Some expected")
    }
  }
}

/**
 * Compute entropy.
 *
 * @param strict     Indicates if strict data handling is required. If so then any non-numeric value fails the
 *                   reduction. If not then non-numeric values are silently ignored.
 * @param nan        Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
 *                   data).
 * @param all        Optional variable type that the content must adhere to.
 * @param negate     Indicator if negative entropy should be returned.
 * @param name       Optional coordinate name for the entropy value. Name must be provided when presenting
 *                   `PresentMultiple`.
 * @param log        The log function to use.
 */
case class Entropy private (strict: Boolean, nan: Boolean, all: Option[Type], negate: Boolean, name: Option[Value],
  log: (Double) => Double) extends Reducer with Prepare with PresentSingleAndMultiple with StrictReduce
  with ElementCounts {

  protected def content(t: T): Option[Content] = {
    if (t.isEmpty && !nan) {
      None
    } else if (t.isEmpty) {
      Some(Content(ContinuousSchema[Codex.DoubleCodex](), Double.NaN))
    } else {
      t.flatMap { case m => Entropy.compute(m.values.toList.sorted, nan, negate, log) }
    }
  }
}

/** Companion object to `Entropy` reducer class. */
object Entropy extends DefaultReducerValues {
  /** Default value for negative entropy. */
  val DefaultNegate: Boolean = false

  /** Default value for variable type. */
  val DefaultAll: Option[Type] = Some(Categorical)

  /** Default logarithm (base 2). */
  def DefaultLog(): (Double) => Double = (x: Double) => math.log(x) / math.log(2)

  /**
   * Compute entropy from value counts.
   *
   * @param counts     Counts of each of a variable's values.
   * @param nan        Indicator if 'NaN' string should be output if the reduction failed (for example if only a
   *                   single category is present).
   * @param negate     Indicator if negative entropy should be returned.
   * @param log        The log function to use.
   */
  def compute(counts: List[Long], nan: Boolean, negate: Boolean, log: (Double) => Double): Option[Content] = {
    (counts.size > 1, nan) match {
      case (true, _) =>
        val entropy = -counts.map {
          case cnt =>
            val f = cnt.toDouble / counts.sum
            f * log(f)
        }.sum

        Some(Content(ContinuousSchema[Codex.DoubleCodex](), if (negate) -entropy else entropy))
      case (false, true) => Some(Content(ContinuousSchema[Codex.DoubleCodex](), Double.NaN))
      case (false, false) => None
    }
  }

  /** Compute entropy. */
  def apply(): Reducer with Prepare with PresentSingle = {
    Entropy(DefaultStrict, DefaultNaN, DefaultAll, DefaultNegate, None, DefaultLog())
  }

  /**
   * Compute entropy.
   *
   * @param strict Indicates if strict data handling is required. If so then any non-numeric value fails the reduction.
   *               If not then non-numeric values are silently ignored.
   * @param nan    Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
   *               data).
   * @param all    Indicator if histogram should apply to all data, or only to categorical variables.
   * @param negate Indicator if negative entropy should be returned.
   */
  def apply(strict: Boolean, nan: Boolean, all: Boolean, negate: Boolean): Reducer with Prepare with PresentSingle = {
    Entropy(strict, nan, if (all) None else DefaultAll, negate, None, DefaultLog())
  }

  /**
   * Compute entropy.
   *
   * @param log The log function to use.
   */
  def apply(log: (Double) => Double): Reducer with Prepare with PresentSingle = {
    Entropy(DefaultStrict, DefaultNaN, DefaultAll, DefaultNegate, None, log)
  }

  /**
   * Compute entropy.
   *
   * @param strict Indicates if strict data handling is required. If so then any non-numeric value fails the reduction.
   *               If not then non-numeric values are silently ignored.
   * @param nan    Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
   *               data).
   * @param all    Indicator if histogram should apply to all data, or only to categorical variables.
   * @param negate Indicator if negative entropy should be returned.
   * @param log    The log function to use.
   */
  def apply(strict: Boolean, nan: Boolean, all: Boolean, negate: Boolean,
    log: (Double) => Double): Reducer with Prepare with PresentSingle = {
    Entropy(strict, nan, if (all) None else DefaultAll, negate, None, log)
  }

  /**
   * Compute entropy.
   *
   * @param name Coordinate name for the entropy value. Name must be provided when presenting `PresentMultiple`.
   */
  def apply[V](name: V)(implicit ev: Valueable[V]): Reducer with Prepare with PresentMultiple = {
    Entropy(DefaultStrict, DefaultNaN, DefaultAll, DefaultNegate, Some(ev.convert(name)), DefaultLog)
  }

  /**
   * Compute entropy.
   *
   * @param name   Coordinate name for the entropy value. Name must be provided when presenting `PresentMultiple`.
   * @param strict Indicates if strict data handling is required. If so then any non-numeric value fails the reduction.
   *               If not then non-numeric values are silently ignored.
   * @param nan    Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
   *               data).
   * @param all    Indicator if histogram should apply to all data, or only to categorical variables.
   * @param negate Indicator if negative entropy should be returned.
   */
  def apply[V](name: V, strict: Boolean, nan: Boolean, all: Boolean, negate: Boolean)(
    implicit ev: Valueable[V]): Reducer with Prepare with PresentMultiple = {
    Entropy(strict, nan, if (all) None else DefaultAll, negate, Some(ev.convert(name)), DefaultLog)
  }

  /**
   * Compute entropy.
   *
   * @param name Coordinate name for the entropy value. Name must be provided when presenting `PresentMultiple`.
   * @param log  The log function to use.
   */
  def apply[V](name: V, log: (Double) => Double)(
    implicit ev: Valueable[V]): Reducer with Prepare with PresentMultiple = {
    Entropy(DefaultStrict, DefaultNaN, DefaultAll, DefaultNegate, Some(ev.convert(name)), log)
  }

  /**
   * Compute entropy.
   *
   * @param name   Coordinate name for the entropy value. Name must be provided when presenting `PresentMultiple`.
   * @param strict Indicates if strict data handling is required. If so then any non-numeric value fails the reduction.
   *               If not then non-numeric values are silently ignored.
   * @param nan    Indicator if 'NaN' string should be output if the reduction failed (for example due to non-numeric
   *               data).
   * @param all    Indicator if histogram should apply to all data, or only to categorical variables.
   * @param negate Indicator if negative entropy should be returned.
   * @param log    The log function to use.
   */
  def apply[V](name: V, strict: Boolean, nan: Boolean, all: Boolean, negate: Boolean, log: (Double) => Double)(
    implicit ev: Valueable[V]): Reducer with Prepare with PresentMultiple = {
    Entropy(strict, nan, if (all) None else DefaultAll, negate, Some(ev.convert(name)), log)
  }
}

