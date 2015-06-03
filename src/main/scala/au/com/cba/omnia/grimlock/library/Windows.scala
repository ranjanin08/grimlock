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

  /** Name pattern for renaming `rem` coordinate. */
  val name: Option[String]

  protected def getCollection(sel: S, coord: Value, curr: Double,
    prev: Option[(Value, Double)]): Collection[Cell[S#M]] = {
    val cell = Cell[S#M](sel.append(getCoordinate(coord)), Content(ContinuousSchema[Codex.DoubleCodex](), curr))

    prev match {
      case None => Collection(cell)
      case Some((c, v)) => Collection(List(Cell[S#M](sel.append(getCoordinate(c)),
        Content(ContinuousSchema[Codex.DoubleCodex](), v)), cell))
    }
  }

  protected def getDouble(con: Content): Double = con.value.asDouble.getOrElse(Double.NaN)

  private def getCoordinate(coord: Value): Value = {
    name.map { case n => StringValue(n.format(coord.toShortString)) }.getOrElse(coord)
  }
}

/**
 * Trait for computing moving average in batch mode; that is, keep the last N values and compute the moving average
 * from it.
 */
trait BatchMovingAverage[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition]
  extends MovingAverage[S, R] {
  type T = (List[(Value, Double)], Option[(Value, Double)])

  /** Size of the window. */
  val window: Int

  /** The dimension in `rem` from which to get the coordinate to append to `sel`. */
  val dim: Dimension

  /** Indicates if averages should be output when a full window isn't available yet. */
  val all: Boolean

  protected val idx: Int

  def initialise(cell: Cell[S], rem: R): T = {
    val curr = getCurrent(rem, cell.content)

    (List(curr), if (all) { Some(curr) } else { None })
  }

  def present(cell: Cell[S], rem: R, t: T): (T, Collection[Cell[S#M]]) = {
    val lst = updateList(rem, cell.content, t._1)
    val out = (all || lst.size == window) match {
      case true => getCollection(cell.position, lst(math.min(idx, lst.size - 1))._1, compute(lst), t._2)
      case false => Collection[Cell[S#M]]()
    }

    ((lst, None), out)
  }

  private def getCurrent(rem: R, con: Content): (Value, Double) = (rem(dim), getDouble(con))

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
 * @param name   Pattern for the new name of the appended coordinate. Use `%1$``s` for the string representations of
 *               the coordinate.
 */
case class SimpleMovingAverage[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition] private (
  window: Int, dim: Dimension, all: Boolean, name: Option[String]) extends BatchMovingAverage[S, R] {
  protected val idx = window - 1

  protected def compute(lst: List[(Value, Double)]): Double = lst.foldLeft(0.0)((c, p) => p._2 + c) / lst.size
}

/** Companion object to the `SimpleMovingAverage` class defining constructors. */
object SimpleMovingAverage {
  /** Default dimension (into `rem`) if none given. */
  val DefaultDimension: Dimension = First

  /** Default indicator if all averages should be output (even when ful window isn't available yet). */
  val DefaultAll: Boolean = false

  /**
   * Compute simple moving average over last `window` values.
   *
   * @param window Size of the window.
   */
  def apply[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition](
    window: Int): SimpleMovingAverage[S, R] = SimpleMovingAverage[S, R](window, DefaultDimension, DefaultAll, None)

  /**
   * Compute simple moving average over last `window` values.
   *
   * @param window Size of the window.
   * @param dim    The dimension in `rem` from which to get the coordinate to append to `sel`.
   */
  def apply[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition](window: Int,
    dim: Dimension): SimpleMovingAverage[S, R] = SimpleMovingAverage[S, R](window, dim, DefaultAll, None)

  /**
   * Compute simple moving average over last `window` values.
   *
   * @param window Size of the window.
   * @param all    Indicates if averages should be output when a full window isn't available yet.
   */
  def apply[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition](window: Int,
    all: Boolean): SimpleMovingAverage[S, R] = SimpleMovingAverage[S, R](window, DefaultDimension, all, None)

  /**
   * Compute simple moving average over last `window` values.
   *
   * @param window Size of the window.
   * @param name   Pattern for the new name of the appended coordinate. Use `%1$``s` for the string representations of
   *               the coordinate.
   */
  def apply[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition](window: Int,
    name: String): SimpleMovingAverage[S, R] = {
    SimpleMovingAverage[S, R](window, DefaultDimension, DefaultAll, Some(name))
  }

  /**
   * Compute simple moving average over last `window` values.
   *
   * @param window Size of the window.
   * @param dim    The dimension in `rem` from which to get the coordinate to append to `sel`.
   * @param all    Indicates if averages should be output when a full window isn't available yet.
   */
  def apply[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition](window: Int, dim: Dimension,
    all: Boolean): SimpleMovingAverage[S, R] = SimpleMovingAverage[S, R](window, dim, all, None)

  /**
   * Compute simple moving average over last `window` values.
   *
   * @param window Size of the window.
   * @param dim    The dimension in `rem` from which to get the coordinate to append to `sel`.
   * @param name   Pattern for the new name of the appended coordinate. Use `%1$``s` for the string representations of
   *               the coordinate.
   */
  def apply[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition](window: Int, dim: Dimension,
    name: String): SimpleMovingAverage[S, R] = SimpleMovingAverage[S, R](window, dim, DefaultAll, Some(name))

  /**
   * Compute simple moving average over last `window` values.
   *
   * @param window Size of the window.
   * @param all    Indicates if averages should be output when a full window isn't available yet.
   * @param name   Pattern for the new name of the appended coordinate. Use `%1$``s` for the string representations of
   *               the coordinate.
   */
  def apply[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition](window: Int, all: Boolean,
    name: String): SimpleMovingAverage[S, R] = SimpleMovingAverage[S, R](window, DefaultDimension, all, Some(name))

  /**
   * Compute simple moving average over last `window` values.
   *
   * @param window Size of the window.
   * @param dim    The dimension in `rem` from which to get the coordinate to append to `sel`.
   * @param all    Indicates if averages should be output when a full window isn't available yet.
   * @param name   Pattern for the new name of the appended coordinate. Use `%1$``s` for the string representations of
   *               the coordinate.
   */
  def apply[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition](window: Int, dim: Dimension,
    all: Boolean, name: String): SimpleMovingAverage[S, R] = SimpleMovingAverage[S, R](window, dim, all, Some(name))
}

/**
 * Compute centered moving average over last `2 * width + 1` values.
 *
 * @param width Number of values before and after a given value to use when computing the moving average.
 * @param dim   The dimension in `rem` from which to get the coordinate to append to `sel`.
 * @param name  Pattern for the new name of the appended coordinate. Use `%1$``s` for the string representations of
 *              the coordinate.
 */
case class CenteredMovingAverage[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition] private (
  width: Int, dim: Dimension, name: Option[String]) extends BatchMovingAverage[S, R] {
  val window = 2 * width + 1
  val all = false
  protected val idx = width

  protected def compute(lst: List[(Value, Double)]): Double = lst.foldLeft(0.0)((c, p) => p._2 + c) / lst.size
}

/** Companion object to the `CenteredMovingAverage` class defining constructors. */
object CenteredMovingAverage {
  /** Default dimension (into `rem`) if none given. */
  val DefaultDimension: Dimension = First

  /**
   * Compute centered moving average over last `2 * width + 1` values.
   *
   * @param width Number of values before and after a given value to use when computing the moving average.
   */
  def apply[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition](
    width: Int): CenteredMovingAverage[S, R] = CenteredMovingAverage[S, R](width, DefaultDimension, None)

  /**
   * Compute centered moving average over last `2 * width + 1` values.
   *
   * @param width Number of values before and after a given value to use when computing the moving average.
   * @param dim   The dimension in `rem` from which to get the coordinate to append to `sel`.
   */
  def apply[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition](width: Int,
    dim: Dimension): CenteredMovingAverage[S, R] = CenteredMovingAverage[S, R](width, dim, None)

  /**
   * Compute centered moving average over last `2 * width + 1` values.
   *
   * @param width Number of values before and after a given value to use when computing the moving average.
   * @param name  Pattern for the new name of the appended coordinate. Use `%1$``s` for the string representations of
   *              the coordinate.
   */
  def apply[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition](width: Int,
    name: String): CenteredMovingAverage[S, R] = CenteredMovingAverage[S, R](width, DefaultDimension, Some(name))

  /**
   * Compute centered moving average over last `2 * width + 1` values.
   *
   * @param width Number of values before and after a given value to use when computing the moving average.
   * @param dim   The dimension in `rem` from which to get the coordinate to append to `sel`.
   * @param name  Pattern for the new name of the appended coordinate. Use `%1$``s` for the string representations of
   *              the coordinate.
   */
  def apply[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition](width: Int, dim: Dimension,
    name: String): CenteredMovingAverage[S, R] = CenteredMovingAverage[S, R](width, dim, Some(name))
}

/**
 * Compute weighted moving average over last `window` values.
 *
 * @param window Size of the window.
 * @param dim    The dimension in `rem` from which to get the coordinate to append to `sel`.
 * @param all    Indicates if averages should be output when a full window isn't available yet.
 * @param name   Pattern for the new name of the appended coordinate. Use `%1$``s` for the string representations of
 *               the coordinate.
 */
case class WeightedMovingAverage[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition] private (
  window: Int, dim: Dimension, all: Boolean, name: Option[String]) extends BatchMovingAverage[S, R] {
  protected val idx = window - 1

  protected def compute(lst: List[(Value, Double)]): Double = {
    val curr = lst.zipWithIndex.foldLeft((0.0, 0))((c, p) => ((p._2 + 1) * p._1._2 + c._1, c._2 + p._2 + 1))

    curr._1 / curr._2
  }
}

/** Companion object to the `WeightedMovingAverage` class defining constructors. */
object WeightedMovingAverage {
  /** Default dimension (into `rem`) if none given. */
  val DefaultDimension: Dimension = First

  /** Default indicator if all averages should be output (even when ful window isn't available yet). */
  val DefaultAll: Boolean = false

  /**
   * Compute weighted moving average over last `window` values.
   *
   * @param window Size of the window.
   */
  def apply[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition](
    window: Int): WeightedMovingAverage[S, R] = WeightedMovingAverage[S, R](window, DefaultDimension, DefaultAll, None)

  /**
   * Compute weighted moving average over last `window` values.
   *
   * @param window Size of the window.
   * @param dim    The dimension in `rem` from which to get the coordinate to append to `sel`.
   */
  def apply[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition](window: Int,
    dim: Dimension): WeightedMovingAverage[S, R] = WeightedMovingAverage[S, R](window, dim, DefaultAll, None)

  /**
   * Compute weighted moving average over last `window` values.
   *
   * @param window Size of the window.
   * @param all    Indicates if averages should be output when a full window isn't available yet.
   */
  def apply[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition](window: Int,
    all: Boolean): WeightedMovingAverage[S, R] = WeightedMovingAverage[S, R](window, DefaultDimension, all, None)

  /**
   * Compute weighted moving average over last `window` values.
   *
   * @param window Size of the window.
   * @param name   Pattern for the new name of the appended coordinate. Use `%1$``s` for the string representations of
   *               the coordinate.
   */
  def apply[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition](window: Int,
    name: String): WeightedMovingAverage[S, R] = {
    WeightedMovingAverage[S, R](window, DefaultDimension, DefaultAll, Some(name))
  }

  /**
   * Compute weighted moving average over last `window` values.
   *
   * @param window Size of the window.
   * @param dim    The dimension in `rem` from which to get the coordinate to append to `sel`.
   * @param all    Indicates if averages should be output when a full window isn't available yet.
   */
  def apply[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition](window: Int, dim: Dimension,
    all: Boolean): WeightedMovingAverage[S, R] = WeightedMovingAverage[S, R](window, dim, all, None)

  /**
   * Compute weighted moving average over last `window` values.
   *
   * @param window Size of the window.
   * @param dim    The dimension in `rem` from which to get the coordinate to append to `sel`.
   * @param name   Pattern for the new name of the appended coordinate. Use `%1$``s` for the string representations of
   *               the coordinate.
   */
  def apply[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition](window: Int, dim: Dimension,
    name: String): WeightedMovingAverage[S, R] = WeightedMovingAverage[S, R](window, dim, DefaultAll, Some(name))

  /**
   * Compute weighted moving average over last `window` values.
   *
   * @param window Size of the window.
   * @param all    Indicates if averages should be output when a full window isn't available yet.
   * @param name   Pattern for the new name of the appended coordinate. Use `%1$``s` for the string representations of
   *               the coordinate.
   */
  def apply[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition](window: Int, all: Boolean,
    name: String): WeightedMovingAverage[S, R] = WeightedMovingAverage[S, R](window, DefaultDimension, all, Some(name))

  /**
   * Compute weighted moving average over last `window` values.
   *
   * @param window Size of the window.
   * @param dim    The dimension in `rem` from which to get the coordinate to append to `sel`.
   * @param all    Indicates if averages should be output when a full window isn't available yet.
   * @param name   Pattern for the new name of the appended coordinate. Use `%1$``s` for the string representations of
   *               the coordinate.
   */
  def apply[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition](window: Int, dim: Dimension,
    all: Boolean, name: String): WeightedMovingAverage[S, R] = {
    WeightedMovingAverage[S, R](window, dim, all, Some(name))
  }
}

/** Trait for computing moving average in online mode. */
trait OnlineMovingAverage[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition]
  extends MovingAverage[S, R] {
  type T = (Double, Long, Option[(Value, Double)])

  /** The dimension in `rem` from which to get the coordinate to append to `sel`. */
  val dim: Dimension

  /** Name pattern for renaming `rem` coordinate. */
  val name: Option[String]

  def initialise(cell: Cell[S], rem: R): T = (getDouble(cell.content), 1, Some((rem(dim), getDouble(cell.content))))

  def present(cell: Cell[S], rem: R, t: T): (T, Collection[Cell[S#M]]) = {
    val curr = compute(getDouble(cell.content), t)

    ((curr, t._2 + 1, None), getCollection(cell.position, rem(dim), curr, t._3))
  }

  protected def compute(curr: Double, t: T): Double
}

/**
 * Compute cumulatve moving average.
 *
 * @param dim  The dimension in `rem` from which to get the coordinate to append to `sel`.
 * @param name Pattern for the new name of the appended coordinate. Use `%1$``s` for the string representations of
 *             the coordinate.
 */
case class CumulativeMovingAverage[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition] private (dim: Dimension, name: Option[String]) extends OnlineMovingAverage[S, R] {
  protected def compute(curr: Double, t: T): Double = (curr + t._2 * t._1) / (t._2 + 1)
}

/** Companion object to the `CumulativeMovingAverage` class defining constructors. */
object CumulativeMovingAverage {
  /** Default dimension (into `rem`) if none given. */
  val DefaultDimension: Dimension = First

  /** Compute cumulatve moving average. */
  def apply[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition](): CumulativeMovingAverage[S, R] = CumulativeMovingAverage[S, R](DefaultDimension, None)

  /**
   * Compute cumulatve moving average.
   *
   * @param dim The dimension in `rem` from which to get the coordinate to append to `sel`.
   */
  def apply[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition](
    dim: Dimension): CumulativeMovingAverage[S, R] = CumulativeMovingAverage[S, R](dim, None)

  /**
   * Compute cumulatve moving average.
   *
   * @param name Pattern for the new name of the appended coordinate. Use `%1$``s` for the string representations of
   *             the coordinate.
   */
  def apply[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition](
    name: String): CumulativeMovingAverage[S, R] = CumulativeMovingAverage[S, R](DefaultDimension, Some(name))

  /**
   * Compute cumulatve moving average.
   *
   * @param dim  The dimension in `rem` from which to get the coordinate to append to `sel`.
   * @param name Pattern for the new name of the appended coordinate. Use `%1$``s` for the string representations of
   *             the coordinate.
   */
  def apply[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition](dim: Dimension,
    name: String): CumulativeMovingAverage[S, R] = CumulativeMovingAverage[S, R](dim, Some(name))
}

/**
 * Compute exponential moving average.
 *
 * @param alpha Degree of weighting coefficient.
 * @param dim   The dimension in `rem` from which to get the coordinate to append to `sel`.
 * @param name  Pattern for the new name of the appended coordinate. Use `%1$``s` for the string representations of
 *              the coordinate.
 */
case class ExponentialMovingAverage[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition] private (alpha: Double, dim: Dimension, name: Option[String]) extends OnlineMovingAverage[S, R] {
  protected def compute(curr: Double, t: T): Double = alpha * curr + (1 - alpha) * t._1
}

/** Companion object to the `ExponentialMovingAverage` class defining constructors. */
object ExponentialMovingAverage {
  /** Default dimension (into `rem`) if none given. */
  val DefaultDimension: Dimension = First

  /**
   * Compute exponential moving average.
   *
   * @param alpha Degree of weighting coefficient.
   */
  def apply[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition](
    alpha: Double): ExponentialMovingAverage[S, R] = ExponentialMovingAverage[S, R](alpha, DefaultDimension, None)

  /**
   * Compute exponential moving average.
   *
   * @param alpha Degree of weighting coefficient.
   * @param dim   The dimension in `rem` from which to get the coordinate to append to `sel`.
   */
  def apply[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition](alpha: Double,
    dim: Dimension): ExponentialMovingAverage[S, R] = ExponentialMovingAverage[S, R](alpha, dim, None)

  /**
   * Compute exponential moving average.
   *
   * @param alpha Degree of weighting coefficient.
   * @param name  Pattern for the new name of the appended coordinate. Use `%1$``s` for the string representations of
   *              the coordinate.
   */
  def apply[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition](alpha: Double,
    name: String): ExponentialMovingAverage[S, R] = ExponentialMovingAverage[S, R](alpha, DefaultDimension, Some(name))

  /**
   * Compute exponential moving average.
   *
   * @param alpha Degree of weighting coefficient.
   * @param dim   The dimension in `rem` from which to get the coordinate to append to `sel`.
   * @param name  Pattern for the new name of the appended coordinate. Use `%1$``s` for the string representations of
   *              the coordinate.
   */
  def apply[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition](alpha: Double,
    dim: Dimension, name: String): ExponentialMovingAverage[S, R] = {
    ExponentialMovingAverage[S, R](alpha, dim, Some(name))
  }
}

// TODO: test, document and add appropriate constructors
case class CumulativeSum[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition](
  separator: String = "|") extends Window[S, R, S#M] {
  type T = (Option[Double], Option[String])

  def initialise(cell: Cell[S], rem: R): T = (cell.content.value.asDouble, Some(rem.toShortString(separator)))

  def present(cell: Cell[S], rem: R, t: T): (T, Collection[Cell[S#M]]) = {
    val coord = rem.toShortString(separator)
    val schema = ContinuousSchema[Codex.DoubleCodex]()

    (t, cell.content.value.asDouble) match {
      case ((None, _), None) =>
        ((None, None), Collection())
      case ((None, _), Some(d)) =>
        ((Some(d), None), Collection(cell.position.append(coord), Content(schema, d)))
      case ((Some(p), None), None) =>
        ((Some(p), None), Collection())
      case ((Some(p), None), Some(d)) =>
        ((Some(p + d), None), Collection(cell.position.append(coord), Content(schema, p + d)))
      case ((Some(p), Some(c)), None) =>
        ((Some(p), None), Collection(cell.position.append(c), Content(schema, p)))
      case ((Some(p), Some(c)), Some(d)) =>
        ((Some(p + d), None), Collection(List(Cell[S#M](cell.position.append(c), Content(schema, p)),
          Cell[S#M](cell.position.append(coord), Content(schema, p + d)))))
    }
  }
}

// TODO: test, document and add appropriate constructors
case class Sliding[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition](
  f: (Double, Double) => Double, name: String = "f(%1$s, %2$s)", separator: String = "|") extends Window[S, R, S#M] {
  type T = (Option[Double], String)

  def initialise(cell: Cell[S], rem: R): T = (cell.content.value.asDouble, rem.toShortString(separator))

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
        ((Some(d), coord), Collection(cell.position.append(name.format(c, coord)), Content(schema, f(p, d))))
    }
  }
}

