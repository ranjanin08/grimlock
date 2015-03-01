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

package au.com.cba.omnia.grimlock.derive

import au.com.cba.omnia.grimlock._
import au.com.cba.omnia.grimlock.content._
import au.com.cba.omnia.grimlock.content.metadata._
import au.com.cba.omnia.grimlock.encoding._
import au.com.cba.omnia.grimlock.position._
import au.com.cba.omnia.grimlock.utility._

/** Base trait for computing a moving average. */
trait MovingAverage { self: Deriver =>
  protected def getCollection[P <: Position, D <: Dimension](slice: Slice[P, D])(sel: slice.S, coord: Value,
    curr: Double, prev: Option[(Value, Double)]): Collection[Cell[slice.S#M]] = {
    val cell = Cell[slice.S#M](sel.append(coord), Content(ContinuousSchema[Codex.DoubleCodex](), curr))

    prev match {
      case None => Collection(cell)
      case Some((c, v)) => Collection(List(Cell[slice.S#M](sel.append(c),
        Content(ContinuousSchema[Codex.DoubleCodex](), v)), cell))
    }
  }

  protected def getDouble(con: Content): Double = con.value.asDouble.getOrElse(Double.NaN)
}

/**
 * Trait for computing moving average in batch mode; that is, keep the last N values and compute the moving average
 * from it.
 */
trait BatchMovingAverage extends Deriver with Initialise with MovingAverage {
  type T = (List[(Value, Double)], Option[(Value, Double)])

  /** Size of the window. */
  val window: Int

  /** The dimension in `rem` from which to get the coordinate to append to `sel`. */
  val dim: Dimension

  /** Indicates if averages should be output when a full window isn't available yet. */
  val all: Boolean

  protected val idx: Int

  def initialise[P <: Position, D <: Dimension](slice: Slice[P, D])(cell: Cell[slice.S], rem: slice.R): T = {
    val curr = getCurrent(rem, cell.content)

    (List(curr), if (all) { Some(curr) } else { None })
  }

  def present[P <: Position, D <: Dimension](slice: Slice[P, D])(cell: Cell[slice.S], rem: slice.R,
    t: T): (T, Collection[Cell[slice.S#M]]) = {
    val lst = updateList(rem, cell.content, t._1)
    val out = (all || lst.size == window) match {
      case true => getCollection(slice)(cell.position, lst(math.min(idx, lst.size - 1))._1, compute(lst), t._2)
      case false => Collection[Cell[slice.S#M]]()
    }

    ((lst, None), out)
  }

  private def getCurrent[P <: Position, D <: Dimension](rem: Slice[P, D]#R, con: Content): (Value, Double) = {
    (rem.get(dim), getDouble(con))
  }

  private def updateList[P <: Position, D <: Dimension](rem: Slice[P, D]#R, con: Content,
    lst: List[(Value, Double)]): List[(Value, Double)] = {
    (if (lst.size == window) { lst.drop(1) } else { lst }) :+ getCurrent(rem, con)
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
case class SimpleMovingAverage(window: Int, dim: Dimension = First, all: Boolean = false) extends BatchMovingAverage {
  protected val idx = window - 1

  protected def compute(lst: List[(Value, Double)]): Double = lst.foldLeft(0.0)((c, p) => p._2 + c) / lst.size
}

/**
 * Compute centered moving average over last `2 * width + 1` values.
 *
 * @param width Number of values before and after a given value to use when computing the moving average.
 * @param dim   The dimension in `rem` from which to get the coordinate to append to `sel`.
 */
case class CenteredMovingAverage(width: Int, dim: Dimension = First) extends BatchMovingAverage {
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
case class WeightedMovingAverage(window: Int, dim: Dimension = First, all: Boolean = false) extends BatchMovingAverage {
  protected val idx = window - 1

  protected def compute(lst: List[(Value, Double)]): Double = {
    val curr = lst.zipWithIndex.foldLeft((0.0, 0))((c, p) => ((p._2 + 1) * p._1._2 + c._1, c._2 + p._2 + 1))

    curr._1 / curr._2
  }
}

/** Trait for computing moving average in online mode. */
trait OnlineMovingAverage extends Deriver with Initialise with MovingAverage {
  type T = (Double, Long, Option[(Value, Double)])

  /** The dimension in `rem` from which to get the coordinate to append to `sel`. */
  val dim: Dimension

  def initialise[P <: Position, D <: Dimension](slice: Slice[P, D])(cell: Cell[slice.S], rem: slice.R): T = {
    (getDouble(cell.content), 1, Some((rem.get(dim), getDouble(cell.content))))
  }

  def present[P <: Position, D <: Dimension](slice: Slice[P, D])(cell: Cell[slice.S], rem: slice.R,
    t: T): (T, Collection[Cell[slice.S#M]]) = {
    val curr = compute(getDouble(cell.content), t)

    ((curr, t._2 + 1, None), getCollection(slice)(cell.position, rem.get(dim), curr, t._3))
  }

  protected def compute(curr: Double, t: T): Double
}

/**
 * Compute cumulatve moving average.
 *
 * @param dim The dimension in `rem` from which to get the coordinate to append to `sel`.
 */
case class CumulativeMovingAverage(dim: Dimension = First) extends OnlineMovingAverage {
  protected def compute(curr: Double, t: T): Double = (curr + t._2 * t._1) / (t._2 + 1)
}

/**
 * Compute exponential moving average.
 *
 * @param alpha Degree of weighting coefficient.
 * @param dim   The dimension in `rem` from which to get the coordinate to append to `sel`.
 */
case class ExponentialMovingAverage(alpha: Double, dim: Dimension = First) extends OnlineMovingAverage {
  protected def compute(curr: Double, t: T): Double = alpha * curr + (1 - alpha) * t._1
}

