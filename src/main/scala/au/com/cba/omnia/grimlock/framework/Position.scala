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

package au.com.cba.omnia.grimlock.framework.position

import au.com.cba.omnia.grimlock.framework._
import au.com.cba.omnia.grimlock.framework.content._
import au.com.cba.omnia.grimlock.framework.encoding._
import au.com.cba.omnia.grimlock.framework.utility._

import scala.reflect.ClassTag
import scala.util.matching.Regex

/** Base trait for dealing with positions. */
trait Position {
  /** List of coordinates of the position. */
  val coordinates: List[Value]

  /**
   * Return the coordinate at dimension (index) `dim`.
   *
   * @param dim Dimension of the coordinate to get.
   */
  def apply(dim: Dimension): Value = coordinates(getIndex(dim))

  /**
   * Update the coordinate at `dim` with `t`.
   *
   * @param dim The dimension to set.
   * @param t   The coordinate to set.
   *
   * @return A position of the same size as `this` but with `t` set at index `dim`.
   */
  def update[T](dim: Dimension, t: T)(implicit ev: Valueable[T]): this.type = {
    same(coordinates.updated(getIndex(dim), ev.convert(t))).asInstanceOf[this.type]
  }

  /**
   * Converts the position to a consise (terse) string.
   *
   * @param separator The separator to use between the coordinates.
   *
   * @return Short string representation.
   */
  def toShortString(separator: String): String = coordinates.map(_.toShortString).mkString(separator)

  /**
   * Compare this object with another position.
   *
   * @param that Position to compare against.
   *
   * @return x < 0 iff this < that, x = 0 iff this = that, x > 0 iff this > that.
   *
   * @note If the comparison is between two positions with different dimensions, then a comparison on the number of
   *       dimensions is performed.
   */
  def compare(that: Position): Int = {
    (coordinates.length == that.coordinates.length) match {
      case true =>
        val cmp = coordinates.zip(that.coordinates).map { case (m, t) => Value.Ordering.compare(m, t) }

        cmp.indexWhere(_ != 0) match {
          case idx if (idx < 0) => 0
          case idx => cmp(idx)
        }
      case false => coordinates.length.compare(that.coordinates.length)
    }
  }

  /** Type for positions of same number of dimensions. */
  protected type S <: Position

  protected def same(cl: List[Value]): S

  protected def getIndex(dim: Dimension): Int = if (dim.index < 0) coordinates.length - 1 else dim.index
}

/** Trait for capturing the dependency between a position and its expansion. */
trait PosExpDep[A <: Position, B <: Position] extends java.io.Serializable

object Position {
  /** Define an ordering between 2 position. Only use with position of the same type coordinates. */
  def Ordering[T <: Position](ascending: Boolean = true): Ordering[T] = {
    new Ordering[T] { def compare(x: T, y: T): Int = x.compare(y) * (if (ascending) { 1 } else { -1 }) }
  }

  /**
   * Return function that returns a string representation of a position.
   *
   * @param separator   The separator to use between various fields.
   * @param descriptive Indicator if descriptive string is required or not.
   */
  def toString[P <: Position](separator: String = "|",
    descriptive: Boolean = false): (P) => TraversableOnce[String] = {
    (t: P) => if (descriptive) { Some(t.toString) } else { Some(t.toShortString(separator)) }
  }

  /** `MapablePosition` object for `PositionND` (N > 1) with `Along`. */
  case object MapAlong extends MapMapablePosition[Position1D] {}

  /** Define dependency between expansion from `Position0D` to `Position1D`. */
  implicit object P0P1 extends PosExpDep[Position0D, Position1D]
  /** Define dependency between expansion from `Position0D` to `Position2D`. */
  implicit object P0P2 extends PosExpDep[Position0D, Position2D]
  /** Define dependency between expansion from `Position0D` to `Position3D`. */
  implicit object P0P3 extends PosExpDep[Position0D, Position3D]
  /** Define dependency between expansion from `Position0D` to `Position4D`. */
  implicit object P0P4 extends PosExpDep[Position0D, Position4D]
  /** Define dependency between expansion from `Position0D` to `Position5D`. */
  implicit object P0P5 extends PosExpDep[Position0D, Position5D]
  /** Define dependency between expansion from `Position0D` to `Position6D`. */
  implicit object P0P6 extends PosExpDep[Position0D, Position6D]
  /** Define dependency between expansion from `Position0D` to `Position7D`. */
  implicit object P0P7 extends PosExpDep[Position0D, Position7D]
  /** Define dependency between expansion from `Position0D` to `Position8D`. */
  implicit object P0P8 extends PosExpDep[Position0D, Position8D]
  /** Define dependency between expansion from `Position0D` to `Position9D`. */
  implicit object P0P9 extends PosExpDep[Position0D, Position9D]

  /** Define dependency between expansion from `Position1D` to `Position2D`. */
  implicit object P1P2 extends PosExpDep[Position1D, Position2D]
  /** Define dependency between expansion from `Position1D` to `Position3D`. */
  implicit object P1P3 extends PosExpDep[Position1D, Position3D]
  /** Define dependency between expansion from `Position1D` to `Position4D`. */
  implicit object P1P4 extends PosExpDep[Position1D, Position4D]
  /** Define dependency between expansion from `Position1D` to `Position5D`. */
  implicit object P1P5 extends PosExpDep[Position1D, Position5D]
  /** Define dependency between expansion from `Position1D` to `Position6D`. */
  implicit object P1P6 extends PosExpDep[Position1D, Position6D]
  /** Define dependency between expansion from `Position1D` to `Position7D`. */
  implicit object P1P7 extends PosExpDep[Position1D, Position7D]
  /** Define dependency between expansion from `Position1D` to `Position8D`. */
  implicit object P1P8 extends PosExpDep[Position1D, Position8D]
  /** Define dependency between expansion from `Position1D` to `Position9D`. */
  implicit object P1P9 extends PosExpDep[Position1D, Position9D]

  /** Define dependency between expansion from `Position2D` to `Position3D`. */
  implicit object P2P3 extends PosExpDep[Position2D, Position3D]
  /** Define dependency between expansion from `Position2D` to `Position4D`. */
  implicit object P2P4 extends PosExpDep[Position2D, Position4D]
  /** Define dependency between expansion from `Position2D` to `Position5D`. */
  implicit object P2P5 extends PosExpDep[Position2D, Position5D]
  /** Define dependency between expansion from `Position2D` to `Position6D`. */
  implicit object P2P6 extends PosExpDep[Position2D, Position6D]
  /** Define dependency between expansion from `Position2D` to `Position7D`. */
  implicit object P2P7 extends PosExpDep[Position2D, Position7D]
  /** Define dependency between expansion from `Position2D` to `Position8D`. */
  implicit object P2P8 extends PosExpDep[Position2D, Position8D]
  /** Define dependency between expansion from `Position2D` to `Position9D`. */
  implicit object P2P9 extends PosExpDep[Position2D, Position9D]

  /** Define dependency between expansion from `Position3D` to `Position4D`. */
  implicit object P3P4 extends PosExpDep[Position3D, Position4D]
  /** Define dependency between expansion from `Position3D` to `Position5D`. */
  implicit object P3P5 extends PosExpDep[Position3D, Position5D]
  /** Define dependency between expansion from `Position3D` to `Position6D`. */
  implicit object P3P6 extends PosExpDep[Position3D, Position6D]
  /** Define dependency between expansion from `Position3D` to `Position7D`. */
  implicit object P3P7 extends PosExpDep[Position3D, Position7D]
  /** Define dependency between expansion from `Position3D` to `Position8D`. */
  implicit object P3P8 extends PosExpDep[Position3D, Position8D]
  /** Define dependency between expansion from `Position3D` to `Position9D`. */
  implicit object P3P9 extends PosExpDep[Position3D, Position9D]

  /** Define dependency between expansion from `Position4D` to `Position5D`. */
  implicit object P4P5 extends PosExpDep[Position4D, Position5D]
  /** Define dependency between expansion from `Position4D` to `Position6D`. */
  implicit object P4P6 extends PosExpDep[Position4D, Position6D]
  /** Define dependency between expansion from `Position4D` to `Position7D`. */
  implicit object P4P7 extends PosExpDep[Position4D, Position7D]
  /** Define dependency between expansion from `Position4D` to `Position8D`. */
  implicit object P4P8 extends PosExpDep[Position4D, Position8D]
  /** Define dependency between expansion from `Position4D` to `Position9D`. */
  implicit object P4P9 extends PosExpDep[Position4D, Position9D]

  /** Define dependency between expansion from `Position5D` to `Position6D`. */
  implicit object P5P6 extends PosExpDep[Position5D, Position6D]
  /** Define dependency between expansion from `Position5D` to `Position7D`. */
  implicit object P5P7 extends PosExpDep[Position5D, Position7D]
  /** Define dependency between expansion from `Position5D` to `Position8D`. */
  implicit object P5P8 extends PosExpDep[Position5D, Position8D]
  /** Define dependency between expansion from `Position5D` to `Position9D`. */
  implicit object P5P9 extends PosExpDep[Position5D, Position9D]

  /** Define dependency between expansion from `Position6D` to `Position7D`. */
  implicit object P6P7 extends PosExpDep[Position6D, Position7D]
  /** Define dependency between expansion from `Position6D` to `Position8D`. */
  implicit object P6P8 extends PosExpDep[Position6D, Position8D]
  /** Define dependency between expansion from `Position6D` to `Position9D`. */
  implicit object P6P9 extends PosExpDep[Position6D, Position9D]

  /** Define dependency between expansion from `Position7D` to `Position8D`. */
  implicit object P7P8 extends PosExpDep[Position7D, Position8D]
  /** Define dependency between expansion from `Position7D` to `Position9D`. */
  implicit object P7P9 extends PosExpDep[Position7D, Position9D]

  /** Define dependency between expansion from `Position8D` to `Position9D`. */
  implicit object P8P9 extends PosExpDep[Position8D, Position9D]

  /** Define dependency between an expandable position and its expansion. */
  implicit def PPM[P <: Position with ExpandablePosition] = new PosExpDep[P, P#M] {}
}

/** Trait for operations that reduce a position by one dimension. */
trait ReduceablePosition { self: Position =>
  /** Type for positions of one less number of dimensions. */
  type L <: Position with ExpandablePosition

  /**
   * Remove the coordinate at dimension `dim`.
   *
   * @param dim The dimension to remove.
   *
   * @return A new position with dimension `dim` removed.
   */
  def remove(dim: Dimension): L = {
    val (h, t) = coordinates.splitAt(getIndex(dim))

    less(h ++ t.tail)
  }

  /**
   * Melt dimension `dim` into `into`.
   *
   * @param dim       The dimension to remove.
   * @param into      The dimension into which to melt.
   * @param separator The separator to use in the new coordinate name.
   *
   * @return A new position with dimension `dim` removed. The coordinate at `unto` will be a string value consisting of
   *         the string representations of the coordinates `dim` and `unto` separated by `separator`.
   *
   * @note `dim` and `into` must not be the same.
   */
  def melt(dim: Dimension, into: Dimension, separator: String): L = {
    val iidx = getIndex(into)
    val didx = getIndex(dim)

    less(coordinates
      .updated(iidx, StringValue(coordinates(iidx).toShortString + separator + coordinates(didx).toShortString))
      .zipWithIndex
      .filter(_._2 != didx)
      .map(_._1))
  }

  protected def less(cl: List[Value]): L
}

/** Trait for operations that expand a position by one dimension. */
trait ExpandablePosition { self: Position =>
  /** Type for positions of one more number of dimensions. */
  type M <: Position with ReduceablePosition

  /**
   * Prepend a coordinate to the position.
   *
   * @param t The coordinate to prepend.
   *
   * @return A new position with the coordinate `t` prepended.
   */
  def prepend[T](t: T)(implicit ev: Valueable[T]): M = more(ev.convert(t) +: coordinates)

  /**
   * Append a coordinate to the position.
   *
   * @param t The coordinate to append.
   *
   * @return A new position with the coordinate `t` appended.
   */
  def append[T](t: T)(implicit ev: Valueable[T]): M = more(coordinates :+ ev.convert(t))

  protected def more(cl: List[Value]): M
}

/** Trait for operations that modify a position (but keep the number of dimensions the same). */
trait PermutablePosition { self: Position =>
  /**
   * Permute the order of coordinates.
   *
   * @param order The new ordering of the coordinates.
   *
   * @return A position of the same size as `this` but with the coordinates ordered according to `order`.
   *
   * @note The ordering must contain each dimension exactly once.
   */
  def permute(order: List[Dimension]): this.type = {
    same(order.map { case d => coordinates(getIndex(d)) }).asInstanceOf[this.type]
  }
}

/** Base trait for converting a position to a `Map`. */
trait MapablePosition[P <: Position, T] {
  /**
   * Convert a cell to `Map` content value.
   *
   * @param pos Position of the cell.
   * @param con Content of the cell.
   *
   * @return The value placed in a `Map` after a call to `toMap` on a `Slice`.
   */
  def toMapValue(pos: P, con: Content): T

  /**
   * Combine two map values.
   *
   * @param x An optional `Map` content value.
   * @param y The `Map` content value to combine with.
   *
   * @return The combined `Map` content value.
   */
  def combineMapValues(x: Option[T], y: T): T
}

/** Trait for mapping over a position. */
trait MapOverPosition { self: Position with ReduceablePosition =>
  /** Type of the `Map` content when `Over.toMap` is called. */
  type O

  /** Object with `MapablePosition` implementation to `Over.toMap` and `Over.combineMaps`. */
  val over: MapablePosition[L, O]
}

/** Trait for mapping along a position. */
trait MapAlongPosition { self: Position =>
  /** Type of the `Map` content when `Along.toMap` is called. */
  type A

  /** Object with `MapablePosition` implementation to `Along.toMap` and `Along.combineMaps`. */
  val along: MapablePosition[Position1D, A]
}

/** Trait for converting (2..N)D positions to `Map` values. */
private[position] trait MapMapablePosition[P <: Position] extends MapablePosition[P, Map[P, Content]] {
  def toMapValue(r: P, c: Content): Map[P, Content] = Map(r -> c)
  def combineMapValues(x: Option[Map[P, Content]], y: Map[P, Content]): Map[P, Content] = x.map(_ ++ y).getOrElse(y)
}

/** `MapablePosition` object for `Position1D` with `Over`. */
private[position] case object P1MapOver extends MapablePosition[Position0D, Content] {
  def toMapValue(r: Position0D, c: Content): Content = c
  def combineMapValues(x: Option[Content], y: Content): Content = y
}

/** `MapablePosition` object for `Position2D` with `Over`. */
private[position] case object P2MapOver extends MapMapablePosition[Position1D] {}

/** `MapablePosition` object for `Position3D` with `Over`. */
private[position] case object P3MapOver extends MapMapablePosition[Position2D] {}

/** `MapablePosition` object for `Position4D` with `Over`. */
private[position] case object P4MapOver extends MapMapablePosition[Position3D] {}

/** `MapablePosition` object for `Position5D` with `Over`. */
private[position] case object P5MapOver extends MapMapablePosition[Position4D] {}

/** `MapablePosition` object for `Position6D` with `Over`. */
private[position] case object P6MapOver extends MapMapablePosition[Position5D] {}

/** `MapablePosition` object for `Position7D` with `Over`. */
private[position] case object P7MapOver extends MapMapablePosition[Position6D] {}

/** `MapablePosition` object for `Position8D` with `Over`. */
private[position] case object P8MapOver extends MapMapablePosition[Position7D] {}

/** `MapablePosition` object for `Position9D` with `Over`. */
private[position] case object P9MapOver extends MapMapablePosition[Position8D] {}

/** `MapablePosition` object for `Position1D` with `Along`. */
private[position] case object P1MapAlong extends MapablePosition[Position1D, Content] {
  def toMapValue(r: Position1D, c: Content): Content = throw new Exception("Can't map along 1D")
  def combineMapValues(x: Option[Content], y: Content): Content = throw new Exception("Can't map along 1D")
}

/** `MapablePosition` object for `PositionND` (N > 1) with `Along`. */
private[position] case object PMapAlong extends MapMapablePosition[Position1D] {}

/**
 * Position for zero dimensions.
 *
 * @note Position0D exists so things like `names(Over(First))` work.
 */
case class Position0D() extends Position with ExpandablePosition {
  type M = Position1D

  val coordinates = List()

  protected type S = Position0D

  protected def same(cl: List[Value]): S = Position0D()
  protected def more(cl: List[Value]): M = Position1D(cl(0))
}

/**
 * Position for 1 dimensional data.
 *
 * @param first Coordinate for the first dimension.
 */
case class Position1D(first: Value) extends Position with ReduceablePosition with ExpandablePosition
  with MapOverPosition with MapAlongPosition {
  type L = Position0D
  type M = Position2D
  type O = Content
  type A = Content

  val over = P1MapOver
  val along = P1MapAlong
  val coordinates = List(first)

  protected type S = Position1D

  protected def less(cl: List[Value]): L = Position0D()
  protected def same(cl: List[Value]): S = Position1D(cl(0))
  protected def more(cl: List[Value]): M = Position2D(cl(0), cl(1))
}

/** Companion object to `Position1D`. */
object Position1D {
  /**
   * Construct a `Position1D` from a type `S` for which there exist a coordinate.
   *
   * @param s The coordinate value from which to create a `Position1D`.
   */
  def apply[S](s: S)(implicit ev1: Valueable[S]): Position1D = Position1D(ev1.convert(s))
}

/**
 * Position for 2 dimensional data.
 *
 * @param first  Coordinate for the first dimension.
 * @param second Coordinate for the second dimension.
 */
case class Position2D(first: Value, second: Value) extends Position with ReduceablePosition with ExpandablePosition
  with PermutablePosition with MapOverPosition with MapAlongPosition {
  type L = Position1D
  type M = Position3D
  type O = Map[Position1D, Content]
  type A = Map[Position1D, Content]

  val over = P2MapOver
  val along = PMapAlong
  val coordinates = List(first, second)

  protected type S = Position2D

  protected def less(cl: List[Value]): L = Position1D(cl(0))
  protected def same(cl: List[Value]): S = Position2D(cl(0), cl(1))
  protected def more(cl: List[Value]): M = Position3D(cl(0), cl(1), cl(2))
}

/** Companion object to `Position2D`. */
object Position2D {
  /**
   * Construct a `Position2D` from types `S` and `T` for which there exist coordinates.
   *
   * @param s The first coordinate value from which to create a `Position2D`.
   * @param t The second coordinate value from which to create a `Position2D`.
   */
  def apply[S, T](s: S, t: T)(implicit ev1: Valueable[S], ev2: Valueable[T]): Position2D = {
    Position2D(ev1.convert(s), ev2.convert(t))
  }
}

/**
 * Position for 3 dimensional data.
 *
 * @param first  Coordinate for the first dimension.
 * @param second Coordinate for the second dimension.
 * @param third  Coordinate for the third dimension.
 */
case class Position3D(first: Value, second: Value, third: Value) extends Position with ReduceablePosition
  with ExpandablePosition with PermutablePosition with MapOverPosition with MapAlongPosition {
  type L = Position2D
  type M = Position4D
  type O = Map[Position2D, Content]
  type A = Map[Position1D, Content]

  val over = P3MapOver
  val along = PMapAlong
  val coordinates = List(first, second, third)

  protected type S = Position3D

  protected def less(cl: List[Value]): L = Position2D(cl(0), cl(1))
  protected def same(cl: List[Value]): S = Position3D(cl(0), cl(1), cl(2))
  protected def more(cl: List[Value]): M = Position4D(cl(0), cl(1), cl(2), cl(3))
}

/** Companion object to `Position3D`. */
object Position3D {
  /**
   * Construct a `Position3D` from types `S`, `T` and `U` for which there exist coordinates.
   *
   * @param s The first coordinate value from which to create a `Position3D`.
   * @param t The second coordinate value from which to create a `Position3D`.
   * @param u The third coordinate value from which to create a `Position3D`.
   */
  def apply[S, T, U](s: S, t: T, u: U)(implicit ev1: Valueable[S], ev2: Valueable[T], ev3: Valueable[U]): Position3D = {
    Position3D(ev1.convert(s), ev2.convert(t), ev3.convert(u))
  }
}

/**
 * Position for 4 dimensional data.
 *
 * @param first  Coordinate for the first dimension.
 * @param second Coordinate for the second dimension.
 * @param third  Coordinate for the third dimension.
 * @param fourth Coordinate for the fourth dimension.
 */
case class Position4D(first: Value, second: Value, third: Value, fourth: Value) extends Position
  with ReduceablePosition with ExpandablePosition with PermutablePosition with MapOverPosition with MapAlongPosition {
  type L = Position3D
  type M = Position5D
  type O = Map[Position3D, Content]
  type A = Map[Position1D, Content]

  val over = P4MapOver
  val along = PMapAlong
  val coordinates = List(first, second, third, fourth)

  protected type S = Position4D

  protected def less(cl: List[Value]): L = Position3D(cl(0), cl(1), cl(2))
  protected def same(cl: List[Value]): S = Position4D(cl(0), cl(1), cl(2), cl(3))
  protected def more(cl: List[Value]): M = Position5D(cl(0), cl(1), cl(2), cl(3), cl(4))
}

/** Companion object to `Position4D`. */
object Position4D {
  /**
   * Construct a `Position4D` from types `S`, `T`, `U` and `V` for which there exist coordinates.
   *
   * @param s The first coordinate value from which to create a `Position4D`.
   * @param t The second coordinate value from which to create a `Position4D`.
   * @param u The third coordinate value from which to create a `Position4D`.
   * @param v The fourth coordinate value from which to create a `Position4D`.
   */
  def apply[S, T, U, V](s: S, t: T, u: U, v: V)(implicit ev1: Valueable[S], ev2: Valueable[T], ev3: Valueable[U],
    ev4: Valueable[V]): Position4D = {
    Position4D(ev1.convert(s), ev2.convert(t), ev3.convert(u), ev4.convert(v))
  }
}

/**
 * Position for 5 dimensional data.
 *
 * @param first  Coordinate for the first dimension.
 * @param second Coordinate for the second dimension.
 * @param third  Coordinate for the third dimension.
 * @param fourth Coordinate for the fourth dimension.
 * @param fifth  Coordinate for the fifth dimension.
 */
case class Position5D(first: Value, second: Value, third: Value, fourth: Value, fifth: Value) extends Position
  with ReduceablePosition with ExpandablePosition with PermutablePosition with MapOverPosition with MapAlongPosition {
  type L = Position4D
  type M = Position6D
  type O = Map[Position4D, Content]
  type A = Map[Position1D, Content]

  val over = P5MapOver
  val along = PMapAlong
  val coordinates = List(first, second, third, fourth, fifth)

  protected type S = Position5D

  protected def less(cl: List[Value]): L = Position4D(cl(0), cl(1), cl(2), cl(3))
  protected def same(cl: List[Value]): S = Position5D(cl(0), cl(1), cl(2), cl(3), cl(4))
  protected def more(cl: List[Value]): M = Position6D(cl(0), cl(1), cl(2), cl(3), cl(4), cl(5))
}

/** Companion object to `Position5D`. */
object Position5D {
  /**
   * Construct a `Position5D` from types `S`, `T`, `U`, `V` and `W` for which there exist coordinates.
   *
   * @param s The first coordinate value from which to create a `Position5D`.
   * @param t The second coordinate value from which to create a `Position5D`.
   * @param u The third coordinate value from which to create a `Position5D`.
   * @param v The fourth coordinate value from which to create a `Position5D`.
   * @param w The fifth coordinate value from which to create a `Position5D`.
   */
  def apply[S, T, U, V, W](s: S, t: T, u: U, v: V, w: W)(implicit ev1: Valueable[S], ev2: Valueable[T],
    ev3: Valueable[U], ev4: Valueable[V], ev5: Valueable[W]): Position5D = {
    Position5D(ev1.convert(s), ev2.convert(t), ev3.convert(u), ev4.convert(v), ev5.convert(w))
  }
}

/**
 * Position for 6 dimensional data.
 *
 * @param first  Coordinate for the first dimension.
 * @param second Coordinate for the second dimension.
 * @param third  Coordinate for the third dimension.
 * @param fourth Coordinate for the fourth dimension.
 * @param fifth  Coordinate for the fifth dimension.
 * @param sixth  Coordinate for the sixth dimension.
 */
case class Position6D(first: Value, second: Value, third: Value, fourth: Value, fifth: Value,
  sixth: Value) extends Position with ReduceablePosition with ExpandablePosition with PermutablePosition
  with MapOverPosition with MapAlongPosition {
  type L = Position5D
  type M = Position7D
  type O = Map[Position5D, Content]
  type A = Map[Position1D, Content]

  val over = P6MapOver
  val along = PMapAlong
  val coordinates = List(first, second, third, fourth, fifth, sixth)

  protected type S = Position6D

  protected def less(cl: List[Value]): L = Position5D(cl(0), cl(1), cl(2), cl(3), cl(4))
  protected def same(cl: List[Value]): S = Position6D(cl(0), cl(1), cl(2), cl(3), cl(4), cl(5))
  protected def more(cl: List[Value]): M = Position7D(cl(0), cl(1), cl(2), cl(3), cl(4), cl(5), cl(6))
}

/** Companion object to `Position6D`. */
object Position6D {
  /**
   * Construct a `Position6D` from types `S`, `T`, `U`, `V`, `W` and `X` for which there exist coordinates.
   *
   * @param s The first coordinate value from which to create a `Position6D`.
   * @param t The second coordinate value from which to create a `Position6D`.
   * @param u The third coordinate value from which to create a `Position6D`.
   * @param v The fourth coordinate value from which to create a `Position6D`.
   * @param w The fifth coordinate value from which to create a `Position6D`.
   * @param x The sixth coordinate value from which to create a `Position6D`.
   */
  def apply[S, T, U, V, W, X](s: S, t: T, u: U, v: V, w: W, x: X)(implicit ev1: Valueable[S], ev2: Valueable[T],
    ev3: Valueable[U], ev4: Valueable[V], ev5: Valueable[W], ev6: Valueable[X]): Position6D = {
    Position6D(ev1.convert(s), ev2.convert(t), ev3.convert(u), ev4.convert(v), ev5.convert(w), ev6.convert(x))
  }
}

/**
 * Position for 7 dimensional data.
 *
 * @param first   Coordinate for the first dimension.
 * @param second  Coordinate for the second dimension.
 * @param third   Coordinate for the third dimension.
 * @param fourth  Coordinate for the fourth dimension.
 * @param fifth   Coordinate for the fifth dimension.
 * @param sixth   Coordinate for the sixth dimension.
 * @param seventh Coordinate for the seventh dimension.
 */
case class Position7D(first: Value, second: Value, third: Value, fourth: Value, fifth: Value, sixth: Value,
  seventh: Value) extends Position with ReduceablePosition with ExpandablePosition with PermutablePosition
  with MapOverPosition with MapAlongPosition {
  type L = Position6D
  type M = Position8D
  type O = Map[Position6D, Content]
  type A = Map[Position1D, Content]

  val over = P7MapOver
  val along = PMapAlong
  val coordinates = List(first, second, third, fourth, fifth, sixth, seventh)

  protected type S = Position7D

  protected def less(cl: List[Value]): L = Position6D(cl(0), cl(1), cl(2), cl(3), cl(4), cl(5))
  protected def same(cl: List[Value]): S = Position7D(cl(0), cl(1), cl(2), cl(3), cl(4), cl(5), cl(6))
  protected def more(cl: List[Value]): M = Position8D(cl(0), cl(1), cl(2), cl(3), cl(4), cl(5), cl(6), cl(7))
}

/** Companion object to `Position7D`. */
object Position7D {
  /**
   * Construct a `Position7D` from types `S`, `T`, `U`, `V`, `W`, `X` and `Y` for which there exist coordinates.
   *
   * @param s The first coordinate value from which to create a `Position7D`.
   * @param t The second coordinate value from which to create a `Position7D`.
   * @param u The third coordinate value from which to create a `Position7D`.
   * @param v The fourth coordinate value from which to create a `Position7D`.
   * @param w The fifth coordinate value from which to create a `Position7D`.
   * @param x The sixth coordinate value from which to create a `Position7D`.
   * @param y The seventh coordinate value from which to create a `Position7D`.
   */
  def apply[S, T, U, V, W, X, Y](s: S, t: T, u: U, v: V, w: W, x: X, y: Y)(implicit ev1: Valueable[S],
    ev2: Valueable[T], ev3: Valueable[U], ev4: Valueable[V], ev5: Valueable[W], ev6: Valueable[X],
    ev7: Valueable[Y]): Position7D = {
    Position7D(ev1.convert(s), ev2.convert(t), ev3.convert(u), ev4.convert(v), ev5.convert(w), ev6.convert(x),
      ev7.convert(y))
  }
}

/**
 * Position for 8 dimensional data.
 *
 * @param first   Coordinate for the first dimension.
 * @param second  Coordinate for the second dimension.
 * @param third   Coordinate for the third dimension.
 * @param fourth  Coordinate for the fourth dimension.
 * @param fifth   Coordinate for the fifth dimension.
 * @param sixth   Coordinate for the sixth dimension.
 * @param seventh Coordinate for the seventh dimension.
 * @param eighth  Coordinate for the eighth dimension.
 */
case class Position8D(first: Value, second: Value, third: Value, fourth: Value, fifth: Value, sixth: Value,
  seventh: Value, eighth: Value) extends Position with ReduceablePosition with ExpandablePosition
  with PermutablePosition with MapOverPosition with MapAlongPosition {
  type L = Position7D
  type M = Position9D
  type O = Map[Position7D, Content]
  type A = Map[Position1D, Content]

  val over = P8MapOver
  val along = PMapAlong
  val coordinates = List(first, second, third, fourth, fifth, sixth, seventh, eighth)

  protected type S = Position8D

  protected def less(cl: List[Value]): L = Position7D(cl(0), cl(1), cl(2), cl(3), cl(4), cl(5), cl(6))
  protected def same(cl: List[Value]): S = Position8D(cl(0), cl(1), cl(2), cl(3), cl(4), cl(5), cl(6), cl(7))
  protected def more(cl: List[Value]): M = Position9D(cl(0), cl(1), cl(2), cl(3), cl(4), cl(5), cl(6), cl(7), cl(8))
}

/** Companion object to `Position8D`. */
object Position8D {
  /**
   * Construct a `Position8D` from types `S`, `T`, `U`, `V`, `W`, `X`, `Y` and `Z` for which there exist coordinates.
   *
   * @param s The first coordinate value from which to create a `Position8D`.
   * @param t The second coordinate value from which to create a `Position8D`.
   * @param u The third coordinate value from which to create a `Position8D`.
   * @param v The fourth coordinate value from which to create a `Position8D`.
   * @param w The fifth coordinate value from which to create a `Position8D`.
   * @param x The sixth coordinate value from which to create a `Position8D`.
   * @param y The seventh coordinate value from which to create a `Position8D`.
   * @param z The eighth coordinate value from which to create a `Position8D`.
   */
  def apply[S, T, U, V, W, X, Y, Z](s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z)(implicit ev1: Valueable[S],
    ev2: Valueable[T], ev3: Valueable[U], ev4: Valueable[V], ev5: Valueable[W], ev6: Valueable[X], ev7: Valueable[Y],
    ev8: Valueable[Z]): Position8D = {
    Position8D(ev1.convert(s), ev2.convert(t), ev3.convert(u), ev4.convert(v), ev5.convert(w), ev6.convert(x),
      ev7.convert(y), ev8.convert(z))
  }
}

/**
 * Position for 9 dimensional data.
 *
 * @param first   Coordinate for the first dimension.
 * @param second  Coordinate for the second dimension.
 * @param third   Coordinate for the third dimension.
 * @param fourth  Coordinate for the fourth dimension.
 * @param fifth   Coordinate for the fifth dimension.
 * @param sixth   Coordinate for the sixth dimension.
 * @param seventh Coordinate for the seventh dimension.
 * @param eighth  Coordinate for the eighth dimension.
 * @param ninth   Coordinate for the ninth dimension.
 */
case class Position9D(first: Value, second: Value, third: Value, fourth: Value, fifth: Value, sixth: Value,
  seventh: Value, eighth: Value, ninth: Value) extends Position with ReduceablePosition with PermutablePosition
  with MapOverPosition with MapAlongPosition {
  type L = Position8D
  type O = Map[Position8D, Content]
  type A = Map[Position1D, Content]

  val over = P9MapOver
  val along = PMapAlong
  val coordinates = List(first, second, third, fourth, fifth, sixth, seventh, eighth, ninth)

  protected type S = Position9D

  protected def less(cl: List[Value]): L = Position8D(cl(0), cl(1), cl(2), cl(3), cl(4), cl(5), cl(6), cl(7))
  protected def same(cl: List[Value]): S = Position9D(cl(0), cl(1), cl(2), cl(3), cl(4), cl(5), cl(6), cl(7), cl(8))
}

/** Companion object to `Position9D`. */
object Position9D {
  /**
   * Construct a `Position9D` from types `S`, `T`, `U`, `V`, `W`, `X`, `Y`, `Z` and `A` for which there exist
   * coordinates.
   *
   * @param s The first coordinate value from which to create a `Position9D`.
   * @param t The second coordinate value from which to create a `Position9D`.
   * @param u The third coordinate value from which to create a `Position9D`.
   * @param v The fourth coordinate value from which to create a `Position9D`.
   * @param w The fifth coordinate value from which to create a `Position9D`.
   * @param x The sixth coordinate value from which to create a `Position9D`.
   * @param y The seventh coordinate value from which to create a `Position9D`.
   * @param z The eighth coordinate value from which to create a `Position9D`.
   * @param a The ninth coordinate value from which to create a `Position9D`.
   */
  def apply[S, T, U, V, W, X, Y, Z, A](s: S, t: T, u: U, v: V, w: W, x: X, y: Y, z: Z, a: A)(implicit ev1: Valueable[S],
    ev2: Valueable[T], ev3: Valueable[U], ev4: Valueable[V], ev5: Valueable[W], ev6: Valueable[X], ev7: Valueable[Y],
    ev8: Valueable[Z], ev9: Valueable[A]): Position9D = {
    Position9D(ev1.convert(s), ev2.convert(t), ev3.convert(u), ev4.convert(v), ev5.convert(w), ev6.convert(x),
      ev7.convert(y), ev8.convert(z), ev9.convert(a))
  }
}

/** Base trait that represents the positions of a matrix. */
trait Positions[P <: Position] {
  /** Type of the underlying data structure (i.e. TypedPipe or RDD). */
  type U[_]

  type NamesTuners <: OneOf

  /**
   * Returns the distinct position(s) (or names) for a given `slice`.
   *
   * @param slice Encapsulates the dimension(s) for which the names are to be returned.
   * @param tuner The tuner for the job.
   *
   * @return A `U[(Slice.S, Long)]` of the distinct position(s) together with a unique index.
   *
   * @note The position(s) are returned with an index so the return value can be used in various `save` methods. The
   *       index itself is unique for each position but no ordering is defined.
   */
  def names[T <: Tuner](slice: Slice[P], tuner: T)(implicit ev1: slice.S =!= Position0D, ev2: ClassTag[slice.S],
    ev3: NamesTuners#V[T]): U[slice.S]

  /**
   * Slice the positions using a regular expression.
   *
   * @param regex     The regular expression to match on.
   * @param keep      Indicator if the matched positions should be kept or removed.
   * @param spearator Separator used to convert each position to string.
   *
   * @return A `U[P]` with only the positions of interest.
   *
   * @note The matching is done by converting each position to its short string reprensentation and then applying the
   *       regular expression.
   */
  def slice(regex: Regex, keep: Boolean, separator: String)(implicit ev: ClassTag[P]): U[P] = {
    slice(keep, p => regex.pattern.matcher(p.toShortString(separator)).matches)
  }

  /**
   * Slice the positions using one or more positions.
   *
   * @param positions The positions to slice on.
   * @param keep      Indicator if the matched positions should be kept or removed.
   *
   * @return A `U[P]` with only the positions of interest.
   */
  def slice[T](positions: T, keep: Boolean)(implicit ev1: PositionListable[T, P], ev2: ClassTag[P]): U[P] = {
    slice(keep, p => ev1.convert(positions).contains(p))
  }

  protected def slice(keep: Boolean, f: P => Boolean)(implicit ev: ClassTag[P]): U[P]
}

/** Type class for transforming a type `T` into a `Position`. */
trait Positionable[T, P <: Position] extends java.io.Serializable {
  /**
   * Returns a position for type `T`.
   *
   * @param t Object that can be converted to a position.
   */
  def convert(t: T): P
}

/** Companion object for the [[Positionable]] type class. */
object Positionable {
  /** Converts a position to a position; that is, it's a pass through. */
  implicit def P2P[T <: Position]: Positionable[T, T] = new Positionable[T, T] { def convert(t: T): T = t }

  /** Converts a `Valueable` to a position. */
  implicit def V2P[T](implicit ev: Valueable[T]): Positionable[T, Position1D] = {
    new Positionable[T, Position1D] { def convert(t: T): Position1D = Position1D(ev.convert(t)) }
  }
}

/** Type class for transforming a type `T` into a `List[Position]`. */
trait PositionListable[T, P <: Position] extends java.io.Serializable {
  /**
   * Returns a `List[Position]` for type `T`.
   *
   * @param t Object that can be converted to a `List[Position]`.
   */
  def convert(t: T): List[P]
}

/** Companion object for the `PositionListable` type class. */
object PositionListable {
  /** Converts a `List[Positionable]` to a `List[Position]`. */
  implicit def LP2LP[T, P <: Position](implicit ev: Positionable[T, P]): PositionListable[List[T], P] = {
    new PositionListable[List[T], P] { def convert(t: List[T]): List[P] = t.map(ev.convert(_)) }
  }

  /** Converts a `Positionable` to a `List[Position]`. */
  implicit def P2PL[T, P <: Position](implicit ev: Positionable[T, P]): PositionListable[T, P] = {
    new PositionListable[T, P] { def convert(t: T): List[P] = List(ev.convert(t)) }
  }
}

/** Type class for transforming a type `T` into a `U[Position]`. */
trait PositionDistributable[T, P <: Position, U[_]] {
  /**
   * Returns a `U[Position]` for type `T`.
   *
   * @param t Object that can be converted to a `U[Position]`.
   */
  def convert(t: T): U[P]
}

