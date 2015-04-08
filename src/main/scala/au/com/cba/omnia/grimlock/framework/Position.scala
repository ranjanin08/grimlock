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

package au.com.cba.omnia.grimlock.position

import au.com.cba.omnia.grimlock._
import au.com.cba.omnia.grimlock.content._
import au.com.cba.omnia.grimlock.encoding._

import scala.reflect._

/** Base trait for dealing with positions. */
trait Position {
  /** List of coordinates of the position. */
  val coordinates: List[Value]

  /**
   * Return the coordinate at dimension (index) `dim`.
   *
   * @param dim Dimension of the coordinate to get.
   */
  def apply(dim: Dimension): Value = coordinates(dim.index)

  /**
   * Update the coordinate at `dim` with `t`.
   *
   * @param dim The dimension to set.
   * @param t   The coordinate to set.
   *
   * @return A position of the same size as `this` but with `t` set at index `dim`.
   */
  def update[T](dim: Dimension, t: T)(implicit ev: Valueable[T]): this.type = {
    same(coordinates.updated(dim.index, ev.convert(t))).asInstanceOf[this.type]
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
}

object Position {
  /** Define an ordering between 2 position. Only use with position of the same type coordinates. */
  def Ordering[T <: Position]: Ordering[T] = new Ordering[T] { def compare(x: T, y: T): Int = x.compare(y) }

  /** `MapablePosition` object for `PositionND` (N > 1) with `Along`. */
  case object MapAlong extends MapMapablePosition[Position1D] {}
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
    val (h, t) = coordinates.splitAt(dim.index)

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
    less(coordinates
      .updated(into.index,
        StringValue(coordinates(into.index).toShortString + separator + coordinates(dim.index).toShortString))
      .zipWithIndex
      .filter(_._2 != dim.index)
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
    same(order.map { case d => coordinates(d.index) }).asInstanceOf[this.type]
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

/** Trait for converting (2..N)D positions to `Map` values. */
trait MapMapablePosition[P <: Position] extends MapablePosition[P, Map[P, Content]] {
  def toMapValue(r: P, c: Content): Map[P, Content] = Map(r -> c)
  def combineMapValues(x: Option[Map[P, Content]], y: Map[P, Content]): Map[P, Content] = x.map(_ ++ y).getOrElse(y)
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

  val over = Position1D.MapOver
  val along = Position1D.MapAlong
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

  /** `MapablePosition` object for `Position1D` with `Over`. */
  case object MapOver extends MapablePosition[Position0D, Content] {
    def toMapValue(r: Position0D, c: Content): Content = c
    def combineMapValues(x: Option[Content], y: Content): Content = y
  }

  /** `MapablePosition` object for `Position1D` with `Along`. */
  case object MapAlong extends MapablePosition[Position1D, Content] {
    def toMapValue(r: Position1D, c: Content): Content = throw new Exception("Can't map along 1D")
    def combineMapValues(x: Option[Content], y: Content): Content = throw new Exception("Can't map along 1D")
  }
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

  val over = Position2D.MapOver
  val along = Position.MapAlong
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

  /** `MapablePosition` object for `Position2D` with `Over`. */
  case object MapOver extends MapMapablePosition[Position1D] {}
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

  val over = Position3D.MapOver
  val along = Position.MapAlong
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

  /** `MapablePosition` object for `Position3D` with `Over`. */
  case object MapOver extends MapMapablePosition[Position2D] {}
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

  val over = Position4D.MapOver
  val along = Position.MapAlong
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

  /** `MapablePosition` object for `Position4D` with `Over`. */
  case object MapOver extends MapMapablePosition[Position3D] {}
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
  with ReduceablePosition with PermutablePosition with MapOverPosition with MapAlongPosition {
  type L = Position4D
  type O = Map[Position4D, Content]
  type A = Map[Position1D, Content]

  val over = Position5D.MapOver
  val along = Position.MapAlong
  val coordinates = List(first, second, third, fourth, fifth)

  protected type S = Position5D

  protected def less(cl: List[Value]): L = Position4D(cl(0), cl(1), cl(2), cl(3))
  protected def same(cl: List[Value]): S = Position5D(cl(0), cl(1), cl(2), cl(3), cl(4))
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

  /** `MapablePosition` object for `Position5D` with `Over`. */
  case object MapOver extends MapMapablePosition[Position4D] {}
}

/** Base trait that represents the positions of a matrix. */
trait Positions[P <: Position] {
  /** Type of the underlying data structure (i.e. TypedPipe or RDD). */
  type U[_]

  /**
   * Returns the distinct position(s) (or names) for a given `slice`.
   *
   * @param slice Encapsulates the dimension(s) for which the names are to be returned.
   *
   * @return A `U[(Slice.S, Long)]` of the distinct position(s) together with a unique index.
   *
   * @note The position(s) are returned with an index so the return value can be used in various `persist` methods. The
   *       index itself is unique for each position but no ordering is defined.
   *
   * @see [[Names]]
   */
  def names[D <: Dimension](slice: Slice[P, D])(implicit ev1: PosDimDep[P, D],
    ev2: ClassTag[slice.S]): U[(slice.S, Long)]

  protected def toString(t: P, separator: String, descriptive: Boolean): String = {
    if (descriptive) { t.toString } else { t.toShortString(separator) }
  }
}

/** Type class for transforming a type `T` into a `Position`. */
trait Positionable[T, P <: Position] {
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
trait PositionListable[T, P <: Position] {
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

