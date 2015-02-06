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

import cascading.flow.FlowDef
import com.twitter.scalding._
import com.twitter.scalding.TDsl._, Dsl._
import com.twitter.scalding.typed.{ IterablePipe, TypedSink }

/** Base trait for dealing with positions. */
trait Position {
  /** List of coordinates of the position. */
  val coordinates: List[Value]

  /**
   * Return the coordinate at dimension (index) `dim`.
   *
   * @param dim Dimension of the coordinate to get.
   */
  def get(dim: Dimension): Value = coordinates(dim.index)

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
    val mine = coordinates
    val theirs = that.coordinates

    (mine.length == theirs.length) match {
      case true =>
        val cmp = mine.zip(theirs).map { case (m, t) => Value.Ordering.compare(m, t) }

        cmp.indexWhere(_ != 0) match {
          case idx if (idx < 0) => 0
          case idx => cmp(idx)
        }
      case false => mine.length.compare(theirs.length)
    }
  }
}

/** Trait for operations that modify a position (but keep the number of dimensions the same). */
trait ModifiablePosition { self: Position =>
  /** Type for positions of same number of dimensions. */
  type S <: Position

  /**
   * Update the coordinate at `dim` with `t`.
   *
   * @param dim The dimension to set.
   * @param t   The coordinate to set.
   *
   * @return A position of the same size as `this` but with `t` set at index `dim`.
   */
  def set[T: Valueable](dim: Dimension, t: T): S = {
    same(coordinates.updated(dim.index, implicitly[Valueable[T]].convert(t)))
  }

  /**
   * Permute the order of coordinates.
   *
   * @param order The new ordering of the coordinates.
   *
   * @return A position of the same size as `this` but with the coordinates ordered according to `order`.
   *
   * @note The ordering must contain each dimension exactly once.
   */
  def permute(order: List[Dimension]): S = same(order.map { case d => coordinates(d.index) })

  protected def same(cl: List[Value]): S
}

/** Base trait for converting a position to a `Map`. */
trait Mapable[P <: Position, T] {
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

/** Trait for converting 1D positions to `Map` values. */
trait Mapable1D[P <: Position] extends Mapable[P, Content] {
  def toMapValue(r: P, c: Content): Content = c
  def combineMapValues(x: Option[Content], y: Content): Content = y
}

/** Trait for converting (2..N)D positions to `Map` values. */
trait MapableXD[P <: Position] extends Mapable[P, Map[P, Content]] {
  def toMapValue(r: P, c: Content): Map[P, Content] = Map(r -> c)
  def combineMapValues(x: Option[Map[P, Content]], y: Map[P, Content]): Map[P, Content] = {
    x match {
      case Some(l) => l ++ y
      case None => y
    }
  }
}

/** `Mapable` object for `PositionXD` with `Along`. */
case object PositionXDAlong extends MapableXD[Position1D] {}

/** Trait for operations that reduce a position by one dimension. */
trait ReduceablePosition { self: Position =>
  /** Type for positions of one less number of dimensions. */
  type L <: Position with ExpandablePosition

  /** Type of the `Map` content when `Over.toMap` is called. */
  type O

  /** Type of the `Map` content when `Along.toMap` is called. */
  type A

  /** Object with `Mapable` implementation to `Over.toMap` and `Over.combineMaps`. */
  val over: Mapable[L, O]

  /** Object with `Mapable` implementation to `Along.toMap` and `Along.combineMaps`. */
  val along: Mapable[Position1D, A]

  /**
   * Remove the coordinate at dimension `dim`.
   *
   * @param dim The dimension to remove.
   *
   * @return A new position with dimension `dim` removed.
   */
  def remove(dim: Dimension): L = {
    val (h, t) = coordinates.splitAt(dim.index)

    less(h ::: t.drop(1))
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
    val (h, t) = coordinates
      .updated(into.index,
        StringValue(coordinates(into.index).toShortString + separator + coordinates(dim.index).toShortString))
      .splitAt(dim.index)

    less(h ::: t.drop(1))
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
  def prepend[T: Valueable](t: T): M = more(implicitly[Valueable[T]].convert(t) +: coordinates)

  /**
   * Append a coordinate to the position.
   *
   * @param t The coordinate to append.
   *
   * @return A new position with the coordinate `t` appended.
   */
  def append[T: Valueable](t: T): M = more(coordinates :+ implicitly[Valueable[T]].convert(t))

  protected def more(cl: List[Value]): M
}

/**
 * Position for zero dimensions.
 *
 * @note Position0D exists so things like `names(Over(1))` work.
 */
case class Position0D() extends Position with ExpandablePosition {
  type M = Position1D

  val coordinates = List()

  def more(cl: List[Value]): M = Position1D(cl(0))
}

/**
 * Position for 1 dimensional data.
 *
 * @param first Coordinate for the first dimension.
 */
case class Position1D(first: Value) extends Position with ModifiablePosition with ReduceablePosition
  with ExpandablePosition {
  type L = Position0D
  type S = Position1D
  type M = Position2D
  type O = Content
  type A = Content

  val over = Position1DOver
  val along = Position1DAlong
  val coordinates = List(first)

  protected def less(cl: List[Value]): L = Position0D()
  protected def same(cl: List[Value]): S = Position1D(cl(0))
  protected def more(cl: List[Value]): M = Position2D(cl(0), cl(1))
}

/** `Mapable` object for `Position1D` with `Over`. */
case object Position1DOver extends Mapable1D[Position0D] {}

/** `Mapable` object for `Position1D` with `Along`. */
case object Position1DAlong extends Mapable1D[Position1D] {}

/** Companion object to `Position1D`. */
object Position1D {
  /**
   * Construct a `Position1D` from a type `S` for which there exist a coordinate.
   *
   * @param s The coordinate value from which to create a `Position1D`.
   */
  def apply[S: Valueable](s: S): Position1D = Position1D(implicitly[Valueable[S]].convert(s))
}

/**
 * Position for 2 dimensional data.
 *
 * @param first  Coordinate for the first dimension.
 * @param second Coordinate for the second dimension.
 */
case class Position2D(first: Value, second: Value) extends Position with ModifiablePosition with ReduceablePosition
  with ExpandablePosition {
  type L = Position1D
  type S = Position2D
  type M = Position3D
  type O = Map[Position1D, Content]
  type A = Map[Position1D, Content]

  val over = Position2DOver
  val along = PositionXDAlong
  val coordinates = List(first, second)

  protected def less(cl: List[Value]): L = Position1D(cl(0))
  protected def same(cl: List[Value]): S = Position2D(cl(0), cl(1))
  protected def more(cl: List[Value]): M = Position3D(cl(0), cl(1), cl(2))
}

/** `Mapable` object for `Position2D` with `Over`. */
case object Position2DOver extends MapableXD[Position1D] {}

/** Companion object to `Position2D`. */
object Position2D {
  /**
   * Construct a `Position2D` from types `S` and `T` for which there exist coordinates.
   *
   * @param s The first coordinate value from which to create a `Position2D`.
   * @param t The second coordinate value from which to create a `Position2D`.
   */
  def apply[S: Valueable, T: Valueable](s: S, t: T): Position2D = {
    Position2D(implicitly[Valueable[S]].convert(s), implicitly[Valueable[T]].convert(t))
  }
}

/**
 * Position for 3 dimensional data.
 *
 * @param first  Coordinate for the first dimension.
 * @param second Coordinate for the second dimension.
 * @param third  Coordinate for the third dimension.
 */
case class Position3D(first: Value, second: Value, third: Value) extends Position with ModifiablePosition
  with ReduceablePosition with ExpandablePosition {
  type L = Position2D
  type S = Position3D
  type M = Position4D
  type O = Map[Position2D, Content]
  type A = Map[Position1D, Content]

  val over = Position3DOver
  val along = PositionXDAlong
  val coordinates = List(first, second, third)

  protected def less(cl: List[Value]): L = Position2D(cl(0), cl(1))
  protected def same(cl: List[Value]): S = Position3D(cl(0), cl(1), cl(2))
  protected def more(cl: List[Value]): M = Position4D(cl(0), cl(1), cl(2), cl(3))
}

/** `Mapable` object for `Position3D` with `Over`. */
case object Position3DOver extends MapableXD[Position2D] {}

/** Companion object to `Position3D`. */
object Position3D {
  /**
   * Construct a `Position3D` from types `S`, `T` and `U` for which there exist coordinates.
   *
   * @param s The first coordinate value from which to create a `Position3D`.
   * @param t The second coordinate value from which to create a `Position3D`.
   * @param u The third coordinate value from which to create a `Position3D`.
   */
  def apply[S: Valueable, T: Valueable, U: Valueable](s: S, t: T, u: U): Position3D = {
    Position3D(implicitly[Valueable[S]].convert(s), implicitly[Valueable[T]].convert(t),
      implicitly[Valueable[U]].convert(u))
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
  with ModifiablePosition with ReduceablePosition with ExpandablePosition {
  type L = Position3D
  type S = Position4D
  type M = Position5D
  type O = Map[Position3D, Content]
  type A = Map[Position1D, Content]

  val over = Position4DOver
  val along = PositionXDAlong
  val coordinates = List(first, second, third, fourth)

  protected def less(cl: List[Value]): L = Position3D(cl(0), cl(1), cl(2))
  protected def same(cl: List[Value]): S = Position4D(cl(0), cl(1), cl(2), cl(3))
  protected def more(cl: List[Value]): M = Position5D(cl(0), cl(1), cl(2), cl(3), cl(4))
}

/** `Mapable` object for `Position4D` with `Over`. */
case object Position4DOver extends MapableXD[Position3D] {}

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
  def apply[S: Valueable, T: Valueable, U: Valueable, V: Valueable](s: S, t: T, u: U, v: V): Position4D = {
    Position4D(implicitly[Valueable[S]].convert(s), implicitly[Valueable[T]].convert(t),
      implicitly[Valueable[U]].convert(u), implicitly[Valueable[V]].convert(v))
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
  with ModifiablePosition with ReduceablePosition {
  type L = Position4D
  type S = Position5D
  type O = Map[Position4D, Content]
  type A = Map[Position1D, Content]

  val over = Position5DOver
  val along = PositionXDAlong
  val coordinates = List(first, second, third, fourth, fifth)

  protected def less(cl: List[Value]): L = Position4D(cl(0), cl(1), cl(2), cl(3))
  protected def same(cl: List[Value]): S = Position5D(cl(0), cl(1), cl(2), cl(3), cl(4))
}

/** `Mapable` object for `Position5D` with `Over`. */
case object Position5DOver extends MapableXD[Position4D] {}

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
  def apply[S: Valueable, T: Valueable, U: Valueable, V: Valueable, W: Valueable](s: S, t: T, u: U, v: V,
    w: W): Position5D = {
    Position5D(implicitly[Valueable[S]].convert(s), implicitly[Valueable[T]].convert(t),
      implicitly[Valueable[U]].convert(u), implicitly[Valueable[V]].convert(v), implicitly[Valueable[W]].convert(w))
  }
}

/**
 * Rich wrapper around a `TypedPipe[Position]`.
 *
 * @param data The `TypedPipe[Position]`.
 */
class Positions[P <: Position](data: TypedPipe[P]) {
  /**
   * Returns the distinct position(s) (or names) for a given `slice`.
   *
   * @param slice Encapsulates the dimension(s) for which the names are to be returned.
   *
   * @return A Scalding `TypedPipe[(Slice.S, Long)]` of the distinct position(s) together with a unique index.
   *
   * @note The position(s) are returned with an index so the return value can be used in various `persist` methods. The
   *       index itself is unique for each position but no ordering is defined.
   *
   * @see [[Names]]
   */
  def names[D <: Dimension](slice: Slice[P, D])(implicit ev: PosDimDep[P, D]): TypedPipe[(slice.S, Long)] = {
    implicit def PositionOrdering[T <: Position] = new Ordering[T] { def compare(l: T, r: T) = l.compare(r) }

    Names.number(data.map { case p => slice.selected(p) }.distinct)
  }

  /**
   * Persist a `TypedPipe[Position]` to disk.
   *
   * @param file        Name of the output file.
   * @param separator   Separator to use between position and content.
   * @param descriptive Indicates if the output should be descriptive.
   *
   * @return A Scalding `TypedPipe[Position]` which is this object's data.
   */
  def persistFile(file: String, separator: String = "|", descriptive: Boolean = false)(implicit flow: FlowDef,
    mode: Mode): TypedPipe[P] = persist(TypedSink(TextLine(file)), separator, descriptive)

  /**
   * Persist a `TypedPipe[Position]` to a sink.
   *
   * @param sink        Sink to write to.
   * @param separator   Separator to use between position and content.
   * @param descriptive Indicates if the output should be descriptive.
   *
   * @return A Scalding `TypedPipe[Position]` which is this object's data.
   */
  def persist(sink: TypedSink[String], separator: String = "|", descriptive: Boolean = false)(implicit flow: FlowDef,
    mode: Mode): TypedPipe[P] = {
    data
      .map {
        case p => descriptive match {
          case true => p.toString
          case false => p.toShortString(separator)
        }
      }
      .write(sink)

    data
  }
}

/** Companion object for the `Positions` class. */
object Positions {
  /** Converts a `TypedPipe[Position]` to a `Positions`. */
  implicit def TPP2P[P <: Position](data: TypedPipe[P]): Positions[P] = new Positions(data)
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
  implicit def V2P[T: Valueable]: Positionable[T, Position1D] = {
    new Positionable[T, Position1D] { def convert(t: T): Position1D = Position1D(implicitly[Valueable[T]].convert(t)) }
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

/** Type class for transforming a type `T` into a `TypedPipe[Position]`. */
trait PositionPipeable[T, P <: Position] {
  /**
   * Returns a `TypedPipe[Position]` for type `T`.
   *
   * @param t Object that can be converted to a `TypedPipe[Position]`.
   */
  def convert(t: T): TypedPipe[P]
}

object PositionPipeable {
  /** Converts a `TypedPipe[Position]` to a `TypedPipe[Position]`; that is, it's a pass through. */
  implicit def TPP2PP[P <: Position]: PositionPipeable[TypedPipe[P], P] = {
    new PositionPipeable[TypedPipe[P], P] { def convert(t: TypedPipe[P]): TypedPipe[P] = t }
  }
  /** Converts a `List[Positionable]` to a `TypedPipe[Position]`. */
  implicit def LP2PP[T, P <: Position](implicit ev: Positionable[T, P], flow: FlowDef,
    mode: Mode): PositionPipeable[List[T], P] = {
    new PositionPipeable[List[T], P] {
      def convert(t: List[T]): TypedPipe[P] = new IterablePipe(t.map(ev.convert(_)), flow, mode)
    }
  }
  /** Converts a `Positionable` to a `TypedPipe[Position]`. */
  implicit def P2PP[T, P <: Position](implicit ev: Positionable[T, P], flow: FlowDef,
    mode: Mode): PositionPipeable[T, P] = {
    new PositionPipeable[T, P] { def convert(t: T): TypedPipe[P] = new IterablePipe(List(ev.convert(t)), flow, mode) }
  }
}

