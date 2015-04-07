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

package au.com.cba.omnia.grimlock

import au.com.cba.omnia.grimlock.position._

import com.twitter.scalding._

import org.apache.spark.rdd._

import scala.reflect._
import scala.util.matching.Regex

/** Base trait that represents the names along the dimensions of a matrix. */
trait Names[P <: Position] {
  /** Type of the underlying data structure (i.e. TypedPipe or RDD). */
  type U[_]

  /**
   * Renumber the indices such that `position` is last.
   *
   * @param position The position to move to the back.
   *
   * @return A `U[(P, Long)]` with `position` at the greatest index and all others renumbered but preserving
   *         their relative ordering.
   */
  def moveToBack[T](position: T)(implicit ev: Positionable[T, P]): U[(P, Long)]

  /**
   * Renumber the indices such that `position` is first.
   *
   * @param position The position to move to the front.
   *
   * @return A `U[(P, Long)]` with `position` at index 0 and all others renumbered but preserving their
   *         relative ordering.
   */
  def moveToFront[T](position: T)(implicit ev: Positionable[T, P]): U[(P, Long)]

  /** Renumber the names. */
  def renumber()(implicit ev: ClassTag[P]): U[(P, Long)]

  /**
   * Set the index of a single position.
   *
   * @param position The position whose index is to be set.
   * @param index    The new index.
   *
   * @return A `U[(P, Long)]` with only the index at the specified position updated.
   */
  def set[T](position: T, index: Long)(implicit ev: Positionable[T, P]): U[(P, Long)] = {
    set(Map(position -> index))
  }

  /**
   * Set the index of multiple positions.
   *
   * @param positions A `Map` of positions (together with their new index) whose index is to be set.
   *
   * @return A `U[(P, Long)]` with only the indices at the specified positions updated.
   */
  def set[T](positions: Map[T, Long])(implicit ev: Positionable[T, P]): U[(P, Long)]

  /**
   * Slice the names using a regular expression.
   *
   * @param regex     The regular expression to match on.
   * @param keep      Indicator if the matched names should be kept or removed.
   * @param spearator Separator used to convert each position to string.
   *
   * @return A `U[(P, Long)]` with only the names of interest.
   *
   * @note The matching is done by converting each position to its short string reprensentation and then applying the
   *       regular expression.
   */
  def slice(regex: Regex, keep: Boolean, separator: String)(implicit ev: ClassTag[P]): U[(P, Long)] = {
    slice(keep, p => regex.pattern.matcher(p.toShortString(separator)).matches)
  }

  /**
   * Slice the names using one or more positions.
   *
   * @param positions The positions to slice on.
   * @param keep      Indicator if the matched names should be kept or removed.
   *
   * @return A `U[(P, Long)]` with only the names of interest.
   */
  def slice[T](positions: T, keep: Boolean)(implicit ev1: PositionListable[T, P], ev2: ClassTag[P]): U[(P, Long)] = {
    slice(keep, p => ev1.convert(positions).contains(p))
  }

  protected def slice(keep: Boolean, f: P => Boolean)(implicit ev: ClassTag[P]): U[(P, Long)]

  protected def toString(t: (P, Long), separator: String, descriptive: Boolean): String = {
    descriptive match {
      case true => t._1.toString + separator + t._2.toString
      case false => t._1.toShortString(separator) + separator + t._2.toString
    }
  }
}

/**
 * Rich wrapper around a `TypedPipe[(Position, Long)]`.
 *
 * @param data `TypedPipe[(Position, Long)]`.
 *
 * @note This class represents the names along the dimensions of a matrix.
 */
class ScaldingNames[P <: Position](val data: TypedPipe[(P, Long)]) extends Names[P] with ScaldingPersist[(P, Long)] {
  type U[A] = TypedPipe[A]

  def moveToFront[T](position: T)(implicit ev: Positionable[T, P]): TypedPipe[(P, Long)] = {
    val pos = ev.convert(position)
    val state = data
      .map { case (p, i) => Map(p -> i) }
      .sum
      .map { case m => Map("curr" -> m(pos)) }

    data
      .flatMapWithValue(state) {
        case ((p, i), so) => so.map {
          case s => (p, if (p == pos) 0 else if (s("curr") > i) i + 1 else i)
        }
      }
  }

  def moveToBack[T](position: T)(implicit ev: Positionable[T, P]): TypedPipe[(P, Long)] = {
    val pos = ev.convert(position)
    val state = data
      .map { case (p, i) => Map(p -> i) }
      .sum
      .map { case m => Map("max" -> m.values.max, "curr" -> m(pos)) }

    data
      .flatMapWithValue(state) {
        case ((p, i), so) => so.map {
          case s => (p, if (s("curr") < i) i - 1 else if (p == pos) s("max") else i)
        }
      }
  }

  def renumber()(implicit ev: ClassTag[P]): TypedPipe[(P, Long)] = ScaldingNames.number(data.map { case (p, i) => p })

  def set[T](positions: Map[T, Long])(implicit ev: Positionable[T, P]): TypedPipe[(P, Long)] = {
    val converted = positions.map { case (k, v) => ev.convert(k) -> v }

    data.map { case (p, i) => (p, converted.getOrElse(p, i)) }
  }

  protected def slice(keep: Boolean, f: P => Boolean)(implicit ev: ClassTag[P]): TypedPipe[(P, Long)] = {
    ScaldingNames.number(data.collect { case (p, i) if !keep ^ f(p) => p })
  }
}

object ScaldingNames {
  /**
   * Number a `TypedPipe[Position]`
   *
   * @param data `TypedPipe[Position]` to number.
   *
   * @return A `TypedPipe[(Position, Long)]`.
   *
   * @note No ordering is defined on the indices for each position, but each index will be unique.
   */
  def number[P <: Position](data: TypedPipe[P]): TypedPipe[(P, Long)] = {
    data
      .groupAll
      .mapValueStream { _.zipWithIndex }
      .map { case ((), (p, i)) => (p, i) }
  }

  /** Conversion from `TypedPipe[(Position, Long)]` to a `ScaldingNames`. */
  implicit def TPPL2N[P <: Position](data: TypedPipe[(P, Long)]): ScaldingNames[P] = new ScaldingNames(data)
}

/**
 * Rich wrapper around a `RDD[(Position, Long)]`.
 *
 * @param data `RDD[(Position, Long)]`.
 *
 * @note This class represents the names along the dimensions of a matrix.
 */
class SparkNames[P <: Position](val data: RDD[(P, Long)]) extends Names[P] with SparkPersist[(P, Long)] {
  type U[A] = RDD[A]

  def moveToFront[T](position: T)(implicit ev: Positionable[T, P]): RDD[(P, Long)] = {
    val pos = ev.convert(position)
    val state = data
      .map { case (p, i) => Map(p -> i) }
      .reduce(_ ++ _)

    data.map { case (p, i) => (p, if (p == pos) 0 else if (state(pos) > i) i + 1 else i) }
  }

  def moveToBack[T](position: T)(implicit ev: Positionable[T, P]): RDD[(P, Long)] = {
    val pos = ev.convert(position)
    val state = data
      .map { case (p, i) => Map(p -> i) }
      .reduce(_ ++ _)

    data.map { case (p, i) => (p, if (state(pos) < i) i - 1 else if (p == pos) state.values.max else i) }
  }

  def renumber()(implicit ev: ClassTag[P]): RDD[(P, Long)] = SparkNames.number(data.map { case (p, i) => p })

  def set[T](positions: Map[T, Long])(implicit ev: Positionable[T, P]): RDD[(P, Long)] = {
    val converted = positions.map { case (k, v) => ev.convert(k) -> v }

    data.map { case (p, i) => (p, converted.getOrElse(p, i)) }
  }

  protected def slice(keep: Boolean, f: P => Boolean)(implicit ev: ClassTag[P]): RDD[(P, Long)] = {
    SparkNames.number(data.collect { case (p, i) if !keep ^ f(p) => p })
  }
}

object SparkNames {
  /**
   * Number a `RDD[Position]`
   *
   * @param data `RDD[Position]` to number.
   *
   * @return A `RDD[(Position, Long)]`.
   *
   * @note No ordering is defined on the indices for each position, but each index will be unique.
   */
  def number[P <: Position](data: RDD[P]): RDD[(P, Long)] = data.zipWithIndex

  /** Conversion from `RDD[(Position, Long)]` to a `SparkNames`. */
  implicit def RDDPL2N[P <: Position](data: RDD[(P, Long)]): SparkNames[P] = new SparkNames(data)
}

/** Type class for transforming a type `T` into a `U[(Q, Long)]`. */
trait Nameable[T, P <: Position, Q <: Position, D <: Dimension, U[_]] {
  /**
   * Returns a `U[(Q, Long)]` for type `T`.
   *
   * @param m The matrix to get names from.
   * @param s Encapsulates the dimension(s) for which to get names.
   * @param t Object that can be converted to a `U[(Q, Long)]`.
   */
  def convert(m: Matrix[P], s: Slice[P, D], t: T): U[(Q, Long)]
}

/** Scalding Companion object for the `Nameable` type class. */
object ScaldingNameable {
  /** Converts a `TypedPipe[(Q, Long)]` into a `TypedPipe[(Q, Long)]`; that is, it is a pass through. */
  implicit def TPQL2N[P <: Position, Q <: Position, D <: Dimension]: Nameable[TypedPipe[(Q, Long)], P, Q, D, TypedPipe] = {
    new Nameable[TypedPipe[(Q, Long)], P, Q, D, TypedPipe] {
      def convert(m: Matrix[P], s: Slice[P, D], t: TypedPipe[(Q, Long)]): TypedPipe[(Q, Long)] = t
    }
  }
  /** Converts a `TypedPipe[Q]` into a `TypedPipe[(Q, Long)]`. */
  implicit def TPQ2N[P <: Position, Q <: Position, D <: Dimension]: Nameable[TypedPipe[Q], P, Q, D, TypedPipe] = {
    new Nameable[TypedPipe[Q], P, Q, D, TypedPipe] {
      def convert(m: Matrix[P], s: Slice[P, D], t: TypedPipe[Q]): TypedPipe[(Q, Long)] = ScaldingNames.number(t)
    }
  }
  /** Converts a `PositionListable` into a `TypedPipe[(Q, Long)]`. */
  implicit def PL2N[T, P <: Position, Q <: Position, D <: Dimension](implicit ev1: PositionListable[T, Q],
    ev2: PosDimDep[P, D], ev3: ClassTag[Q]): Nameable[T, P, Q, D, TypedPipe] = {
    new Nameable[T, P, Q, D, TypedPipe] {
      def convert(m: Matrix[P], s: Slice[P, D], t: T): TypedPipe[(Q, Long)] = {
        new ScaldingNames(m.names(s).asInstanceOf[TypedPipe[(Q, Long)]]).slice(t, true)
      }
    }
  }
}

/** Spark Companion object for the `Nameable` type class. */
object SparkNameable {
  /** Converts a `RDD[(Q, Long)]` into a `RDD[(Q, Long)]`; that is, it is a pass through. */
  implicit def RDDQL2N[P <: Position, Q <: Position, D <: Dimension]: Nameable[RDD[(Q, Long)], P, Q, D, RDD] = {
    new Nameable[RDD[(Q, Long)], P, Q, D, RDD] {
      def convert(m: Matrix[P], s: Slice[P, D], t: RDD[(Q, Long)]): RDD[(Q, Long)] = t
    }
  }
  /** Converts a `RDD[Q]` into a `RDD[(Q, Long)]`. */
  implicit def RDDQ2N[P <: Position, Q <: Position, D <: Dimension]: Nameable[RDD[Q], P, Q, D, RDD] = {
    new Nameable[RDD[Q], P, Q, D, RDD] {
      def convert(m: Matrix[P], s: Slice[P, D], t: RDD[Q]): RDD[(Q, Long)] = SparkNames.number(t)
    }
  }
  /** Converts a `PositionListable` into a `RDD[(Q, Long)]`. */
  implicit def PL2N[T, P <: Position, Q <: Position, D <: Dimension](implicit ev1: PositionListable[T, Q],
    ev2: PosDimDep[P, D], ev3: ClassTag[Q]): Nameable[T, P, Q, D, RDD] = {
    new Nameable[T, P, Q, D, RDD] {
      def convert(m: Matrix[P], s: Slice[P, D], t: T): RDD[(Q, Long)] = {
        new SparkNames(m.names(s).asInstanceOf[RDD[(Q, Long)]]).slice(t, true)
      }
    }
  }
}

