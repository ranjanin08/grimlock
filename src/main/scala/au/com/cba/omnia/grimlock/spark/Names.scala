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

package au.com.cba.omnia.grimlock.spark

import au.com.cba.omnia.grimlock.framework.{
  Matrix => BaseMatrix,
  Names => BaseNames,
  Nameable => BaseNameable,
  Slice
}
import au.com.cba.omnia.grimlock.framework.position._

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Rich wrapper around a `RDD[(Position, Long)]`.
 *
 * @param data `RDD[(Position, Long)]`.
 *
 * @note This class represents the names along the dimensions of a matrix.
 */
class Names[P <: Position](val data: RDD[(P, Long)]) extends BaseNames[P] with Persist[(P, Long)] {
  type U[A] = RDD[A]

  def moveToFront[T](position: T)(implicit ev1: Positionable[T, P], ev2: ClassTag[P]): RDD[(P, Long)] = {
    val pos = ev1.convert(position)
    val state = data.collectAsMap

    data.map { case (p, i) => (p, if (p == pos) 0 else if (state(pos) > i) i + 1 else i) }
  }

  def moveToBack[T](position: T)(implicit ev1: Positionable[T, P], ev2: ClassTag[P]): RDD[(P, Long)] = {
    val pos = ev1.convert(position)
    val state = data.collectAsMap

    data.map { case (p, i) => (p, if (state(pos) < i) i - 1 else if (p == pos) state.values.max else i) }
  }

  def renumber()(implicit ev: ClassTag[P]): RDD[(P, Long)] = Names.number(data.map { case (p, i) => p })

  def set[T](positions: Map[T, Long])(implicit ev: Positionable[T, P]): RDD[(P, Long)] = {
    val converted = positions.map { case (k, v) => ev.convert(k) -> v }

    data.map { case (p, i) => (p, converted.getOrElse(p, i)) }
  }

  protected def slice(keep: Boolean, f: P => Boolean)(implicit ev: ClassTag[P]): RDD[(P, Long)] = {
    Names.number(data.collect { case (p, i) if !keep ^ f(p) => p })
  }
}

/** Companion object for the Spark `Names` class. */
object Names {
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

  /** Conversion from `RDD[(Position, Long)]` to a Spark `Names`. */
  implicit def RDDPL2RDDN[P <: Position](data: RDD[(P, Long)]): Names[P] = new Names(data)
}

/** Spark companion object for the `Nameable` type class. */
object Nameable {
  /** Converts a `RDD[(Q, Long)]` into a `RDD[(Q, Long)]`; that is, it is a pass through. */
  implicit def RDDQL2RDDN[P <: Position, Q <: Position, D <: Dimension]: BaseNameable[RDD[(Q, Long)], P, Q, D, RDD] = {
    new BaseNameable[RDD[(Q, Long)], P, Q, D, RDD] {
      def convert(m: BaseMatrix[P], s: Slice[P, D], t: RDD[(Q, Long)])(implicit ev: ClassTag[s.S]): RDD[(Q, Long)] = t
    }
  }

  /** Converts a `RDD[Q]` into a `RDD[(Q, Long)]`. */
  implicit def RDDQ2RDDN[P <: Position, Q <: Position, D <: Dimension]: BaseNameable[RDD[Q], P, Q, D, RDD] = {
    new BaseNameable[RDD[Q], P, Q, D, RDD] {
      def convert(m: BaseMatrix[P], s: Slice[P, D], t: RDD[Q])(implicit ev: ClassTag[s.S]): RDD[(Q, Long)] = {
        Names.number(t)
      }
    }
  }

  /** Converts a `PositionListable` into a `RDD[(Q, Long)]`. */
  implicit def PL2RDDN[T, P <: Position, Q <: Position, D <: Dimension](implicit ev1: PositionListable[T, Q],
    ev2: PosDimDep[P, D], ev3: ClassTag[Q]): BaseNameable[T, P, Q, D, RDD] = {
    new BaseNameable[T, P, Q, D, RDD] {
      def convert(m: BaseMatrix[P], s: Slice[P, D], t: T)(implicit ev: ClassTag[s.S]): RDD[(Q, Long)] = {
        new Names(m.names(s).asInstanceOf[RDD[(Q, Long)]]).slice(t, true)
      }
    }
  }
}

