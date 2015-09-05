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

package au.com.cba.omnia.grimlock.scalding

import au.com.cba.omnia.grimlock.framework.{
  Default,
  Matrix => BaseMatrix,
  Name,
  Names => BaseNames,
  Nameable => BaseNameable,
  NoParameters
}
import au.com.cba.omnia.grimlock.framework.position._
import au.com.cba.omnia.grimlock.framework.utility._

import cascading.flow.FlowDef
import com.twitter.scalding.Mode
import com.twitter.scalding.typed.TypedPipe

import scala.reflect.ClassTag

/**
 * Rich wrapper around a `TypedPipe[(Position, Long)]`.
 *
 * @param data `TypedPipe[(Position, Long)]`.
 *
 * @note This class represents the names along the dimensions of a matrix.
 */
class Names[P <: Position](val data: TypedPipe[(P, Long)]) extends BaseNames[P] with Persist[(P, Long)] {
  type U[A] = TypedPipe[A]

  def moveToFront[T](position: T)(implicit ev1: Positionable[T, P], ev2: ClassTag[P]): U[(P, Long)] = {
    val pos = ev1.convert(position)
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

  def moveToBack[T](position: T)(implicit ev1: Positionable[T, P], ev2: ClassTag[P]): U[(P, Long)] = {
    val pos = ev1.convert(position)
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

  def renumber()(implicit ev: ClassTag[P]): U[(P, Long)] = Names.number(data.map { case (p, _) => p })

  def saveAsText(file: String, writer: TextWriter = Name.toString())(implicit flow: FlowDef,
    mode: Mode): U[(P, Long)] = saveText(file, writer)

  def set[T](positions: Map[T, Long])(implicit ev: Positionable[T, P]): U[(P, Long)] = {
    val converted = positions.map { case (k, v) => ev.convert(k) -> v }

    data.map { case (p, i) => (p, converted.getOrElse(p, i)) }
  }

  protected def slice(keep: Boolean, f: P => Boolean)(implicit ev: ClassTag[P]): U[(P, Long)] = {
    Names.number(data.collect { case (p, _) if !keep ^ f(p) => p })
  }
}

/** Companion object for the Scalding `Names` class. */
object Names {
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
      .map { case (_, (p, i)) => (p, i) }
  }

  /** Conversion from `TypedPipe[(Position, Long)]` to a Scalding `Names`. */
  implicit def TPPL2TPN[P <: Position](data: TypedPipe[(P, Long)]): Names[P] = new Names(data)
}

/** Scalding Companion object for the `Nameable` type class. */
object Nameable {
  /** Converts a `TypedPipe[(Q, Long)]` into a `TypedPipe[(Q, Long)]`; that is, it is a pass through. */
  implicit def TPQL2TPN[P <: Position, Q <: Position, D <: Dimension]: BaseNameable[TypedPipe[(Q, Long)], P, Q, D, TypedPipe] = {
    new BaseNameable[TypedPipe[(Q, Long)], P, Q, D, TypedPipe] {
      def convert(m: BaseMatrix[P], s: Slice[P, D], t: TypedPipe[(Q, Long)])(implicit ev1: ClassTag[s.S],
        ev2: s.S =!= Position0D, ev3: m.NamesTuners#V[Default[NoParameters.type]]): TypedPipe[(Q, Long)] = t
    }
  }

  /** Converts a `TypedPipe[Q]` into a `TypedPipe[(Q, Long)]`. */
  implicit def TPQ2TPN[P <: Position, Q <: Position, D <: Dimension]: BaseNameable[TypedPipe[Q], P, Q, D, TypedPipe] = {
    new BaseNameable[TypedPipe[Q], P, Q, D, TypedPipe] {
      def convert(m: BaseMatrix[P], s: Slice[P, D], t: TypedPipe[Q])(implicit ev1: ClassTag[s.S],
        ev2: s.S =!= Position0D, ev3: m.NamesTuners#V[Default[NoParameters.type]]): TypedPipe[(Q, Long)] = {
        Names.number(t)
      }
    }
  }

  /** Converts a `PositionListable` into a `TypedPipe[(Q, Long)]`. */
  implicit def PL2TPN[T, P <: Position, Q <: Position, D <: Dimension](implicit ev1: PositionListable[T, Q],
    ev2: PosDimDep[P, D], ev3: ClassTag[Q]): BaseNameable[T, P, Q, D, TypedPipe] = {
    new BaseNameable[T, P, Q, D, TypedPipe] {
      def convert(m: BaseMatrix[P], s: Slice[P, D], t: T)(implicit ev4: ClassTag[s.S], ev5: s.S =!= Position0D,
        ev6: m.NamesTuners#V[Default[NoParameters.type]]): TypedPipe[(Q, Long)] = {
        new Names(m.names(s, Default())(ev2, ev5, ev4, ev6).asInstanceOf[TypedPipe[(Q, Long)]]).slice(t, true)
      }
    }
  }
}

