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

package au.com.cba.omnia.grimlock.scalding.position

import au.com.cba.omnia.grimlock.framework.{ Default, NoParameters, Tuner }
import au.com.cba.omnia.grimlock.framework.position.{
  Positions => FwPositions,
  PositionDistributable => FwPositionDistributable,
  _
}

import au.com.cba.omnia.grimlock.framework.utility.UnionTypes._

import au.com.cba.omnia.grimlock.scalding._

import com.twitter.scalding.typed.{ IterablePipe, TypedPipe }

import scala.reflect.ClassTag

import shapeless.=:!=

/**
 * Rich wrapper around a `TypedPipe[Position]`.
 *
 * @param data The `TypedPipe[Position]`.
 */
case class Positions[
  L <: Position[L] with ExpandablePosition[L, P],
  P <: Position[P] with ReduceablePosition[P, L]
](
  data: TypedPipe[P]
) extends FwPositions[L, P] with Persist[P] {
  type NamesTuners[T] = T Is Default[NoParameters]
  def names[
    S <: Position[S] with ExpandablePosition[S, _],
    R <: Position[R] with ExpandablePosition[R, _],
    T <: Tuner : NamesTuners
  ](
    slice: Slice[L, P, S, R],
    tuner: T = Default()
  )(implicit
    ev1: S =:!= Position0D,
    ev2: ClassTag[S]
  ): U[S] = data.map { case p => slice.selected(p) }.distinct(Position.Ordering[S]())

  def saveAsText(file: String, writer: TextWriter)(implicit ctx: C): U[P] = saveText(file, writer)

  protected def slice(keep: Boolean, f: P => Boolean)(implicit ev: ClassTag[P]): U[P] = {
    data.filter { case p => !keep ^ f(p) }
  }
}

/** Companion object for the Scalding `Positions` class. */
object Positions {
  /** Converts a `TypedPipe[Position1D]` to a `Positions`. */
  implicit def TPP12TPP(data: TypedPipe[Position1D]): Positions[Position0D, Position1D] = Positions(data)

  /** Converts a `TypedPipe[Position2D]` to a `Positions`. */
  implicit def TPP22TPP(data: TypedPipe[Position2D]): Positions[Position1D, Position2D] = Positions(data)

  /** Converts a `TypedPipe[Position3D]` to a `Positions`. */
  implicit def TPP32TPP(data: TypedPipe[Position3D]): Positions[Position2D, Position3D] = Positions(data)

  /** Converts a `TypedPipe[Position4D]` to a `Positions`. */
  implicit def TPP42TPP(data: TypedPipe[Position4D]): Positions[Position3D, Position4D] = Positions(data)

  /** Converts a `TypedPipe[Position5D]` to a `Positions`. */
  implicit def TPP52TPP(data: TypedPipe[Position5D]): Positions[Position4D, Position5D] = Positions(data)

  /** Converts a `TypedPipe[Position6D]` to a `Positions`. */
  implicit def TPP62TPP(data: TypedPipe[Position6D]): Positions[Position5D, Position6D] = Positions(data)

  /** Converts a `TypedPipe[Position7D]` to a `Positions`. */
  implicit def TPP72TPP(data: TypedPipe[Position7D]): Positions[Position6D, Position7D] = Positions(data)

  /** Converts a `TypedPipe[Position8D]` to a `Positions`. */
  implicit def TPP82TPP(data: TypedPipe[Position8D]): Positions[Position7D, Position8D] = Positions(data)

  /** Converts a `TypedPipe[Position9D]` to a `Positions`. */
  implicit def TPP92TPP(data: TypedPipe[Position9D]): Positions[Position8D, Position9D] = Positions(data)
}

/** Scalding companion object for the `PositionDistributable` type class. */
object PositionDistributable {
  /** Converts a `TypedPipe[Position]` to a `TypedPipe[Position]`; that is, it's a pass through. */
  implicit def TPP2TPPD[P <: Position[P]]: FwPositionDistributable[TypedPipe[P], P, TypedPipe] = {
    new FwPositionDistributable[TypedPipe[P], P, TypedPipe] { def convert(t: TypedPipe[P]): TypedPipe[P] = t }
  }

  /** Converts a `List[Positionable]` to a `TypedPipe[Position]`. */
  implicit def LP2TPPD[T <% Positionable[P], P <: Position[P]]: FwPositionDistributable[List[T], P, TypedPipe] = {
    new FwPositionDistributable[List[T], P, TypedPipe] {
      def convert(t: List[T]): TypedPipe[P] = IterablePipe(t.map { case p => p() })
    }
  }

  /** Converts a `Positionable` to a `TypedPipe[Position]`. */
  implicit def P2TPPD[T <% Positionable[P], P <: Position[P]]: FwPositionDistributable[T, P, TypedPipe] = {
    new FwPositionDistributable[T, P, TypedPipe] {
      def convert(t: T): TypedPipe[P] = IterablePipe(List(t()))
    }
  }
}

