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

package au.com.cba.omnia.grimlock.scalding.position

import au.com.cba.omnia.grimlock.framework.{ Default, NoParameters, Tuner }
import au.com.cba.omnia.grimlock.framework.position.{
  Positions => BasePositions,
  PositionDistributable => BasePositionDistributable,
  _
}
import au.com.cba.omnia.grimlock.framework.utility._
import au.com.cba.omnia.grimlock.framework.utility.OneOf._

import au.com.cba.omnia.grimlock.scalding._

import com.twitter.scalding.typed.{ IterablePipe, TypedPipe }

import scala.reflect.ClassTag

/**
 * Rich wrapper around a `TypedPipe[Position]`.
 *
 * @param data The `TypedPipe[Position]`.
 */
class Positions[P <: Position](val data: TypedPipe[P]) extends BasePositions[P] with Persist[P] {
  type U[A] = TypedPipe[A]

  type NamesTuners = OneOf1[Default[NoParameters.type]]
  def names[D <: Dimension, T <: Tuner](slice: Slice[P, D], tuner: T = Default())(implicit ev1: PosDimDep[P, D],
    ev2: slice.S =!= Position0D, ev3: ClassTag[slice.S], ev4: NamesTuners#V[T]): U[slice.S] = {
    data.map { case p => slice.selected(p) }.distinct(Position.Ordering[slice.S]())
  }

  def number(): U[(P, Long)] = Names.number(data)

  protected def slice(keep: Boolean, f: P => Boolean)(implicit ev: ClassTag[P]): U[P] = {
    data.filter { case p => !keep ^ f(p) }
  }
}

/** Companion object for the Scalding `Positions` class. */
object Positions {
  /** Converts a `TypedPipe[Position]` to a `Positions`. */
  implicit def TPP2TPP[P <: Position](data: TypedPipe[P]): Positions[P] = new Positions(data)
}

/** Scalding companion object for the `PositionDistributable` type class. */
object PositionDistributable {
  /** Converts a `TypedPipe[Position]` to a `TypedPipe[Position]`; that is, it's a pass through. */
  implicit def TPP2TPPD[P <: Position]: BasePositionDistributable[TypedPipe[P], P, TypedPipe] = {
    new BasePositionDistributable[TypedPipe[P], P, TypedPipe] { def convert(t: TypedPipe[P]): TypedPipe[P] = t }
  }

  /** Converts a `List[Positionable]` to a `TypedPipe[Position]`. */
  implicit def LP2TPPD[T, P <: Position](
    implicit ev: Positionable[T, P]): BasePositionDistributable[List[T], P, TypedPipe] = {
    new BasePositionDistributable[List[T], P, TypedPipe] {
      def convert(t: List[T]): TypedPipe[P] = new IterablePipe(t.map(ev.convert(_)))
    }
  }

  /** Converts a `Positionable` to a `TypedPipe[Position]`. */
  implicit def P2TPPD[T, P <: Position](implicit ev: Positionable[T, P]): BasePositionDistributable[T, P, TypedPipe] = {
    new BasePositionDistributable[T, P, TypedPipe] {
      def convert(t: T): TypedPipe[P] = new IterablePipe(List(ev.convert(t)))
    }
  }
}

