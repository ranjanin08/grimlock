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
import au.com.cba.omnia.grimlock.utility._

import com.twitter.scalding._
import com.twitter.scalding.typed.IterablePipe

import scala.reflect.ClassTag

/**
 * Rich wrapper around a `TypedPipe[Position]`.
 *
 * @param data The `TypedPipe[Position]`.
 */
class ScaldingPositions[P <: Position](val data: TypedPipe[P]) extends Positions[P] with ScaldingPersist[P] {
  type U[A] = TypedPipe[A]

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
  def names[D <: Dimension](slice: Slice[P, D])(implicit ev1: PosDimDep[P, D], ev2: slice.S =!= Position0D,
    ev3: ClassTag[slice.S]): TypedPipe[(slice.S, Long)] = {
    ScaldingNames.number(data
      .map { case p => slice.selected(p) }
      .distinct(Position.Ordering[slice.S]))
  }
}

/** Companion object for the `ScaldingPositions` class. */
object ScaldingPositions {
  /** Converts a `TypedPipe[Position]` to a `ScaldingPositions`. */
  implicit def TPP2TPP[P <: Position](data: TypedPipe[P]): ScaldingPositions[P] = new ScaldingPositions(data)
}

/** Scalding Companion object for the `PositionDistributable` type class. */
object ScaldingPositionDistributable {
  /** Converts a `TypedPipe[Position]` to a `TypedPipe[Position]`; that is, it's a pass through. */
  implicit def TPP2TPPD[P <: Position]: PositionDistributable[TypedPipe[P], P, TypedPipe] = {
    new PositionDistributable[TypedPipe[P], P, TypedPipe] { def convert(t: TypedPipe[P]): TypedPipe[P] = t }
  }
  /** Converts a `List[Positionable]` to a `TypedPipe[Position]`. */
  implicit def LP2TPPD[T, P <: Position](
    implicit ev: Positionable[T, P]): PositionDistributable[List[T], P, TypedPipe] = {
    new PositionDistributable[List[T], P, TypedPipe] {
      def convert(t: List[T]): TypedPipe[P] = new IterablePipe(t.map(ev.convert(_)))
    }
  }
  /** Converts a `Positionable` to a `TypedPipe[Position]`. */
  implicit def P2TPPD[T, P <: Position](implicit ev: Positionable[T, P]): PositionDistributable[T, P, TypedPipe] = {
    new PositionDistributable[T, P, TypedPipe] {
      def convert(t: T): TypedPipe[P] = new IterablePipe(List(ev.convert(t)))
    }
  }
}

