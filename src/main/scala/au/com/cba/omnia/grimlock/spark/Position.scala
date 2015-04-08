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

import org.apache.spark.SparkContext
import org.apache.spark.rdd._

import scala.reflect._

/**
 * Rich wrapper around a `RDD[Position]`.
 *
 * @param data The `RDD[Position]`.
 */
class SparkPositions[P <: Position](val data: RDD[P]) extends Positions[P] with SparkPersist[P] {
  type U[A] = RDD[A]

  /**
   * Returns the distinct position(s) (or names) for a given `slice`.
   *
   * @param slice Encapsulates the dimension(s) for which the names are to be returned.
   *
   * @return A Spark `RDD[(Slice.S, Long)]` of the distinct position(s) together with a unique index.
   *
   * @note The position(s) are returned with an index so the return value can be used in various `persist` methods. The
   *       index itself is unique for each position but no ordering is defined.
   *
   * @see [[Names]]
   */
  def names[D <: Dimension](slice: Slice[P, D])(implicit ev1: PosDimDep[P, D],
    ev2: ClassTag[slice.S]): RDD[(slice.S, Long)] = {
    SparkNames.number(data
      .map { case p => slice.selected(p) }
      .distinct)
  }
}

/** Companion object for the `SparkPositions` class. */
object SparkPositions {
  /** Converts a `RDD[Position]` to a `SparkPositions`. */
  implicit def RDDP2P[P <: Position](data: RDD[P]): SparkPositions[P] = new SparkPositions(data)
}

/** Spark Companion object for the `PositionDistributable` type class. */
object SparkPositionDistributable {
  /** Converts a `RDD[Position]` to a `RDD[Position]`; that is, it's a pass through. */
  implicit def RDDP2PD[P <: Position]: PositionDistributable[RDD[P], P, RDD] = {
    new PositionDistributable[RDD[P], P, RDD] { def convert(t: RDD[P]): RDD[P] = t }
  }
  /** Converts a `List[Positionable]` to a `RDD[Position]`. */
  implicit def LP2PD[T, P <: Position](implicit ev: Positionable[T, P], sc: SparkContext,
    ct: ClassTag[P]): PositionDistributable[List[T], P, RDD] = {
    new PositionDistributable[List[T], P, RDD] {
      def convert(t: List[T]): RDD[P] = sc.parallelize(t.map(ev.convert(_)))
    }
  }
  /** Converts a `Positionable` to a `RDD[Position]`. */
  implicit def P2PD[T, P <: Position](implicit ev: Positionable[T, P], sc: SparkContext,
    ct: ClassTag[P]): PositionDistributable[T, P, RDD] = {
    new PositionDistributable[T, P, RDD] { def convert(t: T): RDD[P] = sc.parallelize(List(ev.convert(t))) }
  }
}

