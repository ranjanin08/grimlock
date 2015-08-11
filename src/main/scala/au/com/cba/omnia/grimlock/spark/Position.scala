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

package au.com.cba.omnia.grimlock.spark.position

import au.com.cba.omnia.grimlock.framework.position.{
  Positions => BasePositions,
  PositionDistributable => BasePositionDistributable,
  _
}
import au.com.cba.omnia.grimlock.framework.utility._

import au.com.cba.omnia.grimlock.spark._

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Rich wrapper around a `RDD[Position]`.
 *
 * @param data The `RDD[Position]`.
 */
class Positions[P <: Position](val data: RDD[P]) extends BasePositions[P] with Persist[P] {
  type U[A] = RDD[A]

  def names[D <: Dimension](slice: Slice[P, D])(implicit ev1: PosDimDep[P, D], ev2: slice.S =!= Position0D,
    ev3: ClassTag[slice.S]): RDD[(slice.S, Long)] = Names.number(data.map { case p => slice.selected(p) }.distinct)
}

/** Companion object for the Spark `Positions` class. */
object Positions {
  /** Converts a `RDD[Position]` to a Spark `Positions`. */
  implicit def RDDP2RDDP[P <: Position](data: RDD[P]): Positions[P] = new Positions(data)
}

/** Spark Companion object for the `PositionDistributable` type class. */
object PositionDistributable {
  /** Converts a `RDD[Position]` to a `RDD[Position]`; that is, it's a pass through. */
  implicit def RDDP2RDDPD[P <: Position]: BasePositionDistributable[RDD[P], P, RDD] = {
    new BasePositionDistributable[RDD[P], P, RDD] { def convert(t: RDD[P]): RDD[P] = t }
  }

  /** Converts a `List[Positionable]` to a `RDD[Position]`. */
  implicit def LP2RDDPD[T, P <: Position](implicit ev: Positionable[T, P], sc: SparkContext,
    ct: ClassTag[P]): BasePositionDistributable[List[T], P, RDD] = {
    new BasePositionDistributable[List[T], P, RDD] {
      def convert(t: List[T]): RDD[P] = sc.parallelize(t.map(ev.convert(_)))
    }
  }

  /** Converts a `Positionable` to a `RDD[Position]`. */
  implicit def P2RDDPD[T, P <: Position](implicit ev: Positionable[T, P], sc: SparkContext,
    ct: ClassTag[P]): BasePositionDistributable[T, P, RDD] = {
    new BasePositionDistributable[T, P, RDD] { def convert(t: T): RDD[P] = sc.parallelize(List(ev.convert(t))) }
  }
}

