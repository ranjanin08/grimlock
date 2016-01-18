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

package au.com.cba.omnia.grimlock.spark.position

import au.com.cba.omnia.grimlock.framework.{ Default, NoParameters, Reducers, Tuner }
import au.com.cba.omnia.grimlock.framework.position.{
  Positions => FwPositions,
  PositionDistributable => FwPositionDistributable,
  _
}
import au.com.cba.omnia.grimlock.framework.utility._
import au.com.cba.omnia.grimlock.framework.utility.OneOf._

import au.com.cba.omnia.grimlock.spark._

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Rich wrapper around a `RDD[Position]`.
 *
 * @param data The `RDD[Position]`.
 */
class Positions[P <: Position](val data: RDD[P]) extends FwPositions[P] with Persist[P] {
  type U[A] = RDD[A]

  import SparkImplicits._

  type NamesTuners = OneOf2[Default[NoParameters.type], Default[Reducers]]
  def names[T <: Tuner](slice: Slice[P], tuner: T = Default())(implicit ev1: slice.S =!= Position0D,
    ev2: ClassTag[slice.S], ev3: NamesTuners#V[T]): U[slice.S] = {
    data.map { case p => slice.selected(p) }.tunedDistinct(tuner.parameters)(Position.Ordering[slice.S]())
  }

  def saveAsText(file: String, writer: TextWriter = Position.toString()): U[P] = saveText(file, writer)

  protected def slice(keep: Boolean, f: P => Boolean)(implicit ev: ClassTag[P]): U[P] = {
    data.filter { case p => !keep ^ f(p) }
  }
}

/** Companion object for the Spark `Positions` class. */
object Positions {
  /** Converts a `RDD[Position]` to a Spark `Positions`. */
  implicit def RDDP2RDDP[P <: Position](data: RDD[P]): Positions[P] = new Positions(data)
}

/** Spark Companion object for the `PositionDistributable` type class. */
object PositionDistributable {
  /** Converts a `RDD[Position]` to a `RDD[Position]`; that is, it's a pass through. */
  implicit def RDDP2RDDPD[P <: Position]: FwPositionDistributable[RDD[P], P, RDD] = {
    new FwPositionDistributable[RDD[P], P, RDD] { def convert(t: RDD[P]): RDD[P] = t }
  }

  /** Converts a `List[Positionable]` to a `RDD[Position]`. */
  implicit def LP2RDDPD[T <% Positionable[P], P <: Position](implicit sc: SparkContext,
    ct: ClassTag[P]): FwPositionDistributable[List[T], P, RDD] = {
    new FwPositionDistributable[List[T], P, RDD] {
      def convert(t: List[T]): RDD[P] = sc.parallelize(t.map { case p => p() })
    }
  }

  /** Converts a `Positionable` to a `RDD[Position]`. */
  implicit def P2RDDPD[T <% Positionable[P], P <: Position](implicit sc: SparkContext,
    ct: ClassTag[P]): FwPositionDistributable[T, P, RDD] = {
    new FwPositionDistributable[T, P, RDD] { def convert(t: T): RDD[P] = sc.parallelize(List(t())) }
  }
}

