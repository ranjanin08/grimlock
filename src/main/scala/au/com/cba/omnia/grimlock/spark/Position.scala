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
import au.com.cba.omnia.grimlock.framework.utility.UnionTypes._
import au.com.cba.omnia.grimlock.spark._
import au.com.cba.omnia.grimlock.spark.environment._

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

import shapeless.=:!=

/**
 * Rich wrapper around a `RDD[Position]`.
 *
 * @param data The `RDD[Position]`.
 */
case class Positions[
  L <: Position[L] with ExpandablePosition[L, P],
  P <: Position[P] with ReduceablePosition[P, L]
](
  data: RDD[P]
) extends FwPositions[L, P] with Persist[P] {

  import SparkImplicits._

  type NamesTuners[T] = T In OneOf[Default[NoParameters]]#Or[Default[Reducers]]
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
  ): U[S] = {
    data.map { case p => slice.selected(p) }.tunedDistinct(tuner.parameters)(Position.Ordering[S]())
  }

  def saveAsText(file: String, writer: TextWriter)(implicit ctx: C): U[P] = saveText(file, writer)

  protected def slice(keep: Boolean, f: P => Boolean)(implicit ev: ClassTag[P]): U[P] = {
    data.filter { case p => !keep ^ f(p) }
  }
}

/** Companion object for the Spark `Positions` class. */
object Positions {
  /** Converts a `RDD[Position1D]` to a Spark `Positions`. */
  implicit def RDDP12RDDP(data: RDD[Position1D]): Positions[Position0D, Position1D] = Positions(data)

  /** Converts a `RDD[Position2D]` to a Spark `Positions`. */
  implicit def RDDP22RDDP(data: RDD[Position2D]): Positions[Position1D, Position2D] = Positions(data)

  /** Converts a `RDD[Position3D]` to a Spark `Positions`. */
  implicit def RDDP32RDDP(data: RDD[Position3D]): Positions[Position2D, Position3D] = Positions(data)

  /** Converts a `RDD[Position4D]` to a Spark `Positions`. */
  implicit def RDDP42RDDP(data: RDD[Position4D]): Positions[Position3D, Position4D] = Positions(data)

  /** Converts a `RDD[Position5D]` to a Spark `Positions`. */
  implicit def RDDP52RDDP(data: RDD[Position5D]): Positions[Position4D, Position5D] = Positions(data)

  /** Converts a `RDD[Position6D]` to a Spark `Positions`. */
  implicit def RDDP62RDDP(data: RDD[Position6D]): Positions[Position5D, Position6D] = Positions(data)

  /** Converts a `RDD[Position7D]` to a Spark `Positions`. */
  implicit def RDDP72RDDP(data: RDD[Position7D]): Positions[Position6D, Position7D] = Positions(data)

  /** Converts a `RDD[Position8D]` to a Spark `Positions`. */
  implicit def RDDP82RDDP(data: RDD[Position8D]): Positions[Position7D, Position8D] = Positions(data)

  /** Converts a `RDD[Position9D]` to a Spark `Positions`. */
  implicit def RDDP92RDDP(data: RDD[Position9D]): Positions[Position8D, Position9D] = Positions(data)
}

/** Spark Companion object for the `PositionDistributable` type class. */
object PositionDistributable {
  /** Converts a `RDD[Position]` to a `RDD[Position]`; that is, it's a pass through. */
  implicit def RDDP2RDDPD[P <: Position[P]]: FwPositionDistributable[RDD[P], P, RDD] = {
    new FwPositionDistributable[RDD[P], P, RDD] { def convert(t: RDD[P]): RDD[P] = t }
  }

  /** Converts a `List[Positionable]` to a `RDD[Position]`. */
  implicit def LP2RDDPD[
    T <% Positionable[P],
    P <: Position[P]
  ](implicit
    ctx: Context,
    ct: ClassTag[P]
  ): FwPositionDistributable[List[T], P, RDD] = {
    new FwPositionDistributable[List[T], P, RDD] {
      def convert(t: List[T]): RDD[P] = ctx.context.parallelize(t.map { case p => p() })
    }
  }

  /** Converts a `Positionable` to a `RDD[Position]`. */
  implicit def P2RDDPD[
    T <% Positionable[P],
    P <: Position[P]
  ](implicit
    ctx: Context,
    ct: ClassTag[P]
  ): FwPositionDistributable[T, P, RDD] = {
    new FwPositionDistributable[T, P, RDD] { def convert(t: T): RDD[P] = ctx.context.parallelize(List(t())) }
  }
}

