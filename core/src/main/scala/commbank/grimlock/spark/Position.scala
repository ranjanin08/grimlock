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

package commbank.grimlock.spark.position

import commbank.grimlock.framework.{ Default, NoParameters, Reducers, Tuner }
import commbank.grimlock.framework.position.{
  Position,
  Positions => FwPositions,
  Slice
}
import commbank.grimlock.framework.utility.=:!=
import commbank.grimlock.framework.utility.UnionTypes.{ In, OneOf }

import commbank.grimlock.spark.Persist
import commbank.grimlock.spark.SparkImplicits._

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

import shapeless.Nat
import shapeless.nat.{ _0, _1 }
import shapeless.ops.nat.Diff

/**
 * Rich wrapper around a `RDD[Position]`.
 *
 * @param data The `RDD[Position]`.
 */
case class Positions[L <: Nat, P <: Nat](data: RDD[Position[P]]) extends FwPositions[L, P] with Persist[Position[P]] {
  type NamesTuners[T] = T In OneOf[Default[NoParameters]]#Or[Default[Reducers]]
  def names[
    T <: Tuner : NamesTuners
  ](
    slice: Slice[L, P]
  )(
    tuner: T = Default()
  )(implicit
    ev1: slice.S =:!= _0,
    ev2: ClassTag[Position[slice.S]],
    ev3: Diff.Aux[P, _1, L]
  ): U[Position[slice.S]] = data.map(p => slice.selected(p)).tunedDistinct(tuner.parameters)(Position.ordering())

  def saveAsText(ctx: C, file: String, writer: TextWriter): U[Position[P]] = saveText(ctx, file, writer)

  protected def slice(
    keep: Boolean,
    f: Position[P] => Boolean
  )(implicit
    ev: ClassTag[Position[P]]
  ): U[Position[P]] = data.filter { case p => !keep ^ f(p) }
}

