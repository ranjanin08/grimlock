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

package commbank.grimlock.spark.partition

import commbank.grimlock.framework.{ Cell, Default, NoParameters, Reducers, Tuner }
import commbank.grimlock.framework.partition.{ Partitions => FwPartitions }
import commbank.grimlock.framework.utility.UnionTypes.{ In, OneOf }

import commbank.grimlock.spark.Persist
import commbank.grimlock.spark.SparkImplicits._

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

import shapeless.Nat

/**
 * Rich wrapper around a `RDD[(I, Cell[P])]`.
 *
 * @param data The `RDD[(I, Cell[P])]`.
 */
case class Partitions[
  P <: Nat,
  I : Ordering
](
  data: RDD[(I, Cell[P])]
) extends FwPartitions[P, I]
  with Persist[(I, Cell[P])] {
  def add(id: I, partition: U[Cell[P]]): U[(I, Cell[P])] = data ++ (partition.map(c => (id, c)))

  type ForAllTuners[T] = T In OneOf[Default[NoParameters]]#Or[Default[Reducers]]
  def forAll[
    Q <: Nat,
    T <: Tuner : ForAllTuners
  ](
    fn: (I, U[Cell[P]]) => U[Cell[Q]],
    exclude: List[I],
    tuner: T = Default()
  )(implicit
    ev1: ClassTag[I]
  ): U[(I, Cell[Q])] = forEach(ids(tuner).toLocalIterator.toList.filter(!exclude.contains(_)), fn)

  def forEach[Q <: Nat](ids: List[I], fn: (I, U[Cell[P]]) => U[Cell[Q]]): U[(I, Cell[Q])] = ids
    .map(i => fn(i, get(i)).map(c => (i, c)))
    .reduce[U[(I, Cell[Q])]]((x, y) => x ++ y)

  def get(id: I): U[Cell[P]] = data.collect { case (i, c) if (id == i) => c }

  type IdsTuners[T] = T In OneOf[Default[NoParameters]]#Or[Default[Reducers]]
  def ids[T <: Tuner : IdsTuners](tuner: T = Default())(implicit ev1: ClassTag[I]): U[I] = data
    .keys
    .tunedDistinct(tuner.parameters)

  def merge(ids: List[I]): U[Cell[P]] = data.collect { case (i, c) if (ids.contains(i)) => c }

  def remove(id: I): U[(I, Cell[P])] = data.filter { case (i, _) => i != id }

  def saveAsText(ctx: C, file: String, writer: TextWriter): U[(I, Cell[P])] = saveText(ctx, file, writer)
}

