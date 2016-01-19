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

package au.com.cba.omnia.grimlock.spark.partition

import au.com.cba.omnia.grimlock.framework.{ Cell, Default, NoParameters, Reducers, Tuner }
import au.com.cba.omnia.grimlock.framework.partition.{ Partition, Partitions => FwPartitions }
import au.com.cba.omnia.grimlock.framework.position._
import au.com.cba.omnia.grimlock.framework.utility.OneOf._

import au.com.cba.omnia.grimlock.spark._

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Rich wrapper around a `RDD[(I, Cell[P])]`.
 *
 * @param data The `RDD[(I, Cell[P])]`.
 */
case class Partitions[P <: Position, I: Ordering](data: RDD[(I, Cell[P])]) extends FwPartitions[P, I]
  with Persist[(I, Cell[P])] {

  import SparkImplicits._

  def add(id: I, partition: U[Cell[P]]): U[(I, Cell[P])] = data ++ (partition.map { case c => (id, c) })

  type ForAllTuners = OneOf2[Default[NoParameters.type], Default[Reducers]]
  def forAll[Q <: Position, T <: Tuner](fn: (I, U[Cell[P]]) => U[Cell[Q]], exclude: List[I], tuner: T = Default())(
    implicit ev1: ClassTag[I], ev2: ForAllTuners#V[T]): U[(I, Cell[Q])] = {
    forEach(ids(tuner).toLocalIterator.toList.filter(!exclude.contains(_)), fn)
  }

  def forEach[Q <: Position](ids: List[I], fn: (I, U[Cell[P]]) => U[Cell[Q]]): U[(I, Cell[Q])] = {
    ids
      .map { case i => fn(i, get(i)).map { case c => (i, c) } }
      .reduce[U[(I, Cell[Q])]]((x, y) => x ++ y)
  }

  def get(id: I): U[Cell[P]] = data.collect { case (i, c) if (id == i) => c }

  type IdsTuners = OneOf2[Default[NoParameters.type], Default[Reducers]]
  def ids[T <: Tuner](tuner: T = Default())(implicit ev1: ClassTag[I], ev2: IdsTuners#V[T]): U[I] = {
    data.keys.tunedDistinct(tuner.parameters)
  }

  def merge(ids: List[I]): U[Cell[P]] = data.collect { case (i, c) if (ids.contains(i)) => c }

  def remove(id: I): U[(I, Cell[P])] = data.filter { case (i, _) => i != id }

  def saveAsText(file: String, writer: TextWriter)(implicit ctx: C): U[(I, Cell[P])] = saveText(file, writer)
}

/** Companion object for the Spark `Partitions` class. */
object Partitions {
  /** Conversion from `RDD[(I, Cell[P])]` to a Spark `Partitions`. */
  implicit def RDDTC2RDDP[P <: Position, I: Ordering](data: RDD[(I, Cell[P])]): Partitions[P, I] = Partitions(data)
}

