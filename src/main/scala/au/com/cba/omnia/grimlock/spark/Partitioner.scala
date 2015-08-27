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

package au.com.cba.omnia.grimlock.spark.partition

import au.com.cba.omnia.grimlock.framework.{ Cell, Default, NoParameters, Reducers, Tuner }
import au.com.cba.omnia.grimlock.framework.partition.{ Partition, Partitions => BasePartitions }
import au.com.cba.omnia.grimlock.framework.position._
import au.com.cba.omnia.grimlock.framework.utility._
import au.com.cba.omnia.grimlock.framework.utility.OneOf._

import au.com.cba.omnia.grimlock.spark._

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Rich wrapper around a `RDD[(T, Cell[P])]`.
 *
 * @param data The `RDD[(T, Cell[P])]`.
 */
class Partitions[T: Ordering, P <: Position](val data: RDD[(T, Cell[P])]) extends BasePartitions[T, P]
  with Persist[(T, Cell[P])] {
  type U[A] = RDD[A]

  import SparkImplicits._

  def add(id: T, partition: U[Cell[P]]): U[(T, Cell[P])] = data ++ (partition.map { case c => (id, c) })

  def forEach[Q <: Position](ids: List[T], fn: (T, U[Cell[P]]) => U[Cell[Q]]): U[(T, Cell[Q])] = {
    import Partitions._

    // TODO: This reads the data ids.length times. Is there a way to read it only once?
    ids
      .map { case k => fn(k, data.get(k)).map { case c => (k, c) } }
      .reduce[U[(T, Cell[Q])]]((x, y) => x ++ y)
  }

  def get(id: T): U[Cell[P]] = data.collect { case (t, pc) if (id == t) => pc }

  type IdsTuners = OneOf2[Default[NoParameters.type], Default[Reducers]]
  def ids[N <: Tuner](tuner: N = Default())(implicit ev1: ClassTag[T], ev2: IdsTuners#V[N]): U[T] = {
    data.keys.tunedDistinct(tuner.parameters)
  }

  def merge(ids: List[T]): U[Cell[P]] = data.collect { case (t, c) if (ids.contains(t)) => c }

  def remove(id: T): U[(T, Cell[P])] = data.filter { case (t, c) => t != id }

  def saveAsText(file: String,
    writer: ((T, Cell[P])) => TraversableOnce[String] = Partition.toString()): U[(T, Cell[P])] = {
    saveText(file, writer)
  }
}

/** Companion object for the Spark `Partitions` class. */
object Partitions {
  /** Conversion from `RDD[(T, Cell[P])]` to a Spark `Partitions`. */
  implicit def RDDTC2RDDP[T: Ordering, P <: Position](data: RDD[(T, Cell[P])]): Partitions[T, P] = new Partitions(data)
}

