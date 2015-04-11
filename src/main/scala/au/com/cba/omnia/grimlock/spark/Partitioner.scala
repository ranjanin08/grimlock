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

package au.com.cba.omnia.grimlock.partition

import au.com.cba.omnia.grimlock._
import au.com.cba.omnia.grimlock.content._
import au.com.cba.omnia.grimlock.position._
import au.com.cba.omnia.grimlock.utility._

import org.apache.spark.rdd._

import scala.reflect.ClassTag

/**
 * Rich wrapper around a `RDD[(T, Cell[P])]`.
 *
 * @param data The `RDD[(T, Cell[P])]`.
 */
class SparkPartitions[T: Ordering, P <: Position](val data: RDD[(T, Cell[P])]) extends Partitions[T, P]
  with SparkPersist[(T, Cell[P])] {
  type U[A] = RDD[A]

  def add(key: T, partition: RDD[Cell[P]]): RDD[(T, Cell[P])] = data ++ (partition.map { case c => (key, c) })

  def forEach[Q <: Position](keys: List[T], fn: (T, RDD[Cell[P]]) => RDD[Cell[Q]]): RDD[(T, Cell[Q])] = {
    import SparkPartitions._

    // TODO: This reads the data keys.length times. Is there a way to read it only once?
    keys
      .map { case k => fn(k, data.get(k)).map { case c => (k, c) } }
      .reduce[RDD[(T, Cell[Q])]]((x, y) => x ++ y)
  }

  def get(key: T): RDD[Cell[P]] = data.collect { case (t, pc) if (key == t) => pc }

  def keys()(implicit ev: ClassTag[T]): RDD[T] = data.keys.distinct

  def remove(key: T): RDD[(T, Cell[P])] = data.filter { case (t, c) => t != key }
}

/** Companion object for the `SparkPartitions` class. */
object SparkPartitions {
  /** Conversion from `RDD[(T, Cell[P])]` to a `SparkPartitions`. */
  implicit def RDDTC2RDDP[T: Ordering, P <: Position](data: RDD[(T, Cell[P])]): SparkPartitions[T, P] = {
    new SparkPartitions(data)
  }
}

