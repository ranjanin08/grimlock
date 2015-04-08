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

import com.twitter.scalding._
import com.twitter.scalding.typed.Grouped

import scala.reflect._

/**
 * Rich wrapper around a `TypedPipe[(T, Cell[P])]`.
 *
 * @param data The `TypedPipe[(T, Cell[P])]`.
 */
class ScaldingPartitions[T: Ordering, P <: Position](val data: TypedPipe[(T, Cell[P])]) extends Partitions[T, P]
  with ScaldingPersist[(T, Cell[P])] {
  type U[A] = TypedPipe[A]

  def add(key: T, partition: TypedPipe[Cell[P]]): TypedPipe[(T, Cell[P])] = {
    data ++ (partition.map { case c => (key, c) })
  }

  def foreach[Q <: Position](keys: List[T],
    fn: (T, TypedPipe[Cell[P]]) => TypedPipe[Cell[Q]]): TypedPipe[(T, Cell[Q])] = {
    import ScaldingPartitions._

    // TODO: This reads the data keys.length times. Is there a way to read it only once?
    //       Perhaps with Grouped.mapGroup and Execution[T]?
    keys
      .map { case k => fn(k, data.get(k)).map { case c => (k, c) } }
      .reduce[TypedPipe[(T, Cell[Q])]]((x, y) => x ++ y)
  }

  def get(key: T): TypedPipe[Cell[P]] = data.collect { case (t, pc) if (key == t) => pc }

  def keys()(implicit ev: ClassTag[T]): TypedPipe[T] = Grouped(data).keys.distinct

  def remove(key: T): TypedPipe[(T, Cell[P])] = data.filter { case (t, c) => t != key }
}

/** Companion object for the `ScaldingPartitions` class. */
object ScaldingPartitions {
  /** Conversion from `TypedPipe[(T, Cell[P])]` to a `ScaldingPartitions`. */
  implicit def TPTC2P[T: Ordering, P <: Position](data: TypedPipe[(T, Cell[P])]): ScaldingPartitions[T, P] = {
    new ScaldingPartitions(data)
  }
}

