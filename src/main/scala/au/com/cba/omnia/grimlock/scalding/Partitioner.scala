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

package au.com.cba.omnia.grimlock.scalding.partition

import au.com.cba.omnia.grimlock.framework.{ Persist => BasePersist, _ }
import au.com.cba.omnia.grimlock.framework.partition.{ Partitions => BasePartitions, _ }
import au.com.cba.omnia.grimlock.framework.position._

import au.com.cba.omnia.grimlock.scalding._

import com.twitter.scalding.typed.{ Grouped, TypedPipe }

import scala.reflect.ClassTag

/**
 * Rich wrapper around a `TypedPipe[(T, Cell[P])]`.
 *
 * @param data The `TypedPipe[(T, Cell[P])]`.
 */
class Partitions[T: Ordering, P <: Position](val data: TypedPipe[(T, Cell[P])]) extends BasePartitions[T, P]
  with Persist[(T, Cell[P])] {
  type U[A] = TypedPipe[A]

  def add(id: T, partition: U[Cell[P]]): U[(T, Cell[P])] = data ++ (partition.map { case c => (id, c) })

  def forEach[Q <: Position](ids: List[T], fn: (T, U[Cell[P]]) => U[Cell[Q]]): U[(T, Cell[Q])] = {
    import Partitions._

    // TODO: This reads the data ids.length times. Is there a way to read it only once?
    //       Perhaps with Grouped.mapGroup and Execution[T]?
    ids
      .map { case k => fn(k, data.get(k)).map { case c => (k, c) } }
      .reduce[U[(T, Cell[Q])]]((x, y) => x ++ y)
  }

  def get(id: T): U[Cell[P]] = data.collect { case (t, pc) if (id == t) => pc }

  def ids()(implicit ev: ClassTag[T]): U[T] = Grouped(data).keys.distinct

  def merge(ids: List[T]): U[Cell[P]] = data.collect { case (t, c) if (ids.contains(t)) => c }

  def remove(id: T): U[(T, Cell[P])] = data.filter { case (t, c) => t != id }
}

/** Companion object for the Scalding `Partitions` class. */
object Partitions {
  /** Conversion from `TypedPipe[(T, Cell[P])]` to a Scalding `Partitions`. */
  implicit def TPTC2TPP[T: Ordering, P <: Position](data: TypedPipe[(T, Cell[P])]): Partitions[T, P] = {
    new Partitions(data)
  }
}

