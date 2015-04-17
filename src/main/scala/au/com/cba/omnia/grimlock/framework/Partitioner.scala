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

import scala.reflect.ClassTag

/** Base trait for partitioning operations. */
trait Partitioner {
  /** Type of the partition assignments. */
  type T
}

/** Base trait for partitioners. */
trait Assign extends AssignWithValue { self: Partitioner =>
  type V = Any

  def assign[P <: Position](pos: P, ext: V): Collection[T] = assign(pos)

  /**
   * Assign the cell to a partition.
   *
   * @param pos The position of the content.
   *
   * @return Optional of either a `T` or a `List[T]`, where the instances of `T` identify the partitions.
   */
  def assign[P <: Position](pos: P): Collection[T]
}

/** Base trait for partitioners that use a user supplied value. */
trait AssignWithValue { self: Partitioner =>
  /** Type of the external value. */
  type V

  /**
   * Assign the cell to a partition using a user supplied value.
   *
   * @param pos The position of the content.
   * @param ext The user supplied value.
   *
   * @return Optional of either a `T` or a `List[T]`, where the instances of `T` identify the partitions.
   */
  def assign[P <: Position](pos: P, ext: V): Collection[T]
}

/** Base trait that represents the partitions of matrices */
trait Partitions[T, P <: Position] {
  /** Type of the underlying data structure (i.e. TypedPipe or RDD). */
  type U[_]

  /**
   * Add a partition.
   *
   * @param id        The partition identifier.
   * @param partition The partition to add.
   *
   * @return A `U[(T, Cell[P])]` containing existing and new paritions.
   */
  def add(id: T, partition: U[Cell[P]]): U[(T, Cell[P])]

  /**
   * Apply function `fn` to each partition in `ids`.
   *
   * @param ids The list of partitions to apply `fn` to.
   * @param fn  The function to apply to each partition.
   *
   * @return A `U[(T, Cell[Q])]` containing the paritions in `ids` with `fn` applied to them.
   */
  def forEach[Q <: Position](ids: List[T], fn: (T, U[Cell[P]]) => U[Cell[Q]]): U[(T, Cell[Q])]

  /**
   * Return the data for the partition `id`.
   *
   * @param id The partition for which to get the data.
   *
   * @return A `U[Cell[P]]`; that is a matrix.
   */
  def get(id: T): U[Cell[P]]

  /** Return the partition identifiers. */
  def ids()(implicit ev: ClassTag[T]): U[T]

  /**
   * Remove a partition.
   *
   * @param id The identifier for the partition to remove.
   *
   * @return A `U[(T, Cell[P])]` with the selected parition removed.
   */
  def remove(id: T): U[(T, Cell[P])]

  protected def toString(t: (T, Cell[P]), separator: String, descriptive: Boolean): String = {
    t._1.toString + separator + t._2.toString(separator, descriptive)
  }
}

