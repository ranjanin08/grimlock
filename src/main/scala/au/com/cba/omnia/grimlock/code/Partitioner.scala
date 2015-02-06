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

import au.com.cba.omnia.grimlock.content._
import au.com.cba.omnia.grimlock.Matrix.Cell
import au.com.cba.omnia.grimlock.position._
import au.com.cba.omnia.grimlock.utility.Miscellaneous.Collection

import cascading.flow.FlowDef
import com.twitter.scalding._
import com.twitter.scalding.TDsl._, Dsl._
import com.twitter.scalding.typed.TypedSink

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

/**
 * Rich wrapper around a `TypedPipe[(T, (Position, Content))]`.
 *
 * @param data The `TypedPipe[(T, (Position, Content))]`.
 */
class Partitions[T: Ordering, P <: Position](data: TypedPipe[(T, Cell[P])]) {
  // TODO: Add 'keys'/'hasKey'/'set'/'modify' methods?
  // TODO: Add 'foreach' method - to apply function to all data for each key

  /**
   * Return the data for the partition `key`.
   *
   * @param key The partition for which to get the data.
   *
   * @return A `TypedPipe[(Position, Content)]`; that is a matrix.
   */
  def get(key: T): TypedPipe[Cell[P]] = data.collect { case (t, pc) if (key == t) => pc }

  /**
   * Persist partitions to disk.
   *
   * @param file        Name of the output file.
   * @param separator   Separator to use between `T`, the position and content.
   * @param descriptive Indicates if the output should be descriptive.
   *
   * @return A Scalding `TypedPipe[(T, (Position, Content))]` which is this object's data.
   */
  def persistFile(file: String, separator: String = "|", descriptive: Boolean = false)(implicit flow: FlowDef,
    mode: Mode): TypedPipe[(T, Cell[P])] = persist(TypedSink(TextLine(file)), separator, descriptive)

  /**
   * Persist partitions to a sink.
   *
   * @param sink        Sink to write to.
   * @param separator   Separator to use between `T`, the position and content.
   * @param descriptive Indicates if the output should be descriptive.
   *
   * @return A Scalding `TypedPipe[(T, (Position, Content))]` which is this object's data.
   */
  def persist(sink: TypedSink[String], separator: String = "|", descriptive: Boolean = false)(implicit flow: FlowDef,
    mode: Mode): TypedPipe[( T, Cell[P])] = {
    data
      .map {
        case (t, (p, c)) => descriptive match {
          case true => t.toString + separator + p.toString + separator + c.toString
          case false => t.toString + separator + p.toShortString(separator) + separator + c.toShortString(separator)
        }
      }
      .write(sink)

    data
  }
}

object Partitions {
  /** Conversion from `TypedPipe[(T, (Position, Content))]` to a `Partitions`. */
  implicit def TPTPC2P[T: Ordering, P <: Position](data: TypedPipe[(T, Cell[P])]): Partitions[T, P] = {
    new Partitions(data)
  }
}

