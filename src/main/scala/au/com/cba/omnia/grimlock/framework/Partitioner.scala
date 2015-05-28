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

package au.com.cba.omnia.grimlock.framework.partition

import au.com.cba.omnia.grimlock.framework._
import au.com.cba.omnia.grimlock.framework.position._
import au.com.cba.omnia.grimlock.framework.utility._

import scala.reflect.ClassTag

/** Base trait for partitioners. */
trait Partitioner[P <: Position, T] extends PartitionerWithValue[P, T] {
  type V = Any

  def assignWithValue(cell: Cell[P], ext: V): Collection[T] = assign(cell)

  /**
   * Assign the cell to a partition.
   *
   * @param cell The cell to assign to a partition.
   *
   * @return Optional of either a `T` or a `List[T]`, where the instances of `T` identify the partitions.
   */
  def assign(cell: Cell[P]): Collection[T]
}

/** Base trait for partitioners that use a user supplied value. */
trait PartitionerWithValue[P <: Position, T] {
  /** Type of the external value. */
  type V

  /**
   * Assign the cell to a partition using a user supplied value.
   *
   * @param cell The cell to assign to a partition.
   * @param ext The user supplied value.
   *
   * @return Optional of either a `T` or a `List[T]`, where the instances of `T` identify the partitions.
   */
  def assignWithValue(cell: Cell[P], ext: V): Collection[T]
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
   * Merge partitions into a single matrix.
   *
   * @param ids List of partition keys to merge.
   *
   * @return A `U[Cell[P]]` containing the merge partitions.
   */
  def merge(ids: List[T]): U[Cell[P]]

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

/** Type class for transforming a type `T` to a `Partitioner[P, S]`. */
trait Partitionable[T, P <: Position, S] {
  /**
   * Returns a `Partitioner[P, S]` for type `T`.
   *
   * @param t Object that can be converted to a `Partitioner[P, S]`.
   */
  def convert(t: T): Partitioner[P, S]
}

/** Companion object for the `Partitionable` type class. */
object Partitionable {
  /** Converts a `(Cell[P]) => S` to a `Partitioner[P, S]`. */
  implicit def C2P[P <: Position, S]: Partitionable[(Cell[P]) => S, P, S] = {
    new Partitionable[(Cell[P]) => S, P, S] {
      def convert(t: (Cell[P]) => S): Partitioner[P, S] = {
        new Partitioner[P, S] { def assign(cell: Cell[P]): Collection[S] = Collection(t(cell)) }
      }
    }
  }

  /** Converts a `(Cell[P]) => List[S]` to a `Partitioner[P, S]`. */
  implicit def LC2P[P <: Position, S]: Partitionable[(Cell[P]) => List[S], P, S] = {
    new Partitionable[(Cell[P]) => List[S], P, S] {
      def convert(t: (Cell[P]) => List[S]): Partitioner[P, S] = {
        new Partitioner[P, S] { def assign(cell: Cell[P]): Collection[S] = Collection(t(cell)) }
      }
    }
  }

  /** Converts a `(Cell[P]) => Collection[S]` to a `Partitioner[P, S]`. */
  implicit def CC2P[P <: Position, S]: Partitionable[(Cell[P]) => Collection[S], P, S] = {
    new Partitionable[(Cell[P]) => Collection[S], P, S] {
      def convert(t: (Cell[P]) => Collection[S]): Partitioner[P, S] = {
        new Partitioner[P, S] { def assign(cell: Cell[P]): Collection[S] = t(cell) }
      }
    }
  }

  /** Converts a `Partitioner[P, S]` to a `Partitioner[P, S]`; that is, it is a pass through. */
  implicit def P2P[P <: Position, S, T <: Partitioner[P, S]]: Partitionable[T, P, S] = {
    new Partitionable[T, P, S] { def convert(t: T): Partitioner[P, S] = t }
  }

  /** Converts a `List[Partitioner[P, S]]` to a single `Partitioner[P, S]`. */
  implicit def LP2P[P <: Position, S, T <: Partitioner[P, S]]: Partitionable[List[T], P, S] = {
    new Partitionable[List[T], P, S] {
      def convert(t: List[T]): Partitioner[P, S] = {
        new Partitioner[P, S] {
          def assign(cell: Cell[P]): Collection[S] = Collection(t.flatMap { case s => s.assign(cell).toList })
        }
      }
    }
  }
}

/** Type class for transforming a type `T` to a `PartitionerWithValue[P, S]`. */
trait PartitionableWithValue[T, P <: Position, S, W] {
  /**
   * Returns a `PartitionerWithValue[P, S]` for type `T`.
   *
   * @param t Object that can be converted to a `PartitionerWithValue[P, S]`.
   */
  def convert(t: T): PartitionerWithValue[P, S] { type V >: W }
}

/** Companion object for the `PartitionableWithValue` type class. */
object PartitionableWithValue {
  /** Converts a `(Cell[P], W) => S` to a `PartitionerWithValue[P, S] { type V >: W }`. */
  implicit def CWS2PWV[P <: Position, S, W]: PartitionableWithValue[(Cell[P], W) => S, P, S, W] = {
    new PartitionableWithValue[(Cell[P], W) => S, P, S, W] {
      def convert(t: (Cell[P], W) => S): PartitionerWithValue[P, S] { type V >: W } = {
        new PartitionerWithValue[P, S] {
          type V = W

          def assignWithValue(cell: Cell[P], ext: W): Collection[S] = Collection(t(cell, ext))
        }
      }
    }
  }

  /** Converts a `(Cell[P], W) => List[S]` to a `PartitionerWithValue[P, S] { type V >: W }`. */
  implicit def CWLS2PWV[P <: Position, S, W]: PartitionableWithValue[(Cell[P], W) => List[S], P, S, W] = {
    new PartitionableWithValue[(Cell[P], W) => List[S], P, S, W] {
      def convert(t: (Cell[P], W) => List[S]): PartitionerWithValue[P, S] { type V >: W } = {
        new PartitionerWithValue[P, S] {
          type V = W

          def assignWithValue(cell: Cell[P], ext: W): Collection[S] = Collection(t(cell, ext))
        }
      }
    }
  }

  /** Converts a `(Cell[P], W) => Collection[S]` to a `PartitionerWithValue[P, S] { type V >: W }`. */
  implicit def CWCS2PWV[P <: Position, S, W]: PartitionableWithValue[(Cell[P], W) => Collection[S], P, S, W] = {
    new PartitionableWithValue[(Cell[P], W) => Collection[S], P, S, W] {
      def convert(t: (Cell[P], W) => Collection[S]): PartitionerWithValue[P, S] { type V >: W } = {
        new PartitionerWithValue[P, S] {
          type V = W

          def assignWithValue(cell: Cell[P], ext: W): Collection[S] = t(cell, ext)
        }
      }
    }
  }

  /**
   * Converts a `PartitionerWithValue[P, S] { type V >: W }` to a `PartitionerWithValue[P, S] { type V >: W }`;
   * that is, it is a pass through.
   */
  implicit def PW2PWV[P <: Position, S, T <: PartitionerWithValue[P, S] { type V >: W }, W]: PartitionableWithValue[T, P, S, W] = {
    new PartitionableWithValue[T, P, S, W] { def convert(t: T): PartitionerWithValue[P, S] { type V >: W } = t }
  }

  /**
   * Converts a `List[PartitionerWithValue[P, S] { type V >: W }]` to a single
   * `PartitionerWithValue[P, S] { type V >: W }`.
   */
  implicit def LPW2PWV[P <: Position, S, T <: PartitionerWithValue[P, S] { type V >: W }, W]: PartitionableWithValue[List[T], P, S, W] = {
    new PartitionableWithValue[List[T], P, S, W] {
      def convert(t: List[T]): PartitionerWithValue[P, S] { type V >: W } = {
        new PartitionerWithValue[P, S] {
          type V = W

          def assignWithValue(cell: Cell[P], ext: V): Collection[S] = {
            Collection(t.flatMap { case s => s.assignWithValue(cell, ext).toList })
          }
        }
      }
    }
  }
}

