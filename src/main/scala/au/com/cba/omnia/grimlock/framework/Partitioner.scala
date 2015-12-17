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

package au.com.cba.omnia.grimlock.framework.partition

import au.com.cba.omnia.grimlock.framework._
import au.com.cba.omnia.grimlock.framework.position._
import au.com.cba.omnia.grimlock.framework.utility._

import scala.reflect.ClassTag

/** Base trait for partitioners. */
trait Partitioner[P <: Position, I] extends PartitionerWithValue[P, I] {
  type V = Any

  def assignWithValue(cell: Cell[P], ext: V): TraversableOnce[I] = assign(cell)

  /**
   * Assign the cell to a partition.
   *
   * @param cell The cell to assign to a partition.
   *
   * @return Zero or more partitition identifiers.
   */
  def assign(cell: Cell[P]): TraversableOnce[I]
}

/** Base trait for partitioners that use a user supplied value. */
trait PartitionerWithValue[P <: Position, I] {
  /** Type of the external value. */
  type V

  /**
   * Assign the cell to a partition using a user supplied value.
   *
   * @param cell The cell to assign to a partition.
   * @param ext  The user supplied value.
   *
   * @return Zero or more partitition identifiers.
   */
  def assignWithValue(cell: Cell[P], ext: V): TraversableOnce[I]
}

/** Base trait that represents the partitions of matrices */
trait Partitions[P <: Position, I] extends RawData {

  /**
   * Add a partition.
   *
   * @param id        The partition identifier.
   * @param partition The partition to add.
   *
   * @return A `U[(I, Cell[P])]` containing existing and new paritions.
   */
  def add(id: I, partition: U[Cell[P]]): U[(I, Cell[P])]

  /** Specifies tuners permitted on a call to `forAll`. */
  type ForAllTuners <: OneOf

  /**
   * Apply function `fn` to all partitions.
   *
   * @param fn      The function to apply to each partition.
   * @param exclude List of partition ids to exclude from applying `fn` to.
   * @param tuner   The tuner for the job.
   *
   * @return A `U[(I, Cell[Q])]` containing the paritions with `fn` applied to them.
   *
   * @note This will pull all partition ids into memory, so only use this if there is sufficient memory
   *       available to keep all (distinct) partition ids in memory.
   */
  def forAll[Q <: Position, T <: Tuner](fn: (I, U[Cell[P]]) => U[Cell[Q]], exclude: List[I] = List(), tuner: T)(
    implicit ev1: ClassTag[I], ev2: ForAllTuners#V[T]): U[(I, Cell[Q])]

  /**
   * Apply function `fn` to each partition in `ids`.
   *
   * @param ids   List of partition ids to apply `fn` to.
   * @param fn    The function to apply to each partition.
   *
   * @return A `U[(I, Cell[Q])]` containing the paritions with `fn` applied to them.
   */
  def forEach[Q <: Position](ids: List[I], fn: (I, U[Cell[P]]) => U[Cell[Q]]): U[(I, Cell[Q])]

  /**
   * Return the data for the partition `id`.
   *
   * @param id The partition for which to get the data.
   *
   * @return A `U[Cell[P]]`; that is a matrix.
   */
  def get(id: I): U[Cell[P]]

  /** Specifies tuners permitted on a call to `ids`. */
  type IdsTuners <: OneOf

  /**
   * Return the partition identifiers.
   *
   * @param tuner The tuner for the job.
   */
  def ids[T <: Tuner](tuner: T)(implicit ev1: ClassTag[I], ev2: IdsTuners#V[T]): U[I]

  /**
   * Merge partitions into a single matrix.
   *
   * @param ids List of partition keys to merge.
   *
   * @return A `U[Cell[P]]` containing the merge partitions.
   */
  def merge(ids: List[I]): U[Cell[P]]

  /**
   * Remove a partition.
   *
   * @param id The identifier for the partition to remove.
   *
   * @return A `U[(I, Cell[P])]` with the selected parition removed.
   */
  def remove(id: I): U[(I, Cell[P])]
}

/** Object for `Partition` functions. */
object Partition {
  /**
   * Return function that returns a string representation of a partition.
   *
   * @param separator   The separator to use between various fields.
   * @param descriptive Indicator if descriptive string is required or not.
   * @param schema      Indicator if schema is required or not (only used if descriptive is `false`).
   */
  def toString[I, P <: Position](separator: String = "|", descriptive: Boolean = false,
    schema: Boolean = true): ((I, Cell[P])) => TraversableOnce[String] = {
    (p: (I, Cell[P])) => Some(p._1.toString + separator + p._2.toString(separator, descriptive, schema))
  }
}

/** Trait for transforming a type `T` to a `Partitioner[P, I]`. */
trait Partitionable[P <: Position, I] extends java.io.Serializable {
  /** Returns a `Partitioner[P, I]` for this type `T`. */
  def apply(): Partitioner[P, I]
}

/** Companion object for the `Partitionable` trait. */
object Partitionable {
  /** Converts a `(Cell[P]) => I` to a `Partitioner[P, S]`. */
  implicit def SPS2P[P <: Position, I](t: (Cell[P]) => I): Partitionable[P, I] = {
    new Partitionable[P, I] {
      def apply(): Partitioner[P, I] = {
        new Partitioner[P, I] { def assign(cell: Cell[P]): TraversableOnce[I] = Some(t(cell)) }
      }
    }
  }

  /** Converts a `(Cell[P]) => List[S]` to a `Partitioner[P, I]`. */
  implicit def LSPS2P[P <: Position, I](t: (Cell[P]) => List[I]): Partitionable[P, I] = {
    new Partitionable[P, I] {
      def apply(): Partitioner[P, I] = {
        new Partitioner[P, I] { def assign(cell: Cell[P]): TraversableOnce[I] = t(cell) }
      }
    }
  }

  /** Converts a `Partitioner[P, I]` to a `Partitioner[P, I]`; that is, it is a pass through. */
  implicit def PPS2P[P <: Position, I](t: Partitioner[P, I]): Partitionable[P, I] = {
    new Partitionable[P, I] { def apply(): Partitioner[P, I] = t }
  }

  /** Converts a `List[Partitioner[P, I]]` to a single `Partitioner[P, I]`. */
  implicit def LPPS2P[P <: Position, I](t: List[Partitioner[P, I]]): Partitionable[P, I] = {
    new Partitionable[P, I] {
      def apply(): Partitioner[P, I] = {
        new Partitioner[P, I] {
          def assign(cell: Cell[P]): TraversableOnce[I] = t.flatMap { case s => s.assign(cell) }
        }
      }
    }
  }
}

/** Trait for transforming a type `T` to a `PartitionerWithValue[P, I]`. */
trait PartitionableWithValue[P <: Position, I, W] extends java.io.Serializable {
  /** Returns a `PartitionerWithValue[P, I]` for this type `T`. */
  def apply(): PartitionerWithValue[P, I] { type V >: W }
}

/** Companion object for the `PartitionableWithValue` trait. */
object PartitionableWithValue {
  /** Converts a `(Cell[P], W) => I` to a `PartitionerWithValue[P, S] { type V >: W }`. */
  implicit def SPSW2PWV[P <: Position, I, W](t: (Cell[P], W) => I): PartitionableWithValue[P, I, W] = {
    new PartitionableWithValue[P, I, W] {
      def apply(): PartitionerWithValue[P, I] { type V >: W } = {
        new PartitionerWithValue[P, I] {
          type V = W

          def assignWithValue(cell: Cell[P], ext: W): TraversableOnce[I] = Some(t(cell, ext))
        }
      }
    }
  }

  /** Converts a `(Cell[P], W) => List[I]` to a `PartitionerWithValue[P, I] { type V >: W }`. */
  implicit def LSPSW2PWV[P <: Position, I, W](t: (Cell[P], W) => List[I]): PartitionableWithValue[P, I, W] = {
    new PartitionableWithValue[P, I, W] {
      def apply(): PartitionerWithValue[P, I] { type V >: W } = {
        new PartitionerWithValue[P, I] {
          type V = W

          def assignWithValue(cell: Cell[P], ext: W): TraversableOnce[I] = t(cell, ext)
        }
      }
    }
  }

  /**
   * Converts a `PartitionerWithValue[P, I] { type V >: W }` to a `PartitionerWithValue[P, I] { type V >: W }`;
   * that is, it is a pass through.
   */
  implicit def PPSW2PWV[P <: Position, I, W](
    t: PartitionerWithValue[P, I] { type V >: W }): PartitionableWithValue[P, I, W] = {
    new PartitionableWithValue[P, I, W] { def apply(): PartitionerWithValue[P, I] { type V >: W } = t }
  }

  /**
   * Converts a `List[PartitionerWithValue[P, I] { type V >: W }]` to a single
   * `PartitionerWithValue[P, I] { type V >: W }`.
   */
  implicit def LPPSW2PWV[P <: Position, I, W](
    t: List[PartitionerWithValue[P, I] { type V >: W }]): PartitionableWithValue[P, I, W] = {
    new PartitionableWithValue[P, I, W] {
      def apply(): PartitionerWithValue[P, I] { type V >: W } = {
        new PartitionerWithValue[P, I] {
          type V = W

          def assignWithValue(cell: Cell[P], ext: V): TraversableOnce[I] = {
            t.flatMap { case s => s.assignWithValue(cell, ext).toList }
          }
        }
      }
    }
  }
}

