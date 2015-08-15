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
import au.com.cba.omnia.grimlock.framework.utility.OneOf._

import scala.reflect.ClassTag

/** Base trait for partitioners. */
trait Partitioner[P <: Position, T] extends PartitionerWithValue[P, T] {
  type V = Any

  def assignWithValue(cell: Cell[P], ext: V): TraversableOnce[T] = assign(cell)

  /**
   * Assign the cell to a partition.
   *
   * @param cell The cell to assign to a partition.
   *
   * @return Optional of either a `T` or a `List[T]`, where the instances of `T` identify the partitions.
   */
  def assign(cell: Cell[P]): TraversableOnce[T]
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
  def assignWithValue(cell: Cell[P], ext: V): TraversableOnce[T]
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

  /** Specifies tuner permitted on a call to `ids`. */
  type IdsTuners <: OneOf

  /**
   * Return the partition identifiers.
   *
   * @param tuner The tuner for the job.
   */
  def ids[N <: Tuner](tuner: N)(implicit ev1: ClassTag[T], ev2: IdsTuners#V[N]): U[T]

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
  /** Converts a `(Cell[P]) => String` to a `Partitioner[P, String]`. */
  implicit def SPStr2P[P <: Position]: Partitionable[(Cell[P]) => String, P, String] = SPS2P[P, String]

  /** Converts a `(Cell[P]) => Double` to a `Partitioner[P, Double]`. */
  implicit def SPDbl2P[P <: Position]: Partitionable[(Cell[P]) => Double, P, Double] = SPS2P[P, Double]

  /** Converts a `(Cell[P]) => Long` to a `Partitioner[P, Long]`. */
  implicit def SPLng2P[P <: Position]: Partitionable[(Cell[P]) => Long, P, Long] = SPS2P[P, Long]

  /** Converts a `(Cell[P]) => Int` to a `Partitioner[P, Int]`. */
  implicit def SPInt2P[P <: Position]: Partitionable[(Cell[P]) => Int, P, Int] = SPS2P[P, Int]

  /** Converts a `(Cell[P]) => Boolean` to a `Partitioner[P, Boolean]`. */
  implicit def SPBln2P[P <: Position]: Partitionable[(Cell[P]) => Boolean, P, Boolean] = SPS2P[P, Boolean]

  /** Converts a `(Cell[P]) => S` to a `Partitioner[P, S]`. */
  implicit def SPS2P[P <: Position, S]: Partitionable[(Cell[P]) => S, P, S] = {
    new Partitionable[(Cell[P]) => S, P, S] {
      def convert(t: (Cell[P]) => S): Partitioner[P, S] = {
        new Partitioner[P, S] { def assign(cell: Cell[P]): TraversableOnce[S] = Some(t(cell)) }
      }
    }
  }

  /** Converts a `(Cell[P]) => List[String]` to a `Partitioner[P, String]`. */
  implicit def LSPStr2P[P <: Position]: Partitionable[(Cell[P]) => List[String], P, String] = LSPS2P[P, String]

  /** Converts a `(Cell[P]) => List[Double]` to a `Partitioner[P, Double]`. */
  implicit def LSPDbl2P[P <: Position]: Partitionable[(Cell[P]) => List[Double], P, Double] = LSPS2P[P, Double]

  /** Converts a `(Cell[P]) => List[Long]` to a `Partitioner[P, Long]`. */
  implicit def LSPLng2P[P <: Position]: Partitionable[(Cell[P]) => List[Long], P, Long] = LSPS2P[P, Long]

  /** Converts a `(Cell[P]) => List[Int]` to a `Partitioner[P, Int]`. */
  implicit def LSPInt2P[P <: Position]: Partitionable[(Cell[P]) => List[Int], P, Int] = LSPS2P[P, Int]

  /** Converts a `(Cell[P]) => List[Boolean]` to a `Partitioner[P, Boolean]`. */
  implicit def LSPBln2P[P <: Position]: Partitionable[(Cell[P]) => List[Boolean], P, Boolean] = LSPS2P[P, Boolean]

  /** Converts a `(Cell[P]) => List[S]` to a `Partitioner[P, S]`. */
  implicit def LSPS2P[P <: Position, S]: Partitionable[(Cell[P]) => List[S], P, S] = {
    new Partitionable[(Cell[P]) => List[S], P, S] {
      def convert(t: (Cell[P]) => List[S]): Partitioner[P, S] = {
        new Partitioner[P, S] { def assign(cell: Cell[P]): TraversableOnce[S] = t(cell) }
      }
    }
  }

  /** Converts a `Partitioner[P, String]` to a `Partitioner[P, String]`; that is, it is a pass through. */
  implicit def PPStr2P[P <: Position, T <: Partitioner[P, String]]: Partitionable[T, P, String] = PPS2P[P, String, T]

  /** Converts a `Partitioner[P, Double]` to a `Partitioner[P, Double]`; that is, it is a pass through. */
  implicit def PPDbl2P[P <: Position, T <: Partitioner[P, Double]]: Partitionable[T, P, Double] = PPS2P[P, Double, T]

  /** Converts a `Partitioner[P, Long]` to a `Partitioner[P, Long]`; that is, it is a pass through. */
  implicit def PPLng2P[P <: Position, T <: Partitioner[P, Long]]: Partitionable[T, P, Long] = PPS2P[P, Long, T]

  /** Converts a `Partitioner[P, Int]` to a `Partitioner[P, Int]`; that is, it is a pass through. */
  implicit def PPInt2P[P <: Position, T <: Partitioner[P, Int]]: Partitionable[T, P, Int] = PPS2P[P, Int, T]

  /** Converts a `Partitioner[P, Boolean]` to a `Partitioner[P, Boolean]`; that is, it is a pass through. */
  implicit def PPBln2P[P <: Position, T <: Partitioner[P, Boolean]]: Partitionable[T, P, Boolean] = PPS2P[P, Boolean, T]

  /** Converts a `Partitioner[P, S]` to a `Partitioner[P, S]`; that is, it is a pass through. */
  implicit def PPS2P[P <: Position, S, T <: Partitioner[P, S]]: Partitionable[T, P, S] = {
    new Partitionable[T, P, S] { def convert(t: T): Partitioner[P, S] = t }
  }

  /** Converts a `List[Partitioner[P, String]]` to a single `Partitioner[P, String]`. */
  implicit def LPPStr2P[P <: Position, T <: Partitioner[P, String]]: Partitionable[List[T], P, String] = LPPS2P[P, String, T]

  /** Converts a `List[Partitioner[P, Double]]` to a single `Partitioner[P, Double]`. */
  implicit def LPPDbl2P[P <: Position, T <: Partitioner[P, Double]]: Partitionable[List[T], P, Double] = LPPS2P[P, Double, T]

  /** Converts a `List[Partitioner[P, Long]]` to a single `Partitioner[P, Long]`. */
  implicit def LPPLng2P[P <: Position, T <: Partitioner[P, Long]]: Partitionable[List[T], P, Long] = LPPS2P[P, Long, T]

  /** Converts a `List[Partitioner[P, Int]]` to a single `Partitioner[P, Int]`. */
  implicit def LPPInt2P[P <: Position, T <: Partitioner[P, Int]]: Partitionable[List[T], P, Int] = LPPS2P[P, Int, T]

  /** Converts a `List[Partitioner[P, Boolean]]` to a single `Partitioner[P, Boolean]`. */
  implicit def LPPBln2P[P <: Position, T <: Partitioner[P, Boolean]]: Partitionable[List[T], P, Boolean] = LPPS2P[P, Boolean, T]

  /** Converts a `List[Partitioner[P, S]]` to a single `Partitioner[P, S]`. */
  implicit def LPPS2P[P <: Position, S, T <: Partitioner[P, S]]: Partitionable[List[T], P, S] = {
    new Partitionable[List[T], P, S] {
      def convert(t: List[T]): Partitioner[P, S] = {
        new Partitioner[P, S] {
          def assign(cell: Cell[P]): TraversableOnce[S] = t.flatMap { case s => s.assign(cell) }
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
  /** Converts a `(Cell[P], W) => String` to a `PartitionerWithValue[P, String] { type V >: W }`. */
  implicit def SPStrW2PWV[P <: Position, W]: PartitionableWithValue[(Cell[P], W) => String, P, String, W] = SPSW2PWV[P, String, W]

  /** Converts a `(Cell[P], W) => Double` to a `PartitionerWithValue[P, Double] { type V >: W }`. */
  implicit def SPDblW2PWV[P <: Position, W]: PartitionableWithValue[(Cell[P], W) => Double, P, Double, W] = SPSW2PWV[P, Double, W]

  /** Converts a `(Cell[P], W) => Long` to a `PartitionerWithValue[P, Long] { type V >: W }`. */
  implicit def SPLngW2PWV[P <: Position, W]: PartitionableWithValue[(Cell[P], W) => Long, P, Long, W] = SPSW2PWV[P, Long, W]

  /** Converts a `(Cell[P], W) => Int` to a `PartitionerWithValue[P, Int] { type V >: W }`. */
  implicit def SPIntW2PWV[P <: Position, W]: PartitionableWithValue[(Cell[P], W) => Int, P, Int, W] = SPSW2PWV[P, Int, W]

  /** Converts a `(Cell[P], W) => Boolean` to a `PartitionerWithValue[P, Boolean] { type V >: W }`. */
  implicit def SPBlnW2PWV[P <: Position, W]: PartitionableWithValue[(Cell[P], W) => Boolean, P, Boolean, W] = SPSW2PWV[P, Boolean, W]

  /** Converts a `(Cell[P], W) => S` to a `PartitionerWithValue[P, S] { type V >: W }`. */
  implicit def SPSW2PWV[P <: Position, S, W]: PartitionableWithValue[(Cell[P], W) => S, P, S, W] = {
    new PartitionableWithValue[(Cell[P], W) => S, P, S, W] {
      def convert(t: (Cell[P], W) => S): PartitionerWithValue[P, S] { type V >: W } = {
        new PartitionerWithValue[P, S] {
          type V = W

          def assignWithValue(cell: Cell[P], ext: W): TraversableOnce[S] = Some(t(cell, ext))
        }
      }
    }
  }

  /** Converts a `(Cell[P], W) => List[String]` to a `PartitionerWithValue[P, String] { type V >: W }`. */
  implicit def LSPStrW2PWV[P <: Position, W]: PartitionableWithValue[(Cell[P], W) => List[String], P, String, W] = LSPSW2PWV[P, String, W]

  /** Converts a `(Cell[P], W) => List[Double]` to a `PartitionerWithValue[P, Double] { type V >: W }`. */
  implicit def LSPDblW2PWV[P <: Position, W]: PartitionableWithValue[(Cell[P], W) => List[Double], P, Double, W] = LSPSW2PWV[P, Double, W]

  /** Converts a `(Cell[P], W) => List[Long]` to a `PartitionerWithValue[P, Long] { type V >: W }`. */
  implicit def LSPLngW2PWV[P <: Position, W]: PartitionableWithValue[(Cell[P], W) => List[Long], P, Long, W] = LSPSW2PWV[P, Long, W]

  /** Converts a `(Cell[P], W) => List[Int]` to a `PartitionerWithValue[P, Int] { type V >: W }`. */
  implicit def LSPIntW2PWV[P <: Position, W]: PartitionableWithValue[(Cell[P], W) => List[Int], P, Int, W] = LSPSW2PWV[P, Int, W]

  /** Converts a `(Cell[P], W) => List[Boolean]` to a `PartitionerWithValue[P, Boolean] { type V >: W }`. */
  implicit def LSPBlnW2PWV[P <: Position, W]: PartitionableWithValue[(Cell[P], W) => List[Boolean], P, Boolean, W] = LSPSW2PWV[P, Boolean, W]

  /** Converts a `(Cell[P], W) => List[S]` to a `PartitionerWithValue[P, S] { type V >: W }`. */
  implicit def LSPSW2PWV[P <: Position, S, W]: PartitionableWithValue[(Cell[P], W) => List[S], P, S, W] = {
    new PartitionableWithValue[(Cell[P], W) => List[S], P, S, W] {
      def convert(t: (Cell[P], W) => List[S]): PartitionerWithValue[P, S] { type V >: W } = {
        new PartitionerWithValue[P, S] {
          type V = W

          def assignWithValue(cell: Cell[P], ext: W): TraversableOnce[S] = t(cell, ext)
        }
      }
    }
  }

  /**
   * Converts a `PartitionerWithValue[P, String] { type V >: W }` to a
   * `PartitionerWithValue[P, String] { type V >: W }`; that is, it is a pass through.
   */
  implicit def PPStrW2PWV[P <: Position, T <: PartitionerWithValue[P, String] { type V >: W }, W]: PartitionableWithValue[T, P, String, W] = PPSW2PWV[P, String, T, W]

  /**
   * Converts a `PartitionerWithValue[P, Double] { type V >: W }` to a
   * `PartitionerWithValue[P, Double] { type V >: W }`; that is, it is a pass through.
   */
  implicit def PPDblW2PWV[P <: Position, T <: PartitionerWithValue[P, Double] { type V >: W }, W]: PartitionableWithValue[T, P, Double, W] = PPSW2PWV[P, Double, T, W]

  /**
   * Converts a `PartitionerWithValue[P, Long] { type V >: W }` to a `PartitionerWithValue[P, Long] { type V >: W }`;
   * that is, it is a pass through.
   */
  implicit def PPLngW2PWV[P <: Position, T <: PartitionerWithValue[P, Long] { type V >: W }, W]: PartitionableWithValue[T, P, Long, W] = PPSW2PWV[P, Long, T, W]

  /**
   * Converts a `PartitionerWithValue[P, Int] { type V >: W }` to a `PartitionerWithValue[P, Int] { type V >: W }`;
   * that is, it is a pass through.
   */
  implicit def PPIntW2PWV[P <: Position, T <: PartitionerWithValue[P, Int] { type V >: W }, W]: PartitionableWithValue[T, P, Int, W] = PPSW2PWV[P, Int, T, W]

  /**
   * Converts a `PartitionerWithValue[P, Boolean] { type V >: W }` to a
   * `PartitionerWithValue[P, Boolean] { type V >: W }`; that is, it is a pass through.
   */
  implicit def PPBlnW2PWV[P <: Position, T <: PartitionerWithValue[P, Boolean] { type V >: W }, W]: PartitionableWithValue[T, P, Boolean, W] = PPSW2PWV[P, Boolean, T, W]

  /**
   * Converts a `PartitionerWithValue[P, S] { type V >: W }` to a `PartitionerWithValue[P, S] { type V >: W }`;
   * that is, it is a pass through.
   */
  implicit def PPSW2PWV[P <: Position, S, T <: PartitionerWithValue[P, S] { type V >: W }, W]: PartitionableWithValue[T, P, S, W] = {
    new PartitionableWithValue[T, P, S, W] { def convert(t: T): PartitionerWithValue[P, S] { type V >: W } = t }
  }

  /**
   * Converts a `List[PartitionerWithValue[P, String] { type V >: W }]` to a single
   * `PartitionerWithValue[P, String] { type V >: W }`.
   */
  implicit def LPPStrW2PWV[P <: Position, T <: PartitionerWithValue[P, String] { type V >: W }, W]: PartitionableWithValue[List[T], P, String, W] = LPPSW2PWV[P, String, T, W]

  /**
   * Converts a `List[PartitionerWithValue[P, Double] { type V >: W }]` to a single
   * `PartitionerWithValue[P, Double] { type V >: W }`.
   */
  implicit def LPPDblW2PWV[P <: Position, T <: PartitionerWithValue[P, Double] { type V >: W }, W]: PartitionableWithValue[List[T], P, Double, W] = LPPSW2PWV[P, Double, T, W]

  /**
   * Converts a `List[PartitionerWithValue[P, Long] { type V >: W }]` to a single
   * `PartitionerWithValue[P, Long] { type V >: W }`.
   */
  implicit def LPPLngW2PWV[P <: Position, T <: PartitionerWithValue[P, Long] { type V >: W }, W]: PartitionableWithValue[List[T], P, Long, W] = LPPSW2PWV[P, Long, T, W]

  /**
   * Converts a `List[PartitionerWithValue[P, Int] { type V >: W }]` to a single
   * `PartitionerWithValue[P, Int] { type V >: W }`.
   */
  implicit def LPPIntW2PWV[P <: Position, T <: PartitionerWithValue[P, Int] { type V >: W }, W]: PartitionableWithValue[List[T], P, Int, W] = LPPSW2PWV[P, Int, T, W]

  /**
   * Converts a `List[PartitionerWithValue[P, Boolean] { type V >: W }]` to a single
   * `PartitionerWithValue[P, Boolean] { type V >: W }`.
   */
  implicit def LPPBlnW2PWV[P <: Position, T <: PartitionerWithValue[P, Boolean] { type V >: W }, W]: PartitionableWithValue[List[T], P, Boolean, W] = LPPSW2PWV[P, Boolean, T, W]

  /**
   * Converts a `List[PartitionerWithValue[P, S] { type V >: W }]` to a single
   * `PartitionerWithValue[P, S] { type V >: W }`.
   */
  implicit def LPPSW2PWV[P <: Position, S, T <: PartitionerWithValue[P, S] { type V >: W }, W]: PartitionableWithValue[List[T], P, S, W] = {
    new PartitionableWithValue[List[T], P, S, W] {
      def convert(t: List[T]): PartitionerWithValue[P, S] { type V >: W } = {
        new PartitionerWithValue[P, S] {
          type V = W

          def assignWithValue(cell: Cell[P], ext: V): TraversableOnce[S] = {
            t.flatMap { case s => s.assignWithValue(cell, ext).toList }
          }
        }
      }
    }
  }
}

