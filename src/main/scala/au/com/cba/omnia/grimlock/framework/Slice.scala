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

package au.com.cba.omnia.grimlock.framework

import au.com.cba.omnia.grimlock.framework.position._

/** Base trait that encapsulates dimension on which to operate. */
sealed trait Slice[P <: Position, D <: Dimension] {
  /**
   * Return type of the `selected` method; a position of dimension less than `P`.
   *
   * @note `S` and `R` together make `P`.
   */
  type S <: Position with ExpandablePosition

  /**
   * Return type of the `remainder` method; a position of dimension less than `P`.
   *
   * @note `S` and `R` together make `P`.
   */
  type R <: Position with ExpandablePosition

  /** The dimension of this slice. */
  val dimension: D

  /** Returns the selected coordinate(s) for the given `pos`. */
  def selected(pos: P): S
  /** Returns the remaining coordinate(s) for the given `pos`. */
  def remainder(pos: P): R

  /** Type of the content in an in-memory version of a matrix. */
  type C

  /** Convert a cell to an in-memory map. */
  def toMap(cell: Cell[P]): Map[S, C]

  /** Combine two in-memory cells. */
  def combineMaps(pos: P, x: Map[S, C], y: Map[S, C]): Map[S, C]
}

trait Mapable[P <: Position with ReduceablePosition, D <: Dimension] { self: Slice[P, D] =>
  protected def remove(pos: P): pos.L = pos.remove(dimension)
  protected def single(pos: P): Position1D = Position1D(pos(dimension))
}

/**
 * Indicates that the selected coordinate is indexed by `dimension`. In other words, when a groupBy is performed, it is
 * performed using a `Position1D` consisting of the coordinate at index `dimension`.
 *
 * @param dimension Dimension of the selected coordinate.
 */
case class Over[P <: Position with ReduceablePosition with MapOverPosition, D <: Dimension](
  dimension: D) extends Slice[P, D] with Mapable[P, D] {
  type S = Position1D
  type R = P#L
  type C = P#O

  def selected(pos: P): S = single(pos)
  def remainder(pos: P): R = remove(pos)

  def toMap(cell: Cell[P]): Map[S, C] = {
    Map(single(cell.position) -> cell.position.over.toMapValue(remove(cell.position), cell.content))
  }
  def combineMaps(pos: P, x: Map[S, C], y: Map[S, C]): Map[S, C] = {
    x ++ y.map {
      case (k, v) => k -> pos.over.combineMapValues(x.get(k).asInstanceOf[Option[pos.O]], v.asInstanceOf[pos.O])
    }
  }
}

/**
 * Indicates that the selected coordinates are all except the one indexed by `dimension`. In other words, when a
 * groupBy is performed, it is performed using a `Position` (type `ReduceablePosition.L`) consisting of all coordinates
 * except that at index `dimension`.
 *
 * @param dimension Dimension of the coordinate to exclude.
 */
case class Along[P <: Position with ReduceablePosition with MapAlongPosition, D <: Dimension](
  dimension: D) extends Slice[P, D] with Mapable[P, D] {
  type S = P#L
  type R = Position1D
  type C = P#A

  def selected(pos: P): S = remove(pos)
  def remainder(pos: P): R = single(pos)

  def toMap(cell: Cell[P]): Map[S, C] = {
    Map(remove(cell.position) -> cell.position.along.toMapValue(single(cell.position), cell.content))
  }
  def combineMaps(pos: P, x: Map[S, C], y: Map[S, C]): Map[S, C] = {
    x ++ y.map {
      case (k, v) => k -> pos.along.combineMapValues(x.get(k).asInstanceOf[Option[pos.A]], v.asInstanceOf[pos.A])
    }
  }
}

