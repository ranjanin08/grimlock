// Copyright 2014,2015,2016 Commonwealth Bank of Australia
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

package au.com.cba.omnia.grimlock.framework.position

/** Base trait that encapsulates dimension on which to operate. */
sealed trait Slice[P <: Position] {
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
  val dimension: Dimension

  /** Returns the selected coordinate(s) for the given `pos`. */
  def selected(pos: P): S
  /** Returns the remaining coordinate(s) for the given `pos`. */
  def remainder(pos: P): R
}

private[position] trait Mapable[P <: Position with ReduceablePosition] { self: Slice[P] =>
  protected def remove(pos: P): pos.L = pos.remove(dimension)
  protected def single(pos: P): Position1D = Position1D(pos(dimension))
}

/**
 * Indicates that the selected coordinate is indexed by `dimension`. In other words, when a groupBy is performed, it is
 * performed using a `Position1D` consisting of the coordinate at index `dimension`.
 *
 * @param dimension Dimension of the selected coordinate.
 */
trait Over[P <: Position with ReduceablePosition] extends Slice[P] with Mapable[P] {
  type S = Position1D
  type R = P#L

  def selected(pos: P): S = single(pos)
  def remainder(pos: P): R = remove(pos)
}

/** Companion object to `Over` trait. */
object Over {
  /**
   * Indicates that the selected coordinate is indexed by `dimension`. In other words, when a groupBy is performed,
   * it is performed using a `Position1D` consisting of the coordinate at index `dimension`.
   *
   * @param dimension Dimension of the selected coordinate.
   */
  def apply[P <: Position with ReduceablePosition](dimension: Dimension)(
    implicit ev: PosDimDep[P, dimension.type]): Over[P] = OverImpl(dimension)

  /** Standard `unapply` method for pattern matching. */
  def unapply[P <: Position with ReduceablePosition](over: Over[P]): Option[Dimension] = Some(over.dimension)
}

private[position] case class OverImpl[P <: Position with ReduceablePosition](dimension: Dimension) extends Over[P]

/**
 * Indicates that the selected coordinates are all except the one indexed by `dimension`. In other words, when a
 * groupBy is performed, it is performed using a `Position` (type `ReduceablePosition.L`) consisting of all coordinates
 * except that at index `dimension`.
 */
trait Along[P <: Position with ReduceablePosition] extends Slice[P] with Mapable[P] {
  type S = P#L
  type R = Position1D

  def selected(pos: P): S = remove(pos)
  def remainder(pos: P): R = single(pos)
}

/** Companion object to `Along` trait. */
object Along {
  /**
   * Indicates that the selected coordinates are all except the one indexed by `dimension`. In other words, when a
   * groupBy is performed, it is performed using a `Position` (type `ReduceablePosition.L`) consisting of all
   * coordinates except that at index `dimension`.
   *
   * @param dimension Dimension of the coordinate to exclude.
   */
  def apply[P <: Position with ReduceablePosition](dimension: Dimension)(
    implicit ev: PosDimDep[P, dimension.type]): Along[P] = AlongImpl(dimension)

  /** Standard `unapply` method for pattern matching. */
  def unapply[P <: Position with ReduceablePosition](along: Along[P]): Option[Dimension] = Some(along.dimension)
}

private[position] case class AlongImpl[P <: Position with ReduceablePosition](dimension: Dimension) extends Along[P]

