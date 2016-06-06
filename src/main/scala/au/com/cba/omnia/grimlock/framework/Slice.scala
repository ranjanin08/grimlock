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
sealed trait Slice[
  P <: Position[P] with ReduceablePosition[P, _],
  S <: Position[S] with ExpandablePosition[S, _],
  R <: Position[R] with ExpandablePosition[R, _]
] {
  /** The dimension of this slice. */
  val dimension: Dimension

  /** Returns the selected coordinate(s) for the given `pos`. */
  def selected(pos: P): S
  /** Returns the remaining coordinate(s) for the given `pos`. */
  def remainder(pos: P): R
}

/**
 * Indicates that the selected coordinate is indexed by `dimension`. In other words, when a groupBy is performed, it is
 * performed using a `Position1D` consisting of the coordinate at index `dimension`.
 *
 * @param dimension Dimension of the selected coordinate.
 */
trait Over[
  L <: Position[L] with ExpandablePosition[L, P],
  P <: Position[P] with ReduceablePosition[P, L]
] extends Slice[P, Position1D, L] {
  def selected(pos: P): Position1D = Position1D(pos(dimension))
  def remainder(pos: P): L = pos.remove(dimension)
}

/** Companion object to `Over` trait. */
object Over {
  /**
   * Indicates that the selected coordinate is indexed by `dimension`. In other words, when a groupBy is performed,
   * it is performed using a `Position1D` consisting of the coordinate at index `dimension`.
   *
   * @param dimension Dimension of the selected coordinate.
   */
  def apply[
    L <: Position[L] with ExpandablePosition[L, P],
    P <: Position[P] with ReduceablePosition[P, L]
  ](
    dimension: Dimension
  )(implicit
    ev: PosDimDep[P, dimension.type]
  ): Over[L, P] = OverImpl(dimension)

  /** Standard `unapply` method for pattern matching. */
  def unapply(over: Over[_, _]): Option[Dimension] = Some(over.dimension)
}

private[position] case class OverImpl[
  L <: Position[L] with ExpandablePosition[L, P],
  P <: Position[P] with ReduceablePosition[P, L]
](
  dimension: Dimension
) extends Over[L, P]

/**
 * Indicates that the selected coordinates are all except the one indexed by `dimension`. In other words, when a
 * groupBy is performed, it is performed using a `Position` (type `ReduceablePosition.L`) consisting of all coordinates
 * except that at index `dimension`.
 */
trait Along[
  L <: Position[L] with ExpandablePosition[L, P],
  P <: Position[P] with ReduceablePosition[P, L]
] extends Slice[P, L, Position1D] {
  def selected(pos: P): L = pos.remove(dimension)
  def remainder(pos: P): Position1D = Position1D(pos(dimension))
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
  def apply[
    L <: Position[L] with ExpandablePosition[L, P],
    P <: Position[P] with ReduceablePosition[P, L]
  ](
    dimension: Dimension
  )(implicit
    ev: PosDimDep[P, dimension.type]
  ): Along[L, P] = AlongImpl(dimension)

  /** Standard `unapply` method for pattern matching. */
  def unapply(along: Along[_, _]): Option[Dimension] = Some(along.dimension)
}

private[position] case class AlongImpl[
  L <: Position[L] with ExpandablePosition[L, P],
  P <: Position[P] with ReduceablePosition[P, L]
](
  dimension: Dimension
) extends Along[L, P]

