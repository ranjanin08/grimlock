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

package au.com.cba.omnia.grimlock.framework

import au.com.cba.omnia.grimlock.framework.position._

import scala.reflect.ClassTag
import scala.util.matching.Regex

/** Base trait that represents the names along the dimensions of a matrix. */
trait Names[P <: Position] {
  /** Type of the underlying data structure (i.e. TypedPipe or RDD). */
  type U[_]

  /**
   * Renumber the indices such that `position` is last.
   *
   * @param position The position to move to the back.
   *
   * @return A `U[(P, Long)]` with `position` at the greatest index and all others renumbered but preserving
   *         their relative ordering.
   */
  def moveToBack[T](position: T)(implicit ev1: Positionable[T, P], ev2: ClassTag[P]): U[(P, Long)]

  /**
   * Renumber the indices such that `position` is first.
   *
   * @param position The position to move to the front.
   *
   * @return A `U[(P, Long)]` with `position` at index 0 and all others renumbered but preserving their
   *         relative ordering.
   */
  def moveToFront[T](position: T)(implicit ev1: Positionable[T, P], ev2: ClassTag[P]): U[(P, Long)]

  /** Renumber the names. */
  def renumber()(implicit ev: ClassTag[P]): U[(P, Long)]

  /**
   * Set the index of a single position.
   *
   * @param position The position whose index is to be set.
   * @param index    The new index.
   *
   * @return A `U[(P, Long)]` with only the index at the specified position updated.
   */
  def set[T](position: T, index: Long)(implicit ev: Positionable[T, P]): U[(P, Long)] = {
    set(Map(position -> index))
  }

  /**
   * Set the index of multiple positions.
   *
   * @param positions A `Map` of positions (together with their new index) whose index is to be set.
   *
   * @return A `U[(P, Long)]` with only the indices at the specified positions updated.
   */
  def set[T](positions: Map[T, Long])(implicit ev: Positionable[T, P]): U[(P, Long)]

  /**
   * Slice the names using a regular expression.
   *
   * @param regex     The regular expression to match on.
   * @param keep      Indicator if the matched names should be kept or removed.
   * @param spearator Separator used to convert each position to string.
   *
   * @return A `U[(P, Long)]` with only the names of interest.
   *
   * @note The matching is done by converting each position to its short string reprensentation and then applying the
   *       regular expression.
   */
  def slice(regex: Regex, keep: Boolean, separator: String)(implicit ev: ClassTag[P]): U[(P, Long)] = {
    slice(keep, p => regex.pattern.matcher(p.toShortString(separator)).matches)
  }

  /**
   * Slice the names using one or more positions.
   *
   * @param positions The positions to slice on.
   * @param keep      Indicator if the matched names should be kept or removed.
   *
   * @return A `U[(P, Long)]` with only the names of interest.
   */
  def slice[T](positions: T, keep: Boolean)(implicit ev1: PositionListable[T, P], ev2: ClassTag[P]): U[(P, Long)] = {
    slice(keep, p => ev1.convert(positions).contains(p))
  }

  protected def slice(keep: Boolean, f: P => Boolean)(implicit ev: ClassTag[P]): U[(P, Long)]

  protected def toString(t: (P, Long), separator: String, descriptive: Boolean): String = {
    descriptive match {
      case true => t._1.toString + separator + t._2.toString
      case false => t._1.toShortString(separator) + separator + t._2.toString
    }
  }
}

/** Type class for transforming a type `T` into a `U[(Q, Long)]`. */
trait Nameable[T, P <: Position, Q <: Position, D <: Dimension, U[_]] {
  /**
   * Returns a `U[(Q, Long)]` for type `T`.
   *
   * @param m The matrix to get names from.
   * @param s Encapsulates the dimension(s) for which to get names.
   * @param t Object that can be converted to a `U[(Q, Long)]`.
   */
  def convert(m: Matrix[P], s: Slice[P, D], t: T)(implicit ev: ClassTag[s.S]): U[(Q, Long)]
}

