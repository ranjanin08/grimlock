// Copyright 2015 Commonwealth Bank of Australia
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

object Locate {
  /**
   * Extract position for left and right cells.
   *
   * @note An `Option` is returned to allow for additional filtering.
   */
  type Operator[P <: Position, Q <: Position] = (Cell[P], Cell[P]) => Option[Q]

  /**
   * Extract position use a name pattern.
   *
   * @param slice     Encapsulates the dimension(s) from which to construct the name.
   * @param pattern   Name pattern of the new coordinate. Use `%[12]$``s` for the string representations of the
   *                  left and right selected positions respectively.
   * @param all       Indicates if all positions should be returned (true), or only if left and right remainder
   *                  are equal.
   * @param separator Separator to use when converting left and right positions to string.
   *
   * @note If a position is returned then it's always right cell's remainder with an additional coordinate prepended.
   */
  def OperatorString[P <: Position](slice: Slice[P], pattern: String, all: Boolean = false,
    separator: String = "|"): Operator[P, slice.R#M] = {
    (left: Cell[P], right: Cell[P]) =>
      {
        val reml = slice.remainder(left.position)
        val remr = slice.remainder(right.position)

        (all || reml == remr) match {
          case true => Some(reml.prepend(pattern.format(slice.selected(left.position).toShortString(separator),
            slice.selected(right.position).toShortString(separator))))
          case false => None
        }
      }
  }

  /** Extract position for the selected cell and its remainder. */
  type WindowSize1[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position] = (Cell[S], R) => Q

  /**
   * Extract position using a dimension.
   *
   * @param dim The dimension (out of `rem`) to append to the cell's position.
   */
  def WindowDimension[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition](
    dim: Dimension): WindowSize1[S, R, S#M] = (cell: Cell[S], rem: R) => cell.position.append(rem(dim))

  /**
   * Extract position using string of `rem`.
   *
   * @param separator The separator to use for the appended coordinate.
   */
  def WindowString[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition](
    separator: String = "|"): WindowSize1[S, R, S#M] = {
    (cell: Cell[S], rem: R) => cell.position.append(rem.toShortString(separator))
  }

  /** Extract position for the selected cell and its current and prior remainder. */
  type WindowSize2[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position] = (Cell[S], R, R) => Q

  /**
   * Extract position using string of current and previous `rem`.
   *
   * @param pattern   Name pattern of the new coordinate. Use `%[12]$``s` for the string representations of the
   *                  previous and current remainder positions respectively.
   * @param separator The separator to use for the appended coordinate.
   */
  def WindowPairwiseString[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition](
    pattern: String, separator: String = "|"): WindowSize2[S, R, S#M] = {
    (cell: Cell[S], curr: R, prev: R) =>
      cell.position.append(pattern.format(prev.toShortString(separator), curr.toShortString(separator)))
  }
}

