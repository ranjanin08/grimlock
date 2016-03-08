// Copyright 2015,2016 Commonwealth Bank of Australia
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

import au.com.cba.omnia.grimlock.framework.content._
import au.com.cba.omnia.grimlock.framework.encoding._
import au.com.cba.omnia.grimlock.framework.position._

object Locate {
  /** Extract position from cell. */
  type FromCell[P <: Position, Q <: Position] = (Cell[P]) => Option[Q]

  /** Extract position from cell with user provided value. */
  type FromCellWithValue[P <: Position, Q <: Position, V] = (Cell[P], V) => Option[Q]

  /** Extract position from cell and an optional value. */
  type FromCellAndOptionalValue[P <: Position, Q <: Position] = (Cell[P], Option[Value]) => Option[Q]

  /** Extract position for left and right cells. */
  type FromPairwiseCells[P <: Position, Q <: Position] = (Cell[P], Cell[P]) => Option[Q]

  /** Extract position for the selected cell and its remainder. */
  type FromSelectedAndRemainder[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position] = (S, R) => Option[Q]

  /** Extract position for the selected cell and its current and prior remainder. */
  type FromSelectedAndPairwiseRemainder[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position] = (S, R, R) => Option[Q]

  /** Extract position from selected position and a value. */
  type FromSelectedAndOutput[S <: Position with ExpandablePosition, T, Q <: Position] = (S, T) => Option[Q]

  /** Extract position from selected position and a content. */
  type FromSelectedAndContent[S <: Position with ExpandablePosition, Q <: Position] = (S, Content) => Option[Q]

  /**
   * Rename a dimension.
   *
   * @param dim  The dimension to rename.
   * @param name The rename pattern. Use `%1$``s` for the coordinate.
   */
  def RenameDimension[P <: Position](dim: Dimension, name: String)(
    implicit ev: PosDimDep[P, dim.type]): FromCell[P, P] = {
    (cell: Cell[P]) => Some(cell.position.update(dim, name.format(cell.position(dim).toShortString)))
  }

  /**
   * Rename coordinate according a name pattern.
   *
   * @param dim  Dimension for which to update coordinate name.
   * @param name Pattern for the new name of the coordinate at `dim`. Use `%[12]$``s` for the string
   *             representations of the coordinate, and the content.
   */
  def RenameDimensionWithContent[P <: Position](dim: Dimension, name: String = "%1$s=%2$s")(
    implicit ev: PosDimDep[P, dim.type]): FromCell[P, P] = {
    (cell: Cell[P]) =>
      Some(cell.position.update(dim, name.format(cell.position(dim).toShortString, cell.content.value.toShortString)))
  }

  /**
   * Append a coordinate to the position.
   *
   * @param name The coordinate to append to the outcome cell.
   */
  def AppendValue[P <: Position with ExpandablePosition](name: Valueable): FromCell[P, P#M] = {
    (cell: Cell[P]) => Some(cell.position.append(name))
  }

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
  def PrependPairwiseSelectedStringToRemainder[P <: Position](slice: Slice[P], pattern: String, all: Boolean = false,
    separator: String = "|"): FromPairwiseCells[P, slice.R#M] = {
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

  /**
   * Append with a dimension from the remainder.
   *
   * @param dim The dimension (out of `rem`) to append to the cell's position.
   */
  def AppendRemainderDimension[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition](
    dim: Dimension)(implicit ev: PosDimDep[R, dim.type]): FromSelectedAndRemainder[S, R, S#M] = {
    (pos: S, rem: R) => Some(pos.append(rem(dim)))
  }

  /**
   * Extract position using string of `rem`.
   *
   * @param separator The separator to use for the appended coordinate.
   */
  def AppendRemainderString[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition](
    separator: String = "|"): FromSelectedAndRemainder[S, R, S#M] = {
    (pos: S, rem: R) => Some(pos.append(rem.toShortString(separator)))
  }

  /**
   * Extract position using string of current and previous `rem`.
   *
   * @param pattern   Name pattern of the new coordinate. Use `%[12]$``s` for the string representations of the
   *                  previous and current remainder positions respectively.
   * @param separator The separator to use for the appended coordinate.
   */
  def AppendPairwiseString[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition](
    pattern: String, separator: String = "|"): FromSelectedAndPairwiseRemainder[S, R, S#M] = {
    (pos: S, curr: R, prev: R) =>
      Some(pos.append(pattern.format(prev.toShortString(separator), curr.toShortString(separator))))
  }

  /**
   * Expand position by appending a string coordinate from double.
   *
   * @param name The name pattern. Use `%1$``s` for the output pattern.
   */
  def AppendDoubleString[S <: Position with ExpandablePosition](
    name: String = "%1$f%%"): FromSelectedAndOutput[S, Double, S#M] = {
    (pos: S, value: Double) => Some(pos.append(name.format(value)))
  }

  /** Append the content string to the position. */
  def AppendContentString[S <: Position with ExpandablePosition](): FromSelectedAndContent[S, S#M] = {
    (pos: S, con: Content) => Some(pos.append(con.value.toShortString))
  }

  /**
   * Append a coordinate according a name pattern.
   *
   * @param dim  Dimension for which to update coordinate name.
   * @param name Pattern for the new name of the coordinate at `dim`. Use `%[12]$``s` for the string
   *             representations of the coordinate, and the content.
   */
  def AppendDimensionAndContentString[S <: Position with ExpandablePosition](dim: Dimension,
    name: String = "%1$s=%2$s")(implicit ev: PosDimDep[S, dim.type]): FromSelectedAndContent[S, S#M] = {
    (pos: S, con: Content) => Some(pos.append(name.format(pos(dim).toShortString, con.value.toShortString)))
  }
}

