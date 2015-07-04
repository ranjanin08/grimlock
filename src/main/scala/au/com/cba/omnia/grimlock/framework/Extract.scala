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

// TODO: Test these

/** Base trait for extracting data from a user provided value given a cell. */
trait Extract[P <: Position, W, R] extends java.io.Serializable { self =>
  /** Type of the user provided data. */
  type V = W

  /**
   * Extract value for the given cell.
   *
   * @param cell The cell for which to extract a value.
   * @param ext  The user provided data from which to extract.
   *
   * @return Optional value (if present) or `None` otherwise.
   */
  def extract(cell: Cell[P], ext: V): Option[R]

  /**
   * Operator for transforming the returned value.
   *
   * @param presenter The function to apply and transform the returned value.
   *
   * @return An extract that runs `this` and then transfors the returned value.
   */
  def andThenPresent[S](presenter: (R) => Option[S]): Extract[P, W, S] = {
    new Extract[P, W, S] {
      def extract(cell: Cell[P], ext: V): Option[S] = self.extract(cell, ext).flatMap { case r => presenter(r) }
    }
  }
}

/** Companion object for the `Extract` trait. */
object Extract {
  /** Converts a `(Cell[P], W) => Option[R]` to a `Extract[P, W, R]`. */
  implicit def F2E[P <: Position, W, R](e: (Cell[P], W) => Option[R]): Extract[P, W, R] = {
    new Extract[P, W, R] { def extract(cell: Cell[P], ext: V): Option[R] = e(cell, ext) }
  }
}

/**
 * Extract from a `Map[Position1D, R]` using a dimension from the cell.
 *
 * @param dim Dimension used for extracting from the map.
 */
case class ExtractWithDimension[D <: Dimension, P <: Position, R](dim: D)(
  implicit ev: PosDimDep[P, D]) extends Extract[P, Map[Position1D, R], R] {
  def extract(cell: Cell[P], ext: V): Option[R] = ext.get(Position1D(cell.position(dim)))
}

/**
 * Extract from a `Map[Position1D, R]` using the provided key.
 *
 * @param key The key used for extracting from the map.
 */
case class ExtractWithKey[P <: Position, T, R](key: T)(
  implicit ev: Positionable[T, Position1D]) extends Extract[P, Map[Position1D, R], R] {
  def extract(cell: Cell[P], ext: V): Option[R] = ext.get(ev.convert(key))
}

/** Extract from a `Map[P, R]` using the position of the cell. */
case class ExtractWithPosition[P <: Position, R]() extends Extract[P, Map[P, R], R] {
  def extract(cell: Cell[P], ext: V): Option[R] = ext.get(cell.position)
}

/**
 * Extract from a `Map[Position1D, Map[Position1D, R]]` using a dimension from the cell and the provided key.
 *
 * @param dim Dimension used for extracting from the outer map.
 * @param key The key used for extracting from the inner map.
 */
case class ExtractWithDimensionAndKey[D <: Dimension, P <: Position, T, R](dim: D, key: T)(
  implicit ev1: PosDimDep[P, D], ev2: Positionable[T, Position1D])
  extends Extract[P, Map[Position1D, Map[Position1D, R]], R] {
  def extract(cell: Cell[P], ext: V): Option[R] = {
    ext.get(Position1D(cell.position(dim))).flatMap(_.get(ev2.convert(key)))
  }
}

/**
 * Extract from a `Map[S#S, R]` using the selected position(s) of the cell.
 *
 * @param slice The slice used to extract the selected position(s) from the cell which are used as the key
 *              into the map.
 */
case class ExtractWithSelected[D <: Dimension, P <: Position, S <: Slice[P, D], R](slice: S)(
  implicit ev: PosDimDep[P, D]) extends Extract[P, Map[S#S, R], R] {
  def extract(cell: Cell[P], ext: V): Option[R] = ext.get(slice.selected(cell.position))
}

/**
 * Extract from a `Map[S#S, Map[S#R, R]]` using the selected and remainder position(s) of the cell.
 *
 * @param slice The slice used to extract the selected and remainder position(s) from the cell which are used
 *              as the keys into the outer and inner maps.
 */
case class ExtractWithSlice[D <: Dimension, P <: Position, S <: Slice[P, D], R](slice: S)(
  implicit ev: PosDimDep[P, D]) extends Extract[P, Map[S#S, Map[S#R, R]], R] {
  def extract(cell: Cell[P], ext: V): Option[R] = {
    ext.get(slice.selected(cell.position)).flatMap(_.get(slice.remainder(cell.position)))
  }
}

/**
 * Extract from a `Map[S#S, Map[Position1D, R]]` using the selected position(s) of the cell and the provided key.
 *
 * @param slice The slice used to extract the selected position(s) from the cell which are used
 *              as the key into the outer map.
 * @param key   The key used for extracting from the inner map.
 */
case class ExtractWithSliceAndKey[D <: Dimension, P <: Position, S <: Slice[P, D], T, R](slice: S, key: T)(
  implicit ev1: PosDimDep[P, D], ev2: Positionable[T, Position1D]) extends Extract[P, Map[S#S, Map[Position1D, R]], R] {
  def extract(cell: Cell[P], ext: V): Option[R] = {
    ext.get(slice.selected(cell.position)).flatMap(_.get(ev2.convert(key)))
  }
}

