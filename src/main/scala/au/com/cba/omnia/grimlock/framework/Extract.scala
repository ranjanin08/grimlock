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

import au.com.cba.omnia.grimlock.framework.position._

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

/** Extract from a `Map[Position1D, R]` using the provided key. */
trait ExtractWithKey[P <: Position, R] extends Extract[P, Map[Position1D, R], R] {
  /** The key used for extracting from the map. */
  val key: Position1D

  def extract(cell: Cell[P], ext: V): Option[R] = ext.get(key)
}

/** Companion object to `ExtractWithKey`. */
object ExtractWithKey {
  /**
   * Extract from a `Map[Position1D, R]` using the provided key.
   *
   * @param key The key used for extracting from the map.
   */
  def apply[P <: Position, R](key: Positionable[Position1D]): ExtractWithKey[P, R] = ExtractWithKeyImpl(key())
}

private case class ExtractWithKeyImpl[P <: Position, R](key: Position1D) extends ExtractWithKey[P, R]

/** Extract from a `Map[Position1D, R]` using a dimension from the cell. */
trait ExtractWithDimension[P <: Position, R] extends Extract[P, Map[Position1D, R], R] {
  /** Dimension used for extracting from the map. */
  val dimension: Dimension

  def extract(cell: Cell[P], ext: V): Option[R] = ext.get(Position1D(cell.position(dimension)))
}

/** Companion object to `ExtractWithDimension`. */
object ExtractWithDimension {
  /**
   * Extract from a `Map[Position1D, R]` using a dimension from the cell.
   *
   * @param dim Dimension used for extracting from the map.
   */
  def apply[P <: Position, R](dimension: Dimension)(
    implicit ev: PosDimDep[P, dimension.type]): ExtractWithDimension[P, R] = ExtractWithDimensionImpl(dimension)
}

private case class ExtractWithDimensionImpl[P <: Position, R](dimension: Dimension) extends ExtractWithDimension[P, R]

/** Extract from a `Map[Position1D, Map[Position1D, R]]` using a dimension from the cell and the provided key. */
trait ExtractWithDimensionAndKey[P <: Position, R] extends Extract[P, Map[Position1D, Map[Position1D, R]], R] {
  /** Dimension used for extracting from the outer map. */
  val dimension: Dimension

  /** The key used for extracting from the inner map. */
  val key: Position1D

  def extract(cell: Cell[P], ext: V): Option[R] = ext.get(Position1D(cell.position(dimension))).flatMap(_.get(key))
}

/** Companion object to `ExtractWithDimensionAndKey`. */
object ExtractWithDimensionAndKey {
  /**
   * Extract from a `Map[Position1D, Map[Position1D, R]]` using a dimension from the cell and the provided key.
   *
   * @param dim Dimension used for extracting from the outer map.
   * @param key The key used for extracting from the inner map.
   */
  def apply[P <: Position, R](dimension: Dimension, key: Positionable[Position1D])(
    implicit ev1: PosDimDep[P, dimension.type]): ExtractWithDimensionAndKey[P, R] = {
    ExtractWithDimensionAndKeyImpl(dimension, key())
  }
}

private case class ExtractWithDimensionAndKeyImpl[P <: Position, R](dimension: Dimension,
  key: Position1D) extends ExtractWithDimensionAndKey[P, R]

/** Extract from a `Map[P, R]` using the position of the cell. */
case class ExtractWithPosition[P <: Position, R]() extends Extract[P, Map[P, R], R] {
  def extract(cell: Cell[P], ext: V): Option[R] = ext.get(cell.position)
}

/**
 * Extract from a `Map[P, Map[Position1D, R]]` using the position of the cell and the provided key.
 *
 * @param key The key used for extracting from the inner map.
 */
case class ExtractWithPositionAndKey[P <: Position, R](
  key: Positionable[Position1D]) extends Extract[P, Map[P, Map[Position1D, R]], R] {
  def extract(cell: Cell[P], ext: V): Option[R] = ext.get(cell.position).flatMap(_.get(key()))
}

/** Extract from a `Map[slice.S, R]` using the selected position(s) of the cell. */
trait ExtractWithSelected[P <: Position, T <: Position with ExpandablePosition, R] extends Extract[P, Map[T, R], R] {
  /** The slice used to extract the selected position(s) from the cell which are used as the key into the map. */
  val slice: Slice[P] { type S = T }

  def extract(cell: Cell[P], ext: V): Option[R] = ext.get(slice.selected(cell.position))
}

/** Companion object to `ExtractWithSelected`. */
object ExtractWithSelected {
  /**
   * Extract from a `Map[slice.S, R]` using the selected position(s) of the cell.
   *
   * @param slice The slice used to extract the selected position(s) from the cell which are used as the key
   *              into the map.
   */
  def apply[P <: Position, R](slice: Slice[P]): ExtractWithSelected[P, slice.S, R] = ExtractWithSelectedImpl(slice)
}

private case class ExtractWithSelectedImpl[P <: Position, T <: Position with ExpandablePosition, R](
  slice: Slice[P] { type S = T }) extends ExtractWithSelected[P, T, R]

/**
 * Extract from a `Map[slice.S, Map[Position1D, R]]` using the selected position(s) of the cell and the provided key.
 */
trait ExtractWithSelectedAndKey[P <: Position, T <: Position with ExpandablePosition, R]
  extends Extract[P, Map[T, Map[Position1D, R]], R] {
  /** The slice used to extract the selected position(s) from the cell which are used as the key into the map. */
  val slice: Slice[P] { type S = T }

  /** The key used for extracting from the inner map. */
  val key: Positionable[Position1D]

  def extract(cell: Cell[P], ext: V): Option[R] = ext.get(slice.selected(cell.position)).flatMap(_.get(key()))
}

/** Companion object to `ExtractWithSelectedAndKey`. */
object ExtractWithSelectedAndKey {
  /**
   * Extract from a `Map[slice.S, Map[Position1D, R]]` using the selected position(s) of the cell and the provided key.
   *
   * @param slice The slice used to extract the selected position(s) from the cell which are used as the key
   *              into the map.
   * @param key   The key used for extracting from the inner map.
   */
  def apply[P <: Position, R](slice: Slice[P],
    key: Positionable[Position1D]): ExtractWithSelectedAndKey[P, slice.S, R] = ExtractWithSelectedAndKeyImpl(slice, key)
}

private case class ExtractWithSelectedAndKeyImpl[P <: Position, T <: Position with ExpandablePosition, R](
  slice: Slice[P] { type S = T }, key: Positionable[Position1D]) extends ExtractWithSelectedAndKey[P, T, R]

/** Extract from a `Map[slice.S, Map[slice.R, R]]` using the selected and remainder position(s) of the cell. */
trait ExtractWithSlice[P <: Position, T <: Position with ExpandablePosition, U <: Position with ExpandablePosition, R]
  extends Extract[P, Map[T, Map[U, R]], R] {
  /**
   * The slice used to extract the selected and remainder position(s) from the cell which are used as the keys
   * into the outer and inner maps.
   */
  val slice: Slice[P] { type S = T; type R = U }

  def extract(cell: Cell[P], ext: V): Option[R] = {
    ext.get(slice.selected(cell.position)).flatMap(_.get(slice.remainder(cell.position)))
  }
}

/** Companion object to `ExtractWithSlice`. */
object ExtractWithSlice {
  /**
   * Extract from a `Map[slice.S, Map[slice.R, R]]` using the selected and remainder position(s) of the cell.
   *
   * @param slice The slice used to extract the selected and remainder position(s) from the cell which are used
   *              as the keys into the outer and inner maps.
   */
  def apply[P <: Position, R](slice: Slice[P]): ExtractWithSlice[P, slice.S, slice.R, R] = ExtractWithSliceImpl(slice)
}

private case class ExtractWithSliceImpl[P <: Position, T <: Position with ExpandablePosition, U <: Position with ExpandablePosition, R](slice: Slice[P] { type S = T; type R = U }) extends ExtractWithSlice[P, T, U, R]

