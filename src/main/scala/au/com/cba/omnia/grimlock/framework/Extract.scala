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
trait Extract[P <: Position[P], W, T] extends java.io.Serializable { self =>
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
  def extract(cell: Cell[P], ext: V): Option[T]

  /**
   * Operator for transforming the returned value.
   *
   * @param presenter The function to apply and transform the returned value.
   *
   * @return An extract that runs `this` and then transfors the returned value.
   */
  def andThenPresent[U](presenter: (T) => Option[U]): Extract[P, W, U] = {
    new Extract[P, W, U] {
      def extract(cell: Cell[P], ext: V): Option[U] = self.extract(cell, ext).flatMap { case r => presenter(r) }
    }
  }
}

/** Companion object for the `Extract` trait. */
object Extract {
  /** Converts a `(Cell[P], W) => Option[T]` to a `Extract[P, W, T]`. */
  implicit def F2E[P <: Position[P], W, T](e: (Cell[P], W) => Option[T]): Extract[P, W, T] = {
    new Extract[P, W, T] { def extract(cell: Cell[P], ext: V): Option[T] = e(cell, ext) }
  }
}

/** Extract from a `Map[Position1D, T]` using the provided key. */
trait ExtractWithKey[P <: Position[P], T] extends Extract[P, Map[Position1D, T], T] {
  /** The key used for extracting from the map. */
  val key: Position1D

  def extract(cell: Cell[P], ext: V): Option[T] = ext.get(key)
}

/** Companion object to `ExtractWithKey`. */
object ExtractWithKey {
  /**
   * Extract from a `Map[Position1D, T]` using the provided key.
   *
   * @param key The key used for extracting from the map.
   */
  def apply[P <: Position[P], T](key: Positionable[Position1D]): ExtractWithKey[P, T] = ExtractWithKeyImpl(key())
}

private case class ExtractWithKeyImpl[P <: Position[P], T](key: Position1D) extends ExtractWithKey[P, T]

/** Extract from a `Map[Position1D, T]` using a dimension from the cell. */
trait ExtractWithDimension[P <: Position[P], T] extends Extract[P, Map[Position1D, T], T] {
  /** Dimension used for extracting from the map. */
  val dimension: Dimension

  def extract(cell: Cell[P], ext: V): Option[T] = ext.get(Position1D(cell.position(dimension)))
}

/** Companion object to `ExtractWithDimension`. */
object ExtractWithDimension {
  /**
   * Extract from a `Map[Position1D, T]` using a dimension from the cell.
   *
   * @param dim Dimension used for extracting from the map.
   */
  def apply[P <: Position[P], T](dimension: Dimension)(
    implicit ev: PosDimDep[P, dimension.type]): ExtractWithDimension[P, T] = ExtractWithDimensionImpl(dimension)
}

private case class ExtractWithDimensionImpl[P <: Position[P], T](
  dimension: Dimension) extends ExtractWithDimension[P, T]

/** Extract from a `Map[Position1D, Map[Position1D, T]]` using a dimension from the cell and the provided key. */
trait ExtractWithDimensionAndKey[P <: Position[P], T] extends Extract[P, Map[Position1D, Map[Position1D, T]], T] {
  /** Dimension used for extracting from the outer map. */
  val dimension: Dimension

  /** The key used for extracting from the inner map. */
  val key: Position1D

  def extract(cell: Cell[P], ext: V): Option[T] = ext.get(Position1D(cell.position(dimension))).flatMap(_.get(key))
}

/** Companion object to `ExtractWithDimensionAndKey`. */
object ExtractWithDimensionAndKey {
  /**
   * Extract from a `Map[Position1D, Map[Position1D, T]]` using a dimension from the cell and the provided key.
   *
   * @param dim Dimension used for extracting from the outer map.
   * @param key The key used for extracting from the inner map.
   */
  def apply[P <: Position[P], T](dimension: Dimension, key: Positionable[Position1D])(
    implicit ev1: PosDimDep[P, dimension.type]): ExtractWithDimensionAndKey[P, T] = {
    ExtractWithDimensionAndKeyImpl(dimension, key())
  }
}

private case class ExtractWithDimensionAndKeyImpl[P <: Position[P], T](dimension: Dimension,
  key: Position1D) extends ExtractWithDimensionAndKey[P, T]

/** Extract from a `Map[P, T]` using the position of the cell. */
case class ExtractWithPosition[P <: Position[P], T]() extends Extract[P, Map[P, T], T] {
  def extract(cell: Cell[P], ext: V): Option[T] = ext.get(cell.position)
}

/**
 * Extract from a `Map[P, Map[Position1D, T]]` using the position of the cell and the provided key.
 *
 * @param key The key used for extracting from the inner map.
 */
case class ExtractWithPositionAndKey[P <: Position[P], T](
  key: Positionable[Position1D]) extends Extract[P, Map[P, Map[Position1D, T]], T] {
  def extract(cell: Cell[P], ext: V): Option[T] = ext.get(cell.position).flatMap(_.get(key()))
}

/** Extract from a `Map[S, T]` using the selected position(s) of the cell. */
trait ExtractWithSelected[
  P <: Position[P] with ReduceablePosition[P, _],
  S <: Position[S] with ExpandablePosition[S, _],
  R <: Position[R] with ExpandablePosition[R, _],
  T]
  extends Extract[P, Map[S, T], T] {
  /** The slice used to extract the selected position(s) from the cell which are used as the key into the map. */
  val slice: Slice[P, S, R]

  def extract(cell: Cell[P], ext: V): Option[T] = ext.get(slice.selected(cell.position))
}

/** Companion object to `ExtractWithSelected`. */
object ExtractWithSelected {
  /**
   * Extract from a `Map[S, T]` using the selected position(s) of the cell.
   *
   * @param slice The slice used to extract the selected position(s) from the cell which are used as the key
   *              into the map.
   */
  def apply[
    P <: Position[P] with ReduceablePosition[P, _],
    S <: Position[S] with ExpandablePosition[S, _],
    R <: Position[R] with ExpandablePosition[R, _],
    T
  ](
    slice: Slice[P, S, R]
  ): ExtractWithSelected[P, S, R, T] = ExtractWithSelectedImpl(slice)
}

private case class ExtractWithSelectedImpl[
  P <: Position[P] with ReduceablePosition[P, _],
  S <: Position[S] with ExpandablePosition[S, _],
  R <: Position[R] with ExpandablePosition[R, _],
  T
](
  slice: Slice[P, S, R]
) extends ExtractWithSelected[P, S, R, T]

/**
 * Extract from a `Map[S, Map[Position1D, T]]` using the selected position(s) of the cell and the provided key.
 */
trait ExtractWithSelectedAndKey[
  P <: Position[P] with ReduceablePosition[P, _],
  S <: Position[S] with ExpandablePosition[S, _],
  R <: Position[R] with ExpandablePosition[R, _],
  T
] extends Extract[P, Map[S, Map[Position1D, T]], T] {
  /** The slice used to extract the selected position(s) from the cell which are used as the key into the map. */
  val slice: Slice[P, S, R]

  /** The key used for extracting from the inner map. */
  val key: Positionable[Position1D]

  def extract(cell: Cell[P], ext: V): Option[T] = ext.get(slice.selected(cell.position)).flatMap(_.get(key()))
}

/** Companion object to `ExtractWithSelectedAndKey`. */
object ExtractWithSelectedAndKey {
  /**
   * Extract from a `Map[S, Map[Position1D, T]]` using the selected position(s) of the cell and the provided key.
   *
   * @param slice The slice used to extract the selected position(s) from the cell which are used as the key
   *              into the map.
   * @param key   The key used for extracting from the inner map.
   */
  def apply[
    P <: Position[P] with ReduceablePosition[P, _],
    S <: Position[S] with ExpandablePosition[S, _],
    R <: Position[R] with ExpandablePosition[R, _],
    T
  ](
    slice: Slice[P, S, R],
    key: Positionable[Position1D]
  ): ExtractWithSelectedAndKey[P, S, R, T] = ExtractWithSelectedAndKeyImpl(slice, key)
}

private case class ExtractWithSelectedAndKeyImpl[
  P <: Position[P] with ReduceablePosition[P, _],
  S <: Position[S] with ExpandablePosition[S, _],
  R <: Position[R] with ExpandablePosition[R, _],
  T
](
  slice: Slice[P, S, R],
  key: Positionable[Position1D]
) extends ExtractWithSelectedAndKey[P, S, R, T]

/** Extract from a `Map[S, Map[R, T]]` using the selected and remainder position(s) of the cell. */
trait ExtractWithSlice[
  P <: Position[P] with ReduceablePosition[P, _],
  S <: Position[S] with ExpandablePosition[S, _],
  R <: Position[R] with ExpandablePosition[R, _],
  T] extends Extract[P, Map[S, Map[R, T]], T] {
  /**
   * The slice used to extract the selected and remainder position(s) from the cell which are used as the keys
   * into the outer and inner maps.
   */
  val slice: Slice[P, S, R]

  def extract(cell: Cell[P], ext: V): Option[T] = {
    ext.get(slice.selected(cell.position)).flatMap(_.get(slice.remainder(cell.position)))
  }
}

/** Companion object to `ExtractWithSlice`. */
object ExtractWithSlice {
  /**
   * Extract from a `Map[S, Map[R, T]]` using the selected and remainder position(s) of the cell.
   *
   * @param slice The slice used to extract the selected and remainder position(s) from the cell which are used
   *              as the keys into the outer and inner maps.
   */
  def apply[
    P <: Position[P] with ReduceablePosition[P, _],
    S <: Position[S] with ExpandablePosition[S, _],
    R <: Position[R] with ExpandablePosition[R, _],
    T
  ](
    slice: Slice[P, S, R]
  ): ExtractWithSlice[P, S, R, T] = ExtractWithSliceImpl(slice)
}

private case class ExtractWithSliceImpl[
  P <: Position[P] with ReduceablePosition[P, _],
  S <: Position[S] with ExpandablePosition[S, _],
  R <: Position[R] with ExpandablePosition[R, _],
  T
](
  slice: Slice[P, S, R]
) extends ExtractWithSlice[P, S, R, T]

