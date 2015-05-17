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

trait Extract[P <: Position, V, R] extends java.io.Serializable { self =>
  def extract(cell: Cell[P], ext: V): Option[R]

  def andThenPresent[S](presenter: (R) => Option[S]): Extract[P, V, S] = {
    new Extract[P, V, S] {
      def extract(cell: Cell[P], ext: V): Option[S] = self.extract(cell, ext).flatMap { case r => presenter(r) }
    }
  }
}

object Extract {
  implicit def F2E[P <: Position, V, R](e: (Cell[P], V) => Option[R]): Extract[P, V, R] = {
    new Extract[P, V, R] { def extract(cell: Cell[P], ext: V): Option[R] = e(cell, ext) }
  }
}

case class ExtractWithDimension[D <: Dimension, P <: Position, R](dim: D)(
  implicit ev: PosDimDep[P, D]) extends Extract[P, Map[Position1D, R], R] {
  def extract(cell: Cell[P], ext: Map[Position1D, R]): Option[R] = ext.get(Position1D(cell.position(dim)))
}

case class ExtractWithKey[P <: Position, R](key: Position1D) extends Extract[P, Map[Position1D, R], R] {
  def extract(cell: Cell[P], ext: Map[Position1D, R]): Option[R] = ext.get(key)
}

case class ExtractWithPosition[P <: Position, R]() extends Extract[P, Map[P, R], R] {
  def extract(cell: Cell[P], ext: Map[P, R]): Option[R] = ext.get(cell.position)
}

case class ExtractWithDimensionAndKey[D <: Dimension, P <: Position, R](dim: D, key: Position1D)(
  implicit ev1: PosDimDep[P, D]) extends Extract[P, Map[Position1D, Map[Position1D, R]], R] {
  def extract(cell: Cell[P], ext: Map[Position1D, Map[Position1D, R]]): Option[R] = {
    ext.get(Position1D(cell.position(dim))).flatMap(_.get(key))
  }
}

case class ExtractWithSlice[D <: Dimension, P <: Position, S <: Slice[P, D], R](
  slice: S) extends Extract[P, Map[S#S, Map[S#R, R]], R] {
  def extract(cell: Cell[P], ext: Map[S#S, Map[S#R, R]]): Option[R] = {
    ext.get(slice.selected(cell.position)).flatMap(_.get(slice.remainder(cell.position)))
  }
}

case class ExtractWithSliceAndKey[D <: Dimension, P <: Position, S <: Slice[P, D], R](slice: S,
  key: Position1D) extends Extract[P, Map[S#S, Map[Position1D, R]], R] {
  def extract(cell: Cell[P], ext: Map[S#S, Map[Position1D, R]]): Option[R] = {
    ext.get(slice.selected(cell.position)).flatMap(_.get(key))
  }
}

