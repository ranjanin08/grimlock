// Copyright 2016 Commonwealth Bank of Australia
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

/** Trait for compacting a cell to a `Map`. */
trait Compactable[P <: Position with CompactablePosition] extends java.io.Serializable {
  /**
   * Convert a single cell to a `Map`.
   *
   * @param slice Encapsulates the dimension(s) to compact.
   * @param cell  The cell to compact.
   *
   * @return A `Map` with the compacted cell.
   */
  def toMap(slice: Slice[P], cell: Cell[P]): Map[slice.S, P#C[slice.R]] = {
    Map(slice.selected(cell.position) -> cell.position.toMapValue(slice.remainder(cell.position), cell.content))
  }

  /**
   * Combine two compacted cells.
   *
   * @param x The left map to combine.
   * @param y The right map to combine.
   *
   * @return The combined map.
   */
  def combineMaps[S <: Position, R <: Position](x: Map[S, P#C[R]], y: Map[S, P#C[R]]): Map[S, P#C[R]] = {
    x ++ y.map { case (k, v) => k -> combineMapValues(x.get(k), v) }
  }

  protected def combineMapValues[R <: Position](x: Option[P#C[R]], y: P#C[R]): P#C[R]
}

/** Companion object to the `Compactable` trait. */
object Compactable {
  /** Return a `Compactable` for `Position1D`. */
  implicit val cp1: Compactable[Position1D] = new Compactable[Position1D] {
    protected def combineMapValues[R <: Position](x: Option[Position1D#C[R]], y: Position1D#C[R]): Position1D#C[R] = y
  }

  /** Return a `Compactable` for `Position2D`. */
  implicit val cp2: Compactable[Position2D] = new Compactable[Position2D] {
    protected def combineMapValues[R <: Position](x: Option[Position2D#C[R]], y: Position2D#C[R]): Position2D#C[R] = {
      merge(x, y)
    }
  }

  /** Return a `Compactable` for `Position3D`. */
  implicit val cp3: Compactable[Position3D] = new Compactable[Position3D] {
    protected def combineMapValues[R <: Position](x: Option[Position3D#C[R]], y: Position3D#C[R]): Position3D#C[R] = {
      merge(x, y)
    }
  }

  /** Return a `Compactable` for `Position4D`. */
  implicit val cp4: Compactable[Position4D] = new Compactable[Position4D] {
    protected def combineMapValues[R <: Position](x: Option[Position4D#C[R]], y: Position4D#C[R]): Position4D#C[R] = {
      merge(x, y)
    }
  }

  /** Return a `Compactable` for `Position5D`. */
  implicit val cp5: Compactable[Position5D] = new Compactable[Position5D] {
    protected def combineMapValues[R <: Position](x: Option[Position5D#C[R]], y: Position5D#C[R]): Position5D#C[R] = {
      merge(x, y)
    }
  }

  /** Return a `Compactable` for `Position6D`. */
  implicit val cp6: Compactable[Position6D] = new Compactable[Position6D] {
    protected def combineMapValues[R <: Position](x: Option[Position6D#C[R]], y: Position6D#C[R]): Position6D#C[R] = {
      merge(x, y)
    }
  }

  /** Return a `Compactable` for `Position7D`. */
  implicit val cp7: Compactable[Position7D] = new Compactable[Position7D] {
    protected def combineMapValues[R <: Position](x: Option[Position7D#C[R]], y: Position7D#C[R]): Position7D#C[R] = {
      merge(x, y)
    }
  }

  /** Return a `Compactable` for `Position8D`. */
  implicit val cp8: Compactable[Position8D] = new Compactable[Position8D] {
    protected def combineMapValues[R <: Position](x: Option[Position8D#C[R]], y: Position8D#C[R]): Position8D#C[R] = {
      merge(x, y)
    }
  }

  /** Return a `Compactable` for `Position9D`. */
  implicit val cp9: Compactable[Position9D] = new Compactable[Position9D] {
    protected def combineMapValues[R <: Position](x: Option[Position9D#C[R]], y: Position9D#C[R]): Position9D#C[R] = {
      merge(x, y)
    }
  }

  private def merge[K, V](x: Option[Map[K, V]], y: Map[K, V]): Map[K, V] = x.map(_ ++ y).getOrElse(y)
}

