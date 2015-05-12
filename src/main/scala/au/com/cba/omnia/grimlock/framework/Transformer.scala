// Copyright 2014-2015 Commonwealth Bank of Australia
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

package au.com.cba.omnia.grimlock.framework.transform

import au.com.cba.omnia.grimlock.framework._
import au.com.cba.omnia.grimlock.framework.encoding._
import au.com.cba.omnia.grimlock.framework.position._
import au.com.cba.omnia.grimlock.framework.utility._

/** Base trait for transformations from `P` to `Q`. */
trait Transformer[P <: Position, Q <: Position] extends TransformerWithValue[P, Q] { self =>
  type V = Any

  def presentWithValue(cell: Cell[P], ext: V): Collection[Cell[Q]] = present(cell)

  /**
   * Present the transformed content(s).
   *
   * @param cell The cell to transform.
   *
   * @return A `Collection` of transformed cells.
   */
  def present(cell: Cell[P]): Collection[Cell[Q]]

  /**
   * Operator for chaining transformations.
   *
   * @param that The transformation to perform after `this`.
   *
   * @return A transformer that runs `this` and then `that`.
   */
  def andThen[R <: Position](that: Transformer[Q, R]) = {
    new Transformer[P, R] {
      def present(cell: Cell[P]): Collection[Cell[R]] = {
        Collection(self.present(cell).toList.flatMap { case c => that.present(c).toList })
      }
    }
  }

  /**
   * Operator for transforming and then renaming dimensions.
   *
   * @param rename The rename to apply after the transformation.
   *
   * @return A transformer that runs `this` and then renames the resulting dimension(s).
   */
  def andThenRename(rename: (Cell[P], Cell[Q]) => Q) = {
    new Transformer[P, Q] {
      def present(cell: Cell[P]): Collection[Cell[Q]] = {
        Collection(self.present(cell).toList.map { case c => Cell(rename(cell, c), c.content) })
      }
    }
  }

  /**
   * Operator for transforming and then expanding dimensions.
   *
   * @param expand The expansion to apply after the transformation.
   *
   * @return A transformer that runs `this` and then expands the resulting dimensions.
   */
  def andThenExpand[R <: Position](expand: (Cell[P], Cell[Q]) => R)(implicit ev: ExpPosDep[Q, R]) = {
    new Transformer[P, R] {
      def present(cell: Cell[P]): Collection[Cell[R]] = {
        Collection(self.present(cell).toList.map { case c => Cell(expand(cell, c), c.content) })
      }
    }
  }
}

object Transformer {
  def rename[P <: Position, Q <: Position](dim: Dimension, name: String): (Cell[P], Cell[Q]) => Q = {
    (before: Cell[P], after: Cell[Q]) => after.position.update(dim,
      name.format(after.position(dim).toShortString, before.content.value.toShortString))
  }

  def expand[P <: Position, Q <: Position with ExpandablePosition, V](name: V)(
    implicit ev: Valueable[V]): (Cell[P], Cell[Q]) => Q#M = {
    (before: Cell[P], after: Cell[Q]) => after.position.append(name)
  }

  def expand[P <: Position, Q <: Position with ExpandablePosition](dim: Dimension,
    name: String): (Cell[P], Cell[Q]) => Q#M = {
    (before: Cell[P], after: Cell[Q]) => after.position.append(name.format(after.position(dim).toShortString,
      before.content.value.toShortString))
  }

  implicit def C2T[P <: Position, Q <: Position](t: (Cell[P]) => Cell[Q]) = {
    new Transformer[P, Q] { def present(cell: Cell[P]): Collection[Cell[Q]] = Collection(t(cell)) }
  }

  implicit def LC2T[P <: Position, Q <: Position](t: (Cell[P]) => List[Cell[Q]]) = {
    new Transformer[P, Q] { def present(cell: Cell[P]): Collection[Cell[Q]] = Collection(t(cell)) }
  }

  implicit def CC2T[P <: Position, Q <: Position](t: (Cell[P]) => Collection[Cell[Q]]) = {
    new Transformer[P, Q] { def present(cell: Cell[P]): Collection[Cell[Q]] = t(cell) }
  }

  implicit def LT2T[P <: Position, Q <: Position, T <: Transformer[P, Q]](t: List[T]) = {
    new Transformer[P, Q] {
      def present(cell: Cell[P]): Collection[Cell[Q]] = Collection(t.flatMap { case s => s.present(cell).toList })
    }
  }
}

/** Base trait for transformations from `P` to `Q` that use a user supplied value. */
trait TransformerWithValue[P <: Position, Q <: Position] extends java.io.Serializable { self =>
  /** Type of the external value. */
  type V

  /**
   * Present the transformed content(s).
   *
   * @param cell The cell to transform.
   * @param ext  Externally provided data needed for the transformation.
   *
   * @return A `Collection` of transformed cells.
   */
  def presentWithValue(cell: Cell[P], ext: V): Collection[Cell[Q]]

  /**
   * Operator for chaining transformations.
   *
   * @param that The transformation to perform after `this`.
   *
   * @return A transformer that runs `this` and then `that`.
   */
  def andThenWithValue[R <: Position](that: TransformerWithValue[Q, R] { type V >: self.V }) = {
    new TransformerWithValue[P, R] {
      type V = self.V

      def presentWithValue(cell: Cell[P], ext: V): Collection[Cell[R]] = {
        Collection(self.presentWithValue(cell, ext).toList.flatMap { case c => that.presentWithValue(c, ext).toList })
      }
    }
  }

  /**
   * Operator for transforming and then renaming dimensions.
   *
   * @param rename The rename to apply after the transformation.
   *
   * @return A transformer that runs `this` and then renames the resulting dimension(s).
   */
  def andThenRenameWithValue(rename: (Cell[P], Cell[Q], V) => Q) = {
    new TransformerWithValue[P, Q] {
      type V = self.V

      def presentWithValue(cell: Cell[P], ext: V): Collection[Cell[Q]] = {
        Collection(self.presentWithValue(cell, ext).toList.map { case c => Cell(rename(cell, c, ext), c.content) })
      }
    }
  }

  /**
   * Operator for transforming and then expanding dimensions.
   *
   * @param expand The expansion to apply after the transformation.
   *
   * @return A transformer that runs `this` and then expands the resulting dimensions.
   */
  def andThenExpandWithValue[R <: Position](expand: (Cell[P], Cell[Q], V) => R)(implicit ev: ExpPosDep[Q, R]) = {
    new TransformerWithValue[P, R] {
      type V = self.V

      def presentWithValue(cell: Cell[P], ext: V): Collection[Cell[R]] = {
        Collection(self.presentWithValue(cell, ext).toList.map { case c => Cell(expand(cell, c, ext), c.content) })
      }
    }
  }
}

object TransformerWithValue {
  def rename[P <: Position, Q <: Position, V](dim: Dimension, name: String): (Cell[P], Cell[Q], V) => Q = {
    (before: Cell[P], after: Cell[Q], V) => after.position.update(dim,
      name.format(after.position(dim).toShortString, before.content.value.toShortString))
  }

  def expand[P <: Position, Q <: Position with ExpandablePosition, V, W](name: W)(
    implicit ev: Valueable[W]): (Cell[P], Cell[Q], V) => Q#M = {
    (before: Cell[P], after: Cell[Q], ext: V) => after.position.append(name)
  }

  def expand[P <: Position, Q <: Position with ExpandablePosition, V](dim: Dimension,
    name: String): (Cell[P], Cell[Q], V) => Q#M = {
    (before: Cell[P], after: Cell[Q], ext: V) => after.position.append(name.format(after.position(dim).toShortString,
      before.content.value.toShortString))
  }

  implicit def C2TWV[P <: Position, Q <: Position, W](t: (Cell[P], W) => Cell[Q]) = {
    new TransformerWithValue[P, Q] {
      type V = W

      def presentWithValue(cell: Cell[P], ext: W): Collection[Cell[Q]] = Collection(t(cell, ext))
    }
  }

  implicit def LC2TWV[P <: Position, Q <: Position, W](t: (Cell[P], W) => List[Cell[Q]]) = {
    new TransformerWithValue[P, Q] {
      type V = W

      def presentWithValue(cell: Cell[P], ext: W): Collection[Cell[Q]] = Collection(t(cell, ext))
    }
  }

  implicit def CC2TWV[P <: Position, Q <: Position, W](t: (Cell[P], W) => Collection[Cell[Q]]) = {
    new TransformerWithValue[P, Q] {
      type V = W

      def presentWithValue(cell: Cell[P], ext: W): Collection[Cell[Q]] = t(cell, ext)
    }
  }

  implicit def LT2TWV[P <: Position, Q <: Position, T <: TransformerWithValue[P, Q] { type V >: W}, W](t: List[T]) = {
    new TransformerWithValue[P, Q] {
      type V = W

      def presentWithValue(cell: Cell[P], ext: V): Collection[Cell[Q]] = {
        Collection(t.flatMap { case s => s.presentWithValue(cell, ext).toList })
      }
    }
  }
}

