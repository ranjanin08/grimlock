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

package au.com.cba.omnia.grimlock.framework.transform

import au.com.cba.omnia.grimlock.framework._
import au.com.cba.omnia.grimlock.framework.encoding._
import au.com.cba.omnia.grimlock.framework.position._

/** Base trait for transformations from `P` to `Q`. */
trait Transformer[P <: Position, Q <: Position] extends TransformerWithValue[P, Q] { self =>
  type V = Any

  def presentWithValue(cell: Cell[P], ext: V): TraversableOnce[Cell[Q]] = present(cell)

  /**
   * Present the transformed content(s).
   *
   * @param cell The cell to transform.
   *
   * @return A `TraversableOnce` of transformed cells.
   */
  def present(cell: Cell[P]): TraversableOnce[Cell[Q]]

  /**
   * Operator for chaining transformations.
   *
   * @param that The transformation to perform after `this`.
   *
   * @return A transformer that runs `this` and then `that`.
   */
  def andThen[R <: Position](that: Transformer[Q, R]) = {
    new Transformer[P, R] {
      def present(cell: Cell[P]): TraversableOnce[Cell[R]] = self.present(cell).flatMap { case c => that.present(c) }
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
      def present(cell: Cell[P]): TraversableOnce[Cell[Q]] = {
        self.present(cell).map { case c => Cell(rename(cell, c), c.content) }
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
  def andThenExpand[R <: Position](expand: (Cell[P], Cell[Q]) => R)(implicit ev: PosExpDep[Q, R]) = {
    new Transformer[P, R] {
      def present(cell: Cell[P]): TraversableOnce[Cell[R]] = {
        self.present(cell).map { case c => Cell(expand(cell, c), c.content) }
      }
    }
  }
}

/** Companion object to the `Transformer` trait. */
object Transformer {
  /**
   * Rename a dimension.
   *
   * @param dim  The dimension to rename.
   * @param name The rename pattern. Use `%[12]$``s` for the coordinate and original value respectively.
   *
   * @return A function that can be passed to `Transformer.andThenRename`.
   */
  def rename[P <: Position, Q <: Position](dim: Dimension, name: String): (Cell[P], Cell[Q]) => Q = {
    (before: Cell[P], after: Cell[Q]) =>
      after.position.update(dim, name.format(after.position(dim).toShortString, before.content.value.toShortString))
  }

  /**
   * Expand position by appending a coordinate.
   *
   * @param name The value to expand with.
   *
   * @return A function that can be passed to `Transformer.andThenExpand`.
   */
  def expand[P <: Position, Q <: Position with ExpandablePosition, V](name: V)(
    implicit ev: Valueable[V]): (Cell[P], Cell[Q]) => Q#M = {
    (before: Cell[P], after: Cell[Q]) => after.position.append(name)
  }

  /**
   * Expand position by appending a coordinate.
   *
   * @param dim  The dimension used to construct the new coordinate name.
   * @param name The name pattern. Use `%[12]$``s` for the coordinate and original value respectively.
   *
   * @return A function that can be passed to `Transformer.andThenExpand`.
   */
  def expand[P <: Position, Q <: Position with ExpandablePosition](dim: Dimension,
    name: String): (Cell[P], Cell[Q]) => Q#M = {
    (before: Cell[P], after: Cell[Q]) =>
      after.position.append(name.format(after.position(dim).toShortString, before.content.value.toShortString))
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
   * @return A `TraversableOnce` of transformed cells.
   */
  def presentWithValue(cell: Cell[P], ext: V): TraversableOnce[Cell[Q]]

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

      def presentWithValue(cell: Cell[P], ext: V): TraversableOnce[Cell[R]] = {
        self.presentWithValue(cell, ext).flatMap { case c => that.presentWithValue(c, ext) }
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

      def presentWithValue(cell: Cell[P], ext: V): TraversableOnce[Cell[Q]] = {
        self.presentWithValue(cell, ext).map { case c => Cell(rename(cell, c, ext), c.content) }
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
  def andThenExpandWithValue[R <: Position](expand: (Cell[P], Cell[Q], V) => R)(implicit ev: PosExpDep[Q, R]) = {
    new TransformerWithValue[P, R] {
      type V = self.V

      def presentWithValue(cell: Cell[P], ext: V): TraversableOnce[Cell[R]] = {
        self.presentWithValue(cell, ext).map { case c => Cell(expand(cell, c, ext), c.content) }
      }
    }
  }
}

/** Companion object to the `TransformerWithValue` trait. */
object TransformerWithValue {
  /**
   * Rename a dimension.
   *
   * @param dim  The dimension to rename.
   * @param name The rename pattern. Use `%[12]$``s` for the coordinate and original value respectively.
   *
   * @return A function that can be passed to `TransformerWithValue.andThenRenameWithValue`.
   */
  def rename[P <: Position, Q <: Position, V](dim: Dimension, name: String): (Cell[P], Cell[Q], V) => Q = {
    (before: Cell[P], after: Cell[Q], V) =>
      after.position.update(dim, name.format(after.position(dim).toShortString, before.content.value.toShortString))
  }

  /**
   * Expand position by appending a coordinate.
   *
   * @param name The value to expand with.
   *
   * @return A function that can be passed to `TransformerWithValue.andThenExpandWithValue`.
   */
  def expand[P <: Position, Q <: Position with ExpandablePosition, V, W](name: W)(
    implicit ev: Valueable[W]): (Cell[P], Cell[Q], V) => Q#M = {
    (before: Cell[P], after: Cell[Q], ext: V) => after.position.append(name)
  }

  /**
   * Expand position by appending a coordinate.
   *
   * @param dim  The dimension used to construct the new coordinate name.
   * @param name The name pattern. Use `%[12]$``s` for the coordinate and original value respectively.
   *
   * @return A function that can be passed to `TransformerWithValue.andThenExpandWithValue`.
   */
  def expand[P <: Position, Q <: Position with ExpandablePosition, V](dim: Dimension,
    name: String): (Cell[P], Cell[Q], V) => Q#M = {
    (before: Cell[P], after: Cell[Q], ext: V) =>
      after.position.append(name.format(after.position(dim).toShortString, before.content.value.toShortString))
  }
}

/** Trait for transforming a type `T` to a `Transformer[P, Q]`. */
trait Transformable[P <: Position, Q <: Position] extends java.io.Serializable {
  /** Returns a `Transformer[P, Q]` for this type `T`. */
  def apply(): Transformer[P, Q]
}

/** Companion object for the `Transformable` type class. */
object Transformable {
  /** Converts a `(Cell[P]) => Cell[Q]` to a `Transformer[P, Q]`. */
  implicit def CQ2T[P <: Position, Q <: Position](t: (Cell[P]) => Cell[Q]): Transformable[P, Q] = {
    new Transformable[P, Q] {
      def apply(): Transformer[P, Q] = {
        new Transformer[P, Q] { def present(cell: Cell[P]): TraversableOnce[Cell[Q]] = Some(t(cell)) }
      }
    }
  }

  /** Converts a `(Cell[P]) => List[Cell[Q]]` to a `Transformer[P, Q]`. */
  implicit def LCQ2T[P <: Position, Q <: Position](t: (Cell[P]) => List[Cell[Q]]): Transformable[P, Q] = {
    new Transformable[P, Q] {
      def apply(): Transformer[P, Q] = {
        new Transformer[P, Q] { def present(cell: Cell[P]): TraversableOnce[Cell[Q]] = t(cell) }
      }
    }
  }

  /** Converts a `Transformer[P, Q]` to a `Transformer[P, Q]`; that is, it is a pass through. */
  implicit def T2T[P <: Position, Q <: Position](t: Transformer[P, Q]): Transformable[P, Q] = {
    new Transformable[P, Q] { def apply(): Transformer[P, Q] = t }
  }

  /** Converts a `List[Transformer[P, Q]]` to a single `Transformer[P, Q]`. */
  implicit def LT2T[P <: Position, Q <: Position](t: List[Transformer[P, Q]]): Transformable[P, Q] = {
    new Transformable[P, Q] {
      def apply(): Transformer[P, Q] = {
        new Transformer[P, Q] {
          def present(cell: Cell[P]): TraversableOnce[Cell[Q]] = t.flatMap { case s => s.present(cell) }
        }
      }
    }
  }
}

/** Trait for transforming a type `T` to a `TransformerWithValue[P, Q]`. */
trait TransformableWithValue[P <: Position, Q <: Position, W] {
  /** Returns a `TransformerWithValue[P, Q]` for this type `T`. */
  def apply(): TransformerWithValue[P, Q] { type V >: W }
}

/** Companion object for the `TransformableWithValue` trait. */
object TransformableWithValue {
  /** Converts a `(Cell[P], W) => Cell[Q]` to a `TransformerWithValue[P, Q] { type V >: W }`. */
  implicit def CQW2TWV[P <: Position, Q <: Position, W](
    t: (Cell[P], W) => Cell[Q]): TransformableWithValue[ P, Q, W] = {
    new TransformableWithValue[P, Q, W] {
      def apply(): TransformerWithValue[P, Q] { type V >: W } = {
        new TransformerWithValue[P, Q] {
          type V = W

          def presentWithValue(cell: Cell[P], ext: W): TraversableOnce[Cell[Q]] = Some(t(cell, ext))
        }
      }
    }
  }

  /** Converts a `(Cell[P], W) => List[Cell[Q]]` to a `TransformerWithValue[P, Q] { type V >: W }`. */
  implicit def LCQW2TWV[P <: Position, Q <: Position, W](
    t: (Cell[P], W) => List[Cell[Q]]): TransformableWithValue[P, Q, W] = {
    new TransformableWithValue[P, Q, W] {
      def apply(): TransformerWithValue[P, Q] { type V >: W } = {
        new TransformerWithValue[P, Q] {
          type V = W

          def presentWithValue(cell: Cell[P], ext: W): TraversableOnce[Cell[Q]] = t(cell, ext)
        }
      }
    }
  }

  /**
   * Converts a `TransformerWithValue[P, Q] { type V >: W }` to a `TransformerWithValue[P, Q] { type V >: W }`;
   * that is, it is a pass through.
   */
  implicit def TWV2TWV[P <: Position, Q <: Position, W](
    t: TransformerWithValue[P, Q] { type V >: W }): TransformableWithValue[P, Q, W] = {
    new TransformableWithValue[P, Q, W] { def apply(): TransformerWithValue[P, Q] { type V >: W } = t }
  }

  /**
   * Converts a `List[TransformerWithValue[P, Q] { type V >: W }]` to a single
   * `TransformerWithValue[P, Q] { type V >: W }`.
   */
  implicit def LTWV2TWV[P <: Position, Q <: Position, W](
    t: List[TransformerWithValue[P, Q] { type V >: W }]): TransformableWithValue[P, Q, W] = {
    new TransformableWithValue[P, Q, W] {
      def apply(): TransformerWithValue[P, Q] { type V >: W } = {
        new TransformerWithValue[P, Q] {
          type V = W

          def presentWithValue(cell: Cell[P], ext: V): TraversableOnce[Cell[Q]] = {
            t.flatMap { case s => s.presentWithValue(cell, ext) }
          }
        }
      }
    }
  }
}

