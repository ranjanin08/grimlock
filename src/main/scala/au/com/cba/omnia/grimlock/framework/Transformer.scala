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

/** Type class for transforming a type `T` to a `Transformer[P, Q]`. */
trait Transformable[T, P <: Position, Q <: Position] {
  /**
   * Returns a `Transformer[P, Q]` for type `T`.
   *
   * @param t Object that can be converted to a `Transformer[P, Q]`.
   */
  def convert(t: T): Transformer[P, Q]
}

/** Companion object for the `Transformable` type class. */
object Transformable {
  /** Converts a `(Cell[P]) => Cell[P]` to a `Transformer[P, P]`. */
  implicit def CPP2T[P <: Position]: Transformable[(Cell[P]) => Cell[P], P, P] = C2T[P, P]

  /** Converts a `(Cell[P]) => Cell[P#M]` to a `Transformer[P, P#M]`. */
  implicit def CPPM2T[P <: Position with ExpandablePosition]: Transformable[(Cell[P]) => Cell[P#M], P, P#M] = C2T[P, P#M]

  /** Converts a `(Cell[P]) => Cell[Q]` to a `Transformer[P, Q]`. */
  implicit def CPQ2T[P <: Position, Q <: Position](implicit ev: PosExpDep[P, Q]): Transformable[(Cell[P]) => Cell[Q], P, Q] = C2T[P, Q]

  private def C2T[P <: Position, Q <: Position]: Transformable[(Cell[P]) => Cell[Q], P, Q] = {
    new Transformable[(Cell[P]) => Cell[Q], P, Q] {
      def convert(t: (Cell[P]) => Cell[Q]): Transformer[P, Q] = {
        new Transformer[P, Q] { def present(cell: Cell[P]): TraversableOnce[Cell[Q]] = Some(t(cell)) }
      }
    }
  }

  /** Converts a `(Cell[P]) => List[Cell[P]]` to a `Transformer[P, P]`. */
  implicit def LCPP2T[P <: Position]: Transformable[(Cell[P]) => List[Cell[P]], P, P] = LC2T[P, P]

  /** Converts a `(Cell[P]) => List[Cell[P#M]]` to a `Transformer[P, P#M]`. */
  implicit def LCPPM2T[P <: Position with ExpandablePosition]: Transformable[(Cell[P]) => List[Cell[P#M]], P, P#M] = LC2T[P, P#M]

  /** Converts a `(Cell[P]) => List[Cell[Q]]` to a `Transformer[P, Q]`. */
  implicit def LCPQ2T[P <: Position, Q <: Position](implicit ev: PosExpDep[P, Q]): Transformable[(Cell[P]) => List[Cell[Q]], P, Q] = LC2T[P, Q]

  private def LC2T[P <: Position, Q <: Position]: Transformable[(Cell[P]) => List[Cell[Q]], P, Q] = {
    new Transformable[(Cell[P]) => List[Cell[Q]], P, Q] {
      def convert(t: (Cell[P]) => List[Cell[Q]]): Transformer[P, Q] = {
        new Transformer[P, Q] { def present(cell: Cell[P]): TraversableOnce[Cell[Q]] = t(cell) }
      }
    }
  }

  /** Converts a `Transformer[P, P]` to a `Transformer[P, P]`; that is, it is a pass through. */
  implicit def TPP2T[P <: Position, T <: Transformer[P, P]]: Transformable[T, P, P] = T2T[P, P, T]

  /** Converts a `Transformer[P, P#M]` to a `Transformer[P, P#M]`; that is, it is a pass through. */
  implicit def TPPM2T[P <: Position with ExpandablePosition, T <: Transformer[P, P#M]]: Transformable[T, P, P#M] = T2T[P, P#M, T]

  /** Converts a `Transformer[P, Q]` to a `Transformer[P, Q]`; that is, it is a pass through. */
  implicit def TPQ2T[P <: Position, Q <: Position, T <: Transformer[P, Q]](implicit ev: PosExpDep[P, Q]): Transformable[T, P, Q] = T2T[P, Q, T]

  private def T2T[P <: Position, Q <: Position, T <: Transformer[P, Q]]: Transformable[T, P, Q] = {
    new Transformable[T, P, Q] { def convert(t: T): Transformer[P, Q] = t }
  }

  /** Converts a `List[Transformer[P, P]]` to a single `Transformer[P, P]`. */
  implicit def LTPP2T[P <: Position, T <: Transformer[P, P]]: Transformable[List[T], P, P] = LT2T[P, P, T]

  /** Converts a `List[Transformer[P, P#M]]` to a single `Transformer[P, P#M]`. */
  implicit def LTPPM2T[P <: Position with ExpandablePosition, T <: Transformer[P, P#M]]: Transformable[List[T], P, P#M] = LT2T[P, P#M, T]

  /** Converts a `List[Transformer[P, Q]]` to a single `Transformer[P, Q]`. */
  implicit def LTPQ2T[P <: Position, Q <: Position, T <: Transformer[P, Q]](implicit ev: PosExpDep[P, Q]): Transformable[List[T], P, Q] = LT2T[P, Q, T]

  private def LT2T[P <: Position, Q <: Position, T <: Transformer[P, Q]]: Transformable[List[T], P, Q] = {
    new Transformable[List[T], P, Q] {
      def convert(t: List[T]): Transformer[P, Q] = {
        new Transformer[P, Q] {
          def present(cell: Cell[P]): TraversableOnce[Cell[Q]] = t.flatMap { case s => s.present(cell) }
        }
      }
    }
  }
}

/** Type class for transforming a type `T` to a `TransformerWithValue[P, Q]`. */
trait TransformableWithValue[T, P <: Position, Q <: Position, W] {
  /**
   * Returns a `TransformerWithValue[P, Q]` for type `T`.
   *
   * @param t Object that can be converted to a `TransformerWithValue[P, Q]`.
   */
  def convert(t: T): TransformerWithValue[P, Q] { type V >: W }
}

/** Companion object for the `TransformableWithValue` type class. */
object TransformableWithValue {
  /** Converts a `(Cell[P], W) => Cell[P]` to a `TransformerWithValue[P, P] { type V >: W }`. */
  implicit def CPPW2TWV[P <: Position, W]: TransformableWithValue[(Cell[P], W) => Cell[P], P, P, W] = C2TWV[P, P, W]

  /** Converts a `(Cell[P], W) => Cell[P#M]` to a `TransformerWithValue[P, P#M] { type V >: W }`. */
  implicit def CPPMW2TWV[P <: Position with ExpandablePosition, W]: TransformableWithValue[(Cell[P], W) => Cell[P#M], P, P#M, W] = C2TWV[P, P#M, W]

  /** Converts a `(Cell[P], W) => Cell[Q]` to a `TransformerWithValue[P, Q] { type V >: W }`. */
  implicit def CPQW2TWV[P <: Position, Q <: Position, W](implicit ev: PosExpDep[P, Q]): TransformableWithValue[(Cell[P], W) => Cell[Q], P, Q, W] = C2TWV[P, Q, W]

  private def C2TWV[P <: Position, Q <: Position, W]: TransformableWithValue[(Cell[P], W) => Cell[Q], P, Q, W] = {
    new TransformableWithValue[(Cell[P], W) => Cell[Q], P, Q, W] {
      def convert(t: (Cell[P], W) => Cell[Q]): TransformerWithValue[P, Q] { type V >: W } = {
        new TransformerWithValue[P, Q] {
          type V = W

          def presentWithValue(cell: Cell[P], ext: W): TraversableOnce[Cell[Q]] = Some(t(cell, ext))
        }
      }
    }
  }

  /** Converts a `(Cell[P], W) => List[Cell[P]]` to a `TransformerWithValue[P, P] { type V >: W }`. */
  implicit def LCPPW2TWV[P <: Position, W]: TransformableWithValue[(Cell[P], W) => List[Cell[P]], P, P, W] = LC2TWV[P, P, W]

  /** Converts a `(Cell[P], W) => List[Cell[P#M]]` to a `TransformerWithValue[P, P#M] { type V >: W }`. */
  implicit def LCPPMW2TWV[P <: Position with ExpandablePosition, W]: TransformableWithValue[(Cell[P], W) => List[Cell[P#M]], P, P#M, W] = LC2TWV[P, P#M, W]

  /** Converts a `(Cell[P], W) => List[Cell[Q]]` to a `TransformerWithValue[P, Q] { type V >: W }`. */
  implicit def LCPQW2TWV[P <: Position, Q <: Position, W](implicit ev: PosExpDep[P, Q]): TransformableWithValue[(Cell[P], W) => List[Cell[Q]], P, Q, W] = LC2TWV[P, Q, W]

  private def LC2TWV[P <: Position, Q <: Position, W]: TransformableWithValue[(Cell[P], W) => List[Cell[Q]], P, Q, W] = {
    new TransformableWithValue[(Cell[P], W) => List[Cell[Q]], P, Q, W] {
      def convert(t: (Cell[P], W) => List[Cell[Q]]): TransformerWithValue[P, Q] { type V >: W } = {
        new TransformerWithValue[P, Q] {
          type V = W

          def presentWithValue(cell: Cell[P], ext: W): TraversableOnce[Cell[Q]] = t(cell, ext)
        }
      }
    }
  }

  /**
   * Converts a `TransformerWithValue[P, P] { type V >: W }` to a `TransformerWithValue[P, P] { type V >: W }`;
   * that is, it is a pass through.
   */
  implicit def TPPW2TWV[P <: Position, T <: TransformerWithValue[P, P] { type V >: W }, W]: TransformableWithValue[T, P, P, W] = T2TWV[P, P, T, W]

  /**
   * Converts a `TransformerWithValue[P, P#M] { type V >: W }` to a `TransformerWithValue[P, P#M] { type V >: W }`;
   * that is, it is a pass through.
   */
  implicit def TPPMW2TWV[P <: Position with ExpandablePosition, T <: TransformerWithValue[P, P#M] { type V >: W }, W]: TransformableWithValue[T, P, P#M, W] = T2TWV[P, P#M, T, W]

  /**
   * Converts a `TransformerWithValue[P, Q] { type V >: W }` to a `TransformerWithValue[P, Q] { type V >: W }`;
   * that is, it is a pass through.
   */
  implicit def TPQW2TWV[P <: Position, Q <: Position, T <: TransformerWithValue[P, Q] { type V >: W }, W](implicit ev: PosExpDep[P, Q]): TransformableWithValue[T, P, Q, W] = T2TWV[P, Q, T, W]

  private def T2TWV[P <: Position, Q <: Position, T <: TransformerWithValue[P, Q] { type V >: W }, W]: TransformableWithValue[T, P, Q, W] = {
    new TransformableWithValue[T, P, Q, W] { def convert(t: T): TransformerWithValue[P, Q] { type V >: W } = t }
  }

  /**
   * Converts a `List[TransformerWithValue[P, P] { type V >: W }]` to a single
   * `TransformerWithValue[P, P] { type V >: W }`.
   */
  implicit def LTPPW2TWV[P <: Position, T <: TransformerWithValue[P, P] { type V >: W }, W]: TransformableWithValue[List[T], P, P, W] = LT2TWV[P, P, T, W]

  /**
   * Converts a `List[TransformerWithValue[P, P#M] { type V >: W }]` to a single
   * `TransformerWithValue[P, P#M] { type V >: W }`.
   */
  implicit def LTPPMW2TWV[P <: Position with ExpandablePosition, T <: TransformerWithValue[P, P#M] { type V >: W }, W]: TransformableWithValue[List[T], P, P#M, W] = LT2TWV[P, P#M, T, W]

  /**
   * Converts a `List[TransformerWithValue[P, Q] { type V >: W }]` to a single
   * `TransformerWithValue[P, Q] { type V >: W }`.
   */
  implicit def LTPQ2WTWV[P <: Position, Q <: Position, T <: TransformerWithValue[P, Q] { type V >: W }, W](implicit ev: PosExpDep[P, Q]): TransformableWithValue[List[T], P, Q, W] = LT2TWV[P, Q, T, W]

  private def LT2TWV[P <: Position, Q <: Position, T <: TransformerWithValue[P, Q] { type V >: W }, W]: TransformableWithValue[List[T], P, Q, W] = {
    new TransformableWithValue[List[T], P, Q, W] {
      def convert(t: List[T]): TransformerWithValue[P, Q] { type V >: W } = {
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

