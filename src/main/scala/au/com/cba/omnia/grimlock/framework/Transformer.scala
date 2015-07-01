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
  def andThenExpand[R <: Position](expand: (Cell[P], Cell[Q]) => R)(implicit ev: PosExpDep[Q, R]) = {
    new Transformer[P, R] {
      def present(cell: Cell[P]): Collection[Cell[R]] = {
        Collection(self.present(cell).toList.map { case c => Cell(expand(cell, c), c.content) })
      }
    }
  }
}

object Transformer {
  def rename[P <: Position, Q <: Position](dim: Dimension, name: String): (Cell[P], Cell[Q]) => Q = {
    (before: Cell[P], after: Cell[Q]) =>
      after.position.update(dim, name.format(after.position(dim).toShortString, before.content.value.toShortString))
  }

  def expand[P <: Position, Q <: Position with ExpandablePosition, V](name: V)(
    implicit ev: Valueable[V]): (Cell[P], Cell[Q]) => Q#M = {
    (before: Cell[P], after: Cell[Q]) => after.position.append(name)
  }

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
  def andThenExpandWithValue[R <: Position](expand: (Cell[P], Cell[Q], V) => R)(implicit ev: PosExpDep[Q, R]) = {
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
    (before: Cell[P], after: Cell[Q], V) =>
      after.position.update(dim, name.format(after.position(dim).toShortString, before.content.value.toShortString))
  }

  def expand[P <: Position, Q <: Position with ExpandablePosition, V, W](name: W)(
    implicit ev: Valueable[W]): (Cell[P], Cell[Q], V) => Q#M = {
    (before: Cell[P], after: Cell[Q], ext: V) => after.position.append(name)
  }

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
  /** Converts a `(Cell[P]) => Cell[Q]` to a `Transformer[P, Q]`. */
  implicit def C2T[P <: Position, Q <: Position]: Transformable[(Cell[P]) => Cell[Q], P, Q] = {
    new Transformable[(Cell[P]) => Cell[Q], P, Q] {
      def convert(t: (Cell[P]) => Cell[Q]): Transformer[P, Q] = {
        new Transformer[P, Q] { def present(cell: Cell[P]): Collection[Cell[Q]] = Collection(t(cell)) }
      }
    }
  }

  /** Converts a `(Cell[P]) => List[Cell[Q]]` to a `Transformer[P, Q]`. */
  implicit def LC2T[P <: Position, Q <: Position]: Transformable[(Cell[P]) => List[Cell[Q]], P, Q] = {
    new Transformable[(Cell[P]) => List[Cell[Q]], P, Q] {
      def convert(t: (Cell[P]) => List[Cell[Q]]): Transformer[P, Q] = {
        new Transformer[P, Q] { def present(cell: Cell[P]): Collection[Cell[Q]] = Collection(t(cell)) }
      }
    }
  }

  /** Converts a `(Cell[P]) => Collection[Cell[Q]]` to a `Transformer[P, Q]`. */
  implicit def CC2T[P <: Position, Q <: Position]: Transformable[(Cell[P]) => Collection[Cell[Q]], P, Q] = {
    new Transformable[(Cell[P]) => Collection[Cell[Q]], P, Q] {
      def convert(t: (Cell[P]) => Collection[Cell[Q]]): Transformer[P, Q] = {
        new Transformer[P, Q] { def present(cell: Cell[P]): Collection[Cell[Q]] = t(cell) }
      }
    }
  }

  /** Converts a `Transformer[P, Q]` to a `Transformer[P, Q]`; that is, it is a pass through. */
  implicit def TPQ2T[P <: Position, Q <: Position, T <: Transformer[P, Q]]: Transformable[T, P, Q] = {
    new Transformable[T, P, Q] { def convert(t: T): Transformer[P, Q] = t }
  }
  implicit def TPP2T[P <: Position, T <: Transformer[P, P]]: Transformable[T, P, P] = TPQ2T[P, P, T]
  implicit def TPPM2T[P <: Position with ExpandablePosition, T <: Transformer[P, P#M]]: Transformable[T, P, P#M] = TPQ2T[P, P#M, T]

  /** Converts a `List[Transformer[P, Q]]` to a single `Transformer[P, Q]`. */
  implicit def LTPQ2T[P <: Position, Q <: Position, T <: Transformer[P, Q]]: Transformable[List[T], P, Q] = {
    new Transformable[List[T], P, Q] {
      def convert(t: List[T]): Transformer[P, Q] = {
        new Transformer[P, Q] {
          def present(cell: Cell[P]): Collection[Cell[Q]] = Collection(t.flatMap { case s => s.present(cell).toList })
        }
      }
    }
  }
  implicit def LTPP2T[P <: Position, T <: Transformer[P, P]]: Transformable[List[T], P, P] = LTPQ2T[P, P, T]
  implicit def LTPPM2T[P <: Position with ExpandablePosition, T <: Transformer[P, P#M]]: Transformable[List[T], P, P#M] = LTPQ2T[P, P#M, T]
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
  /** Converts a `(Cell[P], W) => Cell[Q]` to a `TransformerWithValue[P, Q] { type V >: W }`. */
  implicit def CWC2TWV[P <: Position, Q <: Position, W]: TransformableWithValue[(Cell[P], W) => Cell[Q], P, Q, W] = {
    new TransformableWithValue[(Cell[P], W) => Cell[Q], P, Q, W] {
      def convert(t: (Cell[P], W) => Cell[Q]): TransformerWithValue[P, Q] { type V >: W } = {
        new TransformerWithValue[P, Q] {
          type V = W

          def presentWithValue(cell: Cell[P], ext: W): Collection[Cell[Q]] = Collection(t(cell, ext))
        }
      }
    }
  }

  /** Converts a `(Cell[P], W) => List[Cell[Q]]` to a `TransformerWithValue[P, Q] { type V >: W }`. */
  implicit def CWLC2TWV[P <: Position, Q <: Position, W]: TransformableWithValue[(Cell[P], W) => List[Cell[Q]], P, Q, W] = {
    new TransformableWithValue[(Cell[P], W) => List[Cell[Q]], P, Q, W] {
      def convert(t: (Cell[P], W) => List[Cell[Q]]): TransformerWithValue[P, Q] { type V >: W } = {
        new TransformerWithValue[P, Q] {
          type V = W

          def presentWithValue(cell: Cell[P], ext: W): Collection[Cell[Q]] = Collection(t(cell, ext))
        }
      }
    }
  }

  /** Converts a `(Cell[P], W) => Collection[Cell[Q]]` to a `TransformerWithValue[P, Q] { type V >: W }`. */
  implicit def CWCC2TWV[P <: Position, Q <: Position, W]: TransformableWithValue[(Cell[P], W) => Collection[Cell[Q]], P, Q, W] = {
    new TransformableWithValue[(Cell[P], W) => Collection[Cell[Q]], P, Q, W] {
      def convert(t: (Cell[P], W) => Collection[Cell[Q]]): TransformerWithValue[P, Q] { type V >: W } = {
        new TransformerWithValue[P, Q] {
          type V = W

          def presentWithValue(cell: Cell[P], ext: W): Collection[Cell[Q]] = t(cell, ext)
        }
      }
    }
  }

  /**
   * Converts a `TransformerWithValue[P, Q] { type V >: W }` to a `TransformerWithValue[P, Q] { type V >: W }`;
   * that is, it is a pass through.
   */
  implicit def TPQ2TWV[P <: Position, Q <: Position, T <: TransformerWithValue[P, Q] { type V >: W }, W]: TransformableWithValue[T, P, Q, W] = {
    new TransformableWithValue[T, P, Q, W] { def convert(t: T): TransformerWithValue[P, Q] { type V >: W } = t }
  }
  implicit def TPP2TWV[P <: Position, T <: TransformerWithValue[P, P] { type V >: W }, W]: TransformableWithValue[T, P, P, W] = TPQ2TWV[P, P, T, W]
  implicit def TPPM2TWV[P <: Position with ExpandablePosition, T <: TransformerWithValue[P, P#M] { type V >: W }, W]: TransformableWithValue[T, P, P#M, W] = TPQ2TWV[P, P#M, T, W]

  /**
   * Converts a `List[TransformerWithValue[P, Q] { type V >: W }]` to a single
   * `TransformerWithValue[P, Q] { type V >: W }`.
   */
  implicit def LTPQ2TWV[P <: Position, Q <: Position, T <: TransformerWithValue[P, Q] { type V >: W }, W]: TransformableWithValue[List[T], P, Q, W] = {
    new TransformableWithValue[List[T], P, Q, W] {
      def convert(t: List[T]): TransformerWithValue[P, Q] { type V >: W } = {
        new TransformerWithValue[P, Q] {
          type V = W

          def presentWithValue(cell: Cell[P], ext: V): Collection[Cell[Q]] = {
            Collection(t.flatMap { case s => s.presentWithValue(cell, ext).toList })
          }
        }
      }
    }
  }
  implicit def LTPP2TWV[P <: Position, T <: TransformerWithValue[P, P] { type V >: W }, W]: TransformableWithValue[List[T], P, P, W] = LTPQ2TWV[P, P, T, W]
  implicit def LTPPM2TWV[P <: Position with ExpandablePosition, T <: TransformerWithValue[P, P#M] { type V >: W }, W]: TransformableWithValue[List[T], P, P#M, W] = LTPQ2TWV[P, P#M, T, W]
}

