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
import au.com.cba.omnia.grimlock.framework.position._
import au.com.cba.omnia.grimlock.framework.utility._

/** Base trait for transformations. */
trait Transformer[P <: Position, Q <: Position]

object Transformer {
  def rename[P <: Position](dim: Dimension, name: String): (Cell[P]) => P = {
    (cell: Cell[P]) => cell.position.update(dim,
      name.format(cell.position(dim).toShortString, cell.content.value.toShortString))
  }

  def renameWithValue[P <: Position, V](dim: Dimension, name: String): (Cell[P], V) => P = {
    (cell: Cell[P], V) => cell.position.update(dim,
      name.format(cell.position(dim).toShortString, cell.content.value.toShortString))
  }
}

/** Base trait for transformations from `P` to `Q`. */
trait Present[P <: Position, Q <: Position] extends PresentWithValue[P, Q] { self: Transformer[P, Q] =>
  type V = Any

  def present(cell: Cell[P], ext: V): Collection[Cell[Q]] = present(cell)

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
  def andThen[R <: Position](that: Transformer[Q, R] with Present[Q, R]) = AndThenTransformer(this, that)

  /**
   * Operator for transforming and then renaming dimensions.
   *
   * @param rename The rename to apply after the transformation.
   *
   * @return A transformer that runs `this` and then renames the resulting dimension(s).
   */
  def andThenRename(rename: (Cell[Q]) => Q): Transformer[P, Q] with Present[P, Q] = {
    new Transformer[P, Q] with Present[P, Q] {
      def present(cell: Cell[P]): Collection[Cell[Q]] = {
        Collection(Present.this.present(cell).toList.map { case c => Cell(rename(c), c.content) })
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
  def andThenExpand[R <: Position](expand: (Cell[Q]) => R)(
    implicit ev: ExpPosDep[Q, R]): Transformer[P, R] with Present[P, R] = {
    new Transformer[P, R] with Present[P, R] {
      def present(cell: Cell[P]): Collection[Cell[R]] = {
        Collection(Present.this.present(cell).toList.map { case c => Cell(expand(c), c.content) })
      }
    }
  }
}

/** Base trait for transformations from `P` to `Q` that use a user supplied value. */
trait PresentWithValue[P <: Position, Q <: Position] { self: Transformer[P, Q] =>
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
  def present(cell: Cell[P], ext: V): Collection[Cell[Q]]

  /**
   * Operator for chaining transformations.
   *
   * @param that The transformation to perform after `this`.
   *
   * @return A transformer that runs `this` and then `that`.
   */
  def andThen[R <: Position](
    that: Transformer[Q, R] with PresentWithValue[Q, R] { type V >: self.V }): Transformer[P, R] = {
    new Transformer[P, R] with PresentWithValue[P, R] {
      type V = self.V

      def present(cell: Cell[P], ext: V): Collection[Cell[R]] = {
        Collection(self.present(cell, ext).toList.flatMap { case c => that.present(c, ext).toList })
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
  def andThenRenameWithValue(rename: (Cell[Q], V) => Q) = AndThenRenameWithValue(this, rename)
  /*: Transformer[P, Q] with PresentWithValue[P, Q] = {
    new Transformer[P, Q] with PresentWithValue[P, Q] {
      type V = self.V

      def present(cell: Cell[P], ext: V): Collection[Cell[Q]] = {
        Collection(self.present(cell, ext).toList.map { case c => Cell(rename(c, ext), c.content) })
      }
    }
  }*/

  /**
   * Operator for transforming and then expanding dimensions.
   *
   * @param expand The expansion to apply after the transformation.
   *
   * @return A transformer that runs `this` and then expands the resulting dimensions.
   */
  def andThenExpandWithValue[R <: Position](expand: (Cell[Q], V) => R)(
    implicit ev: ExpPosDep[Q, R]): Transformer[P, R] with PresentWithValue[P, R] = {
    new Transformer[P, R] with PresentWithValue[P, R] {
      type V = self.V

      def present(cell: Cell[P], ext: V): Collection[Cell[R]] = {
        Collection(self.present(cell, ext).toList.map { case c => Cell(expand(c, ext), c.content) })
      }
    }
  }
}

/**
 * Transformer that is a composition of two transformers.
 *
 * @param first  The first transformation to appy.
 * @param second The second transformation to appy.
 *
 * @note This need not be called in an application. The `andThen` method will create it.
 */
case class AndThenTransformer[P <: Position, Q <: Position, R <: Position](first: Transformer[P, Q] with Present[P, Q],
  second: Transformer[Q, R] with Present[Q, R]) extends Transformer[P, R] with Present[P, R] {
  def present(cell: Cell[P]): Collection[Cell[R]] = {
    Collection(first.present(cell).toList.flatMap { case c => second.present(c).toList })
  }
}

case class AndThenRenameWithValue[P <: Position, Q <: Position, W](
  first: Transformer[P, Q] with PresentWithValue[P, Q] { type V = W },
  rename: (Cell[Q], W) => Q) extends Transformer[P, Q] with PresentWithValue[P, Q] {
  type V = W

  def present(cell: Cell[P], ext: V): Collection[Cell[Q]] = {
    Collection(first.present(cell, ext).toList.map { case c => Cell(rename(c, ext), c.content) })
  }
}

/** Type class for transforming a type `T` to a `Transformer`. */
trait Transformable[P <: Position, Q <: Position, T] {
  /**
   * Returns a `Transformer` for type `T`.
   *
   * @param t Object that can be converted to a `Transformer`.
   */
  def convert(t: T): Transformer[P, Q] with Present[P, Q]
}

/** Companion object for the `Transformable` type class. */
object Transformable {
  /** Converts a function `Cell[P] => Cell[Q]` to a `Transformer`. */
  implicit def C2T[P <: Position, Q <: Position]: Transformable[P, Q, Cell[P] => Cell[Q]] = {
    new Transformable[P, Q, Cell[P] => Cell[Q]] {
      def convert(t: (Cell[P]) => Cell[Q]): Transformer[P, Q] with Present[P, Q] = {
        new Transformer[P, Q] with Present[P, Q] {
          def present(cell: Cell[P]): Collection[Cell[Q]] = Collection(t(cell))
        }
      }
    }
  }

  /** Converts a function `Cell[P] => List[Cell[Q]]` to a `Transformer`. */
  implicit def LC2T[P <: Position, Q <: Position]: Transformable[P, Q, Cell[P] => List[Cell[Q]]] = {
    new Transformable[P, Q, Cell[P] => List[Cell[Q]]] {
      def convert(t: (Cell[P]) => List[Cell[Q]]): Transformer[P, Q] with Present[P, Q] = {
        new Transformer[P, Q] with Present[P, Q] {
          def present(cell: Cell[P]): Collection[Cell[Q]] = Collection(t(cell))
        }
      }
    }
  }

  /** Converts a function `Cell[P] => Collection[Cell[Q]]` to a `Transformer`. */
  implicit def CC2T[P <: Position, Q <: Position]: Transformable[P, Q, Cell[P] => Collection[Cell[Q]]] = {
    new Transformable[P, Q, Cell[P] => Collection[Cell[Q]]] {
      def convert(t: (Cell[P]) => Collection[Cell[Q]]): Transformer[P, Q] with Present[P, Q] = {
        new Transformer[P, Q] with Present[P, Q] { def present(cell: Cell[P]): Collection[Cell[Q]] = t(cell) }
      }
    }
  }

  /** Converts a `Transformer` to a `Transformer`; that is, it is a pass through. */
  implicit def T2T[P <: Position, Q <: Position, T <: Transformer[P, Q] with Present[P, Q]]: Transformable[P, Q, T] = {
    new Transformable[P, Q, T] { def convert(t: T): Transformer[P, Q] with Present[P, Q] = t }
  }

  /** Converts a `List[Transformer]` to a `Transformer`. */
  implicit def LT2T[P <: Position, Q <: Position, T <: Transformer[P, Q] with Present[P, Q]]: Transformable[P, Q, List[T]] = {
    new Transformable[P, Q, List[T]] {
      def convert(t: List[T]): Transformer[P, Q] with Present[P, Q] = {
        new Transformer[P, Q] with Present[P, Q] {
          def present(cell: Cell[P]): Collection[Cell[Q]] = Collection(t.flatMap { case s => s.present(cell).toList })
        }
      }
    }
  }
}

/** Type class for transforming a type `T` to a `Transformer`. */
trait TransformableWithValue[P <: Position, Q <: Position, T, W] {
  /**
   * Returns a `Transformer` for type `T`.
   *
   * @param t Object that can be converted to a `Transformer`.
   */
  def convert(t: T): Transformer[P, Q] with PresentWithValue[P, Q] { type V >: W }
}

/** Companion object for the `TransformableWithValue` type class. */
object TransformableWithValue {
  /** Converts a function `Cell[P] => Cell[Q]` to a `Transformer`. */
  implicit def C2TWV[P <: Position, Q <: Position, W]: TransformableWithValue[P, Q, (Cell[P], W) => Cell[Q], W] = {
    new TransformableWithValue[P, Q, (Cell[P], W) => Cell[Q], W] {
      def convert(t: (Cell[P], W) => Cell[Q]): Transformer[P, Q] with PresentWithValue[P, Q] { type V >: W } = {
        new Transformer[P, Q] with PresentWithValue[P, Q] {
          type V = W
          def present(cell: Cell[P], ext: W): Collection[Cell[Q]] = Collection(t(cell, ext))
        }
      }
    }
  }

  /** Converts a function `Cell[P] => List[Cell[Q]]` to a `Transformer`. */
  implicit def LC2TWV[P <: Position, Q <: Position, W]: TransformableWithValue[P, Q, (Cell[P], W) => List[Cell[Q]], W] = {
    new TransformableWithValue[P, Q, (Cell[P], W) => List[Cell[Q]], W] {
      def convert(t: (Cell[P], W) => List[Cell[Q]]): Transformer[P, Q] with PresentWithValue[P, Q] { type V >: W } = {
        new Transformer[P, Q] with PresentWithValue[P, Q] {
          type V = W
          def present(cell: Cell[P], ext: W): Collection[Cell[Q]] = Collection(t(cell, ext))
        }
      }
    }
  }

  /** Converts a function `Cell[P] => Collection[Cell[Q]]` to a `Transformer`. */
  implicit def CC2TWV[P <: Position, Q <: Position, W]: TransformableWithValue[P, Q, (Cell[P], W) => Collection[Cell[Q]], W] = {
    new TransformableWithValue[P, Q, (Cell[P], W) => Collection[Cell[Q]], W] {
      def convert(
        t: (Cell[P], W) => Collection[Cell[Q]]): Transformer[P, Q] with PresentWithValue[P, Q] { type V >: W } = {
        new Transformer[P, Q] with PresentWithValue[P, Q] {
          type V = W
          def present(cell: Cell[P], ext: W): Collection[Cell[Q]] = t(cell, ext)
        }
      }
    }
  }

  /** Converts a `Transformer` to a `Transformer`; that is, it is a pass through. */
  implicit def T2TWV[P <: Position, Q <: Position, T <: Transformer[P, Q] with Present[P, Q] { type V >: W }, W]: TransformableWithValue[P, Q, T, W] = {
    new TransformableWithValue[P, Q, T, W] {
      def convert(t: T): Transformer[P, Q] with Present[P, Q] { type V >: W } = t
    }
  }

  /** Converts a `List[Transformer]` to a `Transformer`. */
  implicit def LT2TWV[P <: Position, Q <: Position, T <: Transformer[P, Q] with PresentWithValue[P, Q] { type V >: W}, W]: TransformableWithValue[P, Q, List[T], W] = {
    new TransformableWithValue[P, Q, List[T], W] {
      def convert(t: List[T]): Transformer[P, Q] with PresentWithValue[P, Q] { type V >: W } = {
        new Transformer[P, Q] with PresentWithValue[P, Q] {
          type V = W
          def present(cell: Cell[P], ext: V): Collection[Cell[Q]] = {
            Collection(t.flatMap { case s => s.present(cell, ext).toList })
          }
        }
      }
    }
  }
}

