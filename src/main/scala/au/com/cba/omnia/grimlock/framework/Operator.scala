// Copyright 2014,2015,2016 Commonwealth Bank of Australia
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

package au.com.cba.omnia.grimlock.framework.pairwise

import au.com.cba.omnia.grimlock.framework._
import au.com.cba.omnia.grimlock.framework.content._
import au.com.cba.omnia.grimlock.framework.position._

/** Base trait for comparing two positions to determine if pairwise operation is to be applied. */
trait Comparer {
  /**
   * Check, based on left and right positions, if pairwise operation should be computed.
   *
   * @param left  Left position.
   * @param right Right position.
   */
  def keep[P <: Position[P]](left: P, right: P): Boolean
}

/** Case object for computing all pairwise combinations. */
case object All extends Comparer {
  def keep[P <: Position[P]](left: P, right: P): Boolean = true
}

/** Case object for computing diagonal pairwise combinations (i.e. left == right). */
case object Diagonal extends Comparer {
  def keep[P <: Position[P]](left: P, right: P): Boolean = left.compare(right) == 0
}

/** Case object for computing upper triangular pairwise combinations (i.e. right > left). */
case object Upper extends Comparer {
  def keep[P <: Position[P]](left: P, right: P): Boolean = right.compare(left) > 0
}

/** Case object for computing upper triangular or diagonal pairwise combinations (i.e. right >= left). */
case object UpperDiagonal extends Comparer {
  def keep[P <: Position[P]](left: P, right: P): Boolean = right.compare(left) >= 0
}

/** Case object for computing lower triangular pairwise combinations (i.e. left > right). */
case object Lower extends Comparer {
  def keep[P <: Position[P]](left: P, right: P): Boolean = left.compare(right) > 0
}

/** Case object for computing lower triangular or diagonal pairwise combinations (i.e. left >= right). */
case object LowerDiagonal extends Comparer {
  def keep[P <: Position[P]](left: P, right: P): Boolean = left.compare(right) >= 0
}

/** Base trait for computing pairwise values. */
trait Operator[P <: Position[P], Q <: Position[Q]] extends OperatorWithValue[P, Q] { self =>
  type V = Any

  def computeWithValue(left: Cell[P], right: Cell[P], ext: V): TraversableOnce[Cell[Q]] = compute(left, right)

  /**
   * Indicate if the cell is selected as part of the sample.
   *
   * @param left  The left cell to compute with.
   * @param right The right cell to compute with.
   *
   * @note The return value is a `TraversableOnce` to allow, for example, upper or lower triangular matrices to
   *       be returned (this can be done by comparing the selected coordinates)
   */
  def compute(left: Cell[P], right: Cell[P]): TraversableOnce[Cell[Q]]

  /**
   * Operator for preparing content prior to pairwise operations.
   *
   * @param prep The function to apply prior to pariwise operations.
   *
   * @return An operator that prepares the content and then runs `this`.
   */
  override def withPrepare(prep: (Cell[P]) => Content) = {
    new Operator[P, Q] {
      def compute(left: Cell[P], right: Cell[P]): TraversableOnce[Cell[Q]] = {
        self.compute(Cell(left.position, prep(left)), Cell(right.position, prep(right)))
      }
    }
  }

  /**
   * Operator for pairwise operations and then updating the contents.
   *
   * @param mutate The mutation to apply after the pairwise operations.
   *
   * @return An operator that runs `this` and then updates the resulting contents.
   */
  override def andThenMutate(mutate: (Cell[Q]) => Content) = {
    new Operator[P, Q] {
      def compute(left: Cell[P], right: Cell[P]): TraversableOnce[Cell[Q]] = {
        self.compute(left, right).map { case c => Cell(c.position, mutate(c)) }
      }
    }
  }

  /**
   * Operator for pairwise operations and then relocating the contents.
   *
   * @param locate The relocation to apply after the operation.
   *
   * @return An operator that runs `this` and then relocates the resulting content.
   */
  override def andThenRelocate[R <: Position[R]](locate: Locate.FromCell[Q, R])(implicit ev: PosIncDep[Q, R]) = {
    new Operator[P, R] {
      def compute(left: Cell[P], right: Cell[P]): TraversableOnce[Cell[R]] = {
        self.compute(left, right).flatMap { case c => locate(c).map(Cell(_, c.content)) }
      }
    }
  }
}

/** Base trait for computing pairwise values with a user provided value. */
trait OperatorWithValue[P <: Position[P], Q <: Position[Q]] extends java.io.Serializable { self =>
  /** Type of the external value. */
  type V

  /**
   * Indicate if the cell is selected as part of the sample.
   *
   * @param left  The left cell to compute with.
   * @param right The right cell to compute with.
   * @param ext   The user define the value.
   *
   * @note The return value is a `TraversableOnce` to allow, for example, upper or lower triangular matrices to
   *       be returned (this can be done by comparing the selected coordinates).
   */
  def computeWithValue(left: Cell[P], right: Cell[P], ext: V): TraversableOnce[Cell[Q]]

  /**
   * Operator for preparing content prior to pairwise operations.
   *
   * @param prep The function to apply prior to pariwise operations.
   *
   * @return An operator that prepares the content and then runs `this`.
   */
  def withPrepare(prep: (Cell[P]) => Content) = {
    new OperatorWithValue[P, Q] {
      type V = self.V

      def computeWithValue(left: Cell[P], right: Cell[P], ext: V): TraversableOnce[Cell[Q]] = {
        self.computeWithValue(Cell(left.position, prep(left)), Cell(right.position, prep(right)), ext)
      }
    }
  }

  /**
   * Operator for pairwise operations and then updating the contents.
   *
   * @param mutate The mutation to apply after the pairwise operations.
   *
   * @return An operator that runs `this` and then updates the resulting contents.
   */
  def andThenMutate(mutate: (Cell[Q]) => Content) = {
    new OperatorWithValue[P, Q] {
      type V = self.V

      def computeWithValue(left: Cell[P], right: Cell[P], ext: V): TraversableOnce[Cell[Q]] = {
        self.computeWithValue(left, right, ext).map { case c => Cell(c.position, mutate(c)) }
      }
    }
  }

  /**
   * Operator for pairwise operations and then relocating the contents.
   *
   * @param locate The relocation to apply after the operation.
   *
   * @return An operator that runs `this` and then relocates the resulting content.
   */
  def andThenRelocate[R <: Position[R]](locate: Locate.FromCell[Q, R])(implicit ev: PosIncDep[Q, R]) = {
    new OperatorWithValue[P, R] {
      type V = self.V

      def computeWithValue(left: Cell[P], right: Cell[P], ext: V): TraversableOnce[Cell[R]] = {
        self.computeWithValue(left, right, ext).flatMap { case c => locate(c).map(Cell(_, c.content)) }
      }
    }
  }

  /**
   * Operator for preparing content prior to pairwise operations.
   *
   * @param prep The function to apply prior to pariwise operations.
   *
   * @return An operator that prepares the content and then runs `this`.
   */
  def withPrepareWithValue(prep: (Cell[P], V) => Content) = {
    new OperatorWithValue[P, Q] {
      type V = self.V

      def computeWithValue(left: Cell[P], right: Cell[P], ext: V): TraversableOnce[Cell[Q]] = {
        self.computeWithValue(Cell(left.position, prep(left, ext)), Cell(right.position, prep(right, ext)), ext)
      }
    }
  }

  /**
   * Operator for pairwise operations and then updating the contents.
   *
   * @param mutate The mutation to apply after the pairwise operations.
   *
   * @return An operator that runs `this` and then updates the resulting contents.
   */
  def andThenMutateWithValue(mutate: (Cell[Q], V) => Content) = {
    new OperatorWithValue[P, Q] {
      type V = self.V

      def computeWithValue(left: Cell[P], right: Cell[P], ext: V): TraversableOnce[Cell[Q]] = {
        self.computeWithValue(left, right, ext).map { case c => Cell(c.position, mutate(c, ext)) }
      }
    }
  }

  /**
   * Operator for pairwise operations and then relocating the contents.
   *
   * @param locate The relocation to apply after the operation.
   *
   * @return An operator that runs `this` and then relocates the resulting content.
   */
  def andThenRelocateWithValue[R <: Position[R]](locate: Locate.FromCellWithValue[Q, R, V])(
    implicit ev: PosIncDep[Q, R]) = {
    new OperatorWithValue[P, R] {
      type V = self.V

      def computeWithValue(left: Cell[P], right: Cell[P], ext: V): TraversableOnce[Cell[R]] = {
        self.computeWithValue(left, right, ext).flatMap { case c => locate(c, ext).map(Cell(_, c.content)) }
      }
    }
  }
}

/** Trait for transforming a type `T` to an `Operator[P, Q]`. */
trait Operable[P <: Position[P], Q <: Position[Q]] extends java.io.Serializable {
  /** Returns an `Operator[P, Q]` for this type `T`. */
  def apply(): Operator[P, Q]
}

/** Companion object for the `Operable` trait. */
object Operable {
  /** Converts a `(Cell[P], Cell[P]) => Cell[Q]` to a `Operator[P, Q]`. */
  implicit def CCQ2O[P <: Position[P], Q <: Position[Q]](t: (Cell[P], Cell[P]) => Cell[Q]): Operable[P, Q] = {
    new Operable[P, Q] {
      def apply(): Operator[P, Q] = {
        new Operator[P, Q] {
          def compute(left: Cell[P], right: Cell[P]): TraversableOnce[Cell[Q]] = Some(t(left, right))
        }
      }
    }
  }

  /** Converts a `(Cell[P], Cell[P]) => List[Cell[Q]]` to a `Operator[P, Q]`. */
  implicit def CCLQ2O[P <: Position[P], Q <: Position[Q]](t: (Cell[P], Cell[P]) => List[Cell[Q]]): Operable[P, Q] = {
    new Operable[P, Q] {
      def apply(): Operator[P, Q] = {
        new Operator[P, Q] {
          def compute(left: Cell[P], right: Cell[P]): TraversableOnce[Cell[Q]] = t(left, right)
        }
      }
    }
  }

  /** Converts a `Operator[P, Q]` to a `Operator[P, Q]`; that is, it is a pass through. */
  implicit def O2O[P <: Position[P], Q <: Position[Q]](t: Operator[P, Q]): Operable[P, Q] = {
    new Operable[P, Q] { def apply(): Operator[P, Q] = t }
  }

  /** Converts a `List[Operator[P, Q]]` to a single `Operator[P, Q]`. */
  implicit def LO2O[P <: Position[P], Q <: Position[Q]](t: List[Operator[P, Q]]): Operable[P, Q] = {
    new Operable[P, Q] {
      def apply(): Operator[P, Q] = {
        new Operator[P, Q] {
          def compute(left: Cell[P], right: Cell[P]): TraversableOnce[Cell[Q]] = {
            t.flatMap { case s => s.compute(left, right) }
          }
        }
      }
    }
  }
}

/** Trait for transforming a type `T` to an `OperatorWithValue[P, Q]`. */
trait OperableWithValue[P <: Position[P], Q <: Position[Q], W] extends java.io.Serializable {
  /** Returns a `OperatorWithValue[P, Q]` for this type `T`. */
  def apply(): OperatorWithValue[P, Q] { type V >: W }
}

/** Companion object for the `OperableWithValue` trait. */
object OperableWithValue {
  /** Converts a `(Cell[P], Cell[P], W) => Cell[Q]` to an `OperatorWithValue[P, Q] { type V >: W }`. */
  implicit def CCWQ2OWV[P <: Position[P], Q <: Position[Q], W](
    t: (Cell[P], Cell[P], W) => Cell[Q]): OperableWithValue[P, Q, W] = {
    new OperableWithValue[P, Q, W] {
      def apply(): OperatorWithValue[P, Q] { type V >: W } = {
        new OperatorWithValue[P, Q] {
          type V = W

          def computeWithValue(left: Cell[P], right: Cell[P], ext: W): TraversableOnce[Cell[Q]] = {
            Some(t(left, right, ext))
          }
        }
      }
    }
  }

  /** Converts a `(Cell[P], Cell[P], W) => List[Cell[Q]]` to an `OperatorWithValue[P, Q] { type V >: W }`. */
  implicit def CCWLQ2OWV[P <: Position[P], Q <: Position[Q], W](
    t: (Cell[P], Cell[P], W) => List[Cell[Q]]): OperableWithValue[P, Q, W] = {
    new OperableWithValue[P, Q, W] {
      def apply(): OperatorWithValue[P, Q] { type V >: W } = {
        new OperatorWithValue[P, Q] {
          type V = W

          def computeWithValue(left: Cell[P], right: Cell[P], ext: W): TraversableOnce[Cell[Q]] = t(left, right, ext)
        }
      }
    }
  }

  /**
   * Converts an `OperatorWithValue[P, Q] { type V >: W }` to an `OperatorWithValue[P, Q] { type V >: W }`;
   * that is, it is a pass through.
   */
  implicit def OWV2OWV[P <: Position[P], Q <: Position[Q], W](
    t: OperatorWithValue[P, Q] { type V >: W }): OperableWithValue[P, Q, W] = {
    new OperableWithValue[P, Q, W] { def apply(): OperatorWithValue[P, Q] { type V >: W } = t }
  }

  /**
   * Converts a `List[OperatorWithValue[P, Q] { type V >: W }]` to a single `OperatorWithValue[P, Q] { type V >: W }`.
   */
  implicit def LOWV2OWV[P <: Position[P], Q <: Position[Q], W](
    t: List[OperatorWithValue[P, Q] { type V >: W }]): OperableWithValue[P, Q, W] = {
    new OperableWithValue[P, Q, W] {
      def apply(): OperatorWithValue[P, Q] { type V >: W } = {
        new OperatorWithValue[P, Q] {
          type V = W

          def computeWithValue(left: Cell[P], right: Cell[P], ext: V): TraversableOnce[Cell[Q]] = {
            t.flatMap { case s => s.computeWithValue(left, right, ext) }
          }
        }
      }
    }
  }
}

