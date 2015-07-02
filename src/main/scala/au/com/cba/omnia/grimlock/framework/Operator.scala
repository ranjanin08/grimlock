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

package au.com.cba.omnia.grimlock.framework.pairwise

import au.com.cba.omnia.grimlock.framework._
import au.com.cba.omnia.grimlock.framework.position._
import au.com.cba.omnia.grimlock.framework.utility._

/** Base trait for comparing two positions to determine if pairwise operation is to be applied. */
trait Comparer {
  /**
   * Check, based on left and right positions, if pairwise operation should be computed.
   *
   * @param left  Left position.
   * @param right Right position.
   */
  def keep[P <: Position](left: P, right: P): Boolean
}

/** Case object for computing all pairwise combinations. */
case object All extends Comparer {
  def keep[P <: Position](left: P, right: P): Boolean = true
}

/** Case object for computing diagonal pairwise combinations (i.e. left == right). */
case object Diagonal extends Comparer {
  def keep[P <: Position](left: P, right: P): Boolean = left.compare(right) == 0
}

/** Case object for computing upper triangular pairwise combinations (i.e. right > left). */
case object Upper extends Comparer {
  def keep[P <: Position](left: P, right: P): Boolean = right.compare(left) > 0
}

/** Case object for computing upper triangular or diagonal pairwise combinations (i.e. right >= left). */
case object UpperDiagonal extends Comparer {
  def keep[P <: Position](left: P, right: P): Boolean = right.compare(left) >= 0
}

/** Case object for computing lower triangular pairwise combinations (i.e. left > right). */
case object Lower extends Comparer {
  def keep[P <: Position](left: P, right: P): Boolean = left.compare(right) > 0
}

/** Case object for computing lower triangular or diagonal pairwise combinations (i.e. left >= right). */
case object LowerDiagonal extends Comparer {
  def keep[P <: Position](left: P, right: P): Boolean = left.compare(right) >= 0
}

/** Base trait for computing pairwise values. */
trait Operator[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position]
  extends OperatorWithValue[S, R, Q] { self =>
  type V = Any

  def computeWithValue(left: Cell[S], right: Cell[S], rem: R, ext: V): Collection[Cell[Q]] = compute(left, right, rem)

  /**
   * Indicate if the cell is selected as part of the sample.
   *
   * @param left  The selected left cell to compute with.
   * @param right The selected right cell to compute with.
   * @param rem   The remaining coordinates.
   *
   * @note The return value is a `Collection` to allow, for example, upper or lower triangular matrices to be returned
   *       (this can be done by comparing the selected coordinates)
   */
  def compute(left: Cell[S], right: Cell[S], rem: R): Collection[Cell[Q]]

  /**
   * Operator for pairwise operations and then renaming dimensions.
   *
   * @param rename The rename to apply after the operation.
   *
   * @return An operator that runs `this` and then renames the resulting dimension(s).
   */
  def andThenRename(rename: (Cell[S], Cell[S], R, Cell[Q]) => Q) = {
    new Operator[S, R, Q] {
      def compute(left: Cell[S], right: Cell[S], rem: R): Collection[Cell[Q]] = {
        Collection(self.compute(left, right, rem).toList.map {
          case c => Cell(rename(left, right, rem, c), c.content)
        })
      }
    }
  }

  /**
   * Operator for pairwise operations and then expanding dimensions.
   *
   * @param expand The expansion to apply after the operation.
   *
   * @return An operator that runs `this` and then expands the resulting dimensions.
   */
  def andThenExpand[U <: Position](expand: (Cell[S], Cell[S], R, Cell[Q]) => U)(implicit ev: PosExpDep[Q, U]) = {
    new Operator[S, R, U] {
      def compute(left: Cell[S], right: Cell[S], rem: R): Collection[Cell[U]] = {
        Collection(self.compute(left, right, rem).toList.map {
          case c => Cell(expand(left, right, rem, c), c.content)
        })
      }
    }
  }
}

/** Base trait for computing pairwise values with a user provided value. */
trait OperatorWithValue[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position]
  extends java.io.Serializable { self =>
  /** Type of the external value. */
  type V

  /**
   * Indicate if the cell is selected as part of the sample.
   *
   * @param left  The selected left cell to compute with.
   * @param right The selected right cell to compute with.
   * @param rem   The remaining coordinates.
   * @param ext   The user define the value.
   *
   * @note The return value is a `Collection` to allow, for example, upper or lower triangular matrices to be returned
   *       (this can be done by comparing the selected coordinates)
   */
  def computeWithValue(left: Cell[S], right: Cell[S], rem: R, ext: V): Collection[Cell[Q]]

  /**
   * Operator for pairwise operations and then renaming dimensions.
   *
   * @param rename The rename to apply after the operation.
   *
   * @return An operator that runs `this` and then renames the resulting dimension(s).
   */
  def andThenRenameWithValue(rename: (Cell[S], Cell[S], R, V, Cell[Q]) => Q) = {
    new OperatorWithValue[S, R, Q] {
      type V = self.V

      def computeWithValue(left: Cell[S], right: Cell[S], rem: R, ext: V): Collection[Cell[Q]] = {
        Collection(self.computeWithValue(left, right, rem, ext).toList.map {
          case c => Cell(rename(left, right, rem, ext, c), c.content)
        })
      }
    }
  }

  /**
   * Operator for pairwise operations and then expanding dimensions.
   *
   * @param expand The expansion to apply after the operation.
   *
   * @return An operator that runs `this` and then expands the resulting dimensions.
   */
  def andThenExpandWithValue[U <: Position](expand: (Cell[S], Cell[S], R, V, Cell[Q]) => U)(
    implicit ev: PosExpDep[Q, U]) = {
    new OperatorWithValue[S, R, U] {
      type V = self.V

      def computeWithValue(left: Cell[S], right: Cell[S], rem: R, ext: V): Collection[Cell[U]] = {
        Collection(self.computeWithValue(left, right, rem, ext).toList.map {
          case c => Cell(expand(left, right, rem, ext, c), c.content)
        })
      }
    }
  }
}

/** Type class for transforming a type `T` to a `Operator[S, R, Q]`. */
trait Operable[T, S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position] {
  /**
   * Returns a `Operator[S, R, Q]` for type `T`.
   *
   * @param t Object that can be converted to a `Operator[S, R, Q]`.
   */
  def convert(t: T): Operator[S, R, Q]
}

/** Companion object for the `Operable` type class. */
object Operable {
  /** Converts a `(Cell[S], Cell[S], R) => Cell[R#M]` to a `Operator[S, R, R#M]`. */
  implicit def CSRRM2O[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition]: Operable[(Cell[S], Cell[S], R) => Cell[R#M], S, R, R#M] = C2O[S, R, R#M]

  /** Converts a `(Cell[S], Cell[S], R) => Cell[Q]` to a `Operator[S, R, Q]`. */
  implicit def CSRQ2O[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position](implicit ev: PosExpDep[R, Q]): Operable[(Cell[S], Cell[S], R) => Cell[Q], S, R, Q] = C2O[S, R, Q]

  implicit def C2O[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position]: Operable[(Cell[S], Cell[S], R) => Cell[Q], S, R, Q] = {
    new Operable[(Cell[S], Cell[S], R) => Cell[Q], S, R, Q] {
      def convert(t: (Cell[S], Cell[S], R) => Cell[Q]): Operator[S, R, Q] = {
        new Operator[S, R, Q] {
          def compute(left: Cell[S], right: Cell[S], rem: R): Collection[Cell[Q]] = Collection(t(left, right, rem))
        }
      }
    }
  }

  /** Converts a `(Cell[S], Cell[S], R) => List[Cell[R#M]]` to a `Operator[S, R, R#M]`. */
  implicit def LCSRRM2O[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition]: Operable[(Cell[S], Cell[S], R) => List[Cell[R#M]], S, R, R#M] = LC2O[S, R, R#M]

  /** Converts a `(Cell[S], Cell[S], R) => List[Cell[Q]]` to a `Operator[S, R, Q]`. */
  implicit def LCSRQ2O[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position](implicit ev: PosExpDep[R, Q]): Operable[(Cell[S], Cell[S], R) => List[Cell[Q]], S, R, Q] = LC2O[S, R, Q]

  private def LC2O[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position]: Operable[(Cell[S], Cell[S], R) => List[Cell[Q]], S, R, Q] = {
    new Operable[(Cell[S], Cell[S], R) => List[Cell[Q]], S, R, Q] {
      def convert(t: (Cell[S], Cell[S], R) => List[Cell[Q]]): Operator[S, R, Q] = {
        new Operator[S, R, Q] {
          def compute(left: Cell[S], right: Cell[S], rem: R): Collection[Cell[Q]] = Collection(t(left, right, rem))
        }
      }
    }
  }

  /** Converts a `(Cell[S], Cell[S], R) => Collection[Cell[R#M]]` to a `Operator[S, R, R#M]`. */
  implicit def CCSRRM2O[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition]: Operable[(Cell[S], Cell[S], R) => Collection[Cell[R#M]], S, R, R#M] = CC2O[S, R, R#M]

  /** Converts a `(Cell[S], Cell[S], R) => Collection[Cell[Q]]` to a `Operator[S, R, Q]`. */
  implicit def CCSRQ2O[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position](implicit ev: PosExpDep[R, Q]): Operable[(Cell[S], Cell[S], R) => Collection[Cell[Q]], S, R, Q] = CC2O[S, R, Q]

  private def CC2O[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position]: Operable[(Cell[S], Cell[S], R) => Collection[Cell[Q]], S, R, Q] = {
    new Operable[(Cell[S], Cell[S], R) => Collection[Cell[Q]], S, R, Q] {
      def convert(t: (Cell[S], Cell[S], R) => Collection[Cell[Q]]): Operator[S, R, Q] = {
        new Operator[S, R, Q] {
          def compute(left: Cell[S], right: Cell[S], rem: R): Collection[Cell[Q]] = t(left, right, rem)
        }
      }
    }
  }

  /** Converts a `Operator[S, R, R#M]` to a `Operator[S, R, R#M]`; that is, it is a pass through. */
  implicit def OSRRM2O[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, T <: Operator[S, R, R#M]]: Operable[T, S, R, R#M] = O2O[S, R, R#M, T]

  /** Converts a `Operator[S, R, Q]` to a `Operator[S, R, Q]`; that is, it is a pass through. */
  implicit def OSRQ2O[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position, T <: Operator[S, R, Q]](implicit ev: PosExpDep[R, Q]): Operable[T, S, R, Q] = O2O[S, R, Q, T]

  private def O2O[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position, T <: Operator[S, R, Q]]: Operable[T, S, R, Q] = {
    new Operable[T, S, R, Q] { def convert(t: T): Operator[S, R, Q] = t }
  }

  /** Converts a `List[Operator[S, R, R#M]]` to a single `Operator[S, R, R#M]`. */
  implicit def LOSRRM2O[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, T <: Operator[S, R, R#M]]: Operable[List[T], S, R, R#M] = LO2O[S, R, R#M, T]

  /** Converts a `List[Operator[S, R, Q]]` to a single `Operator[S, R, Q]`. */
  implicit def LOSRQ2O[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position, T <: Operator[S, R, Q]](implicit ev: PosExpDep[R, Q]): Operable[List[T], S, R, Q] = LO2O[S, R, Q, T]

  private def LO2O[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position, T <: Operator[S, R, Q]]: Operable[List[T], S, R, Q] = {
    new Operable[List[T], S, R, Q] {
      def convert(t: List[T]): Operator[S, R, Q] = {
        new Operator[S, R, Q] {
          def compute(left: Cell[S], right: Cell[S], rem: R): Collection[Cell[Q]] = {
            Collection(t.flatMap { case s => s.compute(left, right, rem).toList })
          }
        }
      }
    }
  }
}

/** Type class for transforming a type `T` to a `OperatorWithValue`. */
trait OperableWithValue[T, S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position, W] {
  /**
   * Returns a `OperatorWithValue[S, R, Q]` for type `T`.
   *
   * @param t Object that can be converted to a `OperatorWithValue[S, R, Q]`.
   */
  def convert(t: T): OperatorWithValue[S, R, Q] { type V >: W }
}

/** Companion object for the `OperableWithValue` type class. */
object OperableWithValue {
  /** Converts a `(Cell[S], Cell[S], R, W) => Cell[R#M]` to a `OperatorWithValue[S, R, R#M] { type V >: W }`. */
  implicit def CSRRMW2OWV[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, W]: OperableWithValue[(Cell[S], Cell[S], R, W) => Cell[R#M], S, R, R#M, W] = C2OWV[S, R, R#M, W]

  /** Converts a `(Cell[S], Cell[S], R, W) => Cell[Q]` to a `OperatorWithValue[S, R, Q] { type V >: W }`. */
  implicit def CSRQW2OWV[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position, W](implicit ev: PosExpDep[R, Q]): OperableWithValue[(Cell[S], Cell[S], R, W) => Cell[Q], S, R, Q, W] = C2OWV[S, R, Q, W]

  private def C2OWV[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position, W]: OperableWithValue[(Cell[S], Cell[S], R, W) => Cell[Q], S, R, Q, W] = {
    new OperableWithValue[(Cell[S], Cell[S], R, W) => Cell[Q], S, R, Q, W] {
      def convert(t: (Cell[S], Cell[S], R, W) => Cell[Q]): OperatorWithValue[S, R, Q] { type V >: W } = {
        new OperatorWithValue[S, R, Q] {
          type V = W

          def computeWithValue(left: Cell[S], right: Cell[S], rem: R, ext: W): Collection[Cell[Q]] = {
            Collection(t(left, right, rem, ext))
          }
        }
      }
    }
  }

  /** Converts a `(Cell[S], Cell[S], R, W) => List[Cell[R#M]]` to a `OperatorWithValue[S, R, R#M] { type V >: W }`. */
  implicit def LCSRRMW2OWV[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, W]: OperableWithValue[(Cell[S], Cell[S], R, W) => List[Cell[R#M]], S, R, R#M, W] = LC2OWV[S, R, R#M, W]

  /** Converts a `(Cell[S], Cell[S], R, W) => List[Cell[Q]]` to a `OperatorWithValue[S, R, Q] { type V >: W }`. */
  implicit def LCSRQW2OWV[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position, W](implicit ev: PosExpDep[R, Q]): OperableWithValue[(Cell[S], Cell[S], R, W) => List[Cell[Q]], S, R, Q, W] = LC2OWV[S, R, Q, W]

  private def LC2OWV[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position, W]: OperableWithValue[(Cell[S], Cell[S], R, W) => List[Cell[Q]], S, R, Q, W] = {
    new OperableWithValue[(Cell[S], Cell[S], R, W) => List[Cell[Q]], S, R, Q, W] {
      def convert(t: (Cell[S], Cell[S], R, W) => List[Cell[Q]]): OperatorWithValue[S, R, Q] { type V >: W } = {
        new OperatorWithValue[S, R, Q] {
          type V = W

          def computeWithValue(left: Cell[S], right: Cell[S], rem: R, ext: W): Collection[Cell[Q]] = {
            Collection(t(left, right, rem, ext))
          }
        }
      }
    }
  }

  /**
   * Converts a `(Cell[S], Cell[S], R, W) => Collection[Cell[R#M]]` to a
   * `OperatorWithValue[S, R, R#M] { type V >: W }`.
   */
  implicit def CCSRRMW2OWV[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, W]: OperableWithValue[(Cell[S], Cell[S], R, W) => Collection[Cell[R#M]], S, R, R#M, W] = CC2OWV[S, R, R#M, W]

  /** Converts a `(Cell[S], Cell[S], R, W) => Collection[Cell[Q]]` to a `OperatorWithValue[S, R, Q] { type V >: W }`. */
  implicit def CCSRQW2OWV[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position, W](implicit ev: PosExpDep[R, Q]): OperableWithValue[(Cell[S], Cell[S], R, W) => Collection[Cell[Q]], S, R, Q, W] = CC2OWV[S, R, Q, W]

  private def CC2OWV[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position, W]: OperableWithValue[(Cell[S], Cell[S], R, W) => Collection[Cell[Q]], S, R, Q, W] = {
    new OperableWithValue[(Cell[S], Cell[S], R, W) => Collection[Cell[Q]], S, R, Q, W] {
      def convert(t: (Cell[S], Cell[S], R, W) => Collection[Cell[Q]]): OperatorWithValue[S, R, Q] { type V >: W } = {
        new OperatorWithValue[S, R, Q] {
          type V = W

          def computeWithValue(left: Cell[S], right: Cell[S], rem: R, ext: W): Collection[Cell[Q]] = {
            t(left, right, rem, ext)
          }
        }
      }
    }
  }

  /**
   * Converts a `OperatorWithValue[S, R, R#M] { type V >: W }` to a `OperatorWithValue[S, R, R#M] { type V >: W }`;
   * that is, it is a pass through.
   */
  implicit def OSRRMW2OWV[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, T <: OperatorWithValue[S, R, R#M] { type V >: W }, W]: OperableWithValue[T, S, R, R#M, W] = O2OWV[S, R, R#M, T, W]

  /**
   * Converts a `OperatorWithValue[S, R, Q] { type V >: W }` to a `OperatorWithValue[S, R, Q] { type V >: W }`;
   * that is, it is a pass through.
   */
  implicit def OSRQW2OWV[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position, T <: OperatorWithValue[S, R, Q] { type V >: W }, W](implicit ev: PosExpDep[R, Q]): OperableWithValue[T, S, R, Q, W] = O2OWV[S, R, Q, T, W]

  private def O2OWV[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position, T <: OperatorWithValue[S, R, Q] { type V >: W }, W]: OperableWithValue[T, S, R, Q, W] = {
    new OperableWithValue[T, S, R, Q, W] { def convert(t: T): OperatorWithValue[S, R, Q] { type V >: W } = t }
  }

  /**
   * Converts a `List[OperatorWithValue[S, R, R#M] { type V >: W }]` to a single
   * `OperatorWithValue[S, R, R#M] { type V >: W }`.
   */
  implicit def LOSRRMW2OWV[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, T <: OperatorWithValue[S, R, R#M] { type V >: W }, W]: OperableWithValue[List[T], S, R, R#M, W] = LO2OWV[S, R, R#M, T, W]

  /**
   * Converts a `List[OperatorWithValue[S, R, Q] { type V >: W }]` to a single
   * `OperatorWithValue[S, R, Q] { type V >: W }`.
   */
  implicit def LOSRQW2OWV[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position, T <: OperatorWithValue[S, R, Q] { type V >: W }, W](implicit ev: PosExpDep[R, Q]): OperableWithValue[List[T], S, R, Q, W] = LO2OWV[S, R, Q, T, W]

  private def LO2OWV[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, Q <: Position, T <: OperatorWithValue[S, R, Q] { type V >: W }, W]: OperableWithValue[List[T], S, R, Q, W] = {
    new OperableWithValue[List[T], S, R, Q, W] {
      def convert(t: List[T]): OperatorWithValue[S, R, Q] { type V >: W } = {
        new OperatorWithValue[S, R, Q] {
          type V = W

          def computeWithValue(left: Cell[S], right: Cell[S], rem: R, ext: V): Collection[Cell[Q]] = {
            Collection(t.flatMap { case s => s.computeWithValue(left, right, rem, ext).toList })
          }
        }
      }
    }
  }
}

