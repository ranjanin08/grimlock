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

package au.com.cba.omnia.grimlock.framework.aggregate

import au.com.cba.omnia.grimlock.framework._
import au.com.cba.omnia.grimlock.framework.content._
import au.com.cba.omnia.grimlock.framework.position._

/** Base trait that encapsulates the result of an aggregation. */
trait Result[A, T[X] <: Result[X, T]] {
  /** Map over result. */
  def map[B](f: (A) => B): T[B]

  /** FlatMap over result. */
  def flatMap[B](f: (A) => Option[B]): T[B]

  /** Return result as a `TraversableOnce`. */
  def toTraversableOnce: TraversableOnce[A]
}

/** Companion object to `Result`. */
object Result {
  /**
   * Implicit conversion from `Result` to `TraversableOnce`.
   *
   * @param result The `Result` to convert.
   *
   * @return A `TraversableOnce` for this result.
   */
  implicit def result2TraversableOnce[T, R[X] <: Result[X, R]](result: R[T]): TraversableOnce[T] = {
    result.toTraversableOnce
  }
}

/**
 * Aggregation result that consists of at most 1 value.
 *
 * @param result The result of the aggregation.
 */
case class Single[A](result: Option[A]) extends Result[A, Single] {
  def map[B](f: (A) => B): Single[B] = Single(result.map(f(_)))
  def flatMap[B](f: (A) => Option[B]): Single[B] = Single(result.flatMap(f(_)))
  def toTraversableOnce: TraversableOnce[A] = result
}

/** Companion object to `Single`. */
object Single {
  /** Create an empty result. */
  def apply[A](): Single[A] = Single[A](None)

  /**
   * Aggregation result that consists of 1 value.
   *
   * @param result The result of the aggregation.
   */
  def apply[A](result: A): Single[A] = Single[A](Some(result))
}

/**
 * Aggregation result that consists of arbitrary many values.
 *
 * @param result The results of the aggregation.
 */
case class Multiple[A](result: TraversableOnce[A]) extends Result[A, Multiple] {
  def map[B](f: (A) => B): Multiple[B] = Multiple(result.map(f(_)))
  def flatMap[B](f: (A) => Option[B]): Multiple[B] = Multiple(result.flatMap(f(_)))
  def toTraversableOnce: TraversableOnce[A] = result
}

/** Companion object to `Multiple`. */
object Multiple {
  /** Create an empty result. */
  def apply[A](): Multiple[A] = Multiple[A](List())
}

/** Base trait for aggregations. */
trait Aggregator[P <: Position[P], S <: Position[S] with ExpandablePosition[S, _], Q <: Position[Q]]
  extends AggregatorWithValue[P, S, Q] { self =>
  type V = Any

  def prepareWithValue(cell: Cell[P], ext: V): Option[T] = prepare(cell)
  def presentWithValue(pos: S, t: T, ext: V): O[Cell[Q]] = present(pos, t)

  /**
   * Prepare for reduction.
   *
   * @param cell Cell which is to be aggregated. Note that its position is prior to `slice.selected` being applied.
   *
   * @return Optional state to reduce (allow for filtering).
   */
  def prepare(cell: Cell[P]): Option[T]

  /**
   * Present the reduced content.
   *
   * @param pos The reduced position. That is, the position returned by `slice.selected`.
   * @param t   The reduced state.
   *
   * @return Optional cell where the position is `pos` and the content is derived from `t`.
   *
   * @note An `Option` is used in the return type to allow aggregators to be selective in what content they apply to.
   *       For example, computing the mean is undefined for categorical variables. The aggregator now has the option to
   *       return `None`. This in turn permits an external API, for simple cases, where the user need not know about
   *       the types of variables of their data.
   */
  def present(pos: S, t: T): O[Cell[Q]]

  /**
   * Operator for preparing content prior to aggregating.
   *
   * @param prep The function to apply prior to aggregation.
   *
   * @return An aggregator that prepares the content and then runs `this`.
   */
  override def withPrepare(prep: (Cell[P]) => Content) = {
    new Aggregator[P, S, Q] {
      type T = self.T
      type O[A] = self.O[A]

      def prepare(cell: Cell[P]): Option[T] = self.prepare(Cell(cell.position, prep(cell)))
      def reduce(lt: T, rt: T): T = self.reduce(lt, rt)
      def present(pos: S, t: T): O[Cell[Q]] = self.present(pos, t)
    }
  }

  /**
   * Operator for aggregating and then updating the contents.
   *
   * @param mutate The mutation to apply after the aggregation.
   *
   * @return An aggregator that runs `this` and then updates the resulting contents.
   */
  override def andThenMutate(mutate: (Cell[Q]) => Content) = {
    new Aggregator[P, S, Q] {
      type T = self.T
      type O[A] = self.O[A]

      def prepare(cell: Cell[P]): Option[T] = self.prepare(cell)
      def reduce(lt: T, rt: T): T = self.reduce(lt, rt)
      def present(pos: S, t: T): O[Cell[Q]] = self.present(pos, t).map { case c => Cell(c.position, mutate(c)) }
    }
  }

  /**
   * Operator for aggregating and then relocating the contents.
   *
   * @param locate The relocation to apply after the aggregation.
   *
   * @return An aggregator that runs `this` and then relocates the resulting contents.
   */
  override def andThenRelocate[R <: Position[R]](locate: Locate.FromCell[Q, R])(implicit ev: PosIncDep[Q, R]) = {
    new Aggregator[P, S, R] {
      type T = self.T
      type O[A] = self.O[A]

      def prepare(cell: Cell[P]): Option[T] = self.prepare(cell)
      def reduce(lt: T, rt: T): T = self.reduce(lt, rt)
      def present(pos: S, t: T): O[Cell[R]] = {
        self.present(pos, t).flatMap { case c => locate(c).map(Cell(_, c.content)) }
      }
    }
  }
}

/** Base trait for aggregations with a user supplied value. */
trait AggregatorWithValue[P <: Position[P], S <: Position[S] with ExpandablePosition[S, _], Q <: Position[Q]]
  extends java.io.Serializable { self =>
  /** Type of the state being aggregated. */
  type T

  /** Type of the external value. */
  type V

  /** Return type of presented data. */
  type O[A] <: Result[A, O]

  /**
   * Prepare for reduction.
   *
   * @param cell Cell which is to be aggregated. Note that its position is prior to `slice.selected` being applied.
   * @param ext  User provided data required for preparation.
   *
   * @return Optional state to reduce (allow for filtering).
   */
  def prepareWithValue(cell: Cell[P], ext: V): Option[T]

  /**
   * Standard reduce method.
   *
   * @param lt Left state to reduce.
   * @param rt Right state to reduce.
   *
   * @return Reduced state
   */
  def reduce(lt: T, rt: T): T

  /**
   * Present the reduced content.
   *
   * @param pos The reduced position. That is, the position returned by `Slice.selected`.
   * @param t   The reduced state.
   * @param ext User provided data required for presentation.
   *
   * @return Optional cell where the position is `pos` and the content is derived from `t`.
   *
   * @note An `Option` is used in the return type to allow aggregators to be selective in what content they apply to.
   *       For example, computing the mean is undefined for categorical variables. The aggregator now has the option to
   *       return `None`. This in turn permits an external API, for simple cases, where the user need not know about
   *       the types of variables of their data.
   */
  def presentWithValue(pos: S, t: T, ext: V): O[Cell[Q]]

  /**
   * Operator for preparing content prior to aggregating.
   *
   * @param prep The function to apply prior to aggregation.
   *
   * @return An aggregator that prepares the content and then runs `this`.
   */
  def withPrepare(prep: (Cell[P]) => Content) = {
    new AggregatorWithValue[P, S, Q] {
      type T = self.T
      type V = self.V
      type O[A] = self.O[A]

      def prepareWithValue(cell: Cell[P], ext: V): Option[T] = {
        self.prepareWithValue(Cell(cell.position, prep(cell)), ext)
      }
      def reduce(lt: T, rt: T): T = self.reduce(lt, rt)
      def presentWithValue(pos: S, t: T, ext: V): O[Cell[Q]] = self.presentWithValue(pos, t, ext)
    }
  }

  /**
   * Operator for aggregating and then updating the contents.
   *
   * @param mutate The mutation to apply after the aggregation.
   *
   * @return An aggregator that runs `this` and then updates the resulting contents.
   */
  def andThenMutate(mutate: (Cell[Q]) => Content) = {
    new AggregatorWithValue[P, S, Q] {
      type T = self.T
      type V = self.V
      type O[A] = self.O[A]

      def prepareWithValue(cell: Cell[P], ext: V): Option[T] = self.prepareWithValue(cell, ext)
      def reduce(lt: T, rt: T): T = self.reduce(lt, rt)
      def presentWithValue(pos: S, t: T, ext: V): O[Cell[Q]] = {
        self.presentWithValue(pos, t, ext).map { case c => Cell(c.position, mutate(c)) }
      }
    }
  }

  /**
   * Operator for aggregating and then relocating the contents.
   *
   * @param locate The relocation to apply after the aggregation.
   *
   * @return An aggregator that runs `this` and then relocates the resulting contents.
   */
  def andThenRelocate[R <: Position[R]](locate: Locate.FromCell[Q, R])(implicit ev: PosIncDep[Q, R]) = {
    new AggregatorWithValue[P, S, R] {
      type T = self.T
      type V = self.V
      type O[A] = self.O[A]

      def prepareWithValue(cell: Cell[P], ext: V): Option[T] = self.prepareWithValue(cell, ext)
      def reduce(lt: T, rt: T): T = self.reduce(lt, rt)
      def presentWithValue(pos: S, t: T, ext: V): O[Cell[R]] = {
        self.presentWithValue(pos, t, ext).flatMap { case c => locate(c).map(Cell(_, c.content)) }
      }
    }
  }

  /**
   * Operator for preparing content prior to aggregating.
   *
   * @param prep The function to apply prior to aggregation.
   *
   * @return An aggregator that prepares the content and then runs `this`.
   */
  def withPrepareWithValue(prep: (Cell[P], V) => Content) = {
    new AggregatorWithValue[P, S, Q] {
      type T = self.T
      type V = self.V
      type O[A] = self.O[A]

      def prepareWithValue(cell: Cell[P], ext: V): Option[T] = {
        self.prepareWithValue(Cell(cell.position, prep(cell, ext)), ext)
      }
      def reduce(lt: T, rt: T): T = self.reduce(lt, rt)
      def presentWithValue(pos: S, t: T, ext: V): O[Cell[Q]] = self.presentWithValue(pos, t, ext)
    }
  }

  /**
   * Operator for aggregating and then updating the contents.
   *
   * @param mutate The mutation to apply after the aggregation.
   *
   * @return An aggregator that runs `this` and then updates the resulting contents.
   */
  def andThenMutateWithValue(mutate: (Cell[Q], V) => Content) = {
    new AggregatorWithValue[P, S, Q] {
      type T = self.T
      type V = self.V
      type O[A] = self.O[A]

      def prepareWithValue(cell: Cell[P], ext: V): Option[T] = self.prepareWithValue(cell, ext)
      def reduce(lt: T, rt: T): T = self.reduce(lt, rt)
      def presentWithValue(pos: S, t: T, ext: V): O[Cell[Q]] = {
        self.presentWithValue(pos, t, ext).map { case c => Cell(c.position, mutate(c, ext)) }
      }
    }
  }

  /**
   * Operator for aggregating and then relocating the contents.
   *
   * @param locate The relocation to apply after the aggregation.
   *
   * @return An aggregator that runs `this` and then relocates the resulting contents.
   */
  def andThenRelocateWithValue[R <: Position[R]](locate: Locate.FromCellWithValue[Q, R, V])(
    implicit ev: PosIncDep[Q, R]) = {
    new AggregatorWithValue[P, S, R] {
      type T = self.T
      type V = self.V
      type O[A] = self.O[A]

      def prepareWithValue(cell: Cell[P], ext: V): Option[T] = self.prepareWithValue(cell, ext)
      def reduce(lt: T, rt: T): T = self.reduce(lt, rt)
      def presentWithValue(pos: S, t: T, ext: V): O[Cell[R]] = {
        self.presentWithValue(pos, t, ext).flatMap { case c => locate(c, ext).map(Cell(_, c.content)) }
      }
    }
  }
}

/** Trait for transforming a type `T` to a `List[Aggregator[P, S, Q]]`. */
trait Aggregatable[P <: Position[P], S <: Position[S] with ExpandablePosition[S, _], Q <: Position[Q]]
  extends java.io.Serializable {
  /** Returns a `List[Aggregator[P, S, Q]]` for this type `T`. */
  def apply(): List[Aggregator[P, S, Q]]
}

/** Companion object for the `Aggregatable` trait. */
object Aggregatable {
  /** Converts an `Aggregator[P, S, S]` to a `List[Aggregator[P, S, S]]`. */
  implicit def AS2A[P <: Position[P], S <: Position[S] with ExpandablePosition[S, _]](
    t: Aggregator[P, S, S] { type O[A] = Single[A] }): Aggregatable[P, S, S] = {
    new Aggregatable[P, S, S] { def apply(): List[Aggregator[P, S, S]] = List(t) }
  }

  /** Converts an `Aggregator[P, S, Q]` to a `List[Aggregator[P, S, Q]]`. */
  implicit def AQ2A[P <: Position[P], S <: Position[S] with ExpandablePosition[S, _], Q <: Position[Q]](
    t: Aggregator[P, S, Q])(implicit ev: PosExpDep[S, Q]): Aggregatable[P, S, Q] = {
    new Aggregatable[P, S, Q] { def apply(): List[Aggregator[P, S, Q]] = List(t) }
  }

  /** Converts a `List[Aggregator[P, S, Q]]` to a `List[Aggregator[P, S, Q]]`; that is, it's a pass through. */
  implicit def LAQ2A[P <: Position[P], S <: Position[S] with ExpandablePosition[S, _], Q <: Position[Q]](
    t: List[Aggregator[P, S, Q]])(implicit ev: PosExpDep[S, Q]): Aggregatable[P, S, Q] = {
    new Aggregatable[P, S, Q] { def apply(): List[Aggregator[P, S, Q]] = t }
  }
}

/** Trait for transforming a type `T` to a 'List[AggregatorWithValue[P S, Q] { type V >: W }]`. */
trait AggregatableWithValue[P <: Position[P], S <: Position[S] with ExpandablePosition[S, _], Q <: Position[Q], W]
  extends java.io.Serializable {
  /** Returns a `List[AggregatorWithValue[P, S, Q] { type V >: W }]` for this type `T`. */
  def apply(): List[AggregatorWithValue[P, S, Q] { type V >: W }]
}

/** Companion object for the `AggregatableWithValue` trait. */
object AggregatableWithValue {
  /**
   * Converts an `AggregatorWithValue[P, S, S] { type V >: W }` to a
   * `List[AggregatorWithValue[P, S, S] { type V >: W }]`.
   */
  implicit def ASWV2AWV[P <: Position[P], S <: Position[S] with ExpandablePosition[S, _], W](
    t: AggregatorWithValue[P, S, S] { type V >: W; type O[A] = Single[A] }): AggregatableWithValue[P, S, S, W] = {
    new AggregatableWithValue[P, S, S, W] {
      def apply(): List[AggregatorWithValue[P, S, S] { type V >: W }] = List(t)
    }
  }

  /**
   * Converts an `AggregatorWithValue[P, S, Q] { type V >: W }` to a
   * `List[AggregatorWithValue[P, S, Q] { type V >: W }]`.
   */
  implicit def AQWV2AWV[P <: Position[P], S <: Position[S] with ExpandablePosition[S, _], Q <: Position[Q], W](
    t: AggregatorWithValue[P, S, Q] { type V >: W })(
      implicit ev: PosExpDep[S, Q]): AggregatableWithValue[P, S, Q, W] = {
    new AggregatableWithValue[P, S, Q, W] {
      def apply(): List[AggregatorWithValue[P, S, Q] { type V >: W }] = List(t)
    }
  }

  /**
   * Converts a `List[AggregatorWithValue[P, S, Q] { type V >: W }]` to a
   * `List[AggregatorWithValue[P, S, Q] { type V >: W }]`; that is, it is a pass through.
   */
  implicit def LAQWV2AWV[P <: Position[P], S <: Position[S] with ExpandablePosition[S, _], Q <: Position[Q], W](
    t: List[AggregatorWithValue[P, S, Q] { type V >: W }])(
      implicit ev: PosExpDep[S, Q]): AggregatableWithValue[P, S, Q, W] = {
    new AggregatableWithValue[P, S, Q, W] {
      def apply(): List[AggregatorWithValue[P, S, Q] { type V >: W }] = t
    }
  }
}

