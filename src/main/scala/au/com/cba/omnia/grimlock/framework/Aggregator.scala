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

package au.com.cba.omnia.grimlock.framework.aggregate

import au.com.cba.omnia.grimlock.framework._
import au.com.cba.omnia.grimlock.framework.content._
import au.com.cba.omnia.grimlock.framework.encoding._
import au.com.cba.omnia.grimlock.framework.position._
import au.com.cba.omnia.grimlock.framework.utility._

/** Base trait for aggregations. */
trait Aggregator[P <: Position, S <: Position with ExpandablePosition, Q <: Position]
  extends AggregatorWithValue[P, S, Q] { self =>
  type V = Any

  def prepareWithValue(cell: Cell[P], ext: V): T = prepare(cell)
  def presentWithValue(pos: S, t: T, ext: V): Option[Cell[Q]] = present(pos, t)

  /**
   * Prepare for reduction.
   *
   * @param cell Cell which is to be aggregated. Note that its position is prior to `slice.selected` being applied.
   *
   * @return State to reduce.
   */
  def prepare(cell: Cell[P]): T

  /**
   * Present the reduced content.
   *
   * @param pos The reduced position. That is, the position returned by `Slice.selected`.
   * @param t   The reduced state.
   *
   * @return Optional cell where the position is `pos` and the content is derived from `t`.
   *
   * @note An `Option` is used in the return type to allow aggregators to be selective in what content they apply to.
   *       For example, computing the mean is undefined for categorical variables. The aggregator now has the option to
   *       return `None`. This in turn permits an external API, for simple cases, where the user need not know about
   *       the types of variables of their data.
   */
  def present(pos: S, t: T): Option[Cell[Q]]

  /**
   * Operator for aggregating and then renaming dimensions.
   *
   * @param rename The rename to apply after the aggregation.
   *
   * @return An aggregator that runs `this` and then renames the resulting dimension(s).
   */
  def andThenRename(rename: (Cell[Q]) => Q) = {
    new Aggregator[P, S, Q] {
      type T = self.T

      def prepare(cell: Cell[P]): T = self.prepare(cell)
      def reduce(lt: T, rt: T): T = self.reduce(lt, rt)
      def present(pos: S, t: T): Option[Cell[Q]] = self.present(pos, t).map { case c => Cell(rename(c), c.content) }
    }
  }

  /**
   * Operator for aggregating and then expanding dimensions.
   *
   * @param expand The expansion to apply after the aggregation.
   *
   * @return An aggregator that runs `this` and then expands the resulting dimensions.
   */
  def andThenExpand[R <: Position](expand: (Cell[Q]) => R)(implicit ev: PosExpDep[Q, R]) = {
    new Aggregator[P, S, R] {
      type T = self.T

      def prepare(cell: Cell[P]): T = self.prepare(cell)
      def reduce(lt: T, rt: T): T = self.reduce(lt, rt)
      def present(pos: S, t: T): Option[Cell[R]] = self.present(pos, t).map { case c => Cell(expand(c), c.content) }
    }
  }
}

/** Base trait for aggregations with a user supplied value. */
trait AggregatorWithValue[P <: Position, S <: Position with ExpandablePosition, Q <: Position]
  extends java.io.Serializable { self =>
  /** Type of the state being aggregated. */
  type T

  /** Type of the external value. */
  type V

  /**
   * Prepare for reduction.
   *
   * @param cell Cell which is to be aggregated. Note that its position is prior to `slice.selected` being applied.
   * @param ext  User provided data required for preparation.
   *
   * @return State to reduce.
   */
  def prepareWithValue(cell: Cell[P], ext: V): T

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
  def presentWithValue(pos: S, t: T, ext: V): Option[Cell[Q]]

  /**
   * Operator for aggregating and then renaming dimensions.
   *
   * @param rename The rename to apply after the aggregation.
   *
   * @return An aggregator that runs `this` and then renames the resulting dimension(s).
   */
  def andThenRenameWithValue(rename: (Cell[Q], V) => Q) = {
    new AggregatorWithValue[P, S, Q] {
      type T = self.T
      type V = self.V

      def prepareWithValue(cell: Cell[P], ext: V): T = self.prepareWithValue(cell, ext)
      def reduce(lt: T, rt: T): T = self.reduce(lt, rt)
      def presentWithValue(pos: S, t: T, ext: V): Option[Cell[Q]] = {
        self.presentWithValue(pos, t, ext).map { case c => Cell(rename(c, ext), c.content) }
      }
    }
  }

  /**
   * Operator for aggregating and then expanding dimensions.
   *
   * @param expand The expansion to apply after the aggregation.
   *
   * @return An aggregator that runs `this` and then expands the resulting dimensions.
   */
  def andThenExpandWithValue[R <: Position](expand: (Cell[Q], V) => R)(implicit ev: PosExpDep[Q, R]) = {
    new AggregatorWithValue[P, S, R] {
      type T = self.T
      type V = self.V

      def prepareWithValue(cell: Cell[P], ext: V): T = self.prepareWithValue(cell, ext)
      def reduce(lt: T, rt: T): T = self.reduce(lt, rt)
      def presentWithValue(pos: S, t: T, ext: V): Option[Cell[R]] = {
        self.presentWithValue(pos, t, ext).map { case c => Cell(expand(c, ext), c.content) }
      }
    }
  }
}

/** Type class for transforming a type `T` to a `List[Aggregator[P, S, Q]]`. */
trait Aggregatable[T, P <: Position, S <: Position with ExpandablePosition, Q <: Position] {
  /**
   * Returns a `List[Aggregator[P, S, Q]]` for type `T`.
   *
   * @param t Object that can be converted to a `List[Aggregator[P, S, Q]]`.
   */
  def convert(t: T): List[Aggregator[P, S, Q]]
}

/** Companion object for the `Aggregatable` type class. */
object Aggregatable {
  /** Converts an `Aggregator[P, S, S]` to a `List[Aggregator[P, S, S]]`. */
  implicit def APSS2A[P <: Position, S <: Position with ExpandablePosition, T <: Aggregator[P, S, S]]: Aggregatable[T, P, S, S] = A2A[P, S, S, T]

  /** Converts an `Aggregator[P, S, S#M]` to a `List[Aggregator[P, S, S#M]]`. */
  implicit def APSSM2A[P <: Position, S <: Position with ExpandablePosition, T <: Aggregator[P, S, S#M]]: Aggregatable[T, P, S, S#M] = A2A[P, S, S#M, T]

  /** Converts an `Aggregator[P, S, Q]` to a `List[Aggregator[P, S, Q]]`. */
  implicit def APSQ2A[P <: Position, S <: Position with ExpandablePosition, Q <: Position, T <: Aggregator[P, S, Q]](implicit ev: PosExpDep[S, Q]): Aggregatable[T, P, S, Q] = A2A[P, S, Q, T]

  private def A2A[P <: Position, S <: Position with ExpandablePosition, Q <: Position, T <: Aggregator[P, S, Q]]: Aggregatable[T, P, S, Q] = {
    new Aggregatable[T, P, S, Q] { def convert(t: T): List[Aggregator[P, S, Q]] = List(t) }
  }

  /** Converts a `List[Aggregator[P, S, S#M]]` to a `List[Aggregator[P, S, S#M]]`; that is, it's a pass through. */
  implicit def LAPSSM2A[P <: Position, S <: Position with ExpandablePosition, T <: Aggregator[P, S, S#M]]: Aggregatable[List[T], P, S, S#M] = LA2A[P, S, S#M, T]

  /** Converts a `List[Aggregator[P, S, Q]]` to a `List[Aggregator[P, S, Q]]`; that is, it's a pass through. */
  implicit def LAPSQ2A[P <: Position, S <: Position with ExpandablePosition, Q <: Position, T <: Aggregator[P, S, Q]](
      implicit ev: PosExpDep[S, Q]): Aggregatable[List[T], P, S, Q] = LA2A[P, S, Q, T]

  private def LA2A[P <: Position, S <: Position with ExpandablePosition, Q <: Position, T <: Aggregator[P, S, Q]]: Aggregatable[List[T], P, S, Q] = {
    new Aggregatable[List[T], P, S, Q] { def convert(t: List[T]): List[Aggregator[P, S, Q]] = t }
  }
}

/** Type class for transforming a type `T` to a 'List[AggregatorWithValue[P S, Q] { type V >: W }]`. */
trait AggregatableWithValue[T, P <: Position, S <: Position with ExpandablePosition, Q <: Position, W] {
  /**
   * Returns a `List[AggregatorWithValue[P, S, Q] { type V >: W }]` for type `T`.
   *
   * @param t Object that can be converted to a `List[AggregatorWithValue[P, S, Q] { type V >: W }]`.
   */
  def convert(t: T): List[AggregatorWithValue[P, S, Q] { type V >: W }]
}

/** Companion object for the `AggregatableWithValue` type class. */
object AggregatableWithValue {
  /**
   * Converts an `AggregatorWithValue[P, S, S] { type V >: W }` to a
   * `List[AggregatorWithValue[P, S, S] { type V >: W }]`.
   */
  implicit def APSSW2AWV[P <: Position, S <: Position with ExpandablePosition, T <: AggregatorWithValue[P, S, S] { type V >: W }, W]: AggregatableWithValue[T, P, S, S, W] = A2AWV[P, S, S, T, W]

  /**
   * Converts an `AggregatorWithValue[P, S, S#M] { type V >: W }` to a
   * `List[AggregatorWithValue[P, S, S#M] { type V >: W }]`.
   */
  implicit def APSSMW2AWV[P <: Position, S <: Position with ExpandablePosition, T <: AggregatorWithValue[P, S, S#M] { type V >: W }, W]: AggregatableWithValue[T, P, S, S#M, W] = A2AWV[P, S, S#M, T, W]

  /**
   * Converts an `AggregatorWithValue[P, S, Q] { type V >: W }` to a
   * `List[AggregatorWithValue[P, S, Q] { type V >: W }]`.
   */
  implicit def APSQW2AWV[P <: Position, S <: Position with ExpandablePosition, Q <: Position, T <: AggregatorWithValue[P, S, Q] { type V >: W }, W](implicit ev: PosExpDep[S, Q]): AggregatableWithValue[T, P, S, Q, W] = A2AWV[P, S, Q, T, W]

  private def A2AWV[P <: Position, S <: Position with ExpandablePosition, Q <: Position, T <: AggregatorWithValue[P, S, Q] { type V >: W }, W]: AggregatableWithValue[T, P, S, Q, W] = {
    new AggregatableWithValue[T, P, S, Q, W] {
      def convert(t: T): List[AggregatorWithValue[P, S, Q] { type V >: W }] = List(t)
    }
  }

  /**
   * Converts a `List[AggregatorWithValue[P, S, S#M] { type V >: W }]` to a
   * `List[AggregatorWithValue[P, S, S#M] { type V >: W }]`; that is, it is a pass through.
   */
  implicit def LAPSSMW2AWV[P <: Position, S <: Position with ExpandablePosition, T <: AggregatorWithValue[P, S, S#M] { type V >: W }, W]: AggregatableWithValue[List[T], P, S, S#M, W] = LA2AWV[P, S, S#M, T, W]

  /**
   * Converts a `List[AggregatorWithValue[P, S, Q] { type V >: W }]` to a 
   * `List[AggregatorWithValue[P, S, Q] { type V >: W }]`; that is, it is a pass through.
   */
  implicit def LAPSQW2AWV[P <: Position, S <: Position with ExpandablePosition, Q <: Position, T <: AggregatorWithValue[P, S, Q] { type V >: W }, W](implicit ev: PosExpDep[S, Q]): AggregatableWithValue[List[T], P, S, Q, W] = LA2AWV[P, S, Q, T, W]

  private def LA2AWV[P <: Position, S <: Position with ExpandablePosition, Q <: Position, T <: AggregatorWithValue[P, S, Q] { type V >: W }, W]: AggregatableWithValue[List[T], P, S, Q, W] = {
    new AggregatableWithValue[List[T], P, S, Q, W] {
      def convert(t: List[T]): List[AggregatorWithValue[P, S, Q] { type V >: W }] = t
    }
  }
}

