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

import scala.reflect.ClassTag

/** Base trait for aggregattion. */
trait Aggregator {
  /** Type of the state being aggregated. */
  type T

  /** Serialisation ClassTag for state `T`. */
  val ct: ClassTag[T]

  /** Type of the external value. */
  type V

  /**
   * Standard reduce method.
   *
   * @param lt Left state to reduce.
   * @param rt Right state to reduce.
   *
   * @return Reduced state
   */
  def reduce(lt: T, rt: T): T
}

/** Base trait for reduction preparation. */
trait Prepare extends PrepareWithValue { self: Aggregator =>
  type V = Any

  def prepareWithValue[P <: Position, D <: Dimension](slice: Slice[P, D], cell: Cell[P], ext: V): T = {
    prepare(slice, cell)
  }

  /**
   * Prepare for reduction.
   *
   * @param slice Encapsulates the dimension(s) over with to aggregate.
   * @param cell  Cell which is to be aggregated. Note that its position is prior to `slice.selected` being applied.
   *
   * @return State to reduce.
   */
  def prepare[P <: Position, D <: Dimension](slice: Slice[P, D], cell: Cell[P]): T
}

/** Base trait for reduction preparation with a user supplied value. */
trait PrepareWithValue { self: Aggregator =>
  /**
   * Prepare for reduction.
   *
   * @param slice Encapsulates the dimension(s) over with to aggregate.
   * @param cell  Cell which is to be aggregated. Note that its position is prior to `slice.selected` being applied.
   * @param ext   User provided data required for preparation.
   *
   * @return State to reduce.
   */
  def prepareWithValue[P <: Position, D <: Dimension](slice: Slice[P, D], cell: Cell[P], ext: V): T
}

/** Base trait for reductions that return a single value. */
trait PresentSingle extends PresentSingleWithValue { self: Aggregator with Prepare =>
  def presentSingleWithValue[P <: Position](pos: P, t: T, ext: V): Option[Cell[P]] = presentSingle(pos, t)

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
  def presentSingle[P <: Position](pos: P, t: T): Option[Cell[P]]
}

/** Base trait for reductions that return a single value using user supplied data. */
trait PresentSingleWithValue { self: Aggregator with PrepareWithValue =>
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
  def presentSingleWithValue[P <: Position](pos: P, t: T, ext: V): Option[Cell[P]]
}

/** Base trait for reductions that return multiple values. */
trait PresentMultiple { self: Aggregator with Prepare =>
  def presentMultipleWithValue[P <: Position with ExpandablePosition](pos: P, t: T, ext: V): Collection[Cell[P#M]] = {
    presentMultiple(pos, t)
  }

  /**
   * Present the reduced content(s).
   *
   * @param pos The reduced position. That is, the position returned by `Slice.selected`.
   * @param t   The reduced state.
   *
   * @return Optional cell tuple where the position is creating by appending to `pos` (`append` method) and the
   *         content(s) is derived from `t`.
   *
   * @note An `Option` is used in the return type to allow aggregators to be selective in what content they apply to.
   *       For example, computing the mean is undefined for categorical variables. The aggregator now has the option to
   *       return `None`. This in turn permits an external API, for simple cases, where the user need not know about
   *       the types of variables of their data.
   */
  def presentMultiple[P <: Position with ExpandablePosition](pos: P, t: T): Collection[Cell[P#M]]
}

/** Base trait for reductions that return multiple values using user supplied data. */
trait PresentMultipleWithValue { self: Aggregator with PrepareWithValue =>
  /**
   * Present the reduced content(s).
   *
   * @param pos The reduced position. That is, the position returned by `Slice.selected`.
   * @param t   The reduced state.
   * @param ext User provided data required for presentation.
   *
   * @return Optional cell tuple where the position is creating by appending to `pos` (`append` method) and the
   *         content(s) is derived from `t`.
   *
   * @note An `Option` is used in the return type to allow aggregators to be selective in what content they apply to.
   *       For example, computing the mean is undefined for categorical variables. The aggregator now has the option to
   *       return `None`. This in turn permits an external API, for simple cases, where the user need not know about
   *       the types of variables of their data.
   */
  def presentMultipleWithValue[P <: Position with ExpandablePosition](pos: P, t: T, ext: V): Collection[Cell[P#M]]
}

/** Convenience trait for aggregators that present a value both as `PresentSingle` and `PresentMultiple`. */
trait PresentSingleAndMultiple extends Prepare with PresentSingle with PresentMultiple { self: Aggregator =>

  def presentSingle[P <: Position](pos: P, t: T): Option[Cell[P]] = content(t).map { case c => Cell(pos, c) }

  def presentMultiple[P <: Position with ExpandablePosition](pos: P, t: T): Collection[Cell[P#M]] = {
    name match {
      case Some(n) => Collection(content(t).map { case con => Left(Cell[P#M](pos.append(n), con)) })
      case None => Collection[Cell[P#M]]()
    }
  }

  /** Name of the coordinate when presenting as multiple. */
  val name: Option[Value]

  protected def content(t: T): Option[Content]
}

/**
 * Convenience trait for aggregators that present a value both as `PresentSingleWithValue` and
 * `PresentMultipleWithValue`.
 *
 * @note This ignores the user provided value.
 */
trait PresentSingleAndMultipleWithValue extends PrepareWithValue with PresentSingleWithValue with PresentMultipleWithValue { self: Aggregator =>

  def presentSingleWithValue[P <: Position](pos: P, t: T, ext: V): Option[Cell[P]] = {
    content(t).map { case c => Cell(pos, c) }
  }

  def presentMultipleWithValue[P <: Position with ExpandablePosition](pos: P, t: T, ext: V): Collection[Cell[P#M]] = {
    name match {
      case Some(n) => Collection(content(t).map { case con => Left(Cell[P#M](pos.append(n), con)) })
      case None => Collection[Cell[P#M]]()
    }
  }

  /** Name of the coordinate when presenting as multiple. */
  val name: Option[Value]

  protected def content(t: T): Option[Content]
}

/**
 * Aggregator that is a combination of one or more aggregators with `PresentMultiple`.
 *
 * @param aggregators `List` of aggregators that are combined together.
 *
 * @note This need not be called in an application. The `AggregatableMultiple` type class will convert any
 *       `List[Aggregator]` automatically to one of these.
 */
case class CombinationAggregatorMultiple[R <: Aggregator with Prepare with PresentMultiple](aggregators: List[R])
  extends Aggregator with Prepare with PresentMultiple {
  type T = List[Any]

  val ct = ClassTag[List[Any]](List.getClass)

  def prepare[P <: Position, D <: Dimension](slice: Slice[P, D], cell: Cell[P]): T = {
    aggregators.map { case aggregator => aggregator.prepare(slice, cell) }
  }

  def reduce(lt: T, rt: T): T = {
    (aggregators, lt, rt).zipped.map {
      case (aggregator, l, r) => aggregator.reduce(l.asInstanceOf[aggregator.T], r.asInstanceOf[aggregator.T])
    }
  }

  def presentMultiple[P <: Position with ExpandablePosition](pos: P, t: T): Collection[Cell[P#M]] = {
    Collection((aggregators, t).zipped.flatMap {
      case (aggregator, s) => aggregator.presentMultiple(pos, s.asInstanceOf[aggregator.T]).toList
    })
  }
}

/**
 * Aggregator that is a combination of one or more aggregators with `PrepareWithValue` with `PresentMultiple`.
 *
 * @param aggregators `List` of aggregators that are combined together.
 *
 * @note This need not be called in an application. The `AggregatableMultipleWithValue` type class will convert any
 *       `List[Aggregator]` automatically to one of these.
 */
case class CombinationAggregatorMultipleWithValue[R <: Aggregator with PrepareWithValue with PresentMultipleWithValue { type V >: W }, W](
  aggregators: List[R]) extends Aggregator with PrepareWithValue with PresentMultipleWithValue {
  type T = List[Any]
  type V = W

  val ct = ClassTag[List[Any]](List.getClass)

  def prepareWithValue[P <: Position, D <: Dimension](slice: Slice[P, D], cell: Cell[P], ext: V): T = {
    aggregators.map { case aggregator => aggregator.prepareWithValue(slice, cell, ext) }
  }

  def reduce(lt: T, rt: T): T = {
    (aggregators, lt, rt).zipped.map {
      case (aggregator, l, r) => aggregator.reduce(l.asInstanceOf[aggregator.T], r.asInstanceOf[aggregator.T])
    }
  }

  def presentMultipleWithValue[P <: Position with ExpandablePosition](pos: P, t: T, ext: V): Collection[Cell[P#M]] = {
    Collection((aggregators, t).zipped.flatMap {
      case (aggregator, s) => aggregator.presentMultipleWithValue(pos, s.asInstanceOf[aggregator.T], ext).toList
    })
  }
}

/** Type class for transforming a type `T` to an aggregator with `PresentMultiple`. */
trait AggregatableMultiple[T] {
  /**
   * Returns an aggregator with `PresentMultiple` for type `T`.
   *
   * @param t Object that can be converted to an aggregator with `PresentMultiple`.
   */
  def convert(t: T): Aggregator with Prepare with PresentMultiple
}

/** Companion object for the `AggregatableMultiple` type class. */
object AggregatableMultiple {
  /**
   * Converts a `List[Aggregator with PresentMultiple]` to a single aggregator with `PresentMultiple` using
   * `CombinationAggregatorMultiple`.
   */
  implicit def LR2RM[T <: Aggregator with Prepare with PresentMultiple]: AggregatableMultiple[List[T]] = {
    new AggregatableMultiple[List[T]] {
      def convert(t: List[T]): Aggregator with Prepare with PresentMultiple = CombinationAggregatorMultiple(t)
    }
  }
  /**
   * Converts an aggregator with `PresentMultiple` to an aggregator with `PresentMultiple`; that is, it is a pass
   * through.
   */
  implicit def R2RM[T <: Aggregator with Prepare with PresentMultiple]: AggregatableMultiple[T] = {
    new AggregatableMultiple[T] { def convert(t: T): Aggregator with Prepare with PresentMultiple = t }
  }
}

/** Type class for transforming a type `T` to an 'Aggregator with PrepareWithValue with PresentMultipleWithValue`. */
trait AggregatableMultipleWithValue[T, W] {
  /**
   * Returns an `Aggregator with PrepareWithValue with PresentMultipleWithValue` for type `T`.
   *
   * @param t Object that can be converted to an `Aggregator with PrepareWithValue with PresentMultipleWithValue`.
   */
  def convert(t: T): Aggregator with PrepareWithValue with PresentMultipleWithValue { type V >: W }
}

/** Companion object for the `AggregatableMultipleWithValue` type class. */
object AggregatableMultipleWithValue {
  /**
   * Converts a `List[Aggregator with PrepareWithValue with PresentMultipleWithValue]` to a single `Aggregator with
   * PrepareWithValue with PresentMultipleWithValue` using 'CombinationAggregatorMultipleWithValue`.
   */
  implicit def LR2RMWV[T <: Aggregator with PrepareWithValue with PresentMultipleWithValue { type V >: W }, W]: AggregatableMultipleWithValue[List[T], W] = {
    new AggregatableMultipleWithValue[List[T], W] {
      def convert(t: List[T]): Aggregator with PrepareWithValue with PresentMultipleWithValue { type V >: W } = {
        CombinationAggregatorMultipleWithValue[T, W](t)
      }
    }
  }
  /**
   * Converts a `Aggregator with PrepareWithValue with PresentMultipleWithValue` to a `Aggregator with
   * PrepareWithValue with PresentMultipleWithValue`; that is, it is a pass through.
   */
  implicit def R2RMWV[T <: Aggregator with PrepareWithValue with PresentMultipleWithValue { type V >: W }, W]: AggregatableMultipleWithValue[T, W] = {
    new AggregatableMultipleWithValue[T, W] {
      def convert(t: T): Aggregator with PrepareWithValue with PresentMultipleWithValue { type V >: W } = t
    }
  }
}

