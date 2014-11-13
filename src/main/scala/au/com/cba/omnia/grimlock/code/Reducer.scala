// Copyright 2014 Commonwealth Bank of Australia
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

package au.com.cba.omnia.grimlock.reduce

import au.com.cba.omnia.grimlock._
import au.com.cba.omnia.grimlock.content._
import au.com.cba.omnia.grimlock.encoding._
import au.com.cba.omnia.grimlock.Matrix.Cell
import au.com.cba.omnia.grimlock.position._
import au.com.cba.omnia.grimlock.utility.{ Miscellaneous => Misc }

/**
 * Base trait for reductions.
 *
 * @note Aggregator/aggregate are already available on a `TypedPipe`. So, to
 *       prevent name clashes, Reducer/reduce are used instead. The net
 *       effect is still that Reducers aggregator over a matrix.
 */
trait Reducer {
  /** Type of the state being reduced (aggregated). */
  type T

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
trait Prepare extends PrepareWithValue { self: Reducer =>
  type V = Any

  def prepare[P <: Position, D <: Dimension](slc: Slice[P, D], pos: P,
    con: Content, ext: V): T = prepare(slc, pos, con)

  /**
   * Prepare for reduction.
   *
   * @param slc Encapsulates the dimension(s) over with to reduce.
   * @param pos Original position corresponding to the cell. That is, it's the
   *            position prior to `slc.selected` being applied.
   * @param con Content which is to be reduced.
   *
   * @return State to reduce.
   */
  def prepare[P <: Position, D <: Dimension](slc: Slice[P, D], pos: P,
    con: Content): T
}

/** Base trait for reduction preparation with a user supplied value. */
trait PrepareWithValue { self: Reducer =>
  /** Type of the external value. */
  type V

  /**
   * Prepare for reduction.
   *
   * @param slc Encapsulates the dimension(s) over with to reduce.
   * @param pos Original position corresponding to the cell. That is, it's the
   *            position prior to `slc.selected` being applied.
   * @param con Content which is to be reduced.
   * @param ext User provided data required for preparation.
   *
   * @return State to reduce.
   */
  def prepare[P <: Position, D <: Dimension](slc: Slice[P, D], pos: P,
    con: Content, ext: V): T
}

/** Base trait for reductions that return a single value. */
trait PresentSingle { self: Reducer =>
  /**
   * Present the reduced content.
   *
   * @param pos The reduced position. That is, the position returned by
   *            `Slice.selected`.
   * @param t   The reduced state.
   *
   * @return Optional cell where the position is `pos` and the content is
   *         derived from `t`.
   *
   * @note An `Option` is used in the return type to allow reducers to be
   *       selective in what content they apply to. For example, computing
   *       the mean is undefined for categorical variables. The reducer now
   *       has the option to return `None`. This in turn permits an external
   *       API, for simple cases, where the user need not know about the
   *       types of variables of their data.
   */
  def presentSingle[P <: Position](pos: P, t: T): Option[Cell[P]]
}

/** Base trait for reductions that return multiple values. */
trait PresentMultiple { self: Reducer =>
  /**
   * Present the reduced content(s).
   *
   * @param pos The reduced position. That is, the position returned by
   *            `Slice.selected`.
   * @param t   The reduced state.
   *
   * @return Optional cell tuple where the position is creating by appending
   *         to `pos` (`append` method) and the content(s) is derived from `t`.
   *
   * @note An `Option` is used in the return type to allow reducers to be
   *       selective in what content they apply to. For example, computing
   *       the mean is undefined for categorical variables. The reducer now
   *       has the option to return `None`. This in turn permits an external
   *       API, for simple cases, where the user need not know about the
   *       types of variables of their data.
   */
  def presentMultiple[P <: Position with ExpandablePosition](pos: P,
    t: T): Option[Either[Cell[P#M], List[Cell[P#M]]]]
}

/**
 * Convenience trait for reducers that present a value both as `PresentSingle`
 * and `PresentMultiple`.
 */
trait PresentSingleAndMultiple extends PresentSingle with PresentMultiple {
  self: Reducer =>

  def presentSingle[P <: Position](pos: P, t: T): Option[Cell[P]] = {
    content(t).map { case c => (pos, c) }
  }

  def presentMultiple[P <: Position with ExpandablePosition](pos: P,
    t: T): Option[Either[Cell[P#M], List[Cell[P#M]]]] = {
    name match {
      case Some(n) => content(t).map {
        case con => Left((pos.append(n), con))
      }
      case None => Option.empty[Either[Cell[P#M], List[Cell[P#M]]]]
    }
  }

  /** Name of the coordinate when presenting as multiple. */
  val name: Option[Value]

  protected def content(t: T): Option[Content]
}

/**
 * Reducer that is a combination of one or more reducer with `PresentMultiple`.
 *
 * @param reducers `List` of reducers that are combined together.
 *
 * @note This need not be called in an application. The `ReducerableMultiple`
 *       type class will convert any `List[Reducer]` automatically to one of
 *       these.
 */
case class CombinationReducerMultiple[T <: Reducer with Prepare with PresentMultiple](reducers: List[T]) extends Reducer with Prepare
  with PresentMultiple {
  type T = List[Any]

  def prepare[P <: Position, D <: Dimension](slc: Slice[P, D], pos: P,
    con: Content): T = {
    reducers.map { case reducer => reducer.prepare(slc, pos, con) }
  }

  def reduce(lt: T, rt: T): T = {
    (reducers, lt, rt).zipped.map {
      case (reducer, l, r) => reducer.reduce(l.asInstanceOf[reducer.T],
        r.asInstanceOf[reducer.T])
    }
  }

  def presentMultiple[P <: Position with ExpandablePosition](pos: P,
    t: T): Option[Either[Cell[P#M], List[Cell[P#M]]]] = {
    Some(Right((reducers, t).zipped.flatMap {
      case (reducer, s) => Misc.mapFlatten(reducer.presentMultiple(pos,
        s.asInstanceOf[reducer.T]))
    }))
  }
}

/**
 * Reducer that is a combination of one or more reducers with
 * `PrepareWithValue` with `PresentMultiple`.
 *
 * @param reducers `List` of reducers that are combined together.
 *
 * @note This need not be called in an application. The
 *       `ReducerableMultipleWithValue` type class will convert any
 *       `List[Reducer]` automatically to one of these.
 */
case class CombinationReducerMultipleWithValue[T <: Reducer with PrepareWithValue with PresentMultiple, W](reducers: List[T])
  extends Reducer with PrepareWithValue with PresentMultiple {
  type T = List[Any]
  type V = W

  def prepare[P <: Position, D <: Dimension](slc: Slice[P, D], pos: P,
    con: Content, ext: V): T = {
    reducers.map {
      case reducer => reducer.prepare(slc, pos, con,
        ext.asInstanceOf[reducer.V])
    }
  }

  def reduce(lt: T, rt: T): T = {
    (reducers, lt, rt).zipped.map {
      case (reducer, l, r) => reducer.reduce(l.asInstanceOf[reducer.T],
        r.asInstanceOf[reducer.T])
    }
  }

  def presentMultiple[P <: Position with ExpandablePosition](pos: P,
    t: T): Option[Either[Cell[P#M], List[Cell[P#M]]]] = {
    Some(Right((reducers, t).zipped.flatMap {
      case (reducer, s) => Misc.mapFlatten(reducer.presentMultiple(pos,
        s.asInstanceOf[reducer.T]))
    }))
  }
}

/**
 * Type class for transforming a type `T` to a reducer` with `PresentMultiple`.
 */
trait ReducerableMultiple[T] {
  /**
   * Returns a reducer with `PresentMultiple` for type `T`.
   *
   * @param t Object that can be converted to a reducer with `PresentMultiple`.
   */
  def convert(t: T): Reducer with Prepare with PresentMultiple
}

/** Companion object for the `ReducerableMultiple` type class. */
object ReducerableMultiple {
  /**
   * Converts a `List[Reducer with PresentMultiple]` to a single
   * reducer with `PresentMultiple` using `CombinationReducerMultiple`.
   */
  implicit def LR2RM[T <: Reducer with Prepare with PresentMultiple]: ReducerableMultiple[List[T]] = {
    new ReducerableMultiple[List[T]] {
      def convert(t: List[T]): Reducer with Prepare with PresentMultiple = {
        CombinationReducerMultiple(t)
      }
    }
  }
  /**
   * Converts a reducer with `PresentMultiple` to a reducer with
   * `PresentMultiple`; that is, it is a pass through.
   */
  implicit def R2RM[T <: Reducer with Prepare with PresentMultiple]: ReducerableMultiple[T] = {
    new ReducerableMultiple[T] {
      def convert(t: T): Reducer with Prepare with PresentMultiple = t
    }
  }
}

/**
 * Type class for transforming a type `T` to a reducer with `PrepareWithValue`
 * with `PresentMultiple`.
 */
trait ReducerableMultipleWithValue[T, W] {
  /**
   * Returns a reducer with `PrepareWithValue` with `PresentMultiple`
   * for type `T`.
   *
   * @param t Object that can be converted to a reducer with
   *          `PrepareWithValue` with `PresentMultiple`.
   */
  def convert(t: T): Reducer with PrepareWithValue with PresentMultiple
}

/** Companion object for the `ReducerableMultipleWithValue` type class. */
object ReducerableMultipleWithValue {
  /**
   * Converts a `List[Reducer with PrepareWithValue with PresentMultiple]` to
   * a single `Reducer with PrepareWithValue with PresentMultiple` using
   * 'CombinationReducerMultipleWithValue`.
   */
  implicit def LR2RMWV[T <: Reducer with PrepareWithValue with PresentMultiple { type V >: W }, W]: ReducerableMultipleWithValue[List[T], W] = {
    new ReducerableMultipleWithValue[List[T], W] {
      def convert(t: List[T]): Reducer with PrepareWithValue with PresentMultiple = {
        CombinationReducerMultipleWithValue[Reducer with PrepareWithValue with PresentMultiple, W](t)
      }
    }
  }
  /**
   * Converts a `Reducer with PrepareWithValue with PresentMultiple` to a
   * `Reducer with PrepareWithValue with PresentMultiple`; that is, it is
   * a pass through.
   */
  implicit def R2RMWV[T <: Reducer with PrepareWithValue with PresentMultiple { type V >: W }, W]: ReducerableMultipleWithValue[T, W] = {
    new ReducerableMultipleWithValue[T, W] {
      def convert(t: T): Reducer with PrepareWithValue with PresentMultiple = t
    }
  }
}

