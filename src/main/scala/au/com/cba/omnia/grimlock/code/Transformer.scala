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

package au.com.cba.omnia.grimlock.transform

import au.com.cba.omnia.grimlock.content._
import au.com.cba.omnia.grimlock.Matrix.Cell
import au.com.cba.omnia.grimlock.position._
import au.com.cba.omnia.grimlock.utility.{ Miscellaneous => Misc }

// TODO: Add the ability to compose individual transformers. For example,
//       first clamp a variable, then normalise. Perhaps use an andThen
//       construct. See Clamp() for an example.

/** Base trait for transformations. */
trait Transformer

/** Base trait for transformers that do not modify the number of dimensions. */
trait Present { self: Transformer =>
  /**
   * Present the transformed content(s).
   *
   * @param pos The position of the cell.
   * @param con The content to transform.
   *
   * @return Optional of either a cell or a `List` of cells where the position
   *         is creating by modifiying `pos` and the content is derived from
   *         `con`.
   *
   * @note An `Option` is used in the return type to allow reducers to be
   *       selective in what content they apply to. For example, normalising is
   *       undefined for categorical variables. The transformer now has the
   *       option to return `None`. This in turn permits an external API, for
   *       simple cases, where the user need not know about the types of
   *       variables of their data.
   * @note An `Either` is used to all either one-to-one or one-to-many
   *       transformations.
   */
  def present[P <: Position with ModifyablePosition](pos: P,
    con: Content): Option[Either[Cell[P#S], List[Cell[P#S]]]]
}

/**
 * Base trait for transformers that use a user supplied value but do not
 * modify the number of dimensions.
 */
trait PresentWithValue { self: Transformer =>
  /** Type of the external value. */
  type V

  /**
   * Present the transformed content(s).
   *
   * @param pos The position of the cell.
   * @param con The content to transform.
   * @param ext The external value.
   *
   * @return Optional of either a cell or a `List` of cells where the position
   *         is creating by modifiying `pos` and the content is derived from
   *         `con`.
   *
   * @note An `Option` is used in the return type to allow reducers to be
   *       selective in what content they apply to. For example, normalising is
   *       undefined for categorical variables. The transformer now has the
   *       option to return `None`. This in turn permits an external API, for
   *       simple cases, where the user need not know about the types of
   *       variables of their data.
   * @note An `Either` is used to all either one-to-one or one-to-many
   *       transformations.
   */
  def present[P <: Position with ModifyablePosition](pos: P, con: Content,
    ext: V): Option[Either[Cell[P#S], List[Cell[P#S]]]]
}

/**
 * Convenience trait for transformers that present with or without using a
 * user supplied value.
 */
trait PresentAndWithValue extends Present with PresentWithValue {
  self: Transformer =>
  type V = Any

  def present[P <: Position with ModifyablePosition](pos: P, con: Content,
    ext: V) = present(pos, con)
}

/**
 * Base trait for transformers that expand the position by appending
 * a dimension.
 */
trait PresentExpanded { self: Transformer =>
  /**
   * Present the transformed content(s).
   *
   * @param pos The position of the cell.
   * @param con The content to transform.
   *
   * @return Optional of either a cell or a `List` of cells where the position
   *         is creating by appending to `pos` and the content is derived from
   *         `con`.
   *
   * @note An `Option` is used in the return type to allow reducers to be
   *       selective in what content they apply to. For example, normalising is
   *       undefined for categorical variables. The transformer now has the
   *       option to return `None`. This in turn permits an external API, for
   *       simple cases, where the user need not know about the types of
   *       variables of their data.
   * @note An `Either` is used to all either one-to-one or one-to-many
   *       transformations.
   */
  def present[P <: Position with ExpandablePosition](pos: P,
    con: Content): Option[Either[Cell[P#M], List[Cell[P#M]]]]
}

/**
 * Base trait for transformers that use a user supplied value and expand the
 * position by appending a dimension.
 */
trait PresentExpandedWithValue { self: Transformer =>
  /** Type of the external value. */
  type V

  /**
   * Present the transformed content(s).
   *
   * @param pos The position of the cell.
   * @param con The content to transform.
   * @param ext The external value.
   *
   * @return Optional of either a cell or a `List` of cells where the position
   *         is creating by appending to `pos` and the content is derived from
   *         `con`.
   *
   * @note An `Option` is used in the return type to allow reducers to be
   *       selective in what content they apply to. For example, normalising is
   *       undefined for categorical variables. The transformer now has the
   *       option to return `None`. This in turn permits an external API, for
   *       simple cases, where the user need not know about the types of
   *       variables of their data.
   * @note An `Either` is used to all either one-to-one or one-to-many
   *       transformations.
   */
  def present[P <: Position with ExpandablePosition](pos: P, con: Content,
    ext: V): Option[Either[Cell[P#M], List[Cell[P#M]]]]
}

/**
 * Convenience trait for transformers that present an expanded position
 * with or without using a user supplied value.
 */
trait PresentExpandedAndWithValue extends PresentExpanded
  with PresentExpandedWithValue { self: Transformer =>
  type V = Any

  def present[P <: Position with ExpandablePosition](pos: P, con: Content,
    ext: V) = present(pos, con)
}

/**
 * Transformer that is a combination of one or more transformers with
 * `Present`.
 *
 * @param singles `List` of transformers that are combined together.
 *
 * @note This need not be called in an application. The `Transformable` type
 *       class will convert any `List[Transformer]` automatically to one of
 *       these.
 */
case class CombinationTransformer[T <: Transformer with Present](
  singles: List[T]) extends Transformer with Present {
  def present[P <: Position with ModifyablePosition](pos: P, con: Content) = {
    Some(Right(singles.flatMap {
      case s => Misc.mapFlatten(s.present(pos, con))
    }))
  }
}

/**
 * Transformer that is a combination of one or more transformers with
 * `PresentWithValue`.
 *
 * @param singles `List` of transformers that are combined together.
 *
 * @note This need not be called in an application. The
 *       `TransformableWithValue` type class will convert any
 *       `List[Transformer]` automatically to one of these.
 */
case class CombinationTransformerWithValue[T <: Transformer with PresentWithValue, W](singles: List[T]) extends Transformer
  with PresentWithValue {
  type V = W
  def present[P <: Position with ModifyablePosition](pos: P, con: Content,
    ext: V) = {
    Some(Right(singles.flatMap {
      case s => Misc.mapFlatten(s.present(pos, con, ext.asInstanceOf[s.V]))
    }))
  }
}

/**
 * Transformer that is a combination of one or more transformers with
 * `PresentExpanded`.
 *
 * @param singles `List` of transformers that are combined together.
 *
 * @note This need not be called in an application. The
 *       `TransformableExpanded` type class will convert any
 *       `List[Transformer]` automatically to one of these.
 */
case class CombinationTransformerExpanded[T <: Transformer with PresentExpanded](singles: List[T]) extends Transformer
  with PresentExpanded {
  def present[P <: Position with ExpandablePosition](pos: P, con: Content) = {
    Some(Right(singles.flatMap {
      case s => Misc.mapFlatten(s.present(pos, con))
    }))
  }
}

/**
 * Transformer that is a combination of one or more transformers with
 * `PresentExpandedWithValue`.
 *
 * @param singles `List` of transformers that are combined together.
 *
 * @note This need not be called in an application. The
 *       `TransformableExpandedWithValue` type class will convert any
 *       `List[Transformer]` automatically to one of these.
 */
case class CombinationTransformerExpandedWithValue[T <: Transformer with PresentExpandedWithValue, W](singles: List[T]) extends Transformer
  with PresentExpandedWithValue {
  type V = W
  def present[P <: Position with ExpandablePosition](pos: P, con: Content,
    ext: W) = {
    Some(Right(singles.flatMap {
      case s => Misc.mapFlatten(s.present(pos, con, ext.asInstanceOf[s.V]))
    }))
  }
}

/**
 * Type class for transforming a type `T` to a `Transformer with Present`.
 */
trait Transformable[T] {
  /**
   * Returns a `Transformer with Present` for type `T`.
   *
   * @param t Object that can be converted to a `Transformer with Present`.
   */
  def convert(t: T): Transformer with Present
}

/** Companion object for the `Transformable` type class. */
object Transformable {
  /**
   * Converts a `List[Transformer with Present]` to a single
   * `Transformer with Present` using `CombinationTransformer`.
   */
  implicit def LT2T[T <: Transformer with Present]: Transformable[List[T]] = {
    new Transformable[List[T]] {
      def convert(t: List[T]): Transformer with Present = {
        CombinationTransformer(t)
      }
    }
  }
  /**
   * Converts a `Transformer with Present` to a `Transformer with Present`;
   * that is, * it is a pass through.
   */
  implicit def T2T[T <: Transformer with Present]: Transformable[T] = {
    new Transformable[T] {
      def convert(t: T): Transformer with Present = t
    }
  }
}

/**
 * Type class for transforming a type `T` to a
 * `Transformer with PresentWithValue`.
 */
trait TransformableWithValue[T, W] {
  /**
   * Returns a `Transformer with PresentWithValue` for type `T`.
   *
   * @param t Object that can be converted to a
   *          `Transformer with PresentWithValue`.
   */
  def convert(t: T): Transformer with PresentWithValue
}

/** Companion object for the `TransformableWithValue` type class. */
object TransformableWithValue {
  /**
   * Converts a `List[Transformer with PresentWithValue]` to a single
   * `Transformer with PresentWithValue` using
   * `CombinationTransformerWithValue`.
   */
  implicit def LT2TWV[T <: Transformer with PresentWithValue { type V >: W }, W]: TransformableWithValue[List[T], W] = {
    new TransformableWithValue[List[T], W] {
      def convert(t: List[T]): Transformer with PresentWithValue = {
        CombinationTransformerWithValue[Transformer with PresentWithValue, W](t)
      }
    }
  }
  /**
   * Converts a `Transformer with PresentWithValue` to a
   * `Transformer with PresentWithValue`; that is, * it is a pass through.
   */
  implicit def T2TWV[T <: Transformer with PresentWithValue { type V >: W }, W]: TransformableWithValue[T, W] = {
    new TransformableWithValue[T, W] {
      def convert(t: T): Transformer with PresentWithValue = t
    }
  }
}

/**
 * Type class for transforming a type `T` to a
 * `Transformer with PresentExpanded`.
 */
trait TransformableExpanded[T] {
  /**
   * Returns a `Transformer with PresentExpanded` for type `T`.
   *
   * @param t Object that can be converted to a
   *          `Transformer with PresentExpanded`.
   */
  def convert(t: T): Transformer with PresentExpanded
}

/** Companion object for the `TransformableExpanded` type class. */
object TransformableExpanded {
  /**
   * Converts a `List[Transformer with PresentExpanded]` to a single
   * `Transformer with PresentExpanded` using `CombinationTransformerExpanded`.
   */
  implicit def LT2TE[T <: Transformer with PresentExpanded]: TransformableExpanded[List[T]] = {
    new TransformableExpanded[List[T]] {
      def convert(t: List[T]): Transformer with PresentExpanded = {
        CombinationTransformerExpanded(t)
      }
    }
  }
  /**
   * Converts a `Transformer with PresentExpanded` to a
   * `Transformer with PresentExpanded`; that is, it is a pass through.
   */
  implicit def T2TE[T <: Transformer with PresentExpanded]: TransformableExpanded[T] = {
    new TransformableExpanded[T] {
      def convert(t: T): Transformer with PresentExpanded = t
    }
  }
}

/**
 * Type class for transforming a type `T` to a
 * `Transformer with PresentExpandedWithValue`.
 */
trait TransformableExpandedWithValue[T, W] {
  /**
   * Returns a `Transformer with PresentExpandedWithValue` for type `T`.
   *
   * @param t Object that can be converted to a
   *          `Transformer with PresentExpandedWithValue`.
   */
  def convert(t: T): Transformer with PresentExpandedWithValue
}

/** Companion object for the `TransformableExpandedWithValue` type class. */
object TransformableExpandedWithValue {
  /**
   * Converts a `List[Transformer with PresentExpandedWithValue]` to
   * a single `Transformer with PresentExpandedWithValue` using
   * `CombinationTransformerExpandedWithValue`.
   */
  implicit def LT2TEWV[T <: Transformer with PresentExpandedWithValue { type V >: W }, W]: TransformableExpandedWithValue[List[T], W] = {
    new TransformableExpandedWithValue[List[T], W] {
      def convert(t: List[T]): Transformer with PresentExpandedWithValue = {
        CombinationTransformerExpandedWithValue[Transformer with PresentExpandedWithValue, W](t)
      }
    }
  }
  /**
   * Converts a `Transformer with PresentExpandedWithValue` to a
   * `Transformer with PresentExpandedWithValue`; that is, it is a pass through.
   */
  implicit def T2TEWV[T <: Transformer with PresentExpandedWithValue { type V >: W }, W]: TransformableExpandedWithValue[T, W] = {
    new TransformableExpandedWithValue[T, W] {
      def convert(t: T): Transformer with PresentExpandedWithValue = t
    }
  }
}

