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

import au.com.cba.omnia.grimlock._
import au.com.cba.omnia.grimlock.content._
import au.com.cba.omnia.grimlock.content.metadata._
import au.com.cba.omnia.grimlock.encoding._
import au.com.cba.omnia.grimlock.Matrix.Cell
import au.com.cba.omnia.grimlock.position._
import au.com.cba.omnia.grimlock.Type._

/**
 * Convenience trait defining common functionality patterns for implementing
 * transformers.
 */
trait PresentCell[T] {
  protected def present[P <: Position with ModifyablePosition](dim: Dimension,
    pos: P, con: Content, name: Option[String], typ: Option[Type],
    value: T): Option[Either[Cell[P#S], List[Cell[P#S]]]] = {
    checkContent(con, typ) match {
      case true => getCell(dim, pos, con, name, value)
      case false => None
    }
  }

  protected def present[P <: Position with ModifyablePosition](dim: Dimension,
    pos: P, con: Content, name: Option[String], typ: Option[Type],
    f: (T) => T): Option[Either[Cell[P#S], List[Cell[P#S]]]] = {
    (checkContent(con, typ), getValue(con)) match {
      case (true, Some(v)) => getCell(dim, pos, con, name, f(v))
      case _ => None
    }
  }

  protected def present[P <: Position with ModifyablePosition](dim: Dimension,
    pos: P, con: Content, ext: Map[Position1D, Content],
    key: Option[Position1D], name: Option[String], typ: Option[Type],
    f: (T, T) => T,
    inverse: Boolean): Option[Either[Cell[P#S], List[Cell[P#S]]]] = {
    present(dim, pos, con, name, typ, f, inverse,
      getValue(getKey(dim, pos, key), ext))
  }

  protected def present[P <: Position with ModifyablePosition](dim: Dimension,
    pos: P, con: Content, ext: Map[Position1D, Map[Position1D, Content]],
    key: Position1D, name: Option[String], typ: Option[Type], f: (T, T) => T,
    inverse: Boolean): Option[Either[Cell[P#S], List[Cell[P#S]]]] = {
    present(dim, pos, con, name, typ, f, inverse,
      getValue(getKey(dim, pos, None), key, ext))
  }

  protected def present[P <: Position with ModifyablePosition](dim: Dimension,
    pos: P, con: Content, ext: Map[Position1D, Map[Position1D, Content]],
    key1: Position1D, key2: Position1D, name: Option[String], typ: Option[Type],
    f: (T, T, T) => T): Option[Either[Cell[P#S], List[Cell[P#S]]]] = {
    (checkContent(con, typ), getValue(con),
      getValue(getKey(dim, pos, None), key1, ext),
      getValue(getKey(dim, pos, None), key2, ext)) match {
        case (true, Some(v), Some(x), Some(y)) => getCell(dim, pos, con, name,
          f(v, x, y))
        case _ => None
      }
  }

  protected def getContent(value: T): Content
  protected def getValue(con: Content): Option[T]

  private def checkContent(con: Content, typ: Option[Type]): Boolean = {
    typ match {
      case Some(t) => con.schema.kind.isSpecialisationOf(t)
      case None => true
    }
  }

  private def getValue(key: Position1D,
    ext: Map[Position1D, Content]): Option[T] = {
    ext.get(key).flatMap(getValue(_))
  }

  private def getValue(key1: Position1D, key2: Position1D,
    ext: Map[Position1D, Map[Position1D, Content]]): Option[T] = {
    ext.get(key1).flatMap(_.get(key2).flatMap(getValue(_)))
  }

  private def getName[P <: Position with ModifyablePosition](dim: Dimension,
    pos: P, con: Content, name: Option[String]): P#S = {
    name match {
      case Some(n) => pos.set(dim, n.format(pos.get(dim).toShortString,
        con.value.toShortString))
      case None => pos.asInstanceOf[P#S]
    }
  }

  private def getKey[P <: Position](dim: Dimension, pos: P,
    key: Option[Position1D]): Position1D = {
    key match {
      case Some(p) => p
      case None => Position1D(pos.get(dim))
    }
  }

  private def getCell[P <: Position with ModifyablePosition](dim: Dimension,
    pos: P, con: Content, name: Option[String],
    value: T): Option[Either[Cell[P#S], List[Cell[P#S]]]] = {
    Some(Left((getName(dim, pos, con, name), getContent(value))))
  }

  private def present[P <: Position with ModifyablePosition](dim: Dimension,
    pos: P, con: Content, name: Option[String], typ: Option[Type],
    f: (T, T) => T, inverse: Boolean,
    value: Option[T]): Option[Either[Cell[P#S], List[Cell[P#S]]]] = {
    (checkContent(con, typ), getValue(con), value) match {
      case (true, Some(l), Some(r)) => getCell(dim, pos, con, name,
        if (inverse) f(r, l) else f(l, r))
      case _ => None
    }
  }
}

/**
 * Convenience trait defining common functionality patterns for implementing
 * transformers returning `Long` values.
 */
trait PresentLong extends PresentCell[Long] {
  protected def getContent(value: Long): Content = {
    Content(DiscreteSchema[Codex.LongCodex](), value)
  }
  protected def getValue(con: Content): Option[Long] = con.value.asLong
}

/**
 * Convenience trait defining common functionality patterns for implementing
 * transformers returning `Double` values.
 */
trait PresentDouble extends PresentCell[Double] {
  protected def getContent(value: Double): Content = {
    Content(ContinuousSchema[Codex.DoubleCodex](), value)
  }
  protected def getValue(con: Content): Option[Double] = con.value.asDouble
}

/**
 * Create indicator variables.
 *
 * @param dim  Dimension for which to create indicator variables.
 * @param name Optional pattern for the new name of the coordinate at `dim`.
 *             Use `%[12]$``s` for the string representations of the coordinate,
 *             and the content.
 */
case class Indicator private (dim: Dimension, name: Option[String])
  extends Transformer with Present with PresentLong {
  def present[P <: Position with ModifyablePosition](pos: P, con: Content) = {
    present(dim, pos, con, name, None, 1)
  }
}

/** Companion object to the `Indicator` class defining constructors. */
object Indicator {
  /**
   * Create indicator variables.
   *
   * @param dim Dimension for which to create indicator variables.
   */
  def apply(dim: Dimension): Indicator = {
    Indicator(dim, None)
  }

  /**
   * Create indicator variables.
   *
   * @param dim  Dimension for which to create indicator variables.
   * @param name Pattern for the new name of the coordinate at `dim`. Use
   *             `%[12]$``s` for the string representations of the coordinate,
   *             and the content.
   */
  def apply(dim: Dimension, name: String): Indicator = {
    Indicator(dim, Some(name))
  }
}

/**
 * Binarise categorical variables.
 *
 * @param dim  Dimension for for which to create binarised variables.
 * @param name Pattern for the new name of the coordinate at `dim`. Use
 *             `%[12]$``s` for the string representations of the coordinate,
 *             and the content.
 *
 * @note Binarisation is only applied to categorical variables.
 */
case class Binarise(dim: Dimension, name: String = "%1$s=%2$s")
  extends Transformer with Present with PresentLong {
  def present[P <: Position with ModifyablePosition](pos: P, con: Content) = {
    present(dim, pos, con, Some(name), Some(Categorical), 1)
  }
}

/**
 * Normalise numeric variables.
 *
 * @param dim  Dimension for for which to create normalised variables.
 * @param key  Key into the inner map of `V`, identifying the normalisation
 *             constant.
 * @param name Optional pattern for the new name of the coordinate at `dim`.
 *             Use `%[12]$``s` for the string representations of the coordinate,
 *             and the content.
 *
 * @note Normalisation scales a variable in the range [-1, 1]. It is only
 *       applied to numerical variables.
 */
case class Normalise private (dim: Dimension, key: Position1D,
  name: Option[String]) extends Transformer with PresentWithValue
  with PresentDouble {
  type V = Map[Position1D, Map[Position1D, Content]]

  def present[P <: Position with ModifyablePosition](pos: P, con: Content,
    ext: V) = {
    present(dim, pos, con, ext, key, name, Some(Numerical),
      (value: Double, const: Double) => value / const, false)
  }
}

/** Companion object to the `Normalise` class defining constructors. */
object Normalise {
  /**
   * Normalise numeric variables.
   *
   * @param dim Dimension for for which to create normalised variables.
   * @param key Key into the inner map of `V`, identifying the normalisation
   *            constant.
   */
  def apply[T](dim: Dimension, key: T)(
    implicit ev: Positionable[T, Position1D]): Normalise = {
    Normalise(dim, ev.convert(key), None)
  }

  /**
   * Normalise numeric variables.
   *
   * @param dim  Dimension for for which to create normalised variables.
   * @param key  Key into the inner map of `V`, identifying the normalisation
   *             constant.
   * @param name Pattern for the new name of the coordinate at `dim`. Use
   *             `%[12]$``s` for the string representations of the coordinate,
   *             and the content.
   */
  def apply[T](dim: Dimension, key: T, name: String)(
    implicit ev: Positionable[T, Position1D]): Normalise = {
    Normalise(dim, ev.convert(key), Some(name))
  }
}

/**
 * Standardise numeric variables.
 *
 * @param dim       Dimension for for which to create standardised variables.
 * @param mean      Key into the inner map of `V`, identifying the
 *                  standardisation constant for the mean.
 * @param sd        Key into the inner map of `V`, identifying the
 *                  standardisation constant for the standard deviation.
 * @param name      Optional pattern for the new name of the coordinate at
 *                  `dim`. Use `%[12]$``s` for the string representations of
 *                  the coordinate, and the content.
 * @param threshold Minimum standard deviation threshold. Values less than this
 *                  result in standardised value of zero.
 *
 * @note Standardisation results in a variable with zero mean and variance
 *       of one. It is only applied to numerical variables.
 */
case class Standardise private (dim: Dimension, mean: Position1D,
  sd: Position1D, name: Option[String], threshold: Double) extends Transformer
  with PresentWithValue with PresentDouble {
  type V = Map[Position1D, Map[Position1D, Content]]

  def present[P <: Position with ModifyablePosition](pos: P, con: Content,
    ext: V) = {
    present(dim, pos, con, ext, mean, sd, name, Some(Numerical), standardise _)
  }

  private def standardise(v: Double, m: Double, s: Double) = {
    if (math.abs(s) < threshold) 0.0 else (v - m) / s
  }
}

/** Companion object to the `Standardise` class defining constructors. */
object Standardise {
  /**
   * Standardise numeric variables.
   *
   * @param dim  Dimension for for which to create standardised variables.
   * @param mean Key into the inner map of `V`, identifying the standardisation
   *             constant for the mean.
   * @param sd   Key into the inner map of `V`, identifying the standardisation
   *             constant for the standard deviation.
   */
  def apply[T, U](dim: Dimension, mean: T, sd: U)(
    implicit ev1: Positionable[T, Position1D],
    ev2: Positionable[U, Position1D]): Standardise = {
    Standardise(dim, ev1.convert(mean), ev2.convert(sd), None,
      DefaultThreshold)
  }

  /**
   * Standardise numeric variables.
   *
   * @param dim  Dimension for for which to create standardised variables.
   * @param mean Key into the inner map of `V`, identifying the standardisation
   *             constant for the mean.
   * @param sd   Key into the inner map of `V`, identifying the standardisation
   *             constant for the standard deviation.
   * @param name Pattern for the new name of the coordinate at `dim`. Use
   *             `%[12]$``s` for the string representations of the coordinate,
   *             and the content.
   */
  def apply[T, U](dim: Dimension, mean: T, sd: U, name: String)(
    implicit ev1: Positionable[T, Position1D],
    ev2: Positionable[U, Position1D]): Standardise = {
    Standardise(dim, ev1.convert(mean), ev2.convert(sd), Some(name),
      DefaultThreshold)
  }

  /**
   * Standardise numeric variables.
   *
   * @param dim       Dimension for for which to create standardised variables.
   * @param mean      Key into the inner map of `V`, identifying the
   *                  standardisation constant for the mean.
   * @param sd        Key into the inner map of `V`, identifying the
   *                  standardisation constant for the standard deviation.
   * @param threshold Minimum standard deviation threshold. Values less than
   *                  this result in standardised value of zero.
   */
  def apply[T, U](dim: Dimension, mean: T, sd: U, threshold: Double)(
    implicit ev1: Positionable[T, Position1D],
    ev2: Positionable[U, Position1D]): Standardise = {
    Standardise(dim, ev1.convert(mean), ev2.convert(sd), None, threshold)
  }

  /**
   * Standardise numeric variables.
   *
   * @param dim       Dimension for for which to create standardised variables.
   * @param mean      Key into the inner map of `V`, identifying the
   *                  standardisation constant for the mean.
   * @param sd        Key into the inner map of `V`, identifying the
   *                  standardisation constant for the standard deviation.
   * @param name      Pattern for the new name of the coordinate at `dim`. Use
   *                  `%[12]$``s` for the string representations of the
   *                  coordinate, and the content.
   * @param threshold Minimum standard deviation threshold. Values less than
   *                  this result in standardised value of zero.
   */
  def apply[T, U](dim: Dimension, mean: T, sd: U, name: String,
    threshold: Double)(implicit ev1: Positionable[T, Position1D],
      ev2: Positionable[U, Position1D]): Standardise = {
    Standardise(dim, ev1.convert(mean), ev2.convert(sd), Some(name), threshold)
  }

  private val DefaultThreshold = 1e-4
}

/**
 * Clamp numeric variables.
 *
 * @param dim   Dimension for for which to create clamped variables.
 * @param lower Key into the inner map of `V`, identifying the lower clamping
 *              constant for the mean.
 * @param upper Key into the inner map of `V`, identifying the upper clamping
 *              constant for the standard deviation.
 * @param name  Optional pattern for the new name of the coordinate at `dim`.
 *              Use `%[12]$``s` for the string representations of the
 *              coordinate, and the content.
 *
 * @note Clamping results in a variable not smaller (or greater) than the
 *       clamping constants. It is only applied to numerical variables.
 */
case class Clamp private (dim: Dimension, lower: Position1D, upper: Position1D,
  name: Option[String]) extends Transformer with PresentWithValue
  with PresentDouble {
  type V = Map[Position1D, Map[Position1D, Content]]

  def present[P <: Position with ModifyablePosition](pos: P, con: Content,
    ext: V) = {
    present(dim, pos, con, ext, lower, upper, name, Some(Numerical), clamp _)
  }

  private def clamp(v: Double, l: Double, u: Double) = {
    if (v < l) l else if (v > u) u else v
  }
}

/** Companion object to the `Clamp` class defining constructors. */
object Clamp {
  /**
   * Clamp numeric variables.
   *
   * @param dim   Dimension for for which to create clamped variables.
   * @param lower Key into the inner map of `V`, identifying the lower clamping
   *              constant for the mean.
   * @param upper Key into the inner map of `V`, identifying the upper clamping
   *              constant for the standard deviation.
   */
  def apply[T, U](dim: Dimension, lower: T, upper: U)(
    implicit ev1: Positionable[T, Position1D],
    ev2: Positionable[U, Position1D]): Clamp = {
    Clamp(dim, ev1.convert(lower), ev2.convert(upper), None)
  }

  /**
   * Clamp numeric variables.
   *
   * @param dim   Dimension for for which to create clamped variables.
   * @param lower Key into the inner map of `V`, identifying the lower clamping
   *              constant for the mean.
   * @param upper Key into the inner map of `V`, identifying the upper clamping
   *              constant for the standard deviation.
   * @param name  Pattern for the new name of the coordinate at `dim`. Use
   *              `%[12]$``s` for the string representations of the coordinate,
   *              and the content.
   */
  def apply[T, U](dim: Dimension, lower: T, upper: U, name: String)(
    implicit ev1: Positionable[T, Position1D],
    ev2: Positionable[U, Position1D]): Clamp = {
    Clamp(dim, ev1.convert(lower), ev2.convert(upper), Some(name))
  }
}

/**
 * Compute the inverse document frequency.
 *
 * @param dim  Dimension for which to create inverse document frequencies.
 * @param key  Optional key into the map `V` identifying the number of
 *             documents.
 * @param name Optional pattern for the new name of the coordinate at `dim`.
 *             Use `%[12]$``s` for the string representations of the coordinate,
 *             and the content.
 * @param idf  Idf function to use.
 *
 * @note Idf is only applied to numerical variables.
 */
case class Idf private (dim: Dimension, key: Option[Position1D],
  name: Option[String], idf: (Double, Double) => Double) extends Transformer
  with PresentWithValue with PresentDouble {
  type V = Map[Position1D, Content]

  def present[P <: Position with ModifyablePosition](pos: P, con: Content,
    ext: V) = {
    present(dim, pos, con, ext, key, name, Some(Numerical),
      (df: Double, n: Double) => idf(df, n), false)
  }
}

/** Companion object to the `Idf` class defining constructors. */
object Idf {
  /**
   * Standard inverse document frequency.
   *
   * @param log The logarithmic function to use.
   * @param add Offset to add to document frequency.
   */
  def Transform(log: (Double) => Double,
    add: Int): (Double, Double) => Double = {
    (df: Double, n: Double) => log(n / (add + df))
  }

  /**
   * Compute the inverse document frequency.
   *
   * @param dim Dimension for which to create inverse document frequencies.
   */
  def apply(dim: Dimension): Idf = {
    Idf(dim, None, None, DefaultTransform)
  }

  /**
   * Compute the inverse document frequency.
   *
   * @param dim Dimension for which to create inverse document frequencies.
   * @param key Key into the map `V` identifying the number of documents.
   */
  def apply[T](dim: Dimension, key: T)(
    implicit ev: Positionable[T, Position1D]): Idf = {
    Idf(dim, Some(ev.convert(key)), None, DefaultTransform)
  }

  /**
   * Compute the inverse document frequency.
   *
   * @param dim  Dimension for which to create inverse document frequencies.
   * @param name Pattern for the new name of the coordinate at `dim`. Use
   *             `%[12]$``s` for the string representations of the coordinate,
   *             and the content.
   */
  def apply(dim: Dimension, name: String): Idf = {
    Idf(dim, None, Some(name), DefaultTransform)
  }

  /**
   * Compute the inverse document frequency.
   *
   * @param dim Dimension for which to create inverse document frequencies.
   * @param idf Idf function to use.
   */
  def apply(dim: Dimension, idf: (Double, Double) => Double): Idf = {
    Idf(dim, None, None, idf)
  }

  /**
   * Compute the inverse document frequency.
   *
   * @param dim  Dimension for which to create inverse document frequencies.
   * @param key  Key into the map `V` identifying the number of documents.
   * @param name Pattern for the new name of the coordinate at `dim`. Use
   *             `%[12]$``s` for the string representations of the coordinate,
   *             and the content.
   */
  def apply[T](dim: Dimension, key: T, name: String)(
    implicit ev: Positionable[T, Position1D]): Idf = {
    Idf(dim, Some(ev.convert(key)), Some(name), DefaultTransform)
  }

  /**
   * Compute the inverse document frequency.
   *
   * @param dim Dimension for which to create inverse document frequencies.
   * @param key Key into the map `V` identifying the number of documents.
   * @param idf Idf function to use.
   */
  def apply[T](dim: Dimension, key: T, idf: (Double, Double) => Double)(
    implicit ev: Positionable[T, Position1D]): Idf = {
    Idf(dim, Some(ev.convert(key)), None, idf)
  }

  /**
   * Compute the inverse document frequency.
   *
   * @param dim  Dimension for which to create inverse document frequencies.
   * @param name Pattern for the new name of the coordinate at `dim`. Use
   *             `%[12]$``s` for the string representations of the coordinate,
   *             and the content.
   * @param idf  Idf function to use.
   */
  def apply(dim: Dimension, name: String,
    idf: (Double, Double) => Double): Idf = {
    Idf(dim, None, Some(name), idf)
  }

  /**
   * Compute the inverse document frequency.
   *
   * @param dim  Dimension for which to create inverse document frequencies.
   * @param key  Key into the map `V` identifying the number of documents.
   * @param name Pattern for the new name of the coordinate at `dim`. Use
   *             `%[12]$``s` for the string representations of the coordinate,
   *             and the content.
   * @param idf  Idf function to use.
   */
  def apply[T](dim: Dimension, key: T, name: String,
    idf: (Double, Double) => Double)(
      implicit ev: Positionable[T, Position1D]): Idf = {
    Idf(dim, Some(ev.convert(key)), Some(name), idf)
  }

  protected def DefaultTransform() = Transform(math.log, 1)
}

/**
 * Create boolean term frequencies; all term frequencies are binarised.
 *
 * @param dim  Dimension for which to create boolean term frequencies.
 * @param name Optional pattern for the new name of the coordinate at `dim`.
 *             Use `%[12]$``s` for the string representations of the coordinate,
 *             and the content.
 *
 * @note Boolean tf is only applied to numerical variables.
 */
case class BooleanTf private (dim: Dimension, name: Option[String])
  extends Transformer with Present with PresentDouble {
  def present[P <: Position with ModifyablePosition](pos: P, con: Content) = {
    present(dim, pos, con, name, Some(Numerical), 1)
  }
}

/** Companion object to the `BooleanTf` class defining constructors. */
object BooleanTf {
  /**
   * Create boolean term frequencies; all term frequencies are binarised.
   *
   * @param dim Dimension for which to create boolean term frequencies.
   */
  def apply(dim: Dimension): BooleanTf = {
    BooleanTf(dim, None)
  }

  /**
   * Create boolean term frequencies; all term frequencies are binarised.
   *
   * @param dim  Dimension for which to create boolean term frequencies.
   * @param name Pattern for the new name of the coordinate at `dim`. Use
   *             `%[12]$``s` for the string representations of the coordinate,
   *             and the content.
   */
  def apply(dim: Dimension, name: String): BooleanTf = {
    BooleanTf(dim, Some(name))
  }
}

/**
 * Create logarithmic term frequencies.
 *
 * @param dim  Dimension for which to create boolean term frequencies.
 * @param name Optional pattern for the new name of the coordinate at `dim`.
 *             Use `%[12]$``s` for the string representations of the coordinate,
 *             and the content.
 * @param log  Log function to use.
 *
 * @note Logarithmic tf is only applied to numerical variables.
 */
case class LogarithmicTf private (dim: Dimension, name: Option[String],
  log: (Double) => Double) extends Transformer with Present with PresentDouble {
  def present[P <: Position with ModifyablePosition](pos: P, con: Content) = {
    present(dim, pos, con, name, Some(Numerical), (tf: Double) => log(tf + 1))
  }
}

/** Companion object to the `LogarithmicTf` class defining constructors. */
object LogarithmicTf {
  /**
   * Create logarithmic term frequencies.
   *
   * @param dim Dimension for which to create boolean term frequencies.
   */
  def apply(dim: Dimension): LogarithmicTf = {
    LogarithmicTf(dim, None, DefaultLog)
  }

  /**
   * Create logarithmic term frequencies.
   *
   * @param dim  Dimension for which to create boolean term frequencies.
   * @param name Pattern for the new name of the coordinate at `dim`. Use
   *             `%[12]$``s` for the string representations of the coordinate,
   *             and the content.
   */
  def apply(dim: Dimension, name: String): LogarithmicTf = {
    LogarithmicTf(dim, Some(name), DefaultLog)
  }

  /**
   * Create logarithmic term frequencies.
   *
   * @param dim Dimension for which to create boolean term frequencies.
   * @param log Log function to use.
   */
  def apply(dim: Dimension, log: (Double) => Double): LogarithmicTf = {
    LogarithmicTf(dim, None, log)
  }

  /**
   * Create logarithmic term frequencies.
   *
   * @param dim  Dimension for which to create boolean term frequencies.
   * @param name Pattern for the new name of the coordinate at `dim`. Use
   *             `%[12]$``s` for the string representations of the coordinate,
   *             and the content.
   * @param log  Log function to use.
   */
  def apply(dim: Dimension, name: String,
    log: (Double) => Double): LogarithmicTf = {
    LogarithmicTf(dim, Some(name), log)
  }

  protected def DefaultLog(): (Double) => Double = math.log
}

/**
 * Create augmented term frequencies.
 *
 * @param dim  Dimension for which to create boolean term frequencies.
 * @param name Optional pattern for the new name of the coordinate at `dim`.
 *             Use `%[12]$``s` for the string representations of the coordinate,
 *             and the content.
 *
 * @note Augmented tf is only applied to numerical variables.
 */
case class AugmentedTf private (dim: Dimension, name: Option[String])
  extends Transformer with PresentWithValue with PresentDouble {
  type V = Map[Position1D, Content]

  def present[P <: Position with ModifyablePosition](pos: P, con: Content,
    ext: V) = {
    present(dim, pos, con, ext, None, name, Some(Numerical),
      (tf: Double, m: Double) => 0.5 + (0.5 * tf) / m, false)
  }
}

/** Companion object to the `AugmentedTf` class defining constructors. */
object AugmentedTf {
  /**
   * Create augmented term frequencies.
   *
   * @param dim Dimension for which to create boolean term frequencies.
   */
  def apply(dim: Dimension): AugmentedTf = {
    AugmentedTf(dim, None)
  }

  /**
   * Create augmented term frequencies.
   *
   * @param dim  Dimension for which to create boolean term frequencies.
   * @param name Optional pattern for the new name of the coordinate at `dim`.
   *             Use `%[12]$``s` for the string representations of the
   *             coordinate, and the content.
   */
  def apply(dim: Dimension, name: String): AugmentedTf = {
    AugmentedTf(dim, Some(name))
  }
}

/**
 * Create tf-idf values.
 *
 * @param dim  Dimension for which to create term frequencies.
 * @param key  Optional key into the map `V` identifying the inverse
 *             document frequency.
 * @param name Optional pattern for the new name of the coordinate at `dim`.
 *             Use `%[12]$``s` for the string representations of the coordinate,
 *             and the content.
 *
 * @note Tf-idf is only applied to numerical variables.
 */
case class TfIdf private (dim: Dimension, key: Option[Position1D],
  name: Option[String]) extends Transformer with PresentWithValue
  with PresentDouble {
  type V = Map[Position1D, Content]

  def present[P <: Position with ModifyablePosition](pos: P, con: Content,
    ext: V) = {
    present(dim, pos, con, ext, key, name, Some(Numerical),
      (tf: Double, idf: Double) => tf * idf, false)
  }
}

/** Companion object to the `TfIdf` class defining constructors. */
object TfIdf {
  /**
   * Create tf-idf values.
   *
   * @param dim Dimension for which to create term frequencies.
   */
  def apply(dim: Dimension): TfIdf = {
    TfIdf(dim, None, None)
  }

  /**
   * Create tf-idf values.
   *
   * @param dim Dimension for which to create term frequencies.
   * @param key Key into the map `V` identifying the inverse document frequency.
   */
  def apply[T](dim: Dimension, key: T)(
    implicit ev: Positionable[T, Position1D]): TfIdf = {
    TfIdf(dim, Some(ev.convert(key)), None)
  }

  /**
   * Create tf-idf values.
   *
   * @param dim  Dimension for which to create term frequencies.
   * @param name Pattern for the new name of the coordinate at `dim`. Use
   *             `%[12]$``s` for the string representations of the coordinate,
   *             and the content.
   */
  def apply(dim: Dimension, name: String): TfIdf = {
    TfIdf(dim, None, Some(name))
  }

  /**
   * Create tf-idf values.
   *
   * @param dim  Dimension for which to create term frequencies.
   * @param key  Key into the map `V` identifying the inverse document
   *             frequency.
   * @param name Pattern for the new name of the coordinate at `dim`. Use
   *             `%[12]$``s` for the string representations of the coordinate,
   *             and the content.
   */
  def apply[T](dim: Dimension, key: T, name: String)(
    implicit ev: Positionable[T, Position1D]): TfIdf = {
    TfIdf(dim, Some(ev.convert(key)), Some(name))
  }
}

/**
 * Subtract a value.
 *
 * @param dim     Dimension for which to subtract.
 * @param key     Optional key into the map `V` identifying the value to
 *                subtract.
 * @param name    Optional pattern for the new name of the coordinate at `dim`.
 *                Use `%[12]$``s` for the string representations of the
 *                coordinate, and the content.
 * @param inverse Indicator specifying order of subtract.
 *
 * @note Subtract is only applied to numerical variables.
 */
case class Subtract private (dim: Dimension, key: Option[Position1D],
  name: Option[String], inverse: Boolean) extends Transformer
  with PresentWithValue with PresentDouble {
  type V = Map[Position1D, Content]

  def present[P <: Position with ModifyablePosition](pos: P, con: Content,
    ext: V) = {
    present(dim, pos, con, ext, key, name, Some(Numerical),
      (l: Double, r: Double) => l - r, inverse)
  }
}

/** Companion object to the `Subtract` class defining constructors. */
object Subtract {
  /**
   * Subtract a value.
   *
   * @param dim Dimension for which to subtract.
   */
  def apply(dim: Dimension): Subtract = {
    Subtract(dim, None, None, false)
  }

  /**
   * Subtract a value.
   *
   * @param dim  Dimension for which to subtract.
   * @param name Pattern for the new name of the coordinate at `dim`. Use
   *             `%[12]$``s` for the string representations of the coordinate,
   *             and the content.
   */
  def apply(dim: Dimension, name: String): Subtract = {
    Subtract(dim, None, Some(name), false)
  }

  /**
   * Subtract a value.
   *
   * @param dim Dimension for which to subtract.
   * @param key Key into the map `V` identifying the value to subtract.
   */
  def apply[T](dim: Dimension, key: T)(
    implicit ev: Positionable[T, Position1D]): Subtract = {
    Subtract(dim, Some(ev.convert(key)), None, false)
  }

  /**
   * Subtract a value.
   *
   * @param dim     Dimension for which to subtract.
   * @param inverse Indicator specifying order of subtract.
   */
  def apply(dim: Dimension, inverse: Boolean): Subtract = {
    Subtract(dim, None, None, inverse)
  }

  /**
   * Subtract a value.
   *
   * @param dim  Dimension for which to subtract.
   * @param key  Key into the map `V` identifying the value to subtract.
   * @param name Pattern for the new name of the coordinate at `dim`. Use
   *             `%[12]$``s` for the string representations of the coordinate,
   *             and the content.
   */
  def apply[T](dim: Dimension, key: T, name: String)(
    implicit ev: Positionable[T, Position1D]): Subtract = {
    Subtract(dim, Some(ev.convert(key)), Some(name), false)
  }

  /**
   * Subtract a value.
   *
   * @param dim     Dimension for which to subtract.
   * @param key     Key into the map `V` identifying the value to subtract.
   * @param inverse Indicator specifying order of subtract.
   */
  def apply[T](dim: Dimension, key: T, inverse: Boolean)(
    implicit ev: Positionable[T, Position1D]): Subtract = {
    Subtract(dim, Some(ev.convert(key)), None, inverse)
  }

  /**
   * Subtract a value.
   *
   * @param dim     Dimension for which to subtract.
   * @param name    Pattern for the new name of the coordinate at `dim`. Use
   *                `%[12]$``s` for the string representations of the
   *                coordinate, and the content.
   * @param inverse Indicator specifying order of subtract.
   */
  def apply(dim: Dimension, name: String, inverse: Boolean): Subtract = {
    Subtract(dim, None, Some(name), inverse)
  }

  /**
   * Subtract a value.
   *
   * @param dim     Dimension for which to subtract.
   * @param key     Key into the map `V` identifying the value to subtract.
   * @param name    Pattern for the new name of the coordinate at `dim`. Use
   *                `%[12]$``s` for the string representations of the
   *                coordinate, and the content.
   * @param inverse Indicator specifying order of subtract.
   */
  def apply[T](dim: Dimension, key: T, name: String, inverse: Boolean)(
    implicit ev: Positionable[T, Position1D]): Subtract = {
    Subtract(dim, Some(ev.convert(key)), Some(name), inverse)
  }
}

/**
 * Raise value to a power.
 *
 * @param dim   Dimension for which to raise to a power.
 * @param power The power to raise to.
 * @param name  Optional pattern for the new name of the coordinate at `dim`.
 *              Use `%[12]$``s` for the string representations of the
 *              coordinate, and the content.
 *
 * @note Power is only applied to numerical variables.
 */
case class Power private (dim: Dimension, power: Double, name: Option[String])
  extends Transformer with Present with PresentDouble {
  def present[P <: Position with ModifyablePosition](pos: P, con: Content) = {
    present(dim, pos, con, name, Some(Numerical), math.pow(_, power))
  }
}

/** Companion object to the `Power` class defining constructors. */
object Power {
  /**
   * Raise value to a power.
   *
   * @param dim   Dimension for which to raise to a power.
   * @param power The power to raise to.
   */
  def apply(dim: Dimension, power: Double): Power = {
    Power(dim, power, None)
  }

  /**
   * Raise value to a power.
   *
   * @param dim   Dimension for which to raise to a power.
   * @param power The power to raise to.
   * @param name  Pattern for the new name of the coordinate at `dim`. Use
   *              `%[12]$``s` for the string representations of the coordinate,
   *              and the content.
   */
  def apply(dim: Dimension, power: Double, name: String): Power = {
    Power(dim, power, Some(name))
  }
}

/**
 * Take square root of a value.
 *
 * @param dim  Dimension for which to take the square root.
 * @param name Optional pattern for the new name of the coordinate at `dim`.
 *             Use `%[12]$``s` for the string representations of the
 *             coordinate, and the content.
 *
 * @note SquareRoot is only applied to numerical variables.
 */
case class SquareRoot private (dim: Dimension, name: Option[String])
  extends Transformer with Present with PresentDouble {
  def present[P <: Position with ModifyablePosition](pos: P, con: Content) = {
    present(dim, pos, con, name, Some(Numerical), math.sqrt(_))
  }
}

/** Companion object to the `SquareRoot` class defining constructors. */
object SquareRoot {
  /**
   * Take square root of a value.
   *
   * @param dim Dimension for which to take the square root.
   */
  def apply(dim: Dimension): SquareRoot = {
    SquareRoot(dim, None)
  }

  /**
   * Take square root of a value.
   *
   * @param dim  Dimension for which to take the square root.
   * @param name Pattern for the new name of the coordinate at `dim`. Use
   *             `%[12]$``s` for the string representations of the coordinate,
   *             and the content.
   */
  def apply(dim: Dimension, name: String): SquareRoot = {
    SquareRoot(dim, Some(name))
  }
}

/**
 * Divide a value.
 *
 * @param dim     Dimension for which to divide.
 * @param key     Optional key into the map `V` identifying the value to
 *                divide by.
 * @param name    Optional pattern for the new name of the coordinate at `dim`.
 *                Use `%[12]$``s` for the string representations of the
 *                coordinate, and the content.
 * @param inverse Indicator specifying order of division.
 *
 * @note Divide is only applied to numerical variables.
 */
case class Divide private (dim: Dimension, key: Option[Position1D],
  name: Option[String], inverse: Boolean) extends Transformer
  with PresentWithValue with PresentDouble {
  type V = Map[Position1D, Content]

  def present[P <: Position with ModifyablePosition](pos: P, con: Content,
    ext: V) = {
    present(dim, pos, con, ext, key, name, Some(Numerical),
      (l: Double, r: Double) => l / r, inverse)
  }
}

/** Companion object to the `Divide` class defining constructors. */
object Divide {
  /**
   * Divide a value.
   *
   * @param dim Dimension for which to divide.
   */
  def apply(dim: Dimension): Divide = {
    Divide(dim, None, None, false)
  }

  /**
   * Divide a value.
   *
   * @param dim  Dimension for which to divide.
   * @param name Pattern for the new name of the coordinate at `dim`. Use
   *             `%[12]$``s` for the string representations of the
   *             coordinate, and the content.
   */
  def apply(dim: Dimension, name: String): Divide = {
    Divide(dim, None, Some(name), false)
  }

  /**
   * Divide a value.
   *
   * @param dim Dimension for which to divide.
   * @param key Key into the map `V` identifying the value to divide by.
   */
  def apply[T](dim: Dimension, key: T)(
    implicit ev: Positionable[T, Position1D]): Divide = {
    Divide(dim, Some(ev.convert(key)), None, false)
  }

  /**
   * Divide a value.
   *
   * @param dim     Dimension for which to divide.
   * @param inverse Indicator specifying order of division.
   */
  def apply(dim: Dimension, inverse: Boolean): Divide = {
    Divide(dim, None, None, inverse)
  }

  /**
   * Divide a value.
   *
   * @param dim  Dimension for which to divide.
   * @param key  Key into the map `V` identifying the value to divide by.
   * @param name Pattern for the new name of the coordinate at `dim`. Use
   *             `%[12]$``s` for the string representations of the
   *             coordinate, and the content.
   */
  def apply[T](dim: Dimension, key: T, name: String)(
    implicit ev: Positionable[T, Position1D]): Divide = {
    Divide(dim, Some(ev.convert(key)), Some(name), false)
  }

  /**
   * Divide a value.
   *
   * @param dim     Dimension for which to divide.
   * @param key     Key into the map `V` identifying the value to divide by.
   * @param inverse Indicator specifying order of division.
   */
  def apply[T](dim: Dimension, key: T, inverse: Boolean)(
    implicit ev: Positionable[T, Position1D]): Divide = {
    Divide(dim, Some(ev.convert(key)), None, inverse)
  }

  /**
   * Divide a value.
   *
   * @param dim     Dimension for which to divide.
   * @param name    Pattern for the new name of the coordinate at `dim`. Use
   *                `%[12]$``s` for the string representations of the
   *                coordinate, and the content.
   * @param inverse Indicator specifying order of division.
   */
  def apply(dim: Dimension, name: String, inverse: Boolean): Divide = {
    Divide(dim, None, Some(name), inverse)
  }

  /**
   * Divide a value.
   *
   * @param dim     Dimension for which to divide.
   * @param key     Key into the map `V` identifying the value to divide by.
   * @param name    Pattern for the new name of the coordinate at `dim`. Use
   *                `%[12]$``s` for the string representations of the
   *                coordinate, and the content.
   * @param inverse Indicator specifying order of division.
   */
  def apply[T](dim: Dimension, key: T, name: String, inverse: Boolean)(
    implicit ev: Positionable[T, Position1D]): Divide = {
    Divide(dim, Some(ev.convert(key)), Some(name), inverse)
  }
}

