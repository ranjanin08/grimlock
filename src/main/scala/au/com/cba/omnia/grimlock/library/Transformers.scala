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

package au.com.cba.omnia.grimlock.library.transform

import au.com.cba.omnia.grimlock.framework._
import au.com.cba.omnia.grimlock.framework.content._
import au.com.cba.omnia.grimlock.framework.content.metadata._
import au.com.cba.omnia.grimlock.framework.encoding._
import au.com.cba.omnia.grimlock.framework.position._
import au.com.cba.omnia.grimlock.framework.transform._
import au.com.cba.omnia.grimlock.framework.Type._
import au.com.cba.omnia.grimlock.framework.utility._

/** Convenience trait defining common functionality patterns for implementing transformers. */
trait PresentCell[T] {
  protected def present[P <: Position](dim: Dimension, cell: Cell[P], name: Option[String], typ: Option[Type],
    value: T): Collection[Cell[P]] = {
    if (checkContent(cell.content, typ)) { getCell(dim, cell, name, value) } else { Collection[Cell[P]]() }
  }

  protected def present[P <: Position](dim: Dimension, cell: Cell[P], name: Option[String], typ: Option[Type],
    f: (T) => T): Collection[Cell[P]] = {
    (checkContent(cell.content, typ), getValue(cell.content)) match {
      case (true, Some(v)) => getCell(dim, cell, name, f(v))
      case _ => Collection[Cell[P]]()
    }
  }

  protected def present[P <: Position](dim: Dimension, cell: Cell[P], ext: Map[Position1D, Content],
    key: Option[Position1D], name: Option[String], typ: Option[Type], f: (T, T) => T,
    inverse: Boolean): Collection[Cell[P]] = {
    present(dim, cell, name, typ, f, inverse, getValue(getKey(dim, cell.position, key), ext))
  }

  protected def present[P <: Position](dim: Dimension, cell: Cell[P], ext: Map[Position1D, Map[Position1D, Content]],
    key: Position1D, name: Option[String], typ: Option[Type], f: (T, T) => T, inverse: Boolean): Collection[Cell[P]] = {
    present(dim, cell, name, typ, f, inverse, getValue(getKey(dim, cell.position, None), key, ext))
  }

  protected def present[P <: Position](dim: Dimension, cell: Cell[P], ext: Map[Position1D, Map[Position1D, Content]],
    key1: Position1D, key2: Position1D, name: Option[String], typ: Option[Type],
    f: (T, T, T) => T): Collection[Cell[P]] = {
    (checkContent(cell.content, typ), getValue(cell.content), getValue(getKey(dim, cell.position, None), key1, ext),
      getValue(getKey(dim, cell.position, None), key2, ext)) match {
        case (true, Some(v), Some(x), Some(y)) => getCell(dim, cell, name, f(v, x, y))
        case _ => Collection[Cell[P]]()
      }
  }

  protected def getContent(value: T): Content
  protected def getValue(con: Content): Option[T]

  private def checkContent(con: Content, typ: Option[Type]): Boolean = {
    typ.map { t => con.schema.kind.isSpecialisationOf(t) }.getOrElse(true)
  }

  private def getValue(key: Position1D, ext: Map[Position1D, Content]): Option[T] = ext.get(key).flatMap(getValue(_))

  private def getValue(key1: Position1D, key2: Position1D,
    ext: Map[Position1D, Map[Position1D, Content]]): Option[T] = ext.get(key1).flatMap(_.get(key2).flatMap(getValue(_)))

  private def getName[P <: Position](dim: Dimension, cell: Cell[P], name: Option[String]): P = {
    val pos = cell.position
    val con = cell.content

    name match {
      case Some(n) => pos.update(dim, n.format(pos(dim).toShortString, con.value.toShortString))
      case None => pos
    }
  }

  private def getKey[P <: Position](dim: Dimension, pos: P, key: Option[Position1D]): Position1D = {
    key.getOrElse(Position1D(pos(dim)))
  }

  private def getCell[P <: Position](dim: Dimension, cell: Cell[P], name: Option[String],
    value: T): Collection[Cell[P]] = Collection(getName(dim, cell, name), getContent(value))

  private def present[P <: Position](dim: Dimension, cell: Cell[P], name: Option[String], typ: Option[Type],
    f: (T, T) => T, inverse: Boolean, value: Option[T]): Collection[Cell[P]] = {
    (checkContent(cell.content, typ), getValue(cell.content), value) match {
      case (true, Some(l), Some(r)) => getCell(dim, cell, name, if (inverse) f(r, l) else f(l, r))
      case _ => Collection[Cell[P]]()
    }
  }
}

/** Convenience trait defining common functionality patterns for implementing transformers returning `Long` values. */
trait PresentLong extends PresentCell[Long] {
  protected def getContent(value: Long): Content = Content(DiscreteSchema[Codex.LongCodex](), value)
  protected def getValue(con: Content): Option[Long] = con.value.asLong
}

/** Convenience trait defining common functionality patterns for implementing transformers returning `Double` values. */
trait PresentDouble extends PresentCell[Double] {
  protected def getContent(value: Double): Content = Content(ContinuousSchema[Codex.DoubleCodex](), value)
  protected def getValue(con: Content): Option[Double] = con.value.asDouble
}

/**
 * Create indicator variables.
 *
 * @param dim  Dimension for which to create indicator variables.
 * @param name Optional pattern for the new name of the coordinate at `dim`. Use `%[12]$``s` for the string
 *             representations of the coordinate, and the content.
 */
case class Indicator private (dim: Dimension, name: Option[String]) extends Transformer with Present with PresentLong {
  def present[P <: Position](cell: Cell[P]): Collection[Cell[P]] = present(dim, cell, name, None, 1)
}

/** Companion object to the `Indicator` class defining constructors. */
object Indicator {
  /**
   * Create indicator variables.
   *
   * @param dim Dimension for which to create indicator variables.
   */
  def apply(dim: Dimension): Indicator = Indicator(dim, None)

  /**
   * Create indicator variables.
   *
   * @param dim  Dimension for which to create indicator variables.
   * @param name Pattern for the new name of the coordinate at `dim`. Use `%[12]$``s` for the string representations of
   *             the coordinate, and the content.
   */
  def apply(dim: Dimension, name: String): Indicator = Indicator(dim, Some(name))
}

/**
 * Binarise categorical variables.
 *
 * @param dim  Dimension for for which to create binarised variables.
 * @param name Pattern for the new name of the coordinate at `dim`. Use `%[12]$``s` for the string representations of
 *             the coordinate, and the content.
 *
 * @note Binarisation is only applied to categorical variables.
 */
case class Binarise(dim: Dimension, name: String = "%1$s=%2$s") extends Transformer with Present with PresentLong {
  def present[P <: Position](cell: Cell[P]): Collection[Cell[P]] = present(dim, cell, Some(name), Some(Categorical), 1)
}

/**
 * Normalise numeric variables.
 *
 * @param dim  Dimension for for which to create normalised variables.
 * @param key  Key into the inner map of `V`, identifying the normalisation constant.
 * @param name Optional pattern for the new name of the coordinate at `dim`. Use `%[12]$``s` for the string
 *             representations of the coordinate, and the content.
 *
 * @note Normalisation scales a variable in the range [-1, 1]. It is only applied to numerical variables.
 */
case class Normalise private (dim: Dimension, key: Position1D, name: Option[String]) extends Transformer
  with PresentWithValue with PresentDouble {
  type V = Map[Position1D, Map[Position1D, Content]]

  def present[P <: Position](cell: Cell[P], ext: V): Collection[Cell[P]] = {
    present(dim, cell, ext, key, name, Some(Numerical), (value: Double, const: Double) => value / const, false)
  }
}

/** Companion object to the `Normalise` class defining constructors. */
object Normalise {
  /**
   * Normalise numeric variables.
   *
   * @param dim Dimension for for which to create normalised variables.
   * @param key Key into the inner map of `V`, identifying the normalisation constant.
   */
  def apply[T](dim: Dimension, key: T)(implicit ev: Positionable[T, Position1D]): Normalise = {
    Normalise(dim, ev.convert(key), None)
  }

  /**
   * Normalise numeric variables.
   *
   * @param dim  Dimension for for which to create normalised variables.
   * @param key  Key into the inner map of `V`, identifying the normalisation constant.
   * @param name Pattern for the new name of the coordinate at `dim`. Use `%[12]$``s` for the string representations of
   *             the coordinate, and the content.
   */
  def apply[T](dim: Dimension, key: T, name: String)(implicit ev: Positionable[T, Position1D]): Normalise = {
    Normalise(dim, ev.convert(key), Some(name))
  }
}

/**
 * Standardise numeric variables.
 *
 * @param dim       Dimension for for which to create standardised variables.
 * @param mean      Key into the inner map of `V`, identifying the standardisation constant for the mean.
 * @param sd        Key into the inner map of `V`, identifying the standardisation constant for the standard deviation.
 * @param name      Optional pattern for the new name of the coordinate at `dim`. Use `%[12]$``s` for the string
 *                  representations of the coordinate, and the content.
 * @param threshold Minimum standard deviation threshold. Values less than this result in standardised value of zero.
 * @param n         Number of times division by standard deviation.
 *
 * @note Standardisation results in a variable with zero mean and variance of one. It is only applied to numerical
 *       variables.
 */
case class Standardise private (dim: Dimension, mean: Position1D, sd: Position1D, name: Option[String],
  threshold: Double, n: Int) extends Transformer with PresentWithValue with PresentDouble {
  type V = Map[Position1D, Map[Position1D, Content]]

  def present[P <: Position](cell: Cell[P], ext: V): Collection[Cell[P]] = {
    present(dim, cell, ext, mean, sd, name, Some(Numerical), standardise _)
  }

  private def standardise(v: Double, m: Double, s: Double) = if (math.abs(s) < threshold) 0.0 else (v - m) / (n * s)
}

/** Companion object to the `Standardise` class defining constructors. */
object Standardise {
  /**
   * Standardise numeric variables.
   *
   * @param dim  Dimension for for which to create standardised variables.
   * @param mean Key into the inner map of `V`, identifying the standardisation constant for the mean.
   * @param sd   Key into the inner map of `V`, identifying the standardisation constant for the standard deviation.
   */
  def apply[T, U](dim: Dimension, mean: T, sd: U)(implicit ev1: Positionable[T, Position1D],
    ev2: Positionable[U, Position1D]): Standardise = {
    Standardise(dim, ev1.convert(mean), ev2.convert(sd), None, DefaultThreshold, DefaultN)
  }

  /**
   * Standardise numeric variables.
   *
   * @param dim  Dimension for for which to create standardised variables.
   * @param mean Key into the inner map of `V`, identifying the standardisation constant for the mean.
   * @param sd   Key into the inner map of `V`, identifying the standardisation constant for the standard deviation.
   * @param name Pattern for the new name of the coordinate at `dim`. Use `%[12]$``s` for the string representations of
   *             the coordinate, and the content.
   */
  def apply[T, U](dim: Dimension, mean: T, sd: U, name: String)(implicit ev1: Positionable[T, Position1D],
    ev2: Positionable[U, Position1D]): Standardise = {
    Standardise(dim, ev1.convert(mean), ev2.convert(sd), Some(name), DefaultThreshold, DefaultN)
  }

  /**
   * Standardise numeric variables.
   *
   * @param dim       Dimension for for which to create standardised variables.
   * @param mean      Key into the inner map of `V`, identifying the standardisation constant for the mean.
   * @param sd        Key into the inner map of `V`, identifying the standardisation constant for the standard
   *                  deviation.
   * @param threshold Minimum standard deviation threshold. Values less than this result in standardised value of zero.
   */
  def apply[T, U](dim: Dimension, mean: T, sd: U, threshold: Double)(implicit ev1: Positionable[T, Position1D],
    ev2: Positionable[U, Position1D]): Standardise = {
    Standardise(dim, ev1.convert(mean), ev2.convert(sd), None, threshold, DefaultN)
  }

  /**
   * Standardise numeric variables.
   *
   * @param dim       Dimension for for which to create standardised variables.
   * @param mean      Key into the inner map of `V`, identifying the standardisation constant for the mean.
   * @param sd        Key into the inner map of `V`, identifying the standardisation constant for the standard
   *                  deviation.
   * @param n         Number of times division by standard deviation.
   */
  def apply[T, U](dim: Dimension, mean: T, sd: U, n: Int)(implicit ev1: Positionable[T, Position1D],
    ev2: Positionable[U, Position1D]): Standardise = {
    Standardise(dim, ev1.convert(mean), ev2.convert(sd), None, DefaultThreshold, n)
  }

  /**
   * Standardise numeric variables.
   *
   * @param dim       Dimension for for which to create standardised variables.
   * @param mean      Key into the inner map of `V`, identifying the standardisation constant for the mean.
   * @param sd        Key into the inner map of `V`, identifying the standardisation constant for the standard
   *                  deviation.
   * @param name      Pattern for the new name of the coordinate at `dim`. Use `%[12]$``s` for the string
   *                  representations of the coordinate, and the content.
   * @param threshold Minimum standard deviation threshold. Values less than this result in standardised value of zero.
   */
  def apply[T, U](dim: Dimension, mean: T, sd: U, name: String, threshold: Double)(
    implicit ev1: Positionable[T, Position1D], ev2: Positionable[U, Position1D]): Standardise = {
    Standardise(dim, ev1.convert(mean), ev2.convert(sd), Some(name), threshold, DefaultN)
  }

  /**
   * Standardise numeric variables.
   *
   * @param dim       Dimension for for which to create standardised variables.
   * @param mean      Key into the inner map of `V`, identifying the standardisation constant for the mean.
   * @param sd        Key into the inner map of `V`, identifying the standardisation constant for the standard
   *                  deviation.
   * @param name      Pattern for the new name of the coordinate at `dim`. Use `%[12]$``s` for the string
   *                  representations of the coordinate, and the content.
   * @param n         Number of times division by standard deviation.
   */
  def apply[T, U](dim: Dimension, mean: T, sd: U, name: String, n: Int)(implicit ev1: Positionable[T, Position1D],
    ev2: Positionable[U, Position1D]): Standardise = {
    Standardise(dim, ev1.convert(mean), ev2.convert(sd), Some(name), DefaultThreshold, n)
  }

  /**
   * Standardise numeric variables.
   *
   * @param dim       Dimension for for which to create standardised variables.
   * @param mean      Key into the inner map of `V`, identifying the standardisation constant for the mean.
   * @param sd        Key into the inner map of `V`, identifying the standardisation constant for the standard
   *                  deviation.
   * @param threshold Minimum standard deviation threshold. Values less than this result in standardised value of zero.
   * @param n         Number of times division by standard deviation.
   */
  def apply[T, U](dim: Dimension, mean: T, sd: U, threshold: Double, n: Int)(implicit ev1: Positionable[T, Position1D],
    ev2: Positionable[U, Position1D]): Standardise = {
    Standardise(dim, ev1.convert(mean), ev2.convert(sd), None, threshold, n)
  }

  /**
   * Standardise numeric variables.
   *
   * @param dim       Dimension for for which to create standardised variables.
   * @param mean      Key into the inner map of `V`, identifying the standardisation constant for the mean.
   * @param sd        Key into the inner map of `V`, identifying the standardisation constant for the standard
   *                  deviation.
   * @param name      Pattern for the new name of the coordinate at `dim`. Use `%[12]$``s` for the string
   *                  representations of the coordinate, and the content.
   * @param threshold Minimum standard deviation threshold. Values less than this result in standardised value of zero.
   * @param n         Number of times division by standard deviation.
   */
  def apply[T, U](dim: Dimension, mean: T, sd: U, name: String, threshold: Double, n: Int)(
    implicit ev1: Positionable[T, Position1D], ev2: Positionable[U, Position1D]): Standardise = {
    Standardise(dim, ev1.convert(mean), ev2.convert(sd), Some(name), threshold, n)
  }

  private val DefaultThreshold = 1e-4
  private val DefaultN = 1
}

/**
 * Clamp numeric variables.
 *
 * @param dim   Dimension for for which to create clamped variables.
 * @param lower Key into the inner map of `V`, identifying the lower clamping constant for the mean.
 * @param upper Key into the inner map of `V`, identifying the upper clamping constant for the standard deviation.
 * @param name  Optional pattern for the new name of the coordinate at `dim`. Use `%[12]$``s` for the string
 *              representations of the coordinate, and the content.
 *
 * @note Clamping results in a variable not smaller (or greater) than the clamping constants. It is only applied to
 *       numerical variables.
 */
case class Clamp private (dim: Dimension, lower: Position1D, upper: Position1D, name: Option[String])
  extends Transformer with PresentWithValue with PresentDouble {
  type V = Map[Position1D, Map[Position1D, Content]]

  def present[P <: Position](cell: Cell[P], ext: V): Collection[Cell[P]] = {
    present(dim, cell, ext, lower, upper, name, Some(Numerical), clamp _)
  }

  private def clamp(v: Double, l: Double, u: Double) = if (v < l) l else if (v > u) u else v
}

/** Companion object to the `Clamp` class defining constructors. */
object Clamp {
  /**
   * Clamp numeric variables.
   *
   * @param dim   Dimension for for which to create clamped variables.
   * @param lower Key into the inner map of `V`, identifying the lower clamping constant for the mean.
   * @param upper Key into the inner map of `V`, identifying the upper clamping constant for the standard deviation.
   */
  def apply[T, U](dim: Dimension, lower: T, upper: U)(implicit ev1: Positionable[T, Position1D],
    ev2: Positionable[U, Position1D]): Clamp = Clamp(dim, ev1.convert(lower), ev2.convert(upper), None)

  /**
   * Clamp numeric variables.
   *
   * @param dim   Dimension for for which to create clamped variables.
   * @param lower Key into the inner map of `V`, identifying the lower clamping constant for the mean.
   * @param upper Key into the inner map of `V`, identifying the upper clamping constant for the standard deviation.
   * @param name  Pattern for the new name of the coordinate at `dim`. Use `%[12]$``s` for the string representations
   *              of the coordinate, and the content.
   */
  def apply[T, U](dim: Dimension, lower: T, upper: U, name: String)(implicit ev1: Positionable[T, Position1D],
    ev2: Positionable[U, Position1D]): Clamp = Clamp(dim, ev1.convert(lower), ev2.convert(upper), Some(name))
}

/**
 * Compute the inverse document frequency.
 *
 * @param dim  Dimension for which to create inverse document frequencies.
 * @param key  Optional key into the map `V` identifying the number of documents.
 * @param name Optional pattern for the new name of the coordinate at `dim`. Use `%[12]$``s` for the string
 *             representations of the coordinate, and the content.
 * @param idf  Idf function to use.
 *
 * @note Idf is only applied to numerical variables.
 */
case class Idf private (dim: Dimension, key: Option[Position1D], name: Option[String], idf: (Double, Double) => Double)
  extends Transformer with PresentWithValue with PresentDouble {
  type V = Map[Position1D, Content]

  def present[P <: Position](cell: Cell[P], ext: V): Collection[Cell[P]] = {
    present(dim, cell, ext, key, name, Some(Numerical), (df: Double, n: Double) => idf(df, n), false)
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
  def Transform(log: (Double) => Double, add: Int): (Double, Double) => Double = {
    (df: Double, n: Double) => log(n / (add + df))
  }

  /**
   * Compute the inverse document frequency.
   *
   * @param dim Dimension for which to create inverse document frequencies.
   */
  def apply(dim: Dimension): Idf = Idf(dim, None, None, DefaultTransform)

  /**
   * Compute the inverse document frequency.
   *
   * @param dim Dimension for which to create inverse document frequencies.
   * @param key Key into the map `V` identifying the number of documents.
   */
  def apply[T](dim: Dimension, key: T)(implicit ev: Positionable[T, Position1D]): Idf = {
    Idf(dim, Some(ev.convert(key)), None, DefaultTransform)
  }

  /**
   * Compute the inverse document frequency.
   *
   * @param dim  Dimension for which to create inverse document frequencies.
   * @param name Pattern for the new name of the coordinate at `dim`. Use `%[12]$``s` for the string representations of
   *             the coordinate, and the content.
   */
  def apply(dim: Dimension, name: String): Idf = Idf(dim, None, Some(name), DefaultTransform)

  /**
   * Compute the inverse document frequency.
   *
   * @param dim Dimension for which to create inverse document frequencies.
   * @param idf Idf function to use.
   */
  def apply(dim: Dimension, idf: (Double, Double) => Double): Idf = Idf(dim, None, None, idf)

  /**
   * Compute the inverse document frequency.
   *
   * @param dim  Dimension for which to create inverse document frequencies.
   * @param key  Key into the map `V` identifying the number of documents.
   * @param name Pattern for the new name of the coordinate at `dim`. Use `%[12]$``s` for the string representations of
   *             the coordinate, and the content.
   */
  def apply[T](dim: Dimension, key: T, name: String)(implicit ev: Positionable[T, Position1D]): Idf = {
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
    implicit ev: Positionable[T, Position1D]): Idf = Idf(dim, Some(ev.convert(key)), None, idf)

  /**
   * Compute the inverse document frequency.
   *
   * @param dim  Dimension for which to create inverse document frequencies.
   * @param name Pattern for the new name of the coordinate at `dim`. Use `%[12]$``s` for the string representations of
   *             the coordinate, and the content.
   * @param idf  Idf function to use.
   */
  def apply(dim: Dimension, name: String, idf: (Double, Double) => Double): Idf = Idf(dim, None, Some(name), idf)

  /**
   * Compute the inverse document frequency.
   *
   * @param dim  Dimension for which to create inverse document frequencies.
   * @param key  Key into the map `V` identifying the number of documents.
   * @param name Pattern for the new name of the coordinate at `dim`. Use `%[12]$``s` for the string representations of
   *             the coordinate, and the content.
   * @param idf  Idf function to use.
   */
  def apply[T](dim: Dimension, key: T, name: String, idf: (Double, Double) => Double)(
    implicit ev: Positionable[T, Position1D]): Idf = Idf(dim, Some(ev.convert(key)), Some(name), idf)

  protected def DefaultTransform() = Transform(math.log, 1)
}

/**
 * Create boolean term frequencies; all term frequencies are binarised.
 *
 * @param dim  Dimension for which to create boolean term frequencies.
 * @param name Optional pattern for the new name of the coordinate at `dim`. Use `%[12]$``s` for the string
 *             representations of the coordinate, and the content.
 *
 * @note Boolean tf is only applied to numerical variables.
 */
case class BooleanTf private (dim: Dimension, name: Option[String]) extends Transformer with Present
  with PresentDouble {
  def present[P <: Position](cell: Cell[P]): Collection[Cell[P]] = present(dim, cell, name, Some(Numerical), 1)
}

/** Companion object to the `BooleanTf` class defining constructors. */
object BooleanTf {
  /**
   * Create boolean term frequencies; all term frequencies are binarised.
   *
   * @param dim Dimension for which to create boolean term frequencies.
   */
  def apply(dim: Dimension): BooleanTf = BooleanTf(dim, None)

  /**
   * Create boolean term frequencies; all term frequencies are binarised.
   *
   * @param dim  Dimension for which to create boolean term frequencies.
   * @param name Pattern for the new name of the coordinate at `dim`. Use `%[12]$``s` for the string representations of
   *             the coordinate, and the content.
   */
  def apply(dim: Dimension, name: String): BooleanTf = BooleanTf(dim, Some(name))
}

/**
 * Create logarithmic term frequencies.
 *
 * @param dim  Dimension for which to create boolean term frequencies.
 * @param name Optional pattern for the new name of the coordinate at `dim`. Use `%[12]$``s` for the string
 *             representations of the coordinate, and the content.
 * @param log  Log function to use.
 *
 * @note Logarithmic tf is only applied to numerical variables.
 */
case class LogarithmicTf private (dim: Dimension, name: Option[String], log: (Double) => Double) extends Transformer
  with Present with PresentDouble {
  def present[P <: Position](cell: Cell[P]): Collection[Cell[P]] = {
    present(dim, cell, name, Some(Numerical), (tf: Double) => 1 + log(tf))
  }
}

/** Companion object to the `LogarithmicTf` class defining constructors. */
object LogarithmicTf {
  /**
   * Create logarithmic term frequencies.
   *
   * @param dim Dimension for which to create boolean term frequencies.
   */
  def apply(dim: Dimension): LogarithmicTf = LogarithmicTf(dim, None, DefaultLog)

  /**
   * Create logarithmic term frequencies.
   *
   * @param dim  Dimension for which to create boolean term frequencies.
   * @param name Pattern for the new name of the coordinate at `dim`. Use `%[12]$``s` for the string representations of
   *             the coordinate, and the content.
   */
  def apply(dim: Dimension, name: String): LogarithmicTf = LogarithmicTf(dim, Some(name), DefaultLog)

  /**
   * Create logarithmic term frequencies.
   *
   * @param dim Dimension for which to create boolean term frequencies.
   * @param log Log function to use.
   */
  def apply(dim: Dimension, log: (Double) => Double): LogarithmicTf = LogarithmicTf(dim, None, log)

  /**
   * Create logarithmic term frequencies.
   *
   * @param dim  Dimension for which to create boolean term frequencies.
   * @param name Pattern for the new name of the coordinate at `dim`. Use `%[12]$``s` for the string representations of
   *             the coordinate, and the content.
   * @param log  Log function to use.
   */
  def apply(dim: Dimension, name: String, log: (Double) => Double): LogarithmicTf = LogarithmicTf(dim, Some(name), log)

  protected def DefaultLog(): (Double) => Double = math.log
}

/**
 * Create augmented term frequencies.
 *
 * @param dim  Dimension for which to create boolean term frequencies.
 * @param name Optional pattern for the new name of the coordinate at `dim`. Use `%[12]$``s` for the string
 *             representations of the coordinate, and the content.
 *
 * @note Augmented tf is only applied to numerical variables.
 */
case class AugmentedTf private (dim: Dimension, name: Option[String]) extends Transformer with PresentWithValue
  with PresentDouble {
  type V = Map[Position1D, Content]

  def present[P <: Position](cell: Cell[P], ext: V): Collection[Cell[P]] = {
    present(dim, cell, ext, None, name, Some(Numerical), (tf: Double, m: Double) => 0.5 + (0.5 * tf) / m, false)
  }
}

/** Companion object to the `AugmentedTf` class defining constructors. */
object AugmentedTf {
  /**
   * Create augmented term frequencies.
   *
   * @param dim Dimension for which to create boolean term frequencies.
   */
  def apply(dim: Dimension): AugmentedTf = AugmentedTf(dim, None)

  /**
   * Create augmented term frequencies.
   *
   * @param dim  Dimension for which to create boolean term frequencies.
   * @param name Optional pattern for the new name of the coordinate at `dim`. Use `%[12]$``s` for the string
   *             representations of the coordinate, and the content.
   */
  def apply(dim: Dimension, name: String): AugmentedTf = AugmentedTf(dim, Some(name))
}

/**
 * Create tf-idf values.
 *
 * @param dim  Dimension for which to create term frequencies.
 * @param key  Optional key into the map `V` identifying the inverse document frequency.
 * @param name Optional pattern for the new name of the coordinate at `dim`. Use `%[12]$``s` for the string
 *             representations of the coordinate, and the content.
 *
 * @note Tf-idf is only applied to numerical variables.
 */
case class TfIdf private (dim: Dimension, key: Option[Position1D], name: Option[String]) extends Transformer
  with PresentWithValue with PresentDouble {
  type V = Map[Position1D, Content]

  def present[P <: Position](cell: Cell[P], ext: V): Collection[Cell[P]] = {
    present(dim, cell, ext, key, name, Some(Numerical), (tf: Double, idf: Double) => tf * idf, false)
  }
}

/** Companion object to the `TfIdf` class defining constructors. */
object TfIdf {
  /**
   * Create tf-idf values.
   *
   * @param dim Dimension for which to create term frequencies.
   */
  def apply(dim: Dimension): TfIdf = TfIdf(dim, None, None)

  /**
   * Create tf-idf values.
   *
   * @param dim Dimension for which to create term frequencies.
   * @param key Key into the map `V` identifying the inverse document frequency.
   */
  def apply[T](dim: Dimension, key: T)(implicit ev: Positionable[T, Position1D]): TfIdf = {
    TfIdf(dim, Some(ev.convert(key)), None)
  }

  /**
   * Create tf-idf values.
   *
   * @param dim  Dimension for which to create term frequencies.
   * @param name Pattern for the new name of the coordinate at `dim`. Use `%[12]$``s` for the string representations of
   *             the coordinate, and the content.
   */
  def apply(dim: Dimension, name: String): TfIdf = TfIdf(dim, None, Some(name))

  /**
   * Create tf-idf values.
   *
   * @param dim  Dimension for which to create term frequencies.
   * @param key  Key into the map `V` identifying the inverse document frequency.
   * @param name Pattern for the new name of the coordinate at `dim`. Use `%[12]$``s` for the string representations of
   *             the coordinate, and the content.
   */
  def apply[T](dim: Dimension, key: T, name: String)(implicit ev: Positionable[T, Position1D]): TfIdf = {
    TfIdf(dim, Some(ev.convert(key)), Some(name))
  }
}

/**
 * Add a value.
 *
 * @param dim     Dimension for which to add.
 * @param key     Optional key into the map `V` identifying the value to add.
 * @param name    Optional pattern for the new name of the coordinate at `dim`. Use `%[12]$``s` for the string
 *                representations of the coordinate, and the content.
 *
 * @note Add is only applied to numerical variables.
 */
case class Add private (dim: Dimension, key: Option[Position1D], name: Option[String]) extends Transformer
  with PresentWithValue with PresentDouble {
  type V = Map[Position1D, Content]

  def present[P <: Position](cell: Cell[P], ext: V): Collection[Cell[P]] = {
    present(dim, cell, ext, key, name, Some(Numerical), (l: Double, r: Double) => l + r, false)
  }
}

/** Companion object to the `Add` class defining constructors. */
object Add {
  /**
   * Add a value.
   *
   * @param dim Dimension for which to add.
   */
  def apply(dim: Dimension): Add = Add(dim, None, None)

  /**
   * Add a value.
   *
   * @param dim  Dimension for which to add.
   * @param name Pattern for the new name of the coordinate at `dim`. Use `%[12]$``s` for the string representations of
   *             the coordinate, and the content.
   */
  def apply(dim: Dimension, name: String): Add = Add(dim, None, Some(name))

  /**
   * Add a value.
   *
   * @param dim Dimension for which to add.
   * @param key Key into the map `V` identifying the value to add.
   */
  def apply[T](dim: Dimension, key: T)(implicit ev: Positionable[T, Position1D]): Add = {
    Add(dim, Some(ev.convert(key)), None)
  }

  /**
   * Add a value.
   *
   * @param dim  Dimension for which to add.
   * @param key  Key into the map `V` identifying the value to add.
   * @param name Pattern for the new name of the coordinate at `dim`. Use `%[12]$``s` for the string representations of
   *             the coordinate, and the content.
   */
  def apply[T](dim: Dimension, key: T, name: String)(implicit ev: Positionable[T, Position1D]): Add = {
    Add(dim, Some(ev.convert(key)), Some(name))
  }
}

/**
 * Subtract a value.
 *
 * @param dim     Dimension for which to subtract.
 * @param key     Optional key into the map `V` identifying the value to subtract.
 * @param name    Optional pattern for the new name of the coordinate at `dim`. Use `%[12]$``s` for the string
 *                representations of the coordinate, and the content.
 * @param inverse Indicator specifying order of subtract.
 *
 * @note Subtract is only applied to numerical variables.
 */
case class Subtract private (dim: Dimension, key: Option[Position1D], name: Option[String], inverse: Boolean)
  extends Transformer with PresentWithValue with PresentDouble {
  type V = Map[Position1D, Content]

  def present[P <: Position](cell: Cell[P], ext: V): Collection[Cell[P]] = {
    present(dim, cell, ext, key, name, Some(Numerical), (l: Double, r: Double) => l - r, inverse)
  }
}

/** Companion object to the `Subtract` class defining constructors. */
object Subtract {
  /**
   * Subtract a value.
   *
   * @param dim Dimension for which to subtract.
   */
  def apply(dim: Dimension): Subtract = Subtract(dim, None, None, false)

  /**
   * Subtract a value.
   *
   * @param dim  Dimension for which to subtract.
   * @param name Pattern for the new name of the coordinate at `dim`. Use `%[12]$``s` for the string representations of
   *             the coordinate, and the content.
   */
  def apply(dim: Dimension, name: String): Subtract = Subtract(dim, None, Some(name), false)

  /**
   * Subtract a value.
   *
   * @param dim Dimension for which to subtract.
   * @param key Key into the map `V` identifying the value to subtract.
   */
  def apply[T](dim: Dimension, key: T)(implicit ev: Positionable[T, Position1D]): Subtract = {
    Subtract(dim, Some(ev.convert(key)), None, false)
  }

  /**
   * Subtract a value.
   *
   * @param dim     Dimension for which to subtract.
   * @param inverse Indicator specifying order of subtract.
   */
  def apply(dim: Dimension, inverse: Boolean): Subtract = Subtract(dim, None, None, inverse)

  /**
   * Subtract a value.
   *
   * @param dim  Dimension for which to subtract.
   * @param key  Key into the map `V` identifying the value to subtract.
   * @param name Pattern for the new name of the coordinate at `dim`. Use `%[12]$``s` for the string representations of
   *             the coordinate, and the content.
   */
  def apply[T](dim: Dimension, key: T, name: String)(implicit ev: Positionable[T, Position1D]): Subtract = {
    Subtract(dim, Some(ev.convert(key)), Some(name), false)
  }

  /**
   * Subtract a value.
   *
   * @param dim     Dimension for which to subtract.
   * @param key     Key into the map `V` identifying the value to subtract.
   * @param inverse Indicator specifying order of subtract.
   */
  def apply[T](dim: Dimension, key: T, inverse: Boolean)(implicit ev: Positionable[T, Position1D]): Subtract = {
    Subtract(dim, Some(ev.convert(key)), None, inverse)
  }

  /**
   * Subtract a value.
   *
   * @param dim     Dimension for which to subtract.
   * @param name    Pattern for the new name of the coordinate at `dim`. Use `%[12]$``s` for the string representations
   *                of the coordinate, and the content.
   * @param inverse Indicator specifying order of subtract.
   */
  def apply(dim: Dimension, name: String, inverse: Boolean): Subtract = Subtract(dim, None, Some(name), inverse)

  /**
   * Subtract a value.
   *
   * @param dim     Dimension for which to subtract.
   * @param key     Key into the map `V` identifying the value to subtract.
   * @param name    Pattern for the new name of the coordinate at `dim`. Use `%[12]$``s` for the string representations
   *                of the coordinate, and the content.
   * @param inverse Indicator specifying order of subtract.
   */
  def apply[T](dim: Dimension, key: T, name: String, inverse: Boolean)(
    implicit ev: Positionable[T, Position1D]): Subtract = Subtract(dim, Some(ev.convert(key)), Some(name), inverse)
}

/**
 * Multiply a value.
 *
 * @param dim     Dimension for which to multiply.
 * @param key     Optional key into the map `V` identifying the value to multiply by.
 * @param name    Optional pattern for the new name of the coordinate at `dim`. Use `%[12]$``s` for the string
 *                representations of the coordinate, and the content.
 *
 * @note Multiply is only applied to numerical variables.
 */
case class Multiply private (dim: Dimension, key: Option[Position1D], name: Option[String]) extends Transformer
  with PresentWithValue with PresentDouble {
  type V = Map[Position1D, Content]

  def present[P <: Position](cell: Cell[P], ext: V): Collection[Cell[P]] = {
    present(dim, cell, ext, key, name, Some(Numerical), (l: Double, r: Double) => l * r, false)
  }
}

/** Companion object to the `Multiply` class defining constructors. */
object Multiply {
  /**
   * Multiply a value.
   *
   * @param dim Dimension for which to multiply.
   */
  def apply(dim: Dimension): Multiply = Multiply(dim, None, None)

  /**
   * Multiply a value.
   *
   * @param dim  Dimension for which to multiply.
   * @param name Pattern for the new name of the coordinate at `dim`. Use `%[12]$``s` for the string representations of
   *             the coordinate, and the content.
   */
  def apply(dim: Dimension, name: String): Multiply = Multiply(dim, None, Some(name))

  /**
   * Multiply a value.
   *
   * @param dim Dimension for which to multiply.
   * @param key Key into the map `V` identifying the value to multiply by.
   */
  def apply[T](dim: Dimension, key: T)(implicit ev: Positionable[T, Position1D]): Multiply = {
    Multiply(dim, Some(ev.convert(key)), None)
  }

  /**
   * Multiply a value.
   *
   * @param dim  Dimension for which to multiply.
   * @param key  Key into the map `V` identifying the value to multiply by.
   * @param name Pattern for the new name of the coordinate at `dim`. Use `%[12]$``s` for the string representations of
   *             the coordinate, and the content.
   */
  def apply[T](dim: Dimension, key: T, name: String)(implicit ev: Positionable[T, Position1D]): Multiply = {
    Multiply(dim, Some(ev.convert(key)), Some(name))
  }
}

/**
 * Divide a value.
 *
 * @param dim     Dimension for which to divide.
 * @param key     Optional key into the map `V` identifying the value to divide by.
 * @param name    Optional pattern for the new name of the coordinate at `dim`. Use `%[12]$``s` for the string
 *                representations of the coordinate, and the content.
 * @param inverse Indicator specifying order of division.
 *
 * @note Fraction is only applied to numerical variables.
 */
case class Fraction private (dim: Dimension, key: Option[Position1D], name: Option[String], inverse: Boolean)
  extends Transformer with PresentWithValue with PresentDouble {
  type V = Map[Position1D, Content]

  def present[P <: Position](cell: Cell[P], ext: V): Collection[Cell[P]] = {
    present(dim, cell, ext, key, name, Some(Numerical), (l: Double, r: Double) => l / r, inverse)
  }
}

/** Companion object to the `Fraction` class defining constructors. */
object Fraction {
  /**
   * Divide a value.
   *
   * @param dim Dimension for which to divide.
   */
  def apply(dim: Dimension): Fraction = Fraction(dim, None, None, false)

  /**
   * Divide a value.
   *
   * @param dim  Dimension for which to divide.
   * @param name Pattern for the new name of the coordinate at `dim`. Use `%[12]$``s` for the string representations of
   *             the coordinate, and the content.
   */
  def apply(dim: Dimension, name: String): Fraction = Fraction(dim, None, Some(name), false)

  /**
   * Divide a value.
   *
   * @param dim Dimension for which to divide.
   * @param key Key into the map `V` identifying the value to divide by.
   */
  def apply[T](dim: Dimension, key: T)(implicit ev: Positionable[T, Position1D]): Fraction = {
    Fraction(dim, Some(ev.convert(key)), None, false)
  }

  /**
   * Divide a value.
   *
   * @param dim     Dimension for which to divide.
   * @param inverse Indicator specifying order of division.
   */
  def apply(dim: Dimension, inverse: Boolean): Fraction = Fraction(dim, None, None, inverse)

  /**
   * Divide a value.
   *
   * @param dim  Dimension for which to divide.
   * @param key  Key into the map `V` identifying the value to divide by.
   * @param name Pattern for the new name of the coordinate at `dim`. Use `%[12]$``s` for the string representations of
   *             the coordinate, and the content.
   */
  def apply[T](dim: Dimension, key: T, name: String)(implicit ev: Positionable[T, Position1D]): Fraction = {
    Fraction(dim, Some(ev.convert(key)), Some(name), false)
  }

  /**
   * Divide a value.
   *
   * @param dim     Dimension for which to divide.
   * @param key     Key into the map `V` identifying the value to divide by.
   * @param inverse Indicator specifying order of division.
   */
  def apply[T](dim: Dimension, key: T, inverse: Boolean)(implicit ev: Positionable[T, Position1D]): Fraction = {
    Fraction(dim, Some(ev.convert(key)), None, inverse)
  }

  /**
   * Divide a value.
   *
   * @param dim     Dimension for which to divide.
   * @param name    Pattern for the new name of the coordinate at `dim`. Use `%[12]$``s` for the string representations
   *                of the coordinate, and the content.
   * @param inverse Indicator specifying order of division.
   */
  def apply(dim: Dimension, name: String, inverse: Boolean): Fraction = Fraction(dim, None, Some(name), inverse)

  /**
   * Divide a value.
   *
   * @param dim     Dimension for which to divide.
   * @param key     Key into the map `V` identifying the value to divide by.
   * @param name    Pattern for the new name of the coordinate at `dim`. Use `%[12]$``s` for the string representations
   *                of the coordinate, and the content.
   * @param inverse Indicator specifying order of division.
   */
  def apply[T](dim: Dimension, key: T, name: String, inverse: Boolean)(
    implicit ev: Positionable[T, Position1D]): Fraction = Fraction(dim, Some(ev.convert(key)), Some(name), inverse)
}

/**
 * Raise value to a power.
 *
 * @param dim   Dimension for which to raise to a power.
 * @param power The power to raise to.
 * @param name  Optional pattern for the new name of the coordinate at `dim`. Use `%[12]$``s` for the string
 *              representations of the coordinate, and the content.
 *
 * @note Power is only applied to numerical variables.
 */
case class Power private (dim: Dimension, power: Double, name: Option[String]) extends Transformer with Present
  with PresentDouble {
  def present[P <: Position](cell: Cell[P]): Collection[Cell[P]] = {
    present(dim, cell, name, Some(Numerical), (d: Double) => math.pow(d, power))
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
  def apply(dim: Dimension, power: Double): Power = Power(dim, power, None)

  /**
   * Raise value to a power.
   *
   * @param dim   Dimension for which to raise to a power.
   * @param power The power to raise to.
   * @param name  Pattern for the new name of the coordinate at `dim`. Use `%[12]$``s` for the string representations
   *              of the coordinate, and the content.
   */
  def apply(dim: Dimension, power: Double, name: String): Power = Power(dim, power, Some(name))
}

/**
 * Take square root of a value.
 *
 * @param dim  Dimension for which to take the square root.
 * @param name Optional pattern for the new name of the coordinate at `dim`. Use `%[12]$``s` for the string
 *             representations of the coordinate, and the content.
 *
 * @note SquareRoot is only applied to numerical variables.
 */
case class SquareRoot private (dim: Dimension, name: Option[String]) extends Transformer with Present
  with PresentDouble {
  def present[P <: Position](cell: Cell[P]): Collection[Cell[P]] = {
    present(dim, cell, name, Some(Numerical), math.sqrt(_))
  }
}

/** Companion object to the `SquareRoot` class defining constructors. */
object SquareRoot {
  /**
   * Take square root of a value.
   *
   * @param dim Dimension for which to take the square root.
   */
  def apply(dim: Dimension): SquareRoot = SquareRoot(dim, None)

  /**
   * Take square root of a value.
   *
   * @param dim  Dimension for which to take the square root.
   * @param name Pattern for the new name of the coordinate at `dim`. Use `%[12]$``s` for the string representations of
   *             the coordinate, and the content.
   */
  def apply(dim: Dimension, name: String): SquareRoot = SquareRoot(dim, Some(name))
}

/**
 * Convert a numeric value to categorical.
 *
 * @param dim  Dimension for which to convert.
 * @param name Optional pattern for the new name of the coordinate at `dim`. Use `%1$``s` for the string
 *             representations of the coordinate.
 *
 * @note Cut is only applied to numerical variables.
 */
case class Cut private (dim: Dimension, name: Option[String]) extends Transformer with PresentWithValue {
  type V = Map[Position1D, List[Double]]

  def present[P <: Position](cell: Cell[P], ext: V): Collection[Cell[P]] = {
    val pos = cell.position
    val con = cell.content

    val p = name match {
      case Some(n) => pos.update(dim, n.format(pos(dim).toShortString))
      case None => pos
    }

    (con.schema.kind.isSpecialisationOf(Numerical), con.value.asDouble, ext.get(Position1D(pos(dim)))) match {
      case (true, Some(v), Some(r)) =>
        val bins = r.sliding(2).map("(" + _.mkString(",") + "]").toList

        Collection(p, Content(OrdinalSchema[Codex.StringCodex](bins), bins(r.lastIndexWhere(_ < v))))
      case _ => Collection[Cell[P]]()
    }
  }
}

/** Companion object to the `Cut` class defining constructors. */
object Cut {
  /**
   * Convert numerical value to categorical.
   *
   * @param dim  Dimension for which to convert.
   */
  def apply(dim: Dimension): Cut = Cut(dim, None)

  /**
   * Convert numerical value to categorical.
   *
   * @param dim  Dimension for which to convert.
   * @param name Pattern for the new name of the coordinate at `dim`. Use `%1$``s` for the string representations of
   *             the coordinate.
   */
  def apply(dim: Dimension, name: String): Cut = Cut(dim, Some(name))
}

/** Base trait that defined various rules for cutting continuous data. */
trait CutRules {
  /** Type of 'wrapper' around user defined data. */
  type E[_]

  /** Type of statistics data from which the number of bins is comuted. */
  type Stats = Map[Position1D, Map[Position1D, Content]]

  /**
   * Define range of `k` approximately equal size bins.
   *
   * @param ext A `E` containing the feature statistics.
   * @param min Key (into `ext`) that identifies the minimum value.
   * @param max Key (into `ext`) that identifies the maximum value.
   * @param k   The number of bins.
   *
   * @return A `E` holding the break values.
   */
  def fixed[V: Valueable, W: Valueable](ext: E[Stats], min: V, max: W, k: Long): E[Cut#V]

  protected def fixedFromStats[V: Valueable, W: Valueable](stats: Stats, min: V, max: W, k: Long): Cut#V = {
    cut(stats, min, max, _ => Some(k))
  }

  /**
   * Define range of bins based on the square-root choice.
   *
   * @param ext   A `E` containing the feature statistics.
   * @param count Key (into `ext`) that identifies the number of features.
   * @param min   Key (into `ext`) that identifies the minimum value.
   * @param max   Key (into `ext`) that identifies the maximum value.
   *
   * @return A `E` holding the break values.
   */
  def squareRootChoice[V: Valueable, W: Valueable, X: Valueable](ext: E[Stats], count: V, min: W, max: X): E[Cut#V]

  protected def squareRootChoiceFromStats[V: Valueable, W: Valueable, X: Valueable](stats: Stats, count: V, min: W,
    max: X): Cut#V = {
    cut(stats, min, max, extract(_, count).map { case n => math.round(math.sqrt(n)) })
  }

  /**
   * Define range of bins based on Sturges' formula.
   *
   * @param ext   A `E` containing the feature statistics.
   * @param count Key (into `ext`) that identifies the number of features.
   * @param min   Key (into `ext`) that identifies the minimum value.
   * @param max   Key (into `ext`) that identifies the maximum value.
   *
   * @return A `E` holding the break values.
   */
  def sturgesFormula[V: Valueable, W: Valueable, X: Valueable](ext: E[Stats], count: V, min: W, max: X): E[Cut#V]

  protected def sturgesFormulaFromStats[V: Valueable, W: Valueable, X: Valueable](stats: Stats, count: V, min: W,
    max: X): Cut#V = cut(stats, min, max, extract(_, count).map { case n => math.ceil(log2(n) + 1).toLong })

  /**
   * Define range of bins based on the Rice rule.
   *
   * @param ext   A `E` containing the feature statistics.
   * @param count Key (into `ext`) that identifies the number of features.
   * @param min   Key (into `ext`) that identifies the minimum value.
   * @param max   Key (into `ext`) that identifies the maximum value.
   *
   * @return A `E` holding the break values.
   */
  def riceRule[V: Valueable, W: Valueable, X: Valueable](ext: E[Stats], count: V, min: W, max: X): E[Cut#V]

  protected def riceRuleFromStats[V: Valueable, W: Valueable, X: Valueable](stats: Stats, count: V, min: W,
    max: X): Cut#V = {
    cut(stats, min, max, extract(_, count).map { case n => math.ceil(2 * math.pow(n, 1.0 / 3.0)).toLong })
  }

  /**
   * Define range of bins based on Doane's formula.
   *
   * @param ext      A `E` containing the feature statistics.
   * @param count    Key (into `ext`) that identifies the number of features.
   * @param min      Key (into `ext`) that identifies the minimum value.
   * @param max      Key (into `ext`) that identifies the maximum value.
   * @param skewness Key (into `ext`) that identifies the skewwness.
   *
   * @return A `E` holding the break values.
   */
  def doanesFormula[V: Valueable, W: Valueable, X: Valueable, Y: Valueable](ext: E[Stats], count: V, min: W, max: X,
    skewness: Y): E[Cut#V]

  protected def doanesFormulaFromStats[V: Valueable, W: Valueable, X: Valueable, Y: Valueable](stats: Stats, count: V,
    min: W, max: X, skewness: Y): Cut#V = {
    cut(stats, min, max, m => (extract(m, count), extract(m, skewness)) match {
      case (Some(n), Some(s)) =>
        Some(math.round(1 + log2(n) + log2(1 + math.abs(s) / math.sqrt((6 * (n - 2)) / ((n + 1) * (n + 3))))))
      case _ => None
    })
  }

  /**
   * Define range of bins based on Scott's normal reference rule.
   *
   * @param ext   A `E` containing the feature statistics.
   * @param count Key (into `ext`) that identifies the number of features.
   * @param min   Key (into `ext`) that identifies the minimum value.
   * @param max   Key (into `ext`) that identifies the maximum value.
   * @param sd    Key (into `ext`) that identifies the standard deviation.
   *
   * @return A `E` holding the break values.
   */
  def scottsNormalReferenceRule[V: Valueable, W: Valueable, X: Valueable, Y: Valueable](ext: E[Stats], count: V,
    min: W, max: X, sd: Y): E[Cut#V]

  protected def scottsNormalReferenceRuleFromStats[V: Valueable, W: Valueable, X: Valueable, Y: Valueable](stats: Stats,
    count: V, min: W, max: X, sd: Y): Cut#V = {
    cut(stats, min, max, m => (extract(m, count), extract(m, min), extract(m, max), extract(m, sd)) match {
      case (Some(n), Some(l), Some(u), Some(s)) => Some(math.ceil((u - l) / (3.5 * s / math.pow(n, 1.0 / 3.0))).toLong)
      case _ => None
    })
  }

  /**
   * Return a `E` holding the user defined break values.
   *
   * @param range A map (holding for each key) the bins range of that feature.
   */
  def breaks[V: Valueable](range: Map[V, List[Double]]): E[Cut#V]

  protected def breaksFromMap[V: Valueable](range: Map[V, List[Double]]): Cut#V = {
    range.map { case (v, l) => (Position1D(v), l) }
  }

  // TODO: Add 'right' and 'labels' options (analogous to R's)
  private def cut[V: Valueable, W: Valueable](stats: Stats, min: V, max: W,
    bins: (Map[Position1D, Content]) => Option[Long]): Cut#V = {
    stats.flatMap {
      case (pos, map) => (extract(map, min), extract(map, max), bins(map)) match {
        case (Some(l), Some(u), Some(k)) =>
          val delta = math.abs(u - l)
          val range = (l to u by (delta / k)).tail.toList

          Some((pos, (l - 0.001 * delta) :: range))
        case _ => None
      }
    }
  }

  private def extract[V: Valueable](ext: Map[Position1D, Content], key: V): Option[Double] = {
    ext.get(Position1D(key)).flatMap(_.value.asDouble)
  }

  private def log2(x: Double): Double = math.log(x) / math.log(2)
}

// TODO: test, document and add appropriate constructors
case class Compare(f: (Double) => Boolean) extends Transformer with Present {
  def present[P <: Position](cell: Cell[P]): Collection[Cell[P]] = {
    Collection(cell.position,
      Content(NominalSchema[Codex.BooleanCodex](), cell.content.value.asDouble.map { f(_) }.getOrElse(false)))
  }
}

