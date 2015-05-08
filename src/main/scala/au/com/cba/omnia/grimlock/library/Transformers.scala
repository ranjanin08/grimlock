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
  protected def present[P <: Position](dim: Option[Dimension], cell: Cell[P], name: Option[String], typ: Option[Type],
    value: T): Collection[Cell[P]] = {
    if (checkContent(cell.content, typ)) { getCell(dim, cell, name, value) } else { Collection[Cell[P]]() }
  }

  protected def present[P <: Position](dim: Option[Dimension], cell: Cell[P], name: Option[String], typ: Option[Type],
    f: (T) => T): Collection[Cell[P]] = {
    (checkContent(cell.content, typ), getValue(cell.content)) match {
      case (true, Some(v)) => getCell(dim, cell, name, f(v))
      case _ => Collection[Cell[P]]()
    }
  }

  protected def present[P <: Position](dim: Option[Dimension], cell: Cell[P], ext: Map[Position1D, Content],
    key: Option[Position1D], name: Option[String], typ: Option[Type], f: (T, T) => T,
    inverse: Boolean): Collection[Cell[P]] = {
    present(dim, cell, name, typ, f, inverse, getValue(getKey(dim, cell.position, key), ext))
  }

  protected def present[P <: Position](dim: Dimension, cell: Cell[P], ext: Map[Position1D, Map[Position1D, Content]],
    key: Position1D, name: Option[String], typ: Option[Type], f: (T, T) => T, inverse: Boolean): Collection[Cell[P]] = {
    present(Some(dim), cell, name, typ, f, inverse, getValue(getKey(Some(dim), cell.position, None), key, ext))
  }

  protected def present[P <: Position](dim: Dimension, cell: Cell[P], ext: Map[Position1D, Map[Position1D, Content]],
    key1: Position1D, key2: Position1D, name: Option[String], typ: Option[Type],
    f: (T, T, T) => T): Collection[Cell[P]] = {
    (checkContent(cell.content, typ), getValue(cell.content),
      getValue(getKey(Some(dim), cell.position, None), key1, ext),
      getValue(getKey(Some(dim), cell.position, None), key2, ext)) match {
        case (true, Some(v), Some(x), Some(y)) => getCell(Some(dim), cell, name, f(v, x, y))
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

  private def getName[P <: Position](dim: Option[Dimension], cell: Cell[P], name: Option[String]): P = {
    val pos = cell.position
    val con = cell.content

    (dim, name) match {
      case (Some(d), Some(n)) => pos.update(d, n.format(pos(d).toShortString, con.value.toShortString))
      case _ => pos
    }
  }

  private def getKey[P <: Position](dim: Option[Dimension], pos: P, key: Option[Position1D]): Position1D = {
    (dim, key) match {
      case (_, Some(k)) => k
      case (Some(d), None) => Position1D(pos(d))
      case _ => throw new Exception("Missing both dimension and key")
    }
  }

  private def getCell[P <: Position](dim: Option[Dimension], cell: Cell[P], name: Option[String],
    value: T): Collection[Cell[P]] = Collection(getName(dim, cell, name), getContent(value))

  private def present[P <: Position](dim: Option[Dimension], cell: Cell[P], name: Option[String], typ: Option[Type],
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

/** Create indicator variables. */
case class Indicator[P <: Position]() extends Transformer[P, P] with Present[P, P] {
  def present(cell: Cell[P]): Collection[Cell[P]] = {
    Collection(cell.position, Content(DiscreteSchema[Codex.LongCodex](), 1))
  }
}

/**
 * Binarise categorical variables.
 *
 * @param dim  Dimension for which to update coordinate name.
 * @param name Pattern for the new name of the coordinate at `dim`. Use `%[12]$``s` for the string representations of
 *             the coordinate, and the content.
 *
 * @note Binarisation is only applied to categorical variables.
 */
case class Binarise[P <: Position](dim: Dimension,
  name: String = "%1$s=%2$s") extends Transformer[P, P] with Present[P, P] {
  def present(cell: Cell[P]): Collection[Cell[P]] = {
    if (cell.content.schema.kind.isSpecialisationOf(Categorical)) {
      Collection(cell.position.update(dim, name.format(cell.position(dim).toShortString,
        cell.content.value.toShortString)), Content(DiscreteSchema[Codex.LongCodex](), 1))
    } else {
      Collection[Cell[P]]()
    }
  }
}

/**
 * Normalise numeric variables.
 *
 * @param dim Optional dimension for which to update coordinate name.
 * @param key Key into the inner map of `V`, identifying the normalisation constant.
 *
 * @note Normalisation scales a variable in the range [-1, 1]. It is only applied to numerical variables.
 */
case class Normalise[P <: Position, T](dim: Dimension, key: T)(
  implicit ev: Positionable[T, Position1D]) extends Transformer[P, P] with PresentWithValue[P, P] {
  type V = Map[Position1D, Map[Position1D, Content]]

  val key2 = ev.convert(key)

  def present(cell: Cell[P], ext: V): Collection[Cell[P]] = {
    val key1 = Position1D(cell.position(dim))

    (cell.content.schema.kind.isSpecialisationOf(Numerical), cell.content.value.asDouble,
      ext.get(key1).flatMap(_.get(key2).flatMap(_.value.asDouble))) match {
      case (true, Some(v), Some(n)) => Collection(cell.position, Content(ContinuousSchema[Codex.DoubleCodex](), v / n))
      case _ => Collection[Cell[P]]()
    }
  }
}

/**
 * Standardise numeric variables.
 *
 * @param dim       Dimension for which to create standardised variables.
 * @param mean      Key into the inner map of `V`, identifying the mean.
 * @param sd        Key into the inner map of `V`, identifying the standard deviation.
 * @param threshold Minimum standard deviation threshold. Values less than this result in standardised value of zero.
 * @param n         Number of times division by standard deviation.
 *
 * @note Standardisation results in a variable with zero mean and variance of one. It is only applied to numerical
 *       variables.
 */
case class Standardise[P <: Position, T](dim: Dimension, mean: T, sd: T, threshold: Double = 1e-4, n: Int = 1)(
  implicit ev: Positionable[T, Position1D]) extends Transformer[P, P] with PresentWithValue[P, P] {
  type V = Map[Position1D, Map[Position1D, Content]]

  val key2m = ev.convert(mean)
  val key2s = ev.convert(sd)

  def present(cell: Cell[P], ext: V): Collection[Cell[P]] = {
    val key1 = Position1D(cell.position(dim))

    (cell.content.schema.kind.isSpecialisationOf(Numerical), cell.content.value.asDouble,
      ext.get(key1).flatMap(_.get(key2m).flatMap(_.value.asDouble)),
      ext.get(key1).flatMap(_.get(key2s).flatMap(_.value.asDouble))) match {
      case (true, Some(v), Some(m), Some(s)) => Collection(cell.position,
        Content(ContinuousSchema[Codex.DoubleCodex](), if (math.abs(s) < threshold) 0.0 else (v - m) / (n * s)))
      case _ => Collection[Cell[P]]()
    }
  }
}

/**
 * Clamp numeric variables.
 *
 * @param dim   Dimension for which to create clamped variables.
 * @param lower Key into the inner map of `V`, identifying the lower clamping constant.
 * @param upper Key into the inner map of `V`, identifying the upper clamping constant.
 *
 * @note Clamping results in a variable not smaller (or greater) than the clamping constants. It is only applied to
 *       numerical variables.
 */
case class Clamp[P <: Position, T](dim: Dimension, lower: T, upper: T)(
  implicit ev: Positionable[T, Position1D]) extends Transformer[P, P] with PresentWithValue[P, P] {
  type V = Map[Position1D, Map[Position1D, Content]]

  val key2l = ev.convert(lower)
  val key2u = ev.convert(upper)

  def present(cell: Cell[P], ext: V): Collection[Cell[P]] = {
    val key1 = Position1D(cell.position(dim))

    (cell.content.schema.kind.isSpecialisationOf(Numerical), cell.content.value.asDouble,
      ext.get(key1).flatMap(_.get(key2l).flatMap(_.value.asDouble)),
      ext.get(key1).flatMap(_.get(key2u).flatMap(_.value.asDouble))) match {
      case (true, Some(v), Some(l), Some(u)) => Collection(cell.position,
        Content(ContinuousSchema[Codex.DoubleCodex](), if (v < l) l else if (v > u) u else v))
      case _ => Collection[Cell[P]]()
    }
  }
}

/**
 * Compute the inverse document frequency.
 *
 * @param dim  Optional dimension for which to create inverse document frequencies.
 * @param key  Optional key into the map `ext` identifying the number of documents.
 * @param idf  Idf function to use.
 *
 * @note Idf is only applied to numerical variables.
 */
case class Idf[P <: Position] private (dim: Option[Dimension], key: Option[Position1D],
  idf: (Double, Double) => Double) extends Transformer[P, P] with PresentWithValue[P, P] {
  type V = Map[Position1D, Content]

  def present(cell: Cell[P], ext: V): Collection[Cell[P]] = {
    val k = (dim, key) match {
      case (_, Some(k)) => k
      case (Some(d), None) => Position1D(cell.position(d))
      case _ => throw new Exception("Missing both dimension and key")
    }

    (cell.content.schema.kind.isSpecialisationOf(Numerical), cell.content.value.asDouble,
      ext.get(k).flatMap(_.value.asDouble)) match {
      case (true, Some(df), Some(n)) => Collection(cell.position,
        Content(ContinuousSchema[Codex.DoubleCodex](), idf(df, n)))
      case _ => Collection[Cell[P]]()
    }
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

  /** Default idf log transformation. */
  def DefaultTransform() = Transform(math.log, 1)

  /**
   * Compute the inverse document frequency.
   *
   * @param dim Dimension for which to create inverse document frequencies.
   */
  def apply[P <: Position](dim: Dimension): Idf[P] = Idf(Some(dim), None, DefaultTransform())

  /**
   * Compute the inverse document frequency.
   *
   * @param key Key into the map `ext` identifying the number of documents.
   */
  def apply[P <: Position, T](key: T)(implicit ev: Positionable[T, Position1D]): Idf[P] = {
    Idf(None, Some(ev.convert(key)), DefaultTransform())
  }

  /**
   * Compute the inverse document frequency.
   *
   * @param dim Dimension for which to create inverse document frequencies.
   * @param idf Idf function to use.
   */
  def apply[P <: Position](dim: Dimension, idf: (Double, Double) => Double): Idf[P] = Idf(Some(dim), None, idf)

  /**
   * Compute the inverse document frequency.
   *
   * @param key Key into the map `ext` identifying the number of documents.
   * @param idf Idf function to use.
   */
  def apply[P <: Position, T](key: T, idf: (Double, Double) => Double)(
    implicit ev: Positionable[T, Position1D]): Idf[P] = Idf(None, Some(ev.convert(key)), idf)
}

/**
 * Create boolean term frequencies; all term frequencies are binarised.
 *
 * @note Boolean tf is only applied to numerical variables.
 */
case class BooleanTf[P <: Position]() extends Transformer[P, P] with Present[P, P] {
  def present(cell: Cell[P]): Collection[Cell[P]] = {
    (cell.content.schema.kind.isSpecialisationOf(Numerical)) match {
      case true => Collection(cell.position, Content(ContinuousSchema[Codex.DoubleCodex](), 1))
      case false => Collection[Cell[P]]()
    }
  }
}

/**
 * Create logarithmic term frequencies.
 *
 * @param log  Log function to use.
 *
 * @note Logarithmic tf is only applied to numerical variables.
 */
case class LogarithmicTf[P <: Position](
  log: (Double) => Double = math.log) extends Transformer[P, P] with Present[P, P] {
  def present(cell: Cell[P]): Collection[Cell[P]] = {
    (cell.content.schema.kind.isSpecialisationOf(Numerical), cell.content.value.asDouble) match {
      case (true, Some(tf)) => Collection(cell.position, Content(ContinuousSchema[Codex.DoubleCodex](), 1 + log(tf)))
      case _ => Collection[Cell[P]]()
    }
  }
}

/**
 * Create augmented term frequencies.
 *
 * @param dim Dimension for which to create boolean term frequencies.
 *
 * @note Augmented tf is only applied to numerical variables.
 */
case class AugmentedTf[P <: Position](dim: Dimension) extends Transformer[P, P] with PresentWithValue[P, P] {
  type V = Map[Position1D, Content]

  def present(cell: Cell[P], ext: V): Collection[Cell[P]] = {
    (cell.content.schema.kind.isSpecialisationOf(Numerical), cell.content.value.asDouble,
      ext.get(Position1D(cell.position(dim))).flatMap(_.value.asDouble)) match {
      case (true, Some(tf), Some(m)) => Collection(cell.position,
        Content(ContinuousSchema[Codex.DoubleCodex](), 0.5 + (0.5 * tf) / m))
      case _ => Collection[Cell[P]]()
    }
  }
}

/**
 * Create tf-idf values.
 *
 * @param dim Optional dimension for which to create term frequencies or update coordinate name.
 * @param key Optional key into the map `ext` identifying the inverse document frequency.
 *
 * @note Tf-idf is only applied to numerical variables.
 */
case class TfIdf[P <: Position] private (dim: Option[Dimension],
  key: Option[Position1D]) extends Transformer[P, P] with PresentWithValue[P, P] {
  type V = Map[Position1D, Content]

  def present(cell: Cell[P], ext: V): Collection[Cell[P]] = {
    val k = (dim, key) match {
      case (_, Some(k)) => k
      case (Some(d), None) => Position1D(cell.position(d))
      case _ => throw new Exception("Missing both dimension and key")
    }

    (cell.content.schema.kind.isSpecialisationOf(Numerical), cell.content.value.asDouble,
      ext.get(k).flatMap(_.value.asDouble)) match {
      case (true, Some(tf), Some(idf)) => Collection(cell.position,
        Content(ContinuousSchema[Codex.DoubleCodex](), tf * idf))
      case _ => Collection[Cell[P]]()
    }
  }
}

/** Companion object to the `TfIdf` class defining constructors. */
object TfIdf {
  /**
   * Create tf-idf values.
   *
   * @param dim Dimension for which to create term frequencies.
   */
  def apply[P <: Position](dim: Dimension): TfIdf[P] = TfIdf(Some(dim), None)

  /**
   * Create tf-idf values.
   *
   * @param key Key into the map `V` identifying the inverse document frequency.
   */
  def apply[P <: Position, T](key: T)(implicit ev: Positionable[T, Position1D]): TfIdf[P] = {
    TfIdf(None, Some(ev.convert(key)))
  }
}

/**
 * Add a value.
 *
 * @param dim Optional dimension for which to add or update coordinate name.
 * @param key Optional key into the map `ext` identifying the value to add.
 *
 * @note Add is only applied to numerical variables.
 */
case class Add[P <: Position] private (dim: Option[Dimension],
  key: Option[Position1D]) extends Transformer[P, P] with PresentWithValue[P, P] {
  type V = Map[Position1D, Content]

  def present(cell: Cell[P], ext: V): Collection[Cell[P]] = {
    val k = (dim, key) match {
      case (_, Some(k)) => k
      case (Some(d), None) => Position1D(cell.position(d))
      case _ => throw new Exception("Missing both dimension and key")
    }

    (cell.content.schema.kind.isSpecialisationOf(Numerical), cell.content.value.asDouble,
      ext.get(k).flatMap(_.value.asDouble)) match {
      case (true, Some(l), Some(r)) => Collection(cell.position, Content(ContinuousSchema[Codex.DoubleCodex](), l + r))
      case _ => Collection[Cell[P]]()
    }
  }
}

/** Companion object to the `Add` class defining constructors. */
object Add {
  /**
   * Add a value.
   *
   * @param dim Dimension for which to add.
   */
  def apply[P <: Position](dim: Dimension): Add[P] = Add(Some(dim), None)

  /**
   * Add a value.
   *
   * @param key Key into the map `ext` identifying the value to add.
   */
  def apply[P <: Position, T](key: T)(implicit ev: Positionable[T, Position1D]): Add[P] = {
    Add(None, Some(ev.convert(key)))
  }
}

/**
 * Subtract a value.
 *
 * @param dim     Optional dimension for which to subtract or update coordinate name.
 * @param key     Optional key into the map `ext` identifying the value to subtract.
 * @param inverse Indicator specifying order of subtract.
 *
 * @note Subtract is only applied to numerical variables.
 */
case class Subtract[P <: Position] private (dim: Option[Dimension], key: Option[Position1D],
  inverse: Boolean) extends Transformer[P, P] with PresentWithValue[P, P] {
  type V = Map[Position1D, Content]

  def present(cell: Cell[P], ext: V): Collection[Cell[P]] = {
    val k = (dim, key) match {
      case (_, Some(k)) => k
      case (Some(d), None) => Position1D(cell.position(d))
      case _ => throw new Exception("Missing both dimension and key")
    }

    (cell.content.schema.kind.isSpecialisationOf(Numerical), cell.content.value.asDouble,
      ext.get(k).flatMap(_.value.asDouble)) match {
      case (true, Some(l), Some(r)) => Collection(cell.position,
        Content(ContinuousSchema[Codex.DoubleCodex](), if (inverse) r - l else l - r))
      case _ => Collection[Cell[P]]()
    }
  }
}

/** Companion object to the `Subtract` class defining constructors. */
object Subtract {
  /**
   * Subtract a value.
   *
   * @param dim Dimension for which to subtract.
   */
  def apply[P <: Position](dim: Dimension): Subtract[P] =  Subtract(Some(dim), None, false)

  /**
   * Subtract a value.
   *
   * @param key Key into the map `V` identifying the value to subtract.
   */
  def apply[P <: Position, T](key: T)(implicit ev: Positionable[T, Position1D]): Subtract[P] = {
    Subtract(None, Some(ev.convert(key)), false)
  }

  /**
   * Subtract a value.
   *
   * @param dim     Dimension for which to subtract.
   * @param inverse Indicator specifying order of subtract.
   */
  def apply[P <: Position](dim: Dimension, inverse: Boolean): Subtract[P] = Subtract(Some(dim), None, inverse)

  /**
   * Subtract a value.
   *
   * @param key     Key into the map `V` identifying the value to subtract.
   * @param inverse Indicator specifying order of subtract.
   */
  def apply[P <: Position, T](key: T, inverse: Boolean)(implicit ev: Positionable[T, Position1D]): Subtract[P] = {
    Subtract(None, Some(ev.convert(key)), inverse)
  }
}

/**
 * Multiply a value.
 *
 * @param dim Optional dimension for which to multiply or update coordinate name.
 * @param key Optional key into the map `ext` identifying the value to multiply by.
 *
 * @note Multiply is only applied to numerical variables.
 */
case class Multiply[P <: Position] private (dim: Option[Dimension], 
  key: Option[Position1D]) extends Transformer[P, P] with PresentWithValue[P, P] {
  type V = Map[Position1D, Content]

  def present(cell: Cell[P], ext: V): Collection[Cell[P]] = {
    val k = (dim, key) match {
      case (_, Some(k)) => k
      case (Some(d), None) => Position1D(cell.position(d))
      case _ => throw new Exception("Missing both dimension and key")
    }

    (cell.content.schema.kind.isSpecialisationOf(Numerical), cell.content.value.asDouble,
      ext.get(k).flatMap(_.value.asDouble)) match {
      case (true, Some(l), Some(r)) => Collection(cell.position, Content(ContinuousSchema[Codex.DoubleCodex](), l * r))
      case _ => Collection[Cell[P]]()
    }
  }
}

/** Companion object to the `Multiply` class defining constructors. */
object Multiply {
  /**
   * Multiply a value.
   *
   * @param dim Dimension for which to multiply.
   */
  def apply[P <: Position](dim: Dimension): Multiply[P] = Multiply(Some(dim), None)

  /**
   * Multiply a value.
   *
   * @param key Key into the map `ext` identifying the value to multiply by.
   */
  def apply[P <: Position, T](key: T)(implicit ev: Positionable[T, Position1D]): Multiply[P] = {
    Multiply(None, Some(ev.convert(key)))
  }
}

/**
 * Divide a value.
 *
 * @param dim     Optional dimension for which to divide or update coordinate name.
 * @param key     Optional key into the map `ext` identifying the value to divide by.
 * @param inverse Indicator specifying order of division.
 *
 * @note Fraction is only applied to numerical variables.
 */
case class Fraction[P <: Position] private (dim: Option[Dimension], key: Option[Position1D],
  inverse: Boolean) extends Transformer[P, P] with PresentWithValue[P, P] {
  type V = Map[Position1D, Content]

  def present(cell: Cell[P], ext: V): Collection[Cell[P]] = {
    val k = (dim, key) match {
      case (_, Some(k)) => k
      case (Some(d), None) => Position1D(cell.position(d))
      case _ => throw new Exception("Missing both dimension and key")
    }

    (cell.content.schema.kind.isSpecialisationOf(Numerical), cell.content.value.asDouble,
      ext.get(k).flatMap(_.value.asDouble)) match {
      case (true, Some(l), Some(r)) => Collection(cell.position,
        Content(ContinuousSchema[Codex.DoubleCodex](), if (inverse) r / l else l / r))
      case _ => Collection[Cell[P]]()
    }
  }
}

/** Companion object to the `Fraction` class defining constructors. */
object Fraction {
  /**
   * Divide a value.
   *
   * @param dim Dimension for which to divide.
   */
  def apply[P <: Position](dim: Dimension): Fraction[P] = Fraction(Some(dim), None, false)

  /**
   * Divide a value.
   *
   * @param key Key into the map `ext` identifying the value to divide by.
   */
  def apply[P <: Position, T](key: T)(implicit ev: Positionable[T, Position1D]): Fraction[P] = {
    Fraction(None, Some(ev.convert(key)), false)
  }

  /**
   * Divide a value.
   *
   * @param dim     Dimension for which to divide.
   * @param inverse Indicator specifying order of division.
   */
  def apply[P <: Position](dim: Dimension, inverse: Boolean): Fraction[P] = Fraction(Some(dim), None, inverse)

  /**
   * Divide a value.
   *
   * @param key     Key into the map `ext` identifying the value to divide by.
   * @param inverse Indicator specifying order of division.
   */
  def apply[P <: Position, T](key: T, inverse: Boolean)(implicit ev: Positionable[T, Position1D]): Fraction[P] = {
    Fraction(None, Some(ev.convert(key)), inverse)
  }
}

/**
 * Raise value to a power.
 *
 * @param power The power to raise to.
 *
 * @note Power is only applied to numerical variables.
 */
case class Power[P <: Position](power: Double) extends Transformer[P, P] with Present[P, P] {
  def present(cell: Cell[P]): Collection[Cell[P]] = {
    (cell.content.schema.kind.isSpecialisationOf(Numerical), cell.content.value.asDouble) match {
      case (true, Some(d)) => Collection(cell.position,
        Content(ContinuousSchema[Codex.DoubleCodex](), math.pow(d, power)))
      case _ => Collection[Cell[P]]()
    }
  }
}

/**
 * Take square root of a value.
 *
 * @note SquareRoot is only applied to numerical variables.
 */
case class SquareRoot[P <: Position]() extends Transformer[P, P] with Present[P, P] {
  def present(cell: Cell[P]): Collection[Cell[P]] = {
    (cell.content.schema.kind.isSpecialisationOf(Numerical), cell.content.value.asDouble) match {
      case (true, Some(d)) => Collection(cell.position, Content(ContinuousSchema[Codex.DoubleCodex](), math.sqrt(d)))
      case _ => Collection[Cell[P]]()
    }
  }
}

/**
 * Convert a numeric value to categorical.
 *
 * @param dim Dimension for which to convert.
 *
 * @note Cut is only applied to numerical variables.
 */
case class Cut[P <: Position](dim: Dimension) extends Transformer[P, P] with PresentWithValue[P, P] {
  type V = Map[Position1D, List[Double]]

  def present(cell: Cell[P], ext: V): Collection[Cell[P]] = {
    (cell.content.schema.kind.isSpecialisationOf(Numerical), cell.content.value.asDouble,
      ext.get(Position1D(cell.position(dim)))) match {
      case (true, Some(v), Some(r)) =>
        val bins = r.sliding(2).map("(" + _.mkString(",") + "]").toList

        Collection(cell.position, Content(OrdinalSchema[Codex.StringCodex](bins), bins(r.lastIndexWhere(_ < v))))
      case _ => Collection[Cell[P]]()
    }
  }
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
  def fixed[V: Valueable, W: Valueable](ext: E[Stats], min: V, max: W, k: Long): E[Map[Position1D, List[Double]]]

  protected def fixedFromStats[V: Valueable, W: Valueable](stats: Stats, min: V, max: W,
    k: Long): Map[Position1D, List[Double]] = cut(stats, min, max, _ => Some(k))

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
  def squareRootChoice[V: Valueable, W: Valueable, X: Valueable](ext: E[Stats], count: V, min: W,
    max: X): E[Map[Position1D, List[Double]]]

  protected def squareRootChoiceFromStats[V: Valueable, W: Valueable, X: Valueable](stats: Stats, count: V, min: W,
    max: X): Map[Position1D, List[Double]] = {
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
  def sturgesFormula[V: Valueable, W: Valueable, X: Valueable](ext: E[Stats], count: V, min: W,
    max: X): E[Map[Position1D, List[Double]]]

  protected def sturgesFormulaFromStats[V: Valueable, W: Valueable, X: Valueable](stats: Stats, count: V, min: W,
    max: X): Map[Position1D, List[Double]] = {
    cut(stats, min, max, extract(_, count).map { case n => math.ceil(log2(n) + 1).toLong })
  }

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
  def riceRule[V: Valueable, W: Valueable, X: Valueable](ext: E[Stats], count: V, min: W,
    max: X): E[Map[Position1D, List[Double]]]

  protected def riceRuleFromStats[V: Valueable, W: Valueable, X: Valueable](stats: Stats, count: V, min: W,
    max: X): Map[Position1D, List[Double]] = {
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
    skewness: Y): E[Map[Position1D, List[Double]]]

  protected def doanesFormulaFromStats[V: Valueable, W: Valueable, X: Valueable, Y: Valueable](stats: Stats, count: V,
    min: W, max: X, skewness: Y): Map[Position1D, List[Double]] = {
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
    min: W, max: X, sd: Y): E[Map[Position1D, List[Double]]]

  protected def scottsNormalReferenceRuleFromStats[V: Valueable, W: Valueable, X: Valueable, Y: Valueable](stats: Stats,
    count: V, min: W, max: X, sd: Y): Map[Position1D, List[Double]] = {
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
  def breaks[V: Valueable](range: Map[V, List[Double]]): E[Map[Position1D, List[Double]]]

  protected def breaksFromMap[V: Valueable](range: Map[V, List[Double]]): Map[Position1D, List[Double]] = {
    range.map { case (v, l) => (Position1D(v), l) }
  }

  // TODO: Add 'right' and 'labels' options (analogous to R's)
  private def cut[V: Valueable, W: Valueable](stats: Stats, min: V, max: W,
    bins: (Map[Position1D, Content]) => Option[Long]): Map[Position1D, List[Double]] = {
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
case class Compare[P <: Position](f: (Double) => Boolean) extends Transformer[P, P] {
  def present(cell: Cell[P]): Collection[Cell[P]] = {
    Collection(cell.position,
      Content(NominalSchema[Codex.BooleanCodex](), cell.content.value.asDouble.map(f).getOrElse(false)))
  }
}

