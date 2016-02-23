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

package au.com.cba.omnia.grimlock.framework.content.metadata

import java.util.Date

import au.com.cba.omnia.grimlock.framework._
import au.com.cba.omnia.grimlock.framework.content._
import au.com.cba.omnia.grimlock.framework.encoding._

import scala.reflect._

/** Base trait for variable schemas. */
trait Schema {
  type S

  /** The type of variable. */
  val kind: Type

  def validate(value: Value { type V = S }): Boolean

  /**
    * Return a consise (terse) string representation of a schema.
    *
    * @param separator Separator to use between various fields.
    */
  def toShortString(codec: Codec { type C = S }): String = {
    kind.toShortString + round(paramString(true, s => codec.encode(s)))
  }

  override def toString(): String = {
    this.getClass.getName + square(typeString()) + "(" + paramString(false, s => s.toString) + ")"
  }

  protected def paramString(short: Boolean, f: (S) => String): String
  protected def typeString(): String

  private def round(str: String): String = if (str.isEmpty) str else "(" + str + ")"
  private def square(str: String): String = if (str.isEmpty) str else "[" + str + "]"
}

object Schema {
  def fromShortString(codec: Codec, str: String): Option[Schema { type S = codec.C }] = {
    str match {
      case ContinuousSchema.Pattern(_) => ContinuousSchema.fromShortString(str, codec)
      case DiscreteSchema.Pattern(_, _) => DiscreteSchema.fromShortString(str, codec)
      case NominalSchema.Pattern(_) => NominalSchema.fromShortString(str, codec)
      case OrdinalSchema.Pattern(_) => OrdinalSchema.fromShortString(str, codec)
      case DateSchema.Pattern(_) => DateSchema.fromShortString(str, codec)
      case _ => None
    }
  }
}

trait SchemaParameters {
  protected def parse(codec: Codec, value: String): Option[codec.C] = codec.decode(value).map(_.value)

  protected def parseRange(codec: Codec, range: String): Option[(codec.C, codec.C)] = {
    range.split(":") match {
      case Array(lower, upper) =>
        (parse(codec, lower), parse(codec, upper)) match {
          case (Some(low), Some(upp)) => Some((low, upp))
          case _ => None
        }
      case _ => None
    }
  }

  protected def parseSet(codec: Codec, set: String): Option[Set[codec.C]] = {
    val values = set.split("(?<!\\\\),").map(parse(codec, _))

    values.exists(_.isEmpty) match {
      case true => None
      case false => Some(values.map(_.get).toSet)
    }
  }

  protected def writeRange[S](short: Boolean, range: Option[(S, S)], f: (S) => String): String = {
    range.map { case (lower, upper) => f(lower) + (if (short) { ":" } else { "," }) + f(upper) }.getOrElse("")
  }

  protected def writeSet[S](short: Boolean, set: Set[S], f: (S) => String): String = {
    val args = set.map(f(_).replaceAll(",", "\\\\,")).mkString(",")

    if (short) { args } else { "Set(" + args + ")" }
  }
}

/** Base trait for schemas for numerical variables. */
trait NumericalSchema[T] extends Schema {
  type S = T

  /* Optional range of variable */
  val range: Option[(T, T)]

  protected def validateRange(value: Value { type V = S })(implicit ev: Numeric[S]): Boolean = {
    import ev._

    range.map { case (lower, upper) => value.value >= lower && value.value <= upper }.getOrElse(true)
  }
}

/**
 * Schema for continuous variables.
 *
 * @param range The optional range of the variable.
  * @note The constructor is private to ensure a clean interface as provided by the `apply` methods of the companion
 *       object.
 */
case class ContinuousSchema[T: ClassTag] private (range: Option[(T, T)])(
  implicit ev: Numeric[T]) extends NumericalSchema[T] with SchemaParameters {
  val kind = Type.Continuous

  def validate(value: Value { type V = S }): Boolean = validateRange(value)

  protected def paramString(short: Boolean, f: (S) => String): String = writeRange(short, range, f)
  protected def typeString(): String = classTag[T].runtimeClass.getSimpleName
}

object ContinuousSchema extends SchemaParameters {
  /** Pattern for matching short string continuous value with parameters. */
  val Pattern = (Type.Continuous.name + """(?:\((?:(.*?:.*))?\))?""").r

  def apply[T: ClassTag]()(implicit ev: Numeric[T]): ContinuousSchema[T] = ContinuousSchema(None)

  /**
    * Construct a continuous schema with bounded range.
    *
    * @param lower The lower bound (minimum value).
    * @param upper The upper bound (maximum value).
    */
  def apply[T: ClassTag](lower: T, upper: T)(implicit ev: Numeric[T]): ContinuousSchema[T] = {
    ContinuousSchema(Some((lower, upper)))
  }

  /**
    * Parse continuous schema parameters
    *
    * @param str The string representation
    * @param codec The codec to parse with.
    * @return An `Option` of `ContinuousSchema` if all parameters parse correctly, `None` otherwise.
    */
  def fromShortString(str: String, codec: Codec): Option[ContinuousSchema[codec.C]] = {
    (codec.tag, codec.numeric, str) match {
      case (Some(tag), Some(ev), Pattern(null)) => Some(ContinuousSchema()(tag, ev))
      case (Some(tag), Some(ev), Pattern(range)) =>
        parseRange(codec, range).map { case (lower, upper) => ContinuousSchema(lower, upper)(tag, ev) }
      case _ => None
    }
  }
}

/**
 * Schema for discrete variables.
 *
 * @param range   The optional range of the variable
 * @param step    The optional step size.
  * @note The constructor is private to ensure a clean interface as provided by the `apply` methods of the companion
 *       object.
 */
case class DiscreteSchema[T: ClassTag] private (range: Option[(T, T)], step: Option[T])(
  implicit ev: Integral[T]) extends NumericalSchema[T] with SchemaParameters {
  val kind = Type.Discrete

  def validate(value: Value { type V = S }): Boolean = {
    import ev._

    validateRange(value) && step.map { case s => (value.value % s) == 0 }.getOrElse(true)
  }

  protected def paramString(short: Boolean, f: (S) => String): String = {
    step.map { s => writeRange(short, range, f) + "," + f(s) }.getOrElse("")
  }
  protected def typeString(): String = classTag[T].runtimeClass.getSimpleName
}

object DiscreteSchema extends SchemaParameters {
  val Pattern = (Type.Discrete.name + """(?:\((?:(.*?:.*?),(.*))?\))?""").r

  /**
    * Construct a discrete schema with unbounded range and step size 1.
    *
    */
  def apply[T: ClassTag]()(implicit ev: Integral[T]): DiscreteSchema[T] = DiscreteSchema(None, None)

  /**
    * Construct a discrete schema with bounded range and step size.
    *
    * @param lower The lower bound (minimum value).
    * @param upper The upper bound (maximum value).
    * @param step    The step size.
    */
  def apply[T: ClassTag](lower: T, upper: T, step: T)(implicit ev: Integral[T]): DiscreteSchema[T] = {
    DiscreteSchema(Some((lower, upper)), Some(step))
  }

  /**
    * Parse discrete schema parameters
    *
    * @param str The string to parse
    * @param codec The codex to parse with.
    * @return An `Option` of `DiscreteSchema` if all parameters parse correctly, `None` otherwise.
    */
  def fromShortString(str: String, codec: Codec): Option[DiscreteSchema[codec.C]] = {
    (codec.tag, codec.integral, str) match {
      case (Some(tag), Some(ev), Pattern(null, null)) => Some(DiscreteSchema()(tag, ev))
      case (Some(tag), Some(ev), Pattern(range, step)) => {
        (parseRange(codec, range), parse(codec, step)) match {
          case (Some((lower, upper)), Some(step)) => Some(DiscreteSchema(lower, upper, step)(tag, ev))
          case _ => None
        }
      }
      case _ => None
    }
  }
}

/** Base trait for schemas for categorical variables. */
trait CategoricalSchema[T] extends Schema with SchemaParameters {
  type S = T

  /** values the variable can take. */
  val domain: Set[S]

  def validate(value: Value { type V = S }): Boolean = domain.isEmpty || domain.contains(value.value)

  protected def paramString(short: Boolean, f: (S) => String): String = writeSet(short, domain, f)
}

/**
  * Schema for nominal variables.
  *
  * @param domain The values of the variable.
  * @note The constructor is private to ensure a clean interface as provided by the `apply` methods of the companion
  *       object.
  */
case class NominalSchema[T: ClassTag](domain: Set[T] = Set[T]()) extends CategoricalSchema[T] {
  val kind = Type.Nominal

  protected def typeString(): String = classTag[T].runtimeClass.getSimpleName
}

/** Companion object to `NominalSchema`. */
object NominalSchema extends SchemaParameters {
  val Pattern = (Type.Nominal.name + """(?:\((?:(.*?))?\))?""").r

  /**
    * Parse nominal schema parameters
    *
    * @param str The string to parse
    * @param codec  The codex to parse with.
    * @return An `Option` of `NominalSchema` if all parameters parse correctly, `None` otherwise.
    */
  def fromShortString(str: String, codec: Codec): Option[NominalSchema[codec.C]] = {
    (codec.tag, str) match {
      case (Some(tag), Pattern(null)) => Some(NominalSchema()(tag))
      case (Some(tag), Pattern(domain)) => parseSet(codec, domain).map(NominalSchema(_)(tag))
      case _ => None
    }
  }
}

/**
  * Schema for ordinal variables.
  *
  * @param domain The optional values of the variable.
  * @note The constructor is private to ensure a clean interface as provided by the `apply` methods of the companion
  *       object.
  */
case class OrdinalSchema[T: ClassTag](domain: Set[T] = Set[T]()) extends CategoricalSchema[T] {
  val kind = Type.Ordinal

  protected def typeString(): String = classTag[T].runtimeClass.getSimpleName
}

/** Companion object to `OrdinalSchema`. */

object OrdinalSchema extends SchemaParameters {

  /** Pattern for matching short string ordinal value with parameters. */
  val Pattern = (Type.Ordinal.name + """(?:\((?:(.*?))?\))?""").r

  /**
    * Parse ordinal schema parameters
    *
    * @param str The string to parse
    * @param codec  The codex to parse with.
    * @return An `Option` of `OrdinalSchema` if all parameters parse correctly, `None` otherwise.
    */

  def fromShortString(str: String, codec: Codec): Option[OrdinalSchema[codec.C]] = {
    (codec.tag, str) match {
      case (Some(tag), Pattern(null)) => Some(OrdinalSchema()(tag))
      case (Some(tag), Pattern(domain)) => parseSet(codec, domain).map(OrdinalSchema(_)(tag))
      case _ => None
    }
  }
}

/** Schema for date variables. */
case class DateSchema[T: ClassTag] private (dates: Option[Either[(T, T), Set[T]]])(
  implicit ev: T => Date) extends Schema with SchemaParameters {
  type S = T

  val kind = Type.Date

  def validate(value: Value { type V = S }): Boolean = {
    dates match {
      case None => true
      case Some(Left((lower, upper))) => (lower.compareTo(value.value) <= 0) && (upper.compareTo(value.value) >= 0)
      case Some(Right(domain)) => domain.contains(value.value)
    }
  }

  protected def paramString(short: Boolean, f: (S) => String): String = {
    dates match {
      case None => ""
      case Some(Left(range)) => writeRange(short, Some(range), f)
      case Some(Right(domain)) => writeSet(short, domain, f)
    }
  }
  protected def typeString(): String = classTag[T].runtimeClass.getName
}

object DateSchema extends SchemaParameters {
  val Pattern = (Type.Date.name + """(?:\((?:(.*?))?\))?""").r

  def apply[T: ClassTag]()(implicit ev: T => Date): DateSchema[T] = DateSchema[T](None)
  def apply[T: ClassTag](lower: T, upper: T)(implicit ev: T => Date): DateSchema[T] = {
    DateSchema[T](Some(Left((lower, upper))))
  }
  def apply[T: ClassTag](domain: Set[T])(implicit ev: T => Date): DateSchema[T] = DateSchema[T](Some(Right(domain)))

  def fromShortString(str: String, codec: Codec): Option[DateSchema[codec.C]] = {
    (codec.tag, codec.date, str) match {
      case (Some(tag), Some(ev), Pattern(null)) => Some(DateSchema()(tag, ev))
      case (Some(tag), Some(ev), Pattern("")) => Some(DateSchema()(tag, ev))
      case (Some(tag), Some(ev), RangePattern(range)) =>
        parseRange(codec, range).map { case (lower, upper) => DateSchema(lower, upper)(tag, ev) }
      case (Some(tag), Some(ev), Pattern(domain)) => parseSet(codec, domain).map(DateSchema(_)(tag, ev))
      case _ => None
    }
  }

  private val RangePattern = (Type.Date.name + """(?:\((?:(.*?:.*))?\))?""").r
}

/** Schema for structured data variables. */
trait StructuredSchema extends Schema