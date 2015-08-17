// Copyright 2014,2015 Commonwealth Bank of Australia
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

import au.com.cba.omnia.grimlock.framework._
import au.com.cba.omnia.grimlock.framework.content._
import au.com.cba.omnia.grimlock.framework.encoding._

/** Base trait for variable schemas. */
trait Schema {
  /** The type of variable. */
  val kind: Type
  /** The codex used to encode/decode a variable value. */
  val codex: Codex
  /** The name of the schema. */
  val name: String

  /**
   * Check if a value is valid according to this schema.
   *
   * @param value The value to validate.
   */
  def isValid[V <: Value](value: V): Boolean

  /**
   * Decode a string into a content.
   *
   * @param str The string to decode
   *
   * @return `Some[Content]` if the string can successfull be decoded and is valid according to this schema, `None`
   *         otherwise.
   */
  def decode(str: String): Option[Content] = {
    codex.decode(str) match {
      case Some(v) if isValid(v) => Some(Content(Schema.this, v))
      case _ => None
    }
  }

  /**
   * Return a consise (terse) string representation of a schema.
   *
   * @param separator Separator to use between various fields.
   */
  def toShortString(separator: String): String = kind.name + enclose(params()) + separator + codex.name

  override def toString(): String = {
    val par = params()
    val sep = if (par.isEmpty) "" else ","

    name + enclose(codex.toString + sep + par)
  }

  protected def params(): String

  private def enclose(str: String): String = if (str.isEmpty) str else "(" + str + ")"
}

object Schema {
  /**
   * Convert a codex and type string to a schema.
   *
   * @param codex The string name of the codex.
   * @param kind  The string name of the variable type.
   *
   * @return `Some[Schema]` if an approriate schema is found, `None` otherwise.
   */
  def fromString(codex: String, kind: String): Option[Schema] = {
    (kind, codex) match {
      case (Type.Continuous.name, DoubleCodex.name) => Some(ContinuousSchema(DoubleCodex))
      case (Type.Continuous.name, LongCodex.name) => Some(ContinuousSchema(LongCodex))
      case (Type.Discrete.name, LongCodex.name) => Some(DiscreteSchema(LongCodex))
      case (Type.Nominal.name, StringCodex.name) => Some(NominalSchema(StringCodex))
      case (Type.Nominal.name, DoubleCodex.name) => Some(NominalSchema(DoubleCodex))
      case (Type.Nominal.name, LongCodex.name) => Some(NominalSchema(LongCodex))
      case (Type.Ordinal.name, StringCodex.name) => Some(OrdinalSchema(StringCodex))
      case (Type.Ordinal.name, DoubleCodex.name) => Some(OrdinalSchema(DoubleCodex))
      case (Type.Ordinal.name, LongCodex.name) => Some(OrdinalSchema(LongCodex))
      case (ContinuousSchema.pattern(l, u), DoubleCodex.name) => ContinuousSchema.fromString(l, u, DoubleCodex)
      case (ContinuousSchema.pattern(l, u), LongCodex.name) => ContinuousSchema.fromString(l, u, LongCodex)
      case (DiscreteSchema.pattern(l, u, s), LongCodex.name) => DiscreteSchema.fromString(l, u, s, LongCodex)
      case (NominalSchema.pattern(p), StringCodex.name) => NominalSchema.fromString(p, StringCodex)
      case (NominalSchema.pattern(p), DoubleCodex.name) => NominalSchema.fromString(p, DoubleCodex)
      case (NominalSchema.pattern(p), LongCodex.name) => NominalSchema.fromString(p, LongCodex)
      case (OrdinalSchema.pattern(p), StringCodex.name) => OrdinalSchema.fromString(p, StringCodex)
      case (OrdinalSchema.pattern(p), DoubleCodex.name) => OrdinalSchema.fromString(p, DoubleCodex)
      case (OrdinalSchema.pattern(p), LongCodex.name) => OrdinalSchema.fromString(p, LongCodex)
      case (Type.Date.name, DateCodex.pattern(format)) => Some(DateSchema(DateCodex(format)))
      case _ => None
    }
  }

  /**
   * Parse a string value according to a codex.
   *
   * @param param Parameter to parse
   * @param codex The codex to parse with.
   *
   * @return An `Option` of type `C#T`
   */
  def parse[C <: Codex](param: String, codex: C): Option[C#T] = codex.decode(param).map(codex.fromValue(_))
}

/** Base trait for schemas for numerical variables. */
trait NumericalSchema[T] extends Schema {
  /** Optional minimum value of the variable. */
  val minimum: Option[T]
  /** Optional maximum value of the variable. */
  val maximum: Option[T]
}

/**
 * Schema for continuous variables.
 *
 * @param codex   The codex for the value.
 * @param minimum The optional minimum value.
 * @param maximum The optional maximum value.
 *
 * @note The constructor is private to ensure a clean interface as provided by the `apply` methods of the companion
 *       object.
 */
case class ContinuousSchema[C <: Codex] private (codex: C, minimum: Option[C#T], maximum: Option[C#T])(
  implicit num: Numeric[C#T]) extends NumericalSchema[C#T] {
  val kind = Type.Continuous
  val name = "ContinuousSchema"

  def isValid[V <: Value](value: V): Boolean = {
    import num._

    (codex.fromValue(value), minimum, maximum) match {
      case (v, Some(l), Some(u)) => (v >= l && v <= u)
      case (v, None, None) => true
      case _ => false
    }
  }

  protected def params(): String = {
    (minimum, maximum) match {
      case (Some(l), Some(u)) => l.toString + "," + u.toString
      case _ => ""
    }
  }
}

/** Companion object to `ContinuousSchema`. */
object ContinuousSchema {
  /**
   * Construct a continuous schema with unbounded range.
   *
   * @param codex The codex for the value.
   */
  def apply[C <: Codex](codex: C)(implicit num: Numeric[C#T]): ContinuousSchema[C] = ContinuousSchema(codex, None, None)

  /**
   * Construct a continuous schema with bounded range.
   *
   * @param codex   The codex for the value.
   * @param minimum The lower bound (minimum value).
   * @param maximum The upper bound (maximum value).
   */
  def apply[C <: Codex](codex: C, minimum: C#T, maximum: C#T)(implicit num: Numeric[C#T]): ContinuousSchema[C] = {
    ContinuousSchema(codex, Some(minimum), Some(maximum))
  }

  /** Pattern for matching short string continuous value with parameters. */
  val pattern = (Type.Continuous + """\((.*),(.*)\)""").r

  /**
   * Parse continuous schema parameters
   *
   * @param lower Lower bound for the schema.
   * @param upper Upper bound for the schema.
   * @param codex The codex to parse with.
   *
   * @return An `Option` of `ContinuousSchema` if all parameters parse correctly, `None` otherwise.
   */
  def fromString[C <: Codex](lower: String, upper: String, codex: C)(
    implicit num: Numeric[C#T]): Option[ContinuousSchema[C]] = {
    (Schema.parse(lower, codex), Schema.parse(upper, codex)) match {
      case (Some(l), Some(u)) => Some(ContinuousSchema(codex, l, u))
      case _ => None
    }
  }
}

/**
 * Schema for discrete variables.
 *
 * @param codex   The codex for the value.
 * @param minimum The optional minimum value.
 * @param maximum The optional maximum value.
 * @param step    The optional step size.
 *
 * @note The constructor is private to ensure a clean interface as provided by the `apply` methods of the companion
 *       object.
 */
case class DiscreteSchema[C <: Codex] private (codex: C, minimum: Option[C#T], maximum: Option[C#T], step: Option[C#T])(
  implicit int: Integral[C#T]) extends NumericalSchema[C#T] {
  val kind = Type.Discrete
  val name = "DiscreteSchema"

  def isValid[V <: Value](value: V): Boolean = {
    import int._

    (codex.fromValue(value), minimum, maximum, step) match {
      case (v, Some(l), Some(u), Some(s)) => (v >= l && v <= u && (v % s) == 0)
      case (v, None, None, None) => true
      case _ => false
    }
  }

  protected def params(): String = {
    (minimum, maximum, step) match {
      case (Some(l), Some(u), Some(s)) => l.toString + "," + u.toString + "," + s.toString
      case _ => ""
    }
  }
}

/** Companion object to `DiscreteSchema`. */
object DiscreteSchema {
  /**
   * Construct a discrete schema with unbounded range and step size 1.
   *
   * @param codex The codex for the value.
   */
  def apply[C <: Codex](codex: C)(implicit int: Integral[C#T]): DiscreteSchema[C] = {
    DiscreteSchema(codex, None, None, None)
  }

  /**
   * Construct a discrete schema with bounded range and step size.
   *
   * @param codex   The codex for the value.
   * @param minimum The lower bound (minimum value).
   * @param maximum The upper bound (maximum value).
   * @param step    The step size.
   */
  def apply[C <: Codex](codex: C, minimum: C#T, maximum: C#T, step: C#T)(
    implicit int: Integral[C#T]): DiscreteSchema[C] = DiscreteSchema(codex, Some(minimum), Some(maximum), Some(step))

  /** Pattern for matching short string discrete value with parameters. */
  val pattern = (Type.Discrete + """\((.*),(.*),(.*)\)""").r

  /**
   * Parse discrete schema parameters
   *
   * @param lower Lower bound for the schema.
   * @param upper Upper bound for the schema.
   * @param step  Step size for the schema.
   * @param codex The codex to parse with.
   *
   * @return An `Option` of `DiscreteSchema` if all parameters parse correctly, `None` otherwise.
   */
  def fromString[C <: Codex](lower: String, upper: String, step: String, codex: C)(
    implicit num: Integral[C#T]): Option[DiscreteSchema[C]] = {
    (Schema.parse(lower, codex), Schema.parse(upper, codex), Schema.parse(step, codex)) match {
      case (Some(l), Some(u), Some(s)) => Some(DiscreteSchema(codex, l, u, s))
      case _ => None
    }
  }
}

/** Base trait for schemas for categorical variables. */
trait CategoricalSchema[T] extends Schema {
  /** Optional values the variable can take. */
  val domain: Option[List[T]]

  def isValid[V <: Value](value: V): Boolean = {
    (codex.fromValue(value), domain) match {
      case (v, Some(d)) => d.contains(v)
      case (v, None) => true
      case _ => false
    }
  }
}

/**
 * Schema for nominal variables.
 *
 * @param codex  The codex for the value.
 * @param domain The optional values of the variable.
 *
 * @note The constructor is private to ensure a clean interface as provided by the `apply` methods of the companion
 *       object.
 */
case class NominalSchema[C <: Codex] private (codex: C, domain: Option[List[C#T]]) extends CategoricalSchema[C#T] {
  val kind = Type.Nominal
  val name = "NominalSchema"

  protected def params(): String = {
    domain match {
      case Some(d) => d.mkString(",")
      case _ => ""
    }
  }
}

/** Companion object to `NominalSchema`. */
object NominalSchema {
  /**
   * Construct a nominal schema that can take on any value.
   *
   * @param codex The codex for the value.
   */
  def apply[C <: Codex](codex: C): NominalSchema[C] = NominalSchema(codex, None)

  /**
   * Construct a nominal schema.
   *
   * @param codex  The codex for the value.
   * @param domain A list of values the variable can take on.
   */
  def apply[C <: Codex](codex: C, domain: List[C#T]): NominalSchema[C] = NominalSchema(codex, Some(domain))

  /** Pattern for matching short string nominal value with parameters. */
  val pattern = (Type.Nominal + """\((.*)\)""").r

  /**
   * Parse nominal schema parameters
   *
   * @param params String of comma separated values.
   * @param codex  The codex to parse with.
   *
   * @return An `Option` of `NominalSchema` if all parameters parse correctly, `None` otherwise.
   */
  def fromString[C <: Codex](params: String, codex: C): Option[NominalSchema[C]] = {
    // TODO: This fails for values with ',' in them
    val domain = params.split(",").map(Schema.parse(_, codex)).toList

    domain.exists(_.isEmpty) match {
      case true => None
      case false => Some(NominalSchema(codex, domain.flatten))
    }
  }
}

/**
 * Schema for ordinal variables.
 *
 * @param codex  The codex for the value.
 * @param domain The optional values of the variable.
 *
 * @note The constructor is private to ensure a clean interface as provided by the `apply` methods of the companion
 *       object.
 */
case class OrdinalSchema[C <: Codex] private (codex: C, domain: Option[List[C#T]]) extends CategoricalSchema[C#T] {
  val kind = Type.Ordinal
  val name = "OrdinalSchema"

  protected def params(): String = {
    domain match {
      case Some(d) => d.mkString(",")
      case _ => ""
    }
  }
}

/** Companion object to `OrdinalSchema`. */
object OrdinalSchema {
  /**
   * Construct a ordinal schema that can take on any value.
   *
   * @param codex The codex for the value.
   */
  def apply[C <: Codex](codex: C): OrdinalSchema[C] = OrdinalSchema(codex, None)

  /**
   * Construct a ordinal schema.
   *
   * @param codex  The codex for the value.
   * @param domain A list of values the variable can take on.
   */
  def apply[C <: Codex](codex: C, domain: List[C#T]): OrdinalSchema[C] = OrdinalSchema(codex, Some(domain))

  /** Pattern for matching short string ordinal value with parameters. */
  val pattern = (Type.Ordinal + """\((.*)\)""").r

  /**
   * Parse ordinal schema parameters
   *
   * @param params String of comma separated values.
   * @param codex  The codex to parse with.
   *
   * @return An `Option` of `OrdinalSchema` if all parameters parse correctly, `None` otherwise.
   */
  def fromString[C <: Codex](params: String, codex: C): Option[OrdinalSchema[C]] = {
    // TODO: This fails for values with ',' in them
    val domain = params.split(",").map(Schema.parse(_, codex)).toList

    domain.exists(_.isEmpty) match {
      case true => None
      case false => Some(OrdinalSchema(codex, domain.flatten))
    }
  }
}

/** Schema for date variables. */
case class DateSchema[C <: DateCodex](codex: C) extends Schema {
  val kind = Type.Date
  val name = "DateSchema"

  def isValid[V <: Value](value: V): Boolean = codex.fromValue(value).isInstanceOf[java.util.Date]

  protected def params(): String = ""
}

/** Schema for event variables. */
case class EventSchema[C <: EventCodex](codex: C) extends Schema {
  val kind = Type.Event
  val name = "EventSchema"

  def isValid[V <: Value](value: V): Boolean = codex.fromValue(value).isInstanceOf[Event]

  protected def params(): String = ""
}

