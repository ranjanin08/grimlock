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

package au.com.cba.omnia.grimlock.framework.encoding

import java.text.SimpleDateFormat

/** Base trait for encoding/decoding (basic) data types. */
trait Codex {
  /** Name of the codex. */
  val name: String

  /** The data type. */
  type T

  /** The specific type of value. */
  type V <: Value

  /**
   * Convert a basic data type to a value.
   *
   * @param value Data type to wrap in a value.
   */
  def toValue(value: T): V

  /**
   * Extract a basic data type from a value.
   *
   * @param value Value from which to extract the data.
   */
  def fromValue(value: Value): T

  /**
   * Decode a basic data type into a value.
   *
   * @param value String to decode into a value.
   *
   * @return `Some[Value]` if the decode was successful, `None` otherwise.
   */
  def decode(value: String): Option[V] = scala.util.Try(fromString(value)).toOption.map { case t => toValue(t) }

  /**
   * Converts a value to a consise (terse) string.
   *
   * @param t The value to convert to string.
   *
   * @return Short string representation of the value.
   */
  def encode(value: Value): String = toString(fromValue(value))

  /**
   * Compare two value objects.
   *
   * @param x The first value to compare.
   * @param y The second value to compare.
   *
   * @return `Some[Int]` if `x` and `y` can be compared, `None` otherwise. If successful, then the returned value
   *         is < 0 iff x < y, 0 iff x = y, > 0 iff x > y.
   */
  def compare(x: Value, y: Value): Option[Int]

  protected def fromString(value: String): T
  protected def toString(t: T): String
}

object Codex {
  /**
   * Return a codex from it's name.
   *
   * @param str The name of the codex.
   *
   * @return `Some[Codex]` if the name maps to a know codex, `None` otherwise.
   */
  def fromString[T](str: String): Option[Codex] = {
    str match {
      case DateCodex.pattern(format) => Some(DateCodex(format))
      case StringCodex.name => Some(StringCodex)
      case DoubleCodex.name => Some(DoubleCodex)
      case LongCodex.name => Some(LongCodex)
      case BooleanCodex.name => Some(BooleanCodex)
      case _ => None
    }
  }

  /** Shorthand for `DateCodex` type. */
  type DateCodex = au.com.cba.omnia.grimlock.framework.encoding.DateCodex
  /** Shorthand for `StringCodex` type. */
  type StringCodex = StringCodex.type
  /** Shorthand for `DoubleCodex` type. */
  type DoubleCodex = DoubleCodex.type
  /** Shorthand for `LongCodex` type. */
  type LongCodex = LongCodex.type
  /** Shorthand for `BooleanCodex` type. */
  type BooleanCodex = BooleanCodex.type
}

/** Codex for dealing with `java.util.Date`. */
case class DateCodex(format: String = "yyyy-MM-dd") extends Codex {
  val name = s"date(${format})"

  type T = java.util.Date
  type V = DateValue

  def toValue(value: T): V = DateValue(value, this)
  def fromValue(value: Value): T = value.asInstanceOf[V].value
  def compare(x: Value, y: Value): Option[Int] = {
    (x.asDate, y.asDate) match {
      case (Some(l), Some(r)) => Some(l.getTime().compare(r.getTime()))
      case _ => None
    }
  }

  protected def fromString(value: String): T = (new SimpleDateFormat(format)).parse(value)
  protected def toString(value: T): String = (new SimpleDateFormat(format)).format(value)
}

object DateCodex {
  val pattern = """date\((.*)\)""".r
}

/** Codex for dealing with `String`. */
case object StringCodex extends Codex {
  val name = "string"

  type T = String
  type V = StringValue

  def toValue(value: T): V = StringValue(value)
  def fromValue(value: Value): T = value.asInstanceOf[V].value
  def compare(x: Value, y: Value): Option[Int] = {
    (x.asString, y.asString) match {
      case (Some(l), Some(r)) => Some(l.compare(r))
      case _ => None
    }
  }

  protected def fromString(value: String): T = value
  protected def toString(value: T): String = value
}

/** Codex for dealing with `Double`. */
case object DoubleCodex extends Codex {
  val name = "double"

  type T = Double
  type V = DoubleValue

  def toValue(value: T): V = DoubleValue(value)
  def fromValue(value: Value): T = value.asInstanceOf[V].value
  def compare(x: Value, y: Value): Option[Int] = {
    (x.asDouble, y.asDouble) match {
      case (Some(l), Some(r)) => Some(l.compare(r))
      case _ => None
    }
  }

  protected def fromString(value: String): T = value.toDouble
  protected def toString(value: T): String = value.toString
}

/** Codex for dealing with `Long`. */
case object LongCodex extends Codex {
  val name = "long"

  type T = Long
  type V = LongValue

  def toValue(value: T): V = LongValue(value)
  def fromValue(value: Value): T = value.asInstanceOf[V].value
  def compare(x: Value, y: Value): Option[Int] = {
    (x.asLong, y.asLong) match {
      case (Some(l), Some(r)) => Some(l.compare(r))
      case _ => DoubleCodex.compare(x, y)
    }
  }

  protected def fromString(value: String): T = value.toLong
  protected def toString(value: T): String = value.toString
}

/** Codex for dealing with `Boolean`. */
case object BooleanCodex extends Codex {
  val name = "boolean"

  type T = Boolean
  type V = BooleanValue

  def toValue(value: T): V = BooleanValue(value)
  def fromValue(value: Value): T = value.asInstanceOf[V].value
  def compare(x: Value, y: Value): Option[Int] = {
    (x.asBoolean, y.asBoolean) match {
      case (Some(l), Some(r)) => Some(l.compare(r))
      case _ => None
    }
  }

  protected def fromString(value: String): T = value.toBoolean
  protected def toString(value: T): String = value.toString
}

/** Base trait for dealing with events. */
trait EventCodex extends Codex {
  type T <: Event
}

