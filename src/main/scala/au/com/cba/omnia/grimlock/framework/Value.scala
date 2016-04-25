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

package au.com.cba.omnia.grimlock.framework.encoding

import java.util.Date

import scala.util.matching.Regex

/** Hetrogeneous comparison results. */
sealed private trait CompareResult
private case object GreaterEqual extends CompareResult
private case object Greater extends CompareResult
private case object Equal extends CompareResult
private case object Less extends CompareResult
private case object LessEqual extends CompareResult

/** Base trait for representing strutured data. */
trait Structured

/** Base trait for variable values. */
trait Value {
  /** Type of  the value. */
  type V

  /** The encapsulated value. */
  val value: V

  /** The codec used to encode/decode this value. */
  val codec: Codec { type C = V }

  /**
   * Check for equality with `that`.
   *
   * @param that Value to compare against.
   */
  def equ(that: Valueable): Boolean = evaluate(that, Equal)

  /**
   * Check for in-equality with `that`.
   *
   * @param that Value to compare against.
   */
  def neq(that: Valueable): Boolean = !(this equ that)

  /**
   * Check for for match with `that` regular expression.
   *
   * @param that Regular expression to match against.
   *
   * @note This always applies `toShortString` method before matching.
   */
  def like(that: Regex): Boolean = that.pattern.matcher(this.toShortString).matches

  // Note: These next 4 methods implement comparison in a non-standard way when comparing two objects that can't be
  //       compared. In such cases the result is always false. This is the desired behaviour for the 'which' method
  //       (i.e. a filter).

  /**
   * Check if `this` is less than `that`.
   *
   * @param that Value to compare against.
   *
   * @note If `that` is of a different type than `this`, then the result is always `false`.
   */
  def lss(that: Valueable): Boolean = evaluate(that, Less)

  /**
   * Check if `this` is less or equal to `that`.
   *
   * @param that Value to compare against.
   *
   * @note If `that` is of a different type than `this`, then the result is always `false`.
   */
  def leq(that: Valueable): Boolean = evaluate(that, LessEqual)

  /**
   * Check if `this` is greater than `that`.
   *
   * @param that Value to compare against.
   *
   * @note If `that` is of a different type than `this`, then the result is always `false`.
   */
  def gtr(that: Valueable): Boolean = evaluate(that, Greater)

  /**
   * Check if `this` is greater or equal to `that`.
   *
   * @param that Value to compare against.
   *
   * @note If `that` is of a different type than `this`, then the result is always `false`.
   */
  def geq(that: Valueable): Boolean = evaluate(that, GreaterEqual)

  /** Return value as `java.util.Date`. */
  def asDate: Option[Date] = None

  /** Return value as `String`. */
  def asString: Option[String] = None

  /** Return value as `Double`. */
  def asDouble: Option[Double] = None

  /** Return value as `Long`. */
  def asLong: Option[Long] = None

  /** Return value as `Boolean`. */
  def asBoolean: Option[Boolean] = None

  /** Return value as event. */
  def asStructured: Option[Structured] = None

  /** Return a consise (terse) string representation of a value. */
  def toShortString: String = codec.encode(value)

  private def evaluate(that: Valueable, op: CompareResult): Boolean = {
    codec.compare(this, that()) match {
      case Some(0) => (op == Equal) || (op == GreaterEqual) || (op == LessEqual)
      case Some(x) if (x > 0) => (op == Greater) || (op == GreaterEqual)
      case Some(x) if (x < 0) => (op == Less) || (op == LessEqual)
      case _ => false
    }
  }
}

/** Compantion object to the `Value` trait. */
object Value {
  /** Define an ordering between 2 values. Only use with values of the same type. */
  val Ordering: Ordering[Value] = new Ordering[Value] {
    def compare(x: Value, y: Value): Int = {
      x.codec.compare(x, y).getOrElse(throw new Exception("unable to compare different values."))
    }
  }

  /**
   * Concatenates the string representation of two values.
   *
   * @param separator Separator to use between the strings.
   *
   * @return A function that concatenates values as a string.
   */
  def concatenate(separator: String): (Value, Value) => Valueable = {
    (left: Value, right: Value) => left.toShortString + separator + right.toShortString
  }

  /**
   * Parse a value from string.
   *
   * @param str The string to parse.
   * @param cdc The codec to decode with.
   *
   * @return A `Some[Value]` if successful, `None` otherwise.
   */
  def fromShortString(str: String, cdc: Codec): Option[Value { type V = cdc.C }] = cdc.decode(str)
}

/**
 * Value for when the data is of type `java.util.Date`
 *
 * @param value A `java.util.Date`.
 * @param codec The codec used for encoding/decoding `value`.
 */
case class DateValue(value: Date, codec: Codec { type C = Date } = DateCodec()) extends Value {
  type V = Date

  override def asDate = Some(value)
}

/** Companion object to `DateValue` case class. */
object DateValue {
  /** Convenience constructure that returns a `DateValue` from a date and format string. */
  def apply(value: Date, format: String): DateValue = DateValue(value, DateCodec(format))
}

/**
 * Value for when the data is of type `String`.
 *
 * @param value A `String`.
 * @param codec The codec used for encoding/decoding `value`.
 */
case class StringValue(value: String, codec: Codec { type C = String } = StringCodec) extends Value {
  type V = String

  override def asString = Some(value)
}

/**
 * Value for when the data is of type `Double`.
 *
 * @param value A `Double`.
 * @param codec The codec used for encoding/decoding `value`.
 */
case class DoubleValue(value: Double, codec: Codec { type C = Double } = DoubleCodec) extends Value {
  type V = Double

  override def asDouble = Some(value)
}

/**
 * Value for when the data is of type `Long`.
 *
 * @param value A `Long`.
 * @param codec The codec used for encoding/decoding `value`.
 */
case class LongValue(value: Long, codec: Codec { type C = Long } = LongCodec) extends Value {
  type V = Long

  override def asDouble = Some(value)
  override def asLong = Some(value)
}

/**
 * Value for when the data is of type `Boolean`.
 *
 * @param value A `Boolean`.
 * @param codec The codec used for encoding/decoding `value`.
 */
case class BooleanValue(value: Boolean, codec: Codec { type C = Boolean } = BooleanCodec) extends Value {
  type V = Boolean

  override def asDouble = Some(if (value) 1 else 0)
  override def asLong = Some(if (value) 1 else 0)
  override def asBoolean = Some(value)
}

/**
 * Value for when the data is an event type.
 *
 * @param value An event.
 * @param codec The codec used for encoding/decoding `value`.
 */
case class StructuredValue[U <: Structured, C <: StructuredCodec { type C = U }](value: U, codec: C) extends Value {
  type V = U

  override def asStructured = Some(value)
}

/** Trait for transforming a type `T` into a `Value`. */
trait Valueable extends java.io.Serializable {
  /** Type of the actual value. */
  type T

  /** Return a `Value` for this type `T`. */
  def apply(): Value { type V = T }
}

/** Companion object to the `Valuable` trait. */
object Valueable {
  /** Converts a `Value` to a `Value`; that is, it's a pass through. */
  implicit def VV[T <: Value](t: T): Valueable { type T = t.V } = {
    new Valueable {
      type T = t.V

      def apply(): Value { type V = T } = t
    }
  }

  /** Converts a `String` to a `Value`. */
  implicit def SV(t: String): Valueable { type T = String } = {
    new Valueable {
      type T = String

      def apply(): Value { type V = T } = StringValue(t)
    }
  }

  /** Converts a `Double` to a `Value`. */
  implicit def DV(t: Double): Valueable { type T = Double } = {
    new Valueable {
      type T = Double

      def apply(): Value { type V = T } = DoubleValue(t)
    }
  }

  /** Converts a `Long` to a `Value`. */
  implicit def LV(t: Long): Valueable { type T = Long } = {
    new Valueable {
      type T = Long

      def apply(): Value { type V = T } = LongValue(t)
    }
  }

  /** Converts a `Int` to a `Value`. */
  implicit def IV(t: Int): Valueable { type T = Long } = {
    new Valueable {
      type T = Long

      def apply(): Value { type V = T } = LongValue(t)
    }
  }

  /** Converts a `Boolean` to a `Value`. */
  implicit def BV(t: Boolean): Valueable { type T = Boolean } = {
    new Valueable {
      type T = Boolean

      def apply(): Value { type V = T } = BooleanValue(t)
    }
  }
}

