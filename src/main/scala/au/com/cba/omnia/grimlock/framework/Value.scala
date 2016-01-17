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
private[encoding] trait CompareResult
private[encoding] case object GreaterEqual extends CompareResult
private[encoding] case object Greater extends CompareResult
private[encoding] case object Equal extends CompareResult
private[encoding] case object Less extends CompareResult
private[encoding] case object LessEqual extends CompareResult

/** Base trait for representing strutured data. */
trait Structured

/** Base trait for variable values. */
trait Value {
  /** The codex used to encode/decode this value. */
  val codex: Codex

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
  def toShortString: String = codex.encode(this)

  private def evaluate(that: Valueable, op: CompareResult): Boolean = {
    codex.compare(this, that()) match {
      case Some(0) => (op == Equal) || (op == GreaterEqual) || (op == LessEqual)
      case Some(x) if (x > 0) => (op == Greater) || (op == GreaterEqual)
      case Some(x) if (x < 0) => (op == Less) || (op == LessEqual)
      case _ => false
    }
  }
}

object Value {
  /** Define an ordering between 2 values. Only use with values of the same type. */
  val Ordering: Ordering[Value] = new Ordering[Value] {
    def compare(x: Value, y: Value): Int = {
      x.codex.compare(x, y).getOrElse(throw new Exception("unable to compare different values."))
    }
  }
}

/**
 * Value for when the data is of type `java.util.Date`
 *
 * @param value A `java.util.Date`.
 * @param codex The codex used for encoding/decoding `value`.
 */
case class DateValue(value: Date, codex: DateCodex) extends Value {
  override def asDate = Some(value)
}

/**
 * Value for when the data is of type `String`.
 *
 * @param value A `String`.
 */
case class StringValue(value: String) extends Value {
  val codex = StringCodex
  override def asString = Some(value.toString)
}

/**
 * Value for when the data is of type `Double`.
 *
 * @param value A `Double`.
 */
case class DoubleValue(value: Double) extends Value {
  val codex = DoubleCodex
  override def asDouble = Some(value)
}

/**
 * Value for when the data is of type `Long`.
 *
 * @param value A `Long`.
 */
case class LongValue(value: Long) extends Value {
  val codex = LongCodex
  override def asDouble = Some(value)
  override def asLong = Some(value)
}

/**
 * Value for when the data is of type `Boolean`.
 *
 * @param value A `Boolean`.
 */
case class BooleanValue(value: Boolean) extends Value {
  val codex = BooleanCodex
  override def asDouble = Some(if (value) 1 else 0)
  override def asLong = Some(if (value) 1 else 0)
  override def asBoolean = Some(value)
}

/**
 * Value for when the data is an event type.
 *
 * @param value An event.
 * @param codex The codex used for encoding/decoding `value`.
 */
case class StructuredValue[T <: Structured](value: T, codex: StructuredCodex) extends Value {
  override def asStructured = Some(value)
}

/** Trait for transforming a type `T` into a `Value`. */
trait Valueable extends java.io.Serializable {
  /** Returns a `Value` for this type `T`. */
  def apply(): Value
}

/** Companion object for the `Valueable` trait. */
object Valueable {
  /** Converts a `Value` to a `Value`; that is, it's a pass through. */
  implicit def VV[T <: Value](t: T): Valueable = new Valueable { def apply(): Value = t }

  /** Converts a `String` to a `Value`. */
  implicit def SV(t: String): Valueable = new Valueable { def apply(): Value = StringValue(t) }

  /** Converts a `Double` to a `Value`. */
  implicit def DV(t: Double): Valueable = new Valueable { def apply(): Value = DoubleValue(t) }

  /** Converts a `Long` to a `Value`. */
  implicit def LV(t: Long): Valueable = new Valueable { def apply(): Value = LongValue(t) }

  /** Converts a `Int` to a `Value`. */
  implicit def IV(t: Int): Valueable = new Valueable { def apply(): Value = LongValue(t) }

  /** Converts a `Boolean` to a `Value`. */
  implicit def BV(t: Boolean): Valueable = new Valueable { def apply(): Value = BooleanValue(t) }
}

