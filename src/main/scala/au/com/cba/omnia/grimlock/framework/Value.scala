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

import java.util.Date

import scala.util.matching.Regex

/** Base trait for representing events. */
trait Event

/** Base trait for variable values. */
trait Value {
  /** The codex used to encode/decode this value. */
  val codex: Codex

  /**
   * Check for equality with `that`.
   *
   * @param that Value to compare against.
   */
  def equ[T: Valueable](that: T): Boolean = evaluate(that, Equal)

  /**
   * Check for in-equality with `that`.
   *
   * @param that Value to compare against.
   */
  def neq[T: Valueable](that: T): Boolean = !(this equ that)

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
  def lss[T: Valueable](that: T): Boolean = evaluate(that, Less)

  /**
   * Check if `this` is less or equal to `that`.
   *
   * @param that Value to compare against.
   *
   * @note If `that` is of a different type than `this`, then the result is always `false`.
   */
  def leq[T: Valueable](that: T): Boolean = evaluate(that, LessEqual)

  /**
   * Check if `this` is greater than `that`.
   *
   * @param that Value to compare against.
   *
   * @note If `that` is of a different type than `this`, then the result is always `false`.
   */
  def gtr[T: Valueable](that: T): Boolean = evaluate(that, Greater)

  /**
   * Check if `this` is greater or equal to `that`.
   *
   * @param that Value to compare against.
   *
   * @note If `that` is of a different type than `this`, then the result is always `false`.
   */
  def geq[T: Valueable](that: T): Boolean = evaluate(that, GreaterEqual)

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
  def asEvent: Option[Event] = None

  /** Return a consise (terse) string representation of a value. */
  def toShortString: String = codex.encode(this)

  /** Hetrogeneous comparison results. */
  private trait CompareResult
  private case object GreaterEqual extends CompareResult
  private case object Greater extends CompareResult
  private case object Equal extends CompareResult
  private case object Less extends CompareResult
  private case object LessEqual extends CompareResult

  private def evaluate[T: Valueable](that: T, op: CompareResult): Boolean = {
    codex.compare(this, implicitly[Valueable[T]].convert(that)) match {
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
      x.codex.compare(x, y) match {
        case Some(0) => 0
        case Some(x) if (x > 0) => 1
        case Some(x) if (x < 0) => -1
        case _ => throw new Exception("unable to compare different values.")
      }
    }
  }
}

/**
 * Value for when the data is of type `java.util.Date`
 *
 * @param value A `java.util.Date`.
 * @param codex The codex used for encoding/decoding `value`.
 */
case class DateValue(value: Date, codex: DateAndTimeCodex) extends Value {
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
  override def asBoolean = Some(value)
}

/**
 * Value for when the data is an event type.
 *
 * @param value An event.
 * @param codex The codex used for encoding/decoding `value`.
 */
case class EventValue[T <: Event](value: T, codex: EventCodex) extends Value {
  override def asEvent = Some(value)
}

/** Type class for transforming a type `T` into a `Value`. */
trait Valueable[T] extends java.io.Serializable {
  /**
   * Returns a `Value` for type `T`.
   *
   * @param t Object that can be converted to a `Value`.
   */
  def convert(t: T): Value
}

/** Companion object for the `Valueable` type class. */
object Valueable {
  /** Converts a `Value` to a `Value`; that is, it's a pass through. */
  implicit def VV[T <: Value]: Valueable[T] = new Valueable[T] { def convert(t: T): Value = t }
  /** Converts a `String` to a `Value`. */
  implicit def SV: Valueable[String] = new Valueable[String] { def convert(t: String): Value = StringValue(t) }
  /** Converts a `Double` to a `Value`. */
  implicit def DV: Valueable[Double] = new Valueable[Double] { def convert(t: Double): Value = DoubleValue(t) }
  /** Converts a `Long` to a `Value`. */
  implicit def LV: Valueable[Long] = new Valueable[Long] { def convert(t: Long): Value = LongValue(t) }
  /** Converts a `Int` to a `Value`. */
  implicit def IV: Valueable[Int] = new Valueable[Int] { def convert(t: Int): Value = LongValue(t) }
  /** Converts a `Boolean` to a `Value`. */
  implicit def BV: Valueable[Boolean] = new Valueable[Boolean] { def convert(t: Boolean): Value = BooleanValue(t) }
}

