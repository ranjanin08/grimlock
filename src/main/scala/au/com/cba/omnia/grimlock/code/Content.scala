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

package au.com.cba.omnia.grimlock.content

import au.com.cba.omnia.grimlock._
import au.com.cba.omnia.grimlock.encoding._
import au.com.cba.omnia.grimlock.content.metadata._

import com.twitter.scalding._

/** Contents of a cell in a matrix. */
trait Content {
  /** Schema (description) of the value. */
  val schema: Schema
  /** The value of the variable. */
  val value: Value

  override def toString(): String = "Content(" + schema.toString + "," + value.toString + ")"

  /**
   * Converts the content to a consise (terse) string.
   *
   * @param separator The separator to use between the fields.
   *
   * @return Short string representation.
   */
  def toShortString(separator: String): String = schema.toShortString(separator) + separator + value.toShortString
}

object Content {
  /** Standard `unapply` method for pattern matching. */
  def unapply(con: Content): Option[(Schema, Value)] = Some((con.schema, con.value))

  /**
   * Construct a content using a continuous schema and numeric value.
   *
   * @param schema Schema of the variable value.
   * @param value  Numeric value of the variable.
   */
  def apply[T](schema: ContinuousSchema[Codex.DoubleCodex], value: T)(implicit num: Numeric[T]): Content = {
    import num._
    ContentImpl(schema, DoubleValue(value.toDouble))
  }

  /**
   * Construct a content using a continuous schema and integral value.
   *
   * @param schema Schema of the variable value.
   * @param value  Integral value of the variable.
   */
  def apply[T](schema: ContinuousSchema[Codex.LongCodex], value: T)(implicit num: Integral[T]): Content = {
    import num._
    ContentImpl(schema, LongValue(value.toLong))
  }

  /**
   * Construct a content using a discrete schema and integral value.
   *
   * @param schema Schema of the variable value.
   * @param value  Integral value of the variable.
   */
  def apply[T](schema: DiscreteSchema[Codex.LongCodex], value: T)(implicit num: Integral[T]): Content = {
    import num._
    ContentImpl(schema, LongValue(value.toLong))
  }

  /**
   * Construct a content using a nominal schema and value.
   *
   * @param schema Schema of the variable value.
   * @param value  Value of the variable.
   *
   * @note The value is converted, and stored, as `String`.
   */
  def apply[T](schema: NominalSchema[Codex.StringCodex], value: T): Content = {
    ContentImpl(schema, StringValue(value.toString))
  }

  /**
   * Construct a content using a nominal schema and numeric value.
   *
   * @param schema Schema of the variable value.
   * @param value  Numeric value of the variable.
   */
  def apply[T](schema: NominalSchema[Codex.DoubleCodex], value: T)(implicit num: Numeric[T]): Content = {
    import num._
    ContentImpl(schema, DoubleValue(value.toDouble))
  }

  /**
   * Construct a content using a nominal schema and integral value.
   *
   * @param schema Schema of the variable value.
   * @param value  Integral value of the variable.
   */
  def apply[T](schema: NominalSchema[Codex.LongCodex], value: T)(implicit num: Integral[T]): Content = {
    import num._
    ContentImpl(schema, LongValue(value.toLong))
  }

  /**
   * Construct a content using a ordinal schema and value.
   *
   * @param schema Schema of the variable value.
   * @param value  Value of the variable.
   *
   * @note The value is converted, and stored, as `String`.
   */
  def apply[T](schema: OrdinalSchema[Codex.StringCodex], value: T): Content = {
    ContentImpl(schema, StringValue(value.toString))
  }

  /**
   * Construct a content using a ordinal schema and numeric value.
   *
   * @param schema Schema of the variable value.
   * @param value  Numeric value of the variable.
   */
  def apply[T](schema: OrdinalSchema[Codex.DoubleCodex], value: T)(implicit num: Numeric[T]): Content = {
    import num._
    ContentImpl(schema, DoubleValue(value.toDouble))
  }

  /**
   * Construct a content using a ordinal schema and integral value.
   *
   * @param schema Schema of the variable value.
   * @param value  Integral value of the variable.
   */
  def apply[T](schema: OrdinalSchema[Codex.LongCodex], value: T)(implicit num: Integral[T]): Content = {
    import num._
    ContentImpl(schema, LongValue(value.toLong))
  }

  /**
   * Construct a content using a date schema and `java.util.Date` value.
   *
   * @param schema Schema of the variable value.
   * @param value  Date value of the variable.
   */
  def apply[T <: DateAndTimeCodex](schema: DateSchema[T], value: java.util.Date): Content = {
    ContentImpl(schema, DateValue(value, schema.codex))
  }

  private case class ContentImpl(schema: Schema, value: Value) extends Content
}

/**
 * Rich wrapper around a `TypedPipe[Content]`.
 *
 * @param data The `TypedPipe[Content]`.
 */
class Contents(protected val data: TypedPipe[Content]) extends Persist[Content] {
  protected def toString(t: Content, separator: String, descriptive: Boolean): String = {
    if (descriptive) { t.toString } else { t.toShortString(separator) }
  }
}

/** Companion object for the `Contents` class. */
object Contents {
  /** Converts a `TypedPipe[Content]` to a `Contents`. */
  implicit def TPC2C(data: TypedPipe[Content]): Contents = new Contents(data)
}

