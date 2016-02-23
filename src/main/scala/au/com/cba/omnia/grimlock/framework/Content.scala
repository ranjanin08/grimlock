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

package au.com.cba.omnia.grimlock.framework.content

import java.util.regex.Pattern

import au.com.cba.omnia.grimlock.framework._
import au.com.cba.omnia.grimlock.framework.encoding._
import au.com.cba.omnia.grimlock.framework.content.metadata._

/** Contents of a cell in a matrix. */
trait Content {
  type T

  /** Schema (description) of the value. */
  val schema: Schema { type S = T }

  /** The value of the variable. */
  val value: Value { type V = T }

  def isValid() = schema.validate(value)

  override def toString(): String = "Content(" + schema.toString + "," + value.toString + ")"

  /**
    * Converts the content to a consise (terse) string.
    *
    * @return Short string representation.
    */
  def toShortString(): String = value.toShortString

  /**
    * Converts the content to a consise (terse) string.
    *
    * @param separator The separator to use between the fields.
    * @return Short string representation.
    */
  def toShortString(separator: String): String = {
    value.codec.toShortString + separator + schema.toShortString(value.codec) + separator + value.toShortString
  }
}

object Content {
  type Parser = (String) => Option[Content]

  /**
    * Construct a content using a continuous schema and numeric value.
    *
    * @param schema Schema of the variable value.
    * @param value  Numeric value of the variable.
    */
  def apply[X](schema: Schema { type S = X }, value: Valueable { type T = X }): Content = ContentImpl(schema, value())

  def unapply(con: Content): Option[(Schema, Value)] = Some((con.schema, con.value))

  def parse[T](codec: Codec { type C = T }, schema: Schema { type S = T }): Parser = {
    (str: String) => {
      codec
        .decode(str)
        .flatMap {
          case v if (schema.validate(v)) => Some(Content(schema, v))
          case _ => None
        }
    }
  }

  def fromShortString(str: String, separator: String = "|"): Option[Content] = {
    str.split(Pattern.quote(separator)) match {
      case Array(c, s, v) =>
        Codec.fromShortString(c)
          .flatMap {
            case codec =>
              Schema.fromShortString(codec, s).flatMap { case schema => Content.parse[codec.C](codec, schema)(v) }
          }
      case _ => None
    }
  }

  def toString(separator: String = "|", descriptive: Boolean = false): (Content) => TraversableOnce[String] = {
    (t: Content) => if (descriptive) { Some(t.toString) } else { Some(t.toShortString(separator)) }
  }
}

private case class ContentImpl[X](schema: Schema { type S = X }, value: Value { type V = X }) extends Content {
  type T = X
}
