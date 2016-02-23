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

import java.text.SimpleDateFormat
import java.util.Date

import scala.reflect._
import scala.util.Try

/** Base trait for encoding/decoding (basic) data types. */

trait Codec {
  /** The data type. */
  type C

  /**
    * Decode a basic data type into a value.
    *
    * @param str String to decode into a value.
    * @return `Some[Value]` if the decode was successful, `None` otherwise.
    */
  def decode(str: String): Option[Value { type V = C }]

  /**
    * Converts a value to a consise (terse) string.
    *
    * @param value The value to convert to string.
    * @return Short string representation of the value.
    */
  def encode(value: C): String

  /**
    * Compare two value objects.
    *
    * @param x The first value to compare.
    * @param y The second value to compare.
    * @return `Some[Int]` if `x` and `y` can be compared, `None` otherwise. If successful, then the returned value
    *         is < 0 iff x < y, 0 iff x = y, > 0 iff x > y.
    */
  def compare(x: Value, y: Value): Option[Int]

  def toShortString(): String

  def tag(): Option[ClassTag[C]]
  def date(): Option[C => Date] = None
  def numeric(): Option[Numeric[C]] = None
  def integral(): Option[Integral[C]] = None
}

object Codec {
  def fromShortString(str: String): Option[Codec] = {
    str match {
      case DateCodec.Pattern(_) => DateCodec.fromShortString(str)
      case StringCodec.Pattern() => StringCodec.fromShortString(str)
      case DoubleCodec.Pattern() => DoubleCodec.fromShortString(str)
      case LongCodec.Pattern() => LongCodec.fromShortString(str)
      case BooleanCodec.Pattern() => BooleanCodec.fromShortString(str)
      case _ => None
    }
  }
}

case class DateCodec(format: String = "yyyy-MM-dd") extends Codec {
  type C = Date

  def decode(value: String): Option[Value { type V = C }] = {
    Try(DateValue((new SimpleDateFormat(format)).parse(value), this)).toOption
  }
  def encode(value: C): String = (new SimpleDateFormat(format)).format(value)

  def compare(x: Value, y: Value): Option[Int] = {
    (x.asDate, y.asDate) match {
      case (Some(l), Some(r)) => Some(l.getTime().compare(r.getTime()))
      case _ => None
    }
  }

  def toShortString() = s"date(${format})"

  def tag(): Option[ClassTag[C]] = Some(classTag[C])
  override def date(): Option[C => Date] = Some(c => c)
}

object DateCodec {
  val Pattern = """date\((.*)\)""".r

  def fromShortString(str: String): Option[DateCodec] = {
    str match {
      case Pattern(format) => Some(DateCodec(format))
      case _ => None
    }
  }
}

case object StringCodec extends Codec {
  type C = String

  val Pattern = "string".r

  def decode(str: String): Option[Value { type V = C }] = Some(StringValue(str, this))
  def encode(value: C): String = value

  def compare(x: Value, y: Value): Option[Int] = {
    (x.asString, y.asString) match {
      case (Some(l), Some(r)) => Some(l.compare(r))
      case _ => None
    }
  }

  def toShortString() = "string"
  def fromShortString(str: String): Option[StringCodec.type] = {
    str match {
      case Pattern() => Some(StringCodec)
      case _ => None
    }
  }

  def tag(): Option[ClassTag[C]] = Some(classTag[C])
}

case object DoubleCodec extends Codec {
  type C = Double

  val Pattern = "double".r

  def decode(str: String): Option[Value { type V = C }] = Try(DoubleValue(str.toDouble, this)).toOption
  def encode(value: C): String = value.toString

  def compare(x: Value, y: Value): Option[Int] = {
    (x.asDouble, y.asDouble) match {
      case (Some(l), Some(r)) => Some(l.compare(r))
      case _ => None
    }
  }

  def toShortString() = "double"
  def fromShortString(str: String): Option[DoubleCodec.type] = {
    str match {
      case Pattern() => Some(DoubleCodec)
      case _ => None
    }
  }

  def tag(): Option[ClassTag[C]] = Some(classTag[C])
  override def numeric(): Option[Numeric[C]] = Some(Numeric.DoubleIsFractional)
}

case object LongCodec extends Codec {
  type C = Long

  val Pattern = "long|int".r

  def decode(str: String): Option[Value { type V = C }] = Try(LongValue(str.toLong, this)).toOption
  def encode(value: C): String = value.toString

  def compare(x: Value, y: Value): Option[Int] = {
    (x.asLong, y.asLong) match {
      case (Some(l), Some(r)) => Some(l.compare(r))
      case _ => DoubleCodec.compare(x, y)
    }
  }

  def toShortString() = "long"
  def fromShortString(str: String): Option[LongCodec.type] = {
    str match {
      case Pattern() => Some(LongCodec)
      case _ => None
    }
  }

  def tag(): Option[ClassTag[C]] = Some(classTag[C])
  override def numeric(): Option[Numeric[C]] = Some(Numeric.LongIsIntegral)
  override def integral(): Option[Integral[C]] = Some(Numeric.LongIsIntegral)
}

case object BooleanCodec extends Codec {
  type C = Boolean

  val Pattern = "boolean".r

  def decode(str: String): Option[Value { type V = C }] = Try(BooleanValue(str.toBoolean, this)).toOption
  def encode(value: C): String = value.toString

  def compare(x: Value, y: Value): Option[Int] = {
    (x.asBoolean, y.asBoolean) match {
      case (Some(l), Some(r)) => Some(l.compare(r))
      case _ => None
    }
  }

  def toShortString() = "boolean"
  def fromShortString(str: String): Option[BooleanCodec.type] = {
    str match {
      case Pattern() => Some(BooleanCodec)
      case _ => None
    }
  }

  def tag(): Option[ClassTag[C]] = Some(classTag[C])
}

trait StructuredCodec extends Codec {
  type T <: Structured
}
