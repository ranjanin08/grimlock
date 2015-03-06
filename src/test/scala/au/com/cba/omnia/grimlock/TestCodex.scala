// Copyright 2015 Commonwealth Bank of Australia
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

package au.com.cba.omnia.grimlock

import au.com.cba.omnia.grimlock.content._
import au.com.cba.omnia.grimlock.content.metadata._
import au.com.cba.omnia.grimlock.encoding._

import org.scalatest._

class TestDateCodex extends FlatSpec with Matchers {

  val dfmt = new java.text.SimpleDateFormat("yyyy-MM-dd")

  "A DateCodex" should "have a name" in {
    DateCodex.name should be ("date")
  }

  it should "box a correct Value" in {
    DateCodex.toValue(dfmt.parse("2001-01-01")) should be (DateValue(dfmt.parse("2001-01-01"), DateCodex))
  }

  it should "unbox a correct value" in {
    DateCodex.fromValue(DateValue(dfmt.parse("2001-01-01"), DateCodex)) should be (dfmt.parse("2001-01-01"))
  }

  it should "throw an exception for an invalid unbox" in {
    a [ClassCastException] should be thrownBy { DateCodex.fromValue(LongValue(1)) }
    a [ClassCastException] should be thrownBy { DateCodex.fromValue(DoubleValue(1)) }
    a [ClassCastException] should be thrownBy { DateCodex.fromValue(StringValue("a")) }
  }

  it should "decode a correct value" in {
    DateCodex.decode("2001-01-01") should be (Some(DateValue(dfmt.parse("2001-01-01"), DateCodex)))
  }

  it should "not decode an incorrect value" in {
    DateCodex.decode("a") should be (None)
    DateCodex.decode("1") should be (None)
  }

  it should "encode a correct value" in {
    DateCodex.encode(DateValue(dfmt.parse("2001-01-01"), DateCodex)) should be ("2001-01-01")
  }

  it should "not encode an incorrect value" in {
    a [ClassCastException] should be thrownBy { DateCodex.encode(LongValue(1)) }
    a [ClassCastException] should be thrownBy { DateCodex.encode(DoubleValue(1)) }
    a [ClassCastException] should be thrownBy { DateCodex.encode(StringValue("a")) }
  }

  it should "compare a correct value" in {
    DateCodex.compare(DateValue(dfmt.parse("2001-01-01"), DateCodex),
      DateValue(dfmt.parse("2002-01-01"), DateCodex)) should be (Some(-1))
    DateCodex.compare(DateValue(dfmt.parse("2001-01-01"), DateCodex),
      DateValue(dfmt.parse("2001-01-01"), DateCodex)) should be (Some(0))
    DateCodex.compare(DateValue(dfmt.parse("2002-01-01"), DateCodex),
      DateValue(dfmt.parse("2001-01-01"), DateCodex)) should be (Some(1))
  }

  it should "not compare an incorrect value" in {
    DateCodex.compare(DateValue(dfmt.parse("2001-01-01"), DateCodex), LongValue(1)) should be (None)
    DateCodex.compare(LongValue(1), DateValue(dfmt.parse("2001-01-01"), DateCodex)) should be (None)
  }
}

class TestDateTimeCodex extends FlatSpec with Matchers {

  val dfmt = new java.text.SimpleDateFormat("yyyy-MM-dd hh:ss:mm")

  "A DateTimeCodex" should "have a name" in {
    DateTimeCodex.name should be ("date.time")
  }

  it should "box a correct Value" in {
    DateTimeCodex.toValue(dfmt.parse("2001-01-01 01:01:01")) should
      be (DateValue(dfmt.parse("2001-01-01 01:01:01"), DateTimeCodex))
  }

  it should "unbox a correct value" in {
    DateTimeCodex.fromValue(DateValue(dfmt.parse("2001-01-01 01:01:01"), DateCodex)) should
      be (dfmt.parse("2001-01-01 01:01:01"))
  }

  it should "throw an exception for an invalid unbox" in {
    a [ClassCastException] should be thrownBy { DateTimeCodex.fromValue(LongValue(1)) }
    a [ClassCastException] should be thrownBy { DateTimeCodex.fromValue(DoubleValue(1)) }
    a [ClassCastException] should be thrownBy { DateTimeCodex.fromValue(StringValue("a")) }
  }

  it should "decode a correct value" in {
    DateTimeCodex.decode("2001-01-01 01:01:01") should
      be (Some(DateValue(dfmt.parse("2001-01-01 01:01:01"), DateTimeCodex)))
  }

  it should "not decode an incorrect value" in {
    DateTimeCodex.decode("a") should be (None)
    DateTimeCodex.decode("1") should be (None)
  }

  it should "encode a correct value" in {
    DateTimeCodex.encode(DateValue(dfmt.parse("2001-01-01 01:01:01"), DateCodex)) should be ("2001-01-01 01:01:01")
  }

  it should "not encode an incorrect value" in {
    a [ClassCastException] should be thrownBy { DateTimeCodex.encode(LongValue(1)) }
    a [ClassCastException] should be thrownBy { DateTimeCodex.encode(DoubleValue(1)) }
    a [ClassCastException] should be thrownBy { DateTimeCodex.encode(StringValue("a")) }
  }

  it should "compare a correct value" in {
    DateTimeCodex.compare(DateValue(dfmt.parse("2001-01-01 01:01:01"), DateTimeCodex),
      DateValue(dfmt.parse("2002-01-01 01:01:01"), DateTimeCodex)) should be (Some(-1))
    DateTimeCodex.compare(DateValue(dfmt.parse("2001-01-01 01:01:01"), DateTimeCodex),
      DateValue(dfmt.parse("2001-01-01 01:01:01"), DateTimeCodex)) should be (Some(0))
    DateTimeCodex.compare(DateValue(dfmt.parse("2002-01-01 01:01:01"), DateTimeCodex),
      DateValue(dfmt.parse("2001-01-01 01:01:01"), DateTimeCodex)) should be (Some(1))
  }

  it should "not compare an incorrect value" in {
    DateTimeCodex.compare(DateValue(dfmt.parse("2001-01-01 01:01:01"), DateTimeCodex), LongValue(1)) should be (None)
    DateTimeCodex.compare(LongValue(1), DateValue(dfmt.parse("2001-01-01 01:01:01"), DateTimeCodex)) should be (None)
  }
}

class TestStringCodex extends FlatSpec with Matchers {

  "A StringCodex" should "have a name" in {
    StringCodex.name should be ("string")
  }

  it should "box a correct Value" in {
    StringCodex.toValue("abc") should be (StringValue("abc"))
  }

  it should "unbox a correct value" in {
    StringCodex.fromValue(StringValue("abc")) should be ("abc")
  }

  it should "throw an exception for an invalid unbox" in {
    a [ClassCastException] should be thrownBy { StringCodex.fromValue(LongValue(1)) }
    a [ClassCastException] should be thrownBy { StringCodex.fromValue(DoubleValue(1)) }
    a [ClassCastException] should be thrownBy { StringCodex.fromValue(BooleanValue(true)) }
  }

  it should "decode a correct value" in {
    StringCodex.decode("abc") should be (Some(StringValue("abc")))
  }

  it should "encode a correct value" in {
    StringCodex.encode(StringValue("abc")) should be ("abc")
  }

  it should "not encode an incorrect value" in {
    a [ClassCastException] should be thrownBy { StringCodex.encode(LongValue(1)) }
    a [ClassCastException] should be thrownBy { StringCodex.encode(DoubleValue(1)) }
    a [ClassCastException] should be thrownBy { StringCodex.encode(BooleanValue(true)) }
  }

  it should "compare a correct value" in {
    StringCodex.compare(StringValue("abc"), StringValue("bbc")) should be (Some(-1))
    StringCodex.compare(StringValue("abc"), StringValue("abc")) should be (Some(0))
    StringCodex.compare(StringValue("bbc"), StringValue("abc")) should be (Some(1))
  }

  it should "not compare an incorrect value" in {
    StringCodex.compare(StringValue("abc"), LongValue(1)) should be (None)
    StringCodex.compare(LongValue(1), StringValue("abc")) should be (None)
  }
}

class TestDoubleCodex extends FlatSpec with Matchers {

  "A DoubleCodex" should "have a name" in {
    DoubleCodex.name should be ("double")
  }

  it should "box a correct Value" in {
    DoubleCodex.toValue(3.14) should be (DoubleValue(3.14))
  }

  it should "unbox a correct value" in {
    DoubleCodex.fromValue(DoubleValue(3.14)) should be (3.14)
  }

  it should "throw an exception for an invalid unbox" in {
    a [ClassCastException] should be thrownBy { DoubleCodex.fromValue(LongValue(1)) }
    a [ClassCastException] should be thrownBy { DoubleCodex.fromValue(StringValue("a")) }
    a [ClassCastException] should be thrownBy { DoubleCodex.fromValue(BooleanValue(true)) }
  }

  it should "decode a correct value" in {
    DoubleCodex.decode("3.14") should be (Some(DoubleValue(3.14)))
  }

  it should "not decode an incorrect value" in {
    DoubleCodex.decode("a") should be (None)
    DoubleCodex.decode("2001-01-01") should be (None)
  }

  it should "encode a correct value" in {
    DoubleCodex.encode(DoubleValue(3.14)) should be ("3.14")
  }

  it should "not encode an incorrect value" in {
    a [ClassCastException] should be thrownBy { DoubleCodex.encode(LongValue(1)) }
    a [ClassCastException] should be thrownBy { DoubleCodex.encode(StringValue("abc")) }
    a [ClassCastException] should be thrownBy { DoubleCodex.encode(BooleanValue(true)) }
  }

  it should "compare a correct value" in {
    DoubleCodex.compare(DoubleValue(3.14), DoubleValue(4.14)) should be (Some(-1))
    DoubleCodex.compare(DoubleValue(3.14), DoubleValue(3.14)) should be (Some(0))
    DoubleCodex.compare(DoubleValue(4.14), DoubleValue(3.14)) should be (Some(1))

    DoubleCodex.compare(DoubleValue(3), LongValue(4)) should be (Some(-1))
    DoubleCodex.compare(DoubleValue(3), LongValue(3)) should be (Some(0))
    DoubleCodex.compare(DoubleValue(4), LongValue(3)) should be (Some(1))

    DoubleCodex.compare(LongValue(3), DoubleValue(4)) should be (Some(-1))
    DoubleCodex.compare(LongValue(3), DoubleValue(3)) should be (Some(0))
    DoubleCodex.compare(LongValue(4), DoubleValue(3)) should be (Some(1))

    DoubleCodex.compare(LongValue(3), LongValue(4)) should be (Some(-1))
    DoubleCodex.compare(LongValue(3), LongValue(3)) should be (Some(0))
    DoubleCodex.compare(LongValue(4), LongValue(3)) should be (Some(1))
  }

  it should "not compare an incorrect value" in {
    DoubleCodex.compare(DoubleValue(3.14), StringValue("abc")) should be (None)
    DoubleCodex.compare(StringValue("abc"), DoubleValue(3.14)) should be (None)
  }
}

class TestLongCodex extends FlatSpec with Matchers {

  "A LongCodex" should "have a name" in {
    LongCodex.name should be ("long")
  }

  it should "box a correct Value" in {
    LongCodex.toValue(42) should be (LongValue(42))
  }

  it should "unbox a correct value" in {
    LongCodex.fromValue(LongValue(42)) should be (42)
  }

  it should "throw an exception for an invalid unbox" in {
    a [ClassCastException] should be thrownBy { LongCodex.fromValue(DoubleValue(3.14)) }
    a [ClassCastException] should be thrownBy { LongCodex.fromValue(StringValue("a")) }
    a [ClassCastException] should be thrownBy { LongCodex.fromValue(BooleanValue(true)) }
  }

  it should "decode a correct value" in {
    LongCodex.decode("42") should be (Some(LongValue(42)))
  }

  it should "not decode an incorrect value" in {
    LongCodex.decode("a") should be (None)
    LongCodex.decode("2001-01-01") should be (None)
  }

  it should "encode a correct value" in {
    LongCodex.encode(LongValue(42)) should be ("42")
  }

  it should "not encode an incorrect value" in {
    a [ClassCastException] should be thrownBy { LongCodex.encode(DoubleValue(3.14)) }
    a [ClassCastException] should be thrownBy { LongCodex.encode(StringValue("abc")) }
    a [ClassCastException] should be thrownBy { LongCodex.encode(BooleanValue(true)) }
  }

  it should "compare a correct value" in {
    LongCodex.compare(DoubleValue(42), DoubleValue(43)) should be (Some(-1))
    LongCodex.compare(DoubleValue(42), DoubleValue(42)) should be (Some(0))
    LongCodex.compare(DoubleValue(43), DoubleValue(42)) should be (Some(1))

    LongCodex.compare(DoubleValue(3), LongValue(4)) should be (Some(-1))
    LongCodex.compare(DoubleValue(3), LongValue(3)) should be (Some(0))
    LongCodex.compare(DoubleValue(4), LongValue(3)) should be (Some(1))

    LongCodex.compare(LongValue(3), DoubleValue(4)) should be (Some(-1))
    LongCodex.compare(LongValue(3), DoubleValue(3)) should be (Some(0))
    LongCodex.compare(LongValue(4), DoubleValue(3)) should be (Some(1))

    LongCodex.compare(LongValue(3), LongValue(4)) should be (Some(-1))
    LongCodex.compare(LongValue(3), LongValue(3)) should be (Some(0))
    LongCodex.compare(LongValue(4), LongValue(3)) should be (Some(1))
  }

  it should "not compare an incorrect value" in {
    LongCodex.compare(LongValue(42), StringValue("abc")) should be (None)
    LongCodex.compare(StringValue("abc"), LongValue(42)) should be (None)
  }
}

class TestBooleanCodex extends FlatSpec with Matchers {

  "A BooleanCodex" should "have a name" in {
    BooleanCodex.name should be ("boolean")
  }

  it should "box a correct Value" in {
    BooleanCodex.toValue(true) should be (BooleanValue(true))
  }

  it should "unbox a correct value" in {
    BooleanCodex.fromValue(BooleanValue(true)) should be (true)
  }

  it should "throw an exception for an invalid unbox" in {
    a [ClassCastException] should be thrownBy { BooleanCodex.fromValue(LongValue(1)) }
    a [ClassCastException] should be thrownBy { BooleanCodex.fromValue(DoubleValue(3.14)) }
    a [ClassCastException] should be thrownBy { BooleanCodex.fromValue(StringValue("a")) }
  }

  it should "decode a correct value" in {
    BooleanCodex.decode("true") should be (Some(BooleanValue(true)))
    BooleanCodex.decode("false") should be (Some(BooleanValue(false)))
  }

  it should "not decode an incorrect value" in {
    BooleanCodex.decode("a") should be (None)
    BooleanCodex.decode("2001-01-01") should be (None)
  }

  it should "encode a correct value" in {
    BooleanCodex.encode(BooleanValue(true)) should be ("true")
  }

  it should "not encode an incorrect value" in {
    a [ClassCastException] should be thrownBy { BooleanCodex.encode(LongValue(1)) }
    a [ClassCastException] should be thrownBy { BooleanCodex.encode(DoubleValue(3.14)) }
    a [ClassCastException] should be thrownBy { BooleanCodex.encode(StringValue("abc")) }
  }

  it should "compare a correct value" in {
    BooleanCodex.compare(BooleanValue(false), BooleanValue(true)) should be (Some(-1))
    BooleanCodex.compare(BooleanValue(false), BooleanValue(false)) should be (Some(0))
    BooleanCodex.compare(BooleanValue(true), BooleanValue(false)) should be (Some(1))
  }

  it should "not compare an incorrect value" in {
    BooleanCodex.compare(BooleanValue(true), StringValue("abc")) should be (None)
    BooleanCodex.compare(StringValue("abc"), BooleanValue(true)) should be (None)
  }
}

