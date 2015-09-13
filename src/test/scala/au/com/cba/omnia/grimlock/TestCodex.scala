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

import au.com.cba.omnia.grimlock.framework.encoding._

class TestDateCodex extends TestGrimlock {

  val dfmt = new java.text.SimpleDateFormat("yyyy-MM-dd hh:ss:mm")

  "A DateCodex" should "have a name" in {
    DateCodex("yyyy-MM-dd hh:ss:mm").name shouldBe "date(yyyy-MM-dd hh:ss:mm)"
  }

  it should "box a correct Value" in {
    DateCodex("yyyy-MM-dd hh:ss:mm").toValue(dfmt.parse("2001-01-01 01:01:01")) shouldBe
      DateValue(dfmt.parse("2001-01-01 01:01:01"), DateCodex("yyyy-MM-dd hh:ss:mm"))
  }

  it should "unbox a correct value" in {
    DateCodex("yyyy-MM-dd hh:ss:mm")
      .fromValue(DateValue(dfmt.parse("2001-01-01 01:01:01"), DateCodex("yyyy-MM-dd hh:ss:mm"))) shouldBe
        dfmt.parse("2001-01-01 01:01:01")
  }

  it should "throw an exception for an invalid unbox" in {
    a [ClassCastException] should be thrownBy { DateCodex("yyyy-MM-dd hh:ss:mm").fromValue(LongValue(1)) }
    a [ClassCastException] should be thrownBy { DateCodex("yyyy-MM-dd hh:ss:mm").fromValue(DoubleValue(1)) }
    a [ClassCastException] should be thrownBy { DateCodex("yyyy-MM-dd hh:ss:mm").fromValue(StringValue("a")) }
  }

  it should "decode a correct value" in {
    DateCodex("yyyy-MM-dd hh:ss:mm").decode("2001-01-01 01:01:01") shouldBe
      Some(DateValue(dfmt.parse("2001-01-01 01:01:01"), DateCodex("yyyy-MM-dd hh:ss:mm")))
  }

  it should "not decode an incorrect value" in {
    DateCodex("yyyy-MM-dd hh:ss:mm").decode("a") shouldBe None
    DateCodex("yyyy-MM-dd hh:ss:mm").decode("1") shouldBe None
  }

  it should "encode a correct value" in {
    DateCodex("yyyy-MM-dd hh:ss:mm")
      .encode(DateValue(dfmt.parse("2001-01-01 01:01:01"), DateCodex("yyyy-MM-dd hh:ss:mm"))) shouldBe
        "2001-01-01 01:01:01"
  }

  it should "not encode an incorrect value" in {
    a [ClassCastException] should be thrownBy { DateCodex("yyyy-MM-dd hh:ss:mm").encode(LongValue(1)) }
    a [ClassCastException] should be thrownBy { DateCodex("yyyy-MM-dd hh:ss:mm").encode(DoubleValue(1)) }
    a [ClassCastException] should be thrownBy { DateCodex("yyyy-MM-dd hh:ss:mm").encode(StringValue("a")) }
  }

  it should "compare a correct value" in {
    DateCodex("yyyy-MM-dd hh:ss:mm")
      .compare(DateValue(dfmt.parse("2001-01-01 01:01:01"), DateCodex("yyyy-MM-dd hh:ss:mm")),
        DateValue(dfmt.parse("2002-01-01 01:01:01"), DateCodex("yyyy-MM-dd hh:ss:mm"))) shouldBe Some(-1)
    DateCodex("yyyy-MM-dd hh:ss:mm")
      .compare(DateValue(dfmt.parse("2001-01-01 01:01:01"), DateCodex("yyyy-MM-dd hh:ss:mm")),
        DateValue(dfmt.parse("2001-01-01 01:01:01"), DateCodex("yyyy-MM-dd hh:ss:mm"))) shouldBe Some(0)
    DateCodex("yyyy-MM-dd hh:ss:mm")
      .compare(DateValue(dfmt.parse("2002-01-01 01:01:01"), DateCodex("yyyy-MM-dd hh:ss:mm")),
        DateValue(dfmt.parse("2001-01-01 01:01:01"), DateCodex("yyyy-MM-dd hh:ss:mm"))) shouldBe Some(1)
  }

  it should "not compare an incorrect value" in {
    DateCodex("yyyy-MM-dd hh:ss:mm")
      .compare(DateValue(dfmt.parse("2001-01-01 01:01:01"), DateCodex("yyyy-MM-dd hh:ss:mm")), LongValue(1)) shouldBe
        None
    DateCodex("yyyy-MM-dd hh:ss:mm")
      .compare(LongValue(1), DateValue(dfmt.parse("2001-01-01 01:01:01"), DateCodex("yyyy-MM-dd hh:ss:mm"))) shouldBe
        None
  }
}

class TestStringCodex extends TestGrimlock {

  "A StringCodex" should "have a name" in {
    StringCodex.name shouldBe "string"
  }

  it should "box a correct Value" in {
    StringCodex.toValue("abc") shouldBe StringValue("abc")
  }

  it should "unbox a correct value" in {
    StringCodex.fromValue(StringValue("abc")) shouldBe "abc"
  }

  it should "throw an exception for an invalid unbox" in {
    a [ClassCastException] should be thrownBy { StringCodex.fromValue(LongValue(1)) }
    a [ClassCastException] should be thrownBy { StringCodex.fromValue(DoubleValue(1)) }
    a [ClassCastException] should be thrownBy { StringCodex.fromValue(BooleanValue(true)) }
  }

  it should "decode a correct value" in {
    StringCodex.decode("abc") shouldBe Some(StringValue("abc"))
  }

  it should "encode a correct value" in {
    StringCodex.encode(StringValue("abc")) shouldBe "abc"
  }

  it should "not encode an incorrect value" in {
    a [ClassCastException] should be thrownBy { StringCodex.encode(LongValue(1)) }
    a [ClassCastException] should be thrownBy { StringCodex.encode(DoubleValue(1)) }
    a [ClassCastException] should be thrownBy { StringCodex.encode(BooleanValue(true)) }
  }

  it should "compare a correct value" in {
    StringCodex.compare(StringValue("abc"), StringValue("bbc")) shouldBe Some(-1)
    StringCodex.compare(StringValue("abc"), StringValue("abc")) shouldBe Some(0)
    StringCodex.compare(StringValue("bbc"), StringValue("abc")) shouldBe Some(1)
  }

  it should "not compare an incorrect value" in {
    StringCodex.compare(StringValue("abc"), LongValue(1)) shouldBe None
    StringCodex.compare(LongValue(1), StringValue("abc")) shouldBe None
  }
}

class TestDoubleCodex extends TestGrimlock {

  "A DoubleCodex" should "have a name" in {
    DoubleCodex.name shouldBe "double"
  }

  it should "box a correct Value" in {
    DoubleCodex.toValue(3.14) shouldBe DoubleValue(3.14)
  }

  it should "unbox a correct value" in {
    DoubleCodex.fromValue(DoubleValue(3.14)) shouldBe 3.14
  }

  it should "throw an exception for an invalid unbox" in {
    a [ClassCastException] should be thrownBy { DoubleCodex.fromValue(LongValue(1)) }
    a [ClassCastException] should be thrownBy { DoubleCodex.fromValue(StringValue("a")) }
    a [ClassCastException] should be thrownBy { DoubleCodex.fromValue(BooleanValue(true)) }
  }

  it should "decode a correct value" in {
    DoubleCodex.decode("3.14") shouldBe Some(DoubleValue(3.14))
  }

  it should "not decode an incorrect value" in {
    DoubleCodex.decode("a") shouldBe None
    DoubleCodex.decode("2001-01-01") shouldBe None
  }

  it should "encode a correct value" in {
    DoubleCodex.encode(DoubleValue(3.14)) shouldBe "3.14"
  }

  it should "not encode an incorrect value" in {
    a [ClassCastException] should be thrownBy { DoubleCodex.encode(LongValue(1)) }
    a [ClassCastException] should be thrownBy { DoubleCodex.encode(StringValue("abc")) }
    a [ClassCastException] should be thrownBy { DoubleCodex.encode(BooleanValue(true)) }
  }

  it should "compare a correct value" in {
    DoubleCodex.compare(DoubleValue(3.14), DoubleValue(4.14)) shouldBe Some(-1)
    DoubleCodex.compare(DoubleValue(3.14), DoubleValue(3.14)) shouldBe Some(0)
    DoubleCodex.compare(DoubleValue(4.14), DoubleValue(3.14)) shouldBe Some(1)

    DoubleCodex.compare(DoubleValue(3), LongValue(4)) shouldBe Some(-1)
    DoubleCodex.compare(DoubleValue(3), LongValue(3)) shouldBe Some(0)
    DoubleCodex.compare(DoubleValue(4), LongValue(3)) shouldBe Some(1)

    DoubleCodex.compare(LongValue(3), DoubleValue(4)) shouldBe Some(-1)
    DoubleCodex.compare(LongValue(3), DoubleValue(3)) shouldBe Some(0)
    DoubleCodex.compare(LongValue(4), DoubleValue(3)) shouldBe Some(1)

    DoubleCodex.compare(LongValue(3), LongValue(4)) shouldBe Some(-1)
    DoubleCodex.compare(LongValue(3), LongValue(3)) shouldBe Some(0)
    DoubleCodex.compare(LongValue(4), LongValue(3)) shouldBe Some(1)
  }

  it should "not compare an incorrect value" in {
    DoubleCodex.compare(DoubleValue(3.14), StringValue("abc")) shouldBe None
    DoubleCodex.compare(StringValue("abc"), DoubleValue(3.14)) shouldBe None
  }
}

class TestLongCodex extends TestGrimlock {

  "A LongCodex" should "have a name" in {
    LongCodex.name shouldBe "long"
  }

  it should "box a correct Value" in {
    LongCodex.toValue(42) shouldBe LongValue(42)
  }

  it should "unbox a correct value" in {
    LongCodex.fromValue(LongValue(42)) shouldBe 42
  }

  it should "throw an exception for an invalid unbox" in {
    a [ClassCastException] should be thrownBy { LongCodex.fromValue(DoubleValue(3.14)) }
    a [ClassCastException] should be thrownBy { LongCodex.fromValue(StringValue("a")) }
    a [ClassCastException] should be thrownBy { LongCodex.fromValue(BooleanValue(true)) }
  }

  it should "decode a correct value" in {
    LongCodex.decode("42") shouldBe Some(LongValue(42))
  }

  it should "not decode an incorrect value" in {
    LongCodex.decode("a") shouldBe None
    LongCodex.decode("2001-01-01") shouldBe None
  }

  it should "encode a correct value" in {
    LongCodex.encode(LongValue(42)) shouldBe "42"
  }

  it should "not encode an incorrect value" in {
    a [ClassCastException] should be thrownBy { LongCodex.encode(DoubleValue(3.14)) }
    a [ClassCastException] should be thrownBy { LongCodex.encode(StringValue("abc")) }
    a [ClassCastException] should be thrownBy { LongCodex.encode(BooleanValue(true)) }
  }

  it should "compare a correct value" in {
    LongCodex.compare(DoubleValue(42), DoubleValue(43)) shouldBe Some(-1)
    LongCodex.compare(DoubleValue(42), DoubleValue(42)) shouldBe Some(0)
    LongCodex.compare(DoubleValue(43), DoubleValue(42)) shouldBe Some(1)

    LongCodex.compare(DoubleValue(3), LongValue(4)) shouldBe Some(-1)
    LongCodex.compare(DoubleValue(3), LongValue(3)) shouldBe Some(0)
    LongCodex.compare(DoubleValue(4), LongValue(3)) shouldBe Some(1)

    LongCodex.compare(LongValue(3), DoubleValue(4)) shouldBe Some(-1)
    LongCodex.compare(LongValue(3), DoubleValue(3)) shouldBe Some(0)
    LongCodex.compare(LongValue(4), DoubleValue(3)) shouldBe Some(1)

    LongCodex.compare(LongValue(3), LongValue(4)) shouldBe Some(-1)
    LongCodex.compare(LongValue(3), LongValue(3)) shouldBe Some(0)
    LongCodex.compare(LongValue(4), LongValue(3)) shouldBe Some(1)
  }

  it should "not compare an incorrect value" in {
    LongCodex.compare(LongValue(42), StringValue("abc")) shouldBe None
    LongCodex.compare(StringValue("abc"), LongValue(42)) shouldBe None
  }
}

class TestBooleanCodex extends TestGrimlock {

  "A BooleanCodex" should "have a name" in {
    BooleanCodex.name shouldBe "boolean"
  }

  it should "box a correct Value" in {
    BooleanCodex.toValue(true) shouldBe BooleanValue(true)
  }

  it should "unbox a correct value" in {
    BooleanCodex.fromValue(BooleanValue(true)) shouldBe true
  }

  it should "throw an exception for an invalid unbox" in {
    a [ClassCastException] should be thrownBy { BooleanCodex.fromValue(LongValue(1)) }
    a [ClassCastException] should be thrownBy { BooleanCodex.fromValue(DoubleValue(3.14)) }
    a [ClassCastException] should be thrownBy { BooleanCodex.fromValue(StringValue("a")) }
  }

  it should "decode a correct value" in {
    BooleanCodex.decode("true") shouldBe Some(BooleanValue(true))
    BooleanCodex.decode("false") shouldBe Some(BooleanValue(false))
  }

  it should "not decode an incorrect value" in {
    BooleanCodex.decode("a") shouldBe None
    BooleanCodex.decode("2001-01-01") shouldBe None
  }

  it should "encode a correct value" in {
    BooleanCodex.encode(BooleanValue(true)) shouldBe "true"
  }

  it should "not encode an incorrect value" in {
    a [ClassCastException] should be thrownBy { BooleanCodex.encode(LongValue(1)) }
    a [ClassCastException] should be thrownBy { BooleanCodex.encode(DoubleValue(3.14)) }
    a [ClassCastException] should be thrownBy { BooleanCodex.encode(StringValue("abc")) }
  }

  it should "compare a correct value" in {
    BooleanCodex.compare(BooleanValue(false), BooleanValue(true)) shouldBe Some(-1)
    BooleanCodex.compare(BooleanValue(false), BooleanValue(false)) shouldBe Some(0)
    BooleanCodex.compare(BooleanValue(true), BooleanValue(false)) shouldBe Some(1)
  }

  it should "not compare an incorrect value" in {
    BooleanCodex.compare(BooleanValue(true), StringValue("abc")) shouldBe None
    BooleanCodex.compare(StringValue("abc"), BooleanValue(true)) shouldBe None
  }
}

