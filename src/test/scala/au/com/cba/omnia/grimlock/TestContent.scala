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

class TestContent extends TestGrimlock {

  "A Continuous Double Content" should "return its string value" in {
    Content(ContinuousSchema[Codex.DoubleCodex](), 3.14).toString shouldBe
      "Content(ContinuousSchema[DoubleCodex](),DoubleValue(3.14))"
    Content(ContinuousSchema[Codex.DoubleCodex](0, 10), 3.14).toString shouldBe
      "Content(ContinuousSchema[DoubleCodex](0.0,10.0),DoubleValue(3.14))"
  }

  it should "return its short string value" in {
    Content(ContinuousSchema[Codex.DoubleCodex](), 3.14).toShortString("|") shouldBe "continuous|double|3.14"
    Content(ContinuousSchema[Codex.DoubleCodex](0, 10), 3.14).toShortString("|") shouldBe "continuous|double|3.14"
  }

  "A Continuous Long Content" should "return its string value" in {
    Content(ContinuousSchema[Codex.LongCodex](), 42).toString shouldBe
      "Content(ContinuousSchema[LongCodex](),LongValue(42))"
    Content(ContinuousSchema[Codex.LongCodex](0, 100), 42).toString shouldBe
      "Content(ContinuousSchema[LongCodex](0,100),LongValue(42))"
  }

  it should "return its short string value" in {
    Content(ContinuousSchema[Codex.LongCodex](), 42).toShortString("|") shouldBe "continuous|long|42"
    Content(ContinuousSchema[Codex.LongCodex](0, 100), 42).toShortString("|") shouldBe "continuous|long|42"
  }

  "A Discrete Long Content" should "return its string value" in {
    Content(DiscreteSchema[Codex.LongCodex](), 42).toString shouldBe
      "Content(DiscreteSchema[LongCodex](),LongValue(42))"
    Content(DiscreteSchema[Codex.LongCodex](0, 100, 2), 42).toString shouldBe
      "Content(DiscreteSchema[LongCodex](0,100,2),LongValue(42))"
  }

  it should "return its short string value" in {
    Content(DiscreteSchema[Codex.LongCodex](), 42).toShortString("|") shouldBe "discrete|long|42"
    Content(DiscreteSchema[Codex.LongCodex](0, 100, 2), 42).toShortString("|") shouldBe "discrete|long|42"
  }

  "A Nominal String Content" should "return its string value" in {
    Content(NominalSchema[Codex.StringCodex](), "a").toString shouldBe
      "Content(NominalSchema[StringCodex](),StringValue(a))"
    Content(NominalSchema[Codex.StringCodex](List("a", "b", "c")), "a").toString shouldBe
      "Content(NominalSchema[StringCodex](List(a, b, c)),StringValue(a))"
  }

  it should "return its short string value" in {
    Content(NominalSchema[Codex.StringCodex](), "a").toShortString("|") shouldBe "nominal|string|a"
    Content(NominalSchema[Codex.StringCodex](List("a", "b", "c")), "a").toShortString("|") shouldBe "nominal|string|a"
  }

  "A Nominal Double Content" should "return its string value" in {
    Content(NominalSchema[Codex.DoubleCodex](), 1).toString shouldBe
      "Content(NominalSchema[DoubleCodex](),DoubleValue(1.0))"
    Content(NominalSchema[Codex.DoubleCodex](List[Double](1, 2, 3)), 1).toString shouldBe
      "Content(NominalSchema[DoubleCodex](List(1.0, 2.0, 3.0)),DoubleValue(1.0))"
  }

  it should "return its short string value" in {
    Content(NominalSchema[Codex.DoubleCodex](), 1).toShortString("|") shouldBe "nominal|double|1.0"
    Content(NominalSchema[Codex.DoubleCodex](List[Double](1, 2, 3)), 1).toShortString("|") shouldBe
      "nominal|double|1.0"
  }

  "A Nominal Long Content" should "return its string value" in {
    Content(NominalSchema[Codex.LongCodex](), 1).toString shouldBe "Content(NominalSchema[LongCodex](),LongValue(1))"
    Content(NominalSchema[Codex.LongCodex](List[Long](1, 2, 3)), 1).toString shouldBe
      "Content(NominalSchema[LongCodex](List(1, 2, 3)),LongValue(1))"
  }

  it should "return its short string value" in {
    Content(NominalSchema[Codex.LongCodex](), 1).toShortString("|") shouldBe "nominal|long|1"
    Content(NominalSchema[Codex.LongCodex](List[Long](1, 2, 3)), 1).toShortString("|") shouldBe "nominal|long|1"
  }

  "A Ordinal String Content" should "return its string value" in {
    Content(OrdinalSchema[Codex.StringCodex](), "a").toString shouldBe
      "Content(OrdinalSchema[StringCodex](),StringValue(a))"
    Content(OrdinalSchema[Codex.StringCodex](List("a", "b", "c")), "a").toString shouldBe
      "Content(OrdinalSchema[StringCodex](List(a, b, c)),StringValue(a))"
  }

  it should "return its short string value" in {
    Content(OrdinalSchema[Codex.StringCodex](), "a").toShortString("|") shouldBe "ordinal|string|a"
    Content(OrdinalSchema[Codex.StringCodex](List("a", "b", "c")), "a").toShortString("|") shouldBe "ordinal|string|a"
  }

  "A Ordinal Double Content" should "return its string value" in {
    Content(OrdinalSchema[Codex.DoubleCodex](), 1).toString shouldBe
      "Content(OrdinalSchema[DoubleCodex](),DoubleValue(1.0))"
    Content(OrdinalSchema[Codex.DoubleCodex](List[Double](1, 2, 3)), 1).toString shouldBe
      "Content(OrdinalSchema[DoubleCodex](List(1.0, 2.0, 3.0)),DoubleValue(1.0))"
  }

  it should "return its short string value" in {
    Content(OrdinalSchema[Codex.DoubleCodex](), 1).toShortString("|") shouldBe "ordinal|double|1.0"
    Content(OrdinalSchema[Codex.DoubleCodex](List[Double](1, 2, 3)), 1).toShortString("|") shouldBe
      "ordinal|double|1.0"
  }

  "A Ordinal Long Content" should "return its string value" in {
    Content(OrdinalSchema[Codex.LongCodex](), 1).toString shouldBe "Content(OrdinalSchema[LongCodex](),LongValue(1))"
    Content(OrdinalSchema[Codex.LongCodex](List[Long](1, 2, 3)), 1).toString shouldBe
      "Content(OrdinalSchema[LongCodex](List(1, 2, 3)),LongValue(1))"
  }

  it should "return its short string value" in {
    Content(OrdinalSchema[Codex.LongCodex](), 1).toShortString("|") shouldBe "ordinal|long|1"
    Content(OrdinalSchema[Codex.LongCodex](List[Long](1, 2, 3)), 1).toShortString("|") shouldBe "ordinal|long|1"
  }

  val dfmt = new java.text.SimpleDateFormat("yyyy-MM-dd")

  "A Date Date Content" should "return its string value" in {
    Content(DateSchema[Codex.DateCodex](), dfmt.parse("2001-01-01")).toString shouldBe
      "Content(DateSchema[DateCodex](),DateValue(" + dfmt.parse("2001-01-01").toString + ",DateCodex))"
  }

  it should "return its short string value" in {
    Content(DateSchema[Codex.DateCodex](), dfmt.parse("2001-01-01")).toShortString("|") shouldBe "date|date|2001-01-01"
  }

  val dtfmt = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")

  "A Date DateTime Content" should "return its string value" in {
    Content(DateSchema[Codex.DateTimeCodex](), dtfmt.parse("2001-01-01 01:01:01")).toString shouldBe
      "Content(DateSchema[DateTimeCodex](),DateValue(" + dtfmt.parse("2001-01-01 01:01:01").toString +
        ",DateTimeCodex))"
  }

  it should "return its short string value" in {
    Content(DateSchema[Codex.DateTimeCodex](), dtfmt.parse("2001-01-01 01:01:01")).toShortString("|") shouldBe
      "date|date.time|2001-01-01 01:01:01"
  }
}

