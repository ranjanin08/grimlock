// Copyright 2015,2016 Commonwealth Bank of Australia
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

import au.com.cba.omnia.grimlock.framework.content._
import au.com.cba.omnia.grimlock.framework.content.metadata._
import au.com.cba.omnia.grimlock.framework.encoding._

class TestContent extends TestGrimlock {

  "A Continuous Double Content" should "return its string value" in {
    Content(ContinuousSchema[Double](), 3.14).toString shouldBe
      "Content(ContinuousSchema[Double](),DoubleValue(3.14))"
    Content(ContinuousSchema[Double](0, 10), 3.14).toString shouldBe
      "Content(ContinuousSchema[Double](0.0,10.0),DoubleValue(3.14))"
  }

  it should "return its short string value" in {
    Content(ContinuousSchema[Double](), 3.14).toShortString("|") shouldBe "continuous|double|3.14"
    Content(ContinuousSchema[Double](0, 10), 3.14).toShortString("|") shouldBe "continuous(0.0,10.0)|double|3.14"
  }

  "A Continuous Long Content" should "return its string value" in {
    Content(ContinuousSchema[Long](), 42).toString shouldBe "Content(ContinuousSchema[Long](),LongValue(42))"
    Content(ContinuousSchema[Long](0, 100), 42).toString shouldBe
      "Content(ContinuousSchema[Long](0,100),LongValue(42))"
  }

  it should "return its short string value" in {
    Content(ContinuousSchema[Long](), 42).toShortString("|") shouldBe "continuous|long|42"
    Content(ContinuousSchema[Long](0, 100), 42).toShortString("|") shouldBe
      "continuous(0,100)|long|42"
  }

  "A Discrete Long Content" should "return its string value" in {
    Content(DiscreteSchema[Long](), 42).toString shouldBe "Content(DiscreteSchema[Long](),LongValue(42))"
    Content(DiscreteSchema[Long](0, 100, 2), 42).toString shouldBe
      "Content(DiscreteSchema[Long](0,100,2),LongValue(42))"
  }

  it should "return its short string value" in {
    Content(DiscreteSchema[Long](), 42).toShortString("|") shouldBe "discrete|long|42"
    Content(DiscreteSchema[Long](0, 100, 2), 42).toShortString("|") shouldBe
      "discrete(0,100,2)|long|42"
  }

  "A Nominal String Content" should "return its string value" in {
    Content(NominalSchema[String](), "a").toString shouldBe "Content(NominalSchema[String](),StringValue(a))"
    Content(NominalSchema[String](Set("a", "b", "c")), "a").toString shouldBe
      "Content(NominalSchema[String](a,b,c),StringValue(a))"
  }

  it should "return its short string value" in {
    Content(NominalSchema[String](), "a").toShortString("|") shouldBe "nominal|string|a"
    Content(NominalSchema[String](Set("a", "b", "c")), "a").toShortString("|") shouldBe
      "nominal(a,b,c)|string|a"
  }

  "A Nominal Double Content" should "return its string value" in {
    Content(NominalSchema[Double](), 1.0).toString shouldBe "Content(NominalSchema[Double](),DoubleValue(1.0))"
    Content(NominalSchema[Double](Set[Double](1, 2, 3)), 1.0).toString shouldBe
      "Content(NominalSchema[Double](1.0,2.0,3.0),DoubleValue(1.0))"
  }

  it should "return its short string value" in {
    Content(NominalSchema[Double](), 1.0).toShortString("|") shouldBe "nominal|double|1.0"
    Content(NominalSchema[Double](Set[Double](1, 2, 3)), 1.0).toShortString("|") shouldBe
      "nominal(1.0,2.0,3.0)|double|1.0"
  }

  "A Nominal Long Content" should "return its string value" in {
    Content(NominalSchema[Long](), 1).toString shouldBe "Content(NominalSchema[Long](),LongValue(1))"
    Content(NominalSchema[Long](Set[Long](1, 2, 3)), 1).toString shouldBe
      "Content(NominalSchema[Long](1,2,3),LongValue(1))"
  }

  it should "return its short string value" in {
    Content(NominalSchema[Long](), 1).toShortString("|") shouldBe "nominal|long|1"
    Content(NominalSchema[Long](Set[Long](1, 2, 3)), 1).toShortString("|") shouldBe
      "nominal(1,2,3)|long|1"
  }

  "A Ordinal String Content" should "return its string value" in {
    Content(OrdinalSchema[String](), "a").toString shouldBe "Content(OrdinalSchema[String](),StringValue(a))"
    Content(OrdinalSchema[String](Set("a", "b", "c")), "a").toString shouldBe
      "Content(OrdinalSchema[String](a,b,c),StringValue(a))"
  }

  it should "return its short string value" in {
    Content(OrdinalSchema[String](), "a").toShortString("|") shouldBe "ordinal|string|a"
    Content(OrdinalSchema[String](Set("a", "b", "c")), "a").toShortString("|") shouldBe
      "ordinal(a,b,c)|string|a"
  }

  "A Ordinal Double Content" should "return its string value" in {
    Content(OrdinalSchema[Double](), 1.0).toString shouldBe "Content(OrdinalSchema[Double](),DoubleValue(1.0))"
    Content(OrdinalSchema[Double](Set[Double](1, 2, 3)), 1.0).toString shouldBe
      "Content(OrdinalSchema[Double](1.0,2.0,3.0),DoubleValue(1.0))"
  }

  it should "return its short string value" in {
    Content(OrdinalSchema[Double](), 1.0).toShortString("|") shouldBe "ordinal|double|1.0"
    Content(OrdinalSchema[Double](Set[Double](1, 2, 3)), 1.0).toShortString("|") shouldBe
      "ordinal(1.0,2.0,3.0)|double|1.0"
  }

  "A Ordinal Long Content" should "return its string value" in {
    Content(OrdinalSchema[Long](), 1).toString shouldBe "Content(OrdinalSchema[Long](),LongValue(1))"
    Content(OrdinalSchema[Long](Set[Long](1, 2, 3)), 1).toString shouldBe
      "Content(OrdinalSchema[Long](1,2,3),LongValue(1))"
  }

  it should "return its short string value" in {
    Content(OrdinalSchema[Long](), 1).toShortString("|") shouldBe "ordinal|long|1"
    Content(OrdinalSchema[Long](Set[Long](1, 2, 3)), 1).toShortString("|") shouldBe
      "ordinal(1,2,3)|long|1"
  }

  val dfmt = new java.text.SimpleDateFormat("yyyy-MM-dd")

  "A Date Date Content" should "return its string value" in {
    Content(DateSchema[java.util.Date](), DateValue(dfmt.parse("2001-01-01"))).toString shouldBe
      "Content(DateSchema[java.util.Date](),DateValue(" + dfmt.parse("2001-01-01").toString +
        ",DateCodex(yyyy-MM-dd)))"
  }

  it should "return its short string value" in {
    Content(DateSchema[java.util.Date](), DateValue(dfmt.parse("2001-01-01"))).toShortString("|") shouldBe
      "date|date(yyyy-MM-dd)|2001-01-01"
  }

  val dtfmt = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")

  "A Date DateTime Content" should "return its string value" in {
    Content(DateSchema[java.util.Date](), DateValue(dtfmt.parse("2001-01-01 01:01:01"))).toString shouldBe
      "Content(DateSchema[java.util.Date](),DateValue(" + dtfmt.parse("2001-01-01 01:01:01").toString +
        ",DateCodex(yyyy-MM-dd hh:mm:ss)))"
  }

  it should "return its short string value" in {
    Content(DateSchema[java.util.Date](), DateValue(dtfmt.parse("2001-01-01 01:01:01")))
      .toShortString("|") shouldBe "date|date(yyyy-MM-dd hh:mm:ss)|2001-01-01 01:01:01"
  }
}

