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

import au.com.cba.omnia.grimlock.framework.content.metadata._
import au.com.cba.omnia.grimlock.framework.encoding._

class TestContinuousSchema extends TestGrimlock {

  "A ContinuousSchema" should "return its string representation" in {
    ContinuousSchema(DoubleCodex).toString shouldBe "ContinuousSchema(DoubleCodex)"
    ContinuousSchema[Codex.DoubleCodex](DoubleCodex, -3.1415, 1.4142).toString shouldBe
      "ContinuousSchema(DoubleCodex,-3.1415,1.4142)"

    ContinuousSchema(LongCodex).toString shouldBe "ContinuousSchema(LongCodex)"
    ContinuousSchema[Codex.LongCodex](LongCodex, -1, 1).toString shouldBe "ContinuousSchema(LongCodex,-1,1)"
  }

  it should "validate a correct value" in {
    ContinuousSchema(DoubleCodex).isValid(DoubleValue(1)) shouldBe true
    ContinuousSchema[Codex.DoubleCodex](DoubleCodex, -3.1415, 1.4142).isValid(DoubleValue(1)) shouldBe true

    ContinuousSchema(LongCodex).isValid(LongValue(1)) shouldBe true
    ContinuousSchema[Codex.LongCodex](LongCodex, -1, 1).isValid(LongValue(1)) shouldBe true
  }

  it should "not validate an incorrect value" in {
    ContinuousSchema[Codex.DoubleCodex](DoubleCodex, -3.1415, 1.4142).isValid(DoubleValue(4)) shouldBe false
    ContinuousSchema[Codex.DoubleCodex](DoubleCodex, -3.1415, 1.4142).isValid(DoubleValue(-4)) shouldBe false

    ContinuousSchema[Codex.LongCodex](LongCodex, -1, 1).isValid(LongValue(4)) shouldBe false
    ContinuousSchema[Codex.LongCodex](LongCodex, -1, 1).isValid(LongValue(-4)) shouldBe false
  }

  it should "throw an exception for an invalid value" in {
    a [ClassCastException] should be thrownBy { ContinuousSchema(DoubleCodex).isValid(StringValue("a")) }
    a [ClassCastException] should be thrownBy {
      ContinuousSchema[Codex.DoubleCodex](DoubleCodex, -3.1415, 1.4142).isValid(StringValue("a"))
    }

    a [ClassCastException] should be thrownBy { ContinuousSchema(LongCodex).isValid(StringValue("a")) }
    a [ClassCastException] should be thrownBy {
      ContinuousSchema[Codex.LongCodex](LongCodex, -1, 1).isValid(StringValue("a"))
    }
  }

  it should "decode a correct value" in {
    ContinuousSchema(DoubleCodex).decode("1").map(_.value) shouldBe Some(DoubleValue(1))
    ContinuousSchema[Codex.DoubleCodex](DoubleCodex, -3.1415, 1.4142).decode("1").map(_.value) shouldBe
      Some(DoubleValue(1))

    ContinuousSchema(LongCodex).decode("1").map(_.value) shouldBe Some(LongValue(1))
    ContinuousSchema[Codex.LongCodex](LongCodex, -1, 1).decode("1").map(_.value) shouldBe Some(LongValue(1))
  }

  it should "not decode an incorrect value" in {
    ContinuousSchema(DoubleCodex).decode("a") shouldBe None
    ContinuousSchema[Codex.DoubleCodex](DoubleCodex, -3.1415, 1.4142).decode("4") shouldBe None
    ContinuousSchema[Codex.DoubleCodex](DoubleCodex, -3.1415, 1.4142).decode("-4") shouldBe None
    ContinuousSchema[Codex.DoubleCodex](DoubleCodex, -3.1415, 1.4142).decode("a") shouldBe None

    ContinuousSchema(LongCodex).decode("a") shouldBe None
    ContinuousSchema[Codex.LongCodex](LongCodex, -1, 1).decode("4") shouldBe None
    ContinuousSchema[Codex.LongCodex](LongCodex, -1, 1).decode("-4") shouldBe None
    ContinuousSchema[Codex.LongCodex](LongCodex, -1, 1).decode("a") shouldBe None
  }
}

class TestDiscreteSchema extends TestGrimlock {

  "A DiscreteSchema" should "return its string representation" in {
    DiscreteSchema(LongCodex).toString shouldBe "DiscreteSchema(LongCodex)"
    DiscreteSchema[Codex.LongCodex](LongCodex, -1, 1, 1).toString shouldBe "DiscreteSchema(LongCodex,-1,1,1)"
  }

  it should "validate a correct value" in {
    DiscreteSchema(LongCodex).isValid(LongValue(1)) shouldBe true
    DiscreteSchema[Codex.LongCodex](LongCodex, -1, 1, 1).isValid(LongValue(1)) shouldBe true
    DiscreteSchema[Codex.LongCodex](LongCodex, -4, 4, 2).isValid(LongValue(2)) shouldBe true
  }

  it should "not validate an incorrect value" in {
    DiscreteSchema[Codex.LongCodex](LongCodex, -1, 1, 1).isValid(LongValue(4)) shouldBe false
    DiscreteSchema[Codex.LongCodex](LongCodex, -1, 1, 1).isValid(LongValue(-4)) shouldBe false
    DiscreteSchema[Codex.LongCodex](LongCodex, -4, 4, 2).isValid(LongValue(3)) shouldBe false
  }

  it should "throw an exception for an invalid value" in {
    a [ClassCastException] should be thrownBy { DiscreteSchema(LongCodex).isValid(DoubleValue(1)) }
    a [ClassCastException] should be thrownBy {
      DiscreteSchema[Codex.LongCodex](LongCodex, -1, 1, 1).isValid(DoubleValue(1))
    }
    a [ClassCastException] should be thrownBy { DiscreteSchema(LongCodex).isValid(StringValue("a")) }
    a [ClassCastException] should be thrownBy {
      DiscreteSchema[Codex.LongCodex](LongCodex, -1, 1, 1).isValid(StringValue("a"))
    }
  }

  it should "decode a correct value" in {
    DiscreteSchema(LongCodex).decode("1").map(_.value) shouldBe Some(LongValue(1))
    DiscreteSchema[Codex.LongCodex](LongCodex, -1, 1, 1).decode("1").map(_.value) shouldBe Some(LongValue(1))
  }

  it should "not decode an incorrect value" in {
    DiscreteSchema(LongCodex).decode("3.1415") shouldBe None
    DiscreteSchema(LongCodex).decode("a") shouldBe None
    DiscreteSchema[Codex.LongCodex](LongCodex, -1, 1, 1).decode("4") shouldBe None
    DiscreteSchema[Codex.LongCodex](LongCodex, -1, 1, 1).decode("-4") shouldBe None
    DiscreteSchema[Codex.LongCodex](LongCodex, -4, 4, 2).decode("3") shouldBe None
    DiscreteSchema[Codex.LongCodex](LongCodex, -1, 1, 1).decode("a") shouldBe None
  }
}

class TestNominalSchema extends TestGrimlock {

  "A NominalSchema" should "return its string representation" in {
    NominalSchema(LongCodex).toString shouldBe "NominalSchema(LongCodex)"
    NominalSchema[Codex.LongCodex](LongCodex, List[Long](1,2,3)).toString shouldBe "NominalSchema(LongCodex,1,2,3)"

    NominalSchema(DoubleCodex).toString shouldBe "NominalSchema(DoubleCodex)"
    NominalSchema[Codex.DoubleCodex](DoubleCodex, List[Double](1,2,3)).toString shouldBe
      "NominalSchema(DoubleCodex,1.0,2.0,3.0)"

    NominalSchema(StringCodex).toString shouldBe "NominalSchema(StringCodex)"
    NominalSchema[Codex.StringCodex](StringCodex, List("a","b","c")).toString shouldBe
      "NominalSchema(StringCodex,a,b,c)"
  }

  it should "validate a correct value" in {
    NominalSchema(LongCodex).isValid(LongValue(1)) shouldBe true
    NominalSchema[Codex.LongCodex](LongCodex, List[Long](1,2,3)).isValid(LongValue(1)) shouldBe true

    NominalSchema(DoubleCodex).isValid(DoubleValue(1)) shouldBe true
    NominalSchema[Codex.DoubleCodex](DoubleCodex, List[Double](1,2,3)).isValid(DoubleValue(1)) shouldBe true

    NominalSchema(StringCodex).isValid(StringValue("a")) shouldBe true
    NominalSchema[Codex.StringCodex](StringCodex, List("a","b","c")).isValid(StringValue("a")) shouldBe true
  }

  it should "not validate an incorrect value" in {
    NominalSchema[Codex.LongCodex](LongCodex, List[Long](1,2,3)).isValid(LongValue(4)) shouldBe false

    NominalSchema[Codex.DoubleCodex](DoubleCodex, List[Double](1,2,3)).isValid(DoubleValue(4)) shouldBe false

    NominalSchema[Codex.StringCodex](StringCodex, List("a","b","c")).isValid(StringValue("d")) shouldBe false
  }

  it should "throw an exception for an invalid value" in {
    a [ClassCastException] should be thrownBy { NominalSchema(LongCodex).isValid(DoubleValue(1)) }
    a [ClassCastException] should be thrownBy {
      NominalSchema[Codex.LongCodex](LongCodex, List[Long](1,2,3)).isValid(DoubleValue(1))
    }
    a [ClassCastException] should be thrownBy { NominalSchema(LongCodex).isValid(StringValue("a")) }
    a [ClassCastException] should be thrownBy {
      NominalSchema[Codex.LongCodex](LongCodex, List[Long](1,2,3)).isValid(StringValue("a"))
    }

    a [ClassCastException] should be thrownBy { NominalSchema(DoubleCodex).isValid(LongValue(1)) }
    a [ClassCastException] should be thrownBy {
      NominalSchema[Codex.DoubleCodex](DoubleCodex, List[Double](1,2,3)).isValid(LongValue(1))
    }
    a [ClassCastException] should be thrownBy { NominalSchema(DoubleCodex).isValid(StringValue("a")) }
    a [ClassCastException] should be thrownBy {
      NominalSchema[Codex.DoubleCodex](DoubleCodex, List[Double](1,2,3)).isValid(StringValue("a"))
    }

    a [ClassCastException] should be thrownBy { NominalSchema(StringCodex).isValid(LongValue(1)) }
    a [ClassCastException] should be thrownBy {
      NominalSchema[Codex.StringCodex](StringCodex, List("a","b","c")).isValid(LongValue(1))
    }
    a [ClassCastException] should be thrownBy { NominalSchema(StringCodex).isValid(DoubleValue(1)) }
    a [ClassCastException] should be thrownBy {
      NominalSchema[Codex.StringCodex](StringCodex, List("a","b","c")).isValid(DoubleValue(1))
    }
  }

  it should "decode a correct value" in {
    NominalSchema(LongCodex).decode("1").map(_.value) shouldBe Some(LongValue(1))
    NominalSchema[Codex.LongCodex](LongCodex, List[Long](1,2,3)).decode("1").map(_.value) shouldBe Some(LongValue(1))

    NominalSchema(DoubleCodex).decode("1").map(_.value) shouldBe Some(DoubleValue(1))
    NominalSchema[Codex.DoubleCodex](DoubleCodex, List[Double](1,2,3)).decode("1").map(_.value) shouldBe
      Some(DoubleValue(1))

    NominalSchema(StringCodex).decode("a").map(_.value) shouldBe Some(StringValue("a"))
    NominalSchema[Codex.StringCodex](StringCodex, List("a","b","c")).decode("a").map(_.value) shouldBe
      Some(StringValue("a"))
  }

  it should "not decode an incorrect value" in {
    NominalSchema(LongCodex).decode("3.1415") shouldBe None
    NominalSchema(LongCodex).decode("a") shouldBe None
    NominalSchema[Codex.LongCodex](LongCodex, List[Long](1,2,3)).decode("4") shouldBe None
    NominalSchema[Codex.LongCodex](LongCodex, List[Long](1,2,3)).decode("a") shouldBe None

    NominalSchema(DoubleCodex).decode("a") shouldBe None
    NominalSchema[Codex.DoubleCodex](DoubleCodex, List[Double](1,2,3)).decode("4") shouldBe None
    NominalSchema[Codex.DoubleCodex](DoubleCodex, List[Double](1,2,3)).decode("a") shouldBe None

    NominalSchema[Codex.StringCodex](StringCodex, List("a","b","c")).decode("1") shouldBe None
    NominalSchema[Codex.StringCodex](StringCodex, List("a","b","c")).decode("d") shouldBe None
  }
}

class TestOrdinalSchema extends TestGrimlock {

  "A OrdinalSchema" should "return its string representation" in {
    OrdinalSchema(LongCodex).toString shouldBe "OrdinalSchema(LongCodex)"
    OrdinalSchema[Codex.LongCodex](LongCodex, List[Long](1,2,3)).toString shouldBe "OrdinalSchema(LongCodex,1,2,3)"

    OrdinalSchema(DoubleCodex).toString shouldBe "OrdinalSchema(DoubleCodex)"
    OrdinalSchema[Codex.DoubleCodex](DoubleCodex, List[Double](1,2,3)).toString shouldBe
      "OrdinalSchema(DoubleCodex,1.0,2.0,3.0)"

    OrdinalSchema(StringCodex).toString shouldBe "OrdinalSchema(StringCodex)"
    OrdinalSchema[Codex.StringCodex](StringCodex, List("a","b","c")).toString shouldBe
      "OrdinalSchema(StringCodex,a,b,c)"
  }

  it should "validate a correct value" in {
    OrdinalSchema(LongCodex).isValid(LongValue(1)) shouldBe true
    OrdinalSchema[Codex.LongCodex](LongCodex, List[Long](1,2,3)).isValid(LongValue(1)) shouldBe true

    OrdinalSchema(DoubleCodex).isValid(DoubleValue(1)) shouldBe true
    OrdinalSchema[Codex.DoubleCodex](DoubleCodex, List[Double](1,2,3)).isValid(DoubleValue(1)) shouldBe true

    OrdinalSchema(StringCodex).isValid(StringValue("a")) shouldBe true
    OrdinalSchema[Codex.StringCodex](StringCodex, List("a","b","c")).isValid(StringValue("a")) shouldBe true
  }

  it should "not validate an incorrect value" in {
    OrdinalSchema[Codex.LongCodex](LongCodex, List[Long](1,2,3)).isValid(LongValue(4)) shouldBe false

    OrdinalSchema[Codex.DoubleCodex](DoubleCodex, List[Double](1,2,3)).isValid(DoubleValue(4)) shouldBe false

    OrdinalSchema[Codex.StringCodex](StringCodex, List("a","b","c")).isValid(StringValue("d")) shouldBe false
  }

  it should "throw an exception for an invalid value" in {
    a [ClassCastException] should be thrownBy { OrdinalSchema(LongCodex).isValid(DoubleValue(1)) }
    a [ClassCastException] should be thrownBy {
      OrdinalSchema[Codex.LongCodex](LongCodex, List[Long](1,2,3)).isValid(DoubleValue(1))
    }
    a [ClassCastException] should be thrownBy { OrdinalSchema(LongCodex).isValid(StringValue("a")) }
    a [ClassCastException] should be thrownBy {
      OrdinalSchema[Codex.LongCodex](LongCodex, List[Long](1,2,3)).isValid(StringValue("a"))
    }

    a [ClassCastException] should be thrownBy { OrdinalSchema(DoubleCodex).isValid(LongValue(1)) }
    a [ClassCastException] should be thrownBy {
      OrdinalSchema[Codex.DoubleCodex](DoubleCodex, List[Double](1,2,3)).isValid(LongValue(1))
    }
    a [ClassCastException] should be thrownBy { OrdinalSchema(DoubleCodex).isValid(StringValue("a")) }
    a [ClassCastException] should be thrownBy {
      OrdinalSchema[Codex.DoubleCodex](DoubleCodex, List[Double](1,2,3)).isValid(StringValue("a"))
    }

    a [ClassCastException] should be thrownBy { OrdinalSchema(StringCodex).isValid(LongValue(1)) }
    a [ClassCastException] should be thrownBy {
      OrdinalSchema[Codex.StringCodex](StringCodex, List("a","b","c")).isValid(LongValue(1))
    }
    a [ClassCastException] should be thrownBy { OrdinalSchema(StringCodex).isValid(DoubleValue(1)) }
    a [ClassCastException] should be thrownBy {
      OrdinalSchema[Codex.StringCodex](StringCodex, List("a","b","c")).isValid(DoubleValue(1))
    }
  }

  it should "decode a correct value" in {
    OrdinalSchema(LongCodex).decode("1").map(_.value) shouldBe Some(LongValue(1))
    OrdinalSchema[Codex.LongCodex](LongCodex, List[Long](1,2,3)).decode("1").map(_.value) shouldBe Some(LongValue(1))

    OrdinalSchema(DoubleCodex).decode("1").map(_.value) shouldBe Some(DoubleValue(1))
    OrdinalSchema[Codex.DoubleCodex](DoubleCodex, List[Double](1,2,3)).decode("1").map(_.value) shouldBe
      Some(DoubleValue(1))

    OrdinalSchema(StringCodex).decode("a").map(_.value) shouldBe Some(StringValue("a"))
    OrdinalSchema[Codex.StringCodex](StringCodex, List("a","b","c")).decode("a").map(_.value) shouldBe
      Some(StringValue("a"))
  }

  it should "not decode an incorrect value" in {
    OrdinalSchema(LongCodex).decode("3.1415") shouldBe None
    OrdinalSchema(LongCodex).decode("a") shouldBe None
    OrdinalSchema[Codex.LongCodex](LongCodex, List[Long](1,2,3)).decode("4") shouldBe None
    OrdinalSchema[Codex.LongCodex](LongCodex, List[Long](1,2,3)).decode("a") shouldBe None

    OrdinalSchema(DoubleCodex).decode("a") shouldBe None
    OrdinalSchema[Codex.DoubleCodex](DoubleCodex, List[Double](1,2,3)).decode("4") shouldBe None
    OrdinalSchema[Codex.DoubleCodex](DoubleCodex, List[Double](1,2,3)).decode("a") shouldBe None

    OrdinalSchema[Codex.StringCodex](StringCodex, List("a","b","c")).decode("1") shouldBe None
    OrdinalSchema[Codex.StringCodex](StringCodex, List("a","b","c")).decode("d") shouldBe None
  }
}

class TestDateSchema extends TestGrimlock {

  val dfmt = new java.text.SimpleDateFormat("yyyy-MM-dd")
  val dtfmt = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")

  "A DateSchema" should "return its string representation" in {
    DateSchema(DateCodex("yyyy-MM-dd")).toString shouldBe "DateSchema(DateCodex(yyyy-MM-dd))"
    DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")).toString shouldBe "DateSchema(DateCodex(yyyy-MM-dd hh:mm:ss))"
  }

  it should "validate a correct value" in {
    DateSchema(DateCodex("yyyy-MM-dd"))
      .isValid(DateValue(dfmt.parse("2001-01-01"), DateCodex("yyyy-MM-dd"))) shouldBe true
    DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss"))
      .isValid(DateValue(dtfmt.parse("2001-01-01 01:02:02"), DateCodex("yyyy-MM-dd hh:mm:ss"))) shouldBe true
  }

  it should "throw an exception for an invalid value" in {
    a [ClassCastException] should be thrownBy { DateSchema(DateCodex("yyyy-MM-dd")).isValid(LongValue(1)) }
    a [ClassCastException] should be thrownBy { DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")).isValid(LongValue(1)) }
  }

  it should "decode a correct value" in {
    DateSchema(DateCodex("yyyy-MM-dd")).decode("2001-01-01").map(_.value) shouldBe
      Some(DateValue(dfmt.parse("2001-01-01"), DateCodex("yyyy-MM-dd")))
    DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")).decode("2001-01-01 01:02:02").map(_.value) shouldBe
      Some(DateValue(dtfmt.parse("2001-01-01 01:02:02"), DateCodex("yyyy-MM-dd hh:mm:ss")))
  }

  it should "not decode an incorrect value" in {
    DateSchema(DateCodex("yyyy-MM-dd")).decode("a") shouldBe None
    DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")).decode("a") shouldBe None
  }
}

