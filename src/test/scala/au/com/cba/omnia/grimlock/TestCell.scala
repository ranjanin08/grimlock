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

import au.com.cba.omnia.grimlock.framework._
import au.com.cba.omnia.grimlock.framework.content._
import au.com.cba.omnia.grimlock.framework.content.metadata._
import au.com.cba.omnia.grimlock.framework.encoding._
import au.com.cba.omnia.grimlock.framework.position._

class TestCell extends TestGrimlock {

  "A Cell" should "return its string" in {
    Cell(Position2D("foo", 123), Content(ContinuousSchema(DoubleCodex), 3.14)).toString(".", true) shouldBe
      "Position2D(StringValue(foo),LongValue(123)).Content(ContinuousSchema(DoubleCodex),DoubleValue(3.14))"
    Cell(Position2D("foo", 123), Content(ContinuousSchema(DoubleCodex), 3.14)).toString(".", false) shouldBe
      "foo.123.continuous.double.3.14"
    Cell(Position2D("foo", 123), Content(ContinuousSchema(DoubleCodex), 3.14)).toString(".", false, false) shouldBe
      "foo.123.3.14"
  }

  "A Cell" should "relocate" in {
    Cell(Position2D("foo", 123), Content(ContinuousSchema(DoubleCodex), 3.14))
      .relocate(_.position.append("abc")) shouldBe
        (Cell(Position3D("foo", 123, "abc"), Content(ContinuousSchema(DoubleCodex), 3.14)))
  }

  "A Cell" should "mutate" in {
    Cell(Position2D("foo", 123), Content(ContinuousSchema(DoubleCodex), 3.14))
      .mutate(_ => Content(DiscreteSchema(LongCodex), 42)) shouldBe
        (Cell(Position2D("foo", 123), Content(DiscreteSchema(LongCodex), 42)))
  }

  val schema = ContinuousSchema(DoubleCodex)
  val dictionary = Map("123" -> schema)

  "A Cell" should "parse 1D" in {
    Cell.parse1D(":", LongCodex)("123:continuous:double:3.14") shouldBe
      Some(Left(Cell(Position1D(123), Content(ContinuousSchema(DoubleCodex), 3.14))))
    Cell.parse1D(":", LongCodex)("abc:continuous:double:3.14") shouldBe
      Some(Right("Unable to decode: 'abc:continuous:double:3.14'"))
    Cell.parse1D(":", LongCodex)("123:continuous:double:abc") shouldBe
      Some(Right("Unable to decode: '123:continuous:double:abc'"))
    Cell.parse1D(":", LongCodex)("123:continuous:double:3:14") shouldBe
      Some(Right("Unable to decode: '123:continuous:double:3:14'"))
    Cell.parse1D(":", LongCodex)("123:continuous|double:3.14") shouldBe
      Some(Right("Unable to split: '123:continuous|double:3.14'"))
  }

  "A Cell" should "parse 1D with dictionary" in {
    Cell.parse1DWithDictionary(dictionary, ":", LongCodex)("123:3.14") shouldBe
      Some(Left(Cell(Position1D(123), Content(ContinuousSchema(DoubleCodex), 3.14))))
    Cell.parse1DWithDictionary(dictionary, ":", LongCodex)("abc:3.14") shouldBe
      Some(Right("Missing schema for: 'abc:3.14'"))
    Cell.parse1DWithDictionary(dictionary, ":", LongCodex)("123:abc") shouldBe
      Some(Right("Unable to decode: '123:abc'"))
    Cell.parse1DWithDictionary(dictionary, ":", LongCodex)("123:3:14") shouldBe
      Some(Right("Unable to decode: '123:3:14'"))
    Cell.parse1DWithDictionary(dictionary, ":", LongCodex)("123|3.14") shouldBe
      Some(Right("Unable to split: '123|3.14'"))
  }

  "A Cell" should "parse 1D with schema" in {
    Cell.parse1DWithSchema(schema, ":", LongCodex)("123:3.14") shouldBe
      Some(Left(Cell(Position1D(123), Content(ContinuousSchema(DoubleCodex), 3.14))))
    Cell.parse1DWithSchema(schema, ":", LongCodex)("abc:3.14") shouldBe
      Some(Right("Unable to decode: 'abc:3.14'"))
    Cell.parse1DWithSchema(schema, ":", LongCodex)("123:abc") shouldBe
      Some(Right("Unable to decode: '123:abc'"))
    Cell.parse1DWithSchema(schema, ":", LongCodex)("123:3:14") shouldBe
      Some(Right("Unable to decode: '123:3:14'"))
    Cell.parse1DWithSchema(schema, ":", LongCodex)("123|3.14") shouldBe
      Some(Right("Unable to split: '123|3.14'"))
  }

  "A Cell" should "parse 2D" in {
    Cell.parse2D(":", LongCodex, StringCodex)("123:def:continuous:double:3.14") shouldBe
      Some(Left(Cell(Position2D(123, "def"), Content(ContinuousSchema(DoubleCodex), 3.14))))
    Cell.parse2D(":", LongCodex, StringCodex)("abc:def:continuous:double:3.14") shouldBe
      Some(Right("Unable to decode: 'abc:def:continuous:double:3.14'"))
    Cell.parse2D(":", StringCodex, LongCodex)("abc:def:continuous:double:3.14") shouldBe
      Some(Right("Unable to decode: 'abc:def:continuous:double:3.14'"))
    Cell.parse2D(":", LongCodex, StringCodex)("123:def:continuous:double:abc") shouldBe
      Some(Right("Unable to decode: '123:def:continuous:double:abc'"))
    Cell.parse2D(":", LongCodex, StringCodex)("123:def:continuous:double:3:14") shouldBe
      Some(Right("Unable to decode: '123:def:continuous:double:3:14'"))
    Cell.parse2D(":", LongCodex, StringCodex)("123:def:continuous|double:3.14") shouldBe
      Some(Right("Unable to split: '123:def:continuous|double:3.14'"))
  }

  "A Cell" should "parse 2D with dictionary" in {
    Cell.parse2DWithDictionary(dictionary, First, ":", LongCodex, StringCodex)("123:def:3.14") shouldBe
      Some(Left(Cell(Position2D(123, "def"), Content(ContinuousSchema(DoubleCodex), 3.14))))
    Cell.parse2DWithDictionary(dictionary, Second, ":", StringCodex, LongCodex)("def:123:3.14") shouldBe
      Some(Left(Cell(Position2D("def", 123), Content(ContinuousSchema(DoubleCodex), 3.14))))
    Cell.parse2DWithDictionary(dictionary, First, ":", LongCodex, StringCodex)("abc:def:3.14") shouldBe
      Some(Right("Missing schema for: 'abc:def:3.14'"))
    Cell.parse2DWithDictionary(dictionary, Second, ":", LongCodex, StringCodex)("abc:def:3.14") shouldBe
      Some(Right("Missing schema for: 'abc:def:3.14'"))
    Cell.parse2DWithDictionary(dictionary, First, ":", LongCodex, StringCodex)("123:def:abc") shouldBe
      Some(Right("Unable to decode: '123:def:abc'"))
    Cell.parse2DWithDictionary(dictionary, First, ":", LongCodex, StringCodex)("123:def:3:14") shouldBe
      Some(Right("Unable to decode: '123:def:3:14'"))
    Cell.parse2DWithDictionary(dictionary, First, ":", LongCodex, StringCodex)("123|def:3.14") shouldBe
      Some(Right("Unable to split: '123|def:3.14'"))
  }

  "A Cell" should "parse 2D with schema" in {
    Cell.parse2DWithSchema(schema, ":", LongCodex, StringCodex)("123:def:3.14") shouldBe
      Some(Left(Cell(Position2D(123, "def"), Content(ContinuousSchema(DoubleCodex), 3.14))))
    Cell.parse2DWithSchema(schema, ":", LongCodex, StringCodex)("abc:def:3.14") shouldBe
      Some(Right("Unable to decode: 'abc:def:3.14'"))
    Cell.parse2DWithSchema(schema, ":", LongCodex, StringCodex)("123:def:abc") shouldBe
      Some(Right("Unable to decode: '123:def:abc'"))
    Cell.parse2DWithSchema(schema, ":", LongCodex, StringCodex)("123:def:3:14") shouldBe
      Some(Right("Unable to decode: '123:def:3:14'"))
    Cell.parse2DWithSchema(schema, ":", LongCodex, StringCodex)("123:def|3.14") shouldBe
      Some(Right("Unable to split: '123:def|3.14'"))
  }

  "A Cell" should "parse 3D" in {
    Cell.parse3D(":", LongCodex, StringCodex, StringCodex)("123:def:ghi:continuous:double:3.14") shouldBe
      Some(Left(Cell(Position3D(123, "def", "ghi"), Content(ContinuousSchema(DoubleCodex), 3.14))))
    Cell.parse3D(":", LongCodex, StringCodex, StringCodex)("abc:def:ghi:continuous:double:3.14") shouldBe
      Some(Right("Unable to decode: 'abc:def:ghi:continuous:double:3.14'"))
    Cell.parse3D(":", StringCodex, LongCodex, StringCodex)("def:abc:ghi:continuous:double:3.14") shouldBe
      Some(Right("Unable to decode: 'def:abc:ghi:continuous:double:3.14'"))
    Cell.parse3D(":", StringCodex, StringCodex, LongCodex)("def:ghi:abc:continuous:double:3.14") shouldBe
      Some(Right("Unable to decode: 'def:ghi:abc:continuous:double:3.14'"))
    Cell.parse3D(":", LongCodex, StringCodex, StringCodex)("123:def:ghi:continuous:double:abc") shouldBe
      Some(Right("Unable to decode: '123:def:ghi:continuous:double:abc'"))
    Cell.parse3D(":", LongCodex, StringCodex, StringCodex)("123:def:ghi:continuous:double:3:14") shouldBe
      Some(Right("Unable to decode: '123:def:ghi:continuous:double:3:14'"))
    Cell.parse3D(":", LongCodex, StringCodex, StringCodex)("123:def:ghi:continuous|double:3.14") shouldBe
      Some(Right("Unable to split: '123:def:ghi:continuous|double:3.14'"))
  }

  "A Cell" should "parse 3D with dictionary" in {
    Cell.parse3DWithDictionary(dictionary, First, ":", LongCodex, StringCodex, StringCodex)(
      "123:def:ghi:3.14") shouldBe
        Some(Left(Cell(Position3D(123, "def", "ghi"), Content(ContinuousSchema(DoubleCodex), 3.14))))
    Cell.parse3DWithDictionary(dictionary, Second, ":", StringCodex, LongCodex, StringCodex)(
      "def:123:ghi:3.14") shouldBe
        Some(Left(Cell(Position3D("def", 123, "ghi"), Content(ContinuousSchema(DoubleCodex), 3.14))))
    Cell.parse3DWithDictionary(dictionary, Third, ":", StringCodex, StringCodex, LongCodex)(
      "def:ghi:123:3.14") shouldBe
        Some(Left(Cell(Position3D("def", "ghi", 123), Content(ContinuousSchema(DoubleCodex), 3.14))))
    Cell.parse3DWithDictionary(dictionary, First, ":", LongCodex, StringCodex, StringCodex)(
      "abc:def:ghi:3.14") shouldBe Some(Right("Missing schema for: 'abc:def:ghi:3.14'"))
    Cell.parse3DWithDictionary(dictionary, Second, ":", LongCodex, StringCodex, StringCodex)(
      "abc:def:ghi:3.14") shouldBe Some(Right("Missing schema for: 'abc:def:ghi:3.14'"))
    Cell.parse3DWithDictionary(dictionary, Third, ":", LongCodex, StringCodex, StringCodex)(
      "abc:def:ghi:3.14") shouldBe Some(Right("Missing schema for: 'abc:def:ghi:3.14'"))
    Cell.parse3DWithDictionary(dictionary, First, ":", LongCodex, StringCodex, StringCodex)(
      "123:def:ghi:abc") shouldBe Some(Right("Unable to decode: '123:def:ghi:abc'"))
    Cell.parse3DWithDictionary(dictionary, First, ":", LongCodex, StringCodex, StringCodex)(
      "123:def:ghi:3:14") shouldBe Some(Right("Unable to decode: '123:def:ghi:3:14'"))
    Cell.parse3DWithDictionary(dictionary, First, ":", LongCodex, StringCodex, StringCodex)(
      "123|def:ghi:3.14") shouldBe Some(Right("Unable to split: '123|def:ghi:3.14'"))
  }

  "A Cell" should "parse 3D with schema" in {
    Cell.parse3DWithSchema(schema, ":", LongCodex, StringCodex, StringCodex)("123:def:ghi:3.14") shouldBe
      Some(Left(Cell(Position3D(123, "def", "ghi"), Content(ContinuousSchema(DoubleCodex), 3.14))))
    Cell.parse3DWithSchema(schema, ":", LongCodex, StringCodex, StringCodex)("abc:def:ghi:3.14") shouldBe
      Some(Right("Unable to decode: 'abc:def:ghi:3.14'"))
    Cell.parse3DWithSchema(schema, ":", LongCodex, StringCodex, StringCodex)("123:def:ghi:abc") shouldBe
      Some(Right("Unable to decode: '123:def:ghi:abc'"))
    Cell.parse3DWithSchema(schema, ":", LongCodex, StringCodex, StringCodex)("123:def:ghi:3:14") shouldBe
      Some(Right("Unable to decode: '123:def:ghi:3:14'"))
    Cell.parse3DWithSchema(schema, ":", LongCodex, StringCodex, StringCodex)("123:def|ghi:3.14") shouldBe
      Some(Right("Unable to split: '123:def|ghi:3.14'"))
  }

  "A Cell" should "parse 4D" in {
    Cell.parse4D(":", LongCodex, StringCodex, StringCodex, StringCodex)(
      "123:def:ghi:klm:continuous:double:3.14") shouldBe
        Some(Left(Cell(Position4D(123, "def", "ghi", "klm"), Content(ContinuousSchema(DoubleCodex), 3.14))))
    Cell.parse4D(":", LongCodex, StringCodex, StringCodex, StringCodex)(
      "abc:def:ghi:klm:continuous:double:3.14") shouldBe
        Some(Right("Unable to decode: 'abc:def:ghi:klm:continuous:double:3.14'"))
    Cell.parse4D(":", StringCodex, LongCodex, StringCodex, StringCodex)(
      "def:abc:ghi:klm:continuous:double:3.14") shouldBe
        Some(Right("Unable to decode: 'def:abc:ghi:klm:continuous:double:3.14'"))
    Cell.parse4D(":", StringCodex, StringCodex, LongCodex, StringCodex)(
      "def:ghi:abc:klm:continuous:double:3.14") shouldBe
        Some(Right("Unable to decode: 'def:ghi:abc:klm:continuous:double:3.14'"))
    Cell.parse4D(":", StringCodex, StringCodex, StringCodex, LongCodex)(
      "def:ghi:klm:abc:continuous:double:3.14") shouldBe
        Some(Right("Unable to decode: 'def:ghi:klm:abc:continuous:double:3.14'"))
    Cell.parse4D(":", LongCodex, StringCodex, StringCodex, StringCodex)(
      "123:def:ghi:klm:continuous:double:abc") shouldBe
        Some(Right("Unable to decode: '123:def:ghi:klm:continuous:double:abc'"))
    Cell.parse4D(":", LongCodex, StringCodex, StringCodex, StringCodex)(
      "123:def:ghi:klm:continuous:double:3:14") shouldBe
        Some(Right("Unable to decode: '123:def:ghi:klm:continuous:double:3:14'"))
    Cell.parse4D(":", LongCodex, StringCodex, StringCodex, StringCodex)(
      "123:def:ghi:klm:continuous|double:3.14") shouldBe
        Some(Right("Unable to split: '123:def:ghi:klm:continuous|double:3.14'"))
  }

  "A Cell" should "parse 4D with dictionary" in {
    Cell.parse4DWithDictionary(dictionary, First, ":", LongCodex, StringCodex, StringCodex, StringCodex)(
      "123:def:ghi:klm:3.14") shouldBe
        Some(Left(Cell(Position4D(123, "def", "ghi", "klm"), Content(ContinuousSchema(DoubleCodex), 3.14))))
    Cell.parse4DWithDictionary(dictionary, Second, ":", StringCodex, LongCodex, StringCodex, StringCodex)(
      "def:123:ghi:klm:3.14") shouldBe
        Some(Left(Cell(Position4D("def", 123, "ghi", "klm"), Content(ContinuousSchema(DoubleCodex), 3.14))))
    Cell.parse4DWithDictionary(dictionary, Third, ":", StringCodex, StringCodex, LongCodex, StringCodex)(
      "def:ghi:123:klm:3.14") shouldBe
        Some(Left(Cell(Position4D("def", "ghi", 123, "klm"), Content(ContinuousSchema(DoubleCodex), 3.14))))
    Cell.parse4DWithDictionary(dictionary, Fourth, ":", StringCodex, StringCodex, StringCodex, LongCodex)(
      "def:ghi:klm:123:3.14") shouldBe
        Some(Left(Cell(Position4D("def", "ghi", "klm", 123), Content(ContinuousSchema(DoubleCodex), 3.14))))
    Cell.parse4DWithDictionary(dictionary, First, ":", LongCodex, StringCodex, StringCodex, StringCodex)(
      "abc:def:ghi:klm:3.14") shouldBe Some(Right("Missing schema for: 'abc:def:ghi:klm:3.14'"))
    Cell.parse4DWithDictionary(dictionary, Second, ":", LongCodex, StringCodex, StringCodex, StringCodex)(
      "abc:def:ghi:klm:3.14") shouldBe Some(Right("Missing schema for: 'abc:def:ghi:klm:3.14'"))
    Cell.parse4DWithDictionary(dictionary, Third, ":", LongCodex, StringCodex, StringCodex, StringCodex)(
      "abc:def:ghi:klm:3.14") shouldBe Some(Right("Missing schema for: 'abc:def:ghi:klm:3.14'"))
    Cell.parse4DWithDictionary(dictionary, Fourth, ":", LongCodex, StringCodex, StringCodex, StringCodex)(
      "abc:def:ghi:klm:3.14") shouldBe Some(Right("Missing schema for: 'abc:def:ghi:klm:3.14'"))
    Cell.parse4DWithDictionary(dictionary, First, ":", LongCodex, StringCodex, StringCodex, StringCodex)(
      "123:def:ghi:klm:abc") shouldBe Some(Right("Unable to decode: '123:def:ghi:klm:abc'"))
    Cell.parse4DWithDictionary(dictionary, First, ":", LongCodex, StringCodex, StringCodex, StringCodex)(
      "123:def:ghi:klm:3:14") shouldBe Some(Right("Unable to decode: '123:def:ghi:klm:3:14'"))
    Cell.parse4DWithDictionary(dictionary, First, ":", LongCodex, StringCodex, StringCodex, StringCodex)(
      "123|def:ghi:klm:3.14") shouldBe Some(Right("Unable to split: '123|def:ghi:klm:3.14'"))
  }

  "A Cell" should "parse 4D with schema" in {
    Cell.parse4DWithSchema(schema, ":", LongCodex, StringCodex, StringCodex, StringCodex)(
      "123:def:ghi:klm:3.14") shouldBe
        Some(Left(Cell(Position4D(123, "def", "ghi", "klm"), Content(ContinuousSchema(DoubleCodex), 3.14))))
    Cell.parse4DWithSchema(schema, ":", LongCodex, StringCodex, StringCodex, StringCodex)(
      "abc:def:ghi:klm:3.14") shouldBe
        Some(Right("Unable to decode: 'abc:def:ghi:klm:3.14'"))
    Cell.parse4DWithSchema(schema, ":", LongCodex, StringCodex, StringCodex, StringCodex)(
      "123:def:ghi:klm:abc") shouldBe
        Some(Right("Unable to decode: '123:def:ghi:klm:abc'"))
    Cell.parse4DWithSchema(schema, ":", LongCodex, StringCodex, StringCodex, StringCodex)(
      "123:def:ghi:klm:3:14") shouldBe
        Some(Right("Unable to decode: '123:def:ghi:klm:3:14'"))
    Cell.parse4DWithSchema(schema, ":", LongCodex, StringCodex, StringCodex, StringCodex)(
      "123:def|ghi:klm:3.14") shouldBe
        Some(Right("Unable to split: '123:def|ghi:klm:3.14'"))
  }

  "A Cell" should "parse 5D" in {
    Cell.parse5D(":", LongCodex, StringCodex, StringCodex, StringCodex, StringCodex)(
      "123:def:ghi:klm:xyz:continuous:double:3.14") shouldBe
        Some(Left(Cell(Position5D(123, "def", "ghi", "klm", "xyz"), Content(ContinuousSchema(DoubleCodex), 3.14))))
    Cell.parse5D(":", LongCodex, StringCodex, StringCodex, StringCodex, StringCodex)(
      "abc:def:ghi:klm:xyz:continuous:double:3.14") shouldBe
        Some(Right("Unable to decode: 'abc:def:ghi:klm:xyz:continuous:double:3.14'"))
    Cell.parse5D(":", StringCodex, LongCodex, StringCodex, StringCodex, StringCodex)(
      "def:abc:ghi:klm:xyz:continuous:double:3.14") shouldBe
        Some(Right("Unable to decode: 'def:abc:ghi:klm:xyz:continuous:double:3.14'"))
    Cell.parse5D(":", StringCodex, StringCodex, LongCodex, StringCodex, StringCodex)(
      "def:ghi:abc:klm:xyz:continuous:double:3.14") shouldBe
        Some(Right("Unable to decode: 'def:ghi:abc:klm:xyz:continuous:double:3.14'"))
    Cell.parse5D(":", StringCodex, StringCodex, StringCodex, LongCodex, StringCodex)(
      "def:ghi:klm:abc:xyz:continuous:double:3.14") shouldBe
        Some(Right("Unable to decode: 'def:ghi:klm:abc:xyz:continuous:double:3.14'"))
    Cell.parse5D(":", StringCodex, StringCodex, StringCodex, StringCodex, LongCodex)(
      "def:ghi:klm:xyz:abc:continuous:double:3.14") shouldBe
        Some(Right("Unable to decode: 'def:ghi:klm:xyz:abc:continuous:double:3.14'"))
    Cell.parse5D(":", LongCodex, StringCodex, StringCodex, StringCodex, StringCodex)(
      "123:def:ghi:klm:xyz:continuous:double:abc") shouldBe
        Some(Right("Unable to decode: '123:def:ghi:klm:xyz:continuous:double:abc'"))
    Cell.parse5D(":", LongCodex, StringCodex, StringCodex, StringCodex, StringCodex)(
      "123:def:ghi:klm:xyz:continuous:double:3:14") shouldBe
        Some(Right("Unable to decode: '123:def:ghi:klm:xyz:continuous:double:3:14'"))
    Cell.parse5D(":", LongCodex, StringCodex, StringCodex, StringCodex, StringCodex)(
      "123:def:ghi:klm:xyz:continuous|double:3.14") shouldBe
        Some(Right("Unable to split: '123:def:ghi:klm:xyz:continuous|double:3.14'"))
  }

  "A Cell" should "parse 5D with dictionary" in {
    Cell.parse5DWithDictionary(dictionary, First, ":", LongCodex, StringCodex, StringCodex, StringCodex, StringCodex)(
      "123:def:ghi:klm:xyz:3.14") shouldBe
        Some(Left(Cell(Position5D(123, "def", "ghi", "klm", "xyz"), Content(ContinuousSchema(DoubleCodex), 3.14))))
    Cell.parse5DWithDictionary(dictionary, Second, ":", StringCodex, LongCodex, StringCodex, StringCodex, StringCodex)(
      "def:123:ghi:klm:xyz:3.14") shouldBe
        Some(Left(Cell(Position5D("def", 123, "ghi", "klm", "xyz"), Content(ContinuousSchema(DoubleCodex), 3.14))))
    Cell.parse5DWithDictionary(dictionary, Third, ":", StringCodex, StringCodex, LongCodex, StringCodex, StringCodex)(
      "def:ghi:123:klm:xyz:3.14") shouldBe
        Some(Left(Cell(Position5D("def", "ghi", 123, "klm", "xyz"), Content(ContinuousSchema(DoubleCodex), 3.14))))
    Cell.parse5DWithDictionary(dictionary, Fourth, ":", StringCodex, StringCodex, StringCodex, LongCodex, StringCodex)(
      "def:ghi:klm:123:xyz:3.14") shouldBe
        Some(Left(Cell(Position5D("def", "ghi", "klm", 123, "xyz"), Content(ContinuousSchema(DoubleCodex), 3.14))))
    Cell.parse5DWithDictionary(dictionary, Fifth, ":", StringCodex, StringCodex, StringCodex, StringCodex, LongCodex)(
      "def:ghi:klm:xyz:123:3.14") shouldBe
        Some(Left(Cell(Position5D("def", "ghi", "klm", "xyz", 123), Content(ContinuousSchema(DoubleCodex), 3.14))))
    Cell.parse5DWithDictionary(dictionary, First, ":", LongCodex, StringCodex, StringCodex, StringCodex, StringCodex)(
      "abc:def:ghi:klm:xyz:3.14") shouldBe Some(Right("Missing schema for: 'abc:def:ghi:klm:xyz:3.14'"))
    Cell.parse5DWithDictionary(dictionary, Second, ":", LongCodex, StringCodex, StringCodex, StringCodex, StringCodex)(
      "abc:def:ghi:klm:xyz:3.14") shouldBe Some(Right("Missing schema for: 'abc:def:ghi:klm:xyz:3.14'"))
    Cell.parse5DWithDictionary(dictionary, Third, ":", LongCodex, StringCodex, StringCodex, StringCodex, StringCodex)(
      "abc:def:ghi:klm:xyz:3.14") shouldBe Some(Right("Missing schema for: 'abc:def:ghi:klm:xyz:3.14'"))
    Cell.parse5DWithDictionary(dictionary, Fourth, ":", LongCodex, StringCodex, StringCodex, StringCodex, StringCodex)(
      "abc:def:ghi:klm:xyz:3.14") shouldBe Some(Right("Missing schema for: 'abc:def:ghi:klm:xyz:3.14'"))
    Cell.parse5DWithDictionary(dictionary, Fifth, ":", LongCodex, StringCodex, StringCodex, StringCodex, StringCodex)(
      "abc:def:ghi:klm:xyz:3.14") shouldBe Some(Right("Missing schema for: 'abc:def:ghi:klm:xyz:3.14'"))
    Cell.parse5DWithDictionary(dictionary, First, ":", LongCodex, StringCodex, StringCodex, StringCodex, StringCodex)(
      "123:def:ghi:klm:xyz:abc") shouldBe Some(Right("Unable to decode: '123:def:ghi:klm:xyz:abc'"))
    Cell.parse5DWithDictionary(dictionary, First, ":", LongCodex, StringCodex, StringCodex, StringCodex, StringCodex)(
      "123:def:ghi:klm:xyz:3:14") shouldBe Some(Right("Unable to decode: '123:def:ghi:klm:xyz:3:14'"))
    Cell.parse5DWithDictionary(dictionary, First, ":", LongCodex, StringCodex, StringCodex, StringCodex, StringCodex)(
      "123|def:ghi:klm:xyz:3.14") shouldBe Some(Right("Unable to split: '123|def:ghi:klm:xyz:3.14'"))
  }

  "A Cell" should "parse 5D with schema" in {
    Cell.parse5DWithSchema(schema, ":", LongCodex, StringCodex, StringCodex, StringCodex, StringCodex)(
      "123:def:ghi:klm:xyz:3.14") shouldBe
        Some(Left(Cell(Position5D(123, "def", "ghi", "klm", "xyz"), Content(ContinuousSchema(DoubleCodex), 3.14))))
    Cell.parse5DWithSchema(schema, ":", LongCodex, StringCodex, StringCodex, StringCodex, StringCodex)(
      "abc:def:ghi:klm:xyz:3.14") shouldBe
        Some(Right("Unable to decode: 'abc:def:ghi:klm:xyz:3.14'"))
    Cell.parse5DWithSchema(schema, ":", LongCodex, StringCodex, StringCodex, StringCodex, StringCodex)(
      "123:def:ghi:klm:xyz:abc") shouldBe
        Some(Right("Unable to decode: '123:def:ghi:klm:xyz:abc'"))
    Cell.parse5DWithSchema(schema, ":", LongCodex, StringCodex, StringCodex, StringCodex, StringCodex)(
      "123:def:ghi:klm:xyz:3:14") shouldBe
        Some(Right("Unable to decode: '123:def:ghi:klm:xyz:3:14'"))
    Cell.parse5DWithSchema(schema, ":", LongCodex, StringCodex, StringCodex, StringCodex, StringCodex)(
      "123:def|ghi:klm:xyz:3.14") shouldBe
        Some(Right("Unable to split: '123:def|ghi:klm:xyz:3.14'"))
  }

  val columns = List(("abc", ContinuousSchema(DoubleCodex)),
    ("def", ContinuousSchema(DoubleCodex)),
    ("ghi", ContinuousSchema(DoubleCodex)))

  "A Cell" should "parse table" in {
    Cell.parseTable(columns, 0, ":")("3.14:6.28:9.42") shouldBe List(
      Left(Cell(Position2D("3.14", "def"), Content(ContinuousSchema(DoubleCodex), 6.28))),
      Left(Cell(Position2D("3.14", "ghi"), Content(ContinuousSchema(DoubleCodex), 9.42))))
    Cell.parseTable(columns, 1, ":")("3.14:6.28:9.42") shouldBe List(
      Left(Cell(Position2D("6.28", "abc"), Content(ContinuousSchema(DoubleCodex), 3.14))),
      Left(Cell(Position2D("6.28", "ghi"), Content(ContinuousSchema(DoubleCodex), 9.42))))
    Cell.parseTable(columns, 2, ":")("3.14:6.28:9.42") shouldBe List(
      Left(Cell(Position2D("9.42", "abc"), Content(ContinuousSchema(DoubleCodex), 3.14))),
      Left(Cell(Position2D("9.42", "def"), Content(ContinuousSchema(DoubleCodex), 6.28))))
    Cell.parseTable(columns, 0, ":")("3.14:foo:bar") shouldBe List(
      Right("Unable to decode: '3.14:foo:bar'"),
      Right("Unable to decode: '3.14:foo:bar'"))
    Cell.parseTable(columns, 0, ":")("3.14:foo") shouldBe List(Right("Unable to split: '3.14:foo'"))
  }
}

