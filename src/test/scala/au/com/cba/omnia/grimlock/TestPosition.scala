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

import au.com.cba.omnia.grimlock.scalding.position.Positions._

import au.com.cba.omnia.grimlock.spark.position.Positions._

import com.twitter.scalding._
import com.twitter.scalding.bdd._

class TestPosition0D extends TestGrimlock {

  val pos = Position0D()

  "A Position0D" should "return its short string" in {
    pos.toShortString("|") shouldBe ""
  }

  it should "have coordinates" in {
    pos.coordinates shouldBe List()
  }

  it should "throw an exception for an invalid dimension" in {
    a [IndexOutOfBoundsException] shouldBe thrownBy { pos(First) }
    a [IndexOutOfBoundsException] shouldBe thrownBy { pos(Second) }
    a [IndexOutOfBoundsException] shouldBe thrownBy { pos(Third) }
    a [IndexOutOfBoundsException] shouldBe thrownBy { pos(Fourth) }
    a [IndexOutOfBoundsException] shouldBe thrownBy { pos(Fifth) }
  }

  it should "compare" in {
    pos.compare(pos) shouldBe 0
    pos.compare(Position1D("xyz")) should be < 0
  }

  it should "prepend" in {
    pos.prepend(123) shouldBe Position1D(123)
  }

  it should "append" in {
    pos.append(123) shouldBe Position1D(123)
  }
}

class TestPosition1D extends TestGrimlock {

  val pos = Position1D("foo")

  "A Position1D" should "return its short string" in {
    pos.toShortString("|") shouldBe "foo"
  }

  it should "have coordinates" in {
    pos.coordinates shouldBe List(StringValue("foo"))
  }

  it should "return its coordinates" in {
    pos(First) shouldBe StringValue("foo")
  }

  it should "throw an exception for an invalid dimension" in {
    a [IndexOutOfBoundsException] shouldBe thrownBy { pos(Second) }
    a [IndexOutOfBoundsException] shouldBe thrownBy { pos(Third) }
    a [IndexOutOfBoundsException] shouldBe thrownBy { pos(Fourth) }
    a [IndexOutOfBoundsException] shouldBe thrownBy { pos(Fifth) }
  }

  it should "compare" in {
    pos.compare(Position1D("abc")) should be > 0
    pos.compare(pos) shouldBe 0
    pos.compare(Position1D("xyz")) should be < 0
    pos.compare(Position2D("abc", "xyz")) should be < 0
  }

  it should "throw an exception for an invalid value" in {
    a [Exception] shouldBe thrownBy { pos.compare(Position1D(123)) }
  }

  it should "be updated" in {
    pos.update(First, 123) shouldBe Position1D(123)
  }

  it should "throw an exception for an invalid update dimension" in {
    a [IndexOutOfBoundsException] shouldBe thrownBy { pos.update(Second, 123) }
    a [IndexOutOfBoundsException] shouldBe thrownBy { pos.update(Third, 123) }
    a [IndexOutOfBoundsException] shouldBe thrownBy { pos.update(Fourth, 123) }
    a [IndexOutOfBoundsException] shouldBe thrownBy { pos.update(Fifth, 123) }
  }

  val con1 = Content(ContinuousSchema(LongCodex), 1)
  val con2 = Content(ContinuousSchema(LongCodex), 2)

  it should "return a over map" in {
    pos.over.toMapValue(Position0D(), con1) shouldBe con1
  }

  it should "combine two over maps" in {
    pos.over.combineMapValues(Some(con1), con2) shouldBe con2
    pos.over.combineMapValues(None, con2) shouldBe con2
  }

  it should "throw an exception for an along map" in {
    a [Exception] shouldBe thrownBy { pos.along.toMapValue(pos, con1) }
  }

  it should "throw an exception on combine two along maps" in {
    a [Exception] shouldBe thrownBy { pos.along.combineMapValues(Some(con1), con2) }
    a [Exception] shouldBe thrownBy { pos.along.combineMapValues(None, con2) }
  }

  it should "remove" in {
    pos.remove(First) shouldBe Position0D()
  }

  it should "throw an exception for an invalid remove dimension" in {
    a [UnsupportedOperationException] shouldBe thrownBy { pos.remove(Second) }
    a [UnsupportedOperationException] shouldBe thrownBy { pos.remove(Third) }
    a [UnsupportedOperationException] shouldBe thrownBy { pos.remove(Fourth) }
    a [UnsupportedOperationException] shouldBe thrownBy { pos.remove(Fifth) }
  }

  it should "melt" in {
    pos.melt(First, First, "|") shouldBe Position0D()
  }

  it should "throw an exception for an invalid melt dimension" in {
    a [IndexOutOfBoundsException] shouldBe thrownBy { pos.melt(First, Second, "|") }
    a [IndexOutOfBoundsException] shouldBe thrownBy { pos.melt(First, Third, "|") }
    a [IndexOutOfBoundsException] shouldBe thrownBy { pos.melt(First, Fourth, "|") }
    a [IndexOutOfBoundsException] shouldBe thrownBy { pos.melt(First, Fifth, "|") }

    a [IndexOutOfBoundsException] shouldBe thrownBy { pos.melt(Second, First, "|") }
    a [IndexOutOfBoundsException] shouldBe thrownBy { pos.melt(Third, First, "|") }
    a [IndexOutOfBoundsException] shouldBe thrownBy { pos.melt(Fourth, First, "|") }
    a [IndexOutOfBoundsException] shouldBe thrownBy { pos.melt(Fifth, First, "|") }
  }

  it should "prepend" in {
    pos.prepend(123) shouldBe Position2D(123, "foo")
  }

  it should "append" in {
    pos.append(123) shouldBe Position2D("foo", 123)
  }
}

class TestPosition2D extends TestGrimlock {

  val pos = Position2D("foo", 123)

  "A Position2D" should "return its short string" in {
    pos.toShortString("|") shouldBe "foo|123"
  }

  it should "have coordinates" in {
    pos.coordinates shouldBe List(StringValue("foo"), LongValue(123))
  }

  it should "return its coordinates" in {
    pos(First) shouldBe StringValue("foo")
    pos(Second) shouldBe LongValue(123)
  }

  it should "throw an exception for an invalid dimension" in {
    a [IndexOutOfBoundsException] shouldBe thrownBy { pos(Third) }
    a [IndexOutOfBoundsException] shouldBe thrownBy { pos(Fourth) }
    a [IndexOutOfBoundsException] shouldBe thrownBy { pos(Fifth) }
  }

  it should "compare" in {
    pos.compare(Position2D("abc", 456)) should be > 0
    pos.compare(Position2D("foo", 1)) should be > 0
    pos.compare(pos) shouldBe 0
    pos.compare(Position2D("xyz", 1)) should be < 0
    pos.compare(Position2D("foo", 789)) should be < 0
    pos.compare(Position1D("xyz")) should be > 0
    pos.compare(Position3D("abc", 1, "bar")) should be < 0
  }

  it should "throw an exception for an invalid value" in {
    a [Exception] shouldBe thrownBy { pos.compare(Position2D(123, 456)) }
    a [Exception] shouldBe thrownBy { pos.compare(Position2D("xyz", "foo")) }
  }

  it should "be updated" in {
    pos.update(First, 123) shouldBe Position2D(123, 123)
    pos.update(Second, "xyz") shouldBe Position2D("foo", "xyz")
  }

  it should "throw an exception for an invalid update dimension" in {
    a [IndexOutOfBoundsException] shouldBe thrownBy { pos.update(Third, 123) }
    a [IndexOutOfBoundsException] shouldBe thrownBy { pos.update(Fourth, 123) }
    a [IndexOutOfBoundsException] shouldBe thrownBy { pos.update(Fifth, 123) }
  }

  it should "permute" in {
    pos.permute(List(First, Second)) shouldBe pos
    pos.permute(List(Second, First)) shouldBe Position2D(123, "foo")
  }

  it should "throw an exception for an invalid permute dimension" in {
    a [IndexOutOfBoundsException] shouldBe thrownBy { pos.permute(List(Third)) }
    a [IndexOutOfBoundsException] shouldBe thrownBy { pos.permute(List(First, Second, Third)) }
  }

  val con1 = Content(ContinuousSchema(LongCodex), 1)
  val con2 = Content(ContinuousSchema(LongCodex), 2)

  it should "return a over map" in {
    pos.over.toMapValue(Position1D("xyz"), con1) shouldBe Map(Position1D("xyz") -> con1)
  }

  it should "combine two over maps" in {
    pos.over.combineMapValues(Some(Map(Position1D("xyz") -> con1, Position1D("abc") -> con2)),
      Map(Position1D("xyz") -> con2, Position1D("klm") -> con1)) shouldBe
        Map(Position1D("xyz") -> con2, Position1D("abc") -> con2, Position1D("klm") -> con1)
    pos.over.combineMapValues(None, Map(Position1D("xyz") -> con2)) shouldBe Map(Position1D("xyz") -> con2)
  }

  it should "return an along map" in {
    pos.along.toMapValue(Position1D("xyz"), con1) shouldBe Map(Position1D("xyz") -> con1)
  }

  it should "combine two along maps" in {
    pos.along.combineMapValues(Some(Map(Position1D("xyz") -> con1, Position1D("abc") -> con2)),
      Map(Position1D("xyz") -> con2, Position1D("klm") -> con1)) shouldBe
        Map(Position1D("xyz") -> con2, Position1D("abc") -> con2, Position1D("klm") -> con1)
    pos.along.combineMapValues(None, Map(Position1D("xyz") -> con2)) shouldBe Map(Position1D("xyz") -> con2)
  }

  it should "remove" in {
    pos.remove(First) shouldBe Position1D(123)
    pos.remove(Second) shouldBe Position1D("foo")
  }

  it should "throw an exception for an invalid remove dimension" in {
    a [UnsupportedOperationException] shouldBe thrownBy { pos.remove(Third) }
    a [UnsupportedOperationException] shouldBe thrownBy { pos.remove(Fourth) }
    a [UnsupportedOperationException] shouldBe thrownBy { pos.remove(Fifth) }
  }

  it should "melt" in {
    pos.melt(First, First, "|") shouldBe Position1D(123)
    pos.melt(First, Second, "|") shouldBe Position1D("123|foo")
    pos.melt(Second, First, "|") shouldBe Position1D("foo|123")
    pos.melt(Second, Second, "|") shouldBe Position1D("foo")
  }

  it should "throw an exception for an invalid melt dimension" in {
    a [IndexOutOfBoundsException] shouldBe thrownBy { pos.melt(First, Third, "|") }
    a [IndexOutOfBoundsException] shouldBe thrownBy { pos.melt(First, Fourth, "|") }
    a [IndexOutOfBoundsException] shouldBe thrownBy { pos.melt(First, Fifth, "|") }

    a [IndexOutOfBoundsException] shouldBe thrownBy { pos.melt(Third, First, "|") }
    a [IndexOutOfBoundsException] shouldBe thrownBy { pos.melt(Fourth, First, "|") }
    a [IndexOutOfBoundsException] shouldBe thrownBy { pos.melt(Fifth, First, "|") }

    a [IndexOutOfBoundsException] shouldBe thrownBy { pos.melt(Second, Third, "|") }
    a [IndexOutOfBoundsException] shouldBe thrownBy { pos.melt(Second, Fourth, "|") }
    a [IndexOutOfBoundsException] shouldBe thrownBy { pos.melt(Second, Fifth, "|") }

    a [IndexOutOfBoundsException] shouldBe thrownBy { pos.melt(Third, Second, "|") }
    a [IndexOutOfBoundsException] shouldBe thrownBy { pos.melt(Fourth, Second, "|") }
    a [IndexOutOfBoundsException] shouldBe thrownBy { pos.melt(Fifth, Second, "|") }
  }

  it should "prepend" in {
    pos.prepend(123) shouldBe Position3D(123, "foo", 123)
  }

  it should "append" in {
    pos.append(123) shouldBe Position3D("foo", 123, 123)
  }
}

class TestPosition3D extends TestGrimlock {

  val pos = Position3D("foo", 123, "bar")

  "A Position3D" should "return its short string" in {
    pos.toShortString("|") shouldBe "foo|123|bar"
  }

  it should "have coordinates" in {
    pos.coordinates shouldBe List(StringValue("foo"), LongValue(123), StringValue("bar"))
  }

  it should "return its coordinates" in {
    pos(First) shouldBe StringValue("foo")
    pos(Second) shouldBe LongValue(123)
    pos(Third) shouldBe StringValue("bar")
  }

  it should "throw an exception for an invalid dimension" in {
    a [IndexOutOfBoundsException] shouldBe thrownBy { pos(Fourth) }
    a [IndexOutOfBoundsException] shouldBe thrownBy { pos(Fifth) }
  }

  it should "compare" in {
    pos.compare(Position3D("abc", 456, "xyz")) should be > 0
    pos.compare(Position3D("foo", 1, "xyz")) should be > 0
    pos.compare(Position3D("foo", 123, "abc")) should be > 0
    pos.compare(pos) shouldBe 0
    pos.compare(Position3D("xyz", 1, "abc")) should be < 0
    pos.compare(Position3D("foo", 789, "abc")) should be < 0
    pos.compare(Position3D("foo", 123, "xyz")) should be < 0
    pos.compare(Position2D("xyz", 456)) should be > 0
    pos.compare(Position4D("abc", 1, "abc", 1)) should be < 0
  }

  it should "throw an exception for an invalid value" in {
    a [Exception] shouldBe thrownBy { pos.compare(Position3D(123, 123, "bar")) }
    a [Exception] shouldBe thrownBy { pos.compare(Position3D("foo", "123", "bar")) }
    a [Exception] shouldBe thrownBy { pos.compare(Position3D("foo", 123, 123)) }
  }

  it should "be updated" in {
    pos.update(First, 123) shouldBe Position3D(123, 123, "bar")
    pos.update(Second, "xyz") shouldBe Position3D("foo", "xyz", "bar")
    pos.update(Third, 456) shouldBe Position3D("foo", 123, 456)
  }

  it should "throw an exception for an invalid update dimension" in {
    a [IndexOutOfBoundsException] shouldBe thrownBy { pos.update(Fourth, 123) }
    a [IndexOutOfBoundsException] shouldBe thrownBy { pos.update(Fifth, 123) }
  }

  it should "permute" in {
    pos.permute(List(First, Second, Third)) shouldBe pos
    pos.permute(List(First, Third, Second)) shouldBe Position3D("foo", "bar", 123)
    pos.permute(List(Second, First, Third)) shouldBe Position3D(123, "foo", "bar")
    pos.permute(List(Second, Third, First)) shouldBe Position3D(123, "bar", "foo")
    pos.permute(List(Third, First, Second)) shouldBe Position3D("bar", "foo", 123)
    pos.permute(List(Third, Second, First)) shouldBe Position3D("bar", 123, "foo")
  }

  it should "throw an exception for an invalid permute dimension" in {
    a [IndexOutOfBoundsException] shouldBe thrownBy { pos.permute(List(Fourth)) }
    a [IndexOutOfBoundsException] shouldBe thrownBy { pos.permute(List(First, Second, Third, Fourth)) }
  }

  val con1 = Content(ContinuousSchema(LongCodex), 1)
  val con2 = Content(ContinuousSchema(LongCodex), 2)

  it should "return a over map" in {
    pos.over.toMapValue(Position2D("xyz", 456), con1) shouldBe Map(Position2D("xyz", 456) -> con1)
  }

  it should "combine two over maps" in {
    pos.over.combineMapValues(Some(Map(Position2D("xyz", 456) -> con1, Position2D("abc", 123) -> con2)),
      Map(Position2D("xyz", 456) -> con2, Position2D("klm", 321) -> con1)) shouldBe
        Map(Position2D("xyz", 456) -> con2, Position2D("abc", 123) -> con2, Position2D("klm", 321) -> con1)
    pos.over.combineMapValues(None, Map(Position2D("xyz", 456) -> con2)) shouldBe Map(Position2D("xyz", 456) -> con2)
  }

  it should "return an along map" in {
    pos.along.toMapValue(Position1D("xyz"), con1) shouldBe Map(Position1D("xyz") -> con1)
  }

  it should "combine two along maps" in {
    pos.along.combineMapValues(Some(Map(Position1D("xyz") -> con1, Position1D("abc") -> con2)),
      Map(Position1D("xyz") -> con2, Position1D("klm") -> con1)) shouldBe
        Map(Position1D("xyz") -> con2, Position1D("abc") -> con2, Position1D("klm") -> con1)
    pos.along.combineMapValues(None, Map(Position1D("xyz") -> con2)) shouldBe Map(Position1D("xyz") -> con2)
  }

  it should "remove" in {
    pos.remove(First) shouldBe Position2D(123, "bar")
    pos.remove(Second) shouldBe Position2D("foo", "bar")
    pos.remove(Third) shouldBe Position2D("foo", 123)
  }

  it should "throw an exception for an invalid remove dimension" in {
    a [UnsupportedOperationException] shouldBe thrownBy { pos.remove(Fourth) }
    a [UnsupportedOperationException] shouldBe thrownBy { pos.remove(Fifth) }
  }

  it should "melt" in {
    pos.melt(First, First, "|") shouldBe Position2D(123, "bar")
    pos.melt(First, Second, "|") shouldBe Position2D("123|foo", "bar")
    pos.melt(First, Third, "|") shouldBe Position2D(123, "bar|foo")

    pos.melt(Second, First, "|") shouldBe Position2D("foo|123", "bar")
    pos.melt(Second, Second, "|") shouldBe Position2D("foo", "bar")
    pos.melt(Second, Third, "|") shouldBe Position2D("foo", "bar|123")

    pos.melt(Third, First, "|") shouldBe Position2D("foo|bar", 123)
    pos.melt(Third, Second, "|") shouldBe Position2D("foo", "123|bar")
    pos.melt(Third, Third, "|") shouldBe Position2D("foo", 123)
  }

  it should "throw an exception for an invalid melt dimension" in {
    a [IndexOutOfBoundsException] shouldBe thrownBy { pos.melt(First, Fourth, "|") }
    a [IndexOutOfBoundsException] shouldBe thrownBy { pos.melt(First, Fifth, "|") }

    a [IndexOutOfBoundsException] shouldBe thrownBy { pos.melt(Fourth, First, "|") }
    a [IndexOutOfBoundsException] shouldBe thrownBy { pos.melt(Fifth, First, "|") }

    a [IndexOutOfBoundsException] shouldBe thrownBy { pos.melt(Second, Fourth, "|") }
    a [IndexOutOfBoundsException] shouldBe thrownBy { pos.melt(Second, Fifth, "|") }

    a [IndexOutOfBoundsException] shouldBe thrownBy { pos.melt(Fourth, Second, "|") }
    a [IndexOutOfBoundsException] shouldBe thrownBy { pos.melt(Fifth, Second, "|") }

    a [IndexOutOfBoundsException] shouldBe thrownBy { pos.melt(Third, Fourth, "|") }
    a [IndexOutOfBoundsException] shouldBe thrownBy { pos.melt(Third, Fifth, "|") }

    a [IndexOutOfBoundsException] shouldBe thrownBy { pos.melt(Fourth, Third, "|") }
    a [IndexOutOfBoundsException] shouldBe thrownBy { pos.melt(Fifth, Third, "|") }
  }

  it should "prepend" in {
    pos.prepend(123) shouldBe Position4D(123, "foo", 123, "bar")
  }

  it should "append" in {
    pos.append(123) shouldBe Position4D("foo", 123, "bar", 123)
  }
}

class TestPosition4D extends TestGrimlock {

  val pos = Position4D("foo", 123, "bar", 456)

  "A Position4D" should "return its short string" in {
    pos.toShortString("|") shouldBe "foo|123|bar|456"
  }

  it should "have coordinates" in {
    pos.coordinates shouldBe List(StringValue("foo"), LongValue(123), StringValue("bar"), LongValue(456))
  }

  it should "return its coordinates" in {
    pos(First) shouldBe StringValue("foo")
    pos(Second) shouldBe LongValue(123)
    pos(Third) shouldBe StringValue("bar")
    pos(Fourth) shouldBe LongValue(456)
  }

  it should "throw an exception for an invalid dimension" in {
    a [IndexOutOfBoundsException] shouldBe thrownBy { pos(Fifth) }
  }

  it should "compare" in {
    pos.compare(Position4D("abc", 456, "xyz", 789)) should be > 0
    pos.compare(Position4D("foo", 1, "xyz", 789)) should be > 0
    pos.compare(Position4D("foo", 123, "abc", 789)) should be > 0
    pos.compare(Position4D("foo", 123, "bar", 1)) should be > 0
    pos.compare(pos) shouldBe 0
    pos.compare(Position4D("xyz", 1, "abc", 1)) should be < 0
    pos.compare(Position4D("foo", 789, "abc", 1)) should be < 0
    pos.compare(Position4D("foo", 123, "xyz", 1)) should be < 0
    pos.compare(Position4D("foo", 123, "bar", 789)) should be < 0
    pos.compare(Position3D("xyz", 456, "xyz")) should be > 0
    pos.compare(Position5D("abc", 1, "abc", 1, 1)) should be < 0
  }

  it should "throw an exception for an invalid value" in {
    a [Exception] shouldBe thrownBy { pos.compare(Position4D(123, 123, "bar", 456)) }
    a [Exception] shouldBe thrownBy { pos.compare(Position4D("foo", "123", "bar", 456)) }
    a [Exception] shouldBe thrownBy { pos.compare(Position4D("foo", 123, 123, 456)) }
    a [Exception] shouldBe thrownBy { pos.compare(Position4D("foo", 123, "bar", "456")) }
  }

  it should "be updated" in {
    pos.update(First, 123) shouldBe Position4D(123, 123, "bar", 456)
    pos.update(Second, "xyz") shouldBe Position4D("foo", "xyz", "bar", 456)
    pos.update(Third, 456) shouldBe Position4D("foo", 123, 456, 456)
    pos.update(Fourth, "abc") shouldBe Position4D("foo", 123, "bar", "abc")
  }

  it should "throw an exception for an invalid update dimension" in {
    a [IndexOutOfBoundsException] shouldBe thrownBy { pos.update(Fifth, 123) }
  }

  it should "permute" in {
    pos.permute(List(First, Second, Third, Fourth)) shouldBe pos
    pos.permute(List(First, Second, Fourth, Third)) shouldBe Position4D("foo", 123, 456, "bar")
    pos.permute(List(First, Third, Second, Fourth)) shouldBe Position4D("foo", "bar", 123, 456)
    pos.permute(List(First, Third, Fourth, Second)) shouldBe Position4D("foo", "bar", 456, 123)
    pos.permute(List(First, Fourth, Second, Third)) shouldBe Position4D("foo", 456, 123, "bar")
    pos.permute(List(First, Fourth, Third, Second)) shouldBe Position4D("foo", 456, "bar", 123)

    pos.permute(List(Second, First, Third, Fourth)) shouldBe Position4D(123, "foo", "bar", 456)
    pos.permute(List(Second, First, Fourth, Third)) shouldBe Position4D(123, "foo", 456, "bar")
    pos.permute(List(Second, Third, First, Fourth)) shouldBe Position4D(123, "bar", "foo", 456)
    pos.permute(List(Second, Third, Fourth, First)) shouldBe Position4D(123, "bar", 456, "foo")
    pos.permute(List(Second, Fourth, First, Third)) shouldBe Position4D(123, 456, "foo", "bar")
    pos.permute(List(Second, Fourth, Third, First)) shouldBe Position4D(123, 456, "bar", "foo")

    pos.permute(List(Third, First, Second, Fourth)) shouldBe Position4D("bar", "foo", 123, 456)
    pos.permute(List(Third, First, Fourth, Second)) shouldBe Position4D("bar", "foo", 456, 123)
    pos.permute(List(Third, Second, First, Fourth)) shouldBe Position4D("bar", 123, "foo", 456)
    pos.permute(List(Third, Second, Fourth, First)) shouldBe Position4D("bar", 123, 456, "foo")
    pos.permute(List(Third, Fourth, First, Second)) shouldBe Position4D("bar", 456, "foo", 123)
    pos.permute(List(Third, Fourth, Second, First)) shouldBe Position4D("bar", 456, 123, "foo")

    pos.permute(List(Fourth, First, Second, Third)) shouldBe Position4D(456, "foo", 123, "bar")
    pos.permute(List(Fourth, First, Third, Second)) shouldBe Position4D(456, "foo", "bar", 123)
    pos.permute(List(Fourth, Second, First, Third)) shouldBe Position4D(456, 123, "foo", "bar")
    pos.permute(List(Fourth, Second, Third, First)) shouldBe Position4D(456, 123, "bar", "foo")
    pos.permute(List(Fourth, Third, First, Second)) shouldBe Position4D(456, "bar", "foo", 123)
    pos.permute(List(Fourth, Third, Second, First)) shouldBe Position4D(456, "bar", 123, "foo")
  }

  it should "throw an exception for an invalid permute dimension" in {
    a [IndexOutOfBoundsException] shouldBe thrownBy { pos.permute(List(Fifth)) }
    a [IndexOutOfBoundsException] shouldBe thrownBy { pos.permute(List(First, Second, Third, Fourth, Fifth)) }
  }

  val con1 = Content(ContinuousSchema(LongCodex), 1)
  val con2 = Content(ContinuousSchema(LongCodex), 2)

  it should "return a over map" in {
    pos.over.toMapValue(Position3D("xyz", 456, "abc"), con1) shouldBe Map(Position3D("xyz", 456, "abc") -> con1)
  }

  it should "combine two over maps" in {
    pos.over.combineMapValues(Some(Map(Position3D("xyz", 456, 123) -> con1, Position3D("abc", 123, "zyx") -> con2)),
      Map(Position3D("xyz", 456, 123) -> con2, Position3D("klm", 321, "abc") -> con1)) shouldBe
        Map(Position3D("xyz", 456, 123) -> con2, Position3D("abc", 123, "zyx") -> con2,
          Position3D("klm", 321, "abc") -> con1)
    pos.over.combineMapValues(None, Map(Position3D("xyz", 456, 123) -> con2)) shouldBe
      Map(Position3D("xyz", 456, 123) -> con2)
  }

  it should "return an along map" in {
    pos.along.toMapValue(Position1D("xyz"), con1) shouldBe Map(Position1D("xyz") -> con1)
  }

  it should "combine two along maps" in {
    pos.along.combineMapValues(Some(Map(Position1D("xyz") -> con1, Position1D("abc") -> con2)),
      Map(Position1D("xyz") -> con2, Position1D("klm") -> con1)) shouldBe
        Map(Position1D("xyz") -> con2, Position1D("abc") -> con2, Position1D("klm") -> con1)
    pos.along.combineMapValues(None, Map(Position1D("xyz") -> con2)) shouldBe Map(Position1D("xyz") -> con2)
  }

  it should "remove" in {
    pos.remove(First) shouldBe Position3D(123, "bar", 456)
    pos.remove(Second) shouldBe Position3D("foo", "bar", 456)
    pos.remove(Third) shouldBe Position3D("foo", 123, 456)
    pos.remove(Fourth) shouldBe Position3D("foo", 123, "bar")
  }

  it should "throw an exception for an invalid remove dimension" in {
    a [UnsupportedOperationException] shouldBe thrownBy { pos.remove(Fifth) }
  }

  it should "melt" in {
    pos.melt(First, First, "|") shouldBe Position3D(123, "bar", 456)
    pos.melt(First, Second, "|") shouldBe Position3D("123|foo", "bar", 456)
    pos.melt(First, Third, "|") shouldBe Position3D(123, "bar|foo", 456)
    pos.melt(First, Fourth, "|") shouldBe Position3D(123, "bar", "456|foo")

    pos.melt(Second, First, "|") shouldBe Position3D("foo|123", "bar", 456)
    pos.melt(Second, Second, "|") shouldBe Position3D("foo", "bar", 456)
    pos.melt(Second, Third, "|") shouldBe Position3D("foo", "bar|123", 456)
    pos.melt(Second, Fourth, "|") shouldBe Position3D("foo", "bar", "456|123")

    pos.melt(Third, First, "|") shouldBe Position3D("foo|bar", 123, 456)
    pos.melt(Third, Second, "|") shouldBe Position3D("foo", "123|bar", 456)
    pos.melt(Third, Third, "|") shouldBe Position3D("foo", 123, 456)
    pos.melt(Third, Fourth, "|") shouldBe Position3D("foo", 123, "456|bar")

    pos.melt(Fourth, First, "|") shouldBe Position3D("foo|456", 123, "bar")
    pos.melt(Fourth, Second, "|") shouldBe Position3D("foo", "123|456", "bar")
    pos.melt(Fourth, Third, "|") shouldBe Position3D("foo", 123, "bar|456")
    pos.melt(Fourth, Fourth, "|") shouldBe Position3D("foo", 123, "bar")
  }

  it should "throw an exception for an invalid melt dimension" in {
    a [IndexOutOfBoundsException] shouldBe thrownBy { pos.melt(First, Fifth, "|") }
    a [IndexOutOfBoundsException] shouldBe thrownBy { pos.melt(Fifth, First, "|") }

    a [IndexOutOfBoundsException] shouldBe thrownBy { pos.melt(Second, Fifth, "|") }
    a [IndexOutOfBoundsException] shouldBe thrownBy { pos.melt(Fifth, Second, "|") }

    a [IndexOutOfBoundsException] shouldBe thrownBy { pos.melt(Third, Fifth, "|") }
    a [IndexOutOfBoundsException] shouldBe thrownBy { pos.melt(Fifth, Third, "|") }

    a [IndexOutOfBoundsException] shouldBe thrownBy { pos.melt(Fourth, Fifth, "|") }
    a [IndexOutOfBoundsException] shouldBe thrownBy { pos.melt(Fifth, Fourth, "|") }
  }

  it should "prepend" in {
    pos.prepend(123) shouldBe Position5D(123, "foo", 123, "bar", 456)
  }

  it should "append" in {
    pos.append(123) shouldBe Position5D("foo", 123, "bar", 456, 123)
  }
}

class TestPosition5D extends TestGrimlock {

  val pos = Position5D("foo", 123, "bar", 456, "baz")

  "A Position5D" should "return its short string" in {
    pos.toShortString("|") shouldBe "foo|123|bar|456|baz"
  }

  it should "have coordinates" in {
    pos.coordinates shouldBe List(StringValue("foo"), LongValue(123), StringValue("bar"), LongValue(456),
      StringValue("baz"))
  }

  it should "return its coordinates" in {
    pos(First) shouldBe StringValue("foo")
    pos(Second) shouldBe LongValue(123)
    pos(Third) shouldBe StringValue("bar")
    pos(Fourth) shouldBe LongValue(456)
    pos(Fifth) shouldBe StringValue("baz")
  }

  it should "compare" in {
    pos.compare(Position5D("abc", 456, "xyz", 789, "xyz")) should be > 0
    pos.compare(Position5D("foo", 1, "xyz", 789, "xyz")) should be > 0
    pos.compare(Position5D("foo", 123, "abc", 789, "xyz")) should be > 0
    pos.compare(Position5D("foo", 123, "bar", 1, "xyz")) should be > 0
    pos.compare(Position5D("foo", 123, "bar", 456, "abc")) should be > 0
    pos.compare(pos) shouldBe 0
    pos.compare(Position5D("xyz", 1, "abc", 1, "abc")) should be < 0
    pos.compare(Position5D("foo", 789, "abc", 1, "abc")) should be < 0
    pos.compare(Position5D("foo", 123, "xyz", 1, "abc")) should be < 0
    pos.compare(Position5D("foo", 123, "bar", 789, "abc")) should be < 0
    pos.compare(Position5D("foo", 123, "bar", 456, "xyz")) should be < 0
    pos.compare(Position4D("xyz", 456, "xyz", 789)) should be > 0
  }

  it should "throw an exception for an invalid value" in {
    a [Exception] shouldBe thrownBy { pos.compare(Position5D(123, 123, "bar", 456, "baz")) }
    a [Exception] shouldBe thrownBy { pos.compare(Position5D("foo", "123", "bar", 456, "baz")) }
    a [Exception] shouldBe thrownBy { pos.compare(Position5D("foo", 123, 123, 456, "baz")) }
    a [Exception] shouldBe thrownBy { pos.compare(Position5D("foo", 123, "bar", "456", "baz")) }
    a [Exception] shouldBe thrownBy { pos.compare(Position5D("foo", 123, "bar", 456, 123)) }
  }

  it should "be updated" in {
    pos.update(First, 123) shouldBe Position5D(123, 123, "bar", 456, "baz")
    pos.update(Second, "xyz") shouldBe Position5D("foo", "xyz", "bar", 456, "baz")
    pos.update(Third, 456) shouldBe Position5D("foo", 123, 456, 456, "baz")
    pos.update(Fourth, "abc") shouldBe Position5D("foo", 123, "bar", "abc", "baz")
    pos.update(Fifth, 789) shouldBe Position5D("foo", 123, "bar", 456, 789)
  }

  it should "permute" in {
    pos.permute(List(First, Second, Third, Fourth, Fifth)) shouldBe Position5D("foo", 123, "bar", 456, "baz")
    pos.permute(List(First, Second, Third, Fifth, Fourth)) shouldBe Position5D("foo", 123, "bar", "baz", 456)
    pos.permute(List(First, Second, Fourth, Third, Fifth)) shouldBe Position5D("foo", 123, 456, "bar", "baz")
    pos.permute(List(First, Second, Fourth, Fifth, Third)) shouldBe Position5D("foo", 123, 456, "baz", "bar")
    pos.permute(List(First, Second, Fifth, Third, Fourth)) shouldBe Position5D("foo", 123, "baz", "bar", 456)
    pos.permute(List(First, Second, Fifth, Fourth, Third)) shouldBe Position5D("foo", 123, "baz", 456, "bar")
    pos.permute(List(First, Third, Second, Fourth, Fifth)) shouldBe Position5D("foo", "bar", 123, 456, "baz")
    pos.permute(List(First, Third, Second, Fifth, Fourth)) shouldBe Position5D("foo", "bar", 123, "baz", 456)
    pos.permute(List(First, Third, Fourth, Second, Fifth)) shouldBe Position5D("foo", "bar", 456, 123, "baz")
    pos.permute(List(First, Third, Fourth, Fifth, Second)) shouldBe Position5D("foo", "bar", 456, "baz", 123)
    pos.permute(List(First, Third, Fifth, Second, Fourth)) shouldBe Position5D("foo", "bar", "baz", 123, 456)
    pos.permute(List(First, Third, Fifth, Fourth, Second)) shouldBe Position5D("foo", "bar", "baz", 456, 123)
    pos.permute(List(First, Fourth, Second, Third, Fifth)) shouldBe Position5D("foo", 456, 123, "bar", "baz")
    pos.permute(List(First, Fourth, Second, Fifth, Third)) shouldBe Position5D("foo", 456, 123, "baz", "bar")
    pos.permute(List(First, Fourth, Third, Second, Fifth)) shouldBe Position5D("foo", 456, "bar", 123, "baz")
    pos.permute(List(First, Fourth, Third, Fifth, Second)) shouldBe Position5D("foo", 456, "bar", "baz", 123)
    pos.permute(List(First, Fourth, Fifth, Second, Third)) shouldBe Position5D("foo", 456, "baz", 123, "bar")
    pos.permute(List(First, Fourth, Fifth, Third, Second)) shouldBe Position5D("foo", 456, "baz", "bar", 123)
    pos.permute(List(First, Fifth, Second, Third, Fourth)) shouldBe Position5D("foo", "baz", 123, "bar", 456)
    pos.permute(List(First, Fifth, Second, Fourth, Third)) shouldBe Position5D("foo", "baz", 123, 456, "bar")
    pos.permute(List(First, Fifth, Third, Second, Fourth)) shouldBe Position5D("foo", "baz", "bar", 123, 456)
    pos.permute(List(First, Fifth, Third, Fourth, Second)) shouldBe Position5D("foo", "baz", "bar", 456, 123)
    pos.permute(List(First, Fifth, Fourth, Second, Third)) shouldBe Position5D("foo", "baz", 456, 123, "bar")
    pos.permute(List(First, Fifth, Fourth, Third, Second)) shouldBe Position5D("foo", "baz", 456, "bar", 123)

    pos.permute(List(Second, First, Third, Fourth, Fifth)) shouldBe Position5D(123, "foo", "bar", 456, "baz")
    pos.permute(List(Second, First, Third, Fifth, Fourth)) shouldBe Position5D(123, "foo", "bar", "baz", 456)
    pos.permute(List(Second, First, Fourth, Third, Fifth)) shouldBe Position5D(123, "foo", 456, "bar", "baz")
    pos.permute(List(Second, First, Fourth, Fifth, Third)) shouldBe Position5D(123, "foo", 456, "baz", "bar")
    pos.permute(List(Second, First, Fifth, Third, Fourth)) shouldBe Position5D(123, "foo", "baz", "bar", 456)
    pos.permute(List(Second, First, Fifth, Fourth, Third)) shouldBe Position5D(123, "foo", "baz", 456, "bar")
    pos.permute(List(Second, Third, First, Fourth, Fifth)) shouldBe Position5D(123, "bar", "foo", 456, "baz")
    pos.permute(List(Second, Third, First, Fifth, Fourth)) shouldBe Position5D(123, "bar", "foo", "baz", 456)
    pos.permute(List(Second, Third, Fourth, First, Fifth)) shouldBe Position5D(123, "bar", 456, "foo", "baz")
    pos.permute(List(Second, Third, Fourth, Fifth, First)) shouldBe Position5D(123, "bar", 456, "baz", "foo")
    pos.permute(List(Second, Third, Fifth, First, Fourth)) shouldBe Position5D(123, "bar", "baz", "foo", 456)
    pos.permute(List(Second, Third, Fifth, Fourth, First)) shouldBe Position5D(123, "bar", "baz", 456, "foo")
    pos.permute(List(Second, Fourth, First, Third, Fifth)) shouldBe Position5D(123, 456, "foo", "bar", "baz")
    pos.permute(List(Second, Fourth, First, Fifth, Third)) shouldBe Position5D(123, 456, "foo", "baz", "bar")
    pos.permute(List(Second, Fourth, Third, First, Fifth)) shouldBe Position5D(123, 456, "bar", "foo", "baz")
    pos.permute(List(Second, Fourth, Third, Fifth, First)) shouldBe Position5D(123, 456, "bar", "baz", "foo")
    pos.permute(List(Second, Fourth, Fifth, First, Third)) shouldBe Position5D(123, 456, "baz", "foo", "bar")
    pos.permute(List(Second, Fourth, Fifth, Third, First)) shouldBe Position5D(123, 456, "baz", "bar", "foo")
    pos.permute(List(Second, Fifth, First, Third, Fourth)) shouldBe Position5D(123, "baz", "foo", "bar", 456)
    pos.permute(List(Second, Fifth, First, Fourth, Third)) shouldBe Position5D(123, "baz", "foo", 456, "bar")
    pos.permute(List(Second, Fifth, Third, First, Fourth)) shouldBe Position5D(123, "baz", "bar", "foo", 456)
    pos.permute(List(Second, Fifth, Third, Fourth, First)) shouldBe Position5D(123, "baz", "bar", 456, "foo")
    pos.permute(List(Second, Fifth, Fourth, First, Third)) shouldBe Position5D(123, "baz", 456, "foo", "bar")
    pos.permute(List(Second, Fifth, Fourth, Third, First)) shouldBe Position5D(123, "baz", 456, "bar", "foo")

    pos.permute(List(Third, First, Second, Fourth, Fifth)) shouldBe Position5D("bar", "foo", 123, 456, "baz")
    pos.permute(List(Third, First, Second, Fifth, Fourth)) shouldBe Position5D("bar", "foo", 123, "baz", 456)
    pos.permute(List(Third, First, Fourth, Second, Fifth)) shouldBe Position5D("bar", "foo", 456, 123, "baz")
    pos.permute(List(Third, First, Fourth, Fifth, Second)) shouldBe Position5D("bar", "foo", 456, "baz", 123)
    pos.permute(List(Third, First, Fifth, Second, Fourth)) shouldBe Position5D("bar", "foo", "baz", 123, 456)
    pos.permute(List(Third, First, Fifth, Fourth, Second)) shouldBe Position5D("bar", "foo", "baz", 456, 123)
    pos.permute(List(Third, Second, First, Fourth, Fifth)) shouldBe Position5D("bar", 123, "foo", 456, "baz")
    pos.permute(List(Third, Second, First, Fifth, Fourth)) shouldBe Position5D("bar", 123, "foo", "baz", 456)
    pos.permute(List(Third, Second, Fourth, First, Fifth)) shouldBe Position5D("bar", 123, 456, "foo", "baz")
    pos.permute(List(Third, Second, Fourth, Fifth, First)) shouldBe Position5D("bar", 123, 456, "baz", "foo")
    pos.permute(List(Third, Second, Fifth, First, Fourth)) shouldBe Position5D("bar", 123, "baz", "foo", 456)
    pos.permute(List(Third, Second, Fifth, Fourth, First)) shouldBe Position5D("bar", 123, "baz", 456, "foo")
    pos.permute(List(Third, Fourth, First, Second, Fifth)) shouldBe Position5D("bar", 456, "foo", 123, "baz")
    pos.permute(List(Third, Fourth, First, Fifth, Second)) shouldBe Position5D("bar", 456, "foo", "baz", 123)
    pos.permute(List(Third, Fourth, Second, First, Fifth)) shouldBe Position5D("bar", 456, 123, "foo", "baz")
    pos.permute(List(Third, Fourth, Second, Fifth, First)) shouldBe Position5D("bar", 456, 123, "baz", "foo")
    pos.permute(List(Third, Fourth, Fifth, First, Second)) shouldBe Position5D("bar", 456, "baz", "foo", 123)
    pos.permute(List(Third, Fourth, Fifth, Second, First)) shouldBe Position5D("bar", 456, "baz", 123, "foo")
    pos.permute(List(Third, Fifth, First, Second, Fourth)) shouldBe Position5D("bar", "baz", "foo", 123, 456)
    pos.permute(List(Third, Fifth, First, Fourth, Second)) shouldBe Position5D("bar", "baz", "foo", 456, 123)
    pos.permute(List(Third, Fifth, Second, First, Fourth)) shouldBe Position5D("bar", "baz", 123, "foo", 456)
    pos.permute(List(Third, Fifth, Second, Fourth, First)) shouldBe Position5D("bar", "baz", 123, 456, "foo")
    pos.permute(List(Third, Fifth, Fourth, First, Second)) shouldBe Position5D("bar", "baz", 456, "foo", 123)
    pos.permute(List(Third, Fifth, Fourth, Second, First)) shouldBe Position5D("bar", "baz", 456, 123, "foo")

    pos.permute(List(Fourth, First, Second, Third, Fifth)) shouldBe Position5D(456, "foo", 123, "bar", "baz")
    pos.permute(List(Fourth, First, Second, Fifth, Third)) shouldBe Position5D(456, "foo", 123, "baz", "bar")
    pos.permute(List(Fourth, First, Third, Second, Fifth)) shouldBe Position5D(456, "foo", "bar", 123, "baz")
    pos.permute(List(Fourth, First, Third, Fifth, Second)) shouldBe Position5D(456, "foo", "bar", "baz", 123)
    pos.permute(List(Fourth, First, Fifth, Second, Third)) shouldBe Position5D(456, "foo", "baz", 123, "bar")
    pos.permute(List(Fourth, First, Fifth, Third, Second)) shouldBe Position5D(456, "foo", "baz", "bar", 123)
    pos.permute(List(Fourth, Second, First, Third, Fifth)) shouldBe Position5D(456, 123, "foo", "bar", "baz")
    pos.permute(List(Fourth, Second, First, Fifth, Third)) shouldBe Position5D(456, 123, "foo", "baz", "bar")
    pos.permute(List(Fourth, Second, Third, First, Fifth)) shouldBe Position5D(456, 123, "bar", "foo", "baz")
    pos.permute(List(Fourth, Second, Third, Fifth, First)) shouldBe Position5D(456, 123, "bar", "baz", "foo")
    pos.permute(List(Fourth, Second, Fifth, First, Third)) shouldBe Position5D(456, 123, "baz", "foo", "bar")
    pos.permute(List(Fourth, Second, Fifth, Third, First)) shouldBe Position5D(456, 123, "baz", "bar", "foo")
    pos.permute(List(Fourth, Third, First, Second, Fifth)) shouldBe Position5D(456, "bar", "foo", 123, "baz")
    pos.permute(List(Fourth, Third, First, Fifth, Second)) shouldBe Position5D(456, "bar", "foo", "baz", 123)
    pos.permute(List(Fourth, Third, Second, First, Fifth)) shouldBe Position5D(456, "bar", 123, "foo", "baz")
    pos.permute(List(Fourth, Third, Second, Fifth, First)) shouldBe Position5D(456, "bar", 123, "baz", "foo")
    pos.permute(List(Fourth, Third, Fifth, First, Second)) shouldBe Position5D(456, "bar", "baz", "foo", 123)
    pos.permute(List(Fourth, Third, Fifth, Second, First)) shouldBe Position5D(456, "bar", "baz", 123, "foo")
    pos.permute(List(Fourth, Fifth, First, Second, Third)) shouldBe Position5D(456, "baz", "foo", 123, "bar")
    pos.permute(List(Fourth, Fifth, First, Third, Second)) shouldBe Position5D(456, "baz", "foo", "bar", 123)
    pos.permute(List(Fourth, Fifth, Second, First, Third)) shouldBe Position5D(456, "baz", 123, "foo", "bar")
    pos.permute(List(Fourth, Fifth, Second, Third, First)) shouldBe Position5D(456, "baz", 123, "bar", "foo")
    pos.permute(List(Fourth, Fifth, Third, First, Second)) shouldBe Position5D(456, "baz", "bar", "foo", 123)
    pos.permute(List(Fourth, Fifth, Third, Second, First)) shouldBe Position5D(456, "baz", "bar", 123, "foo")

    pos.permute(List(Fifth, First, Second, Third, Fourth)) shouldBe Position5D("baz", "foo", 123, "bar", 456)
    pos.permute(List(Fifth, First, Second, Fourth, Third)) shouldBe Position5D("baz", "foo", 123, 456, "bar")
    pos.permute(List(Fifth, First, Third, Second, Fourth)) shouldBe Position5D("baz", "foo", "bar", 123, 456)
    pos.permute(List(Fifth, First, Third, Fourth, Second)) shouldBe Position5D("baz", "foo", "bar", 456, 123)
    pos.permute(List(Fifth, First, Fourth, Second, Third)) shouldBe Position5D("baz", "foo", 456, 123, "bar")
    pos.permute(List(Fifth, First, Fourth, Third, Second)) shouldBe Position5D("baz", "foo", 456, "bar", 123)
    pos.permute(List(Fifth, Second, First, Third, Fourth)) shouldBe Position5D("baz", 123, "foo", "bar", 456)
    pos.permute(List(Fifth, Second, First, Fourth, Third)) shouldBe Position5D("baz", 123, "foo", 456, "bar")
    pos.permute(List(Fifth, Second, Third, First, Fourth)) shouldBe Position5D("baz", 123, "bar", "foo", 456)
    pos.permute(List(Fifth, Second, Third, Fourth, First)) shouldBe Position5D("baz", 123, "bar", 456, "foo")
    pos.permute(List(Fifth, Second, Fourth, First, Third)) shouldBe Position5D("baz", 123, 456, "foo", "bar")
    pos.permute(List(Fifth, Second, Fourth, Third, First)) shouldBe Position5D("baz", 123, 456, "bar", "foo")
    pos.permute(List(Fifth, Third, First, Second, Fourth)) shouldBe Position5D("baz", "bar", "foo", 123, 456)
    pos.permute(List(Fifth, Third, First, Fourth, Second)) shouldBe Position5D("baz", "bar", "foo", 456, 123)
    pos.permute(List(Fifth, Third, Second, First, Fourth)) shouldBe Position5D("baz", "bar", 123, "foo", 456)
    pos.permute(List(Fifth, Third, Second, Fourth, First)) shouldBe Position5D("baz", "bar", 123, 456, "foo")
    pos.permute(List(Fifth, Third, Fourth, First, Second)) shouldBe Position5D("baz", "bar", 456, "foo", 123)
    pos.permute(List(Fifth, Third, Fourth, Second, First)) shouldBe Position5D("baz", "bar", 456, 123, "foo")
    pos.permute(List(Fifth, Fourth, First, Second, Third)) shouldBe Position5D("baz", 456, "foo", 123, "bar")
    pos.permute(List(Fifth, Fourth, First, Third, Second)) shouldBe Position5D("baz", 456, "foo", "bar", 123)
    pos.permute(List(Fifth, Fourth, Second, First, Third)) shouldBe Position5D("baz", 456, 123, "foo", "bar")
    pos.permute(List(Fifth, Fourth, Second, Third, First)) shouldBe Position5D("baz", 456, 123, "bar", "foo")
    pos.permute(List(Fifth, Fourth, Third, First, Second)) shouldBe Position5D("baz", 456, "bar", "foo", 123)
    pos.permute(List(Fifth, Fourth, Third, Second, First)) shouldBe Position5D("baz", 456, "bar", 123, "foo")
  }

  val con1 = Content(ContinuousSchema(LongCodex), 1)
  val con2 = Content(ContinuousSchema(LongCodex), 2)

  it should "return a over map" in {
    pos.over.toMapValue(Position4D("xyz", 456, "abc", 123), con1) shouldBe
      Map(Position4D("xyz", 456, "abc", 123) -> con1)
  }

  it should "combine two over maps" in {
    pos.over.combineMapValues(Some(Map(Position4D("xyz", 456, 123, "abc") -> con1,
      Position4D("abc", 123, "zyx", 789) -> con2)), Map(Position4D("xyz", 456, 123, "abc") -> con2,
        Position4D("klm", 321, "abc", 654) -> con1)) shouldBe Map(Position4D("xyz", 456, 123, "abc") -> con2,
          Position4D("abc", 123, "zyx", 789) -> con2, Position4D("klm", 321, "abc", 654) -> con1)
    pos.over.combineMapValues(None, Map(Position4D("xyz", 456, 123, "abc") -> con2)) shouldBe
      Map(Position4D("xyz", 456, 123, "abc") -> con2)
  }

  it should "return an along map" in {
    pos.along.toMapValue(Position1D("xyz"), con1) shouldBe Map(Position1D("xyz") -> con1)
  }

  it should "combine two along maps" in {
    pos.along.combineMapValues(Some(Map(Position1D("xyz") -> con1, Position1D("abc") -> con2)),
      Map(Position1D("xyz") -> con2, Position1D("klm") -> con1)) shouldBe
        Map(Position1D("xyz") -> con2, Position1D("abc") -> con2, Position1D("klm") -> con1)
    pos.along.combineMapValues(None, Map(Position1D("xyz") -> con2)) shouldBe Map(Position1D("xyz") -> con2)
  }

  it should "remove" in {
    pos.remove(First) shouldBe Position4D(123, "bar", 456, "baz")
    pos.remove(Second) shouldBe Position4D("foo", "bar", 456, "baz")
    pos.remove(Third) shouldBe Position4D("foo", 123, 456, "baz")
    pos.remove(Fourth) shouldBe Position4D("foo", 123, "bar", "baz")
    pos.remove(Fifth) shouldBe Position4D("foo", 123, "bar", 456)
  }

  it should "melt" in {
    pos.melt(First, First, "|") shouldBe Position4D(123, "bar", 456, "baz")
    pos.melt(First, Second, "|") shouldBe Position4D("123|foo", "bar", 456, "baz")
    pos.melt(First, Third, "|") shouldBe Position4D(123, "bar|foo", 456, "baz")
    pos.melt(First, Fourth, "|") shouldBe Position4D(123, "bar", "456|foo", "baz")
    pos.melt(First, Fifth, "|") shouldBe Position4D(123, "bar", 456, "baz|foo")

    pos.melt(Second, First, "|") shouldBe Position4D("foo|123", "bar", 456, "baz")
    pos.melt(Second, Second, "|") shouldBe Position4D("foo", "bar", 456, "baz")
    pos.melt(Second, Third, "|") shouldBe Position4D("foo", "bar|123", 456, "baz")
    pos.melt(Second, Fourth, "|") shouldBe Position4D("foo", "bar", "456|123", "baz")
    pos.melt(Second, Fifth, "|") shouldBe Position4D("foo", "bar", 456, "baz|123")

    pos.melt(Third, First, "|") shouldBe Position4D("foo|bar", 123, 456, "baz")
    pos.melt(Third, Second, "|") shouldBe Position4D("foo", "123|bar", 456, "baz")
    pos.melt(Third, Third, "|") shouldBe Position4D("foo", 123, 456, "baz")
    pos.melt(Third, Fourth, "|") shouldBe Position4D("foo", 123, "456|bar", "baz")
    pos.melt(Third, Fifth, "|") shouldBe Position4D("foo", 123, 456, "baz|bar")

    pos.melt(Fourth, First, "|") shouldBe Position4D("foo|456", 123, "bar", "baz")
    pos.melt(Fourth, Second, "|") shouldBe Position4D("foo", "123|456", "bar", "baz")
    pos.melt(Fourth, Third, "|") shouldBe Position4D("foo", 123, "bar|456", "baz")
    pos.melt(Fourth, Fourth, "|") shouldBe Position4D("foo", 123, "bar", "baz")
    pos.melt(Fourth, Fifth, "|") shouldBe Position4D("foo", 123, "bar", "baz|456")

    pos.melt(Fifth, First, "|") shouldBe Position4D("foo|baz", 123, "bar", 456)
    pos.melt(Fifth, Second, "|") shouldBe Position4D("foo", "123|baz", "bar", 456)
    pos.melt(Fifth, Third, "|") shouldBe Position4D("foo", 123, "bar|baz", 456)
    pos.melt(Fifth, Fourth, "|") shouldBe Position4D("foo", 123, "bar", "456|baz")
    pos.melt(Fifth, Fifth, "|") shouldBe Position4D("foo", 123, "bar", 456)
  }
}

trait TestPositions extends TestGrimlock {

  val data = List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F").zipWithIndex

  val result1 = data.map { case (s, i) => Position1D(s) }.sorted
  val result2 = data.map { case (s, i) => Position1D(s) }.sorted
  val result3 = data.map { case (s, i) => Position1D(i) }.sorted
  val result4 = data.map { case (s, i) => Position1D(i) }.sorted
  val result5 = data.map { case (s, i) => Position1D(s) }.sorted
  val result6 = data.map { case (s, i) => Position1D(s) }.sorted
  val result7 = data.map { case (s, i) => Position1D(i) }.sorted
  val result8 = data.map { case (s, i) => Position1D(i + 1) }.sorted
  val result9 = data.map { case (s, i) => Position2D(i, i + 1) }.sorted
  val result10 = data.map { case (s, i) => Position2D(s, i + 1) }.sorted
  val result11 = data.map { case (s, i) => Position2D(s, i) }.sorted
  val result12 = data.map { case (s, i) => Position1D(s) }.sorted
  val result13 = data.map { case (s, i) => Position1D(i) }.sorted
  val result14 = data.map { case (s, i) => Position1D(i + 1) }.sorted
  val result15 = data.map { case (s, i) => Position1D(i + 2) }.sorted
  val result16 = data.map { case (s, i) => Position3D(i, i + 1, i + 2) }.sorted
  val result17 = data.map { case (s, i) => Position3D(s, i + 1, i + 2) }.sorted
  val result18 = data.map { case (s, i) => Position3D(s, i, i + 2) }.sorted
  val result19 = data.map { case (s, i) => Position3D(s, i, i + 1) }.sorted
  val result20 = data.map { case (s, i) => Position1D(s) }.sorted
  val result21 = data.map { case (s, i) => Position1D(i) }.sorted
  val result22 = data.map { case (s, i) => Position1D(i + 1) }.sorted
  val result23 = data.map { case (s, i) => Position1D(i + 2) }.sorted
  val result24 = data.map { case (s, i) => Position1D(i + 3) }.sorted
  val result25 = data.map { case (s, i) => Position4D(i, i + 1, i + 2, i + 3) }.sorted
  val result26 = data.map { case (s, i) => Position4D(s, i + 1, i + 2, i + 3) }.sorted
  val result27 = data.map { case (s, i) => Position4D(s, i, i + 2, i + 3) }.sorted
  val result28 = data.map { case (s, i) => Position4D(s, i, i + 1, i + 3) }.sorted
  val result29 = data.map { case (s, i) => Position4D(s, i, i + 1, i + 2) }.sorted
}

class TestScaldingPositions extends TestPositions with TBddDsl {

  "A Positions of Position1D" should "return its First over names" in {
    Given {
      data.map { case (s, i) => Position1D(s) }
    } When {
      positions: TypedPipe[Position1D] =>
        positions.names(Over(First), Default())
    } Then {
      _.toList.sorted shouldBe result1
    }
  }

  "A Positions of Position2D" should "return its First over names" in {
    Given {
      data.map { case (s, i) => Position2D(s, i) }
    } When {
      positions: TypedPipe[Position2D] =>
        positions.names(Over(First), Default())
    } Then {
      _.toList.sorted shouldBe result2
    }
  }

  it should "return its Second over names" in {
    Given {
      data.map { case (s, i) => Position2D(s, i) }
    } When {
      positions: TypedPipe[Position2D] =>
        positions.names(Over(Second), Default())
    } Then {
      _.toList.sorted shouldBe result3
    }
  }

  it should "return its First along names" in {
    Given {
      data.map { case (s, i) => Position2D(s, i) }
    } When {
      positions: TypedPipe[Position2D] =>
        positions.names(Along(First), Default())
    } Then {
      _.toList.sorted shouldBe result4
    }
  }

  it should "return its Second along names" in {
    Given {
      data.map { case (s, i) => Position2D(s, i) }
    } When {
      positions: TypedPipe[Position2D] =>
        positions.names(Along(Second), Default())
    } Then {
      _.toList.sorted shouldBe result5
    }
  }

  "A Positions of Position3D" should "return its First over names" in {
    Given {
      data.map { case (s, i) => Position3D(s, i, i + 1) }
    } When {
      positions: TypedPipe[Position3D] =>
        positions.names(Over(First), Default())
    } Then {
      _.toList.sorted shouldBe result6
    }
  }

  it should "return its Second over names" in {
    Given {
      data.map { case (s, i) => Position3D(s, i, i + 1) }
    } When {
      positions: TypedPipe[Position3D] =>
        positions.names(Over(Second), Default())
    } Then {
      _.toList.sorted shouldBe result7
    }
  }

  it should "return its Third over names" in {
    Given {
      data.map { case (s, i) => Position3D(s, i, i + 1) }
    } When {
      positions: TypedPipe[Position3D] =>
        positions.names(Over(Third), Default())
    } Then {
      _.toList.sorted shouldBe result8
    }
  }

  it should "return its First along names" in {
    Given {
      data.map { case (s, i) => Position3D(s, i, i + 1) }
    } When {
      positions: TypedPipe[Position3D] =>
        positions.names(Along(First), Default())
    } Then {
      _.toList.sorted shouldBe result9
    }
  }

  it should "return its Second along names" in {
    Given {
      data.map { case (s, i) => Position3D(s, i, i + 1) }
    } When {
      positions: TypedPipe[Position3D] =>
        positions.names(Along(Second), Default())
    } Then {
      _.toList.sorted shouldBe result10
    }
  }

  it should "return its Third along names" in {
    Given {
      data.map { case (s, i) => Position3D(s, i, i + 1) }
    } When {
      positions: TypedPipe[Position3D] =>
        positions.names(Along(Third), Default())
    } Then {
      _.toList.sorted shouldBe result11
    }
  }

  "A Positions of Position4D" should "return its First over names" in {
    Given {
      data.map { case (s, i) => Position4D(s, i, i + 1, i + 2) }
    } When {
      positions: TypedPipe[Position4D] =>
        positions.names(Over(First), Default())
    } Then {
      _.toList.sorted shouldBe result12
    }
  }

  it should "return its Second over names" in {
    Given {
      data.map { case (s, i) => Position4D(s, i, i + 1, i + 2) }
    } When {
      positions: TypedPipe[Position4D] =>
        positions.names(Over(Second), Default())
    } Then {
      _.toList.sorted shouldBe result13
    }
  }

  it should "return its Third over names" in {
    Given {
      data.map { case (s, i) => Position4D(s, i, i + 1, i + 2) }
    } When {
      positions: TypedPipe[Position4D] =>
        positions.names(Over(Third), Default())
    } Then {
      _.toList.sorted shouldBe result14
    }
  }

  it should "return its Fourth over names" in {
    Given {
      data.map { case (s, i) => Position4D(s, i, i + 1, i + 2) }
    } When {
      positions: TypedPipe[Position4D] =>
        positions.names(Over(Fourth), Default())
    } Then {
      _.toList.sorted shouldBe result15
    }
  }

  it should "return its First along names" in {
    Given {
      data.map { case (s, i) => Position4D(s, i, i + 1, i + 2) }
    } When {
      positions: TypedPipe[Position4D] =>
        positions.names(Along(First), Default())
    } Then {
      _.toList.sorted shouldBe result16
    }
  }

  it should "return its Second along names" in {
    Given {
      data.map { case (s, i) => Position4D(s, i, i + 1, i + 2) }
    } When {
      positions: TypedPipe[Position4D] =>
        positions.names(Along(Second), Default())
    } Then {
      _.toList.sorted shouldBe result17
    }
  }

  it should "return its Third along names" in {
    Given {
      data.map { case (s, i) => Position4D(s, i, i + 1, i + 2) }
    } When {
      positions: TypedPipe[Position4D] =>
        positions.names(Along(Third), Default())
    } Then {
      _.toList.sorted shouldBe result18
    }
  }

  it should "return its Fourth along names" in {
    Given {
      data.map { case (s, i) => Position4D(s, i, i + 1, i + 2) }
    } When {
      positions: TypedPipe[Position4D] =>
        positions.names(Along(Fourth), Default())
    } Then {
      _.toList.sorted shouldBe result19
    }
  }

  "A Positions of Position5D" should "return its First over names" in {
    Given {
      data.map { case (s, i) => Position5D(s, i, i + 1, i + 2, i + 3) }
    } When {
      positions: TypedPipe[Position5D] =>
        positions.names(Over(First), Default())
    } Then {
      _.toList.sorted shouldBe result20
    }
  }

  it should "return its Second over names" in {
    Given {
      data.map { case (s, i) => Position5D(s, i, i + 1, i + 2, i + 3) }
    } When {
      positions: TypedPipe[Position5D] =>
        positions.names(Over(Second), Default())
    } Then {
      _.toList.sorted shouldBe result21
    }
  }

  it should "return its Third over names" in {
    Given {
      data.map { case (s, i) => Position5D(s, i, i + 1, i + 2, i + 3) }
    } When {
      positions: TypedPipe[Position5D] =>
        positions.names(Over(Third), Default())
    } Then {
      _.toList.sorted shouldBe result22
    }
  }

  it should "return its Fourth over names" in {
    Given {
      data.map { case (s, i) => Position5D(s, i, i + 1, i + 2, i + 3) }
    } When {
      positions: TypedPipe[Position5D] =>
        positions.names(Over(Fourth), Default())
    } Then {
      _.toList.sorted shouldBe result23
    }
  }

  it should "return its Fifth over names" in {
    Given {
      data.map { case (s, i) => Position5D(s, i, i + 1, i + 2, i + 3) }
    } When {
      positions: TypedPipe[Position5D] =>
        positions.names(Over(Fifth), Default())
    } Then {
      _.toList.sorted shouldBe result24
    }
  }

  it should "return its First along names" in {
    Given {
      data.map { case (s, i) => Position5D(s, i, i + 1, i + 2, i + 3) }
    } When {
      positions: TypedPipe[Position5D] =>
        positions.names(Along(First), Default())
    } Then {
      _.toList.sorted shouldBe result25
    }
  }

  it should "return its Second along names" in {
    Given {
      data.map { case (s, i) => Position5D(s, i, i + 1, i + 2, i + 3) }
    } When {
      positions: TypedPipe[Position5D] =>
        positions.names(Along(Second), Default())
    } Then {
      _.toList.sorted shouldBe result26
    }
  }

  it should "return its Third along names" in {
    Given {
      data.map { case (s, i) => Position5D(s, i, i + 1, i + 2, i + 3) }
    } When {
      positions: TypedPipe[Position5D] =>
        positions.names(Along(Third), Default())
    } Then {
      _.toList.sorted shouldBe result27
    }
  }

  it should "return its Fourth along names" in {
    Given {
      data.map { case (s, i) => Position5D(s, i, i + 1, i + 2, i + 3) }
    } When {
      positions: TypedPipe[Position5D] =>
        positions.names(Along(Fourth), Default())
    } Then {
      _.toList.sorted shouldBe result28
    }
  }

  it should "return its Fifth along names" in {
    Given {
      data.map { case (s, i) => Position5D(s, i, i + 1, i + 2, i + 3) }
    } When {
      positions: TypedPipe[Position5D] =>
        positions.names(Along(Fifth), Default())
    } Then {
      _.toList.sorted shouldBe result29
    }
  }
}

class TestSparkPositions extends TestPositions {

  "A Positions of Position1D" should "return its First over names" in {
    toRDD(data)
      .map { case (s, i) => Position1D(s) }
      .names(Over(First), Default())
      .toList.sorted shouldBe result1
  }

  "A Positions of Position2D" should "return its First over names" in {
    toRDD(data)
      .map { case (s, i) => Position2D(s, i) }
      .names(Over(First), Default(Reducers(12)))
      .toList.sorted shouldBe result2
  }

  it should "return its Second over names" in {
    toRDD(data)
      .map { case (s, i) => Position2D(s, i) }
      .names(Over(Second), Default())
      .toList.sorted shouldBe result3
  }

  it should "return its First along names" in {
    toRDD(data)
      .map { case (s, i) => Position2D(s, i) }
      .names(Along(First), Default(Reducers(12)))
      .toList.sorted shouldBe result4
  }

  it should "return its Second along names" in {
    toRDD(data)
      .map { case (s, i) => Position2D(s, i) }
      .names(Along(Second), Default())
      .toList.sorted shouldBe result5
  }

  "A Positions of Position3D" should "return its First over names" in {
    toRDD(data)
      .map { case (s, i) => Position3D(s, i, i + 1) }
      .names(Over(First), Default(Reducers(12)))
      .toList.sorted shouldBe result6
  }

  it should "return its Second over names" in {
    toRDD(data)
      .map { case (s, i) => Position3D(s, i, i + 1) }
      .names(Over(Second), Default())
      .toList.sorted shouldBe result7
  }

  it should "return its Third over names" in {
    toRDD(data)
      .map { case (s, i) => Position3D(s, i, i + 1) }
      .names(Over(Third), Default(Reducers(12)))
      .toList.sorted shouldBe result8
  }

  it should "return its First along names" in {
    toRDD(data)
      .map { case (s, i) => Position3D(s, i, i + 1) }
      .names(Along(First), Default())
      .toList.sorted shouldBe result9
  }

  it should "return its Second along names" in {
    toRDD(data)
      .map { case (s, i) => Position3D(s, i, i + 1) }
      .names(Along(Second), Default(Reducers(12)))
      .toList.sorted shouldBe result10
  }

  it should "return its Third along names" in {
    toRDD(data)
      .map { case (s, i) => Position3D(s, i, i + 1) }
      .names(Along(Third), Default())
      .toList.sorted shouldBe result11
  }

  "A Positions of Position4D" should "return its First over names" in {
    toRDD(data)
      .map { case (s, i) => Position4D(s, i, i + 1, i + 2) }
      .names(Over(First), Default(Reducers(12)))
      .toList.sorted shouldBe result12
  }

  it should "return its Second over names" in {
    toRDD(data)
      .map { case (s, i) => Position4D(s, i, i + 1, i + 2) }
      .names(Over(Second), Default())
      .toList.sorted shouldBe result13
  }

  it should "return its Third over names" in {
    toRDD(data)
      .map { case (s, i) => Position4D(s, i, i + 1, i + 2) }
      .names(Over(Third), Default(Reducers(12)))
      .toList.sorted shouldBe result14
  }

  it should "return its Fourth over names" in {
    toRDD(data)
      .map { case (s, i) => Position4D(s, i, i + 1, i + 2) }
      .names(Over(Fourth), Default())
      .toList.sorted shouldBe result15
  }

  it should "return its First along names" in {
    toRDD(data)
      .map { case (s, i) => Position4D(s, i, i + 1, i + 2) }
      .names(Along(First), Default(Reducers(12)))
      .toList.sorted shouldBe result16
  }

  it should "return its Second along names" in {
    toRDD(data)
      .map { case (s, i) => Position4D(s, i, i + 1, i + 2) }
      .names(Along(Second), Default())
      .toList.sorted shouldBe result17
  }

  it should "return its Third along names" in {
    toRDD(data)
      .map { case (s, i) => Position4D(s, i, i + 1, i + 2) }
      .names(Along(Third), Default(Reducers(12)))
      .toList.sorted shouldBe result18
  }

  it should "return its Fourth along names" in {
    toRDD(data)
      .map { case (s, i) => Position4D(s, i, i + 1, i + 2) }
      .names(Along(Fourth), Default())
      .toList.sorted shouldBe result19
  }

  "A Positions of Position5D" should "return its First over names" in {
    toRDD(data)
      .map { case (s, i) => Position5D(s, i, i + 1, i + 2, i + 3) }
      .names(Over(First), Default(Reducers(12)))
      .toList.sorted shouldBe result20
  }

  it should "return its Second over names" in {
    toRDD(data)
      .map { case (s, i) => Position5D(s, i, i + 1, i + 2, i + 3) }
      .names(Over(Second), Default())
      .toList.sorted shouldBe result21
  }

  it should "return its Third over names" in {
    toRDD(data)
      .map { case (s, i) => Position5D(s, i, i + 1, i + 2, i + 3) }
      .names(Over(Third), Default(Reducers(12)))
      .toList.sorted shouldBe result22
  }

  it should "return its Fourth over names" in {
    toRDD(data)
      .map { case (s, i) => Position5D(s, i, i + 1, i + 2, i + 3) }
      .names(Over(Fourth), Default())
      .toList.sorted shouldBe result23
  }

  it should "return its Fifth over names" in {
    toRDD(data)
      .map { case (s, i) => Position5D(s, i, i + 1, i + 2, i + 3) }
      .names(Over(Fifth), Default(Reducers(12)))
      .toList.sorted shouldBe result24
  }

  it should "return its First along names" in {
    toRDD(data)
      .map { case (s, i) => Position5D(s, i, i + 1, i + 2, i + 3) }
      .names(Along(First), Default())
      .toList.sorted shouldBe result25
  }

  it should "return its Second along names" in {
    toRDD(data)
      .map { case (s, i) => Position5D(s, i, i + 1, i + 2, i + 3) }
      .names(Along(Second), Default(Reducers(12)))
      .toList.sorted shouldBe result26
  }

  it should "return its Third along names" in {
    toRDD(data)
      .map { case (s, i) => Position5D(s, i, i + 1, i + 2, i + 3) }
      .names(Along(Third), Default())
      .toList.sorted shouldBe result27
  }

  it should "return its Fourth along names" in {
    toRDD(data)
      .map { case (s, i) => Position5D(s, i, i + 1, i + 2, i + 3) }
      .names(Along(Fourth), Default(Reducers(12)))
      .toList.sorted shouldBe result28
  }

  it should "return its Fifth along names" in {
    toRDD(data)
      .map { case (s, i) => Position5D(s, i, i + 1, i + 2, i + 3) }
      .names(Along(Fifth), Default())
      .toList.sorted shouldBe result29
  }
}

