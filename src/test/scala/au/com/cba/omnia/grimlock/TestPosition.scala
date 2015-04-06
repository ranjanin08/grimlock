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
import au.com.cba.omnia.grimlock.position._
import au.com.cba.omnia.grimlock.position.ScaldingPositions._

import com.twitter.scalding._
import com.twitter.scalding.bdd._

import org.scalatest._

import scala.collection.mutable

class TestPosition0D extends FlatSpec with Matchers {

  val pos = Position0D()

  "A Position0D" should "return its short string" in {
    pos.toShortString("|") should be ("")
  }

  it should "have coordinates" in {
    pos.coordinates should be (List())
  }

  it should "throw an exception for an invalid dimension" in {
    a [IndexOutOfBoundsException] should be thrownBy { pos(First) }
    a [IndexOutOfBoundsException] should be thrownBy { pos(Second) }
    a [IndexOutOfBoundsException] should be thrownBy { pos(Third) }
    a [IndexOutOfBoundsException] should be thrownBy { pos(Fourth) }
    a [IndexOutOfBoundsException] should be thrownBy { pos(Fifth) }
  }

  it should "compare" in {
    pos.compare(pos) should be (0)
    pos.compare(Position1D("xyz")) should be < 0
  }

  it should "prepend" in {
    pos.prepend(123) should be (Position1D(123))
  }

  it should "append" in {
    pos.append(123) should be (Position1D(123))
  }
}

class TestPosition1D extends FlatSpec with Matchers {

  val pos = Position1D("foo")

  "A Position1D" should "return its short string" in {
    pos.toShortString("|") should be ("foo")
  }

  it should "have coordinates" in {
    pos.coordinates should be (List(StringValue("foo")))
  }

  it should "return its coordinates" in {
    pos(First) should be (StringValue("foo"))
  }

  it should "throw an exception for an invalid dimension" in {
    a [IndexOutOfBoundsException] should be thrownBy { pos(Second) }
    a [IndexOutOfBoundsException] should be thrownBy { pos(Third) }
    a [IndexOutOfBoundsException] should be thrownBy { pos(Fourth) }
    a [IndexOutOfBoundsException] should be thrownBy { pos(Fifth) }
  }

  it should "compare" in {
    pos.compare(Position1D("abc")) should be > 0
    pos.compare(pos) should be (0)
    pos.compare(Position1D("xyz")) should be < 0
    pos.compare(Position2D("abc", "xyz")) should be < 0
  }

  it should "throw an exception for an invalid value" in {
    a [Exception] should be thrownBy { pos.compare(Position1D(123)) }
  }

  it should "be updated" in {
    pos.update(First, 123) should be (Position1D(123))
  }

  it should "throw an exception for an invalid update dimension" in {
    a [UnsupportedOperationException] should be thrownBy { pos.update(Second, 123) }
    a [UnsupportedOperationException] should be thrownBy { pos.update(Third, 123) }
    a [UnsupportedOperationException] should be thrownBy { pos.update(Fourth, 123) }
    a [UnsupportedOperationException] should be thrownBy { pos.update(Fifth, 123) }
  }

  val con1 = Content(ContinuousSchema[Codex.LongCodex](), 1)
  val con2 = Content(ContinuousSchema[Codex.LongCodex](), 2)

  it should "return a over map" in {
    pos.over.toMapValue(Position0D(), con1) should be (con1)
  }

  it should "combine two over maps" in {
    pos.over.combineMapValues(Some(con1), con2) should be (con2)
    pos.over.combineMapValues(None, con2) should be (con2)
  }

  it should "throw an exception for an along map" in {
    a [Exception] should be thrownBy { pos.along.toMapValue(pos, con1) should be (con1) }
  }

  it should "throw an exception on combine two along maps" in {
    a [Exception] should be thrownBy { pos.along.combineMapValues(Some(con1), con2) }
    a [Exception] should be thrownBy { pos.along.combineMapValues(None, con2) }
  }

  it should "remove" in {
    pos.remove(First) should be (Position0D())
  }

  it should "throw an exception for an invalid remove dimension" in {
    a [UnsupportedOperationException] should be thrownBy { pos.remove(Second) }
    a [UnsupportedOperationException] should be thrownBy { pos.remove(Third) }
    a [UnsupportedOperationException] should be thrownBy { pos.remove(Fourth) }
    a [UnsupportedOperationException] should be thrownBy { pos.remove(Fifth) }
  }

  it should "melt" in {
    pos.melt(First, First, "|") should be (Position0D())
  }

  it should "throw an exception for an invalid melt dimension" in {
    a [IndexOutOfBoundsException] should be thrownBy { pos.melt(First, Second, "|") }
    a [IndexOutOfBoundsException] should be thrownBy { pos.melt(First, Third, "|") }
    a [IndexOutOfBoundsException] should be thrownBy { pos.melt(First, Fourth, "|") }
    a [IndexOutOfBoundsException] should be thrownBy { pos.melt(First, Fifth, "|") }

    a [IndexOutOfBoundsException] should be thrownBy { pos.melt(Second, First, "|") }
    a [IndexOutOfBoundsException] should be thrownBy { pos.melt(Third, First, "|") }
    a [IndexOutOfBoundsException] should be thrownBy { pos.melt(Fourth, First, "|") }
    a [IndexOutOfBoundsException] should be thrownBy { pos.melt(Fifth, First, "|") }
  }

  it should "prepend" in {
    pos.prepend(123) should be (Position2D(123, "foo"))
  }

  it should "append" in {
    pos.append(123) should be (Position2D("foo", 123))
  }
}

class TestPosition2D extends FlatSpec with Matchers {

  val pos = Position2D("foo", 123)

  "A Position2D" should "return its short string" in {
    pos.toShortString("|") should be ("foo|123")
  }

  it should "have coordinates" in {
    pos.coordinates should be (List(StringValue("foo"), LongValue(123)))
  }

  it should "return its coordinates" in {
    pos(First) should be (StringValue("foo"))
    pos(Second) should be (LongValue(123))
  }

  it should "throw an exception for an invalid dimension" in {
    a [IndexOutOfBoundsException] should be thrownBy { pos(Third) }
    a [IndexOutOfBoundsException] should be thrownBy { pos(Fourth) }
    a [IndexOutOfBoundsException] should be thrownBy { pos(Fifth) }
  }

  it should "compare" in {
    pos.compare(Position2D("abc", 456)) should be > 0
    pos.compare(Position2D("foo", 1)) should be > 0
    pos.compare(pos) should be (0)
    pos.compare(Position2D("xyz", 1)) should be < 0
    pos.compare(Position2D("foo", 789)) should be < 0
    pos.compare(Position1D("xyz")) should be > 0
    pos.compare(Position3D("abc", 1, "bar")) should be < 0
  }

  it should "throw an exception for an invalid value" in {
    a [Exception] should be thrownBy { pos.compare(Position2D(123, 456)) }
    a [Exception] should be thrownBy { pos.compare(Position2D("xyz", "foo")) }
  }

  it should "be updated" in {
    pos.update(First, 123) should be (Position2D(123, 123))
    pos.update(Second, "xyz") should be (Position2D("foo", "xyz"))
  }

  it should "throw an exception for an invalid update dimension" in {
    a [UnsupportedOperationException] should be thrownBy { pos.update(Third, 123) }
    a [UnsupportedOperationException] should be thrownBy { pos.update(Fourth, 123) }
    a [UnsupportedOperationException] should be thrownBy { pos.update(Fifth, 123) }
  }

  it should "permute" in {
    pos.permute(List(First, Second)) should be (pos)
    pos.permute(List(Second, First)) should be (Position2D(123, "foo"))
  }

  it should "throw an exception for an invalid permute dimension" in {
    a [IndexOutOfBoundsException] should be thrownBy { pos.permute(List(Third)) }
    a [IndexOutOfBoundsException] should be thrownBy { pos.permute(List(First, Second, Third)) }
  }

  val con1 = Content(ContinuousSchema[Codex.LongCodex](), 1)
  val con2 = Content(ContinuousSchema[Codex.LongCodex](), 2)

  it should "return a over map" in {
    pos.over.toMapValue(Position1D("xyz"), con1) should be (Map(Position1D("xyz") -> con1))
  }

  it should "combine two over maps" in {
    pos.over.combineMapValues(Some(Map(Position1D("xyz") -> con1, Position1D("abc") -> con2)),
      Map(Position1D("xyz") -> con2, Position1D("klm") -> con1)) should
        be (Map(Position1D("xyz") -> con2, Position1D("abc") -> con2, Position1D("klm") -> con1))
    pos.over.combineMapValues(None, Map(Position1D("xyz") -> con2)) should be (Map(Position1D("xyz") -> con2))
  }

  it should "return an along map" in {
    pos.along.toMapValue(Position1D("xyz"), con1) should be (Map(Position1D("xyz") -> con1))
  }

  it should "combine two along maps" in {
    pos.along.combineMapValues(Some(Map(Position1D("xyz") -> con1, Position1D("abc") -> con2)),
      Map(Position1D("xyz") -> con2, Position1D("klm") -> con1)) should
        be (Map(Position1D("xyz") -> con2, Position1D("abc") -> con2, Position1D("klm") -> con1))
    pos.along.combineMapValues(None, Map(Position1D("xyz") -> con2)) should be (Map(Position1D("xyz") -> con2))
  }

  it should "remove" in {
    pos.remove(First) should be (Position1D(123))
    pos.remove(Second) should be (Position1D("foo"))
  }

  it should "throw an exception for an invalid remove dimension" in {
    a [UnsupportedOperationException] should be thrownBy { pos.remove(Third) }
    a [UnsupportedOperationException] should be thrownBy { pos.remove(Fourth) }
    a [UnsupportedOperationException] should be thrownBy { pos.remove(Fifth) }
  }

  it should "melt" in {
    pos.melt(First, First, "|") should be (Position1D(123))
    pos.melt(First, Second, "|") should be (Position1D("123|foo"))
    pos.melt(Second, First, "|") should be (Position1D("foo|123"))
    pos.melt(Second, Second, "|") should be (Position1D("foo"))
  }

  it should "throw an exception for an invalid melt dimension" in {
    a [IndexOutOfBoundsException] should be thrownBy { pos.melt(First, Third, "|") }
    a [IndexOutOfBoundsException] should be thrownBy { pos.melt(First, Fourth, "|") }
    a [IndexOutOfBoundsException] should be thrownBy { pos.melt(First, Fifth, "|") }

    a [IndexOutOfBoundsException] should be thrownBy { pos.melt(Third, First, "|") }
    a [IndexOutOfBoundsException] should be thrownBy { pos.melt(Fourth, First, "|") }
    a [IndexOutOfBoundsException] should be thrownBy { pos.melt(Fifth, First, "|") }

    a [IndexOutOfBoundsException] should be thrownBy { pos.melt(Second, Third, "|") }
    a [IndexOutOfBoundsException] should be thrownBy { pos.melt(Second, Fourth, "|") }
    a [IndexOutOfBoundsException] should be thrownBy { pos.melt(Second, Fifth, "|") }

    a [IndexOutOfBoundsException] should be thrownBy { pos.melt(Third, Second, "|") }
    a [IndexOutOfBoundsException] should be thrownBy { pos.melt(Fourth, Second, "|") }
    a [IndexOutOfBoundsException] should be thrownBy { pos.melt(Fifth, Second, "|") }
  }

  it should "prepend" in {
    pos.prepend(123) should be (Position3D(123, "foo", 123))
  }

  it should "append" in {
    pos.append(123) should be (Position3D("foo", 123, 123))
  }
}

class TestPosition3D extends FlatSpec with Matchers {

  val pos = Position3D("foo", 123, "bar")

  "A Position3D" should "return its short string" in {
    pos.toShortString("|") should be ("foo|123|bar")
  }

  it should "have coordinates" in {
    pos.coordinates should be (List(StringValue("foo"), LongValue(123), StringValue("bar")))
  }

  it should "return its coordinates" in {
    pos(First) should be (StringValue("foo"))
    pos(Second) should be (LongValue(123))
    pos(Third) should be (StringValue("bar"))
  }

  it should "throw an exception for an invalid dimension" in {
    a [IndexOutOfBoundsException] should be thrownBy { pos(Fourth) }
    a [IndexOutOfBoundsException] should be thrownBy { pos(Fifth) }
  }

  it should "compare" in {
    pos.compare(Position3D("abc", 456, "xyz")) should be > 0
    pos.compare(Position3D("foo", 1, "xyz")) should be > 0
    pos.compare(Position3D("foo", 123, "abc")) should be > 0
    pos.compare(pos) should be (0)
    pos.compare(Position3D("xyz", 1, "abc")) should be < 0
    pos.compare(Position3D("foo", 789, "abc")) should be < 0
    pos.compare(Position3D("foo", 123, "xyz")) should be < 0
    pos.compare(Position2D("xyz", 456)) should be > 0
    pos.compare(Position4D("abc", 1, "abc", 1)) should be < 0
  }

  it should "throw an exception for an invalid value" in {
    a [Exception] should be thrownBy { pos.compare(Position3D(123, 123, "bar")) }
    a [Exception] should be thrownBy { pos.compare(Position3D("foo", "123", "bar")) }
    a [Exception] should be thrownBy { pos.compare(Position3D("foo", 123, 123)) }
  }

  it should "be updated" in {
    pos.update(First, 123) should be (Position3D(123, 123, "bar"))
    pos.update(Second, "xyz") should be (Position3D("foo", "xyz", "bar"))
    pos.update(Third, 456) should be (Position3D("foo", 123, 456))
  }

  it should "throw an exception for an invalid update dimension" in {
    a [UnsupportedOperationException] should be thrownBy { pos.update(Fourth, 123) }
    a [UnsupportedOperationException] should be thrownBy { pos.update(Fifth, 123) }
  }

  it should "permute" in {
    pos.permute(List(First, Second, Third)) should be (pos)
    pos.permute(List(First, Third, Second)) should be (Position3D("foo", "bar", 123))
    pos.permute(List(Second, First, Third)) should be (Position3D(123, "foo", "bar"))
    pos.permute(List(Second, Third, First)) should be (Position3D(123, "bar", "foo"))
    pos.permute(List(Third, First, Second)) should be (Position3D("bar", "foo", 123))
    pos.permute(List(Third, Second, First)) should be (Position3D("bar", 123, "foo"))
  }

  it should "throw an exception for an invalid permute dimension" in {
    a [IndexOutOfBoundsException] should be thrownBy { pos.permute(List(Fourth)) }
    a [IndexOutOfBoundsException] should be thrownBy { pos.permute(List(First, Second, Third, Fourth)) }
  }

  val con1 = Content(ContinuousSchema[Codex.LongCodex](), 1)
  val con2 = Content(ContinuousSchema[Codex.LongCodex](), 2)

  it should "return a over map" in {
    pos.over.toMapValue(Position2D("xyz", 456), con1) should be (Map(Position2D("xyz", 456) -> con1))
  }

  it should "combine two over maps" in {
    pos.over.combineMapValues(Some(Map(Position2D("xyz", 456) -> con1, Position2D("abc", 123) -> con2)),
      Map(Position2D("xyz", 456) -> con2, Position2D("klm", 321) -> con1)) should
        be (Map(Position2D("xyz", 456) -> con2, Position2D("abc", 123) -> con2, Position2D("klm", 321) -> con1))
    pos.over.combineMapValues(None, Map(Position2D("xyz", 456) -> con2)) should be (Map(Position2D("xyz", 456) -> con2))
  }

  it should "return an along map" in {
    pos.along.toMapValue(Position1D("xyz"), con1) should be (Map(Position1D("xyz") -> con1))
  }

  it should "combine two along maps" in {
    pos.along.combineMapValues(Some(Map(Position1D("xyz") -> con1, Position1D("abc") -> con2)),
      Map(Position1D("xyz") -> con2, Position1D("klm") -> con1)) should
        be (Map(Position1D("xyz") -> con2, Position1D("abc") -> con2, Position1D("klm") -> con1))
    pos.along.combineMapValues(None, Map(Position1D("xyz") -> con2)) should be (Map(Position1D("xyz") -> con2))
  }

  it should "remove" in {
    pos.remove(First) should be (Position2D(123, "bar"))
    pos.remove(Second) should be (Position2D("foo", "bar"))
    pos.remove(Third) should be (Position2D("foo", 123))
  }

  it should "throw an exception for an invalid remove dimension" in {
    a [UnsupportedOperationException] should be thrownBy { pos.remove(Fourth) }
    a [UnsupportedOperationException] should be thrownBy { pos.remove(Fifth) }
  }

  it should "melt" in {
    pos.melt(First, First, "|") should be (Position2D(123, "bar"))
    pos.melt(First, Second, "|") should be (Position2D("123|foo", "bar"))
    pos.melt(First, Third, "|") should be (Position2D(123, "bar|foo"))

    pos.melt(Second, First, "|") should be (Position2D("foo|123", "bar"))
    pos.melt(Second, Second, "|") should be (Position2D("foo", "bar"))
    pos.melt(Second, Third, "|") should be (Position2D("foo", "bar|123"))

    pos.melt(Third, First, "|") should be (Position2D("foo|bar", 123))
    pos.melt(Third, Second, "|") should be (Position2D("foo", "123|bar"))
    pos.melt(Third, Third, "|") should be (Position2D("foo", 123))
  }

  it should "throw an exception for an invalid melt dimension" in {
    a [IndexOutOfBoundsException] should be thrownBy { pos.melt(First, Fourth, "|") }
    a [IndexOutOfBoundsException] should be thrownBy { pos.melt(First, Fifth, "|") }

    a [IndexOutOfBoundsException] should be thrownBy { pos.melt(Fourth, First, "|") }
    a [IndexOutOfBoundsException] should be thrownBy { pos.melt(Fifth, First, "|") }

    a [IndexOutOfBoundsException] should be thrownBy { pos.melt(Second, Fourth, "|") }
    a [IndexOutOfBoundsException] should be thrownBy { pos.melt(Second, Fifth, "|") }

    a [IndexOutOfBoundsException] should be thrownBy { pos.melt(Fourth, Second, "|") }
    a [IndexOutOfBoundsException] should be thrownBy { pos.melt(Fifth, Second, "|") }

    a [IndexOutOfBoundsException] should be thrownBy { pos.melt(Third, Fourth, "|") }
    a [IndexOutOfBoundsException] should be thrownBy { pos.melt(Third, Fifth, "|") }

    a [IndexOutOfBoundsException] should be thrownBy { pos.melt(Fourth, Third, "|") }
    a [IndexOutOfBoundsException] should be thrownBy { pos.melt(Fifth, Third, "|") }
  }

  it should "prepend" in {
    pos.prepend(123) should be (Position4D(123, "foo", 123, "bar"))
  }

  it should "append" in {
    pos.append(123) should be (Position4D("foo", 123, "bar", 123))
  }
}

class TestPosition4D extends FlatSpec with Matchers {

  val pos = Position4D("foo", 123, "bar", 456)

  "A Position4D" should "return its short string" in {
    pos.toShortString("|") should be ("foo|123|bar|456")
  }

  it should "have coordinates" in {
    pos.coordinates should be (List(StringValue("foo"), LongValue(123), StringValue("bar"), LongValue(456)))
  }

  it should "return its coordinates" in {
    pos(First) should be (StringValue("foo"))
    pos(Second) should be (LongValue(123))
    pos(Third) should be (StringValue("bar"))
    pos(Fourth) should be (LongValue(456))
  }

  it should "throw an exception for an invalid dimension" in {
    a [IndexOutOfBoundsException] should be thrownBy { pos(Fifth) }
  }

  it should "compare" in {
    pos.compare(Position4D("abc", 456, "xyz", 789)) should be > 0
    pos.compare(Position4D("foo", 1, "xyz", 789)) should be > 0
    pos.compare(Position4D("foo", 123, "abc", 789)) should be > 0
    pos.compare(Position4D("foo", 123, "bar", 1)) should be > 0
    pos.compare(pos) should be (0)
    pos.compare(Position4D("xyz", 1, "abc", 1)) should be < 0
    pos.compare(Position4D("foo", 789, "abc", 1)) should be < 0
    pos.compare(Position4D("foo", 123, "xyz", 1)) should be < 0
    pos.compare(Position4D("foo", 123, "bar", 789)) should be < 0
    pos.compare(Position3D("xyz", 456, "xyz")) should be > 0
    pos.compare(Position5D("abc", 1, "abc", 1, 1)) should be < 0
  }

  it should "throw an exception for an invalid value" in {
    a [Exception] should be thrownBy { pos.compare(Position4D(123, 123, "bar", 456)) }
    a [Exception] should be thrownBy { pos.compare(Position4D("foo", "123", "bar", 456)) }
    a [Exception] should be thrownBy { pos.compare(Position4D("foo", 123, 123, 456)) }
    a [Exception] should be thrownBy { pos.compare(Position4D("foo", 123, "bar", "456")) }
  }

  it should "be updated" in {
    pos.update(First, 123) should be (Position4D(123, 123, "bar", 456))
    pos.update(Second, "xyz") should be (Position4D("foo", "xyz", "bar", 456))
    pos.update(Third, 456) should be (Position4D("foo", 123, 456, 456))
    pos.update(Fourth, "abc") should be (Position4D("foo", 123, "bar", "abc"))
  }

  it should "throw an exception for an invalid update dimension" in {
    a [UnsupportedOperationException] should be thrownBy { pos.update(Fifth, 123) }
  }

  it should "permute" in {
    pos.permute(List(First, Second, Third, Fourth)) should be (pos)
    pos.permute(List(First, Second, Fourth, Third)) should be (Position4D("foo", 123, 456, "bar"))
    pos.permute(List(First, Third, Second, Fourth)) should be (Position4D("foo", "bar", 123, 456))
    pos.permute(List(First, Third, Fourth, Second)) should be (Position4D("foo", "bar", 456, 123))
    pos.permute(List(First, Fourth, Second, Third)) should be (Position4D("foo", 456, 123, "bar"))
    pos.permute(List(First, Fourth, Third, Second)) should be (Position4D("foo", 456, "bar", 123))

    pos.permute(List(Second, First, Third, Fourth)) should be (Position4D(123, "foo", "bar", 456))
    pos.permute(List(Second, First, Fourth, Third)) should be (Position4D(123, "foo", 456, "bar"))
    pos.permute(List(Second, Third, First, Fourth)) should be (Position4D(123, "bar", "foo", 456))
    pos.permute(List(Second, Third, Fourth, First)) should be (Position4D(123, "bar", 456, "foo"))
    pos.permute(List(Second, Fourth, First, Third)) should be (Position4D(123, 456, "foo", "bar"))
    pos.permute(List(Second, Fourth, Third, First)) should be (Position4D(123, 456, "bar", "foo"))

    pos.permute(List(Third, First, Second, Fourth)) should be (Position4D("bar", "foo", 123, 456))
    pos.permute(List(Third, First, Fourth, Second)) should be (Position4D("bar", "foo", 456, 123))
    pos.permute(List(Third, Second, First, Fourth)) should be (Position4D("bar", 123, "foo", 456))
    pos.permute(List(Third, Second, Fourth, First)) should be (Position4D("bar", 123, 456, "foo"))
    pos.permute(List(Third, Fourth, First, Second)) should be (Position4D("bar", 456, "foo", 123))
    pos.permute(List(Third, Fourth, Second, First)) should be (Position4D("bar", 456, 123, "foo"))

    pos.permute(List(Fourth, First, Second, Third)) should be (Position4D(456, "foo", 123, "bar"))
    pos.permute(List(Fourth, First, Third, Second)) should be (Position4D(456, "foo", "bar", 123))
    pos.permute(List(Fourth, Second, First, Third)) should be (Position4D(456, 123, "foo", "bar"))
    pos.permute(List(Fourth, Second, Third, First)) should be (Position4D(456, 123, "bar", "foo"))
    pos.permute(List(Fourth, Third, First, Second)) should be (Position4D(456, "bar", "foo", 123))
    pos.permute(List(Fourth, Third, Second, First)) should be (Position4D(456, "bar", 123, "foo"))
  }

  it should "throw an exception for an invalid permute dimension" in {
    a [IndexOutOfBoundsException] should be thrownBy { pos.permute(List(Fifth)) }
    a [IndexOutOfBoundsException] should be thrownBy { pos.permute(List(First, Second, Third, Fourth, Fifth)) }
  }

  val con1 = Content(ContinuousSchema[Codex.LongCodex](), 1)
  val con2 = Content(ContinuousSchema[Codex.LongCodex](), 2)

  it should "return a over map" in {
    pos.over.toMapValue(Position3D("xyz", 456, "abc"), con1) should be (Map(Position3D("xyz", 456, "abc") -> con1))
  }

  it should "combine two over maps" in {
    pos.over.combineMapValues(Some(Map(Position3D("xyz", 456, 123) -> con1, Position3D("abc", 123, "zyx") -> con2)),
      Map(Position3D("xyz", 456, 123) -> con2, Position3D("klm", 321, "abc") -> con1)) should
        be (Map(Position3D("xyz", 456, 123) -> con2, Position3D("abc", 123, "zyx") -> con2,
          Position3D("klm", 321, "abc") -> con1))
    pos.over.combineMapValues(None, Map(Position3D("xyz", 456, 123) -> con2)) should
      be (Map(Position3D("xyz", 456, 123) -> con2))
  }

  it should "return an along map" in {
    pos.along.toMapValue(Position1D("xyz"), con1) should be (Map(Position1D("xyz") -> con1))
  }

  it should "combine two along maps" in {
    pos.along.combineMapValues(Some(Map(Position1D("xyz") -> con1, Position1D("abc") -> con2)),
      Map(Position1D("xyz") -> con2, Position1D("klm") -> con1)) should
        be (Map(Position1D("xyz") -> con2, Position1D("abc") -> con2, Position1D("klm") -> con1))
    pos.along.combineMapValues(None, Map(Position1D("xyz") -> con2)) should be (Map(Position1D("xyz") -> con2))
  }

  it should "remove" in {
    pos.remove(First) should be (Position3D(123, "bar", 456))
    pos.remove(Second) should be (Position3D("foo", "bar", 456))
    pos.remove(Third) should be (Position3D("foo", 123, 456))
    pos.remove(Fourth) should be (Position3D("foo", 123, "bar"))
  }

  it should "throw an exception for an invalid remove dimension" in {
    a [UnsupportedOperationException] should be thrownBy { pos.remove(Fifth) }
  }

  it should "melt" in {
    pos.melt(First, First, "|") should be (Position3D(123, "bar", 456))
    pos.melt(First, Second, "|") should be (Position3D("123|foo", "bar", 456))
    pos.melt(First, Third, "|") should be (Position3D(123, "bar|foo", 456))
    pos.melt(First, Fourth, "|") should be (Position3D(123, "bar", "456|foo"))

    pos.melt(Second, First, "|") should be (Position3D("foo|123", "bar", 456))
    pos.melt(Second, Second, "|") should be (Position3D("foo", "bar", 456))
    pos.melt(Second, Third, "|") should be (Position3D("foo", "bar|123", 456))
    pos.melt(Second, Fourth, "|") should be (Position3D("foo", "bar", "456|123"))

    pos.melt(Third, First, "|") should be (Position3D("foo|bar", 123, 456))
    pos.melt(Third, Second, "|") should be (Position3D("foo", "123|bar", 456))
    pos.melt(Third, Third, "|") should be (Position3D("foo", 123, 456))
    pos.melt(Third, Fourth, "|") should be (Position3D("foo", 123, "456|bar"))

    pos.melt(Fourth, First, "|") should be (Position3D("foo|456", 123, "bar"))
    pos.melt(Fourth, Second, "|") should be (Position3D("foo", "123|456", "bar"))
    pos.melt(Fourth, Third, "|") should be (Position3D("foo", 123, "bar|456"))
    pos.melt(Fourth, Fourth, "|") should be (Position3D("foo", 123, "bar"))
  }

  it should "throw an exception for an invalid melt dimension" in {
    a [IndexOutOfBoundsException] should be thrownBy { pos.melt(First, Fifth, "|") }
    a [IndexOutOfBoundsException] should be thrownBy { pos.melt(Fifth, First, "|") }

    a [IndexOutOfBoundsException] should be thrownBy { pos.melt(Second, Fifth, "|") }
    a [IndexOutOfBoundsException] should be thrownBy { pos.melt(Fifth, Second, "|") }

    a [IndexOutOfBoundsException] should be thrownBy { pos.melt(Third, Fifth, "|") }
    a [IndexOutOfBoundsException] should be thrownBy { pos.melt(Fifth, Third, "|") }

    a [IndexOutOfBoundsException] should be thrownBy { pos.melt(Fourth, Fifth, "|") }
    a [IndexOutOfBoundsException] should be thrownBy { pos.melt(Fifth, Fourth, "|") }
  }

  it should "prepend" in {
    pos.prepend(123) should be (Position5D(123, "foo", 123, "bar", 456))
  }

  it should "append" in {
    pos.append(123) should be (Position5D("foo", 123, "bar", 456, 123))
  }
}

class TestPosition5D extends FlatSpec with Matchers {

  val pos = Position5D("foo", 123, "bar", 456, "baz")

  "A Position5D" should "return its short string" in {
    pos.toShortString("|") should be ("foo|123|bar|456|baz")
  }

  it should "have coordinates" in {
    pos.coordinates should be (List(StringValue("foo"), LongValue(123), StringValue("bar"),
      LongValue(456), StringValue("baz")))
  }

  it should "return its coordinates" in {
    pos(First) should be (StringValue("foo"))
    pos(Second) should be (LongValue(123))
    pos(Third) should be (StringValue("bar"))
    pos(Fourth) should be (LongValue(456))
    pos(Fifth) should be (StringValue("baz"))
  }

  it should "compare" in {
    pos.compare(Position5D("abc", 456, "xyz", 789, "xyz")) should be > 0
    pos.compare(Position5D("foo", 1, "xyz", 789, "xyz")) should be > 0
    pos.compare(Position5D("foo", 123, "abc", 789, "xyz")) should be > 0
    pos.compare(Position5D("foo", 123, "bar", 1, "xyz")) should be > 0
    pos.compare(Position5D("foo", 123, "bar", 456, "abc")) should be > 0
    pos.compare(pos) should be (0)
    pos.compare(Position5D("xyz", 1, "abc", 1, "abc")) should be < 0
    pos.compare(Position5D("foo", 789, "abc", 1, "abc")) should be < 0
    pos.compare(Position5D("foo", 123, "xyz", 1, "abc")) should be < 0
    pos.compare(Position5D("foo", 123, "bar", 789, "abc")) should be < 0
    pos.compare(Position5D("foo", 123, "bar", 456, "xyz")) should be < 0
    pos.compare(Position4D("xyz", 456, "xyz", 789)) should be > 0
  }

  it should "throw an exception for an invalid value" in {
    a [Exception] should be thrownBy { pos.compare(Position5D(123, 123, "bar", 456, "baz")) }
    a [Exception] should be thrownBy { pos.compare(Position5D("foo", "123", "bar", 456, "baz")) }
    a [Exception] should be thrownBy { pos.compare(Position5D("foo", 123, 123, 456, "baz")) }
    a [Exception] should be thrownBy { pos.compare(Position5D("foo", 123, "bar", "456", "baz")) }
    a [Exception] should be thrownBy { pos.compare(Position5D("foo", 123, "bar", 456, 123)) }
  }

  it should "be updated" in {
    pos.update(First, 123) should be (Position5D(123, 123, "bar", 456, "baz"))
    pos.update(Second, "xyz") should be (Position5D("foo", "xyz", "bar", 456, "baz"))
    pos.update(Third, 456) should be (Position5D("foo", 123, 456, 456, "baz"))
    pos.update(Fourth, "abc") should be (Position5D("foo", 123, "bar", "abc", "baz"))
    pos.update(Fifth, 789) should be (Position5D("foo", 123, "bar", 456, 789))
  }

  it should "permute" in {
    pos.permute(List(First, Second, Third, Fourth, Fifth)) should be (Position5D("foo", 123, "bar", 456, "baz"))
    pos.permute(List(First, Second, Third, Fifth, Fourth)) should be (Position5D("foo", 123, "bar", "baz", 456))
    pos.permute(List(First, Second, Fourth, Third, Fifth)) should be (Position5D("foo", 123, 456, "bar", "baz"))
    pos.permute(List(First, Second, Fourth, Fifth, Third)) should be (Position5D("foo", 123, 456, "baz", "bar"))
    pos.permute(List(First, Second, Fifth, Third, Fourth)) should be (Position5D("foo", 123, "baz", "bar", 456))
    pos.permute(List(First, Second, Fifth, Fourth, Third)) should be (Position5D("foo", 123, "baz", 456, "bar"))
    pos.permute(List(First, Third, Second, Fourth, Fifth)) should be (Position5D("foo", "bar", 123, 456, "baz"))
    pos.permute(List(First, Third, Second, Fifth, Fourth)) should be (Position5D("foo", "bar", 123, "baz", 456))
    pos.permute(List(First, Third, Fourth, Second, Fifth)) should be (Position5D("foo", "bar", 456, 123, "baz"))
    pos.permute(List(First, Third, Fourth, Fifth, Second)) should be (Position5D("foo", "bar", 456, "baz", 123))
    pos.permute(List(First, Third, Fifth, Second, Fourth)) should be (Position5D("foo", "bar", "baz", 123, 456))
    pos.permute(List(First, Third, Fifth, Fourth, Second)) should be (Position5D("foo", "bar", "baz", 456, 123))
    pos.permute(List(First, Fourth, Second, Third, Fifth)) should be (Position5D("foo", 456, 123, "bar", "baz"))
    pos.permute(List(First, Fourth, Second, Fifth, Third)) should be (Position5D("foo", 456, 123, "baz", "bar"))
    pos.permute(List(First, Fourth, Third, Second, Fifth)) should be (Position5D("foo", 456, "bar", 123, "baz"))
    pos.permute(List(First, Fourth, Third, Fifth, Second)) should be (Position5D("foo", 456, "bar", "baz", 123))
    pos.permute(List(First, Fourth, Fifth, Second, Third)) should be (Position5D("foo", 456, "baz", 123, "bar"))
    pos.permute(List(First, Fourth, Fifth, Third, Second)) should be (Position5D("foo", 456, "baz", "bar", 123))
    pos.permute(List(First, Fifth, Second, Third, Fourth)) should be (Position5D("foo", "baz", 123, "bar", 456))
    pos.permute(List(First, Fifth, Second, Fourth, Third)) should be (Position5D("foo", "baz", 123, 456, "bar"))
    pos.permute(List(First, Fifth, Third, Second, Fourth)) should be (Position5D("foo", "baz", "bar", 123, 456))
    pos.permute(List(First, Fifth, Third, Fourth, Second)) should be (Position5D("foo", "baz", "bar", 456, 123))
    pos.permute(List(First, Fifth, Fourth, Second, Third)) should be (Position5D("foo", "baz", 456, 123, "bar"))
    pos.permute(List(First, Fifth, Fourth, Third, Second)) should be (Position5D("foo", "baz", 456, "bar", 123))

    pos.permute(List(Second, First, Third, Fourth, Fifth)) should be (Position5D(123, "foo", "bar", 456, "baz"))
    pos.permute(List(Second, First, Third, Fifth, Fourth)) should be (Position5D(123, "foo", "bar", "baz", 456))
    pos.permute(List(Second, First, Fourth, Third, Fifth)) should be (Position5D(123, "foo", 456, "bar", "baz"))
    pos.permute(List(Second, First, Fourth, Fifth, Third)) should be (Position5D(123, "foo", 456, "baz", "bar"))
    pos.permute(List(Second, First, Fifth, Third, Fourth)) should be (Position5D(123, "foo", "baz", "bar", 456))
    pos.permute(List(Second, First, Fifth, Fourth, Third)) should be (Position5D(123, "foo", "baz", 456, "bar"))
    pos.permute(List(Second, Third, First, Fourth, Fifth)) should be (Position5D(123, "bar", "foo", 456, "baz"))
    pos.permute(List(Second, Third, First, Fifth, Fourth)) should be (Position5D(123, "bar", "foo", "baz", 456))
    pos.permute(List(Second, Third, Fourth, First, Fifth)) should be (Position5D(123, "bar", 456, "foo", "baz"))
    pos.permute(List(Second, Third, Fourth, Fifth, First)) should be (Position5D(123, "bar", 456, "baz", "foo"))
    pos.permute(List(Second, Third, Fifth, First, Fourth)) should be (Position5D(123, "bar", "baz", "foo", 456))
    pos.permute(List(Second, Third, Fifth, Fourth, First)) should be (Position5D(123, "bar", "baz", 456, "foo"))
    pos.permute(List(Second, Fourth, First, Third, Fifth)) should be (Position5D(123, 456, "foo", "bar", "baz"))
    pos.permute(List(Second, Fourth, First, Fifth, Third)) should be (Position5D(123, 456, "foo", "baz", "bar"))
    pos.permute(List(Second, Fourth, Third, First, Fifth)) should be (Position5D(123, 456, "bar", "foo", "baz"))
    pos.permute(List(Second, Fourth, Third, Fifth, First)) should be (Position5D(123, 456, "bar", "baz", "foo"))
    pos.permute(List(Second, Fourth, Fifth, First, Third)) should be (Position5D(123, 456, "baz", "foo", "bar"))
    pos.permute(List(Second, Fourth, Fifth, Third, First)) should be (Position5D(123, 456, "baz", "bar", "foo"))
    pos.permute(List(Second, Fifth, First, Third, Fourth)) should be (Position5D(123, "baz", "foo", "bar", 456))
    pos.permute(List(Second, Fifth, First, Fourth, Third)) should be (Position5D(123, "baz", "foo", 456, "bar"))
    pos.permute(List(Second, Fifth, Third, First, Fourth)) should be (Position5D(123, "baz", "bar", "foo", 456))
    pos.permute(List(Second, Fifth, Third, Fourth, First)) should be (Position5D(123, "baz", "bar", 456, "foo"))
    pos.permute(List(Second, Fifth, Fourth, First, Third)) should be (Position5D(123, "baz", 456, "foo", "bar"))
    pos.permute(List(Second, Fifth, Fourth, Third, First)) should be (Position5D(123, "baz", 456, "bar", "foo"))

    pos.permute(List(Third, First, Second, Fourth, Fifth)) should be (Position5D("bar", "foo", 123, 456, "baz"))
    pos.permute(List(Third, First, Second, Fifth, Fourth)) should be (Position5D("bar", "foo", 123, "baz", 456))
    pos.permute(List(Third, First, Fourth, Second, Fifth)) should be (Position5D("bar", "foo", 456, 123, "baz"))
    pos.permute(List(Third, First, Fourth, Fifth, Second)) should be (Position5D("bar", "foo", 456, "baz", 123))
    pos.permute(List(Third, First, Fifth, Second, Fourth)) should be (Position5D("bar", "foo", "baz", 123, 456))
    pos.permute(List(Third, First, Fifth, Fourth, Second)) should be (Position5D("bar", "foo", "baz", 456, 123))
    pos.permute(List(Third, Second, First, Fourth, Fifth)) should be (Position5D("bar", 123, "foo", 456, "baz"))
    pos.permute(List(Third, Second, First, Fifth, Fourth)) should be (Position5D("bar", 123, "foo", "baz", 456))
    pos.permute(List(Third, Second, Fourth, First, Fifth)) should be (Position5D("bar", 123, 456, "foo", "baz"))
    pos.permute(List(Third, Second, Fourth, Fifth, First)) should be (Position5D("bar", 123, 456, "baz", "foo"))
    pos.permute(List(Third, Second, Fifth, First, Fourth)) should be (Position5D("bar", 123, "baz", "foo", 456))
    pos.permute(List(Third, Second, Fifth, Fourth, First)) should be (Position5D("bar", 123, "baz", 456, "foo"))
    pos.permute(List(Third, Fourth, First, Second, Fifth)) should be (Position5D("bar", 456, "foo", 123, "baz"))
    pos.permute(List(Third, Fourth, First, Fifth, Second)) should be (Position5D("bar", 456, "foo", "baz", 123))
    pos.permute(List(Third, Fourth, Second, First, Fifth)) should be (Position5D("bar", 456, 123, "foo", "baz"))
    pos.permute(List(Third, Fourth, Second, Fifth, First)) should be (Position5D("bar", 456, 123, "baz", "foo"))
    pos.permute(List(Third, Fourth, Fifth, First, Second)) should be (Position5D("bar", 456, "baz", "foo", 123))
    pos.permute(List(Third, Fourth, Fifth, Second, First)) should be (Position5D("bar", 456, "baz", 123, "foo"))
    pos.permute(List(Third, Fifth, First, Second, Fourth)) should be (Position5D("bar", "baz", "foo", 123, 456))
    pos.permute(List(Third, Fifth, First, Fourth, Second)) should be (Position5D("bar", "baz", "foo", 456, 123))
    pos.permute(List(Third, Fifth, Second, First, Fourth)) should be (Position5D("bar", "baz", 123, "foo", 456))
    pos.permute(List(Third, Fifth, Second, Fourth, First)) should be (Position5D("bar", "baz", 123, 456, "foo"))
    pos.permute(List(Third, Fifth, Fourth, First, Second)) should be (Position5D("bar", "baz", 456, "foo", 123))
    pos.permute(List(Third, Fifth, Fourth, Second, First)) should be (Position5D("bar", "baz", 456, 123, "foo"))

    pos.permute(List(Fourth, First, Second, Third, Fifth)) should be (Position5D(456, "foo", 123, "bar", "baz"))
    pos.permute(List(Fourth, First, Second, Fifth, Third)) should be (Position5D(456, "foo", 123, "baz", "bar"))
    pos.permute(List(Fourth, First, Third, Second, Fifth)) should be (Position5D(456, "foo", "bar", 123, "baz"))
    pos.permute(List(Fourth, First, Third, Fifth, Second)) should be (Position5D(456, "foo", "bar", "baz", 123))
    pos.permute(List(Fourth, First, Fifth, Second, Third)) should be (Position5D(456, "foo", "baz", 123, "bar"))
    pos.permute(List(Fourth, First, Fifth, Third, Second)) should be (Position5D(456, "foo", "baz", "bar", 123))
    pos.permute(List(Fourth, Second, First, Third, Fifth)) should be (Position5D(456, 123, "foo", "bar", "baz"))
    pos.permute(List(Fourth, Second, First, Fifth, Third)) should be (Position5D(456, 123, "foo", "baz", "bar"))
    pos.permute(List(Fourth, Second, Third, First, Fifth)) should be (Position5D(456, 123, "bar", "foo", "baz"))
    pos.permute(List(Fourth, Second, Third, Fifth, First)) should be (Position5D(456, 123, "bar", "baz", "foo"))
    pos.permute(List(Fourth, Second, Fifth, First, Third)) should be (Position5D(456, 123, "baz", "foo", "bar"))
    pos.permute(List(Fourth, Second, Fifth, Third, First)) should be (Position5D(456, 123, "baz", "bar", "foo"))
    pos.permute(List(Fourth, Third, First, Second, Fifth)) should be (Position5D(456, "bar", "foo", 123, "baz"))
    pos.permute(List(Fourth, Third, First, Fifth, Second)) should be (Position5D(456, "bar", "foo", "baz", 123))
    pos.permute(List(Fourth, Third, Second, First, Fifth)) should be (Position5D(456, "bar", 123, "foo", "baz"))
    pos.permute(List(Fourth, Third, Second, Fifth, First)) should be (Position5D(456, "bar", 123, "baz", "foo"))
    pos.permute(List(Fourth, Third, Fifth, First, Second)) should be (Position5D(456, "bar", "baz", "foo", 123))
    pos.permute(List(Fourth, Third, Fifth, Second, First)) should be (Position5D(456, "bar", "baz", 123, "foo"))
    pos.permute(List(Fourth, Fifth, First, Second, Third)) should be (Position5D(456, "baz", "foo", 123, "bar"))
    pos.permute(List(Fourth, Fifth, First, Third, Second)) should be (Position5D(456, "baz", "foo", "bar", 123))
    pos.permute(List(Fourth, Fifth, Second, First, Third)) should be (Position5D(456, "baz", 123, "foo", "bar"))
    pos.permute(List(Fourth, Fifth, Second, Third, First)) should be (Position5D(456, "baz", 123, "bar", "foo"))
    pos.permute(List(Fourth, Fifth, Third, First, Second)) should be (Position5D(456, "baz", "bar", "foo", 123))
    pos.permute(List(Fourth, Fifth, Third, Second, First)) should be (Position5D(456, "baz", "bar", 123, "foo"))

    pos.permute(List(Fifth, First, Second, Third, Fourth)) should be (Position5D("baz", "foo", 123, "bar", 456))
    pos.permute(List(Fifth, First, Second, Fourth, Third)) should be (Position5D("baz", "foo", 123, 456, "bar"))
    pos.permute(List(Fifth, First, Third, Second, Fourth)) should be (Position5D("baz", "foo", "bar", 123, 456))
    pos.permute(List(Fifth, First, Third, Fourth, Second)) should be (Position5D("baz", "foo", "bar", 456, 123))
    pos.permute(List(Fifth, First, Fourth, Second, Third)) should be (Position5D("baz", "foo", 456, 123, "bar"))
    pos.permute(List(Fifth, First, Fourth, Third, Second)) should be (Position5D("baz", "foo", 456, "bar", 123))
    pos.permute(List(Fifth, Second, First, Third, Fourth)) should be (Position5D("baz", 123, "foo", "bar", 456))
    pos.permute(List(Fifth, Second, First, Fourth, Third)) should be (Position5D("baz", 123, "foo", 456, "bar"))
    pos.permute(List(Fifth, Second, Third, First, Fourth)) should be (Position5D("baz", 123, "bar", "foo", 456))
    pos.permute(List(Fifth, Second, Third, Fourth, First)) should be (Position5D("baz", 123, "bar", 456, "foo"))
    pos.permute(List(Fifth, Second, Fourth, First, Third)) should be (Position5D("baz", 123, 456, "foo", "bar"))
    pos.permute(List(Fifth, Second, Fourth, Third, First)) should be (Position5D("baz", 123, 456, "bar", "foo"))
    pos.permute(List(Fifth, Third, First, Second, Fourth)) should be (Position5D("baz", "bar", "foo", 123, 456))
    pos.permute(List(Fifth, Third, First, Fourth, Second)) should be (Position5D("baz", "bar", "foo", 456, 123))
    pos.permute(List(Fifth, Third, Second, First, Fourth)) should be (Position5D("baz", "bar", 123, "foo", 456))
    pos.permute(List(Fifth, Third, Second, Fourth, First)) should be (Position5D("baz", "bar", 123, 456, "foo"))
    pos.permute(List(Fifth, Third, Fourth, First, Second)) should be (Position5D("baz", "bar", 456, "foo", 123))
    pos.permute(List(Fifth, Third, Fourth, Second, First)) should be (Position5D("baz", "bar", 456, 123, "foo"))
    pos.permute(List(Fifth, Fourth, First, Second, Third)) should be (Position5D("baz", 456, "foo", 123, "bar"))
    pos.permute(List(Fifth, Fourth, First, Third, Second)) should be (Position5D("baz", 456, "foo", "bar", 123))
    pos.permute(List(Fifth, Fourth, Second, First, Third)) should be (Position5D("baz", 456, 123, "foo", "bar"))
    pos.permute(List(Fifth, Fourth, Second, Third, First)) should be (Position5D("baz", 456, 123, "bar", "foo"))
    pos.permute(List(Fifth, Fourth, Third, First, Second)) should be (Position5D("baz", 456, "bar", "foo", 123))
    pos.permute(List(Fifth, Fourth, Third, Second, First)) should be (Position5D("baz", 456, "bar", 123, "foo"))
  }

  val con1 = Content(ContinuousSchema[Codex.LongCodex](), 1)
  val con2 = Content(ContinuousSchema[Codex.LongCodex](), 2)

  it should "return a over map" in {
    pos.over.toMapValue(Position4D("xyz", 456, "abc", 123), con1) should
      be (Map(Position4D("xyz", 456, "abc", 123) -> con1))
  }

  it should "combine two over maps" in {
    pos.over.combineMapValues(Some(Map(Position4D("xyz", 456, 123, "abc") -> con1,
      Position4D("abc", 123, "zyx", 789) -> con2)), Map(Position4D("xyz", 456, 123, "abc") -> con2,
        Position4D("klm", 321, "abc", 654) -> con1)) should be (Map(Position4D("xyz", 456, 123, "abc") -> con2,
          Position4D("abc", 123, "zyx", 789) -> con2, Position4D("klm", 321, "abc", 654) -> con1))
    pos.over.combineMapValues(None, Map(Position4D("xyz", 456, 123, "abc") -> con2)) should
      be (Map(Position4D("xyz", 456, 123, "abc") -> con2))
  }

  it should "return an along map" in {
    pos.along.toMapValue(Position1D("xyz"), con1) should be (Map(Position1D("xyz") -> con1))
  }

  it should "combine two along maps" in {
    pos.along.combineMapValues(Some(Map(Position1D("xyz") -> con1, Position1D("abc") -> con2)),
      Map(Position1D("xyz") -> con2, Position1D("klm") -> con1)) should
        be (Map(Position1D("xyz") -> con2, Position1D("abc") -> con2, Position1D("klm") -> con1))
    pos.along.combineMapValues(None, Map(Position1D("xyz") -> con2)) should be (Map(Position1D("xyz") -> con2))
  }

  it should "remove" in {
    pos.remove(First) should be (Position4D(123, "bar", 456, "baz"))
    pos.remove(Second) should be (Position4D("foo", "bar", 456, "baz"))
    pos.remove(Third) should be (Position4D("foo", 123, 456, "baz"))
    pos.remove(Fourth) should be (Position4D("foo", 123, "bar", "baz"))
    pos.remove(Fifth) should be (Position4D("foo", 123, "bar", 456))
  }

  it should "melt" in {
    pos.melt(First, First, "|") should be (Position4D(123, "bar", 456, "baz"))
    pos.melt(First, Second, "|") should be (Position4D("123|foo", "bar", 456, "baz"))
    pos.melt(First, Third, "|") should be (Position4D(123, "bar|foo", 456, "baz"))
    pos.melt(First, Fourth, "|") should be (Position4D(123, "bar", "456|foo", "baz"))
    pos.melt(First, Fifth, "|") should be (Position4D(123, "bar", 456, "baz|foo"))

    pos.melt(Second, First, "|") should be (Position4D("foo|123", "bar", 456, "baz"))
    pos.melt(Second, Second, "|") should be (Position4D("foo", "bar", 456, "baz"))
    pos.melt(Second, Third, "|") should be (Position4D("foo", "bar|123", 456, "baz"))
    pos.melt(Second, Fourth, "|") should be (Position4D("foo", "bar", "456|123", "baz"))
    pos.melt(Second, Fifth, "|") should be (Position4D("foo", "bar", 456, "baz|123"))

    pos.melt(Third, First, "|") should be (Position4D("foo|bar", 123, 456, "baz"))
    pos.melt(Third, Second, "|") should be (Position4D("foo", "123|bar", 456, "baz"))
    pos.melt(Third, Third, "|") should be (Position4D("foo", 123, 456, "baz"))
    pos.melt(Third, Fourth, "|") should be (Position4D("foo", 123, "456|bar", "baz"))
    pos.melt(Third, Fifth, "|") should be (Position4D("foo", 123, 456, "baz|bar"))

    pos.melt(Fourth, First, "|") should be (Position4D("foo|456", 123, "bar", "baz"))
    pos.melt(Fourth, Second, "|") should be (Position4D("foo", "123|456", "bar", "baz"))
    pos.melt(Fourth, Third, "|") should be (Position4D("foo", 123, "bar|456", "baz"))
    pos.melt(Fourth, Fourth, "|") should be (Position4D("foo", 123, "bar", "baz"))
    pos.melt(Fourth, Fifth, "|") should be (Position4D("foo", 123, "bar", "baz|456"))

    pos.melt(Fifth, First, "|") should be (Position4D("foo|baz", 123, "bar", 456))
    pos.melt(Fifth, Second, "|") should be (Position4D("foo", "123|baz", "bar", 456))
    pos.melt(Fifth, Third, "|") should be (Position4D("foo", 123, "bar|baz", 456))
    pos.melt(Fifth, Fourth, "|") should be (Position4D("foo", 123, "bar", "456|baz"))
    pos.melt(Fifth, Fifth, "|") should be (Position4D("foo", 123, "bar", 456))
  }
}

class TypedPositions extends WordSpec with Matchers with TBddDsl {

  val data = List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F").zipWithIndex

  "A Positions of Position1D" should {
    "return its First over names" in {
      Given {
        data.map { case (s, i) => Position1D(s) }
      } When {
        positions: TypedPipe[Position1D] =>
          positions.names(Over(First))
      } Then {
        buffer: mutable.Buffer[(Position1D, Long)] =>
          buffer.toList shouldBe data.map { case (s, i) => (Position1D(s), i) }
      }
    }

    "return its First along names" in {
      Given {
        data.map { case (s, i) => Position1D(s) }
      } When {
        positions: TypedPipe[Position1D] =>
          positions.names(Along(First))
      } Then {
        buffer: mutable.Buffer[(Position0D, Long)] =>
          buffer.toList shouldBe List((Position0D(), 0))
      }
    }
  }

  "A Positions of Position2D" should {
    "return its First over names" in {
      Given {
        data.map { case (s, i) => Position2D(s, i) }
      } When {
        positions: TypedPipe[Position2D] =>
          positions.names(Over(First))
      } Then {
        buffer: mutable.Buffer[(Position1D, Long)] =>
          buffer.toList shouldBe data.map { case (s, i) => (Position1D(s), i) }
      }
    }

    "return its Second over names" in {
      Given {
        data.map { case (s, i) => Position2D(s, i) }
      } When {
        positions: TypedPipe[Position2D] =>
          positions.names(Over(Second))
      } Then {
        buffer: mutable.Buffer[(Position1D, Long)] =>
          buffer.toList shouldBe data.map { case (s, i) => (Position1D(i), i) }
      }
    }

    "return its First along names" in {
      Given {
        data.map { case (s, i) => Position2D(s, i) }
      } When {
        positions: TypedPipe[Position2D] =>
          positions.names(Along(First))
      } Then {
        buffer: mutable.Buffer[(Position1D, Long)] =>
          buffer.toList shouldBe data.map { case (s, i) => (Position1D(i), i) }
      }
    }

    "return its Second along names" in {
      Given {
        data.map { case (s, i) => Position2D(s, i) }
      } When {
        positions: TypedPipe[Position2D] =>
          positions.names(Along(Second))
      } Then {
        buffer: mutable.Buffer[(Position1D, Long)] =>
          buffer.toList shouldBe data.map { case (s, i) => (Position1D(s), i) }
      }
    }
  }

  "A Positions of Position3D" should {
    "return its First over names" in {
      Given {
        data.map { case (s, i) => Position3D(s, i, i + 1) }
      } When {
        positions: TypedPipe[Position3D] =>
          positions.names(Over(First))
      } Then {
        buffer: mutable.Buffer[(Position1D, Long)] =>
          buffer.toList shouldBe data.map { case (s, i) => (Position1D(s), i) }
      }
    }

    "return its Second over names" in {
      Given {
        data.map { case (s, i) => Position3D(s, i, i + 1) }
      } When {
        positions: TypedPipe[Position3D] =>
          positions.names(Over(Second))
      } Then {
        buffer: mutable.Buffer[(Position1D, Long)] =>
          buffer.toList shouldBe data.map { case (s, i) => (Position1D(i), i) }
      }
    }

    "return its Third over names" in {
      Given {
        data.map { case (s, i) => Position3D(s, i, i + 1) }
      } When {
        positions: TypedPipe[Position3D] =>
          positions.names(Over(Third))
      } Then {
        buffer: mutable.Buffer[(Position1D, Long)] =>
          buffer.toList shouldBe data.map { case (s, i) => (Position1D(i + 1), i) }
      }
    }

    "return its First along names" in {
      Given {
        data.map { case (s, i) => Position3D(s, i, i + 1) }
      } When {
        positions: TypedPipe[Position3D] =>
          positions.names(Along(First))
      } Then {
        buffer: mutable.Buffer[(Position2D, Long)] =>
          buffer.toList shouldBe data.map { case (s, i) => (Position2D(i, i + 1), i) }
      }
    }

    "return its Second along names" in {
      Given {
        data.map { case (s, i) => Position3D(s, i, i + 1) }
      } When {
        positions: TypedPipe[Position3D] =>
          positions.names(Along(Second))
      } Then {
        buffer: mutable.Buffer[(Position2D, Long)] =>
          buffer.toList shouldBe data.map { case (s, i) => (Position2D(s, i + 1), i) }
      }
    }

    "return its Third along names" in {
      Given {
        data.map { case (s, i) => Position3D(s, i, i + 1) }
      } When {
        positions: TypedPipe[Position3D] =>
          positions.names(Along(Third))
      } Then {
        buffer: mutable.Buffer[(Position2D, Long)] =>
          buffer.toList shouldBe data.map { case (s, i) => (Position2D(s, i), i) }
      }
    }
  }

  "A Positions of Position4D" should {
    "return its First over names" in {
      Given {
        data.map { case (s, i) => Position4D(s, i, i + 1, i + 2) }
      } When {
        positions: TypedPipe[Position4D] =>
          positions.names(Over(First))
      } Then {
        buffer: mutable.Buffer[(Position1D, Long)] =>
          buffer.toList shouldBe data.map { case (s, i) => (Position1D(s), i) }
      }
    }

    "return its Second over names" in {
      Given {
        data.map { case (s, i) => Position4D(s, i, i + 1, i + 2) }
      } When {
        positions: TypedPipe[Position4D] =>
          positions.names(Over(Second))
      } Then {
        buffer: mutable.Buffer[(Position1D, Long)] =>
          buffer.toList shouldBe data.map { case (s, i) => (Position1D(i), i) }
      }
    }

    "return its Third over names" in {
      Given {
        data.map { case (s, i) => Position4D(s, i, i + 1, i + 2) }
      } When {
        positions: TypedPipe[Position4D] =>
          positions.names(Over(Third))
      } Then {
        buffer: mutable.Buffer[(Position1D, Long)] =>
          buffer.toList shouldBe data.map { case (s, i) => (Position1D(i + 1), i) }
      }
    }

    "return its Fourth over names" in {
      Given {
        data.map { case (s, i) => Position4D(s, i, i + 1, i + 2) }
      } When {
        positions: TypedPipe[Position4D] =>
          positions.names(Over(Fourth))
      } Then {
        buffer: mutable.Buffer[(Position1D, Long)] =>
          buffer.toList shouldBe data.map { case (s, i) => (Position1D(i + 2), i) }
      }
    }

    "return its First along names" in {
      Given {
        data.map { case (s, i) => Position4D(s, i, i + 1, i + 2) }
      } When {
        positions: TypedPipe[Position4D] =>
          positions.names(Along(First))
      } Then {
        buffer: mutable.Buffer[(Position3D, Long)] =>
          buffer.toList shouldBe data.map { case (s, i) => (Position3D(i, i + 1, i + 2), i) }
      }
    }

    "return its Second along names" in {
      Given {
        data.map { case (s, i) => Position4D(s, i, i + 1, i + 2) }
      } When {
        positions: TypedPipe[Position4D] =>
          positions.names(Along(Second))
      } Then {
        buffer: mutable.Buffer[(Position3D, Long)] =>
          buffer.toList shouldBe data.map { case (s, i) => (Position3D(s, i + 1, i + 2), i) }
      }
    }

    "return its Third along names" in {
      Given {
        data.map { case (s, i) => Position4D(s, i, i + 1, i + 2) }
      } When {
        positions: TypedPipe[Position4D] =>
          positions.names(Along(Third))
      } Then {
        buffer: mutable.Buffer[(Position3D, Long)] =>
          buffer.toList shouldBe data.map { case (s, i) => (Position3D(s, i, i + 2), i) }
      }
    }

    "return its Fourth along names" in {
      Given {
        data.map { case (s, i) => Position4D(s, i, i + 1, i + 2) }
      } When {
        positions: TypedPipe[Position4D] =>
          positions.names(Along(Fourth))
      } Then {
        buffer: mutable.Buffer[(Position3D, Long)] =>
          buffer.toList shouldBe data.map { case (s, i) => (Position3D(s, i, i + 1), i) }
      }
    }
  }

  "A Positions of Position5D" should {
    "return its First over names" in {
      Given {
        data.map { case (s, i) => Position5D(s, i, i + 1, i + 2, i + 3) }
      } When {
        positions: TypedPipe[Position5D] =>
          positions.names(Over(First))
      } Then {
        buffer: mutable.Buffer[(Position1D, Long)] =>
          buffer.toList shouldBe data.map { case (s, i) => (Position1D(s), i) }
      }
    }

    "return its Second over names" in {
      Given {
        data.map { case (s, i) => Position5D(s, i, i + 1, i + 2, i + 3) }
      } When {
        positions: TypedPipe[Position5D] =>
          positions.names(Over(Second))
      } Then {
        buffer: mutable.Buffer[(Position1D, Long)] =>
          buffer.toList shouldBe data.map { case (s, i) => (Position1D(i), i) }
      }
    }

    "return its Third over names" in {
      Given {
        data.map { case (s, i) => Position5D(s, i, i + 1, i + 2, i + 3) }
      } When {
        positions: TypedPipe[Position5D] =>
          positions.names(Over(Third))
      } Then {
        buffer: mutable.Buffer[(Position1D, Long)] =>
          buffer.toList shouldBe data.map { case (s, i) => (Position1D(i + 1), i) }
      }
    }

    "return its Fourth over names" in {
      Given {
        data.map { case (s, i) => Position5D(s, i, i + 1, i + 2, i + 3) }
      } When {
        positions: TypedPipe[Position5D] =>
          positions.names(Over(Fourth))
      } Then {
        buffer: mutable.Buffer[(Position1D, Long)] =>
          buffer.toList shouldBe data.map { case (s, i) => (Position1D(i + 2), i) }
      }
    }

    "return its Fifth over names" in {
      Given {
        data.map { case (s, i) => Position5D(s, i, i + 1, i + 2, i + 3) }
      } When {
        positions: TypedPipe[Position5D] =>
          positions.names(Over(Fifth))
      } Then {
        buffer: mutable.Buffer[(Position1D, Long)] =>
          buffer.toList shouldBe data.map { case (s, i) => (Position1D(i + 3), i) }
      }
    }

    "return its First along names" in {
      Given {
        data.map { case (s, i) => Position5D(s, i, i + 1, i + 2, i + 3) }
      } When {
        positions: TypedPipe[Position5D] =>
          positions.names(Along(First))
      } Then {
        buffer: mutable.Buffer[(Position4D, Long)] =>
          buffer.toList shouldBe data.map { case (s, i) => (Position4D(i, i + 1, i + 2, i + 3), i) }
      }
    }

    "return its Second along names" in {
      Given {
        data.map { case (s, i) => Position5D(s, i, i + 1, i + 2, i + 3) }
      } When {
        positions: TypedPipe[Position5D] =>
          positions.names(Along(Second))
      } Then {
        buffer: mutable.Buffer[(Position4D, Long)] =>
          buffer.toList shouldBe data.map { case (s, i) => (Position4D(s, i + 1, i + 2, i + 3), i) }
      }
    }

    "return its Third along names" in {
      Given {
        data.map { case (s, i) => Position5D(s, i, i + 1, i + 2, i + 3) }
      } When {
        positions: TypedPipe[Position5D] =>
          positions.names(Along(Third))
      } Then {
        buffer: mutable.Buffer[(Position4D, Long)] =>
          buffer.toList shouldBe data.map { case (s, i) => (Position4D(s, i, i + 2, i + 3), i) }
      }
    }

    "return its Fourth along names" in {
      Given {
        data.map { case (s, i) => Position5D(s, i, i + 1, i + 2, i + 3) }
      } When {
        positions: TypedPipe[Position5D] =>
          positions.names(Along(Fourth))
      } Then {
        buffer: mutable.Buffer[(Position4D, Long)] =>
          buffer.toList shouldBe data.map { case (s, i) => (Position4D(s, i, i + 1, i + 3), i) }
      }
    }

    "return its Fifth along names" in {
      Given {
        data.map { case (s, i) => Position5D(s, i, i + 1, i + 2, i + 3) }
      } When {
        positions: TypedPipe[Position5D] =>
          positions.names(Along(Fifth))
      } Then {
        buffer: mutable.Buffer[(Position4D, Long)] =>
          buffer.toList shouldBe data.map { case (s, i) => (Position4D(s, i, i + 1, i + 2), i) }
      }
    }
  }
}

