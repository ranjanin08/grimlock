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

class TestRenameDimension extends TestGrimlock {

  val cell = Cell(Position1D("foo"), Content(ContinuousSchema(DoubleCodex), 1))

  "A RenameDimension" should "extract" in {
    val loc = Locate.RenameDimension[Position1D](First, "%1$s.postfix")
    loc(cell) shouldBe (Some(Position1D("foo.postfix")))
  }
}

class TestRenameDimensionWithContent extends TestGrimlock {

  val cell = Cell(Position1D("foo"), Content(ContinuousSchema(DoubleCodex), 1))

  "A RenameDimensionWithContent" should "extract" in {
    val loc = Locate.RenameDimensionWithContent[Position1D](First, "%2$s<-%1$s")
    loc(cell) shouldBe (Some(Position1D("1.0<-foo")))
  }
}

class TestAppendValue extends TestGrimlock {

  val cell = Cell(Position1D("foo"), Content(ContinuousSchema(DoubleCodex), 1))

  "A AppendValue" should "extract" in {
    Locate.AppendValue[Position1D](42)(cell) shouldBe (Some(Position2D("foo", 42)))
  }
}

class TestPrependPairwiseSelectedStringToRemainder extends TestGrimlock {

  val left = Cell(Position3D("left", "abc", 123), Content(ContinuousSchema(DoubleCodex), 1))
  val right = Cell(Position3D("right", "def", 456), Content(ContinuousSchema(DoubleCodex), 2))

  "A PrependPairwiseSelectedToRemainder" should "extract with all" in {
    Locate.PrependPairwiseSelectedStringToRemainder[Position3D](Over(First), "%1$s-%2$s", false)(left, right) shouldBe
      (None)
    Locate.PrependPairwiseSelectedStringToRemainder[Position3D](Over(First), "%1$s-%2$s", false)(right, right) shouldBe
      (Some(Position3D("right-right", "def", 456)))
  }

  it should "extract with non-all" in {
    Locate.PrependPairwiseSelectedStringToRemainder[Position3D](Over(First), "%1$s-%2$s", true)(left, right) shouldBe
      (Some(Position3D("left-right", "abc", 123)))
    Locate.PrependPairwiseSelectedStringToRemainder[Position3D](Over(First), "%1$s-%2$s", true)(right, left) shouldBe
      (Some(Position3D("right-left", "def", 456)))
  }
}

class TestAppendRemainderDimension extends TestGrimlock {

  val sel = Position1D("foo")
  val rem = Position2D("abc", 123)

  "A AppendRemainderDimension" should "extract" in {
    val loc1 = Locate.AppendRemainderDimension[Position1D, Position2D](First)
    loc1(sel, rem) shouldBe (Some(Position2D("foo", "abc")))
    val loc2 = Locate.AppendRemainderDimension[Position1D, Position2D](Second)
    loc2(sel, rem) shouldBe (Some(Position2D("foo", 123)))
  }
}

class TestAppendRemainderString extends TestGrimlock {

  val sel = Position1D("foo")
  val rem = Position2D("abc", 123)

  "A AppendRemainderString" should "extract" in {
    Locate.AppendRemainderString[Position1D, Position2D](":")(sel, rem) shouldBe (Some(Position2D("foo", "abc:123")))
  }
}

class TestAppendPairwiseString extends TestGrimlock {

  val sel = Position1D("foo")
  val curr = Position2D("abc", 123)
  val prev = Position2D("def", 456)

  "A AppendPairwiseString" should "extract" in {
    Locate.AppendPairwiseString[Position1D, Position2D]("g(%2$s, %1$s)", ":")(sel, curr, prev) shouldBe
      (Some(Position2D("foo", "g(abc:123, def:456)")))
  }
}

class TestAppendDoubleString extends TestGrimlock {

  val pos = Position1D("foo")

  "A AppendDoubleString" should "extract" in {
    Locate.AppendDoubleString[Position1D]("value=%1$s")(pos, 42) shouldBe (Some(Position2D("foo", "value=42.0")))
  }
}

class TestAppendContentString extends TestGrimlock {

  val pos = Position1D("foo")
  val con = Content(DiscreteSchema(LongCodex), 42)

  "A AppendContentString" should "extract" in {
    Locate.AppendContentString[Position1D]()(pos, con) shouldBe (Some(Position2D("foo", "42")))
  }
}

class TestAppendDimensionAndContentString extends TestGrimlock {

  val pos = Position1D("foo")
  val con = Content(DiscreteSchema(LongCodex), 42)

  "A AppendDimensionAndContentString" should "extract" in {
    val loc = Locate.AppendDimensionAndContentString[Position1D](First, "%2$s!=%1$s")
    loc(pos, con) shouldBe (Some(Position2D("foo", "42!=foo")))
  }
}

