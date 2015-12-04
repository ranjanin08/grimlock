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

class TestOperatorString extends TestGrimlock {

  val left = Cell(Position3D("left", "abc", 123), Content(ContinuousSchema(DoubleCodex), 1))
  val right = Cell(Position3D("right", "def", 456), Content(ContinuousSchema(DoubleCodex), 2))

  "A OperatorString" should "extract with all" in {
    Locate.OperatorString[Position3D](Over(First), "%1$s-%2$s", false)(left, right) shouldBe (None)
    Locate.OperatorString[Position3D](Over(First), "%1$s-%2$s", false)(right, right) shouldBe
      (Some(Position3D("right-right", "def", 456)))
  }

  it should "extract with non-all" in {
    Locate.OperatorString[Position3D](Over(First), "%1$s-%2$s", true)(left, right) shouldBe
      (Some(Position3D("left-right", "abc", 123)))
    Locate.OperatorString[Position3D](Over(First), "%1$s-%2$s", true)(right, left) shouldBe
      (Some(Position3D("right-left", "def", 456)))
  }
}

class TestWindowDimension extends TestGrimlock {

  val cell = Cell(Position1D("foo"), Content(ContinuousSchema(DoubleCodex), 1))
  val rem = Position2D("abc", 123)

  "A WindowDimension" should "extract" in {
    Locate.WindowDimension[Position1D, Position2D](First)(cell, rem) shouldBe (Position2D("foo", "abc"))
    Locate.WindowDimension[Position1D, Position2D](Second)(cell, rem) shouldBe (Position2D("foo", 123))
  }
}

class TestWindowString extends TestGrimlock {

  val cell = Cell(Position1D("foo"), Content(ContinuousSchema(DoubleCodex), 1))
  val rem = Position2D("abc", 123)

  "A WindowString" should "extract" in {
    Locate.WindowString[Position1D, Position2D](":")(cell, rem) shouldBe (Position2D("foo", "abc:123"))
  }
}

class TestWindowPairwiseString extends TestGrimlock {

  val cell = Cell(Position1D("foo"), Content(ContinuousSchema(DoubleCodex), 1))
  val curr = Position2D("abc", 123)
  val prev = Position2D("def", 456)

  "A WindowPairwiseString" should "extract" in {
    Locate.WindowPairwiseString[Position1D, Position2D]("g(%2$s, %1$s)", ":")(cell, curr, prev) shouldBe
      (Position2D("foo", "g(abc:123, def:456)"))
  }
}

