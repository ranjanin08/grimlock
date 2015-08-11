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
import au.com.cba.omnia.grimlock.framework.squash._

import au.com.cba.omnia.grimlock.library.squash._

trait TestSquashers extends TestGrimlock {

  val dfmt = new java.text.SimpleDateFormat("yyyy-MM-dd")
  val con = Content(ContinuousSchema(LongCodex), 123)
  val cell1 = Cell(Position3D(1, "b", DateValue(dfmt.parse("2001-01-01"), DateCodex())), con)
  val cell2 = Cell(Position3D(2, "a", DateValue(dfmt.parse("2002-01-01"), DateCodex())), con)
}

class TestPreservingMaxPosition extends TestSquashers {

  "A PreservingMaxPosition" should "return the second cell for the first dimension when greater" in {
    PreservingMaxPosition().reduce(First, cell1, cell2) shouldBe cell2
  }

  it should "return the first cell for the first dimension when greater" in {
    PreservingMaxPosition().reduce(First, cell2, cell1) shouldBe cell2
  }

  it should "return the first cell for the first dimension when equal" in {
    PreservingMaxPosition().reduce(First, cell2, cell2) shouldBe cell2
  }

  it should "return the first cell for the second dimension when greater" in {
    PreservingMaxPosition().reduce(Second, cell1, cell2) shouldBe cell1
  }

  it should "return the second cell for the second dimension when greater" in {
    PreservingMaxPosition().reduce(Second, cell2, cell1) shouldBe cell1
  }

  it should "return the first cell for the second dimension when equal" in {
    PreservingMaxPosition().reduce(Second, cell1, cell1) shouldBe cell1
  }

  it should "return the second cell for the third dimension when greater" in {
    PreservingMaxPosition().reduce(Third, cell1, cell2) shouldBe cell2
  }

  it should "return the first cell for the third dimension when greater" in {
    PreservingMaxPosition().reduce(Third, cell2, cell1) shouldBe cell2
  }

  it should "return the first cell for the third dimension when equal" in {
    PreservingMaxPosition().reduce(Third, cell2, cell2) shouldBe cell2
  }
}

class TestPreservingMinPosition extends TestSquashers {

  "A PreservingMinPosition" should "return the first cell for the first dimension when less" in {
    PreservingMinPosition().reduce(First, cell1, cell2) shouldBe cell1
  }

  it should "return the second cell for the first dimension when less" in {
    PreservingMinPosition().reduce(First, cell2, cell1) shouldBe cell1
  }

  it should "return the first cell for the first dimension when equal" in {
    PreservingMinPosition().reduce(First, cell1, cell1) shouldBe cell1
  }

  it should "return the second cell for the second dimension when less" in {
    PreservingMinPosition().reduce(Second, cell1, cell2) shouldBe cell2
  }

  it should "return the first cell for the second dimension when less" in {
    PreservingMinPosition().reduce(Second, cell2, cell1) shouldBe cell2
  }

  it should "return the first cell for the second dimension when equal" in {
    PreservingMinPosition().reduce(Second, cell2, cell2) shouldBe cell2
  }

  it should "return the first cell for the third dimension when less" in {
    PreservingMinPosition().reduce(Third, cell1, cell2) shouldBe cell1
  }

  it should "return the second cell for the third dimension when less" in {
    PreservingMinPosition().reduce(Third, cell2, cell1) shouldBe cell1
  }

  it should "return the first cell for the third dimension when equal" in {
    PreservingMinPosition().reduce(Third, cell1, cell1) shouldBe cell1
  }
}

class TestKeepSlice extends TestSquashers {

  "A KeepSlice" should "return the first cell for the first dimension when equal" in {
    KeepSlice(1).reduce(First, cell1, cell2) shouldBe cell1
  }

  it should "return the second cell for the first dimension when equal" in {
    KeepSlice(2).reduce(First, cell1, cell2) shouldBe cell2
  }

  it should "return the second cell for the first dimension when not equal" in {
    KeepSlice(3).reduce(First, cell1, cell2) shouldBe cell2
  }

  it should "return the second cell for the second dimension when equal" in {
    KeepSlice("b").reduce(Second, cell1, cell2) shouldBe cell1
  }

  it should "return the first cell for the second dimension when equal" in {
    KeepSlice("a").reduce(Second, cell1, cell2) shouldBe cell2
  }

  it should "return the second cell for the second dimension when not equal" in {
    KeepSlice("c").reduce(Second, cell1, cell2) shouldBe cell2
  }

  it should "return the first cell for the third dimension when equal" in {
    KeepSlice(DateValue(dfmt.parse("2001-01-01"), DateCodex())).reduce(Third, cell1, cell2) shouldBe cell1
  }

  it should "return the second cell for the third dimension when equal" in {
    KeepSlice(DateValue(dfmt.parse("2002-01-01"), DateCodex())).reduce(Third, cell1, cell2) shouldBe cell2
  }

  it should "return the second cell for the third dimension when not equal" in {
    KeepSlice(DateValue(dfmt.parse("2003-01-01"), DateCodex())).reduce(Third, cell1, cell2) shouldBe cell2
  }
}

