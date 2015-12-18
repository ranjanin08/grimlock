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

import au.com.cba.omnia.grimlock.library.squash._

trait TestSquashers extends TestGrimlock {

  val dfmt = new java.text.SimpleDateFormat("yyyy-MM-dd")
  val con1 = Content(ContinuousSchema(LongCodex), 123)
  val con2 = Content(ContinuousSchema(LongCodex), 456)
  val cell1 = Cell(Position3D(1, "b", DateValue(dfmt.parse("2001-01-01"), DateCodex())), con1)
  val cell2 = Cell(Position3D(2, "a", DateValue(dfmt.parse("2002-01-01"), DateCodex())), con2)
}

class TestPreservingMaxPosition extends TestSquashers {

  "A PreservingMaxPosition" should "return the second cell for the first dimension when greater" in {
    val squash = PreservingMaxPosition[Position3D]()

    val t1 = squash.prepare(cell1, First)
    val t2 = squash.prepare(cell2, First)

    val t = squash.reduce(t1, t2)

    squash.present(t) shouldBe Some(con2)
  }

  it should "return the first cell for the first dimension when greater" in {
    val squash = PreservingMaxPosition[Position3D]()

    val t1 = squash.prepare(cell2, First)
    val t2 = squash.prepare(cell1, First)

    val t = squash.reduce(t1, t2)

    squash.present(t) shouldBe Some(con2)
  }

  it should "return the first cell for the first dimension when equal" in {
    val squash = PreservingMaxPosition[Position3D]()

    val t1 = squash.prepare(cell2, First)
    val t2 = squash.prepare(cell2, First)

    val t = squash.reduce(t1, t2)

    squash.present(t) shouldBe Some(con2)
  }

  it should "return the first cell for the second dimension when greater" in {
    val squash = PreservingMaxPosition[Position3D]()

    val t1 = squash.prepare(cell1, Second)
    val t2 = squash.prepare(cell2, Second)

    val t = squash.reduce(t1, t2)

    squash.present(t) shouldBe Some(con1)
  }

  it should "return the second cell for the second dimension when greater" in {
    val squash = PreservingMaxPosition[Position3D]()

    val t1 = squash.prepare(cell2, Second)
    val t2 = squash.prepare(cell1, Second)

    val t = squash.reduce(t1, t2)

    squash.present(t) shouldBe Some(con1)
  }

  it should "return the first cell for the second dimension when equal" in {
    val squash = PreservingMaxPosition[Position3D]()

    val t1 = squash.prepare(cell1, Second)
    val t2 = squash.prepare(cell1, Second)

    val t = squash.reduce(t1, t2)

    squash.present(t) shouldBe Some(con1)
  }

  it should "return the second cell for the third dimension when greater" in {
    val squash = PreservingMaxPosition[Position3D]()

    val t1 = squash.prepare(cell1, Third)
    val t2 = squash.prepare(cell2, Third)

    val t = squash.reduce(t1, t2)

    squash.present(t) shouldBe Some(con2)
  }

  it should "return the first cell for the third dimension when greater" in {
    val squash = PreservingMaxPosition[Position3D]()

    val t1 = squash.prepare(cell2, Third)
    val t2 = squash.prepare(cell1, Third)

    val t = squash.reduce(t1, t2)

    squash.present(t) shouldBe Some(con2)
  }

  it should "return the first cell for the third dimension when equal" in {
    val squash = PreservingMaxPosition[Position3D]()

    val t1 = squash.prepare(cell2, Third)
    val t2 = squash.prepare(cell2, Third)

    val t = squash.reduce(t1, t2)

    squash.present(t) shouldBe Some(con2)
  }
}

class TestPreservingMinPosition extends TestSquashers {

  "A PreservingMinPosition" should "return the first cell for the first dimension when less" in {
    val squash = PreservingMinPosition[Position3D]()

    val t1 = squash.prepare(cell1, First)
    val t2 = squash.prepare(cell2, First)

    val t = squash.reduce(t1, t2)

    squash.present(t) shouldBe Some(con1)
  }

  it should "return the second cell for the first dimension when less" in {
    val squash = PreservingMinPosition[Position3D]()

    val t1 = squash.prepare(cell2, First)
    val t2 = squash.prepare(cell1, First)

    val t = squash.reduce(t1, t2)

    squash.present(t) shouldBe Some(con1)
  }

  it should "return the first cell for the first dimension when equal" in {
    val squash = PreservingMinPosition[Position3D]()

    val t1 = squash.prepare(cell1, First)
    val t2 = squash.prepare(cell1, First)

    val t = squash.reduce(t1, t2)

    squash.present(t) shouldBe Some(con1)
  }

  it should "return the second cell for the second dimension when less" in {
    val squash = PreservingMinPosition[Position3D]()

    val t1 = squash.prepare(cell1, Second)
    val t2 = squash.prepare(cell2, Second)

    val t = squash.reduce(t1, t2)

    squash.present(t) shouldBe Some(con2)
  }

  it should "return the first cell for the second dimension when less" in {
    val squash = PreservingMinPosition[Position3D]()

    val t1 = squash.prepare(cell2, Second)
    val t2 = squash.prepare(cell1, Second)

    val t = squash.reduce(t1, t2)

    squash.present(t) shouldBe Some(con2)
  }

  it should "return the first cell for the second dimension when equal" in {
    val squash = PreservingMinPosition[Position3D]()

    val t1 = squash.prepare(cell2, Second)
    val t2 = squash.prepare(cell2, Second)

    val t = squash.reduce(t1, t2)

    squash.present(t) shouldBe Some(con2)
  }

  it should "return the first cell for the third dimension when less" in {
    val squash = PreservingMinPosition[Position3D]()

    val t1 = squash.prepare(cell1, Third)
    val t2 = squash.prepare(cell2, Third)

    val t = squash.reduce(t1, t2)

    squash.present(t) shouldBe Some(con1)
  }

  it should "return the second cell for the third dimension when less" in {
    val squash = PreservingMinPosition[Position3D]()

    val t1 = squash.prepare(cell2, Third)
    val t2 = squash.prepare(cell1, Third)

    val t = squash.reduce(t1, t2)

    squash.present(t) shouldBe Some(con1)
  }

  it should "return the first cell for the third dimension when equal" in {
    val squash = PreservingMinPosition[Position3D]()

    val t1 = squash.prepare(cell1, Third)
    val t2 = squash.prepare(cell1, Third)

    val t = squash.reduce(t1, t2)

    squash.present(t) shouldBe Some(con1)
  }
}

class TestKeepSlice extends TestSquashers {

  "A KeepSlice" should "return the first cell for the first dimension when equal" in {
    val squash = KeepSlice[Position3D](1)

    val t1 = squash.prepare(cell1, First)
    val t2 = squash.prepare(cell2, First)

    val t = squash.reduce(t1, t2)

    squash.present(t) shouldBe Some(con1)
  }

  it should "return the second cell for the first dimension when equal" in {
    val squash = KeepSlice[Position3D](2)

    val t1 = squash.prepare(cell1, First)
    val t2 = squash.prepare(cell2, First)

    val t = squash.reduce(t1, t2)

    squash.present(t) shouldBe Some(con2)
  }

  it should "return the second cell for the first dimension when not equal" in {
    val squash = KeepSlice[Position3D](3)

    val t1 = squash.prepare(cell1, First)
    val t2 = squash.prepare(cell2, First)

    val t = squash.reduce(t1, t2)

    squash.present(t) shouldBe None
  }

  it should "return the second cell for the second dimension when equal" in {
    val squash = KeepSlice[Position3D]("b")

    val t1 = squash.prepare(cell1, Second)
    val t2 = squash.prepare(cell2, Second)

    val t = squash.reduce(t1, t2)

    squash.present(t) shouldBe Some(con1)
  }

  it should "return the first cell for the second dimension when equal" in {
    val squash = KeepSlice[Position3D]("a")

    val t1 = squash.prepare(cell1, Second)
    val t2 = squash.prepare(cell2, Second)

    val t = squash.reduce(t1, t2)

    squash.present(t) shouldBe Some(con2)
  }

  it should "return the second cell for the second dimension when not equal" in {
    val squash = KeepSlice[Position3D]("c")

    val t1 = squash.prepare(cell1, Second)
    val t2 = squash.prepare(cell2, Second)

    val t = squash.reduce(t1, t2)

    squash.present(t) shouldBe None
  }

  it should "return the first cell for the third dimension when equal" in {
    val squash = KeepSlice[Position3D](DateValue(dfmt.parse("2001-01-01"), DateCodex()))

    val t1 = squash.prepare(cell1, Third)
    val t2 = squash.prepare(cell2, Third)

    val t = squash.reduce(t1, t2)

    squash.present(t) shouldBe Some(con1)
  }

  it should "return the second cell for the third dimension when equal" in {
    val squash = KeepSlice[Position3D](DateValue(dfmt.parse("2002-01-01"), DateCodex()))

    val t1 = squash.prepare(cell1, Third)
    val t2 = squash.prepare(cell2, Third)

    val t = squash.reduce(t1, t2)

    squash.present(t) shouldBe Some(con2)
  }

  it should "return the second cell for the third dimension when not equal" in {
    val squash = KeepSlice[Position3D](DateValue(dfmt.parse("2003-01-01"), DateCodex()))

    val t1 = squash.prepare(cell1, Third)
    val t2 = squash.prepare(cell2, Third)

    val t = squash.reduce(t1, t2)

    squash.present(t) shouldBe None
  }
}

