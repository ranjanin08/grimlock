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

  val c1f = Cell(Position2D("b", DateValue(dfmt.parse("2001-01-01"), DateCodex())), con1)
  val c2f = Cell(Position2D("a", DateValue(dfmt.parse("2002-01-01"), DateCodex())), con2)

  val r1f = LongValue(1)
  val r2f = LongValue(2)

  val c1s = Cell(Position2D(1, DateValue(dfmt.parse("2001-01-01"), DateCodex())), con1)
  val c2s = Cell(Position2D(2, DateValue(dfmt.parse("2002-01-01"), DateCodex())), con2)

  val r1s = StringValue("b")
  val r2s = StringValue("a")

  val c1t = Cell(Position2D(1, "b"), con1)
  val c2t = Cell(Position2D(2, "a"), con2)

  val r1t = DateValue(dfmt.parse("2001-01-01"), DateCodex())
  val r2t = DateValue(dfmt.parse("2002-01-01"), DateCodex())
}

class TestPreservingMaxPosition extends TestSquashers {

  "A PreservingMaxPosition" should "return the second cell for the first dimension when greater" in {
    val squash = PreservingMaxPosition[Position3D]()

    val t1 = squash.prepare(c1f, r1f)
    val t2 = squash.prepare(c2f, r2f)

    val t = squash.reduce(t1, t2)

    squash.present(t) shouldBe Some(con2)
  }

  it should "return the first cell for the first dimension when greater" in {
    val squash = PreservingMaxPosition[Position3D]()

    val t1 = squash.prepare(c2f, r2f)
    val t2 = squash.prepare(c1f, r1f)

    val t = squash.reduce(t1, t2)

    squash.present(t) shouldBe Some(con2)
  }

  it should "return the first cell for the first dimension when equal" in {
    val squash = PreservingMaxPosition[Position3D]()

    val t1 = squash.prepare(c2f, r2f)
    val t2 = squash.prepare(c2f, r2f)

    val t = squash.reduce(t1, t2)

    squash.present(t) shouldBe Some(con2)
  }

  it should "return the first cell for the second dimension when greater" in {
    val squash = PreservingMaxPosition[Position3D]()

    val t1 = squash.prepare(c1s, r1s)
    val t2 = squash.prepare(c2s, r2s)

    val t = squash.reduce(t1, t2)

    squash.present(t) shouldBe Some(con1)
  }

  it should "return the second cell for the second dimension when greater" in {
    val squash = PreservingMaxPosition[Position3D]()

    val t1 = squash.prepare(c2s, r2s)
    val t2 = squash.prepare(c1s, r1s)

    val t = squash.reduce(t1, t2)

    squash.present(t) shouldBe Some(con1)
  }

  it should "return the first cell for the second dimension when equal" in {
    val squash = PreservingMaxPosition[Position3D]()

    val t1 = squash.prepare(c1s, r1s)
    val t2 = squash.prepare(c1s, r1s)

    val t = squash.reduce(t1, t2)

    squash.present(t) shouldBe Some(con1)
  }

  it should "return the second cell for the third dimension when greater" in {
    val squash = PreservingMaxPosition[Position3D]()

    val t1 = squash.prepare(c1t, r1t)
    val t2 = squash.prepare(c2t, r2t)

    val t = squash.reduce(t1, t2)

    squash.present(t) shouldBe Some(con2)
  }

  it should "return the first cell for the third dimension when greater" in {
    val squash = PreservingMaxPosition[Position3D]()

    val t1 = squash.prepare(c2t, r2t)
    val t2 = squash.prepare(c1t, r1t)

    val t = squash.reduce(t1, t2)

    squash.present(t) shouldBe Some(con2)
  }

  it should "return the first cell for the third dimension when equal" in {
    val squash = PreservingMaxPosition[Position3D]()

    val t1 = squash.prepare(c2t, r2t)
    val t2 = squash.prepare(c2t, r2t)

    val t = squash.reduce(t1, t2)

    squash.present(t) shouldBe Some(con2)
  }
}

class TestPreservingMinPosition extends TestSquashers {

  "A PreservingMinPosition" should "return the first cell for the first dimension when less" in {
    val squash = PreservingMinPosition[Position3D]()

    val t1 = squash.prepare(c1f, r1f)
    val t2 = squash.prepare(c2f, r2f)

    val t = squash.reduce(t1, t2)

    squash.present(t) shouldBe Some(con1)
  }

  it should "return the second cell for the first dimension when less" in {
    val squash = PreservingMinPosition[Position3D]()

    val t1 = squash.prepare(c2f, r2f)
    val t2 = squash.prepare(c1f, r1f)

    val t = squash.reduce(t1, t2)

    squash.present(t) shouldBe Some(con1)
  }

  it should "return the first cell for the first dimension when equal" in {
    val squash = PreservingMinPosition[Position3D]()

    val t1 = squash.prepare(c1f, r1f)
    val t2 = squash.prepare(c1f, r1f)

    val t = squash.reduce(t1, t2)

    squash.present(t) shouldBe Some(con1)
  }

  it should "return the second cell for the second dimension when less" in {
    val squash = PreservingMinPosition[Position3D]()

    val t1 = squash.prepare(c1s, r1s)
    val t2 = squash.prepare(c2s, r2s)

    val t = squash.reduce(t1, t2)

    squash.present(t) shouldBe Some(con2)
  }

  it should "return the first cell for the second dimension when less" in {
    val squash = PreservingMinPosition[Position3D]()

    val t1 = squash.prepare(c2s, r2s)
    val t2 = squash.prepare(c1s, r1s)

    val t = squash.reduce(t1, t2)

    squash.present(t) shouldBe Some(con2)
  }

  it should "return the first cell for the second dimension when equal" in {
    val squash = PreservingMinPosition[Position3D]()

    val t1 = squash.prepare(c2s, r2s)
    val t2 = squash.prepare(c2s, r2s)

    val t = squash.reduce(t1, t2)

    squash.present(t) shouldBe Some(con2)
  }

  it should "return the first cell for the third dimension when less" in {
    val squash = PreservingMinPosition[Position3D]()

    val t1 = squash.prepare(c1t, r1t)
    val t2 = squash.prepare(c2t, r2t)

    val t = squash.reduce(t1, t2)

    squash.present(t) shouldBe Some(con1)
  }

  it should "return the second cell for the third dimension when less" in {
    val squash = PreservingMinPosition[Position3D]()

    val t1 = squash.prepare(c2t, r2t)
    val t2 = squash.prepare(c1t, r1t)

    val t = squash.reduce(t1, t2)

    squash.present(t) shouldBe Some(con1)
  }

  it should "return the first cell for the third dimension when equal" in {
    val squash = PreservingMinPosition[Position3D]()

    val t1 = squash.prepare(c1t, r1t)
    val t2 = squash.prepare(c1t, r1t)

    val t = squash.reduce(t1, t2)

    squash.present(t) shouldBe Some(con1)
  }
}

class TestKeepSlice extends TestSquashers {

  "A KeepSlice" should "return the first cell for the first dimension when equal" in {
    val squash = KeepSlice[Position3D, Long](1)

    val t1 = squash.prepare(c1f, r1f)
    val t2 = squash.prepare(c2f, r2f)

    val t = squash.reduce(t1, t2)

    squash.present(t) shouldBe Some(con1)
  }

  it should "return the second cell for the first dimension when equal" in {
    val squash = KeepSlice[Position3D, Long](2)

    val t1 = squash.prepare(c1f, r1f)
    val t2 = squash.prepare(c2f, r2f)

    val t = squash.reduce(t1, t2)

    squash.present(t) shouldBe Some(con2)
  }

  it should "return the second cell for the first dimension when not equal" in {
    val squash = KeepSlice[Position3D, Long](3)

    val t1 = squash.prepare(c1f, r1f)
    val t2 = squash.prepare(c2f, r2f)

    val t = squash.reduce(t1, t2)

    squash.present(t) shouldBe None
  }

  it should "return the second cell for the second dimension when equal" in {
    val squash = KeepSlice[Position3D, String]("b")

    val t1 = squash.prepare(c1s, r1s)
    val t2 = squash.prepare(c2s, r2s)

    val t = squash.reduce(t1, t2)

    squash.present(t) shouldBe Some(con1)
  }

  it should "return the first cell for the second dimension when equal" in {
    val squash = KeepSlice[Position3D, String]("a")

    val t1 = squash.prepare(c1s, r1s)
    val t2 = squash.prepare(c2s, r2s)

    val t = squash.reduce(t1, t2)

    squash.present(t) shouldBe Some(con2)
  }

  it should "return the second cell for the second dimension when not equal" in {
    val squash = KeepSlice[Position3D, String]("c")

    val t1 = squash.prepare(c1s, r1s)
    val t2 = squash.prepare(c2s, r2s)

    val t = squash.reduce(t1, t2)

    squash.present(t) shouldBe None
  }

  it should "return the first cell for the third dimension when equal" in {
    val squash = KeepSlice[Position3D, DateValue](DateValue(dfmt.parse("2001-01-01"), DateCodex()))

    val t1 = squash.prepare(c1t, r1t)
    val t2 = squash.prepare(c2t, r2t)

    val t = squash.reduce(t1, t2)

    squash.present(t) shouldBe Some(con1)
  }

  it should "return the second cell for the third dimension when equal" in {
    val squash = KeepSlice[Position3D, DateValue](DateValue(dfmt.parse("2002-01-01"), DateCodex()))

    val t1 = squash.prepare(c1t, r1t)
    val t2 = squash.prepare(c2t, r2t)

    val t = squash.reduce(t1, t2)

    squash.present(t) shouldBe Some(con2)
  }

  it should "return the second cell for the third dimension when not equal" in {
    val squash = KeepSlice[Position3D, DateValue](DateValue(dfmt.parse("2003-01-01"), DateCodex()))

    val t1 = squash.prepare(c1t, r1t)
    val t2 = squash.prepare(c2t, r2t)

    val t = squash.reduce(t1, t2)

    squash.present(t) shouldBe None
  }
}

