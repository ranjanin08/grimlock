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
import au.com.cba.omnia.grimlock.framework.aggregate._
import au.com.cba.omnia.grimlock.framework.content._
import au.com.cba.omnia.grimlock.framework.content.metadata._
import au.com.cba.omnia.grimlock.framework.encoding._
import au.com.cba.omnia.grimlock.framework.position._
import au.com.cba.omnia.grimlock.framework.utility._

import au.com.cba.omnia.grimlock.library.aggregate._

import scala.collection.immutable.TreeMap

trait TestAggregators extends TestGrimlock {
  def getLongContent(value: Long): Content = Content(DiscreteSchema(LongCodex), value)
  def getDoubleContent(value: Double): Content = Content(ContinuousSchema(DoubleCodex), value)
  def getStringContent(value: String): Content = Content(NominalSchema(StringCodex), value)
}

class TestCount extends TestAggregators {

  val cell1 = Cell(Position2D("foo", "one"), getDoubleContent(1))
  val cell2 = Cell(Position2D("foo", "two"), getDoubleContent(2))

  "A Count" should "prepare, reduce and present" in {
    val obj = Count[Position2D, Position1D]()

    val t1 = obj.prepare(cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(cell2)
    t2 shouldBe 1

    val r = obj.reduce(t1, t2)
    r shouldBe 2

    val c = obj.present(Position1D("foo"), r)
    c shouldBe Some(Cell(Position1D("foo"), getLongContent(2)))
  }

  it should "prepare, reduce and present expanded" in {
    val obj = Count[Position2D, Position1D]()
      .andThenExpand(_.position.append("count"))

    val t1 = obj.prepare(cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(cell2)
    t2 shouldBe 1

    val r = obj.reduce(t1, t2)
    r shouldBe 2

    val c = obj.present(Position1D("foo"), r)
    c shouldBe Some(Cell(Position2D("foo", "count"), getLongContent(2)))
  }
}

class TestMean extends TestAggregators {

  val cell1 = Cell(Position2D("foo", "one"), getDoubleContent(1))
  val cell2 = Cell(Position2D("foo", "two"), getDoubleContent(2))
  val cell3 = Cell(Position2D("foo", "bar"), getStringContent("bar"))

  "A Mean" should "prepare, reduce and present" in {
    val obj = Mean[Position2D, Position1D]()

    val t1 = obj.prepare(cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val r = obj.reduce(t1, t2)
    r shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.present(Position1D("foo"), r)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(1.5)))
  }

  it should "prepare, reduce and present with strict and nan" in {
    val obj = Mean[Position2D, Position1D](true, true)

    val t1 = obj.prepare(cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position1D("foo"), r2)
    c.get.position shouldBe Position1D("foo")
    c.get.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Some(0)
  }

  it should "prepare, reduce and present with strict and non-nan" in {
    val obj = Mean[Position2D, Position1D](true, false)

    val t1 = obj.prepare(cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position1D("foo"), r2)
    c shouldBe None
  }

  it should "prepare, reduce and present with non-strict and nan" in {
    val obj = Mean[Position2D, Position1D](false, true)

    val t1 = obj.prepare(cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.present(Position1D("foo"), r2)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(1.5)))
  }

  it should "prepare, reduce and present with non-strict and non-nan" in {
    val obj = Mean[Position2D, Position1D](false, false)

    val t1 = obj.prepare(cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.present(Position1D("foo"), r2)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(1.5)))
  }

  it should "prepare, reduce and present expanded" in {
    val obj = Mean[Position2D, Position1D]()
      .andThenExpand(_.position.append("mean"))

    val t1 = obj.prepare(cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val r = obj.reduce(t1, t2)
    r shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.present(Position1D("foo"), r)
    c shouldBe Some(Cell(Position2D("foo", "mean"), getDoubleContent(1.5)))
  }

  it should "prepare, reduce and present expanded with strict and nan" in {
    val obj = Mean[Position2D, Position1D](true, true)
      .andThenExpand(_.position.append("mean"))

    val t1 = obj.prepare(cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position1D("foo"), r2)
    c.get.position shouldBe Position2D("foo", "mean")
    c.get.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Some(0)
  }

  it should "prepare, reduce and present expanded with strict and non-nan" in {
    val obj = Mean[Position2D, Position1D](true, false)
      .andThenExpand(_.position.append("mean"))

    val t1 = obj.prepare(cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position1D("foo"), r2)
    c shouldBe None
  }

  it should "prepare, reduce and present expanded with non-strict and nan" in {
    val obj = Mean[Position2D, Position1D](false, true)
      .andThenExpand(_.position.append("mean"))

    val t1 = obj.prepare(cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.present(Position1D("foo"), r2)
    c shouldBe Some(Cell(Position2D("foo", "mean"), getDoubleContent(1.5)))
  }

  it should "prepare, reduce and present expanded with non-strict and non-nan" in {
    val obj = Mean[Position2D, Position1D](false, false)
      .andThenExpand(_.position.append("mean"))

    val t1 = obj.prepare(cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.present(Position1D("foo"), r2)
    c shouldBe Some(Cell(Position2D("foo", "mean"), getDoubleContent(1.5)))
  }
}

class TestStandardDeviation extends TestAggregators {

  val cell1 = Cell(Position2D("foo", "one"), getDoubleContent(1))
  val cell2 = Cell(Position2D("foo", "two"), getDoubleContent(2))
  val cell3 = Cell(Position2D("foo", "bar"), getStringContent("bar"))

  "A StandardDeviation" should "prepare, reduce and present" in {
    val obj = StandardDeviation[Position2D, Position1D]()

    val t1 = obj.prepare(cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val r = obj.reduce(t1, t2)
    r shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.present(Position1D("foo"), r)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(0.5)))
  }

  it should "prepare, reduce and present with strict and nan" in {
    val obj = StandardDeviation[Position2D, Position1D](true, true)

    val t1 = obj.prepare(cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position1D("foo"), r2)
    c.get.position shouldBe Position1D("foo")
    c.get.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Some(0)
  }

  it should "prepare, reduce and present with strict and non-nan" in {
    val obj = StandardDeviation[Position2D, Position1D](true, false)

    val t1 = obj.prepare(cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position1D("foo"), r2)
    c shouldBe None
  }

  it should "prepare, reduce and present with non-strict and nan" in {
    val obj = StandardDeviation[Position2D, Position1D](false, true)

    val t1 = obj.prepare(cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.present(Position1D("foo"), r2)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(0.5)))
  }

  it should "prepare, reduce and present with non-strict and non-nan" in {
    val obj = StandardDeviation[Position2D, Position1D](false, false)

    val t1 = obj.prepare(cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.present(Position1D("foo"), r2)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(0.5)))
  }

  it should "prepare, reduce and present expanded" in {
    val obj = StandardDeviation[Position2D, Position1D]()
      .andThenExpand(_.position.append("sd"))

    val t1 = obj.prepare(cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val r = obj.reduce(t1, t2)
    r shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.present(Position1D("foo"), r)
    c shouldBe Some(Cell(Position2D("foo", "sd"), getDoubleContent(0.5)))
  }

  it should "prepare, reduce and present expanded with strict and nan" in {
    val obj = StandardDeviation[Position2D, Position1D](true, true)
      .andThenExpand(_.position.append("sd"))

    val t1 = obj.prepare(cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position1D("foo"), r2)
    c.get.position shouldBe Position2D("foo", "sd")
    c.get.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Some(0)
  }

  it should "prepare, reduce and present expanded with strict and non-nan" in {
    val obj = StandardDeviation[Position2D, Position1D](true, false)
      .andThenExpand(_.position.append("sd"))

    val t1 = obj.prepare(cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position1D("foo"), r2)
    c shouldBe None
  }

  it should "prepare, reduce and present expanded with non-strict and nan" in {
    val obj = StandardDeviation[Position2D, Position1D](false, true)
      .andThenExpand(_.position.append("sd"))

    val t1 = obj.prepare(cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.present(Position1D("foo"), r2)
    c shouldBe Some(Cell(Position2D("foo", "sd"), getDoubleContent(0.5)))
  }

  it should "prepare, reduce and present expanded with non-strict and non-nan" in {
    val obj = StandardDeviation[Position2D, Position1D](false, false)
      .andThenExpand(_.position.append("sd"))

    val t1 = obj.prepare(cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.present(Position1D("foo"), r2)
    c shouldBe Some(Cell(Position2D("foo", "sd"), getDoubleContent(0.5)))
  }
}

class TestSkewness extends TestAggregators {

  val cell1 = Cell(Position2D("foo", "one"), getDoubleContent(1))
  val cell2 = Cell(Position2D("foo", "two"), getDoubleContent(2))
  val cell3 = Cell(Position2D("foo", "bar"), getStringContent("bar"))

  "A Skewness" should "prepare, reduce and present" in {
    val obj = Skewness[Position2D, Position1D]()

    val t1 = obj.prepare(cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val r = obj.reduce(t1, t2)
    r shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.present(Position1D("foo"), r)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(0)))
  }

  it should "prepare, reduce and present with strict and nan" in {
    val obj = Skewness[Position2D, Position1D](true, true)

    val t1 = obj.prepare(cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position1D("foo"), r2)
    c.get.position shouldBe Position1D("foo")
    c.get.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Some(0)
  }

  it should "prepare, reduce and present with strict and non-nan" in {
    val obj = Skewness[Position2D, Position1D](true, false)

    val t1 = obj.prepare(cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position1D("foo"), r2)
    c shouldBe None
  }

  it should "prepare, reduce and present with non-strict and nan" in {
    val obj = Skewness[Position2D, Position1D](false, true)

    val t1 = obj.prepare(cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.present(Position1D("foo"), r2)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(0)))
  }

  it should "prepare, reduce and present with non-strict and non-nan" in {
    val obj = Skewness[Position2D, Position1D](false, false)

    val t1 = obj.prepare(cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.present(Position1D("foo"), r2)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(0)))
  }

  it should "prepare, reduce and present expanded" in {
    val obj = Skewness[Position2D, Position1D]()
      .andThenExpand(_.position.append("skewness"))

    val t1 = obj.prepare(cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val r = obj.reduce(t1, t2)
    r shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.present(Position1D("foo"), r)
    c shouldBe Some(Cell(Position2D("foo", "skewness"), getDoubleContent(0)))
  }

  it should "prepare, reduce and present expanded with strict and nan" in {
    val obj = Skewness[Position2D, Position1D](true, true)
      .andThenExpand(_.position.append("skewness"))

    val t1 = obj.prepare(cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position1D("foo"), r2)
    c.get.position shouldBe Position2D("foo", "skewness")
    c.get.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Some(0)
  }

  it should "prepare, reduce and present expanded with strict and non-nan" in {
    val obj = Skewness[Position2D, Position1D](true, false)
      .andThenExpand(_.position.append("skewness"))

    val t1 = obj.prepare(cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position1D("foo"), r2)
    c shouldBe None
  }

  it should "prepare, reduce and present expanded with non-strict and nan" in {
    val obj = Skewness[Position2D, Position1D](false, true)
      .andThenExpand(_.position.append("skewness"))

    val t1 = obj.prepare(cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.present(Position1D("foo"), r2)
    c shouldBe Some(Cell(Position2D("foo", "skewness"), getDoubleContent(0)))
  }

  it should "prepare, reduce and present expanded with non-strict and non-nan" in {
    val obj = Skewness[Position2D, Position1D](false, false)
      .andThenExpand(_.position.append("skewness"))

    val t1 = obj.prepare(cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.present(Position1D("foo"), r2)
    c shouldBe Some(Cell(Position2D("foo", "skewness"), getDoubleContent(0)))
  }
}

class TestKurtosis extends TestAggregators {

  val cell1 = Cell(Position2D("foo", "one"), getDoubleContent(1))
  val cell2 = Cell(Position2D("foo", "two"), getDoubleContent(2))
  val cell3 = Cell(Position2D("foo", "bar"), getStringContent("bar"))

  "A Kurtosis" should "prepare, reduce and present" in {
    val obj = Kurtosis[Position2D, Position1D]()

    val t1 = obj.prepare(cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val r = obj.reduce(t1, t2)
    r shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.present(Position1D("foo"), r)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(-2)))
  }

  it should "prepare, reduce and present with strict and nan" in {
    val obj = Kurtosis[Position2D, Position1D](true, true)

    val t1 = obj.prepare(cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position1D("foo"), r2)
    c.get.position shouldBe Position1D("foo")
    c.get.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Some(0)
  }

  it should "prepare, reduce and present with strict and non-nan" in {
    val obj = Kurtosis[Position2D, Position1D](true, false)

    val t1 = obj.prepare(cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position1D("foo"), r2)
    c shouldBe None
  }

  it should "prepare, reduce and present with non-strict and nan" in {
    val obj = Kurtosis[Position2D, Position1D](false, true)

    val t1 = obj.prepare(cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.present(Position1D("foo"), r2)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(-2)))
  }

  it should "prepare, reduce and present with non-strict and non-nan" in {
    val obj = Kurtosis[Position2D, Position1D](false, false)

    val t1 = obj.prepare(cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.present(Position1D("foo"), r2)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(-2)))
  }

  it should "prepare, reduce and present expanded" in {
    val obj = Kurtosis[Position2D, Position1D]()
      .andThenExpand(_.position.append("kurtosis"))

    val t1 = obj.prepare(cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val r = obj.reduce(t1, t2)
    r shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.present(Position1D("foo"), r)
    c shouldBe Some(Cell(Position2D("foo", "kurtosis"), getDoubleContent(-2)))
  }

  it should "prepare, reduce and present expanded with strict and nan" in {
    val obj = Kurtosis[Position2D, Position1D](true, true)
      .andThenExpand(_.position.append("kurtosis"))

    val t1 = obj.prepare(cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position1D("foo"), r2)
    c.get.position shouldBe Position2D("foo", "kurtosis")
    c.get.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Some(0)
  }

  it should "prepare, reduce and present expanded with strict and non-nan" in {
    val obj = Kurtosis[Position2D, Position1D](true, false)
      .andThenExpand(_.position.append("kurtosis"))

    val t1 = obj.prepare(cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position1D("foo"), r2)
    c shouldBe None
  }

  it should "prepare, reduce and present expanded with non-strict and nan" in {
    val obj = Kurtosis[Position2D, Position1D](false, true)
      .andThenExpand(_.position.append("kurtosis"))

    val t1 = obj.prepare(cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.present(Position1D("foo"), r2)
    c shouldBe Some(Cell(Position2D("foo", "kurtosis"), getDoubleContent(-2)))
  }

  it should "prepare, reduce and present expanded with non-strict and non-nan" in {
    val obj = Kurtosis[Position2D, Position1D](false, false)
      .andThenExpand(_.position.append("kurtosis"))

    val t1 = obj.prepare(cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.present(Position1D("foo"), r2)
    c shouldBe Some(Cell(Position2D("foo", "kurtosis"), getDoubleContent(-2)))
  }
}

class TestMin extends TestAggregators {

  val cell1 = Cell(Position2D("foo", "one"), getDoubleContent(1))
  val cell2 = Cell(Position2D("foo", "two"), getDoubleContent(2))
  val cell3 = Cell(Position2D("foo", "bar"), getStringContent("bar"))

  "A Min" should "prepare, reduce and present" in {
    val obj = Min[Position2D, Position1D]()

    val t1 = obj.prepare(cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(cell2)
    t2 shouldBe 2

    val r = obj.reduce(t1, t2)
    r shouldBe 1

    val c = obj.present(Position1D("foo"), r)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(1)))
  }

  it should "prepare, reduce and present with strict and nan" in {
    val obj = Min[Position2D, Position1D](true, true)

    val t1 = obj.prepare(cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(cell2)
    t2 shouldBe 2

    val t3 = obj.prepare(cell3)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 1

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val c = obj.present(Position1D("foo"), r2)
    c.get.position shouldBe Position1D("foo")
    c.get.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Some(0)
  }

  it should "prepare, reduce and present with strict and non-nan" in {
    val obj = Min[Position2D, Position1D](true, false)

    val t1 = obj.prepare(cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(cell2)
    t2 shouldBe 2

    val t3 = obj.prepare(cell3)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 1

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val c = obj.present(Position1D("foo"), r2)
    c shouldBe None
  }

  it should "prepare, reduce and present with non-strict and nan" in {
    val obj = Min[Position2D, Position1D](false, true)

    val t1 = obj.prepare(cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(cell2)
    t2 shouldBe 2

    val t3 = obj.prepare(cell3)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 1

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe 1

    val c = obj.present(Position1D("foo"), r2)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(1)))
  }

  it should "prepare, reduce and present with non-strict and non-nan" in {
    val obj = Min[Position2D, Position1D](false, false)

    val t1 = obj.prepare(cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(cell2)
    t2 shouldBe 2

    val t3 = obj.prepare(cell3)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 1

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe 1

    val c = obj.present(Position1D("foo"), r2)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(1)))
  }

  it should "prepare, reduce and present expanded" in {
    val obj = Min[Position2D, Position1D]()
      .andThenExpand(_.position.append("min"))

    val t1 = obj.prepare(cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(cell2)
    t2 shouldBe 2

    val r = obj.reduce(t1, t2)
    r shouldBe 1

    val c = obj.present(Position1D("foo"), r)
    c shouldBe Some(Cell(Position2D("foo", "min"), getDoubleContent(1)))
  }

  it should "prepare, reduce and present expanded with strict and nan" in {
    val obj = Min[Position2D, Position1D](true, true)
      .andThenExpand(_.position.append("min"))

    val t1 = obj.prepare(cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(cell2)
    t2 shouldBe 2

    val t3 = obj.prepare(cell3)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 1

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val c = obj.present(Position1D("foo"), r2)
    c.get.position shouldBe Position2D("foo", "min")
    c.get.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Some(0)
  }

  it should "prepare, reduce and present expanded with strict and non-nan" in {
    val obj = Min[Position2D, Position1D](true, false)
      .andThenExpand(_.position.append("min"))

    val t1 = obj.prepare(cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(cell2)
    t2 shouldBe 2

    val t3 = obj.prepare(cell3)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 1

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val c = obj.present(Position1D("foo"), r2)
    c shouldBe None
  }

  it should "prepare, reduce and present expanded with non-strict and nan" in {
    val obj = Min[Position2D, Position1D](false, true)
      .andThenExpand(_.position.append("min"))

    val t1 = obj.prepare(cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(cell2)
    t2 shouldBe 2

    val t3 = obj.prepare(cell3)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 1

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe 1

    val c = obj.present(Position1D("foo"), r2)
    c shouldBe Some(Cell(Position2D("foo", "min"), getDoubleContent(1)))
  }

  it should "prepare, reduce and present expanded with non-strict and non-nan" in {
    val obj = Min[Position2D, Position1D](false, false)
      .andThenExpand(_.position.append("min"))

    val t1 = obj.prepare(cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(cell2)
    t2 shouldBe 2

    val t3 = obj.prepare(cell3)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 1

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe 1

    val c = obj.present(Position1D("foo"), r2)
    c shouldBe Some(Cell(Position2D("foo", "min"), getDoubleContent(1)))
  }
}

class TestMax extends TestAggregators {

  val cell1 = Cell(Position2D("foo", "one"), getDoubleContent(1))
  val cell2 = Cell(Position2D("foo", "two"), getDoubleContent(2))
  val cell3 = Cell(Position2D("foo", "bar"), getStringContent("bar"))

  "A Max" should "prepare, reduce and present" in {
    val obj = Max[Position2D, Position1D]()

    val t1 = obj.prepare(cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(cell2)
    t2 shouldBe 2

    val r = obj.reduce(t1, t2)
    r shouldBe 2

    val c = obj.present(Position1D("foo"), r)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(2)))
  }

  it should "prepare, reduce and present with strict and nan" in {
    val obj = Max[Position2D, Position1D](true, true)

    val t1 = obj.prepare(cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(cell2)
    t2 shouldBe 2

    val t3 = obj.prepare(cell3)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 2

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val c = obj.present(Position1D("foo"), r2)
    c.get.position shouldBe Position1D("foo")
    c.get.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Some(0)
  }

  it should "prepare, reduce and present with strict and non-nan" in {
    val obj = Max[Position2D, Position1D](true, false)

    val t1 = obj.prepare(cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(cell2)
    t2 shouldBe 2

    val t3 = obj.prepare(cell3)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 2

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val c = obj.present(Position1D("foo"), r2)
    c shouldBe None
  }

  it should "prepare, reduce and present with non-strict and nan" in {
    val obj = Max[Position2D, Position1D](false, true)

    val t1 = obj.prepare(cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(cell2)
    t2 shouldBe 2

    val t3 = obj.prepare(cell3)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 2

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe 2

    val c = obj.present(Position1D("foo"), r2)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(2)))
  }

  it should "prepare, reduce and present with non-strict and non-nan" in {
    val obj = Max[Position2D, Position1D](false, false)

    val t1 = obj.prepare(cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(cell2)
    t2 shouldBe 2

    val t3 = obj.prepare(cell3)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 2

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe 2

    val c = obj.present(Position1D("foo"), r2)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(2)))
  }

  it should "prepare, reduce and present expanded" in {
    val obj = Max[Position2D, Position1D]()
      .andThenExpand(_.position.append("max"))

    val t1 = obj.prepare(cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(cell2)
    t2 shouldBe 2

    val r = obj.reduce(t1, t2)
    r shouldBe 2

    val c = obj.present(Position1D("foo"), r)
    c shouldBe Some(Cell(Position2D("foo", "max"), getDoubleContent(2)))
  }

  it should "prepare, reduce and present expanded with strict and nan" in {
    val obj = Max[Position2D, Position1D](true, true)
      .andThenExpand(_.position.append("max"))

    val t1 = obj.prepare(cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(cell2)
    t2 shouldBe 2

    val t3 = obj.prepare(cell3)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 2

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val c = obj.present(Position1D("foo"), r2)
    c.get.position shouldBe Position2D("foo", "max")
    c.get.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Some(0)
  }

  it should "prepare, reduce and present expanded with strict and non-nan" in {
    val obj = Max[Position2D, Position1D](true, false)
      .andThenExpand(_.position.append("max"))

    val t1 = obj.prepare(cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(cell2)
    t2 shouldBe 2

    val t3 = obj.prepare(cell3)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 2

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val c = obj.present(Position1D("foo"), r2)
    c shouldBe None
  }

  it should "prepare, reduce and present expanded with non-strict and nan" in {
    val obj = Max[Position2D, Position1D](false, true)
      .andThenExpand(_.position.append("max"))

    val t1 = obj.prepare(cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(cell2)
    t2 shouldBe 2

    val t3 = obj.prepare(cell3)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 2

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe 2

    val c = obj.present(Position1D("foo"), r2)
    c shouldBe Some(Cell(Position2D("foo", "max"), getDoubleContent(2)))
  }

  it should "prepare, reduce and present expanded with non-strict and non-nan" in {
    val obj = Max[Position2D, Position1D](false, false)
      .andThenExpand(_.position.append("max"))

    val t1 = obj.prepare(cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(cell2)
    t2 shouldBe 2

    val t3 = obj.prepare(cell3)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 2

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe 2

    val c = obj.present(Position1D("foo"), r2)
    c shouldBe Some(Cell(Position2D("foo", "max"), getDoubleContent(2)))
  }
}

class TestMaxAbs extends TestAggregators {

  val cell1 = Cell(Position2D("foo", "one"), getDoubleContent(1))
  val cell2 = Cell(Position2D("foo", "two"), getDoubleContent(-2))
  val cell3 = Cell(Position2D("foo", "bar"), getStringContent("bar"))

  "A MaxAbs" should "prepare, reduce and present" in {
    val obj = MaxAbs[Position2D, Position1D]()

    val t1 = obj.prepare(cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(cell2)
    t2 shouldBe -2

    val r = obj.reduce(t1, t2)
    r shouldBe 2

    val c = obj.present(Position1D("foo"), r)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(2)))
  }

  it should "prepare, reduce and present with strict and nan" in {
    val obj = MaxAbs[Position2D, Position1D](true, true)

    val t1 = obj.prepare(cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(cell2)
    t2 shouldBe -2

    val t3 = obj.prepare(cell3)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 2

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val c = obj.present(Position1D("foo"), r2)
    c.get.position shouldBe Position1D("foo")
    c.get.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Some(0)
  }

  it should "prepare, reduce and present with strict and non-nan" in {
    val obj = MaxAbs[Position2D, Position1D](true, false)

    val t1 = obj.prepare(cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(cell2)
    t2 shouldBe -2

    val t3 = obj.prepare(cell3)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 2

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val c = obj.present(Position1D("foo"), r2)
    c shouldBe None
  }

  it should "prepare, reduce and present with non-strict and nan" in {
    val obj = MaxAbs[Position2D, Position1D](false, true)

    val t1 = obj.prepare(cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(cell2)
    t2 shouldBe -2

    val t3 = obj.prepare(cell3)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 2

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe 2

    val c = obj.present(Position1D("foo"), r2)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(2)))
  }

  it should "prepare, reduce and present with non-strict and non-nan" in {
    val obj = MaxAbs[Position2D, Position1D](false, false)

    val t1 = obj.prepare(cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(cell2)
    t2 shouldBe -2

    val t3 = obj.prepare(cell3)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 2

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe 2

    val c = obj.present(Position1D("foo"), r2)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(2)))
  }

  it should "prepare, reduce and present expanded" in {
    val obj = MaxAbs[Position2D, Position1D]()
      .andThenExpand(_.position.append("max.abs"))

    val t1 = obj.prepare(cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(cell2)
    t2 shouldBe -2

    val r = obj.reduce(t1, t2)
    r shouldBe 2

    val c = obj.present(Position1D("foo"), r)
    c shouldBe Some(Cell(Position2D("foo", "max.abs"), getDoubleContent(2)))
  }

  it should "prepare, reduce and present expanded with strict and nan" in {
    val obj = MaxAbs[Position2D, Position1D](true, true)
      .andThenExpand(_.position.append("max.abs"))

    val t1 = obj.prepare(cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(cell2)
    t2 shouldBe -2

    val t3 = obj.prepare(cell3)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 2

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val c = obj.present(Position1D("foo"), r2)
    c.get.position shouldBe Position2D("foo", "max.abs")
    c.get.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Some(0)
  }

  it should "prepare, reduce and present expanded with strict and non-nan" in {
    val obj = MaxAbs[Position2D, Position1D](true, false)
      .andThenExpand(_.position.append("max.abs"))

    val t1 = obj.prepare(cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(cell2)
    t2 shouldBe -2

    val t3 = obj.prepare(cell3)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 2

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val c = obj.present(Position1D("foo"), r2)
    c shouldBe None
  }

  it should "prepare, reduce and present expanded with non-strict and nan" in {
    val obj = MaxAbs[Position2D, Position1D](false, true)
      .andThenExpand(_.position.append("max.abs"))

    val t1 = obj.prepare(cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(cell2)
    t2 shouldBe -2

    val t3 = obj.prepare(cell3)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 2

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe 2

    val c = obj.present(Position1D("foo"), r2)
    c shouldBe Some(Cell(Position2D("foo", "max.abs"), getDoubleContent(2)))
  }

  it should "prepare, reduce and present expanded with non-strict and non-nan" in {
    val obj = MaxAbs[Position2D, Position1D](false, false)
      .andThenExpand(_.position.append("max.abs"))

    val t1 = obj.prepare(cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(cell2)
    t2 shouldBe -2

    val t3 = obj.prepare(cell3)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 2

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe 2

    val c = obj.present(Position1D("foo"), r2)
    c shouldBe Some(Cell(Position2D("foo", "max.abs"), getDoubleContent(2)))
  }
}

class TestSum extends TestAggregators {

  val cell1 = Cell(Position2D("foo", "one"), getDoubleContent(1))
  val cell2 = Cell(Position2D("foo", "two"), getDoubleContent(2))
  val cell3 = Cell(Position2D("foo", "bar"), getStringContent("bar"))

  "A Sum" should "prepare, reduce and present" in {
    val obj = Sum[Position2D, Position1D]()

    val t1 = obj.prepare(cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(cell2)
    t2 shouldBe 2

    val r = obj.reduce(t1, t2)
    r shouldBe 3

    val c = obj.present(Position1D("foo"), r)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(3)))
  }

  it should "prepare, reduce and present with strict and nan" in {
    val obj = Sum[Position2D, Position1D](true, true)

    val t1 = obj.prepare(cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(cell2)
    t2 shouldBe 2

    val t3 = obj.prepare(cell3)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 3

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val c = obj.present(Position1D("foo"), r2)
    c.get.position shouldBe Position1D("foo")
    c.get.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Some(0)
  }

  it should "prepare, reduce and present with strict and non-nan" in {
    val obj = Sum[Position2D, Position1D](true, false)

    val t1 = obj.prepare(cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(cell2)
    t2 shouldBe 2

    val t3 = obj.prepare(cell3)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 3

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val c = obj.present(Position1D("foo"), r2)
    c shouldBe None
  }

  it should "prepare, reduce and present with non-strict and nan" in {
    val obj = Sum[Position2D, Position1D](false, true)

    val t1 = obj.prepare(cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(cell2)
    t2 shouldBe 2

    val t3 = obj.prepare(cell3)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 3

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe 3

    val c = obj.present(Position1D("foo"), r2)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(3)))
  }

  it should "prepare, reduce and present with non-strict and non-nan" in {
    val obj = Sum[Position2D, Position1D](false, false)

    val t1 = obj.prepare(cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(cell2)
    t2 shouldBe 2

    val t3 = obj.prepare(cell3)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 3

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe 3

    val c = obj.present(Position1D("foo"), r2)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(3)))
  }

  it should "prepare, reduce and present expanded" in {
    val obj = Sum[Position2D, Position1D]()
      .andThenExpand(_.position.append("sum"))

    val t1 = obj.prepare(cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(cell2)
    t2 shouldBe 2

    val r = obj.reduce(t1, t2)
    r shouldBe 3

    val c = obj.present(Position1D("foo"), r)
    c shouldBe Some(Cell(Position2D("foo", "sum"), getDoubleContent(3)))
  }

  it should "prepare, reduce and present expanded with strict and nan" in {
    val obj = Sum[Position2D, Position1D](true, true)
      .andThenExpand(_.position.append("sum"))

    val t1 = obj.prepare(cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(cell2)
    t2 shouldBe 2

    val t3 = obj.prepare(cell3)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 3

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val c = obj.present(Position1D("foo"), r2)
    c.get.position shouldBe Position2D("foo", "sum")
    c.get.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Some(0)
  }

  it should "prepare, reduce and present expanded with strict and non-nan" in {
    val obj = Sum[Position2D, Position1D](true, false)
      .andThenExpand(_.position.append("sum"))

    val t1 = obj.prepare(cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(cell2)
    t2 shouldBe 2

    val t3 = obj.prepare(cell3)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 3

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val c = obj.present(Position1D("foo"), r2)
    c shouldBe None
  }

  it should "prepare, reduce and present expanded with non-strict and nan" in {
    val obj = Sum[Position2D, Position1D](false, true)
      .andThenExpand(_.position.append("sum"))

    val t1 = obj.prepare(cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(cell2)
    t2 shouldBe 2

    val t3 = obj.prepare(cell3)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 3

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe 3

    val c = obj.present(Position1D("foo"), r2)
    c shouldBe Some(Cell(Position2D("foo", "sum"), getDoubleContent(3)))
  }

  it should "prepare, reduce and present expanded with non-strict and non-nan" in {
    val obj = Sum[Position2D, Position1D](false, false)
      .andThenExpand(_.position.append("sum"))

    val t1 = obj.prepare(cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(cell2)
    t2 shouldBe 2

    val t3 = obj.prepare(cell3)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 3

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe 3

    val c = obj.present(Position1D("foo"), r2)
    c shouldBe Some(Cell(Position2D("foo", "sum"), getDoubleContent(3)))
  }
}

class TestThresholdCount extends TestAggregators {

  val cell1 = Cell(Position2D("foo", "one"), getDoubleContent(-1))
  val cell2 = Cell(Position2D("foo", "two"), getDoubleContent(0))
  val cell3 = Cell(Position2D("foo", "three"), getDoubleContent(1))
  val cell4 = Cell(Position2D("foo", "bar"), getStringContent("bar"))

  def predicate(con: Content) = con.value.asDouble.map(_ <= 0).getOrElse(false)

  "A PredicateCount" should "prepare, reduce and present" in {
    val obj = PredicateCount[Position2D, Position1D](predicate)

    val t1 = obj.prepare(cell1)
    t1 shouldBe (1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe (1)

    val t3 = obj.prepare(cell3)
    t3 shouldBe (0)

    val t4 = obj.prepare(cell4)
    t4 shouldBe (0)

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe (2)

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe (2)

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe (2)

    val c = obj.present(Position1D("foo"), r3)
    c shouldBe (Some(Cell(Position1D("foo"), getLongContent(2))))
  }
}

class TestWeightedSum extends TestAggregators {

  val cell1 = Cell(Position2D("foo", 1), getDoubleContent(-1))
  val cell2 = Cell(Position2D("bar", 2), getDoubleContent(1))
  val cell3 = Cell(Position2D("xyz", 3), getStringContent("abc"))
  val ext = Map(Position1D("foo") -> 3.14, Position1D("bar") -> 6.28, Position1D(2) -> 3.14,
    Position1D("foo.model1") -> 3.14, Position1D("bar.model1") -> 6.28, Position1D("2.model2") -> -3.14)

  type W = Map[Position1D, Double]

  def extractor1 = ExtractWithDimension[Dimension.First, Position2D, Double](First)
  def extractor2 = ExtractWithDimension[Dimension.Second, Position2D, Double](Second)

  case class ExtractWithName(dim: Dimension, name: String) extends Extract[Position2D, W, Double] {
    def extract(cell: Cell[Position2D], ext: W): Option[Double] = {
      ext.get(Position1D(name.format(cell.position(dim).toShortString)))
    }
  }

  "A WeightedSum" should "prepare, reduce and present on the first dimension" in {
    val obj = WeightedSum[Position2D, Position1D, W](extractor1)

    val t1 = obj.prepareWithValue(cell1, ext)
    t1 shouldBe -3.14

    val t2 = obj.prepareWithValue(cell2, ext)
    t2 shouldBe 6.28

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 3.14

    val c = obj.presentWithValue(Position1D("foo"), r1, ext)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(3.14)))
  }

  it should "prepare, reduce and present on the second dimension" in {
    val obj = WeightedSum[Position2D, Position1D, W](extractor2)

    val t1 = obj.prepareWithValue(cell1, ext)
    t1 shouldBe 0

    val t2 = obj.prepareWithValue(cell2, ext)
    t2 shouldBe 3.14

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 3.14

    val c = obj.presentWithValue(Position1D("foo"), r1, ext)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(3.14)))
  }

  it should "prepare, reduce and present with strict and nan" in {
    val obj = WeightedSum[Position2D, Position1D, W](extractor1, true, true)

    val t1 = obj.prepareWithValue(cell1, ext)
    t1 shouldBe -3.14

    val t2 = obj.prepareWithValue(cell2, ext)
    t2 shouldBe 6.28

    val t3 = obj.prepareWithValue(cell3, ext)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 3.14

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position1D("foo"), r2, ext)
    c.get.position shouldBe Position1D("foo")
    c.get.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Some(0)
  }

  it should "prepare, reduce and present with strict and non-nan" in {
    val obj = WeightedSum[Position2D, Position1D, W](extractor1, true, false)

    val t1 = obj.prepareWithValue(cell1, ext)
    t1 shouldBe -3.14

    val t2 = obj.prepareWithValue(cell2, ext)
    t2 shouldBe 6.28

    val t3 = obj.prepareWithValue(cell3, ext)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 3.14

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position1D("foo"), r2, ext)
    c shouldBe None
  }

  it should "prepare, reduce and present with non-strict and nan" in {
    val obj = WeightedSum[Position2D, Position1D, W](extractor1, false, true)

    val t1 = obj.prepareWithValue(cell1, ext)
    t1 shouldBe -3.14

    val t2 = obj.prepareWithValue(cell2, ext)
    t2 shouldBe 6.28

    val t3 = obj.prepareWithValue(cell3, ext)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 3.14

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe 3.14

    val c = obj.presentWithValue(Position1D("foo"), r2, ext)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(3.14)))
  }

  it should "prepare, reduce and present with non-strict and non-nan" in {
    val obj = WeightedSum[Position2D, Position1D, W](extractor1, false, false)

    val t1 = obj.prepareWithValue(cell1, ext)
    t1 shouldBe -3.14

    val t2 = obj.prepareWithValue(cell2, ext)
    t2 shouldBe 6.28

    val t3 = obj.prepareWithValue(cell3, ext)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 3.14

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe 3.14

    val c = obj.presentWithValue(Position1D("foo"), r2, ext)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(3.14)))
  }

  it should "prepare, reduce and present expanded on the first dimension" in {
    val obj = WeightedSum[Position2D, Position1D, W](extractor1)
      .andThenExpandWithValue((c: Cell[Position1D], e: W) => c.position.append("result"))

    val t1 = obj.prepareWithValue(cell1, ext)
    t1 shouldBe -3.14

    val t2 = obj.prepareWithValue(cell2, ext)
    t2 shouldBe 6.28

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 3.14

    val c = obj.presentWithValue(Position1D("foo"), r1, ext)
    c shouldBe Some(Cell(Position2D("foo", "result"), getDoubleContent(3.14)))
  }

  it should "prepare, reduce and present expanded on the second dimension" in {
    val obj = WeightedSum[Position2D, Position1D, W](extractor2)
      .andThenExpandWithValue((c: Cell[Position1D], e: W) => c.position.append("result"))

    val t1 = obj.prepareWithValue(cell1, ext)
    t1 shouldBe 0

    val t2 = obj.prepareWithValue(cell2, ext)
    t2 shouldBe 3.14

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 3.14

    val c = obj.presentWithValue(Position1D("foo"), r1, ext)
    c shouldBe Some(Cell(Position2D("foo", "result"), getDoubleContent(3.14)))
  }

  it should "prepare, reduce and present expanded with strict and nan" in {
    val obj = WeightedSum[Position2D, Position1D, W](extractor1, true, true)
      .andThenExpandWithValue((c: Cell[Position1D], e: W) => c.position.append("result"))

    val t1 = obj.prepareWithValue(cell1, ext)
    t1 shouldBe -3.14

    val t2 = obj.prepareWithValue(cell2, ext)
    t2 shouldBe 6.28

    val t3 = obj.prepareWithValue(cell3, ext)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 3.14

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position1D("foo"), r2, ext)
    c.get.position shouldBe Position2D("foo", "result")
    c.get.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Some(0)
  }

  it should "prepare, reduce and present expanded with strict and non-nan" in {
    val obj = WeightedSum[Position2D, Position1D, W](extractor1, true, false)
      .andThenExpandWithValue((c: Cell[Position1D], e: W) => c.position.append("result"))

    val t1 = obj.prepareWithValue(cell1, ext)
    t1 shouldBe -3.14

    val t2 = obj.prepareWithValue(cell2, ext)
    t2 shouldBe 6.28

    val t3 = obj.prepareWithValue(cell3, ext)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 3.14

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position1D("foo"), r2, ext)
    c shouldBe None
  }

  it should "prepare, reduce and present expanded with non-strict and nan" in {
    val obj = WeightedSum[Position2D, Position1D, W](extractor1, false, true)
      .andThenExpandWithValue((c: Cell[Position1D], e: W) => c.position.append("result"))

    val t1 = obj.prepareWithValue(cell1, ext)
    t1 shouldBe -3.14

    val t2 = obj.prepareWithValue(cell2, ext)
    t2 shouldBe 6.28

    val t3 = obj.prepareWithValue(cell3, ext)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 3.14

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe 3.14

    val c = obj.presentWithValue(Position1D("foo"), r2, ext)
    c shouldBe Some(Cell(Position2D("foo", "result"), getDoubleContent(3.14)))
  }

  it should "prepare, reduce and present expanded with non-strict and non-nan" in {
    val obj = WeightedSum[Position2D, Position1D, W](extractor1, false, false)
      .andThenExpandWithValue((c: Cell[Position1D], e: W) => c.position.append("result"))

    val t1 = obj.prepareWithValue(cell1, ext)
    t1 shouldBe -3.14

    val t2 = obj.prepareWithValue(cell2, ext)
    t2 shouldBe 6.28

    val t3 = obj.prepareWithValue(cell3, ext)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 3.14

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe 3.14

    val c = obj.presentWithValue(Position1D("foo"), r2, ext)
    c shouldBe Some(Cell(Position2D("foo", "result"), getDoubleContent(3.14)))
  }

  it should "prepare, reduce and present multiple on the first dimension with format" in {
    val obj = WeightedSum[Position2D, Position1D, W](ExtractWithName(First, "%1$s.model1"))
      .andThenExpandWithValue((c: Cell[Position1D], e: W) => c.position.append("result"))

    val t1 = obj.prepareWithValue(cell1, ext)
    t1 shouldBe -3.14

    val t2 = obj.prepareWithValue(cell2, ext)
    t2 shouldBe 6.28

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 3.14

    val c = obj.presentWithValue(Position1D("foo"), r1, ext)
    c shouldBe Some(Cell(Position2D("foo", "result"), getDoubleContent(3.14)))
  }

  it should "prepare, reduce and present multiple on the second dimension with format" in {
    val obj = WeightedSum[Position2D, Position1D, W](ExtractWithName(Second, "%1$s.model2"))
      .andThenExpandWithValue((c: Cell[Position1D], e: W) => c.position.append("result"))

    val t1 = obj.prepareWithValue(cell1, ext)
    t1 shouldBe 0

    val t2 = obj.prepareWithValue(cell2, ext)
    t2 shouldBe -3.14

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe -3.14

    val c = obj.presentWithValue(Position1D("foo"), r1, ext)
    c shouldBe Some(Cell(Position2D("foo", "result"), getDoubleContent(-3.14)))
  }

  it should "prepare, reduce and present multiple with strict and nan with format" in {
    val obj = WeightedSum[Position2D, Position1D, W](ExtractWithName(First, "%1$s.model1"), true, true)
      .andThenExpandWithValue((c: Cell[Position1D], e: W) => c.position.append("result"))

    val t1 = obj.prepareWithValue(cell1, ext)
    t1 shouldBe -3.14

    val t2 = obj.prepareWithValue(cell2, ext)
    t2 shouldBe 6.28

    val t3 = obj.prepareWithValue(cell3, ext)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 3.14

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position1D("foo"), r2, ext)
    c.get.position shouldBe Position2D("foo", "result")
    c.get.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Some(0)
  }

  it should "prepare, reduce and present multiple with strict and non-nan with format" in {
    val obj = WeightedSum[Position2D, Position1D, W](ExtractWithName(First, "%1$s.model1"), true, false)
      .andThenExpandWithValue((c: Cell[Position1D], e: W) => c.position.append("result"))

    val t1 = obj.prepareWithValue(cell1, ext)
    t1 shouldBe -3.14

    val t2 = obj.prepareWithValue(cell2, ext)
    t2 shouldBe 6.28

    val t3 = obj.prepareWithValue(cell3, ext)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 3.14

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position1D("foo"), r2, ext)
    c shouldBe None
  }

  it should "prepare, reduce and present multiple with non-strict and nan with format" in {
    val obj = WeightedSum[Position2D, Position1D, W](ExtractWithName(First, "%1$s.model1"), false, true)
      .andThenExpandWithValue((c: Cell[Position1D], e: W) => c.position.append("result"))

    val t1 = obj.prepareWithValue(cell1, ext)
    t1 shouldBe -3.14

    val t2 = obj.prepareWithValue(cell2, ext)
    t2 shouldBe 6.28

    val t3 = obj.prepareWithValue(cell3, ext)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 3.14

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe 3.14

    val c = obj.presentWithValue(Position1D("foo"), r2, ext)
    c shouldBe Some(Cell(Position2D("foo", "result"), getDoubleContent(3.14)))
  }

  it should "prepare, reduce and present multiple with non-strict and non-nan with format" in {
    val obj = WeightedSum[Position2D, Position1D, W](ExtractWithName(First, "%1$s.model1"), false, false)
      .andThenExpandWithValue((c: Cell[Position1D], e: W) => c.position.append("result"))

    val t1 = obj.prepareWithValue(cell1, ext)
    t1 shouldBe -3.14

    val t2 = obj.prepareWithValue(cell2, ext)
    t2 shouldBe 6.28

    val t3 = obj.prepareWithValue(cell3, ext)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 3.14

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe 3.14

    val c = obj.presentWithValue(Position1D("foo"), r2, ext)
    c shouldBe Some(Cell(Position2D("foo", "result"), getDoubleContent(3.14)))
  }
}

class TestDistinctCount extends TestAggregators {

  val cell1 = Cell(Position2D("foo", 1), getDoubleContent(1))
  val cell2 = Cell(Position2D("foo", 2), getDoubleContent(1))
  val cell3 = Cell(Position2D("foo", 3), getDoubleContent(1))
  val cell4 = Cell(Position2D("abc", 4), getStringContent("abc"))
  val cell5 = Cell(Position2D("xyz", 4), getStringContent("abc"))
  val cell6 = Cell(Position2D("bar", 5), getLongContent(123))

  "A DistinctCount" should "prepare, reduce and present" in {
    val obj = DistinctCount[Position2D, Position1D]()

    val t1 = obj.prepare(cell1)
    t1 shouldBe Set(DoubleValue(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Set(DoubleValue(1))

    val t3 = obj.prepare(cell3)
    t3 shouldBe Set(DoubleValue(1))

    val t4 = obj.prepare(cell4)
    t4 shouldBe Set(StringValue("abc"))

    val t5 = obj.prepare(cell5)
    t5 shouldBe Set(StringValue("abc"))

    val t6 = obj.prepare(cell6)
    t6 shouldBe Set(LongValue(123))

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Set(DoubleValue(1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Set(DoubleValue(1))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe Set(DoubleValue(1), StringValue("abc"))

    val r4 = obj.reduce(r3, t5)
    r4 shouldBe Set(DoubleValue(1), StringValue("abc"))

    val r5 = obj.reduce(r4, t6)
    r5 shouldBe Set(DoubleValue(1), StringValue("abc"), LongValue(123))

    val c = obj.present(Position1D("foo"), r5)
    c shouldBe Some(Cell(Position1D("foo"), getLongContent(3)))
  }

  it should "prepare, reduce and present expanded" in {
    val obj = DistinctCount[Position2D, Position1D]()
      .andThenExpand(_.position.append("count"))

    val t1 = obj.prepare(cell1)
    t1 shouldBe Set(DoubleValue(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Set(DoubleValue(1))

    val t3 = obj.prepare(cell3)
    t3 shouldBe Set(DoubleValue(1))

    val t4 = obj.prepare(cell4)
    t4 shouldBe Set(StringValue("abc"))

    val t5 = obj.prepare(cell5)
    t5 shouldBe Set(StringValue("abc"))

    val t6 = obj.prepare(cell6)
    t6 shouldBe Set(LongValue(123))

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Set(DoubleValue(1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Set(DoubleValue(1))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe Set(DoubleValue(1), StringValue("abc"))

    val r4 = obj.reduce(r3, t5)
    r4 shouldBe Set(DoubleValue(1), StringValue("abc"))

    val r5 = obj.reduce(r4, t6)
    r5 shouldBe Set(DoubleValue(1), StringValue("abc"), LongValue(123))

    val c = obj.present(Position1D("foo"), r5)
    c shouldBe Some(Cell(Position2D("foo", "count"), getLongContent(3)))
  }
}

class TestEntropy extends TestAggregators {

  val cell1 = Cell(Position2D("foo", "abc"), getLongContent(1))
  val cell2 = Cell(Position2D("foo", "xyz"), getLongContent(2))
  val cell3 = Cell(Position2D("foo", "123"), getStringContent("456"))

  def log2(x: Double) = math.log(x) / math.log(2)
  def log4(x: Double) = math.log(x) / math.log(4)

  def extractor = ExtractWithDimension[Dimension.First, Position2D, Double](First)

  val count = Map(Position1D("foo") -> 3.0)

  type W = Map[Position1D, Double]

  "An Entropy" should "prepare, reduce and present" in {
    val obj = Entropy[Position2D, Position1D, W](extractor)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe ((1, 1.0/3 * log2(1.0/3)))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe ((1, 2.0/3 * log2(2.0/3)))

    val t3 = obj.prepareWithValue(cell3, count)
    t3._1 shouldBe 1
    t3._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe ((2, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val r2 = obj.reduce(r1, t3)
    r2._1 shouldBe 3
    r2._2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position1D("foo"), r2, count)
    c shouldBe None
  }

  it should "prepare, reduce and present with strict, nan negate" in {
    val obj = Entropy[Position2D, Position1D, W](extractor, true, true, true)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe ((1, 1.0/3 * log2(1.0/3)))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe ((1, 2.0/3 * log2(2.0/3)))

    val t3 = obj.prepareWithValue(cell3, count)
    t3._1 shouldBe 1
    t3._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe ((2, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val r2 = obj.reduce(r1, t3)
    r2._1 shouldBe 3
    r2._2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position1D("foo"), r2, count)
    c.get.position shouldBe Position1D("foo")
    c.get.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Some(0)
  }

  it should "prepare, reduce and present strict, nan and non-negate" in {
    val obj = Entropy[Position2D, Position1D, W](extractor, true, true, false)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe ((1, 1.0/3 * log2(1.0/3)))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe ((1, 2.0/3 * log2(2.0/3)))

    val t3 = obj.prepareWithValue(cell3, count)
    t3._1 shouldBe 1
    t3._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe ((2, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val r2 = obj.reduce(r1, t3)
    r2._1 shouldBe 3
    r2._2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position1D("foo"), r2, count)
    c.get.position shouldBe Position1D("foo")
    c.get.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Some(0)
  }

  it should "prepare, reduce and present single with strict, non-nan and negate" in {
    val obj = Entropy[Position2D, Position1D, W](extractor, true, false, true)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe ((1, 1.0/3 * log2(1.0/3)))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe ((1, 2.0/3 * log2(2.0/3)))

    val t3 = obj.prepareWithValue(cell3, count)
    t3._1 shouldBe 1
    t3._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe ((2, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val r2 = obj.reduce(r1, t3)
    r2._1 shouldBe 3
    r2._2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position1D("foo"), r2, count)
    c shouldBe None
  }

  it should "prepare, reduce and present with strict, non-nan and non-negate" in {
    val obj = Entropy[Position2D, Position1D, W](extractor, true, false, false)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe ((1, 1.0/3 * log2(1.0/3)))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe ((1, 2.0/3 * log2(2.0/3)))

    val t3 = obj.prepareWithValue(cell3, count)
    t3._1 shouldBe 1
    t3._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe ((2, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val r2 = obj.reduce(r1, t3)
    r2._1 shouldBe 3
    r2._2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position1D("foo"), r2, count)
    c shouldBe None
  }

  it should "prepare, reduce and present with non-strict, nan and negate" in {
    val obj = Entropy[Position2D, Position1D, W](extractor, false, true, true)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe ((1, 1.0/3 * log2(1.0/3)))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe ((1, 2.0/3 * log2(2.0/3)))

    val t3 = obj.prepareWithValue(cell3, count)
    t3._1 shouldBe 1
    t3._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe ((2, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe ((3, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val c = obj.presentWithValue(Position1D("foo"), r2, count)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))
  }

  it should "prepare, reduce and present with non-strict, nan and non-negate" in {
    val obj = Entropy[Position2D, Position1D, W](extractor, false, true, false)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe ((1, 1.0/3 * log2(1.0/3)))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe ((1, 2.0/3 * log2(2.0/3)))

    val t3 = obj.prepareWithValue(cell3, count)
    t3._1 shouldBe 1
    t3._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe ((2, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe ((3, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val c = obj.presentWithValue(Position1D("foo"), r2, count)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(- (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3)))))
  }

  it should "prepare, reduce and present with non-strict, non-nan and negate" in {
    val obj = Entropy[Position2D, Position1D, W](extractor, false, false, true)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe ((1, 1.0/3 * log2(1.0/3)))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe ((1, 2.0/3 * log2(2.0/3)))

    val t3 = obj.prepareWithValue(cell3, count)
    t3._1 shouldBe 1
    t3._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe ((2, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe ((3, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val c = obj.presentWithValue(Position1D("foo"), r2, count)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))
  }

  it should "prepare, reduce and present with non-strict, non-nan and non-negate" in {
    val obj = Entropy[Position2D, Position1D, W](extractor, false, false, false)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe ((1, 1.0/3 * log2(1.0/3)))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe ((1, 2.0/3 * log2(2.0/3)))

    val t3 = obj.prepareWithValue(cell3, count)
    t3._1 shouldBe 1
    t3._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe ((2, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe ((3, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val c = obj.presentWithValue(Position1D("foo"), r2, count)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(- (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3)))))
  }

  it should "prepare, reduce and present with log" in {
    val obj = Entropy[Position2D, Position1D, W](extractor, log = log4 _)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe ((1, 1.0/3 * log4(1.0/3)))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe ((1, 2.0/3 * log4(2.0/3)))

    val t3 = obj.prepareWithValue(cell3, count)
    t3._1 shouldBe 1
    t3._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe ((2, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val r2 = obj.reduce(r1, t3)
    r2._1 shouldBe 3
    r2._2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position1D("foo"), r2, count)
    c shouldBe None
  }

  it should "prepare, reduce and present with strict, nan and negate with log" in {
    val obj = Entropy[Position2D, Position1D, W](extractor, true, true, true, log4 _)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe ((1, 1.0/3 * log4(1.0/3)))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe ((1, 2.0/3 * log4(2.0/3)))

    val t3 = obj.prepareWithValue(cell3, count)
    t3._1 shouldBe 1
    t3._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe ((2, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val r2 = obj.reduce(r1, t3)
    r2._1 shouldBe 3
    r2._2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position1D("foo"), r2, count)
    c.get.position shouldBe Position1D("foo")
    c.get.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Some(0)
  }

  it should "prepare, reduce and present with strict, nan and non-negate with log" in {
    val obj = Entropy[Position2D, Position1D, W](extractor, true, true, false, log4 _)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe ((1, 1.0/3 * log4(1.0/3)))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe ((1, 2.0/3 * log4(2.0/3)))

    val t3 = obj.prepareWithValue(cell3, count)
    t3._1 shouldBe 1
    t3._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe ((2, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val r2 = obj.reduce(r1, t3)
    r2._1 shouldBe 3
    r2._2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position1D("foo"), r2, count)
    c.get.position shouldBe Position1D("foo")
    c.get.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Some(0)
  }

  it should "prepare, reduce and present with strict, non-nan and negate with log" in {
    val obj = Entropy[Position2D, Position1D, W](extractor, true, false, true, log4 _)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe ((1, 1.0/3 * log4(1.0/3)))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe ((1, 2.0/3 * log4(2.0/3)))

    val t3 = obj.prepareWithValue(cell3, count)
    t3._1 shouldBe 1
    t3._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe ((2, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val r2 = obj.reduce(r1, t3)
    r2._1 shouldBe 3
    r2._2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position1D("foo"), r2, count)
    c shouldBe None
  }

  it should "prepare, reduce and present with strict, non-nan and non-negate with log" in {
    val obj = Entropy[Position2D, Position1D, W](extractor, true, false, false, log4 _)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe ((1, 1.0/3 * log4(1.0/3)))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe ((1, 2.0/3 * log4(2.0/3)))

    val t3 = obj.prepareWithValue(cell3, count)
    t3._1 shouldBe 1
    t3._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe ((2, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val r2 = obj.reduce(r1, t3)
    r2._1 shouldBe 3
    r2._2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position1D("foo"), r2, count)
    c shouldBe None
  }

  it should "prepare, reduce and present with non-strict, nan and negate with log" in {
    val obj = Entropy[Position2D, Position1D, W](extractor, false, true, true, log4 _)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe ((1, 1.0/3 * log4(1.0/3)))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe ((1, 2.0/3 * log4(2.0/3)))

    val t3 = obj.prepareWithValue(cell3, count)
    t3._1 shouldBe 1
    t3._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe ((2, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe ((3, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val c = obj.presentWithValue(Position1D("foo"), r2, count)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))
  }

  it should "prepare, reduce and present with non-strict, nan and non-negate with log" in {
    val obj = Entropy[Position2D, Position1D, W](extractor, false, true, false, log4 _)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe ((1, 1.0/3 * log4(1.0/3)))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe ((1, 2.0/3 * log4(2.0/3)))

    val t3 = obj.prepareWithValue(cell3, count)
    t3._1 shouldBe 1
    t3._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe ((2, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe ((3, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val c = obj.presentWithValue(Position1D("foo"), r2, count)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(- (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3)))))
  }

  it should "prepare, reduce and present with non-strict, non-nan and negate with log" in {
    val obj = Entropy[Position2D, Position1D, W](extractor, false, false, true, log4 _)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe ((1, 1.0/3 * log4(1.0/3)))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe ((1, 2.0/3 * log4(2.0/3)))

    val t3 = obj.prepareWithValue(cell3, count)
    t3._1 shouldBe 1
    t3._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe ((2, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe ((3, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val c = obj.presentWithValue(Position1D("foo"), r2, count)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))
  }

  it should "prepare, reduce and present with non-strict, non-nan and non-negate with log" in {
    val obj = Entropy[Position2D, Position1D, W](extractor, false, false, false, log4 _)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe ((1, 1.0/3 * log4(1.0/3)))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe ((1, 2.0/3 * log4(2.0/3)))

    val t3 = obj.prepareWithValue(cell3, count)
    t3._1 shouldBe 1
    t3._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe ((2, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe ((3, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val c = obj.presentWithValue(Position1D("foo"), r2, count)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(- (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3)))))
  }

  it should "prepare, reduce and present expanded" in {
    val obj = Entropy[Position2D, Position1D, W](extractor)
      .andThenExpandWithValue((c: Cell[Position1D], e: W) => c.position.append("entropy"))

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe ((1, 1.0/3 * log2(1.0/3)))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe ((1, 2.0/3 * log2(2.0/3)))

    val t3 = obj.prepareWithValue(cell3, count)
    t3._1 shouldBe 1
    t3._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe ((2, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val r2 = obj.reduce(r1, t3)
    r2._1 shouldBe 3
    r2._2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position1D("foo"), r2, count)
    c shouldBe None
  }

  it should "prepare, reduce and present expanded with strict, nan and negate" in {
    val obj = Entropy[Position2D, Position1D, W](extractor, true, true, true)
      .andThenExpandWithValue((c: Cell[Position1D], e: W) => c.position.append("entropy"))

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe ((1, 1.0/3 * log2(1.0/3)))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe ((1, 2.0/3 * log2(2.0/3)))

    val t3 = obj.prepareWithValue(cell3, count)
    t3._1 shouldBe 1
    t3._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe ((2, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val r2 = obj.reduce(r1, t3)
    r2._1 shouldBe 3
    r2._2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position1D("foo"), r2, count)
    c.get.position shouldBe Position2D("foo", "entropy")
    c.get.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Some(0)
  }

  it should "prepare, reduce and present multiple with strict, nan and non-negate" in {
    val obj = Entropy[Position2D, Position1D, W](extractor, true, true, false)
      .andThenExpandWithValue((c: Cell[Position1D], e: W) => c.position.append("entropy"))

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe ((1, 1.0/3 * log2(1.0/3)))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe ((1, 2.0/3 * log2(2.0/3)))

    val t3 = obj.prepareWithValue(cell3, count)
    t3._1 shouldBe 1
    t3._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe ((2, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val r2 = obj.reduce(r1, t3)
    r2._1 shouldBe 3
    r2._2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position1D("foo"), r2, count)
    c.get.position shouldBe Position2D("foo", "entropy")
    c.get.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Some(0)
  }

  it should "prepare, reduce and present expanded with strict, non-nan and negate" in {
    val obj = Entropy[Position2D, Position1D, W](extractor, true, false, true)
      .andThenExpandWithValue((c: Cell[Position1D], e: W) => c.position.append("entropy"))

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe ((1, 1.0/3 * log2(1.0/3)))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe ((1, 2.0/3 * log2(2.0/3)))

    val t3 = obj.prepareWithValue(cell3, count)
    t3._1 shouldBe 1
    t3._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe ((2, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val r2 = obj.reduce(r1, t3)
    r2._1 shouldBe 3
    r2._2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position1D("foo"), r2, count)
    c shouldBe None
  }

  it should "prepare, reduce and present expanded with strict, non-nan and non-negate" in {
    val obj = Entropy[Position2D, Position1D, W](extractor, true, false, false)
      .andThenExpandWithValue((c: Cell[Position1D], e: W) => c.position.append("entropy"))

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe ((1, 1.0/3 * log2(1.0/3)))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe ((1, 2.0/3 * log2(2.0/3)))

    val t3 = obj.prepareWithValue(cell3, count)
    t3._1 shouldBe 1
    t3._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe ((2, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val r2 = obj.reduce(r1, t3)
    r2._1 shouldBe 3
    r2._2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position1D("foo"), r2, count)
    c shouldBe None
  }

  it should "prepare, reduce and present expanded with non-strict, nan and negate" in {
    val obj = Entropy[Position2D, Position1D, W](extractor, false, true, true)
      .andThenExpandWithValue((c: Cell[Position1D], e: W) => c.position.append("entropy"))

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe ((1, 1.0/3 * log2(1.0/3)))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe ((1, 2.0/3 * log2(2.0/3)))

    val t3 = obj.prepareWithValue(cell3, count)
    t3._1 shouldBe 1
    t3._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe ((2, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe ((3, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val c = obj.presentWithValue(Position1D("foo"), r2, count)
    c shouldBe Some(Cell(Position2D("foo", "entropy"), getDoubleContent(2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))
  }

  it should "prepare, reduce and present expanded with non-strict, nan and non-negate" in {
    val obj = Entropy[Position2D, Position1D, W](extractor, false, true, false)
      .andThenExpandWithValue((c: Cell[Position1D], e: W) => c.position.append("entropy"))

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe ((1, 1.0/3 * log2(1.0/3)))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe ((1, 2.0/3 * log2(2.0/3)))

    val t3 = obj.prepareWithValue(cell3, count)
    t3._1 shouldBe 1
    t3._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe ((2, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe ((3, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val c = obj.presentWithValue(Position1D("foo"), r2, count)
    c shouldBe Some(Cell(Position2D("foo", "entropy"), getDoubleContent(- (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3)))))
  }

  it should "prepare, reduce and present expanded with non-strict, non-nan and negate" in {
    val obj = Entropy[Position2D, Position1D, W](extractor, false, false, true)
      .andThenExpandWithValue((c: Cell[Position1D], e: W) => c.position.append("entropy"))

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe ((1, 1.0/3 * log2(1.0/3)))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe ((1, 2.0/3 * log2(2.0/3)))

    val t3 = obj.prepareWithValue(cell3, count)
    t3._1 shouldBe 1
    t3._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe ((2, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe ((3, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val c = obj.presentWithValue(Position1D("foo"), r2, count)
    c shouldBe Some(Cell(Position2D("foo", "entropy"), getDoubleContent(2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))
  }

  it should "prepare, reduce and present expanded with non-strict, non-nan and non-negate" in {
    val obj = Entropy[Position2D, Position1D, W](extractor, false, false, false)
      .andThenExpandWithValue((c: Cell[Position1D], e: W) => c.position.append("entropy"))

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe ((1, 1.0/3 * log2(1.0/3)))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe ((1, 2.0/3 * log2(2.0/3)))

    val t3 = obj.prepareWithValue(cell3, count)
    t3._1 shouldBe 1
    t3._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe ((2, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe ((3, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val c = obj.presentWithValue(Position1D("foo"), r2, count)
    c shouldBe Some(Cell(Position2D("foo", "entropy"), getDoubleContent(- (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3)))))
  }

  it should "prepare, reduce and present expanded with log" in {
    val obj = Entropy[Position2D, Position1D, W](extractor, log = log4 _)
      .andThenExpandWithValue((c: Cell[Position1D], e: W) => c.position.append("entropy"))

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe ((1, 1.0/3 * log4(1.0/3)))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe ((1, 2.0/3 * log4(2.0/3)))

    val t3 = obj.prepareWithValue(cell3, count)
    t3._1 shouldBe 1
    t3._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe ((2, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val r2 = obj.reduce(r1, t3)
    r2._1 shouldBe 3
    r2._2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position1D("foo"), r2, count)
    c shouldBe None
  }

  it should "prepare, reduce and present expanded with strict, nan and negate with log" in {
    val obj = Entropy[Position2D, Position1D, W](extractor, true, true, true, log4 _)
      .andThenExpandWithValue((c: Cell[Position1D], e: W) => c.position.append("entropy"))

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe ((1, 1.0/3 * log4(1.0/3)))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe ((1, 2.0/3 * log4(2.0/3)))

    val t3 = obj.prepareWithValue(cell3, count)
    t3._1 shouldBe 1
    t3._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe ((2, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val r2 = obj.reduce(r1, t3)
    r2._1 shouldBe 3
    r2._2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position1D("foo"), r2, count)
    c.get.position shouldBe Position2D("foo", "entropy")
    c.get.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Some(0)
  }

  it should "prepare, reduce and present expanded with strict, nan and non-negate with log" in {
    val obj = Entropy[Position2D, Position1D, W](extractor, true, true, false, log4 _)
      .andThenExpandWithValue((c: Cell[Position1D], e: W) => c.position.append("entropy"))

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe ((1, 1.0/3 * log4(1.0/3)))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe ((1, 2.0/3 * log4(2.0/3)))

    val t3 = obj.prepareWithValue(cell3, count)
    t3._1 shouldBe 1
    t3._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe ((2, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val r2 = obj.reduce(r1, t3)
    r2._1 shouldBe 3
    r2._2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position1D("foo"), r2, count)
    c.get.position shouldBe Position2D("foo", "entropy")
    c.get.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Some(0)
  }

  it should "prepare, reduce and present expanded with strict, non-nan and negate with log" in {
    val obj = Entropy[Position2D, Position1D, W](extractor, true, false, true, log4 _)
      .andThenExpandWithValue((c: Cell[Position1D], e: W) => c.position.append("entropy"))

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe ((1, 1.0/3 * log4(1.0/3)))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe ((1, 2.0/3 * log4(2.0/3)))

    val t3 = obj.prepareWithValue(cell3, count)
    t3._1 shouldBe 1
    t3._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe ((2, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val r2 = obj.reduce(r1, t3)
    r2._1 shouldBe 3
    r2._2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position1D("foo"), r2, count)
    c shouldBe None
  }

  it should "prepare, reduce and present expanded with strict, non-nan and non-negate with log" in {
    val obj = Entropy[Position2D, Position1D, W](extractor, true, false, false, log4 _)
      .andThenExpandWithValue((c: Cell[Position1D], e: W) => c.position.append("entropy"))

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe ((1, 1.0/3 * log4(1.0/3)))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe ((1, 2.0/3 * log4(2.0/3)))

    val t3 = obj.prepareWithValue(cell3, count)
    t3._1 shouldBe 1
    t3._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe ((2, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val r2 = obj.reduce(r1, t3)
    r2._1 shouldBe 3
    r2._2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position1D("foo"), r2, count)
    c shouldBe None
  }

  it should "prepare, reduce and present expanded with non-strict, nan and negate with log" in {
    val obj = Entropy[Position2D, Position1D, W](extractor, false, true, true, log4 _)
      .andThenExpandWithValue((c: Cell[Position1D], e: W) => c.position.append("entropy"))

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe ((1, 1.0/3 * log4(1.0/3)))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe ((1, 2.0/3 * log4(2.0/3)))

    val t3 = obj.prepareWithValue(cell3, count)
    t3._1 shouldBe 1
    t3._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe ((2, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe ((3, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val c = obj.presentWithValue(Position1D("foo"), r2, count)
    c shouldBe Some(Cell(Position2D("foo", "entropy"), getDoubleContent(2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))
  }

  it should "prepare, reduce and present expanded with non-strict, nan and non-negate with log" in {
    val obj = Entropy[Position2D, Position1D, W](extractor, false, true, false, log4 _)
      .andThenExpandWithValue((c: Cell[Position1D], e: W) => c.position.append("entropy"))

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe ((1, 1.0/3 * log4(1.0/3)))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe ((1, 2.0/3 * log4(2.0/3)))

    val t3 = obj.prepareWithValue(cell3, count)
    t3._1 shouldBe 1
    t3._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe ((2, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe ((3, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val c = obj.presentWithValue(Position1D("foo"), r2, count)
    c shouldBe Some(Cell(Position2D("foo", "entropy"), getDoubleContent(- (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3)))))
  }

  it should "prepare, reduce and present expanded with non-strict, non-nan and negate with log" in {
    val obj = Entropy[Position2D, Position1D, W](extractor, false, false, true, log4 _)
      .andThenExpandWithValue((c: Cell[Position1D], e: W) => c.position.append("entropy"))

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe ((1, 1.0/3 * log4(1.0/3)))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe ((1, 2.0/3 * log4(2.0/3)))

    val t3 = obj.prepareWithValue(cell3, count)
    t3._1 shouldBe 1
    t3._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe ((2, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe ((3, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val c = obj.presentWithValue(Position1D("foo"), r2, count)
    c shouldBe Some(Cell(Position2D("foo", "entropy"), getDoubleContent(2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))
  }

  it should "prepare, reduce and present expanded with non-strict, non-nan and non-negate with log" in {
    val obj = Entropy[Position2D, Position1D, W](extractor, false, false, false, log4 _)
      .andThenExpandWithValue((c: Cell[Position1D], e: W) => c.position.append("entropy"))

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe ((1, 1.0/3 * log4(1.0/3)))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe ((1, 2.0/3 * log4(2.0/3)))

    val t3 = obj.prepareWithValue(cell3, count)
    t3._1 shouldBe 1
    t3._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe ((2, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe ((3, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val c = obj.presentWithValue(Position1D("foo"), r2, count)
    c shouldBe Some(Cell(Position2D("foo", "entropy"), getDoubleContent(- (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3)))))
  }
}

