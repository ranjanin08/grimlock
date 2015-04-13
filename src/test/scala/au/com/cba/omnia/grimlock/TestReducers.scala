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
import au.com.cba.omnia.grimlock.reduce._
import au.com.cba.omnia.grimlock.utility._

import scala.collection.immutable.TreeMap

trait TestReducers extends TestGrimlock {
  def getLongContent(value: Long): Content = Content(DiscreteSchema[Codex.LongCodex](), value)
  def getDoubleContent(value: Double): Content = Content(ContinuousSchema[Codex.DoubleCodex](), value)
  def getStringContent(value: String): Content = Content(NominalSchema[Codex.StringCodex](), value)
}

class TestCount extends TestReducers {

  val cell1 = Cell(Position2D("foo", "one"), getDoubleContent(1))
  val cell2 = Cell(Position2D("foo", "two"), getDoubleContent(2))
  val slice = Over[Position2D, First.type](First)

  "A Count" should "prepare, reduce and present single" in {
    val obj = Count()

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe 1

    val r = obj.reduce(t1, t2)
    r shouldBe 2

    val c = obj.presentSingle(Position1D("foo"), r)
    c shouldBe Some(Cell(Position1D("foo"), getLongContent(2)))
  }

  it should "prepare, reduce and present multiple" in {
    val obj = Count("count")

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe 1

    val r = obj.reduce(t1, t2)
    r shouldBe 2

    val c = obj.presentMultiple(Position1D("foo"), r)
    c shouldBe Collection(Position2D("foo", "count"), getLongContent(2))
  }
}

class TestMean extends TestReducers {

  val cell1 = Cell(Position2D("foo", "one"), getDoubleContent(1))
  val cell2 = Cell(Position2D("foo", "two"), getDoubleContent(2))
  val cell3 = Cell(Position2D("foo", "bar"), getStringContent("bar"))
  val slice = Over[Position2D, First.type](First)

  "A Mean" should "prepare, reduce and present single" in {
    val obj = Mean()

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val r = obj.reduce(t1, t2)
    r shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.presentSingle(Position1D("foo"), r)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(1.5)))
  }

  it should "prepare, reduce and present single with strict and nan" in {
    val obj = Mean(true, true)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val c = obj.presentSingle(Position1D("foo"), r2)
    c.get.position shouldBe Position1D("foo")
    c.get.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Some(0)
  }

  it should "prepare, reduce and present single with strict and non-nan" in {
    val obj = Mean(true, false)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val c = obj.presentSingle(Position1D("foo"), r2)
    c shouldBe None
  }

  it should "prepare, reduce and present single with non-strict and nan" in {
    val obj = Mean(false, true)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.presentSingle(Position1D("foo"), r2)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(1.5)))
  }

  it should "prepare, reduce and present single with non-strict and non-nan" in {
    val obj = Mean(false, false)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.presentSingle(Position1D("foo"), r2)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(1.5)))
  }

  it should "prepare, reduce and present multiple" in {
    val obj = Mean("mean")

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val r = obj.reduce(t1, t2)
    r shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.presentMultiple(Position1D("foo"), r)
    c shouldBe Collection(List(Cell(Position2D("foo", "mean"), getDoubleContent(1.5))))
  }

  it should "prepare, reduce and present multiple with strict and nan" in {
    val obj = Mean("mean", true, true)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val c = obj.presentMultiple(Position1D("foo"), r2)
    c.toList()(0).position shouldBe Position2D("foo", "mean")
    c.toList()(0).content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Some(0)
  }

  it should "prepare, reduce and present multiple with strict and non-nan" in {
    val obj = Mean("mean", true, false)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val c = obj.presentMultiple(Position1D("foo"), r2)
    c shouldBe Collection()
  }

  it should "prepare, reduce and present multiple with non-strict and nan" in {
    val obj = Mean("mean", false, true)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.presentMultiple(Position1D("foo"), r2)
    c shouldBe Collection(List(Cell(Position2D("foo", "mean"), getDoubleContent(1.5))))
  }

  it should "prepare, reduce and present multiple with non-strict and non-nan" in {
    val obj = Mean("mean", false, false)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.presentMultiple(Position1D("foo"), r2)
    c shouldBe Collection(List(Cell(Position2D("foo", "mean"), getDoubleContent(1.5))))
  }
}

class TestStandardDeviation extends TestReducers {

  val cell1 = Cell(Position2D("foo", "one"), getDoubleContent(1))
  val cell2 = Cell(Position2D("foo", "two"), getDoubleContent(2))
  val cell3 = Cell(Position2D("foo", "bar"), getStringContent("bar"))
  val slice = Over[Position2D, First.type](First)

  "A StandardDeviation" should "prepare, reduce and present single" in {
    val obj = StandardDeviation()

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val r = obj.reduce(t1, t2)
    r shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.presentSingle(Position1D("foo"), r)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(0.5)))
  }

  it should "prepare, reduce and present single with strict and nan" in {
    val obj = StandardDeviation(true, true)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val c = obj.presentSingle(Position1D("foo"), r2)
    c.get.position shouldBe Position1D("foo")
    c.get.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Some(0)
  }

  it should "prepare, reduce and present single with strict and non-nan" in {
    val obj = StandardDeviation(true, false)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val c = obj.presentSingle(Position1D("foo"), r2)
    c shouldBe None
  }

  it should "prepare, reduce and present single with non-strict and nan" in {
    val obj = StandardDeviation(false, true)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.presentSingle(Position1D("foo"), r2)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(0.5)))
  }

  it should "prepare, reduce and present single with non-strict and non-nan" in {
    val obj = StandardDeviation(false, false)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.presentSingle(Position1D("foo"), r2)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(0.5)))
  }

  it should "prepare, reduce and present multiple" in {
    val obj = StandardDeviation("sd")

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val r = obj.reduce(t1, t2)
    r shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.presentMultiple(Position1D("foo"), r)
    c shouldBe Collection(List(Cell(Position2D("foo", "sd"), getDoubleContent(0.5))))
  }

  it should "prepare, reduce and present multiple with strict and nan" in {
    val obj = StandardDeviation("sd", true, true)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val c = obj.presentMultiple(Position1D("foo"), r2)
    c.toList()(0).position shouldBe Position2D("foo", "sd")
    c.toList()(0).content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Some(0)
  }

  it should "prepare, reduce and present multiple with strict and non-nan" in {
    val obj = StandardDeviation("sd", true, false)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val c = obj.presentMultiple(Position1D("foo"), r2)
    c shouldBe Collection()
  }

  it should "prepare, reduce and present multiple with non-strict and nan" in {
    val obj = StandardDeviation("sd", false, true)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.presentMultiple(Position1D("foo"), r2)
    c shouldBe Collection(List(Cell(Position2D("foo", "sd"), getDoubleContent(0.5))))
  }

  it should "prepare, reduce and present multiple with non-strict and non-nan" in {
    val obj = StandardDeviation("sd", false, false)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.presentMultiple(Position1D("foo"), r2)
    c shouldBe Collection(List(Cell(Position2D("foo", "sd"), getDoubleContent(0.5))))
  }
}

class TestSkewness extends TestReducers {

  val cell1 = Cell(Position2D("foo", "one"), getDoubleContent(1))
  val cell2 = Cell(Position2D("foo", "two"), getDoubleContent(2))
  val cell3 = Cell(Position2D("foo", "bar"), getStringContent("bar"))
  val slice = Over[Position2D, First.type](First)

  "A Skewness" should "prepare, reduce and present single" in {
    val obj = Skewness()

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val r = obj.reduce(t1, t2)
    r shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.presentSingle(Position1D("foo"), r)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(0)))
  }

  it should "prepare, reduce and present single with strict and nan" in {
    val obj = Skewness(true, true)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val c = obj.presentSingle(Position1D("foo"), r2)
    c.get.position shouldBe Position1D("foo")
    c.get.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Some(0)
  }

  it should "prepare, reduce and present single with strict and non-nan" in {
    val obj = Skewness(true, false)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val c = obj.presentSingle(Position1D("foo"), r2)
    c shouldBe None
  }

  it should "prepare, reduce and present single with non-strict and nan" in {
    val obj = Skewness(false, true)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.presentSingle(Position1D("foo"), r2)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(0)))
  }

  it should "prepare, reduce and present single with non-strict and non-nan" in {
    val obj = Skewness(false, false)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.presentSingle(Position1D("foo"), r2)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(0)))
  }

  it should "prepare, reduce and present multiple" in {
    val obj = Skewness("skewness")

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val r = obj.reduce(t1, t2)
    r shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.presentMultiple(Position1D("foo"), r)
    c shouldBe Collection(List(Cell(Position2D("foo", "skewness"), getDoubleContent(0))))
  }

  it should "prepare, reduce and present multiple with strict and nan" in {
    val obj = Skewness("skewness", true, true)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val c = obj.presentMultiple(Position1D("foo"), r2)
    c.toList()(0).position shouldBe Position2D("foo", "skewness")
    c.toList()(0).content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Some(0)
  }

  it should "prepare, reduce and present multiple with strict and non-nan" in {
    val obj = Skewness("skewness", true, false)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val c = obj.presentMultiple(Position1D("foo"), r2)
    c shouldBe Collection()
  }

  it should "prepare, reduce and present multiple with non-strict and nan" in {
    val obj = Skewness("skewness", false, true)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.presentMultiple(Position1D("foo"), r2)
    c shouldBe Collection(List(Cell(Position2D("foo", "skewness"), getDoubleContent(0))))
  }

  it should "prepare, reduce and present multiple with non-strict and non-nan" in {
    val obj = Skewness("skewness", false, false)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.presentMultiple(Position1D("foo"), r2)
    c shouldBe Collection(List(Cell(Position2D("foo", "skewness"), getDoubleContent(0))))
  }
}

class TestKurtosis extends TestReducers {

  val cell1 = Cell(Position2D("foo", "one"), getDoubleContent(1))
  val cell2 = Cell(Position2D("foo", "two"), getDoubleContent(2))
  val cell3 = Cell(Position2D("foo", "bar"), getStringContent("bar"))
  val slice = Over[Position2D, First.type](First)

  "A Kurtosis" should "prepare, reduce and present single" in {
    val obj = Kurtosis()

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val r = obj.reduce(t1, t2)
    r shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.presentSingle(Position1D("foo"), r)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(-2)))
  }

  it should "prepare, reduce and present single with strict and nan" in {
    val obj = Kurtosis(true, true)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val c = obj.presentSingle(Position1D("foo"), r2)
    c.get.position shouldBe Position1D("foo")
    c.get.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Some(0)
  }

  it should "prepare, reduce and present single with strict and non-nan" in {
    val obj = Kurtosis(true, false)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val c = obj.presentSingle(Position1D("foo"), r2)
    c shouldBe None
  }

  it should "prepare, reduce and present single with non-strict and nan" in {
    val obj = Kurtosis(false, true)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.presentSingle(Position1D("foo"), r2)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(-2)))
  }

  it should "prepare, reduce and present single with non-strict and non-nan" in {
    val obj = Kurtosis(false, false)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.presentSingle(Position1D("foo"), r2)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(-2)))
  }

  it should "prepare, reduce and present multiple" in {
    val obj = Kurtosis("kurtosis")

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val r = obj.reduce(t1, t2)
    r shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.presentMultiple(Position1D("foo"), r)
    c shouldBe Collection(List(Cell(Position2D("foo", "kurtosis"), getDoubleContent(-2))))
  }

  it should "prepare, reduce and present multiple with strict and nan" in {
    val obj = Kurtosis("kurtosis", true, true)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val c = obj.presentMultiple(Position1D("foo"), r2)
    c.toList()(0).position shouldBe Position2D("foo", "kurtosis")
    c.toList()(0).content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Some(0)
  }

  it should "prepare, reduce and present multiple with strict and non-nan" in {
    val obj = Kurtosis("kurtosis", true, false)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val c = obj.presentMultiple(Position1D("foo"), r2)
    c shouldBe Collection()
  }

  it should "prepare, reduce and present multiple with non-strict and nan" in {
    val obj = Kurtosis("kurtosis", false, true)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.presentMultiple(Position1D("foo"), r2)
    c shouldBe Collection(List(Cell(Position2D("foo", "kurtosis"), getDoubleContent(-2))))
  }

  it should "prepare, reduce and present multiple with non-strict and non-nan" in {
    val obj = Kurtosis("kurtosis", false, false)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.presentMultiple(Position1D("foo"), r2)
    c shouldBe Collection(List(Cell(Position2D("foo", "kurtosis"), getDoubleContent(-2))))
  }
}

class TestMoments extends TestReducers {

  val cell1 = Cell(Position2D("foo", "one"), getDoubleContent(1))
  val cell2 = Cell(Position2D("foo", "two"), getDoubleContent(2))
  val cell3 = Cell(Position2D("foo", "bar"), getStringContent("bar"))
  val slice = Over[Position2D, First.type](First)

  "A Moments" should "prepare, reduce and present single mean" in {
    val obj = Moments(Mean)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val r = obj.reduce(t1, t2)
    r shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.presentSingle(Position1D("foo"), r)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(1.5)))
  }

  it should "prepare, reduce and present single standard deviation" in {
    val obj = Moments(StandardDeviation)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val r = obj.reduce(t1, t2)
    r shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.presentSingle(Position1D("foo"), r)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(0.5)))
  }

  it should "prepare, reduce and present single skewness" in {
    val obj = Moments(Skewness)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val r = obj.reduce(t1, t2)
    r shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.presentSingle(Position1D("foo"), r)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(0.0)))
  }

  it should "prepare, reduce and present single kurtosis" in {
    val obj = Moments(Kurtosis)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val r = obj.reduce(t1, t2)
    r shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.presentSingle(Position1D("foo"), r)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(-2)))
  }

  it should "prepare, reduce and present single mean with strict and nan" in {
    val obj = Moments(Mean, true, true)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val c = obj.presentSingle(Position1D("foo"), r2)
    c.get.position shouldBe Position1D("foo")
    c.get.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Some(0)
  }

  it should "prepare, reduce and present single standard deviation with strict and non-nan" in {
    val obj = Moments(StandardDeviation, true, false)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val c = obj.presentSingle(Position1D("foo"), r2)
    c shouldBe None
  }

  it should "prepare, reduce and present single skewness with non-strict and nan" in {
    val obj = Moments(Skewness, false, true)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.presentSingle(Position1D("foo"), r2)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(0)))
  }

  it should "prepare, reduce and present single kurtosis with non-strict and non-nan" in {
    val obj = Moments(Kurtosis, false, false)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.presentSingle(Position1D("foo"), r2)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(-2)))
  }

  it should "prepare, reduce and present one moment as multiple" in {
    val obj = Moments((Kurtosis, "kurtosis"))

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val r = obj.reduce(t1, t2)
    r shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.presentMultiple(Position1D("foo"), r)
    c shouldBe Collection(List(Cell(Position2D("foo", "kurtosis"), getDoubleContent(-2))))
  }

  it should "prepare, reduce and present two moments as multiple" in {
    val obj = Moments((Kurtosis, "kurtosis"), (Skewness, "skewness"))

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val r = obj.reduce(t1, t2)
    r shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.presentMultiple(Position1D("foo"), r)
    c shouldBe Collection(List(Cell(Position2D("foo", "kurtosis"), getDoubleContent(-2)),
      Cell(Position2D("foo", "skewness"), getDoubleContent(0))))
  }

  it should "prepare, reduce and present three moments as multiple" in {
    val obj = Moments((Kurtosis, "kurtosis"), (Skewness, "skewness"), (Mean, "mean"))

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val r = obj.reduce(t1, t2)
    r shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.presentMultiple(Position1D("foo"), r)
    c shouldBe Collection(List(Cell(Position2D("foo", "kurtosis"), getDoubleContent(-2)),
      Cell(Position2D("foo", "skewness"), getDoubleContent(0)),
      Cell(Position2D("foo", "mean"), getDoubleContent(1.5))))
  }

  it should "prepare, reduce and present four moments as multiple" in {
    val obj = Moments((Kurtosis, "kurtosis"), (Skewness, "skewness"), (Mean, "mean"), (StandardDeviation, "sd"))

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val r = obj.reduce(t1, t2)
    r shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.presentMultiple(Position1D("foo"), r)
    c shouldBe Collection(List(Cell(Position2D("foo", "kurtosis"), getDoubleContent(-2)),
      Cell(Position2D("foo", "skewness"), getDoubleContent(0)),
      Cell(Position2D("foo", "mean"), getDoubleContent(1.5)),
      Cell(Position2D("foo", "sd"), getDoubleContent(0.5))))
  }

  it should "prepare, reduce and present one moment as multiple with strict and nan" in {
    val obj = Moments((Skewness, "skewness"), true, true)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val c = obj.presentMultiple(Position1D("foo"), r2)
    c.toList()(0).position shouldBe Position2D("foo", "skewness")
    c.toList()(0).content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Some(0)
  }

  it should "prepare, reduce and present multiple with strict and non-nan" in {
    val obj = Moments((Skewness, "skewness"), (StandardDeviation, "sd"), true, false)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val c = obj.presentMultiple(Position1D("foo"), r2)
    c shouldBe Collection()
  }

  it should "prepare, reduce and present multiple with non-strict and nan" in {
    val obj = Moments((Skewness, "skewness"), (StandardDeviation, "sd"), (Kurtosis, "kurtosis"), false, true)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.presentMultiple(Position1D("foo"), r2)
    c shouldBe Collection(List(Cell(Position2D("foo", "skewness"), getDoubleContent(0)),
      Cell(Position2D("foo", "sd"), getDoubleContent(0.5)),
      Cell(Position2D("foo", "kurtosis"), getDoubleContent(-2))))
  }

  it should "prepare, reduce and present multiple with non-strict and non-nan" in {
    val obj = Moments((Skewness, "skewness"), (StandardDeviation, "sd"), (Kurtosis, "kurtosis"), (Mean, "mean"),
      false, false)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.presentMultiple(Position1D("foo"), r2)
    c shouldBe Collection(List(Cell(Position2D("foo", "skewness"), getDoubleContent(0)),
      Cell(Position2D("foo", "sd"), getDoubleContent(0.5)),
      Cell(Position2D("foo", "kurtosis"), getDoubleContent(-2)),
      Cell(Position2D("foo", "mean"), getDoubleContent(1.5))))
  }

  it should "prepare, reduce and present mean as multiple" in {
    val obj = Moments("mean")

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val r = obj.reduce(t1, t2)
    r shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.presentMultiple(Position1D("foo"), r)
    c shouldBe Collection(List(Cell(Position2D("foo", "mean"), getDoubleContent(1.5))))
  }

  it should "prepare, reduce and present mean and standard deviation as multiple" in {
    val obj = Moments("mean", "sd")

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val r = obj.reduce(t1, t2)
    r shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.presentMultiple(Position1D("foo"), r)
    c shouldBe Collection(List(Cell(Position2D("foo", "mean"), getDoubleContent(1.5)),
      Cell(Position2D("foo", "sd"), getDoubleContent(0.5))))
  }

  it should "prepare, reduce and present mean, standard deviation and skewness as multiple" in {
    val obj = Moments("mean", "sd", "skewness")

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val r = obj.reduce(t1, t2)
    r shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.presentMultiple(Position1D("foo"), r)
    c shouldBe Collection(List(Cell(Position2D("foo", "mean"), getDoubleContent(1.5)),
      Cell(Position2D("foo", "sd"), getDoubleContent(0.5)),
      Cell(Position2D("foo", "skewness"), getDoubleContent(0))))
  }

  it should "prepare, reduce and present mean, standard deviation, skewness and kurtosis as multiple" in {
    val obj = Moments("mean", "sd", "skewness", "kurtosis")

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val r = obj.reduce(t1, t2)
    r shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.presentMultiple(Position1D("foo"), r)
    c shouldBe Collection(List(Cell(Position2D("foo", "mean"), getDoubleContent(1.5)),
      Cell(Position2D("foo", "sd"), getDoubleContent(0.5)),
      Cell(Position2D("foo", "skewness"), getDoubleContent(0)),
      Cell(Position2D("foo", "kurtosis"), getDoubleContent(-2))))
  }

  it should "prepare, reduce and present mean as multiple with strict and nan" in {
    val obj = Moments("mean", true, true)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val c = obj.presentMultiple(Position1D("foo"), r2)
    c.toList()(0).position shouldBe Position2D("foo", "mean")
    c.toList()(0).content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Some(0)
  }

  it should "prepare, reduce and present mean and standard deviation as multiple with strict and non-nan" in {
    val obj = Moments("mean", "sd", true, false)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val c = obj.presentMultiple(Position1D("foo"), r2)
    c shouldBe Collection()
  }

  it should "prepare, reduce and present mean, standard deviation and skewness as multiple with non-strict and nan" in {
    val obj = Moments("mean", "sd", "skewness", false, true)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.presentMultiple(Position1D("foo"), r2)
    c shouldBe Collection(List(Cell(Position2D("foo", "mean"), getDoubleContent(1.5)),
      Cell(Position2D("foo", "sd"), getDoubleContent(0.5)),
      Cell(Position2D("foo", "skewness"), getDoubleContent(0))))
  }

  it should "prepare, reduce and present all four moments as multiple with non-strict and non-nan" in {
    val obj = Moments("mean", "sd", "skewness", "kurtosis", false, false)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe com.twitter.algebird.Moments(1)

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe com.twitter.algebird.Moments(2)

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[com.twitter.algebird.Moments].mean.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.presentMultiple(Position1D("foo"), r2)
    c shouldBe Collection(List(Cell(Position2D("foo", "mean"), getDoubleContent(1.5)),
      Cell(Position2D("foo", "sd"), getDoubleContent(0.5)),
      Cell(Position2D("foo", "skewness"), getDoubleContent(0)),
      Cell(Position2D("foo", "kurtosis"), getDoubleContent(-2))))
  }
}

class TestMin extends TestReducers {

  val cell1 = Cell(Position2D("foo", "one"), getDoubleContent(1))
  val cell2 = Cell(Position2D("foo", "two"), getDoubleContent(2))
  val cell3 = Cell(Position2D("foo", "bar"), getStringContent("bar"))
  val slice = Over[Position2D, First.type](First)

  "A Min" should "prepare, reduce and present single" in {
    val obj = Min()

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe 2

    val r = obj.reduce(t1, t2)
    r shouldBe 1

    val c = obj.presentSingle(Position1D("foo"), r)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(1)))
  }

  it should "prepare, reduce and present single with strict and nan" in {
    val obj = Min(true, true)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe 2

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 1

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val c = obj.presentSingle(Position1D("foo"), r2)
    c.get.position shouldBe Position1D("foo")
    c.get.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Some(0)
  }

  it should "prepare, reduce and present single with strict and non-nan" in {
    val obj = Min(true, false)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe 2

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 1

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val c = obj.presentSingle(Position1D("foo"), r2)
    c shouldBe None
  }

  it should "prepare, reduce and present single with non-strict and nan" in {
    val obj = Min(false, true)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe 2

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 1

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe 1

    val c = obj.presentSingle(Position1D("foo"), r2)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(1)))
  }

  it should "prepare, reduce and present single with non-strict and non-nan" in {
    val obj = Min(false, false)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe 2

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 1

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe 1

    val c = obj.presentSingle(Position1D("foo"), r2)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(1)))
  }

  it should "prepare, reduce and present multiple" in {
    val obj = Min("min")

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe 2

    val r = obj.reduce(t1, t2)
    r shouldBe 1

    val c = obj.presentMultiple(Position1D("foo"), r)
    c shouldBe Collection(Position2D("foo", "min"), getDoubleContent(1))
  }

  it should "prepare, reduce and present multiple with strict and nan" in {
    val obj = Min("min", true, true)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe 2

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 1

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val c = obj.presentMultiple(Position1D("foo"), r2)
    c.toList()(0).position shouldBe Position2D("foo", "min")
    c.toList()(0).content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Some(0)
  }

  it should "prepare, reduce and present multiple with strict and non-nan" in {
    val obj = Min("min", true, false)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe 2

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 1

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val c = obj.presentMultiple(Position1D("foo"), r2)
    c shouldBe Collection()
  }

  it should "prepare, reduce and present multiple with non-strict and nan" in {
    val obj = Min("min", false, true)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe 2

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 1

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe 1

    val c = obj.presentMultiple(Position1D("foo"), r2)
    c shouldBe Collection(Position2D("foo", "min"), getDoubleContent(1))
  }

  it should "prepare, reduce and present multiple with non-strict and non-nan" in {
    val obj = Min("min", false, false)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe 2

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 1

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe 1

    val c = obj.presentMultiple(Position1D("foo"), r2)
    c shouldBe Collection(Position2D("foo", "min"), getDoubleContent(1))
  }
}

class TestMax extends TestReducers {

  val cell1 = Cell(Position2D("foo", "one"), getDoubleContent(1))
  val cell2 = Cell(Position2D("foo", "two"), getDoubleContent(2))
  val cell3 = Cell(Position2D("foo", "bar"), getStringContent("bar"))
  val slice = Over[Position2D, First.type](First)

  "A Max" should "prepare, reduce and present single" in {
    val obj = Max()

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe 2

    val r = obj.reduce(t1, t2)
    r shouldBe 2

    val c = obj.presentSingle(Position1D("foo"), r)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(2)))
  }

  it should "prepare, reduce and present single with strict and nan" in {
    val obj = Max(true, true)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe 2

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 2

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val c = obj.presentSingle(Position1D("foo"), r2)
    c.get.position shouldBe Position1D("foo")
    c.get.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Some(0)
  }

  it should "prepare, reduce and present single with strict and non-nan" in {
    val obj = Max(true, false)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe 2

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 2

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val c = obj.presentSingle(Position1D("foo"), r2)
    c shouldBe None
  }

  it should "prepare, reduce and present single with non-strict and nan" in {
    val obj = Max(false, true)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe 2

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 2

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe 2

    val c = obj.presentSingle(Position1D("foo"), r2)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(2)))
  }

  it should "prepare, reduce and present single with non-strict and non-nan" in {
    val obj = Max(false, false)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe 2

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 2

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe 2

    val c = obj.presentSingle(Position1D("foo"), r2)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(2)))
  }

  it should "prepare, reduce and present multiple" in {
    val obj = Max("max")

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe 2

    val r = obj.reduce(t1, t2)
    r shouldBe 2

    val c = obj.presentMultiple(Position1D("foo"), r)
    c shouldBe Collection(Position2D("foo", "max"), getDoubleContent(2))
  }

  it should "prepare, reduce and present multiple with strict and nan" in {
    val obj = Max("max", true, true)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe 2

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 2

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val c = obj.presentMultiple(Position1D("foo"), r2)
    c.toList()(0).position shouldBe Position2D("foo", "max")
    c.toList()(0).content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Some(0)
  }

  it should "prepare, reduce and present multiple with strict and non-nan" in {
    val obj = Max("max", true, false)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe 2

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 2

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val c = obj.presentMultiple(Position1D("foo"), r2)
    c shouldBe Collection()
  }

  it should "prepare, reduce and present multiple with non-strict and nan" in {
    val obj = Max("max", false, true)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe 2

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 2

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe 2

    val c = obj.presentMultiple(Position1D("foo"), r2)
    c shouldBe Collection(Position2D("foo", "max"), getDoubleContent(2))
  }

  it should "prepare, reduce and present multiple with non-strict and non-nan" in {
    val obj = Max("max", false, false)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe 2

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 2

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe 2

    val c = obj.presentMultiple(Position1D("foo"), r2)
    c shouldBe Collection(Position2D("foo", "max"), getDoubleContent(2))
  }
}

class TestMaxAbs extends TestReducers {

  val cell1 = Cell(Position2D("foo", "one"), getDoubleContent(1))
  val cell2 = Cell(Position2D("foo", "two"), getDoubleContent(-2))
  val cell3 = Cell(Position2D("foo", "bar"), getStringContent("bar"))
  val slice = Over[Position2D, First.type](First)

  "A MaxAbs" should "prepare, reduce and present single" in {
    val obj = MaxAbs()

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe -2

    val r = obj.reduce(t1, t2)
    r shouldBe 2

    val c = obj.presentSingle(Position1D("foo"), r)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(2)))
  }

  it should "prepare, reduce and present single with strict and nan" in {
    val obj = MaxAbs(true, true)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe -2

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 2

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val c = obj.presentSingle(Position1D("foo"), r2)
    c.get.position shouldBe Position1D("foo")
    c.get.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Some(0)
  }

  it should "prepare, reduce and present single with strict and non-nan" in {
    val obj = MaxAbs(true, false)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe -2

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 2

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val c = obj.presentSingle(Position1D("foo"), r2)
    c shouldBe None
  }

  it should "prepare, reduce and present single with non-strict and nan" in {
    val obj = MaxAbs(false, true)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe -2

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 2

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe 2

    val c = obj.presentSingle(Position1D("foo"), r2)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(2)))
  }

  it should "prepare, reduce and present single with non-strict and non-nan" in {
    val obj = MaxAbs(false, false)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe -2

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 2

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe 2

    val c = obj.presentSingle(Position1D("foo"), r2)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(2)))
  }

  it should "prepare, reduce and present multiple" in {
    val obj = MaxAbs("max.abs")

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe -2

    val r = obj.reduce(t1, t2)
    r shouldBe 2

    val c = obj.presentMultiple(Position1D("foo"), r)
    c shouldBe Collection(Position2D("foo", "max.abs"), getDoubleContent(2))
  }

  it should "prepare, reduce and present multiple with strict and nan" in {
    val obj = MaxAbs("max.abs", true, true)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe -2

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 2

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val c = obj.presentMultiple(Position1D("foo"), r2)
    c.toList()(0).position shouldBe Position2D("foo", "max.abs")
    c.toList()(0).content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Some(0)
  }

  it should "prepare, reduce and present multiple with strict and non-nan" in {
    val obj = MaxAbs("max.abs", true, false)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe -2

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 2

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val c = obj.presentMultiple(Position1D("foo"), r2)
    c shouldBe Collection()
  }

  it should "prepare, reduce and present multiple with non-strict and nan" in {
    val obj = MaxAbs("max.abs", false, true)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe -2

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 2

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe 2

    val c = obj.presentMultiple(Position1D("foo"), r2)
    c shouldBe Collection(Position2D("foo", "max.abs"), getDoubleContent(2))
  }

  it should "prepare, reduce and present multiple with non-strict and non-nan" in {
    val obj = MaxAbs("max.abs", false, false)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe -2

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 2

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe 2

    val c = obj.presentMultiple(Position1D("foo"), r2)
    c shouldBe Collection(Position2D("foo", "max.abs"), getDoubleContent(2))
  }
}

class TestSum extends TestReducers {

  val cell1 = Cell(Position2D("foo", "one"), getDoubleContent(1))
  val cell2 = Cell(Position2D("foo", "two"), getDoubleContent(2))
  val cell3 = Cell(Position2D("foo", "bar"), getStringContent("bar"))
  val slice = Over[Position2D, First.type](First)

  "A Sum" should "prepare, reduce and present single" in {
    val obj = Sum()

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe 2

    val r = obj.reduce(t1, t2)
    r shouldBe 3

    val c = obj.presentSingle(Position1D("foo"), r)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(3)))
  }

  it should "prepare, reduce and present single with strict and nan" in {
    val obj = Sum(true, true)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe 2

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 3

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val c = obj.presentSingle(Position1D("foo"), r2)
    c.get.position shouldBe Position1D("foo")
    c.get.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Some(0)
  }

  it should "prepare, reduce and present single with strict and non-nan" in {
    val obj = Sum(true, false)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe 2

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 3

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val c = obj.presentSingle(Position1D("foo"), r2)
    c shouldBe None
  }

  it should "prepare, reduce and present single with non-strict and nan" in {
    val obj = Sum(false, true)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe 2

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 3

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe 3

    val c = obj.presentSingle(Position1D("foo"), r2)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(3)))
  }

  it should "prepare, reduce and present single with non-strict and non-nan" in {
    val obj = Sum(false, false)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe 2

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 3

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe 3

    val c = obj.presentSingle(Position1D("foo"), r2)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(3)))
  }

  it should "prepare, reduce and present multiple" in {
    val obj = Sum("sum")

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe 2

    val r = obj.reduce(t1, t2)
    r shouldBe 3

    val c = obj.presentMultiple(Position1D("foo"), r)
    c shouldBe Collection(Position2D("foo", "sum"), getDoubleContent(3))
  }

  it should "prepare, reduce and present multiple with strict and nan" in {
    val obj = Sum("sum", true, true)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe 2

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 3

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val c = obj.presentMultiple(Position1D("foo"), r2)
    c.toList()(0).position shouldBe Position2D("foo", "sum")
    c.toList()(0).content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Some(0)
  }

  it should "prepare, reduce and present multiple with strict and non-nan" in {
    val obj = Sum("sum", true, false)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe 2

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 3

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val c = obj.presentMultiple(Position1D("foo"), r2)
    c shouldBe Collection()
  }

  it should "prepare, reduce and present multiple with non-strict and nan" in {
    val obj = Sum("sum", false, true)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe 2

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 3

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe 3

    val c = obj.presentMultiple(Position1D("foo"), r2)
    c shouldBe Collection(Position2D("foo", "sum"), getDoubleContent(3))
  }

  it should "prepare, reduce and present multiple with non-strict and non-nan" in {
    val obj = Sum("sum", false, false)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe 1

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe 2

    val t3 = obj.prepare(slice, cell3)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 3

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe 3

    val c = obj.presentMultiple(Position1D("foo"), r2)
    c shouldBe Collection(Position2D("foo", "sum"), getDoubleContent(3))
  }
}

class TestHistogram extends TestReducers {

  val cell1 = Cell(Position2D("foo", 1), getStringContent("abc"))
  val cell2 = Cell(Position2D("foo", 2), getStringContent("xyz"))
  val cell3 = Cell(Position2D("foo", 3), getStringContent("abc"))
  val cell4 = Cell(Position2D("foo", 4), getDoubleContent(3.14))
  val slice = Over[Position2D, First.type](First)

  "A Histogram" should "prepare, reduce and present single" in {
    val obj = Histogram("%1$s=%2$s")

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map("abc" -> 1))

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map("abc" -> 2, "xyz" -> 1))

    val c = obj.presentMultiple(Position2D("foo", "bar"), r2)
    c shouldBe Collection(List(Cell(Position3D("foo", "bar", "foo|bar=abc"), getLongContent(2)),
      Cell(Position3D("foo", "bar", "foo|bar=xyz"), getLongContent(1))))
  }

  it should "prepare, reduce and present single with separator" in {
    val obj = Histogram("%1$s=%2$s", ".")

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map("abc" -> 1))

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map("abc" -> 2, "xyz" -> 1))

    val c = obj.presentMultiple(Position2D("foo", "bar"), r2)
    c shouldBe Collection(List(Cell(Position3D("foo", "bar", "foo.bar=abc"), getLongContent(2)),
      Cell(Position3D("foo", "bar", "foo.bar=xyz"), getLongContent(1))))
  }

  it should "prepare, reduce and present single with strict, all and frequency" in {
    val obj = Histogram("%1$s=%2$s", true, true, true)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map("abc" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe Some(Map("3.14" -> 1))

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map("abc" -> 2, "xyz" -> 1))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe Some(Map("abc" -> 2, "xyz" -> 1, "3.14" -> 1))

    val c = obj.presentMultiple(Position2D("foo", "bar"), r3)
    c shouldBe Collection(List(Cell(Position3D("foo", "bar", "foo|bar=abc"), getLongContent(2)),
      Cell(Position3D("foo", "bar", "foo|bar=xyz"), getLongContent(1)),
      Cell(Position3D("foo", "bar", "foo|bar=3.14"), getLongContent(1))))
  }

  it should "prepare, reduce and present single with strict, all and non-frequency" in {
    val obj = Histogram("%1$s=%2$s", true, true, false)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map("abc" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe Some(Map("3.14" -> 1))

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map("abc" -> 2, "xyz" -> 1))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe Some(Map("abc" -> 2, "xyz" -> 1, "3.14" -> 1))

    val c = obj.presentMultiple(Position2D("foo", "bar"), r3)
    c shouldBe Collection(List(Cell(Position3D("foo", "bar", "foo|bar=abc"), getDoubleContent(2.0 / 4.0)),
      Cell(Position3D("foo", "bar", "foo|bar=xyz"), getDoubleContent(1.0 / 4.0)),
      Cell(Position3D("foo", "bar", "foo|bar=3.14"), getDoubleContent(1.0 / 4.0))))
  }

  it should "prepare, reduce and present single with strict, non-all and frequency" in {
    val obj = Histogram("%1$s=%2$s", true, false, true)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map("abc" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe None

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map("abc" -> 2, "xyz" -> 1))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe None

    val c = obj.presentMultiple(Position2D("foo", "bar"), r3)
    c shouldBe Collection()
  }

  it should "prepare, reduce and present single with strict, non-all and non-frequency" in {
    val obj = Histogram("%1$s=%2$s", true, false, false)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map("abc" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe None

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map("abc" -> 2, "xyz" -> 1))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe None

    val c = obj.presentMultiple(Position2D("foo", "bar"), r3)
    c shouldBe Collection()
  }

  it should "prepare, reduce and present single with non-strict, all and frequency" in {
    val obj = Histogram("%1$s=%2$s", false, true, true)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map("abc" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe Some(Map("3.14" -> 1))

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map("abc" -> 2, "xyz" -> 1))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe Some(Map("abc" -> 2, "xyz" -> 1, "3.14" -> 1))

    val c = obj.presentMultiple(Position2D("foo", "bar"), r3)
    c shouldBe Collection(List(Cell(Position3D("foo", "bar", "foo|bar=abc"), getLongContent(2)),
      Cell(Position3D("foo", "bar", "foo|bar=xyz"), getLongContent(1)),
      Cell(Position3D("foo", "bar", "foo|bar=3.14"), getLongContent(1))))
  }

  it should "prepare, reduce and present single with non-strict, all and non-frequency" in {
    val obj = Histogram("%1$s=%2$s", false, true, false)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map("abc" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe Some(Map("3.14" -> 1))

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map("abc" -> 2, "xyz" -> 1))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe Some(Map("abc" -> 2, "xyz" -> 1, "3.14" -> 1))

    val c = obj.presentMultiple(Position2D("foo", "bar"), r3)
    c shouldBe Collection(List(Cell(Position3D("foo", "bar", "foo|bar=abc"), getDoubleContent(2.0 / 4.0)),
      Cell(Position3D("foo", "bar", "foo|bar=xyz"), getDoubleContent(1.0 / 4.0)),
      Cell(Position3D("foo", "bar", "foo|bar=3.14"), getDoubleContent(1.0 / 4.0))))
  }

  it should "prepare, reduce and present single with non-strict, non-all and frequency" in {
    val obj = Histogram("%1$s=%2$s", false, false, true)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map("abc" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe None

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map("abc" -> 2, "xyz" -> 1))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe Some(Map("abc" -> 2, "xyz" -> 1))

    val c = obj.presentMultiple(Position2D("foo", "bar"), r3)
    c shouldBe Collection(List(Cell(Position3D("foo", "bar", "foo|bar=abc"), getLongContent(2)),
      Cell(Position3D("foo", "bar", "foo|bar=xyz"), getLongContent(1))))
  }

  it should "prepare, reduce and present single with non-strict, non-all and non-frequency" in {
    val obj = Histogram("%1$s=%2$s", false, false, false)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map("abc" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe None

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map("abc" -> 2, "xyz" -> 1))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe Some(Map("abc" -> 2, "xyz" -> 1))

    val c = obj.presentMultiple(Position2D("foo", "bar"), r3)
    c shouldBe Collection(List(Cell(Position3D("foo", "bar", "foo|bar=abc"), getDoubleContent(2.0 / 3.0)),
      Cell(Position3D("foo", "bar", "foo|bar=xyz"), getDoubleContent(1.0 / 3.0))))
  }

  it should "prepare, reduce and present single with non-strict, all, non-frequency and separator" in {
    val obj = Histogram("%1$s=%2$s", false, true, false, ".")

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map("abc" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe Some(Map("3.14" -> 1))

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map("abc" -> 2, "xyz" -> 1))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe Some(Map("abc" -> 2, "xyz" -> 1, "3.14" -> 1))

    val c = obj.presentMultiple(Position2D("foo", "bar"), r3)
    c shouldBe Collection(List(Cell(Position3D("foo", "bar", "foo.bar=abc"), getDoubleContent(2.0 / 4.0)),
      Cell(Position3D("foo", "bar", "foo.bar=xyz"), getDoubleContent(1.0 / 4.0)),
      Cell(Position3D("foo", "bar", "foo.bar=3.14"), getDoubleContent(1.0 / 4.0))))
  }

  def log2(x: Double) = math.log(x) / math.log(2)

  it should "prepare, reduce and present single with statistics" in {
    val obj = Histogram("%1$s=%2$s", List(Histogram.numberOfCategories("num.cat"), Histogram.entropy("entropy"),
      Histogram.frequencyRatio("freq.ratio")))

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map("abc" -> 1))

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map("abc" -> 2, "xyz" -> 1))

    val c = obj.presentMultiple(Position2D("foo", "bar"), r2)
    c shouldBe Collection(List(Cell(Position3D("foo", "bar", "num.cat"), getLongContent(2)),
      Cell(Position3D("foo", "bar", "entropy"), getDoubleContent(- (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3)))),
      Cell(Position3D("foo", "bar", "freq.ratio"), getDoubleContent(2)),
      Cell(Position3D("foo", "bar", "foo|bar=abc"), getLongContent(2)),
      Cell(Position3D("foo", "bar", "foo|bar=xyz"), getLongContent(1))))
  }

  it should "prepare, reduce and present single with separator with statistics" in {
    val obj = Histogram("%1$s=%2$s", List(Histogram.numberOfCategories("num.cat"), Histogram.entropy("entropy"),
      Histogram.frequencyRatio("freq.ratio")), ".")

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map("abc" -> 1))

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map("abc" -> 2, "xyz" -> 1))

    val c = obj.presentMultiple(Position2D("foo", "bar"), r2)
    c shouldBe Collection(List(Cell(Position3D("foo", "bar", "num.cat"), getLongContent(2)),
      Cell(Position3D("foo", "bar", "entropy"), getDoubleContent(- (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3)))),
      Cell(Position3D("foo", "bar", "freq.ratio"), getDoubleContent(2)),
      Cell(Position3D("foo", "bar", "foo.bar=abc"), getLongContent(2)),
      Cell(Position3D("foo", "bar", "foo.bar=xyz"), getLongContent(1))))
  }

  it should "prepare, reduce and present single with strict, all and frequency with statistics" in {
    val obj = Histogram("%1$s=%2$s", List(Histogram.numberOfCategories("num.cat"), Histogram.entropy("entropy"),
      Histogram.frequencyRatio("freq.ratio")), true, true, true)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map("abc" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe Some(Map("3.14" -> 1))

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map("abc" -> 2, "xyz" -> 1))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe Some(Map("abc" -> 2, "xyz" -> 1, "3.14" -> 1))

    val c = obj.presentMultiple(Position2D("foo", "bar"), r3)
    c shouldBe Collection(List(Cell(Position3D("foo", "bar", "num.cat"), getLongContent(3)),
      Cell(Position3D("foo", "bar", "entropy"),
        getDoubleContent(- (2.0/4 * log2(2.0/4) + 1.0/4 * log2(1.0/4) + 1.0/4 * log2(1.0/4)))),
      Cell(Position3D("foo", "bar", "freq.ratio"), getDoubleContent(2)),
      Cell(Position3D("foo", "bar", "foo|bar=abc"), getLongContent(2)),
      Cell(Position3D("foo", "bar", "foo|bar=xyz"), getLongContent(1)),
      Cell(Position3D("foo", "bar", "foo|bar=3.14"), getLongContent(1))))
  }

  it should "prepare, reduce and present single with strict, all and non-frequency with statistics" in {
    val obj = Histogram("%1$s=%2$s", List(Histogram.numberOfCategories("num.cat"), Histogram.entropy("entropy"),
      Histogram.frequencyRatio("freq.ratio")), true, true, false)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map("abc" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe Some(Map("3.14" -> 1))

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map("abc" -> 2, "xyz" -> 1))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe Some(Map("abc" -> 2, "xyz" -> 1, "3.14" -> 1))

    val c = obj.presentMultiple(Position2D("foo", "bar"), r3)
    c shouldBe Collection(List(Cell(Position3D("foo", "bar", "num.cat"), getLongContent(3)),
      Cell(Position3D("foo", "bar", "entropy"),
        getDoubleContent(- (2.0/4 * log2(2.0/4) + 1.0/4 * log2(1.0/4) + 1.0/4 * log2(1.0/4)))),
      Cell(Position3D("foo", "bar", "freq.ratio"), getDoubleContent(2)),
      Cell(Position3D("foo", "bar", "foo|bar=abc"), getDoubleContent(2.0 / 4.0)),
      Cell(Position3D("foo", "bar", "foo|bar=xyz"), getDoubleContent(1.0 / 4.0)),
      Cell(Position3D("foo", "bar", "foo|bar=3.14"), getDoubleContent(1.0 / 4.0))))
  }

  it should "prepare, reduce and present single with strict, non-all and frequency with statistics" in {
    val obj = Histogram("%1$s=%2$s", List(Histogram.numberOfCategories("num.cat"), Histogram.entropy("entropy"),
      Histogram.frequencyRatio("freq.ratio")), true, false, true)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map("abc" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe None

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map("abc" -> 2, "xyz" -> 1))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe None

    val c = obj.presentMultiple(Position2D("foo", "bar"), r3)
    c shouldBe Collection()
  }

  it should "prepare, reduce and present single with strict, non-all and non-frequency with statistics" in {
    val obj = Histogram("%1$s=%2$s", List(Histogram.numberOfCategories("num.cat"), Histogram.entropy("entropy"),
      Histogram.frequencyRatio("freq.ratio")), true, false, false)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map("abc" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe None

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map("abc" -> 2, "xyz" -> 1))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe None

    val c = obj.presentMultiple(Position2D("foo", "bar"), r3)
    c shouldBe Collection()
  }

  it should "prepare, reduce and present single with non-strict, all and frequency with statistics" in {
    val obj = Histogram("%1$s=%2$s", List(Histogram.numberOfCategories("num.cat"), Histogram.entropy("entropy"),
      Histogram.frequencyRatio("freq.ratio")), false, true, true)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map("abc" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe Some(Map("3.14" -> 1))

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map("abc" -> 2, "xyz" -> 1))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe Some(Map("abc" -> 2, "xyz" -> 1, "3.14" -> 1))

    val c = obj.presentMultiple(Position2D("foo", "bar"), r3)
    c shouldBe Collection(List(Cell(Position3D("foo", "bar", "num.cat"), getLongContent(3)),
      Cell(Position3D("foo", "bar", "entropy"),
        getDoubleContent(- (2.0/4 * log2(2.0/4) + 1.0/4 * log2(1.0/4) + 1.0/4 * log2(1.0/4)))),
      Cell(Position3D("foo", "bar", "freq.ratio"), getDoubleContent(2)),
      Cell(Position3D("foo", "bar", "foo|bar=abc"), getLongContent(2)),
      Cell(Position3D("foo", "bar", "foo|bar=xyz"), getLongContent(1)),
      Cell(Position3D("foo", "bar", "foo|bar=3.14"), getLongContent(1))))
  }

  it should "prepare, reduce and present single with non-strict, all and non-frequency with statistics" in {
    val obj = Histogram("%1$s=%2$s", List(Histogram.numberOfCategories("num.cat"), Histogram.entropy("entropy"),
      Histogram.frequencyRatio("freq.ratio")), false, true, false)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map("abc" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe Some(Map("3.14" -> 1))

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map("abc" -> 2, "xyz" -> 1))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe Some(Map("abc" -> 2, "xyz" -> 1, "3.14" -> 1))

    val c = obj.presentMultiple(Position2D("foo", "bar"), r3)
    c shouldBe Collection(List(Cell(Position3D("foo", "bar", "num.cat"), getLongContent(3)),
      Cell(Position3D("foo", "bar", "entropy"),
        getDoubleContent(- (2.0/4 * log2(2.0/4) + 1.0/4 * log2(1.0/4) + 1.0/4 * log2(1.0/4)))),
      Cell(Position3D("foo", "bar", "freq.ratio"), getDoubleContent(2)),
      Cell(Position3D("foo", "bar", "foo|bar=abc"), getDoubleContent(2.0 / 4.0)),
      Cell(Position3D("foo", "bar", "foo|bar=xyz"), getDoubleContent(1.0 / 4.0)),
      Cell(Position3D("foo", "bar", "foo|bar=3.14"), getDoubleContent(1.0 / 4.0))))
  }

  it should "prepare, reduce and present single with non-strict, non-all and frequency with statistics" in {
    val obj = Histogram("%1$s=%2$s", List(Histogram.numberOfCategories("num.cat"), Histogram.entropy("entropy"),
      Histogram.frequencyRatio("freq.ratio")), false, false, true)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map("abc" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe None

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map("abc" -> 2, "xyz" -> 1))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe Some(Map("abc" -> 2, "xyz" -> 1))

    val c = obj.presentMultiple(Position2D("foo", "bar"), r3)
    c shouldBe Collection(List(Cell(Position3D("foo", "bar", "num.cat"), getLongContent(2)),
      Cell(Position3D("foo", "bar", "entropy"), getDoubleContent(- (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3)))),
      Cell(Position3D("foo", "bar", "freq.ratio"), getDoubleContent(2)),
      Cell(Position3D("foo", "bar", "foo|bar=abc"), getLongContent(2)),
      Cell(Position3D("foo", "bar", "foo|bar=xyz"), getLongContent(1))))
  }

  it should "prepare, reduce and present single with non-strict, non-all and non-frequency with statistics" in {
    val obj = Histogram("%1$s=%2$s", List(Histogram.numberOfCategories("num.cat"), Histogram.entropy("entropy"),
      Histogram.frequencyRatio("freq.ratio")), false, false, false)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map("abc" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe None

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map("abc" -> 2, "xyz" -> 1))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe Some(Map("abc" -> 2, "xyz" -> 1))

    val c = obj.presentMultiple(Position2D("foo", "bar"), r3)
    c shouldBe Collection(List(Cell(Position3D("foo", "bar", "num.cat"), getLongContent(2)),
      Cell(Position3D("foo", "bar", "entropy"), getDoubleContent(- (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3)))),
      Cell(Position3D("foo", "bar", "freq.ratio"), getDoubleContent(2)),
      Cell(Position3D("foo", "bar", "foo|bar=abc"), getDoubleContent(2.0 / 3.0)),
      Cell(Position3D("foo", "bar", "foo|bar=xyz"), getDoubleContent(1.0 / 3.0))))
  }

  it should "prepare, reduce and present single with non-strict, all, non-frequency and separator with statistics" in {
    val obj = Histogram("%1$s=%2$s", List(Histogram.numberOfCategories("num.cat"),
      Histogram.entropy("entropy"), Histogram.frequencyRatio("freq.ratio")), false, true, false, ".")

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map("abc" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe Some(Map("3.14" -> 1))

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map("abc" -> 2, "xyz" -> 1))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe Some(Map("abc" -> 2, "xyz" -> 1, "3.14" -> 1))

    val c = obj.presentMultiple(Position2D("foo", "bar"), r3)
    c shouldBe Collection(List(Cell(Position3D("foo", "bar", "num.cat"), getLongContent(3)),
      Cell(Position3D("foo", "bar", "entropy"),
        getDoubleContent(- (2.0/4 * log2(2.0/4) + 1.0/4 * log2(1.0/4) + 1.0/4 * log2(1.0/4)))),
      Cell(Position3D("foo", "bar", "freq.ratio"), getDoubleContent(2)),
      Cell(Position3D("foo", "bar", "foo.bar=abc"), getDoubleContent(2.0 / 4.0)),
      Cell(Position3D("foo", "bar", "foo.bar=xyz"), getDoubleContent(1.0 / 4.0)),
      Cell(Position3D("foo", "bar", "foo.bar=3.14"), getDoubleContent(1.0 / 4.0))))
  }

  "A numberOfCategories" should "compute" in {
    Histogram.numberOfCategories("num.cat").compute(Position1D("foo"), List(1, 2)) shouldBe
      Some(Cell(Position2D("foo", "num.cat"), getLongContent(2)))
  }

  it should "compute with a single bin" in {
    Histogram.numberOfCategories("num.cat").compute(Position1D("foo"), List(2)) shouldBe
      Some(Cell(Position2D("foo", "num.cat"), getLongContent(1)))
  }

  "An entropy" should "compute" in {
    Histogram.entropy("entropy", false).compute(Position1D("foo"), List(1, 2)) shouldBe
      Some(Cell(Position2D("foo", "entropy"), getDoubleContent(- (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3)))))
  }

  it should "compute with a single bin" in {
    Histogram.entropy("entropy", false).compute(Position1D("foo"), List(2)) shouldBe None

    val c = Histogram.entropy("entropy", true).compute(Position1D("foo"), List(2))
    c.get.position shouldBe Position2D("foo", "entropy")
    c.get.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Some(0)
  }

  "A frequencyRatio" should "compute" in {
    Histogram.frequencyRatio("freq.ratio", false).compute(Position1D("foo"), List(1, 2)) shouldBe
      Some(Cell(Position2D("foo", "freq.ratio"), getDoubleContent(2)))
  }

  it should "compute with a single bin" in {
    Histogram.frequencyRatio("freq.ratio", false).compute(Position1D("foo"), List(2)) shouldBe None

    val c = Histogram.frequencyRatio("freq.ratio", true).compute(Position1D("foo"), List(2))
    c.get.position shouldBe Position2D("foo", "freq.ratio")
    c.get.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Some(0)
  }
}

class TestThresholdCount extends TestReducers {

  val cell1 = Cell(Position2D("foo", "one"), getDoubleContent(-1))
  val cell2 = Cell(Position2D("foo", "two"), getDoubleContent(0))
  val cell3 = Cell(Position2D("foo", "three"), getDoubleContent(1))
  val cell4 = Cell(Position2D("foo", "bar"), getStringContent("bar"))
  val slice = Over[Position2D, First.type](First)

  "A ThresholdCount" should "prepare, reduce and present multiple" in {
    val obj = ThresholdCount("leq.cnt", "gtr.cnt")

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe ((1, 0))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe ((1, 0))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe ((0, 1))

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe ((2, 0))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe ((2, 1))

    val c = obj.presentMultiple(Position1D("foo"), r2)
    c shouldBe Collection(List(Cell(Position2D("foo", "leq.cnt"), getLongContent(2)),
      Cell(Position2D("foo", "gtr.cnt"), getLongContent(1))))
  }

  it should "prepare, reduce and present multiple with strict and nan" in {
    val obj = ThresholdCount("leq.cnt", "gtr.cnt", true, true)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe ((1, 0))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe ((1, 0))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe ((0, 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe ((-1, -1))

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe ((2, 0))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe ((2, 1))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe ((-1, -1))

    val c = obj.presentMultiple(Position1D("foo"), r3)
    c shouldBe Collection(List(Cell(Position2D("foo", "leq.cnt"), getLongContent(-1)),
      Cell(Position2D("foo", "gtr.cnt"), getLongContent(-1))))
  }

  it should "prepare, reduce and present multiple with strict and non-nan" in {
    val obj = ThresholdCount("leq.cnt", "gtr.cnt", true, false)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe ((1, 0))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe ((1, 0))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe ((0, 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe ((-1, -1))

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe ((2, 0))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe ((2, 1))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe ((-1, -1))

    val c = obj.presentMultiple(Position1D("foo"), r3)
    c shouldBe Collection()
  }

  it should "prepare, reduce and present multiple with non-strict and nan" in {
    val obj = ThresholdCount("leq.cnt", "gtr.cnt", false, true)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe ((1, 0))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe ((1, 0))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe ((0, 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe ((-1, -1))

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe ((2, 0))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe ((2, 1))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe ((2, 1))

    val c = obj.presentMultiple(Position1D("foo"), r3)
    c shouldBe Collection(List(Cell(Position2D("foo", "leq.cnt"), getLongContent(2)),
      Cell(Position2D("foo", "gtr.cnt"), getLongContent(1))))
  }

  it should "prepare, reduce and present multiple with non-strict and non-nan" in {
    val obj = ThresholdCount("leq.cnt", "gtr.cnt", false, false)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe ((1, 0))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe ((1, 0))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe ((0, 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe ((-1, -1))

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe ((2, 0))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe ((2, 1))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe ((2, 1))

    val c = obj.presentMultiple(Position1D("foo"), r3)
    c shouldBe Collection(List(Cell(Position2D("foo", "leq.cnt"), getLongContent(2)),
      Cell(Position2D("foo", "gtr.cnt"), getLongContent(1))))
  }

  it should "prepare, reduce and present multiple with a threshold" in {
    val obj = ThresholdCount("leq.cnt", "gtr.cnt", -0.5)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe ((1, 0))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe ((0, 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe ((0, 1))

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe ((1, 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe ((1, 2))

    val c = obj.presentMultiple(Position1D("foo"), r2)
    c shouldBe Collection(List(Cell(Position2D("foo", "leq.cnt"), getLongContent(1)),
      Cell(Position2D("foo", "gtr.cnt"), getLongContent(2))))
  }

  it should "prepare, reduce and present multiple with strict and nan with a threshold" in {
    val obj = ThresholdCount("leq.cnt", "gtr.cnt", true, true, -0.5)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe ((1, 0))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe ((0, 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe ((0, 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe ((-1, -1))

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe ((1, 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe ((1, 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe ((-1, -1))

    val c = obj.presentMultiple(Position1D("foo"), r3)
    c shouldBe Collection(List(Cell(Position2D("foo", "leq.cnt"), getLongContent(-1)),
      Cell(Position2D("foo", "gtr.cnt"), getLongContent(-1))))
  }

  it should "prepare, reduce and present multiple with strict and non-nan with a threshold" in {
    val obj = ThresholdCount("leq.cnt", "gtr.cnt", true, false, -0.5)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe ((1, 0))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe ((0, 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe ((0, 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe ((-1, -1))

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe ((1, 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe ((1, 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe ((-1, -1))

    val c = obj.presentMultiple(Position1D("foo"), r3)
    c shouldBe Collection()
  }

  it should "prepare, reduce and present multiple with non-strict and nan with a threshold" in {
    val obj = ThresholdCount("leq.cnt", "gtr.cnt", false, true, -0.5)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe ((1, 0))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe ((0, 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe ((0, 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe ((-1, -1))

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe ((1, 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe ((1, 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe ((1, 2))

    val c = obj.presentMultiple(Position1D("foo"), r3)
    c shouldBe Collection(List(Cell(Position2D("foo", "leq.cnt"), getLongContent(1)),
      Cell(Position2D("foo", "gtr.cnt"), getLongContent(2))))
  }

  it should "prepare, reduce and present multiple with non-strict and non-nan with a threshold" in {
    val obj = ThresholdCount("leq.cnt", "gtr.cnt", false, false, -0.5)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe ((1, 0))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe ((0, 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe ((0, 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe ((-1, -1))

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe ((1, 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe ((1, 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe ((1, 2))

    val c = obj.presentMultiple(Position1D("foo"), r3)
    c shouldBe Collection(List(Cell(Position2D("foo", "leq.cnt"), getLongContent(1)),
      Cell(Position2D("foo", "gtr.cnt"), getLongContent(2))))
  }
}

class TestWeightedSum extends TestReducers {

  val cell1 = Cell(Position2D("foo", 1), getDoubleContent(-1))
  val cell2 = Cell(Position2D("bar", 2), getDoubleContent(1))
  val cell3 = Cell(Position2D("xyz", 3), getStringContent("abc"))
  val slice = Over[Position2D, First.type](First)
  val ext = Map(Position1D("foo") -> getDoubleContent(3.14), Position1D("bar") -> getDoubleContent(6.28),
    Position1D(2) -> getDoubleContent(3.14), Position1D("foo.model1") -> getDoubleContent(3.14),
    Position1D("bar.model1") -> getDoubleContent(6.28), Position1D("2.model2") -> getDoubleContent(-3.14))

  "A WeightedSum" should "prepare, reduce and present single on the first dimension" in {
    val obj = WeightedSum(First)

    val t1 = obj.prepare(slice, cell1, ext)
    t1 shouldBe -3.14

    val t2 = obj.prepare(slice, cell2, ext)
    t2 shouldBe 6.28

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 3.14

    val c = obj.presentSingle(Position1D("foo"), r1)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(3.14)))
  }

  it should "prepare, reduce and present single on the second dimension" in {
    val obj = WeightedSum(Second)

    val t1 = obj.prepare(slice, cell1, ext)
    t1 shouldBe 0

    val t2 = obj.prepare(slice, cell2, ext)
    t2 shouldBe 3.14

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 3.14

    val c = obj.presentSingle(Position1D("foo"), r1)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(3.14)))
  }

  it should "prepare, reduce and present single with strict and nan" in {
    val obj = WeightedSum(First, true, true)

    val t1 = obj.prepare(slice, cell1, ext)
    t1 shouldBe -3.14

    val t2 = obj.prepare(slice, cell2, ext)
    t2 shouldBe 6.28

    val t3 = obj.prepare(slice, cell3, ext)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 3.14

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val c = obj.presentSingle(Position1D("foo"), r2)
    c.get.position shouldBe Position1D("foo")
    c.get.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Some(0)
  }

  it should "prepare, reduce and present single with strict and non-nan" in {
    val obj = WeightedSum(First, true, false)

    val t1 = obj.prepare(slice, cell1, ext)
    t1 shouldBe -3.14

    val t2 = obj.prepare(slice, cell2, ext)
    t2 shouldBe 6.28

    val t3 = obj.prepare(slice, cell3, ext)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 3.14

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val c = obj.presentSingle(Position1D("foo"), r2)
    c shouldBe None
  }

  it should "prepare, reduce and present single with non-strict and nan" in {
    val obj = WeightedSum(First, false, true)

    val t1 = obj.prepare(slice, cell1, ext)
    t1 shouldBe -3.14

    val t2 = obj.prepare(slice, cell2, ext)
    t2 shouldBe 6.28

    val t3 = obj.prepare(slice, cell3, ext)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 3.14

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe 3.14

    val c = obj.presentSingle(Position1D("foo"), r2)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(3.14)))
  }

  it should "prepare, reduce and present single with non-strict and non-nan" in {
    val obj = WeightedSum(First, false, false)

    val t1 = obj.prepare(slice, cell1, ext)
    t1 shouldBe -3.14

    val t2 = obj.prepare(slice, cell2, ext)
    t2 shouldBe 6.28

    val t3 = obj.prepare(slice, cell3, ext)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 3.14

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe 3.14

    val c = obj.presentSingle(Position1D("foo"), r2)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(3.14)))
  }

  it should "prepare, reduce and present multiple on the first dimension" in {
    val obj = WeightedSum(First, "result")

    val t1 = obj.prepare(slice, cell1, ext)
    t1 shouldBe -3.14

    val t2 = obj.prepare(slice, cell2, ext)
    t2 shouldBe 6.28

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 3.14

    val c = obj.presentMultiple(Position1D("foo"), r1)
    c shouldBe Collection(Cell(Position2D("foo", "result"), getDoubleContent(3.14)))
  }

  it should "prepare, reduce and present multiple on the second dimension" in {
    val obj = WeightedSum(Second, "result")

    val t1 = obj.prepare(slice, cell1, ext)
    t1 shouldBe 0

    val t2 = obj.prepare(slice, cell2, ext)
    t2 shouldBe 3.14

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 3.14

    val c = obj.presentMultiple(Position1D("foo"), r1)
    c shouldBe Collection(Cell(Position2D("foo", "result"), getDoubleContent(3.14)))
  }

  it should "prepare, reduce and present multiple with strict and nan" in {
    val obj = WeightedSum(First, "result", true, true)

    val t1 = obj.prepare(slice, cell1, ext)
    t1 shouldBe -3.14

    val t2 = obj.prepare(slice, cell2, ext)
    t2 shouldBe 6.28

    val t3 = obj.prepare(slice, cell3, ext)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 3.14

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val c = obj.presentMultiple(Position1D("foo"), r2)
    c.toList()(0).position shouldBe Position2D("foo", "result")
    c.toList()(0).content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Some(0)
  }

  it should "prepare, reduce and present multiple with strict and non-nan" in {
    val obj = WeightedSum(First, "result", true, false)

    val t1 = obj.prepare(slice, cell1, ext)
    t1 shouldBe -3.14

    val t2 = obj.prepare(slice, cell2, ext)
    t2 shouldBe 6.28

    val t3 = obj.prepare(slice, cell3, ext)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 3.14

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val c = obj.presentMultiple(Position1D("foo"), r2)
    c shouldBe Collection()
  }

  it should "prepare, reduce and present multiple with non-strict and nan" in {
    val obj = WeightedSum(First, "result", false, true)

    val t1 = obj.prepare(slice, cell1, ext)
    t1 shouldBe -3.14

    val t2 = obj.prepare(slice, cell2, ext)
    t2 shouldBe 6.28

    val t3 = obj.prepare(slice, cell3, ext)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 3.14

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe 3.14

    val c = obj.presentMultiple(Position1D("foo"), r2)
    c shouldBe Collection(Cell(Position2D("foo", "result"), getDoubleContent(3.14)))
  }

  it should "prepare, reduce and present multiple with non-strict and non-nan" in {
    val obj = WeightedSum(First, "result", false, false)

    val t1 = obj.prepare(slice, cell1, ext)
    t1 shouldBe -3.14

    val t2 = obj.prepare(slice, cell2, ext)
    t2 shouldBe 6.28

    val t3 = obj.prepare(slice, cell3, ext)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 3.14

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe 3.14

    val c = obj.presentMultiple(Position1D("foo"), r2)
    c shouldBe Collection(Cell(Position2D("foo", "result"), getDoubleContent(3.14)))
  }

  it should "prepare, reduce and present multiple on the first dimension with format" in {
    val obj = WeightedSum(First, "result", "%1$s.model1")

    val t1 = obj.prepare(slice, cell1, ext)
    t1 shouldBe -3.14

    val t2 = obj.prepare(slice, cell2, ext)
    t2 shouldBe 6.28

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 3.14

    val c = obj.presentMultiple(Position1D("foo"), r1)
    c shouldBe Collection(Cell(Position2D("foo", "result"), getDoubleContent(3.14)))
  }

  it should "prepare, reduce and present multiple on the second dimension with format" in {
    val obj = WeightedSum(Second, "result", "%1$s.model2")

    val t1 = obj.prepare(slice, cell1, ext)
    t1 shouldBe 0

    val t2 = obj.prepare(slice, cell2, ext)
    t2 shouldBe -3.14

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe -3.14

    val c = obj.presentMultiple(Position1D("foo"), r1)
    c shouldBe Collection(Cell(Position2D("foo", "result"), getDoubleContent(-3.14)))
  }

  it should "prepare, reduce and present multiple with strict and nan with format" in {
    val obj = WeightedSum(First, "result", "%1$s.model1", true, true)

    val t1 = obj.prepare(slice, cell1, ext)
    t1 shouldBe -3.14

    val t2 = obj.prepare(slice, cell2, ext)
    t2 shouldBe 6.28

    val t3 = obj.prepare(slice, cell3, ext)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 3.14

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val c = obj.presentMultiple(Position1D("foo"), r2)
    c.toList()(0).position shouldBe Position2D("foo", "result")
    c.toList()(0).content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Some(0)
  }

  it should "prepare, reduce and present multiple with strict and non-nan with format" in {
    val obj = WeightedSum(First, "result", "%1$s.model1", true, false)

    val t1 = obj.prepare(slice, cell1, ext)
    t1 shouldBe -3.14

    val t2 = obj.prepare(slice, cell2, ext)
    t2 shouldBe 6.28

    val t3 = obj.prepare(slice, cell3, ext)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 3.14

    val r2 = obj.reduce(r1, t3)
    r2.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val c = obj.presentMultiple(Position1D("foo"), r2)
    c shouldBe Collection()
  }

  it should "prepare, reduce and present multiple with non-strict and nan with format" in {
    val obj = WeightedSum(First, "result", "%1$s.model1", false, true)

    val t1 = obj.prepare(slice, cell1, ext)
    t1 shouldBe -3.14

    val t2 = obj.prepare(slice, cell2, ext)
    t2 shouldBe 6.28

    val t3 = obj.prepare(slice, cell3, ext)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 3.14

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe 3.14

    val c = obj.presentMultiple(Position1D("foo"), r2)
    c shouldBe Collection(Cell(Position2D("foo", "result"), getDoubleContent(3.14)))
  }

  it should "prepare, reduce and present multiple with non-strict and non-nan with format" in {
    val obj = WeightedSum(First, "result", "%1$s.model1", false, false)

    val t1 = obj.prepare(slice, cell1, ext)
    t1 shouldBe -3.14

    val t2 = obj.prepare(slice, cell2, ext)
    t2 shouldBe 6.28

    val t3 = obj.prepare(slice, cell3, ext)
    t3.asInstanceOf[Double].compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe 3.14

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe 3.14

    val c = obj.presentMultiple(Position1D("foo"), r2)
    c shouldBe Collection(Cell(Position2D("foo", "result"), getDoubleContent(3.14)))
  }
}

class TestDistinctCount extends TestReducers {

  val cell1 = Cell(Position2D("foo", 1), getDoubleContent(1))
  val cell2 = Cell(Position2D("foo", 2), getDoubleContent(1))
  val cell3 = Cell(Position2D("foo", 3), getDoubleContent(1))
  val cell4 = Cell(Position2D("abc", 4), getStringContent("abc"))
  val cell5 = Cell(Position2D("xyz", 4), getStringContent("abc"))
  val cell6 = Cell(Position2D("bar", 5), getLongContent(123))
  val slice = Over[Position2D, First.type](First)

  "A DistinctCount" should "prepare, reduce and present" in {
    val obj = DistinctCount()

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Set(DoubleValue(1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Set(DoubleValue(1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Set(DoubleValue(1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe Set(StringValue("abc"))

    val t5 = obj.prepare(slice, cell5)
    t5 shouldBe Set(StringValue("abc"))

    val t6 = obj.prepare(slice, cell6)
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

    val c = obj.presentSingle(Position1D("foo"), r5)
    c shouldBe Some(Cell(Position1D("foo"), getLongContent(3)))
  }

  it should "prepare, reduce and present with name" in {
    val obj = DistinctCount("count")

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Set(DoubleValue(1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Set(DoubleValue(1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Set(DoubleValue(1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe Set(StringValue("abc"))

    val t5 = obj.prepare(slice, cell5)
    t5 shouldBe Set(StringValue("abc"))

    val t6 = obj.prepare(slice, cell6)
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

    val c = obj.presentMultiple(Position1D("foo"), r5)
    c shouldBe Collection(Position2D("foo", "count"), getLongContent(3))
  }
}

class TestQuantiles extends TestReducers {

  val cell1 = Cell(Position1D("foo"), getDoubleContent(3))
  val cell2 = Cell(Position1D("foo"), getDoubleContent(7))
  val cell3 = Cell(Position1D("foo"), getDoubleContent(7))
  val cell4 = Cell(Position1D("foo"), getStringContent("abc"))
  val slice = Over[Position1D, First.type](First)

  "A Quantiles" should "prepare, reduce and present" in {
    val obj = Quantiles(2)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(new TreeMap[Double, Long]() + (3.0 -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(new TreeMap[Double, Long]() + (7.0 -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(new TreeMap[Double, Long]() + (7.0 -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe None

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(new TreeMap[Double, Long]() + (3.0 -> 1) + (7.0 -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(new TreeMap[Double, Long]() + (3.0 -> 1) + (7.0 -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe None

    val c = obj.presentMultiple(Position1D("foo"), r3)
    c shouldBe Collection()
  }

  it should "prepare, reduce and present with non-strict" in {
    val obj = Quantiles(2, false, "median")

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(new TreeMap[Double, Long]() + (3.0 -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(new TreeMap[Double, Long]() + (7.0 -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(new TreeMap[Double, Long]() + (7.0 -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe None

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(new TreeMap[Double, Long]() + (3.0 -> 1) + (7.0 -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(new TreeMap[Double, Long]() + (3.0 -> 1) + (7.0 -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe Some(new TreeMap[Double, Long]() + (3.0 -> 1) + (7.0 -> 2))

    val c = obj.presentMultiple(Position1D("foo"), r3)
    c shouldBe Collection(List(Cell(Position2D("foo", "median"), getDoubleContent(7))))
  }

  val cells1 = List(Cell(Position1D("foo"), getDoubleContent(3)),
    Cell(Position1D("foo"), getDoubleContent(7)),
    Cell(Position1D("foo"), getDoubleContent(8)),
    Cell(Position1D("foo"), getDoubleContent(16)),
    Cell(Position1D("foo"), getDoubleContent(9)),
    Cell(Position1D("foo"), getDoubleContent(13)),
    Cell(Position1D("foo"), getDoubleContent(8)),
    Cell(Position1D("foo"), getDoubleContent(6)),
    Cell(Position1D("foo"), getDoubleContent(15)),
    Cell(Position1D("foo"), getDoubleContent(10)),
    Cell(Position1D("foo"), getDoubleContent(20)))

  it should "prepare, reduce and present quartiles" in {
    val obj = Quantiles(4)

    val t = cells1.map { case c => obj.prepare(slice, c) }
    val r = t.tail.tail.foldLeft(obj.reduce(t.head, t.tail.head)) { case (b,a) => obj.reduce(b, a) }
    val c = obj.presentMultiple(Position1D("foo"), r) shouldBe Collection(List(
      Cell(Position2D("foo", "quantile.1=0.250"), getDoubleContent(7.0)),
      Cell(Position2D("foo", "quantile.2=0.500"), getDoubleContent(9.0)),
      Cell(Position2D("foo", "quantile.3=0.750"), getDoubleContent(15.0))))
  }

  val cells2 = List(Cell(Position1D("foo"), getDoubleContent(40)),
    Cell(Position1D("foo"), getDoubleContent(15)),
    Cell(Position1D("foo"), getDoubleContent(35)),
    Cell(Position1D("foo"), getDoubleContent(20)),
    Cell(Position1D("foo"), getDoubleContent(50)))

  it should "prepare, reduce and present deciles" in {
    val obj = Quantiles(10)

    val t = cells2.map { case c => obj.prepare(slice, c) }
    val r = t.tail.tail.foldLeft(obj.reduce(t.head, t.tail.head)) { case (b,a) => obj.reduce(b, a) }
    val c = obj.presentMultiple(Position1D("foo"), r) shouldBe Collection(List(
      Cell(Position2D("foo", "quantile.1=0.100"), getDoubleContent(15.0)),
      Cell(Position2D("foo", "quantile.2=0.200"), getDoubleContent(15.0)),
      Cell(Position2D("foo", "quantile.3=0.300"), getDoubleContent(20.0)),
      Cell(Position2D("foo", "quantile.4=0.400"), getDoubleContent(20.0)),
      Cell(Position2D("foo", "quantile.5=0.500"), getDoubleContent(35.0)),
      Cell(Position2D("foo", "quantile.6=0.600"), getDoubleContent(35.0)),
      Cell(Position2D("foo", "quantile.7=0.700"), getDoubleContent(40.0)),
      Cell(Position2D("foo", "quantile.8=0.800"), getDoubleContent(40.0)),
      Cell(Position2D("foo", "quantile.9=0.900"), getDoubleContent(50.0))))
  }

  val cells3 = List(Cell(Position1D("foo"), getDoubleContent(3)),
    Cell(Position1D("foo"), getDoubleContent(7)),
    Cell(Position1D("foo"), getDoubleContent(16)),
    Cell(Position1D("foo"), getDoubleContent(8)),
    Cell(Position1D("foo"), getDoubleContent(10)),
    Cell(Position1D("foo"), getDoubleContent(6)),
    Cell(Position1D("foo"), getDoubleContent(13)),
    Cell(Position1D("foo"), getDoubleContent(15)),
    Cell(Position1D("foo"), getDoubleContent(8)),
    Cell(Position1D("foo"), getDoubleContent(20)))

  it should "prepare, reduce and present quartiles vigintiles" in {
    val obj = Quantiles(20)

    val t = cells3.map { case c => obj.prepare(slice, c) }
    val r = t.tail.tail.foldLeft(obj.reduce(t.head, t.tail.head)) { case (b,a) => obj.reduce(b, a) }
    val c = obj.presentMultiple(Position1D("foo"), r) shouldBe Collection(List(
      Cell(Position2D("foo", "quantile.1=0.050"), getDoubleContent(3.0)),
      Cell(Position2D("foo", "quantile.2=0.100"), getDoubleContent(3.0)),
      Cell(Position2D("foo", "quantile.3=0.150"), getDoubleContent(6.0)),
      Cell(Position2D("foo", "quantile.4=0.200"), getDoubleContent(6.0)),
      Cell(Position2D("foo", "quantile.5=0.250"), getDoubleContent(7.0)),
      Cell(Position2D("foo", "quantile.6=0.300"), getDoubleContent(7.0)),
      Cell(Position2D("foo", "quantile.7=0.350"), getDoubleContent(8.0)),
      Cell(Position2D("foo", "quantile.8=0.400"), getDoubleContent(8.0)),
      Cell(Position2D("foo", "quantile.9=0.450"), getDoubleContent(8.0)),
      Cell(Position2D("foo", "quantile.10=0.500"), getDoubleContent(8.0)),
      Cell(Position2D("foo", "quantile.11=0.550"), getDoubleContent(10.0)),
      Cell(Position2D("foo", "quantile.12=0.600"), getDoubleContent(10.0)),
      Cell(Position2D("foo", "quantile.13=0.650"), getDoubleContent(13.0)),
      Cell(Position2D("foo", "quantile.14=0.700"), getDoubleContent(13.0)),
      Cell(Position2D("foo", "quantile.15=0.750"), getDoubleContent(15.0)),
      Cell(Position2D("foo", "quantile.16=0.800"), getDoubleContent(15.0)),
      Cell(Position2D("foo", "quantile.17=0.850"), getDoubleContent(16.0)),
      Cell(Position2D("foo", "quantile.18=0.900"), getDoubleContent(16.0)),
      Cell(Position2D("foo", "quantile.19=0.950"), getDoubleContent(20.0))))
  }
}

class TestEntropy extends TestReducers {

  val cell1 = Cell(Position1D("foo"), getStringContent("abc"))
  val cell2 = Cell(Position1D("foo"), getStringContent("xyz"))
  val cell3 = Cell(Position1D("foo"), getStringContent("xyz"))
  val cell4 = Cell(Position1D("foo"), getDoubleContent(3.14))
  val slice = Over[Position1D, First.type](First)

  def log2(x: Double) = math.log(x) / math.log(2)
  def log4(x: Double) = math.log(x) / math.log(4)

  "An Entropy" should "prepare, reduce and present single" in {
    val obj = Entropy()

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val c = obj.presentSingle(Position1D("foo"), r2)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(- (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3)))))
  }

  it should "prepare, reduce and present single with strict, nan, all and negate" in {
    val obj = Entropy(true, true, true, true)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe Some(Map[String, Long]("3.14" -> 1))

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2, "3.14" -> 1))

    val c = obj.presentSingle(Position1D("foo"), r3)
    c shouldBe Some(Cell(Position1D("foo"),
      getDoubleContent(2.0/4 * log2(2.0/4) + 1.0/4 * log2(1.0/4) + 1.0/4 * log2(1.0/4))))
  }

  it should "prepare, reduce and present single with strict, nan, all and non-negate" in {
    val obj = Entropy(true, true, true, false)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe Some(Map[String, Long]("3.14" -> 1))

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2, "3.14" -> 1))

    val c = obj.presentSingle(Position1D("foo"), r3)
    c shouldBe Some(Cell(Position1D("foo"),
      getDoubleContent(- (2.0/4 * log2(2.0/4) + 1.0/4 * log2(1.0/4) + 1.0/4 * log2(1.0/4)))))
  }

  it should "prepare, reduce and present single with strict, nan, non-all and negate" in {
    val obj = Entropy(true, true, false, true)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe None

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe None

    val c = obj.presentSingle(Position1D("foo"), r3)
    c.get.position shouldBe Position1D("foo")
    c.get.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Some(0)
  }

  it should "prepare, reduce and present single with strict, nan, non-all and non-negate" in {
    val obj = Entropy(true, true, false, false)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe None

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe None

    val c = obj.presentSingle(Position1D("foo"), r3)
    c.get.position shouldBe Position1D("foo")
    c.get.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Some(0)
  }

  it should "prepare, reduce and present single with strict, non-nan, all and negate" in {
    val obj = Entropy(true, false, true, true)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe Some(Map[String, Long]("3.14" -> 1))

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2, "3.14" -> 1))

    val c = obj.presentSingle(Position1D("foo"), r3)
    c shouldBe Some(Cell(Position1D("foo"),
      getDoubleContent(2.0/4 * log2(2.0/4) + 1.0/4 * log2(1.0/4) + 1.0/4 * log2(1.0/4))))
  }

  it should "prepare, reduce and present single with strict, non-nan, all and non-negate" in {
    val obj = Entropy(true, false, true, false)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe Some(Map[String, Long]("3.14" -> 1))

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2, "3.14" -> 1))

    val c = obj.presentSingle(Position1D("foo"), r3)
    c shouldBe Some(Cell(Position1D("foo"),
      getDoubleContent(- (2.0/4 * log2(2.0/4) + 1.0/4 * log2(1.0/4) + 1.0/4 * log2(1.0/4)))))
  }

  it should "prepare, reduce and present single with strict, non-nan, non-all and negate" in {
    val obj = Entropy(true, false, false, true)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe None

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe None

    val c = obj.presentSingle(Position1D("foo"), r3)
    c shouldBe None
  }

  it should "prepare, reduce and present single with strict, non-nan, non-all and non-negate" in {
    val obj = Entropy(true, false, false, false)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe None

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe None

    val c = obj.presentSingle(Position1D("foo"), r3)
    c shouldBe None
  }

  it should "prepare, reduce and present single with non-strict, nan, all and negate" in {
    val obj = Entropy(false, true, true, true)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe Some(Map[String, Long]("3.14" -> 1))

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2, "3.14" -> 1))

    val c = obj.presentSingle(Position1D("foo"), r3)
    c shouldBe Some(Cell(Position1D("foo"),
      getDoubleContent(2.0/4 * log2(2.0/4) + 1.0/4 * log2(1.0/4) + 1.0/4 * log2(1.0/4))))
  }

  it should "prepare, reduce and present single with non-strict, nan, all and non-negate" in {
    val obj = Entropy(false, true, true, false)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe Some(Map[String, Long]("3.14" -> 1))

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2, "3.14" -> 1))

    val c = obj.presentSingle(Position1D("foo"), r3)
    c shouldBe Some(Cell(Position1D("foo"),
      getDoubleContent(- (2.0/4 * log2(2.0/4) + 1.0/4 * log2(1.0/4) + 1.0/4 * log2(1.0/4)))))
  }

  it should "prepare, reduce and present single with non-strict, nan, non-all and negate" in {
    val obj = Entropy(false, true, false, true)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe None

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val c = obj.presentSingle(Position1D("foo"), r3)
    c shouldBe Some(Cell(Position1D("foo"),
      getDoubleContent(2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))
  }

  it should "prepare, reduce and present single with non-strict, nan, non-all and non-negate" in {
    val obj = Entropy(false, true, false, false)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe None

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val c = obj.presentSingle(Position1D("foo"), r3)
    c shouldBe Some(Cell(Position1D("foo"),
      getDoubleContent(- (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3)))))
  }

  it should "prepare, reduce and present single with non-strict, non-nan, all and negate" in {
    val obj = Entropy(false, false, true, true)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe Some(Map[String, Long]("3.14" -> 1))

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2, "3.14" -> 1))

    val c = obj.presentSingle(Position1D("foo"), r3)
    c shouldBe Some(Cell(Position1D("foo"),
      getDoubleContent(2.0/4 * log2(2.0/4) + 1.0/4 * log2(1.0/4) + 1.0/4 * log2(1.0/4))))
  }

  it should "prepare, reduce and present single with non-strict, non-nan, all and non-negate" in {
    val obj = Entropy(false, false, true, false)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe Some(Map[String, Long]("3.14" -> 1))

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2, "3.14" -> 1))

    val c = obj.presentSingle(Position1D("foo"), r3)
    c shouldBe Some(Cell(Position1D("foo"),
      getDoubleContent(- (2.0/4 * log2(2.0/4) + 1.0/4 * log2(1.0/4) + 1.0/4 * log2(1.0/4)))))
  }

  it should "prepare, reduce and present single with non-strict, non-nan, non-all and negate" in {
    val obj = Entropy(false, false, false, true)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe None

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val c = obj.presentSingle(Position1D("foo"), r3)
    c shouldBe Some(Cell(Position1D("foo"),
      getDoubleContent(2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))
  }

  it should "prepare, reduce and present single with non-strict, non-nan, non-all and non-negate" in {
    val obj = Entropy(false, false, false, false)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe None

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val c = obj.presentSingle(Position1D("foo"), r3)
    c shouldBe Some(Cell(Position1D("foo"),
      getDoubleContent(- (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3)))))
  }

  it should "prepare, reduce and present single with log" in {
    val obj = Entropy(log4 _)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val c = obj.presentSingle(Position1D("foo"), r2)
    c shouldBe Some(Cell(Position1D("foo"), getDoubleContent(- (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3)))))
  }

  it should "prepare, reduce and present single with strict, nan, all and negate with log" in {
    val obj = Entropy(true, true, true, true, log4 _)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe Some(Map[String, Long]("3.14" -> 1))

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2, "3.14" -> 1))

    val c = obj.presentSingle(Position1D("foo"), r3)
    c shouldBe Some(Cell(Position1D("foo"),
      getDoubleContent(2.0/4 * log4(2.0/4) + 1.0/4 * log4(1.0/4) + 1.0/4 * log4(1.0/4))))
  }

  it should "prepare, reduce and present single with strict, nan, all and non-negate with log" in {
    val obj = Entropy(true, true, true, false, log4 _)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe Some(Map[String, Long]("3.14" -> 1))

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2, "3.14" -> 1))

    val c = obj.presentSingle(Position1D("foo"), r3)
    c shouldBe Some(Cell(Position1D("foo"),
      getDoubleContent(- (2.0/4 * log4(2.0/4) + 1.0/4 * log4(1.0/4) + 1.0/4 * log4(1.0/4)))))
  }

  it should "prepare, reduce and present single with strict, nan, non-all and negate with log" in {
    val obj = Entropy(true, true, false, true, log4 _)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe None

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe None

    val c = obj.presentSingle(Position1D("foo"), r3)
    c.get.position shouldBe Position1D("foo")
    c.get.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Some(0)
  }

  it should "prepare, reduce and present single with strict, nan, non-all and non-negate with log" in {
    val obj = Entropy(true, true, false, false, log4 _)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe None

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe None

    val c = obj.presentSingle(Position1D("foo"), r3)
    c.get.position shouldBe Position1D("foo")
    c.get.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Some(0)
  }

  it should "prepare, reduce and present single with strict, non-nan, all and negate with log" in {
    val obj = Entropy(true, false, true, true, log4 _)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe Some(Map[String, Long]("3.14" -> 1))

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2, "3.14" -> 1))

    val c = obj.presentSingle(Position1D("foo"), r3)
    c shouldBe Some(Cell(Position1D("foo"),
      getDoubleContent(2.0/4 * log4(2.0/4) + 1.0/4 * log4(1.0/4) + 1.0/4 * log4(1.0/4))))
  }

  it should "prepare, reduce and present single with strict, non-nan, all and non-negate with log" in {
    val obj = Entropy(true, false, true, false, log4 _)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe Some(Map[String, Long]("3.14" -> 1))

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2, "3.14" -> 1))

    val c = obj.presentSingle(Position1D("foo"), r3)
    c shouldBe Some(Cell(Position1D("foo"),
      getDoubleContent(- (2.0/4 * log4(2.0/4) + 1.0/4 * log4(1.0/4) + 1.0/4 * log4(1.0/4)))))
  }

  it should "prepare, reduce and present single with strict, non-nan, non-all and negate with log" in {
    val obj = Entropy(true, false, false, true, log4 _)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe None

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe None

    val c = obj.presentSingle(Position1D("foo"), r3)
    c shouldBe None
  }

  it should "prepare, reduce and present single with strict, non-nan, non-all and non-negate with log" in {
    val obj = Entropy(true, false, false, false, log4 _)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe None

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe None

    val c = obj.presentSingle(Position1D("foo"), r3)
    c shouldBe None
  }

  it should "prepare, reduce and present single with non-strict, nan, all and negate with log" in {
    val obj = Entropy(false, true, true, true, log4 _)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe Some(Map[String, Long]("3.14" -> 1))

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2, "3.14" -> 1))

    val c = obj.presentSingle(Position1D("foo"), r3)
    c shouldBe Some(Cell(Position1D("foo"),
      getDoubleContent(2.0/4 * log4(2.0/4) + 1.0/4 * log4(1.0/4) + 1.0/4 * log4(1.0/4))))
  }

  it should "prepare, reduce and present single with non-strict, nan, all and non-negate with log" in {
    val obj = Entropy(false, true, true, false, log4 _)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe Some(Map[String, Long]("3.14" -> 1))

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2, "3.14" -> 1))

    val c = obj.presentSingle(Position1D("foo"), r3)
    c shouldBe Some(Cell(Position1D("foo"),
      getDoubleContent(- (2.0/4 * log4(2.0/4) + 1.0/4 * log4(1.0/4) + 1.0/4 * log4(1.0/4)))))
  }

  it should "prepare, reduce and present single with non-strict, nan, non-all and negate with log" in {
    val obj = Entropy(false, true, false, true, log4 _)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe None

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val c = obj.presentSingle(Position1D("foo"), r3)
    c shouldBe Some(Cell(Position1D("foo"),
      getDoubleContent(2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))
  }

  it should "prepare, reduce and present single with non-strict, nan, non-all and non-negate with log" in {
    val obj = Entropy(false, true, false, false, log4 _)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe None

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val c = obj.presentSingle(Position1D("foo"), r3)
    c shouldBe Some(Cell(Position1D("foo"),
      getDoubleContent(- (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3)))))
  }

  it should "prepare, reduce and present single with non-strict, non-nan, all and negate with log" in {
    val obj = Entropy(false, false, true, true, log4 _)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe Some(Map[String, Long]("3.14" -> 1))

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2, "3.14" -> 1))

    val c = obj.presentSingle(Position1D("foo"), r3)
    c shouldBe Some(Cell(Position1D("foo"),
      getDoubleContent(2.0/4 * log4(2.0/4) + 1.0/4 * log4(1.0/4) + 1.0/4 * log4(1.0/4))))
  }

  it should "prepare, reduce and present single with non-strict, non-nan, all and non-negate with log" in {
    val obj = Entropy(false, false, true, false, log4 _)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe Some(Map[String, Long]("3.14" -> 1))

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2, "3.14" -> 1))

    val c = obj.presentSingle(Position1D("foo"), r3)
    c shouldBe Some(Cell(Position1D("foo"),
      getDoubleContent(- (2.0/4 * log4(2.0/4) + 1.0/4 * log4(1.0/4) + 1.0/4 * log4(1.0/4)))))
  }

  it should "prepare, reduce and present single with non-strict, non-nan, non-all and negate with log" in {
    val obj = Entropy(false, false, false, true, log4 _)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe None

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val c = obj.presentSingle(Position1D("foo"), r3)
    c shouldBe Some(Cell(Position1D("foo"),
      getDoubleContent(2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))
  }

  it should "prepare, reduce and present single with non-strict, non-nan, non-all and non-negate with log" in {
    val obj = Entropy(false, false, false, false, log4 _)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe None

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val c = obj.presentSingle(Position1D("foo"), r3)
    c shouldBe Some(Cell(Position1D("foo"),
      getDoubleContent(- (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3)))))
  }

  it should "prepare, reduce and present multiple" in {
    val obj = Entropy("entropy")

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val c = obj.presentMultiple(Position1D("foo"), r2)
    c shouldBe Collection(Position2D("foo", "entropy"),
      getDoubleContent(- (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))
  }

  it should "prepare, reduce and present multiple with strict, nan, all and negate" in {
    val obj = Entropy("entropy", true, true, true, true)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe Some(Map[String, Long]("3.14" -> 1))

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2, "3.14" -> 1))

    val c = obj.presentMultiple(Position1D("foo"), r3)
    c shouldBe Collection(Position2D("foo", "entropy"),
      getDoubleContent(2.0/4 * log2(2.0/4) + 1.0/4 * log2(1.0/4) + 1.0/4 * log2(1.0/4)))
  }

  it should "prepare, reduce and present multiple with strict, nan, all and non-negate" in {
    val obj = Entropy("entropy", true, true, true, false)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe Some(Map[String, Long]("3.14" -> 1))

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2, "3.14" -> 1))

    val c = obj.presentMultiple(Position1D("foo"), r3)
    c shouldBe Collection(Position2D("foo", "entropy"),
      getDoubleContent(- (2.0/4 * log2(2.0/4) + 1.0/4 * log2(1.0/4) + 1.0/4 * log2(1.0/4))))
  }

  it should "prepare, reduce and present multiple with strict, nan, non-all and negate" in {
    val obj = Entropy("entropy", true, true, false, true)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe None

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe None

    val c = obj.presentMultiple(Position1D("foo"), r3)
    c.toList()(0).position shouldBe Position2D("foo", "entropy")
    c.toList()(0).content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Some(0)
  }

  it should "prepare, reduce and present multiple with strict, nan, non-all and non-negate" in {
    val obj = Entropy("entropy", true, true, false, false)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe None

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe None

    val c = obj.presentMultiple(Position1D("foo"), r3)
    c.toList()(0).position shouldBe Position2D("foo", "entropy")
    c.toList()(0).content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Some(0)
  }

  it should "prepare, reduce and present multiple with strict, non-nan, all and negate" in {
    val obj = Entropy("entropy", true, false, true, true)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe Some(Map[String, Long]("3.14" -> 1))

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2, "3.14" -> 1))

    val c = obj.presentMultiple(Position1D("foo"), r3)
    c shouldBe Collection(Position2D("foo", "entropy"),
      getDoubleContent(2.0/4 * log2(2.0/4) + 1.0/4 * log2(1.0/4) + 1.0/4 * log2(1.0/4)))
  }

  it should "prepare, reduce and present multiple with strict, non-nan, all and non-negate" in {
    val obj = Entropy("entropy", true, false, true, false)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe Some(Map[String, Long]("3.14" -> 1))

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2, "3.14" -> 1))

    val c = obj.presentMultiple(Position1D("foo"), r3)
    c shouldBe Collection(Position2D("foo", "entropy"),
      getDoubleContent(- (2.0/4 * log2(2.0/4) + 1.0/4 * log2(1.0/4) + 1.0/4 * log2(1.0/4))))
  }

  it should "prepare, reduce and present multiple with strict, non-nan, non-all and negate" in {
    val obj = Entropy("entropy", true, false, false, true)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe None

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe None

    val c = obj.presentMultiple(Position1D("foo"), r3)
    c shouldBe Collection()
  }

  it should "prepare, reduce and present multiple with strict, non-nan, non-all and non-negate" in {
    val obj = Entropy("entropy", true, false, false, false)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe None

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe None

    val c = obj.presentMultiple(Position1D("foo"), r3)
    c shouldBe Collection()
  }

  it should "prepare, reduce and present multiple with non-strict, nan, all and negate" in {
    val obj = Entropy("entropy", false, true, true, true)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe Some(Map[String, Long]("3.14" -> 1))

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2, "3.14" -> 1))

    val c = obj.presentMultiple(Position1D("foo"), r3)
    c shouldBe Collection(Position2D("foo", "entropy"),
      getDoubleContent(2.0/4 * log2(2.0/4) + 1.0/4 * log2(1.0/4) + 1.0/4 * log2(1.0/4)))
  }

  it should "prepare, reduce and present multiple with non-strict, nan, all and non-negate" in {
    val obj = Entropy("entropy", false, true, true, false)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe Some(Map[String, Long]("3.14" -> 1))

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2, "3.14" -> 1))

    val c = obj.presentMultiple(Position1D("foo"), r3)
    c shouldBe Collection(Position2D("foo", "entropy"),
      getDoubleContent(- (2.0/4 * log2(2.0/4) + 1.0/4 * log2(1.0/4) + 1.0/4 * log2(1.0/4))))
  }

  it should "prepare, reduce and present multiple with non-strict, nan, non-all and negate" in {
    val obj = Entropy("entropy", false, true, false, true)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe None

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val c = obj.presentMultiple(Position1D("foo"), r3)
    c shouldBe Collection(Position2D("foo", "entropy"),
      getDoubleContent(2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3)))
  }

  it should "prepare, reduce and present multiple with non-strict, nan, non-all and non-negate" in {
    val obj = Entropy("entropy", false, true, false, false)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe None

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val c = obj.presentMultiple(Position1D("foo"), r3)
    c shouldBe Collection(Position2D("foo", "entropy"),
      getDoubleContent(- (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))
  }

  it should "prepare, reduce and present multiple with non-strict, non-nan, all and negate" in {
    val obj = Entropy("entropy", false, false, true, true)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe Some(Map[String, Long]("3.14" -> 1))

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2, "3.14" -> 1))

    val c = obj.presentMultiple(Position1D("foo"), r3)
    c shouldBe Collection(Position2D("foo", "entropy"),
      getDoubleContent(2.0/4 * log2(2.0/4) + 1.0/4 * log2(1.0/4) + 1.0/4 * log2(1.0/4)))
  }

  it should "prepare, reduce and present multiple with non-strict, non-nan, all and non-negate" in {
    val obj = Entropy("entropy", false, false, true, false)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe Some(Map[String, Long]("3.14" -> 1))

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2, "3.14" -> 1))

    val c = obj.presentMultiple(Position1D("foo"), r3)
    c shouldBe Collection(Position2D("foo", "entropy"),
      getDoubleContent(- (2.0/4 * log2(2.0/4) + 1.0/4 * log2(1.0/4) + 1.0/4 * log2(1.0/4))))
  }

  it should "prepare, reduce and present multiple with non-strict, non-nan, non-all and negate" in {
    val obj = Entropy("entropy", false, false, false, true)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe None

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val c = obj.presentMultiple(Position1D("foo"), r3)
    c shouldBe Collection(Position2D("foo", "entropy"),
      getDoubleContent(2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3)))
  }

  it should "prepare, reduce and present multiple with non-strict, non-nan, non-all and non-negate" in {
    val obj = Entropy("entropy", false, false, false, false)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe None

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val c = obj.presentMultiple(Position1D("foo"), r3)
    c shouldBe Collection(Position2D("foo", "entropy"),
      getDoubleContent(- (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))
  }

  it should "prepare, reduce and present multiple with log" in {
    val obj = Entropy("entropy", log4 _)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val c = obj.presentMultiple(Position1D("foo"), r2)
    c shouldBe Collection(Position2D("foo", "entropy"),
      getDoubleContent(- (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))
  }

  it should "prepare, reduce and present multiple with strict, nan, all and negate with log" in {
    val obj = Entropy("entropy", true, true, true, true, log4 _)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe Some(Map[String, Long]("3.14" -> 1))

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2, "3.14" -> 1))

    val c = obj.presentMultiple(Position1D("foo"), r3)
    c shouldBe Collection(Position2D("foo", "entropy"),
      getDoubleContent(2.0/4 * log4(2.0/4) + 1.0/4 * log4(1.0/4) + 1.0/4 * log4(1.0/4)))
  }

  it should "prepare, reduce and present multiple with strict, nan, all and non-negate with log" in {
    val obj = Entropy("entropy", true, true, true, false, log4 _)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe Some(Map[String, Long]("3.14" -> 1))

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2, "3.14" -> 1))

    val c = obj.presentMultiple(Position1D("foo"), r3)
    c shouldBe Collection(Position2D("foo", "entropy"),
      getDoubleContent(- (2.0/4 * log4(2.0/4) + 1.0/4 * log4(1.0/4) + 1.0/4 * log4(1.0/4))))
  }

  it should "prepare, reduce and present multiple with strict, nan, non-all and negate with log" in {
    val obj = Entropy("entropy", true, true, false, true, log4 _)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe None

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe None

    val c = obj.presentMultiple(Position1D("foo"), r3)
    c.toList()(0).position shouldBe Position2D("foo", "entropy")
    c.toList()(0).content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Some(0)
  }

  it should "prepare, reduce and present multiple with strict, nan, non-all and non-negate with log" in {
    val obj = Entropy("entropy", true, true, false, false, log4 _)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe None

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe None

    val c = obj.presentMultiple(Position1D("foo"), r3)
    c.toList()(0).position shouldBe Position2D("foo", "entropy")
    c.toList()(0).content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Some(0)
  }

  it should "prepare, reduce and present multiple with strict, non-nan, all and negate with log" in {
    val obj = Entropy("entropy", true, false, true, true, log4 _)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe Some(Map[String, Long]("3.14" -> 1))

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2, "3.14" -> 1))

    val c = obj.presentMultiple(Position1D("foo"), r3)
    c shouldBe Collection(Position2D("foo", "entropy"),
      getDoubleContent(2.0/4 * log4(2.0/4) + 1.0/4 * log4(1.0/4) + 1.0/4 * log4(1.0/4)))
  }

  it should "prepare, reduce and present multiple with strict, non-nan, all and non-negate with log" in {
    val obj = Entropy("entropy", true, false, true, false, log4 _)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe Some(Map[String, Long]("3.14" -> 1))

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2, "3.14" -> 1))

    val c = obj.presentMultiple(Position1D("foo"), r3)
    c shouldBe Collection(Position2D("foo", "entropy"),
      getDoubleContent(- (2.0/4 * log4(2.0/4) + 1.0/4 * log4(1.0/4) + 1.0/4 * log4(1.0/4))))
  }

  it should "prepare, reduce and present multiple with strict, non-nan, non-all and negate with log" in {
    val obj = Entropy("entropy", true, false, false, true, log4 _)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe None

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe None

    val c = obj.presentMultiple(Position1D("foo"), r3)
    c shouldBe Collection()
  }

  it should "prepare, reduce and present multiple with strict, non-nan, non-all and non-negate with log" in {
    val obj = Entropy("entropy", true, false, false, false, log4 _)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe None

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe None

    val c = obj.presentMultiple(Position1D("foo"), r3)
    c shouldBe Collection()
  }

  it should "prepare, reduce and present multiple with non-strict, nan, all and negate with log" in {
    val obj = Entropy("entropy", false, true, true, true, log4 _)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe Some(Map[String, Long]("3.14" -> 1))

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2, "3.14" -> 1))

    val c = obj.presentMultiple(Position1D("foo"), r3)
    c shouldBe Collection(Position2D("foo", "entropy"),
      getDoubleContent(2.0/4 * log4(2.0/4) + 1.0/4 * log4(1.0/4) + 1.0/4 * log4(1.0/4)))
  }

  it should "prepare, reduce and present multiple with non-strict, nan, all and non-negate with log" in {
    val obj = Entropy("entropy", false, true, true, false, log4 _)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe Some(Map[String, Long]("3.14" -> 1))

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2, "3.14" -> 1))

    val c = obj.presentMultiple(Position1D("foo"), r3)
    c shouldBe Collection(Position2D("foo", "entropy"),
      getDoubleContent(- (2.0/4 * log4(2.0/4) + 1.0/4 * log4(1.0/4) + 1.0/4 * log4(1.0/4))))
  }

  it should "prepare, reduce and present multiple with non-strict, nan, non-all and negate with log" in {
    val obj = Entropy("entropy", false, true, false, true, log4 _)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe None

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val c = obj.presentMultiple(Position1D("foo"), r3)
    c shouldBe Collection(Position2D("foo", "entropy"),
      getDoubleContent(2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3)))
  }

  it should "prepare, reduce and present multiple with non-strict, nan, non-all and non-negate with log" in {
    val obj = Entropy("entropy", false, true, false, false, log4 _)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe None

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val c = obj.presentMultiple(Position1D("foo"), r3)
    c shouldBe Collection(Position2D("foo", "entropy"),
      getDoubleContent(- (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))
  }

  it should "prepare, reduce and present multiple with non-strict, non-nan, all and negate with log" in {
    val obj = Entropy("entropy", false, false, true, true, log4 _)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe Some(Map[String, Long]("3.14" -> 1))

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2, "3.14" -> 1))

    val c = obj.presentMultiple(Position1D("foo"), r3)
    c shouldBe Collection(Position2D("foo", "entropy"),
      getDoubleContent(2.0/4 * log4(2.0/4) + 1.0/4 * log4(1.0/4) + 1.0/4 * log4(1.0/4)))
  }

  it should "prepare, reduce and present multiple with non-strict, non-nan, all and non-negate with log" in {
    val obj = Entropy("entropy", false, false, true, false, log4 _)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe Some(Map[String, Long]("3.14" -> 1))

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2, "3.14" -> 1))

    val c = obj.presentMultiple(Position1D("foo"), r3)
    c shouldBe Collection(Position2D("foo", "entropy"),
      getDoubleContent(- (2.0/4 * log4(2.0/4) + 1.0/4 * log4(1.0/4) + 1.0/4 * log4(1.0/4))))
  }

  it should "prepare, reduce and present multiple with non-strict, non-nan, non-all and negate with log" in {
    val obj = Entropy("entropy", false, false, false, true, log4 _)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe None

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val c = obj.presentMultiple(Position1D("foo"), r3)
    c shouldBe Collection(Position2D("foo", "entropy"),
      getDoubleContent(2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3)))
  }

  it should "prepare, reduce and present multiple with non-strict, non-nan, non-all and non-negate with log" in {
    val obj = Entropy("entropy", false, false, false, false, log4 _)

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe Some(Map[String, Long]("abc" -> 1))

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t3 = obj.prepare(slice, cell3)
    t3 shouldBe Some(Map[String, Long]("xyz" -> 1))

    val t4 = obj.prepare(slice, cell4)
    t4 shouldBe None

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 1))

    val r2 = obj.reduce(r1, t3)
    r2 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val r3 = obj.reduce(r2, t4)
    r3 shouldBe Some(Map[String, Long]("abc" -> 1, "xyz" -> 2))

    val c = obj.presentMultiple(Position1D("foo"), r3)
    c shouldBe Collection(Position2D("foo", "entropy"),
      getDoubleContent(- (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))
  }

  "A compute" should "compute" in {
    Entropy.compute(List(1,2), true, false, log2 _) shouldBe
      Some(getDoubleContent(- (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    Entropy.compute(List(1,2), true, false, log4 _) shouldBe
      Some(getDoubleContent(- (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))
  }

  it should "compute with nan" in {
    Entropy.compute(List(2), true, false, log2 _).flatMap(_.value.asDouble.map(_.compare(Double.NaN))) shouldBe Some(0)
    Entropy.compute(List(2), false, false, log2 _) shouldBe None
  }

  it should "compute and negate" in {
    Entropy.compute(List(1,2), true, true, log2 _) shouldBe
      Some(getDoubleContent(2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3)))
    Entropy.compute(List(1,2), true, true, log4 _) shouldBe
      Some(getDoubleContent(2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3)))
  }
}

class TestCombinationReducerMultiple extends TestReducers {

  val cell1 = Cell(Position2D("foo", "one"), getDoubleContent(1))
  val cell2 = Cell(Position2D("foo", "two"), getDoubleContent(2))
  val slice = Over[Position2D, First.type](First)

  "A CombinationReducerMultiple" should "prepare, reduce and present" in {
    val obj = CombinationReducerMultiple(List(Min("min"), Max("max")))

    val t1 = obj.prepare(slice, cell1)
    t1 shouldBe List(1, 1)

    val t2 = obj.prepare(slice, cell2)
    t2 shouldBe List(2, 2)

    val r = obj.reduce(t1, t2)
    r shouldBe List(1, 2)

    val c = obj.presentMultiple(Position1D("foo"), r)
    c shouldBe Collection(List(Cell(Position2D("foo", "min"), getDoubleContent(1)),
      Cell(Position2D("foo", "max"), getDoubleContent(2))))
  }
}

class TestCombinationReducerMultipleWithValue extends TestReducers {

  val cell1 = Cell(Position2D("foo", 1), getDoubleContent(-1))
  val cell2 = Cell(Position2D("bar", 2), getDoubleContent(1))
  val slice = Over[Position2D, First.type](First)
  val ext = Map(Position1D("foo.model1") -> getDoubleContent(3.14), Position1D("bar.model1") -> getDoubleContent(6.28),
    Position1D("bar.model2") -> getDoubleContent(-3.14))

  type R = Reducer with PrepareWithValue with PresentMultiple { type V >: ext.type }

  "A CombinationReducerMultipleWithValue" should "prepare, reduce and present" in {
    val obj = CombinationReducerMultipleWithValue[R, ext.type](List(WeightedSum(First, "result1", "%1$s.model1"),
      WeightedSum(First, "result2", "%1$s.model2")))

    val t1 = obj.prepare(slice, cell1, ext)
    t1 shouldBe List(-3.14, 0)

    val t2 = obj.prepare(slice, cell2, ext)
    t2 shouldBe List(6.28, -3.14)

    val r1 = obj.reduce(t1, t2)
    r1 shouldBe List(3.14, -3.14)

    val c = obj.presentMultiple(Position1D("foo"), r1)
    c shouldBe Collection(List(Cell(Position2D("foo", "result1"), getDoubleContent(3.14)),
      Cell(Position2D("foo", "result2"), getDoubleContent(-3.14))))
  }
}

