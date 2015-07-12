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
import au.com.cba.omnia.grimlock.framework.utility._
import au.com.cba.omnia.grimlock.framework.window._

import au.com.cba.omnia.grimlock.library.window._

trait TestBatchMovingAverage extends TestGrimlock {
  // test initilise
  val cell = Cell(Position1D("foo"), Content(ContinuousSchema[Codex.LongCodex](), 1))
  val rem = Position2D("bar", "baz")
  val first = (rem, 1.0)
  val second = (rem, 1.0)

  // test present
  val sel = Position1D("sales")

  def createContent(value: Long): Content = Content(ContinuousSchema[Codex.LongCodex](), value)
  def createCollection(year: String, value: Double) = {
    Collection(Position2D("sales", year), Content(ContinuousSchema[Codex.DoubleCodex](), value))
  }
}

class TestSimpleMovingAverage extends TestBatchMovingAverage {

  "A SimpleMovingAverage" should "initialise correctly" in {
    SimpleMovingAverage(1, Locate.WindowDimension[Position1D, Position2D](First), false)
      .initialise(cell, rem) shouldBe ((List(first), Collection()))
    SimpleMovingAverage(1, Locate.WindowDimension[Position1D, Position2D](First), true)
      .initialise(cell, rem) shouldBe ((List(first), Collection(Cell(Position2D("foo", "bar"),
        Content(ContinuousSchema[Codex.DoubleCodex](), 1.0)))))
    SimpleMovingAverage(1, Locate.WindowDimension[Position1D, Position2D](Second), false)
      .initialise(cell, rem) shouldBe ((List(second), Collection()))
    SimpleMovingAverage(1, Locate.WindowDimension[Position1D, Position2D](Second), true)
      .initialise(cell, rem) shouldBe ((List(second), Collection(Cell(Position2D("foo", "baz"),
        Content(ContinuousSchema[Codex.DoubleCodex](), 1.0)))))
    SimpleMovingAverage(5, Locate.WindowDimension[Position1D, Position2D](First), false)
      .initialise(cell, rem) shouldBe ((List(first), Collection()))
    SimpleMovingAverage(5, Locate.WindowDimension[Position1D, Position2D](First), true)
      .initialise(cell, rem) shouldBe ((List(first), Collection(Cell(Position2D("foo", "bar"),
        Content(ContinuousSchema[Codex.DoubleCodex](), 1.0)))))
    SimpleMovingAverage(5, Locate.WindowDimension[Position1D, Position2D](Second), false)
      .initialise(cell, rem) shouldBe ((List(second), Collection()))
    SimpleMovingAverage(5, Locate.WindowDimension[Position1D, Position2D](Second), true)
      .initialise(cell, rem) shouldBe ((List(second), Collection(Cell(Position2D("foo", "baz"),
        Content(ContinuousSchema[Codex.DoubleCodex](), 1.0)))))
  }

  it should "present correctly" in {
    val obj = SimpleMovingAverage(5, Locate.WindowDimension[Position1D, Position1D](First), false)

    val init = obj.initialise(Cell(sel, createContent(4)), Position1D("2003"))
    init shouldBe ((List((Position1D("2003"), 4.0)), Collection()))

    val first = obj.present(Cell(sel, createContent(6)), Position1D("2004"), init._1)
    first shouldBe ((List((Position1D("2003"), 4.0), (Position1D("2004"), 6.0)), Collection()))

    val second = obj.present(Cell(sel, createContent(5)), Position1D("2005"), first._1)
    second shouldBe ((List((Position1D("2003"), 4.0), (Position1D("2004"), 6.0), (Position1D("2005"), 5.0)),
      Collection()))

    val third = obj.present(Cell(sel, createContent(8)), Position1D("2006"), second._1)
    third shouldBe ((List((Position1D("2003"), 4.0), (Position1D("2004"), 6.0), (Position1D("2005"), 5.0),
      (Position1D("2006"), 8.0)), Collection()))

    val fourth = obj.present(Cell(sel, createContent(9)), Position1D("2007"), third._1)
    fourth shouldBe ((List((Position1D("2003"), 4.0), (Position1D("2004"), 6.0), (Position1D("2005"), 5.0),
      (Position1D("2006"), 8.0), (Position1D("2007"), 9.0)), createCollection("2007", 6.4)))

    val fifth = obj.present(Cell(sel, createContent(5)), Position1D("2008"), fourth._1)
    fifth shouldBe ((List((Position1D("2004"), 6.0), (Position1D("2005"), 5.0), (Position1D("2006"), 8.0),
      (Position1D("2007"), 9.0), (Position1D("2008"), 5.0)), createCollection("2008", 6.6)))
  }

  it should "present all correctly" in {
    val obj = SimpleMovingAverage(5, Locate.WindowDimension[Position1D, Position1D](First), true)

    val init = obj.initialise(Cell(sel, createContent(4)), Position1D("2003"))
    init shouldBe ((List((Position1D("2003"), 4.0)), createCollection("2003", 4.0)))

    val first = obj.present(Cell(sel, createContent(6)), Position1D("2004"), init._1)
    first shouldBe ((List((Position1D("2003"), 4.0), (Position1D("2004"), 6.0)), createCollection("2004", 5)))

    val second = obj.present(Cell(sel, createContent(5)), Position1D("2005"), first._1)
    second shouldBe ((List((Position1D("2003"), 4.0), (Position1D("2004"), 6.0), (Position1D("2005"), 5.0)),
      createCollection("2005", 5)))

    val third = obj.present(Cell(sel, createContent(8)), Position1D("2006"), second._1)
    third shouldBe ((List((Position1D("2003"), 4.0), (Position1D("2004"), 6.0), (Position1D("2005"), 5.0),
      (Position1D("2006"), 8.0)), createCollection("2006", 5.75)))

    val fourth = obj.present(Cell(sel, createContent(9)), Position1D("2007"), third._1)
    fourth shouldBe ((List((Position1D("2003"), 4.0), (Position1D("2004"), 6.0), (Position1D("2005"), 5.0),
      (Position1D("2006"), 8.0), (Position1D("2007"), 9.0)), createCollection("2007", 6.4)))

    val fifth = obj.present(Cell(sel, createContent(5)), Position1D("2008"), fourth._1)
    fifth shouldBe ((List((Position1D("2004"), 6.0), (Position1D("2005"), 5.0), (Position1D("2006"), 8.0),
      (Position1D("2007"), 9.0), (Position1D("2008"), 5.0)), createCollection("2008", 6.6)))
  }
}

class TestCenteredMovingAverage extends TestBatchMovingAverage {

  "A CenteredMovingAverage" should "initialise correctly" in {
    CenteredMovingAverage(1, Locate.WindowDimension[Position1D, Position2D](First)).initialise(cell, rem) shouldBe
      ((List(first), Collection()))
    CenteredMovingAverage(1, Locate.WindowDimension[Position1D, Position2D](Second)).initialise(cell, rem) shouldBe
      ((List(second), Collection()))
    CenteredMovingAverage(5, Locate.WindowDimension[Position1D, Position2D](First)).initialise(cell, rem) shouldBe
      ((List(first), Collection()))
    CenteredMovingAverage(5, Locate.WindowDimension[Position1D, Position2D](Second)).initialise(cell, rem) shouldBe
      ((List(second), Collection()))
  }

  it should "present correctly" in {
    val obj = CenteredMovingAverage(2, Locate.WindowDimension[Position1D, Position1D](First))

    val init = obj.initialise(Cell(sel, createContent(4)), Position1D("2003"))
    init shouldBe ((List((Position1D("2003"), 4.0)), Collection()))

    val first = obj.present(Cell(sel, createContent(6)), Position1D("2004"), init._1)
    first shouldBe ((List((Position1D("2003"), 4.0), (Position1D("2004"), 6.0)), Collection()))

    val second = obj.present(Cell(sel, createContent(5)), Position1D("2005"), first._1)
    second shouldBe ((List((Position1D("2003"), 4.0), (Position1D("2004"), 6.0), (Position1D("2005"), 5.0)),
      Collection()))

    val third = obj.present(Cell(sel, createContent(8)), Position1D("2006"), second._1)
    third shouldBe ((List((Position1D("2003"), 4.0), (Position1D("2004"), 6.0), (Position1D("2005"), 5.0),
      (Position1D("2006"), 8.0)), Collection()))

    val fourth = obj.present(Cell(sel, createContent(9)), Position1D("2007"), third._1)
    fourth shouldBe ((List((Position1D("2003"), 4.0), (Position1D("2004"), 6.0), (Position1D("2005"), 5.0),
      (Position1D("2006"), 8.0), (Position1D("2007"), 9.0)), createCollection("2005", 6.4)))

    val fifth = obj.present(Cell(sel, createContent(5)), Position1D("2008"), fourth._1)
    fifth shouldBe ((List((Position1D("2004"), 6.0), (Position1D("2005"), 5.0), (Position1D("2006"), 8.0),
      (Position1D("2007"), 9.0), (Position1D("2008"), 5.0)), createCollection("2006", 6.6)))
  }
}

class TestWeightedMovingAverage extends TestBatchMovingAverage {

  "A WeightedMovingAverage" should "initialise correctly" in {
    WeightedMovingAverage(1, Locate.WindowDimension[Position1D, Position2D](First), false)
      .initialise(cell, rem) shouldBe ((List(first), Collection()))
    WeightedMovingAverage(1, Locate.WindowDimension[Position1D, Position2D](First), true)
      .initialise(cell, rem) shouldBe ((List(first), Collection(Cell(Position2D("foo", "bar"),
        Content(ContinuousSchema[Codex.DoubleCodex](), 1.0)))))
    WeightedMovingAverage(1, Locate.WindowDimension[Position1D, Position2D](Second), false)
      .initialise(cell, rem) shouldBe ((List(second), Collection()))
    WeightedMovingAverage(1, Locate.WindowDimension[Position1D, Position2D](Second), true)
      .initialise(cell, rem) shouldBe ((List(second), Collection(Cell(Position2D("foo", "baz"),
        Content(ContinuousSchema[Codex.DoubleCodex](), 1.0)))))
    WeightedMovingAverage(5, Locate.WindowDimension[Position1D, Position2D](First), false)
      .initialise(cell, rem) shouldBe ((List(first), Collection()))
    WeightedMovingAverage(5, Locate.WindowDimension[Position1D, Position2D](First), true)
      .initialise(cell, rem) shouldBe ((List(first), Collection(Cell(Position2D("foo", "bar"),
        Content(ContinuousSchema[Codex.DoubleCodex](), 1.0)))))
    WeightedMovingAverage(5, Locate.WindowDimension[Position1D, Position2D](Second), false)
      .initialise(cell, rem) shouldBe ((List(second), Collection()))
    WeightedMovingAverage(5, Locate.WindowDimension[Position1D, Position2D](Second), true)
      .initialise(cell, rem) shouldBe ((List(second), Collection(Cell(Position2D("foo", "baz"),
        Content(ContinuousSchema[Codex.DoubleCodex](), 1.0)))))
  }

  it should "present correctly" in {
    val obj = WeightedMovingAverage(5, Locate.WindowDimension[Position1D, Position1D](First), false)

    val init = obj.initialise(Cell(sel, createContent(4)), Position1D("2003"))
    init shouldBe ((List((Position1D("2003"), 4.0)), Collection()))

    val first = obj.present(Cell(sel, createContent(6)), Position1D("2004"), init._1)
    first shouldBe ((List((Position1D("2003"), 4.0), (Position1D("2004"), 6.0)), Collection()))

    val second = obj.present(Cell(sel, createContent(5)), Position1D("2005"), first._1)
    second shouldBe ((List((Position1D("2003"), 4.0), (Position1D("2004"), 6.0), (Position1D("2005"), 5.0)),
      Collection()))

    val third = obj.present(Cell(sel, createContent(8)), Position1D("2006"), second._1)
    third shouldBe ((List((Position1D("2003"), 4.0), (Position1D("2004"), 6.0), (Position1D("2005"), 5.0),
      (Position1D("2006"), 8.0)), Collection()))

    val fourth = obj.present(Cell(sel, createContent(9)), Position1D("2007"), third._1)
    fourth shouldBe ((List((Position1D("2003"), 4.0), (Position1D("2004"), 6.0), (Position1D("2005"), 5.0),
      (Position1D("2006"), 8.0), (Position1D("2007"), 9.0)), createCollection("2007", 7.2)))

    val fifth = obj.present(Cell(sel, createContent(5)), Position1D("2008"), fourth._1)
    fifth shouldBe ((List((Position1D("2004"), 6.0), (Position1D("2005"), 5.0), (Position1D("2006"), 8.0),
      (Position1D("2007"), 9.0), (Position1D("2008"), 5.0)), createCollection("2008", 6.733333333333333)))
  }

  it should "present all correctly" in {
    val obj = WeightedMovingAverage(5, Locate.WindowDimension[Position1D, Position1D](First), true)

    val init = obj.initialise(Cell(sel, createContent(4)), Position1D("2003"))
    init shouldBe ((List((Position1D("2003"), 4.0)), createCollection("2003", 4.0)))

    val first = obj.present(Cell(sel, createContent(6)), Position1D("2004"), init._1)
    first shouldBe ((List((Position1D("2003"), 4.0), (Position1D("2004"), 6.0)),
      createCollection("2004", 5.333333333333333)))

    val second = obj.present(Cell(sel, createContent(5)), Position1D("2005"), first._1)
    second shouldBe ((List((Position1D("2003"), 4.0), (Position1D("2004"), 6.0), (Position1D("2005"), 5.0)),
      createCollection("2005", 5.166666666666667)))

    val third = obj.present(Cell(sel, createContent(8)), Position1D("2006"), second._1)
    third shouldBe ((List((Position1D("2003"), 4.0), (Position1D("2004"), 6.0), (Position1D("2005"), 5.0),
      (Position1D("2006"), 8.0)), createCollection("2006", 6.3)))

    val fourth = obj.present(Cell(sel, createContent(9)), Position1D("2007"), third._1)
    fourth shouldBe ((List((Position1D("2003"), 4.0), (Position1D("2004"), 6.0), (Position1D("2005"), 5.0),
      (Position1D("2006"), 8.0), (Position1D("2007"), 9.0)), createCollection("2007", 7.2)))

    val fifth = obj.present(Cell(sel, createContent(5)), Position1D("2008"), fourth._1)
    fifth shouldBe ((List((Position1D("2004"), 6.0), (Position1D("2005"), 5.0), (Position1D("2006"), 8.0),
      (Position1D("2007"), 9.0), (Position1D("2008"), 5.0)), createCollection("2008", 6.733333333333333)))
  }
}

trait TestOnlineMovingAverage extends TestGrimlock {
  // test initilise
  val cell = Cell(Position1D("foo"), Content(ContinuousSchema[Codex.LongCodex](), 1))
  val rem = Position2D("bar", "baz")
  val first = ((1.0, 1), Collection(Cell(Position2D("foo", "bar"), createContent(1.0))))
  val second = ((1.0, 1), Collection(Cell(Position2D("foo", "baz"), createContent(1.0))))
  // test present
  val sel = Position0D()

  def createContent(value: Double): Content = Content(ContinuousSchema[Codex.DoubleCodex](), value)
  def createCollection(str: String, value: Double) = {
    Collection(Position1D(str), Content(ContinuousSchema[Codex.DoubleCodex](), value))
  }
}

class TestCumulativeMovingAverage extends TestOnlineMovingAverage {

  "A CumulativeMovingAverage" should "initialise correctly" in {
    CumulativeMovingAverage(Locate.WindowDimension[Position1D, Position2D](First))
      .initialise(cell, rem) shouldBe first
    CumulativeMovingAverage(Locate.WindowDimension[Position1D, Position2D](Second))
      .initialise(cell, rem) shouldBe second
  }

  it should "present correctly" in {
    val obj = CumulativeMovingAverage(Locate.WindowDimension[Position0D, Position1D](First))

    val init = obj.initialise(Cell(sel, createContent(1)), Position1D("val.1"))
    init shouldBe (((1.0, 1), createCollection("val.1", 1.0)))

    val first = obj.present(Cell(sel, createContent(2)), Position1D("val.2"), init._1)
    first shouldBe (((1.5, 2), createCollection("val.2", 1.5)))

    val second = obj.present(Cell(sel, createContent(3)), Position1D("val.3"), first._1)
    second shouldBe (((2.0, 3), createCollection("val.3", 2)))

    val third = obj.present(Cell(sel, createContent(4)), Position1D("val.4"), second._1)
    third shouldBe (((2.5, 4), createCollection("val.4", 2.5)))

    val fourth = obj.present(Cell(sel, createContent(5)), Position1D("val.5"), third._1)
    fourth shouldBe (((3.0, 5), createCollection("val.5", 3)))
  }
}

class TestExponentialMovingAverage extends TestOnlineMovingAverage {

  "A ExponentialMovingAverage" should "initialise correctly" in {
    ExponentialMovingAverage(0.33, Locate.WindowDimension[Position1D, Position2D](First))
      .initialise(cell, rem) shouldBe first
    ExponentialMovingAverage(3, Locate.WindowDimension[Position1D, Position2D](First))
      .initialise(cell, rem) shouldBe first
    ExponentialMovingAverage(0.33, Locate.WindowDimension[Position1D, Position2D](Second))
      .initialise(cell, rem) shouldBe second
    ExponentialMovingAverage(3, Locate.WindowDimension[Position1D, Position2D](Second))
      .initialise(cell, rem) shouldBe second
  }

  it should "present correctly" in {
    val obj = ExponentialMovingAverage(0.33, Locate.WindowDimension[Position0D, Position1D](First))

    val init = obj.initialise(Cell(sel, createContent(16)), Position1D("day.1"))
    init shouldBe (((16.0, 1), createCollection("day.1", 16.0)))

    val first = obj.present(Cell(sel, createContent(17)), Position1D("day.2"), init._1)
    first shouldBe (((16.33, 2), createCollection("day.2", 16.33)))

    val second = obj.present(Cell(sel, createContent(17)), Position1D("day.3"), first._1)
    second shouldBe (((16.551099999999998, 3), createCollection("day.3", 16.551099999999998)))

    val third = obj.present(Cell(sel, createContent(10)), Position1D("day.4"), second._1)
    third shouldBe (((14.389236999999998, 4), createCollection("day.4", 14.389236999999998)))

    val fourth = obj.present(Cell(sel, createContent(17)), Position1D("day.5"), third._1)
    fourth shouldBe (((15.250788789999998, 5), createCollection("day.5", 15.250788789999998)))
  }
}

trait TestWindow extends TestGrimlock {

  val cell1 = Cell(Position1D("foo"), Content(ContinuousSchema[Codex.LongCodex](), 1))
  val cell2 = Cell(Position1D("foo"), Content(NominalSchema[Codex.StringCodex](), "abc"))
  val rem = Position2D("bar", "baz")
}

class TestCumulativeSum extends TestWindow {

  val result1 = (Some(1.0), createCollection(1))
  val result2 = (None, Collection())

  def createCollection(value: Double) = {
    Collection(Position2D("foo", "bar|baz"), Content(ContinuousSchema[Codex.DoubleCodex](), value))
  }

  "A CumulativeSum" should "initialise correctly" in {
    CumulativeSum(Locate.WindowString[Position1D, Position2D](), true).initialise(cell1, rem) shouldBe result1
    CumulativeSum(Locate.WindowString[Position1D, Position2D](), false).initialise(cell1, rem) shouldBe result1
    val init = CumulativeSum(Locate.WindowString[Position1D, Position2D](), true).initialise(cell2, rem)
    init._1.map(_.compare(Double.NaN)) shouldBe (Some(0))
    init._2.toList()(0).position shouldBe (Position2D("foo", "bar|baz"))
    init._2.toList()(0).content.value.asDouble.map(_.compare(Double.NaN)) shouldBe (Some(0))
    CumulativeSum(Locate.WindowString[Position1D, Position2D](), false).initialise(cell2, rem) shouldBe result2
  }

  it should "present correctly strict" in {
    val obj = CumulativeSum(Locate.WindowString[Position1D, Position2D](), true)

    val init = obj.initialise(cell1, rem)
    init shouldBe ((Some(1.0), createCollection(1.0)))

    val first = obj.present(cell1, rem, init._1)
    first shouldBe ((Some(2.0), createCollection(2.0)))

    val second = obj.present(cell2, rem, first._1)
    second._1.map(_.compare(Double.NaN)) shouldBe (Some(0))
    second._2.toList()(0).position shouldBe (Position2D("foo", "bar|baz"))
    second._2.toList()(0).content.value.asDouble.map(_.compare(Double.NaN)) shouldBe (Some(0))

    val third = obj.present(cell1, rem, second._1)
    third._1.map(_.compare(Double.NaN)) shouldBe (Some(0))
    third._2.toList()(0).position shouldBe (Position2D("foo", "bar|baz"))
    third._2.toList()(0).content.value.asDouble.map(_.compare(Double.NaN)) shouldBe (Some(0))
  }

  it should "present correctly strict on first" in {
    val obj = CumulativeSum(Locate.WindowString[Position1D, Position2D](), true)

    val init = obj.initialise(cell2, rem)
    init._1.map(_.compare(Double.NaN)) shouldBe (Some(0))
    init._2.toList()(0).position shouldBe (Position2D("foo", "bar|baz"))
    init._2.toList()(0).content.value.asDouble.map(_.compare(Double.NaN)) shouldBe (Some(0))

    val first = obj.present(cell1, rem, init._1)
    first._1.map(_.compare(Double.NaN)) shouldBe (Some(0))
    first._2.toList()(0).position shouldBe (Position2D("foo", "bar|baz"))
    first._2.toList()(0).content.value.asDouble.map(_.compare(Double.NaN)) shouldBe (Some(0))

    val second = obj.present(cell2, rem, first._1)
    second._1.map(_.compare(Double.NaN)) shouldBe (Some(0))
    second._2.toList()(0).position shouldBe (Position2D("foo", "bar|baz"))
    second._2.toList()(0).content.value.asDouble.map(_.compare(Double.NaN)) shouldBe (Some(0))

    val third = obj.present(cell1, rem, second._1)
    third._1.map(_.compare(Double.NaN)) shouldBe (Some(0))
    third._2.toList()(0).position shouldBe (Position2D("foo", "bar|baz"))
    third._2.toList()(0).content.value.asDouble.map(_.compare(Double.NaN)) shouldBe (Some(0))
  }

  it should "present correctly non-strict" in {
    val obj = CumulativeSum(Locate.WindowString[Position1D, Position2D](), false)

    val init = obj.initialise(cell1, rem)
    init shouldBe ((Some(1.0), createCollection(1.0)))

    val first = obj.present(cell1, rem, init._1)
    first shouldBe ((Some(2.0), createCollection(2.0)))

    val second = obj.present(cell2, rem, first._1)
    second shouldBe ((Some(2.0), Collection()))

    val third = obj.present(cell1, rem, second._1)
    third shouldBe ((Some(3.0), createCollection(3.0)))
  }

  it should "present correctly non-strict on first" in {
    val obj = CumulativeSum(Locate.WindowString[Position1D, Position2D](), false)

    val init = obj.initialise(cell2, rem)
    init shouldBe ((None, Collection()))

    val first = obj.present(cell1, rem, init._1)
    first shouldBe ((Some(1.0), createCollection(1.0)))

    val second = obj.present(cell2, rem, first._1)
    second shouldBe ((Some(1.0), Collection()))

    val third = obj.present(cell1, rem, second._1)
    third shouldBe ((Some(2.0), createCollection(2.0)))
  }
}

class TestBinOp extends TestWindow {

  val result1 = ((Some(1.0), rem), Collection())
  val result2 = ((None, rem), Collection())

  def createCollection(value: Double) = {
    Collection(Position2D("foo", "p(bar|baz, bar|baz)"), Content(ContinuousSchema[Codex.DoubleCodex](), value))
  }

  "A BinOp" should "initialise correctly" in {
    BinOp(_ + _, Locate.WindowPairwiseString[Position1D, Position2D]("p(%1$s, %2$s)"), true)
      .initialise(cell1, rem) shouldBe result1
    BinOp(_ + _, Locate.WindowPairwiseString[Position1D, Position2D]("p(%1$s, %2$s)"), false)
      .initialise(cell1, rem) shouldBe result1
    val init = BinOp(_ + _, Locate.WindowPairwiseString[Position1D, Position2D]("p(%1$s, %2$s)"), true)
      .initialise(cell2, rem)
    init._1._1.map(_.compare(Double.NaN)) shouldBe (Some(0))
    init._1._2 shouldBe (rem)
    init._2 shouldBe (Collection())
    BinOp(_ + _, Locate.WindowPairwiseString[Position1D, Position2D]("p(%1$s, %2$s)"), false)
      .initialise(cell2, rem) shouldBe result2
  }

  it should "present correctly strict" in {
    val obj = BinOp(_ + _, Locate.WindowPairwiseString[Position1D, Position2D]("p(%1$s, %2$s)"), true)

    val init = obj.initialise(cell1, rem)
    init shouldBe (((Some(1.0), rem), Collection()))

    val first = obj.present(cell1, rem, init._1)
    first shouldBe (((Some(1.0), rem), createCollection(2.0)))

    val second = obj.present(cell2, rem, first._1)
    second._1._1.map(_.compare(Double.NaN)) shouldBe (Some(0))
    second._1._2 shouldBe (rem)
    second._2.toList()(0).position shouldBe (Position2D("foo", "p(bar|baz, bar|baz)"))
    second._2.toList()(0).content.value.asDouble.map(_.compare(Double.NaN)) shouldBe (Some(0))

    val third = obj.present(cell1, rem, second._1)
    third._1._1.map(_.compare(Double.NaN)) shouldBe (Some(0))
    third._1._2 shouldBe (rem)
    third._2.toList()(0).position shouldBe (Position2D("foo", "p(bar|baz, bar|baz)"))
    third._2.toList()(0).content.value.asDouble.map(_.compare(Double.NaN)) shouldBe (Some(0))
  }

  it should "present correctly strict on first" in {
    val obj = BinOp(_ + _, Locate.WindowPairwiseString[Position1D, Position2D]("p(%1$s, %2$s)"), true)

    val init = obj.initialise(cell2, rem)
    init._1._1.map(_.compare(Double.NaN)) shouldBe (Some(0))
    init._1._2 shouldBe (rem)
    init._2. shouldBe (Collection())

    val first = obj.present(cell1, rem, init._1)
    first._1._1.map(_.compare(Double.NaN)) shouldBe (Some(0))
    first._1._2 shouldBe (rem)
    first._2.toList()(0).position shouldBe (Position2D("foo", "p(bar|baz, bar|baz)"))
    first._2.toList()(0).content.value.asDouble.map(_.compare(Double.NaN)) shouldBe (Some(0))

    val second = obj.present(cell2, rem, first._1)
    second._1._1.map(_.compare(Double.NaN)) shouldBe (Some(0))
    second._1._2 shouldBe (rem)
    second._2.toList()(0).position shouldBe (Position2D("foo", "p(bar|baz, bar|baz)"))
    second._2.toList()(0).content.value.asDouble.map(_.compare(Double.NaN)) shouldBe (Some(0))

    val third = obj.present(cell1, rem, second._1)
    third._1._1.map(_.compare(Double.NaN)) shouldBe (Some(0))
    third._1._2 shouldBe (rem)
    third._2.toList()(0).position shouldBe (Position2D("foo", "p(bar|baz, bar|baz)"))
    third._2.toList()(0).content.value.asDouble.map(_.compare(Double.NaN)) shouldBe (Some(0))
  }

  it should "present correctly non-strict" in {
    val obj = BinOp(_ + _, Locate.WindowPairwiseString[Position1D, Position2D]("p(%1$s, %2$s)"), false)

    val init = obj.initialise(cell1, rem)
    init shouldBe (((Some(1.0), rem), Collection()))

    val first = obj.present(cell1, rem, init._1)
    first shouldBe (((Some(1.0), rem), createCollection(2.0)))

    val second = obj.present(cell2, rem, first._1)
    second shouldBe (((Some(1.0), rem), Collection()))

    val third = obj.present(cell1, rem, second._1)
    third shouldBe (((Some(1.0), rem), createCollection(2.0)))
  }

  it should "present correctly non-strict on first" in {
    val obj = BinOp(_ + _, Locate.WindowPairwiseString[Position1D, Position2D]("p(%1$s, %2$s)"), false)

    val init = obj.initialise(cell2, rem)
    init shouldBe (((None, rem), Collection()))

    val first = obj.present(cell1, rem, init._1)
    first shouldBe (((Some(1.0), rem), Collection()))

    val second = obj.present(cell2, rem, first._1)
    second shouldBe (((Some(1.0), rem), Collection()))

    val third = obj.present(cell1, rem, second._1)
    third shouldBe (((Some(1.0), rem), createCollection(2.0)))
  }
}

class TestQuantile extends TestGrimlock {

  val probs = List(0.25, 0.5, 0.75)
  val count = ExtractWithDimensionAndKey[Dimension.First, Position1D, String, Content](First, "count")
    .andThenPresent(_.value.asLong)
  val min = ExtractWithDimensionAndKey[Dimension.First, Position1D, String, Content](First, "min")
    .andThenPresent(_.value.asDouble)
  val max = ExtractWithDimensionAndKey[Dimension.First, Position1D, String, Content](First, "max")
    .andThenPresent(_.value.asDouble)
  val quantiser = Quantile.Type7
  val name = "quantile=%1$f"
  val cell1 = Cell(Position1D("abc"), Content(ContinuousSchema[Codex.LongCodex](), 1))
  val cell2 = Cell(Position1D("abc"), Content(ContinuousSchema[Codex.LongCodex](), 2))
  val cell4 = Cell(Position1D("abc"), Content(ContinuousSchema[Codex.LongCodex](), 4))
  val cellX = Cell(Position1D("abc"), Content(NominalSchema[Codex.StringCodex](), "def"))
  val cellY = Cell(Position1D("def"), Content(ContinuousSchema[Codex.LongCodex](), 3))
  val rem = Position1D("not.used")
  val ext = Map(Position1D("abc") -> Map(Position1D("count") -> Content(DiscreteSchema[Codex.LongCodex](), 10),
    Position1D("min") -> Content(ContinuousSchema[Codex.DoubleCodex](), 1),
    Position1D("max") -> Content(ContinuousSchema[Codex.DoubleCodex](), 10)))

  type W = Map[Position1D, Map[Position1D, Content]]

  val result1 = ((1.0, 0L, List((3L, 0.25, "quantile=25.000000"), (5L, 0.5, "quantile=50.000000"),
    (7L, 0.75, "quantile=75.000000"))), Collection(List(
      Cell(Position2D("abc", "quantile=0.000000"), Content(ContinuousSchema[Codex.DoubleCodex](), 1)),
      Cell(Position2D("abc", "quantile=100.000000"), Content(ContinuousSchema[Codex.DoubleCodex](), 10)))))

  val result2 = ((Double.NaN, 0L, List((3L, 0.25, "quantile=25.000000"), (5L, 0.5, "quantile=50.000000"),
    (7L, 0.75, "quantile=75.000000"))), Collection(List(
      Cell(Position2D("abc", "quantile=0.000000"), Content(ContinuousSchema[Codex.DoubleCodex](), 1)),
      Cell(Position2D("abc", "quantile=100.000000"), Content(ContinuousSchema[Codex.DoubleCodex](), 10)))))

  val result3 = ((Double.NaN, 0L, List()), Collection(List()))

  val result4 = ((2.0, 1L, List((3L, 0.25, "quantile=25.000000"), (5L, 0.5, "quantile=50.000000"),
    (7L, 0.75, "quantile=75.000000"))), Collection(List()))

  val result5 = ((4.0, 4L, List((3L, 0.25, "quantile=25.000000"), (5L, 0.5, "quantile=50.000000"),
    (7L, 0.75, "quantile=75.000000"))), Collection(List(Cell(Position2D("abc", "quantile=25.000000"),
      Content(ContinuousSchema[Codex.DoubleCodex](), 3.25)))))

  val result6 = ((Double.NaN, 4L, List()), Collection(List()))

  val result7 = ((Double.NaN, 4L, List((3L, 0.25, "quantile=25.000000"), (5L, 0.5, "quantile=50.000000"),
    (7L, 0.75, "quantile=75.000000"))), Collection(List(Cell(Position2D("abc", "quantile=25.000000"),
      Content(ContinuousSchema[Codex.DoubleCodex](), Double.NaN)))))

  val result8 = ((Double.NaN, 1L, List((3L, 0.25, "quantile=25.000000"), (5L, 0.5, "quantile=50.000000"),
    (7L, 0.75, "quantile=75.000000"))), Collection(List()))

  val result9 = ((3.0, 1L, List((3L, 0.25, "quantile=25.000000"), (5L, 0.5, "quantile=50.000000"),
    (7L, 0.75, "quantile=75.000000"))), Collection(List()))

  "A Quantile" should "initialise correctly" in {
    Quantile[Position1D, Position1D, W](probs, count, min, max, quantiser, name)
      .initialiseWithValue(cell1, rem, ext) shouldBe (result1)

    val init2 = Quantile[Position1D, Position1D, W](probs, count, min, max, quantiser, name)
      .initialiseWithValue(cellX, rem, ext)
    init2._1._1.compare(Double.NaN) shouldBe (0)
    init2._1._2 shouldBe (result2._1._2)
    init2._1._3 shouldBe (result2._1._3)
    init2._2 shouldBe (result2._2)

    val init3 = Quantile[Position1D, Position1D, W](probs, count, min, max, quantiser, name)
      .initialiseWithValue(cellY, rem, ext)
    init3._1._1.compare(Double.NaN) shouldBe (0)
    init3._1._2 shouldBe (result3._1._2)
    init3._1._3 shouldBe (result3._1._3)
    init3._2 shouldBe (result3._2)
  }

  it should "present correctly not at boundary" in {
    Quantile[Position1D, Position1D, W](probs, count, min, max, quantiser, name)
      .presentWithValue(cell2, rem, ext, result1._1) shouldBe (result4)
  }

  it should "present correctly at boundary" in {
    val t = (3.0, 3L, List((3L, 0.25, "quantile=25.000000"), (5L, 0.5, "quantile=50.000000"),
      (7L, 0.75, "quantile=75.000000")))

    Quantile[Position1D, Position1D, W](probs, count, min, max, quantiser, name)
      .presentWithValue(cell4, rem, ext, t) shouldBe (result5)
  }

  it should "present correctly with missing state" in {
    val t = (3.0, 3L, List())

    val present6 = Quantile[Position1D, Position1D, W](probs, count, min, max, quantiser, name)
      .presentWithValue(cell4, rem, ext, t)
    present6._1._1.compare(Double.NaN) shouldBe (0)
    present6._1._2 shouldBe (result6._1._2)
    present6._1._3 shouldBe (result6._1._3)
    present6._2 shouldBe (result6._2)
  }

  it should "present correctly with NaN" in {
    val t = (Double.NaN, 3L, List((3L, 0.25, "quantile=25.000000"), (5L, 0.5, "quantile=50.000000"),
      (7L, 0.75, "quantile=75.000000")))

    val present7 = Quantile[Position1D, Position1D, W](probs, count, min, max, quantiser, name)
      .presentWithValue(cell4, rem, ext, t)
    present7._1._1.compare(Double.NaN) shouldBe (0)
    present7._1._2 shouldBe (result7._1._2)
    present7._1._3 shouldBe (result7._1._3)
    present7._2.toList()(0).position shouldBe (result7._2.toList()(0).position)
    present7._2.toList()(0).content.value.asDouble.map(_.compare(Double.NaN)) shouldBe (Some(0))
  }

  it should "present with bad data" in {
    val present8 = Quantile[Position1D, Position1D, W](probs, count, min, max, quantiser, name)
      .presentWithValue(cellX, rem, ext, result1._1)
    present8._1._1.compare(Double.NaN) shouldBe (0)
    present8._1._2 shouldBe (result8._1._2)
    present8._1._3 shouldBe (result8._1._3)
    present8._2 shouldBe (result8._2)
  }

  it should "present with missing ext" in {
    Quantile[Position1D, Position1D, W](probs, count, min, max, quantiser, name)
      .presentWithValue(cellY, rem, ext, result1._1) shouldBe (result9)
  }
}

class TestCombinationWindow extends TestGrimlock {

  def renamer(name: String)(sel: Cell[Position1D], rem: Position1D, cell: Cell[Position2D]): Position2D = {
    cell.position.update(Second, name.format(cell.position(Second).toShortString))
  }

  "A CombinationWindow" should "present correctly" in {
    val sel = Position1D("sales")
    val obj = Windowable.LWSRSM2W[Position1D, Position1D, Window[Position1D, Position1D, Position2D]].convert(List(
      SimpleMovingAverage(5, Locate.WindowDimension[Position1D, Position1D](First), false)
        .andThenRename(renamer("%1$s.simple")),
      WeightedMovingAverage(5, Locate.WindowDimension[Position1D, Position1D](First), false)
        .andThenRename(renamer("%1$s.weighted"))))

    val init = obj.initialise(Cell(sel, createContent(4)), Position1D("2003"))
    init shouldBe ((List(List((Position1D("2003"), 4.0)), List((Position1D("2003"), 4.0))), Collection(List())))

    val first = obj.present(Cell(sel, createContent(6)), Position1D("2004"), init._1)
    first shouldBe ((List(List((Position1D("2003"), 4.0), (Position1D("2004"), 6.0)),
      List((Position1D("2003"), 4.0), (Position1D("2004"), 6.0))), Collection(List())))

    val second = obj.present(Cell(sel, createContent(5)), Position1D("2005"), first._1)
    second shouldBe ((List(
      List((Position1D("2003"), 4.0), (Position1D("2004"), 6.0), (Position1D("2005"), 5.0)),
      List((Position1D("2003"), 4.0), (Position1D("2004"), 6.0), (Position1D("2005"), 5.0))),
      Collection(List())))

    val third = obj.present(Cell(sel, createContent(8)), Position1D("2006"), second._1)
    third shouldBe ((List(
      List((Position1D("2003"), 4.0), (Position1D("2004"), 6.0), (Position1D("2005"), 5.0),
        (Position1D("2006"), 8.0)),
      List((Position1D("2003"), 4.0), (Position1D("2004"), 6.0), (Position1D("2005"), 5.0),
        (Position1D("2006"), 8.0))), Collection(List())))

    val fourth = obj.present(Cell(sel, createContent(9)), Position1D("2007"), third._1)
    fourth shouldBe ((List(
      List((Position1D("2003"), 4.0), (Position1D("2004"), 6.0), (Position1D("2005"), 5.0),
        (Position1D("2006"), 8.0), (Position1D("2007"), 9.0)),
      List((Position1D("2003"), 4.0), (Position1D("2004"), 6.0), (Position1D("2005"), 5.0),
        (Position1D("2006"), 8.0), (Position1D("2007"), 9.0))), createCollection("2007", 6.4, 7.2)))

    val fifth = obj.present(Cell(sel, createContent(5)), Position1D("2008"), fourth._1)
    fifth shouldBe ((List(
      List((Position1D("2004"), 6.0), (Position1D("2005"), 5.0), (Position1D("2006"), 8.0),
        (Position1D("2007"), 9.0), (Position1D("2008"), 5.0)),
      List((Position1D("2004"), 6.0), (Position1D("2005"), 5.0), (Position1D("2006"), 8.0),
        (Position1D("2007"), 9.0), (Position1D("2008"), 5.0))),
      createCollection("2008", 6.6, 6.733333333333333)))
  }

  def createContent(value: Long): Content = Content(ContinuousSchema[Codex.LongCodex](), value)
  def createCollection(year: String, value1: Double, value2: Double) = {
    Collection(List(
      Cell(Position2D("sales", year + ".simple"), Content(ContinuousSchema[Codex.DoubleCodex](), value1)),
      Cell(Position2D("sales", year + ".weighted"), Content(ContinuousSchema[Codex.DoubleCodex](), value2))))
  }
}

