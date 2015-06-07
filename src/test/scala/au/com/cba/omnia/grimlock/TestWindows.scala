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
  val first = (rem(First), 1.0)
  val second = (rem(Second), 1.0)

  // test present
  val sel = Position1D("sales")

  def createContent(value: Long): Content = Content(ContinuousSchema[Codex.LongCodex](), value)
  def createCollection(year: String, value: Double) = {
    Collection(Position2D("sales", year), Content(ContinuousSchema[Codex.DoubleCodex](), value))
  }
}

class TestSimpleMovingAverage extends TestBatchMovingAverage {

  "A SimpleMovingAverage" should "initialise correctly" in {
    SimpleMovingAverage[Position1D, Position2D](1, First, false).initialise(cell, rem) shouldBe
      ((List(first), Collection()))
    SimpleMovingAverage[Position1D, Position2D](1, First, true).initialise(cell, rem) shouldBe
      ((List(first), Collection(Cell(Position2D("foo", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 1.0)))))
    SimpleMovingAverage[Position1D, Position2D](1, Second, false).initialise(cell, rem) shouldBe
      ((List(second), Collection()))
    SimpleMovingAverage[Position1D, Position2D](1, Second, true).initialise(cell, rem) shouldBe
      ((List(second), Collection(Cell(Position2D("foo", "baz"), Content(ContinuousSchema[Codex.DoubleCodex](), 1.0)))))
    SimpleMovingAverage[Position1D, Position2D](5, First, false).initialise(cell, rem) shouldBe
      ((List(first), Collection()))
    SimpleMovingAverage[Position1D, Position2D](5, First, true).initialise(cell, rem) shouldBe
      ((List(first), Collection(Cell(Position2D("foo", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 1.0)))))
    SimpleMovingAverage[Position1D, Position2D](5, Second, false).initialise(cell, rem) shouldBe
      ((List(second), Collection()))
    SimpleMovingAverage[Position1D, Position2D](5, Second, true).initialise(cell, rem) shouldBe
      ((List(second), Collection(Cell(Position2D("foo", "baz"), Content(ContinuousSchema[Codex.DoubleCodex](), 1.0)))))
  }

  it should "present correctly" in {
    val obj = SimpleMovingAverage[Position1D, Position1D](5, First, false)

    val init = obj.initialise(Cell(sel, createContent(4)), Position1D("2003"))
    init shouldBe ((List((StringValue("2003"), 4.0)), Collection()))

    val first = obj.present(Cell(sel, createContent(6)), Position1D("2004"), init._1)
    first shouldBe ((List((StringValue("2003"), 4.0), (StringValue("2004"), 6.0)), Collection()))

    val second = obj.present(Cell(sel, createContent(5)), Position1D("2005"), first._1)
    second shouldBe ((List((StringValue("2003"), 4.0), (StringValue("2004"), 6.0), (StringValue("2005"), 5.0)),
      Collection()))

    val third = obj.present(Cell(sel, createContent(8)), Position1D("2006"), second._1)
    third shouldBe ((List((StringValue("2003"), 4.0), (StringValue("2004"), 6.0), (StringValue("2005"), 5.0),
      (StringValue("2006"), 8.0)), Collection()))

    val fourth = obj.present(Cell(sel, createContent(9)), Position1D("2007"), third._1)
    fourth shouldBe ((List((StringValue("2003"), 4.0), (StringValue("2004"), 6.0), (StringValue("2005"), 5.0),
      (StringValue("2006"), 8.0), (StringValue("2007"), 9.0)), createCollection("2007", 6.4)))

    val fifth = obj.present(Cell(sel, createContent(5)), Position1D("2008"), fourth._1)
    fifth shouldBe ((List((StringValue("2004"), 6.0), (StringValue("2005"), 5.0), (StringValue("2006"), 8.0),
      (StringValue("2007"), 9.0), (StringValue("2008"), 5.0)), createCollection("2008", 6.6)))
  }

  it should "present all correctly" in {
    val obj = SimpleMovingAverage[Position1D, Position1D](5, First, true)

    val init = obj.initialise(Cell(sel, createContent(4)), Position1D("2003"))
    init shouldBe ((List((StringValue("2003"), 4.0)), createCollection("2003", 4.0)))

    val first = obj.present(Cell(sel, createContent(6)), Position1D("2004"), init._1)
    first shouldBe ((List((StringValue("2003"), 4.0), (StringValue("2004"), 6.0)), createCollection("2004", 5)))

    val second = obj.present(Cell(sel, createContent(5)), Position1D("2005"), first._1)
    second shouldBe ((List((StringValue("2003"), 4.0), (StringValue("2004"), 6.0), (StringValue("2005"), 5.0)),
      createCollection("2005", 5)))

    val third = obj.present(Cell(sel, createContent(8)), Position1D("2006"), second._1)
    third shouldBe ((List((StringValue("2003"), 4.0), (StringValue("2004"), 6.0), (StringValue("2005"), 5.0),
      (StringValue("2006"), 8.0)), createCollection("2006", 5.75)))

    val fourth = obj.present(Cell(sel, createContent(9)), Position1D("2007"), third._1)
    fourth shouldBe ((List((StringValue("2003"), 4.0), (StringValue("2004"), 6.0), (StringValue("2005"), 5.0),
      (StringValue("2006"), 8.0), (StringValue("2007"), 9.0)), createCollection("2007", 6.4)))

    val fifth = obj.present(Cell(sel, createContent(5)), Position1D("2008"), fourth._1)
    fifth shouldBe ((List((StringValue("2004"), 6.0), (StringValue("2005"), 5.0), (StringValue("2006"), 8.0),
      (StringValue("2007"), 9.0), (StringValue("2008"), 5.0)), createCollection("2008", 6.6)))
  }
}

class TestCenteredMovingAverage extends TestBatchMovingAverage {

  "A CenteredMovingAverage" should "initialise correctly" in {
    CenteredMovingAverage[Position1D, Position2D](1, First).initialise(cell, rem) shouldBe
      ((List(first), Collection()))
    CenteredMovingAverage[Position1D, Position2D](1, Second).initialise(cell, rem) shouldBe
      ((List(second), Collection()))
    CenteredMovingAverage[Position1D, Position2D](5, First).initialise(cell, rem) shouldBe
      ((List(first), Collection()))
    CenteredMovingAverage[Position1D, Position2D](5, Second).initialise(cell, rem) shouldBe
      ((List(second), Collection()))
  }

  it should "present correctly" in {
    val obj = CenteredMovingAverage[Position1D, Position1D](2, First)

    val init = obj.initialise(Cell(sel, createContent(4)), Position1D("2003"))
    init shouldBe ((List((StringValue("2003"), 4.0)), Collection()))

    val first = obj.present(Cell(sel, createContent(6)), Position1D("2004"), init._1)
    first shouldBe ((List((StringValue("2003"), 4.0), (StringValue("2004"), 6.0)), Collection()))

    val second = obj.present(Cell(sel, createContent(5)), Position1D("2005"), first._1)
    second shouldBe ((List((StringValue("2003"), 4.0), (StringValue("2004"), 6.0), (StringValue("2005"), 5.0)),
      Collection()))

    val third = obj.present(Cell(sel, createContent(8)), Position1D("2006"), second._1)
    third shouldBe ((List((StringValue("2003"), 4.0), (StringValue("2004"), 6.0), (StringValue("2005"), 5.0),
      (StringValue("2006"), 8.0)), Collection()))

    val fourth = obj.present(Cell(sel, createContent(9)), Position1D("2007"), third._1)
    fourth shouldBe ((List((StringValue("2003"), 4.0), (StringValue("2004"), 6.0), (StringValue("2005"), 5.0),
      (StringValue("2006"), 8.0), (StringValue("2007"), 9.0)), createCollection("2005", 6.4)))

    val fifth = obj.present(Cell(sel, createContent(5)), Position1D("2008"), fourth._1)
    fifth shouldBe ((List((StringValue("2004"), 6.0), (StringValue("2005"), 5.0), (StringValue("2006"), 8.0),
      (StringValue("2007"), 9.0), (StringValue("2008"), 5.0)), createCollection("2006", 6.6)))
  }
}

class TestWeightedMovingAverage extends TestBatchMovingAverage {

  "A WeightedMovingAverage" should "initialise correctly" in {
    WeightedMovingAverage[Position1D, Position2D](1, First, false).initialise(cell, rem) shouldBe
      ((List(first), Collection()))
    WeightedMovingAverage[Position1D, Position2D](1, First, true).initialise(cell, rem) shouldBe
      ((List(first), Collection(Cell(Position2D("foo", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 1.0)))))
    WeightedMovingAverage[Position1D, Position2D](1, Second, false).initialise(cell, rem) shouldBe
      ((List(second), Collection()))
    WeightedMovingAverage[Position1D, Position2D](1, Second, true).initialise(cell, rem) shouldBe
      ((List(second), Collection(Cell(Position2D("foo", "baz"), Content(ContinuousSchema[Codex.DoubleCodex](), 1.0)))))
    WeightedMovingAverage[Position1D, Position2D](5, First, false).initialise(cell, rem) shouldBe
      ((List(first), Collection()))
    WeightedMovingAverage[Position1D, Position2D](5, First, true).initialise(cell, rem) shouldBe
      ((List(first), Collection(Cell(Position2D("foo", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 1.0)))))
    WeightedMovingAverage[Position1D, Position2D](5, Second, false).initialise(cell, rem) shouldBe
      ((List(second), Collection()))
    WeightedMovingAverage[Position1D, Position2D](5, Second, true).initialise(cell, rem) shouldBe
      ((List(second), Collection(Cell(Position2D("foo", "baz"), Content(ContinuousSchema[Codex.DoubleCodex](), 1.0)))))
  }

  it should "present correctly" in {
    val obj = WeightedMovingAverage[Position1D, Position1D](5, First, false)

    val init = obj.initialise(Cell(sel, createContent(4)), Position1D("2003"))
    init shouldBe ((List((StringValue("2003"), 4.0)), Collection()))

    val first = obj.present(Cell(sel, createContent(6)), Position1D("2004"), init._1)
    first shouldBe ((List((StringValue("2003"), 4.0), (StringValue("2004"), 6.0)), Collection()))

    val second = obj.present(Cell(sel, createContent(5)), Position1D("2005"), first._1)
    second shouldBe ((List((StringValue("2003"), 4.0), (StringValue("2004"), 6.0), (StringValue("2005"), 5.0)),
      Collection()))

    val third = obj.present(Cell(sel, createContent(8)), Position1D("2006"), second._1)
    third shouldBe ((List((StringValue("2003"), 4.0), (StringValue("2004"), 6.0), (StringValue("2005"), 5.0),
      (StringValue("2006"), 8.0)), Collection()))

    val fourth = obj.present(Cell(sel, createContent(9)), Position1D("2007"), third._1)
    fourth shouldBe ((List((StringValue("2003"), 4.0), (StringValue("2004"), 6.0), (StringValue("2005"), 5.0),
      (StringValue("2006"), 8.0), (StringValue("2007"), 9.0)), createCollection("2007", 7.2)))

    val fifth = obj.present(Cell(sel, createContent(5)), Position1D("2008"), fourth._1)
    fifth shouldBe ((List((StringValue("2004"), 6.0), (StringValue("2005"), 5.0), (StringValue("2006"), 8.0),
      (StringValue("2007"), 9.0), (StringValue("2008"), 5.0)), createCollection("2008", 6.733333333333333)))
  }

  it should "present all correctly" in {
    val obj = WeightedMovingAverage[Position1D, Position1D](5, First, true)

    val init = obj.initialise(Cell(sel, createContent(4)), Position1D("2003"))
    init shouldBe ((List((StringValue("2003"), 4.0)), createCollection("2003", 4.0)))

    val first = obj.present(Cell(sel, createContent(6)), Position1D("2004"), init._1)
    first shouldBe ((List((StringValue("2003"), 4.0), (StringValue("2004"), 6.0)),
      createCollection("2004", 5.333333333333333)))

    val second = obj.present(Cell(sel, createContent(5)), Position1D("2005"), first._1)
    second shouldBe ((List((StringValue("2003"), 4.0), (StringValue("2004"), 6.0), (StringValue("2005"), 5.0)),
      createCollection("2005", 5.166666666666667)))

    val third = obj.present(Cell(sel, createContent(8)), Position1D("2006"), second._1)
    third shouldBe ((List((StringValue("2003"), 4.0), (StringValue("2004"), 6.0), (StringValue("2005"), 5.0),
      (StringValue("2006"), 8.0)), createCollection("2006", 6.3)))

    val fourth = obj.present(Cell(sel, createContent(9)), Position1D("2007"), third._1)
    fourth shouldBe ((List((StringValue("2003"), 4.0), (StringValue("2004"), 6.0), (StringValue("2005"), 5.0),
      (StringValue("2006"), 8.0), (StringValue("2007"), 9.0)), createCollection("2007", 7.2)))

    val fifth = obj.present(Cell(sel, createContent(5)), Position1D("2008"), fourth._1)
    fifth shouldBe ((List((StringValue("2004"), 6.0), (StringValue("2005"), 5.0), (StringValue("2006"), 8.0),
      (StringValue("2007"), 9.0), (StringValue("2008"), 5.0)), createCollection("2008", 6.733333333333333)))
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
    CumulativeMovingAverage[Position1D, Position2D](First).initialise(cell, rem) shouldBe first
    CumulativeMovingAverage[Position1D, Position2D](Second).initialise(cell, rem) shouldBe second
  }

  it should "present correctly" in {
    val obj = CumulativeMovingAverage[Position0D, Position1D](First)

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
    ExponentialMovingAverage[Position1D, Position2D](0.33, First).initialise(cell, rem) shouldBe first
    ExponentialMovingAverage[Position1D, Position2D](3, First).initialise(cell, rem) shouldBe first
    ExponentialMovingAverage[Position1D, Position2D](0.33, Second).initialise(cell, rem) shouldBe second
    ExponentialMovingAverage[Position1D, Position2D](3, Second).initialise(cell, rem) shouldBe second
  }

  it should "present correctly" in {
    val obj = ExponentialMovingAverage[Position0D, Position1D](0.33, First)

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

class TestCombinationWindow extends TestGrimlock {

  def renamer(name: String)(sel: Cell[Position1D], rem: Position1D, cell: Cell[Position2D]): Position2D = {
    cell.position.update(Second, name.format(cell.position(Second).toShortString))
  }

  "A CombinationWindow" should "present correctly" in {
    val sel = Position1D("sales")
    val obj = Windowable.LW2W[Position1D, Position1D, Position2D, Window[Position1D, Position1D, Position2D]]
      .convert(List(
        SimpleMovingAverage[Position1D, Position1D](5, First, false).andThenRename(renamer("%1$s.simple")),
        WeightedMovingAverage[Position1D, Position1D](5, First, false).andThenRename(renamer("%1$s.weighted"))))

    val init = obj.initialise(Cell(sel, createContent(4)), Position1D("2003"))
    init shouldBe ((List(List((StringValue("2003"), 4.0)), List((StringValue("2003"), 4.0))), Collection(List())))

    val first = obj.present(Cell(sel, createContent(6)), Position1D("2004"), init._1)
    first shouldBe ((List(List((StringValue("2003"), 4.0), (StringValue("2004"), 6.0)),
      List((StringValue("2003"), 4.0), (StringValue("2004"), 6.0))), Collection(List())))

    val second = obj.present(Cell(sel, createContent(5)), Position1D("2005"), first._1)
    second shouldBe ((List(
      List((StringValue("2003"), 4.0), (StringValue("2004"), 6.0), (StringValue("2005"), 5.0)),
      List((StringValue("2003"), 4.0), (StringValue("2004"), 6.0), (StringValue("2005"), 5.0))),
      Collection(List())))

    val third = obj.present(Cell(sel, createContent(8)), Position1D("2006"), second._1)
    third shouldBe ((List(
      List((StringValue("2003"), 4.0), (StringValue("2004"), 6.0), (StringValue("2005"), 5.0),
        (StringValue("2006"), 8.0)),
      List((StringValue("2003"), 4.0), (StringValue("2004"), 6.0), (StringValue("2005"), 5.0),
        (StringValue("2006"), 8.0))), Collection(List())))

    val fourth = obj.present(Cell(sel, createContent(9)), Position1D("2007"), third._1)
    fourth shouldBe ((List(
      List((StringValue("2003"), 4.0), (StringValue("2004"), 6.0), (StringValue("2005"), 5.0),
        (StringValue("2006"), 8.0), (StringValue("2007"), 9.0)),
      List((StringValue("2003"), 4.0), (StringValue("2004"), 6.0), (StringValue("2005"), 5.0),
        (StringValue("2006"), 8.0), (StringValue("2007"), 9.0))), createCollection("2007", 6.4, 7.2)))

    val fifth = obj.present(Cell(sel, createContent(5)), Position1D("2008"), fourth._1)
    fifth shouldBe ((List(
      List((StringValue("2004"), 6.0), (StringValue("2005"), 5.0), (StringValue("2006"), 8.0),
        (StringValue("2007"), 9.0), (StringValue("2008"), 5.0)),
      List((StringValue("2004"), 6.0), (StringValue("2005"), 5.0), (StringValue("2006"), 8.0),
        (StringValue("2007"), 9.0), (StringValue("2008"), 5.0))),
      createCollection("2008", 6.6, 6.733333333333333)))
  }

  def createContent(value: Long): Content = Content(ContinuousSchema[Codex.LongCodex](), value)
  def createCollection(year: String, value1: Double, value2: Double) = {
    Collection(List(
      Cell(Position2D("sales", year + ".simple"), Content(ContinuousSchema[Codex.DoubleCodex](), value1)),
      Cell(Position2D("sales", year + ".weighted"), Content(ContinuousSchema[Codex.DoubleCodex](), value2))))
  }
}

