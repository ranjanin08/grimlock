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

import au.com.cba.omnia.grimlock._
import au.com.cba.omnia.grimlock.content._
import au.com.cba.omnia.grimlock.content.metadata._
import au.com.cba.omnia.grimlock.derive._
import au.com.cba.omnia.grimlock.encoding._
import au.com.cba.omnia.grimlock.position._
import au.com.cba.omnia.grimlock.utility._

import org.scalatest._

trait TestBatchMovingAverage {
  // test initilise
  val sliceInitialise = Over[Position3D, First.type](First)
  val cell = Cell(Position1D("foo"), Content(ContinuousSchema[Codex.LongCodex](), 1))
  val rem = Position2D("bar", "baz")
  val first = (rem(First), 1.0)
  val second = (rem(Second), 1.0)

  // test present
  val slicePresent = Over[Position2D, Second.type](Second)
  val sel = Position1D("sales")

  def createContent(value: Long): Content = Content(ContinuousSchema[Codex.LongCodex](), value)
  def createCollection(year: String, value: Double) = {
    Collection(Position2D("sales", year), Content(ContinuousSchema[Codex.DoubleCodex](), value))
  }
}

class TestSimpleMovingAverage extends FlatSpec with Matchers with TestBatchMovingAverage {

  "A SimpleMovingAverage" should "initialise correctly" in {
    SimpleMovingAverage(1, First, false).initialise(sliceInitialise)(cell, rem) should be ((List(first), None))
    SimpleMovingAverage(1, First, true).initialise(sliceInitialise)(cell, rem) should be ((List(first), Some(first)))
    SimpleMovingAverage(1, Second, false).initialise(sliceInitialise)(cell, rem) should be ((List(second), None))
    SimpleMovingAverage(1, Second, true).initialise(sliceInitialise)(cell, rem) should be ((List(second), Some(second)))
    SimpleMovingAverage(5, First, false).initialise(sliceInitialise)(cell, rem) should be ((List(first), None))
    SimpleMovingAverage(5, First, true).initialise(sliceInitialise)(cell, rem) should be ((List(first), Some(first)))
    SimpleMovingAverage(5, Second, false).initialise(sliceInitialise)(cell, rem) should be ((List(second), None))
    SimpleMovingAverage(5, Second, true).initialise(sliceInitialise)(cell, rem) should be ((List(second), Some(second)))
  }

  it should "present correctly" in {
    val obj = SimpleMovingAverage(5, First, false)

    val init = obj.initialise(slicePresent)(Cell(sel, createContent(4)), Position1D("2003"))
    init should be ((List((StringValue("2003"), 4.0)), None))

    val first = obj.present(slicePresent)(Cell(sel, createContent(6)), Position1D("2004"), init)
    first should be (((List((StringValue("2003"), 4.0),
      (StringValue("2004"), 6.0)), None), Collection()))

    val second = obj.present(slicePresent)(Cell(sel, createContent(5)), Position1D("2005"), first._1)
    second should be (((List((StringValue("2003"), 4.0),
      (StringValue("2004"), 6.0),
      (StringValue("2005"), 5.0)), None), Collection()))

    val third = obj.present(slicePresent)(Cell(sel, createContent(8)), Position1D("2006"), second._1)
    third should be (((List((StringValue("2003"), 4.0),
      (StringValue("2004"), 6.0),
      (StringValue("2005"), 5.0),
      (StringValue("2006"), 8.0)), None), Collection()))

    val fourth = obj.present(slicePresent)(Cell(sel, createContent(9)), Position1D("2007"), third._1)
    fourth should be (((List((StringValue("2003"), 4.0),
      (StringValue("2004"), 6.0),
      (StringValue("2005"), 5.0),
      (StringValue("2006"), 8.0),
      (StringValue("2007"), 9.0)), None), createCollection("2007", 6.4)))

    val fifth = obj.present(slicePresent)(Cell(sel, createContent(5)), Position1D("2008"), fourth._1)
    fifth should be (((List((StringValue("2004"), 6.0),
      (StringValue("2005"), 5.0),
      (StringValue("2006"), 8.0),
      (StringValue("2007"), 9.0),
      (StringValue("2008"), 5.0)), None), createCollection("2008", 6.6)))
  }

  it should "present all correctly" in {
    val obj = SimpleMovingAverage(5, First, true)

    val init = obj.initialise(slicePresent)(Cell(sel, createContent(4)), Position1D("2003"))
    init should be ((List((StringValue("2003"), 4.0)), Some((StringValue("2003"), 4.0))))

    val first = obj.present(slicePresent)(Cell(sel, createContent(6)), Position1D("2004"), init)
    first should be (((List((StringValue("2003"), 4.0),
      (StringValue("2004"), 6.0)), None), Collection(List(
        Cell(Position2D("sales", "2003"), Content(ContinuousSchema[Codex.DoubleCodex](), 4)),
        Cell(Position2D("sales", "2004"), Content(ContinuousSchema[Codex.DoubleCodex](), 5))))))

    val second = obj.present(slicePresent)(Cell(sel, createContent(5)), Position1D("2005"), first._1)
    second should be (((List((StringValue("2003"), 4.0),
      (StringValue("2004"), 6.0),
      (StringValue("2005"), 5.0)), None), createCollection("2005", 5)))

    val third = obj.present(slicePresent)(Cell(sel, createContent(8)), Position1D("2006"), second._1)
    third should be (((List((StringValue("2003"), 4.0),
      (StringValue("2004"), 6.0),
      (StringValue("2005"), 5.0),
      (StringValue("2006"), 8.0)), None), createCollection("2006", 5.75)))

    val fourth = obj.present(slicePresent)(Cell(sel, createContent(9)), Position1D("2007"), third._1)
    fourth should be (((List((StringValue("2003"), 4.0),
      (StringValue("2004"), 6.0),
      (StringValue("2005"), 5.0),
      (StringValue("2006"), 8.0),
      (StringValue("2007"), 9.0)), None), createCollection("2007", 6.4)))

    val fifth = obj.present(slicePresent)(Cell(sel, createContent(5)), Position1D("2008"), fourth._1)
    fifth should be (((List((StringValue("2004"), 6.0),
      (StringValue("2005"), 5.0),
      (StringValue("2006"), 8.0),
      (StringValue("2007"), 9.0),
      (StringValue("2008"), 5.0)), None), createCollection("2008", 6.6)))
  }
}

class TestCenteredMovingAverage extends FlatSpec with Matchers with TestBatchMovingAverage {

  "A CenteredMovingAverage" should "initialise correctly" in {
    CenteredMovingAverage(1, First).initialise(sliceInitialise)(cell, rem) should be ((List(first), None))
    CenteredMovingAverage(1, Second).initialise(sliceInitialise)(cell, rem) should be ((List(second), None))
    CenteredMovingAverage(5, First).initialise(sliceInitialise)(cell, rem) should be ((List(first), None))
    CenteredMovingAverage(5, Second).initialise(sliceInitialise)(cell, rem) should be ((List(second), None))
  }

  it should "present correctly" in {
    val obj = CenteredMovingAverage(2, First)

    val init = obj.initialise(slicePresent)(Cell(sel, createContent(4)), Position1D("2003"))
    init should be ((List((StringValue("2003"), 4.0)), None))

    val first = obj.present(slicePresent)(Cell(sel, createContent(6)), Position1D("2004"), init)
    first should be (((List((StringValue("2003"), 4.0),
      (StringValue("2004"), 6.0)), None), Collection()))

    val second = obj.present(slicePresent)(Cell(sel, createContent(5)), Position1D("2005"), first._1)
    second should be (((List((StringValue("2003"), 4.0),
      (StringValue("2004"), 6.0),
      (StringValue("2005"), 5.0)), None), Collection()))

    val third = obj.present(slicePresent)(Cell(sel, createContent(8)), Position1D("2006"), second._1)
    third should be (((List((StringValue("2003"), 4.0),
      (StringValue("2004"), 6.0),
      (StringValue("2005"), 5.0),
      (StringValue("2006"), 8.0)), None), Collection()))

    val fourth = obj.present(slicePresent)(Cell(sel, createContent(9)), Position1D("2007"), third._1)
    fourth should be (((List((StringValue("2003"), 4.0),
      (StringValue("2004"), 6.0),
      (StringValue("2005"), 5.0),
      (StringValue("2006"), 8.0),
      (StringValue("2007"), 9.0)), None), createCollection("2005", 6.4)))

    val fifth = obj.present(slicePresent)(Cell(sel, createContent(5)), Position1D("2008"), fourth._1)
    fifth should be (((List((StringValue("2004"), 6.0),
      (StringValue("2005"), 5.0),
      (StringValue("2006"), 8.0),
      (StringValue("2007"), 9.0),
      (StringValue("2008"), 5.0)), None), createCollection("2006", 6.6)))
  }
}

class TestWeightedMovingAverage extends FlatSpec with Matchers with TestBatchMovingAverage {

  "A WeightedMovingAverage" should "initialise correctly" in {
    WeightedMovingAverage(1, First, false).initialise(sliceInitialise)(cell, rem) should be ((List(first), None))
    WeightedMovingAverage(1, First, true).initialise(sliceInitialise)(cell, rem) should be ((List(first), Some(first)))
    WeightedMovingAverage(1, Second, false).initialise(sliceInitialise)(cell, rem) should be ((List(second), None))
    WeightedMovingAverage(1, Second, true).initialise(sliceInitialise)(cell, rem) should
      be ((List(second), Some(second)))
    WeightedMovingAverage(5, First, false).initialise(sliceInitialise)(cell, rem) should be ((List(first), None))
    WeightedMovingAverage(5, First, true).initialise(sliceInitialise)(cell, rem) should be ((List(first), Some(first)))
    WeightedMovingAverage(5, Second, false).initialise(sliceInitialise)(cell, rem) should be ((List(second), None))
    WeightedMovingAverage(5, Second, true).initialise(sliceInitialise)(cell, rem) should
      be ((List(second), Some(second)))
  }

  it should "present correctly" in {
    val obj = WeightedMovingAverage(5, First, false)

    val init = obj.initialise(slicePresent)(Cell(sel, createContent(4)), Position1D("2003"))
    init should be ((List((StringValue("2003"), 4.0)), None))

    val first = obj.present(slicePresent)(Cell(sel, createContent(6)), Position1D("2004"), init)
    first should be (((List((StringValue("2003"), 4.0),
      (StringValue("2004"), 6.0)), None), Collection()))

    val second = obj.present(slicePresent)(Cell(sel, createContent(5)), Position1D("2005"), first._1)
    second should be (((List((StringValue("2003"), 4.0),
      (StringValue("2004"), 6.0),
      (StringValue("2005"), 5.0)), None), Collection()))

    val third = obj.present(slicePresent)(Cell(sel, createContent(8)), Position1D("2006"), second._1)
    third should be (((List((StringValue("2003"), 4.0),
      (StringValue("2004"), 6.0),
      (StringValue("2005"), 5.0),
      (StringValue("2006"), 8.0)), None), Collection()))

    val fourth = obj.present(slicePresent)(Cell(sel, createContent(9)), Position1D("2007"), third._1)
    fourth should be (((List((StringValue("2003"), 4.0),
      (StringValue("2004"), 6.0),
      (StringValue("2005"), 5.0),
      (StringValue("2006"), 8.0),
      (StringValue("2007"), 9.0)), None), createCollection("2007", 7.2)))

    val fifth = obj.present(slicePresent)(Cell(sel, createContent(5)), Position1D("2008"), fourth._1)
    fifth should be (((List((StringValue("2004"), 6.0),
      (StringValue("2005"), 5.0),
      (StringValue("2006"), 8.0),
      (StringValue("2007"), 9.0),
      (StringValue("2008"), 5.0)), None), createCollection("2008", 6.733333333333333)))
  }

  it should "present all correctly" in {
    val obj = WeightedMovingAverage(5, First, true)

    val init = obj.initialise(slicePresent)(Cell(sel, createContent(4)), Position1D("2003"))
    init should be ((List((StringValue("2003"), 4.0)), Some((StringValue("2003"), 4.0))))

    val first = obj.present(slicePresent)(Cell(sel, createContent(6)), Position1D("2004"), init)
    first should be (((List((StringValue("2003"), 4.0),
      (StringValue("2004"), 6.0)), None), Collection(List(
        Cell(Position2D("sales", "2003"), Content(ContinuousSchema[Codex.DoubleCodex](), 4)),
        Cell(Position2D("sales", "2004"), Content(ContinuousSchema[Codex.DoubleCodex](), 5.333333333333333))))))

    val second = obj.present(slicePresent)(Cell(sel, createContent(5)), Position1D("2005"), first._1)
    second should be (((List((StringValue("2003"), 4.0),
      (StringValue("2004"), 6.0),
      (StringValue("2005"), 5.0)), None), createCollection("2005", 5.166666666666667)))

    val third = obj.present(slicePresent)(Cell(sel, createContent(8)), Position1D("2006"), second._1)
    third should be (((List((StringValue("2003"), 4.0),
      (StringValue("2004"), 6.0),
      (StringValue("2005"), 5.0),
      (StringValue("2006"), 8.0)), None), createCollection("2006", 6.3)))

    val fourth = obj.present(slicePresent)(Cell(sel, createContent(9)), Position1D("2007"), third._1)
    fourth should be (((List((StringValue("2003"), 4.0),
      (StringValue("2004"), 6.0),
      (StringValue("2005"), 5.0),
      (StringValue("2006"), 8.0),
      (StringValue("2007"), 9.0)), None), createCollection("2007", 7.2)))

    val fifth = obj.present(slicePresent)(Cell(sel, createContent(5)), Position1D("2008"), fourth._1)
    fifth should be (((List((StringValue("2004"), 6.0),
      (StringValue("2005"), 5.0),
      (StringValue("2006"), 8.0),
      (StringValue("2007"), 9.0),
      (StringValue("2008"), 5.0)), None), createCollection("2008", 6.733333333333333)))
  }
}

trait TestOnlineMovingAverage {
  // test initilise
  val sliceInitialise = Over[Position3D, First.type](First)
  val cell = Cell(Position1D("foo"), Content(ContinuousSchema[Codex.LongCodex](), 1))
  val rem = Position2D("bar", "baz")
  val first = (1.0, 1, Some((rem(First), 1.0)))
  val second = (1.0, 1, Some((rem(Second), 1.0)))

  // test present
  val slicePresent = Along[Position1D, First.type](First)
  val sel = Position0D()

  def createContent(value: Double): Content = Content(ContinuousSchema[Codex.DoubleCodex](), value)
  def createCollection(str: String, value: Double) = {
    Collection(Position1D(str), Content(ContinuousSchema[Codex.DoubleCodex](), value))
  }
}

class TestCumulativeMovingAverage extends FlatSpec with Matchers with TestOnlineMovingAverage {

  "A CumulativeMovingAverage" should "initialise correctly" in {
    CumulativeMovingAverage(First).initialise(sliceInitialise)(cell, rem) should be (first)
    CumulativeMovingAverage(Second).initialise(sliceInitialise)(cell, rem) should be (second)
  }

  it should "present correctly" in {
    val obj = CumulativeMovingAverage(First)

    val init = obj.initialise(slicePresent)(Cell(sel, createContent(1)), Position1D("val.1"))
    init should be ((1.0, 1, Some((StringValue("val.1"), 1.0))))

    val first = obj.present(slicePresent)(Cell(sel, createContent(2)), Position1D("val.2"), init)
    first should be (((1.5, 2, None), Collection(List(Cell(Position1D("val.1"), createContent(1.0)),
      Cell(Position1D("val.2"), createContent(1.5))))))

    val second = obj.present(slicePresent)(Cell(sel, createContent(3)), Position1D("val.3"), first._1)
    second should be (((2.0, 3, None), createCollection("val.3", 2)))

    val third = obj.present(slicePresent)(Cell(sel, createContent(4)), Position1D("val.4"), second._1)
    third should be (((2.5, 4, None), createCollection("val.4", 2.5)))

    val fourth = obj.present(slicePresent)(Cell(sel, createContent(5)), Position1D("val.5"), third._1)
    fourth should be (((3.0, 5, None), createCollection("val.5", 3)))
  }
}

class TestExponentialMovingAverage extends FlatSpec with Matchers with TestOnlineMovingAverage {

  "A ExponentialMovingAverage" should "initialise correctly" in {
    ExponentialMovingAverage(0.33, First).initialise(sliceInitialise)(cell, rem) should be (first)
    ExponentialMovingAverage(3, First).initialise(sliceInitialise)(cell, rem) should be (first)
    ExponentialMovingAverage(0.33, Second).initialise(sliceInitialise)(cell, rem) should be (second)
    ExponentialMovingAverage(3, Second).initialise(sliceInitialise)(cell, rem) should be (second)
  }

  it should "present correctly" in {
    val obj = ExponentialMovingAverage(0.33, First)

    val init = obj.initialise(slicePresent)(Cell(sel, createContent(16)), Position1D("day.1"))
    init should be ((16.0, 1, Some((StringValue("day.1"), 16.0))))

    val first = obj.present(slicePresent)(Cell(sel, createContent(17)), Position1D("day.2"), init)
    first should be (((16.33, 2, None), Collection(List(Cell(Position1D("day.1"), createContent(16)),
      Cell(Position1D("day.2"), createContent(16.33))))))

    val second = obj.present(slicePresent)(Cell(sel, createContent(17)), Position1D("day.3"), first._1)
    second should be (((16.551099999999998, 3, None), createCollection("day.3", 16.551099999999998)))

    val third = obj.present(slicePresent)(Cell(sel, createContent(10)), Position1D("day.4"), second._1)
    third should be (((14.389236999999998, 4, None), createCollection("day.4", 14.389236999999998)))

    val fourth = obj.present(slicePresent)(Cell(sel, createContent(17)), Position1D("day.5"), third._1)
    fourth should be (((15.250788789999998, 5, None), createCollection("day.5", 15.250788789999998)))
  }
}

class TestCombinationDeriver extends FlatSpec with Matchers {

  "A CombinationDeriver" should "present correctly" in {
    val slice = Over[Position2D, Second.type](Second)
    val sel = Position1D("sales")
    val obj = CombinationDeriver(List(
      SimpleMovingAverage(5, First, false, "%1$s.simple"),
      WeightedMovingAverage(5, First, false, "%1$s.weighted")))

    val init = obj.initialise(slice)(Cell(sel, createContent(4)), Position1D("2003"))
    init should be (List((List((StringValue("2003"), 4.0)), None),
      (List((StringValue("2003"), 4.0)), None)))

    val first = obj.present(slice)(Cell(sel, createContent(6)), Position1D("2004"), init)
    first should be ((List(
      (List((StringValue("2003"), 4.0), (StringValue("2004"), 6.0)), None),
      (List((StringValue("2003"), 4.0), (StringValue("2004"), 6.0)), None)),
      Collection(List())))

    val second = obj.present(slice)(Cell(sel, createContent(5)), Position1D("2005"), first._1)
    second should be ((List(
      (List((StringValue("2003"), 4.0), (StringValue("2004"), 6.0), (StringValue("2005"), 5.0)), None),
      (List((StringValue("2003"), 4.0), (StringValue("2004"), 6.0), (StringValue("2005"), 5.0)), None)),
      Collection(List())))

    val third = obj.present(slice)(Cell(sel, createContent(8)), Position1D("2006"), second._1)
    third should be ((List(
      (List((StringValue("2003"), 4.0), (StringValue("2004"), 6.0),
        (StringValue("2005"), 5.0), (StringValue("2006"), 8.0)), None),
      (List((StringValue("2003"), 4.0), (StringValue("2004"), 6.0),
        (StringValue("2005"), 5.0), (StringValue("2006"), 8.0)), None)),
      Collection(List())))

    val fourth = obj.present(slice)(Cell(sel, createContent(9)), Position1D("2007"), third._1)
    fourth should be ((List(
      (List((StringValue("2003"), 4.0), (StringValue("2004"), 6.0), (StringValue("2005"), 5.0),
        (StringValue("2006"), 8.0), (StringValue("2007"), 9.0)), None),
      (List((StringValue("2003"), 4.0), (StringValue("2004"), 6.0), (StringValue("2005"), 5.0),
        (StringValue("2006"), 8.0), (StringValue("2007"), 9.0)), None)),
      createCollection("2007", 6.4, 7.2)))

    val fifth = obj.present(slice)(Cell(sel, createContent(5)), Position1D("2008"), fourth._1)
    fifth should be ((List(
      (List((StringValue("2004"), 6.0), (StringValue("2005"), 5.0), (StringValue("2006"), 8.0),
        (StringValue("2007"), 9.0), (StringValue("2008"), 5.0)), None),
      (List((StringValue("2004"), 6.0), (StringValue("2005"), 5.0), (StringValue("2006"), 8.0),
        (StringValue("2007"), 9.0), (StringValue("2008"), 5.0)), None)),
      createCollection("2008", 6.6, 6.733333333333333)))
  }

  def createContent(value: Long): Content = Content(ContinuousSchema[Codex.LongCodex](), value)
  def createCollection(year: String, value1: Double, value2: Double) = {
    Collection(List(
      Cell(Position2D("sales", year + ".simple"), Content(ContinuousSchema[Codex.DoubleCodex](), value1)),
      Cell(Position2D("sales", year + ".weighted"), Content(ContinuousSchema[Codex.DoubleCodex](), value2))))
  }
}

