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
import au.com.cba.omnia.grimlock.framework.window._

import au.com.cba.omnia.grimlock.library.window._

trait TestBatchMovingAverage extends TestGrimlock {
  // test prepare&initilise
  val cell = Cell(Position3D("foo", "bar", "baz"), Content(ContinuousSchema(LongCodex), 1))
  val rem = Position2D("bar", "baz")
  val in = 1.0
  val first = (rem, in)
  val second = (rem, in)

  // test present
  val sel = Position1D("sales")

  def createCell(year: String, value: Double) = {
    List(Cell(Position2D("sales", year), Content(ContinuousSchema(DoubleCodex), value)))
  }
}

class TestSimpleMovingAverage extends TestBatchMovingAverage {

  "A SimpleMovingAverage" should "prepare correctly" in {
    SimpleMovingAverage(1, Locate.AppendRemainderDimension[Position1D, Position2D](First), false)
      .prepare(cell) shouldBe (in)
  }

  it should "initialise correctly" in {
    SimpleMovingAverage(1, Locate.AppendRemainderDimension[Position1D, Position2D](First), false)
      .initialise(rem, in) shouldBe ((List(first), List()))
    SimpleMovingAverage(1, Locate.AppendRemainderDimension[Position1D, Position2D](First), true)
      .initialise(rem, in) shouldBe ((List(first), List((rem, 1.0))))
    SimpleMovingAverage(1, Locate.AppendRemainderDimension[Position1D, Position2D](Second), false)
      .initialise(rem, in) shouldBe ((List(second), List()))
    SimpleMovingAverage(1, Locate.AppendRemainderDimension[Position1D, Position2D](Second), true)
      .initialise(rem, in) shouldBe ((List(second), List((rem, 1.0))))
    SimpleMovingAverage(5, Locate.AppendRemainderDimension[Position1D, Position2D](First), false)
      .initialise(rem, in) shouldBe ((List(first), List()))
    SimpleMovingAverage(5, Locate.AppendRemainderDimension[Position1D, Position2D](First), true)
      .initialise(rem, in) shouldBe ((List(first), List((rem, 1.0))))
    SimpleMovingAverage(5, Locate.AppendRemainderDimension[Position1D, Position2D](Second), false)
      .initialise(rem, in) shouldBe ((List(second), List()))
    SimpleMovingAverage(5, Locate.AppendRemainderDimension[Position1D, Position2D](Second), true)
      .initialise(rem, in) shouldBe ((List(second), List((rem, 1.0))))
  }

  it should "update correctly" in {
    val obj = SimpleMovingAverage(5, Locate.AppendRemainderDimension[Position1D, Position1D](First), false)

    val init = obj.initialise(Position1D("2003"), 4.0)
    init shouldBe ((List((Position1D("2003"), 4.0)), List()))

    val first = obj.update(Position1D("2004"), 6.0, init._1)
    first shouldBe ((List((Position1D("2003"), 4.0), (Position1D("2004"), 6.0)), List()))

    val second = obj.update(Position1D("2005"), 5.0, first._1)
    second shouldBe ((List((Position1D("2003"), 4.0), (Position1D("2004"), 6.0), (Position1D("2005"), 5.0)), List()))

    val third = obj.update(Position1D("2006"), 8.0, second._1)
    third shouldBe ((List((Position1D("2003"), 4.0), (Position1D("2004"), 6.0), (Position1D("2005"), 5.0),
      (Position1D("2006"), 8.0)), List()))

    val fourth = obj.update(Position1D("2007"), 9.0, third._1)
    fourth shouldBe ((List((Position1D("2003"), 4.0), (Position1D("2004"), 6.0), (Position1D("2005"), 5.0),
      (Position1D("2006"), 8.0), (Position1D("2007"), 9.0)), List((Position1D("2007"), 6.4))))

    val fifth = obj.update(Position1D("2008"), 5.0, fourth._1)
    fifth shouldBe ((List((Position1D("2004"), 6.0), (Position1D("2005"), 5.0), (Position1D("2006"), 8.0),
      (Position1D("2007"), 9.0), (Position1D("2008"), 5.0)), List((Position1D("2008"), 6.6))))
  }

  it should "update all correctly" in {
    val obj = SimpleMovingAverage(5, Locate.AppendRemainderDimension[Position1D, Position1D](First), true)

    val init = obj.initialise(Position1D("2003"), 4.0)
    init shouldBe ((List((Position1D("2003"), 4.0)), List((Position1D("2003"), 4.0))))

    val first = obj.update(Position1D("2004"), 6.0, init._1)
    first shouldBe ((List((Position1D("2003"), 4.0), (Position1D("2004"), 6.0)), List((Position1D("2004"), 5))))

    val second = obj.update(Position1D("2005"), 5.0, first._1)
    second shouldBe ((List((Position1D("2003"), 4.0), (Position1D("2004"), 6.0), (Position1D("2005"), 5.0)),
      List((Position1D("2005"), 5))))

    val third = obj.update(Position1D("2006"), 8.0, second._1)
    third shouldBe ((List((Position1D("2003"), 4.0), (Position1D("2004"), 6.0), (Position1D("2005"), 5.0),
      (Position1D("2006"), 8.0)), List((Position1D("2006"), 5.75))))

    val fourth = obj.update(Position1D("2007"), 9.0, third._1)
    fourth shouldBe ((List((Position1D("2003"), 4.0), (Position1D("2004"), 6.0), (Position1D("2005"), 5.0),
      (Position1D("2006"), 8.0), (Position1D("2007"), 9.0)), List((Position1D("2007"), 6.4))))

    val fifth = obj.update(Position1D("2008"), 5.0, fourth._1)
    fifth shouldBe ((List((Position1D("2004"), 6.0), (Position1D("2005"), 5.0), (Position1D("2006"), 8.0),
      (Position1D("2007"), 9.0), (Position1D("2008"), 5.0)), List((Position1D("2008"), 6.6))))
  }

  it should "present" in {
    SimpleMovingAverage(1, Locate.AppendRemainderDimension[Position1D, Position1D](First), false)
      .present(sel, (Position1D("2008"), 6.6)) shouldBe createCell("2008", 6.6)
  }
}

class TestCenteredMovingAverage extends TestBatchMovingAverage {

  "A CenteredMovingAverage" should "prepare correctly" in {
    CenteredMovingAverage(1, Locate.AppendRemainderDimension[Position1D, Position2D](First))
      .prepare(cell) shouldBe (in)
  }

  it should "initialise correctly" in {
    CenteredMovingAverage(1, Locate.AppendRemainderDimension[Position1D, Position2D](First))
      .initialise(rem, in) shouldBe ((List(first), List()))
    CenteredMovingAverage(1, Locate.AppendRemainderDimension[Position1D, Position2D](Second))
      .initialise(rem, in) shouldBe ((List(second), List()))
    CenteredMovingAverage(5, Locate.AppendRemainderDimension[Position1D, Position2D](First))
      .initialise(rem, in) shouldBe ((List(first), List()))
    CenteredMovingAverage(5, Locate.AppendRemainderDimension[Position1D, Position2D](Second))
      .initialise(rem, in) shouldBe ((List(second), List()))
  }

  it should "update correctly" in {
    val obj = CenteredMovingAverage(2, Locate.AppendRemainderDimension[Position1D, Position1D](First))

    val init = obj.initialise(Position1D("2003"), 4.0)
    init shouldBe ((List((Position1D("2003"), 4.0)), List()))

    val first = obj.update(Position1D("2004"), 6.0, init._1)
    first shouldBe ((List((Position1D("2003"), 4.0), (Position1D("2004"), 6.0)), List()))

    val second = obj.update(Position1D("2005"), 5.0, first._1)
    second shouldBe ((List((Position1D("2003"), 4.0), (Position1D("2004"), 6.0), (Position1D("2005"), 5.0)), List()))

    val third = obj.update(Position1D("2006"), 8.0, second._1)
    third shouldBe ((List((Position1D("2003"), 4.0), (Position1D("2004"), 6.0), (Position1D("2005"), 5.0),
      (Position1D("2006"), 8.0)), List()))

    val fourth = obj.update(Position1D("2007"), 9.0, third._1)
    fourth shouldBe ((List((Position1D("2003"), 4.0), (Position1D("2004"), 6.0), (Position1D("2005"), 5.0),
      (Position1D("2006"), 8.0), (Position1D("2007"), 9.0)), List((Position1D("2005"), 6.4))))

    val fifth = obj.update(Position1D("2008"), 5.0, fourth._1)
    fifth shouldBe ((List((Position1D("2004"), 6.0), (Position1D("2005"), 5.0), (Position1D("2006"), 8.0),
      (Position1D("2007"), 9.0), (Position1D("2008"), 5.0)), List((Position1D("2006"), 6.6))))
  }

  it should "present correctly" in {
    CenteredMovingAverage(2, Locate.AppendRemainderDimension[Position1D, Position1D](First))
      .present(sel, (Position1D("2006"), 6.6)) shouldBe createCell("2006", 6.6)
  }
}

class TestWeightedMovingAverage extends TestBatchMovingAverage {

  "A WeightedMovingAverage" should "prepare correctly" in {
    WeightedMovingAverage(1, Locate.AppendRemainderDimension[Position1D, Position2D](First), false)
      .prepare(cell) shouldBe (in)
  }

  it should "initialise correctly" in {
    WeightedMovingAverage(1, Locate.AppendRemainderDimension[Position1D, Position2D](First), false)
      .initialise(rem, in) shouldBe ((List(first), List()))
    WeightedMovingAverage(1, Locate.AppendRemainderDimension[Position1D, Position2D](First), true)
      .initialise(rem, in) shouldBe ((List(first), List((rem, in))))
    WeightedMovingAverage(1, Locate.AppendRemainderDimension[Position1D, Position2D](Second), false)
      .initialise(rem, in) shouldBe ((List(second), List()))
    WeightedMovingAverage(1, Locate.AppendRemainderDimension[Position1D, Position2D](Second), true)
      .initialise(rem, in) shouldBe ((List(second), List((rem, in))))
    WeightedMovingAverage(5, Locate.AppendRemainderDimension[Position1D, Position2D](First), false)
      .initialise(rem, in) shouldBe ((List(first), List()))
    WeightedMovingAverage(5, Locate.AppendRemainderDimension[Position1D, Position2D](First), true)
      .initialise(rem, in) shouldBe ((List(first), List((rem, in))))
    WeightedMovingAverage(5, Locate.AppendRemainderDimension[Position1D, Position2D](Second), false)
      .initialise(rem, in) shouldBe ((List(second), List()))
    WeightedMovingAverage(5, Locate.AppendRemainderDimension[Position1D, Position2D](Second), true)
      .initialise(rem, in) shouldBe ((List(second), List((rem, in))))
  }

  it should "update correctly" in {
    val obj = WeightedMovingAverage(5, Locate.AppendRemainderDimension[Position1D, Position1D](First), false)

    val init = obj.initialise(Position1D("2003"), 4.0)
    init shouldBe ((List((Position1D("2003"), 4.0)), List()))

    val first = obj.update(Position1D("2004"), 6.0, init._1)
    first shouldBe ((List((Position1D("2003"), 4.0), (Position1D("2004"), 6.0)), List()))

    val second = obj.update(Position1D("2005"), 5.0, first._1)
    second shouldBe ((List((Position1D("2003"), 4.0), (Position1D("2004"), 6.0), (Position1D("2005"), 5.0)), List()))

    val third = obj.update(Position1D("2006"), 8.0, second._1)
    third shouldBe ((List((Position1D("2003"), 4.0), (Position1D("2004"), 6.0), (Position1D("2005"), 5.0),
      (Position1D("2006"), 8.0)), List()))

    val fourth = obj.update(Position1D("2007"), 9.0, third._1)
    fourth shouldBe ((List((Position1D("2003"), 4.0), (Position1D("2004"), 6.0), (Position1D("2005"), 5.0),
      (Position1D("2006"), 8.0), (Position1D("2007"), 9.0)), List((Position1D("2007"), 7.2))))

    val fifth = obj.update(Position1D("2008"), 5.0, fourth._1)
    fifth shouldBe ((List((Position1D("2004"), 6.0), (Position1D("2005"), 5.0), (Position1D("2006"), 8.0),
      (Position1D("2007"), 9.0), (Position1D("2008"), 5.0)), List((Position1D("2008"), 6.733333333333333))))
  }

  it should "update all correctly" in {
    val obj = WeightedMovingAverage(5, Locate.AppendRemainderDimension[Position1D, Position1D](First), true)

    val init = obj.initialise(Position1D("2003"), 4.0)
    init shouldBe ((List((Position1D("2003"), 4.0)), List((Position1D("2003"), 4.0))))

    val first = obj.update(Position1D("2004"), 6.0, init._1)
    first shouldBe ((List((Position1D("2003"), 4.0), (Position1D("2004"), 6.0)),
      List((Position1D("2004"), 5.333333333333333))))

    val second = obj.update(Position1D("2005"), 5.0, first._1)
    second shouldBe ((List((Position1D("2003"), 4.0), (Position1D("2004"), 6.0), (Position1D("2005"), 5.0)),
      List((Position1D("2005"), 5.166666666666667))))

    val third = obj.update(Position1D("2006"), 8.0, second._1)
    third shouldBe ((List((Position1D("2003"), 4.0), (Position1D("2004"), 6.0), (Position1D("2005"), 5.0),
      (Position1D("2006"), 8.0)), List((Position1D("2006"), 6.3))))

    val fourth = obj.update(Position1D("2007"), 9.0, third._1)
    fourth shouldBe ((List((Position1D("2003"), 4.0), (Position1D("2004"), 6.0), (Position1D("2005"), 5.0),
      (Position1D("2006"), 8.0), (Position1D("2007"), 9.0)), List((Position1D("2007"), 7.2))))

    val fifth = obj.update(Position1D("2008"), 5.0, fourth._1)
    fifth shouldBe ((List((Position1D("2004"), 6.0), (Position1D("2005"), 5.0), (Position1D("2006"), 8.0),
      (Position1D("2007"), 9.0), (Position1D("2008"), 5.0)), List((Position1D("2008"), 6.733333333333333))))
  }

  it should "present correctly" in {
    WeightedMovingAverage(5, Locate.AppendRemainderDimension[Position1D, Position1D](First), false)
      .present(sel, (Position1D("2008"), 6.733333333333333)) shouldBe createCell("2008", 6.733333333333333)
  }
}

trait TestOnlineMovingAverage extends TestGrimlock {
  // test prepare&initilise
  val cell = Cell(Position3D("foo", "bar", "baz"), Content(ContinuousSchema(LongCodex), 1))
  val rem = Position2D("bar", "baz")
  val in = 1.0
  val first = ((1.0, 1), List((rem, in)))
  val second = ((1.0, 1), List((rem, in)))
  // test present
  val sel = Position0D()

  def createCell(str: String, value: Double) = {
    List(Cell(Position1D(str), Content(ContinuousSchema(DoubleCodex), value)))
  }
}

class TestCumulativeMovingAverage extends TestOnlineMovingAverage {

  "A CumulativeMovingAverage" should "prepare correctly" in {
    CumulativeMovingAverage(Locate.AppendRemainderDimension[Position1D, Position2D](First))
      .prepare(cell) shouldBe in
  }

  it should "initialise correctly" in {
    CumulativeMovingAverage(Locate.AppendRemainderDimension[Position1D, Position2D](First))
      .initialise(rem, in) shouldBe first
    CumulativeMovingAverage(Locate.AppendRemainderDimension[Position1D, Position2D](Second))
      .initialise(rem, in) shouldBe second
  }

  it should "update correctly" in {
    val obj = CumulativeMovingAverage(Locate.AppendRemainderDimension[Position0D, Position1D](First))

    val init = obj.initialise(Position1D("val.1"), 1.0)
    init shouldBe (((1.0, 1), List((Position1D("val.1"), 1.0))))

    val first = obj.update(Position1D("val.2"), 2.0, init._1)
    first shouldBe (((1.5, 2), List((Position1D("val.2"), 1.5))))

    val second = obj.update(Position1D("val.3"), 3.0, first._1)
    second shouldBe (((2.0, 3), List((Position1D("val.3"), 2))))

    val third = obj.update(Position1D("val.4"), 4.0, second._1)
    third shouldBe (((2.5, 4), List((Position1D("val.4"), 2.5))))

    val fourth = obj.update(Position1D("val.5"), 5.0, third._1)
    fourth shouldBe (((3.0, 5), List((Position1D("val.5"), 3))))
  }

  it should "present correctly" in {
    CumulativeMovingAverage(Locate.AppendRemainderDimension[Position0D, Position1D](First))
      .present(sel, (Position1D("val.5"), 3.0)) shouldBe createCell("val.5", 3)
  }
}

class TestExponentialMovingAverage extends TestOnlineMovingAverage {

  "A ExponentialMovingAverage" should "prepare correctly" in {
    ExponentialMovingAverage(3, Locate.AppendRemainderDimension[Position1D, Position2D](First))
      .prepare(cell) shouldBe in
  }

  it should "initialise correctly" in {
    ExponentialMovingAverage(0.33, Locate.AppendRemainderDimension[Position1D, Position2D](First))
      .initialise(rem, in) shouldBe first
    ExponentialMovingAverage(3, Locate.AppendRemainderDimension[Position1D, Position2D](First))
      .initialise(rem, in) shouldBe first
    ExponentialMovingAverage(0.33, Locate.AppendRemainderDimension[Position1D, Position2D](Second))
      .initialise(rem, in) shouldBe second
    ExponentialMovingAverage(3, Locate.AppendRemainderDimension[Position1D, Position2D](Second))
      .initialise(rem, in) shouldBe second
  }

  it should "update correctly" in {
    val obj = ExponentialMovingAverage(0.33, Locate.AppendRemainderDimension[Position0D, Position1D](First))

    val init = obj.initialise(Position1D("day.1"), 16.0)
    init shouldBe (((16.0, 1), List((Position1D("day.1"), 16.0))))

    val first = obj.update(Position1D("day.2"), 17.0, init._1)
    first shouldBe (((16.33, 2), List((Position1D("day.2"), 16.33))))

    val second = obj.update(Position1D("day.3"), 17.0, first._1)
    second shouldBe (((16.551099999999998, 3), List((Position1D("day.3"), 16.551099999999998))))

    val third = obj.update(Position1D("day.4"), 10.0, second._1)
    third shouldBe (((14.389236999999998, 4), List((Position1D("day.4"), 14.389236999999998))))

    val fourth = obj.update(Position1D("day.5"), 17.0, third._1)
    fourth shouldBe (((15.250788789999998, 5), List((Position1D("day.5"), 15.250788789999998))))
  }

  it should "present correctly" in {
    ExponentialMovingAverage(0.33, Locate.AppendRemainderDimension[Position0D, Position1D](First))
      .present(sel, (Position1D("day.5"), 15.250788789999998)) shouldBe createCell("day.5", 15.250788789999998)
  }
}

trait TestWindow extends TestGrimlock {

  val cell1 = Cell(Position3D("foo", "bar", "baz"), Content(ContinuousSchema(LongCodex), 1))
  val cell2 = Cell(Position3D("foo", "bar", "baz"), Content(NominalSchema(StringCodex), "abc"))
  val sel = Position1D("foo")
  val rem = Position2D("bar", "baz")
  val in1 = Some(1.0)
  val in2f = None
  val in2t = Some(Double.NaN)
}

class TestCumulativeSum extends TestWindow {

  def createCell(value: Double) = {
    List(Cell(Position2D("foo", "bar|baz"), Content(ContinuousSchema(DoubleCodex), value)))
  }

  "A CumulativeSum" should "prepare correctly" in {
    CumulativeSum(Locate.AppendRemainderString[Position1D, Position2D](), true)
      .prepare(cell1) shouldBe in1
    CumulativeSum(Locate.AppendRemainderString[Position1D, Position2D](), false)
      .prepare(cell1) shouldBe in1
    CumulativeSum(Locate.AppendRemainderString[Position1D, Position2D](), true)
      .prepare(cell2).map(_.compare(Double.NaN)) shouldBe (Some(0))
    CumulativeSum(Locate.AppendRemainderString[Position1D, Position2D](), false)
      .prepare(cell2) shouldBe in2f
  }

  it should "initialise correctly" in {
    val obj = CumulativeSum(Locate.AppendRemainderString[Position1D, Position2D](), true)

    obj.initialise(rem, Some(1.0)) shouldBe ((Some(1.0), List((rem, 1.0))))
    obj.initialise(rem, None) shouldBe ((None, List()))

    val init = obj.initialise(rem, Some(Double.NaN))
    init._1.map(_.compare(Double.NaN)) shouldBe (Some(0))
    init._2.toList.map { case (r, d) => (r, d.compare(Double.NaN)) } shouldBe (List((rem, 0)))
  }

  it should "update correctly strict" in {
    val obj = CumulativeSum(Locate.AppendRemainderString[Position1D, Position2D](), true)

    val init = obj.initialise(rem, in1)
    init shouldBe ((Some(1.0), List((rem, 1.0))))

    val first = obj.update(rem, in1, init._1)
    first shouldBe ((Some(2.0), List((rem, 2.0))))

    val second = obj.update(rem, in2t, first._1)
    second._1.map(_.compare(Double.NaN)) shouldBe (Some(0))
    second._2.toList.map { case (r, d) => (r, d.compare(Double.NaN)) } shouldBe (List((rem, 0)))

    val third = obj.update(rem, in1, second._1)
    third._1.map(_.compare(Double.NaN)) shouldBe (Some(0))
    third._2.toList.map { case (r, d) => (r, d.compare(Double.NaN)) } shouldBe (List((rem, 0)))
  }

  it should "update correctly strict on first" in {
    val obj = CumulativeSum(Locate.AppendRemainderString[Position1D, Position2D](), true)

    val init = obj.initialise(rem, in2t)
    init._1.map(_.compare(Double.NaN)) shouldBe (Some(0))
    init._2.toList.map { case (r, d) => (r, d.compare(Double.NaN)) } shouldBe (List((rem, 0)))

    val first = obj.update(rem, in1, init._1)
    first._1.map(_.compare(Double.NaN)) shouldBe (Some(0))
    first._2.toList.map { case (r, d) => (r, d.compare(Double.NaN)) } shouldBe (List((rem, 0)))

    val second = obj.update(rem, in2t, first._1)
    second._1.map(_.compare(Double.NaN)) shouldBe (Some(0))
    second._2.toList.map { case (r, d) => (r, d.compare(Double.NaN)) } shouldBe (List((rem, 0)))

    val third = obj.update(rem, in1, second._1)
    third._1.map(_.compare(Double.NaN)) shouldBe (Some(0))
    third._2.toList.map { case (r, d) => (r, d.compare(Double.NaN)) } shouldBe (List((rem, 0)))
  }

  it should "update correctly non-strict" in {
    val obj = CumulativeSum(Locate.AppendRemainderString[Position1D, Position2D](), false)

    val init = obj.initialise(rem, in1)
    init shouldBe ((Some(1.0), List((rem, 1.0))))

    val first = obj.update(rem, in1, init._1)
    first shouldBe ((Some(2.0), List((rem, 2.0))))

    val second = obj.update(rem, in2f, first._1)
    second shouldBe ((Some(2.0), List()))

    val third = obj.update(rem, in1, second._1)
    third shouldBe ((Some(3.0), List((rem, 3.0))))
  }

  it should "update correctly non-strict on first" in {
    val obj = CumulativeSum(Locate.AppendRemainderString[Position1D, Position2D](), false)

    val init = obj.initialise(rem, in2f)
    init shouldBe ((None, List()))

    val first = obj.update(rem, in1, init._1)
    first shouldBe ((Some(1.0), List((rem, 1.0))))

    val second = obj.update(rem, in2f, first._1)
    second shouldBe ((Some(1.0), List()))

    val third = obj.update(rem, in1, second._1)
    third shouldBe ((Some(2.0), List((rem, 2.0))))
  }

  it should "present correctly strict" in {
    CumulativeSum(Locate.AppendRemainderString[Position1D, Position2D](), true)
      .present(sel, (rem, 1.0)) shouldBe (createCell(1.0))
  }
}

class TestBinOp extends TestWindow {

  def createCell(value: Double) = {
    List(Cell(Position2D("foo", "p(bar|baz, bar|baz)"), Content(ContinuousSchema(DoubleCodex), value)))
  }

  "A BinOp" should "prepare correctly" in {
    BinOp(_ + _, Locate.AppendPairwiseString[Position1D, Position2D]("p(%1$s, %2$s)"), true)
      .prepare(cell1) shouldBe in1
    BinOp(_ + _, Locate.AppendPairwiseString[Position1D, Position2D]("p(%1$s, %2$s)"), false)
      .prepare(cell1) shouldBe in1
    BinOp(_ + _, Locate.AppendPairwiseString[Position1D, Position2D]("p(%1$s, %2$s)"), true)
      .prepare(cell2).map(_.compare(Double.NaN)) shouldBe (Some(0))
    BinOp(_ + _, Locate.AppendPairwiseString[Position1D, Position2D]("p(%1$s, %2$s)"), false)
      .prepare(cell2) shouldBe in2f
  }

  it should "initialise correctly" in {
    BinOp(_ + _, Locate.AppendPairwiseString[Position1D, Position2D]("p(%1$s, %2$s)"), true)
      .initialise(rem, in1) shouldBe (((in1, rem), List()))
    BinOp(_ + _, Locate.AppendPairwiseString[Position1D, Position2D]("p(%1$s, %2$s)"), false)
      .initialise(rem, in1) shouldBe (((in1, rem), List()))
    val init = BinOp(_ + _, Locate.AppendPairwiseString[Position1D, Position2D]("p(%1$s, %2$s)"), true)
      .initialise(rem, in2t)
    init._1._1.map(_.compare(Double.NaN)) shouldBe (Some(0))
    init._1._2 shouldBe (rem)
    init._2 shouldBe (List())
    BinOp(_ + _, Locate.AppendPairwiseString[Position1D, Position2D]("p(%1$s, %2$s)"), false)
      .initialise(rem, in2f) shouldBe (((None, rem), List()))
  }

  it should "update correctly strict" in {
    val obj = BinOp(_ + _, Locate.AppendPairwiseString[Position1D, Position2D]("p(%1$s, %2$s)"), true)

    val init = obj.initialise(rem, in1)
    init shouldBe (((Some(1.0), rem), List()))

    val first = obj.update(rem, in1, init._1)
    first shouldBe (((Some(1.0), rem), List((2.0, rem, rem))))

    val second = obj.update(rem, in2t, first._1)
    second._1._1.map(_.compare(Double.NaN)) shouldBe (Some(0))
    second._1._2 shouldBe (rem)
    second._2.toList.map { case (d, c, p) => (d.compare(Double.NaN), c, p) } shouldBe (List((0, rem, rem)))

    val third = obj.update(rem, in1, second._1)
    third._1._1.map(_.compare(Double.NaN)) shouldBe (Some(0))
    third._1._2 shouldBe (rem)
    third._2.toList.map { case (d, c, p) => (d.compare(Double.NaN), c, p) } shouldBe (List((0, rem, rem)))
  }

  it should "update correctly strict on first" in {
    val obj = BinOp(_ + _, Locate.AppendPairwiseString[Position1D, Position2D]("p(%1$s, %2$s)"), true)

    val init = obj.initialise(rem, in2t)
    init._1._1.map(_.compare(Double.NaN)) shouldBe (Some(0))
    init._1._2 shouldBe (rem)
    init._2. shouldBe (List())

    val first = obj.update(rem, in1, init._1)
    first._1._1.map(_.compare(Double.NaN)) shouldBe (Some(0))
    first._1._2 shouldBe (rem)
    first._2.toList.map { case (d, c, p) => (d.compare(Double.NaN), c, p) } shouldBe (List((0, rem, rem)))

    val second = obj.update(rem, in2t, first._1)
    second._1._1.map(_.compare(Double.NaN)) shouldBe (Some(0))
    second._1._2 shouldBe (rem)
    second._2.toList.map { case (d, c, p) => (d.compare(Double.NaN), c, p) } shouldBe (List((0, rem, rem)))

    val third = obj.update(rem, in1, second._1)
    third._1._1.map(_.compare(Double.NaN)) shouldBe (Some(0))
    third._1._2 shouldBe (rem)
    third._2.toList.map { case (d, c, p) => (d.compare(Double.NaN), c, p) } shouldBe (List((0, rem, rem)))
  }

  it should "update correctly non-strict" in {
    val obj = BinOp(_ + _, Locate.AppendPairwiseString[Position1D, Position2D]("p(%1$s, %2$s)"), false)

    val init = obj.initialise(rem, in1)
    init shouldBe (((Some(1.0), rem), List()))

    val first = obj.update(rem, in1, init._1)
    first shouldBe (((Some(1.0), rem), List((2.0, rem, rem))))

    val second = obj.update(rem, in2f, first._1)
    second shouldBe (((Some(1.0), rem), List()))

    val third = obj.update(rem, in1, second._1)
    third shouldBe (((Some(1.0), rem), List((2.0, rem, rem))))
  }

  it should "update correctly non-strict on first" in {
    val obj = BinOp(_ + _, Locate.AppendPairwiseString[Position1D, Position2D]("p(%1$s, %2$s)"), false)

    val init = obj.initialise(rem, in2f)
    init shouldBe (((None, rem), List()))

    val first = obj.update(rem, in1, init._1)
    first shouldBe (((Some(1.0), rem), List()))

    val second = obj.update(rem, in2f, first._1)
    second shouldBe (((Some(1.0), rem), List()))

    val third = obj.update(rem, in1, second._1)
    third shouldBe (((Some(1.0), rem), List((2.0, rem, rem))))
  }

  it should "present correctly" in {
    BinOp(_ + _, Locate.AppendPairwiseString[Position1D, Position2D]("p(%1$s, %2$s)"), false)
      .present(sel, (1.0, rem, rem)) shouldBe createCell(1.0)
  }
}

class TestCombinationWindow extends TestGrimlock {

  def renamer(name: String)(cell: Cell[Position2D]): Option[Position2D] = {
    Some(cell.position.update(Second, name.format(cell.position(Second).toShortString)))
  }

  "A CombinationWindow" should "present correctly" in {
    val sel = Position1D("sales")
    val obj = Windowable.LW2W[Position2D, Position1D, Position1D, Position2D](List(
      SimpleMovingAverage(5, Locate.AppendRemainderDimension[Position1D, Position1D](First), false)
        .andThenRelocate(renamer("%1$s.simple")),
      WeightedMovingAverage(5, Locate.AppendRemainderDimension[Position1D, Position1D](First), false)
        .andThenRelocate(renamer("%1$s.weighted"))))()

    val prep3 = obj.prepare(Cell(Position2D("sales", "2003"), createContent(4)))
    prep3 shouldBe (List(4.0, 4.0))

    val init = obj.initialise(Position1D("2003"), prep3)
    init shouldBe ((List(List((Position1D("2003"), 4.0)), List((Position1D("2003"), 4.0))), List(List(List(), List()))))

    val prep4 = obj.prepare(Cell(Position2D("sales", "2004"), createContent(6)))
    prep4 shouldBe (List(6.0, 6.0))

    val first = obj.update(Position1D("2004"), prep4, init._1)
    first shouldBe ((List(List((Position1D("2003"), 4.0), (Position1D("2004"), 6.0)),
      List((Position1D("2003"), 4.0), (Position1D("2004"), 6.0))), List(List(List(), List()))))

    val prep5 = obj.prepare(Cell(Position2D("sales", "2005"), createContent(5)))
    prep5 shouldBe (List(5.0, 5.0))

    val second = obj.update(Position1D("2005"), prep5, first._1)
    second shouldBe ((List(
      List((Position1D("2003"), 4.0), (Position1D("2004"), 6.0), (Position1D("2005"), 5.0)),
      List((Position1D("2003"), 4.0), (Position1D("2004"), 6.0), (Position1D("2005"), 5.0))),
      List(List(List(), List()))))

    val prep6 = obj.prepare(Cell(Position2D("sales", "2006"), createContent(8)))
    prep6 shouldBe (List(8.0, 8.0))

    val third = obj.update(Position1D("2006"), prep6, second._1)
    third shouldBe ((List(
      List((Position1D("2003"), 4.0), (Position1D("2004"), 6.0), (Position1D("2005"), 5.0), (Position1D("2006"), 8.0)),
      List((Position1D("2003"), 4.0), (Position1D("2004"), 6.0), (Position1D("2005"), 5.0), (Position1D("2006"), 8.0))),
      List(List(List(), List()))))

    val prep7 = obj.prepare(Cell(Position2D("sales", "2007"), createContent(9)))
    prep7 shouldBe (List(9.0, 9.0))

    val fourth = obj.update(Position1D("2007"), prep7, third._1)
    fourth shouldBe ((List(
      List((Position1D("2003"), 4.0), (Position1D("2004"), 6.0), (Position1D("2005"), 5.0),
        (Position1D("2006"), 8.0), (Position1D("2007"), 9.0)),
      List((Position1D("2003"), 4.0), (Position1D("2004"), 6.0), (Position1D("2005"), 5.0),
        (Position1D("2006"), 8.0), (Position1D("2007"), 9.0))),
      List(List(List((Position1D("2007"), 6.4)), List((Position1D("2007"), 7.2))))))

    val prep8 = obj.prepare(Cell(Position2D("sales", "2008"), createContent(5)))
    prep8 shouldBe (List(5.0, 5.0))

    val fifth = obj.update(Position1D("2008"), prep8, fourth._1)
    fifth shouldBe ((List(
      List((Position1D("2004"), 6.0), (Position1D("2005"), 5.0), (Position1D("2006"), 8.0),
        (Position1D("2007"), 9.0), (Position1D("2008"), 5.0)),
      List((Position1D("2004"), 6.0), (Position1D("2005"), 5.0), (Position1D("2006"), 8.0),
        (Position1D("2007"), 9.0), (Position1D("2008"), 5.0))),
      List(List(List((Position1D("2008"), 6.6)), List((Position1D("2008"), 6.733333333333333))))))

    val cells = obj.present(sel, fifth._2.toList(0))
    cells shouldBe createCell("2008", 6.6, 6.733333333333333)
  }

  def createContent(value: Long): Content = Content(ContinuousSchema(LongCodex), value)
  def createCell(year: String, value1: Double, value2: Double) = {
    List(
      Cell(Position2D("sales", year + ".simple"), Content(ContinuousSchema(DoubleCodex), value1)),
      Cell(Position2D("sales", year + ".weighted"), Content(ContinuousSchema(DoubleCodex), value2)))
  }
}

