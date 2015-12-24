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
import au.com.cba.omnia.grimlock.framework.pairwise._
import au.com.cba.omnia.grimlock.framework.encoding._
import au.com.cba.omnia.grimlock.framework.position._

import au.com.cba.omnia.grimlock.library.pairwise._

trait TestOperators extends TestGrimlock {

   val left = Cell(Position3D("left 1", "left 2", "reml"), Content(ContinuousSchema(LongCodex), 2))
   val right = Cell(Position3D("right 1", "right 2", "remr"), Content(ContinuousSchema(LongCodex), 4))
   val reml = Position1D("reml")
   val remr = Position1D("remr")
   val separator = "."
   val pattern: String

  def getPosition(call: Int, swap: Boolean = false): Position2D = {
    val (first, second) = call match {
      case 1 => ("left 1.left 2", "right 1.right 2")
      case 2 => ("left 1.left 2", "left 1.left 2")
      case 3 => ("right 1.right 2", "left 1.left 2")
    }

    val third = swap match {
      case true => "remr"
      case false => "reml"
    }

    Position2D(pattern.format(first, second), third)
  }
  def getContent(value: Double): Content = Content(ContinuousSchema(DoubleCodex), value)
}

class TestComparer extends TestOperators {

  val pattern = "not.used"

  "All" should "keep all" in {
    All.keep(left.position, right.position) shouldBe (true)
    All.keep(left.position, left.position) shouldBe (true)
    All.keep(right.position, left.position) shouldBe (true)
  }

  "Lower" should "keep lower" in {
    Lower.keep(left.position, right.position) shouldBe (false)
    Lower.keep(left.position, left.position) shouldBe (false)
    Lower.keep(right.position, left.position) shouldBe (true)
  }

  "Lower Diagonal" should "keep lower diagonal" in {
    LowerDiagonal.keep(left.position, right.position) shouldBe (false)
    LowerDiagonal.keep(left.position, left.position) shouldBe (true)
    LowerDiagonal.keep(right.position, left.position) shouldBe (true)
  }

  "Diagonal" should "keep diagonal" in {
    Diagonal.keep(left.position, right.position) shouldBe (false)
    Diagonal.keep(left.position, left.position) shouldBe (true)
    Diagonal.keep(right.position, left.position) shouldBe (false)
  }

  "Upper Diagonal" should "keep upper diagonal" in {
    UpperDiagonal.keep(left.position, right.position) shouldBe (true)
    UpperDiagonal.keep(left.position, left.position) shouldBe (true)
    UpperDiagonal.keep(right.position, left.position) shouldBe (false)
  }

  "Upper" should "keep upper" in {
    Upper.keep(left.position, right.position) shouldBe (true)
    Upper.keep(left.position, left.position) shouldBe (false)
    Upper.keep(right.position, left.position) shouldBe (false)
  }
}

class TestPlus extends TestOperators {

   val pattern = "(%1$s plus %2$s)"

  "A Plus" should "compute" in {
    val obj = Plus(Locate.PrependPairwiseSelectedStringToRemainder[Position3D](Along(Third), pattern, true, separator))

    obj.compute(left, right) shouldBe List(Cell(getPosition(1), getContent(2 + 4)))
    obj.compute(left, left) shouldBe List(Cell(getPosition(2), getContent(2 + 2)))
    obj.compute(right, left) shouldBe List(Cell(getPosition(3, true), getContent(4 + 2)))
  }
}

class TestMinus extends TestOperators {

   val pattern = "(%1$s minus %2$s)"

  "A Minus" should "compute" in {
    val obj = Minus(Locate.PrependPairwiseSelectedStringToRemainder[Position3D](Along(Third), pattern,
      true, separator), false)

    obj.compute(left, right) shouldBe List(Cell(getPosition(1), getContent(2 - 4)))
    obj.compute(left, left) shouldBe List(Cell(getPosition(2), getContent(2 - 2)))
    obj.compute(right, left) shouldBe List(Cell(getPosition(3, true), getContent(4 - 2)))
  }

  it should "compute inverse" in {
    val obj = Minus(Locate.PrependPairwiseSelectedStringToRemainder[Position3D](Along(Third), pattern,
      true, separator), true)

    obj.compute(left, right) shouldBe List(Cell(getPosition(1), getContent(4 - 2)))
    obj.compute(left, left) shouldBe List(Cell(getPosition(2), getContent(2 - 2)))
    obj.compute(right, left) shouldBe List(Cell(getPosition(3, true), getContent(2 - 4)))
  }
}

class TestTimes extends TestOperators {

   val pattern = "(%1$s times %2$s)"

  "A Times" should "compute" in {
    val obj = Times(Locate.PrependPairwiseSelectedStringToRemainder[Position3D](Along(Third), pattern, true, separator))

    obj.compute(left, right) shouldBe List(Cell(getPosition(1), getContent(2 * 4)))
    obj.compute(left, left) shouldBe List(Cell(getPosition(2), getContent(2 * 2)))
    obj.compute(right, left) shouldBe List(Cell(getPosition(3, true), getContent(4 * 2)))
  }
}

class TestDivide extends TestOperators {

   val pattern = "(%1$s divide %2$s)"

  "A Divide" should "compute" in {
    val obj = Divide(Locate.PrependPairwiseSelectedStringToRemainder[Position3D](Along(Third), pattern,
      true, separator), false)

    obj.compute(left, right) shouldBe List(Cell(getPosition(1), getContent(2.0 / 4.0)))
    obj.compute(left, left) shouldBe List(Cell(getPosition(2), getContent(2.0 / 2.0)))
    obj.compute(right, left) shouldBe List(Cell(getPosition(3, true), getContent(4.0 / 2.0)))
  }

  it should "compute inverse" in {
    val obj = Divide(Locate.PrependPairwiseSelectedStringToRemainder[Position3D](Along(Third), pattern,
      true, separator), true)

    obj.compute(left, right) shouldBe List(Cell(getPosition(1), getContent(4.0 / 2.0)))
    obj.compute(left, left) shouldBe List(Cell(getPosition(2), getContent(2.0 / 2.0)))
    obj.compute(right, left) shouldBe List(Cell(getPosition(3, true), getContent(2.0 / 4.0)))
  }
}

class TestConcatenate extends TestOperators {

   val pattern = "concat(%1$s,%2$s)"
   val format = "%1$s+%2$s"

  "A Concatenate" should "compute" in {
    val obj = Concatenate(Locate.PrependPairwiseSelectedStringToRemainder[Position3D](Along(Third), pattern,
      true, separator), format)

    obj.compute(left, right) shouldBe List(Cell(getPosition(1), getContent(2, 4)))
    obj.compute(left, left) shouldBe List(Cell(getPosition(2), getContent(2, 2)))
    obj.compute(right, left) shouldBe List(Cell(getPosition(3, true), getContent(4, 2)))
  }

  def getContent(left: Long, right: Long): Content = {
    Content(NominalSchema(StringCodex), format.format(left.toString, right.toString))
  }
}

class TestCombinationOperator extends TestOperators {

  val pattern = "not.used"

  "A CombinationOperator" should "compute" in {
    val obj = Operable.LO2O(List(
      Plus(Locate.PrependPairwiseSelectedStringToRemainder[Position3D](Along(Third), "(%1$s+%2$s)", true)),
      Minus(Locate.PrependPairwiseSelectedStringToRemainder[Position3D](Along(Third), "(%1$s-%2$s)", true))))()

    obj.compute(left, right) shouldBe List(Cell(getPositionWithBar(1, "(%1$s+%2$s)"), getContent(2 + 4)),
      Cell(getPositionWithBar(1, "(%1$s-%2$s)"), getContent(2 - 4)))
    obj.compute(left, left) shouldBe List(Cell(getPositionWithBar(2, "(%1$s+%2$s)"), getContent(2 + 2)),
      Cell(getPositionWithBar(2, "(%1$s-%2$s)"), getContent(2 - 2)))
    obj.compute(right, left) shouldBe List(Cell(getPositionWithBar(3, "(%1$s+%2$s)", true), getContent(4 + 2)),
      Cell(getPositionWithBar(3, "(%1$s-%2$s)", true), getContent(4 - 2)))
  }

  def getPositionWithBar(call: Int, pattern: String, swap: Boolean = false): Position2D = {
    val (first, second) = call match {
      case 1 => ("left 1|left 2", "right 1|right 2")
      case 2 => ("left 1|left 2", "left 1|left 2")
      case 3 => ("right 1|right 2", "left 1|left 2")
    }

    val third = swap match {
      case true => "remr"
      case false => "reml"
    }

    Position2D(pattern.format(first, second), third)
  }
}

