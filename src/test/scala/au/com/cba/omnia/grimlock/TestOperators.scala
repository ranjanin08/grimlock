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
import au.com.cba.omnia.grimlock.framework.utility._

import au.com.cba.omnia.grimlock.library.pairwise._

trait TestOperators extends TestGrimlock {

   val left = Cell(Position2D("left 1", "left 2"), Content(ContinuousSchema[Codex.LongCodex](), 2))
   val right = Cell(Position2D("right 1", "right 2"), Content(ContinuousSchema[Codex.LongCodex](), 4))
   val rem = Position1D("rem")
   val separator = "."
   val pattern: String

  def getPosition(call: Int): Position2D = {
    val (first, second) = call match {
      case 1 => ("left 1.left 2", "right 1.right 2")
      case 2 => ("left 1.left 2", "left 1.left 2")
      case 3 => ("right 1.right 2", "left 1.left 2")
    }

    Position2D(pattern.format(first, second), "rem")
  }
  def getContent(value: Double): Content = Content(ContinuousSchema[Codex.DoubleCodex](), value)
}

class TestPlus extends TestOperators {

   val pattern = "(%1$s plus %2$s)"

  "A Plus" should "compute with all" in {
    val obj = Plus[Position2D, Position1D](pattern, separator, All)

    obj.compute(left, right, rem) shouldBe Collection(getPosition(1), getContent(2 + 4))
    obj.compute(left, left, rem) shouldBe Collection(getPosition(2), getContent(2 + 2))
    obj.compute(right, left, rem) shouldBe Collection(getPosition(3), getContent(4 + 2))
  }

  it should "compute with lower" in {
    val obj = Plus[Position2D, Position1D](pattern, separator, Lower)

    obj.compute(left, right, rem) shouldBe Collection()
    obj.compute(left, left, rem) shouldBe Collection()
    obj.compute(right, left, rem) shouldBe Collection(getPosition(3), getContent(4 + 2))
  }

  it should "compute with lower diagonal" in {
    val obj = Plus[Position2D, Position1D](pattern, separator, LowerDiagonal)

    obj.compute(left, right, rem) shouldBe Collection()
    obj.compute(left, left, rem) shouldBe Collection(getPosition(2), getContent(2 + 2))
    obj.compute(right, left, rem) shouldBe Collection(getPosition(3), getContent(4 + 2))
  }

  it should "compute with diagonal" in {
    val obj = Plus[Position2D, Position1D](pattern, separator, Diagonal)

    obj.compute(left, right, rem) shouldBe Collection()
    obj.compute(left, left, rem) shouldBe Collection(getPosition(2), getContent(2 + 2))
    obj.compute(right, left, rem) shouldBe Collection()
  }

  it should "compute with upper diagonal" in {
    val obj = Plus[Position2D, Position1D](pattern, separator, UpperDiagonal)

    obj.compute(left, right, rem) shouldBe Collection(getPosition(1), getContent(2 + 4))
    obj.compute(left, left, rem) shouldBe Collection(getPosition(2), getContent(2 + 2))
    obj.compute(right, left, rem) shouldBe Collection()
  }

  it should "compute with upper" in {
    val obj = Plus[Position2D, Position1D](pattern, separator, Upper)

    obj.compute(left, right, rem) shouldBe Collection(getPosition(1), getContent(2 + 4))
    obj.compute(left, left, rem) shouldBe Collection()
    obj.compute(right, left, rem) shouldBe Collection()
  }
}

class TestMinus extends TestOperators {

   val pattern = "(%1$s minus %2$s)"

  "A Minus" should "compute with all" in {
    val obj = Minus[Position2D, Position1D](pattern, separator, false, All)

    obj.compute(left, right, rem) shouldBe Collection(getPosition(1), getContent(2 - 4))
    obj.compute(left, left, rem) shouldBe Collection(getPosition(2), getContent(2 - 2))
    obj.compute(right, left, rem) shouldBe Collection(getPosition(3), getContent(4 - 2))
  }

  it should "compute with lower" in {
    val obj = Minus[Position2D, Position1D](pattern, separator, false, Lower)

    obj.compute(left, right, rem) shouldBe Collection()
    obj.compute(left, left, rem) shouldBe Collection()
    obj.compute(right, left, rem) shouldBe Collection(getPosition(3), getContent(4 - 2))
  }

  it should "compute with lower diagonal" in {
    val obj = Minus[Position2D, Position1D](pattern, separator, false, LowerDiagonal)

    obj.compute(left, right, rem) shouldBe Collection()
    obj.compute(left, left, rem) shouldBe Collection(getPosition(2), getContent(2 - 2))
    obj.compute(right, left, rem) shouldBe Collection(getPosition(3), getContent(4 - 2))
  }

  it should "compute with diagonal" in {
    val obj = Minus[Position2D, Position1D](pattern, separator, false, Diagonal)

    obj.compute(left, right, rem) shouldBe Collection()
    obj.compute(left, left, rem) shouldBe Collection(getPosition(2), getContent(2 - 2))
    obj.compute(right, left, rem) shouldBe Collection()
  }

  it should "compute with upper diagonal" in {
    val obj = Minus[Position2D, Position1D](pattern, separator, false, UpperDiagonal)

    obj.compute(left, right, rem) shouldBe Collection(getPosition(1), getContent(2 - 4))
    obj.compute(left, left, rem) shouldBe Collection(getPosition(2), getContent(2 - 2))
    obj.compute(right, left, rem) shouldBe Collection()
  }

  it should "compute with upper" in {
    val obj = Minus[Position2D, Position1D](pattern, separator, false, Upper)

    obj.compute(left, right, rem) shouldBe Collection(getPosition(1), getContent(2 - 4))
    obj.compute(left, left, rem) shouldBe Collection()
    obj.compute(right, left, rem) shouldBe Collection()
  }

  it should "compute inverse with all" in {
    val obj = Minus[Position2D, Position1D](pattern, separator, true, All)

    obj.compute(left, right, rem) shouldBe Collection(getPosition(1), getContent(4 - 2))
    obj.compute(left, left, rem) shouldBe Collection(getPosition(2), getContent(2 - 2))
    obj.compute(right, left, rem) shouldBe Collection(getPosition(3), getContent(2 - 4))
  }

  it should "compute inverse with lower" in {
    val obj = Minus[Position2D, Position1D](pattern, separator, true, Lower)

    obj.compute(left, right, rem) shouldBe Collection()
    obj.compute(left, left, rem) shouldBe Collection()
    obj.compute(right, left, rem) shouldBe Collection(getPosition(3), getContent(2 - 4))
  }

  it should "compute inverse with lower diagonal" in {
    val obj = Minus[Position2D, Position1D](pattern, separator, true, LowerDiagonal)

    obj.compute(left, right, rem) shouldBe Collection()
    obj.compute(left, left, rem) shouldBe Collection(getPosition(2), getContent(2 - 2))
    obj.compute(right, left, rem) shouldBe Collection(getPosition(3), getContent(2 - 4))
  }

  it should "compute inverse with diagonal" in {
    val obj = Minus[Position2D, Position1D](pattern, separator, true, Diagonal)

    obj.compute(left, right, rem) shouldBe Collection()
    obj.compute(left, left, rem) shouldBe Collection(getPosition(2), getContent(2 - 2))
    obj.compute(right, left, rem) shouldBe Collection()
  }

  it should "compute inverse with upper diagonal" in {
    val obj = Minus[Position2D, Position1D](pattern, separator, true, UpperDiagonal)

    obj.compute(left, right, rem) shouldBe Collection(getPosition(1), getContent(4 - 2))
    obj.compute(left, left, rem) shouldBe Collection(getPosition(2), getContent(2 - 2))
    obj.compute(right, left, rem) shouldBe Collection()
  }

  it should "compute inverse with upper" in {
    val obj = Minus[Position2D, Position1D](pattern, separator, true, Upper)

    obj.compute(left, right, rem) shouldBe Collection(getPosition(1), getContent(4 - 2))
    obj.compute(left, left, rem) shouldBe Collection()
    obj.compute(right, left, rem) shouldBe Collection()
  }
}

class TestTimes extends TestOperators {

   val pattern = "(%1$s times %2$s)"

  "A Times" should "compute with all" in {
    val obj = Times[Position2D, Position1D](pattern, separator, All)

    obj.compute(left, right, rem) shouldBe Collection(getPosition(1), getContent(2 * 4))
    obj.compute(left, left, rem) shouldBe Collection(getPosition(2), getContent(2 * 2))
    obj.compute(right, left, rem) shouldBe Collection(getPosition(3), getContent(4 * 2))
  }

  it should "compute with lower" in {
    val obj = Times[Position2D, Position1D](pattern, separator, Lower)

    obj.compute(left, right, rem) shouldBe Collection()
    obj.compute(left, left, rem) shouldBe Collection()
    obj.compute(right, left, rem) shouldBe Collection(getPosition(3), getContent(4 * 2))
  }

  it should "compute with lower diagonal" in {
    val obj = Times[Position2D, Position1D](pattern, separator, LowerDiagonal)

    obj.compute(left, right, rem) shouldBe Collection()
    obj.compute(left, left, rem) shouldBe Collection(getPosition(2), getContent(2 * 2))
    obj.compute(right, left, rem) shouldBe Collection(getPosition(3), getContent(4 * 2))
  }

  it should "compute with diagonal" in {
    val obj = Times[Position2D, Position1D](pattern, separator, Diagonal)

    obj.compute(left, right, rem) shouldBe Collection()
    obj.compute(left, left, rem) shouldBe Collection(getPosition(2), getContent(2 * 2))
    obj.compute(right, left, rem) shouldBe Collection()
  }

  it should "compute with upper diagonal" in {
    val obj = Times[Position2D, Position1D](pattern, separator, UpperDiagonal)

    obj.compute(left, right, rem) shouldBe Collection(getPosition(1), getContent(2 * 4))
    obj.compute(left, left, rem) shouldBe Collection(getPosition(2), getContent(2 * 2))
    obj.compute(right, left, rem) shouldBe Collection()
  }

  it should "compute with upper" in {
    val obj = Times[Position2D, Position1D](pattern, separator, Upper)

    obj.compute(left, right, rem) shouldBe Collection(getPosition(1), getContent(2 * 4))
    obj.compute(left, left, rem) shouldBe Collection()
    obj.compute(right, left, rem) shouldBe Collection()
  }
}

class TestDivide extends TestOperators {

   val pattern = "(%1$s divide %2$s)"

  "A Divide" should "compute with all" in {
    val obj = Divide[Position2D, Position1D](pattern, separator, false, All)

    obj.compute(left, right, rem) shouldBe Collection(getPosition(1), getContent(2.0 / 4.0))
    obj.compute(left, left, rem) shouldBe Collection(getPosition(2), getContent(2.0 / 2.0))
    obj.compute(right, left, rem) shouldBe Collection(getPosition(3), getContent(4.0 / 2.0))
  }

  it should "compute with lower" in {
    val obj = Divide[Position2D, Position1D](pattern, separator, false, Lower)

    obj.compute(left, right, rem) shouldBe Collection()
    obj.compute(left, left, rem) shouldBe Collection()
    obj.compute(right, left, rem) shouldBe Collection(getPosition(3), getContent(4.0 / 2.0))
  }

  it should "compute with lower diagonal" in {
    val obj = Divide[Position2D, Position1D](pattern, separator, false, LowerDiagonal)

    obj.compute(left, right, rem) shouldBe Collection()
    obj.compute(left, left, rem) shouldBe Collection(getPosition(2), getContent(2.0 / 2.0))
    obj.compute(right, left, rem) shouldBe Collection(getPosition(3), getContent(4.0 / 2.0))
  }

  it should "compute with diagonal" in {
    val obj = Divide[Position2D, Position1D](pattern, separator, false, Diagonal)

    obj.compute(left, right, rem) shouldBe Collection()
    obj.compute(left, left, rem) shouldBe Collection(getPosition(2), getContent(2.0 / 2.0))
    obj.compute(right, left, rem) shouldBe Collection()
  }

  it should "compute with upper diagonal" in {
    val obj = Divide[Position2D, Position1D](pattern, separator, false, UpperDiagonal)

    obj.compute(left, right, rem) shouldBe Collection(getPosition(1), getContent(2.0 / 4.0))
    obj.compute(left, left, rem) shouldBe Collection(getPosition(2), getContent(2.0 / 2.0))
    obj.compute(right, left, rem) shouldBe Collection()
  }

  it should "compute with upper" in {
    val obj = Divide[Position2D, Position1D](pattern, separator, false, Upper)

    obj.compute(left, right, rem) shouldBe Collection(getPosition(1), getContent(2.0 / 4.0))
    obj.compute(left, left, rem) shouldBe Collection()
    obj.compute(right, left, rem) shouldBe Collection()
  }

  it should "compute inverse with all" in {
    val obj = Divide[Position2D, Position1D](pattern, separator, true, All)

    obj.compute(left, right, rem) shouldBe Collection(getPosition(1), getContent(4.0 / 2.0))
    obj.compute(left, left, rem) shouldBe Collection(getPosition(2), getContent(2.0 / 2.0))
    obj.compute(right, left, rem) shouldBe Collection(getPosition(3), getContent(2.0 / 4.0))
  }

  it should "compute inverse with lower" in {
    val obj = Divide[Position2D, Position1D](pattern, separator, true, Lower)

    obj.compute(left, right, rem) shouldBe Collection()
    obj.compute(left, left, rem) shouldBe Collection()
    obj.compute(right, left, rem) shouldBe Collection(getPosition(3), getContent(2.0 / 4.0))
  }

  it should "compute inverse with lower diagonal" in {
    val obj = Divide[Position2D, Position1D](pattern, separator, true, LowerDiagonal)

    obj.compute(left, right, rem) shouldBe Collection()
    obj.compute(left, left, rem) shouldBe Collection(getPosition(2), getContent(2.0 / 2.0))
    obj.compute(right, left, rem) shouldBe Collection(getPosition(3), getContent(2.0 / 4.0))
  }

  it should "compute inverse with diagonal" in {
    val obj = Divide[Position2D, Position1D](pattern, separator, true, Diagonal)

    obj.compute(left, right, rem) shouldBe Collection()
    obj.compute(left, left, rem) shouldBe Collection(getPosition(2), getContent(2.0 / 2.0))
    obj.compute(right, left, rem) shouldBe Collection()
  }

  it should "compute inverse with upper diagonal" in {
    val obj = Divide[Position2D, Position1D](pattern, separator, true, UpperDiagonal)

    obj.compute(left, right, rem) shouldBe Collection(getPosition(1), getContent(4.0 / 2.0))
    obj.compute(left, left, rem) shouldBe Collection(getPosition(2), getContent(2.0 / 2.0))
    obj.compute(right, left, rem) shouldBe Collection()
  }

  it should "compute inverse with upper" in {
    val obj = Divide[Position2D, Position1D](pattern, separator, true, Upper)

    obj.compute(left, right, rem) shouldBe Collection(getPosition(1), getContent(4.0 / 2.0))
    obj.compute(left, left, rem) shouldBe Collection()
    obj.compute(right, left, rem) shouldBe Collection()
  }
}

class TestConcatenate extends TestOperators {

   val pattern = "concat(%1$s,%2$s)"
   val format = "%1$s+%2$s"

  "A Concatenate" should "compute with all" in {
    val obj = Concatenate[Position2D, Position1D](pattern, format, separator, All)

    obj.compute(left, right, rem) shouldBe Collection(getPosition(1), getContent(2, 4))
    obj.compute(left, left, rem) shouldBe Collection(getPosition(2), getContent(2, 2))
    obj.compute(right, left, rem) shouldBe Collection(getPosition(3), getContent(4, 2))
  }

  it should "compute with lower" in {
    val obj = Concatenate[Position2D, Position1D](pattern, format, separator, Lower)

    obj.compute(left, right, rem) shouldBe Collection()
    obj.compute(left, left, rem) shouldBe Collection()
    obj.compute(right, left, rem) shouldBe Collection(getPosition(3), getContent(4, 2))
  }

  it should "compute with lower diagonal" in {
    val obj = Concatenate[Position2D, Position1D](pattern, format, separator, LowerDiagonal)

    obj.compute(left, right, rem) shouldBe Collection()
    obj.compute(left, left, rem) shouldBe Collection(getPosition(2), getContent(2, 2))
    obj.compute(right, left, rem) shouldBe Collection(getPosition(3), getContent(4, 2))
  }

  it should "compute with diagonal" in {
    val obj = Concatenate[Position2D, Position1D](pattern, format, separator, Diagonal)

    obj.compute(left, right, rem) shouldBe Collection()
    obj.compute(left, left, rem) shouldBe Collection(getPosition(2), getContent(2, 2))
    obj.compute(right, left, rem) shouldBe Collection()
  }

  it should "compute with upper diagonal" in {
    val obj = Concatenate[Position2D, Position1D](pattern, format, separator, UpperDiagonal)

    obj.compute(left, right, rem) shouldBe Collection(getPosition(1), getContent(2, 4))
    obj.compute(left, left, rem) shouldBe Collection(getPosition(2), getContent(2, 2))
    obj.compute(right, left, rem) shouldBe Collection()
  }

  it should "compute with upper" in {
    val obj = Concatenate[Position2D, Position1D](pattern, format, separator, Upper)

    obj.compute(left, right, rem) shouldBe Collection(getPosition(1), getContent(2, 4))
    obj.compute(left, left, rem) shouldBe Collection()
    obj.compute(right, left, rem) shouldBe Collection()
  }

  def getContent(left: Long, right: Long): Content = {
    Content(NominalSchema[Codex.StringCodex](), format.format(left.toString, right.toString))
  }
}

class TestCombinationOperator extends TestOperators {

  val pattern = "not.used"

  "A CombinationOperator" should "compute" in {
    val obj = Operable.LO2O[Position2D, Position1D, Position2D, Operator[Position2D, Position1D, Position2D]].convert(
      List(Plus[Position2D, Position1D](comparer=LowerDiagonal), Minus[Position2D, Position1D](comparer=LowerDiagonal)))

    obj.compute(left, right, rem) shouldBe Collection(List())
    obj.compute(left, left, rem) shouldBe Collection(List(
      Cell(getPosition(2, "(%1$s+%2$s)"), getContent(2 + 2)), Cell(getPosition(2, "(%1$s-%2$s)"), getContent(2 - 2))))
    obj.compute(right, left, rem) shouldBe Collection(List(
      Cell(getPosition(3, "(%1$s+%2$s)"), getContent(4 + 2)), Cell(getPosition(3, "(%1$s-%2$s)"), getContent(4 - 2))))
  }

  def getPosition(call: Int, pattern: String): Position2D = {
    val (first, second) = call match {
      case 1 => ("left 1|left 2", "right 1|right 2")
      case 2 => ("left 1|left 2", "left 1|left 2")
      case 3 => ("right 1|right 2", "left 1|left 2")
    }

    Position2D(pattern.format(first, second), "rem")
  }
}

