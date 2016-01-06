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

case class PlusWithValue(
  pos: Locate.FromPairwiseCells[Position1D, Position1D]) extends OperatorWithValue[Position1D, Position1D] {
    type V = Map[Position1D, Content]

    val plus = Plus(pos)

    def computeWithValue(left: Cell[Position1D], right: Cell[Position1D], ext: V): TraversableOnce[Cell[Position1D]] = {
      val offset = ext(left.position).value.asDouble.get

      plus.compute(left, right).map {
        case Cell(pos, Content(_, DoubleValue(d))) => Cell(pos, Content(ContinuousSchema(DoubleCodex), d + offset))
      }
    }
}

class TestWithPrepareOperator extends TestOperators {

  val pattern = "not.used"

  val str = Cell(Position1D("x"), getDoubleContent(2))
  val dbl = Cell(Position1D("y"), getDoubleContent(4))
  val lng = Cell(Position1D("z"), getDoubleContent(8))

  val ext = Map(Position1D("x") -> getDoubleContent(3),
    Position1D("y") -> getDoubleContent(4),
    Position1D("z") -> getDoubleContent(5))

  def prepare(cell: Cell[Position1D]): Content = {
    cell.content.value match {
      case DoubleValue(2) => cell.content
      case DoubleValue(4) => getStringContent("not.supported")
      case DoubleValue(8) => getLongContent(8)
    }
  }

  def prepareWithValue(cell: Cell[Position1D], ext: Map[Position1D, Content]): Content = {
    (cell.content.value, ext(cell.position).value) match {
      case (DoubleValue(2), DoubleValue(d)) => getDoubleContent(2 * d)
      case (DoubleValue(4), _) => getStringContent("not.supported")
      case (DoubleValue(8), DoubleValue(d)) => getLongContent(d.toLong)
    }
  }

  val locate = (left: Cell[Position1D], right: Cell[Position1D]) => left.position.toOption

  def getLongContent(value: Long): Content = Content(DiscreteSchema(LongCodex), value)
  def getDoubleContent(value: Double): Content = Content(ContinuousSchema(DoubleCodex), value)
  def getStringContent(value: String): Content = Content(NominalSchema(StringCodex), value)

  "An Operator" should "withPrepare correctly" in {
    val obj = Plus[Position1D, Position1D](locate).withPrepare(prepare)

    obj.compute(str, lng) shouldBe (List(Cell(str.position, getDoubleContent(2 + 8))))
    obj.compute(str, dbl) shouldBe (List())
  }

  it should "withPrepareWithValue correctly (without value)" in {
    val obj = PlusWithValue(locate).withPrepare(prepare)

    obj.computeWithValue(str, lng, ext).toList shouldBe (List(Cell(str.position, getDoubleContent(2 + 8 + 3))))
    obj.computeWithValue(str, dbl, ext).toList shouldBe (List())
  }

  it should "withPrepareWithValue correctly" in {
    val obj = PlusWithValue(locate).withPrepareWithValue(prepareWithValue)

    obj.computeWithValue(str, lng, ext).toList shouldBe (List(Cell(str.position, getDoubleContent(2 * 3 + 5 + 3))))
    obj.computeWithValue(str, dbl, ext).toList shouldBe (List())
  }
}

class TestAndThenMutateOperator extends TestOperators {

  val pattern = "not.used"

  val str = Cell(Position1D("x"), getDoubleContent(2))
  val dbl = Cell(Position1D("y"), getDoubleContent(4))
  val lng = Cell(Position1D("z"), getDoubleContent(8))

  val ext = Map(Position1D("x") -> getDoubleContent(3),
    Position1D("y") -> getDoubleContent(4),
    Position1D("z") -> getDoubleContent(5))

  def mutate(cell: Cell[Position1D]): Content = {
    cell.content.value match {
      case DoubleValue(10) => cell.content
      case DoubleValue(6) => getStringContent("not.supported")

      case DoubleValue(13) => cell.content
      case DoubleValue(9) => getStringContent("not.supported")
    }
  }

  def mutateWithValue(cell: Cell[Position1D], ext: Map[Position1D, Content]): Content = {
    (cell.content.value, ext(cell.position).value) match {
      case (DoubleValue(13), DoubleValue(d)) => getDoubleContent(2 * d)
      case (DoubleValue(9), _) => getStringContent("not.supported")
    }
  }

  val locate = (left: Cell[Position1D], right: Cell[Position1D]) => left.position.toOption

  def getLongContent(value: Long): Content = Content(DiscreteSchema(LongCodex), value)
  def getDoubleContent(value: Double): Content = Content(ContinuousSchema(DoubleCodex), value)
  def getStringContent(value: String): Content = Content(NominalSchema(StringCodex), value)

  "An Operator" should "andThenMutate correctly" in {
    val obj = Plus[Position1D, Position1D](locate).andThenMutate(mutate)

    obj.compute(str, lng).toList shouldBe (List(Cell(str.position, getDoubleContent(2 + 8))))
    obj.compute(str, dbl).toList shouldBe (List(Cell(str.position, getStringContent("not.supported"))))
  }

  it should "andThenMutateWithValue correctly (without value)" in {
    val obj = PlusWithValue(locate).andThenMutate(mutate)

    obj.computeWithValue(str, lng, ext).toList shouldBe (List(Cell(str.position, getDoubleContent(2 + 8 + 3))))
    obj.computeWithValue(str, dbl, ext).toList shouldBe (List(Cell(str.position, getStringContent("not.supported"))))
  }

  it should "andThenMutateWithValue correctly" in {
    val obj = PlusWithValue(locate).andThenMutateWithValue(mutateWithValue)

    obj.computeWithValue(str, lng, ext).toList shouldBe (List(Cell(str.position, getDoubleContent(2 * 3))))
    obj.computeWithValue(str, dbl, ext).toList shouldBe (List(Cell(str.position, getStringContent("not.supported"))))
  }
}

