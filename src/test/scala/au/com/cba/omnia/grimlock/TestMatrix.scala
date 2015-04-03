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
import au.com.cba.omnia.grimlock.derive._
import au.com.cba.omnia.grimlock.encoding._
import au.com.cba.omnia.grimlock.Matrix._
import au.com.cba.omnia.grimlock.pairwise._
import au.com.cba.omnia.grimlock.partition._
import au.com.cba.omnia.grimlock.position._
import au.com.cba.omnia.grimlock.reduce._
import au.com.cba.omnia.grimlock.sample._
import au.com.cba.omnia.grimlock.squash._
import au.com.cba.omnia.grimlock.transform._
import au.com.cba.omnia.grimlock.Type._
import au.com.cba.omnia.grimlock.utility._

import com.twitter.scalding._
import com.twitter.scalding.bdd._
import com.twitter.scalding.typed.ValuePipe

import org.scalatest._

import scala.collection.mutable

class TestCell extends FlatSpec with Matchers {

  "A Cell" should "return its string" in {
    Cell(Position2D("foo", 123), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)).toString(".", true) should
      be ("Position2D(StringValue(foo),LongValue(123)).Content(ContinuousSchema[DoubleCodex](),DoubleValue(3.14))")
    Cell(Position2D("foo", 123), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)).toString(".", false) should
      be ("foo.123.continuous.double.3.14")
  }
}

trait TestMatrix {

  val data1 = List(Cell(Position1D("foo"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
    Cell(Position1D("bar"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
    Cell(Position1D("baz"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position1D("qux"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))

  val data2 = List(Cell(Position2D("foo", 1), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
    Cell(Position2D("bar", 1), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
    Cell(Position2D("baz", 1), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position2D("qux", 1), Content(OrdinalSchema[Codex.StringCodex](), "12.56")),
    Cell(Position2D("foo", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position2D("bar", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position2D("baz", 2), Content(DiscreteSchema[Codex.LongCodex](), 19)),
    Cell(Position2D("foo", 3), Content(NominalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position2D("bar", 3), Content(OrdinalSchema[Codex.LongCodex](), 19)),
    Cell(Position2D("foo", 4), Content(DateSchema[Codex.DateTimeCodex](),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))

  val data3 = List(Cell(Position3D("foo", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
    Cell(Position3D("bar", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
    Cell(Position3D("baz", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position3D("qux", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")),
    Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position3D("baz", 2, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
    Cell(Position3D("foo", 3, "xyz"), Content(NominalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position3D("bar", 3, "xyz"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
    Cell(Position3D("foo", 4, "xyz"), Content(DateSchema[Codex.DateTimeCodex](),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))

  val num1 = List(Cell(Position1D("foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
    Cell(Position1D("bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position1D("baz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
    Cell(Position1D("qux"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)))

  val num2 = List(Cell(Position2D("foo", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
    Cell(Position2D("bar", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position2D("baz", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
    Cell(Position2D("qux", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position2D("foo", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position2D("bar", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position2D("baz", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
    Cell(Position2D("foo", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
    Cell(Position2D("bar", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
    Cell(Position2D("foo", 4), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)))

  val num3 = List(Cell(Position3D("foo", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
    Cell(Position3D("bar", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position3D("baz", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
    Cell(Position3D("qux", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position3D("baz", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
    Cell(Position3D("foo", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
    Cell(Position3D("bar", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
    Cell(Position3D("foo", 4, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)))

  implicit def PositionOrdering[T <: Position] = Position.Ordering[T]
}

class TestMatrixNames extends WordSpec with Matchers with TBddDsl with TestMatrix {

  "A Matrix.names" should {
    "return its first over names in 1D" in {
      Given {
        data1
      } When {
        cells: TypedPipe[Cell[Position1D]] =>
          cells.names(Over(First))
      } Then {
        buffer: mutable.Buffer[(Position1D, Long)] =>
          buffer.toList shouldBe List((Position1D("bar"), 0), (Position1D("baz"), 1), (Position1D("foo"), 2),
            (Position1D("qux"), 3))
      }
    }

    "return its first over names in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.names(Over(First))
      } Then {
        buffer: mutable.Buffer[(Position1D, Long)] =>
          buffer.toList shouldBe List((Position1D("bar"), 0), (Position1D("baz"), 1), (Position1D("foo"), 2),
            (Position1D("qux"), 3))
      }
    }

    "return its first along names in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.names(Along(First))
      } Then {
        buffer: mutable.Buffer[(Position1D, Long)] =>
          buffer.toList shouldBe List((Position1D(1), 0), (Position1D(2), 1), (Position1D(3), 2), (Position1D(4), 3))
      }
    }

    "return its second over names in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.names(Over(Second))
      } Then {
        buffer: mutable.Buffer[(Position1D, Long)] =>
          buffer.toList shouldBe List((Position1D(1), 0), (Position1D(2), 1), (Position1D(3), 2), (Position1D(4), 3))
      }
    }

    "return its second along names in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.names(Along(Second))
      } Then {
        buffer: mutable.Buffer[(Position1D, Long)] =>
          buffer.toList shouldBe List((Position1D("bar"), 0), (Position1D("baz"), 1), (Position1D("foo"), 2),
            (Position1D("qux"), 3))
      }
    }

    "return its first over names in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.names(Over(First))
      } Then {
        buffer: mutable.Buffer[(Position1D, Long)] =>
          buffer.toList shouldBe List((Position1D("bar"), 0), (Position1D("baz"), 1), (Position1D("foo"), 2),
            (Position1D("qux"), 3))
      }
    }

    "return its first along names in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.names(Along(First))
      } Then {
        buffer: mutable.Buffer[(Position2D, Long)] =>
          buffer.toList shouldBe List((Position2D(1, "xyz"), 0), (Position2D(2, "xyz"), 1), (Position2D(3, "xyz"), 2),
            (Position2D(4, "xyz"), 3))
      }
    }

    "return its second over names in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.names(Over(Second))
      } Then {
        buffer: mutable.Buffer[(Position1D, Long)] =>
          buffer.toList shouldBe List((Position1D(1), 0), (Position1D(2), 1), (Position1D(3), 2), (Position1D(4), 3))
      }
    }

    "return its second along names in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.names(Along(Second))
      } Then {
        buffer: mutable.Buffer[(Position2D, Long)] =>
          buffer.toList shouldBe List((Position2D("bar", "xyz"), 0), (Position2D("baz", "xyz"), 1),
            (Position2D("foo", "xyz"), 2), (Position2D("qux", "xyz"), 3))
      }
    }

    "return its third over names in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.names(Over(Third))
      } Then {
        buffer: mutable.Buffer[(Position1D, Long)] =>
          buffer.toList shouldBe List((Position1D("xyz"), 0))
      }
    }

    "return its third along names in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.names(Along(Third))
      } Then {
        buffer: mutable.Buffer[(Position2D, Long)] =>
          buffer.toList shouldBe List((Position2D("bar", 1), 0), (Position2D("bar", 2), 1), (Position2D("bar", 3), 2),
            (Position2D("baz", 1), 3), (Position2D("baz", 2), 4), (Position2D("foo", 1), 5), (Position2D("foo", 2), 6),
            (Position2D("foo", 3), 7), (Position2D("foo", 4), 8), (Position2D("qux", 1), 9))
      }
    }
  }
}

class TestMatrixTypes extends WordSpec with Matchers with TBddDsl with TestMatrix {

  "A Matrix.types" should {
    "return its first over types in 1D" in {
      Given {
        data1
      } When {
        cells: TypedPipe[Cell[Position1D]] =>
          cells.types(Over(First), false)
      } Then {
        buffer: mutable.Buffer[(Position1D, Type)] =>
          buffer.toList shouldBe List((Position1D("bar"), Categorical), (Position1D("baz"), Categorical),
            (Position1D("foo"), Categorical), (Position1D("qux"), Categorical))
      }
    }

    "return its first over specific types in 1D" in {
      Given {
        data1
      } When {
        cells: TypedPipe[Cell[Position1D]] =>
          cells.types(Over(First), true)
      } Then {
        buffer: mutable.Buffer[(Position1D, Type)] =>
          buffer.toList shouldBe List((Position1D("bar"), Ordinal), (Position1D("baz"), Ordinal),
            (Position1D("foo"), Ordinal), (Position1D("qux"), Ordinal))
      }
    }

    "return its first over types in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.types(Over(First), false)
      } Then {
        buffer: mutable.Buffer[(Position1D, Type)] =>
          buffer.toList shouldBe List((Position1D("bar"), Mixed), (Position1D("baz"), Mixed),
            (Position1D("foo"), Mixed), (Position1D("qux"), Categorical))
      }
    }

    "return its first over specific types in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.types(Over(First), true)
      } Then {
        buffer: mutable.Buffer[(Position1D, Type)] =>
          buffer.toList shouldBe List((Position1D("bar"), Mixed), (Position1D("baz"), Mixed),
            (Position1D("foo"), Mixed), (Position1D("qux"), Ordinal))
      }
    }

    "return its first along types in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.types(Along(First), false)
      } Then {
        buffer: mutable.Buffer[(Position1D, Type)] =>
          buffer.toList shouldBe List((Position1D(1), Categorical), (Position1D(2), Numerical),
            (Position1D(3), Categorical), (Position1D(4), Date))
      }
    }

    "return its first along specific types in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.types(Along(First), true)
      } Then {
        buffer: mutable.Buffer[(Position1D, Type)] =>
          buffer.toList shouldBe List((Position1D(1), Ordinal), (Position1D(2), Numerical),
            (Position1D(3), Categorical), (Position1D(4), Date))
      }
    }

    "return its second over types in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.types(Over(Second), false)
      } Then {
        buffer: mutable.Buffer[(Position1D, Type)] =>
          buffer.toList shouldBe List((Position1D(1), Categorical), (Position1D(2), Numerical),
            (Position1D(3), Categorical), (Position1D(4), Date))
      }
    }

    "return its second over specific types in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.types(Over(Second), true)
      } Then {
        buffer: mutable.Buffer[(Position1D, Type)] =>
          buffer.toList shouldBe List((Position1D(1), Ordinal), (Position1D(2), Numerical),
            (Position1D(3), Categorical), (Position1D(4), Date))
      }
    }

    "return its second along types in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.types(Along(Second), false)
      } Then {
        buffer: mutable.Buffer[(Position1D, Type)] =>
          buffer.toList shouldBe List((Position1D("bar"), Mixed), (Position1D("baz"), Mixed),
            (Position1D("foo"), Mixed), (Position1D("qux"), Categorical))
      }
    }

    "return its second along specific types in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.types(Along(Second), true)
      } Then {
        buffer: mutable.Buffer[(Position1D, Type)] =>
          buffer.toList shouldBe List((Position1D("bar"), Mixed), (Position1D("baz"), Mixed),
            (Position1D("foo"), Mixed), (Position1D("qux"), Ordinal))
      }
    }

    "return its first over types in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.types(Over(First), false)
      } Then {
        buffer: mutable.Buffer[(Position1D, Type)] =>
          buffer.toList shouldBe List((Position1D("bar"), Mixed), (Position1D("baz"), Mixed),
            (Position1D("foo"), Mixed), (Position1D("qux"), Categorical))
      }
    }

    "return its first over specific types in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.types(Over(First), true)
      } Then {
        buffer: mutable.Buffer[(Position1D, Type)] =>
          buffer.toList shouldBe List((Position1D("bar"), Mixed), (Position1D("baz"), Mixed),
            (Position1D("foo"), Mixed), (Position1D("qux"), Ordinal))
      }
    }

    "return its first along types in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.types(Along(First), false)
      } Then {
        buffer: mutable.Buffer[(Position2D, Type)] =>
          buffer.toList shouldBe List((Position2D(1, "xyz"), Categorical), (Position2D(2, "xyz"), Numerical),
            (Position2D(3, "xyz"), Categorical), (Position2D(4, "xyz"), Date))
      }
    }

    "return its first along specific types in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.types(Along(First), true)
      } Then {
        buffer: mutable.Buffer[(Position2D, Type)] =>
          buffer.toList shouldBe List((Position2D(1, "xyz"), Ordinal), (Position2D(2, "xyz"), Numerical),
            (Position2D(3, "xyz"), Categorical), (Position2D(4, "xyz"), Date))
      }
    }

    "return its second over types in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.types(Over(Second), false)
      } Then {
        buffer: mutable.Buffer[(Position1D, Type)] =>
          buffer.toList shouldBe List((Position1D(1), Categorical), (Position1D(2), Numerical),
            (Position1D(3), Categorical), (Position1D(4), Date))
      }
    }

    "return its second over specific types in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.types(Over(Second), true)
      } Then {
        buffer: mutable.Buffer[(Position1D, Type)] =>
          buffer.toList shouldBe List((Position1D(1), Ordinal), (Position1D(2), Numerical),
            (Position1D(3), Categorical), (Position1D(4), Date))
      }
    }

    "return its second along types in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.types(Along(Second), false)
      } Then {
        buffer: mutable.Buffer[(Position2D, Type)] =>
          buffer.toList shouldBe List((Position2D("bar", "xyz"), Mixed), (Position2D("baz", "xyz"), Mixed),
            (Position2D("foo", "xyz"), Mixed), (Position2D("qux", "xyz"), Categorical))
      }
    }

    "return its second along specific types in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.types(Along(Second), true)
      } Then {
        buffer: mutable.Buffer[(Position2D, Type)] =>
          buffer.toList shouldBe List((Position2D("bar", "xyz"), Mixed), (Position2D("baz", "xyz"), Mixed),
            (Position2D("foo", "xyz"), Mixed), (Position2D("qux", "xyz"), Ordinal))
      }
    }

    "return its third over types in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.types(Over(Third), false)
      } Then {
        buffer: mutable.Buffer[(Position1D, Type)] =>
          buffer.toList shouldBe List((Position1D("xyz"), Mixed))
      }
    }

    "return its third over specific types in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.types(Over(Third), true)
      } Then {
        buffer: mutable.Buffer[(Position1D, Type)] =>
          buffer.toList shouldBe List((Position1D("xyz"), Mixed))
      }
    }

    "return its third along types in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.types(Along(Third), false)
      } Then {
        buffer: mutable.Buffer[(Position2D, Type)] =>
          buffer.toList shouldBe List((Position2D("bar", 1), Categorical), (Position2D("bar", 2), Numerical),
            (Position2D("bar", 3), Categorical), (Position2D("baz", 1), Categorical),
            (Position2D("baz", 2), Numerical), (Position2D("foo", 1), Categorical),
            (Position2D("foo", 2), Numerical), (Position2D("foo", 3), Categorical),
            (Position2D("foo", 4), Date), (Position2D("qux", 1), Categorical))
      }
    }

    "return its third along specific types in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.types(Along(Third), true)
      } Then {
        buffer: mutable.Buffer[(Position2D, Type)] =>
          buffer.toList shouldBe List((Position2D("bar", 1), Ordinal), (Position2D("bar", 2), Continuous),
            (Position2D("bar", 3), Ordinal), (Position2D("baz", 1), Ordinal),
            (Position2D("baz", 2), Discrete), (Position2D("foo", 1), Ordinal),
            (Position2D("foo", 2), Continuous), (Position2D("foo", 3), Nominal),
            (Position2D("foo", 4), Date), (Position2D("qux", 1), Ordinal))
      }
    }
  }
}

class TestMatrixSize extends WordSpec with Matchers with TBddDsl with TestMatrix {

  "A Matrix.size" should {
    "return its first size in 1D" in {
      Given {
        data1
      } When {
        cells: TypedPipe[Cell[Position1D]] =>
          cells.size(First, false)
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList shouldBe List(Cell(Position1D("First"), Content(DiscreteSchema[Codex.LongCodex](), 4)))
      }
    }

    "return its first distinct size in 1D" in {
      Given {
        data1
      } When {
        cells: TypedPipe[Cell[Position1D]] =>
          cells.size(First, true)
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList shouldBe List(Cell(Position1D("First"), Content(DiscreteSchema[Codex.LongCodex](), 4)))
      }
    }

    "return its first size in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.size(First, false)
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList shouldBe List(Cell(Position1D("First"), Content(DiscreteSchema[Codex.LongCodex](), 4)))
      }
    }

    "return its first distinct size in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.size(First, true)
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList shouldBe List(Cell(Position1D("First"),
            Content(DiscreteSchema[Codex.LongCodex](), data2.length)))
      }
    }

    "return its second size in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.size(Second, false)
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList shouldBe List(Cell(Position1D("Second"), Content(DiscreteSchema[Codex.LongCodex](), 4)))
      }
    }

    "return its second distinct size in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.size(Second, true)
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList shouldBe List(Cell(Position1D("Second"),
            Content(DiscreteSchema[Codex.LongCodex](), data2.length)))
      }
    }

    "return its first size in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.size(First, false)
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList shouldBe List(Cell(Position1D("First"), Content(DiscreteSchema[Codex.LongCodex](), 4)))
      }
    }

    "return its first distinct size in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.size(First, true)
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList shouldBe List(Cell(Position1D("First"),
            Content(DiscreteSchema[Codex.LongCodex](), data3.length)))
      }
    }

    "return its second size in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.size(Second, false)
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList shouldBe List(Cell(Position1D("Second"), Content(DiscreteSchema[Codex.LongCodex](), 4)))
      }
    }

    "return its second distinct size in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.size(Second, true)
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList shouldBe List(Cell(Position1D("Second"),
            Content(DiscreteSchema[Codex.LongCodex](), data3.length)))
      }
    }

    "return its third size in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.size(Third, false)
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList shouldBe List(Cell(Position1D("Third"), Content(DiscreteSchema[Codex.LongCodex](), 1)))
      }
    }

    "return its third distinct size in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.size(Third, true)
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList shouldBe List(Cell(Position1D("Third"),
            Content(DiscreteSchema[Codex.LongCodex](), data3.length)))
      }
    }

    "return its distinct size" in {
      Given {
        List(Cell(Position2D(1, 1), Content(OrdinalSchema[Codex.StringCodex](), "a")),
          Cell(Position2D(2, 2), Content(OrdinalSchema[Codex.StringCodex](), "b")),
          Cell(Position2D(3, 3), Content(OrdinalSchema[Codex.StringCodex](), "c")))
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.size(Second, true)
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList shouldBe List(Cell(Position1D("Second"), Content(DiscreteSchema[Codex.LongCodex](), 3)))
      }
    }
  }
}

class TestMatrixShape extends WordSpec with Matchers with TBddDsl with TestMatrix {

  "A Matrix.shape" should {
    "return its shape in 1D" in {
      Given {
        data1
      } When {
        cells: TypedPipe[Cell[Position1D]] =>
          cells.shape()
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList shouldBe List(Cell(Position1D("First"), Content(DiscreteSchema[Codex.LongCodex](), 4)))
      }
    }

    "return its shape in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.shape()
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList shouldBe List(Cell(Position1D("First"), Content(DiscreteSchema[Codex.LongCodex](), 4)),
            Cell(Position1D("Second"), Content(DiscreteSchema[Codex.LongCodex](), 4)))
      }
    }

    "return its shape in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.shape()
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList shouldBe List(Cell(Position1D("First"), Content(DiscreteSchema[Codex.LongCodex](), 4)),
            Cell(Position1D("Second"), Content(DiscreteSchema[Codex.LongCodex](), 4)),
            Cell(Position1D("Third"), Content(DiscreteSchema[Codex.LongCodex](), 1)))
      }
    }
  }
}

class TestMatrixSlice extends WordSpec with Matchers with TBddDsl with TestMatrix {

  "A Matrix.slice" should {
    "return its first over slice in 1D" in {
      Given {
        data1
      } When {
        cells: TypedPipe[Cell[Position1D]] =>
          cells.slice(Over(First), List("bar", "qux"), false)
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList shouldBe List(Cell(Position1D("baz"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position1D("foo"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")))
      }
    }

    "return its first over inverse slice in 1D" in {
      Given {
        data1
      } When {
        cells: TypedPipe[Cell[Position1D]] =>
          cells.slice(Over(First), List("bar", "qux"), true)
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList shouldBe List(Cell(Position1D("bar"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position1D("qux"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))
      }
    }

    "return its first over slice in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.slice(Over(First), List("bar", "qux"), false)
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList shouldBe List(Cell(Position2D("baz", 1), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position2D("baz", 2), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position2D("foo", 1), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
            Cell(Position2D("foo", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position2D("foo", 3), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position2D("foo", 4), Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))
      }
    }

    "return its first over inverse slice in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.slice(Over(First), List("bar", "qux"), true)
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList shouldBe List(Cell(Position2D("bar", 1), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position2D("bar", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position2D("bar", 3), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position2D("qux", 1), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))
      }
    }

    "return its first along slice in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.slice(Along(First), List(1, 3), false)
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList shouldBe List(Cell(Position2D("foo", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position2D("bar", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position2D("baz", 2), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position2D("foo", 4), Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))
      }
    }

    "return its first along inverse slice in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.slice(Along(First), List(1, 3), true)
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList shouldBe List(Cell(Position2D("foo", 1), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
            Cell(Position2D("bar", 1), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position2D("baz", 1), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position2D("qux", 1), Content(OrdinalSchema[Codex.StringCodex](), "12.56")),
            Cell(Position2D("foo", 3), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position2D("bar", 3), Content(OrdinalSchema[Codex.LongCodex](), 19)))
      }
    }

    "return its second over slice in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.slice(Over(Second), List(1, 3), false)
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList shouldBe List(Cell(Position2D("foo", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position2D("bar", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position2D("baz", 2), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position2D("foo", 4), Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))
      }
    }

    "return its second over inverse slice in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.slice(Over(Second), List(1, 3), true)
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList shouldBe List(Cell(Position2D("foo", 1), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
            Cell(Position2D("bar", 1), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position2D("baz", 1), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position2D("qux", 1), Content(OrdinalSchema[Codex.StringCodex](), "12.56")),
            Cell(Position2D("foo", 3), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position2D("bar", 3), Content(OrdinalSchema[Codex.LongCodex](), 19)))
      }
    }

    "return its second along slice in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.slice(Along(Second), List("bar", "qux"), false)
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList shouldBe List(Cell(Position2D("baz", 1), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position2D("baz", 2), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position2D("foo", 1), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
            Cell(Position2D("foo", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position2D("foo", 3), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position2D("foo", 4), Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))
      }
    }

    "return its second along inverse slice in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.slice(Along(Second), List("bar", "qux"), true)
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList shouldBe List(Cell(Position2D("bar", 1), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position2D("bar", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position2D("bar", 3), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position2D("qux", 1), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))
      }
    }

    "return its first over slice in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.slice(Over(First), List("bar", "qux"), false)
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList shouldBe List(
            Cell(Position3D("baz", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position3D("baz", 2, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position3D("foo", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
            Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position3D("foo", 3, "xyz"), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position3D("foo", 4, "xyz"), Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))
      }
    }

    "return its first over inverse slice in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.slice(Over(First), List("bar", "qux"), true)
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList shouldBe List(
            Cell(Position3D("bar", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position3D("bar", 3, "xyz"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position3D("qux", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))
      }
    }

    "return its first along slice in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.slice(Along(First), List(Position2D(1, "xyz"), Position2D(3, "xyz")), false)
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList shouldBe List(
            Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position3D("baz", 2, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position3D("foo", 4, "xyz"), Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))
      }
    }

    "return its first along inverse slice in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.slice(Along(First), List(Position2D(1, "xyz"), Position2D(3, "xyz")), true)
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList shouldBe List(
            Cell(Position3D("foo", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
            Cell(Position3D("bar", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position3D("baz", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position3D("qux", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")),
            Cell(Position3D("foo", 3, "xyz"), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position3D("bar", 3, "xyz"), Content(OrdinalSchema[Codex.LongCodex](), 19)))
      }
    }

    "return its second over slice in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.slice(Over(Second), List(1, 3), false)
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList shouldBe List(
            Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position3D("baz", 2, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position3D("foo", 4, "xyz"), Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))
      }
    }

    "return its second over inverse slice in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.slice(Over(Second), List(1, 3), true)
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList shouldBe List(
            Cell(Position3D("foo", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
            Cell(Position3D("bar", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position3D("baz", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position3D("qux", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")),
            Cell(Position3D("foo", 3, "xyz"), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position3D("bar", 3, "xyz"), Content(OrdinalSchema[Codex.LongCodex](), 19)))
      }
    }

    "return its second along slice in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.slice(Along(Second), List(Position2D("bar", "xyz"), Position2D("qux", "xyz")), false)
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList shouldBe List(
            Cell(Position3D("baz", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position3D("baz", 2, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position3D("foo", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
            Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position3D("foo", 3, "xyz"), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position3D("foo", 4, "xyz"), Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))
      }
    }

    "return its second along inverse slice in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.slice(Along(Second), List(Position2D("bar", "xyz"), Position2D("qux", "xyz")), true)
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList shouldBe List(
            Cell(Position3D("bar", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position3D("bar", 3, "xyz"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position3D("qux", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))
      }
    }

    "return its third over slice in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.slice(Over(Third), "xyz", false)
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList shouldBe List()
      }
    }

    "return its third over inverse slice in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.slice(Over(Third), "xyz", true)
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList shouldBe data3
      }
    }

    "return its third along slice in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.slice(Along(Third), List(Position2D("foo", 3), Position2D("baz", 1)), false)
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList shouldBe List(
            Cell(Position3D("bar", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position3D("bar", 3, "xyz"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position3D("baz", 2, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position3D("foo", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
            Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position3D("foo", 4, "xyz"), Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
            Cell(Position3D("qux", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))
      }
    }

    "return its third along inverse slice in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.slice(Along(Third), List(Position2D("foo", 3), Position2D("baz", 1)), true)
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList shouldBe List(
            Cell(Position3D("baz", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position3D("foo", 3, "xyz"), Content(NominalSchema[Codex.StringCodex](), "9.42")))
      }
    }
  }
}

class TestMatrixWhich extends WordSpec with Matchers with TBddDsl with TestMatrix {

  def predicate[P <: Position](cell: Cell[P]): Boolean = {
    (cell.content.schema == NominalSchema[Codex.StringCodex]()) ||
    (cell.content.schema.codex == DateTimeCodex) ||
    (cell.content.value equ "12.56")
  }

  "A Matrix.which" should {
    "return its coordinates in 1D" in {
      Given {
        data1
      } When {
        cells: TypedPipe[Cell[Position1D]] =>
          cells.which(predicate)
      } Then {
        buffer: mutable.Buffer[Position1D] =>
          buffer.toList shouldBe List(Position1D("qux"))
      }
    }

    "return its first over coordinates in 1D" in {
      Given {
        data1
      } When {
        cells: TypedPipe[Cell[Position1D]] =>
          cells.which(Over(First), List("bar", "qux"), predicate)
      } Then {
        buffer: mutable.Buffer[Position1D] =>
          buffer.toList shouldBe List(Position1D("qux"))
      }
    }

    "return its first over multiple coordinates in 1D" in {
      Given {
        data1
      } When {
        cells: TypedPipe[Cell[Position1D]] =>
          cells.which(Over(First), List((List("bar", "qux"), (c: Cell[Position1D]) => predicate(c)),
            (List("foo"), (c: Cell[Position1D]) => !predicate(c))))
      } Then {
        buffer: mutable.Buffer[Position1D] =>
          buffer.toList shouldBe List(Position1D("foo"), Position1D("qux"))
      }
    }

    "return its coordinates in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.which(predicate)
      } Then {
        buffer: mutable.Buffer[Position2D] =>
          buffer.toList shouldBe List(Position2D("qux", 1), Position2D("foo", 3), Position2D("foo", 4))
      }
    }

    "return its first over coordinates in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.which(Over(First), List("bar", "qux"), predicate)
      } Then {
        buffer: mutable.Buffer[Position2D] =>
          buffer.toList shouldBe List(Position2D("qux", 1))
      }
    }

    "return its first along coordinates in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.which(Along(First), List(2, 4), predicate)
      } Then {
        buffer: mutable.Buffer[Position2D] =>
          buffer.toList shouldBe List(Position2D("foo", 4))
      }
    }

    "return its second over coordinates in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.which(Over(Second), List(2, 4), predicate)
      } Then {
        buffer: mutable.Buffer[Position2D] =>
          buffer.toList shouldBe List(Position2D("foo", 4))
      }
    }

    "return its second along coordinates in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.which(Along(Second), List("bar", "qux"), predicate)
      } Then {
        buffer: mutable.Buffer[Position2D] =>
          buffer.toList shouldBe List(Position2D("qux", 1))
      }
    }

    "return its first over multiple coordinates in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.which(Over(First), List((List("bar", "qux"), (c: Cell[Position2D]) => predicate(c)),
            (List("foo"), (c: Cell[Position2D]) => !predicate(c))))
      } Then {
        buffer: mutable.Buffer[Position2D] =>
          buffer.toList shouldBe List(Position2D("foo", 1), Position2D("foo", 2), Position2D("qux", 1))
      }
    }

    "return its first along multiple coordinates in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.which(Along(First), List((List(2, 4), (c: Cell[Position2D]) => predicate(c)),
            (List(2), (c: Cell[Position2D]) => !predicate(c))))
      } Then {
        buffer: mutable.Buffer[Position2D] =>
          buffer.toList shouldBe List(Position2D("foo", 2), Position2D("bar", 2), Position2D("baz", 2),
            Position2D("foo", 4))
      }
    }

    "return its second over multiple coordinates in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.which(Over(Second), List((List(2, 4), (c: Cell[Position2D]) => predicate(c)),
            (List(2), (c: Cell[Position2D]) => !predicate(c))))
      } Then {
        buffer: mutable.Buffer[Position2D] =>
          buffer.toList shouldBe List(Position2D("foo", 2), Position2D("bar", 2), Position2D("baz", 2),
            Position2D("foo", 4))
      }
    }

    "return its second along multiple coordinates in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.which(Along(Second), List((List("bar", "qux"), (c: Cell[Position2D]) => predicate(c)),
            (List("foo"), (c: Cell[Position2D]) => !predicate(c))))
      } Then {
        buffer: mutable.Buffer[Position2D] =>
          buffer.toList shouldBe List(Position2D("foo", 1), Position2D("foo", 2), Position2D("qux", 1))
      }
    }

    "return its coordinates in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.which(predicate)
      } Then {
        buffer: mutable.Buffer[Position3D] =>
          buffer.toList shouldBe List(Position3D("qux", 1, "xyz"), Position3D("foo", 3, "xyz"),
            Position3D("foo", 4, "xyz"))
      }
    }

    "return its first over coordinates in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.which(Over(First), List("bar", "qux"), predicate)
      } Then {
        buffer: mutable.Buffer[Position3D] =>
          buffer.toList shouldBe List(Position3D("qux", 1, "xyz"))
      }
    }

    "return its first along coordinates in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.which(Along(First), List(Position2D(2, "xyz"), Position2D(4, "xyz")), predicate)
      } Then {
        buffer: mutable.Buffer[Position3D] =>
          buffer.toList shouldBe List(Position3D("foo", 4, "xyz"))
      }
    }

    "return its second over coordinates in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.which(Over(Second), List(2, 4), predicate)
      } Then {
        buffer: mutable.Buffer[Position3D] =>
          buffer.toList shouldBe List(Position3D("foo", 4, "xyz"))
      }
    }

    "return its second along coordinates in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.which(Along(Second), List(Position2D("bar", "xyz"), Position2D("qux", "xyz")), predicate)
      } Then {
        buffer: mutable.Buffer[Position3D] =>
          buffer.toList shouldBe List(Position3D("qux", 1, "xyz"))
      }
    }

    "return its third over coordinates in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.which(Over(Third), "xyz", predicate)
      } Then {
        buffer: mutable.Buffer[Position3D] =>
          buffer.toList shouldBe List(Position3D("qux", 1, "xyz"), Position3D("foo", 3, "xyz"),
            Position3D("foo", 4, "xyz"))
      }
    }

    "return its third along coordinates in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.which(Along(Third), List(Position2D("bar", 2), Position2D("qux", 1)), predicate)
      } Then {
        buffer: mutable.Buffer[Position3D] =>
          buffer.toList shouldBe List(Position3D("qux", 1, "xyz"))
      }
    }

    "return its first over multiple coordinates in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.which(Over(First), List((List("bar", "qux"), (c: Cell[Position3D]) => predicate(c)),
            (List("foo"), (c: Cell[Position3D]) => !predicate(c))))
      } Then {
        buffer: mutable.Buffer[Position3D] =>
          buffer.toList shouldBe List(Position3D("foo", 1, "xyz"), Position3D("foo", 2, "xyz"),
            Position3D("qux", 1, "xyz"))
      }
    }

    "return its first along multiple coordinates in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.which(Along(First), List((List(Position2D(2, "xyz"), Position2D(4, "xyz")),
            (c: Cell[Position3D]) => predicate(c)), (List(Position2D(2, "xyz")),
            (c: Cell[Position3D]) => !predicate(c))))
      } Then {
        buffer: mutable.Buffer[Position3D] =>
          buffer.toList shouldBe List(Position3D("foo", 2, "xyz"), Position3D("bar", 2, "xyz"),
            Position3D("baz", 2, "xyz"), Position3D("foo", 4, "xyz"))
      }
    }

    "return its second over multiple coordinates in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.which(Over(Second), List((List(2, 4), (c: Cell[Position3D]) => predicate(c)),
            (List(2), (c: Cell[Position3D]) => !predicate(c))))
      } Then {
        buffer: mutable.Buffer[Position3D] =>
          buffer.toList shouldBe List(Position3D("foo", 2, "xyz"), Position3D("bar", 2, "xyz"),
            Position3D("baz", 2, "xyz"), Position3D("foo", 4, "xyz"))
      }
    }

    "return its second along multiple coordinates in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.which(Along(Second), List((List(Position2D("bar", "xyz"), Position2D("qux", "xyz")),
            (c: Cell[Position3D]) => predicate(c)), (List(Position2D("foo", "xyz")),
            (c: Cell[Position3D]) => !predicate(c))))
      } Then {
        buffer: mutable.Buffer[Position3D] =>
          buffer.toList shouldBe List(Position3D("foo", 1, "xyz"), Position3D("foo", 2, "xyz"),
            Position3D("qux", 1, "xyz"))
      }
    }

    "return its third over multiple coordinates in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.which(Over(Third), List(("xyz", (c: Cell[Position3D]) => predicate(c)),
            ("xyz", (c: Cell[Position3D]) => !predicate(c))))
      } Then {
        buffer: mutable.Buffer[Position3D] =>
          buffer.toList shouldBe data3.map(_.position)
      }
    }

    "return its third along multiple coordinates in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.which(Along(Third), List((List(Position2D("foo", 1), Position2D("qux", 1)),
            (c: Cell[Position3D]) => predicate(c)), (List(Position2D("foo", 2)),
            (c: Cell[Position3D]) => !predicate(c))))
      } Then {
        buffer: mutable.Buffer[Position3D] =>
          buffer.toList shouldBe List(Position3D("foo", 2, "xyz"), Position3D("qux", 1, "xyz"))
      }
    }
  }
}

class TestMatrixGet extends WordSpec with Matchers with TBddDsl with TestMatrix {

  "A Matrix.get" should {
    "return its cells in 1D" in {
      Given {
        data1
      } When {
        cells: TypedPipe[Cell[Position1D]] =>
          cells.get("qux")
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList shouldBe List(Cell(Position1D("qux"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))
      }
    }

    "return its cells in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.get(List(Position2D("foo", 3), Position2D("qux", 1), Position2D("baz", 4)))
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList shouldBe List(Cell(Position2D("foo", 3), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position2D("qux", 1), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))
      }
    }

    "return its cells in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.get(List(Position3D("foo", 3, "xyz"), Position3D("qux", 1, "xyz"), Position3D("baz", 4, "xyz")))
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList shouldBe List(
            Cell(Position3D("foo", 3, "xyz"), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position3D("qux", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))
      }
    }
  }
}

class TestMatrixToMap extends WordSpec with Matchers with TBddDsl with TestMatrix {

  "A Matrix.toMap" should {
    "return its first over map in 1D" in {
      Given {
        data1
      } When {
        cells: TypedPipe[Cell[Position1D]] =>
          cells.toMap(Over(First)).toTypedPipe
      } Then {
        buffer: mutable.Buffer[Map[Position1D, Content]] =>
          buffer.toList shouldBe List(data1.map { case c => c.position -> c.content }.toMap)
      }
    }

    "return its first over map in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.toMap(Over(First)).toTypedPipe
      } Then {
        buffer: mutable.Buffer[Map[Position1D, Map[Position1D, Content]]] =>
          buffer.toList shouldBe List(Map(
            Position1D("foo") -> Map(Position1D(1) -> Content(OrdinalSchema[Codex.StringCodex](), "3.14"),
              Position1D(2) -> Content(ContinuousSchema[Codex.DoubleCodex](), 6.28),
              Position1D(3) -> Content(NominalSchema[Codex.StringCodex](), "9.42"),
              Position1D(4) -> Content(DateSchema[Codex.DateTimeCodex](),
                (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
            Position1D("bar") -> Map(Position1D(1) -> Content(OrdinalSchema[Codex.StringCodex](), "6.28"),
              Position1D(2) -> Content(ContinuousSchema[Codex.DoubleCodex](), 12.56),
              Position1D(3) -> Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Position1D("baz") -> Map(Position1D(1) -> Content(OrdinalSchema[Codex.StringCodex](), "9.42"),
              Position1D(2) -> Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Position1D("qux") -> Map(Position1D(1) -> Content(OrdinalSchema[Codex.StringCodex](), "12.56"))))
      }
    }

    "return its first along map in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.toMap(Along(First)).toTypedPipe
      } Then {
        buffer: mutable.Buffer[Map[Position1D, Map[Position1D, Content]]] =>
          buffer.toList shouldBe List(Map(
            Position1D(1) -> Map(Position1D("foo") -> Content(OrdinalSchema[Codex.StringCodex](), "3.14"),
              Position1D("bar") -> Content(OrdinalSchema[Codex.StringCodex](), "6.28"),
              Position1D("baz") -> Content(OrdinalSchema[Codex.StringCodex](), "9.42"),
              Position1D("qux") -> Content(OrdinalSchema[Codex.StringCodex](), "12.56")),
            Position1D(2) -> Map(Position1D("foo") -> Content(ContinuousSchema[Codex.DoubleCodex](), 6.28),
              Position1D("bar") -> Content(ContinuousSchema[Codex.DoubleCodex](), 12.56),
              Position1D("baz") -> Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Position1D(3) -> Map(Position1D("foo") -> Content(NominalSchema[Codex.StringCodex](), "9.42"),
              Position1D("bar") -> Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Position1D(4) -> Map(Position1D("foo") -> Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00")))))
      }
    }

    "return its second over map in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.toMap(Over(Second)).toTypedPipe
      } Then {
        buffer: mutable.Buffer[Map[Position1D, Map[Position1D, Content]]] =>
          buffer.toList shouldBe List(Map(
            Position1D(1) -> Map(Position1D("foo") -> Content(OrdinalSchema[Codex.StringCodex](), "3.14"),
              Position1D("bar") -> Content(OrdinalSchema[Codex.StringCodex](), "6.28"),
              Position1D("baz") -> Content(OrdinalSchema[Codex.StringCodex](), "9.42"),
              Position1D("qux") -> Content(OrdinalSchema[Codex.StringCodex](), "12.56")),
            Position1D(2) -> Map(Position1D("foo") -> Content(ContinuousSchema[Codex.DoubleCodex](), 6.28),
              Position1D("bar") -> Content(ContinuousSchema[Codex.DoubleCodex](), 12.56),
              Position1D("baz") -> Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Position1D(3) -> Map(Position1D("foo") -> Content(NominalSchema[Codex.StringCodex](), "9.42"),
              Position1D("bar") -> Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Position1D(4) -> Map(Position1D("foo") -> Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00")))))
      }
    }

    "return its second along map in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.toMap(Along(Second)).toTypedPipe
      } Then {
        buffer: mutable.Buffer[Map[Position1D, Map[Position1D, Content]]] =>
          buffer.toList shouldBe List(Map(
            Position1D("foo") -> Map(Position1D(1) -> Content(OrdinalSchema[Codex.StringCodex](), "3.14"),
              Position1D(2) -> Content(ContinuousSchema[Codex.DoubleCodex](), 6.28),
              Position1D(3) -> Content(NominalSchema[Codex.StringCodex](), "9.42"),
              Position1D(4) -> Content(DateSchema[Codex.DateTimeCodex](),
                (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
            Position1D("bar") -> Map(Position1D(1) -> Content(OrdinalSchema[Codex.StringCodex](), "6.28"),
              Position1D(2) -> Content(ContinuousSchema[Codex.DoubleCodex](), 12.56),
              Position1D(3) -> Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Position1D("baz") -> Map(Position1D(1) -> Content(OrdinalSchema[Codex.StringCodex](), "9.42"),
              Position1D(2) -> Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Position1D("qux") -> Map(Position1D(1) -> Content(OrdinalSchema[Codex.StringCodex](), "12.56"))))
      }
    }

    "return its first over map in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.toMap(Over(First)).toTypedPipe
      } Then {
        buffer: mutable.Buffer[Map[Position1D, Map[Position2D, Content]]] =>
          buffer.toList shouldBe List(Map(
            Position1D("foo") -> Map(Position2D(1, "xyz") -> Content(OrdinalSchema[Codex.StringCodex](), "3.14"),
              Position2D(2, "xyz") -> Content(ContinuousSchema[Codex.DoubleCodex](), 6.28),
              Position2D(3, "xyz") -> Content(NominalSchema[Codex.StringCodex](), "9.42"),
              Position2D(4, "xyz") -> Content(DateSchema[Codex.DateTimeCodex](),
                (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
            Position1D("bar") -> Map(Position2D(1, "xyz") -> Content(OrdinalSchema[Codex.StringCodex](), "6.28"),
              Position2D(2, "xyz") -> Content(ContinuousSchema[Codex.DoubleCodex](), 12.56),
              Position2D(3, "xyz") -> Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Position1D("baz") -> Map(Position2D(1, "xyz") -> Content(OrdinalSchema[Codex.StringCodex](), "9.42"),
              Position2D(2, "xyz") -> Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Position1D("qux") -> Map(Position2D(1, "xyz") -> Content(OrdinalSchema[Codex.StringCodex](), "12.56"))))
      }
    }

    "return its first along map in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.toMap(Along(First)).toTypedPipe
      } Then {
        buffer: mutable.Buffer[Map[Position2D, Map[Position1D, Content]]] =>
          buffer.toList shouldBe List(Map(
            Position2D(1, "xyz") -> Map(Position1D("foo") -> Content(OrdinalSchema[Codex.StringCodex](), "3.14"),
              Position1D("bar") -> Content(OrdinalSchema[Codex.StringCodex](), "6.28"),
              Position1D("baz") -> Content(OrdinalSchema[Codex.StringCodex](), "9.42"),
              Position1D("qux") -> Content(OrdinalSchema[Codex.StringCodex](), "12.56")),
            Position2D(2, "xyz") -> Map(Position1D("foo") -> Content(ContinuousSchema[Codex.DoubleCodex](), 6.28),
              Position1D("bar") -> Content(ContinuousSchema[Codex.DoubleCodex](), 12.56),
              Position1D("baz") -> Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Position2D(3, "xyz") -> Map(Position1D("foo") -> Content(NominalSchema[Codex.StringCodex](), "9.42"),
              Position1D("bar") -> Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Position2D(4, "xyz") -> Map(Position1D("foo") -> Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00")))))
      }
    }

    "return its second over map in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.toMap(Over(Second)).toTypedPipe
      } Then {
        buffer: mutable.Buffer[Map[Position1D, Map[Position2D, Content]]] =>
          buffer.toList shouldBe List(Map(
            Position1D(1) -> Map(Position2D("foo", "xyz") -> Content(OrdinalSchema[Codex.StringCodex](), "3.14"),
              Position2D("bar", "xyz") -> Content(OrdinalSchema[Codex.StringCodex](), "6.28"),
              Position2D("baz", "xyz") -> Content(OrdinalSchema[Codex.StringCodex](), "9.42"),
              Position2D("qux", "xyz") -> Content(OrdinalSchema[Codex.StringCodex](), "12.56")),
            Position1D(2) -> Map(Position2D("foo", "xyz") -> Content(ContinuousSchema[Codex.DoubleCodex](), 6.28),
              Position2D("bar", "xyz") -> Content(ContinuousSchema[Codex.DoubleCodex](), 12.56),
              Position2D("baz", "xyz") -> Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Position1D(3) -> Map(Position2D("foo", "xyz") -> Content(NominalSchema[Codex.StringCodex](), "9.42"),
              Position2D("bar", "xyz") -> Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Position1D(4) -> Map(Position2D("foo", "xyz") -> Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00")))))
      }
    }

    "return its second along map in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.toMap(Along(Second)).toTypedPipe
      } Then {
        buffer: mutable.Buffer[Map[Position2D, Map[Position1D, Content]]] =>
          buffer.toList shouldBe List(Map(
            Position2D("foo", "xyz") -> Map(Position1D(1) -> Content(OrdinalSchema[Codex.StringCodex](), "3.14"),
              Position1D(2) -> Content(ContinuousSchema[Codex.DoubleCodex](), 6.28),
              Position1D(3) -> Content(NominalSchema[Codex.StringCodex](), "9.42"),
              Position1D(4) -> Content(DateSchema[Codex.DateTimeCodex](),
                (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
            Position2D("bar", "xyz") -> Map(Position1D(1) -> Content(OrdinalSchema[Codex.StringCodex](), "6.28"),
              Position1D(2) -> Content(ContinuousSchema[Codex.DoubleCodex](), 12.56),
              Position1D(3) -> Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Position2D("baz", "xyz") -> Map(Position1D(1) -> Content(OrdinalSchema[Codex.StringCodex](), "9.42"),
              Position1D(2) -> Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Position2D("qux", "xyz") -> Map(Position1D(1) -> Content(OrdinalSchema[Codex.StringCodex](), "12.56"))))
      }
    }

    "return its third over map in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.toMap(Over(Third)).toTypedPipe
      } Then {
        buffer: mutable.Buffer[Map[Position1D, Map[Position2D, Content]]] =>
          buffer.toList shouldBe List(Map(
            Position1D("xyz") -> Map(
              Position2D("foo", 1) -> Content(OrdinalSchema[Codex.StringCodex](), "3.14"),
              Position2D("bar", 1) -> Content(OrdinalSchema[Codex.StringCodex](), "6.28"),
              Position2D("baz", 1) -> Content(OrdinalSchema[Codex.StringCodex](), "9.42"),
              Position2D("qux", 1) -> Content(OrdinalSchema[Codex.StringCodex](), "12.56"),
              Position2D("foo", 2) -> Content(ContinuousSchema[Codex.DoubleCodex](), 6.28),
              Position2D("bar", 2) -> Content(ContinuousSchema[Codex.DoubleCodex](), 12.56),
              Position2D("baz", 2) -> Content(DiscreteSchema[Codex.LongCodex](), 19),
              Position2D("foo", 3) -> Content(NominalSchema[Codex.StringCodex](), "9.42"),
              Position2D("bar", 3) -> Content(OrdinalSchema[Codex.LongCodex](), 19),
              Position2D("foo", 4) -> Content(DateSchema[Codex.DateTimeCodex](),
                (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00")))))
      }
    }

    "return its third along map in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.toMap(Along(Third)).toTypedPipe
      } Then {
        buffer: mutable.Buffer[Map[Position2D, Map[Position1D, Content]]] =>
          buffer.toList shouldBe List(Map(
            Position2D("foo", 1) -> Map(Position1D("xyz") -> Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
            Position2D("foo", 2) -> Map(Position1D("xyz") -> Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Position2D("foo", 3) -> Map(Position1D("xyz") -> Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Position2D("foo", 4) -> Map(Position1D("xyz") -> Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
            Position2D("bar", 1) -> Map(Position1D("xyz") -> Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Position2D("bar", 2) -> Map(Position1D("xyz") -> Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Position2D("bar", 3) -> Map(Position1D("xyz") -> Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Position2D("baz", 1) -> Map(Position1D("xyz") -> Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Position2D("baz", 2) -> Map(Position1D("xyz") -> Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Position2D("qux", 1) -> Map(Position1D("xyz") -> Content(OrdinalSchema[Codex.StringCodex](), "12.56"))))
      }
    }
  }
}

class TestMatrixReduce extends WordSpec with Matchers with TBddDsl with TestMatrix {

  val ext = ValuePipe(Map(Position1D("foo") -> Content(ContinuousSchema[Codex.DoubleCodex](), 1.0 / 1),
    Position1D("bar") -> Content(ContinuousSchema[Codex.DoubleCodex](), 1.0 / 2),
    Position1D("baz") -> Content(ContinuousSchema[Codex.DoubleCodex](), 1.0 / 3),
    Position1D("qux") -> Content(ContinuousSchema[Codex.DoubleCodex](), 1.0 / 4),
    Position1D("foo.2") -> Content(ContinuousSchema[Codex.DoubleCodex](), 1),
    Position1D("bar.2") -> Content(ContinuousSchema[Codex.DoubleCodex](), 1),
    Position1D("baz.2") -> Content(ContinuousSchema[Codex.DoubleCodex](), 1),
    Position1D("qux.2") -> Content(ContinuousSchema[Codex.DoubleCodex](), 1),
    Position1D(1) -> Content(ContinuousSchema[Codex.DoubleCodex](), 1.0 / 2),
    Position1D(2) -> Content(ContinuousSchema[Codex.DoubleCodex](), 1.0 / 4),
    Position1D(3) -> Content(ContinuousSchema[Codex.DoubleCodex](), 1.0 / 6),
    Position1D(4) -> Content(ContinuousSchema[Codex.DoubleCodex](), 1.0 / 8),
    Position1D("1.2") -> Content(ContinuousSchema[Codex.DoubleCodex](), 1),
    Position1D("2.2") -> Content(ContinuousSchema[Codex.DoubleCodex](), 1),
    Position1D("3.2") -> Content(ContinuousSchema[Codex.DoubleCodex](), 1),
    Position1D("4.2") -> Content(ContinuousSchema[Codex.DoubleCodex](), 1),
    Position1D("xyz") -> Content(ContinuousSchema[Codex.DoubleCodex](), 1 / 3.14),
    Position1D("xyz.2") -> Content(ContinuousSchema[Codex.DoubleCodex](), 1 / 6.28)))

  "A Matrix.reduce" should {
    "return its first over aggregates in 2D" in {
      Given {
        num2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.reduce(Over(First), Min())
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList shouldBe List(Cell(Position1D("bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position1D("baz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
            Cell(Position1D("foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
            Cell(Position1D("qux"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)))
      }
    }

    "return its first along aggregates in 2D" in {
      Given {
        num2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.reduce(Along(First), Max())
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList shouldBe List(Cell(Position1D(1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position1D(2), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
            Cell(Position1D(3), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
            Cell(Position1D(4), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)))
      }
    }

    "return its second over aggregates in 2D" in {
      Given {
        num2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.reduce(Over(Second), Max())
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList shouldBe List(Cell(Position1D(1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position1D(2), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
            Cell(Position1D(3), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
            Cell(Position1D(4), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)))
      }
    }

    "return its second along aggregates in 2D" in {
      Given {
        num2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.reduce(Along(Second), Min())
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList shouldBe List(Cell(Position1D("bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position1D("baz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
            Cell(Position1D("foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
            Cell(Position1D("qux"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)))
      }
    }

    "return its first over aggregates in 3D" in {
      Given {
        num3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.reduce(Over(First), Min())
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList shouldBe List(Cell(Position1D("bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position1D("baz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
            Cell(Position1D("foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
            Cell(Position1D("qux"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)))
      }
    }

    "return its first along aggregates in 3D" in {
      Given {
        num3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.reduce(Along(First), Max())
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList shouldBe List(
            Cell(Position2D(1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position2D(2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
            Cell(Position2D(3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
            Cell(Position2D(4, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)))
      }
    }

    "return its second over aggregates in 3D" in {
      Given {
        num3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.reduce(Over(Second), Max())
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList shouldBe List(Cell(Position1D(1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position1D(2), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
            Cell(Position1D(3), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
            Cell(Position1D(4), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)))
      }
    }

    "return its second along aggregates in 3D" in {
      Given {
        num3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.reduce(Along(Second), Min())
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList shouldBe List(
            Cell(Position2D("bar", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position2D("baz", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
            Cell(Position2D("foo", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
            Cell(Position2D("qux", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)))
      }
    }

    "return its third over aggregates in 3D" in {
      Given {
        num3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.reduce(Over(Third), Max())
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList shouldBe List(Cell(Position1D("xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)))
      }
    }

    "return its third along aggregates in 3D" in {
      Given {
        num3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.reduce(Along(Third), Min())
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList shouldBe List(Cell(Position2D("bar", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position2D("bar", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position2D("bar", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
            Cell(Position2D("baz", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
            Cell(Position2D("baz", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
            Cell(Position2D("foo", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
            Cell(Position2D("foo", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position2D("foo", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
            Cell(Position2D("foo", 4), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position2D("qux", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)))
      }
    }
  }

  "A Matrix.reduceWithValue" should {
    "return its first over aggregates in 2D" in {
      Given {
        num2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.reduceWithValue(Over(First), WeightedSum(First), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList shouldBe List(
            Cell(Position1D("bar"), Content(ContinuousSchema[Codex.DoubleCodex](),
              (6.28 + 12.56 + 18.84) * (1.0 / 2))),
            Cell(Position1D("baz"), Content(ContinuousSchema[Codex.DoubleCodex](), (9.42 + 18.84) * (1.0 / 3))),
            Cell(Position1D("foo"), Content(ContinuousSchema[Codex.DoubleCodex](),
              (3.14 + 6.28 + 9.42 + 12.56) * (1.0 / 1))),
            Cell(Position1D("qux"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 * (1.0 / 4))))
      }
    }

    "return its first along aggregates in 2D" in {
      Given {
        num2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.reduceWithValue(Along(First), WeightedSum(Second), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList shouldBe List(
            Cell(Position1D(1), Content(ContinuousSchema[Codex.DoubleCodex](),
              (3.14 + 6.28 + 9.42 + 12.56) * (1.0 / 2))),
            Cell(Position1D(2), Content(ContinuousSchema[Codex.DoubleCodex](), (6.28 + 12.56 + 18.84) * (1.0 / 4))),
            Cell(Position1D(3), Content(ContinuousSchema[Codex.DoubleCodex](), (9.42 + 18.84) * (1.0 / 6))),
            Cell(Position1D(4), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 * (1.0 / 8))))
      }
    }

    "return its second over aggregates in 2D" in {
      Given {
        num2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.reduceWithValue(Over(Second), WeightedSum(Second), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList shouldBe List(
            Cell(Position1D(1), Content(ContinuousSchema[Codex.DoubleCodex](),
              (3.14 + 6.28 + 9.42 + 12.56) * (1.0 / 2))),
            Cell(Position1D(2), Content(ContinuousSchema[Codex.DoubleCodex](), (6.28 + 12.56 + 18.84) * (1.0 / 4))),
            Cell(Position1D(3), Content(ContinuousSchema[Codex.DoubleCodex](), (9.42 + 18.84) * (1.0 / 6))),
            Cell(Position1D(4), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 * (1.0 / 8))))
      }
    }

    "return its second along aggregates in 2D" in {
      Given {
        num2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.reduceWithValue(Along(Second), WeightedSum(First), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList shouldBe List(
            Cell(Position1D("bar"), Content(ContinuousSchema[Codex.DoubleCodex](),
              (6.28 + 12.56 + 18.84) * (1.0 / 2))),
            Cell(Position1D("baz"), Content(ContinuousSchema[Codex.DoubleCodex](), (9.42 + 18.84) * (1.0 / 3))),
            Cell(Position1D("foo"), Content(ContinuousSchema[Codex.DoubleCodex](),
              (3.14 + 6.28 + 9.42 + 12.56) * (1.0 / 1))),
            Cell(Position1D("qux"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 * (1.0 / 4))))
      }
    }

    "return its first over aggregates in 3D" in {
      Given {
        num3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.reduceWithValue(Over(First), WeightedSum(First), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList shouldBe List(
            Cell(Position1D("bar"), Content(ContinuousSchema[Codex.DoubleCodex](),
              (6.28 + 12.56 + 18.84) * (1.0 / 2))),
            Cell(Position1D("baz"), Content(ContinuousSchema[Codex.DoubleCodex](), (9.42 + 18.84) * (1.0 / 3))),
            Cell(Position1D("foo"), Content(ContinuousSchema[Codex.DoubleCodex](),
              (3.14 + 6.28 + 9.42 + 12.56) * (1.0 / 1))),
            Cell(Position1D("qux"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 * (1.0 / 4))))
      }
    }

    "return its first along aggregates in 3D" in {
      Given {
        num3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.reduceWithValue(Along(First), WeightedSum(Second), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList shouldBe List(
            Cell(Position2D(1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](),
              (3.14 + 6.28 + 9.42 + 12.56) * (1.0 / 2))),
            Cell(Position2D(2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](),
              (6.28 + 12.56 + 18.84) * (1.0 / 4))),
            Cell(Position2D(3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), (9.42 + 18.84) * (1.0 / 6))),
            Cell(Position2D(4, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 * (1.0 / 8))))
      }
    }

    "return its second over aggregates in 3D" in {
      Given {
        num3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.reduceWithValue(Over(Second), WeightedSum(Second), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList shouldBe List(
            Cell(Position1D(1), Content(ContinuousSchema[Codex.DoubleCodex](),
              (3.14 + 6.28 + 9.42 + 12.56) * (1.0 / 2))),
            Cell(Position1D(2), Content(ContinuousSchema[Codex.DoubleCodex](), (6.28 + 12.56 + 18.84) * (1.0 / 4))),
            Cell(Position1D(3), Content(ContinuousSchema[Codex.DoubleCodex](), (9.42 + 18.84) * (1.0 / 6))),
            Cell(Position1D(4), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 * (1.0 / 8))))
      }
    }

    "return its second along aggregates in 3D" in {
      Given {
        num3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.reduceWithValue(Along(Second), WeightedSum(First), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList shouldBe List(
            Cell(Position2D("bar", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](),
              (6.28 + 12.56 + 18.84) * (1.0 / 2))),
            Cell(Position2D("baz", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), (9.42 + 18.84) * (1.0 / 3))),
            Cell(Position2D("foo", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](),
              (3.14 + 6.28 + 9.42 + 12.56) * (1.0 / 1))),
            Cell(Position2D("qux", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 * (1.0 / 4))))
      }
    }

    "return its third over aggregates in 3D" in {
      Given {
        num3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.reduceWithValue(Over(Third), WeightedSum(Third), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList shouldBe List(Cell(Position1D("xyz"), Content(ContinuousSchema[Codex.DoubleCodex](),
            (3.14 + 2 * 6.28 + 2 * 9.42 + 3 * 12.56 + 2 * 18.84) / 3.14)))
      }
    }

    "return its third along aggregates in 3D" in {
      Given {
        num3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.reduceWithValue(Along(Third), WeightedSum(Third), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList shouldBe List(
            Cell(Position2D("bar", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 / 3.14)),
            Cell(Position2D("bar", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 / 3.14)),
            Cell(Position2D("bar", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 / 3.14)),
            Cell(Position2D("baz", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 / 3.14)),
            Cell(Position2D("baz", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 / 3.14)),
            Cell(Position2D("foo", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 / 3.14)),
            Cell(Position2D("foo", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 / 3.14)),
            Cell(Position2D("foo", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 / 3.14)),
            Cell(Position2D("foo", 4), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 / 3.14)),
            Cell(Position2D("qux", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 / 3.14)))
      }
    }
  }

  "A Matrix.reduceAndExpand" should {
    "return its first over aggregates in 1D" in {
      Given {
        num1
      } When {
        cells: TypedPipe[Cell[Position1D]] =>
          cells.reduceAndExpand(Over(First), Min("min"))
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList shouldBe List(
            Cell(Position2D("bar", "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position2D("baz", "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
            Cell(Position2D("foo", "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
            Cell(Position2D("qux", "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)))
      }
    }

    "return its first along aggregates in 1D" in {
      Given {
        num1
      } When {
        cells: TypedPipe[Cell[Position1D]] =>
          cells.reduceAndExpand(Along(First), List(Min("min"), Max("max")))
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList shouldBe List(Cell(Position1D("min"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
            Cell(Position1D("max"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)))
      }
    }

    "return its first over aggregates in 2D" in {
      Given {
        num2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.reduceAndExpand(Over(First), Min("min"))
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList shouldBe List(
            Cell(Position2D("bar", "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position2D("baz", "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
            Cell(Position2D("foo", "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
            Cell(Position2D("qux", "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)))
      }
    }

    "return its first along aggregates in 2D" in {
      Given {
        num2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.reduceAndExpand(Along(First), List(Min("min"), Max("max")))
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList shouldBe List(Cell(Position2D(1, "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
            Cell(Position2D(1, "max"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position2D(2, "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position2D(2, "max"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
            Cell(Position2D(3, "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
            Cell(Position2D(3, "max"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
            Cell(Position2D(4, "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position2D(4, "max"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)))
      }
    }

    "return its second over aggregates in 2D" in {
      Given {
        num2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.reduceAndExpand(Over(Second), List(Min("min"), Max("max")))
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList shouldBe List(Cell(Position2D(1, "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
            Cell(Position2D(1, "max"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position2D(2, "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position2D(2, "max"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
            Cell(Position2D(3, "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
            Cell(Position2D(3, "max"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
            Cell(Position2D(4, "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position2D(4, "max"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)))
      }
    }

    "return its second along aggregates in 2D" in {
      Given {
        num2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.reduceAndExpand(Along(Second), Min("min"))
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList shouldBe List(
            Cell(Position2D("bar", "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position2D("baz", "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
            Cell(Position2D("foo", "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
            Cell(Position2D("qux", "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)))
      }
    }

    "return its first over aggregates in 3D" in {
      Given {
        num3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.reduceAndExpand(Over(First), Min("min"))
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList shouldBe List(
            Cell(Position2D("bar", "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position2D("baz", "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
            Cell(Position2D("foo", "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
            Cell(Position2D("qux", "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)))
      }
    }

    "return its first along aggregates in 3D" in {
      Given {
        num3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.reduceAndExpand(Along(First), List(Min("min"), Max("max")))
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList shouldBe List(
            Cell(Position3D(1, "xyz", "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
            Cell(Position3D(1, "xyz", "max"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position3D(2, "xyz", "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position3D(2, "xyz", "max"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
            Cell(Position3D(3, "xyz", "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
            Cell(Position3D(3, "xyz", "max"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
            Cell(Position3D(4, "xyz", "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position3D(4, "xyz", "max"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)))
      }
    }

    "return its second over aggregates in 3D" in {
      Given {
        num3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.reduceAndExpand(Over(Second), List(Min("min"), Max("max")))
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList shouldBe List(Cell(Position2D(1, "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
            Cell(Position2D(1, "max"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position2D(2, "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position2D(2, "max"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
            Cell(Position2D(3, "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
            Cell(Position2D(3, "max"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
            Cell(Position2D(4, "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position2D(4, "max"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)))
      }
    }

    "return its second along aggregates in 3D" in {
      Given {
        num3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.reduceAndExpand(Along(Second), Min("min"))
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList shouldBe List(
            Cell(Position3D("bar", "xyz", "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position3D("baz", "xyz", "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
            Cell(Position3D("foo", "xyz", "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
            Cell(Position3D("qux", "xyz", "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)))
      }
    }

    "return its third over aggregates in 3D" in {
      Given {
        num3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.reduceAndExpand(Over(Third), List(Min("min"), Max("max")))
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList shouldBe List(
            Cell(Position2D("xyz", "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
            Cell(Position2D("xyz", "max"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)))
      }
    }

    "return its third along aggregates in 3D" in {
      Given {
        num3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.reduceAndExpand(Along(Third), Min("min"))
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList shouldBe List(
            Cell(Position3D("bar", 1, "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position3D("bar", 2, "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position3D("bar", 3, "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
            Cell(Position3D("baz", 1, "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
            Cell(Position3D("baz", 2, "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
            Cell(Position3D("foo", 1, "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
            Cell(Position3D("foo", 2, "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position3D("foo", 3, "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
            Cell(Position3D("foo", 4, "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position3D("qux", 1, "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)))
      }
    }
  }

  "A Matrix.reduceAndExpandWithValue" should {
    "return its first over aggregates in 1D" in {
      Given {
        num1
      } When {
        cells: TypedPipe[Cell[Position1D]] =>
          cells.reduceAndExpandWithValue(Over(First), WeightedSum(First, "sum"), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList shouldBe List(
            Cell(Position2D("bar", "sum"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 / 2)),
            Cell(Position2D("baz", "sum"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 * (1.0 / 3))),
            Cell(Position2D("foo", "sum"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 / 1)),
            Cell(Position2D("qux", "sum"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 / 4)))
      }
    }

    "return its first along aggregates in 1D" in {
      Given {
        num1
      } When {
        cells: TypedPipe[Cell[Position1D]] =>
          cells.reduceAndExpandWithValue(Along(First), List(WeightedSum(First, "sum.1"),
            WeightedSum(First, "sum.2", "%1$s.2")), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList shouldBe List(Cell(Position1D("sum.1"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position1D("sum.2"), Content(ContinuousSchema[Codex.DoubleCodex](), 31.40)))
      }
    }

    "return its first over aggregates in 2D" in {
      Given {
        num2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.reduceAndExpandWithValue(Over(First), WeightedSum(First, "sum"), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList shouldBe List(
            Cell(Position2D("bar", "sum"),
              Content(ContinuousSchema[Codex.DoubleCodex](), (6.28 + 12.56 + 18.84) * (1.0 / 2))),
            Cell(Position2D("baz", "sum"),
              Content(ContinuousSchema[Codex.DoubleCodex](), (9.42 + 18.84) * (1.0 / 3))),
            Cell(Position2D("foo", "sum"),
              Content(ContinuousSchema[Codex.DoubleCodex](), (3.14 + 6.28 + 9.42 + 12.56) * (1.0 / 1))),
            Cell(Position2D("qux", "sum"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 * (1.0 / 4))))
      }
    }

    "return its first along aggregates in 2D" in {
      Given {
        num2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.reduceAndExpandWithValue(Along(First), List(WeightedSum(Second, "sum.1"),
            WeightedSum(First, "sum.2", "%1$s.2")), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList shouldBe List(
            Cell(Position2D(1, "sum.1"),
              Content(ContinuousSchema[Codex.DoubleCodex](), (3.14 + 6.28 + 9.42 + 12.56) * (1.0 / 2))),
            Cell(Position2D(1, "sum.2"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 6.28 + 9.42 + 12.56)),
            Cell(Position2D(2, "sum.1"),
              Content(ContinuousSchema[Codex.DoubleCodex](), (6.28 + 12.56 + 18.84) * (1.0 / 4))),
            Cell(Position2D(2, "sum.2"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 12.56 + 18.84)),
            Cell(Position2D(3, "sum.1"), Content(ContinuousSchema[Codex.DoubleCodex](), (9.42 + 18.84) * (1.0 / 6))),
            Cell(Position2D(3, "sum.2"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 18.84)),
            Cell(Position2D(4, "sum.1"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 * (1.0 / 8))),
            Cell(Position2D(4, "sum.2"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)))
      }
    }

    "return its second over aggregates in 2D" in {
      Given {
        num2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.reduceAndExpandWithValue(Over(Second), List(WeightedSum(Second, "sum.1"),
            WeightedSum(Second, "sum.2", "%1$s.2")), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList shouldBe List(
            Cell(Position2D(1, "sum.1"),
              Content(ContinuousSchema[Codex.DoubleCodex](), (3.14 + 6.28 + 9.42 + 12.56) * (1.0 / 2))),
            Cell(Position2D(1, "sum.2"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 6.28 + 9.42 + 12.56)),
            Cell(Position2D(2, "sum.1"),
              Content(ContinuousSchema[Codex.DoubleCodex](), (6.28 + 12.56 + 18.84) * (1.0 / 4))),
            Cell(Position2D(2, "sum.2"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 12.56 + 18.84)),
            Cell(Position2D(3, "sum.1"), Content(ContinuousSchema[Codex.DoubleCodex](), (9.42 + 18.84) * (1.0 / 6))),
            Cell(Position2D(3, "sum.2"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 18.84)),
            Cell(Position2D(4, "sum.1"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 * (1.0 / 8))),
            Cell(Position2D(4, "sum.2"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)))
      }
    }

    "return its second along aggregates in 2D" in {
      Given {
        num2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.reduceAndExpandWithValue(Along(Second), WeightedSum(First, "sum"), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList shouldBe List(
            Cell(Position2D("bar", "sum"),
              Content(ContinuousSchema[Codex.DoubleCodex](), (6.28 + 12.56 + 18.84) * (1.0 / 2))),
            Cell(Position2D("baz", "sum"),
              Content(ContinuousSchema[Codex.DoubleCodex](), (9.42 + 18.84) * (1.0 / 3))),
            Cell(Position2D("foo", "sum"),
              Content(ContinuousSchema[Codex.DoubleCodex](), (3.14 + 6.28 + 9.42 + 12.56) * (1.0 / 1))),
            Cell(Position2D("qux", "sum"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 * (1.0 / 4))))
      }
    }

    "return its first over aggregates in 3D" in {
      Given {
        num3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.reduceAndExpandWithValue(Over(First), WeightedSum(First, "sum"), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList shouldBe List(
            Cell(Position2D("bar", "sum"),
              Content(ContinuousSchema[Codex.DoubleCodex](), (6.28 + 12.56 + 18.84) * (1.0 / 2))),
            Cell(Position2D("baz", "sum"),
              Content(ContinuousSchema[Codex.DoubleCodex](), (9.42 + 18.84) * (1.0 / 3))),
            Cell(Position2D("foo", "sum"),
              Content(ContinuousSchema[Codex.DoubleCodex](), (3.14 + 6.28 + 9.42 + 12.56) * (1.0 / 1))),
            Cell(Position2D("qux", "sum"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 * (1.0 / 4))))
      }
    }

    "return its first along aggregates in 3D" in {
      Given {
        num3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.reduceAndExpandWithValue(Along(First), List(WeightedSum(Second, "sum.1"),
            WeightedSum(Second, "sum.2", "%1$s.2")), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList shouldBe List(
            Cell(Position3D(1, "xyz", "sum.1"),
              Content(ContinuousSchema[Codex.DoubleCodex](), (3.14 + 6.28 + 9.42 + 12.56) * (1.0 / 2))),
            Cell(Position3D(1, "xyz", "sum.2"),
              Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 6.28 + 9.42 + 12.56)),
            Cell(Position3D(2, "xyz", "sum.1"),
              Content(ContinuousSchema[Codex.DoubleCodex](), (6.28 + 12.56 + 18.84) * (1.0 / 4))),
            Cell(Position3D(2, "xyz", "sum.2"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 12.56 + 18.84)),
            Cell(Position3D(3, "xyz", "sum.1"),
              Content(ContinuousSchema[Codex.DoubleCodex](), (9.42 + 18.84) * (1.0 / 6))),
            Cell(Position3D(3, "xyz", "sum.2"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 18.84)),
            Cell(Position3D(4, "xyz", "sum.1"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 * (1.0 / 8))),
            Cell(Position3D(4, "xyz", "sum.2"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)))
      }
    }

    "return its second over aggregates in 3D" in {
      Given {
        num3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.reduceAndExpandWithValue(Over(Second), List(WeightedSum(Second, "sum.1"),
            WeightedSum(Second, "sum.2", "%1$s.2")), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList shouldBe List(
            Cell(Position2D(1, "sum.1"),
              Content(ContinuousSchema[Codex.DoubleCodex](), (3.14 + 6.28 + 9.42 + 12.56) * (1.0 / 2))),
            Cell(Position2D(1, "sum.2"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 6.28 + 9.42 + 12.56)),
            Cell(Position2D(2, "sum.1"),
              Content(ContinuousSchema[Codex.DoubleCodex](), (6.28 + 12.56 + 18.84) * (1.0 / 4))),
            Cell(Position2D(2, "sum.2"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 12.56 + 18.84)),
            Cell(Position2D(3, "sum.1"), Content(ContinuousSchema[Codex.DoubleCodex](), (9.42 + 18.84) * (1.0 / 6))),
            Cell(Position2D(3, "sum.2"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 18.84)),
            Cell(Position2D(4, "sum.1"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 * (1.0 / 8))),
            Cell(Position2D(4, "sum.2"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)))
      }
    }

    "return its second along aggregates in 3D" in {
      Given {
        num3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.reduceAndExpandWithValue(Along(Second), WeightedSum(First, "sum"), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList shouldBe List(
            Cell(Position3D("bar", "xyz", "sum"),
              Content(ContinuousSchema[Codex.DoubleCodex](), (6.28 + 12.56 + 18.84) * (1.0 / 2))),
            Cell(Position3D("baz", "xyz", "sum"),
              Content(ContinuousSchema[Codex.DoubleCodex](), (9.42 + 18.84) * (1.0 / 3))),
            Cell(Position3D("foo", "xyz", "sum"),
              Content(ContinuousSchema[Codex.DoubleCodex](), (3.14 + 6.28 + 9.42 + 12.56) * (1.0 / 1))),
            Cell(Position3D("qux", "xyz", "sum"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 * (1.0 / 4))))
      }
    }

    "return its third over aggregates in 3D" in {
      Given {
        num3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.reduceAndExpandWithValue(Over(Third), List(WeightedSum(Third, "sum.1"),
            WeightedSum(Third, "sum.2", "%1$s.2")), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList shouldBe List(
            Cell(Position2D("xyz", "sum.1"), Content(ContinuousSchema[Codex.DoubleCodex](),
              (3.14 + 2 * 6.28 + 2 * 9.42 + 3 * 12.56 + 2 * 18.84) / 3.14)),
            Cell(Position2D("xyz", "sum.2"), Content(ContinuousSchema[Codex.DoubleCodex](),
              (3.14 + 2 * 6.28 + 2 * 9.42 + 3 * 12.56 + 2 * 18.84) / 6.28)))
      }
    }

    "return its third along aggregates in 3D" in {
      Given {
        num3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.reduceAndExpandWithValue(Along(Third), WeightedSum(Third, "sum"), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList shouldBe List(
            Cell(Position3D("bar", 1, "sum"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 / 3.14)),
            Cell(Position3D("bar", 2, "sum"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 / 3.14)),
            Cell(Position3D("bar", 3, "sum"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 / 3.14)),
            Cell(Position3D("baz", 1, "sum"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 / 3.14)),
            Cell(Position3D("baz", 2, "sum"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 / 3.14)),
            Cell(Position3D("foo", 1, "sum"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 / 3.14)),
            Cell(Position3D("foo", 2, "sum"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 / 3.14)),
            Cell(Position3D("foo", 3, "sum"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 / 3.14)),
            Cell(Position3D("foo", 4, "sum"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 / 3.14)),
            Cell(Position3D("qux", 1, "sum"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 / 3.14)))
      }
    }
  }
}

class TestMatrixPartition extends WordSpec with Matchers with TBddDsl with TestMatrix {

  case class TestPartitioner(dim: Dimension) extends Partitioner with Assign {
    type T = String
    def assign[P <: Position](pos: P): Collection[T] = Collection(pos(dim).toShortString)
  }

  case class TestPartitionerWithValue() extends Partitioner with AssignWithValue {
    type T = String
    type V = Dimension
    def assign[P <: Position](pos: P, ext: V): Collection[T] = Collection(pos(ext).toShortString)
  }

  "A Matrix.partition" should {
    "return its first partitions in 1D" in {
      Given {
        data1
      } When {
        cells: TypedPipe[Cell[Position1D]] =>
          cells.partition(TestPartitioner(First))
      } Then {
        buffer: mutable.Buffer[(String, Cell[Position1D])] =>
          buffer.toList shouldBe data1.map { case c => (c.position(First).toShortString, c) }
      }
    }

    "return its first partitions in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.partition(TestPartitioner(First))
      } Then {
        buffer: mutable.Buffer[(String, Cell[Position2D])] =>
          buffer.toList shouldBe data2.map { case c => (c.position(First).toShortString, c) }
      }
    }

    "return its second partitions in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.partition(TestPartitioner(Second))
      } Then {
        buffer: mutable.Buffer[(String, Cell[Position2D])] =>
          buffer.toList shouldBe data2.map { case c => (c.position(Second).toShortString, c) }
      }
    }

    "return its first partitions in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.partition(TestPartitioner(First))
      } Then {
        buffer: mutable.Buffer[(String, Cell[Position3D])] =>
          buffer.toList shouldBe data3.map { case c => (c.position(First).toShortString, c) }
      }
    }

    "return its second partitions in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.partition(TestPartitioner(Second))
      } Then {
        buffer: mutable.Buffer[(String, Cell[Position3D])] =>
          buffer.toList shouldBe data3.map { case c => (c.position(Second).toShortString, c) }
      }
    }

    "return its third partitions in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.partition(TestPartitioner(Third))
      } Then {
        buffer: mutable.Buffer[(String, Cell[Position3D])] =>
          buffer.toList shouldBe data3.map { case c => (c.position(Third).toShortString, c) }
      }
    }
  }

  "A Matrix.partitionWithValue" should {
    "return its first partitions in 1D" in {
      Given {
        data1
      } When {
        cells: TypedPipe[Cell[Position1D]] =>
          cells.partitionWithValue(TestPartitionerWithValue(), ValuePipe(First))
      } Then {
        buffer: mutable.Buffer[(String, Cell[Position1D])] =>
          buffer.toList shouldBe data1.map { case c => (c.position(First).toShortString, c) }
      }
    }

    "return its first partitions in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.partitionWithValue(TestPartitionerWithValue(), ValuePipe(First))
      } Then {
        buffer: mutable.Buffer[(String, Cell[Position2D])] =>
          buffer.toList shouldBe data2.map { case c => (c.position(First).toShortString, c) }
      }
    }

    "return its second partitions in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.partitionWithValue(TestPartitionerWithValue(), ValuePipe(Second))
      } Then {
        buffer: mutable.Buffer[(String, Cell[Position2D])] =>
          buffer.toList shouldBe data2.map { case c => (c.position(Second).toShortString, c) }
      }
    }

    "return its first partitions in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.partitionWithValue(TestPartitionerWithValue(), ValuePipe(First))
      } Then {
        buffer: mutable.Buffer[(String, Cell[Position3D])] =>
          buffer.toList shouldBe data3.map { case c => (c.position(First).toShortString, c) }
      }
    }

    "return its second partitions in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.partitionWithValue(TestPartitionerWithValue(), ValuePipe(Second))
      } Then {
        buffer: mutable.Buffer[(String, Cell[Position3D])] =>
          buffer.toList shouldBe data3.map { case c => (c.position(Second).toShortString, c) }
      }
    }

    "return its third partitions in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.partitionWithValue(TestPartitionerWithValue(), ValuePipe(Third))
      } Then {
        buffer: mutable.Buffer[(String, Cell[Position3D])] =>
          buffer.toList shouldBe data3.map { case c => (c.position(Third).toShortString, c) }
      }
    }
  }
}

class TestMatrixRefine extends WordSpec with Matchers with TBddDsl with TestMatrix {

  def filter[P <: Position](cell: Cell[P]): Boolean = {
    cell.position.coordinates.contains(StringValue("foo")) || cell.position.coordinates.contains(LongValue(2))
  }

  def filterWithValue[P <: Position](cell: Cell[P], ext: String): Boolean = {
    cell.position.coordinates.contains(StringValue(ext))
  }

  "A Matrix.refine" should {
    "return its refined data in 1D" in {
      Given {
        data1
      } When {
        cells: TypedPipe[Cell[Position1D]] =>
          cells.refine(filter)
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList shouldBe List(Cell(Position1D("foo"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")))
      }
    }

    "return its refined data in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.refine(filter)
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList shouldBe List(Cell(Position2D("foo", 1), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
            Cell(Position2D("foo", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position2D("bar", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position2D("baz", 2), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position2D("foo", 3), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position2D("foo", 4), Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))
      }
    }

    "return its refined data in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.refine(filter)
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList shouldBe List(
            Cell(Position3D("foo", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
            Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position3D("baz", 2, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position3D("foo", 3, "xyz"), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position3D("foo", 4, "xyz"), Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))
      }
    }
  }

  "A Matrix.refineWithValue" should {
    "return its refined data in 1D" in {
      Given {
        data1
      } When {
        cells: TypedPipe[Cell[Position1D]] =>
          cells.refineWithValue(filterWithValue, ValuePipe("foo"))
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList shouldBe List(Cell(Position1D("foo"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")))
      }
    }

    "return its refined data in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.refineWithValue(filterWithValue, ValuePipe("foo"))
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList shouldBe List(Cell(Position2D("foo", 1), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
            Cell(Position2D("foo", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position2D("foo", 3), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position2D("foo", 4), Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))
      }
    }

    "return its refined data in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.refineWithValue(filterWithValue, ValuePipe("foo"))
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList shouldBe List(
            Cell(Position3D("foo", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
            Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position3D("foo", 3, "xyz"), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position3D("foo", 4, "xyz"), Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))
      }
    }
  }
}

class TestMatrixSample extends WordSpec with Matchers with TBddDsl with TestMatrix {

  case class TestSampler() extends Sampler with Select {
    def select[P <: Position](pos: P): Boolean = {
      pos.coordinates.contains(StringValue("foo")) || pos.coordinates.contains(LongValue(2))
    }
  }

  case class TestSamplerWithValue() extends Sampler with SelectWithValue {
    type V = String
    def select[P <: Position](pos: P, ext: V): Boolean = pos.coordinates.contains(StringValue(ext))
  }

  "A Matrix.sample" should {
    "return its sampled data in 1D" in {
      Given {
        data1
      } When {
        cells: TypedPipe[Cell[Position1D]] =>
          cells.sample(TestSampler())
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList shouldBe List(Cell(Position1D("foo"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")))
      }
    }

    "return its sampled data in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.sample(TestSampler())
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList shouldBe List(Cell(Position2D("foo", 1), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
            Cell(Position2D("foo", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position2D("bar", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position2D("baz", 2), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position2D("foo", 3), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position2D("foo", 4), Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))
      }
    }

    "return its sampled data in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.sample(TestSampler())
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList shouldBe List(
            Cell(Position3D("foo", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
            Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position3D("baz", 2, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position3D("foo", 3, "xyz"), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position3D("foo", 4, "xyz"), Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))
      }
    }
  }

  "A Matrix.sampleWithValue" should {
    "return its sampled data in 1D" in {
      Given {
        data1
      } When {
        cells: TypedPipe[Cell[Position1D]] =>
          cells.sampleWithValue(TestSamplerWithValue(), ValuePipe("foo"))
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList shouldBe List(Cell(Position1D("foo"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")))
      }
    }

    "return its sampled data in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.sampleWithValue(TestSamplerWithValue(), ValuePipe("foo"))
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList shouldBe List(Cell(Position2D("foo", 1), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
            Cell(Position2D("foo", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position2D("foo", 3), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position2D("foo", 4), Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))
      }
    }

    "return its sampled data in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.sampleWithValue(TestSamplerWithValue(), ValuePipe("foo"))
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList shouldBe List(
            Cell(Position3D("foo", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
            Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position3D("foo", 3, "xyz"), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position3D("foo", 4, "xyz"), Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))
      }
    }
  }
}

class TestMatrixDomain extends WordSpec with Matchers with TBddDsl with TestMatrix {

  "A Matrix.domain" should {
    "return its domain in 1D" in {
      Given {
        List(Cell(Position1D(1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
          Cell(Position1D(2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
          Cell(Position1D(3), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)))
      } When {
        cells: TypedPipe[Cell[Position1D]] =>
          cells.domain()
      } Then {
        buffer: mutable.Buffer[Position1D] =>
          buffer.toList shouldBe List(Position1D(1), Position1D(2), Position1D(3))
      }
    }

    "return its domain in 2D" in {
      Given {
        List(Cell(Position2D(1, 3), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
          Cell(Position2D(2, 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
          Cell(Position2D(3, 1), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)))
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.domain()
      } Then {
        buffer: mutable.Buffer[Position2D] =>
          buffer.toList shouldBe List(Position2D(1, 1), Position2D(1, 2), Position2D(1, 3), Position2D(2, 1),
            Position2D(2, 2), Position2D(2, 3), Position2D(3, 1), Position2D(3, 2), Position2D(3, 3))
      }
    }

    "return its domain in 3D" in {
      Given {
        List(Cell(Position3D(1, 1, 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
          Cell(Position3D(2, 2, 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
          Cell(Position3D(3, 3, 3), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
          Cell(Position3D(1, 2, 3), Content(ContinuousSchema[Codex.DoubleCodex](), 0)))
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.domain()
      } Then {
        buffer: mutable.Buffer[Position3D] =>
          val l = List(1, 2, 3)
          val i = for (a <- l; b <- l; c <- l) yield Iterable(Position3D(a, b, c))

          buffer.toList shouldBe i.toList.flatten
      }
    }

    "return its domain in 4D" in {
      Given {
        List(Cell(Position4D(1, 4, 2, 3), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
          Cell(Position4D(2, 3, 1, 4), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
          Cell(Position4D(3, 2, 4, 1), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
          Cell(Position4D(4, 1, 3, 2), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
          Cell(Position4D(1, 2, 3, 4), Content(ContinuousSchema[Codex.DoubleCodex](), 0)))
      } When {
        cells: TypedPipe[Cell[Position4D]] =>
          cells.domain()
      } Then {
        buffer: mutable.Buffer[Position4D] =>
          val l = List(1, 2, 3, 4)
          val i = for (a <- l; b <- l; c <- l; d <- l) yield Iterable(Position4D(a, b, c, d))

          buffer.toList shouldBe i.toList.flatten
      }
    }

    "return its domain in 5D" in {
      Given {
        List(Cell(Position5D(1, 5, 4, 3, 2), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
          Cell(Position5D(2, 1, 5, 4, 3), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
          Cell(Position5D(3, 2, 1, 5, 4), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
          Cell(Position5D(4, 3, 2, 1, 5), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
          Cell(Position5D(5, 4, 3, 2, 1), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
          Cell(Position5D(1, 2, 3, 4, 5), Content(ContinuousSchema[Codex.DoubleCodex](), 0)))
      } When {
        cells: TypedPipe[Cell[Position5D]] =>
          cells.domain()
      } Then {
        buffer: mutable.Buffer[Position5D] =>
          val l = List(1, 2, 3, 4, 5)
          val i = for (a <- l; b <- l; c <- l; d <- l; e <- l) yield Iterable(Position5D(a, b, c, d, e))

          buffer.toList shouldBe i.toList.flatten
      }
    }
  }
}

class TestMatrixJoin extends WordSpec with Matchers with TBddDsl with TestMatrix {

  "A Matrix.join" should {
    "return its first over join in 2D" in {
      Given {
        data2
      } And {
        List(Cell(Position2D("bar", 5), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
          Cell(Position2D("baz", 5), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
          Cell(Position2D("qux", 5), Content(OrdinalSchema[Codex.StringCodex](), "12.56")),
          Cell(Position2D("bar", 6), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
          Cell(Position2D("baz", 6), Content(DiscreteSchema[Codex.LongCodex](), 19)),
          Cell(Position2D("bar", 7), Content(OrdinalSchema[Codex.LongCodex](), 19)))
      } When {
        (cells: TypedPipe[Cell[Position2D]], that: TypedPipe[Cell[Position2D]]) =>
          cells.join(Over(First), that)
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("bar", 1), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position2D("bar", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position2D("bar", 3), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position2D("bar", 5), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position2D("bar", 6), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position2D("bar", 7), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position2D("baz", 1), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position2D("baz", 2), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position2D("baz", 5), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position2D("baz", 6), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position2D("qux", 1), Content(OrdinalSchema[Codex.StringCodex](), "12.56")),
            Cell(Position2D("qux", 5), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))
      }
    }

    "return its first along join in 2D" in {
      Given {
        data2
      } And {
        List(Cell(Position2D("foo.2", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
          Cell(Position2D("bar.2", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
          Cell(Position2D("baz.2", 2), Content(DiscreteSchema[Codex.LongCodex](), 19)),
          Cell(Position2D("foo.2", 3), Content(NominalSchema[Codex.StringCodex](), "9.42")),
          Cell(Position2D("bar.2", 3), Content(OrdinalSchema[Codex.LongCodex](), 19)))
      } When {
        (cells: TypedPipe[Cell[Position2D]], that: TypedPipe[Cell[Position2D]]) =>
          that.join(Along(First), cells)
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("bar", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position2D("bar", 3), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position2D("bar.2", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position2D("bar.2", 3), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position2D("baz", 2), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position2D("baz.2", 2), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position2D("foo", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position2D("foo", 3), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position2D("foo.2", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position2D("foo.2", 3), Content(NominalSchema[Codex.StringCodex](), "9.42")))
      }
    }

    "return its second over join in 2D" in {
      Given {
        data2
      } And {
        List(Cell(Position2D("foo.2", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
          Cell(Position2D("bar.2", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
          Cell(Position2D("baz.2", 2), Content(DiscreteSchema[Codex.LongCodex](), 19)),
          Cell(Position2D("foo.2", 3), Content(NominalSchema[Codex.StringCodex](), "9.42")),
          Cell(Position2D("bar.2", 3), Content(OrdinalSchema[Codex.LongCodex](), 19)))
      } When {
        (cells: TypedPipe[Cell[Position2D]], that: TypedPipe[Cell[Position2D]]) =>
          that.join(Over(Second), cells)
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("bar", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position2D("bar", 3), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position2D("bar.2", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position2D("bar.2", 3), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position2D("baz", 2), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position2D("baz.2", 2), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position2D("foo", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position2D("foo", 3), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position2D("foo.2", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position2D("foo.2", 3), Content(NominalSchema[Codex.StringCodex](), "9.42")))
      }
    }

    "return its second along join in 2D" in {
      Given {
        data2
      } And {
        List(Cell(Position2D("bar", 5), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
          Cell(Position2D("baz", 5), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
          Cell(Position2D("qux", 5), Content(OrdinalSchema[Codex.StringCodex](), "12.56")),
          Cell(Position2D("bar", 6), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
          Cell(Position2D("baz", 6), Content(DiscreteSchema[Codex.LongCodex](), 19)),
          Cell(Position2D("bar", 7), Content(OrdinalSchema[Codex.LongCodex](), 19)))
      } When {
        (cells: TypedPipe[Cell[Position2D]], that: TypedPipe[Cell[Position2D]]) =>
          cells.join(Along(Second), that)
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("bar", 1), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position2D("bar", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position2D("bar", 3), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position2D("bar", 5), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position2D("bar", 6), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position2D("bar", 7), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position2D("baz", 1), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position2D("baz", 2), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position2D("baz", 5), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position2D("baz", 6), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position2D("qux", 1), Content(OrdinalSchema[Codex.StringCodex](), "12.56")),
            Cell(Position2D("qux", 5), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))
      }
    }

    "return its first over join in 3D" in {
      Given {
        data3
      } And {
        List(Cell(Position3D("bar", 5, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
          Cell(Position3D("baz", 5, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
          Cell(Position3D("qux", 5, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")),
          Cell(Position3D("bar", 6, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
          Cell(Position3D("baz", 6, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
          Cell(Position3D("bar", 7, "xyz"), Content(OrdinalSchema[Codex.LongCodex](), 19)))
      } When {
        (cells: TypedPipe[Cell[Position3D]], that: TypedPipe[Cell[Position3D]]) =>
          cells.join(Over(First), that)
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position3D("bar", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position3D("bar", 3, "xyz"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position3D("bar", 5, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position3D("bar", 6, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position3D("bar", 7, "xyz"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position3D("baz", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position3D("baz", 2, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position3D("baz", 5, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position3D("baz", 6, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position3D("qux", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")),
            Cell(Position3D("qux", 5, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))
      }
    }

    "return its first along join in 3D" in {
      Given {
        data3
      } And {
        List(Cell(Position3D("foo.2", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
          Cell(Position3D("bar.2", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
          Cell(Position3D("baz.2", 2, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
          Cell(Position3D("foo.2", 3, "xyz"), Content(NominalSchema[Codex.StringCodex](), "9.42")),
          Cell(Position3D("bar.2", 3, "xyz"), Content(OrdinalSchema[Codex.LongCodex](), 19)))
      } When {
        (cells: TypedPipe[Cell[Position3D]], that: TypedPipe[Cell[Position3D]]) =>
          that.join(Along(First), cells)
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position3D("bar", 3, "xyz"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position3D("bar.2", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position3D("bar.2", 3, "xyz"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position3D("baz", 2, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position3D("baz.2", 2, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position3D("foo", 3, "xyz"), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position3D("foo.2", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position3D("foo.2", 3, "xyz"), Content(NominalSchema[Codex.StringCodex](), "9.42")))
      }
    }

    "return its second over join in 3D" in {
      Given {
        data3
      } And {
        List(Cell(Position3D("foo.2", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
          Cell(Position3D("bar.2", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
          Cell(Position3D("baz.2", 2, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
          Cell(Position3D("foo.2", 3, "xyz"), Content(NominalSchema[Codex.StringCodex](), "9.42")),
          Cell(Position3D("bar.2", 3, "xyz"), Content(OrdinalSchema[Codex.LongCodex](), 19)))
      } When {
        (cells: TypedPipe[Cell[Position3D]], that: TypedPipe[Cell[Position3D]]) =>
          that.join(Over(Second), cells)
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position3D("bar", 3, "xyz"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position3D("bar.2", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position3D("bar.2", 3, "xyz"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position3D("baz", 2, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position3D("baz.2", 2, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position3D("foo", 3, "xyz"), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position3D("foo.2", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position3D("foo.2", 3, "xyz"), Content(NominalSchema[Codex.StringCodex](), "9.42")))
      }
    }

    "return its second along join in 3D" in {
      Given {
        data3
      } And {
        List(Cell(Position3D("bar", 5, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
          Cell(Position3D("baz", 5, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
          Cell(Position3D("qux", 5, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")),
          Cell(Position3D("bar", 6, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
          Cell(Position3D("baz", 6, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
          Cell(Position3D("bar", 7, "xyz"), Content(OrdinalSchema[Codex.LongCodex](), 19)))
      } When {
        (cells: TypedPipe[Cell[Position3D]], that: TypedPipe[Cell[Position3D]]) =>
          cells.join(Along(Second), that)
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position3D("bar", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position3D("bar", 3, "xyz"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position3D("bar", 5, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position3D("bar", 6, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position3D("bar", 7, "xyz"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position3D("baz", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position3D("baz", 2, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position3D("baz", 5, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position3D("baz", 6, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position3D("qux", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")),
            Cell(Position3D("qux", 5, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))
      }
    }

    "return its third over join in 3D" in {
      Given {
        data3
      } And {
        List(Cell(Position3D("foo.2", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
          Cell(Position3D("bar.2", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
          Cell(Position3D("baz.2", 2, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
          Cell(Position3D("foo.2", 3, "xyz"), Content(NominalSchema[Codex.StringCodex](), "9.42")),
          Cell(Position3D("bar.2", 3, "xyz"), Content(OrdinalSchema[Codex.LongCodex](), 19)))
      } When {
        (cells: TypedPipe[Cell[Position3D]], that: TypedPipe[Cell[Position3D]]) =>
          that.join(Over(Third), cells)
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position3D("bar", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position3D("bar", 3, "xyz"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position3D("bar.2", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position3D("bar.2", 3, "xyz"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position3D("baz", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position3D("baz", 2, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position3D("baz.2", 2, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position3D("foo", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
            Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position3D("foo", 3, "xyz"), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position3D("foo", 4, "xyz"), Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
            Cell(Position3D("foo.2", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position3D("foo.2", 3, "xyz"), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position3D("qux", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))
      }
    }

    "return its third along join in 3D" in {
      Given {
        data3
      } And {
        List(Cell(Position3D("bar", 1, "xyz.2"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
          Cell(Position3D("baz", 1, "xyz.2"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
          Cell(Position3D("qux", 1, "xyz.2"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")),
          Cell(Position3D("bar", 2, "xyz.2"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
          Cell(Position3D("baz", 2, "xyz.2"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
          Cell(Position3D("bar", 3, "xyz.2"), Content(OrdinalSchema[Codex.LongCodex](), 19)))
      } When {
        (cells: TypedPipe[Cell[Position3D]], that: TypedPipe[Cell[Position3D]]) =>
          cells.join(Along(Third), that)
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position3D("bar", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position3D("bar", 1, "xyz.2"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position3D("bar", 2, "xyz.2"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position3D("bar", 3, "xyz"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position3D("bar", 3, "xyz.2"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position3D("baz", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position3D("baz", 1, "xyz.2"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position3D("baz", 2, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position3D("baz", 2, "xyz.2"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position3D("qux", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")),
            Cell(Position3D("qux", 1, "xyz.2"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))
      }
    }
  }
}

class TestMatrixUnique extends WordSpec with Matchers with TBddDsl with TestMatrix {

  "A Matrix.unique" should {
    "return its content in 1D" in {
      Given {
        data1
      } When {
        cells: TypedPipe[Cell[Position1D]] =>
          cells.unique()
      } Then {
        buffer: mutable.Buffer[Content] =>
          buffer.toList shouldBe List(Content(OrdinalSchema[Codex.StringCodex](), "12.56"),
            Content(OrdinalSchema[Codex.StringCodex](), "3.14"),
            Content(OrdinalSchema[Codex.StringCodex](), "6.28"),
            Content(OrdinalSchema[Codex.StringCodex](), "9.42"))
      }
    }

    "return its first over content in 1D" in {
      Given {
        data1
      } When {
        cells: TypedPipe[Cell[Position1D]] =>
          cells.unique(Over(First))
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList shouldBe List(Cell(Position1D("bar"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position1D("baz"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position1D("foo"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
            Cell(Position1D("qux"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))
      }
    }

    "return its content in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.unique()
      } Then {
        buffer: mutable.Buffer[Content] =>
          buffer.toList shouldBe List(Content(ContinuousSchema[Codex.DoubleCodex](), 12.56),
            Content(ContinuousSchema[Codex.DoubleCodex](), 6.28),
            Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00")),
            Content(DiscreteSchema[Codex.LongCodex](), 19),
            Content(NominalSchema[Codex.StringCodex](), "9.42"),
            Content(OrdinalSchema[Codex.LongCodex](), 19),
            Content(OrdinalSchema[Codex.StringCodex](), "12.56"),
            Content(OrdinalSchema[Codex.StringCodex](), "3.14"),
            Content(OrdinalSchema[Codex.StringCodex](), "6.28"),
            Content(OrdinalSchema[Codex.StringCodex](), "9.42"))
      }
    }

    "return its first over content in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.unique(Over(First))
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList shouldBe List(Cell(Position1D("bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position1D("bar"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position1D("bar"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position1D("baz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position1D("baz"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position1D("foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position1D("foo"), Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
            Cell(Position1D("foo"), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position1D("foo"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
            Cell(Position1D("qux"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))
      }
    }

    "return its first along content in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.unique(Along(First))
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList shouldBe List(Cell(Position1D(1), Content(OrdinalSchema[Codex.StringCodex](), "12.56")),
            Cell(Position1D(1), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
            Cell(Position1D(1), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position1D(1), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position1D(2), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position1D(2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position1D(2), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position1D(3), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position1D(3), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position1D(4), Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))
      }
    }

    "return its second over content in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.unique(Over(Second))
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList shouldBe List(Cell(Position1D(1), Content(OrdinalSchema[Codex.StringCodex](), "12.56")),
            Cell(Position1D(1), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
            Cell(Position1D(1), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position1D(1), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position1D(2), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position1D(2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position1D(2), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position1D(3), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position1D(3), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position1D(4), Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))
      }
    }

    "return its second along content in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.unique(Along(Second))
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList shouldBe List(Cell(Position1D("bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position1D("bar"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position1D("bar"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position1D("baz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position1D("baz"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position1D("foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position1D("foo"), Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
            Cell(Position1D("foo"), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position1D("foo"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
            Cell(Position1D("qux"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))
      }
    }

    "return its content in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.unique()
      } Then {
        buffer: mutable.Buffer[Content] =>
          buffer.toList shouldBe List(Content(ContinuousSchema[Codex.DoubleCodex](), 12.56),
            Content(ContinuousSchema[Codex.DoubleCodex](), 6.28),
            Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00")),
            Content(DiscreteSchema[Codex.LongCodex](), 19),
            Content(NominalSchema[Codex.StringCodex](), "9.42"),
            Content(OrdinalSchema[Codex.LongCodex](), 19),
            Content(OrdinalSchema[Codex.StringCodex](), "12.56"),
            Content(OrdinalSchema[Codex.StringCodex](), "3.14"),
            Content(OrdinalSchema[Codex.StringCodex](), "6.28"),
            Content(OrdinalSchema[Codex.StringCodex](), "9.42"))
      }
    }

    "return its first over content in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.unique(Over(First))
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList shouldBe List(Cell(Position1D("bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position1D("bar"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position1D("bar"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position1D("baz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position1D("baz"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position1D("foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position1D("foo"), Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
            Cell(Position1D("foo"), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position1D("foo"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
            Cell(Position1D("qux"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))
      }
    }

    "return its first along content in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.unique(Along(First))
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList shouldBe List(Cell(Position2D(1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")),
            Cell(Position2D(1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
            Cell(Position2D(1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position2D(1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position2D(2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position2D(2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position2D(2, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position2D(3, "xyz"), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position2D(3, "xyz"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position2D(4, "xyz"), Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))
      }
    }

    "return its second over content in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.unique(Over(Second))
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList shouldBe List(Cell(Position1D(1), Content(OrdinalSchema[Codex.StringCodex](), "12.56")),
            Cell(Position1D(1), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
            Cell(Position1D(1), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position1D(1), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position1D(2), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position1D(2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position1D(2), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position1D(3), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position1D(3), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position1D(4), Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))
      }
    }

    "return its second along content in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.unique(Along(Second))
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList shouldBe List(
            Cell(Position2D("bar", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position2D("bar", "xyz"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position2D("bar", "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position2D("baz", "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position2D("baz", "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position2D("foo", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position2D("foo", "xyz"), Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
            Cell(Position2D("foo", "xyz"), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position2D("foo", "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
            Cell(Position2D("qux", "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))
      }
    }

    "return its third over content in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.unique(Over(Third))
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList shouldBe List(Cell(Position1D("xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position1D("xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position1D("xyz"), Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
            Cell(Position1D("xyz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position1D("xyz"), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position1D("xyz"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position1D("xyz"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")),
            Cell(Position1D("xyz"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
            Cell(Position1D("xyz"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position1D("xyz"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")))
      }
    }

    "return its third along content in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.unique(Along(Third))
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList shouldBe List(Cell(Position2D("bar", 1), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position2D("bar", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position2D("bar", 3), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position2D("baz", 1), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position2D("baz", 2), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position2D("foo", 1), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
            Cell(Position2D("foo", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position2D("foo", 3), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position2D("foo", 4), Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
            Cell(Position2D("qux", 1), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))
      }
    }
  }
}

class TestMatrixPairwise extends WordSpec with Matchers with TBddDsl with TestMatrix {

  case class PlusX() extends Operator with ComputeWithValue {
    type V = Double

    val plus = Plus()

    def compute[P <: Position, D <: Dimension](slice: Slice[P, D], ext: V)(left: Cell[slice.S], right: Cell[slice.S],
      rem: slice.R): Collection[Cell[slice.R#M]] = {
      Collection(plus.compute(slice)(left, right, rem).toList.map {
        case Cell(pos, Content(_, DoubleValue(d))) => Cell(pos, Content(ContinuousSchema[Codex.DoubleCodex](), d + ext)) 
      })
    }
  }

  case class MinusX() extends Operator with ComputeWithValue {
    type V = Double

    val minus = Minus()

    def compute[P <: Position, D <: Dimension](slice: Slice[P, D], ext: V)(left: Cell[slice.S], right: Cell[slice.S],
      rem: slice.R): Collection[Cell[slice.R#M]] = {
      Collection(minus.compute(slice)(left, right, rem).toList.map {
        case Cell(pos, Content(_, DoubleValue(d))) => Cell(pos, Content(ContinuousSchema[Codex.DoubleCodex](), d - ext)) 
      })
    }
  }

  val ext = ValuePipe(1.0)

  "A Matrix.pairwise" should {
    "return its first over pairwise in 1D" in {
      Given {
        num1
      } When {
        cells: TypedPipe[Cell[Position1D]] =>
          cells.pairwise(Over(First), Plus())
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position1D("(baz+bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 6.28)),
            Cell(Position1D("(foo+bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 6.28)),
            Cell(Position1D("(foo+baz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 9.42)),
            Cell(Position1D("(qux+bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 6.28)),
            Cell(Position1D("(qux+baz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 9.42)),
            Cell(Position1D("(qux+foo)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 3.14)))
      }
    }

    "return its first over pairwise in 2D" in {
      Given {
        num2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.pairwise(Over(First), Plus())
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("(baz+bar)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 6.28)),
            Cell(Position2D("(baz+bar)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 12.56)),
            Cell(Position2D("(foo+bar)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 6.28)),
            Cell(Position2D("(foo+bar)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 12.56)),
            Cell(Position2D("(foo+bar)", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 18.84)),
            Cell(Position2D("(foo+baz)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 9.42)),
            Cell(Position2D("(foo+baz)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 18.84)),
            Cell(Position2D("(qux+bar)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 6.28)),
            Cell(Position2D("(qux+baz)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 9.42)),
            Cell(Position2D("(qux+foo)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 3.14)))
      }
    }

    "return its first along pairwise in 2D" in {
      Given {
        num2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.pairwise(Along(First), List(Plus(), Minus()))
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("(2+1)", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 6.28)),
            Cell(Position2D("(2+1)", "baz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 9.42)),
            Cell(Position2D("(2+1)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 3.14)),
            Cell(Position2D("(2-1)", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 6.28)),
            Cell(Position2D("(2-1)", "baz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 9.42)),
            Cell(Position2D("(2-1)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 - 3.14)),
            Cell(Position2D("(3+1)", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 6.28)),
            Cell(Position2D("(3+1)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 3.14)),
            Cell(Position2D("(3+2)", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 12.56)),
            Cell(Position2D("(3+2)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 6.28)),
            Cell(Position2D("(3-1)", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 6.28)),
            Cell(Position2D("(3-1)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 3.14)),
            Cell(Position2D("(3-2)", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 12.56)),
            Cell(Position2D("(3-2)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 6.28)),
            Cell(Position2D("(4+1)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 3.14)),
            Cell(Position2D("(4+2)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 6.28)),
            Cell(Position2D("(4+3)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 9.42)),
            Cell(Position2D("(4-1)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 3.14)),
            Cell(Position2D("(4-2)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 6.28)),
            Cell(Position2D("(4-3)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 9.42)))
      }
    }

    "return its second over pairwise in 2D" in {
      Given {
        num2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.pairwise(Over(Second), List(Plus(), Minus()))
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("(2+1)", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 6.28)),
            Cell(Position2D("(2+1)", "baz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 9.42)),
            Cell(Position2D("(2+1)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 3.14)),
            Cell(Position2D("(2-1)", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 6.28)),
            Cell(Position2D("(2-1)", "baz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 9.42)),
            Cell(Position2D("(2-1)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 - 3.14)),
            Cell(Position2D("(3+1)", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 6.28)),
            Cell(Position2D("(3+1)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 3.14)),
            Cell(Position2D("(3+2)", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 12.56)),
            Cell(Position2D("(3+2)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 6.28)),
            Cell(Position2D("(3-1)", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 6.28)),
            Cell(Position2D("(3-1)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 3.14)),
            Cell(Position2D("(3-2)", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 12.56)),
            Cell(Position2D("(3-2)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 6.28)),
            Cell(Position2D("(4+1)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 3.14)),
            Cell(Position2D("(4+2)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 6.28)),
            Cell(Position2D("(4+3)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 9.42)),
            Cell(Position2D("(4-1)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 3.14)),
            Cell(Position2D("(4-2)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 6.28)),
            Cell(Position2D("(4-3)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 9.42)))
      }
    }

    "return its second along pairwise in 2D" in {
      Given {
        num2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.pairwise(Along(Second), Plus())
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("(baz+bar)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 6.28)),
            Cell(Position2D("(baz+bar)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 12.56)),
            Cell(Position2D("(foo+bar)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 6.28)),
            Cell(Position2D("(foo+bar)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 12.56)),
            Cell(Position2D("(foo+bar)", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 18.84)),
            Cell(Position2D("(foo+baz)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 9.42)),
            Cell(Position2D("(foo+baz)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 18.84)),
            Cell(Position2D("(qux+bar)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 6.28)),
            Cell(Position2D("(qux+baz)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 9.42)),
            Cell(Position2D("(qux+foo)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 3.14)))
      }
    }

    "return its first over pairwise in 3D" in {
      Given {
        num3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.pairwise(Over(First), Plus())
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position3D("(baz+bar)", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 6.28)),
            Cell(Position3D("(baz+bar)", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 12.56)),
            Cell(Position3D("(foo+bar)", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 6.28)),
            Cell(Position3D("(foo+bar)", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 12.56)),
            Cell(Position3D("(foo+bar)", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 18.84)),
            Cell(Position3D("(foo+baz)", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 9.42)),
            Cell(Position3D("(foo+baz)", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 18.84)),
            Cell(Position3D("(qux+bar)", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 6.28)),
            Cell(Position3D("(qux+baz)", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 9.42)),
            Cell(Position3D("(qux+foo)", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 3.14)))
      }
    }

    "return its first along pairwise in 3D" in {
      Given {
        num3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.pairwise(Along(First), List(Plus(), Minus()))
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("(2|xyz+1|xyz)", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 6.28)),
            Cell(Position2D("(2|xyz+1|xyz)", "baz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 9.42)),
            Cell(Position2D("(2|xyz+1|xyz)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 3.14)),
            Cell(Position2D("(2|xyz-1|xyz)", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 6.28)),
            Cell(Position2D("(2|xyz-1|xyz)", "baz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 9.42)),
            Cell(Position2D("(2|xyz-1|xyz)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 - 3.14)),
            Cell(Position2D("(3|xyz+1|xyz)", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 6.28)),
            Cell(Position2D("(3|xyz+1|xyz)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 3.14)),
            Cell(Position2D("(3|xyz+2|xyz)", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 12.56)),
            Cell(Position2D("(3|xyz+2|xyz)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 6.28)),
            Cell(Position2D("(3|xyz-1|xyz)", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 6.28)),
            Cell(Position2D("(3|xyz-1|xyz)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 3.14)),
            Cell(Position2D("(3|xyz-2|xyz)", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 12.56)),
            Cell(Position2D("(3|xyz-2|xyz)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 6.28)),
            Cell(Position2D("(4|xyz+1|xyz)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 3.14)),
            Cell(Position2D("(4|xyz+2|xyz)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 6.28)),
            Cell(Position2D("(4|xyz+3|xyz)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 9.42)),
            Cell(Position2D("(4|xyz-1|xyz)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 3.14)),
            Cell(Position2D("(4|xyz-2|xyz)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 6.28)),
            Cell(Position2D("(4|xyz-3|xyz)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 9.42)))
      }
    }

    "return its second over pairwise in 3D" in {
      Given {
        num3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.pairwise(Over(Second), List(Plus(), Minus()))
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position3D("(2+1)", "bar", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 6.28)),
            Cell(Position3D("(2+1)", "baz", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 9.42)),
            Cell(Position3D("(2+1)", "foo", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 3.14)),
            Cell(Position3D("(2-1)", "bar", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 6.28)),
            Cell(Position3D("(2-1)", "baz", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 9.42)),
            Cell(Position3D("(2-1)", "foo", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 - 3.14)),
            Cell(Position3D("(3+1)", "bar", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 6.28)),
            Cell(Position3D("(3+1)", "foo", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 3.14)),
            Cell(Position3D("(3+2)", "bar", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 12.56)),
            Cell(Position3D("(3+2)", "foo", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 6.28)),
            Cell(Position3D("(3-1)", "bar", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 6.28)),
            Cell(Position3D("(3-1)", "foo", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 3.14)),
            Cell(Position3D("(3-2)", "bar", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 12.56)),
            Cell(Position3D("(3-2)", "foo", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 6.28)),
            Cell(Position3D("(4+1)", "foo", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 3.14)),
            Cell(Position3D("(4+2)", "foo", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 6.28)),
            Cell(Position3D("(4+3)", "foo", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 9.42)),
            Cell(Position3D("(4-1)", "foo", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 3.14)),
            Cell(Position3D("(4-2)", "foo", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 6.28)),
            Cell(Position3D("(4-3)", "foo", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 9.42)))
      }
    }

    "return its second along pairwise in 3D" in {
      Given {
        num3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.pairwise(Along(Second), Plus())
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("(baz|xyz+bar|xyz)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 6.28)),
            Cell(Position2D("(baz|xyz+bar|xyz)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 12.56)),
            Cell(Position2D("(foo|xyz+bar|xyz)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 6.28)),
            Cell(Position2D("(foo|xyz+bar|xyz)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 12.56)),
            Cell(Position2D("(foo|xyz+bar|xyz)", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 18.84)),
            Cell(Position2D("(foo|xyz+baz|xyz)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 9.42)),
            Cell(Position2D("(foo|xyz+baz|xyz)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 18.84)),
            Cell(Position2D("(qux|xyz+bar|xyz)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 6.28)),
            Cell(Position2D("(qux|xyz+baz|xyz)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 9.42)),
            Cell(Position2D("(qux|xyz+foo|xyz)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 3.14)))
      }
    }

    "return its third over pairwise in 3D" in {
      Given {
        num3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.pairwise(Over(Third), List(Plus(), Minus()))
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList.sortBy(_.position) shouldBe List()
      }
    }

    "return its third along pairwise in 3D" in {
      Given {
        num3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.pairwise(Along(Third), Plus())
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("(bar|2+bar|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 6.28)),
            Cell(Position2D("(bar|3+bar|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 6.28)),
            Cell(Position2D("(bar|3+bar|2)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 12.56)),
            Cell(Position2D("(baz|1+bar|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 6.28)),
            Cell(Position2D("(baz|1+bar|2)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 12.56)),
            Cell(Position2D("(baz|1+bar|3)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 18.84)),
            Cell(Position2D("(baz|2+bar|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 6.28)),
            Cell(Position2D("(baz|2+bar|2)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 12.56)),
            Cell(Position2D("(baz|2+bar|3)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 18.84)),
            Cell(Position2D("(baz|2+baz|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 9.42)),
            Cell(Position2D("(foo|1+bar|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 6.28)),
            Cell(Position2D("(foo|1+bar|2)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 12.56)),
            Cell(Position2D("(foo|1+bar|3)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 18.84)),
            Cell(Position2D("(foo|1+baz|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 9.42)),
            Cell(Position2D("(foo|1+baz|2)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 18.84)),
            Cell(Position2D("(foo|2+bar|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 6.28)),
            Cell(Position2D("(foo|2+bar|2)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 12.56)),
            Cell(Position2D("(foo|2+bar|3)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 18.84)),
            Cell(Position2D("(foo|2+baz|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 9.42)),
            Cell(Position2D("(foo|2+baz|2)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 18.84)),
            Cell(Position2D("(foo|2+foo|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 3.14)),
            Cell(Position2D("(foo|3+bar|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 6.28)),
            Cell(Position2D("(foo|3+bar|2)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 12.56)),
            Cell(Position2D("(foo|3+bar|3)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 18.84)),
            Cell(Position2D("(foo|3+baz|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 9.42)),
            Cell(Position2D("(foo|3+baz|2)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 18.84)),
            Cell(Position2D("(foo|3+foo|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 3.14)),
            Cell(Position2D("(foo|3+foo|2)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 6.28)),
            Cell(Position2D("(foo|4+bar|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 6.28)),
            Cell(Position2D("(foo|4+bar|2)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 12.56)),
            Cell(Position2D("(foo|4+bar|3)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 18.84)),
            Cell(Position2D("(foo|4+baz|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 9.42)),
            Cell(Position2D("(foo|4+baz|2)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 18.84)),
            Cell(Position2D("(foo|4+foo|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 3.14)),
            Cell(Position2D("(foo|4+foo|2)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 6.28)),
            Cell(Position2D("(foo|4+foo|3)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 9.42)),
            Cell(Position2D("(qux|1+bar|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 6.28)),
            Cell(Position2D("(qux|1+bar|2)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 12.56)),
            Cell(Position2D("(qux|1+bar|3)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 18.84)),
            Cell(Position2D("(qux|1+baz|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 9.42)),
            Cell(Position2D("(qux|1+baz|2)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 18.84)),
            Cell(Position2D("(qux|1+foo|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 3.14)),
            Cell(Position2D("(qux|1+foo|2)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 6.28)),
            Cell(Position2D("(qux|1+foo|3)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 9.42)),
            Cell(Position2D("(qux|1+foo|4)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 12.56)))
      }
    }
  }

  "A Matrix.pairwiseWithValue" should {
    "return its first over pairwise in 1D" in {
      Given {
        num1
      } When {
        cells: TypedPipe[Cell[Position1D]] =>
          cells.pairwiseWithValue(Over(First), PlusX(), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position1D("(baz+bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 6.28 + 1)),
            Cell(Position1D("(foo+bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 6.28 + 1)),
            Cell(Position1D("(foo+baz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 9.42 + 1)),
            Cell(Position1D("(qux+bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 6.28 + 1)),
            Cell(Position1D("(qux+baz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 9.42 + 1)),
            Cell(Position1D("(qux+foo)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 3.14 + 1)))
      }
    }

    "return its first over pairwise in 2D" in {
      Given {
        num2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.pairwiseWithValue(Over(First), PlusX(), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("(baz+bar)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 6.28 + 1)),
            Cell(Position2D("(baz+bar)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 12.56 + 1)),
            Cell(Position2D("(foo+bar)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 6.28 + 1)),
            Cell(Position2D("(foo+bar)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 12.56 + 1)),
            Cell(Position2D("(foo+bar)", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 18.84 + 1)),
            Cell(Position2D("(foo+baz)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 9.42 + 1)),
            Cell(Position2D("(foo+baz)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 18.84 + 1)),
            Cell(Position2D("(qux+bar)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 6.28 + 1)),
            Cell(Position2D("(qux+baz)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 9.42 + 1)),
            Cell(Position2D("(qux+foo)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 3.14 + 1)))
      }
    }

    "return its first along pairwise in 2D" in {
      Given {
        num2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.pairwiseWithValue(Along(First), List(PlusX(), MinusX()), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("(2+1)", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 6.28 + 1)),
            Cell(Position2D("(2+1)", "baz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 9.42 + 1)),
            Cell(Position2D("(2+1)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 3.14 + 1)),
            Cell(Position2D("(2-1)", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 6.28 - 1)),
            Cell(Position2D("(2-1)", "baz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 9.42 - 1)),
            Cell(Position2D("(2-1)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 - 3.14 - 1)),
            Cell(Position2D("(3+1)", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 6.28 + 1)),
            Cell(Position2D("(3+1)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 3.14 + 1)),
            Cell(Position2D("(3+2)", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 12.56 + 1)),
            Cell(Position2D("(3+2)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 6.28 + 1)),
            Cell(Position2D("(3-1)", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 6.28 - 1)),
            Cell(Position2D("(3-1)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 3.14 - 1)),
            Cell(Position2D("(3-2)", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 12.56 - 1)),
            Cell(Position2D("(3-2)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 6.28 - 1)),
            Cell(Position2D("(4+1)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 3.14 + 1)),
            Cell(Position2D("(4+2)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 6.28 + 1)),
            Cell(Position2D("(4+3)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 9.42 + 1)),
            Cell(Position2D("(4-1)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 3.14 - 1)),
            Cell(Position2D("(4-2)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 6.28 - 1)),
            Cell(Position2D("(4-3)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 9.42 - 1)))
      }
    }

    "return its second over pairwise in 2D" in {
      Given {
        num2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.pairwiseWithValue(Over(Second), List(PlusX(), MinusX()), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("(2+1)", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 6.28 + 1)),
            Cell(Position2D("(2+1)", "baz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 9.42 + 1)),
            Cell(Position2D("(2+1)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 3.14 + 1)),
            Cell(Position2D("(2-1)", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 6.28 - 1)),
            Cell(Position2D("(2-1)", "baz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 9.42 - 1)),
            Cell(Position2D("(2-1)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 - 3.14 - 1)),
            Cell(Position2D("(3+1)", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 6.28 + 1)),
            Cell(Position2D("(3+1)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 3.14 + 1)),
            Cell(Position2D("(3+2)", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 12.56 + 1)),
            Cell(Position2D("(3+2)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 6.28 + 1)),
            Cell(Position2D("(3-1)", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 6.28 - 1)),
            Cell(Position2D("(3-1)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 3.14 - 1)),
            Cell(Position2D("(3-2)", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 12.56 - 1)),
            Cell(Position2D("(3-2)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 6.28 - 1)),
            Cell(Position2D("(4+1)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 3.14 + 1)),
            Cell(Position2D("(4+2)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 6.28 + 1)),
            Cell(Position2D("(4+3)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 9.42 + 1)),
            Cell(Position2D("(4-1)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 3.14 - 1)),
            Cell(Position2D("(4-2)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 6.28 - 1)),
            Cell(Position2D("(4-3)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 9.42 - 1)))
      }
    }

    "return its second along pairwise in 2D" in {
      Given {
        num2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.pairwiseWithValue(Along(Second), PlusX(), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("(baz+bar)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 6.28 + 1)),
            Cell(Position2D("(baz+bar)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 12.56 + 1)),
            Cell(Position2D("(foo+bar)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 6.28 + 1)),
            Cell(Position2D("(foo+bar)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 12.56 + 1)),
            Cell(Position2D("(foo+bar)", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 18.84 + 1)),
            Cell(Position2D("(foo+baz)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 9.42 + 1)),
            Cell(Position2D("(foo+baz)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 18.84 + 1)),
            Cell(Position2D("(qux+bar)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 6.28 + 1)),
            Cell(Position2D("(qux+baz)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 9.42 + 1)),
            Cell(Position2D("(qux+foo)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 3.14 + 1)))
      }
    }

    "return its first over pairwise in 3D" in {
      Given {
        num3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.pairwiseWithValue(Over(First), PlusX(), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position3D("(baz+bar)", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 6.28 + 1)),
            Cell(Position3D("(baz+bar)", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 12.56 + 1)),
            Cell(Position3D("(foo+bar)", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 6.28 + 1)),
            Cell(Position3D("(foo+bar)", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 12.56 + 1)),
            Cell(Position3D("(foo+bar)", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 18.84 + 1)),
            Cell(Position3D("(foo+baz)", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 9.42 + 1)),
            Cell(Position3D("(foo+baz)", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 18.84 + 1)),
            Cell(Position3D("(qux+bar)", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 6.28 + 1)),
            Cell(Position3D("(qux+baz)", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 9.42 + 1)),
            Cell(Position3D("(qux+foo)", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 3.14 + 1)))
      }
    }

    "return its first along pairwise in 3D" in {
      Given {
        num3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.pairwiseWithValue(Along(First), List(PlusX(), MinusX()), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("(2|xyz+1|xyz)", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 6.28 + 1)),
            Cell(Position2D("(2|xyz+1|xyz)", "baz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 9.42 + 1)),
            Cell(Position2D("(2|xyz+1|xyz)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 3.14 + 1)),
            Cell(Position2D("(2|xyz-1|xyz)", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 6.28 - 1)),
            Cell(Position2D("(2|xyz-1|xyz)", "baz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 9.42 - 1)),
            Cell(Position2D("(2|xyz-1|xyz)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 - 3.14 - 1)),
            Cell(Position2D("(3|xyz+1|xyz)", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 6.28 + 1)),
            Cell(Position2D("(3|xyz+1|xyz)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 3.14 + 1)),
            Cell(Position2D("(3|xyz+2|xyz)", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 12.56 + 1)),
            Cell(Position2D("(3|xyz+2|xyz)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 6.28 + 1)),
            Cell(Position2D("(3|xyz-1|xyz)", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 6.28 - 1)),
            Cell(Position2D("(3|xyz-1|xyz)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 3.14 - 1)),
            Cell(Position2D("(3|xyz-2|xyz)", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 12.56 - 1)),
            Cell(Position2D("(3|xyz-2|xyz)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 6.28 - 1)),
            Cell(Position2D("(4|xyz+1|xyz)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 3.14 + 1)),
            Cell(Position2D("(4|xyz+2|xyz)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 6.28 + 1)),
            Cell(Position2D("(4|xyz+3|xyz)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 9.42 + 1)),
            Cell(Position2D("(4|xyz-1|xyz)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 3.14 - 1)),
            Cell(Position2D("(4|xyz-2|xyz)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 6.28 - 1)),
            Cell(Position2D("(4|xyz-3|xyz)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 9.42 - 1)))
      }
    }

    "return its second over pairwise in 3D" in {
      Given {
        num3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.pairwiseWithValue(Over(Second), List(PlusX(), MinusX()), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position3D("(2+1)", "bar", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 6.28 + 1)),
            Cell(Position3D("(2+1)", "baz", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 9.42 + 1)),
            Cell(Position3D("(2+1)", "foo", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 3.14 + 1)),
            Cell(Position3D("(2-1)", "bar", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 6.28 - 1)),
            Cell(Position3D("(2-1)", "baz", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 9.42 - 1)),
            Cell(Position3D("(2-1)", "foo", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 - 3.14 - 1)),
            Cell(Position3D("(3+1)", "bar", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 6.28 + 1)),
            Cell(Position3D("(3+1)", "foo", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 3.14 + 1)),
            Cell(Position3D("(3+2)", "bar", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 12.56 + 1)),
            Cell(Position3D("(3+2)", "foo", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 6.28 + 1)),
            Cell(Position3D("(3-1)", "bar", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 6.28 - 1)),
            Cell(Position3D("(3-1)", "foo", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 3.14 - 1)),
            Cell(Position3D("(3-2)", "bar", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 12.56 - 1)),
            Cell(Position3D("(3-2)", "foo", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 6.28 - 1)),
            Cell(Position3D("(4+1)", "foo", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 3.14 + 1)),
            Cell(Position3D("(4+2)", "foo", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 6.28 + 1)),
            Cell(Position3D("(4+3)", "foo", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 9.42 + 1)),
            Cell(Position3D("(4-1)", "foo", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 3.14 - 1)),
            Cell(Position3D("(4-2)", "foo", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 6.28 - 1)),
            Cell(Position3D("(4-3)", "foo", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 9.42 - 1)))
      }
    }

    "return its second along pairwise in 3D" in {
      Given {
        num3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.pairwiseWithValue(Along(Second), PlusX(), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("(baz|xyz+bar|xyz)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 6.28 + 1)),
            Cell(Position2D("(baz|xyz+bar|xyz)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 12.56 + 1)),
            Cell(Position2D("(foo|xyz+bar|xyz)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 6.28 + 1)),
            Cell(Position2D("(foo|xyz+bar|xyz)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 12.56 + 1)),
            Cell(Position2D("(foo|xyz+bar|xyz)", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 18.84 + 1)),
            Cell(Position2D("(foo|xyz+baz|xyz)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 9.42 + 1)),
            Cell(Position2D("(foo|xyz+baz|xyz)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 18.84 + 1)),
            Cell(Position2D("(qux|xyz+bar|xyz)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 6.28 + 1)),
            Cell(Position2D("(qux|xyz+baz|xyz)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 9.42 + 1)),
            Cell(Position2D("(qux|xyz+foo|xyz)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 3.14 + 1)))
      }
    }

    "return its third over pairwise in 3D" in {
      Given {
        num3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.pairwiseWithValue(Over(Third), List(PlusX(), MinusX()), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList.sortBy(_.position) shouldBe List()
      }
    }

    "return its third along pairwise in 3D" in {
      Given {
        num3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.pairwiseWithValue(Along(Third), PlusX(), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("(bar|2+bar|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 6.28 + 1)),
            Cell(Position2D("(bar|3+bar|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 6.28 + 1)),
            Cell(Position2D("(bar|3+bar|2)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 12.56 + 1)),
            Cell(Position2D("(baz|1+bar|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 6.28 + 1)),
            Cell(Position2D("(baz|1+bar|2)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 12.56 + 1)),
            Cell(Position2D("(baz|1+bar|3)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 18.84 + 1)),
            Cell(Position2D("(baz|2+bar|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 6.28 + 1)),
            Cell(Position2D("(baz|2+bar|2)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 12.56 + 1)),
            Cell(Position2D("(baz|2+bar|3)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 18.84 + 1)),
            Cell(Position2D("(baz|2+baz|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 9.42 + 1)),
            Cell(Position2D("(foo|1+bar|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 6.28 + 1)),
            Cell(Position2D("(foo|1+bar|2)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 12.56 + 1)),
            Cell(Position2D("(foo|1+bar|3)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 18.84 + 1)),
            Cell(Position2D("(foo|1+baz|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 9.42 + 1)),
            Cell(Position2D("(foo|1+baz|2)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 18.84 + 1)),
            Cell(Position2D("(foo|2+bar|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 6.28 + 1)),
            Cell(Position2D("(foo|2+bar|2)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 12.56 + 1)),
            Cell(Position2D("(foo|2+bar|3)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 18.84 + 1)),
            Cell(Position2D("(foo|2+baz|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 9.42 + 1)),
            Cell(Position2D("(foo|2+baz|2)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 18.84 + 1)),
            Cell(Position2D("(foo|2+foo|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 3.14 + 1)),
            Cell(Position2D("(foo|3+bar|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 6.28 + 1)),
            Cell(Position2D("(foo|3+bar|2)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 12.56 + 1)),
            Cell(Position2D("(foo|3+bar|3)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 18.84 + 1)),
            Cell(Position2D("(foo|3+baz|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 9.42 + 1)),
            Cell(Position2D("(foo|3+baz|2)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 18.84 + 1)),
            Cell(Position2D("(foo|3+foo|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 3.14 + 1)),
            Cell(Position2D("(foo|3+foo|2)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 6.28 + 1)),
            Cell(Position2D("(foo|4+bar|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 6.28 + 1)),
            Cell(Position2D("(foo|4+bar|2)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 12.56 + 1)),
            Cell(Position2D("(foo|4+bar|3)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 18.84 + 1)),
            Cell(Position2D("(foo|4+baz|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 9.42 + 1)),
            Cell(Position2D("(foo|4+baz|2)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 18.84 + 1)),
            Cell(Position2D("(foo|4+foo|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 3.14 + 1)),
            Cell(Position2D("(foo|4+foo|2)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 6.28 + 1)),
            Cell(Position2D("(foo|4+foo|3)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 9.42 + 1)),
            Cell(Position2D("(qux|1+bar|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 6.28 + 1)),
            Cell(Position2D("(qux|1+bar|2)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 12.56 + 1)),
            Cell(Position2D("(qux|1+bar|3)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 18.84 + 1)),
            Cell(Position2D("(qux|1+baz|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 9.42 + 1)),
            Cell(Position2D("(qux|1+baz|2)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 18.84 + 1)),
            Cell(Position2D("(qux|1+foo|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 3.14 + 1)),
            Cell(Position2D("(qux|1+foo|2)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 6.28 + 1)),
            Cell(Position2D("(qux|1+foo|3)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 9.42 + 1)),
            Cell(Position2D("(qux|1+foo|4)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 12.56 + 1)))
      }
    }
  }

  "A Matrix.pairwiseBetween" should {
    "return its first over pairwise in 1D" in {
      Given {
        num1
      } And {
        List(Cell(Position1D("bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 1)),
          Cell(Position1D("baz"), Content(ContinuousSchema[Codex.DoubleCodex](), 2)))
      } When {
        (cells: TypedPipe[Cell[Position1D]], that: TypedPipe[Cell[Position1D]]) =>
          cells.pairwiseBetween(Over(First), that, Plus())
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position1D("(baz+bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 1)),
            Cell(Position1D("(foo+bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 1)),
            Cell(Position1D("(foo+baz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 2)),
            Cell(Position1D("(qux+bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 1)),
            Cell(Position1D("(qux+baz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 2)))
      }
    }

    "return its first over pairwise in 2D" in {
      Given {
        num2
      } And {
        List(Cell(Position2D("bar", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 1)),
          Cell(Position2D("bar", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 2)),
          Cell(Position2D("bar", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 3)),
          Cell(Position2D("baz", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 4)),
          Cell(Position2D("baz", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 5)))
      } When {
        (cells: TypedPipe[Cell[Position2D]], that: TypedPipe[Cell[Position2D]]) =>
          cells.pairwiseBetween(Over(First), that, Plus())
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("(baz+bar)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 1)),
            Cell(Position2D("(baz+bar)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 2)),
            Cell(Position2D("(foo+bar)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 1)),
            Cell(Position2D("(foo+bar)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 2)),
            Cell(Position2D("(foo+bar)", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 3)),
            Cell(Position2D("(foo+baz)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 4)),
            Cell(Position2D("(foo+baz)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 5)),
            Cell(Position2D("(qux+bar)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 1)),
            Cell(Position2D("(qux+baz)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 4)))
      }
    }

    "return its first along pairwise in 2D" in {
      Given {
        num2
      } And {
        List(Cell(Position2D("bar", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 1)),
          Cell(Position2D("baz", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 2)),
          Cell(Position2D("foo", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 3)),
          Cell(Position2D("foo", 4), Content(ContinuousSchema[Codex.DoubleCodex](), 4)))
      } When {
        (cells: TypedPipe[Cell[Position2D]], that: TypedPipe[Cell[Position2D]]) =>
          cells.pairwiseBetween(Along(First), that, List(Plus(), Minus()))
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("(3+2)", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 1)),
            Cell(Position2D("(3+2)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 3)),
            Cell(Position2D("(3-2)", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 1)),
            Cell(Position2D("(3-2)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 3)),
            Cell(Position2D("(4+2)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 3)),
            Cell(Position2D("(4-2)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 3)))
      }
    }

    "return its second over pairwise in 2D" in {
      Given {
        num2
      } And {
        List(Cell(Position2D("bar", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 1)),
          Cell(Position2D("baz", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 2)),
          Cell(Position2D("foo", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 3)),
          Cell(Position2D("foo", 4), Content(ContinuousSchema[Codex.DoubleCodex](), 4)))
      } When {
        (cells: TypedPipe[Cell[Position2D]], that: TypedPipe[Cell[Position2D]]) =>
          cells.pairwiseBetween(Over(Second), that, List(Plus(), Minus()))
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("(3+2)", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 1)),
            Cell(Position2D("(3+2)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 3)),
            Cell(Position2D("(3-2)", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 1)),
            Cell(Position2D("(3-2)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 3)),
            Cell(Position2D("(4+2)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 3)),
            Cell(Position2D("(4-2)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 3)))
      }
    }

    "return its second along pairwise in 2D" in {
      Given {
        num2
      } And {
        List(Cell(Position2D("bar", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 1)),
          Cell(Position2D("bar", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 2)),
          Cell(Position2D("bar", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 3)),
          Cell(Position2D("baz", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 4)),
          Cell(Position2D("baz", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 5)))
      } When {
        (cells: TypedPipe[Cell[Position2D]], that: TypedPipe[Cell[Position2D]]) =>
          cells.pairwiseBetween(Along(Second), that, Plus())
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("(baz+bar)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 1)),
            Cell(Position2D("(baz+bar)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 2)),
            Cell(Position2D("(foo+bar)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 1)),
            Cell(Position2D("(foo+bar)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 2)),
            Cell(Position2D("(foo+bar)", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 3)),
            Cell(Position2D("(foo+baz)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 4)),
            Cell(Position2D("(foo+baz)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 5)),
            Cell(Position2D("(qux+bar)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 1)),
            Cell(Position2D("(qux+baz)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 4)))
      }
    }

    "return its first over pairwise in 3D" in {
      Given {
        num3
      } And {
        List(Cell(Position3D("bar", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 1)),
          Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 2)),
          Cell(Position3D("bar", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3)),
          Cell(Position3D("baz", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 4)),
          Cell(Position3D("baz", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 5)))
      } When {
        (cells: TypedPipe[Cell[Position3D]], that: TypedPipe[Cell[Position3D]]) =>
          cells.pairwiseBetween(Over(First), that, Plus())
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position3D("(baz+bar)", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 1)),
            Cell(Position3D("(baz+bar)", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 2)),
            Cell(Position3D("(foo+bar)", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 1)),
            Cell(Position3D("(foo+bar)", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 2)),
            Cell(Position3D("(foo+bar)", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 3)),
            Cell(Position3D("(foo+baz)", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 4)),
            Cell(Position3D("(foo+baz)", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 5)),
            Cell(Position3D("(qux+bar)", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 1)),
            Cell(Position3D("(qux+baz)", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 4)))
      }
    }

    "return its first along pairwise in 3D" in {
      Given {
        num3
      } And {
        List(Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 1)),
          Cell(Position3D("baz", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 2)),
          Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3)),
          Cell(Position3D("foo", 4, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 4)))
      } When {
        (cells: TypedPipe[Cell[Position3D]], that: TypedPipe[Cell[Position3D]]) =>
          cells.pairwiseBetween(Along(First), that, List(Plus(), Minus()))
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("(3|xyz+2|xyz)", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 1)),
            Cell(Position2D("(3|xyz+2|xyz)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 3)),
            Cell(Position2D("(3|xyz-2|xyz)", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 1)),
            Cell(Position2D("(3|xyz-2|xyz)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 3)),
            Cell(Position2D("(4|xyz+2|xyz)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 3)),
            Cell(Position2D("(4|xyz-2|xyz)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 3)))
      }
    }

    "return its second over pairwise in 3D" in {
      Given {
        num3
      } And {
        List(Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 1)),
          Cell(Position3D("baz", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 2)),
          Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3)),
          Cell(Position3D("foo", 4, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 4)))
      } When {
        (cells: TypedPipe[Cell[Position3D]], that: TypedPipe[Cell[Position3D]]) =>
          cells.pairwiseBetween(Over(Second), that, List(Plus(), Minus()))
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position3D("(3+2)", "bar", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 1)),
            Cell(Position3D("(3+2)", "foo", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 3)),
            Cell(Position3D("(3-2)", "bar", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 1)),
            Cell(Position3D("(3-2)", "foo", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 3)),
            Cell(Position3D("(4+2)", "foo", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 3)),
            Cell(Position3D("(4-2)", "foo", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 3)))
      }
    }

    "return its second along pairwise in 3D" in {
      Given {
        num3
      } And {
        List(Cell(Position3D("bar", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 1)),
          Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 2)),
          Cell(Position3D("bar", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3)),
          Cell(Position3D("baz", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 4)),
          Cell(Position3D("baz", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 5)))
      } When {
        (cells: TypedPipe[Cell[Position3D]], that: TypedPipe[Cell[Position3D]]) =>
          cells.pairwiseBetween(Along(Second), that, Plus())
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("(baz|xyz+bar|xyz)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 1)),
            Cell(Position2D("(baz|xyz+bar|xyz)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 2)),
            Cell(Position2D("(foo|xyz+bar|xyz)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 1)),
            Cell(Position2D("(foo|xyz+bar|xyz)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 2)),
            Cell(Position2D("(foo|xyz+bar|xyz)", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 3)),
            Cell(Position2D("(foo|xyz+baz|xyz)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 4)),
            Cell(Position2D("(foo|xyz+baz|xyz)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 5)),
            Cell(Position2D("(qux|xyz+bar|xyz)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 1)),
            Cell(Position2D("(qux|xyz+baz|xyz)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 4)))
      }
    }

    "return its third over pairwise in 3D" in {
      Given {
        num3
      } And {
        List(Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 1)),
          Cell(Position3D("baz", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 2)),
          Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3)),
          Cell(Position3D("foo", 4, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 4)))
      } When {
        (cells: TypedPipe[Cell[Position3D]], that: TypedPipe[Cell[Position3D]]) =>
          cells.pairwiseBetween(Over(Third), that, List(Plus(), Minus()))
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList.sortBy(_.position) shouldBe List()
      }
    }

    "return its third along pairwise in 3D" in {
      Given {
        num3
      } And {
        List(Cell(Position3D("bar", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 1)),
          Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 2)),
          Cell(Position3D("bar", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3)),
          Cell(Position3D("baz", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 4)),
          Cell(Position3D("baz", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 5)))
      } When {
        (cells: TypedPipe[Cell[Position3D]], that: TypedPipe[Cell[Position3D]]) =>
          cells.pairwiseBetween(Along(Third), that, Plus())
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("(bar|2+bar|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 1)),
            Cell(Position2D("(bar|3+bar|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 1)),
            Cell(Position2D("(bar|3+bar|2)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 2)),
            Cell(Position2D("(baz|1+bar|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 1)),
            Cell(Position2D("(baz|1+bar|2)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 2)),
            Cell(Position2D("(baz|1+bar|3)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 3)),
            Cell(Position2D("(baz|2+bar|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 1)),
            Cell(Position2D("(baz|2+bar|2)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 2)),
            Cell(Position2D("(baz|2+bar|3)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 3)),
            Cell(Position2D("(baz|2+baz|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 4)),
            Cell(Position2D("(foo|1+bar|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 1)),
            Cell(Position2D("(foo|1+bar|2)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 2)),
            Cell(Position2D("(foo|1+bar|3)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 3)),
            Cell(Position2D("(foo|1+baz|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 4)),
            Cell(Position2D("(foo|1+baz|2)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 5)),
            Cell(Position2D("(foo|2+bar|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 1)),
            Cell(Position2D("(foo|2+bar|2)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 2)),
            Cell(Position2D("(foo|2+bar|3)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 3)),
            Cell(Position2D("(foo|2+baz|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 4)),
            Cell(Position2D("(foo|2+baz|2)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 5)),
            Cell(Position2D("(foo|3+bar|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 1)),
            Cell(Position2D("(foo|3+bar|2)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 2)),
            Cell(Position2D("(foo|3+bar|3)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 3)),
            Cell(Position2D("(foo|3+baz|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 4)),
            Cell(Position2D("(foo|3+baz|2)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 5)),
            Cell(Position2D("(foo|4+bar|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 1)),
            Cell(Position2D("(foo|4+bar|2)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 2)),
            Cell(Position2D("(foo|4+bar|3)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 3)),
            Cell(Position2D("(foo|4+baz|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 4)),
            Cell(Position2D("(foo|4+baz|2)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 5)),
            Cell(Position2D("(qux|1+bar|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 1)),
            Cell(Position2D("(qux|1+bar|2)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 2)),
            Cell(Position2D("(qux|1+bar|3)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 3)),
            Cell(Position2D("(qux|1+baz|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 4)),
            Cell(Position2D("(qux|1+baz|2)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 5)))
      }
    }
  }

  "A Matrix.pairwiseBetweenWithValue" should {
    "return its first over pairwise in 1D" in {
      Given {
        num1
      } And {
        List(Cell(Position1D("bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 1)),
          Cell(Position1D("baz"), Content(ContinuousSchema[Codex.DoubleCodex](), 2)))
      } When {
        (cells: TypedPipe[Cell[Position1D]], that: TypedPipe[Cell[Position1D]]) =>
          cells.pairwiseBetweenWithValue(Over(First), that, PlusX(), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position1D("(baz+bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 1 + 1)),
            Cell(Position1D("(foo+bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 1 + 1)),
            Cell(Position1D("(foo+baz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 2 + 1)),
            Cell(Position1D("(qux+bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 1 + 1)),
            Cell(Position1D("(qux+baz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 2 + 1)))
      }
    }

    "return its first over pairwise in 2D" in {
      Given {
        num2
      } And {
        List(Cell(Position2D("bar", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 1)),
          Cell(Position2D("bar", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 2)),
          Cell(Position2D("bar", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 3)),
          Cell(Position2D("baz", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 4)),
          Cell(Position2D("baz", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 5)))
      } When {
        (cells: TypedPipe[Cell[Position2D]], that: TypedPipe[Cell[Position2D]]) =>
          cells.pairwiseBetweenWithValue(Over(First), that, PlusX(), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("(baz+bar)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 1 + 1)),
            Cell(Position2D("(baz+bar)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 2 + 1)),
            Cell(Position2D("(foo+bar)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 1 + 1)),
            Cell(Position2D("(foo+bar)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 2 + 1)),
            Cell(Position2D("(foo+bar)", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 3 + 1)),
            Cell(Position2D("(foo+baz)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 4 + 1)),
            Cell(Position2D("(foo+baz)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 5 + 1)),
            Cell(Position2D("(qux+bar)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 1 + 1)),
            Cell(Position2D("(qux+baz)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 4 + 1)))
      }
    }

    "return its first along pairwise in 2D" in {
      Given {
        num2
      } And {
        List(Cell(Position2D("bar", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 1)),
          Cell(Position2D("baz", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 2)),
          Cell(Position2D("foo", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 3)),
          Cell(Position2D("foo", 4), Content(ContinuousSchema[Codex.DoubleCodex](), 4)))
      } When {
        (cells: TypedPipe[Cell[Position2D]], that: TypedPipe[Cell[Position2D]]) =>
          cells.pairwiseBetweenWithValue(Along(First), that, List(PlusX(), MinusX()), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("(3+2)", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 1 + 1)),
            Cell(Position2D("(3+2)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 3 + 1)),
            Cell(Position2D("(3-2)", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 1 - 1)),
            Cell(Position2D("(3-2)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 3 - 1)),
            Cell(Position2D("(4+2)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 3 + 1)),
            Cell(Position2D("(4-2)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 3 - 1)))
      }
    }

    "return its second over pairwise in 2D" in {
      Given {
        num2
      } And {
        List(Cell(Position2D("bar", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 1)),
          Cell(Position2D("baz", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 2)),
          Cell(Position2D("foo", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 3)),
          Cell(Position2D("foo", 4), Content(ContinuousSchema[Codex.DoubleCodex](), 4)))
      } When {
        (cells: TypedPipe[Cell[Position2D]], that: TypedPipe[Cell[Position2D]]) =>
          cells.pairwiseBetweenWithValue(Over(Second), that, List(PlusX(), MinusX()), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("(3+2)", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 1 + 1)),
            Cell(Position2D("(3+2)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 3 + 1)),
            Cell(Position2D("(3-2)", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 1 - 1)),
            Cell(Position2D("(3-2)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 3 - 1)),
            Cell(Position2D("(4+2)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 3 + 1)),
            Cell(Position2D("(4-2)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 3 - 1)))
      }
    }

    "return its second along pairwise in 2D" in {
      Given {
        num2
      } And {
        List(Cell(Position2D("bar", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 1)),
          Cell(Position2D("bar", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 2)),
          Cell(Position2D("bar", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 3)),
          Cell(Position2D("baz", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 4)),
          Cell(Position2D("baz", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 5)))
      } When {
        (cells: TypedPipe[Cell[Position2D]], that: TypedPipe[Cell[Position2D]]) =>
          cells.pairwiseBetweenWithValue(Along(Second), that, PlusX(), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("(baz+bar)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 1 + 1)),
            Cell(Position2D("(baz+bar)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 2 + 1)),
            Cell(Position2D("(foo+bar)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 1 + 1)),
            Cell(Position2D("(foo+bar)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 2 + 1)),
            Cell(Position2D("(foo+bar)", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 3 + 1)),
            Cell(Position2D("(foo+baz)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 4 + 1)),
            Cell(Position2D("(foo+baz)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 5 + 1)),
            Cell(Position2D("(qux+bar)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 1 + 1)),
            Cell(Position2D("(qux+baz)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 4 + 1)))
      }
    }

    "return its first over pairwise in 3D" in {
      Given {
        num3
      } And {
        List(Cell(Position3D("bar", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 1)),
          Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 2)),
          Cell(Position3D("bar", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3)),
          Cell(Position3D("baz", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 4)),
          Cell(Position3D("baz", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 5)))
      } When {
        (cells: TypedPipe[Cell[Position3D]], that: TypedPipe[Cell[Position3D]]) =>
          cells.pairwiseBetweenWithValue(Over(First), that, PlusX(), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position3D("(baz+bar)", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 1 + 1)),
            Cell(Position3D("(baz+bar)", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 2 + 1)),
            Cell(Position3D("(foo+bar)", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 1 + 1)),
            Cell(Position3D("(foo+bar)", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 2 + 1)),
            Cell(Position3D("(foo+bar)", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 3 + 1)),
            Cell(Position3D("(foo+baz)", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 4 + 1)),
            Cell(Position3D("(foo+baz)", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 5 + 1)),
            Cell(Position3D("(qux+bar)", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 1 + 1)),
            Cell(Position3D("(qux+baz)", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 4 + 1)))
      }
    }

    "return its first along pairwise in 3D" in {
      Given {
        num3
      } And {
        List(Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 1)),
          Cell(Position3D("baz", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 2)),
          Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3)),
          Cell(Position3D("foo", 4, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 4)))
      } When {
        (cells: TypedPipe[Cell[Position3D]], that: TypedPipe[Cell[Position3D]]) =>
          cells.pairwiseBetweenWithValue(Along(First), that, List(PlusX(), MinusX()), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("(3|xyz+2|xyz)", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 1 + 1)),
            Cell(Position2D("(3|xyz+2|xyz)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 3 + 1)),
            Cell(Position2D("(3|xyz-2|xyz)", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 1 - 1)),
            Cell(Position2D("(3|xyz-2|xyz)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 3 - 1)),
            Cell(Position2D("(4|xyz+2|xyz)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 3 + 1)),
            Cell(Position2D("(4|xyz-2|xyz)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 3 - 1)))
      }
    }

    "return its second over pairwise in 3D" in {
      Given {
        num3
      } And {
        List(Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 1)),
          Cell(Position3D("baz", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 2)),
          Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3)),
          Cell(Position3D("foo", 4, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 4)))
      } When {
        (cells: TypedPipe[Cell[Position3D]], that: TypedPipe[Cell[Position3D]]) =>
          cells.pairwiseBetweenWithValue(Over(Second), that, List(PlusX(), MinusX()), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position3D("(3+2)", "bar", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 1 + 1)),
            Cell(Position3D("(3+2)", "foo", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 3 + 1)),
            Cell(Position3D("(3-2)", "bar", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 1 - 1)),
            Cell(Position3D("(3-2)", "foo", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 3 - 1)),
            Cell(Position3D("(4+2)", "foo", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 3 + 1)),
            Cell(Position3D("(4-2)", "foo", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 3 - 1)))
      }
    }

    "return its second along pairwise in 3D" in {
      Given {
        num3
      } And {
        List(Cell(Position3D("bar", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 1)),
          Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 2)),
          Cell(Position3D("bar", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3)),
          Cell(Position3D("baz", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 4)),
          Cell(Position3D("baz", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 5)))
      } When {
        (cells: TypedPipe[Cell[Position3D]], that: TypedPipe[Cell[Position3D]]) =>
          cells.pairwiseBetweenWithValue(Along(Second), that, PlusX(), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("(baz|xyz+bar|xyz)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 1 + 1)),
            Cell(Position2D("(baz|xyz+bar|xyz)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 2 + 1)),
            Cell(Position2D("(foo|xyz+bar|xyz)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 1 + 1)),
            Cell(Position2D("(foo|xyz+bar|xyz)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 2 + 1)),
            Cell(Position2D("(foo|xyz+bar|xyz)", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 3 + 1)),
            Cell(Position2D("(foo|xyz+baz|xyz)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 4 + 1)),
            Cell(Position2D("(foo|xyz+baz|xyz)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 5 + 1)),
            Cell(Position2D("(qux|xyz+bar|xyz)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 1 + 1)),
            Cell(Position2D("(qux|xyz+baz|xyz)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 4 + 1)))
      }
    }

    "return its third over pairwise in 3D" in {
      Given {
        num3
      } And {
        List(Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 1)),
          Cell(Position3D("baz", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 2)),
          Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3)),
          Cell(Position3D("foo", 4, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 4)))
      } When {
        (cells: TypedPipe[Cell[Position3D]], that: TypedPipe[Cell[Position3D]]) =>
          cells.pairwiseBetweenWithValue(Over(Third), that, List(PlusX(), MinusX()), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList.sortBy(_.position) shouldBe List()
      }
    }

    "return its third along pairwise in 3D" in {
      Given {
        num3
      } And {
        List(Cell(Position3D("bar", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 1)),
          Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 2)),
          Cell(Position3D("bar", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3)),
          Cell(Position3D("baz", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 4)),
          Cell(Position3D("baz", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 5)))
      } When {
        (cells: TypedPipe[Cell[Position3D]], that: TypedPipe[Cell[Position3D]]) =>
          cells.pairwiseBetweenWithValue(Along(Third), that, PlusX(), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("(bar|2+bar|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 1 + 1)),
            Cell(Position2D("(bar|3+bar|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 1 + 1)),
            Cell(Position2D("(bar|3+bar|2)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 2 + 1)),
            Cell(Position2D("(baz|1+bar|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 1 + 1)),
            Cell(Position2D("(baz|1+bar|2)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 2 + 1)),
            Cell(Position2D("(baz|1+bar|3)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 3 + 1)),
            Cell(Position2D("(baz|2+bar|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 1 + 1)),
            Cell(Position2D("(baz|2+bar|2)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 2 + 1)),
            Cell(Position2D("(baz|2+bar|3)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 3 + 1)),
            Cell(Position2D("(baz|2+baz|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 4 + 1)),
            Cell(Position2D("(foo|1+bar|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 1 + 1)),
            Cell(Position2D("(foo|1+bar|2)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 2 + 1)),
            Cell(Position2D("(foo|1+bar|3)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 3 + 1)),
            Cell(Position2D("(foo|1+baz|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 4 + 1)),
            Cell(Position2D("(foo|1+baz|2)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 5 + 1)),
            Cell(Position2D("(foo|2+bar|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 1 + 1)),
            Cell(Position2D("(foo|2+bar|2)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 2 + 1)),
            Cell(Position2D("(foo|2+bar|3)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 3 + 1)),
            Cell(Position2D("(foo|2+baz|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 4 + 1)),
            Cell(Position2D("(foo|2+baz|2)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 5 + 1)),
            Cell(Position2D("(foo|3+bar|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 1 + 1)),
            Cell(Position2D("(foo|3+bar|2)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 2 + 1)),
            Cell(Position2D("(foo|3+bar|3)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 3 + 1)),
            Cell(Position2D("(foo|3+baz|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 4 + 1)),
            Cell(Position2D("(foo|3+baz|2)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 5 + 1)),
            Cell(Position2D("(foo|4+bar|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 1 + 1)),
            Cell(Position2D("(foo|4+bar|2)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 2 + 1)),
            Cell(Position2D("(foo|4+bar|3)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 3 + 1)),
            Cell(Position2D("(foo|4+baz|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 4 + 1)),
            Cell(Position2D("(foo|4+baz|2)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 5 + 1)),
            Cell(Position2D("(qux|1+bar|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 1 + 1)),
            Cell(Position2D("(qux|1+bar|2)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 2 + 1)),
            Cell(Position2D("(qux|1+bar|3)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 3 + 1)),
            Cell(Position2D("(qux|1+baz|1)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 4 + 1)),
            Cell(Position2D("(qux|1+baz|2)", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 5 + 1)))
      }
    }
  }
}

class TestMatrixChange extends WordSpec with Matchers with TBddDsl with TestMatrix {

  "A Matrix.change" should {
    "return its first over data in 1D" in {
      Given {
        data1
      } When {
        cells: TypedPipe[Cell[Position1D]] =>
          cells.change(Over(First), "foo", ContinuousSchema[Codex.DoubleCodex]())
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position1D("bar"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position1D("baz"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position1D("foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
            Cell(Position1D("qux"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))
      }
    }

    "return its first over data in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.change(Over(First), "foo", ContinuousSchema[Codex.DoubleCodex]())
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("bar", 1), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position2D("bar", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position2D("bar", 3), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position2D("baz", 1), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position2D("baz", 2), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position2D("foo", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
            Cell(Position2D("foo", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position2D("foo", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
            Cell(Position2D("qux", 1), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))
      }
    }

    "return its first along data in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.change(Along(First), List(3, 4), ContinuousSchema[Codex.DoubleCodex]())
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("bar", 1), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position2D("bar", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position2D("bar", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 19)),
            Cell(Position2D("baz", 1), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position2D("baz", 2), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position2D("foo", 1), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
            Cell(Position2D("foo", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position2D("foo", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
            Cell(Position2D("qux", 1), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))
      }
    }

    "return its second over data in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.change(Over(Second), List(3, 4), ContinuousSchema[Codex.DoubleCodex]())
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("bar", 1), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position2D("bar", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position2D("bar", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 19)),
            Cell(Position2D("baz", 1), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position2D("baz", 2), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position2D("foo", 1), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
            Cell(Position2D("foo", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position2D("foo", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
            Cell(Position2D("qux", 1), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))
      }
    }

    "return its second along data in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.change(Along(Second), "foo", ContinuousSchema[Codex.DoubleCodex]())
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("bar", 1), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position2D("bar", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position2D("bar", 3), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position2D("baz", 1), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position2D("baz", 2), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position2D("foo", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
            Cell(Position2D("foo", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position2D("foo", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
            Cell(Position2D("qux", 1), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))
      }
    }

    "return its first over data in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.change(Over(First), "foo", ContinuousSchema[Codex.DoubleCodex]())
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position3D("bar", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position3D("bar", 3, "xyz"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position3D("baz", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position3D("baz", 2, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position3D("foo", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
            Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position3D("foo", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
            Cell(Position3D("qux", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))
      }
    }

    "return its first along data in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.change(Along(First), List(Position2D(3, "xyz"), Position2D(4, "xyz")),
            ContinuousSchema[Codex.DoubleCodex]())
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position3D("bar", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position3D("bar", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 19)),
            Cell(Position3D("baz", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position3D("baz", 2, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position3D("foo", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
            Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position3D("foo", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
            Cell(Position3D("qux", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))
      }
    }

    "return its second over data in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.change(Over(Second), List(3, 4), ContinuousSchema[Codex.DoubleCodex]())
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position3D("bar", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position3D("bar", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 19)),
            Cell(Position3D("baz", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position3D("baz", 2, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position3D("foo", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
            Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position3D("foo", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
            Cell(Position3D("qux", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))
      }
    }

    "return its second along data in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.change(Along(Second), Position2D("foo", "xyz"), ContinuousSchema[Codex.DoubleCodex]())
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position3D("bar", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position3D("bar", 3, "xyz"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position3D("baz", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position3D("baz", 2, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position3D("foo", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
            Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position3D("foo", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
            Cell(Position3D("qux", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))
      }
    }

    "return its third over data in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.change(Over(Third), List("xyz"), ContinuousSchema[Codex.DoubleCodex]())
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position3D("bar", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position3D("bar", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 19)),
            Cell(Position3D("baz", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
            Cell(Position3D("baz", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 19)),
            Cell(Position3D("foo", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
            Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position3D("foo", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
            Cell(Position3D("qux", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)))
      }
    }

    "return its third along data in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.change(Along(Third), Position2D("foo", 1), ContinuousSchema[Codex.DoubleCodex]())
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position3D("bar", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position3D("bar", 3, "xyz"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position3D("baz", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position3D("baz", 2, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position3D("foo", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
            Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position3D("foo", 3, "xyz"), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position3D("foo", 4, "xyz"), Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
            Cell(Position3D("qux", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))
      }
    }
  }
}

class TestMatrixSet extends WordSpec with Matchers with TBddDsl with TestMatrix {

  "A Matrix.set" should {
    "return its updated data in 1D" in {
      Given {
        data1
      } When {
        cells: TypedPipe[Cell[Position1D]] =>
          cells.set("foo", Content(ContinuousSchema[Codex.DoubleCodex](), 1))
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position1D("bar"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position1D("baz"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position1D("foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 1)),
            Cell(Position1D("qux"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))
      }
    }

    "return its updated and added data in 1D" in {
      Given {
        data1
      } When {
        cells: TypedPipe[Cell[Position1D]] =>
          cells.set(List("foo", "quxx"), Content(ContinuousSchema[Codex.DoubleCodex](), 1))
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position1D("bar"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position1D("baz"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position1D("foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 1)),
            Cell(Position1D("qux"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")),
            Cell(Position1D("quxx"), Content(ContinuousSchema[Codex.DoubleCodex](), 1)))
      }
    }

    "return its matrix updated data in 1D" in {
      Given {
        data1
      } And {
        List(Cell(Position1D("foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 1)),
          Cell(Position1D("quxx"), Content(ContinuousSchema[Codex.DoubleCodex](), 2)))
      } When {
        (cells: TypedPipe[Cell[Position1D]], that: TypedPipe[Cell[Position1D]]) =>
          cells.set(that)
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position1D("bar"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position1D("baz"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position1D("foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 1)),
            Cell(Position1D("qux"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")),
            Cell(Position1D("quxx"), Content(ContinuousSchema[Codex.DoubleCodex](), 2)))
      }
    }

    "return its updated data in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.set(Position2D("foo", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 1))
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("bar", 1), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position2D("bar", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position2D("bar", 3), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position2D("baz", 1), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position2D("baz", 2), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position2D("foo", 1), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
            Cell(Position2D("foo", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 1)),
            Cell(Position2D("foo", 3), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position2D("foo", 4), Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
            Cell(Position2D("qux", 1), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))
      }
    }

    "return its updated and added data in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.set(List(Position2D("foo", 2), Position2D("quxx", 5)),
            Content(ContinuousSchema[Codex.DoubleCodex](), 1))
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("bar", 1), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position2D("bar", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position2D("bar", 3), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position2D("baz", 1), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position2D("baz", 2), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position2D("foo", 1), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
            Cell(Position2D("foo", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 1)),
            Cell(Position2D("foo", 3), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position2D("foo", 4), Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
            Cell(Position2D("qux", 1), Content(OrdinalSchema[Codex.StringCodex](), "12.56")),
            Cell(Position2D("quxx", 5), Content(ContinuousSchema[Codex.DoubleCodex](), 1)))
      }
    }

    "return its matrix updated data in 2D" in {
      Given {
        data2
      } And {
        List(Cell(Position2D("foo", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 1)),
          Cell(Position2D("quxx", 5), Content(ContinuousSchema[Codex.DoubleCodex](), 2)))
      } When {
        (cells: TypedPipe[Cell[Position2D]], that: TypedPipe[Cell[Position2D]]) =>
          cells.set(that)
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("bar", 1), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position2D("bar", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position2D("bar", 3), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position2D("baz", 1), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position2D("baz", 2), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position2D("foo", 1), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
            Cell(Position2D("foo", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 1)),
            Cell(Position2D("foo", 3), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position2D("foo", 4), Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
            Cell(Position2D("qux", 1), Content(OrdinalSchema[Codex.StringCodex](), "12.56")),
            Cell(Position2D("quxx", 5), Content(ContinuousSchema[Codex.DoubleCodex](), 2)))
      }
    }

    "return its updated data in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.set(Position3D("foo", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 1))
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position3D("bar", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position3D("bar", 3, "xyz"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position3D("baz", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position3D("baz", 2, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position3D("foo", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
            Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 1)),
            Cell(Position3D("foo", 3, "xyz"), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position3D("foo", 4, "xyz"), Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
            Cell(Position3D("qux", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))
      }
    }

    "return its updated and added data in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.set(List(Position3D("foo", 2, "xyz"), Position3D("quxx", 5, "abc")),
            Content(ContinuousSchema[Codex.DoubleCodex](), 1))
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position3D("bar", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position3D("bar", 3, "xyz"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position3D("baz", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position3D("baz", 2, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position3D("foo", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
            Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 1)),
            Cell(Position3D("foo", 3, "xyz"), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position3D("foo", 4, "xyz"), Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
            Cell(Position3D("qux", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")),
            Cell(Position3D("quxx", 5, "abc"), Content(ContinuousSchema[Codex.DoubleCodex](), 1)))
      }
    }

    "return its matrix updated data in 3D" in {
      Given {
        data3
      } And {
        List(Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 1)),
          Cell(Position3D("quxx", 5, "abc"), Content(ContinuousSchema[Codex.DoubleCodex](), 2)))
      } When {
        (cells: TypedPipe[Cell[Position3D]], that: TypedPipe[Cell[Position3D]]) =>
          cells.set(that)
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position3D("bar", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position3D("bar", 3, "xyz"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position3D("baz", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position3D("baz", 2, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position3D("foo", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
            Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 1)),
            Cell(Position3D("foo", 3, "xyz"), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position3D("foo", 4, "xyz"), Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
            Cell(Position3D("qux", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")),
            Cell(Position3D("quxx", 5, "abc"), Content(ContinuousSchema[Codex.DoubleCodex](), 2)))
      }
    }
  }
}

class TestMatrixTransform extends WordSpec with Matchers with TBddDsl with TestMatrix {

  val ext = ValuePipe(Map(
    Position1D("foo") -> Map(Position1D("max.abs") -> Content(ContinuousSchema[Codex.DoubleCodex](), 3.14),
      Position1D("mean") -> Content(ContinuousSchema[Codex.DoubleCodex](), 3.14),
      Position1D("sd") -> Content(ContinuousSchema[Codex.DoubleCodex](), 1)),
    Position1D("bar") -> Map(Position1D("max.abs") -> Content(ContinuousSchema[Codex.DoubleCodex](), 6.28),
      Position1D("mean") -> Content(ContinuousSchema[Codex.DoubleCodex](), 3.14),
      Position1D("sd") -> Content(ContinuousSchema[Codex.DoubleCodex](), 2)),
    Position1D("baz") -> Map(Position1D("max.abs") -> Content(ContinuousSchema[Codex.DoubleCodex](), 9.42),
      Position1D("mean") -> Content(ContinuousSchema[Codex.DoubleCodex](), 3.14),
      Position1D("sd") -> Content(ContinuousSchema[Codex.DoubleCodex](), 3)),
    Position1D("qux") -> Map(Position1D("max.abs") -> Content(ContinuousSchema[Codex.DoubleCodex](), 12.56),
      Position1D("mean") -> Content(ContinuousSchema[Codex.DoubleCodex](), 3.14),
      Position1D("sd") -> Content(ContinuousSchema[Codex.DoubleCodex](), 4))))

  case class IndicatorX(dim: Dimension, name: String) extends transform.Transformer with PresentExpanded {
    val indicator = Indicator(dim, name)
    def present[P <: Position with ExpandablePosition](cell: Cell[P]): Collection[Cell[P#M]] = {
      Collection(indicator.present(cell).toList.map { case c => Cell[P#M](c.position.append("ind"), c.content) })
    }
  }

  case class BinariseX(dim: Dimension) extends transform.Transformer with PresentExpanded {
    val binarise = Binarise(dim)
    def present[P <: Position with ExpandablePosition](cell: Cell[P]): Collection[Cell[P#M]] = {
      Collection(binarise.present(cell).toList.map { case c => Cell[P#M](c.position.append("bin"), c.content) })
    }
  }

  case class NormaliseX(dim: Dimension, key: String, name: String) extends transform.Transformer
    with PresentExpandedWithValue {
    type V = Normalise#V
    val normalise = Normalise(dim, key, name)
    def present[P <: Position with ExpandablePosition](cell: Cell[P], ext: V): Collection[Cell[P#M]] = {
      Collection(normalise.present(cell, ext).toList.map { case c => Cell[P#M](c.position.append("nrm"), c.content) })
    }
  }

  case class StandardiseX(dim: Dimension, mean: String, sd: String, name: String) extends transform.Transformer
    with PresentExpandedWithValue {
    type V = Standardise#V
    val standardise = Standardise(dim, mean, sd, name)
    def present[P <: Position with ExpandablePosition](cell: Cell[P], ext: V): Collection[Cell[P#M]] = {
      Collection(standardise.present(cell, ext).toList.map { case c => Cell[P#M](c.position.append("std"), c.content) })
    }
  }

  "A Matrix.transform" should {
    "return its transformed data in 1D" in {
      Given {
        data1
      } When {
        cells: TypedPipe[Cell[Position1D]] =>
          cells.transform(Indicator(First, "%1$s.ind"))
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position1D("bar.ind"), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position1D("baz.ind"), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position1D("foo.ind"), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position1D("qux.ind"), Content(DiscreteSchema[Codex.LongCodex](), 1)))
      }
    }

    "return its transformed data in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.transform(List(Indicator(First, "%1$s.ind"), Binarise(First)))
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("bar.ind", 1), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position2D("bar.ind", 2), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position2D("bar.ind", 3), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position2D("bar=19", 3), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position2D("bar=6.28", 1), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position2D("baz.ind", 1), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position2D("baz.ind", 2), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position2D("baz=9.42", 1), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position2D("foo.ind", 1), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position2D("foo.ind", 2), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position2D("foo.ind", 3), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position2D("foo.ind", 4), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position2D("foo=3.14", 1), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position2D("foo=9.42", 3), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position2D("qux.ind", 1), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position2D("qux=12.56", 1), Content(DiscreteSchema[Codex.LongCodex](), 1)))
      }
    }

    "return its transformed data in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.transform(List(Indicator(First, "%1$s.ind"), Binarise(First)))
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position3D("bar.ind", 1, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position3D("bar.ind", 2, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position3D("bar.ind", 3, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position3D("bar=19", 3, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position3D("bar=6.28", 1, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position3D("baz.ind", 1, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position3D("baz.ind", 2, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position3D("baz=9.42", 1, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position3D("foo.ind", 1, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position3D("foo.ind", 2, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position3D("foo.ind", 3, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position3D("foo.ind", 4, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position3D("foo=3.14", 1, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position3D("foo=9.42", 3, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position3D("qux.ind", 1, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position3D("qux=12.56", 1, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 1)))
      }
    }
  }

  "A Matrix.transformWithValue" should {
    "return its transformed data in 1D" in {
      Given {
        num1
      } When {
        cells: TypedPipe[Cell[Position1D]] =>
          cells.transformWithValue(List(Normalise(First, "max.abs", "%1$s.n"),
            Standardise(First, "mean", "sd", "%1$s.s")), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position1D("bar.n"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 / 6.28)),
            Cell(Position1D("bar.s"), Content(ContinuousSchema[Codex.DoubleCodex](), (6.28 - 3.14) / 2)),
            Cell(Position1D("baz.n"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 / 9.42)),
            Cell(Position1D("baz.s"), Content(ContinuousSchema[Codex.DoubleCodex](), (9.42 - 3.14) / 3)),
            Cell(Position1D("foo.n"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 / 3.14)),
            Cell(Position1D("foo.s"), Content(ContinuousSchema[Codex.DoubleCodex](), (3.14 - 3.14) / 1)),
            Cell(Position1D("qux.n"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 / 12.56)),
            Cell(Position1D("qux.s"), Content(ContinuousSchema[Codex.DoubleCodex](), (12.56 - 3.14) / 4)))
      }
    }

    "return its transformed data in 2D" in {
      Given {
        num2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.transformWithValue(Normalise(First, "max.abs", "%1$s.n"), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("bar.n", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 / 6.28)),
            Cell(Position2D("bar.n", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 / 6.28)),
            Cell(Position2D("bar.n", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 / 6.28)),
            Cell(Position2D("baz.n", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 / 9.42)),
            Cell(Position2D("baz.n", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 / 9.42)),
            Cell(Position2D("foo.n", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 / 3.14)),
            Cell(Position2D("foo.n", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 / 3.14)),
            Cell(Position2D("foo.n", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 / 3.14)),
            Cell(Position2D("foo.n", 4), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 / 3.14)),
            Cell(Position2D("qux.n", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 / 12.56)))
      }
    }

    "return its transformed data in 3D" in {
      Given {
        num3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.transformWithValue(Normalise(First, "max.abs", "%1$s.n"), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position3D("bar.n", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 / 6.28)),
            Cell(Position3D("bar.n", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 / 6.28)),
            Cell(Position3D("bar.n", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 / 6.28)),
            Cell(Position3D("baz.n", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 / 9.42)),
            Cell(Position3D("baz.n", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 / 9.42)),
            Cell(Position3D("foo.n", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 / 3.14)),
            Cell(Position3D("foo.n", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 / 3.14)),
            Cell(Position3D("foo.n", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 / 3.14)),
            Cell(Position3D("foo.n", 4, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 / 3.14)),
            Cell(Position3D("qux.n", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 / 12.56)))
      }
    }
  }

  "A Matrix.transformAndExpand" should {
    "return its transformed data in 1D" in {
      Given {
        data1
      } When {
        cells: TypedPipe[Cell[Position1D]] =>
          cells.transformAndExpand(IndicatorX(First, "%1$s.ind"))
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("bar.ind", "ind"), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position2D("baz.ind", "ind"), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position2D("foo.ind", "ind"), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position2D("qux.ind", "ind"), Content(DiscreteSchema[Codex.LongCodex](), 1)))
      }
    }

    "return its transformed data in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.transformAndExpand(List(IndicatorX(First, "%1$s.ind"), BinariseX(First)))
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position3D("bar.ind", 1, "ind"), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position3D("bar.ind", 2, "ind"), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position3D("bar.ind", 3, "ind"), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position3D("bar=19", 3, "bin"), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position3D("bar=6.28", 1, "bin"), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position3D("baz.ind", 1, "ind"), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position3D("baz.ind", 2, "ind"), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position3D("baz=9.42", 1, "bin"), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position3D("foo.ind", 1, "ind"), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position3D("foo.ind", 2, "ind"), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position3D("foo.ind", 3, "ind"), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position3D("foo.ind", 4, "ind"), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position3D("foo=3.14", 1, "bin"), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position3D("foo=9.42", 3, "bin"), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position3D("qux.ind", 1, "ind"), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position3D("qux=12.56", 1, "bin"), Content(DiscreteSchema[Codex.LongCodex](), 1)))
      }
    }

    "return its transformed data in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.transformAndExpand(List(IndicatorX(First, "%1$s.ind"), BinariseX(First)))
      } Then {
        buffer: mutable.Buffer[Cell[Position4D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position4D("bar.ind", 1, "xyz", "ind"), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position4D("bar.ind", 2, "xyz", "ind"), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position4D("bar.ind", 3, "xyz", "ind"), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position4D("bar=19", 3, "xyz", "bin"), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position4D("bar=6.28", 1, "xyz", "bin"), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position4D("baz.ind", 1, "xyz", "ind"), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position4D("baz.ind", 2, "xyz", "ind"), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position4D("baz=9.42", 1, "xyz", "bin"), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position4D("foo.ind", 1, "xyz", "ind"), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position4D("foo.ind", 2, "xyz", "ind"), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position4D("foo.ind", 3, "xyz", "ind"), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position4D("foo.ind", 4, "xyz", "ind"), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position4D("foo=3.14", 1, "xyz", "bin"), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position4D("foo=9.42", 3, "xyz", "bin"), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position4D("qux.ind", 1, "xyz", "ind"), Content(DiscreteSchema[Codex.LongCodex](), 1)),
            Cell(Position4D("qux=12.56", 1, "xyz", "bin"), Content(DiscreteSchema[Codex.LongCodex](), 1)))
      }
    }
  }

  "A Matrix.transformAndExpandWithValue" should {
    "return its transformed data in 1D" in {
      Given {
        num1
      } When {
        cells: TypedPipe[Cell[Position1D]] =>
          cells.transformAndExpandWithValue(List(NormaliseX(First, "max.abs", "%1$s.n"),
            StandardiseX(First, "mean", "sd", "%1$s.s")), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("bar.n", "nrm"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 / 6.28)),
            Cell(Position2D("bar.s", "std"), Content(ContinuousSchema[Codex.DoubleCodex](), (6.28 - 3.14) / 2)),
            Cell(Position2D("baz.n", "nrm"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 / 9.42)),
            Cell(Position2D("baz.s", "std"), Content(ContinuousSchema[Codex.DoubleCodex](), (9.42 - 3.14) / 3)),
            Cell(Position2D("foo.n", "nrm"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 / 3.14)),
            Cell(Position2D("foo.s", "std"), Content(ContinuousSchema[Codex.DoubleCodex](), (3.14 - 3.14) / 1)),
            Cell(Position2D("qux.n", "nrm"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 / 12.56)),
            Cell(Position2D("qux.s", "std"), Content(ContinuousSchema[Codex.DoubleCodex](), (12.56 - 3.14) / 4)))
      }
    }

    "return its transformed data in 2D" in {
      Given {
        num2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.transformAndExpandWithValue(NormaliseX(First, "max.abs", "%1$s.n"), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position3D("bar.n", 1, "nrm"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 / 6.28)),
            Cell(Position3D("bar.n", 2, "nrm"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 / 6.28)),
            Cell(Position3D("bar.n", 3, "nrm"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 / 6.28)),
            Cell(Position3D("baz.n", 1, "nrm"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 / 9.42)),
            Cell(Position3D("baz.n", 2, "nrm"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 / 9.42)),
            Cell(Position3D("foo.n", 1, "nrm"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 / 3.14)),
            Cell(Position3D("foo.n", 2, "nrm"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 / 3.14)),
            Cell(Position3D("foo.n", 3, "nrm"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 / 3.14)),
            Cell(Position3D("foo.n", 4, "nrm"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 / 3.14)),
            Cell(Position3D("qux.n", 1, "nrm"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 / 12.56)))
      }
    }

    "return its transformed data in 3D" in {
      Given {
        num3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.transformAndExpandWithValue(NormaliseX(First, "max.abs", "%1$s.n"), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position4D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position4D("bar.n", 1, "xyz", "nrm"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 / 6.28)),
            Cell(Position4D("bar.n", 2, "xyz", "nrm"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 / 6.28)),
            Cell(Position4D("bar.n", 3, "xyz", "nrm"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 / 6.28)),
            Cell(Position4D("baz.n", 1, "xyz", "nrm"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 / 9.42)),
            Cell(Position4D("baz.n", 2, "xyz", "nrm"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 / 9.42)),
            Cell(Position4D("foo.n", 1, "xyz", "nrm"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 / 3.14)),
            Cell(Position4D("foo.n", 2, "xyz", "nrm"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 / 3.14)),
            Cell(Position4D("foo.n", 3, "xyz", "nrm"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 / 3.14)),
            Cell(Position4D("foo.n", 4, "xyz", "nrm"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 / 3.14)),
            Cell(Position4D("qux.n", 1, "xyz", "nrm"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 / 12.56)))
      }
    }
  }
}

class TestMatrixDerive extends WordSpec with Matchers with TBddDsl with TestMatrix {

  case class Delta(times: Int) extends Deriver with Initialise {
    type T = Cell[Position]

    def initialise[P <: Position, D <: Dimension](slice: Slice[P, D])(cell: Cell[slice.S], rem: slice.R): T = {
      Cell(rem, cell.content)
    }

    def present[P <: Position, D <: Dimension](slice: Slice[P, D])(cell: Cell[slice.S], rem: slice.R,
      t: T): (T, Collection[Cell[slice.S#M]]) = {
      val delta = cell.content.value.asDouble.flatMap {
        case dc => t.content.value.asDouble.map {
          case dt => Left(Cell[slice.S#M](cell.position.append(times + "*(" + rem.toShortString("|") + "-" +
            t.position.toShortString("|") + ")"), Content(ContinuousSchema[Codex.DoubleCodex](), times * (dc - dt))))
        }
      }

      (Cell(rem, cell.content), Collection(delta))
    }
  }

  case class DeltaWithValue(key: String) extends Deriver with InitialiseWithValue {
    type T = Cell[Position]
    type V = Map[String, Int]

    def initialise[P <: Position, D <: Dimension](slice: Slice[P, D], ext: V)(cell: Cell[slice.S], rem: slice.R): T = {
      Cell(rem, cell.content)
    }

    def present[P <: Position, D <: Dimension](slice: Slice[P, D], ext: V)(cell: Cell[slice.S], rem: slice.R,
      t: T): (T, Collection[Cell[slice.S#M]]) = {
      val delta = cell.content.value.asDouble.flatMap {
        case dc => t.content.value.asDouble.map {
          case dt => Left(Cell[slice.S#M](cell.position.append(ext(key) + "*(" + rem.toShortString("|") + "-" +
            t.position.toShortString("|") + ")"), Content(ContinuousSchema[Codex.DoubleCodex](), ext(key) * (dc - dt))))
        }
      }

      (Cell(rem, cell.content), Collection(delta))
    }
  }

  val ext = ValuePipe(Map("one" -> 1, "two" -> 2))

  "A Matrix.derive" should {
    "return its first along derived data in 1D" in {
      Given {
        num1
      } When {
        cells: TypedPipe[Cell[Position1D]] =>
          cells.derive(Along(First), List(Delta(1), Delta(2)))
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position1D("1*(baz-bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 6.28)),
            Cell(Position1D("1*(foo-baz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 - 9.42)),
            Cell(Position1D("1*(qux-foo)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 3.14)),
            Cell(Position1D("2*(baz-bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 2 * (9.42 - 6.28))),
            Cell(Position1D("2*(foo-baz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 2 * (3.14 - 9.42))),
            Cell(Position1D("2*(qux-foo)"), Content(ContinuousSchema[Codex.DoubleCodex](), 2 * (12.56 - 3.14))))
      }
    }

    "return its first over derived data in 2D" in {
      Given {
        num2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.derive(Over(First), Delta(1))
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("bar", "1*(2-1)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 6.28)),
            Cell(Position2D("bar", "1*(3-2)"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 12.56)),
            Cell(Position2D("baz", "1*(2-1)"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 9.42)),
            Cell(Position2D("foo", "1*(2-1)"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 - 3.14)),
            Cell(Position2D("foo", "1*(3-2)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 6.28)),
            Cell(Position2D("foo", "1*(4-3)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 9.42)))
      }
    }

    "return its first along derived data in 2D" in {
      Given {
        num2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.derive(Along(First), Delta(1))
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D(1, "1*(baz-bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 6.28)),
            Cell(Position2D(1, "1*(foo-baz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 - 9.42)),
            Cell(Position2D(1, "1*(qux-foo)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 3.14)),
            Cell(Position2D(2, "1*(baz-bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 12.56)),
            Cell(Position2D(2, "1*(foo-baz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 - 18.84)),
            Cell(Position2D(3, "1*(foo-bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 18.84)))
      }
    }

    "return its second over derived data in 2D" in {
      Given {
        num2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.derive(Over(Second), Delta(1))
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D(1, "1*(baz-bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 6.28)),
            Cell(Position2D(1, "1*(foo-baz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 - 9.42)),
            Cell(Position2D(1, "1*(qux-foo)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 3.14)),
            Cell(Position2D(2, "1*(baz-bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 12.56)),
            Cell(Position2D(2, "1*(foo-baz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 - 18.84)),
            Cell(Position2D(3, "1*(foo-bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 18.84)))
      }
    }

    "return its second along derived data in 2D" in {
      Given {
        num2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.derive(Along(Second), Delta(1))
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("bar", "1*(2-1)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 6.28)),
            Cell(Position2D("bar", "1*(3-2)"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 12.56)),
            Cell(Position2D("baz", "1*(2-1)"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 9.42)),
            Cell(Position2D("foo", "1*(2-1)"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 - 3.14)),
            Cell(Position2D("foo", "1*(3-2)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 6.28)),
            Cell(Position2D("foo", "1*(4-3)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 9.42)))
      }
    }

    "return its first over derived data in 3D" in {
      Given {
        num3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.derive(Over(First), Delta(1))
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("bar", "1*(2|xyz-1|xyz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 6.28)),
            Cell(Position2D("bar", "1*(3|xyz-2|xyz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 12.56)),
            Cell(Position2D("baz", "1*(2|xyz-1|xyz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 9.42)),
            Cell(Position2D("foo", "1*(2|xyz-1|xyz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 - 3.14)),
            Cell(Position2D("foo", "1*(3|xyz-2|xyz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 6.28)),
            Cell(Position2D("foo", "1*(4|xyz-3|xyz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 9.42)))
      }
    }

    "return its first along derived data in 3D" in {
      Given {
        num3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.derive(Along(First), Delta(1))
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position3D(1, "xyz", "1*(baz-bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 6.28)),
            Cell(Position3D(1, "xyz", "1*(foo-baz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 - 9.42)),
            Cell(Position3D(1, "xyz", "1*(qux-foo)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 3.14)),
            Cell(Position3D(2, "xyz", "1*(baz-bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 12.56)),
            Cell(Position3D(2, "xyz", "1*(foo-baz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 - 18.84)),
            Cell(Position3D(3, "xyz", "1*(foo-bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 18.84)))
      }
    }

    "return its second over derived data in 3D" in {
      Given {
        num3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.derive(Over(Second), Delta(1))
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D(1, "1*(baz|xyz-bar|xyz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 6.28)),
            Cell(Position2D(1, "1*(foo|xyz-baz|xyz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 - 9.42)),
            Cell(Position2D(1, "1*(qux|xyz-foo|xyz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 3.14)),
            Cell(Position2D(2, "1*(baz|xyz-bar|xyz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 12.56)),
            Cell(Position2D(2, "1*(foo|xyz-baz|xyz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 - 18.84)),
            Cell(Position2D(3, "1*(foo|xyz-bar|xyz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 18.84)))
      }
    }

    "return its second along derived data in 3D" in {
      Given {
        num3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.derive(Along(Second), Delta(1))
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position3D("bar", "xyz", "1*(2-1)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 6.28)),
            Cell(Position3D("bar", "xyz", "1*(3-2)"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 12.56)),
            Cell(Position3D("baz", "xyz", "1*(2-1)"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 9.42)),
            Cell(Position3D("foo", "xyz", "1*(2-1)"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 - 3.14)),
            Cell(Position3D("foo", "xyz", "1*(3-2)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 6.28)),
            Cell(Position3D("foo", "xyz", "1*(4-3)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 9.42)))
      }
    }

    "return its third over derived data in 3D" in {
      Given {
        num3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.derive(Over(Third), Delta(1))
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("xyz", "1*(bar|2-bar|1)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 6.28)),
            Cell(Position2D("xyz", "1*(bar|3-bar|2)"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 12.56)),
            Cell(Position2D("xyz", "1*(baz|1-bar|3)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 18.84)),
            Cell(Position2D("xyz", "1*(baz|2-baz|1)"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 9.42)),
            Cell(Position2D("xyz", "1*(foo|1-baz|2)"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 - 18.84)),
            Cell(Position2D("xyz", "1*(foo|2-foo|1)"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 - 3.14)),
            Cell(Position2D("xyz", "1*(foo|3-foo|2)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 6.28)),
            Cell(Position2D("xyz", "1*(foo|4-foo|3)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 9.42)),
            Cell(Position2D("xyz", "1*(qux|1-foo|4)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 12.56)))
      }
    }

    "return its third along derived data in 3D" in {
      Given {
        num3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.derive(Along(Third), Delta(1))
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList.sortBy(_.position) shouldBe List()
      }
    }
  }

  "A Matrix.deriveWithValue" should {
    "return its first along derived data in 1D" in {
      Given {
        num1
      } When {
        cells: TypedPipe[Cell[Position1D]] =>
          cells.deriveWithValue(Along(First), List(DeltaWithValue("one"), DeltaWithValue("two")), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position1D("1*(baz-bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 6.28)),
            Cell(Position1D("1*(foo-baz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 - 9.42)),
            Cell(Position1D("1*(qux-foo)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 3.14)),
            Cell(Position1D("2*(baz-bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 2 * (9.42 - 6.28))),
            Cell(Position1D("2*(foo-baz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 2 * (3.14 - 9.42))),
            Cell(Position1D("2*(qux-foo)"), Content(ContinuousSchema[Codex.DoubleCodex](), 2 * (12.56 - 3.14))))
      }
    }

    "return its first over derived data in 2D" in {
      Given {
        num2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.deriveWithValue(Over(First), DeltaWithValue("one"), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("bar", "1*(2-1)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 6.28)),
            Cell(Position2D("bar", "1*(3-2)"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 12.56)),
            Cell(Position2D("baz", "1*(2-1)"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 9.42)),
            Cell(Position2D("foo", "1*(2-1)"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 - 3.14)),
            Cell(Position2D("foo", "1*(3-2)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 6.28)),
            Cell(Position2D("foo", "1*(4-3)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 9.42)))
      }
    }

    "return its first along derived data in 2D" in {
      Given {
        num2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.deriveWithValue(Along(First), DeltaWithValue("one"), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D(1, "1*(baz-bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 6.28)),
            Cell(Position2D(1, "1*(foo-baz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 - 9.42)),
            Cell(Position2D(1, "1*(qux-foo)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 3.14)),
            Cell(Position2D(2, "1*(baz-bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 12.56)),
            Cell(Position2D(2, "1*(foo-baz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 - 18.84)),
            Cell(Position2D(3, "1*(foo-bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 18.84)))
      }
    }

    "return its second over derived data in 2D" in {
      Given {
        num2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.deriveWithValue(Over(Second), DeltaWithValue("one"), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D(1, "1*(baz-bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 6.28)),
            Cell(Position2D(1, "1*(foo-baz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 - 9.42)),
            Cell(Position2D(1, "1*(qux-foo)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 3.14)),
            Cell(Position2D(2, "1*(baz-bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 12.56)),
            Cell(Position2D(2, "1*(foo-baz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 - 18.84)),
            Cell(Position2D(3, "1*(foo-bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 18.84)))
      }
    }

    "return its second along derived data in 2D" in {
      Given {
        num2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.deriveWithValue(Along(Second), DeltaWithValue("one"), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("bar", "1*(2-1)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 6.28)),
            Cell(Position2D("bar", "1*(3-2)"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 12.56)),
            Cell(Position2D("baz", "1*(2-1)"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 9.42)),
            Cell(Position2D("foo", "1*(2-1)"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 - 3.14)),
            Cell(Position2D("foo", "1*(3-2)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 6.28)),
            Cell(Position2D("foo", "1*(4-3)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 9.42)))
      }
    }

    "return its first over derived data in 3D" in {
      Given {
        num3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.deriveWithValue(Over(First), DeltaWithValue("one"), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("bar", "1*(2|xyz-1|xyz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 6.28)),
            Cell(Position2D("bar", "1*(3|xyz-2|xyz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 12.56)),
            Cell(Position2D("baz", "1*(2|xyz-1|xyz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 9.42)),
            Cell(Position2D("foo", "1*(2|xyz-1|xyz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 - 3.14)),
            Cell(Position2D("foo", "1*(3|xyz-2|xyz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 6.28)),
            Cell(Position2D("foo", "1*(4|xyz-3|xyz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 9.42)))
      }
    }

    "return its first along derived data in 3D" in {
      Given {
        num3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.deriveWithValue(Along(First), DeltaWithValue("one"), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position3D(1, "xyz", "1*(baz-bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 6.28)),
            Cell(Position3D(1, "xyz", "1*(foo-baz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 - 9.42)),
            Cell(Position3D(1, "xyz", "1*(qux-foo)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 3.14)),
            Cell(Position3D(2, "xyz", "1*(baz-bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 12.56)),
            Cell(Position3D(2, "xyz", "1*(foo-baz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 - 18.84)),
            Cell(Position3D(3, "xyz", "1*(foo-bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 18.84)))
      }
    }

    "return its second over derived data in 3D" in {
      Given {
        num3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.deriveWithValue(Over(Second), DeltaWithValue("one"), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D(1, "1*(baz|xyz-bar|xyz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 6.28)),
            Cell(Position2D(1, "1*(foo|xyz-baz|xyz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 - 9.42)),
            Cell(Position2D(1, "1*(qux|xyz-foo|xyz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 3.14)),
            Cell(Position2D(2, "1*(baz|xyz-bar|xyz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 12.56)),
            Cell(Position2D(2, "1*(foo|xyz-baz|xyz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 - 18.84)),
            Cell(Position2D(3, "1*(foo|xyz-bar|xyz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 18.84)))
      }
    }

    "return its second along derived data in 3D" in {
      Given {
        num3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.deriveWithValue(Along(Second), DeltaWithValue("one"), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position3D("bar", "xyz", "1*(2-1)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 6.28)),
            Cell(Position3D("bar", "xyz", "1*(3-2)"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 12.56)),
            Cell(Position3D("baz", "xyz", "1*(2-1)"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 9.42)),
            Cell(Position3D("foo", "xyz", "1*(2-1)"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 - 3.14)),
            Cell(Position3D("foo", "xyz", "1*(3-2)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 6.28)),
            Cell(Position3D("foo", "xyz", "1*(4-3)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 9.42)))
      }
    }

    "return its third over derived data in 3D" in {
      Given {
        num3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.deriveWithValue(Over(Third), DeltaWithValue("one"), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("xyz", "1*(bar|2-bar|1)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 6.28)),
            Cell(Position2D("xyz", "1*(bar|3-bar|2)"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 12.56)),
            Cell(Position2D("xyz", "1*(baz|1-bar|3)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 18.84)),
            Cell(Position2D("xyz", "1*(baz|2-baz|1)"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 9.42)),
            Cell(Position2D("xyz", "1*(foo|1-baz|2)"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 - 18.84)),
            Cell(Position2D("xyz", "1*(foo|2-foo|1)"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 - 3.14)),
            Cell(Position2D("xyz", "1*(foo|3-foo|2)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 6.28)),
            Cell(Position2D("xyz", "1*(foo|4-foo|3)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 9.42)),
            Cell(Position2D("xyz", "1*(qux|1-foo|4)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 12.56)))
      }
    }

    "return its third along derived data in 3D" in {
      Given {
        num3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.deriveWithValue(Along(Third), DeltaWithValue("one"), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList.sortBy(_.position) shouldBe List()
      }
    }
  }
}

class TestMatrixFill extends WordSpec with Matchers with TBddDsl with TestMatrix {

  "A Matrix.fillHomogenous" should {
    "return its filled data in 2D" in {
      Given {
        num2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.fillHomogenous(Content(ContinuousSchema[Codex.DoubleCodex](), 0))
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("bar", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position2D("bar", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position2D("bar", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
            Cell(Position2D("bar", 4), Content(ContinuousSchema[Codex.DoubleCodex](), 0)),
            Cell(Position2D("baz", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
            Cell(Position2D("baz", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
            Cell(Position2D("baz", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 0)),
            Cell(Position2D("baz", 4), Content(ContinuousSchema[Codex.DoubleCodex](), 0)),
            Cell(Position2D("foo", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
            Cell(Position2D("foo", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position2D("foo", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
            Cell(Position2D("foo", 4), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position2D("qux", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position2D("qux", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 0)),
            Cell(Position2D("qux", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 0)),
            Cell(Position2D("qux", 4), Content(ContinuousSchema[Codex.DoubleCodex](), 0)))
      }
    }

    "return its filled data in 3D" in {
      Given {
        num3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.fillHomogenous(Content(ContinuousSchema[Codex.DoubleCodex](), 0))
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position3D("bar", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position3D("bar", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
            Cell(Position3D("bar", 4, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 0)),
            Cell(Position3D("baz", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
            Cell(Position3D("baz", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
            Cell(Position3D("baz", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 0)),
            Cell(Position3D("baz", 4, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 0)),
            Cell(Position3D("foo", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
            Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position3D("foo", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
            Cell(Position3D("foo", 4, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position3D("qux", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position3D("qux", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 0)),
            Cell(Position3D("qux", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 0)),
            Cell(Position3D("qux", 4, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 0)))
      }
    }
  }

  "A Matrix.fillHetrogenous" should {
    "return its first over filled data in 2D" in {
      Given {
        num2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.fillHetrogenous(Over(First))(cells.reduce(Over(First), Mean()))
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("bar", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position2D("bar", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position2D("bar", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
            Cell(Position2D("bar", 4), Content(ContinuousSchema[Codex.DoubleCodex](), (6.28 + 12.56 + 18.84) / 3)),
            Cell(Position2D("baz", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
            Cell(Position2D("baz", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
            Cell(Position2D("baz", 3), Content(ContinuousSchema[Codex.DoubleCodex](), (9.42 + 18.84) / 2)),
            Cell(Position2D("baz", 4), Content(ContinuousSchema[Codex.DoubleCodex](), (9.42 + 18.84) / 2)),
            Cell(Position2D("foo", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
            Cell(Position2D("foo", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position2D("foo", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
            Cell(Position2D("foo", 4), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position2D("qux", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position2D("qux", 2), Content(ContinuousSchema[Codex.DoubleCodex](), (12.56) / 1)),
            Cell(Position2D("qux", 3), Content(ContinuousSchema[Codex.DoubleCodex](), (12.56) / 1)),
            Cell(Position2D("qux", 4), Content(ContinuousSchema[Codex.DoubleCodex](), (12.56) / 1)))
      }
    }

    "return its first along filled data in 2D" in {
      Given {
        num2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.fillHetrogenous(Along(First))(cells.reduce(Along(First), Mean()))
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("bar", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position2D("bar", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position2D("bar", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
            Cell(Position2D("bar", 4), Content(ContinuousSchema[Codex.DoubleCodex](), (12.56) / 1)),
            Cell(Position2D("baz", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
            Cell(Position2D("baz", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
            Cell(Position2D("baz", 3), Content(ContinuousSchema[Codex.DoubleCodex](), (9.42 + 18.84) / 2)),
            Cell(Position2D("baz", 4), Content(ContinuousSchema[Codex.DoubleCodex](), (12.56) / 1)),
            Cell(Position2D("foo", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
            Cell(Position2D("foo", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position2D("foo", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
            Cell(Position2D("foo", 4), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position2D("qux", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position2D("qux", 2), Content(ContinuousSchema[Codex.DoubleCodex](), (6.28 + 12.56 + 18.84) / 3)),
            Cell(Position2D("qux", 3), Content(ContinuousSchema[Codex.DoubleCodex](), (9.42 + 18.84) / 2)),
            Cell(Position2D("qux", 4), Content(ContinuousSchema[Codex.DoubleCodex](), (12.56) / 1)))
      }
    }

    "return its second over filled data in 2D" in {
      Given {
        num2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.fillHetrogenous(Over(Second))(cells.reduce(Over(Second), Mean()))
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("bar", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position2D("bar", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position2D("bar", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
            Cell(Position2D("bar", 4), Content(ContinuousSchema[Codex.DoubleCodex](), (12.56) / 1)),
            Cell(Position2D("baz", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
            Cell(Position2D("baz", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
            Cell(Position2D("baz", 3), Content(ContinuousSchema[Codex.DoubleCodex](), (9.42 + 18.84) / 2)),
            Cell(Position2D("baz", 4), Content(ContinuousSchema[Codex.DoubleCodex](), (12.56) / 1)),
            Cell(Position2D("foo", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
            Cell(Position2D("foo", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position2D("foo", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
            Cell(Position2D("foo", 4), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position2D("qux", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position2D("qux", 2), Content(ContinuousSchema[Codex.DoubleCodex](), (6.28 + 12.56 + 18.84) / 3)),
            Cell(Position2D("qux", 3), Content(ContinuousSchema[Codex.DoubleCodex](), (9.42 + 18.84) / 2)),
            Cell(Position2D("qux", 4), Content(ContinuousSchema[Codex.DoubleCodex](), (12.56) / 1)))
      }
    }

    "return its second along filled data in 2D" in {
      Given {
        num2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.fillHetrogenous(Along(Second))(cells.reduce(Along(Second), Mean()))
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("bar", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position2D("bar", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position2D("bar", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
            Cell(Position2D("bar", 4), Content(ContinuousSchema[Codex.DoubleCodex](), (6.28 + 12.56 + 18.84) / 3)),
            Cell(Position2D("baz", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
            Cell(Position2D("baz", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
            Cell(Position2D("baz", 3), Content(ContinuousSchema[Codex.DoubleCodex](), (9.42 + 18.84) / 2)),
            Cell(Position2D("baz", 4), Content(ContinuousSchema[Codex.DoubleCodex](), (9.42 + 18.84) / 2)),
            Cell(Position2D("foo", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
            Cell(Position2D("foo", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position2D("foo", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
            Cell(Position2D("foo", 4), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position2D("qux", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position2D("qux", 2), Content(ContinuousSchema[Codex.DoubleCodex](), (12.56) / 1)),
            Cell(Position2D("qux", 3), Content(ContinuousSchema[Codex.DoubleCodex](), (12.56) / 1)),
            Cell(Position2D("qux", 4), Content(ContinuousSchema[Codex.DoubleCodex](), (12.56) / 1)))
      }
    }

    "return its first over filled data in 3D" in {
      Given {
        num3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.fillHetrogenous(Over(First))(cells.reduce(Over(First), Mean()))
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position3D("bar", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position3D("bar", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
            Cell(Position3D("bar", 4, "xyz"),
              Content(ContinuousSchema[Codex.DoubleCodex](), (6.28 + 12.56 + 18.84) / 3)),
            Cell(Position3D("baz", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
            Cell(Position3D("baz", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
            Cell(Position3D("baz", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), (9.42 + 18.84) / 2)),
            Cell(Position3D("baz", 4, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), (9.42 + 18.84) / 2)),
            Cell(Position3D("foo", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
            Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position3D("foo", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
            Cell(Position3D("foo", 4, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position3D("qux", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position3D("qux", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), (12.56) / 1)),
            Cell(Position3D("qux", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), (12.56) / 1)),
            Cell(Position3D("qux", 4, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), (12.56) / 1)))
      }
    }

    "return its first along filled data in 3D" in {
      Given {
        num3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.fillHetrogenous(Along(First))(cells.reduce(Along(First), Mean()))
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position3D("bar", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position3D("bar", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
            Cell(Position3D("bar", 4, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), (12.56) / 1)),
            Cell(Position3D("baz", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
            Cell(Position3D("baz", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
            Cell(Position3D("baz", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), (9.42 + 18.84) / 2)),
            Cell(Position3D("baz", 4, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), (12.56) / 1)),
            Cell(Position3D("foo", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
            Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position3D("foo", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
            Cell(Position3D("foo", 4, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position3D("qux", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position3D("qux", 2, "xyz"),
              Content(ContinuousSchema[Codex.DoubleCodex](), (6.28 + 12.56 + 18.84) / 3)),
            Cell(Position3D("qux", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), (9.42 + 18.84) / 2)),
            Cell(Position3D("qux", 4, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), (12.56) / 1)))
      }
    }

    "return its second over filled data in 3D" in {
      Given {
        num3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.fillHetrogenous(Over(Second))(cells.reduce(Over(Second), Mean()))
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position3D("bar", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position3D("bar", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
            Cell(Position3D("bar", 4, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), (12.56) / 1)),
            Cell(Position3D("baz", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
            Cell(Position3D("baz", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
            Cell(Position3D("baz", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), (9.42 + 18.84) / 2)),
            Cell(Position3D("baz", 4, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), (12.56) / 1)),
            Cell(Position3D("foo", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
            Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position3D("foo", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
            Cell(Position3D("foo", 4, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position3D("qux", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position3D("qux", 2, "xyz"),
              Content(ContinuousSchema[Codex.DoubleCodex](), (6.28 + 12.56 + 18.84) / 3)),
            Cell(Position3D("qux", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), (9.42 + 18.84) / 2)),
            Cell(Position3D("qux", 4, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), (12.56) / 1)))
      }
    }

    "return its second along filled data in 3D" in {
      Given {
        num3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.fillHetrogenous(Along(Second))(cells.reduce(Along(Second), Mean()))
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position3D("bar", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position3D("bar", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
            Cell(Position3D("bar", 4, "xyz"),
              Content(ContinuousSchema[Codex.DoubleCodex](), (6.28 + 12.56 + 18.84) / 3)),
            Cell(Position3D("baz", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
            Cell(Position3D("baz", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
            Cell(Position3D("baz", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), (9.42 + 18.84) / 2)),
            Cell(Position3D("baz", 4, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), (9.42 + 18.84) / 2)),
            Cell(Position3D("foo", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
            Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position3D("foo", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
            Cell(Position3D("foo", 4, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position3D("qux", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position3D("qux", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), (12.56) / 1)),
            Cell(Position3D("qux", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), (12.56) / 1)),
            Cell(Position3D("qux", 4, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), (12.56) / 1)))
      }
    }

    "return its third over filled data in 3D" in {
      Given {
        num3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.fillHetrogenous(Over(Third))(cells.reduce(Over(Third), Mean()))
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position3D("bar", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position3D("bar", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
            Cell(Position3D("bar", 4, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](),
              (3.14 + 2 * 6.28 + 2 * 9.42 + 3 * 12.56 + 2 * 18.84) / 10)),
            Cell(Position3D("baz", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
            Cell(Position3D("baz", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
            Cell(Position3D("baz", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](),
              (3.14 + 2 * 6.28 + 2 * 9.42 + 3 * 12.56 + 2 * 18.84) / 10)),
            Cell(Position3D("baz", 4, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](),
              (3.14 + 2 * 6.28 + 2 * 9.42 + 3 * 12.56 + 2 * 18.84) / 10)),
            Cell(Position3D("foo", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
            Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position3D("foo", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
            Cell(Position3D("foo", 4, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position3D("qux", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position3D("qux", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](),
              (3.14 + 2 * 6.28 + 2 * 9.42 + 3 * 12.56 + 2 * 18.84) / 10)),
            Cell(Position3D("qux", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](),
              (3.14 + 2 * 6.28 + 2 * 9.42 + 3 * 12.56 + 2 * 18.84) / 10)),
            Cell(Position3D("qux", 4, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](),
              (3.14 + 2 * 6.28 + 2 * 9.42 + 3 * 12.56 + 2 * 18.84) / 10)))
      }
    }

    "return its third along filled data in 3D" in {
      Given {
        num3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.fillHetrogenous(Along(Third))(cells.reduce(Along(Third), Mean()))
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position3D("bar", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position3D("bar", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
            Cell(Position3D("baz", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
            Cell(Position3D("baz", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
            Cell(Position3D("foo", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
            Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position3D("foo", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
            Cell(Position3D("foo", 4, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position3D("qux", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)))
      }
    }
  }
}

class TestMatrixRename extends WordSpec with Matchers with TBddDsl with TestMatrix {

  def renamer[P <: Position](dim: Dimension, cell: Cell[P]): P = {
    cell.position.update(dim, cell.position(dim).toShortString + ".new").asInstanceOf[P]
  }

  def renamerWithValue[P <: Position](dim: Dimension, cell: Cell[P], ext: String): P = {
    cell.position.update(dim, cell.position(dim).toShortString + ext).asInstanceOf[P]
  }

  val ext = ValuePipe(".new")

  "A Matrix.rename" should {
    "return its first renamed data in 1D" in {
      Given {
        data1
      } When {
        cells: TypedPipe[Cell[Position1D]] =>
          cells.rename(First, renamer)
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position1D("bar.new"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position1D("baz.new"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position1D("foo.new"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
            Cell(Position1D("qux.new"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))
      }
    }

    "return its first renamed data in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.rename(First, renamer)
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("bar.new", 1), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position2D("bar.new", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position2D("bar.new", 3), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position2D("baz.new", 1), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position2D("baz.new", 2), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position2D("foo.new", 1), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
            Cell(Position2D("foo.new", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position2D("foo.new", 3), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position2D("foo.new", 4), Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
            Cell(Position2D("qux.new", 1), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))
      }
    }

    "return its second renamed data in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.rename(Second, renamer)
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("bar", "1.new"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position2D("bar", "2.new"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position2D("bar", "3.new"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position2D("baz", "1.new"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position2D("baz", "2.new"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position2D("foo", "1.new"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
            Cell(Position2D("foo", "2.new"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position2D("foo", "3.new"), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position2D("foo", "4.new"), Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
            Cell(Position2D("qux", "1.new"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))
      }
    }

    "return its first renamed data in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.rename(First, renamer)
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position3D("bar.new", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position3D("bar.new", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position3D("bar.new", 3, "xyz"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position3D("baz.new", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position3D("baz.new", 2, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position3D("foo.new", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
            Cell(Position3D("foo.new", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position3D("foo.new", 3, "xyz"), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position3D("foo.new", 4, "xyz"), Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
            Cell(Position3D("qux.new", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))
      }
    }

    "return its second renamed data in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.rename(Second, renamer)
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position3D("bar", "1.new", "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position3D("bar", "2.new", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position3D("bar", "3.new", "xyz"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position3D("baz", "1.new", "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position3D("baz", "2.new", "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position3D("foo", "1.new", "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
            Cell(Position3D("foo", "2.new", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position3D("foo", "3.new", "xyz"), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position3D("foo", "4.new", "xyz"), Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
            Cell(Position3D("qux", "1.new", "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))
      }
    }

    "return its third renamed data in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.rename(Third, renamer)
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position3D("bar", 1, "xyz.new"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position3D("bar", 2, "xyz.new"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position3D("bar", 3, "xyz.new"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position3D("baz", 1, "xyz.new"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position3D("baz", 2, "xyz.new"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position3D("foo", 1, "xyz.new"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
            Cell(Position3D("foo", 2, "xyz.new"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position3D("foo", 3, "xyz.new"), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position3D("foo", 4, "xyz.new"), Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
            Cell(Position3D("qux", 1, "xyz.new"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))
      }
    }
  }

  "A Matrix.renameWithValue" should {
    "return its first renamed data in 1D" in {
      Given {
        data1
      } When {
        cells: TypedPipe[Cell[Position1D]] =>
          cells.renameWithValue(First, renamerWithValue, ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position1D("bar.new"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position1D("baz.new"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position1D("foo.new"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
            Cell(Position1D("qux.new"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))
      }
    }

    "return its first renamed data in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.renameWithValue(First, renamerWithValue, ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("bar.new", 1), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position2D("bar.new", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position2D("bar.new", 3), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position2D("baz.new", 1), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position2D("baz.new", 2), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position2D("foo.new", 1), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
            Cell(Position2D("foo.new", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position2D("foo.new", 3), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position2D("foo.new", 4), Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
            Cell(Position2D("qux.new", 1), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))
      }
    }

    "return its second renamed data in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.renameWithValue(Second, renamerWithValue, ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("bar", "1.new"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position2D("bar", "2.new"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position2D("bar", "3.new"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position2D("baz", "1.new"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position2D("baz", "2.new"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position2D("foo", "1.new"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
            Cell(Position2D("foo", "2.new"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position2D("foo", "3.new"), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position2D("foo", "4.new"), Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
            Cell(Position2D("qux", "1.new"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))
      }
    }

    "return its first renamed data in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.renameWithValue(First, renamerWithValue, ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position3D("bar.new", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position3D("bar.new", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position3D("bar.new", 3, "xyz"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position3D("baz.new", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position3D("baz.new", 2, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position3D("foo.new", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
            Cell(Position3D("foo.new", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position3D("foo.new", 3, "xyz"), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position3D("foo.new", 4, "xyz"), Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
            Cell(Position3D("qux.new", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))
      }
    }

    "return its second renamed data in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.renameWithValue(Second, renamerWithValue, ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position3D("bar", "1.new", "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position3D("bar", "2.new", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position3D("bar", "3.new", "xyz"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position3D("baz", "1.new", "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position3D("baz", "2.new", "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position3D("foo", "1.new", "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
            Cell(Position3D("foo", "2.new", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position3D("foo", "3.new", "xyz"), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position3D("foo", "4.new", "xyz"), Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
            Cell(Position3D("qux", "1.new", "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))
      }
    }

    "return its third renamed data in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.renameWithValue(Third, renamerWithValue, ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position3D("bar", 1, "xyz.new"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position3D("bar", 2, "xyz.new"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position3D("bar", 3, "xyz.new"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position3D("baz", 1, "xyz.new"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position3D("baz", 2, "xyz.new"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position3D("foo", 1, "xyz.new"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
            Cell(Position3D("foo", 2, "xyz.new"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position3D("foo", 3, "xyz.new"), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position3D("foo", 4, "xyz.new"), Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
            Cell(Position3D("qux", 1, "xyz.new"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))
      }
    }
  }
}

class TestMatrixSquash extends WordSpec with Matchers with TBddDsl with TestMatrix {

  case class PreservingMaxPositionWithValue() extends Squasher with ReduceWithValue {
    type V = String

    val squasher = PreservingMaxPosition()

    def reduce[P <: Position](dim: Dimension, x: Cell[P], y: Cell[P], ext: V): Cell[P] = {
      if (ext == "ext") squasher.reduce(dim, x, y) else x
    }
  }

  val ext = ValuePipe("ext")

  "A Matrix.squash" should {
    "return its first squashed data in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.squash(First, PreservingMaxPosition())
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position1D(1), Content(OrdinalSchema[Codex.StringCodex](), "12.56")),
            Cell(Position1D(2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position1D(3), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position1D(4), Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))
      }
    }

    "return its second squashed data in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.squash(Second, PreservingMaxPosition())
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position1D("bar"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position1D("baz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position1D("foo"), Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
            Cell(Position1D("qux"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))
      }
    }

    "return its first squashed data in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.squash(First, PreservingMaxPosition())
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D(1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")),
            Cell(Position2D(2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position2D(3, "xyz"), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position2D(4, "xyz"), Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))
      }
    }

    "return its second squashed data in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.squash(Second, PreservingMaxPosition())
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("bar", "xyz"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position2D("baz", "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position2D("foo", "xyz"), Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
            Cell(Position2D("qux", "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))
      }
    }

    "return its third squashed data in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.squash(Third, PreservingMaxPosition())
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("bar", 1), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position2D("bar", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position2D("bar", 3), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position2D("baz", 1), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position2D("baz", 2), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position2D("foo", 1), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
            Cell(Position2D("foo", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position2D("foo", 3), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position2D("foo", 4), Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
            Cell(Position2D("qux", 1), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))
      }
    }
  }

  "A Matrix.squashWithValue" should {
    "return its first squashed data in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.squashWithValue(First, PreservingMaxPositionWithValue(), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position1D(1), Content(OrdinalSchema[Codex.StringCodex](), "12.56")),
            Cell(Position1D(2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position1D(3), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position1D(4), Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))
      }
    }

    "return its second squashed data in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.squashWithValue(Second, PreservingMaxPositionWithValue(), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position1D("bar"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position1D("baz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position1D("foo"), Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
            Cell(Position1D("qux"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))
      }
    }

    "return its first squashed data in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.squashWithValue(First, PreservingMaxPositionWithValue(), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D(1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")),
            Cell(Position2D(2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position2D(3, "xyz"), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position2D(4, "xyz"), Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))
      }
    }

    "return its second squashed data in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.squashWithValue(Second, PreservingMaxPositionWithValue(), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("bar", "xyz"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position2D("baz", "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position2D("foo", "xyz"), Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
            Cell(Position2D("qux", "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))
      }
    }

    "return its third squashed data in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.squashWithValue(Third, PreservingMaxPositionWithValue(), ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("bar", 1), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position2D("bar", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position2D("bar", 3), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position2D("baz", 1), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position2D("baz", 2), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position2D("foo", 1), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
            Cell(Position2D("foo", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position2D("foo", 3), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position2D("foo", 4), Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
            Cell(Position2D("qux", 1), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))
      }
    }
  }
}

class TestMatrixMelt extends WordSpec with Matchers with TBddDsl with TestMatrix {

  "A Matrix.melt" should {
    "return its first melted data in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.melt(First, Second)
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position1D("1.bar"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position1D("1.baz"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position1D("1.foo"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
            Cell(Position1D("1.qux"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")),
            Cell(Position1D("2.bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position1D("2.baz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position1D("2.foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position1D("3.bar"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position1D("3.foo"), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position1D("4.foo"), Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))
      }
    }

    "return its second melted data in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.melt(Second, First)
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position1D("bar.1"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position1D("bar.2"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position1D("bar.3"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position1D("baz.1"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position1D("baz.2"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position1D("foo.1"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
            Cell(Position1D("foo.2"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position1D("foo.3"), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position1D("foo.4"), Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
            Cell(Position1D("qux.1"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))
      }
    }

    "return its first melted data in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.melt(First, Third)
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D(1, "xyz.bar"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position2D(1, "xyz.baz"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position2D(1, "xyz.foo"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
            Cell(Position2D(1, "xyz.qux"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")),
            Cell(Position2D(2, "xyz.bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position2D(2, "xyz.baz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position2D(2, "xyz.foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position2D(3, "xyz.bar"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position2D(3, "xyz.foo"), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position2D(4, "xyz.foo"), Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))
      }
    }

    "return its second melted data in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.melt(Second, Third)
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("bar", "xyz.1"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position2D("bar", "xyz.2"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position2D("bar", "xyz.3"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position2D("baz", "xyz.1"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position2D("baz", "xyz.2"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position2D("foo", "xyz.1"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
            Cell(Position2D("foo", "xyz.2"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position2D("foo", "xyz.3"), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position2D("foo", "xyz.4"), Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
            Cell(Position2D("qux", "xyz.1"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))
      }
    }

    "return its third melted data in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.melt(Third, First)
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("bar.xyz", 1), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position2D("bar.xyz", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position2D("bar.xyz", 3), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position2D("baz.xyz", 1), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position2D("baz.xyz", 2), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position2D("foo.xyz", 1), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
            Cell(Position2D("foo.xyz", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position2D("foo.xyz", 3), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position2D("foo.xyz", 4), Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
            Cell(Position2D("qux.xyz", 1), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))
      }
    }
  }
}

class TestMatrixExpand extends WordSpec with Matchers with TBddDsl with TestMatrix {

  def expander[P <: Position with ExpandablePosition](cell: Cell[P]): P#M = cell.position.append("abc")

  def expanderWithValue[P <: Position with ExpandablePosition](cell: Cell[P], ext: String): P#M = {
    cell.position.append(ext)
  }

  val ext = ValuePipe("abc")

  "A Matrix.expand" should {
    "return its expanded data in 1D" in {
      Given {
        data1
      } When {
        cells: TypedPipe[Cell[Position1D]] =>
          cells.expand(expander)
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("bar", "abc"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position2D("baz", "abc"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position2D("foo", "abc"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
            Cell(Position2D("qux", "abc"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))
      }
    }

    "return its expanded data in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.expand(expander)
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position3D("bar", 1, "abc"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position3D("bar", 2, "abc"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position3D("bar", 3, "abc"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position3D("baz", 1, "abc"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position3D("baz", 2, "abc"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position3D("foo", 1, "abc"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
            Cell(Position3D("foo", 2, "abc"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position3D("foo", 3, "abc"), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position3D("foo", 4, "abc"), Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
            Cell(Position3D("qux", 1, "abc"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))
      }
    }

    "return its expanded data in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.expand(expander)
      } Then {
        buffer: mutable.Buffer[Cell[Position4D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position4D("bar", 1, "xyz", "abc"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position4D("bar", 2, "xyz", "abc"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position4D("bar", 3, "xyz", "abc"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position4D("baz", 1, "xyz", "abc"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position4D("baz", 2, "xyz", "abc"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position4D("foo", 1, "xyz", "abc"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
            Cell(Position4D("foo", 2, "xyz", "abc"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position4D("foo", 3, "xyz", "abc"), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position4D("foo", 4, "xyz", "abc"), Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
            Cell(Position4D("qux", 1, "xyz", "abc"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))
      }
    }
  }

  "A Matrix.expandWithValue" should {
    "return its expanded data in 1D" in {
      Given {
        data1
      } When {
        cells: TypedPipe[Cell[Position1D]] =>
          cells.expandWithValue(expanderWithValue, ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position2D("bar", "abc"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position2D("baz", "abc"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position2D("foo", "abc"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
            Cell(Position2D("qux", "abc"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))
      }
    }

    "return its expanded data in 2D" in {
      Given {
        data2
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.expandWithValue(expanderWithValue, ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position3D("bar", 1, "abc"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position3D("bar", 2, "abc"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position3D("bar", 3, "abc"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position3D("baz", 1, "abc"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position3D("baz", 2, "abc"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position3D("foo", 1, "abc"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
            Cell(Position3D("foo", 2, "abc"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position3D("foo", 3, "abc"), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position3D("foo", 4, "abc"), Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
            Cell(Position3D("qux", 1, "abc"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))
      }
    }

    "return its expanded data in 3D" in {
      Given {
        data3
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.expandWithValue(expanderWithValue, ext)
      } Then {
        buffer: mutable.Buffer[Cell[Position4D]] =>
          buffer.toList.sortBy(_.position) shouldBe List(
            Cell(Position4D("bar", 1, "xyz", "abc"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
            Cell(Position4D("bar", 2, "xyz", "abc"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position4D("bar", 3, "xyz", "abc"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
            Cell(Position4D("baz", 1, "xyz", "abc"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position4D("baz", 2, "xyz", "abc"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
            Cell(Position4D("foo", 1, "xyz", "abc"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
            Cell(Position4D("foo", 2, "xyz", "abc"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position4D("foo", 3, "xyz", "abc"), Content(NominalSchema[Codex.StringCodex](), "9.42")),
            Cell(Position4D("foo", 4, "xyz", "abc"), Content(DateSchema[Codex.DateTimeCodex](),
              (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
            Cell(Position4D("qux", 1, "xyz", "abc"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))
      }
    }
  }
}

class TestMatrixPermute extends WordSpec with Matchers with TBddDsl with TestMatrix {

  "A Matrix.permute" should {
    "return its permutation in 2D" in {
      Given {
        List(Cell(Position2D(1, 3), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
          Cell(Position2D(2, 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
          Cell(Position2D(3, 1), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)))
      } When {
        cells: TypedPipe[Cell[Position2D]] =>
          cells.permute(Second, First)
      } Then {
        buffer: mutable.Buffer[Cell[Position2D]] =>
          buffer.toList shouldBe List(Cell(Position2D(3, 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
            Cell(Position2D(2, 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position2D(1, 3), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)))
      }
    }

    "return its permutation in 3D" in {
      Given {
        List(Cell(Position3D(1, 2, 3), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
          Cell(Position3D(2, 2, 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
          Cell(Position3D(3, 2, 1), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)))
      } When {
        cells: TypedPipe[Cell[Position3D]] =>
          cells.permute(Second, Third, First)
      } Then {
        buffer: mutable.Buffer[Cell[Position3D]] =>
          buffer.toList shouldBe 
        List(Cell(Position3D(2, 3, 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
          Cell(Position3D(2, 2, 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
          Cell(Position3D(2, 1, 3), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)))
      }
    }

    "return its permutation in 4D" in {
      Given {
        List(Cell(Position4D(1, 2, 3, 4), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
          Cell(Position4D(2, 2, 2, 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
          Cell(Position4D(1, 1, 4, 4), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
          Cell(Position4D(4, 1, 3, 2), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)))
      } When {
        cells: TypedPipe[Cell[Position4D]] =>
          cells.permute(Fourth, Third, First, Second)
      } Then {
        buffer: mutable.Buffer[Cell[Position4D]] =>
          buffer.toList shouldBe List(
            Cell(Position4D(4, 3, 1, 2), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
            Cell(Position4D(2, 2, 2, 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position4D(4, 4, 1, 1), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
            Cell(Position4D(2, 3, 4, 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)))
      }
    }

    "return its permutation in 5D" in {
      Given {
        List(Cell(Position5D(1, 2, 3, 4, 5), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
          Cell(Position5D(2, 2, 2, 2, 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
          Cell(Position5D(1, 1, 3, 5, 5), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
          Cell(Position5D(4, 4, 4, 1, 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
          Cell(Position5D(5, 4, 3, 2, 1), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)))
      } When {
        cells: TypedPipe[Cell[Position5D]] =>
          cells.permute(Fourth, Second, First, Fifth, Third)
      } Then {
        buffer: mutable.Buffer[Cell[Position5D]] =>
          buffer.toList shouldBe List(
            Cell(Position5D(4, 2, 1, 5, 3), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
            Cell(Position5D(2, 2, 2, 2, 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
            Cell(Position5D(5, 1, 1, 5, 3), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
            Cell(Position5D(1, 4, 4, 1, 4), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
            Cell(Position5D(2, 4, 5, 1, 3), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)))
      }
    }
  }
}

