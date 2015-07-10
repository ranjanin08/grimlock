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
import au.com.cba.omnia.grimlock.framework.pairwise._
import au.com.cba.omnia.grimlock.framework.partition._
import au.com.cba.omnia.grimlock.framework.position._
import au.com.cba.omnia.grimlock.framework.sample._
import au.com.cba.omnia.grimlock.framework.squash._
import au.com.cba.omnia.grimlock.framework.transform._
import au.com.cba.omnia.grimlock.framework.Type._
import au.com.cba.omnia.grimlock.framework.utility._
import au.com.cba.omnia.grimlock.framework.window._

import au.com.cba.omnia.grimlock.library.aggregate._
import au.com.cba.omnia.grimlock.library.pairwise._
import au.com.cba.omnia.grimlock.library.squash._
import au.com.cba.omnia.grimlock.library.transform._

import au.com.cba.omnia.grimlock.scalding.Matrix._
import au.com.cba.omnia.grimlock.scalding.Matrixable._
import au.com.cba.omnia.grimlock.scalding.position.PositionDistributable._
import au.com.cba.omnia.grimlock.scalding.Nameable._

import au.com.cba.omnia.grimlock.spark.Matrix._
import au.com.cba.omnia.grimlock.spark.Matrixable._
import au.com.cba.omnia.grimlock.spark.position.PositionDistributable._
import au.com.cba.omnia.grimlock.spark.Nameable._

import com.twitter.scalding._
import com.twitter.scalding.bdd._
import com.twitter.scalding.typed.ValuePipe

class TestCell extends TestGrimlock {

  "A Cell" should "return its string" in {
    Cell(Position2D("foo", 123), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)).toString(".", true) shouldBe
      "Position2D(StringValue(foo),LongValue(123)).Content(ContinuousSchema[DoubleCodex](),DoubleValue(3.14))"
    Cell(Position2D("foo", 123), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)).toString(".", false) shouldBe
      "foo.123.continuous.double.3.14"
  }
}

trait TestMatrix extends TestGrimlock {

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
}

class TestScaldingMatrixNames extends TestMatrix with TBddDsl {

  "A Matrix.names" should "return its first over names in 1D" in {
    Given {
      data1
    } When {
      cells: TypedPipe[Cell[Position1D]] =>
        cells.names(Over(First))
    } Then {
      _.toList.sortBy(_._1) shouldBe List((Position1D("bar"), 0), (Position1D("baz"), 1), (Position1D("foo"), 2),
        (Position1D("qux"), 3))
    }
  }

  it should "return its first over names in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.names(Over(First))
    } Then {
      _.toList.sortBy(_._1) shouldBe List((Position1D("bar"), 0), (Position1D("baz"), 1), (Position1D("foo"), 2),
        (Position1D("qux"), 3))
    }
  }

  it should "return its first along names in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.names(Along(First))
    } Then {
      _.toList.sortBy(_._1) shouldBe List((Position1D(1), 0), (Position1D(2), 1), (Position1D(3), 2),
        (Position1D(4), 3))
    }
  }

  it should "return its second over names in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.names(Over(Second))
    } Then {
      _.toList.sortBy(_._1) shouldBe List((Position1D(1), 0), (Position1D(2), 1), (Position1D(3), 2),
        (Position1D(4), 3))
    }
  }

  it should "return its second along names in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.names(Along(Second))
    } Then {
      _.toList.sortBy(_._1) shouldBe List((Position1D("bar"), 0), (Position1D("baz"), 1), (Position1D("foo"), 2),
        (Position1D("qux"), 3))
    }
  }

  it should "return its first over names in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.names(Over(First))
    } Then {
      _.toList.sortBy(_._1) shouldBe List((Position1D("bar"), 0), (Position1D("baz"), 1), (Position1D("foo"), 2),
        (Position1D("qux"), 3))
    }
  }

  it should "return its first along names in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.names(Along(First))
    } Then {
      _.toList.sortBy(_._1) shouldBe List((Position2D(1, "xyz"), 0), (Position2D(2, "xyz"), 1),
        (Position2D(3, "xyz"), 2), (Position2D(4, "xyz"), 3))
    }
  }

  it should "return its second over names in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.names(Over(Second))
    } Then {
      _.toList.sortBy(_._1) shouldBe List((Position1D(1), 0), (Position1D(2), 1), (Position1D(3), 2),
        (Position1D(4), 3))
    }
  }

  it should "return its second along names in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.names(Along(Second))
    } Then {
      _.toList.sortBy(_._1) shouldBe List((Position2D("bar", "xyz"), 0), (Position2D("baz", "xyz"), 1),
        (Position2D("foo", "xyz"), 2), (Position2D("qux", "xyz"), 3))
    }
  }

  it should "return its third over names in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.names(Over(Third))
    } Then {
      _.toList.sortBy(_._1) shouldBe List((Position1D("xyz"), 0))
    }
  }

  it should "return its third along names in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.names(Along(Third))
    } Then {
      _.toList.sortBy(_._1) shouldBe List((Position2D("bar", 1), 0), (Position2D("bar", 2), 1),
        (Position2D("bar", 3), 2), (Position2D("baz", 1), 3), (Position2D("baz", 2), 4), (Position2D("foo", 1), 5),
        (Position2D("foo", 2), 6), (Position2D("foo", 3), 7), (Position2D("foo", 4), 8), (Position2D("qux", 1), 9))
    }
  }
}

class TestSparkMatrixNames extends TestMatrix {

  "A Matrix.names" should "return its first over names in 1D" in {
    toRDD(data1)
      .names(Over(First))
      .toList.sortBy(_._1) should be (List((Position1D("bar"), 2), (Position1D("baz"), 1), (Position1D("foo"), 0),
        (Position1D("qux"), 3)))
  }

  it should "return its first over names in 2D" in {
    toRDD(data2)
      .names(Over(First))
      .toList.sortBy(_._1) should be (List((Position1D("bar"), 2), (Position1D("baz"), 1), (Position1D("foo"), 0),
        (Position1D("qux"), 3)))
  }

  it should "return its first along names in 2D" in {
    toRDD(data2)
      .names(Along(First))
      .toList.sortBy(_._1) should be (List((Position1D(1), 3), (Position1D(2), 0), (Position1D(3), 1),
        (Position1D(4), 2)))
  }

  it should "return its second over names in 2D" in {
    toRDD(data2)
      .names(Over(Second))
      .toList.sortBy(_._1) should be (List((Position1D(1), 3), (Position1D(2), 0), (Position1D(3), 1),
        (Position1D(4), 2)))
  }

  it should "return its second along names in 2D" in {
    toRDD(data2)
      .names(Along(Second))
      .toList.sortBy(_._1) should be (List((Position1D("bar"), 2), (Position1D("baz"), 1), (Position1D("foo"), 0),
        (Position1D("qux"), 3)))
  }

  it should "return its first over names in 3D" in {
    toRDD(data3)
      .names(Over(First))
      .toList.sortBy(_._1) should be (List((Position1D("bar"), 2), (Position1D("baz"), 1), (Position1D("foo"), 0),
        (Position1D("qux"), 3)))
  }

  it should "return its first along names in 3D" in {
    toRDD(data3)
      .names(Along(First))
      .toList.sortBy(_._1) should be (List((Position2D(1, "xyz"), 3), (Position2D(2, "xyz"), 2),
        (Position2D(3, "xyz"), 1), (Position2D(4, "xyz"), 0)))
  }

  it should "return its second over names in 3D" in {
    toRDD(data3)
      .names(Over(Second))
      .toList.sortBy(_._1) should be (List((Position1D(1), 3), (Position1D(2), 0), (Position1D(3), 1),
        (Position1D(4), 2)))
  }

  it should "return its second along names in 3D" in {
    toRDD(data3)
      .names(Along(Second))
      .toList.sortBy(_._1) should be (List((Position2D("bar", "xyz"), 2), (Position2D("baz", "xyz"), 1),
        (Position2D("foo", "xyz"), 0), (Position2D("qux", "xyz"), 3)))
  }

  it should "return its third over names in 3D" in {
    toRDD(data3)
      .names(Over(Third))
      .toList.sortBy(_._1) should be (List((Position1D("xyz"), 0)))
  }

  it should "return its third along names in 3D" in {
    toRDD(data3)
      .names(Along(Third))
      .toList.sortBy(_._1) should be (List((Position2D("bar", 1), 1), (Position2D("bar", 2), 6),
        (Position2D("bar", 3), 2), (Position2D("baz", 1), 9), (Position2D("baz", 2), 4), (Position2D("foo", 1), 0),
        (Position2D("foo", 2), 3), (Position2D("foo", 3), 8), (Position2D("foo", 4), 5), (Position2D("qux", 1), 7)))
  }
}

trait TestMatrixTypes extends TestMatrix {

  val result1 = List((Position1D("bar"), Categorical), (Position1D("baz"), Categorical),
    (Position1D("foo"), Categorical), (Position1D("qux"), Categorical))

  val result2 =  List((Position1D("bar"), Ordinal), (Position1D("baz"), Ordinal), (Position1D("foo"), Ordinal),
    (Position1D("qux"), Ordinal))

  val result3 = List((Position1D("bar"), Mixed), (Position1D("baz"), Mixed), (Position1D("foo"), Mixed),
    (Position1D("qux"), Categorical))

  val result4 = List((Position1D("bar"), Mixed), (Position1D("baz"), Mixed), (Position1D("foo"), Mixed),
    (Position1D("qux"), Ordinal))

  val result5 = List((Position1D(1), Categorical), (Position1D(2), Numerical), (Position1D(3), Categorical),
    (Position1D(4), Date))

  val result6 = List((Position1D(1), Ordinal), (Position1D(2), Numerical), (Position1D(3), Categorical),
    (Position1D(4), Date))

  val result7 = List((Position1D(1), Categorical), (Position1D(2), Numerical), (Position1D(3), Categorical),
    (Position1D(4), Date))

  val result8 = List((Position1D(1), Ordinal), (Position1D(2), Numerical), (Position1D(3), Categorical),
    (Position1D(4), Date))

  val result9 = List((Position1D("bar"), Mixed), (Position1D("baz"), Mixed), (Position1D("foo"), Mixed),
    (Position1D("qux"), Categorical))

  val result10 = List((Position1D("bar"), Mixed), (Position1D("baz"), Mixed), (Position1D("foo"), Mixed),
    (Position1D("qux"), Ordinal))

  val result11 = List((Position1D("bar"), Mixed), (Position1D("baz"), Mixed), (Position1D("foo"), Mixed),
    (Position1D("qux"), Categorical))

  val result12 = List((Position1D("bar"), Mixed), (Position1D("baz"), Mixed), (Position1D("foo"), Mixed),
    (Position1D("qux"), Ordinal))

  val result13 = List((Position2D(1, "xyz"), Categorical), (Position2D(2, "xyz"), Numerical),
    (Position2D(3, "xyz"), Categorical), (Position2D(4, "xyz"), Date))

  val result14 = List((Position2D(1, "xyz"), Ordinal), (Position2D(2, "xyz"), Numerical),
    (Position2D(3, "xyz"), Categorical), (Position2D(4, "xyz"), Date))

  val result15 = List((Position1D(1), Categorical), (Position1D(2), Numerical), (Position1D(3), Categorical),
    (Position1D(4), Date))

  val result16 = List((Position1D(1), Ordinal), (Position1D(2), Numerical), (Position1D(3), Categorical),
    (Position1D(4), Date))

  val result17 = List((Position2D("bar", "xyz"), Mixed), (Position2D("baz", "xyz"), Mixed),
    (Position2D("foo", "xyz"), Mixed), (Position2D("qux", "xyz"), Categorical))

  val result18 = List((Position2D("bar", "xyz"), Mixed), (Position2D("baz", "xyz"), Mixed),
    (Position2D("foo", "xyz"), Mixed), (Position2D("qux", "xyz"), Ordinal))

  val result19 = List((Position1D("xyz"), Mixed))

  val result20 = List((Position1D("xyz"), Mixed))

  val result21 = List((Position2D("bar", 1), Categorical), (Position2D("bar", 2), Numerical),
    (Position2D("bar", 3), Categorical), (Position2D("baz", 1), Categorical), (Position2D("baz", 2), Numerical),
    (Position2D("foo", 1), Categorical), (Position2D("foo", 2), Numerical), (Position2D("foo", 3), Categorical),
    (Position2D("foo", 4), Date), (Position2D("qux", 1), Categorical))

  val result22 = List((Position2D("bar", 1), Ordinal), (Position2D("bar", 2), Continuous),
    (Position2D("bar", 3), Ordinal), (Position2D("baz", 1), Ordinal), (Position2D("baz", 2), Discrete),
    (Position2D("foo", 1), Ordinal), (Position2D("foo", 2), Continuous), (Position2D("foo", 3), Nominal),
    (Position2D("foo", 4), Date), (Position2D("qux", 1), Ordinal))
}

class TestScaldingMatrixTypes extends TestMatrixTypes with TBddDsl {

  "A Matrix.types" should "return its first over types in 1D" in {
    Given {
      data1
    } When {
      cells: TypedPipe[Cell[Position1D]] =>
        cells.types(Over(First), false)
    } Then {
      _.toList.sortBy(_._1) shouldBe result1
    }
  }

  it should "return its first over specific types in 1D" in {
    Given {
      data1
    } When {
      cells: TypedPipe[Cell[Position1D]] =>
        cells.types(Over(First), true)
    } Then {
      _.toList.sortBy(_._1) shouldBe result2
    }
  }

  it should "return its first over types in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.types(Over(First), false)
    } Then {
      _.toList.sortBy(_._1) shouldBe result3
    }
  }

  it should "return its first over specific types in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.types(Over(First), true)
    } Then {
      _.toList.sortBy(_._1) shouldBe result4
    }
  }

  it should "return its first along types in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.types(Along(First), false)
    } Then {
      _.toList.sortBy(_._1) shouldBe result5
    }
  }

  it should "return its first along specific types in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.types(Along(First), true)
    } Then {
      _.toList.sortBy(_._1) shouldBe result6
    }
  }

  it should "return its second over types in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.types(Over(Second), false)
    } Then {
       _.toList.sortBy(_._1) shouldBe result7
    }
  }

  it should "return its second over specific types in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.types(Over(Second), true)
    } Then {
      _.toList.sortBy(_._1) shouldBe result8
    }
  }

  it should "return its second along types in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.types(Along(Second), false)
    } Then {
      _.toList.sortBy(_._1) shouldBe result9
    }
  }

  it should "return its second along specific types in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.types(Along(Second), true)
    } Then {
      _.toList.sortBy(_._1) shouldBe result10
    }
  }

  it should "return its first over types in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.types(Over(First), false)
    } Then {
      _.toList.sortBy(_._1) shouldBe result11
    }
  }

  it should "return its first over specific types in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.types(Over(First), true)
    } Then {
      _.toList.sortBy(_._1) shouldBe result12
    }
  }

  it should "return its first along types in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.types(Along(First), false)
    } Then {
      _.toList.sortBy(_._1) shouldBe result13
    }
  }

  it should "return its first along specific types in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.types(Along(First), true)
    } Then {
      _.toList.sortBy(_._1) shouldBe result14
    }
  }

  it should "return its second over types in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.types(Over(Second), false)
    } Then {
      _.toList.sortBy(_._1) shouldBe result15
    }
  }

  it should "return its second over specific types in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.types(Over(Second), true)
    } Then {
      _.toList.sortBy(_._1) shouldBe result16
    }
  }

  it should "return its second along types in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.types(Along(Second), false)
    } Then {
      _.toList.sortBy(_._1) shouldBe result17
    }
  }

  it should "return its second along specific types in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.types(Along(Second), true)
    } Then {
      _.toList.sortBy(_._1) shouldBe result18
    }
  }

  it should "return its third over types in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.types(Over(Third), false)
    } Then {
      _.toList.sortBy(_._1) shouldBe result19
    }
  }

  it should "return its third over specific types in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.types(Over(Third), true)
    } Then {
      _.toList.sortBy(_._1) shouldBe result20
    }
  }

  it should "return its third along types in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.types(Along(Third), false)
    } Then {
      _.toList.sortBy(_._1) shouldBe result21
    }
  }

  it should "return its third along specific types in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.types(Along(Third), true)
    } Then {
      _.toList.sortBy(_._1) shouldBe result22
    }
  }
}

class TestSparkMatrixTypes extends TestMatrixTypes {

  "A Matrix.types" should "return its first over types in 1D" in {
    toRDD(data1)
      .types(Over(First), false)
      .toList.sortBy(_._1) shouldBe result1
  }

  it should "return its first over specific types in 1D" in {
    toRDD(data1)
      .types(Over(First), true)
      .toList.sortBy(_._1) shouldBe result2
  }

  it should "return its first over types in 2D" in {
    toRDD(data2)
      .types(Over(First), false)
      .toList.sortBy(_._1) shouldBe result3
  }

  it should "return its first over specific types in 2D" in {
    toRDD(data2)
      .types(Over(First), true)
      .toList.sortBy(_._1) shouldBe result4
  }

  it should "return its first along types in 2D" in {
    toRDD(data2)
      .types(Along(First), false)
      .toList.sortBy(_._1) shouldBe result5
  }

  it should "return its first along specific types in 2D" in {
    toRDD(data2)
      .types(Along(First), true)
      .toList.sortBy(_._1) shouldBe result6
  }

  it should "return its second over types in 2D" in {
    toRDD(data2)
      .types(Over(Second), false)
      .toList.sortBy(_._1) shouldBe result7
  }

  it should "return its second over specific types in 2D" in {
    toRDD(data2)
      .types(Over(Second), true)
      .toList.sortBy(_._1) shouldBe result8
  }

  it should "return its second along types in 2D" in {
    toRDD(data2)
      .types(Along(Second), false)
      .toList.sortBy(_._1) shouldBe result9
  }

  it should "return its second along specific types in 2D" in {
    toRDD(data2)
      .types(Along(Second), true)
      .toList.sortBy(_._1) shouldBe result10
  }

  it should "return its first over types in 3D" in {
    toRDD(data3)
      .types(Over(First), false)
      .toList.sortBy(_._1) shouldBe result11
  }

  it should "return its first over specific types in 3D" in {
    toRDD(data3)
      .types(Over(First), true)
      .toList.sortBy(_._1) shouldBe result12
  }

  it should "return its first along types in 3D" in {
    toRDD(data3)
      .types(Along(First), false)
      .toList.sortBy(_._1) shouldBe result13
  }

  it should "return its first along specific types in 3D" in {
    toRDD(data3)
      .types(Along(First), true)
      .toList.sortBy(_._1) shouldBe result14
  }

  it should "return its second over types in 3D" in {
    toRDD(data3)
      .types(Over(Second), false)
      .toList.sortBy(_._1) shouldBe result15
  }

  it should "return its second over specific types in 3D" in {
    toRDD(data3)
      .types(Over(Second), true)
      .toList.sortBy(_._1) shouldBe result16
  }

  it should "return its second along types in 3D" in {
    toRDD(data3)
      .types(Along(Second), false)
      .toList.sortBy(_._1) shouldBe result17
  }

  it should "return its second along specific types in 3D" in {
    toRDD(data3)
      .types(Along(Second), true)
      .toList.sortBy(_._1) shouldBe result18
  }

  it should "return its third over types in 3D" in {
    toRDD(data3)
      .types(Over(Third), false)
      .toList.sortBy(_._1) shouldBe result19
  }

  it should "return its third over specific types in 3D" in {
    toRDD(data3)
      .types(Over(Third), true)
      .toList.sortBy(_._1) shouldBe result20
  }

  it should "return its third along types in 3D" in {
    toRDD(data3)
      .types(Along(Third), false)
      .toList.sortBy(_._1) shouldBe result21
  }

  it should "return its third along specific types in 3D" in {
    toRDD(data3)
      .types(Along(Third), true)
      .toList.sortBy(_._1) shouldBe result22
  }
}

trait TestMatrixSize extends TestMatrix {

  val dataA = List(Cell(Position2D(1, 1), Content(OrdinalSchema[Codex.StringCodex](), "a")),
    Cell(Position2D(2, 2), Content(OrdinalSchema[Codex.StringCodex](), "b")),
    Cell(Position2D(3, 3), Content(OrdinalSchema[Codex.StringCodex](), "c")))

  val result1 = List(Cell(Position1D("First"), Content(DiscreteSchema[Codex.LongCodex](), 4)))

  val result2 = List(Cell(Position1D("First"), Content(DiscreteSchema[Codex.LongCodex](), 4)))

  val result3 = List(Cell(Position1D("First"), Content(DiscreteSchema[Codex.LongCodex](), 4)))

  val result4 = List(Cell(Position1D("First"), Content(DiscreteSchema[Codex.LongCodex](), data2.length)))

  val result5 = List(Cell(Position1D("Second"), Content(DiscreteSchema[Codex.LongCodex](), 4)))

  val result6 = List(Cell(Position1D("Second"), Content(DiscreteSchema[Codex.LongCodex](), data2.length)))

  val result7 = List(Cell(Position1D("First"), Content(DiscreteSchema[Codex.LongCodex](), 4)))

  val result8 = List(Cell(Position1D("First"), Content(DiscreteSchema[Codex.LongCodex](), data3.length)))

  val result9 = List(Cell(Position1D("Second"), Content(DiscreteSchema[Codex.LongCodex](), 4)))

  val result10 = List(Cell(Position1D("Second"), Content(DiscreteSchema[Codex.LongCodex](), data3.length)))

  val result11 = List(Cell(Position1D("Third"), Content(DiscreteSchema[Codex.LongCodex](), 1)))

  val result12 = List(Cell(Position1D("Third"), Content(DiscreteSchema[Codex.LongCodex](), data3.length)))

  val result13 = List(Cell(Position1D("Second"), Content(DiscreteSchema[Codex.LongCodex](), 3)))
}

class TestScaldingMatrixSize extends TestMatrixSize with TBddDsl {

  "A Matrix.size" should "return its first size in 1D" in {
    Given {
      data1
    } When {
      cells: TypedPipe[Cell[Position1D]] =>
        cells.size(First, false)
    } Then {
      _.toList shouldBe result1
    }
  }

  it should "return its first distinct size in 1D" in {
    Given {
      data1
    } When {
      cells: TypedPipe[Cell[Position1D]] =>
        cells.size(First, true)
    } Then {
      _.toList shouldBe result2
    }
  }

  it should "return its first size in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.size(First, false)
    } Then {
      _.toList shouldBe result3
    }
  }

  it should "return its first distinct size in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.size(First, true)
    } Then {
      _.toList shouldBe result4
    }
  }

  it should "return its second size in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.size(Second, false)
    } Then {
      _.toList shouldBe result5
    }
  }

  it should "return its second distinct size in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.size(Second, true)
    } Then {
      _.toList shouldBe result6
    }
  }

  it should "return its first size in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.size(First, false)
    } Then {
      _.toList shouldBe result7
    }
  }

  it should "return its first distinct size in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.size(First, true)
    } Then {
      _.toList shouldBe result8
    }
  }

  it should "return its second size in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.size(Second, false)
    } Then {
      _.toList shouldBe result9
    }
  }

  it should "return its second distinct size in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.size(Second, true)
    } Then {
      _.toList shouldBe result10
    }
  }

  it should "return its third size in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.size(Third, false)
    } Then {
      _.toList shouldBe result11
    }
  }

  it should "return its third distinct size in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.size(Third, true)
    } Then {
      _.toList shouldBe result12
    }
  }

  it should "return its distinct size" in {
    Given {
      dataA
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.size(Second, true)
    } Then {
      _.toList shouldBe result13
    }
  }
}

class TestSparkMatrixSize extends TestMatrixSize {

  "A Matrix.size" should "return its first size in 1D" in {
    toRDD(data1)
      .size(First, false)
      .toList shouldBe result1
  }

  it should "return its first distinct size in 1D" in {
    toRDD(data1)
      .size(First, true)
      .toList shouldBe result2
  }

  it should "return its first size in 2D" in {
    toRDD(data2)
      .size(First, false)
      .toList shouldBe result3
  }

  it should "return its first distinct size in 2D" in {
    toRDD(data2)
      .size(First, true)
      .toList shouldBe result4
  }

  it should "return its second size in 2D" in {
    toRDD(data2)
      .size(Second, false)
      .toList shouldBe result5
  }

  it should "return its second distinct size in 2D" in {
    toRDD(data2)
      .size(Second, true)
      .toList shouldBe result6
  }

  it should "return its first size in 3D" in {
    toRDD(data3)
      .size(First, false)
      .toList shouldBe result7
  }

  it should "return its first distinct size in 3D" in {
    toRDD(data3)
      .size(First, true)
      .toList shouldBe result8
  }

  it should "return its second size in 3D" in {
    toRDD(data3)
      .size(Second, false)
      .toList shouldBe result9
  }

  it should "return its second distinct size in 3D" in {
    toRDD(data3)
      .size(Second, true)
      .toList shouldBe result10
  }

  it should "return its third size in 3D" in {
    toRDD(data3)
      .size(Third, false)
      .toList shouldBe result11
  }

  it should "return its third distinct size in 3D" in {
    toRDD(data3)
      .size(Third, true)
      .toList shouldBe result12
  }

  it should "return its distinct size" in {
    toRDD(dataA)
      .size(Second, true)
      .toList shouldBe result13
  }
}

trait TestMatrixShape extends TestMatrix {

  val result1 = List(Cell(Position1D("First"), Content(DiscreteSchema[Codex.LongCodex](), 4)))

  val result2 = List(Cell(Position1D("First"), Content(DiscreteSchema[Codex.LongCodex](), 4)),
    Cell(Position1D("Second"), Content(DiscreteSchema[Codex.LongCodex](), 4)))

  val result3 = List(Cell(Position1D("First"), Content(DiscreteSchema[Codex.LongCodex](), 4)),
    Cell(Position1D("Second"), Content(DiscreteSchema[Codex.LongCodex](), 4)),
    Cell(Position1D("Third"), Content(DiscreteSchema[Codex.LongCodex](), 1)))
}

class TestScaldingMatrixShape extends TestMatrixShape with TBddDsl {

  "A Matrix.shape" should "return its shape in 1D" in {
    Given {
      data1
    } When {
      cells: TypedPipe[Cell[Position1D]] =>
        cells.shape()
    } Then {
      _.toList shouldBe result1
    }
  }

  it should "return its shape in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.shape()
    } Then {
      _.toList shouldBe result2
    }
  }

  it should "return its shape in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.shape()
    } Then {
      _.toList shouldBe result3
    }
  }
}

class TestSparkMatrixShape extends TestMatrixShape {

  "A Matrix.shape" should "return its shape in 1D" in {
    toRDD(data1)
      .shape()
      .toList shouldBe result1
  }

  it should "return its shape in 2D" in {
    toRDD(data2)
      .shape()
      .toList shouldBe result2
  }

  it should "return its shape in 3D" in {
    toRDD(data3)
      .shape()
      .toList shouldBe result3
  }
}

trait TestMatrixSlice extends TestMatrix {

  val result1 = List(Cell(Position1D("baz"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position1D("foo"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")))

  val result2 = List(Cell(Position1D("bar"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
    Cell(Position1D("qux"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))

  val result3 = List(Cell(Position2D("baz", 1), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position2D("baz", 2), Content(DiscreteSchema[Codex.LongCodex](), 19)),
    Cell(Position2D("foo", 1), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
    Cell(Position2D("foo", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position2D("foo", 3), Content(NominalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position2D("foo", 4), Content(DateSchema[Codex.DateTimeCodex](),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))

  val result4 = List(Cell(Position2D("bar", 1), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
    Cell(Position2D("bar", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position2D("bar", 3), Content(OrdinalSchema[Codex.LongCodex](), 19)),
    Cell(Position2D("qux", 1), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))

  val result5 = List(Cell(Position2D("bar", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position2D("baz", 2), Content(DiscreteSchema[Codex.LongCodex](), 19)),
    Cell(Position2D("foo", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position2D("foo", 4), Content(DateSchema[Codex.DateTimeCodex](),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))

  val result6 = List(Cell(Position2D("bar", 1), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
    Cell(Position2D("bar", 3), Content(OrdinalSchema[Codex.LongCodex](), 19)),
    Cell(Position2D("baz", 1), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position2D("foo", 1), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
    Cell(Position2D("foo", 3), Content(NominalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position2D("qux", 1), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))

  val result7 = List(Cell(Position2D("bar", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position2D("baz", 2), Content(DiscreteSchema[Codex.LongCodex](), 19)),
    Cell(Position2D("foo", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position2D("foo", 4), Content(DateSchema[Codex.DateTimeCodex](),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))

  val result8 = List(Cell(Position2D("bar", 1), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
    Cell(Position2D("bar", 3), Content(OrdinalSchema[Codex.LongCodex](), 19)),
    Cell(Position2D("baz", 1), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position2D("foo", 1), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
    Cell(Position2D("foo", 3), Content(NominalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position2D("qux", 1), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))

  val result9 = List(Cell(Position2D("baz", 1), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position2D("baz", 2), Content(DiscreteSchema[Codex.LongCodex](), 19)),
    Cell(Position2D("foo", 1), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
    Cell(Position2D("foo", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position2D("foo", 3), Content(NominalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position2D("foo", 4), Content(DateSchema[Codex.DateTimeCodex](),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))

  val result10 = List(Cell(Position2D("bar", 1), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
    Cell(Position2D("bar", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position2D("bar", 3), Content(OrdinalSchema[Codex.LongCodex](), 19)),
    Cell(Position2D("qux", 1), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))

  val result11 = List(Cell(Position3D("baz", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position3D("baz", 2, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
    Cell(Position3D("foo", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
    Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position3D("foo", 3, "xyz"), Content(NominalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position3D("foo", 4, "xyz"), Content(DateSchema[Codex.DateTimeCodex](),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))

  val result12 = List(Cell(Position3D("bar", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
    Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position3D("bar", 3, "xyz"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
    Cell(Position3D("qux", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))

  val result13 = List(Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position3D("baz", 2, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
    Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position3D("foo", 4, "xyz"), Content(DateSchema[Codex.DateTimeCodex](),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))

  val result14 = List(Cell(Position3D("bar", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
    Cell(Position3D("bar", 3, "xyz"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
    Cell(Position3D("baz", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position3D("foo", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
    Cell(Position3D("foo", 3, "xyz"), Content(NominalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position3D("qux", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))

  val result15 = List(Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position3D("baz", 2, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
    Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position3D("foo", 4, "xyz"), Content(DateSchema[Codex.DateTimeCodex](),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))

  val result16 = List(Cell(Position3D("bar", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
    Cell(Position3D("bar", 3, "xyz"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
    Cell(Position3D("baz", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position3D("foo", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
    Cell(Position3D("foo", 3, "xyz"), Content(NominalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position3D("qux", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))

  val result17 = List(Cell(Position3D("baz", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position3D("baz", 2, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
    Cell(Position3D("foo", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
    Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position3D("foo", 3, "xyz"), Content(NominalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position3D("foo", 4, "xyz"), Content(DateSchema[Codex.DateTimeCodex](),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))

  val result18 = List(Cell(Position3D("bar", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
    Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position3D("bar", 3, "xyz"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
    Cell(Position3D("qux", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))

  val result19 = List()

  val result20 = data3.sortBy(_.position)

  val result21 = List(Cell(Position3D("bar", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
    Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position3D("bar", 3, "xyz"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
    Cell(Position3D("baz", 2, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
    Cell(Position3D("foo", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
    Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position3D("foo", 4, "xyz"), Content(DateSchema[Codex.DateTimeCodex](),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
    Cell(Position3D("qux", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))

  val result22 = List(Cell(Position3D("baz", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position3D("foo", 3, "xyz"), Content(NominalSchema[Codex.StringCodex](), "9.42")))
}

class TestScaldingMatrixSlice extends TestMatrixSlice with TBddDsl {

  "A Matrix.slice" should "return its first over slice in 1D" in {
    Given {
      data1
    } When {
      cells: TypedPipe[Cell[Position1D]] =>
        cells.slice(Over(First), List("bar", "qux"), false)
    } Then {
      _.toList.sortBy(_.position) shouldBe result1
    }
  }

  it should "return its first over inverse slice in 1D" in {
    Given {
      data1
    } When {
      cells: TypedPipe[Cell[Position1D]] =>
        cells.slice(Over(First), List("bar", "qux"), true)
    } Then {
      _.toList.sortBy(_.position) shouldBe result2
    }
  }

  it should "return its first over slice in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.slice(Over(First), List("bar", "qux"), false)
    } Then {
      _.toList.sortBy(_.position) shouldBe result3
    }
  }

  it should "return its first over inverse slice in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.slice(Over(First), List("bar", "qux"), true)
    } Then {
      _.toList.sortBy(_.position) shouldBe result4
    }
  }

  it should "return its first along slice in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.slice(Along(First), List(1, 3), false)
    } Then {
      _.toList.sortBy(_.position) shouldBe result5
    }
  }

  it should "return its first along inverse slice in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.slice(Along(First), List(1, 3), true)
    } Then {
      _.toList.sortBy(_.position) shouldBe result6
    }
  }

  it should "return its second over slice in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.slice(Over(Second), List(1, 3), false)
    } Then {
      _.toList.sortBy(_.position) shouldBe result7
    }
  }

  it should "return its second over inverse slice in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.slice(Over(Second), List(1, 3), true)
    } Then {
      _.toList.sortBy(_.position) shouldBe result8
    }
  }

  it should "return its second along slice in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.slice(Along(Second), List("bar", "qux"), false)
    } Then {
      _.toList.sortBy(_.position) shouldBe result9
    }
  }

  it should "return its second along inverse slice in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.slice(Along(Second), List("bar", "qux"), true)
    } Then {
      _.toList.sortBy(_.position) shouldBe result10
    }
  }

  it should "return its first over slice in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.slice(Over(First), List("bar", "qux"), false)
    } Then {
      _.toList.sortBy(_.position) shouldBe result11
    }
  }

  it should "return its first over inverse slice in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.slice(Over(First), List("bar", "qux"), true)
    } Then {
      _.toList.sortBy(_.position) shouldBe result12
    }
  }

  it should "return its first along slice in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.slice(Along(First), List(Position2D(1, "xyz"), Position2D(3, "xyz")), false)
    } Then {
      _.toList.sortBy(_.position) shouldBe result13
    }
  }

  it should "return its first along inverse slice in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.slice(Along(First), List(Position2D(1, "xyz"), Position2D(3, "xyz")), true)
    } Then {
      _.toList.sortBy(_.position) shouldBe result14
    }
  }

  it should "return its second over slice in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.slice(Over(Second), List(1, 3), false)
    } Then {
      _.toList.sortBy(_.position) shouldBe result15
    }
  }

  it should "return its second over inverse slice in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.slice(Over(Second), List(1, 3), true)
    } Then {
      _.toList.sortBy(_.position) shouldBe result16
    }
  }

  it should "return its second along slice in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.slice(Along(Second), List(Position2D("bar", "xyz"), Position2D("qux", "xyz")), false)
    } Then {
      _.toList.sortBy(_.position) shouldBe result17
    }
  }

  it should "return its second along inverse slice in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.slice(Along(Second), List(Position2D("bar", "xyz"), Position2D("qux", "xyz")), true)
    } Then {
      _.toList.sortBy(_.position) shouldBe result18
    }
  }

  it should "return its third over slice in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.slice(Over(Third), "xyz", false)
    } Then {
      _.toList.sortBy(_.position) shouldBe result19
    }
  }

  it should "return its third over inverse slice in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.slice(Over(Third), "xyz", true)
    } Then {
      _.toList.sortBy(_.position) shouldBe result20
    }
  }

  it should "return its third along slice in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.slice(Along(Third), List(Position2D("foo", 3), Position2D("baz", 1)), false)
    } Then {
      _.toList.sortBy(_.position) shouldBe result21
    }
  }

  it should "return its third along inverse slice in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.slice(Along(Third), List(Position2D("foo", 3), Position2D("baz", 1)), true)
    } Then {
      _.toList.sortBy(_.position) shouldBe result22
    }
  }
}

class TestSparkMatrixSlice extends TestMatrixSlice {

  "A Matrix.slice" should "return its first over slice in 1D" in {
    toRDD(data1)
      .slice(Over(First), List("bar", "qux"), false)
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first over inverse slice in 1D" in {
    toRDD(data1)
      .slice(Over(First), List("bar", "qux"), true)
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its first over slice in 2D" in {
    toRDD(data2)
      .slice(Over(First), List("bar", "qux"), false)
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its first over inverse slice in 2D" in {
    toRDD(data2)
      .slice(Over(First), List("bar", "qux"), true)
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its first along slice in 2D" in {
    toRDD(data2)
      .slice(Along(First), List(1, 3), false)
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its first along inverse slice in 2D" in {
    toRDD(data2)
      .slice(Along(First), List(1, 3), true)
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its second over slice in 2D" in {
    toRDD(data2)
      .slice(Over(Second), List(1, 3), false)
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its second over inverse slice in 2D" in {
    toRDD(data2)
      .slice(Over(Second), List(1, 3), true)
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its second along slice in 2D" in {
    toRDD(data2)
      .slice(Along(Second), List("bar", "qux"), false)
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its second along inverse slice in 2D" in {
    toRDD(data2)
      .slice(Along(Second), List("bar", "qux"), true)
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its first over slice in 3D" in {
    toRDD(data3)
      .slice(Over(First), List("bar", "qux"), false)
      .toList.sortBy(_.position) shouldBe result11
  }

  it should "return its first over inverse slice in 3D" in {
    toRDD(data3)
      .slice(Over(First), List("bar", "qux"), true)
      .toList.sortBy(_.position) shouldBe result12
  }

  it should "return its first along slice in 3D" in {
    toRDD(data3)
      .slice(Along(First), List(Position2D(1, "xyz"), Position2D(3, "xyz")), false)
      .toList.sortBy(_.position) shouldBe result13
  }

  it should "return its first along inverse slice in 3D" in {
    toRDD(data3)
      .slice(Along(First), List(Position2D(1, "xyz"), Position2D(3, "xyz")), true)
      .toList.sortBy(_.position) shouldBe result14
  }

  it should "return its second over slice in 3D" in {
    toRDD(data3)
      .slice(Over(Second), List(1, 3), false)
      .toList.sortBy(_.position) shouldBe result15
  }

  it should "return its second over inverse slice in 3D" in {
    toRDD(data3)
      .slice(Over(Second), List(1, 3), true)
      .toList.sortBy(_.position) shouldBe result16
  }

  it should "return its second along slice in 3D" in {
    toRDD(data3)
      .slice(Along(Second), List(Position2D("bar", "xyz"), Position2D("qux", "xyz")), false)
      .toList.sortBy(_.position) shouldBe result17
  }

  it should "return its second along inverse slice in 3D" in {
    toRDD(data3)
      .slice(Along(Second), List(Position2D("bar", "xyz"), Position2D("qux", "xyz")), true)
      .toList.sortBy(_.position) shouldBe result18
  }

  it should "return its third over slice in 3D" in {
    toRDD(data3)
      .slice(Over(Third), "xyz", false)
      .toList.sortBy(_.position) shouldBe result19
  }

  it should "return its third over inverse slice in 3D" in {
    toRDD(data3)
      .slice(Over(Third), "xyz", true)
      .toList.sortBy(_.position) shouldBe result20
  }

  it should "return its third along slice in 3D" in {
    toRDD(data3)
      .slice(Along(Third), List(Position2D("foo", 3), Position2D("baz", 1)), false)
      .toList.sortBy(_.position) shouldBe result21
  }

  it should "return its third along inverse slice in 3D" in {
    toRDD(data3)
      .slice(Along(Third), List(Position2D("foo", 3), Position2D("baz", 1)), true)
      .toList.sortBy(_.position) shouldBe result22
  }
}

trait TestMatrixWhich extends TestMatrix {

  val result1 = List(Position1D("qux"))

  val result2 = List(Position1D("qux"))

  val result3 = List(Position1D("foo"), Position1D("qux"))

  val result4 = List(Position2D("foo", 3), Position2D("foo", 4), Position2D("qux", 1))

  val result5 = List(Position2D("qux", 1))

  val result6 = List(Position2D("foo", 4))

  val result7 = List(Position2D("foo", 4))

  val result8 = List(Position2D("qux", 1))

  val result9 = List(Position2D("foo", 1), Position2D("foo", 2), Position2D("qux", 1))

  val result10 = List(Position2D("bar", 2), Position2D("baz", 2), Position2D("foo", 2), Position2D("foo", 4))

  val result11 = List(Position2D("bar", 2), Position2D("baz", 2), Position2D("foo", 2), Position2D("foo", 4))

  val result12 = List(Position2D("foo", 1), Position2D("foo", 2), Position2D("qux", 1))

  val result13 = List(Position3D("foo", 3, "xyz"), Position3D("foo", 4, "xyz"), Position3D("qux", 1, "xyz"))

  val result14 = List(Position3D("qux", 1, "xyz"))

  val result15 = List(Position3D("foo", 4, "xyz"))

  val result16 = List(Position3D("foo", 4, "xyz"))

  val result17 = List(Position3D("qux", 1, "xyz"))

  val result18 = List(Position3D("foo", 3, "xyz"), Position3D("foo", 4, "xyz"), Position3D("qux", 1, "xyz"))

  val result19 = List(Position3D("qux", 1, "xyz"))

  val result20 = List(Position3D("foo", 1, "xyz"), Position3D("foo", 2, "xyz"), Position3D("qux", 1, "xyz"))

  val result21 = List(Position3D("bar", 2, "xyz"), Position3D("baz", 2, "xyz"), Position3D("foo", 2, "xyz"),
    Position3D("foo", 4, "xyz"))

  val result22 = List(Position3D("bar", 2, "xyz"), Position3D("baz", 2, "xyz"), Position3D("foo", 2, "xyz"),
    Position3D("foo", 4, "xyz"))

  val result23 = List(Position3D("foo", 1, "xyz"), Position3D("foo", 2, "xyz"), Position3D("qux", 1, "xyz"))

  val result24 = data3.map(_.position).sorted

  val result25 = List(Position3D("foo", 2, "xyz"), Position3D("qux", 1, "xyz"))
}

object TestMatrixWhich {

  def predicate[P <: Position](cell: Cell[P]): Boolean = {
    (cell.content.schema == NominalSchema[Codex.StringCodex]()) ||
    (cell.content.schema.codex == DateTimeCodex) ||
    (cell.content.value equ "12.56")
  }
}

class TestScaldingMatrixWhich extends TestMatrixWhich with TBddDsl {

  "A Matrix.which" should "return its coordinates in 1D" in {
    Given {
      data1
    } When {
      cells: TypedPipe[Cell[Position1D]] =>
        cells.which(TestMatrixWhich.predicate)
    } Then {
      _.toList.sorted shouldBe result1
    }
  }

  it should "return its first over coordinates in 1D" in {
    Given {
      data1
    } When {
      cells: TypedPipe[Cell[Position1D]] =>
        cells.which(Over(First), List("bar", "qux"), TestMatrixWhich.predicate)
    } Then {
      _.toList.sorted shouldBe result2
    }
  }

  it should "return its first over multiple coordinates in 1D" in {
    Given {
      data1
    } When {
      cells: TypedPipe[Cell[Position1D]] =>
        cells.which(Over(First), List((List("bar", "qux"), (c: Cell[Position1D]) => TestMatrixWhich.predicate(c)),
          (List("foo"), (c: Cell[Position1D]) => !TestMatrixWhich.predicate(c))))
    } Then {
      _.toList.sorted shouldBe result3
    }
  }

  it should "return its coordinates in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.which(TestMatrixWhich.predicate)
    } Then {
      _.toList.sorted shouldBe result4
    }
  }

  it should "return its first over coordinates in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.which(Over(First), List("bar", "qux"), TestMatrixWhich.predicate)
    } Then {
      _.toList.sorted shouldBe result5
    }
  }

  it should "return its first along coordinates in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.which(Along(First), List(2, 4), TestMatrixWhich.predicate)
    } Then {
      _.toList.sorted shouldBe result6
    }
  }

  it should "return its second over coordinates in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.which(Over(Second), List(2, 4), TestMatrixWhich.predicate)
    } Then {
      _.toList.sorted shouldBe result7
    }
  }

  it should "return its second along coordinates in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.which(Along(Second), List("bar", "qux"), TestMatrixWhich.predicate)
    } Then {
      _.toList.sorted shouldBe result8
    }
  }

  it should "return its first over multiple coordinates in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.which(Over(First), List((List("bar", "qux"), (c: Cell[Position2D]) => TestMatrixWhich.predicate(c)),
          (List("foo"), (c: Cell[Position2D]) => !TestMatrixWhich.predicate(c))))
    } Then {
      _.toList.sorted shouldBe result9
    }
  }

  it should "return its first along multiple coordinates in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.which(Along(First), List((List(2, 4), (c: Cell[Position2D]) => TestMatrixWhich.predicate(c)),
          (List(2), (c: Cell[Position2D]) => !TestMatrixWhich.predicate(c))))
    } Then {
      _.toList.sorted shouldBe result10
    }
  }

  it should "return its second over multiple coordinates in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.which(Over(Second), List((List(2, 4), (c: Cell[Position2D]) => TestMatrixWhich.predicate(c)),
          (List(2), (c: Cell[Position2D]) => !TestMatrixWhich.predicate(c))))
    } Then {
      _.toList.sorted shouldBe result11
    }
  }

  it should "return its second along multiple coordinates in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.which(Along(Second), List((List("bar", "qux"), (c: Cell[Position2D]) => TestMatrixWhich.predicate(c)),
          (List("foo"), (c: Cell[Position2D]) => !TestMatrixWhich.predicate(c))))
    } Then {
      _.toList.sorted shouldBe result12
    }
  }

  it should "return its coordinates in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.which(TestMatrixWhich.predicate)
    } Then {
      _.toList.sorted shouldBe result13
    }
  }

  it should "return its first over coordinates in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.which(Over(First), List("bar", "qux"), TestMatrixWhich.predicate)
    } Then {
      _.toList.sorted shouldBe result14
    }
  }

  it should "return its first along coordinates in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.which(Along(First), List(Position2D(2, "xyz"), Position2D(4, "xyz")), TestMatrixWhich.predicate)
    } Then {
      _.toList.sorted shouldBe result15
    }
  }

  it should "return its second over coordinates in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.which(Over(Second), List(2, 4), TestMatrixWhich.predicate)
    } Then {
      _.toList.sorted shouldBe result16
    }
  }

  it should "return its second along coordinates in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.which(Along(Second), List(Position2D("bar", "xyz"), Position2D("qux", "xyz")), TestMatrixWhich.predicate)
    } Then {
      _.toList.sorted shouldBe result17
    }
  }

  it should "return its third over coordinates in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.which(Over(Third), "xyz", TestMatrixWhich.predicate)
    } Then {
      _.toList.sorted shouldBe result18
    }
  }

  it should "return its third along coordinates in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.which(Along(Third), List(Position2D("bar", 2), Position2D("qux", 1)), TestMatrixWhich.predicate)
    } Then {
      _.toList.sorted shouldBe result19
    }
  }

  it should "return its first over multiple coordinates in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.which(Over(First), List((List("bar", "qux"), (c: Cell[Position3D]) => TestMatrixWhich.predicate(c)),
          (List("foo"), (c: Cell[Position3D]) => !TestMatrixWhich.predicate(c))))
    } Then {
      _.toList.sorted shouldBe result20
    }
  }

  it should "return its first along multiple coordinates in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.which(Along(First), List((List(Position2D(2, "xyz"), Position2D(4, "xyz")),
          (c: Cell[Position3D]) => TestMatrixWhich.predicate(c)), (List(Position2D(2, "xyz")),
          (c: Cell[Position3D]) => !TestMatrixWhich.predicate(c))))
    } Then {
      _.toList.sorted shouldBe result21
    }
  }

  it should "return its second over multiple coordinates in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.which(Over(Second), List((List(2, 4), (c: Cell[Position3D]) => TestMatrixWhich.predicate(c)),
          (List(2), (c: Cell[Position3D]) => !TestMatrixWhich.predicate(c))))
    } Then {
      _.toList.sorted shouldBe result22
    }
  }

  it should "return its second along multiple coordinates in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.which(Along(Second), List((List(Position2D("bar", "xyz"), Position2D("qux", "xyz")),
          (c: Cell[Position3D]) => TestMatrixWhich.predicate(c)), (List(Position2D("foo", "xyz")),
          (c: Cell[Position3D]) => !TestMatrixWhich.predicate(c))))
    } Then {
      _.toList.sorted shouldBe result23
    }
  }

  it should "return its third over multiple coordinates in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.which(Over(Third), List(("xyz", (c: Cell[Position3D]) => TestMatrixWhich.predicate(c)),
          ("xyz", (c: Cell[Position3D]) => !TestMatrixWhich.predicate(c))))
    } Then {
      _.toList.sorted shouldBe result24
    }
  }

  it should "return its third along multiple coordinates in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.which(Along(Third), List((List(Position2D("foo", 1), Position2D("qux", 1)),
          (c: Cell[Position3D]) => TestMatrixWhich.predicate(c)), (List(Position2D("foo", 2)),
          (c: Cell[Position3D]) => !TestMatrixWhich.predicate(c))))
    } Then {
      _.toList.sorted shouldBe result25
    }
  }
}

class TestSparkMatrixWhich extends TestMatrixWhich {

  "A Matrix.which" should "return its coordinates in 1D" in {
    toRDD(data1)
      .which(TestMatrixWhich.predicate)
      .toList.sorted shouldBe result1
  }

  it should "return its first over coordinates in 1D" in {
    toRDD(data1)
      .which(Over(First), List("bar", "qux"), TestMatrixWhich.predicate)
      .toList.sorted shouldBe result2
  }

  it should "return its first over multiple coordinates in 1D" in {
    toRDD(data1)
      .which(Over(First), List((List("bar", "qux"), (c: Cell[Position1D]) => TestMatrixWhich.predicate(c)),
        (List("foo"), (c: Cell[Position1D]) => !TestMatrixWhich.predicate(c))))
      .toList.sorted shouldBe result3
  }

  it should "return its coordinates in 2D" in {
    toRDD(data2)
      .which(TestMatrixWhich.predicate)
      .toList.sorted shouldBe result4
  }

  it should "return its first over coordinates in 2D" in {
    toRDD(data2)
      .which(Over(First), List("bar", "qux"), TestMatrixWhich.predicate)
      .toList.sorted shouldBe result5
  }

  it should "return its first along coordinates in 2D" in {
    toRDD(data2)
      .which(Along(First), List(2, 4), TestMatrixWhich.predicate)
      .toList.sorted shouldBe result6
  }

  it should "return its second over coordinates in 2D" in {
    toRDD(data2)
      .which(Over(Second), List(2, 4), TestMatrixWhich.predicate)
      .toList.sorted shouldBe result7
  }

  it should "return its second along coordinates in 2D" in {
    toRDD(data2)
      .which(Along(Second), List("bar", "qux"), TestMatrixWhich.predicate)
      .toList.sorted shouldBe result8
  }

  it should "return its first over multiple coordinates in 2D" in {
    toRDD(data2)
      .which(Over(First), List((List("bar", "qux"), (c: Cell[Position2D]) => TestMatrixWhich.predicate(c)),
        (List("foo"), (c: Cell[Position2D]) => !TestMatrixWhich.predicate(c))))
      .toList.sorted shouldBe result9
  }

  it should "return its first along multiple coordinates in 2D" in {
    toRDD(data2)
      .which(Along(First), List((List(2, 4), (c: Cell[Position2D]) => TestMatrixWhich.predicate(c)),
        (List(2), (c: Cell[Position2D]) => !TestMatrixWhich.predicate(c))))
      .toList.sorted shouldBe result10
  }

  it should "return its second over multiple coordinates in 2D" in {
    toRDD(data2)
      .which(Over(Second), List((List(2, 4), (c: Cell[Position2D]) => TestMatrixWhich.predicate(c)),
        (List(2), (c: Cell[Position2D]) => !TestMatrixWhich.predicate(c))))
      .toList.sorted shouldBe result11
  }

  it should "return its second along multiple coordinates in 2D" in {
    toRDD(data2)
      .which(Along(Second), List((List("bar", "qux"), (c: Cell[Position2D]) => TestMatrixWhich.predicate(c)),
        (List("foo"), (c: Cell[Position2D]) => !TestMatrixWhich.predicate(c))))
      .toList.sorted shouldBe result12
  }

  it should "return its coordinates in 3D" in {
    toRDD(data3)
      .which(TestMatrixWhich.predicate)
      .toList.sorted shouldBe result13
  }

  it should "return its first over coordinates in 3D" in {
    toRDD(data3)
      .which(Over(First), List("bar", "qux"), TestMatrixWhich.predicate)
      .toList.sorted shouldBe result14
  }

  it should "return its first along coordinates in 3D" in {
    toRDD(data3)
      .which(Along(First), List(Position2D(2, "xyz"), Position2D(4, "xyz")), TestMatrixWhich.predicate)
      .toList.sorted shouldBe result15
  }

  it should "return its second over coordinates in 3D" in {
    toRDD(data3)
      .which(Over(Second), List(2, 4), TestMatrixWhich.predicate)
      .toList.sorted shouldBe result16
  }

  it should "return its second along coordinates in 3D" in {
    toRDD(data3)
      .which(Along(Second), List(Position2D("bar", "xyz"), Position2D("qux", "xyz")), TestMatrixWhich.predicate)
      .toList.sorted shouldBe result17
  }

  it should "return its third over coordinates in 3D" in {
    toRDD(data3)
      .which(Over(Third), "xyz", TestMatrixWhich.predicate)
      .toList.sorted shouldBe result18
  }

  it should "return its third along coordinates in 3D" in {
    toRDD(data3)
      .which(Along(Third), List(Position2D("bar", 2), Position2D("qux", 1)), TestMatrixWhich.predicate)
      .toList.sorted shouldBe result19
  }

  it should "return its first over multiple coordinates in 3D" in {
    toRDD(data3)
      .which(Over(First), List((List("bar", "qux"), (c: Cell[Position3D]) => TestMatrixWhich.predicate(c)),
        (List("foo"), (c: Cell[Position3D]) => !TestMatrixWhich.predicate(c))))
      .toList.sorted shouldBe result20
  }

  it should "return its first along multiple coordinates in 3D" in {
    toRDD(data3)
      .which(Along(First), List((List(Position2D(2, "xyz"), Position2D(4, "xyz")),
        (c: Cell[Position3D]) => TestMatrixWhich.predicate(c)), (List(Position2D(2, "xyz")),
        (c: Cell[Position3D]) => !TestMatrixWhich.predicate(c))))
      .toList.sorted shouldBe result21
  }

  it should "return its second over multiple coordinates in 3D" in {
    toRDD(data3)
      .which(Over(Second), List((List(2, 4), (c: Cell[Position3D]) => TestMatrixWhich.predicate(c)),
        (List(2), (c: Cell[Position3D]) => !TestMatrixWhich.predicate(c))))
      .toList.sorted shouldBe result22
  }

  it should "return its second along multiple coordinates in 3D" in {
    toRDD(data3)
      .which(Along(Second), List((List(Position2D("bar", "xyz"), Position2D("qux", "xyz")),
        (c: Cell[Position3D]) => TestMatrixWhich.predicate(c)), (List(Position2D("foo", "xyz")),
        (c: Cell[Position3D]) => !TestMatrixWhich.predicate(c))))
      .toList.sorted shouldBe result23
  }

  it should "return its third over multiple coordinates in 3D" in {
    toRDD(data3)
      .which(Over(Third), List(("xyz", (c: Cell[Position3D]) => TestMatrixWhich.predicate(c)),
        ("xyz", (c: Cell[Position3D]) => !TestMatrixWhich.predicate(c))))
      .toList.sorted shouldBe result24
  }

  it should "return its third along multiple coordinates in 3D" in {
    toRDD(data3)
      .which(Along(Third), List((List(Position2D("foo", 1), Position2D("qux", 1)),
        (c: Cell[Position3D]) => TestMatrixWhich.predicate(c)), (List(Position2D("foo", 2)),
        (c: Cell[Position3D]) => !TestMatrixWhich.predicate(c))))
      .toList.sorted shouldBe result25
  }
}

trait TestMatrixGet extends TestMatrix {

  val result1 = List(Cell(Position1D("qux"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))

  val result2 = List(Cell(Position2D("foo", 3), Content(NominalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position2D("qux", 1), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))

  val result3 = List(Cell(Position3D("foo", 3, "xyz"), Content(NominalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position3D("qux", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))
}

class TestScaldingMatrixGet extends TestMatrixGet with TBddDsl {

  "A Matrix.get" should "return its cells in 1D" in {
    Given {
      data1
    } When {
      cells: TypedPipe[Cell[Position1D]] =>
        cells.get("qux")
    } Then {
      _.toList.sortBy(_.position) shouldBe result1
    }
  }

  it should "return its cells in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.get(List(Position2D("foo", 3), Position2D("qux", 1), Position2D("baz", 4)))
    } Then {
      _.toList.sortBy(_.position) shouldBe result2
    }
  }

  it should "return its cells in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.get(List(Position3D("foo", 3, "xyz"), Position3D("qux", 1, "xyz"), Position3D("baz", 4, "xyz")))
    } Then {
      _.toList.sortBy(_.position) shouldBe result3
    }
  }
}

class TestSparkMatrixGet extends TestMatrixGet {

  "A Matrix.get" should "return its cells in 1D" in {
    toRDD(data1)
      .get("qux")
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its cells in 2D" in {
    toRDD(data2)
      .get(List(Position2D("foo", 3), Position2D("qux", 1), Position2D("baz", 4)))
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its cells in 3D" in {
    toRDD(data3)
      .get(List(Position3D("foo", 3, "xyz"), Position3D("qux", 1, "xyz"), Position3D("baz", 4, "xyz")))
      .toList.sortBy(_.position) shouldBe result3
  }
}

trait TestMatrixToMap extends TestMatrix {
  val result1 = data1.map { case c => c.position -> c.content }.toMap

  val result2 = Map(
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
        Position1D("qux") -> Map(Position1D(1) -> Content(OrdinalSchema[Codex.StringCodex](), "12.56")))

  val result3 = Map(
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
          (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))

  val result4 = Map(
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
          (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))

  val result5 = Map(
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
        Position1D("qux") -> Map(Position1D(1) -> Content(OrdinalSchema[Codex.StringCodex](), "12.56")))

  val result6 = Map(
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
        Position1D("qux") -> Map(Position2D(1, "xyz") -> Content(OrdinalSchema[Codex.StringCodex](), "12.56")))

  val result7 = Map(
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
          (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))

  val result8 = Map(
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
          (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))

  val result9 = Map(
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
        Position2D("qux", "xyz") -> Map(Position1D(1) -> Content(OrdinalSchema[Codex.StringCodex](), "12.56")))

  val result10 = Map(Position1D("xyz") -> Map(
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
          (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))

  val result11 = Map(
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
        Position2D("qux", 1) -> Map(Position1D("xyz") -> Content(OrdinalSchema[Codex.StringCodex](), "12.56")))
}

class TestScaldingMatrixToMap extends TestMatrixToMap with TBddDsl {

  "A Matrix.toMap" should "return its first over map in 1D" in {
    Given {
      data1
    } When {
      cells: TypedPipe[Cell[Position1D]] =>
        cells.toMap(Over(First)).toTypedPipe
    } Then {
      _.toList shouldBe List(result1)
    }
  }

  it should "return its first over map in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.toMap(Over(First)).toTypedPipe
    } Then {
      _.toList shouldBe List(result2)
    }
  }

  it should "return its first along map in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.toMap(Along(First)).toTypedPipe
    } Then {
      _.toList shouldBe List(result3)
    }
  }

  it should "return its second over map in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.toMap(Over(Second)).toTypedPipe
    } Then {
      _.toList shouldBe List(result4)
    }
  }

  it should "return its second along map in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.toMap(Along(Second)).toTypedPipe
    } Then {
      _.toList shouldBe List(result5)
    }
  }

  it should "return its first over map in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.toMap(Over(First)).toTypedPipe
    } Then {
      _.toList shouldBe List(result6)
    }
  }

  it should "return its first along map in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.toMap(Along(First)).toTypedPipe
    } Then {
      _.toList shouldBe List(result7)
    }
  }

  it should "return its second over map in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.toMap(Over(Second)).toTypedPipe
    } Then {
      _.toList shouldBe List(result8)
    }
  }

  it should "return its second along map in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.toMap(Along(Second)).toTypedPipe
    } Then {
      _.toList shouldBe List(result9)
    }
  }

  it should "return its third over map in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.toMap(Over(Third)).toTypedPipe
    } Then {
      _.toList shouldBe List(result10)
    }
  }

  it should "return its third along map in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.toMap(Along(Third)).toTypedPipe
    } Then {
      _.toList shouldBe List(result11)
    }
  }
}

class TestSparkMatrixToMap extends TestMatrixToMap {

  "A Matrix.toMap" should "return its first over map in 1D" in {
    toRDD(data1).toMap(Over(First)) shouldBe result1
  }

  it should "return its first over map in 2D" in {
    toRDD(data2).toMap(Over(First)) shouldBe result2
  }

  it should "return its first along map in 2D" in {
    toRDD(data2).toMap(Along(First)) shouldBe result3
  }

  it should "return its second over map in 2D" in {
    toRDD(data2).toMap(Over(Second)) shouldBe result4
  }

  it should "return its second along map in 2D" in {
    toRDD(data2).toMap(Along(Second)) shouldBe result5
  }

  it should "return its first over map in 3D" in {
    toRDD(data3).toMap(Over(First)) shouldBe result6
  }

  it should "return its first along map in 3D" in {
    toRDD(data3).toMap(Along(First)) shouldBe result7
  }

  it should "return its second over map in 3D" in {
    toRDD(data3).toMap(Over(Second)) shouldBe result8
  }

  it should "return its second along map in 3D" in {
    toRDD(data3).toMap(Along(Second)) shouldBe result9
  }

  it should "return its third over map in 3D" in {
    toRDD(data3).toMap(Over(Third)) shouldBe result10
  }

  it should "return its third along map in 3D" in {
    toRDD(data3).toMap(Along(Third)) shouldBe result11
  }
}

trait TestMatrixSummarise extends TestMatrix {

  val ext = Map(Position1D("foo") -> 1.0 / 1,
    Position1D("bar") -> 1.0 / 2,
    Position1D("baz") -> 1.0 / 3,
    Position1D("qux") -> 1.0 / 4,
    Position1D("foo.2") -> 1.0,
    Position1D("bar.2") -> 1.0,
    Position1D("baz.2") -> 1.0,
    Position1D("qux.2") -> 1.0,
    Position1D(1) -> 1.0 / 2,
    Position1D(2) -> 1.0 / 4,
    Position1D(3) -> 1.0 / 6,
    Position1D(4) -> 1.0 / 8,
    Position1D("1.2") -> 1.0,
    Position1D("2.2") -> 1.0,
    Position1D("3.2") -> 1.0,
    Position1D("4.2") -> 1.0,
    Position1D("xyz") -> 1 / 3.14,
    Position1D("xyz.2") -> 1 / 6.28)

  type W = Map[Position1D, Double]

  val result1 = List(Cell(Position1D("bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position1D("baz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
    Cell(Position1D("foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
    Cell(Position1D("qux"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)))

  val result2 = List(Cell(Position1D(1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position1D(2), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
    Cell(Position1D(3), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
    Cell(Position1D(4), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)))

  val result3 = List(Cell(Position1D(1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position1D(2), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
    Cell(Position1D(3), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
    Cell(Position1D(4), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)))

  val result4 = List(Cell(Position1D("bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position1D("baz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
    Cell(Position1D("foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
    Cell(Position1D("qux"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)))

  val result5 = List(Cell(Position1D("bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position1D("baz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
    Cell(Position1D("foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
    Cell(Position1D("qux"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)))

  val result6 = List(Cell(Position2D(1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position2D(2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
    Cell(Position2D(3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
    Cell(Position2D(4, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)))

  val result7 = List(Cell(Position1D(1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position1D(2), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
    Cell(Position1D(3), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
    Cell(Position1D(4), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)))

  val result8 = List(Cell(Position2D("bar", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position2D("baz", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
    Cell(Position2D("foo", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
    Cell(Position2D("qux", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)))

  val result9 = List(Cell(Position1D("xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)))

  val result10 = List(Cell(Position2D("bar", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position2D("bar", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position2D("bar", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
    Cell(Position2D("baz", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
    Cell(Position2D("baz", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
    Cell(Position2D("foo", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
    Cell(Position2D("foo", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position2D("foo", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
    Cell(Position2D("foo", 4), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position2D("qux", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)))

  val result11 = List(
    Cell(Position1D("bar"), Content(ContinuousSchema[Codex.DoubleCodex](), (6.28 + 12.56 + 18.84) * (1.0 / 2))),
    Cell(Position1D("baz"), Content(ContinuousSchema[Codex.DoubleCodex](), (9.42 + 18.84) * (1.0 / 3))),
    Cell(Position1D("foo"), Content(ContinuousSchema[Codex.DoubleCodex](), (3.14 + 6.28 + 9.42 + 12.56) * (1.0 / 1))),
    Cell(Position1D("qux"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 * (1.0 / 4))))

  val result12 = List(
    Cell(Position1D(1), Content(ContinuousSchema[Codex.DoubleCodex](), (3.14 + 6.28 + 9.42 + 12.56) * (1.0 / 2))),
    Cell(Position1D(2), Content(ContinuousSchema[Codex.DoubleCodex](), (6.28 + 12.56 + 18.84) * (1.0 / 4))),
    Cell(Position1D(3), Content(ContinuousSchema[Codex.DoubleCodex](), (9.42 + 18.84) * (1.0 / 6))),
    Cell(Position1D(4), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 * (1.0 / 8))))

  val result13 = List(
    Cell(Position1D(1), Content(ContinuousSchema[Codex.DoubleCodex](), (3.14 + 6.28 + 9.42 + 12.56) * (1.0 / 2))),
    Cell(Position1D(2), Content(ContinuousSchema[Codex.DoubleCodex](), (6.28 + 12.56 + 18.84) * (1.0 / 4))),
    Cell(Position1D(3), Content(ContinuousSchema[Codex.DoubleCodex](), (9.42 + 18.84) * (1.0 / 6))),
    Cell(Position1D(4), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 * (1.0 / 8))))

  val result14 = List(
    Cell(Position1D("bar"), Content(ContinuousSchema[Codex.DoubleCodex](), (6.28 + 12.56 + 18.84) * (1.0 / 2))),
    Cell(Position1D("baz"), Content(ContinuousSchema[Codex.DoubleCodex](), (9.42 + 18.84) * (1.0 / 3))),
    Cell(Position1D("foo"), Content(ContinuousSchema[Codex.DoubleCodex](), (3.14 + 6.28 + 9.42 + 12.56) * (1.0 / 1))),
    Cell(Position1D("qux"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 * (1.0 / 4))))

  val result15 = List(
    Cell(Position1D("bar"), Content(ContinuousSchema[Codex.DoubleCodex](), (6.28 + 12.56 + 18.84) * (1.0 / 2))),
    Cell(Position1D("baz"), Content(ContinuousSchema[Codex.DoubleCodex](), (9.42 + 18.84) * (1.0 / 3))),
    Cell(Position1D("foo"), Content(ContinuousSchema[Codex.DoubleCodex](), (3.14 + 6.28 + 9.42 + 12.56) * (1.0 / 1))),
    Cell(Position1D("qux"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 * (1.0 / 4))))

  val result16 = List(Cell(Position2D(1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](),
      (3.14 + 6.28 + 9.42 + 12.56) * (1.0 / 2))),
    Cell(Position2D(2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), (6.28 + 12.56 + 18.84) * (1.0 / 4))),
    Cell(Position2D(3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), (9.42 + 18.84) * (1.0 / 6))),
    Cell(Position2D(4, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 * (1.0 / 8))))

  val result17 = List(
    Cell(Position1D(1), Content(ContinuousSchema[Codex.DoubleCodex](), (3.14 + 6.28 + 9.42 + 12.56) * (1.0 / 2))),
    Cell(Position1D(2), Content(ContinuousSchema[Codex.DoubleCodex](), (6.28 + 12.56 + 18.84) * (1.0 / 4))),
    Cell(Position1D(3), Content(ContinuousSchema[Codex.DoubleCodex](), (9.42 + 18.84) * (1.0 / 6))),
    Cell(Position1D(4), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 * (1.0 / 8))))

  val result18 = List(
    Cell(Position2D("bar", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), (6.28 + 12.56 + 18.84) * (1.0 / 2))),
    Cell(Position2D("baz", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), (9.42 + 18.84) * (1.0 / 3))),
    Cell(Position2D("foo", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](),
      (3.14 + 6.28 + 9.42 + 12.56) * (1.0 / 1))),
    Cell(Position2D("qux", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 * (1.0 / 4))))

  val result19 = List(Cell(Position1D("xyz"), Content(ContinuousSchema[Codex.DoubleCodex](),
    (3.14 + 2 * 6.28 + 2 * 9.42 + 3 * 12.56 + 2 * 18.84) / 3.14)))

  val result20 = List(Cell(Position2D("bar", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 / 3.14)),
    Cell(Position2D("bar", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 / 3.14)),
    Cell(Position2D("bar", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 / 3.14)),
    Cell(Position2D("baz", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 / 3.14)),
    Cell(Position2D("baz", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 / 3.14)),
    Cell(Position2D("foo", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 / 3.14)),
    Cell(Position2D("foo", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 / 3.14)),
    Cell(Position2D("foo", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 / 3.14)),
    Cell(Position2D("foo", 4), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 / 3.14)),
    Cell(Position2D("qux", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 / 3.14)))

  val result21 = List(Cell(Position2D("bar", "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position2D("baz", "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
    Cell(Position2D("foo", "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
    Cell(Position2D("qux", "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)))

  val result22 = List(Cell(Position1D("max"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position1D("min"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)))

  val result23 = List(Cell(Position2D("bar", "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position2D("baz", "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
    Cell(Position2D("foo", "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
    Cell(Position2D("qux", "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)))

  val result24 = List(Cell(Position2D(1, "max"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position2D(1, "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
    Cell(Position2D(2, "max"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
    Cell(Position2D(2, "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position2D(3, "max"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
    Cell(Position2D(3, "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
    Cell(Position2D(4, "max"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position2D(4, "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)))

  val result25 = List(Cell(Position2D(1, "max"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position2D(1, "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
    Cell(Position2D(2, "max"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
    Cell(Position2D(2, "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position2D(3, "max"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
    Cell(Position2D(3, "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
    Cell(Position2D(4, "max"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position2D(4, "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)))

  val result26 = List(Cell(Position2D("bar", "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position2D("baz", "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
    Cell(Position2D("foo", "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
    Cell(Position2D("qux", "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)))

  val result27 = List(Cell(Position2D("bar", "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position2D("baz", "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
    Cell(Position2D("foo", "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
    Cell(Position2D("qux", "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)))

  val result28 = List(Cell(Position3D(1, "xyz", "max"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position3D(1, "xyz", "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
    Cell(Position3D(2, "xyz", "max"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
    Cell(Position3D(2, "xyz", "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position3D(3, "xyz", "max"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
    Cell(Position3D(3, "xyz", "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
    Cell(Position3D(4, "xyz", "max"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position3D(4, "xyz", "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)))

  val result29 = List(Cell(Position2D(1, "max"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position2D(1, "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
    Cell(Position2D(2, "max"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
    Cell(Position2D(2, "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position2D(3, "max"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
    Cell(Position2D(3, "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
    Cell(Position2D(4, "max"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position2D(4, "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)))

  val result30 = List(Cell(Position3D("bar", "xyz", "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position3D("baz", "xyz", "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
    Cell(Position3D("foo", "xyz", "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
    Cell(Position3D("qux", "xyz", "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)))

  val result31 = List(Cell(Position2D("xyz", "max"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
    Cell(Position2D("xyz", "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)))

  val result32 = List(Cell(Position3D("bar", 1, "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position3D("bar", 2, "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position3D("bar", 3, "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
    Cell(Position3D("baz", 1, "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
    Cell(Position3D("baz", 2, "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
    Cell(Position3D("foo", 1, "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
    Cell(Position3D("foo", 2, "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position3D("foo", 3, "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
    Cell(Position3D("foo", 4, "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position3D("qux", 1, "min"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)))

  val result33 = List(Cell(Position2D("bar", "sum"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 / 2)),
    Cell(Position2D("baz", "sum"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 * (1.0 / 3))),
    Cell(Position2D("foo", "sum"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 / 1)),
    Cell(Position2D("qux", "sum"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 / 4)))

  val result34 = List(Cell(Position1D("sum.1"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position1D("sum.2"), Content(ContinuousSchema[Codex.DoubleCodex](), 31.40)))

  val result35 = List(Cell(Position2D("bar", "sum"),
      Content(ContinuousSchema[Codex.DoubleCodex](), (6.28 + 12.56 + 18.84) * (1.0 / 2))),
    Cell(Position2D("baz", "sum"), Content(ContinuousSchema[Codex.DoubleCodex](), (9.42 + 18.84) * (1.0 / 3))),
    Cell(Position2D("foo", "sum"),
      Content(ContinuousSchema[Codex.DoubleCodex](), (3.14 + 6.28 + 9.42 + 12.56) * (1.0 / 1))),
    Cell(Position2D("qux", "sum"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 * (1.0 / 4))))

  val result36 = List(Cell(Position2D(1, "sum.1"),
      Content(ContinuousSchema[Codex.DoubleCodex](), (3.14 + 6.28 + 9.42 + 12.56) * (1.0 / 2))),
    Cell(Position2D(1, "sum.2"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 6.28 + 9.42 + 12.56)),
    Cell(Position2D(2, "sum.1"), Content(ContinuousSchema[Codex.DoubleCodex](), (6.28 + 12.56 + 18.84) * (1.0 / 4))),
    Cell(Position2D(2, "sum.2"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 12.56 + 18.84)),
    Cell(Position2D(3, "sum.1"), Content(ContinuousSchema[Codex.DoubleCodex](), (9.42 + 18.84) * (1.0 / 6))),
    Cell(Position2D(3, "sum.2"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 18.84)),
    Cell(Position2D(4, "sum.1"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 * (1.0 / 8))),
    Cell(Position2D(4, "sum.2"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)))

  val result37 = List(Cell(Position2D(1, "sum.1"),
      Content(ContinuousSchema[Codex.DoubleCodex](), (3.14 + 6.28 + 9.42 + 12.56) * (1.0 / 2))),
    Cell(Position2D(1, "sum.2"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 6.28 + 9.42 + 12.56)),
    Cell(Position2D(2, "sum.1"), Content(ContinuousSchema[Codex.DoubleCodex](), (6.28 + 12.56 + 18.84) * (1.0 / 4))),
    Cell(Position2D(2, "sum.2"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 12.56 + 18.84)),
    Cell(Position2D(3, "sum.1"), Content(ContinuousSchema[Codex.DoubleCodex](), (9.42 + 18.84) * (1.0 / 6))),
    Cell(Position2D(3, "sum.2"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 18.84)),
    Cell(Position2D(4, "sum.1"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 * (1.0 / 8))),
    Cell(Position2D(4, "sum.2"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)))

  val result38 = List(Cell(Position2D("bar", "sum"),
      Content(ContinuousSchema[Codex.DoubleCodex](), (6.28 + 12.56 + 18.84) * (1.0 / 2))),
    Cell(Position2D("baz", "sum"), Content(ContinuousSchema[Codex.DoubleCodex](), (9.42 + 18.84) * (1.0 / 3))),
    Cell(Position2D("foo", "sum"),
      Content(ContinuousSchema[Codex.DoubleCodex](), (3.14 + 6.28 + 9.42 + 12.56) * (1.0 / 1))),
    Cell(Position2D("qux", "sum"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 * (1.0 / 4))))

  val result39 = List(Cell(Position2D("bar", "sum"),
      Content(ContinuousSchema[Codex.DoubleCodex](), (6.28 + 12.56 + 18.84) * (1.0 / 2))),
    Cell(Position2D("baz", "sum"), Content(ContinuousSchema[Codex.DoubleCodex](), (9.42 + 18.84) * (1.0 / 3))),
    Cell(Position2D("foo", "sum"),
      Content(ContinuousSchema[Codex.DoubleCodex](), (3.14 + 6.28 + 9.42 + 12.56) * (1.0 / 1))),
    Cell(Position2D("qux", "sum"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 * (1.0 / 4))))

  val result40 = List(Cell(Position3D(1, "xyz", "sum.1"),
      Content(ContinuousSchema[Codex.DoubleCodex](), (3.14 + 6.28 + 9.42 + 12.56) * (1.0 / 2))),
    Cell(Position3D(1, "xyz", "sum.2"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 6.28 + 9.42 + 12.56)),
    Cell(Position3D(2, "xyz", "sum.1"),
      Content(ContinuousSchema[Codex.DoubleCodex](), (6.28 + 12.56 + 18.84) * (1.0 / 4))),
    Cell(Position3D(2, "xyz", "sum.2"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 12.56 + 18.84)),
    Cell(Position3D(3, "xyz", "sum.1"), Content(ContinuousSchema[Codex.DoubleCodex](), (9.42 + 18.84) * (1.0 / 6))),
    Cell(Position3D(3, "xyz", "sum.2"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 18.84)),
    Cell(Position3D(4, "xyz", "sum.1"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 * (1.0 / 8))),
    Cell(Position3D(4, "xyz", "sum.2"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)))

  val result41 = List(Cell(Position2D(1, "sum.1"),
      Content(ContinuousSchema[Codex.DoubleCodex](), (3.14 + 6.28 + 9.42 + 12.56) * (1.0 / 2))),
    Cell(Position2D(1, "sum.2"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 6.28 + 9.42 + 12.56)),
    Cell(Position2D(2, "sum.1"), Content(ContinuousSchema[Codex.DoubleCodex](), (6.28 + 12.56 + 18.84) * (1.0 / 4))),
    Cell(Position2D(2, "sum.2"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 12.56 + 18.84)),
    Cell(Position2D(3, "sum.1"), Content(ContinuousSchema[Codex.DoubleCodex](), (9.42 + 18.84) * (1.0 / 6))),
    Cell(Position2D(3, "sum.2"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 18.84)),
    Cell(Position2D(4, "sum.1"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 * (1.0 / 8))),
    Cell(Position2D(4, "sum.2"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)))

  val result42 = List(Cell(Position3D("bar", "xyz", "sum"),
      Content(ContinuousSchema[Codex.DoubleCodex](), (6.28 + 12.56 + 18.84) * (1.0 / 2))),
    Cell(Position3D("baz", "xyz", "sum"), Content(ContinuousSchema[Codex.DoubleCodex](), (9.42 + 18.84) * (1.0 / 3))),
    Cell(Position3D("foo", "xyz", "sum"),
      Content(ContinuousSchema[Codex.DoubleCodex](), (3.14 + 6.28 + 9.42 + 12.56) * (1.0 / 1))),
    Cell(Position3D("qux", "xyz", "sum"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 * (1.0 / 4))))

  val result43 = List(Cell(Position2D("xyz", "sum.1"), Content(ContinuousSchema[Codex.DoubleCodex](),
      (3.14 + 2 * 6.28 + 2 * 9.42 + 3 * 12.56 + 2 * 18.84) / 3.14)),
    Cell(Position2D("xyz", "sum.2"), Content(ContinuousSchema[Codex.DoubleCodex](),
      (3.14 + 2 * 6.28 + 2 * 9.42 + 3 * 12.56 + 2 * 18.84) / 6.28)))

  val result44 = List(Cell(Position3D("bar", 1, "sum"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 / 3.14)),
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

object TestMatrixSummarise {
  case class ExtractWithName[P <: Position](dim: Dimension, name: String)
    extends Extract[P, Map[Position1D, Double], Double] {
    def extract(cell: Cell[P], ext: Map[Position1D, Double]): Option[Double] = {
      ext.get(Position1D(name.format(cell.position(dim).toShortString)))
    }
  }
}

class TestScaldingMatrixSummarise extends TestMatrixSummarise with TBddDsl {

  "A Matrix.summarise" should "return its first over aggregates in 2D" in {
    Given {
      num2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.summarise(Over(First), Min[Position2D, Position1D]())
    } Then {
      _.toList.sortBy(_.position) shouldBe result1
    }
  }

  it should "return its first along aggregates in 2D" in {
    Given {
      num2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.summarise(Along(First), Max[Position2D, Position1D]())
    } Then {
      _.toList.sortBy(_.position) shouldBe result2
    }
  }

  it should "return its second over aggregates in 2D" in {
    Given {
      num2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.summarise(Over(Second), Max[Position2D, Position1D]())
    } Then {
      _.toList.sortBy(_.position) shouldBe result3
    }
  }

  it should "return its second along aggregates in 2D" in {
    Given {
      num2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.summarise(Along(Second), Min[Position2D, Position1D]())
    } Then {
      _.toList.sortBy(_.position) shouldBe result4
    }
  }

  it should "return its first over aggregates in 3D" in {
    Given {
      num3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.summarise(Over(First), Min[Position3D, Position1D]())
    } Then {
      _.toList.sortBy(_.position) shouldBe result5
    }
  }

  it should "return its first along aggregates in 3D" in {
    Given {
      num3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.summarise(Along(First), Max[Position3D, Position2D]())
    } Then {
      _.toList.sortBy(_.position) shouldBe result6
    }
  }

  it should "return its second over aggregates in 3D" in {
    Given {
      num3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.summarise(Over(Second), Max[Position3D, Position1D]())
    } Then {
      _.toList.sortBy(_.position) shouldBe result7
    }
  }

  it should "return its second along aggregates in 3D" in {
    Given {
      num3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.summarise(Along(Second), Min[Position3D, Position2D]())
    } Then {
      _.toList.sortBy(_.position) shouldBe result8
    }
  }

  it should "return its third over aggregates in 3D" in {
    Given {
      num3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.summarise(Over(Third), Max[Position3D, Position1D]())
    } Then {
      _.toList.sortBy(_.position) shouldBe result9
    }
  }

  it should "return its third along aggregates in 3D" in {
    Given {
      num3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.summarise(Along(Third), Min[Position3D, Position2D]())
    } Then {
      _.toList.sortBy(_.position) shouldBe result10
    }
  }

  "A Matrix.summariseWithValue" should "return its first over aggregates in 2D" in {
    Given {
      num2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.summariseWithValue(Over(First), WeightedSum[Position2D, Position1D, W](ExtractWithDimension(First)),
          ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result11
    }
  }

  it should "return its first along aggregates in 2D" in {
    Given {
      num2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.summariseWithValue(Along(First), WeightedSum[Position2D, Position1D, W](ExtractWithDimension(Second)),
          ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result12
    }
  }

  it should "return its second over aggregates in 2D" in {
    Given {
      num2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.summariseWithValue(Over(Second), WeightedSum[Position2D, Position1D, W](ExtractWithDimension(Second)),
          ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result13
    }
  }

  it should "return its second along aggregates in 2D" in {
    Given {
      num2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.summariseWithValue(Along(Second), WeightedSum[Position2D, Position1D, W](ExtractWithDimension(First)),
          ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result14
    }
  }

  it should "return its first over aggregates in 3D" in {
    Given {
      num3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.summariseWithValue(Over(First), WeightedSum[Position3D, Position1D, W](ExtractWithDimension(First)),
          ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result15
    }
  }

  it should "return its first along aggregates in 3D" in {
    Given {
      num3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.summariseWithValue(Along(First), WeightedSum[Position3D, Position2D, W](ExtractWithDimension(Second)),
          ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result16
    }
  }

  it should "return its second over aggregates in 3D" in {
    Given {
      num3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.summariseWithValue(Over(Second), WeightedSum[Position3D, Position1D, W](ExtractWithDimension(Second)),
          ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result17
    }
  }

  it should "return its second along aggregates in 3D" in {
    Given {
      num3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.summariseWithValue(Along(Second), WeightedSum[Position3D, Position2D, W](ExtractWithDimension(First)),
          ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result18
    }
  }

  it should "return its third over aggregates in 3D" in {
    Given {
      num3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.summariseWithValue(Over(Third), WeightedSum[Position3D, Position1D, W](ExtractWithDimension(Third)),
          ValuePipe(ext))
    } Then {
        _.toList.sortBy(_.position) shouldBe result19
    }
  }

  it should "return its third along aggregates in 3D" in {
    Given {
      num3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.summariseWithValue(Along(Third), WeightedSum[Position3D, Position2D, W](ExtractWithDimension(Third)),
          ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result20
    }
  }

  "A Matrix.summariseAndExpand" should "return its first over aggregates in 1D" in {
    Given {
      num1
    } When {
      cells: TypedPipe[Cell[Position1D]] =>
        cells.summarise(Over(First), Min[Position1D, Position1D]().andThenExpand(_.position.append("min")))
    } Then {
      _.toList.sortBy(_.position) shouldBe result21
    }
  }

  it should "return its first along aggregates in 1D" in {
    Given {
      num1
    } When {
      cells: TypedPipe[Cell[Position1D]] =>
        cells.summarise(Along(First), List(Min[Position1D, Position0D]().andThenExpand(_.position.append("min")),
          Max[Position1D, Position0D]().andThenExpand(_.position.append("max"))))
    } Then {
      _.toList.sortBy(_.position) shouldBe result22
    }
  }

  it should "return its first over aggregates in 2D" in {
    Given {
      num2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.summarise(Over(First), Min[Position2D, Position1D]().andThenExpand(_.position.append("min")))
    } Then {
      _.toList.sortBy(_.position) shouldBe result23
    }
  }

  it should "return its first along aggregates in 2D" in {
    Given {
      num2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.summarise(Along(First), List(Min[Position2D, Position1D]().andThenExpand(_.position.append("min")),
          Max[Position2D, Position1D]().andThenExpand(_.position.append("max"))))
    } Then {
      _.toList.sortBy(_.position) shouldBe result24
    }
  }

  it should "return its second over aggregates in 2D" in {
    Given {
      num2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.summarise(Over(Second), List(Min[Position2D, Position1D]().andThenExpand(_.position.append("min")),
          Max[Position2D, Position1D]().andThenExpand(_.position.append("max"))))
    } Then {
      _.toList.sortBy(_.position) shouldBe result25
    }
  }

  it should "return its second along aggregates in 2D" in {
    Given {
      num2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.summarise(Along(Second), Min[Position2D, Position1D]().andThenExpand(_.position.append("min")))
    } Then {
      _.toList.sortBy(_.position) shouldBe result26
    }
  }

  it should "return its first over aggregates in 3D" in {
    Given {
      num3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.summarise(Over(First), Min[Position3D, Position1D]().andThenExpand(_.position.append("min")))
    } Then {
      _.toList.sortBy(_.position) shouldBe result27
    }
  }

  it should "return its first along aggregates in 3D" in {
    Given {
      num3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.summarise(Along(First), List(Min[Position3D, Position2D]().andThenExpand(_.position.append("min")),
          Max[Position3D, Position2D]().andThenExpand(_.position.append("max"))))
    } Then {
      _.toList.sortBy(_.position) shouldBe result28
    }
  }

  it should "return its second over aggregates in 3D" in {
    Given {
      num3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.summarise(Over(Second), List(Min[Position3D, Position1D]().andThenExpand(_.position.append("min")),
          Max[Position3D, Position1D]().andThenExpand(_.position.append("max"))))
    } Then {
      _.toList.sortBy(_.position) shouldBe result29
    }
  }

  it should "return its second along aggregates in 3D" in {
    Given {
      num3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
      cells.summarise(Along(Second), Min[Position3D, Position2D]().andThenExpand(_.position.append("min")))
    } Then {
      _.toList.sortBy(_.position) shouldBe result30
    }
  }

  it should "return its third over aggregates in 3D" in {
    Given {
      num3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.summarise(Over(Third), List(Min[Position3D, Position1D]().andThenExpand(_.position.append("min")),
          Max[Position3D, Position1D]().andThenExpand(_.position.append("max"))))
    } Then {
      _.toList.sortBy(_.position) shouldBe result31
    }
  }

  it should "return its third along aggregates in 3D" in {
    Given {
      num3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.summarise(Along(Third), Min[Position3D, Position2D]().andThenExpand(_.position.append("min")))
    } Then {
      _.toList.sortBy(_.position) shouldBe result32
    }
  }

  "A Matrix.summariseAndExpandWithValue" should "return its first over aggregates in 1D" in {
    Given {
      num1
    } When {
      cells: TypedPipe[Cell[Position1D]] =>
        cells.summariseWithValue(Over(First), WeightedSum[Position1D, Position1D, W](ExtractWithDimension(First))
            .andThenExpandWithValue((c: Cell[Position1D], e: W) => c.position.append("sum")),
          ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result33
    }
  }

  it should "return its first along aggregates in 1D" in {
    Given {
      num1
    } When {
      cells: TypedPipe[Cell[Position1D]] =>
        cells.summariseWithValue(Along(First), List(
            WeightedSum[Position1D, Position0D, W](ExtractWithDimension(First))
              .andThenExpandWithValue((c: Cell[Position0D], e: W) => c.position.append("sum.1")),
            WeightedSum[Position1D, Position0D, W](TestMatrixSummarise.ExtractWithName(First, "%1$s.2"))
              .andThenExpandWithValue((c: Cell[Position0D], e: W) => c.position.append("sum.2"))),
          ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result34
    }
  }

  it should "return its first over aggregates in 2D" in {
    Given {
      num2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.summariseWithValue(Over(First), WeightedSum[Position2D, Position1D, W](ExtractWithDimension(First))
            .andThenExpandWithValue((c: Cell[Position1D], e: W) => c.position.append("sum")),
          ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result35
    }
  }

  it should "return its first along aggregates in 2D" in {
    Given {
      num2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.summariseWithValue(Along(First), List(
            WeightedSum[Position2D, Position1D, W](ExtractWithDimension(Second))
              .andThenExpandWithValue((c: Cell[Position1D], e: W) => c.position.append("sum.1")),
            WeightedSum[Position2D, Position1D, W](TestMatrixSummarise.ExtractWithName(First, "%1$s.2"))
              .andThenExpandWithValue((c: Cell[Position1D], e: W) => c.position.append("sum.2"))),
          ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result36
    }
  }

  it should "return its second over aggregates in 2D" in {
    Given {
      num2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.summariseWithValue(Over(Second), List(
            WeightedSum[Position2D, Position1D, W](ExtractWithDimension(Second))
              .andThenExpandWithValue((c: Cell[Position1D], e: W) => c.position.append("sum.1")),
            WeightedSum[Position2D, Position1D, W](TestMatrixSummarise.ExtractWithName(Second, "%1$s.2"))
              .andThenExpandWithValue((c: Cell[Position1D], e: W) => c.position.append("sum.2"))),
          ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result37
    }
  }

  it should "return its second along aggregates in 2D" in {
    Given {
      num2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.summariseWithValue(Along(Second), WeightedSum[Position2D, Position1D, W](ExtractWithDimension(First))
            .andThenExpandWithValue((c: Cell[Position1D], e: W) => c.position.append("sum")),
          ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result38
    }
  }

  it should "return its first over aggregates in 3D" in {
    Given {
      num3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.summariseWithValue(Over(First), WeightedSum[Position3D, Position1D, W](ExtractWithDimension(First))
            .andThenExpandWithValue((c: Cell[Position1D], e: W) => c.position.append("sum")),
          ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result39
    }
  }

  it should "return its first along aggregates in 3D" in {
    Given {
      num3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.summariseWithValue(Along(First), List(
            WeightedSum[Position3D, Position2D, W](ExtractWithDimension(Second))
              .andThenExpandWithValue((c: Cell[Position2D], e: W) => c.position.append("sum.1")),
            WeightedSum[Position3D, Position2D, W](TestMatrixSummarise.ExtractWithName(Second, "%1$s.2"))
              .andThenExpandWithValue((c: Cell[Position2D], e: W) => c.position.append("sum.2"))),
          ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result40
    }
  }

  it should "return its second over aggregates in 3D" in {
    Given {
      num3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.summariseWithValue(Over(Second), List(
            WeightedSum[Position3D, Position1D, W](ExtractWithDimension(Second))
              .andThenExpandWithValue((c: Cell[Position1D], e: W) => c.position.append("sum.1")),
            WeightedSum[Position3D, Position1D, W](TestMatrixSummarise.ExtractWithName(Second, "%1$s.2"))
              .andThenExpandWithValue((c: Cell[Position1D], e: W) => c.position.append("sum.2"))),
          ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result41
    }
  }

  it should "return its second along aggregates in 3D" in {
    Given {
      num3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.summariseWithValue(Along(Second),
          WeightedSum[Position3D, Position2D, W](ExtractWithDimension(First))
            .andThenExpandWithValue((c: Cell[Position2D], e: W) => c.position.append("sum")),
          ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result42
    }
  }

  it should "return its third over aggregates in 3D" in {
    Given {
      num3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.summariseWithValue(Over(Third), List(
            WeightedSum[Position3D, Position1D, W](ExtractWithDimension(Third))
              .andThenExpandWithValue((c: Cell[Position1D], e: W) => c.position.append("sum.1")),
            WeightedSum[Position3D, Position1D, W](TestMatrixSummarise.ExtractWithName(Third, "%1$s.2"))
              .andThenExpandWithValue((c: Cell[Position1D], e: W) => c.position.append("sum.2"))),
          ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result43
    }
  }

  it should "return its third along aggregates in 3D" in {
    Given {
      num3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.summariseWithValue(Along(Third), WeightedSum[Position3D, Position2D, W](ExtractWithDimension(Third))
            .andThenExpandWithValue((c: Cell[Position2D], e: W) => c.position.append("sum")),
          ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result44
    }
  }
}

class TestSparkMatrixSummarise extends TestMatrixSummarise {

  "A Matrix.summarise" should "return its first over aggregates in 2D" in {
    toRDD(num2)
      .summarise(Over(First), Min[Position2D, Position1D]())
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first along aggregates in 2D" in {
    toRDD(num2)
      .summarise(Along(First), Max[Position2D, Position1D]())
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its second over aggregates in 2D" in {
    toRDD(num2)
      .summarise(Over(Second), Max[Position2D, Position1D]())
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its second along aggregates in 2D" in {
    toRDD(num2)
      .summarise(Along(Second), Min[Position2D, Position1D]())
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its first over aggregates in 3D" in {
    toRDD(num3)
      .summarise(Over(First), Min[Position3D, Position1D]())
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its first along aggregates in 3D" in {
    toRDD(num3)
      .summarise(Along(First), Max[Position3D, Position2D]())
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its second over aggregates in 3D" in {
    toRDD(num3)
      .summarise(Over(Second), Max[Position3D, Position1D]())
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its second along aggregates in 3D" in {
    toRDD(num3)
      .summarise(Along(Second), Min[Position3D, Position2D]())
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its third over aggregates in 3D" in {
    toRDD(num3)
      .summarise(Over(Third), Max[Position3D, Position1D]())
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its third along aggregates in 3D" in {
    toRDD(num3)
      .summarise(Along(Third), Min[Position3D, Position2D]())
      .toList.sortBy(_.position) shouldBe result10
  }

  "A Matrix.summariseWithValue" should "return its first over aggregates in 2D" in {
    toRDD(num2)
      .summariseWithValue(Over(First), WeightedSum[Position2D, Position1D, W](ExtractWithDimension(First)), ext)
      .toList.sortBy(_.position) shouldBe result11
  }

  it should "return its first along aggregates in 2D" in {
    toRDD(num2)
      .summariseWithValue(Along(First), WeightedSum[Position2D, Position1D, W](ExtractWithDimension(Second)), ext)
      .toList.sortBy(_.position) shouldBe result12
  }

  it should "return its second over aggregates in 2D" in {
    toRDD(num2)
      .summariseWithValue(Over(Second), WeightedSum[Position2D, Position1D, W](ExtractWithDimension(Second)), ext)
      .toList.sortBy(_.position) shouldBe result13
  }

  it should "return its second along aggregates in 2D" in {
    toRDD(num2)
      .summariseWithValue(Along(Second), WeightedSum[Position2D, Position1D, W](ExtractWithDimension(First)), ext)
      .toList.sortBy(_.position) shouldBe result14
  }

  it should "return its first over aggregates in 3D" in {
    toRDD(num3)
      .summariseWithValue(Over(First), WeightedSum[Position3D, Position1D, W](ExtractWithDimension(First)), ext)
      .toList.sortBy(_.position) shouldBe result15
  }

  it should "return its first along aggregates in 3D" in {
    toRDD(num3)
      .summariseWithValue(Along(First), WeightedSum[Position3D, Position2D, W](ExtractWithDimension(Second)), ext)
      .toList.sortBy(_.position) shouldBe result16
  }

  it should "return its second over aggregates in 3D" in {
    toRDD(num3)
      .summariseWithValue(Over(Second), WeightedSum[Position3D, Position1D, W](ExtractWithDimension(Second)), ext)
      .toList.sortBy(_.position) shouldBe result17
  }

  it should "return its second along aggregates in 3D" in {
    toRDD(num3)
      .summariseWithValue(Along(Second), WeightedSum[Position3D, Position2D, W](ExtractWithDimension(First)), ext)
      .toList.sortBy(_.position) shouldBe result18
  }

  it should "return its third over aggregates in 3D" in {
    toRDD(num3)
      .summariseWithValue(Over(Third), WeightedSum[Position3D, Position1D, W](ExtractWithDimension(Third)), ext)
      .toList.sortBy(_.position) shouldBe result19
  }

  it should "return its third along aggregates in 3D" in {
    toRDD(num3)
      .summariseWithValue(Along(Third), WeightedSum[Position3D, Position2D, W](ExtractWithDimension(Third)), ext)
      .toList.sortBy(_.position) shouldBe result20
  }

  "A Matrix.summariseAndExpand" should "return its first over aggregates in 1D" in {
    toRDD(num1)
      .summarise(Over(First), Min[Position1D, Position1D]().andThenExpand(_.position.append("min")))
      .toList.sortBy(_.position) shouldBe result21
  }

  it should "return its first along aggregates in 1D" in {
    toRDD(num1)
      .summarise(Along(First), List(Min[Position1D, Position0D]().andThenExpand(_.position.append("min")),
        Max[Position1D, Position0D]().andThenExpand(_.position.append("max"))))
      .toList.sortBy(_.position) shouldBe result22
  }

  it should "return its first over aggregates in 2D" in {
    toRDD(num2)
      .summarise(Over(First), Min[Position2D, Position1D]().andThenExpand(_.position.append("min")))
      .toList.sortBy(_.position) shouldBe result23
  }

  it should "return its first along aggregates in 2D" in {
    toRDD(num2)
      .summarise(Along(First), List(Min[Position2D, Position1D]().andThenExpand(_.position.append("min")),
        Max[Position2D, Position1D]().andThenExpand(_.position.append("max"))))
      .toList.sortBy(_.position) shouldBe result24
  }

  it should "return its second over aggregates in 2D" in {
    toRDD(num2)
      .summarise(Over(Second), List(Min[Position2D, Position1D]().andThenExpand(_.position.append("min")),
        Max[Position2D, Position1D]().andThenExpand(_.position.append("max"))))
      .toList.sortBy(_.position) shouldBe result25
  }

  it should "return its second along aggregates in 2D" in {
    toRDD(num2)
      .summarise(Along(Second), Min[Position2D, Position1D]().andThenExpand(_.position.append("min")))
      .toList.sortBy(_.position) shouldBe result26
  }

  it should "return its first over aggregates in 3D" in {
    toRDD(num3)
      .summarise(Over(First), Min[Position3D, Position1D]().andThenExpand(_.position.append("min")))
      .toList.sortBy(_.position) shouldBe result27
  }

  it should "return its first along aggregates in 3D" in {
    toRDD(num3)
      .summarise(Along(First), List(Min[Position3D, Position2D]().andThenExpand(_.position.append("min")),
        Max[Position3D, Position2D]().andThenExpand(_.position.append("max"))))
      .toList.sortBy(_.position) shouldBe result28
  }

  it should "return its second over aggregates in 3D" in {
    toRDD(num3)
      .summarise(Over(Second), List(Min[Position3D, Position1D]().andThenExpand(_.position.append("min")),
        Max[Position3D, Position1D]().andThenExpand(_.position.append("max"))))
      .toList.sortBy(_.position) shouldBe result29
  }

  it should "return its second along aggregates in 3D" in {
    toRDD(num3)
      .summarise(Along(Second), Min[Position3D, Position2D]().andThenExpand(_.position.append("min")))
      .toList.sortBy(_.position) shouldBe result30
  }

  it should "return its third over aggregates in 3D" in {
    toRDD(num3)
      .summarise(Over(Third), List(Min[Position3D, Position1D]().andThenExpand(_.position.append("min")),
        Max[Position3D, Position1D]().andThenExpand(_.position.append("max"))))
      .toList.sortBy(_.position) shouldBe result31
  }

  it should "return its third along aggregates in 3D" in {
    toRDD(num3)
      .summarise(Along(Third), Min[Position3D, Position2D]().andThenExpand(_.position.append("min")))
      .toList.sortBy(_.position) shouldBe result32
  }

  "A Matrix.summariseAndExpandWithValue" should "return its first over aggregates in 1D" in {
    toRDD(num1)
      .summariseWithValue(Over(First), WeightedSum[Position1D, Position1D, W](ExtractWithDimension(First))
        .andThenExpandWithValue((c: Cell[Position1D], e: W) => c.position.append("sum")), ext)
      .toList.sortBy(_.position) shouldBe result33
  }

  it should "return its first along aggregates in 1D" in {
    toRDD(num1)
      .summariseWithValue(Along(First), List(
        WeightedSum[Position1D, Position0D, W](ExtractWithDimension(First))
          .andThenExpandWithValue((c: Cell[Position0D], e: W) => c.position.append("sum.1")),
        WeightedSum[Position1D, Position0D, W](TestMatrixSummarise.ExtractWithName(First, "%1$s.2"))
          .andThenExpandWithValue((c: Cell[Position0D], e: W) => c.position.append("sum.2"))), ext)
      .toList.sortBy(_.position) shouldBe result34
  }

  it should "return its first over aggregates in 2D" in {
    toRDD(num2)
      .summariseWithValue(Over(First), WeightedSum[Position2D, Position1D, W](ExtractWithDimension(First))
        .andThenExpandWithValue((c: Cell[Position1D], e: W) => c.position.append("sum")), ext)
      .toList.sortBy(_.position) shouldBe result35
  }

  it should "return its first along aggregates in 2D" in {
    toRDD(num2)
      .summariseWithValue(Along(First), List(
        WeightedSum[Position2D, Position1D, W](ExtractWithDimension(Second))
          .andThenExpandWithValue((c: Cell[Position1D], e: W) => c.position.append("sum.1")),
        WeightedSum[Position2D, Position1D, W](TestMatrixSummarise.ExtractWithName(First, "%1$s.2"))
          .andThenExpandWithValue((c: Cell[Position1D], e: W) => c.position.append("sum.2"))), ext)
      .toList.sortBy(_.position) shouldBe result36
  }

  it should "return its second over aggregates in 2D" in {
    toRDD(num2)
      .summariseWithValue(Over(Second), List(
        WeightedSum[Position2D, Position1D, W](ExtractWithDimension(Second))
          .andThenExpandWithValue((c: Cell[Position1D], e: W) => c.position.append("sum.1")),
        WeightedSum[Position2D, Position1D, W](TestMatrixSummarise.ExtractWithName(Second, "%1$s.2"))
          .andThenExpandWithValue((c: Cell[Position1D], e: W) => c.position.append("sum.2"))), ext)
      .toList.sortBy(_.position) shouldBe result37
  }

  it should "return its second along aggregates in 2D" in {
    toRDD(num2)
      .summariseWithValue(Along(Second), WeightedSum[Position2D, Position1D, W](ExtractWithDimension(First))
        .andThenExpandWithValue((c: Cell[Position1D], e: W) => c.position.append("sum")), ext)
      .toList.sortBy(_.position) shouldBe result38
  }

  it should "return its first over aggregates in 3D" in {
    toRDD(num3)
      .summariseWithValue(Over(First), WeightedSum[Position3D, Position1D, W](ExtractWithDimension(First))
        .andThenExpandWithValue((c: Cell[Position1D], e: W) => c.position.append("sum")), ext)
      .toList.sortBy(_.position) shouldBe result39
  }

  it should "return its first along aggregates in 3D" in {
    toRDD(num3)
      .summariseWithValue(Along(First), List(
        WeightedSum[Position3D, Position2D, W](ExtractWithDimension(Second))
          .andThenExpandWithValue((c: Cell[Position2D], e: W) => c.position.append("sum.1")),
        WeightedSum[Position3D, Position2D, W](TestMatrixSummarise.ExtractWithName(Second, "%1$s.2"))
          .andThenExpandWithValue((c: Cell[Position2D], e: W) => c.position.append("sum.2"))), ext)
      .toList.sortBy(_.position) shouldBe result40
  }

  it should "return its second over aggregates in 3D" in {
    toRDD(num3)
      .summariseWithValue(Over(Second), List(
          WeightedSum[Position3D, Position1D, W](ExtractWithDimension(Second))
            .andThenExpandWithValue((c: Cell[Position1D], e: W) => c.position.append("sum.1")),
          WeightedSum[Position3D, Position1D, W](TestMatrixSummarise.ExtractWithName(Second, "%1$s.2"))
            .andThenExpandWithValue((c: Cell[Position1D], e: W) => c.position.append("sum.2"))),
        ext)
      .toList.sortBy(_.position) shouldBe result41
  }

  it should "return its second along aggregates in 3D" in {
    toRDD(num3)
      .summariseWithValue(Along(Second), WeightedSum[Position3D, Position2D, W](ExtractWithDimension(First))
        .andThenExpandWithValue((c: Cell[Position2D], e: W) => c.position.append("sum")), ext)
      .toList.sortBy(_.position) shouldBe result42
  }

  it should "return its third over aggregates in 3D" in {
    toRDD(num3)
      .summariseWithValue(Over(Third), List(
        WeightedSum[Position3D, Position1D, W](ExtractWithDimension(Third))
          .andThenExpandWithValue((c: Cell[Position1D], e: W) => c.position.append("sum.1")),
        WeightedSum[Position3D, Position1D, W](TestMatrixSummarise.ExtractWithName(Third, "%1$s.2"))
          .andThenExpandWithValue((c: Cell[Position1D], e: W) => c.position.append("sum.2"))), ext)
      .toList.sortBy(_.position) shouldBe result43
  }

  it should "return its third along aggregates in 3D" in {
    toRDD(num3)
      .summariseWithValue(Along(Third), WeightedSum[Position3D, Position2D, W](ExtractWithDimension(Third))
        .andThenExpandWithValue((c: Cell[Position2D], e: W) => c.position.append("sum")), ext)
      .toList.sortBy(_.position) shouldBe result44
  }
}

trait TestMatrixSplit extends TestMatrix {

  implicit val TO1 = TestMatrixSplit.TupleOrdering[Position1D]
  implicit val TO2 = TestMatrixSplit.TupleOrdering[Position2D]
  implicit val TO3 = TestMatrixSplit.TupleOrdering[Position3D]

  val result1 = data1.map { case c => (c.position(First).toShortString, c) }.sorted

  val result2 = data2.map { case c => (c.position(First).toShortString, c) }.sorted

  val result3 = data2.map { case c => (c.position(Second).toShortString, c) }.sorted

  val result4 = data3.map { case c => (c.position(First).toShortString, c) }.sorted

  val result5 = data3.map { case c => (c.position(Second).toShortString, c) }.sorted

  val result6 = data3.map { case c => (c.position(Third).toShortString, c) }.sorted

  val result7 = data1.map { case c => (c.position(First).toShortString, c) }.sorted

  val result8 = data2.map { case c => (c.position(First).toShortString, c) }.sorted

  val result9 = data2.map { case c => (c.position(Second).toShortString, c) }.sorted

  val result10 = data3.map { case c => (c.position(First).toShortString, c) }.sorted

  val result11 = data3.map { case c => (c.position(Second).toShortString, c) }.sorted

  val result12 = data3.map { case c => (c.position(Third).toShortString, c) }.sorted

  type W = Dimension
}

object TestMatrixSplit {

  case class TestPartitioner[P <: Position](dim: Dimension) extends Partitioner[P, String] {
    def assign(cell: Cell[P]): Collection[String] = Collection(cell.position(dim).toShortString)
  }

  case class TestPartitionerWithValue[P <: Position]() extends PartitionerWithValue[P, String] {
    type V = Dimension
    def assignWithValue(cell: Cell[P], ext: V): Collection[String] = Collection(cell.position(ext).toShortString)
  }

  def TupleOrdering[P <: Position]() = new Ordering[(String, Cell[P])] {
    def compare(x: (String, Cell[P]), y: (String, Cell[P])): Int = {
      x._1.compare(y._1) match {
        case cmp if (cmp == 0) => x._2.position.compare(y._2.position)
        case cmp => cmp
      }
    }
  }
}

class TestScaldingMatrixSplit extends TestMatrixSplit with TBddDsl {

  "A Matrix.split" should "return its first partitions in 1D" in {
    Given {
      data1
    } When {
      cells: TypedPipe[Cell[Position1D]] =>
        cells.split(TestMatrixSplit.TestPartitioner[Position1D](First))
    } Then {
      _.toList.sorted shouldBe result1
    }
  }

  it should "return its first partitions in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.split(TestMatrixSplit.TestPartitioner[Position2D](First))
    } Then {
      _.toList.sorted shouldBe result2
    }
  }

  it should "return its second partitions in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.split(TestMatrixSplit.TestPartitioner[Position2D](Second))
    } Then {
      _.toList.sorted shouldBe result3
    }
  }

  it should "return its first partitions in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.split(TestMatrixSplit.TestPartitioner[Position3D](First))
    } Then {
      _.toList.sorted shouldBe result4
    }
  }

  it should "return its second partitions in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.split(TestMatrixSplit.TestPartitioner[Position3D](Second))
    } Then {
      _.toList.sorted shouldBe result5
    }
  }

  it should "return its third partitions in 3D" in {
    Given {
      data3
      } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.split(TestMatrixSplit.TestPartitioner[Position3D](Third))
    } Then {
      _.toList.sorted shouldBe result6
    }
  }

  "A Matrix.splitWithValue" should "return its first partitions in 1D" in {
    Given {
      data1
    } When {
      cells: TypedPipe[Cell[Position1D]] =>
        cells.splitWithValue(TestMatrixSplit.TestPartitionerWithValue[Position1D](), ValuePipe(First))
    } Then {
      _.toList.sorted shouldBe result7
    }
  }

  it should "return its first partitions in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.splitWithValue(TestMatrixSplit.TestPartitionerWithValue[Position2D](), ValuePipe(First))
    } Then {
      _.toList.sorted shouldBe result8
    }
  }

  it should "return its second partitions in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.splitWithValue(TestMatrixSplit.TestPartitionerWithValue[Position2D](), ValuePipe(Second))
    } Then {
      _.toList.sorted shouldBe result9
    }
  }

  it should "return its first partitions in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.splitWithValue(TestMatrixSplit.TestPartitionerWithValue[Position3D](), ValuePipe(First))
    } Then {
      _.toList.sorted shouldBe result10
    }
  }

  it should "return its second partitions in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.splitWithValue(TestMatrixSplit.TestPartitionerWithValue[Position3D](), ValuePipe(Second))
    } Then {
      _.toList.sorted shouldBe result11
    }
  }

  it should "return its third partitions in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.splitWithValue(TestMatrixSplit.TestPartitionerWithValue[Position3D](), ValuePipe(Third))
    } Then {
      _.toList.sorted shouldBe result12
    }
  }
}

class TestSparkMatrixSplit extends TestMatrixSplit {

  "A Matrix.split" should "return its first partitions in 1D" in {
    toRDD(data1)
      .split(TestMatrixSplit.TestPartitioner[Position1D](First))
      .toList.sorted shouldBe result1
  }

  it should "return its first partitions in 2D" in {
    toRDD(data2)
      .split(TestMatrixSplit.TestPartitioner[Position2D](First))
      .toList.sorted shouldBe result2
  }

  it should "return its second partitions in 2D" in {
    toRDD(data2)
      .split(TestMatrixSplit.TestPartitioner[Position2D](Second))
      .toList.sorted shouldBe result3
  }

  it should "return its first partitions in 3D" in {
    toRDD(data3)
      .split(TestMatrixSplit.TestPartitioner[Position3D](First))
      .toList.sorted shouldBe result4
  }

  it should "return its second partitions in 3D" in {
    toRDD(data3)
      .split(TestMatrixSplit.TestPartitioner[Position3D](Second))
      .toList.sorted shouldBe result5
  }

  it should "return its third partitions in 3D" in {
    toRDD(data3)
      .split(TestMatrixSplit.TestPartitioner[Position3D](Third))
      .toList.sorted shouldBe result6
  }

  "A Matrix.splitWithValue" should "return its first partitions in 1D" in {
    toRDD(data1)
      .splitWithValue(TestMatrixSplit.TestPartitionerWithValue[Position1D](), First)
      .toList.sorted shouldBe result7
  }

  it should "return its first partitions in 2D" in {
    toRDD(data2)
      .splitWithValue(TestMatrixSplit.TestPartitionerWithValue[Position2D](), First)
      .toList.sorted shouldBe result8
  }

  it should "return its second partitions in 2D" in {
    toRDD(data2)
      .splitWithValue(TestMatrixSplit.TestPartitionerWithValue[Position2D](), Second)
      .toList.sorted shouldBe result9
  }

  it should "return its first partitions in 3D" in {
    toRDD(data3)
      .splitWithValue(TestMatrixSplit.TestPartitionerWithValue[Position3D](), First)
      .toList.sorted shouldBe result10
  }

  it should "return its second partitions in 3D" in {
    toRDD(data3)
      .splitWithValue(TestMatrixSplit.TestPartitionerWithValue[Position3D](), Second)
      .toList.sorted shouldBe result11
  }

  it should "return its third partitions in 3D" in {
    toRDD(data3)
      .splitWithValue(TestMatrixSplit.TestPartitionerWithValue[Position3D](), Third)
      .toList.sorted shouldBe result12
  }
}

trait TestMatrixSample extends TestMatrix {

  val ext = "foo"

  val result1 = List(Cell(Position1D("foo"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")))

  val result2 = List(Cell(Position2D("bar", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position2D("baz", 2), Content(DiscreteSchema[Codex.LongCodex](), 19)),
    Cell(Position2D("foo", 1), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
    Cell(Position2D("foo", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position2D("foo", 3), Content(NominalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position2D("foo", 4), Content(DateSchema[Codex.DateTimeCodex](),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))

  val result3 = List(Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position3D("baz", 2, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
    Cell(Position3D("foo", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
    Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position3D("foo", 3, "xyz"), Content(NominalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position3D("foo", 4, "xyz"), Content(DateSchema[Codex.DateTimeCodex](),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))

  val result4 = List(Cell(Position1D("foo"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")))

  val result5 = List(Cell(Position2D("foo", 1), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
    Cell(Position2D("foo", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position2D("foo", 3), Content(NominalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position2D("foo", 4), Content(DateSchema[Codex.DateTimeCodex](),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))

  val result6 = List(Cell(Position3D("foo", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
    Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position3D("foo", 3, "xyz"), Content(NominalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position3D("foo", 4, "xyz"), Content(DateSchema[Codex.DateTimeCodex](),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))
}

object TestMatrixSample {

  case class TestSampler[P <: Position]() extends Sampler[P] {
    def select(cell: Cell[P]): Boolean = {
      cell.position.coordinates.contains(StringValue("foo")) || cell.position.coordinates.contains(LongValue(2))
    }
  }

  case class TestSamplerWithValue[P <: Position]() extends SamplerWithValue[P] {
    type V = String
    def selectWithValue(cell: Cell[P], ext: V): Boolean = cell.position.coordinates.contains(StringValue(ext))
  }
}

class TestScaldingMatrixSample extends TestMatrixSample with TBddDsl {

  "A Matrix.sample" should "return its sampled data in 1D" in {
    Given {
      data1
    } When {
      cells: TypedPipe[Cell[Position1D]] =>
        cells.sample(TestMatrixSample.TestSampler[Position1D]())
    } Then {
      _.toList.sortBy(_.position) shouldBe result1
    }
  }

  it should "return its sampled data in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.sample(TestMatrixSample.TestSampler[Position2D]())
    } Then {
      _.toList.sortBy(_.position) shouldBe result2
    }
  }

  it should "return its sampled data in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.sample(TestMatrixSample.TestSampler[Position3D]())
    } Then {
      _.toList.sortBy(_.position) shouldBe result3
    }
  }

  "A Matrix.sampleWithValue" should "return its sampled data in 1D" in {
    Given {
      data1
    } When {
      cells: TypedPipe[Cell[Position1D]] =>
        cells.sampleWithValue(TestMatrixSample.TestSamplerWithValue[Position1D](), ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result4
    }
  }

  it should "return its sampled data in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.sampleWithValue(TestMatrixSample.TestSamplerWithValue[Position2D](), ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result5
    }
  }

  it should "return its sampled data in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.sampleWithValue(TestMatrixSample.TestSamplerWithValue[Position3D](), ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result6
    }
  }
}

class TestSparkMatrixSample extends TestMatrixSample {

  "A Matrix.sample" should "return its sampled data in 1D" in {
    toRDD(data1)
      .sample(TestMatrixSample.TestSampler[Position1D]())
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its sampled data in 2D" in {
    toRDD(data2)
      .sample(TestMatrixSample.TestSampler[Position2D]())
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its sampled data in 3D" in {
    toRDD(data3)
      .sample(TestMatrixSample.TestSampler[Position3D]())
      .toList.sortBy(_.position) shouldBe result3
  }

  "A Matrix.sampleWithValue" should "return its sampled data in 1D" in {
    toRDD(data1)
      .sampleWithValue(TestMatrixSample.TestSamplerWithValue[Position1D](), ext)
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its sampled data in 2D" in {
    toRDD(data2)
      .sampleWithValue(TestMatrixSample.TestSamplerWithValue[Position2D](), ext)
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its sampled data in 3D" in {
    toRDD(data3)
      .sampleWithValue(TestMatrixSample.TestSamplerWithValue[Position3D](), ext)
      .toList.sortBy(_.position) shouldBe result6
  }
}

class TestMatrixDomain extends TestMatrix {

  val dataA = List(Cell(Position1D(1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
    Cell(Position1D(2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position1D(3), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)))

  val dataB = List(Cell(Position2D(1, 3), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
    Cell(Position2D(2, 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position2D(3, 1), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)))

  val dataC = List(Cell(Position3D(1, 1, 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
    Cell(Position3D(2, 2, 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position3D(3, 3, 3), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
    Cell(Position3D(1, 2, 3), Content(ContinuousSchema[Codex.DoubleCodex](), 0)))

  val dataD = List(Cell(Position4D(1, 4, 2, 3), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
    Cell(Position4D(2, 3, 1, 4), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position4D(3, 2, 4, 1), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
    Cell(Position4D(4, 1, 3, 2), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position4D(1, 2, 3, 4), Content(ContinuousSchema[Codex.DoubleCodex](), 0)))

  val dataE = List(Cell(Position5D(1, 5, 4, 3, 2), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
    Cell(Position5D(2, 1, 5, 4, 3), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position5D(3, 2, 1, 5, 4), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
    Cell(Position5D(4, 3, 2, 1, 5), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position5D(5, 4, 3, 2, 1), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
    Cell(Position5D(1, 2, 3, 4, 5), Content(ContinuousSchema[Codex.DoubleCodex](), 0)))

  val result1 = List(Position1D(1), Position1D(2), Position1D(3))

  val result2 = List(Position2D(1, 1), Position2D(1, 2), Position2D(1, 3), Position2D(2, 1), Position2D(2, 2),
    Position2D(2, 3), Position2D(3, 1), Position2D(3, 2), Position2D(3, 3))

  private val l3 = List(1, 2, 3)
  private val i3 = for (a <- l3; b <- l3; c <- l3) yield Iterable(Position3D(a, b, c))
  val result3 = i3.toList.flatten.sorted

  private val l4 = List(1, 2, 3, 4)
  private val i4 = for (a <- l4; b <- l4; c <- l4; d <- l4) yield Iterable(Position4D(a, b, c, d))
  val result4 = i4.toList.flatten.sorted

  private val l5 = List(1, 2, 3, 4, 5)
  private val i5 = for (a <- l5; b <- l5; c <- l5; d <- l5; e <- l5) yield Iterable(Position5D(a, b, c, d, e))
  val result5 = i5.toList.flatten.sorted
}

class TestScaldingMatrixDomain extends TestMatrixDomain with TBddDsl {

  "A Matrix.domain" should "return its domain in 1D" in {
    Given {
      dataA
    } When {
      cells: TypedPipe[Cell[Position1D]] =>
        cells.domain()
    } Then {
      _.toList.sorted shouldBe result1
    }
  }

  it should "return its domain in 2D" in {
    Given {
      dataB
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.domain()
    } Then {
      _.toList.sorted shouldBe result2
    }
  }

  it should "return its domain in 3D" in {
    Given {
      dataC
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.domain()
    } Then {
      _.toList.sorted shouldBe result3
    }
  }

  it should "return its domain in 4D" in {
    Given {
      dataD
    } When {
      cells: TypedPipe[Cell[Position4D]] =>
        cells.domain()
    } Then {
      _.toList.sorted shouldBe result4
    }
  }

  it should "return its domain in 5D" in {
    Given {
      dataE
    } When {
      cells: TypedPipe[Cell[Position5D]] =>
        cells.domain()
    } Then {
      _.toList.sorted shouldBe result5
    }
  }
}

class TestSparkMatrixDomain extends TestMatrixDomain {

  "A Matrix.domain" should "return its domain in 1D" in {
    toRDD(dataA)
      .domain()
      .toList.sorted shouldBe result1
  }

  it should "return its domain in 2D" in {
    toRDD(dataB)
      .domain()
      .toList.sorted shouldBe result2
  }

  it should "return its domain in 3D" in {
    toRDD(dataC)
      .domain()
      .toList.sorted shouldBe result3
  }

  it should "return its domain in 4D" in {
    toRDD(dataD)
      .domain()
      .toList.sorted shouldBe result4
  }

  it should "return its domain in 5D" in {
    toRDD(dataE)
      .domain()
      .toList.sorted shouldBe result5
  }
}

trait TestMatrixJoin extends TestMatrix {

  val dataA = List(Cell(Position2D("bar", 5), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
    Cell(Position2D("baz", 5), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position2D("qux", 5), Content(OrdinalSchema[Codex.StringCodex](), "12.56")),
    Cell(Position2D("bar", 6), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position2D("baz", 6), Content(DiscreteSchema[Codex.LongCodex](), 19)),
    Cell(Position2D("bar", 7), Content(OrdinalSchema[Codex.LongCodex](), 19)))

  val dataB = List(Cell(Position2D("foo.2", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position2D("bar.2", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position2D("baz.2", 2), Content(DiscreteSchema[Codex.LongCodex](), 19)),
    Cell(Position2D("foo.2", 3), Content(NominalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position2D("bar.2", 3), Content(OrdinalSchema[Codex.LongCodex](), 19)))

  val dataC = List(Cell(Position2D("foo.2", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position2D("bar.2", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position2D("baz.2", 2), Content(DiscreteSchema[Codex.LongCodex](), 19)),
    Cell(Position2D("foo.2", 3), Content(NominalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position2D("bar.2", 3), Content(OrdinalSchema[Codex.LongCodex](), 19)))

  val dataD = List(Cell(Position2D("bar", 5), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
    Cell(Position2D("baz", 5), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position2D("qux", 5), Content(OrdinalSchema[Codex.StringCodex](), "12.56")),
    Cell(Position2D("bar", 6), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position2D("baz", 6), Content(DiscreteSchema[Codex.LongCodex](), 19)),
    Cell(Position2D("bar", 7), Content(OrdinalSchema[Codex.LongCodex](), 19)))

  val dataE = List(Cell(Position3D("bar", 5, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
    Cell(Position3D("baz", 5, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position3D("qux", 5, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")),
    Cell(Position3D("bar", 6, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position3D("baz", 6, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
    Cell(Position3D("bar", 7, "xyz"), Content(OrdinalSchema[Codex.LongCodex](), 19)))

  val dataF = List(Cell(Position3D("foo.2", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position3D("bar.2", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position3D("baz.2", 2, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
    Cell(Position3D("foo.2", 3, "xyz"), Content(NominalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position3D("bar.2", 3, "xyz"), Content(OrdinalSchema[Codex.LongCodex](), 19)))

  val dataG = List(Cell(Position3D("foo.2", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position3D("bar.2", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position3D("baz.2", 2, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
    Cell(Position3D("foo.2", 3, "xyz"), Content(NominalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position3D("bar.2", 3, "xyz"), Content(OrdinalSchema[Codex.LongCodex](), 19)))

  val dataH = List(Cell(Position3D("bar", 5, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
    Cell(Position3D("baz", 5, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position3D("qux", 5, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")),
    Cell(Position3D("bar", 6, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position3D("baz", 6, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
    Cell(Position3D("bar", 7, "xyz"), Content(OrdinalSchema[Codex.LongCodex](), 19)))

  val dataI = List(Cell(Position3D("foo.2", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position3D("bar.2", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position3D("baz.2", 2, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
    Cell(Position3D("foo.2", 3, "xyz"), Content(NominalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position3D("bar.2", 3, "xyz"), Content(OrdinalSchema[Codex.LongCodex](), 19)))

  val dataJ = List(Cell(Position3D("bar", 1, "xyz.2"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
    Cell(Position3D("baz", 1, "xyz.2"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position3D("qux", 1, "xyz.2"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")),
    Cell(Position3D("bar", 2, "xyz.2"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position3D("baz", 2, "xyz.2"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
    Cell(Position3D("bar", 3, "xyz.2"), Content(OrdinalSchema[Codex.LongCodex](), 19)))

  val result1 = List(Cell(Position2D("bar", 1), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
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

  val result2 = List(Cell(Position2D("bar", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position2D("bar", 3), Content(OrdinalSchema[Codex.LongCodex](), 19)),
    Cell(Position2D("bar.2", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position2D("bar.2", 3), Content(OrdinalSchema[Codex.LongCodex](), 19)),
    Cell(Position2D("baz", 2), Content(DiscreteSchema[Codex.LongCodex](), 19)),
    Cell(Position2D("baz.2", 2), Content(DiscreteSchema[Codex.LongCodex](), 19)),
    Cell(Position2D("foo", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position2D("foo", 3), Content(NominalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position2D("foo.2", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position2D("foo.2", 3), Content(NominalSchema[Codex.StringCodex](), "9.42")))

  val result3 = List(Cell(Position2D("bar", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position2D("bar", 3), Content(OrdinalSchema[Codex.LongCodex](), 19)),
    Cell(Position2D("bar.2", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position2D("bar.2", 3), Content(OrdinalSchema[Codex.LongCodex](), 19)),
    Cell(Position2D("baz", 2), Content(DiscreteSchema[Codex.LongCodex](), 19)),
    Cell(Position2D("baz.2", 2), Content(DiscreteSchema[Codex.LongCodex](), 19)),
    Cell(Position2D("foo", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position2D("foo", 3), Content(NominalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position2D("foo.2", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position2D("foo.2", 3), Content(NominalSchema[Codex.StringCodex](), "9.42")))

  val result4 = List(Cell(Position2D("bar", 1), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
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

  val result5 = List(Cell(Position3D("bar", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
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

  val result6 = List(Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position3D("bar", 3, "xyz"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
    Cell(Position3D("bar.2", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position3D("bar.2", 3, "xyz"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
    Cell(Position3D("baz", 2, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
    Cell(Position3D("baz.2", 2, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
    Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position3D("foo", 3, "xyz"), Content(NominalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position3D("foo.2", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position3D("foo.2", 3, "xyz"), Content(NominalSchema[Codex.StringCodex](), "9.42")))

  val result7 = List(Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position3D("bar", 3, "xyz"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
    Cell(Position3D("bar.2", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position3D("bar.2", 3, "xyz"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
    Cell(Position3D("baz", 2, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
    Cell(Position3D("baz.2", 2, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
    Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position3D("foo", 3, "xyz"), Content(NominalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position3D("foo.2", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position3D("foo.2", 3, "xyz"), Content(NominalSchema[Codex.StringCodex](), "9.42")))

  val result8 = List(Cell(Position3D("bar", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
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

  val result9 = List(Cell(Position3D("bar", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
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

  val result10 = List(Cell(Position3D("bar", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
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

class TestScaldingMatrixJoin extends TestMatrixJoin with TBddDsl {

  "A Matrix.join" should "return its first over join in 2D" in {
    Given {
      data2
    } And {
      dataA
    } When {
      (cells: TypedPipe[Cell[Position2D]], that: TypedPipe[Cell[Position2D]]) =>
        cells.join(Over(First), that)
    } Then {
      _.toList.sortBy(_.position) shouldBe result1
    }
  }

  it should "return its first along join in 2D" in {
    Given {
      data2
    } And {
      dataB
    } When {
      (cells: TypedPipe[Cell[Position2D]], that: TypedPipe[Cell[Position2D]]) =>
        that.join(Along(First), cells)
    } Then {
      _.toList.sortBy(_.position) shouldBe result2
    }
  }

  it should "return its second over join in 2D" in {
    Given {
      data2
    } And {
      dataC
    } When {
      (cells: TypedPipe[Cell[Position2D]], that: TypedPipe[Cell[Position2D]]) =>
        that.join(Over(Second), cells)
    } Then {
      _.toList.sortBy(_.position) shouldBe result3
    }
  }

  it should "return its second along join in 2D" in {
    Given {
      data2
    } And {
      dataD
    } When {
      (cells: TypedPipe[Cell[Position2D]], that: TypedPipe[Cell[Position2D]]) =>
        cells.join(Along(Second), that)
    } Then {
      _.toList.sortBy(_.position) shouldBe result4
    }
  }

  it should "return its first over join in 3D" in {
    Given {
      data3
    } And {
      dataE
    } When {
      (cells: TypedPipe[Cell[Position3D]], that: TypedPipe[Cell[Position3D]]) =>
        cells.join(Over(First), that)
    } Then {
      _.toList.sortBy(_.position) shouldBe result5
    }
  }

  it should "return its first along join in 3D" in {
    Given {
      data3
    } And {
      dataF
    } When {
      (cells: TypedPipe[Cell[Position3D]], that: TypedPipe[Cell[Position3D]]) =>
        that.join(Along(First), cells)
    } Then {
      _.toList.sortBy(_.position) shouldBe result6
    }
  }

  it should "return its second over join in 3D" in {
    Given {
      data3
    } And {
      dataG
    } When {
      (cells: TypedPipe[Cell[Position3D]], that: TypedPipe[Cell[Position3D]]) =>
        that.join(Over(Second), cells)
    } Then {
      _.toList.sortBy(_.position) shouldBe result7
    }
  }

  it should "return its second along join in 3D" in {
    Given {
      data3
    } And {
      dataH
    } When {
      (cells: TypedPipe[Cell[Position3D]], that: TypedPipe[Cell[Position3D]]) =>
        cells.join(Along(Second), that)
    } Then {
      _.toList.sortBy(_.position) shouldBe result8
    }
  }

  it should "return its third over join in 3D" in {
    Given {
      data3
    } And {
      dataI
    } When {
      (cells: TypedPipe[Cell[Position3D]], that: TypedPipe[Cell[Position3D]]) =>
        that.join(Over(Third), cells)
    } Then {
      _.toList.sortBy(_.position) shouldBe result9
    }
  }

  it should "return its third along join in 3D" in {
    Given {
      data3
    } And {
      dataJ
    } When {
      (cells: TypedPipe[Cell[Position3D]], that: TypedPipe[Cell[Position3D]]) =>
        cells.join(Along(Third), that)
    } Then {
      _.toList.sortBy(_.position) shouldBe result10
    }
  }
}

class TestSparkMatrixJoin extends TestMatrixJoin {

  "A Matrix.join" should "return its first over join in 2D" in {
    toRDD(data2)
      .join(Over(First), toRDD(dataA))
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first along join in 2D" in {
    toRDD(dataB)
      .join(Along(First), toRDD(data2))
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its second over join in 2D" in {
    toRDD(dataC)
      .join(Over(Second), toRDD(data2))
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its second along join in 2D" in {
    toRDD(data2)
      .join(Along(Second), toRDD(dataD))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its first over join in 3D" in {
    toRDD(data3)
      .join(Over(First), toRDD(dataE))
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its first along join in 3D" in {
    toRDD(dataF)
      .join(Along(First), toRDD(data3))
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its second over join in 3D" in {
    toRDD(dataG)
      .join(Over(Second), toRDD(data3))
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its second along join in 3D" in {
    toRDD(data3)
      .join(Along(Second), toRDD(dataH))
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its third over join in 3D" in {
    toRDD(dataI)
      .join(Over(Third), toRDD(data3))
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its third along join in 3D" in {
    toRDD(data3)
      .join(Along(Third), toRDD(dataJ))
      .toList.sortBy(_.position) shouldBe result10
  }
}

trait TestMatrixUnique extends TestMatrix {

  val result1 = List(Content(OrdinalSchema[Codex.StringCodex](), "12.56"),
    Content(OrdinalSchema[Codex.StringCodex](), "3.14"),
    Content(OrdinalSchema[Codex.StringCodex](), "6.28"),
    Content(OrdinalSchema[Codex.StringCodex](), "9.42"))

  val result2 = List(Cell(Position1D("bar"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
    Cell(Position1D("baz"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position1D("foo"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
    Cell(Position1D("qux"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))

  val result3 = List(Content(ContinuousSchema[Codex.DoubleCodex](), 12.56),
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

  val result4 = List(Cell(Position1D("bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
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

  val result5 = List(Cell(Position1D(1), Content(OrdinalSchema[Codex.StringCodex](), "12.56")),
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

  val result6 = List(Cell(Position1D(1), Content(OrdinalSchema[Codex.StringCodex](), "12.56")),
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

  val result7 = List(Cell(Position1D("bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
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

  val result8 = List(Content(ContinuousSchema[Codex.DoubleCodex](), 12.56),
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

  val result9 = List(Cell(Position1D("bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
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

  val result10 = List(Cell(Position2D(1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")),
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

  val result11 = List(Cell(Position1D(1), Content(OrdinalSchema[Codex.StringCodex](), "12.56")),
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

  val result12 = List(Cell(Position2D("bar", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
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

  val result13 = List(Cell(Position1D("xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
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

  val result14 = List(Cell(Position2D("bar", 1), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
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

class TestScaldingMatrixUnique extends TestMatrixUnique with TBddDsl {

  "A Matrix.unique" should "return its content in 1D" in {
    Given {
      data1
    } When {
      cells: TypedPipe[Cell[Position1D]] =>
        cells.unique()
    } Then {
      _.toList.sortBy(_.toString) shouldBe result1
    }
  }

  it should "return its first over content in 1D" in {
    Given {
      data1
    } When {
      cells: TypedPipe[Cell[Position1D]] =>
        cells.unique(Over(First))
    } Then {
      _.toList.sortBy(_.toString) shouldBe result2
    }
  }

  it should "return its content in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.unique()
    } Then {
      _.toList.sortBy(_.toString) shouldBe result3
    }
  }

  it should "return its first over content in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.unique(Over(First))
    } Then {
      _.toList.sortBy(_.toString) shouldBe result4
    }
  }

  it should "return its first along content in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.unique(Along(First))
    } Then {
      _.toList.sortBy(_.toString) shouldBe result5
    }
  }

  it should "return its second over content in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.unique(Over(Second))
    } Then {
      _.toList.sortBy(_.toString) shouldBe result6
    }
  }

  it should "return its second along content in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.unique(Along(Second))
    } Then {
      _.toList.sortBy(_.toString) shouldBe result7
    }
  }

  it should "return its content in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.unique()
    } Then {
      _.toList.sortBy(_.toString) shouldBe result8
    }
  }

  it should "return its first over content in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.unique(Over(First))
    } Then {
      _.toList.sortBy(_.toString) shouldBe result9
    }
  }

  it should "return its first along content in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.unique(Along(First))
    } Then {
      _.toList.sortBy(_.toString) shouldBe result10
    }
  }

  it should "return its second over content in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.unique(Over(Second))
    } Then {
      _.toList.sortBy(_.toString) shouldBe result11
    }
  }

  it should "return its second along content in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.unique(Along(Second))
    } Then {
      _.toList.sortBy(_.toString) shouldBe result12
    }
  }

  it should "return its third over content in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.unique(Over(Third))
    } Then {
      _.toList.sortBy(_.toString) shouldBe result13
    }
  }

  it should "return its third along content in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.unique(Along(Third))
    } Then {
      _.toList.sortBy(_.toString) shouldBe result14
    }
  }
}

class TestSparkMatrixUnique extends TestMatrixUnique {

  "A Matrix.unique" should "return its content in 1D" in {
    toRDD(data1)
      .unique()
      .toList.sortBy(_.toString) shouldBe result1
  }

  it should "return its first over content in 1D" in {
    toRDD(data1)
      .unique(Over(First))
      .toList.sortBy(_.toString) shouldBe result2
  }

  it should "return its content in 2D" in {
    toRDD(data2)
      .unique()
      .toList.sortBy(_.toString) shouldBe result3
  }

  it should "return its first over content in 2D" in {
    toRDD(data2)
      .unique(Over(First))
      .toList.sortBy(_.toString) shouldBe result4
  }

  it should "return its first along content in 2D" in {
    toRDD(data2)
      .unique(Along(First))
      .toList.sortBy(_.toString) shouldBe result5
  }

  it should "return its second over content in 2D" in {
    toRDD(data2)
      .unique(Over(Second))
      .toList.sortBy(_.toString) shouldBe result6
  }

  it should "return its second along content in 2D" in {
    toRDD(data2)
      .unique(Along(Second))
      .toList.sortBy(_.toString) shouldBe result7
  }

  it should "return its content in 3D" in {
    toRDD(data3)
      .unique()
      .toList.sortBy(_.toString) shouldBe result8
  }

  it should "return its first over content in 3D" in {
    toRDD(data3)
      .unique(Over(First))
      .toList.sortBy(_.toString) shouldBe result9
  }

  it should "return its first along content in 3D" in {
    toRDD(data3)
      .unique(Along(First))
      .toList.sortBy(_.toString) shouldBe result10
  }

  it should "return its second over content in 3D" in {
    toRDD(data3)
      .unique(Over(Second))
      .toList.sortBy(_.toString) shouldBe result11
  }

  it should "return its second along content in 3D" in {
    toRDD(data3)
      .unique(Along(Second))
      .toList.sortBy(_.toString) shouldBe result12
  }

  it should "return its third over content in 3D" in {
    toRDD(data3)
      .unique(Over(Third))
      .toList.sortBy(_.toString) shouldBe result13
  }

  it should "return its third along content in 3D" in {
    toRDD(data3)
      .unique(Along(Third))
      .toList.sortBy(_.toString) shouldBe result14
  }
}

trait TestMatrixPairwise extends TestMatrix {

  val ext = 1.0

  val dataA = List(Cell(Position1D("bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 1)),
    Cell(Position1D("baz"), Content(ContinuousSchema[Codex.DoubleCodex](), 2)))

  val dataB = List(Cell(Position2D("bar", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 1)),
    Cell(Position2D("bar", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 2)),
    Cell(Position2D("bar", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 3)),
    Cell(Position2D("baz", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 4)),
    Cell(Position2D("baz", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 5)))

  val dataC = List(Cell(Position2D("bar", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 1)),
    Cell(Position2D("baz", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 2)),
    Cell(Position2D("foo", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 3)),
    Cell(Position2D("foo", 4), Content(ContinuousSchema[Codex.DoubleCodex](), 4)))

  val dataD = List(Cell(Position2D("bar", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 1)),
    Cell(Position2D("baz", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 2)),
    Cell(Position2D("foo", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 3)),
    Cell(Position2D("foo", 4), Content(ContinuousSchema[Codex.DoubleCodex](), 4)))

  val dataE = List(Cell(Position2D("bar", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 1)),
    Cell(Position2D("bar", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 2)),
    Cell(Position2D("bar", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 3)),
    Cell(Position2D("baz", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 4)),
    Cell(Position2D("baz", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 5)))

  val dataF = List(Cell(Position3D("bar", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 1)),
    Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 2)),
    Cell(Position3D("bar", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3)),
    Cell(Position3D("baz", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 4)),
    Cell(Position3D("baz", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 5)))

  val dataG = List(Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 1)),
    Cell(Position3D("baz", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 2)),
    Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3)),
    Cell(Position3D("foo", 4, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 4)))

  val dataH = List(Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 1)),
    Cell(Position3D("baz", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 2)),
    Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3)),
    Cell(Position3D("foo", 4, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 4)))

  val dataI = List(Cell(Position3D("bar", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 1)),
    Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 2)),
    Cell(Position3D("bar", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3)),
    Cell(Position3D("baz", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 4)),
    Cell(Position3D("baz", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 5)))

  val dataJ = List(Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 1)),
    Cell(Position3D("baz", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 2)),
    Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3)),
    Cell(Position3D("foo", 4, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 4)))

  val dataK = List(Cell(Position3D("bar", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 1)),
    Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 2)),
    Cell(Position3D("bar", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3)),
    Cell(Position3D("baz", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 4)),
    Cell(Position3D("baz", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 5)))

  val dataL = List(Cell(Position1D("bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 1)),
    Cell(Position1D("baz"), Content(ContinuousSchema[Codex.DoubleCodex](), 2)))

  val dataM = List(Cell(Position2D("bar", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 1)),
    Cell(Position2D("bar", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 2)),
    Cell(Position2D("bar", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 3)),
    Cell(Position2D("baz", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 4)),
    Cell(Position2D("baz", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 5)))

  val dataN = List(Cell(Position2D("bar", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 1)),
    Cell(Position2D("baz", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 2)),
    Cell(Position2D("foo", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 3)),
    Cell(Position2D("foo", 4), Content(ContinuousSchema[Codex.DoubleCodex](), 4)))

  val dataO = List(Cell(Position2D("bar", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 1)),
    Cell(Position2D("baz", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 2)),
    Cell(Position2D("foo", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 3)),
    Cell(Position2D("foo", 4), Content(ContinuousSchema[Codex.DoubleCodex](), 4)))

  val dataP = List(Cell(Position2D("bar", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 1)),
    Cell(Position2D("bar", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 2)),
    Cell(Position2D("bar", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 3)),
    Cell(Position2D("baz", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 4)),
    Cell(Position2D("baz", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 5)))

  val dataQ = List(Cell(Position3D("bar", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 1)),
    Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 2)),
    Cell(Position3D("bar", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3)),
    Cell(Position3D("baz", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 4)),
    Cell(Position3D("baz", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 5)))

  val dataR = List(Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 1)),
    Cell(Position3D("baz", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 2)),
    Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3)),
    Cell(Position3D("foo", 4, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 4)))

  val dataS = List(Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 1)),
    Cell(Position3D("baz", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 2)),
    Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3)),
    Cell(Position3D("foo", 4, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 4)))

  val dataT = List(Cell(Position3D("bar", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 1)),
    Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 2)),
    Cell(Position3D("bar", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3)),
    Cell(Position3D("baz", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 4)),
    Cell(Position3D("baz", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 5)))

  val dataU = List(Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 1)),
    Cell(Position3D("baz", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 2)),
    Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3)),
    Cell(Position3D("foo", 4, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 4)))

  val dataV = List(Cell(Position3D("bar", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 1)),
    Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 2)),
    Cell(Position3D("bar", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3)),
    Cell(Position3D("baz", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 4)),
    Cell(Position3D("baz", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 5)))

  val result1 = List(Cell(Position1D("(baz+bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 6.28)),
    Cell(Position1D("(foo+bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 6.28)),
    Cell(Position1D("(foo+baz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 9.42)),
    Cell(Position1D("(qux+bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 6.28)),
    Cell(Position1D("(qux+baz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 9.42)),
    Cell(Position1D("(qux+foo)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 3.14)))

  val result2 = List(Cell(Position2D("(baz+bar)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 6.28)),
    Cell(Position2D("(baz+bar)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 12.56)),
    Cell(Position2D("(foo+bar)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 6.28)),
    Cell(Position2D("(foo+bar)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 12.56)),
    Cell(Position2D("(foo+bar)", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 18.84)),
    Cell(Position2D("(foo+baz)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 9.42)),
    Cell(Position2D("(foo+baz)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 18.84)),
    Cell(Position2D("(qux+bar)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 6.28)),
    Cell(Position2D("(qux+baz)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 9.42)),
    Cell(Position2D("(qux+foo)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 3.14)))

  val result3 = List(Cell(Position2D("(2+1)", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 6.28)),
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

  val result4 = List(Cell(Position2D("(2+1)", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 6.28)),
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

  val result5 = List(Cell(Position2D("(baz+bar)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 6.28)),
    Cell(Position2D("(baz+bar)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 12.56)),
    Cell(Position2D("(foo+bar)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 6.28)),
    Cell(Position2D("(foo+bar)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 12.56)),
    Cell(Position2D("(foo+bar)", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 18.84)),
    Cell(Position2D("(foo+baz)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 9.42)),
    Cell(Position2D("(foo+baz)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 18.84)),
    Cell(Position2D("(qux+bar)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 6.28)),
    Cell(Position2D("(qux+baz)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 9.42)),
    Cell(Position2D("(qux+foo)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 3.14)))

  val result6 = List(
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

  val result7 = List(
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

  val result8 = List(
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

  val result9 = List(
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

  val result10 = List()

  val result11 = List(
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

  val result12 = List(Cell(Position1D("(baz+bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 6.28 + 1)),
    Cell(Position1D("(foo+bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 6.28 + 1)),
    Cell(Position1D("(foo+baz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 9.42 + 1)),
    Cell(Position1D("(qux+bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 6.28 + 1)),
    Cell(Position1D("(qux+baz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 9.42 + 1)),
    Cell(Position1D("(qux+foo)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 3.14 + 1)))

  val result13 = List(Cell(Position2D("(baz+bar)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 6.28 + 1)),
    Cell(Position2D("(baz+bar)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 12.56 + 1)),
    Cell(Position2D("(foo+bar)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 6.28 + 1)),
    Cell(Position2D("(foo+bar)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 12.56 + 1)),
    Cell(Position2D("(foo+bar)", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 18.84 + 1)),
    Cell(Position2D("(foo+baz)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 9.42 + 1)),
    Cell(Position2D("(foo+baz)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 18.84 + 1)),
    Cell(Position2D("(qux+bar)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 6.28 + 1)),
    Cell(Position2D("(qux+baz)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 9.42 + 1)),
    Cell(Position2D("(qux+foo)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 3.14 + 1)))

  val result14 = List(
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

  val result15 = List(
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

  val result16 = List(Cell(Position2D("(baz+bar)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 6.28 + 1)),
    Cell(Position2D("(baz+bar)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 12.56 + 1)),
    Cell(Position2D("(foo+bar)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 6.28 + 1)),
    Cell(Position2D("(foo+bar)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 12.56 + 1)),
    Cell(Position2D("(foo+bar)", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 18.84 + 1)),
    Cell(Position2D("(foo+baz)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 9.42 + 1)),
    Cell(Position2D("(foo+baz)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 18.84 + 1)),
    Cell(Position2D("(qux+bar)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 6.28 + 1)),
    Cell(Position2D("(qux+baz)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 9.42 + 1)),
    Cell(Position2D("(qux+foo)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 3.14 + 1)))

  val result17 = List(
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

  val result18 = List(
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

  val result19 = List(
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

  val result20 = List(
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

  val result21 = List()

  val result22 = List(
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

  val result23 = List(Cell(Position1D("(baz+bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 1)),
    Cell(Position1D("(foo+bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 1)),
    Cell(Position1D("(foo+baz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 2)),
    Cell(Position1D("(qux+bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 1)),
    Cell(Position1D("(qux+baz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 2)))

  val result24 = List(Cell(Position2D("(baz+bar)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 1)),
    Cell(Position2D("(baz+bar)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 2)),
    Cell(Position2D("(foo+bar)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 1)),
    Cell(Position2D("(foo+bar)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 2)),
    Cell(Position2D("(foo+bar)", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 3)),
    Cell(Position2D("(foo+baz)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 4)),
    Cell(Position2D("(foo+baz)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 5)),
    Cell(Position2D("(qux+bar)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 1)),
    Cell(Position2D("(qux+baz)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 4)))

  val result25 = List(Cell(Position2D("(3+2)", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 1)),
    Cell(Position2D("(3+2)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 3)),
    Cell(Position2D("(3-2)", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 1)),
    Cell(Position2D("(3-2)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 3)),
    Cell(Position2D("(4+2)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 3)),
    Cell(Position2D("(4-2)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 3)))

  val result26 = List(Cell(Position2D("(3+2)", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 1)),
    Cell(Position2D("(3+2)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 3)),
    Cell(Position2D("(3-2)", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 1)),
    Cell(Position2D("(3-2)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 3)),
    Cell(Position2D("(4+2)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 3)),
    Cell(Position2D("(4-2)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 3)))

  val result27 = List(Cell(Position2D("(baz+bar)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 1)),
    Cell(Position2D("(baz+bar)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 2)),
    Cell(Position2D("(foo+bar)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 1)),
    Cell(Position2D("(foo+bar)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 2)),
    Cell(Position2D("(foo+bar)", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 3)),
    Cell(Position2D("(foo+baz)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 4)),
    Cell(Position2D("(foo+baz)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 5)),
    Cell(Position2D("(qux+bar)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 1)),
    Cell(Position2D("(qux+baz)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 4)))

  val result28 = List(Cell(Position3D("(baz+bar)", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 1)),
    Cell(Position3D("(baz+bar)", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 2)),
    Cell(Position3D("(foo+bar)", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 1)),
    Cell(Position3D("(foo+bar)", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 2)),
    Cell(Position3D("(foo+bar)", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 3)),
    Cell(Position3D("(foo+baz)", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 4)),
    Cell(Position3D("(foo+baz)", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 5)),
    Cell(Position3D("(qux+bar)", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 1)),
    Cell(Position3D("(qux+baz)", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 4)))

  val result29 = List(
    Cell(Position2D("(3|xyz+2|xyz)", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 1)),
    Cell(Position2D("(3|xyz+2|xyz)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 3)),
    Cell(Position2D("(3|xyz-2|xyz)", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 1)),
    Cell(Position2D("(3|xyz-2|xyz)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 3)),
    Cell(Position2D("(4|xyz+2|xyz)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 3)),
    Cell(Position2D("(4|xyz-2|xyz)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 3)))

  val result30 = List(
    Cell(Position3D("(3+2)", "bar", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 1)),
    Cell(Position3D("(3+2)", "foo", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 3)),
    Cell(Position3D("(3-2)", "bar", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 1)),
    Cell(Position3D("(3-2)", "foo", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 3)),
    Cell(Position3D("(4+2)", "foo", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 3)),
    Cell(Position3D("(4-2)", "foo", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 3)))

  val result31 = List(
    Cell(Position2D("(baz|xyz+bar|xyz)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 1)),
    Cell(Position2D("(baz|xyz+bar|xyz)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 2)),
    Cell(Position2D("(foo|xyz+bar|xyz)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 1)),
    Cell(Position2D("(foo|xyz+bar|xyz)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 2)),
    Cell(Position2D("(foo|xyz+bar|xyz)", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 3)),
    Cell(Position2D("(foo|xyz+baz|xyz)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 4)),
    Cell(Position2D("(foo|xyz+baz|xyz)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 5)),
    Cell(Position2D("(qux|xyz+bar|xyz)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 1)),
    Cell(Position2D("(qux|xyz+baz|xyz)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 4)))

  val result32 = List()

  val result33 = List(
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

  val result34 = List(Cell(Position1D("(baz+bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 1 + 1)),
    Cell(Position1D("(foo+bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 1 + 1)),
    Cell(Position1D("(foo+baz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 2 + 1)),
    Cell(Position1D("(qux+bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 1 + 1)),
    Cell(Position1D("(qux+baz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 2 + 1)))

  val result35 = List(
    Cell(Position2D("(baz+bar)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 1 + 1)),
    Cell(Position2D("(baz+bar)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 2 + 1)),
    Cell(Position2D("(foo+bar)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 1 + 1)),
    Cell(Position2D("(foo+bar)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 2 + 1)),
    Cell(Position2D("(foo+bar)", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 3 + 1)),
    Cell(Position2D("(foo+baz)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 4 + 1)),
    Cell(Position2D("(foo+baz)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 5 + 1)),
    Cell(Position2D("(qux+bar)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 1 + 1)),
    Cell(Position2D("(qux+baz)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 4 + 1)))

  val result36 = List(Cell(Position2D("(3+2)", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 1 + 1)),
    Cell(Position2D("(3+2)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 3 + 1)),
    Cell(Position2D("(3-2)", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 1 - 1)),
    Cell(Position2D("(3-2)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 3 - 1)),
    Cell(Position2D("(4+2)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 3 + 1)),
    Cell(Position2D("(4-2)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 3 - 1)))

  val result37 = List(Cell(Position2D("(3+2)", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 1 + 1)),
    Cell(Position2D("(3+2)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 3 + 1)),
    Cell(Position2D("(3-2)", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 1 - 1)),
    Cell(Position2D("(3-2)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 3 - 1)),
    Cell(Position2D("(4+2)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 3 + 1)),
    Cell(Position2D("(4-2)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 3 - 1)))

  val result38 = List(Cell(Position2D("(baz+bar)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 1 + 1)),
    Cell(Position2D("(baz+bar)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 2 + 1)),
    Cell(Position2D("(foo+bar)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 1 + 1)),
    Cell(Position2D("(foo+bar)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 2 + 1)),
    Cell(Position2D("(foo+bar)", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 3 + 1)),
    Cell(Position2D("(foo+baz)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 4 + 1)),
    Cell(Position2D("(foo+baz)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 5 + 1)),
    Cell(Position2D("(qux+bar)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 1 + 1)),
    Cell(Position2D("(qux+baz)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 4 + 1)))

  val result39 = List(
    Cell(Position3D("(baz+bar)", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 1 + 1)),
    Cell(Position3D("(baz+bar)", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 2 + 1)),
    Cell(Position3D("(foo+bar)", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 1 + 1)),
    Cell(Position3D("(foo+bar)", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 2 + 1)),
    Cell(Position3D("(foo+bar)", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 3 + 1)),
    Cell(Position3D("(foo+baz)", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 4 + 1)),
    Cell(Position3D("(foo+baz)", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 5 + 1)),
    Cell(Position3D("(qux+bar)", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 1 + 1)),
    Cell(Position3D("(qux+baz)", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 4 + 1)))

  val result40 = List(
    Cell(Position2D("(3|xyz+2|xyz)", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 1 + 1)),
    Cell(Position2D("(3|xyz+2|xyz)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 3 + 1)),
    Cell(Position2D("(3|xyz-2|xyz)", "bar"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 1 - 1)),
    Cell(Position2D("(3|xyz-2|xyz)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 3 - 1)),
    Cell(Position2D("(4|xyz+2|xyz)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 3 + 1)),
    Cell(Position2D("(4|xyz-2|xyz)", "foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 3 - 1)))

  val result41 = List(
    Cell(Position3D("(3+2)", "bar", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 1 + 1)),
    Cell(Position3D("(3+2)", "foo", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 3 + 1)),
    Cell(Position3D("(3-2)", "bar", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 1 - 1)),
    Cell(Position3D("(3-2)", "foo", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 3 - 1)),
    Cell(Position3D("(4+2)", "foo", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 3 + 1)),
    Cell(Position3D("(4-2)", "foo", "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 3 - 1)))

  val result42 = List(
    Cell(Position2D("(baz|xyz+bar|xyz)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 1 + 1)),
    Cell(Position2D("(baz|xyz+bar|xyz)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 + 2 + 1)),
    Cell(Position2D("(foo|xyz+bar|xyz)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 1 + 1)),
    Cell(Position2D("(foo|xyz+bar|xyz)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 2 + 1)),
    Cell(Position2D("(foo|xyz+bar|xyz)", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 + 3 + 1)),
    Cell(Position2D("(foo|xyz+baz|xyz)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 + 4 + 1)),
    Cell(Position2D("(foo|xyz+baz|xyz)", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 + 5 + 1)),
    Cell(Position2D("(qux|xyz+bar|xyz)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 1 + 1)),
    Cell(Position2D("(qux|xyz+baz|xyz)", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 + 4 + 1)))

  val result43 = List()

  val result44 = List(
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

object TestMatrixPairwise {

  case class PlusX[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition]()
    extends OperatorWithValue[S, R, R#M] {
    type V = Double

    val plus = Plus(StringLocate[S, R]("(%1$s+%2$s)"))

    def computeWithValue(left: Cell[S], reml: R, right: Cell[S], remr: R, ext: V): Collection[Cell[R#M]] = {
      Collection(plus.compute(left, reml, right, remr).toList.map {
        case Cell(pos, Content(_, DoubleValue(d))) => Cell(pos, Content(ContinuousSchema[Codex.DoubleCodex](), d + ext))
      })
    }
  }

  case class MinusX[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition]()
    extends OperatorWithValue[S, R, R#M] {
    type V = Double

    val minus = Minus(StringLocate[S, R]("(%1$s-%2$s)"))

    def computeWithValue(left: Cell[S], reml: R, right: Cell[S], remr: R, ext: V): Collection[Cell[R#M]] = {
      Collection(minus.compute(left, reml, right, remr).toList.map {
        case Cell(pos, Content(_, DoubleValue(d))) => Cell(pos, Content(ContinuousSchema[Codex.DoubleCodex](), d - ext))
      })
    }
  }
}

class TestScaldingMatrixPairwise extends TestMatrixPairwise with TBddDsl {

  "A Matrix.pairwise" should "return its first over pairwise in 1D" in {
    Given {
      num1
    } When {
      cells: TypedPipe[Cell[Position1D]] =>
        cells.pairwise(Over(First), Lower, Plus(StringLocate[Position1D, Position0D]("(%1$s+%2$s)")))
    } Then {
      _.toList.sortBy(_.position) shouldBe result1
    }
  }

  it should "return its first over pairwise in 2D" in {
    Given {
      num2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.pairwise(Over(First), Lower, Plus(StringLocate[Position1D, Position1D]("(%1$s+%2$s)")))
    } Then {
      _.toList.sortBy(_.position) shouldBe result2
    }
  }

  it should "return its first along pairwise in 2D" in {
    Given {
      num2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.pairwise(Along(First), Lower, List(
          Plus(StringLocate[Position1D, Position1D]("(%1$s+%2$s)")),
          Minus(StringLocate[Position1D, Position1D]("(%1$s-%2$s)"))))
    } Then {
      _.toList.sortBy(_.position) shouldBe result3
    }
  }

  it should "return its second over pairwise in 2D" in {
    Given {
      num2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.pairwise(Over(Second), Lower, List(
          Plus(StringLocate[Position1D, Position1D]("(%1$s+%2$s)")),
          Minus(StringLocate[Position1D, Position1D]("(%1$s-%2$s)"))))
    } Then {
      _.toList.sortBy(_.position) shouldBe result4
    }
  }

  it should "return its second along pairwise in 2D" in {
    Given {
      num2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.pairwise(Along(Second), Lower, Plus(StringLocate[Position1D, Position1D]("(%1$s+%2$s)")))
    } Then {
      _.toList.sortBy(_.position) shouldBe result5
    }
  }

  it should "return its first over pairwise in 3D" in {
    Given {
      num3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.pairwise(Over(First), Lower, Plus(StringLocate[Position1D, Position2D]("(%1$s+%2$s)")))
    } Then {
      _.toList.sortBy(_.position) shouldBe result6
    }
  }

  it should "return its first along pairwise in 3D" in {
    Given {
      num3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.pairwise(Along(First), Lower, List(
          Plus(StringLocate[Position2D, Position1D]("(%1$s+%2$s)")),
          Minus(StringLocate[Position2D, Position1D]("(%1$s-%2$s)"))))
    } Then {
      _.toList.sortBy(_.position) shouldBe result7
    }
  }

  it should "return its second over pairwise in 3D" in {
    Given {
      num3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.pairwise(Over(Second), Lower, List(
          Plus(StringLocate[Position1D, Position2D]("(%1$s+%2$s)")),
          Minus(StringLocate[Position1D, Position2D]("(%1$s-%2$s)"))))
    } Then {
      _.toList.sortBy(_.position) shouldBe result8
    }
  }

  it should "return its second along pairwise in 3D" in {
    Given {
      num3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.pairwise(Along(Second), Lower, Plus(StringLocate[Position2D, Position1D]("(%1$s+%2$s)")))
    } Then {
      _.toList.sortBy(_.position) shouldBe result9
    }
  }

  it should "return its third over pairwise in 3D" in {
    Given {
      num3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.pairwise(Over(Third), Lower, List(
          Plus(StringLocate[Position1D, Position2D]("(%1$s+%2$s)")),
          Minus(StringLocate[Position1D, Position2D]("(%1$s-%2$s)"))))
    } Then {
      _.toList.sortBy(_.position) shouldBe result10
    }
  }

  it should "return its third along pairwise in 3D" in {
    Given {
      num3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.pairwise(Along(Third), Lower, Plus(StringLocate[Position2D, Position1D]("(%1$s+%2$s)")))
    } Then {
      _.toList.sortBy(_.position) shouldBe result11
    }
  }

  "A Matrix.pairwiseWithValue" should "return its first over pairwise in 1D" in {
    Given {
      num1
    } When {
      cells: TypedPipe[Cell[Position1D]] =>
        cells.pairwiseWithValue(Over(First), Lower, TestMatrixPairwise.PlusX[Position1D, Position0D](), ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result12
    }
  }

  it should "return its first over pairwise in 2D" in {
    Given {
      num2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.pairwiseWithValue(Over(First), Lower, TestMatrixPairwise.PlusX[Position1D, Position1D](), ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result13
    }
  }

  it should "return its first along pairwise in 2D" in {
    Given {
      num2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.pairwiseWithValue(Along(First), Lower, List(TestMatrixPairwise.PlusX[Position1D, Position1D](),
          TestMatrixPairwise.MinusX[Position1D, Position1D]()), ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result14
    }
  }

  it should "return its second over pairwise in 2D" in {
    Given {
      num2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.pairwiseWithValue(Over(Second), Lower, List(TestMatrixPairwise.PlusX[Position1D, Position1D](),
          TestMatrixPairwise.MinusX[Position1D, Position1D]()), ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result15
    }
  }

  it should "return its second along pairwise in 2D" in {
    Given {
      num2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.pairwiseWithValue(Along(Second), Lower, TestMatrixPairwise.PlusX[Position1D, Position1D](),
          ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result16
    }
  }

  it should "return its first over pairwise in 3D" in {
    Given {
      num3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.pairwiseWithValue(Over(First), Lower, TestMatrixPairwise.PlusX[Position1D, Position2D](), ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result17
    }
  }

  it should "return its first along pairwise in 3D" in {
    Given {
      num3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.pairwiseWithValue(Along(First), Lower, List(TestMatrixPairwise.PlusX[Position2D, Position1D](),
          TestMatrixPairwise.MinusX[Position2D, Position1D]()), ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result18
    }
  }

  it should "return its second over pairwise in 3D" in {
    Given {
      num3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.pairwiseWithValue(Over(Second), Lower, List(TestMatrixPairwise.PlusX[Position1D, Position2D](),
          TestMatrixPairwise.MinusX[Position1D, Position2D]()), ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result19
    }
  }

  it should "return its second along pairwise in 3D" in {
    Given {
      num3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.pairwiseWithValue(Along(Second), Lower, TestMatrixPairwise.PlusX[Position2D, Position1D](),
          ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result20
    }
  }

  it should "return its third over pairwise in 3D" in {
    Given {
      num3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.pairwiseWithValue(Over(Third), Lower, List(TestMatrixPairwise.PlusX[Position1D, Position2D](),
          TestMatrixPairwise.MinusX[Position1D, Position2D]()), ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result21
    }
  }

  it should "return its third along pairwise in 3D" in {
    Given {
      num3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.pairwiseWithValue(Along(Third), Lower, TestMatrixPairwise.PlusX[Position2D, Position1D](), ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result22
    }
  }

  "A Matrix.pairwiseBetween" should "return its first over pairwise in 1D" in {
    Given {
      num1
    } And {
      dataA
    } When {
      (cells: TypedPipe[Cell[Position1D]], that: TypedPipe[Cell[Position1D]]) =>
        cells.pairwiseBetween(Over(First), Lower, that, Plus(StringLocate[Position1D, Position0D]("(%1$s+%2$s)")))
    } Then {
      _.toList.sortBy(_.position) shouldBe result23
    }
  }

  it should "return its first over pairwise in 2D" in {
    Given {
      num2
    } And {
      dataB
    } When {
      (cells: TypedPipe[Cell[Position2D]], that: TypedPipe[Cell[Position2D]]) =>
        cells.pairwiseBetween(Over(First), Lower, that, Plus(StringLocate[Position1D, Position1D]("(%1$s+%2$s)")))
    } Then {
      _.toList.sortBy(_.position) shouldBe result24
    }
  }

  it should "return its first along pairwise in 2D" in {
    Given {
      num2
    } And {
      dataC
    } When {
      (cells: TypedPipe[Cell[Position2D]], that: TypedPipe[Cell[Position2D]]) =>
        cells.pairwiseBetween(Along(First), Lower, that, List(
          Plus(StringLocate[Position1D, Position1D]("(%1$s+%2$s)")),
          Minus(StringLocate[Position1D, Position1D]("(%1$s-%2$s)"))))
    } Then {
      _.toList.sortBy(_.position) shouldBe result25
    }
  }

  it should "return its second over pairwise in 2D" in {
    Given {
      num2
    } And {
      dataD
    } When {
      (cells: TypedPipe[Cell[Position2D]], that: TypedPipe[Cell[Position2D]]) =>
        cells.pairwiseBetween(Over(Second), Lower, that, List(
          Plus(StringLocate[Position1D, Position1D]("(%1$s+%2$s)")),
          Minus(StringLocate[Position1D, Position1D]("(%1$s-%2$s)"))))
    } Then {
      _.toList.sortBy(_.position) shouldBe result26
    }
  }

  it should "return its second along pairwise in 2D" in {
    Given {
      num2
    } And {
      dataE
    } When {
      (cells: TypedPipe[Cell[Position2D]], that: TypedPipe[Cell[Position2D]]) =>
        cells.pairwiseBetween(Along(Second), Lower, that, Plus(StringLocate[Position1D, Position1D]("(%1$s+%2$s)")))
    } Then {
      _.toList.sortBy(_.position) shouldBe result27
    }
  }

  it should "return its first over pairwise in 3D" in {
    Given {
      num3
    } And {
      dataF
    } When {
      (cells: TypedPipe[Cell[Position3D]], that: TypedPipe[Cell[Position3D]]) =>
        cells.pairwiseBetween(Over(First), Lower, that, Plus(StringLocate[Position1D, Position2D]("(%1$s+%2$s)")))
    } Then {
      _.toList.sortBy(_.position) shouldBe result28
    }
  }

  it should "return its first along pairwise in 3D" in {
    Given {
      num3
    } And {
      dataG
    } When {
      (cells: TypedPipe[Cell[Position3D]], that: TypedPipe[Cell[Position3D]]) =>
        cells.pairwiseBetween(Along(First), Lower, that, List(
          Plus(StringLocate[Position2D, Position1D]("(%1$s+%2$s)")),
          Minus(StringLocate[Position2D, Position1D]("(%1$s-%2$s)"))))
    } Then {
      _.toList.sortBy(_.position) shouldBe result29
    }
  }

  it should "return its second over pairwise in 3D" in {
    Given {
      num3
    } And {
      dataH
    } When {
      (cells: TypedPipe[Cell[Position3D]], that: TypedPipe[Cell[Position3D]]) =>
        cells.pairwiseBetween(Over(Second), Lower, that, List(
          Plus(StringLocate[Position1D, Position2D]("(%1$s+%2$s)")),
          Minus(StringLocate[Position1D, Position2D]("(%1$s-%2$s)"))))
    } Then {
      _.toList.sortBy(_.position) shouldBe result30
    }
  }

  it should "return its second along pairwise in 3D" in {
    Given {
      num3
    } And {
      dataI
    } When {
      (cells: TypedPipe[Cell[Position3D]], that: TypedPipe[Cell[Position3D]]) =>
        cells.pairwiseBetween(Along(Second), Lower, that, Plus(StringLocate[Position2D, Position1D]("(%1$s+%2$s)")))
    } Then {
      _.toList.sortBy(_.position) shouldBe result31
    }
  }

  it should "return its third over pairwise in 3D" in {
    Given {
      num3
    } And {
      dataJ
    } When {
      (cells: TypedPipe[Cell[Position3D]], that: TypedPipe[Cell[Position3D]]) =>
        cells.pairwiseBetween(Over(Third), Lower, that, List(
          Plus(StringLocate[Position1D, Position2D]("(%1$s+%2$s)")),
          Minus(StringLocate[Position1D, Position2D]("(%1$s-%2$s)"))))
    } Then {
      _.toList.sortBy(_.position) shouldBe result32
    }
  }

  it should "return its third along pairwise in 3D" in {
    Given {
      num3
    } And {
      dataK
    } When {
      (cells: TypedPipe[Cell[Position3D]], that: TypedPipe[Cell[Position3D]]) =>
        cells.pairwiseBetween(Along(Third), Lower, that, Plus(StringLocate[Position2D, Position1D]("(%1$s+%2$s)")))
    } Then {
      _.toList.sortBy(_.position) shouldBe result33
    }
  }

  "A Matrix.pairwiseBetweenWithValue" should "return its first over pairwise in 1D" in {
    Given {
      num1
    } And {
      dataL
    } When {
      (cells: TypedPipe[Cell[Position1D]], that: TypedPipe[Cell[Position1D]]) =>
        cells.pairwiseBetweenWithValue(Over(First), Lower, that, TestMatrixPairwise.PlusX[Position1D, Position0D](),
          ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result34
    }
  }

  it should "return its first over pairwise in 2D" in {
    Given {
      num2
    } And {
      dataM
    } When {
      (cells: TypedPipe[Cell[Position2D]], that: TypedPipe[Cell[Position2D]]) =>
        cells.pairwiseBetweenWithValue(Over(First), Lower, that, TestMatrixPairwise.PlusX[Position1D, Position1D](),
          ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result35
    }
  }

  it should "return its first along pairwise in 2D" in {
    Given {
      num2
    } And {
      dataN
    } When {
      (cells: TypedPipe[Cell[Position2D]], that: TypedPipe[Cell[Position2D]]) =>
        cells.pairwiseBetweenWithValue(Along(First), Lower, that, List(
          TestMatrixPairwise.PlusX[Position1D, Position1D](), TestMatrixPairwise.MinusX[Position1D, Position1D]()),
          ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result36
    }
  }

  it should "return its second over pairwise in 2D" in {
    Given {
      num2
    } And {
      dataO
    } When {
      (cells: TypedPipe[Cell[Position2D]], that: TypedPipe[Cell[Position2D]]) =>
        cells.pairwiseBetweenWithValue(Over(Second), Lower, that, List(
          TestMatrixPairwise.PlusX[Position1D, Position1D](), TestMatrixPairwise.MinusX[Position1D, Position1D]()),
          ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result37
    }
  }

  it should "return its second along pairwise in 2D" in {
    Given {
      num2
    } And {
      dataP
    } When {
      (cells: TypedPipe[Cell[Position2D]], that: TypedPipe[Cell[Position2D]]) =>
        cells.pairwiseBetweenWithValue(Along(Second), Lower, that, TestMatrixPairwise.PlusX[Position1D, Position1D](),
          ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result38
    }
  }

  it should "return its first over pairwise in 3D" in {
    Given {
      num3
    } And {
      dataQ
    } When {
      (cells: TypedPipe[Cell[Position3D]], that: TypedPipe[Cell[Position3D]]) =>
        cells.pairwiseBetweenWithValue(Over(First), Lower, that, TestMatrixPairwise.PlusX[Position1D, Position2D](),
          ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result39
    }
  }

  it should "return its first along pairwise in 3D" in {
    Given {
      num3
    } And {
      dataR
    } When {
      (cells: TypedPipe[Cell[Position3D]], that: TypedPipe[Cell[Position3D]]) =>
        cells.pairwiseBetweenWithValue(Along(First), Lower, that, List(
          TestMatrixPairwise.PlusX[Position2D, Position1D](), TestMatrixPairwise.MinusX[Position2D, Position1D]()),
          ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result40
    }
  }

  it should "return its second over pairwise in 3D" in {
    Given {
      num3
    } And {
      dataS
    } When {
      (cells: TypedPipe[Cell[Position3D]], that: TypedPipe[Cell[Position3D]]) =>
        cells.pairwiseBetweenWithValue(Over(Second), Lower, that, List(
          TestMatrixPairwise.PlusX[Position1D, Position2D](), TestMatrixPairwise.MinusX[Position1D, Position2D]()),
          ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result41
    }
  }

  it should "return its second along pairwise in 3D" in {
    Given {
      num3
    } And {
      dataT
    } When {
      (cells: TypedPipe[Cell[Position3D]], that: TypedPipe[Cell[Position3D]]) =>
        cells.pairwiseBetweenWithValue(Along(Second), Lower, that, TestMatrixPairwise.PlusX[Position2D, Position1D](),
          ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result42
    }
  }

  it should "return its third over pairwise in 3D" in {
    Given {
      num3
    } And {
      dataU
    } When {
      (cells: TypedPipe[Cell[Position3D]], that: TypedPipe[Cell[Position3D]]) =>
        cells.pairwiseBetweenWithValue(Over(Third), Lower, that, List(
          TestMatrixPairwise.PlusX[Position1D, Position2D](), TestMatrixPairwise.MinusX[Position1D, Position2D]()),
          ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result43
    }
  }

  it should "return its third along pairwise in 3D" in {
    Given {
      num3
    } And {
      dataV
    } When {
      (cells: TypedPipe[Cell[Position3D]], that: TypedPipe[Cell[Position3D]]) =>
        cells.pairwiseBetweenWithValue(Along(Third), Lower, that, TestMatrixPairwise.PlusX[Position2D, Position1D](),
          ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result44
    }
  }
}

class TestSparkMatrixPairwise extends TestMatrixPairwise {

  "A Matrix.pairwise" should "return its first over pairwise in 1D" in {
    toRDD(num1)
      .pairwise(Over(First), Lower, Plus(StringLocate[Position1D, Position0D]("(%1$s+%2$s)")))
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first over pairwise in 2D" in {
    toRDD(num2)
      .pairwise(Over(First), Lower, Plus(StringLocate[Position1D, Position1D]("(%1$s+%2$s)")))
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its first along pairwise in 2D" in {
    toRDD(num2)
      .pairwise(Along(First), Lower, List(
        Plus(StringLocate[Position1D, Position1D]("(%1$s+%2$s)")),
        Minus(StringLocate[Position1D, Position1D]("(%1$s-%2$s)"))))
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its second over pairwise in 2D" in {
    toRDD(num2)
      .pairwise(Over(Second), Lower, List(
        Plus(StringLocate[Position1D, Position1D]("(%1$s+%2$s)")),
        Minus(StringLocate[Position1D, Position1D]("(%1$s-%2$s)"))))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its second along pairwise in 2D" in {
    toRDD(num2)
      .pairwise(Along(Second), Lower, Plus(StringLocate[Position1D, Position1D]("(%1$s+%2$s)")))
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its first over pairwise in 3D" in {
    toRDD(num3)
      .pairwise(Over(First), Lower, Plus(StringLocate[Position1D, Position2D]("(%1$s+%2$s)")))
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its first along pairwise in 3D" in {
    toRDD(num3)
      .pairwise(Along(First), Lower, List(
        Plus(StringLocate[Position2D, Position1D]("(%1$s+%2$s)")),
        Minus(StringLocate[Position2D, Position1D]("(%1$s-%2$s)"))))
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its second over pairwise in 3D" in {
    toRDD(num3)
      .pairwise(Over(Second), Lower, List(
        Plus(StringLocate[Position1D, Position2D]("(%1$s+%2$s)")),
        Minus(StringLocate[Position1D, Position2D]("(%1$s-%2$s)"))))
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its second along pairwise in 3D" in {
    toRDD(num3)
      .pairwise(Along(Second), Lower, Plus(StringLocate[Position2D, Position1D]("(%1$s+%2$s)")))
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its third over pairwise in 3D" in {
    toRDD(num3)
      .pairwise(Over(Third), Lower, List(
        Plus(StringLocate[Position1D, Position2D]("(%1$s+%2$s)")),
        Minus(StringLocate[Position1D, Position2D]("(%1$s-%2$s)"))))
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its third along pairwise in 3D" in {
    toRDD(num3)
      .pairwise(Along(Third), Lower, Plus(StringLocate[Position2D, Position1D]("(%1$s+%2$s)")))
      .toList.sortBy(_.position) shouldBe result11
  }

  "A Matrix.pairwiseWithValue" should "return its first over pairwise in 1D" in {
    toRDD(num1)
      .pairwiseWithValue(Over(First), Lower, TestMatrixPairwise.PlusX[Position1D, Position0D](), ext)
      .toList.sortBy(_.position) shouldBe result12
  }

  it should "return its first over pairwise in 2D" in {
    toRDD(num2)
      .pairwiseWithValue(Over(First), Lower, TestMatrixPairwise.PlusX[Position1D, Position1D](), ext)
      .toList.sortBy(_.position) shouldBe result13
  }

  it should "return its first along pairwise in 2D" in {
    toRDD(num2)
      .pairwiseWithValue(Along(First), Lower, List(TestMatrixPairwise.PlusX[Position1D, Position1D](),
        TestMatrixPairwise.MinusX[Position1D, Position1D]()), ext)
      .toList.sortBy(_.position) shouldBe result14
  }

  it should "return its second over pairwise in 2D" in {
    toRDD(num2)
      .pairwiseWithValue(Over(Second), Lower, List(TestMatrixPairwise.PlusX[Position1D, Position1D](),
        TestMatrixPairwise.MinusX[Position1D, Position1D]()), ext)
      .toList.sortBy(_.position) shouldBe result15
  }

  it should "return its second along pairwise in 2D" in {
    toRDD(num2)
      .pairwiseWithValue(Along(Second), Lower, TestMatrixPairwise.PlusX[Position1D, Position1D](), ext)
      .toList.sortBy(_.position) shouldBe result16
  }

  it should "return its first over pairwise in 3D" in {
    toRDD(num3)
      .pairwiseWithValue(Over(First), Lower, TestMatrixPairwise.PlusX[Position1D, Position2D](), ext)
      .toList.sortBy(_.position) shouldBe result17
  }

  it should "return its first along pairwise in 3D" in {
    toRDD(num3)
      .pairwiseWithValue(Along(First), Lower, List(TestMatrixPairwise.PlusX[Position2D, Position1D](),
        TestMatrixPairwise.MinusX[Position2D, Position1D]()), ext)
      .toList.sortBy(_.position) shouldBe result18
  }

  it should "return its second over pairwise in 3D" in {
    toRDD(num3)
      .pairwiseWithValue(Over(Second), Lower, List(TestMatrixPairwise.PlusX[Position1D, Position2D](),
        TestMatrixPairwise.MinusX[Position1D, Position2D]()), ext)
      .toList.sortBy(_.position) shouldBe result19
  }

  it should "return its second along pairwise in 3D" in {
    toRDD(num3)
      .pairwiseWithValue(Along(Second), Lower, TestMatrixPairwise.PlusX[Position2D, Position1D](), ext)
      .toList.sortBy(_.position) shouldBe result20
  }

  it should "return its third over pairwise in 3D" in {
    toRDD(num3)
      .pairwiseWithValue(Over(Third), Lower, List(TestMatrixPairwise.PlusX[Position1D, Position2D](),
        TestMatrixPairwise.MinusX[Position1D, Position2D]()), ext)
      .toList.sortBy(_.position) shouldBe result21
  }

  it should "return its third along pairwise in 3D" in {
    toRDD(num3)
      .pairwiseWithValue(Along(Third), Lower, TestMatrixPairwise.PlusX[Position2D, Position1D](), ext)
      .toList.sortBy(_.position) shouldBe result22
  }

  "A Matrix.pairwiseBetween" should "return its first over pairwise in 1D" in {
    toRDD(num1)
      .pairwiseBetween(Over(First), Lower, toRDD(dataA), Plus(StringLocate[Position1D, Position0D]("(%1$s+%2$s)")))
      .toList.sortBy(_.position) shouldBe result23
  }

  it should "return its first over pairwise in 2D" in {
    toRDD(num2)
      .pairwiseBetween(Over(First), Lower, toRDD(dataB), Plus(StringLocate[Position1D, Position1D]("(%1$s+%2$s)")))
      .toList.sortBy(_.position) shouldBe result24
  }

  it should "return its first along pairwise in 2D" in {
    toRDD(num2)
      .pairwiseBetween(Along(First), Lower, toRDD(dataC), List(
        Plus(StringLocate[Position1D, Position1D]("(%1$s+%2$s)")),
        Minus(StringLocate[Position1D, Position1D]("(%1$s-%2$s)"))))
      .toList.sortBy(_.position) shouldBe result25
  }

  it should "return its second over pairwise in 2D" in {
    toRDD(num2)
      .pairwiseBetween(Over(Second), Lower, toRDD(dataD), List(
        Plus(StringLocate[Position1D, Position1D]("(%1$s+%2$s)")),
        Minus(StringLocate[Position1D, Position1D]("(%1$s-%2$s)"))))
      .toList.sortBy(_.position) shouldBe result26
  }

  it should "return its second along pairwise in 2D" in {
    toRDD(num2)
      .pairwiseBetween(Along(Second), Lower, toRDD(dataE), Plus(StringLocate[Position1D, Position1D]("(%1$s+%2$s)")))
      .toList.sortBy(_.position) shouldBe result27
  }

  it should "return its first over pairwise in 3D" in {
    toRDD(num3)
      .pairwiseBetween(Over(First), Lower, toRDD(dataF), Plus(StringLocate[Position1D, Position2D]("(%1$s+%2$s)")))
      .toList.sortBy(_.position) shouldBe result28
  }

  it should "return its first along pairwise in 3D" in {
    toRDD(num3)
      .pairwiseBetween(Along(First), Lower, toRDD(dataG), List(
        Plus(StringLocate[Position2D, Position1D]("(%1$s+%2$s)")),
        Minus(StringLocate[Position2D, Position1D]("(%1$s-%2$s)"))))
      .toList.sortBy(_.position) shouldBe result29
  }

  it should "return its second over pairwise in 3D" in {
    toRDD(num3)
      .pairwiseBetween(Over(Second), Lower, toRDD(dataH), List(
        Plus(StringLocate[Position1D, Position2D]("(%1$s+%2$s)")),
        Minus(StringLocate[Position1D, Position2D]("(%1$s-%2$s)"))))
      .toList.sortBy(_.position) shouldBe result30
  }

  it should "return its second along pairwise in 3D" in {
    toRDD(num3)
      .pairwiseBetween(Along(Second), Lower, toRDD(dataI), Plus(StringLocate[Position2D, Position1D]("(%1$s+%2$s)")))
      .toList.sortBy(_.position) shouldBe result31
  }

  it should "return its third over pairwise in 3D" in {
    toRDD(num3)
      .pairwiseBetween(Over(Third), Lower, toRDD(dataJ), List(
        Plus(StringLocate[Position1D, Position2D]("(%1$s+%2$s)")),
        Minus(StringLocate[Position1D, Position2D]("(%1$s-%2$s)"))))
      .toList.sortBy(_.position) shouldBe result32
  }

  it should "return its third along pairwise in 3D" in {
    toRDD(num3)
      .pairwiseBetween(Along(Third), Lower, toRDD(dataK), Plus(StringLocate[Position2D, Position1D]("(%1$s+%2$s)")))
      .toList.sortBy(_.position) shouldBe result33
  }

  "A Matrix.pairwiseBetweenWithValue" should "return its first over pairwise in 1D" in {
    toRDD(num1)
      .pairwiseBetweenWithValue(Over(First), Lower, toRDD(dataL), TestMatrixPairwise.PlusX[Position1D, Position0D](),
        ext)
      .toList.sortBy(_.position) shouldBe result34
  }

  it should "return its first over pairwise in 2D" in {
    toRDD(num2)
      .pairwiseBetweenWithValue(Over(First), Lower, toRDD(dataM), TestMatrixPairwise.PlusX[Position1D, Position1D](),
        ext)
      .toList.sortBy(_.position) shouldBe result35
  }

  it should "return its first along pairwise in 2D" in {
    toRDD(num2)
      .pairwiseBetweenWithValue(Along(First), Lower, toRDD(dataN), List(
        TestMatrixPairwise.PlusX[Position1D, Position1D](), TestMatrixPairwise.MinusX[Position1D, Position1D]()), ext)
      .toList.sortBy(_.position) shouldBe result36
  }

  it should "return its second over pairwise in 2D" in {
    toRDD(num2)
      .pairwiseBetweenWithValue(Over(Second), Lower, toRDD(dataO), List(
        TestMatrixPairwise.PlusX[Position1D, Position1D](), TestMatrixPairwise.MinusX[Position1D, Position1D]()), ext)
      .toList.sortBy(_.position) shouldBe result37
  }

  it should "return its second along pairwise in 2D" in {
    toRDD(num2)
      .pairwiseBetweenWithValue(Along(Second), Lower, toRDD(dataP), TestMatrixPairwise.PlusX[Position1D, Position1D](),
        ext)
      .toList.sortBy(_.position) shouldBe result38
  }

  it should "return its first over pairwise in 3D" in {
    toRDD(num3)
      .pairwiseBetweenWithValue(Over(First), Lower, toRDD(dataQ), TestMatrixPairwise.PlusX[Position1D, Position2D](),
        ext)
      .toList.sortBy(_.position) shouldBe result39
  }

  it should "return its first along pairwise in 3D" in {
    toRDD(num3)
      .pairwiseBetweenWithValue(Along(First), Lower, toRDD(dataR), List(
        TestMatrixPairwise.PlusX[Position2D, Position1D](), TestMatrixPairwise.MinusX[Position2D, Position1D]()), ext)
      .toList.sortBy(_.position) shouldBe result40
  }

  it should "return its second over pairwise in 3D" in {
    toRDD(num3)
      .pairwiseBetweenWithValue(Over(Second), Lower, toRDD(dataS), List(
        TestMatrixPairwise.PlusX[Position1D, Position2D](), TestMatrixPairwise.MinusX[Position1D, Position2D]()), ext)
      .toList.sortBy(_.position) shouldBe result41
  }

  it should "return its second along pairwise in 3D" in {
    toRDD(num3)
      .pairwiseBetweenWithValue(Along(Second), Lower, toRDD(dataT), TestMatrixPairwise.PlusX[Position2D, Position1D](),
        ext)
      .toList.sortBy(_.position) shouldBe result42
  }

  it should "return its third over pairwise in 3D" in {
    toRDD(num3)
      .pairwiseBetweenWithValue(Over(Third), Lower, toRDD(dataU), List(
        TestMatrixPairwise.PlusX[Position1D, Position2D](), TestMatrixPairwise.MinusX[Position1D, Position2D]()), ext)
      .toList.sortBy(_.position) shouldBe result43
  }

  it should "return its third along pairwise in 3D" in {
    toRDD(num3)
      .pairwiseBetweenWithValue(Along(Third), Lower, toRDD(dataV), TestMatrixPairwise.PlusX[Position2D, Position1D](),
        ext)
      .toList.sortBy(_.position) shouldBe result44
  }
}

trait TestMatrixChange extends TestMatrix {

  val result1 = List(Cell(Position1D("bar"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
    Cell(Position1D("baz"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position1D("foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
    Cell(Position1D("qux"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))

  val result2 = List(Cell(Position2D("bar", 1), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
    Cell(Position2D("bar", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position2D("bar", 3), Content(OrdinalSchema[Codex.LongCodex](), 19)),
    Cell(Position2D("baz", 1), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position2D("baz", 2), Content(DiscreteSchema[Codex.LongCodex](), 19)),
    Cell(Position2D("foo", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
    Cell(Position2D("foo", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position2D("foo", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
    Cell(Position2D("qux", 1), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))

  val result3 = List(Cell(Position2D("bar", 1), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
    Cell(Position2D("bar", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position2D("bar", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 19)),
    Cell(Position2D("baz", 1), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position2D("baz", 2), Content(DiscreteSchema[Codex.LongCodex](), 19)),
    Cell(Position2D("foo", 1), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
    Cell(Position2D("foo", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position2D("foo", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
    Cell(Position2D("qux", 1), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))

  val result4 = List(Cell(Position2D("bar", 1), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
    Cell(Position2D("bar", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position2D("bar", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 19)),
    Cell(Position2D("baz", 1), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position2D("baz", 2), Content(DiscreteSchema[Codex.LongCodex](), 19)),
    Cell(Position2D("foo", 1), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
    Cell(Position2D("foo", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position2D("foo", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
    Cell(Position2D("qux", 1), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))

  val result5 = List(Cell(Position2D("bar", 1), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
    Cell(Position2D("bar", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position2D("bar", 3), Content(OrdinalSchema[Codex.LongCodex](), 19)),
    Cell(Position2D("baz", 1), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position2D("baz", 2), Content(DiscreteSchema[Codex.LongCodex](), 19)),
    Cell(Position2D("foo", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
    Cell(Position2D("foo", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position2D("foo", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
    Cell(Position2D("qux", 1), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))

  val result6 = List(Cell(Position3D("bar", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
    Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position3D("bar", 3, "xyz"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
    Cell(Position3D("baz", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position3D("baz", 2, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
    Cell(Position3D("foo", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
    Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position3D("foo", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
    Cell(Position3D("qux", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))

  val result7 = List(Cell(Position3D("bar", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
    Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position3D("bar", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 19)),
    Cell(Position3D("baz", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position3D("baz", 2, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
    Cell(Position3D("foo", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
    Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position3D("foo", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
    Cell(Position3D("qux", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))

  val result8 = List(Cell(Position3D("bar", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
    Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position3D("bar", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 19)),
    Cell(Position3D("baz", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position3D("baz", 2, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
    Cell(Position3D("foo", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
    Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position3D("foo", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
    Cell(Position3D("qux", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))

  val result9 = List(Cell(Position3D("bar", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
    Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position3D("bar", 3, "xyz"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
    Cell(Position3D("baz", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position3D("baz", 2, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
    Cell(Position3D("foo", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
    Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position3D("foo", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
    Cell(Position3D("qux", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))

  val result10 = List(Cell(Position3D("bar", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position3D("bar", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 19)),
    Cell(Position3D("baz", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
    Cell(Position3D("baz", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 19)),
    Cell(Position3D("foo", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
    Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position3D("foo", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
    Cell(Position3D("qux", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)))

  val result11 = List(Cell(Position3D("bar", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
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

class TestScaldingMatrixChange extends TestMatrixChange with TBddDsl {

  "A Matrix.change" should "return its first over data in 1D" in {
    Given {
      data1
    } When {
      cells: TypedPipe[Cell[Position1D]] =>
        cells.change(Over(First), "foo", ContinuousSchema[Codex.DoubleCodex]())
    } Then {
      _.toList.sortBy(_.position) shouldBe result1
    }
  }

  it should "return its first over data in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.change(Over(First), "foo", ContinuousSchema[Codex.DoubleCodex]())
    } Then {
      _.toList.sortBy(_.position) shouldBe result2
    }
  }

  it should "return its first along data in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.change(Along(First), List(3, 4), ContinuousSchema[Codex.DoubleCodex]())
    } Then {
      _.toList.sortBy(_.position) shouldBe result3
    }
  }

  it should "return its second over data in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.change(Over(Second), List(3, 4), ContinuousSchema[Codex.DoubleCodex]())
    } Then {
      _.toList.sortBy(_.position) shouldBe result4
    }
  }

  it should "return its second along data in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.change(Along(Second), "foo", ContinuousSchema[Codex.DoubleCodex]())
    } Then {
      _.toList.sortBy(_.position) shouldBe result5
    }
  }

  it should "return its first over data in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.change(Over(First), "foo", ContinuousSchema[Codex.DoubleCodex]())
    } Then {
      _.toList.sortBy(_.position) shouldBe result6
    }
  }

  it should "return its first along data in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.change(Along(First), List(Position2D(3, "xyz"), Position2D(4, "xyz")),
          ContinuousSchema[Codex.DoubleCodex]())
    } Then {
      _.toList.sortBy(_.position) shouldBe result7
    }
  }

  it should "return its second over data in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.change(Over(Second), List(3, 4), ContinuousSchema[Codex.DoubleCodex]())
    } Then {
      _.toList.sortBy(_.position) shouldBe result8
    }
  }

  it should "return its second along data in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.change(Along(Second), Position2D("foo", "xyz"), ContinuousSchema[Codex.DoubleCodex]())
    } Then {
      _.toList.sortBy(_.position) shouldBe result9
    }
  }

  it should "return its third over data in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.change(Over(Third), List("xyz"), ContinuousSchema[Codex.DoubleCodex]())
    } Then {
      _.toList.sortBy(_.position) shouldBe result10
    }
  }

  it should "return its third along data in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.change(Along(Third), Position2D("foo", 1), ContinuousSchema[Codex.DoubleCodex]())
    } Then {
      _.toList.sortBy(_.position) shouldBe result11
    }
  }
}

class TestSparkMatrixChange extends TestMatrixChange {

  "A Matrix.change" should "return its first over data in 1D" in {
    toRDD(data1)
      .change(Over(First), "foo", ContinuousSchema[Codex.DoubleCodex]())
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first over data in 2D" in {
    toRDD(data2)
      .change(Over(First), "foo", ContinuousSchema[Codex.DoubleCodex]())
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its first along data in 2D" in {
    toRDD(data2)
      .change(Along(First), List(3, 4), ContinuousSchema[Codex.DoubleCodex]())
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its second over data in 2D" in {
    toRDD(data2)
      .change(Over(Second), List(3, 4), ContinuousSchema[Codex.DoubleCodex]())
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its second along data in 2D" in {
    toRDD(data2)
      .change(Along(Second), "foo", ContinuousSchema[Codex.DoubleCodex]())
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its first over data in 3D" in {
    toRDD(data3)
      .change(Over(First), "foo", ContinuousSchema[Codex.DoubleCodex]())
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its first along data in 3D" in {
    toRDD(data3)
      .change(Along(First), List(Position2D(3, "xyz"), Position2D(4, "xyz")), ContinuousSchema[Codex.DoubleCodex]())
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its second over data in 3D" in {
    toRDD(data3)
      .change(Over(Second), List(3, 4), ContinuousSchema[Codex.DoubleCodex]())
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its second along data in 3D" in {
    toRDD(data3)
      .change(Along(Second), Position2D("foo", "xyz"), ContinuousSchema[Codex.DoubleCodex]())
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its third over data in 3D" in {
    toRDD(data3)
      .change(Over(Third), List("xyz"), ContinuousSchema[Codex.DoubleCodex]())
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its third along data in 3D" in {
    toRDD(data3)
      .change(Along(Third), Position2D("foo", 1), ContinuousSchema[Codex.DoubleCodex]())
      .toList.sortBy(_.position) shouldBe result11
  }
}

trait TestMatrixSet extends TestMatrix {

  val dataA = List(Cell(Position1D("foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 1)),
    Cell(Position1D("quxx"), Content(ContinuousSchema[Codex.DoubleCodex](), 2)))

  val dataB = List(Cell(Position2D("foo", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 1)),
    Cell(Position2D("quxx", 5), Content(ContinuousSchema[Codex.DoubleCodex](), 2)))

  val dataC = List(Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 1)),
    Cell(Position3D("quxx", 5, "abc"), Content(ContinuousSchema[Codex.DoubleCodex](), 2)))

  val result1 = List(Cell(Position1D("bar"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
    Cell(Position1D("baz"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position1D("foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 1)),
    Cell(Position1D("qux"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))

  val result2 = List(Cell(Position1D("bar"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
    Cell(Position1D("baz"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position1D("foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 1)),
    Cell(Position1D("qux"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")),
    Cell(Position1D("quxx"), Content(ContinuousSchema[Codex.DoubleCodex](), 1)))

  val result3 = List(Cell(Position1D("bar"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
    Cell(Position1D("baz"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position1D("foo"), Content(ContinuousSchema[Codex.DoubleCodex](), 1)),
    Cell(Position1D("qux"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")),
    Cell(Position1D("quxx"), Content(ContinuousSchema[Codex.DoubleCodex](), 2)))

  val result4 = List(Cell(Position2D("bar", 1), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
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

  val result5 = List(Cell(Position2D("bar", 1), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
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

  val result6 = List(Cell(Position2D("bar", 1), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
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

  val result7 = List(Cell(Position3D("bar", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
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

  val result8 = List(Cell(Position3D("bar", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
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

  val result9 = List(Cell(Position3D("bar", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
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

class TestScaldingMatrixSet extends TestMatrixSet with TBddDsl {

  "A Matrix.set" should "return its updated data in 1D" in {
    Given {
      data1
    } When {
      cells: TypedPipe[Cell[Position1D]] =>
        cells.set("foo", Content(ContinuousSchema[Codex.DoubleCodex](), 1))
    } Then {
      _.toList.sortBy(_.position) shouldBe result1
    }
  }

  it should "return its updated and added data in 1D" in {
    Given {
      data1
    } When {
      cells: TypedPipe[Cell[Position1D]] =>
        cells.set(List("foo", "quxx"), Content(ContinuousSchema[Codex.DoubleCodex](), 1))
    } Then {
      _.toList.sortBy(_.position) shouldBe result2
    }
  }

  it should "return its matrix updated data in 1D" in {
    Given {
      data1
    } And {
      dataA
    } When {
      (cells: TypedPipe[Cell[Position1D]], that: TypedPipe[Cell[Position1D]]) =>
        cells.set(that)
    } Then {
      _.toList.sortBy(_.position) shouldBe result3
    }
  }

  it should "return its updated data in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.set(Position2D("foo", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 1))
    } Then {
      _.toList.sortBy(_.position) shouldBe result4
    }
  }

  it should "return its updated and added data in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.set(List(Position2D("foo", 2), Position2D("quxx", 5)),
          Content(ContinuousSchema[Codex.DoubleCodex](), 1))
    } Then {
      _.toList.sortBy(_.position) shouldBe result5
    }
  }

  it should "return its matrix updated data in 2D" in {
    Given {
      data2
    } And {
      dataB
    } When {
      (cells: TypedPipe[Cell[Position2D]], that: TypedPipe[Cell[Position2D]]) =>
        cells.set(that)
    } Then {
      _.toList.sortBy(_.position) shouldBe result6
    }
  }

  it should "return its updated data in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.set(Position3D("foo", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 1))
    } Then {
      _.toList.sortBy(_.position) shouldBe result7
    }
  }

  it should "return its updated and added data in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.set(List(Position3D("foo", 2, "xyz"), Position3D("quxx", 5, "abc")),
          Content(ContinuousSchema[Codex.DoubleCodex](), 1))
    } Then {
      _.toList.sortBy(_.position) shouldBe result8
    }
  }

  it should "return its matrix updated data in 3D" in {
    Given {
      data3
    } And {
      dataC
    } When {
      (cells: TypedPipe[Cell[Position3D]], that: TypedPipe[Cell[Position3D]]) =>
        cells.set(that)
    } Then {
      _.toList.sortBy(_.position) shouldBe result9
    }
  }
}

class TestSparkMatrixSet extends TestMatrixSet {

  "A Matrix.set" should "return its updated data in 1D" in {
    toRDD(data1)
      .set("foo", Content(ContinuousSchema[Codex.DoubleCodex](), 1))
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its updated and added data in 1D" in {
    toRDD(data1)
      .set(List("foo", "quxx"), Content(ContinuousSchema[Codex.DoubleCodex](), 1))
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its matrix updated data in 1D" in {
    toRDD(data1)
      .set(toRDD(dataA))
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its updated data in 2D" in {
    toRDD(data2)
      .set(Position2D("foo", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 1))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its updated and added data in 2D" in {
    toRDD(data2)
      .set(List(Position2D("foo", 2), Position2D("quxx", 5)), Content(ContinuousSchema[Codex.DoubleCodex](), 1))
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its matrix updated data in 2D" in {
    toRDD(data2)
      .set(toRDD(dataB))
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its updated data in 3D" in {
    toRDD(data3)
      .set(Position3D("foo", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 1))
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its updated and added data in 3D" in {
    toRDD(data3)
      .set(List(Position3D("foo", 2, "xyz"), Position3D("quxx", 5, "abc")),
        Content(ContinuousSchema[Codex.DoubleCodex](), 1))
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its matrix updated data in 3D" in {
    toRDD(data3)
      .set(toRDD(dataC))
      .toList.sortBy(_.position) shouldBe result9
  }
}

trait TestMatrixTransform extends TestMatrix {

  val ext = Map(
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
      Position1D("sd") -> Content(ContinuousSchema[Codex.DoubleCodex](), 4)))

  val result1 = List(Cell(Position1D("bar.ind"), Content(DiscreteSchema[Codex.LongCodex](), 1)),
    Cell(Position1D("baz.ind"), Content(DiscreteSchema[Codex.LongCodex](), 1)),
    Cell(Position1D("foo.ind"), Content(DiscreteSchema[Codex.LongCodex](), 1)),
    Cell(Position1D("qux.ind"), Content(DiscreteSchema[Codex.LongCodex](), 1)))

  val result2 = List(Cell(Position2D("bar.ind", 1), Content(DiscreteSchema[Codex.LongCodex](), 1)),
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

  val result3 = List(Cell(Position3D("bar.ind", 1, "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 1)),
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

  val result4 = List(Cell(Position1D("bar.n"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 / 6.28)),
    Cell(Position1D("bar.s"), Content(ContinuousSchema[Codex.DoubleCodex](), (6.28 - 3.14) / 2)),
    Cell(Position1D("baz.n"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 / 9.42)),
    Cell(Position1D("baz.s"), Content(ContinuousSchema[Codex.DoubleCodex](), (9.42 - 3.14) / 3)),
    Cell(Position1D("foo.n"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 / 3.14)),
    Cell(Position1D("foo.s"), Content(ContinuousSchema[Codex.DoubleCodex](), (3.14 - 3.14) / 1)),
    Cell(Position1D("qux.n"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 / 12.56)),
    Cell(Position1D("qux.s"), Content(ContinuousSchema[Codex.DoubleCodex](), (12.56 - 3.14) / 4)))

  val result5 = List(Cell(Position2D("bar.n", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 / 6.28)),
    Cell(Position2D("bar.n", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 / 6.28)),
    Cell(Position2D("bar.n", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 / 6.28)),
    Cell(Position2D("baz.n", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 / 9.42)),
    Cell(Position2D("baz.n", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 / 9.42)),
    Cell(Position2D("foo.n", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 / 3.14)),
    Cell(Position2D("foo.n", 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 / 3.14)),
    Cell(Position2D("foo.n", 3), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 / 3.14)),
    Cell(Position2D("foo.n", 4), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 / 3.14)),
    Cell(Position2D("qux.n", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 / 12.56)))

  val result6 = List(Cell(Position3D("bar.n", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 / 6.28)),
    Cell(Position3D("bar.n", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 / 6.28)),
    Cell(Position3D("bar.n", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 / 6.28)),
    Cell(Position3D("baz.n", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 / 9.42)),
    Cell(Position3D("baz.n", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 / 9.42)),
    Cell(Position3D("foo.n", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 / 3.14)),
    Cell(Position3D("foo.n", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 / 3.14)),
    Cell(Position3D("foo.n", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 / 3.14)),
    Cell(Position3D("foo.n", 4, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 / 3.14)),
    Cell(Position3D("qux.n", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 / 12.56)))

  val result7 = List(Cell(Position2D("bar.ind", "ind"), Content(DiscreteSchema[Codex.LongCodex](), 1)),
    Cell(Position2D("baz.ind", "ind"), Content(DiscreteSchema[Codex.LongCodex](), 1)),
    Cell(Position2D("foo.ind", "ind"), Content(DiscreteSchema[Codex.LongCodex](), 1)),
    Cell(Position2D("qux.ind", "ind"), Content(DiscreteSchema[Codex.LongCodex](), 1)))

  val result8 = List(Cell(Position3D("bar.ind", 1, "ind"), Content(DiscreteSchema[Codex.LongCodex](), 1)),
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

  val result9 = List(Cell(Position4D("bar.ind", 1, "xyz", "ind"), Content(DiscreteSchema[Codex.LongCodex](), 1)),
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

  val result10 = List(Cell(Position2D("bar.n", "nrm"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 / 6.28)),
    Cell(Position2D("bar.s", "std"), Content(ContinuousSchema[Codex.DoubleCodex](), (6.28 - 3.14) / 2)),
    Cell(Position2D("baz.n", "nrm"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 / 9.42)),
    Cell(Position2D("baz.s", "std"), Content(ContinuousSchema[Codex.DoubleCodex](), (9.42 - 3.14) / 3)),
    Cell(Position2D("foo.n", "nrm"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 / 3.14)),
    Cell(Position2D("foo.s", "std"), Content(ContinuousSchema[Codex.DoubleCodex](), (3.14 - 3.14) / 1)),
    Cell(Position2D("qux.n", "nrm"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 / 12.56)),
    Cell(Position2D("qux.s", "std"), Content(ContinuousSchema[Codex.DoubleCodex](), (12.56 - 3.14) / 4)))

  val result11 = List(Cell(Position3D("bar.n", 1, "nrm"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 / 6.28)),
    Cell(Position3D("bar.n", 2, "nrm"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 / 6.28)),
    Cell(Position3D("bar.n", 3, "nrm"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 / 6.28)),
    Cell(Position3D("baz.n", 1, "nrm"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 / 9.42)),
    Cell(Position3D("baz.n", 2, "nrm"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 / 9.42)),
    Cell(Position3D("foo.n", 1, "nrm"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 / 3.14)),
    Cell(Position3D("foo.n", 2, "nrm"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 / 3.14)),
    Cell(Position3D("foo.n", 3, "nrm"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 / 3.14)),
    Cell(Position3D("foo.n", 4, "nrm"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 / 3.14)),
    Cell(Position3D("qux.n", 1, "nrm"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 / 12.56)))

  val result12 = List(
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

  type W = Map[Position1D, Map[Position1D, Content]]

  def extractor[D <: Dimension, P <: Position](dim: D, key: String)(implicit ev: PosDimDep[P, D]) = {
    ExtractWithDimensionAndKey[D, P, String, Content](dim, key).andThenPresent(_.value.asDouble)
  }
}

class TestScaldingMatrixTransform extends TestMatrixTransform with TBddDsl {

  "A Matrix.transform" should "return its transformed data in 1D" in {
    Given {
      data1
    } When {
      cells: TypedPipe[Cell[Position1D]] =>
        cells.transform[Position1D, Transformer[Position1D, Position1D]](
          Indicator().andThenRename(Transformer.rename(First, "%1$s.ind")))
    } Then {
      _.toList.sortBy(_.position) shouldBe result1
    }
  }

  it should "return its transformed data in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.transform[Position2D, List[Transformer[Position2D, Position2D]]](List(
          Indicator().andThenRename(Transformer.rename(First, "%1$s.ind")),
          Binarise(Binarise.rename(First))))
    } Then {
      _.toList.sortBy(_.position) shouldBe result2
    }
  }

  it should "return its transformed data in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.transform[Position3D, List[Transformer[Position3D, Position3D]]](List(
          Indicator().andThenRename(Transformer.rename(First, "%1$s.ind")),
          Binarise(Binarise.rename(First))))
    } Then {
      _.toList.sortBy(_.position) shouldBe result3
    }
  }

  "A Matrix.transformWithValue" should "return its transformed data in 1D" in {
    Given {
      num1
    } When {
      cells: TypedPipe[Cell[Position1D]] =>
        cells.transformWithValue[Position1D, List[TransformerWithValue[Position1D,Position1D] { type V >: W }], W](List(
          Normalise(extractor[Dimension.First, Position1D](First, "max.abs"))
            .andThenRenameWithValue(TransformerWithValue.rename(First, "%1$s.n")),
          Standardise(extractor[Dimension.First, Position1D](First, "mean"),
            extractor[Dimension.First, Position1D](First, "sd"))
            .andThenRenameWithValue(TransformerWithValue.rename(First, "%1$s.s"))),
          ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result4
    }
  }

  it should "return its transformed data in 2D" in {
    Given {
      num2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.transformWithValue[Position2D, TransformerWithValue[Position2D, Position2D] { type V >: W }, W](
          Normalise(extractor[Dimension.First, Position2D](First, "max.abs"))
            .andThenRenameWithValue(TransformerWithValue.rename(First, "%1$s.n")),
          ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result5
    }
  }

  it should "return its transformed data in 3D" in {
    Given {
      num3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.transformWithValue[Position3D, TransformerWithValue[Position3D, Position3D] { type V >: W }, W](
          Normalise(extractor[Dimension.First, Position3D](First, "max.abs"))
            .andThenRenameWithValue(TransformerWithValue.rename(First, "%1$s.n")),
          ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result6
    }
  }

  "A Matrix.transformAndExpand" should "return its transformed data in 1D" in {
    Given {
      data1
    } When {
      cells: TypedPipe[Cell[Position1D]] =>
        cells.transform[Position2D, Transformer[Position1D, Position2D]](Indicator()
          .andThenRename(Transformer.rename(First, "%1$s.ind"))
          .andThenExpand(Transformer.expand[Position1D, Position1D, String]("ind")))
    } Then {
      _.toList.sortBy(_.position) shouldBe result7
    }
  }

  it should "return its transformed data in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.transform[Position3D, List[Transformer[Position2D, Position3D]]](List(
          Indicator()
            .andThenRename(Transformer.rename(First, "%1$s.ind"))
            .andThenExpand(Transformer.expand[Position2D, Position2D, String]("ind")),
          Binarise(Binarise.rename(First))
            .andThenExpand(Transformer.expand[Position2D, Position2D, String]("bin"))))
    } Then {
      _.toList.sortBy(_.position) shouldBe result8
    }
  }

  it should "return its transformed data in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.transform[Position4D, List[Transformer[Position3D, Position4D]]](List(
          Indicator()
            .andThenRename(Transformer.rename(First, "%1$s.ind"))
            .andThenExpand(Transformer.expand[Position3D, Position3D, String]("ind")),
          Binarise(Binarise.rename(First))
            .andThenExpand(Transformer.expand[Position3D, Position3D, String]("bin"))))
    } Then {
      _.toList.sortBy(_.position) shouldBe result9
    }
  }

  "A Matrix.transformAndExpandWithValue" should "return its transformed data in 1D" in {
    Given {
      num1
    } When {
      cells: TypedPipe[Cell[Position1D]] =>
        cells.transformWithValue[Position2D, List[TransformerWithValue[Position1D,Position2D] { type V >: W }], W](List(
          Normalise(extractor[Dimension.First, Position1D](First, "max.abs"))
            .andThenRenameWithValue(TransformerWithValue.rename(First, "%1$s.n"))
            .andThenExpandWithValue(TransformerWithValue.expand[Position1D, Position1D, W, String]("nrm")),
          Standardise(extractor[Dimension.First, Position1D](First, "mean"),
            extractor[Dimension.First, Position1D](First, "sd"))
            .andThenRenameWithValue(TransformerWithValue.rename(First, "%1$s.s"))
            .andThenExpandWithValue(TransformerWithValue.expand[Position1D, Position1D, W, String]("std"))),
          ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result10
    }
  }

  it should "return its transformed data in 2D" in {
    Given {
      num2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.transformWithValue[Position3D, TransformerWithValue[Position2D, Position3D] { type V >: W }, W](
          Normalise(extractor[Dimension.First, Position2D](First, "max.abs"))
            .andThenRenameWithValue(TransformerWithValue.rename(First, "%1$s.n"))
            .andThenExpandWithValue(TransformerWithValue.expand[Position2D, Position2D, W, String]("nrm")),
          ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result11
    }
  }

  it should "return its transformed data in 3D" in {
    Given {
      num3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.transformWithValue[Position4D, TransformerWithValue[Position3D, Position4D] { type V >: W }, W](
          Normalise(extractor[Dimension.First, Position3D](First, "max.abs"))
            .andThenRenameWithValue(TransformerWithValue.rename(First, "%1$s.n"))
            .andThenExpandWithValue(TransformerWithValue.expand[Position3D, Position3D, W, String]("nrm")),
          ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result12
    }
  }
}

class TestSparkMatrixTransform extends TestMatrixTransform {

  "A Matrix.transform" should "return its transformed data in 1D" in {
    toRDD(data1)
      .transform[Position1D, Transformer[Position1D, Position1D]](
        Indicator().andThenRename(Transformer.rename(First, "%1$s.ind")))
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its transformed data in 2D" in {
    toRDD(data2)
      .transform[Position2D, List[Transformer[Position2D, Position2D]]](List(
        Indicator().andThenRename(Transformer.rename(First, "%1$s.ind")),
        Binarise(Binarise.rename(First))))
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its transformed data in 3D" in {
    toRDD(data3)
      .transform[Position3D, List[Transformer[Position3D, Position3D]]](List(
        Indicator().andThenRename(Transformer.rename(First, "%1$s.ind")),
        Binarise(Binarise.rename(First))))
      .toList.sortBy(_.position) shouldBe result3
  }

  "A Matrix.transformWithValue" should "return its transformed data in 1D" in {
    toRDD(num1)
      .transformWithValue[Position1D, List[TransformerWithValue[Position1D, Position1D] { type V >: W }], W](List(
        Normalise(extractor[Dimension.First, Position1D](First, "max.abs"))
          .andThenRenameWithValue(TransformerWithValue.rename(First, "%1$s.n")),
        Standardise(extractor[Dimension.First, Position1D](First, "mean"),
          extractor[Dimension.First, Position1D](First, "sd"))
          .andThenRenameWithValue(TransformerWithValue.rename(First, "%1$s.s"))), ext)
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its transformed data in 2D" in {
    toRDD(num2)
      .transformWithValue[Position2D, TransformerWithValue[Position2D, Position2D] { type V >: W }, W](
        Normalise(extractor[Dimension.First, Position2D](First, "max.abs"))
          .andThenRenameWithValue(TransformerWithValue.rename(First, "%1$s.n")), ext)
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its transformed data in 3D" in {
    toRDD(num3)
      .transformWithValue[Position3D, TransformerWithValue[Position3D, Position3D] { type V >: W }, W](
        Normalise(extractor[Dimension.First, Position3D](First, "max.abs"))
          .andThenRenameWithValue(TransformerWithValue.rename(First, "%1$s.n")), ext)
      .toList.sortBy(_.position) shouldBe result6
  }

  "A Matrix.transformAndExpand" should "return its transformed data in 1D" in {
    toRDD(data1)
      .transform[Position2D, Transformer[Position1D, Position2D]](Indicator()
        .andThenRename(Transformer.rename(First, "%1$s.ind"))
        .andThenExpand(Transformer.expand[Position1D, Position1D, String]("ind")))
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its transformed data in 2D" in {
    toRDD(data2)
      .transform[Position3D, List[Transformer[Position2D, Position3D]]](List(
        Indicator()
          .andThenRename(Transformer.rename(First, "%1$s.ind"))
          .andThenExpand(Transformer.expand[Position2D, Position2D, String]("ind")),
        Binarise(Binarise.rename(First))
          .andThenExpand(Transformer.expand[Position2D, Position2D, String]("bin"))))
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its transformed data in 3D" in {
    toRDD(data3)
      .transform[Position4D, List[Transformer[Position3D, Position4D]]](List(
        Indicator()
          .andThenRename(Transformer.rename(First, "%1$s.ind"))
          .andThenExpand(Transformer.expand[Position3D, Position3D, String]("ind")),
        Binarise(Binarise.rename(First))
          .andThenExpand(Transformer.expand[Position3D, Position3D, String]("bin"))))
      .toList.sortBy(_.position) shouldBe result9
  }

  "A Matrix.transformAndExpandWithValue" should "return its transformed data in 1D" in {
    toRDD(num1)
      .transformWithValue[Position2D, List[TransformerWithValue[Position1D, Position2D] { type V >: W }], W](List(
        Normalise(extractor[Dimension.First, Position1D](First, "max.abs"))
          .andThenRenameWithValue(TransformerWithValue.rename(First, "%1$s.n"))
          .andThenExpandWithValue(TransformerWithValue.expand[Position1D, Position1D, W, String]("nrm")),
        Standardise(extractor[Dimension.First, Position1D](First, "mean"),
          extractor[Dimension.First, Position1D](First, "sd"))
          .andThenRenameWithValue(TransformerWithValue.rename(First, "%1$s.s"))
          .andThenExpandWithValue(TransformerWithValue.expand[Position1D, Position1D, W, String]("std"))), ext)
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its transformed data in 2D" in {
    toRDD(num2)
      .transformWithValue[Position3D, TransformerWithValue[Position2D, Position3D] { type V >: W }, W](
        Normalise(extractor[Dimension.First, Position2D](First, "max.abs"))
          .andThenRenameWithValue(TransformerWithValue.rename(First, "%1$s.n"))
          .andThenExpandWithValue(TransformerWithValue.expand[Position2D, Position2D, W, String]("nrm")), ext)
      .toList.sortBy(_.position) shouldBe result11
  }

  it should "return its transformed data in 3D" in {
    toRDD(num3)
      .transformWithValue[Position4D, TransformerWithValue[Position3D, Position4D] { type V >: W }, W](
        Normalise(extractor[Dimension.First, Position3D](First, "max.abs"))
          .andThenRenameWithValue(TransformerWithValue.rename(First, "%1$s.n"))
          .andThenExpandWithValue(TransformerWithValue.expand[Position3D, Position3D, W, String]("nrm")), ext)
      .toList.sortBy(_.position) shouldBe result12
  }
}

trait TestMatrixSlide extends TestMatrix {

  val ext = Map("one" -> 1, "two" -> 2)

  val result1 = List(Cell(Position1D("1*(baz-bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 6.28)),
    Cell(Position1D("1*(foo-baz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 - 9.42)),
    Cell(Position1D("1*(qux-foo)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 3.14)),
    Cell(Position1D("2*(baz-bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 2 * (9.42 - 6.28))),
    Cell(Position1D("2*(foo-baz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 2 * (3.14 - 9.42))),
    Cell(Position1D("2*(qux-foo)"), Content(ContinuousSchema[Codex.DoubleCodex](), 2 * (12.56 - 3.14))))

  val result2 = List(Cell(Position2D("bar", "1*(2-1)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 6.28)),
    Cell(Position2D("bar", "1*(3-2)"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 12.56)),
    Cell(Position2D("baz", "1*(2-1)"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 9.42)),
    Cell(Position2D("foo", "1*(2-1)"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 - 3.14)),
    Cell(Position2D("foo", "1*(3-2)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 6.28)),
    Cell(Position2D("foo", "1*(4-3)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 9.42)))

  val result3 = List(Cell(Position2D(1, "1*(baz-bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 6.28)),
    Cell(Position2D(1, "1*(foo-baz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 - 9.42)),
    Cell(Position2D(1, "1*(qux-foo)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 3.14)),
    Cell(Position2D(2, "1*(baz-bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 12.56)),
    Cell(Position2D(2, "1*(foo-baz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 - 18.84)),
    Cell(Position2D(3, "1*(foo-bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 18.84)))

  val result4 = List(Cell(Position2D(1, "1*(baz-bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 6.28)),
    Cell(Position2D(1, "1*(foo-baz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 - 9.42)),
    Cell(Position2D(1, "1*(qux-foo)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 3.14)),
    Cell(Position2D(2, "1*(baz-bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 12.56)),
    Cell(Position2D(2, "1*(foo-baz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 - 18.84)),
    Cell(Position2D(3, "1*(foo-bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 18.84)))

  val result5 = List(Cell(Position2D("bar", "1*(2-1)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 6.28)),
    Cell(Position2D("bar", "1*(3-2)"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 12.56)),
    Cell(Position2D("baz", "1*(2-1)"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 9.42)),
    Cell(Position2D("foo", "1*(2-1)"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 - 3.14)),
    Cell(Position2D("foo", "1*(3-2)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 6.28)),
    Cell(Position2D("foo", "1*(4-3)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 9.42)))

  val result6 = List(
    Cell(Position2D("bar", "1*(2|xyz-1|xyz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 6.28)),
    Cell(Position2D("bar", "1*(3|xyz-2|xyz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 12.56)),
    Cell(Position2D("baz", "1*(2|xyz-1|xyz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 9.42)),
    Cell(Position2D("foo", "1*(2|xyz-1|xyz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 - 3.14)),
    Cell(Position2D("foo", "1*(3|xyz-2|xyz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 6.28)),
    Cell(Position2D("foo", "1*(4|xyz-3|xyz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 9.42)))

  val result7 = List(
    Cell(Position3D(1, "xyz", "1*(baz-bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 6.28)),
    Cell(Position3D(1, "xyz", "1*(foo-baz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 - 9.42)),
    Cell(Position3D(1, "xyz", "1*(qux-foo)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 3.14)),
    Cell(Position3D(2, "xyz", "1*(baz-bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 12.56)),
    Cell(Position3D(2, "xyz", "1*(foo-baz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 - 18.84)),
    Cell(Position3D(3, "xyz", "1*(foo-bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 18.84)))

  val result8 = List(
    Cell(Position2D(1, "1*(baz|xyz-bar|xyz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 6.28)),
    Cell(Position2D(1, "1*(foo|xyz-baz|xyz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 - 9.42)),
    Cell(Position2D(1, "1*(qux|xyz-foo|xyz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 3.14)),
    Cell(Position2D(2, "1*(baz|xyz-bar|xyz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 12.56)),
    Cell(Position2D(2, "1*(foo|xyz-baz|xyz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 - 18.84)),
    Cell(Position2D(3, "1*(foo|xyz-bar|xyz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 18.84)))

  val result9 = List(
    Cell(Position3D("bar", "xyz", "1*(2-1)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 6.28)),
    Cell(Position3D("bar", "xyz", "1*(3-2)"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 12.56)),
    Cell(Position3D("baz", "xyz", "1*(2-1)"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 9.42)),
    Cell(Position3D("foo", "xyz", "1*(2-1)"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 - 3.14)),
    Cell(Position3D("foo", "xyz", "1*(3-2)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 6.28)),
    Cell(Position3D("foo", "xyz", "1*(4-3)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 9.42)))

  val result10 = List(
    Cell(Position2D("xyz", "1*(bar|2-bar|1)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 6.28)),
    Cell(Position2D("xyz", "1*(bar|3-bar|2)"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 12.56)),
    Cell(Position2D("xyz", "1*(baz|1-bar|3)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 18.84)),
    Cell(Position2D("xyz", "1*(baz|2-baz|1)"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 9.42)),
    Cell(Position2D("xyz", "1*(foo|1-baz|2)"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 - 18.84)),
    Cell(Position2D("xyz", "1*(foo|2-foo|1)"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 - 3.14)),
    Cell(Position2D("xyz", "1*(foo|3-foo|2)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 6.28)),
    Cell(Position2D("xyz", "1*(foo|4-foo|3)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 9.42)),
    Cell(Position2D("xyz", "1*(qux|1-foo|4)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 12.56)))

  val result11 = List()

  val result12 = List(Cell(Position1D("1*(baz-bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 6.28)),
    Cell(Position1D("1*(foo-baz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 - 9.42)),
    Cell(Position1D("1*(qux-foo)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 3.14)),
    Cell(Position1D("2*(baz-bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 2 * (9.42 - 6.28))),
    Cell(Position1D("2*(foo-baz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 2 * (3.14 - 9.42))),
    Cell(Position1D("2*(qux-foo)"), Content(ContinuousSchema[Codex.DoubleCodex](), 2 * (12.56 - 3.14))))

  val result13 = List(Cell(Position2D("bar", "1*(2-1)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 6.28)),
    Cell(Position2D("bar", "1*(3-2)"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 12.56)),
    Cell(Position2D("baz", "1*(2-1)"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 9.42)),
    Cell(Position2D("foo", "1*(2-1)"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 - 3.14)),
    Cell(Position2D("foo", "1*(3-2)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 6.28)),
    Cell(Position2D("foo", "1*(4-3)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 9.42)))

  val result14 = List(Cell(Position2D(1, "1*(baz-bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 6.28)),
    Cell(Position2D(1, "1*(foo-baz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 - 9.42)),
    Cell(Position2D(1, "1*(qux-foo)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 3.14)),
    Cell(Position2D(2, "1*(baz-bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 12.56)),
    Cell(Position2D(2, "1*(foo-baz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 - 18.84)),
    Cell(Position2D(3, "1*(foo-bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 18.84)))

  val result15 = List(Cell(Position2D(1, "1*(baz-bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 6.28)),
    Cell(Position2D(1, "1*(foo-baz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 - 9.42)),
    Cell(Position2D(1, "1*(qux-foo)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 3.14)),
    Cell(Position2D(2, "1*(baz-bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 12.56)),
    Cell(Position2D(2, "1*(foo-baz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 - 18.84)),
    Cell(Position2D(3, "1*(foo-bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 18.84)))

  val result16 = List(Cell(Position2D("bar", "1*(2-1)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 6.28)),
    Cell(Position2D("bar", "1*(3-2)"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 12.56)),
    Cell(Position2D("baz", "1*(2-1)"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 9.42)),
    Cell(Position2D("foo", "1*(2-1)"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 - 3.14)),
    Cell(Position2D("foo", "1*(3-2)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 6.28)),
    Cell(Position2D("foo", "1*(4-3)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 9.42)))

  val result17 = List(
    Cell(Position2D("bar", "1*(2|xyz-1|xyz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 6.28)),
    Cell(Position2D("bar", "1*(3|xyz-2|xyz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 12.56)),
    Cell(Position2D("baz", "1*(2|xyz-1|xyz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 9.42)),
    Cell(Position2D("foo", "1*(2|xyz-1|xyz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 - 3.14)),
    Cell(Position2D("foo", "1*(3|xyz-2|xyz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 6.28)),
    Cell(Position2D("foo", "1*(4|xyz-3|xyz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 9.42)))

  val result18 = List(
    Cell(Position3D(1, "xyz", "1*(baz-bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 6.28)),
    Cell(Position3D(1, "xyz", "1*(foo-baz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 - 9.42)),
    Cell(Position3D(1, "xyz", "1*(qux-foo)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 3.14)),
    Cell(Position3D(2, "xyz", "1*(baz-bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 12.56)),
    Cell(Position3D(2, "xyz", "1*(foo-baz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 - 18.84)),
    Cell(Position3D(3, "xyz", "1*(foo-bar)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 18.84)))

  val result19 = List(
    Cell(Position2D(1, "1*(baz|xyz-bar|xyz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 6.28)),
    Cell(Position2D(1, "1*(foo|xyz-baz|xyz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 - 9.42)),
    Cell(Position2D(1, "1*(qux|xyz-foo|xyz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 3.14)),
    Cell(Position2D(2, "1*(baz|xyz-bar|xyz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 12.56)),
    Cell(Position2D(2, "1*(foo|xyz-baz|xyz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 - 18.84)),
    Cell(Position2D(3, "1*(foo|xyz-bar|xyz)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 18.84)))

  val result20 = List(
    Cell(Position3D("bar", "xyz", "1*(2-1)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 6.28)),
    Cell(Position3D("bar", "xyz", "1*(3-2)"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 12.56)),
    Cell(Position3D("baz", "xyz", "1*(2-1)"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 9.42)),
    Cell(Position3D("foo", "xyz", "1*(2-1)"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 - 3.14)),
    Cell(Position3D("foo", "xyz", "1*(3-2)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 6.28)),
    Cell(Position3D("foo", "xyz", "1*(4-3)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 9.42)))

  val result21 = List(
    Cell(Position2D("xyz", "1*(bar|2-bar|1)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 6.28)),
    Cell(Position2D("xyz", "1*(bar|3-bar|2)"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 12.56)),
    Cell(Position2D("xyz", "1*(baz|1-bar|3)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 18.84)),
    Cell(Position2D("xyz", "1*(baz|2-baz|1)"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84 - 9.42)),
    Cell(Position2D("xyz", "1*(foo|1-baz|2)"), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14 - 18.84)),
    Cell(Position2D("xyz", "1*(foo|2-foo|1)"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28 - 3.14)),
    Cell(Position2D("xyz", "1*(foo|3-foo|2)"), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42 - 6.28)),
    Cell(Position2D("xyz", "1*(foo|4-foo|3)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 9.42)),
    Cell(Position2D("xyz", "1*(qux|1-foo|4)"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56 - 12.56)))

  val result22 = List()
}

object TestMatrixSlide {

  case class Delta[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition](
    times: Int) extends Window[S, R, S#M] {
    type T = Cell[R]

    def initialise(cell: Cell[S], rem: R): (T, Collection[Cell[S#M]]) = (Cell(rem, cell.content), Collection())

    def present(cell: Cell[S], rem: R, t: T): (T, Collection[Cell[S#M]]) = {
      val delta = cell.content.value.asDouble.flatMap {
        case dc => t.content.value.asDouble.map {
          case dt => Left(Cell[S#M](cell.position.append(times + "*(" + rem.toShortString("|") + "-" +
            t.position.toShortString("|") + ")"), Content(ContinuousSchema[Codex.DoubleCodex](), times * (dc - dt))))
        }
      }

      (Cell(rem, cell.content), Collection(delta))
    }
  }

  case class DeltaWithValue[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition](
    key: String) extends WindowWithValue[S, R, S#M] {
    type T = Cell[R]
    type V = Map[String, Int]

    def initialiseWithValue(cell: Cell[S], rem: R, ext: V): (T, Collection[Cell[S#M]]) = {
      (Cell(rem, cell.content), Collection())
    }

    def presentWithValue(cell: Cell[S], rem: R, ext: V, t: T): (T, Collection[Cell[S#M]]) = {
      val delta = cell.content.value.asDouble.flatMap {
        case dc => t.content.value.asDouble.map {
          case dt => Left(Cell[S#M](cell.position.append(ext(key) + "*(" + rem.toShortString("|") + "-" +
            t.position.toShortString("|") + ")"), Content(ContinuousSchema[Codex.DoubleCodex](), ext(key) * (dc - dt))))
        }
      }

      (Cell(rem, cell.content), Collection(delta))
    }
  }
}

class TestScaldingMatrixSlide extends TestMatrixSlide with TBddDsl {

  "A Matrix.slide" should "return its first along derived data in 1D" in {
    Given {
      num1
    } When {
      cells: TypedPipe[Cell[Position1D]] =>
        cells.slide(Along(First), List(TestMatrixSlide.Delta[Position0D, Position1D](1),
          TestMatrixSlide.Delta[Position0D, Position1D](2)))
    } Then {
      _.toList.sortBy(_.position) shouldBe result1
    }
  }

  it should "return its first over derived data in 2D" in {
    Given {
      num2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.slide(Over(First), TestMatrixSlide.Delta[Position1D, Position1D](1))
    } Then {
      _.toList.sortBy(_.position) shouldBe result2
    }
  }

  it should "return its first along derived data in 2D" in {
    Given {
      num2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.slide(Along(First), TestMatrixSlide.Delta[Position1D, Position1D](1))
    } Then {
      _.toList.sortBy(_.position) shouldBe result3
    }
  }

  it should "return its second over derived data in 2D" in {
    Given {
      num2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.slide(Over(Second), TestMatrixSlide.Delta[Position1D, Position1D](1))
    } Then {
      _.toList.sortBy(_.position) shouldBe result4
    }
  }

  it should "return its second along derived data in 2D" in {
    Given {
      num2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.slide(Along(Second), TestMatrixSlide.Delta[Position1D, Position1D](1))
    } Then {
      _.toList.sortBy(_.position) shouldBe result5
    }
  }

  it should "return its first over derived data in 3D" in {
    Given {
      num3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.slide(Over(First), TestMatrixSlide.Delta[Position1D, Position2D](1))
    } Then {
      _.toList.sortBy(_.position) shouldBe result6
    }
  }

  it should "return its first along derived data in 3D" in {
    Given {
      num3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.slide(Along(First), TestMatrixSlide.Delta[Position2D, Position1D](1))
    } Then {
      _.toList.sortBy(_.position) shouldBe result7
    }
  }

  it should "return its second over derived data in 3D" in {
    Given {
      num3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.slide(Over(Second), TestMatrixSlide.Delta[Position1D, Position2D](1))
    } Then {
      _.toList.sortBy(_.position) shouldBe result8
    }
  }

  it should "return its second along derived data in 3D" in {
    Given {
      num3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.slide(Along(Second), TestMatrixSlide.Delta[Position2D, Position1D](1))
    } Then {
      _.toList.sortBy(_.position) shouldBe result9
    }
  }

  it should "return its third over derived data in 3D" in {
    Given {
      num3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.slide(Over(Third), TestMatrixSlide.Delta[Position1D, Position2D](1))
    } Then {
      _.toList.sortBy(_.position) shouldBe result10
    }
  }

  it should "return its third along derived data in 3D" in {
    Given {
      num3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.slide(Along(Third), TestMatrixSlide.Delta[Position2D, Position1D](1))
    } Then {
      _.toList.sortBy(_.position) shouldBe result11
    }
  }

  "A Matrix.slideWithValue" should "return its first along derived data in 1D" in {
    Given {
      num1
    } When {
      cells: TypedPipe[Cell[Position1D]] =>
        cells.slideWithValue(Along(First), List(TestMatrixSlide.DeltaWithValue[Position0D, Position1D]("one"),
          TestMatrixSlide.DeltaWithValue[Position0D, Position1D]("two")), ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result12
    }
  }

  it should "return its first over derived data in 2D" in {
    Given {
      num2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.slideWithValue(Over(First), TestMatrixSlide.DeltaWithValue[Position1D, Position1D]("one"), ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result13
    }
  }

  it should "return its first along derived data in 2D" in {
    Given {
      num2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.slideWithValue(Along(First), TestMatrixSlide.DeltaWithValue[Position1D, Position1D]("one"),
          ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result14
    }
  }

  it should "return its second over derived data in 2D" in {
    Given {
      num2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.slideWithValue(Over(Second), TestMatrixSlide.DeltaWithValue[Position1D, Position1D]("one"),
          ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result15
    }
  }

  it should "return its second along derived data in 2D" in {
    Given {
      num2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.slideWithValue(Along(Second), TestMatrixSlide.DeltaWithValue[Position1D, Position1D]("one"),
          ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result16
    }
  }

  it should "return its first over derived data in 3D" in {
    Given {
      num3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.slideWithValue(Over(First), TestMatrixSlide.DeltaWithValue[Position1D, Position2D]("one"), ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result17
    }
  }

  it should "return its first along derived data in 3D" in {
    Given {
      num3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.slideWithValue(Along(First), TestMatrixSlide.DeltaWithValue[Position2D, Position1D]("one"),
          ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result18
    }
  }

  it should "return its second over derived data in 3D" in {
    Given {
      num3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.slideWithValue(Over(Second), TestMatrixSlide.DeltaWithValue[Position1D, Position2D]("one"),
          ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result19
    }
  }

  it should "return its second along derived data in 3D" in {
    Given {
      num3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.slideWithValue(Along(Second), TestMatrixSlide.DeltaWithValue[Position2D, Position1D]("one"),
          ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result20
    }
  }

  it should "return its third over derived data in 3D" in {
    Given {
      num3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.slideWithValue(Over(Third), TestMatrixSlide.DeltaWithValue[Position1D, Position2D]("one"), ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result21
    }
  }

  it should "return its third along derived data in 3D" in {
    Given {
      num3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.slideWithValue(Along(Third), TestMatrixSlide.DeltaWithValue[Position2D, Position1D]("one"),
          ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result22
    }
  }
}

class TestSparkMatrixSlide extends TestMatrixSlide {

  "A Matrix.slide" should "return its first along derived data in 1D" in {
    toRDD(num1)
      .slide(Along(First), List(TestMatrixSlide.Delta[Position0D, Position1D](1),
        TestMatrixSlide.Delta[Position0D, Position1D](2)))
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first over derived data in 2D" in {
    toRDD(num2)
      .slide(Over(First), TestMatrixSlide.Delta[Position1D, Position1D](1))
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its first along derived data in 2D" in {
    toRDD(num2)
      .slide(Along(First), TestMatrixSlide.Delta[Position1D, Position1D](1))
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its second over derived data in 2D" in {
    toRDD(num2)
      .slide(Over(Second), TestMatrixSlide.Delta[Position1D, Position1D](1))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its second along derived data in 2D" in {
    toRDD(num2)
      .slide(Along(Second), TestMatrixSlide.Delta[Position1D, Position1D](1))
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its first over derived data in 3D" in {
    toRDD(num3)
      .slide(Over(First), TestMatrixSlide.Delta[Position1D, Position2D](1))
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its first along derived data in 3D" in {
    toRDD(num3)
      .slide(Along(First), TestMatrixSlide.Delta[Position2D, Position1D](1))
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its second over derived data in 3D" in {
    toRDD(num3)
      .slide(Over(Second), TestMatrixSlide.Delta[Position1D, Position2D](1))
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its second along derived data in 3D" in {
    toRDD(num3)
      .slide(Along(Second), TestMatrixSlide.Delta[Position2D, Position1D](1))
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its third over derived data in 3D" in {
    toRDD(num3)
      .slide(Over(Third), TestMatrixSlide.Delta[Position1D, Position2D](1))
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its third along derived data in 3D" in {
    toRDD(num3)
      .slide(Along(Third), TestMatrixSlide.Delta[Position2D, Position1D](1))
      .toList.sortBy(_.position) shouldBe result11
  }

  "A Matrix.slideWithValue" should "return its first along derived data in 1D" in {
    toRDD(num1)
      .slideWithValue(Along(First), List(TestMatrixSlide.DeltaWithValue[Position0D, Position1D]("one"),
        TestMatrixSlide.DeltaWithValue[Position0D, Position1D]("two")), ext)
      .toList.sortBy(_.position) shouldBe result12
  }

  it should "return its first over derived data in 2D" in {
    toRDD(num2)
      .slideWithValue(Over(First), TestMatrixSlide.DeltaWithValue[Position1D, Position1D]("one"), ext)
      .toList.sortBy(_.position) shouldBe result13
  }

  it should "return its first along derived data in 2D" in {
    toRDD(num2)
      .slideWithValue(Along(First), TestMatrixSlide.DeltaWithValue[Position1D, Position1D]("one"), ext)
      .toList.sortBy(_.position) shouldBe result14
  }

  it should "return its second over derived data in 2D" in {
    toRDD(num2)
      .slideWithValue(Over(Second), TestMatrixSlide.DeltaWithValue[Position1D, Position1D]("one"), ext)
      .toList.sortBy(_.position) shouldBe result15
  }

  it should "return its second along derived data in 2D" in {
    toRDD(num2)
      .slideWithValue(Along(Second),
        TestMatrixSlide.DeltaWithValue[Position1D, Position1D]("one"), ext)
      .toList.sortBy(_.position) shouldBe result16
  }

  it should "return its first over derived data in 3D" in {
    toRDD(num3)
      .slideWithValue(Over(First), TestMatrixSlide.DeltaWithValue[Position1D, Position2D]("one"), ext)
      .toList.sortBy(_.position) shouldBe result17
  }

  it should "return its first along derived data in 3D" in {
    toRDD(num3)
      .slideWithValue(Along(First), TestMatrixSlide.DeltaWithValue[Position2D, Position1D]("one"), ext)
      .toList.sortBy(_.position) shouldBe result18
  }

  it should "return its second over derived data in 3D" in {
    toRDD(num3)
      .slideWithValue(Over(Second), TestMatrixSlide.DeltaWithValue[Position1D, Position2D]("one"), ext)
      .toList.sortBy(_.position) shouldBe result19
  }

  it should "return its second along derived data in 3D" in {
    toRDD(num3)
      .slideWithValue(Along(Second), TestMatrixSlide.DeltaWithValue[Position2D, Position1D]("one"), ext)
      .toList.sortBy(_.position) shouldBe result20
  }

  it should "return its third over derived data in 3D" in {
    toRDD(num3)
      .slideWithValue(Over(Third), TestMatrixSlide.DeltaWithValue[Position1D, Position2D]("one"), ext)
      .toList.sortBy(_.position) shouldBe result21
  }

  it should "return its third along derived data in 3D" in {
    toRDD(num3)
      .slideWithValue(Along(Third), TestMatrixSlide.DeltaWithValue[Position2D, Position1D]("one"), ext)
      .toList.sortBy(_.position) shouldBe result22
  }
}

trait TestMatrixFill extends TestMatrix {

  val result1 = List(Cell(Position2D("bar", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
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

  val result2 = List(Cell(Position3D("bar", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
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

  val result3 = List(Cell(Position2D("bar", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
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

  val result4 = List(Cell(Position2D("bar", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
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

  val result5 = List(Cell(Position2D("bar", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
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

  val result6 = List(Cell(Position2D("bar", 1), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
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

  val result7 = List(Cell(Position3D("bar", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position3D("bar", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
    Cell(Position3D("bar", 4, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), (6.28 + 12.56 + 18.84) / 3)),
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

  val result8 = List(Cell(Position3D("bar", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
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
    Cell(Position3D("qux", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), (6.28 + 12.56 + 18.84) / 3)),
    Cell(Position3D("qux", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), (9.42 + 18.84) / 2)),
    Cell(Position3D("qux", 4, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), (12.56) / 1)))

  val result9 = List(Cell(Position3D("bar", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
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
    Cell(Position3D("qux", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), (6.28 + 12.56 + 18.84) / 3)),
    Cell(Position3D("qux", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), (9.42 + 18.84) / 2)),
    Cell(Position3D("qux", 4, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), (12.56) / 1)))

  val result10 = List(Cell(Position3D("bar", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position3D("bar", 3, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
    Cell(Position3D("bar", 4, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), (6.28 + 12.56 + 18.84) / 3)),
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

  val result11 = List(Cell(Position3D("bar", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
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

  val result12 = List(Cell(Position3D("bar", 1, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
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

class TestScaldingMatrixFill extends TestMatrixFill with TBddDsl {

  "A Matrix.fill" should "return its filled data in 2D" in {
    Given {
      num2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.fill(Content(ContinuousSchema[Codex.DoubleCodex](), 0))
    } Then {
      _.toList.sortBy(_.position) shouldBe result1
    }
  }

  it should "return its filled data in 3D" in {
    Given {
      num3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.fill(Content(ContinuousSchema[Codex.DoubleCodex](), 0))
    } Then {
      _.toList.sortBy(_.position) shouldBe result2
    }
  }

  "A Matrix.fill" should "return its first over filled data in 2D" in {
    Given {
      num2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.fill(Over(First), cells.summarise[Dimension.First, Position1D, Mean[Position2D, Position1D]](
          Over[Position2D, Dimension.First](First), Mean[Position2D, Position1D]()))
    } Then {
      _.toList.sortBy(_.position) shouldBe result3
    }
  }

  it should "return its first along filled data in 2D" in {
    Given {
      num2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.fill(Along(First), cells.summarise[Dimension.First, Position1D, Mean[Position2D, Position1D]](
          Along[Position2D, Dimension.First](First), Mean[Position2D, Position1D]()))
    } Then {
      _.toList.sortBy(_.position) shouldBe result4
    }
  }

  it should "return its second over filled data in 2D" in {
    Given {
      num2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.fill(Over(Second), cells.summarise[Dimension.Second, Position1D, Mean[Position2D, Position1D]](
          Over[Position2D, Dimension.Second](Second), Mean[Position2D, Position1D]()))
    } Then {
      _.toList.sortBy(_.position) shouldBe result5
    }
  }

  it should "return its second along filled data in 2D" in {
    Given {
      num2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.fill(Along(Second), cells.summarise[Dimension.Second, Position1D, Mean[Position2D, Position1D]](
          Along[Position2D, Dimension.Second](Second), Mean[Position2D, Position1D]()))
    } Then {
      _.toList.sortBy(_.position) shouldBe result6
    }
  }

  it should "return its first over filled data in 3D" in {
    Given {
      num3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.fill(Over(First), cells.summarise[Dimension.First, Position1D, Mean[Position3D, Position1D]](
          Over[Position3D, Dimension.First](First), Mean[Position3D, Position1D]()))
    } Then {
      _.toList.sortBy(_.position) shouldBe result7
    }
  }

  it should "return its first along filled data in 3D" in {
    Given {
      num3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.fill(Along(First), cells.summarise[Dimension.First, Position2D, Mean[Position3D, Position2D]](
          Along[Position3D, Dimension.First](First), Mean[Position3D, Position2D]()))
    } Then {
      _.toList.sortBy(_.position) shouldBe result8
    }
  }

  it should "return its second over filled data in 3D" in {
    Given {
      num3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.fill(Over(Second), cells.summarise[Dimension.Second, Position1D, Mean[Position3D, Position1D]](
          Over[Position3D, Dimension.Second](Second), Mean[Position3D, Position1D]()))
    } Then {
      _.toList.sortBy(_.position) shouldBe result9
    }
  }

  it should "return its second along filled data in 3D" in {
    Given {
      num3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.fill(Along(Second), cells.summarise[Dimension.Second, Position2D, Mean[Position3D, Position2D]](
          Along[Position3D, Dimension.Second](Second), Mean[Position3D, Position2D]()))
    } Then {
      _.toList.sortBy(_.position) shouldBe result10
    }
  }

  it should "return its third over filled data in 3D" in {
    Given {
      num3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.fill(Over(Third), cells.summarise[Dimension.Third, Position1D, Mean[Position3D, Position1D]](
          Over[Position3D, Dimension.Third](Third), Mean[Position3D, Position1D]()))
    } Then {
      _.toList.sortBy(_.position) shouldBe result11
    }
  }

  it should "return its third along filled data in 3D" in {
    Given {
      num3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.fill(Along(Third), cells.summarise[Dimension.Third, Position2D, Mean[Position3D, Position2D]](
          Along[Position3D, Dimension.Third](Third), Mean[Position3D, Position2D]()))
    } Then {
      _.toList.sortBy(_.position) shouldBe result12
    }
  }
}

class TestSparkMatrixFill extends TestMatrixFill {

  "A Matrix.fill" should "return its filled data in 2D" in {
    toRDD(num2)
      .fill(Content(ContinuousSchema[Codex.DoubleCodex](), 0))
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its filled data in 3D" in {
    toRDD(num3)
      .fill(Content(ContinuousSchema[Codex.DoubleCodex](), 0))
      .toList.sortBy(_.position) shouldBe result2
  }

  "A Matrix.fill" should "return its first over filled data in 2D" in {
    val cells = toRDD(num2)

    cells
      .fill(Over(First), cells.summarise[Dimension.First, Position1D, Mean[Position2D, Position1D]](
        Over[Position2D, Dimension.First](First), Mean[Position2D, Position1D]()))
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its first along filled data in 2D" in {
    val cells = toRDD(num2)

    cells
      .fill(Along(First), cells.summarise[Dimension.First, Position1D, Mean[Position2D, Position1D]](
        Along[Position2D, Dimension.First](First), Mean[Position2D, Position1D]()))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its second over filled data in 2D" in {
    val cells = toRDD(num2)

    cells
      .fill(Over(Second), cells.summarise[Dimension.Second, Position1D, Mean[Position2D, Position1D]](
        Over[Position2D, Dimension.Second](Second), Mean[Position2D, Position1D]()))
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its second along filled data in 2D" in {
    val cells = toRDD(num2)

    cells
      .fill(Along(Second), cells.summarise[Dimension.Second, Position1D, Mean[Position2D, Position1D]](
        Along[Position2D, Dimension.Second](Second), Mean[Position2D, Position1D]()))
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its first over filled data in 3D" in {
    val cells = toRDD(num3)

    cells
      .fill(Over(First), cells.summarise[Dimension.First, Position1D, Mean[Position3D, Position1D]](
        Over[Position3D, Dimension.First](First), Mean[Position3D, Position1D]()))
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its first along filled data in 3D" in {
    val cells = toRDD(num3)

    cells
      .fill(Along(First), cells.summarise[Dimension.First, Position2D, Mean[Position3D, Position2D]](
        Along[Position3D, Dimension.First](First), Mean[Position3D, Position2D]()))
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its second over filled data in 3D" in {
    val cells = toRDD(num3)

    cells
      .fill(Over(Second), cells.summarise[Dimension.Second, Position1D, Mean[Position3D, Position1D]](
        Over[Position3D, Dimension.Second](Second), Mean[Position3D, Position1D]()))
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its second along filled data in 3D" in {
    val cells = toRDD(num3)

    cells
      .fill(Along(Second), cells.summarise[Dimension.Second, Position2D, Mean[Position3D, Position2D]](
        Along[Position3D, Dimension.Second](Second), Mean[Position3D, Position2D]()))
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its third over filled data in 3D" in {
    val cells = toRDD(num3)

    cells
      .fill(Over(Third), cells.summarise[Dimension.Third, Position1D, Mean[Position3D, Position1D]](
        Over[Position3D, Dimension.Third](Third), Mean[Position3D, Position1D]()))
      .toList.sortBy(_.position) shouldBe result11
  }

  it should "return its third along filled data in 3D" in {
    val cells = toRDD(num3)

    cells
      .fill(Along(Third), cells.summarise[Dimension.Third, Position2D, Mean[Position3D, Position2D]](
        Along[Position3D, Dimension.Third](Third), Mean[Position3D, Position2D]()))
      .toList.sortBy(_.position) shouldBe result12
  }
}

trait TestMatrixRename extends TestMatrix {

  val ext = ".new"

  val result1 = List(Cell(Position1D("bar.new"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
    Cell(Position1D("baz.new"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position1D("foo.new"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
    Cell(Position1D("qux.new"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))

  val result2 = List(Cell(Position2D("bar.new", 1), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
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

  val result3 = List(Cell(Position2D("bar", "1.new"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
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

  val result4 = List(Cell(Position3D("bar.new", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
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

  val result5 = List(Cell(Position3D("bar", "1.new", "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
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

  val result6 = List(Cell(Position3D("bar", 1, "xyz.new"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
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

  val result7 = List(Cell(Position1D("bar.new"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
    Cell(Position1D("baz.new"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position1D("foo.new"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
    Cell(Position1D("qux.new"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))

  val result8 = List(Cell(Position2D("bar.new", 1), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
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

  val result9 = List(Cell(Position2D("bar", "1.new"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
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

  val result10 = List(Cell(Position3D("bar.new", 1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
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

  val result11 = List(Cell(Position3D("bar", "1.new", "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
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

  val result12 = List(Cell(Position3D("bar", 1, "xyz.new"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
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

object TestMatrixRename {

  def renamer[P <: Position](dim: Dimension)(cell: Cell[P]): P = {
    cell.position.update(dim, cell.position(dim).toShortString + ".new")
  }

  def renamerWithValue[P <: Position](dim: Dimension)(cell: Cell[P], ext: String): P = {
    cell.position.update(dim, cell.position(dim).toShortString + ext)
  }
}

class TestScaldingMatrixRename extends TestMatrixRename with TBddDsl {

  "A Matrix.rename" should "return its first renamed data in 1D" in {
    Given {
      data1
    } When {
      cells: TypedPipe[Cell[Position1D]] =>
        cells.rename(TestMatrixRename.renamer(First))
    } Then {
      _.toList.sortBy(_.position) shouldBe result1
    }
  }

  it should "return its first renamed data in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.rename(TestMatrixRename.renamer(First))
    } Then {
      _.toList.sortBy(_.position) shouldBe result2
    }
  }

  it should "return its second renamed data in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.rename(TestMatrixRename.renamer(Second))
    } Then {
      _.toList.sortBy(_.position) shouldBe result3
    }
  }

  it should "return its first renamed data in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.rename(TestMatrixRename.renamer(First))
    } Then {
      _.toList.sortBy(_.position) shouldBe result4
    }
  }

  it should "return its second renamed data in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.rename(TestMatrixRename.renamer(Second))
    } Then {
      _.toList.sortBy(_.position) shouldBe result5
    }
  }

  it should "return its third renamed data in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.rename(TestMatrixRename.renamer(Third))
    } Then {
      _.toList.sortBy(_.position) shouldBe result6
    }
  }

  "A Matrix.renameWithValue" should "return its first renamed data in 1D" in {
    Given {
      data1
    } When {
      cells: TypedPipe[Cell[Position1D]] =>
        cells.renameWithValue(TestMatrixRename.renamerWithValue(First), ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result7
    }
  }

  it should "return its first renamed data in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.renameWithValue(TestMatrixRename.renamerWithValue(First), ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result8
    }
  }

  it should "return its second renamed data in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.renameWithValue(TestMatrixRename.renamerWithValue(Second), ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result9
    }
  }

  it should "return its first renamed data in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.renameWithValue(TestMatrixRename.renamerWithValue(First), ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result10
    }
  }

  it should "return its second renamed data in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.renameWithValue(TestMatrixRename.renamerWithValue(Second), ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result11
    }
  }

  it should "return its third renamed data in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.renameWithValue(TestMatrixRename.renamerWithValue(Third), ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result12
    }
  }
}

class TestSparkMatrixRename extends TestMatrixRename {

  "A Matrix.rename" should "return its first renamed data in 1D" in {
    toRDD(data1)
      .rename(TestMatrixRename.renamer(First))
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first renamed data in 2D" in {
    toRDD(data2)
      .rename(TestMatrixRename.renamer(First))
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its second renamed data in 2D" in {
    toRDD(data2)
      .rename(TestMatrixRename.renamer(Second))
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its first renamed data in 3D" in {
    toRDD(data3)
      .rename(TestMatrixRename.renamer(First))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its second renamed data in 3D" in {
    toRDD(data3)
      .rename(TestMatrixRename.renamer(Second))
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its third renamed data in 3D" in {
    toRDD(data3)
      .rename(TestMatrixRename.renamer(Third))
      .toList.sortBy(_.position) shouldBe result6
  }

  "A Matrix.renameWithValue" should "return its first renamed data in 1D" in {
    toRDD(data1)
      .renameWithValue(TestMatrixRename.renamerWithValue(First), ext)
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its first renamed data in 2D" in {
    toRDD(data2)
      .renameWithValue(TestMatrixRename.renamerWithValue(First), ext)
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its second renamed data in 2D" in {
    toRDD(data2)
      .renameWithValue(TestMatrixRename.renamerWithValue(Second), ext)
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its first renamed data in 3D" in {
    toRDD(data3)
      .renameWithValue(TestMatrixRename.renamerWithValue(First), ext)
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its second renamed data in 3D" in {
    toRDD(data3)
      .renameWithValue(TestMatrixRename.renamerWithValue(Second), ext)
      .toList.sortBy(_.position) shouldBe result11
  }

  it should "return its third renamed data in 3D" in {
    toRDD(data3)
      .renameWithValue(TestMatrixRename.renamerWithValue(Third), ext)
      .toList.sortBy(_.position) shouldBe result12
  }
}

trait TestMatrixSquash extends TestMatrix {

  val ext = "ext"

  val result1 = List(Cell(Position1D(1), Content(OrdinalSchema[Codex.StringCodex](), "12.56")),
    Cell(Position1D(2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position1D(3), Content(NominalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position1D(4), Content(DateSchema[Codex.DateTimeCodex](),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))

  val result2 = List(Cell(Position1D("bar"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
    Cell(Position1D("baz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
    Cell(Position1D("foo"), Content(DateSchema[Codex.DateTimeCodex](),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
    Cell(Position1D("qux"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))

  val result3 = List(Cell(Position2D(1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")),
    Cell(Position2D(2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position2D(3, "xyz"), Content(NominalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position2D(4, "xyz"), Content(DateSchema[Codex.DateTimeCodex](),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))

  val result4 = List(Cell(Position2D("bar", "xyz"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
    Cell(Position2D("baz", "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
    Cell(Position2D("foo", "xyz"), Content(DateSchema[Codex.DateTimeCodex](),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
    Cell(Position2D("qux", "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))

  val result5 = List(Cell(Position2D("bar", 1), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
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

  val result6 = List(Cell(Position1D(1), Content(OrdinalSchema[Codex.StringCodex](), "12.56")),
    Cell(Position1D(2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position1D(3), Content(NominalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position1D(4), Content(DateSchema[Codex.DateTimeCodex](),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))

  val result7 = List(Cell(Position1D("bar"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
    Cell(Position1D("baz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
    Cell(Position1D("foo"), Content(DateSchema[Codex.DateTimeCodex](),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
    Cell(Position1D("qux"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))

  val result8 = List(Cell(Position2D(1, "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")),
    Cell(Position2D(2, "xyz"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position2D(3, "xyz"), Content(NominalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position2D(4, "xyz"), Content(DateSchema[Codex.DateTimeCodex](),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))

  val result9 = List(Cell(Position2D("bar", "xyz"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
    Cell(Position2D("baz", "xyz"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
    Cell(Position2D("foo", "xyz"), Content(DateSchema[Codex.DateTimeCodex](),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
    Cell(Position2D("qux", "xyz"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))

  val result10 = List(Cell(Position2D("bar", 1), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
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

object TestMatrixSquash {

  case class PreservingMaxPositionWithValue[P <: Position]() extends SquasherWithValue[P] {
    type V = String

    val squasher = PreservingMaxPosition[P]()

    def reduceWithValue(dim: Dimension, x: Cell[P], y: Cell[P], ext: V): Cell[P] = {
      if (ext == "ext") squasher.reduce(dim, x, y) else x
    }
  }
}

class TestScaldingMatrixSquash extends TestMatrixSquash with TBddDsl {

  "A Matrix.squash" should "return its first squashed data in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.squash(First, PreservingMaxPosition[Position2D]())
    } Then {
      _.toList.sortBy(_.position) shouldBe result1
    }
  }

  it should "return its second squashed data in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.squash(Second, PreservingMaxPosition[Position2D]())
    } Then {
      _.toList.sortBy(_.position) shouldBe result2
    }
  }

  it should "return its first squashed data in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.squash(First, PreservingMaxPosition[Position3D]())
    } Then {
      _.toList.sortBy(_.position) shouldBe result3
    }
  }

  it should "return its second squashed data in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.squash(Second, PreservingMaxPosition[Position3D]())
    } Then {
      _.toList.sortBy(_.position) shouldBe result4
    }
  }

  it should "return its third squashed data in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.squash(Third, PreservingMaxPosition[Position3D]())
    } Then {
      _.toList.sortBy(_.position) shouldBe result5
    }
  }

  "A Matrix.squashWithValue" should "return its first squashed data in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.squashWithValue(First, TestMatrixSquash.PreservingMaxPositionWithValue[Position2D](), ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result6
    }
  }

  it should "return its second squashed data in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.squashWithValue(Second, TestMatrixSquash.PreservingMaxPositionWithValue[Position2D](), ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result7
    }
  }

  it should "return its first squashed data in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.squashWithValue(First, TestMatrixSquash.PreservingMaxPositionWithValue[Position3D](), ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result8
    }
  }

  it should "return its second squashed data in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.squashWithValue(Second, TestMatrixSquash.PreservingMaxPositionWithValue[Position3D](), ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result9
    }
  }

  it should "return its third squashed data in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.squashWithValue(Third, TestMatrixSquash.PreservingMaxPositionWithValue[Position3D](), ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result10
    }
  }
}

class TestSparkMatrixSquash extends TestMatrixSquash {

  "A Matrix.squash" should "return its first squashed data in 2D" in {
    toRDD(data2)
      .squash(First, PreservingMaxPosition[Position2D]())
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its second squashed data in 2D" in {
    toRDD(data2)
      .squash(Second, PreservingMaxPosition[Position2D]())
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its first squashed data in 3D" in {
    toRDD(data3)
      .squash(First, PreservingMaxPosition[Position3D]())
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its second squashed data in 3D" in {
    toRDD(data3)
      .squash(Second, PreservingMaxPosition[Position3D]())
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its third squashed data in 3D" in {
    toRDD(data3)
      .squash(Third, PreservingMaxPosition[Position3D]())
      .toList.sortBy(_.position) shouldBe result5
  }

  "A Matrix.squashWithValue" should "return its first squashed data in 2D" in {
    toRDD(data2)
      .squashWithValue(First, TestMatrixSquash.PreservingMaxPositionWithValue[Position2D](), ext)
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its second squashed data in 2D" in {
    toRDD(data2)
      .squashWithValue(Second, TestMatrixSquash.PreservingMaxPositionWithValue[Position2D](), ext)
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its first squashed data in 3D" in {
    toRDD(data3)
      .squashWithValue(First, TestMatrixSquash.PreservingMaxPositionWithValue[Position3D](), ext)
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its second squashed data in 3D" in {
    toRDD(data3)
      .squashWithValue(Second, TestMatrixSquash.PreservingMaxPositionWithValue[Position3D](), ext)
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its third squashed data in 3D" in {
    toRDD(data3)
      .squashWithValue(Third, TestMatrixSquash.PreservingMaxPositionWithValue[Position3D](), ext)
      .toList.sortBy(_.position) shouldBe result10
  }
}

trait TestMatrixMelt extends TestMatrix {

  val result1 = List(Cell(Position1D("1.bar"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
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

  val result2 = List(Cell(Position1D("bar.1"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
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

  val result3 = List(Cell(Position2D(1, "xyz.bar"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
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

  val result4 = List(Cell(Position2D("bar", "xyz.1"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
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

  val result5 = List(Cell(Position2D("bar.xyz", 1), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
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

class TestScaldingMatrixMelt extends TestMatrixMelt with TBddDsl {

  "A Matrix.melt" should "return its first melted data in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.melt(First, Second)
    } Then {
      _.toList.sortBy(_.position) shouldBe result1
    }
  }

  it should "return its second melted data in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.melt(Second, First)
    } Then {
      _.toList.sortBy(_.position) shouldBe result2
    }
  }

  it should "return its first melted data in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.melt(First, Third)
    } Then {
      _.toList.sortBy(_.position) shouldBe result3
    }
  }

  it should "return its second melted data in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.melt(Second, Third)
    } Then {
      _.toList.sortBy(_.position) shouldBe result4
    }
  }

  it should "return its third melted data in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.melt(Third, First)
    } Then {
      _.toList.sortBy(_.position) shouldBe result5
    }
  }
}

class TestSparkMatrixMelt extends TestMatrixMelt {

  "A Matrix.melt" should "return its first melted data in 2D" in {
    toRDD(data2)
      .melt(First, Second)
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its second melted data in 2D" in {
    toRDD(data2)
      .melt(Second, First)
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its first melted data in 3D" in {
    toRDD(data3)
      .melt(First, Third)
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its second melted data in 3D" in {
    toRDD(data3)
      .melt(Second, Third)
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its third melted data in 3D" in {
    toRDD(data3)
      .melt(Third, First)
      .toList.sortBy(_.position) shouldBe result5
  }
}

trait TestMatrixExpand extends TestMatrix {

  val ext = "abc"

  val result1 = List(Cell(Position2D("bar", "abc"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
    Cell(Position2D("baz", "abc"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position2D("foo", "abc"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
    Cell(Position2D("qux", "abc"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))

  val result2 = List(Cell(Position3D("bar", "abc", "def"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
    Cell(Position3D("baz", "abc", "def"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position3D("foo", "abc", "def"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
    Cell(Position3D("qux", "abc", "def"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))

  val result3 = List(Cell(Position4D("bar", "abc", "def", "ghi"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
    Cell(Position4D("baz", "abc", "def", "ghi"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position4D("foo", "abc", "def", "ghi"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
    Cell(Position4D("qux", "abc", "def", "ghi"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))

  val result4 = List(
    Cell(Position5D("bar", "abc", "def", "ghi", "jkl"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
    Cell(Position5D("baz", "abc", "def", "ghi", "jkl"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position5D("foo", "abc", "def", "ghi", "jkl"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
    Cell(Position5D("qux", "abc", "def", "ghi", "jkl"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))

  val result5 = List(Cell(Position3D("bar", 1, "abc"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
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

  val result6 = List(Cell(Position4D("bar", 1, "abc", "def"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
    Cell(Position4D("bar", 2, "abc", "def"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position4D("bar", 3, "abc", "def"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
    Cell(Position4D("baz", 1, "abc", "def"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position4D("baz", 2, "abc", "def"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
    Cell(Position4D("foo", 1, "abc", "def"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
    Cell(Position4D("foo", 2, "abc", "def"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position4D("foo", 3, "abc", "def"), Content(NominalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position4D("foo", 4, "abc", "def"), Content(DateSchema[Codex.DateTimeCodex](),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
    Cell(Position4D("qux", 1, "abc", "def"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))

  val result7 = List(
    Cell(Position5D("bar", 1, "abc", "def", "ghi"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
    Cell(Position5D("bar", 2, "abc", "def", "ghi"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position5D("bar", 3, "abc", "def", "ghi"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
    Cell(Position5D("baz", 1, "abc", "def", "ghi"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position5D("baz", 2, "abc", "def", "ghi"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
    Cell(Position5D("foo", 1, "abc", "def", "ghi"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
    Cell(Position5D("foo", 2, "abc", "def", "ghi"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position5D("foo", 3, "abc", "def", "ghi"), Content(NominalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position5D("foo", 4, "abc", "def", "ghi"), Content(DateSchema[Codex.DateTimeCodex](),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
    Cell(Position5D("qux", 1, "abc", "def", "ghi"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))

  val result8 = List(Cell(Position4D("bar", 1, "xyz", "abc"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
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

  val result9 = List(
    Cell(Position5D("bar", 1, "xyz", "abc", "def"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
    Cell(Position5D("bar", 2, "xyz", "abc", "def"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position5D("bar", 3, "xyz", "abc", "def"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
    Cell(Position5D("baz", 1, "xyz", "abc", "def"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position5D("baz", 2, "xyz", "abc", "def"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
    Cell(Position5D("foo", 1, "xyz", "abc", "def"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
    Cell(Position5D("foo", 2, "xyz", "abc", "def"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position5D("foo", 3, "xyz", "abc", "def"), Content(NominalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position5D("foo", 4, "xyz", "abc", "def"), Content(DateSchema[Codex.DateTimeCodex](),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
    Cell(Position5D("qux", 1, "xyz", "abc", "def"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))

  val result10 = List(Cell(Position2D("bar", "abc"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
    Cell(Position2D("baz", "abc"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position2D("foo", "abc"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
    Cell(Position2D("qux", "abc"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))

  val result11 = List(Cell(Position3D("bar", "abc", "def"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
    Cell(Position3D("baz", "abc", "def"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position3D("foo", "abc", "def"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
    Cell(Position3D("qux", "abc", "def"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))

  val result12 = List(Cell(Position4D("bar", "abc", "def", "ghi"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
    Cell(Position4D("baz", "abc", "def", "ghi"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position4D("foo", "abc", "def", "ghi"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
    Cell(Position4D("qux", "abc", "def", "ghi"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))

  val result13 = List(
    Cell(Position5D("bar", "abc", "def", "ghi", "jkl"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
    Cell(Position5D("baz", "abc", "def", "ghi", "jkl"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position5D("foo", "abc", "def", "ghi", "jkl"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
    Cell(Position5D("qux", "abc", "def", "ghi", "jkl"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))

  val result14 = List(Cell(Position3D("bar", 1, "abc"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
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

  val result15 = List(Cell(Position4D("bar", 1, "abc", "def"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
    Cell(Position4D("bar", 2, "abc", "def"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position4D("bar", 3, "abc", "def"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
    Cell(Position4D("baz", 1, "abc", "def"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position4D("baz", 2, "abc", "def"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
    Cell(Position4D("foo", 1, "abc", "def"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
    Cell(Position4D("foo", 2, "abc", "def"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position4D("foo", 3, "abc", "def"), Content(NominalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position4D("foo", 4, "abc", "def"), Content(DateSchema[Codex.DateTimeCodex](),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
    Cell(Position4D("qux", 1, "abc", "def"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))

  val result16 = List(
    Cell(Position5D("bar", 1, "abc", "def", "ghi"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
    Cell(Position5D("bar", 2, "abc", "def", "ghi"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position5D("bar", 3, "abc", "def", "ghi"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
    Cell(Position5D("baz", 1, "abc", "def", "ghi"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position5D("baz", 2, "abc", "def", "ghi"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
    Cell(Position5D("foo", 1, "abc", "def", "ghi"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
    Cell(Position5D("foo", 2, "abc", "def", "ghi"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position5D("foo", 3, "abc", "def", "ghi"), Content(NominalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position5D("foo", 4, "abc", "def", "ghi"), Content(DateSchema[Codex.DateTimeCodex](),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
    Cell(Position5D("qux", 1, "abc", "def", "ghi"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))

  val result17 = List(Cell(Position4D("bar", 1, "xyz", "abc"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
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

  val result18 = List(
    Cell(Position5D("bar", 1, "xyz", "abc", "def"), Content(OrdinalSchema[Codex.StringCodex](), "6.28")),
    Cell(Position5D("bar", 2, "xyz", "abc", "def"), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position5D("bar", 3, "xyz", "abc", "def"), Content(OrdinalSchema[Codex.LongCodex](), 19)),
    Cell(Position5D("baz", 1, "xyz", "abc", "def"), Content(OrdinalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position5D("baz", 2, "xyz", "abc", "def"), Content(DiscreteSchema[Codex.LongCodex](), 19)),
    Cell(Position5D("foo", 1, "xyz", "abc", "def"), Content(OrdinalSchema[Codex.StringCodex](), "3.14")),
    Cell(Position5D("foo", 2, "xyz", "abc", "def"), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position5D("foo", 3, "xyz", "abc", "def"), Content(NominalSchema[Codex.StringCodex](), "9.42")),
    Cell(Position5D("foo", 4, "xyz", "abc", "def"), Content(DateSchema[Codex.DateTimeCodex](),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
    Cell(Position5D("qux", 1, "xyz", "abc", "def"), Content(OrdinalSchema[Codex.StringCodex](), "12.56")))
}

object TestMatrixExpand {

  type PwE = Position with ExpandablePosition

  def expander1D[P <: PwE](cell: Cell[P]): P#M = cell.position.append("abc")
  def expander2D[P <: PwE, Q <: PwE](cell: Cell[P])(implicit ev: P#M =:= Q): Q#M = {
    cell.position.append("abc").append("def")
  }
  def expander3D[P <: PwE, Q <: PwE, R <: PwE](cell: Cell[P])(implicit ev1: P#M =:= Q, ev2: Q#M =:= R): R#M = {
    cell.position.append("abc").append("def").append("ghi")
  }
  def expander4D[P <: PwE, Q <: PwE, R <: PwE, S <: PwE](cell: Cell[P])(implicit ev1: P#M =:= Q, ev2: Q#M =:= R,
    ev3: R#M =:= S): S#M = cell.position.append("abc").append("def").append("ghi").append("jkl")

  val expand1D2D = expander1D[Position1D] _
  val expand1D3D = expander2D[Position1D, Position2D] _
  val expand1D4D = expander3D[Position1D, Position2D, Position3D] _
  val expand1D5D = expander4D[Position1D, Position2D, Position3D, Position4D] _

  val expand2D3D = expander1D[Position2D] _
  val expand2D4D = expander2D[Position2D, Position3D] _
  val expand2D5D = expander3D[Position2D, Position3D, Position4D] _

  val expand3D4D = expander1D[Position3D] _
  val expand3D5D = expander2D[Position3D, Position4D] _

  def expander1DWithValue[P <: PwE](cell: Cell[P], ext: String): P#M = cell.position.append(ext)
  def expander2DWithValue[P <: PwE, Q <: PwE](cell: Cell[P], ext: String)(implicit ev: P#M =:= Q): Q#M = {
    cell.position.append(ext).append("def")
  }
  def expander3DWithValue[P <: PwE, Q <: PwE, R <: PwE](cell: Cell[P], ext: String)(implicit ev1: P#M =:= Q,
    ev2: Q#M =:= R): R#M = cell.position.append(ext).append("def").append("ghi")
  def expander4DWithValue[P <: PwE, Q <: PwE, R <: PwE, S <: PwE](cell: Cell[P], ext: String)(implicit ev1: P#M =:= Q,
    ev2: Q#M =:= R, ev3: R#M =:= S): S#M = cell.position.append(ext).append("def").append("ghi").append("jkl")

  val expand1D2DWithValue = expander1DWithValue[Position1D] _
  val expand1D3DWithValue = expander2DWithValue[Position1D, Position2D] _
  val expand1D4DWithValue = expander3DWithValue[Position1D, Position2D, Position3D] _
  val expand1D5DWithValue = expander4DWithValue[Position1D, Position2D, Position3D, Position4D] _

  val expand2D3DWithValue = expander1DWithValue[Position2D] _
  val expand2D4DWithValue = expander2DWithValue[Position2D, Position3D] _
  val expand2D5DWithValue = expander3DWithValue[Position2D, Position3D, Position4D] _

  val expand3D4DWithValue = expander1DWithValue[Position3D] _
  val expand3D5DWithValue = expander2DWithValue[Position3D, Position4D] _
}

class TestScaldingMatrixExpand extends TestMatrixExpand with TBddDsl {

  "A Matrix.expand" should "return its 1D expanded data in 1D" in {
    Given {
      data1
    } When {
      cells: TypedPipe[Cell[Position1D]] =>
        cells.expand(TestMatrixExpand.expand1D2D)
    } Then {
      _.toList.sortBy(_.position) shouldBe result1
    }
  }

  it should "return its 2D expanded data in 1D" in {
    Given {
      data1
    } When {
      cells: TypedPipe[Cell[Position1D]] =>
        cells.expand(TestMatrixExpand.expand1D3D)
    } Then {
      _.toList.sortBy(_.position) shouldBe result2
    }
  }

  it should "return its 3D expanded data in 1D" in {
    Given {
      data1
    } When {
      cells: TypedPipe[Cell[Position1D]] =>
        cells.expand(TestMatrixExpand.expand1D4D)
    } Then {
      _.toList.sortBy(_.position) shouldBe result3
    }
  }

  it should "return its 4D expanded data in 1D" in {
    Given {
      data1
    } When {
      cells: TypedPipe[Cell[Position1D]] =>
        cells.expand(TestMatrixExpand.expand1D5D)
    } Then {
      _.toList.sortBy(_.position) shouldBe result4
    }
  }

  it should "return its 1D expanded data in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.expand(TestMatrixExpand.expand2D3D)
    } Then {
      _.toList.sortBy(_.position) shouldBe result5
    }
  }

  it should "return its 2D expanded data in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.expand(TestMatrixExpand.expand2D4D)
    } Then {
      _.toList.sortBy(_.position) shouldBe result6
    }
  }

  it should "return its 3D expanded data in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.expand(TestMatrixExpand.expand2D5D)
    } Then {
      _.toList.sortBy(_.position) shouldBe result7
    }
  }

  it should "return its 1D expanded data in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.expand(TestMatrixExpand.expand3D4D)
    } Then {
      _.toList.sortBy(_.position) shouldBe result8
    }
  }

  it should "return its 2D expanded data in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.expand(TestMatrixExpand.expand3D5D)
    } Then {
      _.toList.sortBy(_.position) shouldBe result9
    }
  }

  "A Matrix.expandWithValue" should "return its 1D expanded data in 1D" in {
    Given {
      data1
    } When {
      cells: TypedPipe[Cell[Position1D]] =>
        cells.expandWithValue(TestMatrixExpand.expand1D2DWithValue, ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result10
    }
  }

  it should "return its 2D expanded data in 1D" in {
    Given {
      data1
    } When {
      cells: TypedPipe[Cell[Position1D]] =>
        cells.expandWithValue(TestMatrixExpand.expand1D3DWithValue, ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result11
    }
  }

  it should "return its 3D expanded data in 1D" in {
    Given {
      data1
    } When {
      cells: TypedPipe[Cell[Position1D]] =>
        cells.expandWithValue(TestMatrixExpand.expand1D4DWithValue, ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result12
    }
  }

  it should "return its 4D expanded data in 1D" in {
    Given {
      data1
    } When {
      cells: TypedPipe[Cell[Position1D]] =>
        cells.expandWithValue(TestMatrixExpand.expand1D5DWithValue, ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result13
    }
  }

  it should "return its 1D expanded data in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.expandWithValue(TestMatrixExpand.expand2D3DWithValue, ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result14
    }
  }

  it should "return its 2D expanded data in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.expandWithValue(TestMatrixExpand.expand2D4DWithValue, ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result15
    }
  }

  it should "return its 3D expanded data in 2D" in {
    Given {
      data2
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.expandWithValue(TestMatrixExpand.expand2D5DWithValue, ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result16
    }
  }

  it should "return its 1D expanded data in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.expandWithValue(TestMatrixExpand.expand3D4DWithValue, ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result17
    }
  }

  it should "return its 2D expanded data in 3D" in {
    Given {
      data3
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.expandWithValue(TestMatrixExpand.expand3D5DWithValue, ValuePipe(ext))
    } Then {
      _.toList.sortBy(_.position) shouldBe result18
    }
  }
}

class TestSparkMatrixExpand extends TestMatrixExpand {

  "A Matrix.expand" should "return its 1D expanded data in 1D" in {
    toRDD(data1)
      .expand(TestMatrixExpand.expand1D2D)
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its 2D expanded data in 1D" in {
    toRDD(data1)
      .expand(TestMatrixExpand.expand1D3D)
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its 3D expanded data in 1D" in {
    toRDD(data1)
      .expand(TestMatrixExpand.expand1D4D)
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its 4D expanded data in 1D" in {
    toRDD(data1)
      .expand(TestMatrixExpand.expand1D5D)
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its expanded 1D data in 2D" in {
    toRDD(data2)
      .expand(TestMatrixExpand.expand2D3D)
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its expanded 2D data in 2D" in {
    toRDD(data2)
      .expand(TestMatrixExpand.expand2D4D)
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its expanded 3D data in 2D" in {
    toRDD(data2)
      .expand(TestMatrixExpand.expand2D5D)
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its expanded 1D data in 3D" in {
    toRDD(data3)
      .expand(TestMatrixExpand.expand3D4D)
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its expanded 2D data in 3D" in {
    toRDD(data3)
      .expand(TestMatrixExpand.expand3D5D)
      .toList.sortBy(_.position) shouldBe result9
  }

  "A Matrix.expandWithValue" should "return its 1D expanded data in 1D" in {
    toRDD(data1)
      .expandWithValue(TestMatrixExpand.expand1D2DWithValue, ext)
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its 2D expanded data in 1D" in {
    toRDD(data1)
      .expandWithValue(TestMatrixExpand.expand1D3DWithValue, ext)
      .toList.sortBy(_.position) shouldBe result11
  }

  it should "return its 3D expanded data in 1D" in {
    toRDD(data1)
      .expandWithValue(TestMatrixExpand.expand1D4DWithValue, ext)
      .toList.sortBy(_.position) shouldBe result12
  }

  it should "return its 4D expanded data in 1D" in {
    toRDD(data1)
      .expandWithValue(TestMatrixExpand.expand1D5DWithValue, ext)
      .toList.sortBy(_.position) shouldBe result13
  }

  it should "return its 1D expanded data in 2D" in {
    toRDD(data2)
      .expandWithValue(TestMatrixExpand.expand2D3DWithValue, ext)
      .toList.sortBy(_.position) shouldBe result14
  }

  it should "return its 2D expanded data in 2D" in {
    toRDD(data2)
      .expandWithValue(TestMatrixExpand.expand2D4DWithValue, ext)
      .toList.sortBy(_.position) shouldBe result15
  }

  it should "return its 3D expanded data in 2D" in {
    toRDD(data2)
      .expandWithValue(TestMatrixExpand.expand2D5DWithValue, ext)
      .toList.sortBy(_.position) shouldBe result16
  }

  it should "return its 1D expanded data in 3D" in {
    toRDD(data3)
      .expandWithValue(TestMatrixExpand.expand3D4DWithValue, ext)
      .toList.sortBy(_.position) shouldBe result17
  }

  it should "return its 2D expanded data in 3D" in {
    toRDD(data3)
      .expandWithValue(TestMatrixExpand.expand3D5DWithValue, ext)
      .toList.sortBy(_.position) shouldBe result18
  }
}

trait TestMatrixPermute extends TestMatrix {

  val dataA = List(Cell(Position2D(1, 3), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
    Cell(Position2D(2, 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position2D(3, 1), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)))

  val dataB = List(Cell(Position3D(1, 2, 3), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
    Cell(Position3D(2, 2, 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position3D(3, 2, 1), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)))

  val dataC = List(Cell(Position4D(1, 2, 3, 4), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
    Cell(Position4D(2, 2, 2, 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position4D(1, 1, 4, 4), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
    Cell(Position4D(4, 1, 3, 2), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)))

  val dataD = List(Cell(Position5D(1, 2, 3, 4, 5), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
    Cell(Position5D(2, 2, 2, 2, 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position5D(1, 1, 3, 5, 5), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
    Cell(Position5D(4, 4, 4, 1, 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position5D(5, 4, 3, 2, 1), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)))

  val result1 = List(Cell(Position2D(1, 3), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
    Cell(Position2D(2, 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position2D(3, 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)))

  val result2 = List(Cell(Position3D(2, 1, 3), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)),
    Cell(Position3D(2, 2, 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position3D(2, 3, 1), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)))

  val result3 = List(Cell(Position4D(2, 2, 2, 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position4D(2, 3, 4, 1), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position4D(4, 3, 1, 2), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
    Cell(Position4D(4, 4, 1, 1), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)))

  val result4 = List(Cell(Position5D(1, 4, 4, 1, 4), Content(ContinuousSchema[Codex.DoubleCodex](), 12.56)),
    Cell(Position5D(2, 2, 2, 2, 2), Content(ContinuousSchema[Codex.DoubleCodex](), 6.28)),
    Cell(Position5D(2, 4, 5, 1, 3), Content(ContinuousSchema[Codex.DoubleCodex](), 18.84)),
    Cell(Position5D(4, 2, 1, 5, 3), Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)),
    Cell(Position5D(5, 1, 1, 5, 3), Content(ContinuousSchema[Codex.DoubleCodex](), 9.42)))
}

class TestScaldingMatrixPermute extends TestMatrixPermute with TBddDsl {

  "A Matrix.permute" should "return its permutation in 2D" in {
    Given {
      dataA
    } When {
      cells: TypedPipe[Cell[Position2D]] =>
        cells.permute(Second, First)
    } Then {
      _.toList.sortBy(_.position) shouldBe result1
    }
  }

  it should "return its permutation in 3D" in {
    Given {
      dataB
    } When {
      cells: TypedPipe[Cell[Position3D]] =>
        cells.permute(Second, Third, First)
    } Then {
      _.toList.sortBy(_.position) shouldBe result2
    }
  }

  it should "return its permutation in 4D" in {
    Given {
      dataC
    } When {
      cells: TypedPipe[Cell[Position4D]] =>
        cells.permute(Fourth, Third, First, Second)
    } Then {
      _.toList.sortBy(_.position) shouldBe result3
    }
  }

  it should "return its permutation in 5D" in {
    Given {
      dataD
    } When {
      cells: TypedPipe[Cell[Position5D]] =>
        cells.permute(Fourth, Second, First, Fifth, Third)
    } Then {
      _.toList.sortBy(_.position) shouldBe result4
    }
  }
}

class TestSparkMatrixPermute extends TestMatrixPermute {

  "A Matrix.permute" should "return its permutation in 2D" in {
    toRDD(dataA)
      .permute(Second, First)
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its permutation in 3D" in {
    toRDD(dataB)
      .permute(Second, Third, First)
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its permutation in 4D" in {
    toRDD(dataC)
      .permute(Fourth, Third, First, Second)
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its permutation in 5D" in {
    toRDD(dataD)
      .permute(Fourth, Second, First, Fifth, Third)
      .toList.sortBy(_.position) shouldBe result4
  }
}

