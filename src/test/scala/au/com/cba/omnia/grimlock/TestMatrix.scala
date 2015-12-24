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
import au.com.cba.omnia.grimlock.framework.pairwise._
import au.com.cba.omnia.grimlock.framework.partition._
import au.com.cba.omnia.grimlock.framework.position._
import au.com.cba.omnia.grimlock.framework.position.Positionable._
import au.com.cba.omnia.grimlock.framework.sample._
import au.com.cba.omnia.grimlock.framework.squash._
import au.com.cba.omnia.grimlock.framework.Type._
import au.com.cba.omnia.grimlock.framework.window._

import au.com.cba.omnia.grimlock.library.aggregate._
import au.com.cba.omnia.grimlock.library.pairwise._
import au.com.cba.omnia.grimlock.library.squash._
import au.com.cba.omnia.grimlock.library.transform._

import au.com.cba.omnia.grimlock.scalding._
import au.com.cba.omnia.grimlock.scalding.Matrix._
import au.com.cba.omnia.grimlock.scalding.Matrixable._
import au.com.cba.omnia.grimlock.scalding.position.PositionDistributable._
import au.com.cba.omnia.grimlock.scalding.position.Positions._

import au.com.cba.omnia.grimlock.spark.Matrix._
import au.com.cba.omnia.grimlock.spark.Matrixable._
import au.com.cba.omnia.grimlock.spark.position.PositionDistributable._
import au.com.cba.omnia.grimlock.spark.position.Positions._

import com.twitter.scalding.typed.{ TypedPipe, ValuePipe }

trait TestMatrix extends TestGrimlock {

  val data1 = List(Cell(Position1D("foo"), Content(OrdinalSchema(StringCodex), "3.14")),
    Cell(Position1D("bar"), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position1D("baz"), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position1D("qux"), Content(OrdinalSchema(StringCodex), "12.56")))

  val data2 = List(Cell(Position2D("foo", 1), Content(OrdinalSchema(StringCodex), "3.14")),
    Cell(Position2D("bar", 1), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position2D("baz", 1), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position2D("qux", 1), Content(OrdinalSchema(StringCodex), "12.56")),
    Cell(Position2D("foo", 2), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position2D("bar", 2), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position2D("baz", 2), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position2D("foo", 3), Content(NominalSchema(StringCodex), "9.42")),
    Cell(Position2D("bar", 3), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position2D("foo", 4), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))

  val data3 = List(Cell(Position3D("foo", 1, "xyz"), Content(OrdinalSchema(StringCodex), "3.14")),
    Cell(Position3D("bar", 1, "xyz"), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position3D("baz", 1, "xyz"), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position3D("qux", 1, "xyz"), Content(OrdinalSchema(StringCodex), "12.56")),
    Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("baz", 2, "xyz"), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position3D("foo", 3, "xyz"), Content(NominalSchema(StringCodex), "9.42")),
    Cell(Position3D("bar", 3, "xyz"), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position3D("foo", 4, "xyz"), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))

  val num1 = List(Cell(Position1D("foo"), Content(ContinuousSchema(DoubleCodex), 3.14)),
    Cell(Position1D("bar"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position1D("baz"), Content(ContinuousSchema(DoubleCodex), 9.42)),
    Cell(Position1D("qux"), Content(ContinuousSchema(DoubleCodex), 12.56)))

  val num2 = List(Cell(Position2D("foo", 1), Content(ContinuousSchema(DoubleCodex), 3.14)),
    Cell(Position2D("bar", 1), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position2D("baz", 1), Content(ContinuousSchema(DoubleCodex), 9.42)),
    Cell(Position2D("qux", 1), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position2D("foo", 2), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position2D("bar", 2), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position2D("baz", 2), Content(ContinuousSchema(DoubleCodex), 18.84)),
    Cell(Position2D("foo", 3), Content(ContinuousSchema(DoubleCodex), 9.42)),
    Cell(Position2D("bar", 3), Content(ContinuousSchema(DoubleCodex), 18.84)),
    Cell(Position2D("foo", 4), Content(ContinuousSchema(DoubleCodex), 12.56)))

  val num3 = List(Cell(Position3D("foo", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 3.14)),
    Cell(Position3D("bar", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position3D("baz", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42)),
    Cell(Position3D("qux", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("baz", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 18.84)),
    Cell(Position3D("foo", 3, "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42)),
    Cell(Position3D("bar", 3, "xyz"), Content(ContinuousSchema(DoubleCodex), 18.84)),
    Cell(Position3D("foo", 4, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)))
}

class TestScaldingMatrixNames extends TestMatrix {

  "A Matrix.names" should "return its first over names in 1D" in {
    toPipe(data1)
      .names(Over(First), Default())
      .toList.sorted shouldBe List(Position1D("bar"), Position1D("baz"), Position1D("foo"), Position1D("qux"))
  }

  it should "return its first over names in 2D" in {
    toPipe(data2)
      .names(Over(First), Default(Reducers(123)))
      .toList.sorted shouldBe List(Position1D("bar"), Position1D("baz"), Position1D("foo"), Position1D("qux"))
  }

  it should "return its first along names in 2D" in {
    toPipe(data2)
      .names(Along(First), Default())
      .toList.sorted shouldBe List(Position1D(1), Position1D(2), Position1D(3), Position1D(4))
  }

  it should "return its second over names in 2D" in {
    toPipe(data2)
      .names(Over(Second), Default(Reducers(123)))
      .toList.sorted shouldBe List(Position1D(1), Position1D(2), Position1D(3), Position1D(4))
  }

  it should "return its second along names in 2D" in {
    toPipe(data2)
      .names(Along(Second), Default())
      .toList.sorted shouldBe List(Position1D("bar"), Position1D("baz"), Position1D("foo"), Position1D("qux"))
  }

  it should "return its first over names in 3D" in {
    toPipe(data3)
      .names(Over(First), Default(Reducers(123)))
      .toList.sorted shouldBe List(Position1D("bar"), Position1D("baz"), Position1D("foo"), Position1D("qux"))
  }

  it should "return its first along names in 3D" in {
    toPipe(data3)
      .names(Along(First), Default())
      .toList.sorted shouldBe List(Position2D(1, "xyz"), Position2D(2, "xyz"), Position2D(3, "xyz"),
        Position2D(4, "xyz"))
  }

  it should "return its second over names in 3D" in {
    toPipe(data3)
      .names(Over(Second), Default(Reducers(123)))
      .toList.sorted shouldBe List(Position1D(1), Position1D(2), Position1D(3), Position1D(4))
  }

  it should "return its second along names in 3D" in {
    toPipe(data3)
      .names(Along(Second), Default())
      .toList.sorted shouldBe List(Position2D("bar", "xyz"), Position2D("baz", "xyz"), Position2D("foo", "xyz"),
        Position2D("qux", "xyz"))
  }

  it should "return its third over names in 3D" in {
    toPipe(data3)
      .names(Over(Third), Default(Reducers(123)))
      .toList.sorted shouldBe List(Position1D("xyz"))
  }

  it should "return its third along names in 3D" in {
    toPipe(data3)
      .names(Along(Third), Default())
      .toList.sorted shouldBe List(Position2D("bar", 1), Position2D("bar", 2), Position2D("bar", 3),
        Position2D("baz", 1), Position2D("baz", 2), Position2D("foo", 1), Position2D("foo", 2), Position2D("foo", 3),
        Position2D("foo", 4), Position2D("qux", 1))
  }
}

class TestSparkMatrixNames extends TestMatrix {

  "A Matrix.names" should "return its first over names in 1D" in {
    toRDD(data1)
      .names(Over(First), Default())
      .toList.sorted should be (List(Position1D("bar"), Position1D("baz"), Position1D("foo"), Position1D("qux")))
  }

  it should "return its first over names in 2D" in {
    toRDD(data2)
      .names(Over(First), Default(Reducers(12)))
      .toList.sorted should be (List(Position1D("bar"), Position1D("baz"), Position1D("foo"), Position1D("qux")))
  }

  it should "return its first along names in 2D" in {
    toRDD(data2)
      .names(Along(First), Default())
      .toList.sorted should be (List(Position1D(1), Position1D(2), Position1D(3), Position1D(4)))
  }

  it should "return its second over names in 2D" in {
    toRDD(data2)
      .names(Over(Second), Default(Reducers(12)))
      .toList.sorted should be (List(Position1D(1), Position1D(2), Position1D(3), Position1D(4)))
  }

  it should "return its second along names in 2D" in {
    toRDD(data2)
      .names(Along(Second), Default())
      .toList.sorted should be (List(Position1D("bar"), Position1D("baz"), Position1D("foo"), Position1D("qux")))
  }

  it should "return its first over names in 3D" in {
    toRDD(data3)
      .names(Over(First), Default(Reducers(12)))
      .toList.sorted should be (List(Position1D("bar"), Position1D("baz"), Position1D("foo"), Position1D("qux")))
  }

  it should "return its first along names in 3D" in {
    toRDD(data3)
      .names(Along(First), Default())
      .toList.sorted should be (List(Position2D(1, "xyz"), Position2D(2, "xyz"), Position2D(3, "xyz"),
        Position2D(4, "xyz")))
  }

  it should "return its second over names in 3D" in {
    toRDD(data3)
      .names(Over(Second), Default(Reducers(12)))
      .toList.sorted should be (List(Position1D(1), Position1D(2), Position1D(3), Position1D(4)))
  }

  it should "return its second along names in 3D" in {
    toRDD(data3)
      .names(Along(Second), Default())
      .toList.sorted should be (List(Position2D("bar", "xyz"), Position2D("baz", "xyz"), Position2D("foo", "xyz"),
        Position2D("qux", "xyz")))
  }

  it should "return its third over names in 3D" in {
    toRDD(data3)
      .names(Over(Third), Default(Reducers(12)))
      .toList.sorted should be (List(Position1D("xyz")))
  }

  it should "return its third along names in 3D" in {
    toRDD(data3)
      .names(Along(Third), Default())
      .toList.sorted should be (List(Position2D("bar", 1), Position2D("bar", 2), Position2D("bar", 3),
        Position2D("baz", 1), Position2D("baz", 2), Position2D("foo", 1), Position2D("foo", 2), Position2D("foo", 3),
        Position2D("foo", 4), Position2D("qux", 1)))
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

class TestScaldingMatrixTypes extends TestMatrixTypes {

  "A Matrix.types" should "return its first over types in 1D" in {
    toPipe(data1)
      .types(Over(First), false, Default())
      .toList.sortBy(_._1) shouldBe result1
  }

  it should "return its first over specific types in 1D" in {
    toPipe(data1)
      .types(Over(First), true, Default(Reducers(123)))
      .toList.sortBy(_._1) shouldBe result2
  }

  it should "return its first over types in 2D" in {
    toPipe(data2)
      .types(Over(First), false, Default())
      .toList.sortBy(_._1) shouldBe result3
  }

  it should "return its first over specific types in 2D" in {
    toPipe(data2)
      .types(Over(First), true, Default(Reducers(123)))
      .toList.sortBy(_._1) shouldBe result4
  }

  it should "return its first along types in 2D" in {
    toPipe(data2)
      .types(Along(First), false, Default())
      .toList.sortBy(_._1) shouldBe result5
  }

  it should "return its first along specific types in 2D" in {
    toPipe(data2)
      .types(Along(First), true, Default(Reducers(123)))
      .toList.sortBy(_._1) shouldBe result6
  }

  it should "return its second over types in 2D" in {
    toPipe(data2)
      .types(Over(Second), false, Default())
      .toList.sortBy(_._1) shouldBe result7
  }

  it should "return its second over specific types in 2D" in {
    toPipe(data2)
      .types(Over(Second), true, Default(Reducers(123)))
      .toList.sortBy(_._1) shouldBe result8
  }

  it should "return its second along types in 2D" in {
    toPipe(data2)
      .types(Along(Second), false, Default())
      .toList.sortBy(_._1) shouldBe result9
  }

  it should "return its second along specific types in 2D" in {
    toPipe(data2)
      .types(Along(Second), true, Default(Reducers(123)))
      .toList.sortBy(_._1) shouldBe result10
  }

  it should "return its first over types in 3D" in {
    toPipe(data3)
      .types(Over(First), false, Default())
      .toList.sortBy(_._1) shouldBe result11
  }

  it should "return its first over specific types in 3D" in {
    toPipe(data3)
      .types(Over(First), true, Default(Reducers(123)))
      .toList.sortBy(_._1) shouldBe result12
  }

  it should "return its first along types in 3D" in {
    toPipe(data3)
      .types(Along(First), false, Default())
      .toList.sortBy(_._1) shouldBe result13
  }

  it should "return its first along specific types in 3D" in {
    toPipe(data3)
      .types(Along(First), true, Default(Reducers(123)))
      .toList.sortBy(_._1) shouldBe result14
  }

  it should "return its second over types in 3D" in {
    toPipe(data3)
      .types(Over(Second), false, Default())
      .toList.sortBy(_._1) shouldBe result15
  }

  it should "return its second over specific types in 3D" in {
    toPipe(data3)
      .types(Over(Second), true, Default(Reducers(123)))
      .toList.sortBy(_._1) shouldBe result16
  }

  it should "return its second along types in 3D" in {
    toPipe(data3)
      .types(Along(Second), false, Default())
      .toList.sortBy(_._1) shouldBe result17
  }

  it should "return its second along specific types in 3D" in {
    toPipe(data3)
      .types(Along(Second), true, Default(Reducers(123)))
      .toList.sortBy(_._1) shouldBe result18
  }

  it should "return its third over types in 3D" in {
    toPipe(data3)
      .types(Over(Third), false, Default())
      .toList.sortBy(_._1) shouldBe result19
  }

  it should "return its third over specific types in 3D" in {
    toPipe(data3)
      .types(Over(Third), true, Default(Reducers(123)))
      .toList.sortBy(_._1) shouldBe result20
  }

  it should "return its third along types in 3D" in {
    toPipe(data3)
      .types(Along(Third), false, Default())
      .toList.sortBy(_._1) shouldBe result21
  }

  it should "return its third along specific types in 3D" in {
    toPipe(data3)
      .types(Along(Third), true, Default(Reducers(123)))
      .toList.sortBy(_._1) shouldBe result22
  }
}

class TestSparkMatrixTypes extends TestMatrixTypes {

  "A Matrix.types" should "return its first over types in 1D" in {
    toRDD(data1)
      .types(Over(First), false, Default())
      .toList.sortBy(_._1) shouldBe result1
  }

  it should "return its first over specific types in 1D" in {
    toRDD(data1)
      .types(Over(First), true, Default(Reducers(12)))
      .toList.sortBy(_._1) shouldBe result2
  }

  it should "return its first over types in 2D" in {
    toRDD(data2)
      .types(Over(First), false, Default())
      .toList.sortBy(_._1) shouldBe result3
  }

  it should "return its first over specific types in 2D" in {
    toRDD(data2)
      .types(Over(First), true, Default(Reducers(12)))
      .toList.sortBy(_._1) shouldBe result4
  }

  it should "return its first along types in 2D" in {
    toRDD(data2)
      .types(Along(First), false, Default())
      .toList.sortBy(_._1) shouldBe result5
  }

  it should "return its first along specific types in 2D" in {
    toRDD(data2)
      .types(Along(First), true, Default(Reducers(12)))
      .toList.sortBy(_._1) shouldBe result6
  }

  it should "return its second over types in 2D" in {
    toRDD(data2)
      .types(Over(Second), false, Default())
      .toList.sortBy(_._1) shouldBe result7
  }

  it should "return its second over specific types in 2D" in {
    toRDD(data2)
      .types(Over(Second), true, Default(Reducers(12)))
      .toList.sortBy(_._1) shouldBe result8
  }

  it should "return its second along types in 2D" in {
    toRDD(data2)
      .types(Along(Second), false, Default())
      .toList.sortBy(_._1) shouldBe result9
  }

  it should "return its second along specific types in 2D" in {
    toRDD(data2)
      .types(Along(Second), true, Default(Reducers(12)))
      .toList.sortBy(_._1) shouldBe result10
  }

  it should "return its first over types in 3D" in {
    toRDD(data3)
      .types(Over(First), false, Default())
      .toList.sortBy(_._1) shouldBe result11
  }

  it should "return its first over specific types in 3D" in {
    toRDD(data3)
      .types(Over(First), true, Default(Reducers(12)))
      .toList.sortBy(_._1) shouldBe result12
  }

  it should "return its first along types in 3D" in {
    toRDD(data3)
      .types(Along(First), false, Default())
      .toList.sortBy(_._1) shouldBe result13
  }

  it should "return its first along specific types in 3D" in {
    toRDD(data3)
      .types(Along(First), true, Default(Reducers(12)))
      .toList.sortBy(_._1) shouldBe result14
  }

  it should "return its second over types in 3D" in {
    toRDD(data3)
      .types(Over(Second), false, Default())
      .toList.sortBy(_._1) shouldBe result15
  }

  it should "return its second over specific types in 3D" in {
    toRDD(data3)
      .types(Over(Second), true, Default(Reducers(12)))
      .toList.sortBy(_._1) shouldBe result16
  }

  it should "return its second along types in 3D" in {
    toRDD(data3)
      .types(Along(Second), false, Default())
      .toList.sortBy(_._1) shouldBe result17
  }

  it should "return its second along specific types in 3D" in {
    toRDD(data3)
      .types(Along(Second), true, Default(Reducers(12)))
      .toList.sortBy(_._1) shouldBe result18
  }

  it should "return its third over types in 3D" in {
    toRDD(data3)
      .types(Over(Third), false, Default())
      .toList.sortBy(_._1) shouldBe result19
  }

  it should "return its third over specific types in 3D" in {
    toRDD(data3)
      .types(Over(Third), true, Default(Reducers(12)))
      .toList.sortBy(_._1) shouldBe result20
  }

  it should "return its third along types in 3D" in {
    toRDD(data3)
      .types(Along(Third), false, Default())
      .toList.sortBy(_._1) shouldBe result21
  }

  it should "return its third along specific types in 3D" in {
    toRDD(data3)
      .types(Along(Third), true, Default(Reducers(12)))
      .toList.sortBy(_._1) shouldBe result22
  }
}

trait TestMatrixSize extends TestMatrix {

  val dataA = List(Cell(Position2D(1, 1), Content(OrdinalSchema(StringCodex), "a")),
    Cell(Position2D(2, 2), Content(OrdinalSchema(StringCodex), "b")),
    Cell(Position2D(3, 3), Content(OrdinalSchema(StringCodex), "c")))

  val result1 = List(Cell(Position1D("First"), Content(DiscreteSchema(LongCodex), 4)))

  val result2 = List(Cell(Position1D("First"), Content(DiscreteSchema(LongCodex), 4)))

  val result3 = List(Cell(Position1D("First"), Content(DiscreteSchema(LongCodex), 4)))

  val result4 = List(Cell(Position1D("First"), Content(DiscreteSchema(LongCodex), data2.length)))

  val result5 = List(Cell(Position1D("Second"), Content(DiscreteSchema(LongCodex), 4)))

  val result6 = List(Cell(Position1D("Second"), Content(DiscreteSchema(LongCodex), data2.length)))

  val result7 = List(Cell(Position1D("First"), Content(DiscreteSchema(LongCodex), 4)))

  val result8 = List(Cell(Position1D("First"), Content(DiscreteSchema(LongCodex), data3.length)))

  val result9 = List(Cell(Position1D("Second"), Content(DiscreteSchema(LongCodex), 4)))

  val result10 = List(Cell(Position1D("Second"), Content(DiscreteSchema(LongCodex), data3.length)))

  val result11 = List(Cell(Position1D("Third"), Content(DiscreteSchema(LongCodex), 1)))

  val result12 = List(Cell(Position1D("Third"), Content(DiscreteSchema(LongCodex), data3.length)))

  val result13 = List(Cell(Position1D("Second"), Content(DiscreteSchema(LongCodex), 3)))
}

class TestScaldingMatrixSize extends TestMatrixSize {

  "A Matrix.size" should "return its first size in 1D" in {
    toPipe(data1)
      .size(First, false, Default())
      .toList shouldBe result1
  }

  it should "return its first distinct size in 1D" in {
    toPipe(data1)
      .size(First, true, Default(Reducers(123)))
      .toList shouldBe result2
  }

  it should "return its first size in 2D" in {
    toPipe(data2)
      .size(First, false, Default())
      .toList shouldBe result3
  }

  it should "return its first distinct size in 2D" in {
    toPipe(data2)
      .size(First, true, Default(Reducers(123)))
      .toList shouldBe result4
  }

  it should "return its second size in 2D" in {
    toPipe(data2)
      .size(Second, false, Default())
      .toList shouldBe result5
  }

  it should "return its second distinct size in 2D" in {
    toPipe(data2)
      .size(Second, true, Default(Reducers(123)))
      .toList shouldBe result6
  }

  it should "return its first size in 3D" in {
    toPipe(data3)
      .size(First, false, Default())
      .toList shouldBe result7
  }

  it should "return its first distinct size in 3D" in {
    toPipe(data3)
      .size(First, true, Default(Reducers(123)))
      .toList shouldBe result8
  }

  it should "return its second size in 3D" in {
    toPipe(data3)
      .size(Second, false, Default())
      .toList shouldBe result9
  }

  it should "return its second distinct size in 3D" in {
    toPipe(data3)
      .size(Second, true, Default(Reducers(123)))
      .toList shouldBe result10
  }

  it should "return its third size in 3D" in {
    toPipe(data3)
      .size(Third, false, Default())
      .toList shouldBe result11
  }

  it should "return its third distinct size in 3D" in {
    toPipe(data3)
      .size(Third, true, Default(Reducers(123)))
      .toList shouldBe result12
  }

  it should "return its distinct size" in {
    toPipe(dataA)
      .size(Second, true, Default())
      .toList shouldBe result13
  }
}

class TestSparkMatrixSize extends TestMatrixSize {

  "A Matrix.size" should "return its first size in 1D" in {
    toRDD(data1)
      .size(First, false, Default())
      .toList shouldBe result1
  }

  it should "return its first distinct size in 1D" in {
    toRDD(data1)
      .size(First, true, Default(Reducers(12)))
      .toList shouldBe result2
  }

  it should "return its first size in 2D" in {
    toRDD(data2)
      .size(First, false, Default())
      .toList shouldBe result3
  }

  it should "return its first distinct size in 2D" in {
    toRDD(data2)
      .size(First, true, Default(Reducers(12)))
      .toList shouldBe result4
  }

  it should "return its second size in 2D" in {
    toRDD(data2)
      .size(Second, false, Default())
      .toList shouldBe result5
  }

  it should "return its second distinct size in 2D" in {
    toRDD(data2)
      .size(Second, true, Default(Reducers(12)))
      .toList shouldBe result6
  }

  it should "return its first size in 3D" in {
    toRDD(data3)
      .size(First, false, Default())
      .toList shouldBe result7
  }

  it should "return its first distinct size in 3D" in {
    toRDD(data3)
      .size(First, true, Default(Reducers(12)))
      .toList shouldBe result8
  }

  it should "return its second size in 3D" in {
    toRDD(data3)
      .size(Second, false, Default())
      .toList shouldBe result9
  }

  it should "return its second distinct size in 3D" in {
    toRDD(data3)
      .size(Second, true, Default(Reducers(12)))
      .toList shouldBe result10
  }

  it should "return its third size in 3D" in {
    toRDD(data3)
      .size(Third, false, Default())
      .toList shouldBe result11
  }

  it should "return its third distinct size in 3D" in {
    toRDD(data3)
      .size(Third, true, Default(Reducers(12)))
      .toList shouldBe result12
  }

  it should "return its distinct size" in {
    toRDD(dataA)
      .size(Second, true, Default())
      .toList shouldBe result13
  }
}

trait TestMatrixShape extends TestMatrix {

  val result1 = List(Cell(Position1D("First"), Content(DiscreteSchema(LongCodex), 4)))

  val result2 = List(Cell(Position1D("First"), Content(DiscreteSchema(LongCodex), 4)),
    Cell(Position1D("Second"), Content(DiscreteSchema(LongCodex), 4)))

  val result3 = List(Cell(Position1D("First"), Content(DiscreteSchema(LongCodex), 4)),
    Cell(Position1D("Second"), Content(DiscreteSchema(LongCodex), 4)),
    Cell(Position1D("Third"), Content(DiscreteSchema(LongCodex), 1)))
}

class TestScaldingMatrixShape extends TestMatrixShape {

  "A Matrix.shape" should "return its shape in 1D" in {
    toPipe(data1)
      .shape(Default())
      .toList shouldBe result1
  }

  it should "return its shape in 2D" in {
    toPipe(data2)
      .shape(Default(Reducers(123)))
      .toList shouldBe result2
  }

  it should "return its shape in 3D" in {
    toPipe(data3)
      .shape(Default())
      .toList shouldBe result3
  }
}

class TestSparkMatrixShape extends TestMatrixShape {

  "A Matrix.shape" should "return its shape in 1D" in {
    toRDD(data1)
      .shape(Default())
      .toList shouldBe result1
  }

  it should "return its shape in 2D" in {
    toRDD(data2)
      .shape(Default(Reducers(12)))
      .toList shouldBe result2
  }

  it should "return its shape in 3D" in {
    toRDD(data3)
      .shape(Default())
      .toList shouldBe result3
  }
}

trait TestMatrixSlice extends TestMatrix {

  val result1 = List(Cell(Position1D("baz"), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position1D("foo"), Content(OrdinalSchema(StringCodex), "3.14")))

  val result2 = List(Cell(Position1D("bar"), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position1D("qux"), Content(OrdinalSchema(StringCodex), "12.56")))

  val result3 = List(Cell(Position2D("baz", 1), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position2D("baz", 2), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position2D("foo", 1), Content(OrdinalSchema(StringCodex), "3.14")),
    Cell(Position2D("foo", 2), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position2D("foo", 3), Content(NominalSchema(StringCodex), "9.42")),
    Cell(Position2D("foo", 4), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))

  val result4 = List(Cell(Position2D("bar", 1), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position2D("bar", 2), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position2D("bar", 3), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position2D("qux", 1), Content(OrdinalSchema(StringCodex), "12.56")))

  val result5 = List(Cell(Position2D("bar", 2), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position2D("baz", 2), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position2D("foo", 2), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position2D("foo", 4), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))

  val result6 = List(Cell(Position2D("bar", 1), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position2D("bar", 3), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position2D("baz", 1), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position2D("foo", 1), Content(OrdinalSchema(StringCodex), "3.14")),
    Cell(Position2D("foo", 3), Content(NominalSchema(StringCodex), "9.42")),
    Cell(Position2D("qux", 1), Content(OrdinalSchema(StringCodex), "12.56")))

  val result7 = List(Cell(Position2D("bar", 2), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position2D("baz", 2), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position2D("foo", 2), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position2D("foo", 4), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))

  val result8 = List(Cell(Position2D("bar", 1), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position2D("bar", 3), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position2D("baz", 1), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position2D("foo", 1), Content(OrdinalSchema(StringCodex), "3.14")),
    Cell(Position2D("foo", 3), Content(NominalSchema(StringCodex), "9.42")),
    Cell(Position2D("qux", 1), Content(OrdinalSchema(StringCodex), "12.56")))

  val result9 = List(Cell(Position2D("baz", 1), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position2D("baz", 2), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position2D("foo", 1), Content(OrdinalSchema(StringCodex), "3.14")),
    Cell(Position2D("foo", 2), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position2D("foo", 3), Content(NominalSchema(StringCodex), "9.42")),
    Cell(Position2D("foo", 4), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))

  val result10 = List(Cell(Position2D("bar", 1), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position2D("bar", 2), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position2D("bar", 3), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position2D("qux", 1), Content(OrdinalSchema(StringCodex), "12.56")))

  val result11 = List(Cell(Position3D("baz", 1, "xyz"), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position3D("baz", 2, "xyz"), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position3D("foo", 1, "xyz"), Content(OrdinalSchema(StringCodex), "3.14")),
    Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position3D("foo", 3, "xyz"), Content(NominalSchema(StringCodex), "9.42")),
    Cell(Position3D("foo", 4, "xyz"), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))

  val result12 = List(Cell(Position3D("bar", 1, "xyz"), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("bar", 3, "xyz"), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position3D("qux", 1, "xyz"), Content(OrdinalSchema(StringCodex), "12.56")))

  val result13 = List(Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("baz", 2, "xyz"), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position3D("foo", 4, "xyz"), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))

  val result14 = List(Cell(Position3D("bar", 1, "xyz"), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position3D("bar", 3, "xyz"), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position3D("baz", 1, "xyz"), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position3D("foo", 1, "xyz"), Content(OrdinalSchema(StringCodex), "3.14")),
    Cell(Position3D("foo", 3, "xyz"), Content(NominalSchema(StringCodex), "9.42")),
    Cell(Position3D("qux", 1, "xyz"), Content(OrdinalSchema(StringCodex), "12.56")))

  val result15 = List(Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("baz", 2, "xyz"), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position3D("foo", 4, "xyz"), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))

  val result16 = List(Cell(Position3D("bar", 1, "xyz"), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position3D("bar", 3, "xyz"), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position3D("baz", 1, "xyz"), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position3D("foo", 1, "xyz"), Content(OrdinalSchema(StringCodex), "3.14")),
    Cell(Position3D("foo", 3, "xyz"), Content(NominalSchema(StringCodex), "9.42")),
    Cell(Position3D("qux", 1, "xyz"), Content(OrdinalSchema(StringCodex), "12.56")))

  val result17 = List(Cell(Position3D("baz", 1, "xyz"), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position3D("baz", 2, "xyz"), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position3D("foo", 1, "xyz"), Content(OrdinalSchema(StringCodex), "3.14")),
    Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position3D("foo", 3, "xyz"), Content(NominalSchema(StringCodex), "9.42")),
    Cell(Position3D("foo", 4, "xyz"), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))

  val result18 = List(Cell(Position3D("bar", 1, "xyz"), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("bar", 3, "xyz"), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position3D("qux", 1, "xyz"), Content(OrdinalSchema(StringCodex), "12.56")))

  val result19 = List()

  val result20 = data3.sortBy(_.position)

  val result21 = List(Cell(Position3D("bar", 1, "xyz"), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("bar", 3, "xyz"), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position3D("baz", 2, "xyz"), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position3D("foo", 1, "xyz"), Content(OrdinalSchema(StringCodex), "3.14")),
    Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position3D("foo", 4, "xyz"), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
    Cell(Position3D("qux", 1, "xyz"), Content(OrdinalSchema(StringCodex), "12.56")))

  val result22 = List(Cell(Position3D("baz", 1, "xyz"), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position3D("foo", 3, "xyz"), Content(NominalSchema(StringCodex), "9.42")))
}

class TestScaldingMatrixSlice extends TestMatrixSlice {

  "A Matrix.slice" should "return its first over slice in 1D" in {
    toPipe(data1)
      .slice(Over(First), List("bar", "qux"), false, InMemory())
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first over inverse slice in 1D" in {
    toPipe(data1)
      .slice(Over(First), List("bar", "qux"), true, Default())
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its first over slice in 2D" in {
    toPipe(data2)
      .slice(Over(First), List("bar", "qux"), false, Default(Reducers(123)))
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its first over inverse slice in 2D" in {
    toPipe(data2)
      .slice(Over(First), List("bar", "qux"), true, Unbalanced(Reducers(123)))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its first along slice in 2D" in {
    toPipe(data2)
      .slice(Along(First), List(1, 3), false, InMemory())
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its first along inverse slice in 2D" in {
    toPipe(data2)
      .slice(Along(First), List(1, 3), true, Default())
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its second over slice in 2D" in {
    toPipe(data2)
      .slice(Over(Second), List(1, 3), false, Default(Reducers(123)))
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its second over inverse slice in 2D" in {
    toPipe(data2)
      .slice(Over(Second), List(1, 3), true, Unbalanced(Reducers(123)))
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its second along slice in 2D" in {
    toPipe(data2)
      .slice(Along(Second), List("bar", "qux"), false, InMemory())
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its second along inverse slice in 2D" in {
    toPipe(data2)
      .slice(Along(Second), List("bar", "qux"), true, Default())
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its first over slice in 3D" in {
    toPipe(data3)
      .slice(Over(First), List("bar", "qux"), false, Default(Reducers(123)))
      .toList.sortBy(_.position) shouldBe result11
  }

  it should "return its first over inverse slice in 3D" in {
    toPipe(data3)
      .slice(Over(First), List("bar", "qux"), true, Unbalanced(Reducers(123)))
      .toList.sortBy(_.position) shouldBe result12
  }

  it should "return its first along slice in 3D" in {
    toPipe(data3)
      .slice(Along(First), List(Position2D(1, "xyz"), Position2D(3, "xyz")), false, InMemory())
      .toList.sortBy(_.position) shouldBe result13
  }

  it should "return its first along inverse slice in 3D" in {
    toPipe(data3)
      .slice(Along(First), List(Position2D(1, "xyz"), Position2D(3, "xyz")), true, Default())
      .toList.sortBy(_.position) shouldBe result14
  }

  it should "return its second over slice in 3D" in {
    toPipe(data3)
      .slice(Over(Second), List(1, 3), false, Default(Reducers(123)))
      .toList.sortBy(_.position) shouldBe result15
  }

  it should "return its second over inverse slice in 3D" in {
    toPipe(data3)
      .slice(Over(Second), List(1, 3), true, Unbalanced(Reducers(123)))
      .toList.sortBy(_.position) shouldBe result16
  }

  it should "return its second along slice in 3D" in {
    toPipe(data3)
      .slice(Along(Second), List(Position2D("bar", "xyz"), Position2D("qux", "xyz")), false, InMemory())
      .toList.sortBy(_.position) shouldBe result17
  }

  it should "return its second along inverse slice in 3D" in {
    toPipe(data3)
      .slice(Along(Second), List(Position2D("bar", "xyz"), Position2D("qux", "xyz")), true, Default())
      .toList.sortBy(_.position) shouldBe result18
  }

  it should "return its third over slice in 3D" in {
    toPipe(data3)
      .slice(Over(Third), "xyz", false, Default(Reducers(123)))
      .toList.sortBy(_.position) shouldBe result19
  }

  it should "return its third over inverse slice in 3D" in {
    toPipe(data3)
      .slice(Over(Third), "xyz", true, Unbalanced(Reducers(123)))
      .toList.sortBy(_.position) shouldBe result20
  }

  it should "return its third along slice in 3D" in {
    toPipe(data3)
      .slice(Along(Third), List(Position2D("foo", 3), Position2D("baz", 1)), false, InMemory())
      .toList.sortBy(_.position) shouldBe result21
  }

  it should "return its third along inverse slice in 3D" in {
    toPipe(data3)
      .slice(Along(Third), List(Position2D("foo", 3), Position2D("baz", 1)), true, Default())
      .toList.sortBy(_.position) shouldBe result22
  }

  it should "return empty data - InMemory" in {
    toPipe(data3)
      .slice(Along(Third), List.empty[Position2D], true, InMemory())
      .toList.sortBy(_.position) shouldBe List()
  }

  it should "return all data - InMemory" in {
    toPipe(data3)
      .slice(Along(Third), List.empty[Position2D], false, InMemory())
      .toList.sortBy(_.position) shouldBe data3.sortBy(_.position)
  }

  it should "return empty data - Default" in {
    toPipe(data3)
      .slice(Along(Third), List.empty[Position2D], true, Default())
      .toList.sortBy(_.position) shouldBe List()
  }

  it should "return all data - Default" in {
    toPipe(data3)
      .slice(Along(Third), List.empty[Position2D], false, Default())
      .toList.sortBy(_.position) shouldBe data3.sortBy(_.position)
  }
}

class TestSparkMatrixSlice extends TestMatrixSlice {

  "A Matrix.slice" should "return its first over slice in 1D" in {
    toRDD(data1)
      .slice(Over(First), List("bar", "qux"), false, Default())
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first over inverse slice in 1D" in {
    toRDD(data1)
      .slice(Over(First), List("bar", "qux"), true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its first over slice in 2D" in {
    toRDD(data2)
      .slice(Over(First), List("bar", "qux"), false, Default())
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its first over inverse slice in 2D" in {
    toRDD(data2)
      .slice(Over(First), List("bar", "qux"), true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its first along slice in 2D" in {
    toRDD(data2)
      .slice(Along(First), List(1, 3), false, Default())
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its first along inverse slice in 2D" in {
    toRDD(data2)
      .slice(Along(First), List(1, 3), true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its second over slice in 2D" in {
    toRDD(data2)
      .slice(Over(Second), List(1, 3), false, Default())
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its second over inverse slice in 2D" in {
    toRDD(data2)
      .slice(Over(Second), List(1, 3), true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its second along slice in 2D" in {
    toRDD(data2)
      .slice(Along(Second), List("bar", "qux"), false, Default())
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its second along inverse slice in 2D" in {
    toRDD(data2)
      .slice(Along(Second), List("bar", "qux"), true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its first over slice in 3D" in {
    toRDD(data3)
      .slice(Over(First), List("bar", "qux"), false, Default())
      .toList.sortBy(_.position) shouldBe result11
  }

  it should "return its first over inverse slice in 3D" in {
    toRDD(data3)
      .slice(Over(First), List("bar", "qux"), true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result12
  }

  it should "return its first along slice in 3D" in {
    toRDD(data3)
      .slice(Along(First), List(Position2D(1, "xyz"), Position2D(3, "xyz")), false, Default())
      .toList.sortBy(_.position) shouldBe result13
  }

  it should "return its first along inverse slice in 3D" in {
    toRDD(data3)
      .slice(Along(First), List(Position2D(1, "xyz"), Position2D(3, "xyz")), true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result14
  }

  it should "return its second over slice in 3D" in {
    toRDD(data3)
      .slice(Over(Second), List(1, 3), false, Default())
      .toList.sortBy(_.position) shouldBe result15
  }

  it should "return its second over inverse slice in 3D" in {
    toRDD(data3)
      .slice(Over(Second), List(1, 3), true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result16
  }

  it should "return its second along slice in 3D" in {
    toRDD(data3)
      .slice(Along(Second), List(Position2D("bar", "xyz"), Position2D("qux", "xyz")), false, Default())
      .toList.sortBy(_.position) shouldBe result17
  }

  it should "return its second along inverse slice in 3D" in {
    toRDD(data3)
      .slice(Along(Second), List(Position2D("bar", "xyz"), Position2D("qux", "xyz")), true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result18
  }

  it should "return its third over slice in 3D" in {
    toRDD(data3)
      .slice(Over(Third), "xyz", false, Default())
      .toList.sortBy(_.position) shouldBe result19
  }

  it should "return its third over inverse slice in 3D" in {
    toRDD(data3)
      .slice(Over(Third), "xyz", true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result20
  }

  it should "return its third along slice in 3D" in {
    toRDD(data3)
      .slice(Along(Third), List(Position2D("foo", 3), Position2D("baz", 1)), false, Default())
      .toList.sortBy(_.position) shouldBe result21
  }

  it should "return its third along inverse slice in 3D" in {
    toRDD(data3)
      .slice(Along(Third), List(Position2D("foo", 3), Position2D("baz", 1)), true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result22
  }

  it should "return empty data - Default" in {
    toRDD(data3)
      .slice(Along(Third), List.empty[Position2D], true, Default())
      .toList.sortBy(_.position) shouldBe List()
  }

  it should "return all data - Default" in {
    toRDD(data3)
      .slice(Along(Third), List.empty[Position2D], false, Default())
      .toList.sortBy(_.position) shouldBe data3.sortBy(_.position)
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
    (cell.content.schema == NominalSchema(StringCodex)) ||
    (cell.content.schema.codex.isInstanceOf[Codex.DateCodex]) ||
    (cell.content.value equ "12.56")
  }
}

class TestScaldingMatrixWhich extends TestMatrixWhich {

  import au.com.cba.omnia.grimlock.scalding.Predicateable._

  "A Matrix.which" should "return its coordinates in 1D" in {
    toPipe(data1)
      .which(TestMatrixWhich.predicate)
      .toList.sorted shouldBe result1
  }

  it should "return its first over coordinates in 1D" in {
    toPipe(data1)
      .whichByPositions(Over(First),
        (List("bar", "qux"), (c: Cell[Position1D]) => TestMatrixWhich.predicate(c)), InMemory())
      .toList.sorted shouldBe result2
  }

  it should "return its first over multiple coordinates in 1D" in {
    toPipe(data1)
      .whichByPositions(Over(First), List(
        (List("bar", "qux"), (c: Cell[Position1D]) => TestMatrixWhich.predicate(c)),
        (List("foo"), (c: Cell[Position1D]) => !TestMatrixWhich.predicate(c))), Default())
      .toList.sorted shouldBe result3
  }

  it should "return its coordinates in 2D" in {
    toPipe(data2)
      .which(TestMatrixWhich.predicate)
      .toList.sorted shouldBe result4
  }

  it should "return its first over coordinates in 2D" in {
    toPipe(data2)
      .whichByPositions(Over(First),
        (List("bar", "qux"), (c: Cell[Position2D]) => TestMatrixWhich.predicate(c)), Default(Reducers(123)))
      .toList.sorted shouldBe result5
  }

  it should "return its first along coordinates in 2D" in {
    toPipe(data2)
      .whichByPositions(Along(First), (List(2, 4), (c: Cell[Position2D]) => TestMatrixWhich.predicate(c)),
        Unbalanced(Reducers(123)))
      .toList.sorted shouldBe result6
  }

  it should "return its second over coordinates in 2D" in {
    toPipe(data2)
      .whichByPositions(Over(Second), (List(2, 4), (c: Cell[Position2D]) => TestMatrixWhich.predicate(c)), InMemory())
      .toList.sorted shouldBe result7
  }

  it should "return its second along coordinates in 2D" in {
    toPipe(data2)
      .whichByPositions(Along(Second),
        (List("bar", "qux"), (c: Cell[Position2D]) => TestMatrixWhich.predicate(c)), Default())
      .toList.sorted shouldBe result8
  }

  it should "return its first over multiple coordinates in 2D" in {
    toPipe(data2)
      .whichByPositions(Over(First), List(
        (List("bar", "qux"), (c: Cell[Position2D]) => TestMatrixWhich.predicate(c)),
        (List("foo"), (c: Cell[Position2D]) => !TestMatrixWhich.predicate(c))), Default(Reducers(123)))
      .toList.sorted shouldBe result9
  }

  it should "return its first along multiple coordinates in 2D" in {
    toPipe(data2)
      .whichByPositions(Along(First), List(
        (List(2, 4), (c: Cell[Position2D]) => TestMatrixWhich.predicate(c)),
        (List(2), (c: Cell[Position2D]) => !TestMatrixWhich.predicate(c))), Unbalanced(Reducers(123)))
      .toList.sorted shouldBe result10
  }

  it should "return its second over multiple coordinates in 2D" in {
    toPipe(data2)
      .whichByPositions(Over(Second), List(
        (List(2, 4), (c: Cell[Position2D]) => TestMatrixWhich.predicate(c)),
        (List(2), (c: Cell[Position2D]) => !TestMatrixWhich.predicate(c))), InMemory())
      .toList.sorted shouldBe result11
  }

  it should "return its second along multiple coordinates in 2D" in {
    toPipe(data2)
      .whichByPositions(Along(Second), List(
        (List("bar", "qux"), (c: Cell[Position2D]) => TestMatrixWhich.predicate(c)),
        (List("foo"), (c: Cell[Position2D]) => !TestMatrixWhich.predicate(c))), Default())
      .toList.sorted shouldBe result12
  }

  it should "return its coordinates in 3D" in {
    toPipe(data3)
      .which(TestMatrixWhich.predicate)
      .toList.sorted shouldBe result13
  }

  it should "return its first over coordinates in 3D" in {
    toPipe(data3)
      .whichByPositions(Over(First),
        (List("bar", "qux"), (c: Cell[Position3D]) => TestMatrixWhich.predicate(c)), Default(Reducers(123)))
      .toList.sorted shouldBe result14
  }

  it should "return its first along coordinates in 3D" in {
    toPipe(data3)
      .whichByPositions(Along(First),
        (List(Position2D(2, "xyz"), Position2D(4, "xyz")), (c: Cell[Position3D]) => TestMatrixWhich.predicate(c)),
          Unbalanced(Reducers(123)))
      .toList.sorted shouldBe result15
  }

  it should "return its second over coordinates in 3D" in {
    toPipe(data3)
      .whichByPositions(Over(Second), (List(2, 4), (c: Cell[Position3D]) => TestMatrixWhich.predicate(c)), InMemory())
      .toList.sorted shouldBe result16
  }

  it should "return its second along coordinates in 3D" in {
    toPipe(data3)
      .whichByPositions(Along(Second), (List(Position2D("bar", "xyz"), Position2D("qux", "xyz")),
        (c: Cell[Position3D]) => TestMatrixWhich.predicate(c)), Default())
      .toList.sorted shouldBe result17
  }

  it should "return its third over coordinates in 3D" in {
    toPipe(data3)
      .whichByPositions(Over(Third), ("xyz", (c: Cell[Position3D]) => TestMatrixWhich.predicate(c)),
        Default(Reducers(123)))
      .toList.sorted shouldBe result18
  }

  it should "return its third along coordinates in 3D" in {
    toPipe(data3)
      .whichByPositions(Along(Third), (List(Position2D("bar", 2), Position2D("qux", 1)),
        (c: Cell[Position3D]) => TestMatrixWhich.predicate(c)), Unbalanced(Reducers(123)))
      .toList.sorted shouldBe result19
  }

  it should "return its first over multiple coordinates in 3D" in {
    toPipe(data3)
      .whichByPositions(Over(First), List(
        (List("bar", "qux"), (c: Cell[Position3D]) => TestMatrixWhich.predicate(c)),
        (List("foo"), (c: Cell[Position3D]) => !TestMatrixWhich.predicate(c))), InMemory())
      .toList.sorted shouldBe result20
  }

  it should "return its first along multiple coordinates in 3D" in {
    toPipe(data3)
      .whichByPositions(Along(First), List(
        (List(Position2D(2, "xyz"), Position2D(4, "xyz")), (c: Cell[Position3D]) => TestMatrixWhich.predicate(c)),
        (List(Position2D(2, "xyz")), (c: Cell[Position3D]) => !TestMatrixWhich.predicate(c))), Default())
      .toList.sorted shouldBe result21
  }

  it should "return its second over multiple coordinates in 3D" in {
    toPipe(data3)
      .whichByPositions(Over(Second), List(
        (List(2, 4), (c: Cell[Position3D]) => TestMatrixWhich.predicate(c)),
        (List(2), (c: Cell[Position3D]) => !TestMatrixWhich.predicate(c))), Default(Reducers(123)))
      .toList.sorted shouldBe result22
  }

  it should "return its second along multiple coordinates in 3D" in {
    toPipe(data3)
      .whichByPositions(Along(Second), List(
        (List(Position2D("bar", "xyz"), Position2D("qux", "xyz")),
        (c: Cell[Position3D]) => TestMatrixWhich.predicate(c)), (List(Position2D("foo", "xyz")),
        (c: Cell[Position3D]) => !TestMatrixWhich.predicate(c))), Unbalanced(Reducers(123)))
      .toList.sorted shouldBe result23
  }

  it should "return its third over multiple coordinates in 3D" in {
    toPipe(data3)
      .whichByPositions(Over(Third), List(
        ("xyz", (c: Cell[Position3D]) => TestMatrixWhich.predicate(c)),
        ("xyz", (c: Cell[Position3D]) => !TestMatrixWhich.predicate(c))), InMemory())
      .toList.sorted shouldBe result24
  }

  it should "return its third along multiple coordinates in 3D" in {
    toPipe(data3)
      .whichByPositions(Along(Third), List(
        (List(Position2D("foo", 1), Position2D("qux", 1)), (c: Cell[Position3D]) => TestMatrixWhich.predicate(c)),
        (List(Position2D("foo", 2)), (c: Cell[Position3D]) => !TestMatrixWhich.predicate(c))), Default())
      .toList.sorted shouldBe result25
  }

  it should "return empty data - InMemory" in {
    toPipe(data3)
      .whichByPositions(Along(Third), List(
        (List.empty[Position2D], (c: Cell[Position3D]) => !TestMatrixWhich.predicate(c))), InMemory())
      .toList.sorted shouldBe List()
  }

  it should "return empty data - Default" in {
    toPipe(data3)
      .whichByPositions(Along(Third), List(
        (List.empty[Position2D], (c: Cell[Position3D]) => !TestMatrixWhich.predicate(c))), Default())
      .toList.sorted shouldBe List()
  }
}

class TestSparkMatrixWhich extends TestMatrixWhich {

  import au.com.cba.omnia.grimlock.spark.Predicateable._

  "A Matrix.which" should "return its coordinates in 1D" in {
    toRDD(data1)
      .which(TestMatrixWhich.predicate)
      .toList.sorted shouldBe result1
  }

  it should "return its first over coordinates in 1D" in {
    toRDD(data1)
      .whichByPositions(Over(First), (List("bar", "qux"), (c: Cell[Position1D]) => TestMatrixWhich.predicate(c)),
        Default())
      .toList.sorted shouldBe result2
  }

  it should "return its first over multiple coordinates in 1D" in {
    toRDD(data1)
      .whichByPositions(Over(First), List(
        (List("bar", "qux"), (c: Cell[Position1D]) => TestMatrixWhich.predicate(c)),
        (List("foo"), (c: Cell[Position1D]) => !TestMatrixWhich.predicate(c))), Default(Reducers(12)))
      .toList.sorted shouldBe result3
  }

  it should "return its coordinates in 2D" in {
    toRDD(data2)
      .which(TestMatrixWhich.predicate)
      .toList.sorted shouldBe result4
  }

  it should "return its first over coordinates in 2D" in {
    toRDD(data2)
      .whichByPositions(Over(First), (List("bar", "qux"), (c: Cell[Position2D]) => TestMatrixWhich.predicate(c)),
        Default())
      .toList.sorted shouldBe result5
  }

  it should "return its first along coordinates in 2D" in {
    toRDD(data2)
      .whichByPositions(Along(First), (List(2, 4), (c: Cell[Position2D]) => TestMatrixWhich.predicate(c)),
        Default(Reducers(12)))
      .toList.sorted shouldBe result6
  }

  it should "return its second over coordinates in 2D" in {
    toRDD(data2)
      .whichByPositions(Over(Second), (List(2, 4), (c: Cell[Position2D]) => TestMatrixWhich.predicate(c)), Default())
      .toList.sorted shouldBe result7
  }

  it should "return its second along coordinates in 2D" in {
    toRDD(data2)
      .whichByPositions(Along(Second), (List("bar", "qux"), (c: Cell[Position2D]) => TestMatrixWhich.predicate(c)),
        Default(Reducers(12)))
      .toList.sorted shouldBe result8
  }

  it should "return its first over multiple coordinates in 2D" in {
    toRDD(data2)
      .whichByPositions(Over(First), List(
        (List("bar", "qux"), (c: Cell[Position2D]) => TestMatrixWhich.predicate(c)),
        (List("foo"), (c: Cell[Position2D]) => !TestMatrixWhich.predicate(c))), Default())
      .toList.sorted shouldBe result9
  }

  it should "return its first along multiple coordinates in 2D" in {
    toRDD(data2)
      .whichByPositions(Along(First), List(
        (List(2, 4), (c: Cell[Position2D]) => TestMatrixWhich.predicate(c)),
        (List(2), (c: Cell[Position2D]) => !TestMatrixWhich.predicate(c))), Default(Reducers(12)))
      .toList.sorted shouldBe result10
  }

  it should "return its second over multiple coordinates in 2D" in {
    toRDD(data2)
      .whichByPositions(Over(Second), List(
        (List(2, 4), (c: Cell[Position2D]) => TestMatrixWhich.predicate(c)),
        (List(2), (c: Cell[Position2D]) => !TestMatrixWhich.predicate(c))), Default())
      .toList.sorted shouldBe result11
  }

  it should "return its second along multiple coordinates in 2D" in {
    toRDD(data2)
      .whichByPositions(Along(Second), List(
        (List("bar", "qux"), (c: Cell[Position2D]) => TestMatrixWhich.predicate(c)),
        (List("foo"), (c: Cell[Position2D]) => !TestMatrixWhich.predicate(c))), Default(Reducers(12)))
      .toList.sorted shouldBe result12
  }

  it should "return its coordinates in 3D" in {
    toRDD(data3)
      .which(TestMatrixWhich.predicate)
      .toList.sorted shouldBe result13
  }

  it should "return its first over coordinates in 3D" in {
    toRDD(data3)
      .whichByPositions(Over(First), (List("bar", "qux"), (c: Cell[Position3D]) => TestMatrixWhich.predicate(c)),
        Default())
      .toList.sorted shouldBe result14
  }

  it should "return its first along coordinates in 3D" in {
    toRDD(data3)
      .whichByPositions(Along(First),
        (List(Position2D(2, "xyz"), Position2D(4, "xyz")), (c: Cell[Position3D]) => TestMatrixWhich.predicate(c)),
          Default(Reducers(12)))
      .toList.sorted shouldBe result15
  }

  it should "return its second over coordinates in 3D" in {
    toRDD(data3)
      .whichByPositions(Over(Second), (List(2, 4), (c: Cell[Position3D]) => TestMatrixWhich.predicate(c)), Default())
      .toList.sorted shouldBe result16
  }

  it should "return its second along coordinates in 3D" in {
    toRDD(data3)
      .whichByPositions(Along(Second), (List(Position2D("bar", "xyz"), Position2D("qux", "xyz")),
        (c: Cell[Position3D]) => TestMatrixWhich.predicate(c)), Default(Reducers(12)))
      .toList.sorted shouldBe result17
  }

  it should "return its third over coordinates in 3D" in {
    toRDD(data3)
      .whichByPositions(Over(Third), ("xyz", (c: Cell[Position3D]) => TestMatrixWhich.predicate(c)), Default())
      .toList.sorted shouldBe result18
  }

  it should "return its third along coordinates in 3D" in {
    toRDD(data3)
      .whichByPositions(Along(Third), (List(Position2D("bar", 2), Position2D("qux", 1)),
        (c: Cell[Position3D]) => TestMatrixWhich.predicate(c)), Default(Reducers(12)))
      .toList.sorted shouldBe result19
  }

  it should "return its first over multiple coordinates in 3D" in {
    toRDD(data3)
      .whichByPositions(Over(First), List(
        (List("bar", "qux"), (c: Cell[Position3D]) => TestMatrixWhich.predicate(c)),
        (List("foo"), (c: Cell[Position3D]) => !TestMatrixWhich.predicate(c))), Default())
      .toList.sorted shouldBe result20
  }

  it should "return its first along multiple coordinates in 3D" in {
    toRDD(data3)
      .whichByPositions(Along(First), List(
        (List(Position2D(2, "xyz"), Position2D(4, "xyz")), (c: Cell[Position3D]) => TestMatrixWhich.predicate(c)),
        (List(Position2D(2, "xyz")), (c: Cell[Position3D]) => !TestMatrixWhich.predicate(c))), Default(Reducers(12)))
      .toList.sorted shouldBe result21
  }

  it should "return its second over multiple coordinates in 3D" in {
    toRDD(data3)
      .whichByPositions(Over(Second), List(
        (List(2, 4), (c: Cell[Position3D]) => TestMatrixWhich.predicate(c)),
        (List(2), (c: Cell[Position3D]) => !TestMatrixWhich.predicate(c))), Default())
      .toList.sorted shouldBe result22
  }

  it should "return its second along multiple coordinates in 3D" in {
    toRDD(data3)
      .whichByPositions(Along(Second), List(
        (List(Position2D("bar", "xyz"), Position2D("qux", "xyz")),
        (c: Cell[Position3D]) => TestMatrixWhich.predicate(c)), (List(Position2D("foo", "xyz")),
        (c: Cell[Position3D]) => !TestMatrixWhich.predicate(c))), Default(Reducers(12)))
      .toList.sorted shouldBe result23
  }

  it should "return its third over multiple coordinates in 3D" in {
    toRDD(data3)
      .whichByPositions(Over(Third), List(
        ("xyz", (c: Cell[Position3D]) => TestMatrixWhich.predicate(c)),
        ("xyz", (c: Cell[Position3D]) => !TestMatrixWhich.predicate(c))), Default())
      .toList.sorted shouldBe result24
  }

  it should "return its third along multiple coordinates in 3D" in {
    toRDD(data3)
      .whichByPositions(Along(Third), List(
        (List(Position2D("foo", 1), Position2D("qux", 1)), (c: Cell[Position3D]) => TestMatrixWhich.predicate(c)),
        (List(Position2D("foo", 2)), (c: Cell[Position3D]) => !TestMatrixWhich.predicate(c))), Default(Reducers(12)))
      .toList.sorted shouldBe result25
  }

  it should "return empty data - Default" in {
    toRDD(data3)
      .whichByPositions(Along(Third), List(
        (List.empty[Position2D], (c: Cell[Position3D]) => !TestMatrixWhich.predicate(c))), Default())
      .toList.sorted shouldBe List()
  }
}

trait TestMatrixGet extends TestMatrix {

  val result1 = List(Cell(Position1D("qux"), Content(OrdinalSchema(StringCodex), "12.56")))

  val result2 = List(Cell(Position2D("foo", 3), Content(NominalSchema(StringCodex), "9.42")),
    Cell(Position2D("qux", 1), Content(OrdinalSchema(StringCodex), "12.56")))

  val result3 = List(Cell(Position3D("foo", 3, "xyz"), Content(NominalSchema(StringCodex), "9.42")),
    Cell(Position3D("qux", 1, "xyz"), Content(OrdinalSchema(StringCodex), "12.56")))
}

class TestScaldingMatrixGet extends TestMatrixGet {

  "A Matrix.get" should "return its cells in 1D" in {
    toPipe(data1)
      .get("qux", InMemory())
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its cells in 2D" in {
    toPipe(data2)
      .get(List(Position2D("foo", 3), Position2D("qux", 1), Position2D("baz", 4)), Default())
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its cells in 3D" in {
    toPipe(data3)
      .get(List(Position3D("foo", 3, "xyz"), Position3D("qux", 1, "xyz"), Position3D("baz", 4, "xyz")),
        Unbalanced(Reducers(123)))
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return empty data - InMemory" in {
    toPipe(data3)
      .get(List.empty[Position3D], InMemory())
      .toList.sortBy(_.position) shouldBe List()
  }

  it should "return empty data - Default" in {
    toPipe(data3)
      .get(List.empty[Position3D], Default())
      .toList.sortBy(_.position) shouldBe List()
  }
}

class TestSparkMatrixGet extends TestMatrixGet {

  "A Matrix.get" should "return its cells in 1D" in {
    toRDD(data1)
      .get("qux", Default())
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its cells in 2D" in {
    toRDD(data2)
      .get(List(Position2D("foo", 3), Position2D("qux", 1), Position2D("baz", 4)), Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its cells in 3D" in {
    toRDD(data3)
      .get(List(Position3D("foo", 3, "xyz"), Position3D("qux", 1, "xyz"), Position3D("baz", 4, "xyz")),
        Default())
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return empty data - Default" in {
    toRDD(data3)
      .get(List.empty[Position3D], Default())
      .toList.sortBy(_.position) shouldBe List()
  }
}

trait TestMatrixCompact extends TestMatrix {
  val result1 = data1.map { case c => c.position -> c.content }.toMap

  val result2 = Map(
        Position1D("foo") -> Map(Position1D(1) -> Content(OrdinalSchema(StringCodex), "3.14"),
          Position1D(2) -> Content(ContinuousSchema(DoubleCodex), 6.28),
          Position1D(3) -> Content(NominalSchema(StringCodex), "9.42"),
          Position1D(4) -> Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
            (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
        Position1D("bar") -> Map(Position1D(1) -> Content(OrdinalSchema(StringCodex), "6.28"),
          Position1D(2) -> Content(ContinuousSchema(DoubleCodex), 12.56),
          Position1D(3) -> Content(OrdinalSchema(LongCodex), 19)),
        Position1D("baz") -> Map(Position1D(1) -> Content(OrdinalSchema(StringCodex), "9.42"),
          Position1D(2) -> Content(DiscreteSchema(LongCodex), 19)),
        Position1D("qux") -> Map(Position1D(1) -> Content(OrdinalSchema(StringCodex), "12.56")))

  val result3 = Map(
        Position1D(1) -> Map(Position1D("foo") -> Content(OrdinalSchema(StringCodex), "3.14"),
          Position1D("bar") -> Content(OrdinalSchema(StringCodex), "6.28"),
          Position1D("baz") -> Content(OrdinalSchema(StringCodex), "9.42"),
          Position1D("qux") -> Content(OrdinalSchema(StringCodex), "12.56")),
        Position1D(2) -> Map(Position1D("foo") -> Content(ContinuousSchema(DoubleCodex), 6.28),
          Position1D("bar") -> Content(ContinuousSchema(DoubleCodex), 12.56),
          Position1D("baz") -> Content(DiscreteSchema(LongCodex), 19)),
        Position1D(3) -> Map(Position1D("foo") -> Content(NominalSchema(StringCodex), "9.42"),
          Position1D("bar") -> Content(OrdinalSchema(LongCodex), 19)),
        Position1D(4) -> Map(Position1D("foo") -> Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
          (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))

  val result4 = Map(
        Position1D(1) -> Map(Position1D("foo") -> Content(OrdinalSchema(StringCodex), "3.14"),
          Position1D("bar") -> Content(OrdinalSchema(StringCodex), "6.28"),
          Position1D("baz") -> Content(OrdinalSchema(StringCodex), "9.42"),
          Position1D("qux") -> Content(OrdinalSchema(StringCodex), "12.56")),
        Position1D(2) -> Map(Position1D("foo") -> Content(ContinuousSchema(DoubleCodex), 6.28),
          Position1D("bar") -> Content(ContinuousSchema(DoubleCodex), 12.56),
          Position1D("baz") -> Content(DiscreteSchema(LongCodex), 19)),
        Position1D(3) -> Map(Position1D("foo") -> Content(NominalSchema(StringCodex), "9.42"),
          Position1D("bar") -> Content(OrdinalSchema(LongCodex), 19)),
        Position1D(4) -> Map(Position1D("foo") -> Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
          (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))

  val result5 = Map(
        Position1D("foo") -> Map(Position1D(1) -> Content(OrdinalSchema(StringCodex), "3.14"),
          Position1D(2) -> Content(ContinuousSchema(DoubleCodex), 6.28),
          Position1D(3) -> Content(NominalSchema(StringCodex), "9.42"),
          Position1D(4) -> Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
            (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
        Position1D("bar") -> Map(Position1D(1) -> Content(OrdinalSchema(StringCodex), "6.28"),
          Position1D(2) -> Content(ContinuousSchema(DoubleCodex), 12.56),
          Position1D(3) -> Content(OrdinalSchema(LongCodex), 19)),
        Position1D("baz") -> Map(Position1D(1) -> Content(OrdinalSchema(StringCodex), "9.42"),
          Position1D(2) -> Content(DiscreteSchema(LongCodex), 19)),
        Position1D("qux") -> Map(Position1D(1) -> Content(OrdinalSchema(StringCodex), "12.56")))

  val result6 = Map(
        Position1D("foo") -> Map(Position2D(1, "xyz") -> Content(OrdinalSchema(StringCodex), "3.14"),
          Position2D(2, "xyz") -> Content(ContinuousSchema(DoubleCodex), 6.28),
          Position2D(3, "xyz") -> Content(NominalSchema(StringCodex), "9.42"),
          Position2D(4, "xyz") -> Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
            (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
        Position1D("bar") -> Map(Position2D(1, "xyz") -> Content(OrdinalSchema(StringCodex), "6.28"),
          Position2D(2, "xyz") -> Content(ContinuousSchema(DoubleCodex), 12.56),
          Position2D(3, "xyz") -> Content(OrdinalSchema(LongCodex), 19)),
        Position1D("baz") -> Map(Position2D(1, "xyz") -> Content(OrdinalSchema(StringCodex), "9.42"),
          Position2D(2, "xyz") -> Content(DiscreteSchema(LongCodex), 19)),
        Position1D("qux") -> Map(Position2D(1, "xyz") -> Content(OrdinalSchema(StringCodex), "12.56")))

  val result7 = Map(
        Position2D(1, "xyz") -> Map(Position1D("foo") -> Content(OrdinalSchema(StringCodex), "3.14"),
          Position1D("bar") -> Content(OrdinalSchema(StringCodex), "6.28"),
          Position1D("baz") -> Content(OrdinalSchema(StringCodex), "9.42"),
          Position1D("qux") -> Content(OrdinalSchema(StringCodex), "12.56")),
        Position2D(2, "xyz") -> Map(Position1D("foo") -> Content(ContinuousSchema(DoubleCodex), 6.28),
          Position1D("bar") -> Content(ContinuousSchema(DoubleCodex), 12.56),
          Position1D("baz") -> Content(DiscreteSchema(LongCodex), 19)),
        Position2D(3, "xyz") -> Map(Position1D("foo") -> Content(NominalSchema(StringCodex), "9.42"),
          Position1D("bar") -> Content(OrdinalSchema(LongCodex), 19)),
        Position2D(4, "xyz") -> Map(Position1D("foo") -> Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
          (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))

  val result8 = Map(
        Position1D(1) -> Map(Position2D("foo", "xyz") -> Content(OrdinalSchema(StringCodex), "3.14"),
          Position2D("bar", "xyz") -> Content(OrdinalSchema(StringCodex), "6.28"),
          Position2D("baz", "xyz") -> Content(OrdinalSchema(StringCodex), "9.42"),
          Position2D("qux", "xyz") -> Content(OrdinalSchema(StringCodex), "12.56")),
        Position1D(2) -> Map(Position2D("foo", "xyz") -> Content(ContinuousSchema(DoubleCodex), 6.28),
          Position2D("bar", "xyz") -> Content(ContinuousSchema(DoubleCodex), 12.56),
          Position2D("baz", "xyz") -> Content(DiscreteSchema(LongCodex), 19)),
        Position1D(3) -> Map(Position2D("foo", "xyz") -> Content(NominalSchema(StringCodex), "9.42"),
          Position2D("bar", "xyz") -> Content(OrdinalSchema(LongCodex), 19)),
        Position1D(4) -> Map(Position2D("foo", "xyz") -> Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
          (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))

  val result9 = Map(
        Position2D("foo", "xyz") -> Map(Position1D(1) -> Content(OrdinalSchema(StringCodex), "3.14"),
          Position1D(2) -> Content(ContinuousSchema(DoubleCodex), 6.28),
          Position1D(3) -> Content(NominalSchema(StringCodex), "9.42"),
          Position1D(4) -> Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
            (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
        Position2D("bar", "xyz") -> Map(Position1D(1) -> Content(OrdinalSchema(StringCodex), "6.28"),
          Position1D(2) -> Content(ContinuousSchema(DoubleCodex), 12.56),
          Position1D(3) -> Content(OrdinalSchema(LongCodex), 19)),
        Position2D("baz", "xyz") -> Map(Position1D(1) -> Content(OrdinalSchema(StringCodex), "9.42"),
          Position1D(2) -> Content(DiscreteSchema(LongCodex), 19)),
        Position2D("qux", "xyz") -> Map(Position1D(1) -> Content(OrdinalSchema(StringCodex), "12.56")))

  val result10 = Map(Position1D("xyz") -> Map(
        Position2D("foo", 1) -> Content(OrdinalSchema(StringCodex), "3.14"),
        Position2D("bar", 1) -> Content(OrdinalSchema(StringCodex), "6.28"),
        Position2D("baz", 1) -> Content(OrdinalSchema(StringCodex), "9.42"),
        Position2D("qux", 1) -> Content(OrdinalSchema(StringCodex), "12.56"),
        Position2D("foo", 2) -> Content(ContinuousSchema(DoubleCodex), 6.28),
        Position2D("bar", 2) -> Content(ContinuousSchema(DoubleCodex), 12.56),
        Position2D("baz", 2) -> Content(DiscreteSchema(LongCodex), 19),
        Position2D("foo", 3) -> Content(NominalSchema(StringCodex), "9.42"),
        Position2D("bar", 3) -> Content(OrdinalSchema(LongCodex), 19),
        Position2D("foo", 4) -> Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
          (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))

  val result11 = Map(
        Position2D("foo", 1) -> Map(Position1D("xyz") -> Content(OrdinalSchema(StringCodex), "3.14")),
        Position2D("foo", 2) -> Map(Position1D("xyz") -> Content(ContinuousSchema(DoubleCodex), 6.28)),
        Position2D("foo", 3) -> Map(Position1D("xyz") -> Content(NominalSchema(StringCodex), "9.42")),
        Position2D("foo", 4) -> Map(Position1D("xyz") -> Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
          (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
        Position2D("bar", 1) -> Map(Position1D("xyz") -> Content(OrdinalSchema(StringCodex), "6.28")),
        Position2D("bar", 2) -> Map(Position1D("xyz") -> Content(ContinuousSchema(DoubleCodex), 12.56)),
        Position2D("bar", 3) -> Map(Position1D("xyz") -> Content(OrdinalSchema(LongCodex), 19)),
        Position2D("baz", 1) -> Map(Position1D("xyz") -> Content(OrdinalSchema(StringCodex), "9.42")),
        Position2D("baz", 2) -> Map(Position1D("xyz") -> Content(DiscreteSchema(LongCodex), 19)),
        Position2D("qux", 1) -> Map(Position1D("xyz") -> Content(OrdinalSchema(StringCodex), "12.56")))
}

class TestScaldingMatrixCompact extends TestMatrixCompact {

  "A Matrix.compact" should "return its first over map in 1D" in {
    toPipe(data1)
      .compact(Over(First), Default()).toTypedPipe
      .toList shouldBe List(result1)
  }

  it should "return its first over map in 2D" in {
    toPipe(data2)
      .compact(Over(First), Default(Reducers(123))).toTypedPipe
      .toList shouldBe List(result2)
  }

  it should "return its first along map in 2D" in {
    toPipe(data2)
      .compact(Along(First), Default()).toTypedPipe
      .toList shouldBe List(result3)
  }

  it should "return its second over map in 2D" in {
    toPipe(data2)
      .compact(Over(Second), Default(Reducers(123))).toTypedPipe
      .toList shouldBe List(result4)
  }

  it should "return its second along map in 2D" in {
    toPipe(data2)
      .compact(Along(Second), Default()).toTypedPipe
      .toList shouldBe List(result5)
  }

  it should "return its first over map in 3D" in {
    toPipe(data3)
      .compact(Over(First), Default(Reducers(123))).toTypedPipe
      .toList shouldBe List(result6)
  }

  it should "return its first along map in 3D" in {
    toPipe(data3)
      .compact(Along(First), Default()).toTypedPipe
      .toList shouldBe List(result7)
  }

  it should "return its second over map in 3D" in {
    toPipe(data3)
      .compact(Over(Second), Default(Reducers(123))).toTypedPipe
      .toList shouldBe List(result8)
  }

  it should "return its second along map in 3D" in {
    toPipe(data3)
      .compact(Along(Second), Default()).toTypedPipe
      .toList shouldBe List(result9)
  }

  it should "return its third over map in 3D" in {
    toPipe(data3)
      .compact(Over(Third), Default(Reducers(123))).toTypedPipe
      .toList shouldBe List(result10)
  }

  it should "return its third along map in 3D" in {
    toPipe(data3)
      .compact(Along(Third), Default()).toTypedPipe
      .toList shouldBe List(result11)
  }
}

class TestSparkMatrixCompact extends TestMatrixCompact {

  "A Matrix.compact" should "return its first over map in 1D" in {
    toRDD(data1).compact(Over(First), Default()) shouldBe result1
  }

  it should "return its first over map in 2D" in {
    toRDD(data2).compact(Over(First), Default(Reducers(12))) shouldBe result2
  }

  it should "return its first along map in 2D" in {
    toRDD(data2).compact(Along(First), Default()) shouldBe result3
  }

  it should "return its second over map in 2D" in {
    toRDD(data2).compact(Over(Second), Default(Reducers(12))) shouldBe result4
  }

  it should "return its second along map in 2D" in {
    toRDD(data2).compact(Along(Second), Default()) shouldBe result5
  }

  it should "return its first over map in 3D" in {
    toRDD(data3).compact(Over(First), Default(Reducers(12))) shouldBe result6
  }

  it should "return its first along map in 3D" in {
    toRDD(data3).compact(Along(First), Default()) shouldBe result7
  }

  it should "return its second over map in 3D" in {
    toRDD(data3).compact(Over(Second), Default(Reducers(12))) shouldBe result8
  }

  it should "return its second along map in 3D" in {
    toRDD(data3).compact(Along(Second), Default()) shouldBe result9
  }

  it should "return its third over map in 3D" in {
    toRDD(data3).compact(Over(Third), Default(Reducers(12))) shouldBe result10
  }

  it should "return its third along map in 3D" in {
    toRDD(data3).compact(Along(Third), Default()) shouldBe result11
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

  val result1 = List(Cell(Position1D("bar"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position1D("baz"), Content(ContinuousSchema(DoubleCodex), 9.42)),
    Cell(Position1D("foo"), Content(ContinuousSchema(DoubleCodex), 3.14)),
    Cell(Position1D("qux"), Content(ContinuousSchema(DoubleCodex), 12.56)))

  val result2 = List(Cell(Position1D(1), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position1D(2), Content(ContinuousSchema(DoubleCodex), 18.84)),
    Cell(Position1D(3), Content(ContinuousSchema(DoubleCodex), 18.84)),
    Cell(Position1D(4), Content(ContinuousSchema(DoubleCodex), 12.56)))

  val result3 = List(Cell(Position1D(1), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position1D(2), Content(ContinuousSchema(DoubleCodex), 18.84)),
    Cell(Position1D(3), Content(ContinuousSchema(DoubleCodex), 18.84)),
    Cell(Position1D(4), Content(ContinuousSchema(DoubleCodex), 12.56)))

  val result4 = List(Cell(Position1D("bar"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position1D("baz"), Content(ContinuousSchema(DoubleCodex), 9.42)),
    Cell(Position1D("foo"), Content(ContinuousSchema(DoubleCodex), 3.14)),
    Cell(Position1D("qux"), Content(ContinuousSchema(DoubleCodex), 12.56)))

  val result5 = List(Cell(Position1D("bar"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position1D("baz"), Content(ContinuousSchema(DoubleCodex), 9.42)),
    Cell(Position1D("foo"), Content(ContinuousSchema(DoubleCodex), 3.14)),
    Cell(Position1D("qux"), Content(ContinuousSchema(DoubleCodex), 12.56)))

  val result6 = List(Cell(Position2D(1, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position2D(2, "xyz"), Content(ContinuousSchema(DoubleCodex), 18.84)),
    Cell(Position2D(3, "xyz"), Content(ContinuousSchema(DoubleCodex), 18.84)),
    Cell(Position2D(4, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)))

  val result7 = List(Cell(Position1D(1), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position1D(2), Content(ContinuousSchema(DoubleCodex), 18.84)),
    Cell(Position1D(3), Content(ContinuousSchema(DoubleCodex), 18.84)),
    Cell(Position1D(4), Content(ContinuousSchema(DoubleCodex), 12.56)))

  val result8 = List(Cell(Position2D("bar", "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position2D("baz", "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42)),
    Cell(Position2D("foo", "xyz"), Content(ContinuousSchema(DoubleCodex), 3.14)),
    Cell(Position2D("qux", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)))

  val result9 = List(Cell(Position1D("xyz"), Content(ContinuousSchema(DoubleCodex), 18.84)))

  val result10 = List(Cell(Position2D("bar", 1), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position2D("bar", 2), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position2D("bar", 3), Content(ContinuousSchema(DoubleCodex), 18.84)),
    Cell(Position2D("baz", 1), Content(ContinuousSchema(DoubleCodex), 9.42)),
    Cell(Position2D("baz", 2), Content(ContinuousSchema(DoubleCodex), 18.84)),
    Cell(Position2D("foo", 1), Content(ContinuousSchema(DoubleCodex), 3.14)),
    Cell(Position2D("foo", 2), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position2D("foo", 3), Content(ContinuousSchema(DoubleCodex), 9.42)),
    Cell(Position2D("foo", 4), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position2D("qux", 1), Content(ContinuousSchema(DoubleCodex), 12.56)))

  val result11 = List(
    Cell(Position1D("bar"), Content(ContinuousSchema(DoubleCodex), (6.28 + 12.56 + 18.84) * (1.0 / 2))),
    Cell(Position1D("baz"), Content(ContinuousSchema(DoubleCodex), (9.42 + 18.84) * (1.0 / 3))),
    Cell(Position1D("foo"), Content(ContinuousSchema(DoubleCodex), (3.14 + 6.28 + 9.42 + 12.56) * (1.0 / 1))),
    Cell(Position1D("qux"), Content(ContinuousSchema(DoubleCodex), 12.56 * (1.0 / 4))))

  val result12 = List(
    Cell(Position1D(1), Content(ContinuousSchema(DoubleCodex), (3.14 + 6.28 + 9.42 + 12.56) * (1.0 / 2))),
    Cell(Position1D(2), Content(ContinuousSchema(DoubleCodex), (6.28 + 12.56 + 18.84) * (1.0 / 4))),
    Cell(Position1D(3), Content(ContinuousSchema(DoubleCodex), (9.42 + 18.84) * (1.0 / 6))),
    Cell(Position1D(4), Content(ContinuousSchema(DoubleCodex), 12.56 * (1.0 / 8))))

  val result13 = List(
    Cell(Position1D(1), Content(ContinuousSchema(DoubleCodex), (3.14 + 6.28 + 9.42 + 12.56) * (1.0 / 2))),
    Cell(Position1D(2), Content(ContinuousSchema(DoubleCodex), (6.28 + 12.56 + 18.84) * (1.0 / 4))),
    Cell(Position1D(3), Content(ContinuousSchema(DoubleCodex), (9.42 + 18.84) * (1.0 / 6))),
    Cell(Position1D(4), Content(ContinuousSchema(DoubleCodex), 12.56 * (1.0 / 8))))

  val result14 = List(
    Cell(Position1D("bar"), Content(ContinuousSchema(DoubleCodex), (6.28 + 12.56 + 18.84) * (1.0 / 2))),
    Cell(Position1D("baz"), Content(ContinuousSchema(DoubleCodex), (9.42 + 18.84) * (1.0 / 3))),
    Cell(Position1D("foo"), Content(ContinuousSchema(DoubleCodex), (3.14 + 6.28 + 9.42 + 12.56) * (1.0 / 1))),
    Cell(Position1D("qux"), Content(ContinuousSchema(DoubleCodex), 12.56 * (1.0 / 4))))

  val result15 = List(
    Cell(Position1D("bar"), Content(ContinuousSchema(DoubleCodex), (6.28 + 12.56 + 18.84) * (1.0 / 2))),
    Cell(Position1D("baz"), Content(ContinuousSchema(DoubleCodex), (9.42 + 18.84) * (1.0 / 3))),
    Cell(Position1D("foo"), Content(ContinuousSchema(DoubleCodex), (3.14 + 6.28 + 9.42 + 12.56) * (1.0 / 1))),
    Cell(Position1D("qux"), Content(ContinuousSchema(DoubleCodex), 12.56 * (1.0 / 4))))

  val result16 = List(Cell(Position2D(1, "xyz"), Content(ContinuousSchema(DoubleCodex),
      (3.14 + 6.28 + 9.42 + 12.56) * (1.0 / 2))),
    Cell(Position2D(2, "xyz"), Content(ContinuousSchema(DoubleCodex), (6.28 + 12.56 + 18.84) * (1.0 / 4))),
    Cell(Position2D(3, "xyz"), Content(ContinuousSchema(DoubleCodex), (9.42 + 18.84) * (1.0 / 6))),
    Cell(Position2D(4, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 * (1.0 / 8))))

  val result17 = List(
    Cell(Position1D(1), Content(ContinuousSchema(DoubleCodex), (3.14 + 6.28 + 9.42 + 12.56) * (1.0 / 2))),
    Cell(Position1D(2), Content(ContinuousSchema(DoubleCodex), (6.28 + 12.56 + 18.84) * (1.0 / 4))),
    Cell(Position1D(3), Content(ContinuousSchema(DoubleCodex), (9.42 + 18.84) * (1.0 / 6))),
    Cell(Position1D(4), Content(ContinuousSchema(DoubleCodex), 12.56 * (1.0 / 8))))

  val result18 = List(
    Cell(Position2D("bar", "xyz"), Content(ContinuousSchema(DoubleCodex), (6.28 + 12.56 + 18.84) * (1.0 / 2))),
    Cell(Position2D("baz", "xyz"), Content(ContinuousSchema(DoubleCodex), (9.42 + 18.84) * (1.0 / 3))),
    Cell(Position2D("foo", "xyz"), Content(ContinuousSchema(DoubleCodex),
      (3.14 + 6.28 + 9.42 + 12.56) * (1.0 / 1))),
    Cell(Position2D("qux", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 * (1.0 / 4))))

  val result19 = List(Cell(Position1D("xyz"), Content(ContinuousSchema(DoubleCodex),
    (3.14 + 2 * 6.28 + 2 * 9.42 + 3 * 12.56 + 2 * 18.84) / 3.14)))

  val result20 = List(Cell(Position2D("bar", 1), Content(ContinuousSchema(DoubleCodex), 6.28 / 3.14)),
    Cell(Position2D("bar", 2), Content(ContinuousSchema(DoubleCodex), 12.56 / 3.14)),
    Cell(Position2D("bar", 3), Content(ContinuousSchema(DoubleCodex), 18.84 / 3.14)),
    Cell(Position2D("baz", 1), Content(ContinuousSchema(DoubleCodex), 9.42 / 3.14)),
    Cell(Position2D("baz", 2), Content(ContinuousSchema(DoubleCodex), 18.84 / 3.14)),
    Cell(Position2D("foo", 1), Content(ContinuousSchema(DoubleCodex), 3.14 / 3.14)),
    Cell(Position2D("foo", 2), Content(ContinuousSchema(DoubleCodex), 6.28 / 3.14)),
    Cell(Position2D("foo", 3), Content(ContinuousSchema(DoubleCodex), 9.42 / 3.14)),
    Cell(Position2D("foo", 4), Content(ContinuousSchema(DoubleCodex), 12.56 / 3.14)),
    Cell(Position2D("qux", 1), Content(ContinuousSchema(DoubleCodex), 12.56 / 3.14)))

  val result21 = List(Cell(Position2D("bar", "min"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position2D("baz", "min"), Content(ContinuousSchema(DoubleCodex), 9.42)),
    Cell(Position2D("foo", "min"), Content(ContinuousSchema(DoubleCodex), 3.14)),
    Cell(Position2D("qux", "min"), Content(ContinuousSchema(DoubleCodex), 12.56)))

  val result22 = List(Cell(Position1D("max"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position1D("min"), Content(ContinuousSchema(DoubleCodex), 3.14)))

  val result23 = List(Cell(Position2D("bar", "min"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position2D("baz", "min"), Content(ContinuousSchema(DoubleCodex), 9.42)),
    Cell(Position2D("foo", "min"), Content(ContinuousSchema(DoubleCodex), 3.14)),
    Cell(Position2D("qux", "min"), Content(ContinuousSchema(DoubleCodex), 12.56)))

  val result24 = List(Cell(Position2D(1, "max"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position2D(1, "min"), Content(ContinuousSchema(DoubleCodex), 3.14)),
    Cell(Position2D(2, "max"), Content(ContinuousSchema(DoubleCodex), 18.84)),
    Cell(Position2D(2, "min"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position2D(3, "max"), Content(ContinuousSchema(DoubleCodex), 18.84)),
    Cell(Position2D(3, "min"), Content(ContinuousSchema(DoubleCodex), 9.42)),
    Cell(Position2D(4, "max"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position2D(4, "min"), Content(ContinuousSchema(DoubleCodex), 12.56)))

  val result25 = List(Cell(Position2D(1, "max"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position2D(1, "min"), Content(ContinuousSchema(DoubleCodex), 3.14)),
    Cell(Position2D(2, "max"), Content(ContinuousSchema(DoubleCodex), 18.84)),
    Cell(Position2D(2, "min"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position2D(3, "max"), Content(ContinuousSchema(DoubleCodex), 18.84)),
    Cell(Position2D(3, "min"), Content(ContinuousSchema(DoubleCodex), 9.42)),
    Cell(Position2D(4, "max"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position2D(4, "min"), Content(ContinuousSchema(DoubleCodex), 12.56)))

  val result26 = List(Cell(Position2D("bar", "min"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position2D("baz", "min"), Content(ContinuousSchema(DoubleCodex), 9.42)),
    Cell(Position2D("foo", "min"), Content(ContinuousSchema(DoubleCodex), 3.14)),
    Cell(Position2D("qux", "min"), Content(ContinuousSchema(DoubleCodex), 12.56)))

  val result27 = List(Cell(Position2D("bar", "min"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position2D("baz", "min"), Content(ContinuousSchema(DoubleCodex), 9.42)),
    Cell(Position2D("foo", "min"), Content(ContinuousSchema(DoubleCodex), 3.14)),
    Cell(Position2D("qux", "min"), Content(ContinuousSchema(DoubleCodex), 12.56)))

  val result28 = List(Cell(Position3D(1, "xyz", "max"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D(1, "xyz", "min"), Content(ContinuousSchema(DoubleCodex), 3.14)),
    Cell(Position3D(2, "xyz", "max"), Content(ContinuousSchema(DoubleCodex), 18.84)),
    Cell(Position3D(2, "xyz", "min"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position3D(3, "xyz", "max"), Content(ContinuousSchema(DoubleCodex), 18.84)),
    Cell(Position3D(3, "xyz", "min"), Content(ContinuousSchema(DoubleCodex), 9.42)),
    Cell(Position3D(4, "xyz", "max"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D(4, "xyz", "min"), Content(ContinuousSchema(DoubleCodex), 12.56)))

  val result29 = List(Cell(Position2D(1, "max"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position2D(1, "min"), Content(ContinuousSchema(DoubleCodex), 3.14)),
    Cell(Position2D(2, "max"), Content(ContinuousSchema(DoubleCodex), 18.84)),
    Cell(Position2D(2, "min"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position2D(3, "max"), Content(ContinuousSchema(DoubleCodex), 18.84)),
    Cell(Position2D(3, "min"), Content(ContinuousSchema(DoubleCodex), 9.42)),
    Cell(Position2D(4, "max"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position2D(4, "min"), Content(ContinuousSchema(DoubleCodex), 12.56)))

  val result30 = List(Cell(Position3D("bar", "xyz", "min"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position3D("baz", "xyz", "min"), Content(ContinuousSchema(DoubleCodex), 9.42)),
    Cell(Position3D("foo", "xyz", "min"), Content(ContinuousSchema(DoubleCodex), 3.14)),
    Cell(Position3D("qux", "xyz", "min"), Content(ContinuousSchema(DoubleCodex), 12.56)))

  val result31 = List(Cell(Position2D("xyz", "max"), Content(ContinuousSchema(DoubleCodex), 18.84)),
    Cell(Position2D("xyz", "min"), Content(ContinuousSchema(DoubleCodex), 3.14)))

  val result32 = List(Cell(Position3D("bar", 1, "min"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position3D("bar", 2, "min"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("bar", 3, "min"), Content(ContinuousSchema(DoubleCodex), 18.84)),
    Cell(Position3D("baz", 1, "min"), Content(ContinuousSchema(DoubleCodex), 9.42)),
    Cell(Position3D("baz", 2, "min"), Content(ContinuousSchema(DoubleCodex), 18.84)),
    Cell(Position3D("foo", 1, "min"), Content(ContinuousSchema(DoubleCodex), 3.14)),
    Cell(Position3D("foo", 2, "min"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position3D("foo", 3, "min"), Content(ContinuousSchema(DoubleCodex), 9.42)),
    Cell(Position3D("foo", 4, "min"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("qux", 1, "min"), Content(ContinuousSchema(DoubleCodex), 12.56)))

  val result33 = List(Cell(Position2D("bar", "sum"), Content(ContinuousSchema(DoubleCodex), 6.28 / 2)),
    Cell(Position2D("baz", "sum"), Content(ContinuousSchema(DoubleCodex), 9.42 * (1.0 / 3))),
    Cell(Position2D("foo", "sum"), Content(ContinuousSchema(DoubleCodex), 3.14 / 1)),
    Cell(Position2D("qux", "sum"), Content(ContinuousSchema(DoubleCodex), 12.56 / 4)))

  val result34 = List(Cell(Position1D("sum.1"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position1D("sum.2"), Content(ContinuousSchema(DoubleCodex), 31.40)))

  val result35 = List(Cell(Position2D("bar", "sum"),
      Content(ContinuousSchema(DoubleCodex), (6.28 + 12.56 + 18.84) * (1.0 / 2))),
    Cell(Position2D("baz", "sum"), Content(ContinuousSchema(DoubleCodex), (9.42 + 18.84) * (1.0 / 3))),
    Cell(Position2D("foo", "sum"),
      Content(ContinuousSchema(DoubleCodex), (3.14 + 6.28 + 9.42 + 12.56) * (1.0 / 1))),
    Cell(Position2D("qux", "sum"), Content(ContinuousSchema(DoubleCodex), 12.56 * (1.0 / 4))))

  val result36 = List(Cell(Position2D(1, "sum.1"),
      Content(ContinuousSchema(DoubleCodex), (3.14 + 6.28 + 9.42 + 12.56) * (1.0 / 2))),
    Cell(Position2D(1, "sum.2"), Content(ContinuousSchema(DoubleCodex), 3.14 + 6.28 + 9.42 + 12.56)),
    Cell(Position2D(2, "sum.1"), Content(ContinuousSchema(DoubleCodex), (6.28 + 12.56 + 18.84) * (1.0 / 4))),
    Cell(Position2D(2, "sum.2"), Content(ContinuousSchema(DoubleCodex), 6.28 + 12.56 + 18.84)),
    Cell(Position2D(3, "sum.1"), Content(ContinuousSchema(DoubleCodex), (9.42 + 18.84) * (1.0 / 6))),
    Cell(Position2D(3, "sum.2"), Content(ContinuousSchema(DoubleCodex), 9.42 + 18.84)),
    Cell(Position2D(4, "sum.1"), Content(ContinuousSchema(DoubleCodex), 12.56 * (1.0 / 8))),
    Cell(Position2D(4, "sum.2"), Content(ContinuousSchema(DoubleCodex), 12.56)))

  val result37 = List(Cell(Position2D(1, "sum.1"),
      Content(ContinuousSchema(DoubleCodex), (3.14 + 6.28 + 9.42 + 12.56) * (1.0 / 2))),
    Cell(Position2D(1, "sum.2"), Content(ContinuousSchema(DoubleCodex), 3.14 + 6.28 + 9.42 + 12.56)),
    Cell(Position2D(2, "sum.1"), Content(ContinuousSchema(DoubleCodex), (6.28 + 12.56 + 18.84) * (1.0 / 4))),
    Cell(Position2D(2, "sum.2"), Content(ContinuousSchema(DoubleCodex), 6.28 + 12.56 + 18.84)),
    Cell(Position2D(3, "sum.1"), Content(ContinuousSchema(DoubleCodex), (9.42 + 18.84) * (1.0 / 6))),
    Cell(Position2D(3, "sum.2"), Content(ContinuousSchema(DoubleCodex), 9.42 + 18.84)),
    Cell(Position2D(4, "sum.1"), Content(ContinuousSchema(DoubleCodex), 12.56 * (1.0 / 8))),
    Cell(Position2D(4, "sum.2"), Content(ContinuousSchema(DoubleCodex), 12.56)))

  val result38 = List(Cell(Position2D("bar", "sum"),
      Content(ContinuousSchema(DoubleCodex), (6.28 + 12.56 + 18.84) * (1.0 / 2))),
    Cell(Position2D("baz", "sum"), Content(ContinuousSchema(DoubleCodex), (9.42 + 18.84) * (1.0 / 3))),
    Cell(Position2D("foo", "sum"),
      Content(ContinuousSchema(DoubleCodex), (3.14 + 6.28 + 9.42 + 12.56) * (1.0 / 1))),
    Cell(Position2D("qux", "sum"), Content(ContinuousSchema(DoubleCodex), 12.56 * (1.0 / 4))))

  val result39 = List(Cell(Position2D("bar", "sum"),
      Content(ContinuousSchema(DoubleCodex), (6.28 + 12.56 + 18.84) * (1.0 / 2))),
    Cell(Position2D("baz", "sum"), Content(ContinuousSchema(DoubleCodex), (9.42 + 18.84) * (1.0 / 3))),
    Cell(Position2D("foo", "sum"),
      Content(ContinuousSchema(DoubleCodex), (3.14 + 6.28 + 9.42 + 12.56) * (1.0 / 1))),
    Cell(Position2D("qux", "sum"), Content(ContinuousSchema(DoubleCodex), 12.56 * (1.0 / 4))))

  val result40 = List(Cell(Position3D(1, "xyz", "sum.1"),
      Content(ContinuousSchema(DoubleCodex), (3.14 + 6.28 + 9.42 + 12.56) * (1.0 / 2))),
    Cell(Position3D(1, "xyz", "sum.2"), Content(ContinuousSchema(DoubleCodex), 3.14 + 6.28 + 9.42 + 12.56)),
    Cell(Position3D(2, "xyz", "sum.1"),
      Content(ContinuousSchema(DoubleCodex), (6.28 + 12.56 + 18.84) * (1.0 / 4))),
    Cell(Position3D(2, "xyz", "sum.2"), Content(ContinuousSchema(DoubleCodex), 6.28 + 12.56 + 18.84)),
    Cell(Position3D(3, "xyz", "sum.1"), Content(ContinuousSchema(DoubleCodex), (9.42 + 18.84) * (1.0 / 6))),
    Cell(Position3D(3, "xyz", "sum.2"), Content(ContinuousSchema(DoubleCodex), 9.42 + 18.84)),
    Cell(Position3D(4, "xyz", "sum.1"), Content(ContinuousSchema(DoubleCodex), 12.56 * (1.0 / 8))),
    Cell(Position3D(4, "xyz", "sum.2"), Content(ContinuousSchema(DoubleCodex), 12.56)))

  val result41 = List(Cell(Position2D(1, "sum.1"),
      Content(ContinuousSchema(DoubleCodex), (3.14 + 6.28 + 9.42 + 12.56) * (1.0 / 2))),
    Cell(Position2D(1, "sum.2"), Content(ContinuousSchema(DoubleCodex), 3.14 + 6.28 + 9.42 + 12.56)),
    Cell(Position2D(2, "sum.1"), Content(ContinuousSchema(DoubleCodex), (6.28 + 12.56 + 18.84) * (1.0 / 4))),
    Cell(Position2D(2, "sum.2"), Content(ContinuousSchema(DoubleCodex), 6.28 + 12.56 + 18.84)),
    Cell(Position2D(3, "sum.1"), Content(ContinuousSchema(DoubleCodex), (9.42 + 18.84) * (1.0 / 6))),
    Cell(Position2D(3, "sum.2"), Content(ContinuousSchema(DoubleCodex), 9.42 + 18.84)),
    Cell(Position2D(4, "sum.1"), Content(ContinuousSchema(DoubleCodex), 12.56 * (1.0 / 8))),
    Cell(Position2D(4, "sum.2"), Content(ContinuousSchema(DoubleCodex), 12.56)))

  val result42 = List(Cell(Position3D("bar", "xyz", "sum"),
      Content(ContinuousSchema(DoubleCodex), (6.28 + 12.56 + 18.84) * (1.0 / 2))),
    Cell(Position3D("baz", "xyz", "sum"), Content(ContinuousSchema(DoubleCodex), (9.42 + 18.84) * (1.0 / 3))),
    Cell(Position3D("foo", "xyz", "sum"),
      Content(ContinuousSchema(DoubleCodex), (3.14 + 6.28 + 9.42 + 12.56) * (1.0 / 1))),
    Cell(Position3D("qux", "xyz", "sum"), Content(ContinuousSchema(DoubleCodex), 12.56 * (1.0 / 4))))

  val result43 = List(Cell(Position2D("xyz", "sum.1"), Content(ContinuousSchema(DoubleCodex),
      (3.14 + 2 * 6.28 + 2 * 9.42 + 3 * 12.56 + 2 * 18.84) / 3.14)),
    Cell(Position2D("xyz", "sum.2"), Content(ContinuousSchema(DoubleCodex),
      (3.14 + 2 * 6.28 + 2 * 9.42 + 3 * 12.56 + 2 * 18.84) / 6.28)))

  val result44 = List(Cell(Position3D("bar", 1, "sum"), Content(ContinuousSchema(DoubleCodex), 6.28 / 3.14)),
    Cell(Position3D("bar", 2, "sum"), Content(ContinuousSchema(DoubleCodex), 12.56 / 3.14)),
    Cell(Position3D("bar", 3, "sum"), Content(ContinuousSchema(DoubleCodex), 18.84 / 3.14)),
    Cell(Position3D("baz", 1, "sum"), Content(ContinuousSchema(DoubleCodex), 9.42 / 3.14)),
    Cell(Position3D("baz", 2, "sum"), Content(ContinuousSchema(DoubleCodex), 18.84 / 3.14)),
    Cell(Position3D("foo", 1, "sum"), Content(ContinuousSchema(DoubleCodex), 3.14 / 3.14)),
    Cell(Position3D("foo", 2, "sum"), Content(ContinuousSchema(DoubleCodex), 6.28 / 3.14)),
    Cell(Position3D("foo", 3, "sum"), Content(ContinuousSchema(DoubleCodex), 9.42 / 3.14)),
    Cell(Position3D("foo", 4, "sum"), Content(ContinuousSchema(DoubleCodex), 12.56 / 3.14)),
    Cell(Position3D("qux", 1, "sum"), Content(ContinuousSchema(DoubleCodex), 12.56 / 3.14)))
}

object TestMatrixSummarise {
  case class ExtractWithName[P <: Position](dim: Dimension, name: String)
    extends Extract[P, Map[Position1D, Double], Double] {
    def extract(cell: Cell[P], ext: Map[Position1D, Double]): Option[Double] = {
      ext.get(Position1D(name.format(cell.position(dim).toShortString)))
    }
  }
}

class TestScaldingMatrixSummarise extends TestMatrixSummarise {

  "A Matrix.summarise" should "return its first over aggregates in 2D" in {
    toPipe(num2)
      .summarise(Over(First), Min[Position2D, Position1D](), Default())
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first along aggregates in 2D" in {
    toPipe(num2)
      .summarise(Along(First), Max[Position2D, Position1D](), Default(Reducers(123)))
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its second over aggregates in 2D" in {
    toPipe(num2)
      .summarise(Over(Second), Max[Position2D, Position1D](), Default())
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its second along aggregates in 2D" in {
    toPipe(num2)
      .summarise(Along(Second), Min[Position2D, Position1D](), Default(Reducers(123)))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its first over aggregates in 3D" in {
    toPipe(num3)
      .summarise(Over(First), Min[Position3D, Position1D](), Default())
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its first along aggregates in 3D" in {
    toPipe(num3)
      .summarise(Along(First), Max[Position3D, Position2D](), Default(Reducers(123)))
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its second over aggregates in 3D" in {
    toPipe(num3)
      .summarise(Over(Second), Max[Position3D, Position1D](), Default())
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its second along aggregates in 3D" in {
    toPipe(num3)
      .summarise(Along(Second), Min[Position3D, Position2D](), Default(Reducers(123)))
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its third over aggregates in 3D" in {
    toPipe(num3)
      .summarise(Over(Third), Max[Position3D, Position1D](), Default())
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its third along aggregates in 3D" in {
    toPipe(num3)
      .summarise(Along(Third), Min[Position3D, Position2D](), Default(Reducers(123)))
      .toList.sortBy(_.position) shouldBe result10
  }

  "A Matrix.summariseWithValue" should "return its first over aggregates in 2D" in {
    toPipe(num2)
      .summariseWithValue(Over(First), WeightedSum[Position2D, Position1D, W](ExtractWithDimension(First)),
        ValuePipe(ext), Default())
      .toList.sortBy(_.position) shouldBe result11
  }

  it should "return its first along aggregates in 2D" in {
    toPipe(num2)
      .summariseWithValue(Along(First), WeightedSum[Position2D, Position1D, W](ExtractWithDimension(Second)),
        ValuePipe(ext), Default(Reducers(123)))
      .toList.sortBy(_.position) shouldBe result12
  }

  it should "return its second over aggregates in 2D" in {
    toPipe(num2)
      .summariseWithValue(Over(Second), WeightedSum[Position2D, Position1D, W](ExtractWithDimension(Second)),
        ValuePipe(ext), Default())
      .toList.sortBy(_.position) shouldBe result13
  }

  it should "return its second along aggregates in 2D" in {
    toPipe(num2)
      .summariseWithValue(Along(Second), WeightedSum[Position2D, Position1D, W](ExtractWithDimension(First)),
        ValuePipe(ext), Default(Reducers(123)))
      .toList.sortBy(_.position) shouldBe result14
  }

  it should "return its first over aggregates in 3D" in {
    toPipe(num3)
      .summariseWithValue(Over(First), WeightedSum[Position3D, Position1D, W](ExtractWithDimension(First)),
        ValuePipe(ext), Default())
      .toList.sortBy(_.position) shouldBe result15
  }

  it should "return its first along aggregates in 3D" in {
    toPipe(num3)
      .summariseWithValue(Along(First), WeightedSum[Position3D, Position2D, W](ExtractWithDimension(Second)),
        ValuePipe(ext), Default(Reducers(123)))
      .toList.sortBy(_.position) shouldBe result16
  }

  it should "return its second over aggregates in 3D" in {
    toPipe(num3)
      .summariseWithValue(Over(Second), WeightedSum[Position3D, Position1D, W](ExtractWithDimension(Second)),
        ValuePipe(ext), Default())
      .toList.sortBy(_.position) shouldBe result17
  }

  it should "return its second along aggregates in 3D" in {
    toPipe(num3)
      .summariseWithValue(Along(Second), WeightedSum[Position3D, Position2D, W](ExtractWithDimension(First)),
        ValuePipe(ext), Default(Reducers(123)))
      .toList.sortBy(_.position) shouldBe result18
  }

  it should "return its third over aggregates in 3D" in {
    toPipe(num3)
      .summariseWithValue(Over(Third), WeightedSum[Position3D, Position1D, W](ExtractWithDimension(Third)),
        ValuePipe(ext), Default())
      .toList.sortBy(_.position) shouldBe result19
  }

  it should "return its third along aggregates in 3D" in {
    toPipe(num3)
      .summariseWithValue(Along(Third), WeightedSum[Position3D, Position2D, W](ExtractWithDimension(Third)),
        ValuePipe(ext), Default(Reducers(123)))
      .toList.sortBy(_.position) shouldBe result20
  }

  "A Matrix.summariseAndExpand" should "return its first over aggregates in 1D" in {
    toPipe(num1)
      .summarise(Over(First), Min[Position1D, Position1D]().andThenRelocate(_.position.append("min").toOption),
        Default())
      .toList.sortBy(_.position) shouldBe result21
  }

  it should "return its first along aggregates in 1D" in {
    toPipe(num1)
      .summarise(Along(First), List(Min[Position1D, Position0D]().andThenRelocate(_.position.append("min").toOption),
        Max[Position1D, Position0D]().andThenRelocate(_.position.append("max").toOption)), Default(Reducers(123)))
      .toList.sortBy(_.position) shouldBe result22
  }

  it should "return its first over aggregates in 2D" in {
    toPipe(num2)
      .summarise(Over(First), Min[Position2D, Position1D]().andThenRelocate(_.position.append("min").toOption),
        Default())
      .toList.sortBy(_.position) shouldBe result23
  }

  it should "return its first along aggregates in 2D" in {
    toPipe(num2)
      .summarise(Along(First), List(Min[Position2D, Position1D]().andThenRelocate(_.position.append("min").toOption),
        Max[Position2D, Position1D]().andThenRelocate(_.position.append("max").toOption)), Default(Reducers(123)))
      .toList.sortBy(_.position) shouldBe result24
  }

  it should "return its second over aggregates in 2D" in {
    toPipe(num2)
      .summarise(Over(Second), List(Min[Position2D, Position1D]().andThenRelocate(_.position.append("min").toOption),
        Max[Position2D, Position1D]().andThenRelocate(_.position.append("max").toOption)), Default())
      .toList.sortBy(_.position) shouldBe result25
  }

  it should "return its second along aggregates in 2D" in {
    toPipe(num2)
      .summarise(Along(Second), Min[Position2D, Position1D]().andThenRelocate(_.position.append("min").toOption),
        Default(Reducers(123)))
      .toList.sortBy(_.position) shouldBe result26
  }

  it should "return its first over aggregates in 3D" in {
    toPipe(num3)
      .summarise(Over(First), Min[Position3D, Position1D]().andThenRelocate(_.position.append("min").toOption),
        Default())
      .toList.sortBy(_.position) shouldBe result27
  }

  it should "return its first along aggregates in 3D" in {
    toPipe(num3)
      .summarise(Along(First), List(Min[Position3D, Position2D]().andThenRelocate(_.position.append("min").toOption),
        Max[Position3D, Position2D]().andThenRelocate(_.position.append("max").toOption)), Default(Reducers(123)))
      .toList.sortBy(_.position) shouldBe result28
  }

  it should "return its second over aggregates in 3D" in {
    toPipe(num3)
      .summarise(Over(Second), List(Min[Position3D, Position1D]().andThenRelocate(_.position.append("min").toOption),
        Max[Position3D, Position1D]().andThenRelocate(_.position.append("max").toOption)), Default())
      .toList.sortBy(_.position) shouldBe result29
  }

  it should "return its second along aggregates in 3D" in {
    toPipe(num3)
      .summarise(Along(Second), Min[Position3D, Position2D]().andThenRelocate(_.position.append("min").toOption),
        Default(Reducers(123)))
      .toList.sortBy(_.position) shouldBe result30
  }

  it should "return its third over aggregates in 3D" in {
    toPipe(num3)
      .summarise(Over(Third), List(Min[Position3D, Position1D]().andThenRelocate(_.position.append("min").toOption),
        Max[Position3D, Position1D]().andThenRelocate(_.position.append("max").toOption)), Default())
      .toList.sortBy(_.position) shouldBe result31
  }

  it should "return its third along aggregates in 3D" in {
    toPipe(num3)
      .summarise(Along(Third), Min[Position3D, Position2D]().andThenRelocate(_.position.append("min").toOption),
        Default(Reducers(123)))
      .toList.sortBy(_.position) shouldBe result32
  }

  "A Matrix.summariseAndExpandWithValue" should "return its first over aggregates in 1D" in {
    toPipe(num1)
      .summariseWithValue(Over(First), WeightedSum[Position1D, Position1D, W](ExtractWithDimension(First))
        .andThenRelocateWithValue((c: Cell[Position1D], e: W) => c.position.append("sum").toOption),
        ValuePipe(ext), Default())
      .toList.sortBy(_.position) shouldBe result33
  }

  it should "return its first along aggregates in 1D" in {
    toPipe(num1)
      .summariseWithValue(Along(First), List(
        WeightedSum[Position1D, Position0D, W](ExtractWithDimension(First))
          .andThenRelocateWithValue((c: Cell[Position0D], e: W) => c.position.append("sum.1").toOption),
        WeightedSum[Position1D, Position0D, W](TestMatrixSummarise.ExtractWithName(First, "%1$s.2"))
          .andThenRelocateWithValue((c: Cell[Position0D], e: W) => c.position.append("sum.2").toOption)),
        ValuePipe(ext), Default(Reducers(123)))
      .toList.sortBy(_.position) shouldBe result34
  }

  it should "return its first over aggregates in 2D" in {
    toPipe(num2)
      .summariseWithValue(Over(First), WeightedSum[Position2D, Position1D, W](ExtractWithDimension(First))
          .andThenRelocateWithValue((c: Cell[Position1D], e: W) => c.position.append("sum").toOption),
        ValuePipe(ext), Default())
      .toList.sortBy(_.position) shouldBe result35
  }

  it should "return its first along aggregates in 2D" in {
    toPipe(num2)
      .summariseWithValue(Along(First), List(
        WeightedSum[Position2D, Position1D, W](ExtractWithDimension(Second))
          .andThenRelocateWithValue((c: Cell[Position1D], e: W) => c.position.append("sum.1").toOption),
        WeightedSum[Position2D, Position1D, W](TestMatrixSummarise.ExtractWithName(First, "%1$s.2"))
          .andThenRelocateWithValue((c: Cell[Position1D], e: W) => c.position.append("sum.2").toOption)),
        ValuePipe(ext), Default(Reducers(123)))
      .toList.sortBy(_.position) shouldBe result36
  }

  it should "return its second over aggregates in 2D" in {
    toPipe(num2)
      .summariseWithValue(Over(Second), List(
        WeightedSum[Position2D, Position1D, W](ExtractWithDimension(Second))
          .andThenRelocateWithValue((c: Cell[Position1D], e: W) => c.position.append("sum.1").toOption),
        WeightedSum[Position2D, Position1D, W](TestMatrixSummarise.ExtractWithName(Second, "%1$s.2"))
          .andThenRelocateWithValue((c: Cell[Position1D], e: W) => c.position.append("sum.2").toOption)),
        ValuePipe(ext), Default())
      .toList.sortBy(_.position) shouldBe result37
  }

  it should "return its second along aggregates in 2D" in {
    toPipe(num2)
      .summariseWithValue(Along(Second), WeightedSum[Position2D, Position1D, W](ExtractWithDimension(First))
          .andThenRelocateWithValue((c: Cell[Position1D], e: W) => c.position.append("sum").toOption),
        ValuePipe(ext), Default(Reducers(123)))
      .toList.sortBy(_.position) shouldBe result38
  }

  it should "return its first over aggregates in 3D" in {
    toPipe(num3)
      .summariseWithValue(Over(First), WeightedSum[Position3D, Position1D, W](ExtractWithDimension(First))
          .andThenRelocateWithValue((c: Cell[Position1D], e: W) => c.position.append("sum").toOption),
        ValuePipe(ext), Default())
      .toList.sortBy(_.position) shouldBe result39
  }

  it should "return its first along aggregates in 3D" in {
    toPipe(num3)
      .summariseWithValue(Along(First), List(
        WeightedSum[Position3D, Position2D, W](ExtractWithDimension(Second))
          .andThenRelocateWithValue((c: Cell[Position2D], e: W) => c.position.append("sum.1").toOption),
        WeightedSum[Position3D, Position2D, W](TestMatrixSummarise.ExtractWithName(Second, "%1$s.2"))
          .andThenRelocateWithValue((c: Cell[Position2D], e: W) => c.position.append("sum.2").toOption)),
        ValuePipe(ext), Default(Reducers(123)))
      .toList.sortBy(_.position) shouldBe result40
  }

  it should "return its second over aggregates in 3D" in {
    toPipe(num3)
      .summariseWithValue(Over(Second), List(
        WeightedSum[Position3D, Position1D, W](ExtractWithDimension(Second))
          .andThenRelocateWithValue((c: Cell[Position1D], e: W) => c.position.append("sum.1").toOption),
        WeightedSum[Position3D, Position1D, W](TestMatrixSummarise.ExtractWithName(Second, "%1$s.2"))
          .andThenRelocateWithValue((c: Cell[Position1D], e: W) => c.position.append("sum.2").toOption)),
        ValuePipe(ext), Default())
      .toList.sortBy(_.position) shouldBe result41
  }

  it should "return its second along aggregates in 3D" in {
    toPipe(num3)
      .summariseWithValue(Along(Second),
        WeightedSum[Position3D, Position2D, W](ExtractWithDimension(First))
          .andThenRelocateWithValue((c: Cell[Position2D], e: W) => c.position.append("sum").toOption),
        ValuePipe(ext), Default(Reducers(123)))
      .toList.sortBy(_.position) shouldBe result42
  }

  it should "return its third over aggregates in 3D" in {
    toPipe(num3)
      .summariseWithValue(Over(Third), List(
        WeightedSum[Position3D, Position1D, W](ExtractWithDimension(Third))
          .andThenRelocateWithValue((c: Cell[Position1D], e: W) => c.position.append("sum.1").toOption),
        WeightedSum[Position3D, Position1D, W](TestMatrixSummarise.ExtractWithName(Third, "%1$s.2"))
          .andThenRelocateWithValue((c: Cell[Position1D], e: W) => c.position.append("sum.2").toOption)),
        ValuePipe(ext), Default())
      .toList.sortBy(_.position) shouldBe result43
  }

  it should "return its third along aggregates in 3D" in {
    toPipe(num3)
      .summariseWithValue(Along(Third), WeightedSum[Position3D, Position2D, W](ExtractWithDimension(Third))
          .andThenRelocateWithValue((c: Cell[Position2D], e: W) => c.position.append("sum").toOption),
        ValuePipe(ext), Default(Reducers(123)))
      .toList.sortBy(_.position) shouldBe result44
  }
}

class TestSparkMatrixSummarise extends TestMatrixSummarise {

  "A Matrix.summarise" should "return its first over aggregates in 2D" in {
    toRDD(num2)
      .summarise(Over(First), Min[Position2D, Position1D](), Default())
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first along aggregates in 2D" in {
    toRDD(num2)
      .summarise(Along(First), Max[Position2D, Position1D](), Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its second over aggregates in 2D" in {
    toRDD(num2)
      .summarise(Over(Second), Max[Position2D, Position1D](), Default())
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its second along aggregates in 2D" in {
    toRDD(num2)
      .summarise(Along(Second), Min[Position2D, Position1D](), Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its first over aggregates in 3D" in {
    toRDD(num3)
      .summarise(Over(First), Min[Position3D, Position1D](), Default())
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its first along aggregates in 3D" in {
    toRDD(num3)
      .summarise(Along(First), Max[Position3D, Position2D](), Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its second over aggregates in 3D" in {
    toRDD(num3)
      .summarise(Over(Second), Max[Position3D, Position1D](), Default())
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its second along aggregates in 3D" in {
    toRDD(num3)
      .summarise(Along(Second), Min[Position3D, Position2D](), Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its third over aggregates in 3D" in {
    toRDD(num3)
      .summarise(Over(Third), Max[Position3D, Position1D](), Default())
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its third along aggregates in 3D" in {
    toRDD(num3)
      .summarise(Along(Third), Min[Position3D, Position2D](), Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result10
  }

  "A Matrix.summariseWithValue" should "return its first over aggregates in 2D" in {
    toRDD(num2)
      .summariseWithValue(Over(First), WeightedSum[Position2D, Position1D, W](ExtractWithDimension(First)), ext,
        Default())
      .toList.sortBy(_.position) shouldBe result11
  }

  it should "return its first along aggregates in 2D" in {
    toRDD(num2)
      .summariseWithValue(Along(First), WeightedSum[Position2D, Position1D, W](ExtractWithDimension(Second)), ext,
        Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result12
  }

  it should "return its second over aggregates in 2D" in {
    toRDD(num2)
      .summariseWithValue(Over(Second), WeightedSum[Position2D, Position1D, W](ExtractWithDimension(Second)), ext,
        Default())
      .toList.sortBy(_.position) shouldBe result13
  }

  it should "return its second along aggregates in 2D" in {
    toRDD(num2)
      .summariseWithValue(Along(Second), WeightedSum[Position2D, Position1D, W](ExtractWithDimension(First)), ext,
        Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result14
  }

  it should "return its first over aggregates in 3D" in {
    toRDD(num3)
      .summariseWithValue(Over(First), WeightedSum[Position3D, Position1D, W](ExtractWithDimension(First)), ext,
        Default())
      .toList.sortBy(_.position) shouldBe result15
  }

  it should "return its first along aggregates in 3D" in {
    toRDD(num3)
      .summariseWithValue(Along(First), WeightedSum[Position3D, Position2D, W](ExtractWithDimension(Second)), ext,
        Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result16
  }

  it should "return its second over aggregates in 3D" in {
    toRDD(num3)
      .summariseWithValue(Over(Second), WeightedSum[Position3D, Position1D, W](ExtractWithDimension(Second)), ext,
        Default())
      .toList.sortBy(_.position) shouldBe result17
  }

  it should "return its second along aggregates in 3D" in {
    toRDD(num3)
      .summariseWithValue(Along(Second), WeightedSum[Position3D, Position2D, W](ExtractWithDimension(First)), ext,
        Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result18
  }

  it should "return its third over aggregates in 3D" in {
    toRDD(num3)
      .summariseWithValue(Over(Third), WeightedSum[Position3D, Position1D, W](ExtractWithDimension(Third)), ext,
        Default())
      .toList.sortBy(_.position) shouldBe result19
  }

  it should "return its third along aggregates in 3D" in {
    toRDD(num3)
      .summariseWithValue(Along(Third), WeightedSum[Position3D, Position2D, W](ExtractWithDimension(Third)), ext,
        Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result20
  }

  "A Matrix.summariseAndExpand" should "return its first over aggregates in 1D" in {
    toRDD(num1)
      .summarise(Over(First), Min[Position1D, Position1D]().andThenRelocate(_.position.append("min").toOption),
        Default())
      .toList.sortBy(_.position) shouldBe result21
  }

  it should "return its first along aggregates in 1D" in {
    toRDD(num1)
      .summarise(Along(First), List(Min[Position1D, Position0D]().andThenRelocate(_.position.append("min").toOption),
        Max[Position1D, Position0D]().andThenRelocate(_.position.append("max").toOption)), Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result22
  }

  it should "return its first over aggregates in 2D" in {
    toRDD(num2)
      .summarise(Over(First), Min[Position2D, Position1D]().andThenRelocate(_.position.append("min").toOption),
        Default())
      .toList.sortBy(_.position) shouldBe result23
  }

  it should "return its first along aggregates in 2D" in {
    toRDD(num2)
      .summarise(Along(First), List(Min[Position2D, Position1D]().andThenRelocate(_.position.append("min").toOption),
        Max[Position2D, Position1D]().andThenRelocate(_.position.append("max").toOption)), Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result24
  }

  it should "return its second over aggregates in 2D" in {
    toRDD(num2)
      .summarise(Over(Second), List(Min[Position2D, Position1D]().andThenRelocate(_.position.append("min").toOption),
        Max[Position2D, Position1D]().andThenRelocate(_.position.append("max").toOption)), Default())
      .toList.sortBy(_.position) shouldBe result25
  }

  it should "return its second along aggregates in 2D" in {
    toRDD(num2)
      .summarise(Along(Second), Min[Position2D, Position1D]().andThenRelocate(_.position.append("min").toOption),
        Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result26
  }

  it should "return its first over aggregates in 3D" in {
    toRDD(num3)
      .summarise(Over(First), Min[Position3D, Position1D]().andThenRelocate(_.position.append("min").toOption),
        Default())
      .toList.sortBy(_.position) shouldBe result27
  }

  it should "return its first along aggregates in 3D" in {
    toRDD(num3)
      .summarise(Along(First), List(Min[Position3D, Position2D]().andThenRelocate(_.position.append("min").toOption),
        Max[Position3D, Position2D]().andThenRelocate(_.position.append("max").toOption)), Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result28
  }

  it should "return its second over aggregates in 3D" in {
    toRDD(num3)
      .summarise(Over(Second), List(Min[Position3D, Position1D]().andThenRelocate(_.position.append("min").toOption),
        Max[Position3D, Position1D]().andThenRelocate(_.position.append("max").toOption)), Default())
      .toList.sortBy(_.position) shouldBe result29
  }

  it should "return its second along aggregates in 3D" in {
    toRDD(num3)
      .summarise(Along(Second), Min[Position3D, Position2D]().andThenRelocate(_.position.append("min").toOption),
        Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result30
  }

  it should "return its third over aggregates in 3D" in {
    toRDD(num3)
      .summarise(Over(Third), List(Min[Position3D, Position1D]().andThenRelocate(_.position.append("min").toOption),
        Max[Position3D, Position1D]().andThenRelocate(_.position.append("max").toOption)), Default())
      .toList.sortBy(_.position) shouldBe result31
  }

  it should "return its third along aggregates in 3D" in {
    toRDD(num3)
      .summarise(Along(Third), Min[Position3D, Position2D]().andThenRelocate(_.position.append("min").toOption),
        Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result32
  }

  "A Matrix.summariseAndExpandWithValue" should "return its first over aggregates in 1D" in {
    toRDD(num1)
      .summariseWithValue(Over(First), WeightedSum[Position1D, Position1D, W](ExtractWithDimension(First))
        .andThenRelocateWithValue((c: Cell[Position1D], e: W) => c.position.append("sum").toOption), ext, Default())
      .toList.sortBy(_.position) shouldBe result33
  }

  it should "return its first along aggregates in 1D" in {
    toRDD(num1)
      .summariseWithValue(Along(First), List(
        WeightedSum[Position1D, Position0D, W](ExtractWithDimension(First))
          .andThenRelocateWithValue((c: Cell[Position0D], e: W) => c.position.append("sum.1").toOption),
        WeightedSum[Position1D, Position0D, W](TestMatrixSummarise.ExtractWithName(First, "%1$s.2"))
          .andThenRelocateWithValue((c: Cell[Position0D], e: W) => c.position.append("sum.2").toOption)), ext,
        Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result34
  }

  it should "return its first over aggregates in 2D" in {
    toRDD(num2)
      .summariseWithValue(Over(First), WeightedSum[Position2D, Position1D, W](ExtractWithDimension(First))
        .andThenRelocateWithValue((c: Cell[Position1D], e: W) => c.position.append("sum").toOption), ext, Default())
      .toList.sortBy(_.position) shouldBe result35
  }

  it should "return its first along aggregates in 2D" in {
    toRDD(num2)
      .summariseWithValue(Along(First), List(
        WeightedSum[Position2D, Position1D, W](ExtractWithDimension(Second))
          .andThenRelocateWithValue((c: Cell[Position1D], e: W) => c.position.append("sum.1").toOption),
        WeightedSum[Position2D, Position1D, W](TestMatrixSummarise.ExtractWithName(First, "%1$s.2"))
          .andThenRelocateWithValue((c: Cell[Position1D], e: W) => c.position.append("sum.2").toOption)), ext,
        Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result36
  }

  it should "return its second over aggregates in 2D" in {
    toRDD(num2)
      .summariseWithValue(Over(Second), List(
        WeightedSum[Position2D, Position1D, W](ExtractWithDimension(Second))
          .andThenRelocateWithValue((c: Cell[Position1D], e: W) => c.position.append("sum.1").toOption),
        WeightedSum[Position2D, Position1D, W](TestMatrixSummarise.ExtractWithName(Second, "%1$s.2"))
          .andThenRelocateWithValue((c: Cell[Position1D], e: W) => c.position.append("sum.2").toOption)), ext,
        Default())
      .toList.sortBy(_.position) shouldBe result37
  }

  it should "return its second along aggregates in 2D" in {
    toRDD(num2)
      .summariseWithValue(Along(Second), WeightedSum[Position2D, Position1D, W](ExtractWithDimension(First))
        .andThenRelocateWithValue((c: Cell[Position1D], e: W) => c.position.append("sum").toOption), ext,
      Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result38
  }

  it should "return its first over aggregates in 3D" in {
    toRDD(num3)
      .summariseWithValue(Over(First), WeightedSum[Position3D, Position1D, W](ExtractWithDimension(First))
        .andThenRelocateWithValue((c: Cell[Position1D], e: W) => c.position.append("sum").toOption), ext, Default())
      .toList.sortBy(_.position) shouldBe result39
  }

  it should "return its first along aggregates in 3D" in {
    toRDD(num3)
      .summariseWithValue(Along(First), List(
        WeightedSum[Position3D, Position2D, W](ExtractWithDimension(Second))
          .andThenRelocateWithValue((c: Cell[Position2D], e: W) => c.position.append("sum.1").toOption),
        WeightedSum[Position3D, Position2D, W](TestMatrixSummarise.ExtractWithName(Second, "%1$s.2"))
          .andThenRelocateWithValue((c: Cell[Position2D], e: W) => c.position.append("sum.2").toOption)), ext,
        Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result40
  }

  it should "return its second over aggregates in 3D" in {
    toRDD(num3)
      .summariseWithValue(Over(Second), List(
          WeightedSum[Position3D, Position1D, W](ExtractWithDimension(Second))
            .andThenRelocateWithValue((c: Cell[Position1D], e: W) => c.position.append("sum.1").toOption),
          WeightedSum[Position3D, Position1D, W](TestMatrixSummarise.ExtractWithName(Second, "%1$s.2"))
            .andThenRelocateWithValue((c: Cell[Position1D], e: W) => c.position.append("sum.2").toOption)),
        ext, Default())
      .toList.sortBy(_.position) shouldBe result41
  }

  it should "return its second along aggregates in 3D" in {
    toRDD(num3)
      .summariseWithValue(Along(Second), WeightedSum[Position3D, Position2D, W](ExtractWithDimension(First))
        .andThenRelocateWithValue((c: Cell[Position2D], e: W) => c.position.append("sum").toOption), ext,
      Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result42
  }

  it should "return its third over aggregates in 3D" in {
    toRDD(num3)
      .summariseWithValue(Over(Third), List(
        WeightedSum[Position3D, Position1D, W](ExtractWithDimension(Third))
          .andThenRelocateWithValue((c: Cell[Position1D], e: W) => c.position.append("sum.1").toOption),
        WeightedSum[Position3D, Position1D, W](TestMatrixSummarise.ExtractWithName(Third, "%1$s.2"))
          .andThenRelocateWithValue((c: Cell[Position1D], e: W) => c.position.append("sum.2").toOption)), ext,
        Default())
      .toList.sortBy(_.position) shouldBe result43
  }

  it should "return its third along aggregates in 3D" in {
    toRDD(num3)
      .summariseWithValue(Along(Third), WeightedSum[Position3D, Position2D, W](ExtractWithDimension(Third))
        .andThenRelocateWithValue((c: Cell[Position2D], e: W) => c.position.append("sum").toOption), ext,
      Default(Reducers(12)))
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
    def assign(cell: Cell[P]): TraversableOnce[String] = Some(cell.position(dim).toShortString)
  }

  case class TestPartitionerWithValue[P <: Position]() extends PartitionerWithValue[P, String] {
    type V = Dimension
    def assignWithValue(cell: Cell[P], ext: V): TraversableOnce[String] = Some(cell.position(ext).toShortString)
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

class TestScaldingMatrixSplit extends TestMatrixSplit {

  "A Matrix.split" should "return its first partitions in 1D" in {
    toPipe(data1)
      .split(TestMatrixSplit.TestPartitioner[Position1D](First))
      .toList.sorted shouldBe result1
  }

  it should "return its first partitions in 2D" in {
    toPipe(data2)
      .split(TestMatrixSplit.TestPartitioner[Position2D](First))
      .toList.sorted shouldBe result2
  }

  it should "return its second partitions in 2D" in {
    toPipe(data2)
      .split(TestMatrixSplit.TestPartitioner[Position2D](Second))
      .toList.sorted shouldBe result3
  }

  it should "return its first partitions in 3D" in {
    toPipe(data3)
      .split(TestMatrixSplit.TestPartitioner[Position3D](First))
      .toList.sorted shouldBe result4
  }

  it should "return its second partitions in 3D" in {
    toPipe(data3)
      .split(TestMatrixSplit.TestPartitioner[Position3D](Second))
      .toList.sorted shouldBe result5
  }

  it should "return its third partitions in 3D" in {
    toPipe(data3)
      .split(TestMatrixSplit.TestPartitioner[Position3D](Third))
      .toList.sorted shouldBe result6
  }

  "A Matrix.splitWithValue" should "return its first partitions in 1D" in {
    toPipe(data1)
      .splitWithValue(TestMatrixSplit.TestPartitionerWithValue[Position1D](), ValuePipe(First))
      .toList.sorted shouldBe result7
  }

  it should "return its first partitions in 2D" in {
    toPipe(data2)
      .splitWithValue(TestMatrixSplit.TestPartitionerWithValue[Position2D](), ValuePipe(First))
      .toList.sorted shouldBe result8
  }

  it should "return its second partitions in 2D" in {
    toPipe(data2)
      .splitWithValue(TestMatrixSplit.TestPartitionerWithValue[Position2D](), ValuePipe(Second))
      .toList.sorted shouldBe result9
  }

  it should "return its first partitions in 3D" in {
    toPipe(data3)
      .splitWithValue(TestMatrixSplit.TestPartitionerWithValue[Position3D](), ValuePipe(First))
      .toList.sorted shouldBe result10
  }

  it should "return its second partitions in 3D" in {
    toPipe(data3)
      .splitWithValue(TestMatrixSplit.TestPartitionerWithValue[Position3D](), ValuePipe(Second))
      .toList.sorted shouldBe result11
  }

  it should "return its third partitions in 3D" in {
    toPipe(data3)
      .splitWithValue(TestMatrixSplit.TestPartitionerWithValue[Position3D](), ValuePipe(Third))
      .toList.sorted shouldBe result12
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

trait TestMatrixSubset extends TestMatrix {

  val ext = "foo"

  val result1 = List(Cell(Position1D("foo"), Content(OrdinalSchema(StringCodex), "3.14")))

  val result2 = List(Cell(Position2D("bar", 2), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position2D("baz", 2), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position2D("foo", 1), Content(OrdinalSchema(StringCodex), "3.14")),
    Cell(Position2D("foo", 2), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position2D("foo", 3), Content(NominalSchema(StringCodex), "9.42")),
    Cell(Position2D("foo", 4), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))

  val result3 = List(Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("baz", 2, "xyz"), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position3D("foo", 1, "xyz"), Content(OrdinalSchema(StringCodex), "3.14")),
    Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position3D("foo", 3, "xyz"), Content(NominalSchema(StringCodex), "9.42")),
    Cell(Position3D("foo", 4, "xyz"), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))

  val result4 = List(Cell(Position1D("foo"), Content(OrdinalSchema(StringCodex), "3.14")))

  val result5 = List(Cell(Position2D("foo", 1), Content(OrdinalSchema(StringCodex), "3.14")),
    Cell(Position2D("foo", 2), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position2D("foo", 3), Content(NominalSchema(StringCodex), "9.42")),
    Cell(Position2D("foo", 4), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))

  val result6 = List(Cell(Position3D("foo", 1, "xyz"), Content(OrdinalSchema(StringCodex), "3.14")),
    Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position3D("foo", 3, "xyz"), Content(NominalSchema(StringCodex), "9.42")),
    Cell(Position3D("foo", 4, "xyz"), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))
}

object TestMatrixSubset {

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

class TestScaldingMatrixSubset extends TestMatrixSubset {

  "A Matrix.subset" should "return its sampled data in 1D" in {
    toPipe(data1)
      .subset(TestMatrixSubset.TestSampler[Position1D]())
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its sampled data in 2D" in {
    toPipe(data2)
      .subset(TestMatrixSubset.TestSampler[Position2D]())
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its sampled data in 3D" in {
    toPipe(data3)
      .subset(TestMatrixSubset.TestSampler[Position3D]())
      .toList.sortBy(_.position) shouldBe result3
  }

  "A Matrix.subsetWithValue" should "return its sampled data in 1D" in {
    toPipe(data1)
      .subsetWithValue(TestMatrixSubset.TestSamplerWithValue[Position1D](), ValuePipe(ext))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its sampled data in 2D" in {
    toPipe(data2)
      .subsetWithValue(TestMatrixSubset.TestSamplerWithValue[Position2D](), ValuePipe(ext))
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its sampled data in 3D" in {
    toPipe(data3)
      .subsetWithValue(TestMatrixSubset.TestSamplerWithValue[Position3D](), ValuePipe(ext))
      .toList.sortBy(_.position) shouldBe result6
  }
}

class TestSparkMatrixSubset extends TestMatrixSubset {

  "A Matrix.subset" should "return its sampled data in 1D" in {
    toRDD(data1)
      .subset(TestMatrixSubset.TestSampler[Position1D]())
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its sampled data in 2D" in {
    toRDD(data2)
      .subset(TestMatrixSubset.TestSampler[Position2D]())
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its sampled data in 3D" in {
    toRDD(data3)
      .subset(TestMatrixSubset.TestSampler[Position3D]())
      .toList.sortBy(_.position) shouldBe result3
  }

  "A Matrix.subsetWithValue" should "return its sampled data in 1D" in {
    toRDD(data1)
      .subsetWithValue(TestMatrixSubset.TestSamplerWithValue[Position1D](), ext)
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its sampled data in 2D" in {
    toRDD(data2)
      .subsetWithValue(TestMatrixSubset.TestSamplerWithValue[Position2D](), ext)
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its sampled data in 3D" in {
    toRDD(data3)
      .subsetWithValue(TestMatrixSubset.TestSamplerWithValue[Position3D](), ext)
      .toList.sortBy(_.position) shouldBe result6
  }
}

class TestMatrixDomain extends TestMatrix {

  val dataA = List(Cell(Position1D(1), Content(ContinuousSchema(DoubleCodex), 3.14)),
    Cell(Position1D(2), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position1D(3), Content(ContinuousSchema(DoubleCodex), 9.42)))

  val dataB = List(Cell(Position2D(1, 3), Content(ContinuousSchema(DoubleCodex), 3.14)),
    Cell(Position2D(2, 2), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position2D(3, 1), Content(ContinuousSchema(DoubleCodex), 9.42)))

  val dataC = List(Cell(Position3D(1, 1, 1), Content(ContinuousSchema(DoubleCodex), 3.14)),
    Cell(Position3D(2, 2, 2), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position3D(3, 3, 3), Content(ContinuousSchema(DoubleCodex), 9.42)),
    Cell(Position3D(1, 2, 3), Content(ContinuousSchema(DoubleCodex), 0)))

  val dataD = List(Cell(Position4D(1, 4, 2, 3), Content(ContinuousSchema(DoubleCodex), 3.14)),
    Cell(Position4D(2, 3, 1, 4), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position4D(3, 2, 4, 1), Content(ContinuousSchema(DoubleCodex), 9.42)),
    Cell(Position4D(4, 1, 3, 2), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position4D(1, 2, 3, 4), Content(ContinuousSchema(DoubleCodex), 0)))

  val dataE = List(Cell(Position5D(1, 5, 4, 3, 2), Content(ContinuousSchema(DoubleCodex), 3.14)),
    Cell(Position5D(2, 1, 5, 4, 3), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position5D(3, 2, 1, 5, 4), Content(ContinuousSchema(DoubleCodex), 9.42)),
    Cell(Position5D(4, 3, 2, 1, 5), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position5D(5, 4, 3, 2, 1), Content(ContinuousSchema(DoubleCodex), 18.84)),
    Cell(Position5D(1, 2, 3, 4, 5), Content(ContinuousSchema(DoubleCodex), 0)))

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

class TestScaldingMatrixDomain extends TestMatrixDomain {

  "A Matrix.domain" should "return its domain in 1D" in {
    toPipe(dataA)
      .domain(Default())
      .toList.sorted shouldBe result1
  }

  it should "return its domain in 2D" in {
    toPipe(dataB)
      .domain(Default())
      .toList.sorted shouldBe result2
  }

  it should "return its domain in 3D" in {
    toPipe(dataC)
      .domain(Default())
      .toList.sorted shouldBe result3
  }

  it should "return its domain in 4D" in {
    toPipe(dataD)
      .domain(Default())
      .toList.sorted shouldBe result4
  }

  it should "return its domain in 5D" in {
    toPipe(dataE)
      .domain(Default())
      .toList.sorted shouldBe result5
  }
}

class TestSparkMatrixDomain extends TestMatrixDomain {

  "A Matrix.domain" should "return its domain in 1D" in {
    toRDD(dataA)
      .domain(Default())
      .toList.sorted shouldBe result1
  }

  it should "return its domain in 2D" in {
    toRDD(dataB)
      .domain(Default())
      .toList.sorted shouldBe result2
  }

  it should "return its domain in 3D" in {
    toRDD(dataC)
      .domain(Default())
      .toList.sorted shouldBe result3
  }

  it should "return its domain in 4D" in {
    toRDD(dataD)
      .domain(Default())
      .toList.sorted shouldBe result4
  }

  it should "return its domain in 5D" in {
    toRDD(dataE)
      .domain(Default())
      .toList.sorted shouldBe result5
  }
}

trait TestMatrixJoin extends TestMatrix {

  val dataA = List(Cell(Position2D("bar", 5), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position2D("baz", 5), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position2D("qux", 5), Content(OrdinalSchema(StringCodex), "12.56")),
    Cell(Position2D("bar", 6), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position2D("baz", 6), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position2D("bar", 7), Content(OrdinalSchema(LongCodex), 19)))

  val dataB = List(Cell(Position2D("foo.2", 2), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position2D("bar.2", 2), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position2D("baz.2", 2), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position2D("foo.2", 3), Content(NominalSchema(StringCodex), "9.42")),
    Cell(Position2D("bar.2", 3), Content(OrdinalSchema(LongCodex), 19)))

  val dataC = List(Cell(Position2D("foo.2", 2), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position2D("bar.2", 2), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position2D("baz.2", 2), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position2D("foo.2", 3), Content(NominalSchema(StringCodex), "9.42")),
    Cell(Position2D("bar.2", 3), Content(OrdinalSchema(LongCodex), 19)))

  val dataD = List(Cell(Position2D("bar", 5), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position2D("baz", 5), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position2D("qux", 5), Content(OrdinalSchema(StringCodex), "12.56")),
    Cell(Position2D("bar", 6), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position2D("baz", 6), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position2D("bar", 7), Content(OrdinalSchema(LongCodex), 19)))

  val dataE = List(Cell(Position3D("bar", 5, "xyz"), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position3D("baz", 5, "xyz"), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position3D("qux", 5, "xyz"), Content(OrdinalSchema(StringCodex), "12.56")),
    Cell(Position3D("bar", 6, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("baz", 6, "xyz"), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position3D("bar", 7, "xyz"), Content(OrdinalSchema(LongCodex), 19)))

  val dataF = List(Cell(Position3D("foo.2", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position3D("bar.2", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("baz.2", 2, "xyz"), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position3D("foo.2", 3, "xyz"), Content(NominalSchema(StringCodex), "9.42")),
    Cell(Position3D("bar.2", 3, "xyz"), Content(OrdinalSchema(LongCodex), 19)))

  val dataG = List(Cell(Position3D("foo.2", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position3D("bar.2", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("baz.2", 2, "xyz"), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position3D("foo.2", 3, "xyz"), Content(NominalSchema(StringCodex), "9.42")),
    Cell(Position3D("bar.2", 3, "xyz"), Content(OrdinalSchema(LongCodex), 19)))

  val dataH = List(Cell(Position3D("bar", 5, "xyz"), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position3D("baz", 5, "xyz"), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position3D("qux", 5, "xyz"), Content(OrdinalSchema(StringCodex), "12.56")),
    Cell(Position3D("bar", 6, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("baz", 6, "xyz"), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position3D("bar", 7, "xyz"), Content(OrdinalSchema(LongCodex), 19)))

  val dataI = List(Cell(Position3D("foo.2", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position3D("bar.2", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("baz.2", 2, "xyz"), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position3D("foo.2", 3, "xyz"), Content(NominalSchema(StringCodex), "9.42")),
    Cell(Position3D("bar.2", 3, "xyz"), Content(OrdinalSchema(LongCodex), 19)))

  val dataJ = List(Cell(Position3D("bar", 1, "xyz.2"), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position3D("baz", 1, "xyz.2"), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position3D("qux", 1, "xyz.2"), Content(OrdinalSchema(StringCodex), "12.56")),
    Cell(Position3D("bar", 2, "xyz.2"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("baz", 2, "xyz.2"), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position3D("bar", 3, "xyz.2"), Content(OrdinalSchema(LongCodex), 19)))

  val result1 = List(Cell(Position2D("bar", 1), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position2D("bar", 2), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position2D("bar", 3), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position2D("bar", 5), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position2D("bar", 6), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position2D("bar", 7), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position2D("baz", 1), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position2D("baz", 2), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position2D("baz", 5), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position2D("baz", 6), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position2D("qux", 1), Content(OrdinalSchema(StringCodex), "12.56")),
    Cell(Position2D("qux", 5), Content(OrdinalSchema(StringCodex), "12.56")))

  val result2 = List(Cell(Position2D("bar", 2), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position2D("bar", 3), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position2D("bar.2", 2), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position2D("bar.2", 3), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position2D("baz", 2), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position2D("baz.2", 2), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position2D("foo", 2), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position2D("foo", 3), Content(NominalSchema(StringCodex), "9.42")),
    Cell(Position2D("foo.2", 2), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position2D("foo.2", 3), Content(NominalSchema(StringCodex), "9.42")))

  val result3 = List(Cell(Position2D("bar", 2), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position2D("bar", 3), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position2D("bar.2", 2), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position2D("bar.2", 3), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position2D("baz", 2), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position2D("baz.2", 2), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position2D("foo", 2), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position2D("foo", 3), Content(NominalSchema(StringCodex), "9.42")),
    Cell(Position2D("foo.2", 2), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position2D("foo.2", 3), Content(NominalSchema(StringCodex), "9.42")))

  val result4 = List(Cell(Position2D("bar", 1), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position2D("bar", 2), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position2D("bar", 3), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position2D("bar", 5), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position2D("bar", 6), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position2D("bar", 7), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position2D("baz", 1), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position2D("baz", 2), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position2D("baz", 5), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position2D("baz", 6), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position2D("qux", 1), Content(OrdinalSchema(StringCodex), "12.56")),
    Cell(Position2D("qux", 5), Content(OrdinalSchema(StringCodex), "12.56")))

  val result5 = List(Cell(Position3D("bar", 1, "xyz"), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("bar", 3, "xyz"), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position3D("bar", 5, "xyz"), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position3D("bar", 6, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("bar", 7, "xyz"), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position3D("baz", 1, "xyz"), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position3D("baz", 2, "xyz"), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position3D("baz", 5, "xyz"), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position3D("baz", 6, "xyz"), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position3D("qux", 1, "xyz"), Content(OrdinalSchema(StringCodex), "12.56")),
    Cell(Position3D("qux", 5, "xyz"), Content(OrdinalSchema(StringCodex), "12.56")))

  val result6 = List(Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("bar", 3, "xyz"), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position3D("bar.2", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("bar.2", 3, "xyz"), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position3D("baz", 2, "xyz"), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position3D("baz.2", 2, "xyz"), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position3D("foo", 3, "xyz"), Content(NominalSchema(StringCodex), "9.42")),
    Cell(Position3D("foo.2", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position3D("foo.2", 3, "xyz"), Content(NominalSchema(StringCodex), "9.42")))

  val result7 = List(Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("bar", 3, "xyz"), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position3D("bar.2", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("bar.2", 3, "xyz"), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position3D("baz", 2, "xyz"), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position3D("baz.2", 2, "xyz"), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position3D("foo", 3, "xyz"), Content(NominalSchema(StringCodex), "9.42")),
    Cell(Position3D("foo.2", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position3D("foo.2", 3, "xyz"), Content(NominalSchema(StringCodex), "9.42")))

  val result8 = List(Cell(Position3D("bar", 1, "xyz"), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("bar", 3, "xyz"), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position3D("bar", 5, "xyz"), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position3D("bar", 6, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("bar", 7, "xyz"), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position3D("baz", 1, "xyz"), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position3D("baz", 2, "xyz"), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position3D("baz", 5, "xyz"), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position3D("baz", 6, "xyz"), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position3D("qux", 1, "xyz"), Content(OrdinalSchema(StringCodex), "12.56")),
    Cell(Position3D("qux", 5, "xyz"), Content(OrdinalSchema(StringCodex), "12.56")))

  val result9 = List(Cell(Position3D("bar", 1, "xyz"), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("bar", 3, "xyz"), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position3D("bar.2", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("bar.2", 3, "xyz"), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position3D("baz", 1, "xyz"), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position3D("baz", 2, "xyz"), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position3D("baz.2", 2, "xyz"), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position3D("foo", 1, "xyz"), Content(OrdinalSchema(StringCodex), "3.14")),
    Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position3D("foo", 3, "xyz"), Content(NominalSchema(StringCodex), "9.42")),
    Cell(Position3D("foo", 4, "xyz"), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
    Cell(Position3D("foo.2", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position3D("foo.2", 3, "xyz"), Content(NominalSchema(StringCodex), "9.42")),
    Cell(Position3D("qux", 1, "xyz"), Content(OrdinalSchema(StringCodex), "12.56")))

  val result10 = List(Cell(Position3D("bar", 1, "xyz"), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position3D("bar", 1, "xyz.2"), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("bar", 2, "xyz.2"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("bar", 3, "xyz"), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position3D("bar", 3, "xyz.2"), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position3D("baz", 1, "xyz"), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position3D("baz", 1, "xyz.2"), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position3D("baz", 2, "xyz"), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position3D("baz", 2, "xyz.2"), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position3D("qux", 1, "xyz"), Content(OrdinalSchema(StringCodex), "12.56")),
    Cell(Position3D("qux", 1, "xyz.2"), Content(OrdinalSchema(StringCodex), "12.56")))
}

class TestScaldingMatrixJoin extends TestMatrixJoin {

  "A Matrix.join" should "return its first over join in 2D" in {
    toPipe(data2)
      .join(Over(First), toPipe(dataA), InMemory())
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first along join in 2D" in {
    toPipe(dataB)
      .join(Along(First), toPipe(data2), InMemory(Reducers(123)))
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its second over join in 2D" in {
    toPipe(dataC)
      .join(Over(Second), toPipe(data2), Default())
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its second along join in 2D" in {
    toPipe(data2)
      .join(Along(Second), toPipe(dataD), Default(Reducers(456)))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its first over join in 3D" in {
    toPipe(data3)
      .join(Over(First), toPipe(dataE), Default(Reducers(123), Reducers(456)))
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its first along join in 3D" in {
    toPipe(dataF)
      .join(Along(First), toPipe(data3), Unbalanced(Reducers(456)))
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its second over join in 3D" in {
    toPipe(dataG)
      .join(Over(Second), toPipe(data3), Unbalanced(Reducers(123), Reducers(456)))
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its second along join in 3D" in {
    toPipe(data3)
      .join(Along(Second), toPipe(dataH), InMemory())
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its third over join in 3D" in {
    toPipe(dataI)
      .join(Over(Third), toPipe(data3), InMemory(Reducers(123)))
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its third along join in 3D" in {
    toPipe(data3)
      .join(Along(Third), toPipe(dataJ), Default())
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return empty data - InMemory" in {
    toPipe(data3)
      .join(Along(Third), TypedPipe.empty, InMemory())
      .toList.sortBy(_.position) shouldBe List()
  }

  it should "return empty data - Default" in {
    toPipe(data3)
      .join(Along(Third), TypedPipe.empty, Default())
      .toList.sortBy(_.position) shouldBe List()
  }
}

class TestSparkMatrixJoin extends TestMatrixJoin {

  "A Matrix.join" should "return its first over join in 2D" in {
    toRDD(data2)
      .join(Over(First), toRDD(dataA), Default())
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first along join in 2D" in {
    toRDD(dataB)
      .join(Along(First), toRDD(data2), Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its second over join in 2D" in {
    toRDD(dataC)
      .join(Over(Second), toRDD(data2), Default(Reducers(12), Reducers(34)))
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its second along join in 2D" in {
    toRDD(data2)
      .join(Along(Second), toRDD(dataD), Default())
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its first over join in 3D" in {
    toRDD(data3)
      .join(Over(First), toRDD(dataE), Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its first along join in 3D" in {
    toRDD(dataF)
      .join(Along(First), toRDD(data3), Default(Reducers(12), Reducers(34)))
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its second over join in 3D" in {
    toRDD(dataG)
      .join(Over(Second), toRDD(data3), Default())
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its second along join in 3D" in {
    toRDD(data3)
      .join(Along(Second), toRDD(dataH), Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its third over join in 3D" in {
    toRDD(dataI)
      .join(Over(Third), toRDD(data3), Default(Reducers(12), Reducers(34)))
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its third along join in 3D" in {
    toRDD(data3)
      .join(Along(Third), toRDD(dataJ), Default())
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return empty data - Default" in {
    toRDD(data3)
      .join(Along(Third), toRDD(List.empty[Cell[Position3D]]), Default())
      .toList.sortBy(_.position) shouldBe List()
  }
}

trait TestMatrixUnique extends TestMatrix {

  val result1 = List(Content(OrdinalSchema(StringCodex), "12.56"),
    Content(OrdinalSchema(StringCodex), "3.14"),
    Content(OrdinalSchema(StringCodex), "6.28"),
    Content(OrdinalSchema(StringCodex), "9.42"))

  val result2 = List((Position1D("bar"), Content(OrdinalSchema(StringCodex), "6.28")),
    (Position1D("baz"), Content(OrdinalSchema(StringCodex), "9.42")),
    (Position1D("foo"), Content(OrdinalSchema(StringCodex), "3.14")),
    (Position1D("qux"), Content(OrdinalSchema(StringCodex), "12.56")))

  val result3 = List(Content(ContinuousSchema(DoubleCodex), 12.56),
    Content(ContinuousSchema(DoubleCodex), 6.28),
    Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00")),
    Content(DiscreteSchema(LongCodex), 19),
    Content(NominalSchema(StringCodex), "9.42"),
    Content(OrdinalSchema(LongCodex), 19),
    Content(OrdinalSchema(StringCodex), "12.56"),
    Content(OrdinalSchema(StringCodex), "3.14"),
    Content(OrdinalSchema(StringCodex), "6.28"),
    Content(OrdinalSchema(StringCodex), "9.42"))

  val result4 = List((Position1D("bar"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    (Position1D("bar"), Content(OrdinalSchema(LongCodex), 19)),
    (Position1D("bar"), Content(OrdinalSchema(StringCodex), "6.28")),
    (Position1D("baz"), Content(DiscreteSchema(LongCodex), 19)),
    (Position1D("baz"), Content(OrdinalSchema(StringCodex), "9.42")),
    (Position1D("foo"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    (Position1D("foo"), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
    (Position1D("foo"), Content(NominalSchema(StringCodex), "9.42")),
    (Position1D("foo"), Content(OrdinalSchema(StringCodex), "3.14")),
    (Position1D("qux"), Content(OrdinalSchema(StringCodex), "12.56")))

  val result5 = List((Position1D(1), Content(OrdinalSchema(StringCodex), "12.56")),
    (Position1D(1), Content(OrdinalSchema(StringCodex), "3.14")),
    (Position1D(1), Content(OrdinalSchema(StringCodex), "6.28")),
    (Position1D(1), Content(OrdinalSchema(StringCodex), "9.42")),
    (Position1D(2), Content(ContinuousSchema(DoubleCodex), 12.56)),
    (Position1D(2), Content(ContinuousSchema(DoubleCodex), 6.28)),
    (Position1D(2), Content(DiscreteSchema(LongCodex), 19)),
    (Position1D(3), Content(NominalSchema(StringCodex), "9.42")),
    (Position1D(3), Content(OrdinalSchema(LongCodex), 19)),
    (Position1D(4), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))

  val result6 = List((Position1D(1), Content(OrdinalSchema(StringCodex), "12.56")),
    (Position1D(1), Content(OrdinalSchema(StringCodex), "3.14")),
    (Position1D(1), Content(OrdinalSchema(StringCodex), "6.28")),
    (Position1D(1), Content(OrdinalSchema(StringCodex), "9.42")),
    (Position1D(2), Content(ContinuousSchema(DoubleCodex), 12.56)),
    (Position1D(2), Content(ContinuousSchema(DoubleCodex), 6.28)),
    (Position1D(2), Content(DiscreteSchema(LongCodex), 19)),
    (Position1D(3), Content(NominalSchema(StringCodex), "9.42")),
    (Position1D(3), Content(OrdinalSchema(LongCodex), 19)),
    (Position1D(4), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))

  val result7 = List((Position1D("bar"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    (Position1D("bar"), Content(OrdinalSchema(LongCodex), 19)),
    (Position1D("bar"), Content(OrdinalSchema(StringCodex), "6.28")),
    (Position1D("baz"), Content(DiscreteSchema(LongCodex), 19)),
    (Position1D("baz"), Content(OrdinalSchema(StringCodex), "9.42")),
    (Position1D("foo"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    (Position1D("foo"), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
    (Position1D("foo"), Content(NominalSchema(StringCodex), "9.42")),
    (Position1D("foo"), Content(OrdinalSchema(StringCodex), "3.14")),
    (Position1D("qux"), Content(OrdinalSchema(StringCodex), "12.56")))

  val result8 = List(Content(ContinuousSchema(DoubleCodex), 12.56),
    Content(ContinuousSchema(DoubleCodex), 6.28),
    Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00")),
    Content(DiscreteSchema(LongCodex), 19),
    Content(NominalSchema(StringCodex), "9.42"),
    Content(OrdinalSchema(LongCodex), 19),
    Content(OrdinalSchema(StringCodex), "12.56"),
    Content(OrdinalSchema(StringCodex), "3.14"),
    Content(OrdinalSchema(StringCodex), "6.28"),
    Content(OrdinalSchema(StringCodex), "9.42"))

  val result9 = List((Position1D("bar"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    (Position1D("bar"), Content(OrdinalSchema(LongCodex), 19)),
    (Position1D("bar"), Content(OrdinalSchema(StringCodex), "6.28")),
    (Position1D("baz"), Content(DiscreteSchema(LongCodex), 19)),
    (Position1D("baz"), Content(OrdinalSchema(StringCodex), "9.42")),
    (Position1D("foo"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    (Position1D("foo"), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
    (Position1D("foo"), Content(NominalSchema(StringCodex), "9.42")),
    (Position1D("foo"), Content(OrdinalSchema(StringCodex), "3.14")),
    (Position1D("qux"), Content(OrdinalSchema(StringCodex), "12.56")))

  val result10 = List((Position2D(1, "xyz"), Content(OrdinalSchema(StringCodex), "12.56")),
    (Position2D(1, "xyz"), Content(OrdinalSchema(StringCodex), "3.14")),
    (Position2D(1, "xyz"), Content(OrdinalSchema(StringCodex), "6.28")),
    (Position2D(1, "xyz"), Content(OrdinalSchema(StringCodex), "9.42")),
    (Position2D(2, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    (Position2D(2, "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    (Position2D(2, "xyz"), Content(DiscreteSchema(LongCodex), 19)),
    (Position2D(3, "xyz"), Content(NominalSchema(StringCodex), "9.42")),
    (Position2D(3, "xyz"), Content(OrdinalSchema(LongCodex), 19)),
    (Position2D(4, "xyz"), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))

  val result11 = List((Position1D(1), Content(OrdinalSchema(StringCodex), "12.56")),
    (Position1D(1), Content(OrdinalSchema(StringCodex), "3.14")),
    (Position1D(1), Content(OrdinalSchema(StringCodex), "6.28")),
    (Position1D(1), Content(OrdinalSchema(StringCodex), "9.42")),
    (Position1D(2), Content(ContinuousSchema(DoubleCodex), 12.56)),
    (Position1D(2), Content(ContinuousSchema(DoubleCodex), 6.28)),
    (Position1D(2), Content(DiscreteSchema(LongCodex), 19)),
    (Position1D(3), Content(NominalSchema(StringCodex), "9.42")),
    (Position1D(3), Content(OrdinalSchema(LongCodex), 19)),
    (Position1D(4), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))

  val result12 = List((Position2D("bar", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    (Position2D("bar", "xyz"), Content(OrdinalSchema(LongCodex), 19)),
    (Position2D("bar", "xyz"), Content(OrdinalSchema(StringCodex), "6.28")),
    (Position2D("baz", "xyz"), Content(DiscreteSchema(LongCodex), 19)),
    (Position2D("baz", "xyz"), Content(OrdinalSchema(StringCodex), "9.42")),
    (Position2D("foo", "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    (Position2D("foo", "xyz"), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
    (Position2D("foo", "xyz"), Content(NominalSchema(StringCodex), "9.42")),
    (Position2D("foo", "xyz"), Content(OrdinalSchema(StringCodex), "3.14")),
    (Position2D("qux", "xyz"), Content(OrdinalSchema(StringCodex), "12.56")))

  val result13 = List((Position1D("xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    (Position1D("xyz"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    (Position1D("xyz"), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
    (Position1D("xyz"), Content(DiscreteSchema(LongCodex), 19)),
    (Position1D("xyz"), Content(NominalSchema(StringCodex), "9.42")),
    (Position1D("xyz"), Content(OrdinalSchema(LongCodex), 19)),
    (Position1D("xyz"), Content(OrdinalSchema(StringCodex), "12.56")),
    (Position1D("xyz"), Content(OrdinalSchema(StringCodex), "3.14")),
    (Position1D("xyz"), Content(OrdinalSchema(StringCodex), "6.28")),
    (Position1D("xyz"), Content(OrdinalSchema(StringCodex), "9.42")))

  val result14 = List((Position2D("bar", 1), Content(OrdinalSchema(StringCodex), "6.28")),
    (Position2D("bar", 2), Content(ContinuousSchema(DoubleCodex), 12.56)),
    (Position2D("bar", 3), Content(OrdinalSchema(LongCodex), 19)),
    (Position2D("baz", 1), Content(OrdinalSchema(StringCodex), "9.42")),
    (Position2D("baz", 2), Content(DiscreteSchema(LongCodex), 19)),
    (Position2D("foo", 1), Content(OrdinalSchema(StringCodex), "3.14")),
    (Position2D("foo", 2), Content(ContinuousSchema(DoubleCodex), 6.28)),
    (Position2D("foo", 3), Content(NominalSchema(StringCodex), "9.42")),
    (Position2D("foo", 4), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
    (Position2D("qux", 1), Content(OrdinalSchema(StringCodex), "12.56")))
}

class TestScaldingMatrixUnique extends TestMatrixUnique {

  "A Matrix.unique" should "return its content in 1D" in {
    toPipe(data1)
      .unique(Default())
      .toList.sortBy(_.toString) shouldBe result1
  }

  it should "return its first over content in 1D" in {
    toPipe(data1)
      .uniqueByPositions(Over(First), Default(Reducers(123)))
      .toList.sortBy(_.toString) shouldBe result2
  }

  it should "return its content in 2D" in {
    toPipe(data2)
      .unique(Default())
      .toList.sortBy(_.toString) shouldBe result3
  }

  it should "return its first over content in 2D" in {
    toPipe(data2)
      .uniqueByPositions(Over(First), Default(Reducers(123)))
      .toList.sortBy(_.toString) shouldBe result4
  }

  it should "return its first along content in 2D" in {
    toPipe(data2)
      .uniqueByPositions(Along(First), Default())
      .toList.sortBy(_.toString) shouldBe result5
  }

  it should "return its second over content in 2D" in {
    toPipe(data2)
      .uniqueByPositions(Over(Second), Default(Reducers(123)))
      .toList.sortBy(_.toString) shouldBe result6
  }

  it should "return its second along content in 2D" in {
    toPipe(data2)
      .uniqueByPositions(Along(Second), Default())
      .toList.sortBy(_.toString) shouldBe result7
  }

  it should "return its content in 3D" in {
    toPipe(data3)
      .unique(Default(Reducers(123)))
      .toList.sortBy(_.toString) shouldBe result8
  }

  it should "return its first over content in 3D" in {
    toPipe(data3)
      .uniqueByPositions(Over(First), Default())
      .toList.sortBy(_.toString) shouldBe result9
  }

  it should "return its first along content in 3D" in {
    toPipe(data3)
      .uniqueByPositions(Along(First), Default(Reducers(123)))
      .toList.sortBy(_.toString) shouldBe result10
  }

  it should "return its second over content in 3D" in {
    toPipe(data3)
      .uniqueByPositions(Over(Second), Default())
      .toList.sortBy(_.toString) shouldBe result11
  }

  it should "return its second along content in 3D" in {
    toPipe(data3)
      .uniqueByPositions(Along(Second), Default(Reducers(123)))
      .toList.sortBy(_.toString) shouldBe result12
  }

  it should "return its third over content in 3D" in {
    toPipe(data3)
      .uniqueByPositions(Over(Third), Default())
      .toList.sortBy(_.toString) shouldBe result13
  }

  it should "return its third along content in 3D" in {
    toPipe(data3)
      .uniqueByPositions(Along(Third), Default(Reducers(123)))
      .toList.sortBy(_.toString) shouldBe result14
  }
}

class TestSparkMatrixUnique extends TestMatrixUnique {

  "A Matrix.unique" should "return its content in 1D" in {
    toRDD(data1)
      .unique(Default())
      .toList.sortBy(_.toString) shouldBe result1
  }

  it should "return its first over content in 1D" in {
    toRDD(data1)
      .uniqueByPositions(Over(First), Default(Reducers(12)))
      .toList.sortBy(_.toString) shouldBe result2
  }

  it should "return its content in 2D" in {
    toRDD(data2)
      .unique(Default())
      .toList.sortBy(_.toString) shouldBe result3
  }

  it should "return its first over content in 2D" in {
    toRDD(data2)
      .uniqueByPositions(Over(First), Default(Reducers(12)))
      .toList.sortBy(_.toString) shouldBe result4
  }

  it should "return its first along content in 2D" in {
    toRDD(data2)
      .uniqueByPositions(Along(First), Default())
      .toList.sortBy(_.toString) shouldBe result5
  }

  it should "return its second over content in 2D" in {
    toRDD(data2)
      .uniqueByPositions(Over(Second), Default(Reducers(12)))
      .toList.sortBy(_.toString) shouldBe result6
  }

  it should "return its second along content in 2D" in {
    toRDD(data2)
      .uniqueByPositions(Along(Second), Default())
      .toList.sortBy(_.toString) shouldBe result7
  }

  it should "return its content in 3D" in {
    toRDD(data3)
      .unique(Default(Reducers(12)))
      .toList.sortBy(_.toString) shouldBe result8
  }

  it should "return its first over content in 3D" in {
    toRDD(data3)
      .uniqueByPositions(Over(First), Default())
      .toList.sortBy(_.toString) shouldBe result9
  }

  it should "return its first along content in 3D" in {
    toRDD(data3)
      .uniqueByPositions(Along(First), Default(Reducers(12)))
      .toList.sortBy(_.toString) shouldBe result10
  }

  it should "return its second over content in 3D" in {
    toRDD(data3)
      .uniqueByPositions(Over(Second), Default())
      .toList.sortBy(_.toString) shouldBe result11
  }

  it should "return its second along content in 3D" in {
    toRDD(data3)
      .uniqueByPositions(Along(Second), Default(Reducers(12)))
      .toList.sortBy(_.toString) shouldBe result12
  }

  it should "return its third over content in 3D" in {
    toRDD(data3)
      .uniqueByPositions(Over(Third), Default())
      .toList.sortBy(_.toString) shouldBe result13
  }

  it should "return its third along content in 3D" in {
    toRDD(data3)
      .uniqueByPositions(Along(Third), Default(Reducers(12)))
      .toList.sortBy(_.toString) shouldBe result14
  }
}

trait TestMatrixPairwise extends TestMatrix {

  val ext = 1.0

  val dataA = List(Cell(Position1D("bar"), Content(ContinuousSchema(DoubleCodex), 1)),
    Cell(Position1D("baz"), Content(ContinuousSchema(DoubleCodex), 2)))

  val dataB = List(Cell(Position2D("bar", 1), Content(ContinuousSchema(DoubleCodex), 1)),
    Cell(Position2D("bar", 2), Content(ContinuousSchema(DoubleCodex), 2)),
    Cell(Position2D("bar", 3), Content(ContinuousSchema(DoubleCodex), 3)),
    Cell(Position2D("baz", 1), Content(ContinuousSchema(DoubleCodex), 4)),
    Cell(Position2D("baz", 2), Content(ContinuousSchema(DoubleCodex), 5)))

  val dataC = List(Cell(Position2D("bar", 2), Content(ContinuousSchema(DoubleCodex), 1)),
    Cell(Position2D("baz", 2), Content(ContinuousSchema(DoubleCodex), 2)),
    Cell(Position2D("foo", 2), Content(ContinuousSchema(DoubleCodex), 3)),
    Cell(Position2D("foo", 4), Content(ContinuousSchema(DoubleCodex), 4)))

  val dataD = List(Cell(Position2D("bar", 2), Content(ContinuousSchema(DoubleCodex), 1)),
    Cell(Position2D("baz", 2), Content(ContinuousSchema(DoubleCodex), 2)),
    Cell(Position2D("foo", 2), Content(ContinuousSchema(DoubleCodex), 3)),
    Cell(Position2D("foo", 4), Content(ContinuousSchema(DoubleCodex), 4)))

  val dataE = List(Cell(Position2D("bar", 1), Content(ContinuousSchema(DoubleCodex), 1)),
    Cell(Position2D("bar", 2), Content(ContinuousSchema(DoubleCodex), 2)),
    Cell(Position2D("bar", 3), Content(ContinuousSchema(DoubleCodex), 3)),
    Cell(Position2D("baz", 1), Content(ContinuousSchema(DoubleCodex), 4)),
    Cell(Position2D("baz", 2), Content(ContinuousSchema(DoubleCodex), 5)))

  val dataF = List(Cell(Position3D("bar", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 1)),
    Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 2)),
    Cell(Position3D("bar", 3, "xyz"), Content(ContinuousSchema(DoubleCodex), 3)),
    Cell(Position3D("baz", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 4)),
    Cell(Position3D("baz", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 5)))

  val dataG = List(Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 1)),
    Cell(Position3D("baz", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 2)),
    Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 3)),
    Cell(Position3D("foo", 4, "xyz"), Content(ContinuousSchema(DoubleCodex), 4)))

  val dataH = List(Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 1)),
    Cell(Position3D("baz", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 2)),
    Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 3)),
    Cell(Position3D("foo", 4, "xyz"), Content(ContinuousSchema(DoubleCodex), 4)))

  val dataI = List(Cell(Position3D("bar", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 1)),
    Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 2)),
    Cell(Position3D("bar", 3, "xyz"), Content(ContinuousSchema(DoubleCodex), 3)),
    Cell(Position3D("baz", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 4)),
    Cell(Position3D("baz", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 5)))

  val dataJ = List(Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 1)),
    Cell(Position3D("baz", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 2)),
    Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 3)),
    Cell(Position3D("foo", 4, "xyz"), Content(ContinuousSchema(DoubleCodex), 4)))

  val dataK = List(Cell(Position3D("bar", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 1)),
    Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 2)),
    Cell(Position3D("bar", 3, "xyz"), Content(ContinuousSchema(DoubleCodex), 3)),
    Cell(Position3D("baz", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 4)),
    Cell(Position3D("baz", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 5)))

  val dataL = List(Cell(Position1D("bar"), Content(ContinuousSchema(DoubleCodex), 1)),
    Cell(Position1D("baz"), Content(ContinuousSchema(DoubleCodex), 2)))

  val dataM = List(Cell(Position2D("bar", 1), Content(ContinuousSchema(DoubleCodex), 1)),
    Cell(Position2D("bar", 2), Content(ContinuousSchema(DoubleCodex), 2)),
    Cell(Position2D("bar", 3), Content(ContinuousSchema(DoubleCodex), 3)),
    Cell(Position2D("baz", 1), Content(ContinuousSchema(DoubleCodex), 4)),
    Cell(Position2D("baz", 2), Content(ContinuousSchema(DoubleCodex), 5)))

  val dataN = List(Cell(Position2D("bar", 2), Content(ContinuousSchema(DoubleCodex), 1)),
    Cell(Position2D("baz", 2), Content(ContinuousSchema(DoubleCodex), 2)),
    Cell(Position2D("foo", 2), Content(ContinuousSchema(DoubleCodex), 3)),
    Cell(Position2D("foo", 4), Content(ContinuousSchema(DoubleCodex), 4)))

  val dataO = List(Cell(Position2D("bar", 2), Content(ContinuousSchema(DoubleCodex), 1)),
    Cell(Position2D("baz", 2), Content(ContinuousSchema(DoubleCodex), 2)),
    Cell(Position2D("foo", 2), Content(ContinuousSchema(DoubleCodex), 3)),
    Cell(Position2D("foo", 4), Content(ContinuousSchema(DoubleCodex), 4)))

  val dataP = List(Cell(Position2D("bar", 1), Content(ContinuousSchema(DoubleCodex), 1)),
    Cell(Position2D("bar", 2), Content(ContinuousSchema(DoubleCodex), 2)),
    Cell(Position2D("bar", 3), Content(ContinuousSchema(DoubleCodex), 3)),
    Cell(Position2D("baz", 1), Content(ContinuousSchema(DoubleCodex), 4)),
    Cell(Position2D("baz", 2), Content(ContinuousSchema(DoubleCodex), 5)))

  val dataQ = List(Cell(Position3D("bar", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 1)),
    Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 2)),
    Cell(Position3D("bar", 3, "xyz"), Content(ContinuousSchema(DoubleCodex), 3)),
    Cell(Position3D("baz", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 4)),
    Cell(Position3D("baz", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 5)))

  val dataR = List(Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 1)),
    Cell(Position3D("baz", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 2)),
    Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 3)),
    Cell(Position3D("foo", 4, "xyz"), Content(ContinuousSchema(DoubleCodex), 4)))

  val dataS = List(Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 1)),
    Cell(Position3D("baz", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 2)),
    Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 3)),
    Cell(Position3D("foo", 4, "xyz"), Content(ContinuousSchema(DoubleCodex), 4)))

  val dataT = List(Cell(Position3D("bar", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 1)),
    Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 2)),
    Cell(Position3D("bar", 3, "xyz"), Content(ContinuousSchema(DoubleCodex), 3)),
    Cell(Position3D("baz", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 4)),
    Cell(Position3D("baz", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 5)))

  val dataU = List(Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 1)),
    Cell(Position3D("baz", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 2)),
    Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 3)),
    Cell(Position3D("foo", 4, "xyz"), Content(ContinuousSchema(DoubleCodex), 4)))

  val dataV = List(Cell(Position3D("bar", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 1)),
    Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 2)),
    Cell(Position3D("bar", 3, "xyz"), Content(ContinuousSchema(DoubleCodex), 3)),
    Cell(Position3D("baz", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 4)),
    Cell(Position3D("baz", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 5)))

  val result1 = List(Cell(Position1D("(baz+bar)"), Content(ContinuousSchema(DoubleCodex), 9.42 + 6.28)),
    Cell(Position1D("(foo+bar)"), Content(ContinuousSchema(DoubleCodex), 3.14 + 6.28)),
    Cell(Position1D("(foo+baz)"), Content(ContinuousSchema(DoubleCodex), 3.14 + 9.42)),
    Cell(Position1D("(qux+bar)"), Content(ContinuousSchema(DoubleCodex), 12.56 + 6.28)),
    Cell(Position1D("(qux+baz)"), Content(ContinuousSchema(DoubleCodex), 12.56 + 9.42)),
    Cell(Position1D("(qux+foo)"), Content(ContinuousSchema(DoubleCodex), 12.56 + 3.14)))

  val result2 = List(Cell(Position2D("(baz+bar)", 1), Content(ContinuousSchema(DoubleCodex), 9.42 + 6.28)),
    Cell(Position2D("(baz+bar)", 2), Content(ContinuousSchema(DoubleCodex), 18.84 + 12.56)),
    Cell(Position2D("(foo+bar)", 1), Content(ContinuousSchema(DoubleCodex), 3.14 + 6.28)),
    Cell(Position2D("(foo+bar)", 2), Content(ContinuousSchema(DoubleCodex), 6.28 + 12.56)),
    Cell(Position2D("(foo+bar)", 3), Content(ContinuousSchema(DoubleCodex), 9.42 + 18.84)),
    Cell(Position2D("(foo+baz)", 1), Content(ContinuousSchema(DoubleCodex), 3.14 + 9.42)),
    Cell(Position2D("(foo+baz)", 2), Content(ContinuousSchema(DoubleCodex), 6.28 + 18.84)),
    Cell(Position2D("(qux+bar)", 1), Content(ContinuousSchema(DoubleCodex), 12.56 + 6.28)),
    Cell(Position2D("(qux+baz)", 1), Content(ContinuousSchema(DoubleCodex), 12.56 + 9.42)),
    Cell(Position2D("(qux+foo)", 1), Content(ContinuousSchema(DoubleCodex), 12.56 + 3.14)))

  val result3 = List(Cell(Position2D("(2+1)", "bar"), Content(ContinuousSchema(DoubleCodex), 12.56 + 6.28)),
    Cell(Position2D("(2+1)", "baz"), Content(ContinuousSchema(DoubleCodex), 18.84 + 9.42)),
    Cell(Position2D("(2+1)", "foo"), Content(ContinuousSchema(DoubleCodex), 6.28 + 3.14)),
    Cell(Position2D("(2-1)", "bar"), Content(ContinuousSchema(DoubleCodex), 12.56 - 6.28)),
    Cell(Position2D("(2-1)", "baz"), Content(ContinuousSchema(DoubleCodex), 18.84 - 9.42)),
    Cell(Position2D("(2-1)", "foo"), Content(ContinuousSchema(DoubleCodex), 6.28 - 3.14)),
    Cell(Position2D("(3+1)", "bar"), Content(ContinuousSchema(DoubleCodex), 18.84 + 6.28)),
    Cell(Position2D("(3+1)", "foo"), Content(ContinuousSchema(DoubleCodex), 9.42 + 3.14)),
    Cell(Position2D("(3+2)", "bar"), Content(ContinuousSchema(DoubleCodex), 18.84 + 12.56)),
    Cell(Position2D("(3+2)", "foo"), Content(ContinuousSchema(DoubleCodex), 9.42 + 6.28)),
    Cell(Position2D("(3-1)", "bar"), Content(ContinuousSchema(DoubleCodex), 18.84 - 6.28)),
    Cell(Position2D("(3-1)", "foo"), Content(ContinuousSchema(DoubleCodex), 9.42 - 3.14)),
    Cell(Position2D("(3-2)", "bar"), Content(ContinuousSchema(DoubleCodex), 18.84 - 12.56)),
    Cell(Position2D("(3-2)", "foo"), Content(ContinuousSchema(DoubleCodex), 9.42 - 6.28)),
    Cell(Position2D("(4+1)", "foo"), Content(ContinuousSchema(DoubleCodex), 12.56 + 3.14)),
    Cell(Position2D("(4+2)", "foo"), Content(ContinuousSchema(DoubleCodex), 12.56 + 6.28)),
    Cell(Position2D("(4+3)", "foo"), Content(ContinuousSchema(DoubleCodex), 12.56 + 9.42)),
    Cell(Position2D("(4-1)", "foo"), Content(ContinuousSchema(DoubleCodex), 12.56 - 3.14)),
    Cell(Position2D("(4-2)", "foo"), Content(ContinuousSchema(DoubleCodex), 12.56 - 6.28)),
    Cell(Position2D("(4-3)", "foo"), Content(ContinuousSchema(DoubleCodex), 12.56 - 9.42)))

  val result4 = List(Cell(Position2D("(2+1)", "bar"), Content(ContinuousSchema(DoubleCodex), 12.56 + 6.28)),
    Cell(Position2D("(2+1)", "baz"), Content(ContinuousSchema(DoubleCodex), 18.84 + 9.42)),
    Cell(Position2D("(2+1)", "foo"), Content(ContinuousSchema(DoubleCodex), 6.28 + 3.14)),
    Cell(Position2D("(2-1)", "bar"), Content(ContinuousSchema(DoubleCodex), 12.56 - 6.28)),
    Cell(Position2D("(2-1)", "baz"), Content(ContinuousSchema(DoubleCodex), 18.84 - 9.42)),
    Cell(Position2D("(2-1)", "foo"), Content(ContinuousSchema(DoubleCodex), 6.28 - 3.14)),
    Cell(Position2D("(3+1)", "bar"), Content(ContinuousSchema(DoubleCodex), 18.84 + 6.28)),
    Cell(Position2D("(3+1)", "foo"), Content(ContinuousSchema(DoubleCodex), 9.42 + 3.14)),
    Cell(Position2D("(3+2)", "bar"), Content(ContinuousSchema(DoubleCodex), 18.84 + 12.56)),
    Cell(Position2D("(3+2)", "foo"), Content(ContinuousSchema(DoubleCodex), 9.42 + 6.28)),
    Cell(Position2D("(3-1)", "bar"), Content(ContinuousSchema(DoubleCodex), 18.84 - 6.28)),
    Cell(Position2D("(3-1)", "foo"), Content(ContinuousSchema(DoubleCodex), 9.42 - 3.14)),
    Cell(Position2D("(3-2)", "bar"), Content(ContinuousSchema(DoubleCodex), 18.84 - 12.56)),
    Cell(Position2D("(3-2)", "foo"), Content(ContinuousSchema(DoubleCodex), 9.42 - 6.28)),
    Cell(Position2D("(4+1)", "foo"), Content(ContinuousSchema(DoubleCodex), 12.56 + 3.14)),
    Cell(Position2D("(4+2)", "foo"), Content(ContinuousSchema(DoubleCodex), 12.56 + 6.28)),
    Cell(Position2D("(4+3)", "foo"), Content(ContinuousSchema(DoubleCodex), 12.56 + 9.42)),
    Cell(Position2D("(4-1)", "foo"), Content(ContinuousSchema(DoubleCodex), 12.56 - 3.14)),
    Cell(Position2D("(4-2)", "foo"), Content(ContinuousSchema(DoubleCodex), 12.56 - 6.28)),
    Cell(Position2D("(4-3)", "foo"), Content(ContinuousSchema(DoubleCodex), 12.56 - 9.42)))

  val result5 = List(Cell(Position2D("(baz+bar)", 1), Content(ContinuousSchema(DoubleCodex), 9.42 + 6.28)),
    Cell(Position2D("(baz+bar)", 2), Content(ContinuousSchema(DoubleCodex), 18.84 + 12.56)),
    Cell(Position2D("(foo+bar)", 1), Content(ContinuousSchema(DoubleCodex), 3.14 + 6.28)),
    Cell(Position2D("(foo+bar)", 2), Content(ContinuousSchema(DoubleCodex), 6.28 + 12.56)),
    Cell(Position2D("(foo+bar)", 3), Content(ContinuousSchema(DoubleCodex), 9.42 + 18.84)),
    Cell(Position2D("(foo+baz)", 1), Content(ContinuousSchema(DoubleCodex), 3.14 + 9.42)),
    Cell(Position2D("(foo+baz)", 2), Content(ContinuousSchema(DoubleCodex), 6.28 + 18.84)),
    Cell(Position2D("(qux+bar)", 1), Content(ContinuousSchema(DoubleCodex), 12.56 + 6.28)),
    Cell(Position2D("(qux+baz)", 1), Content(ContinuousSchema(DoubleCodex), 12.56 + 9.42)),
    Cell(Position2D("(qux+foo)", 1), Content(ContinuousSchema(DoubleCodex), 12.56 + 3.14)))

  val result6 = List(
    Cell(Position3D("(baz+bar)", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42 + 6.28)),
    Cell(Position3D("(baz+bar)", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 18.84 + 12.56)),
    Cell(Position3D("(foo+bar)", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 3.14 + 6.28)),
    Cell(Position3D("(foo+bar)", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28 + 12.56)),
    Cell(Position3D("(foo+bar)", 3, "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42 + 18.84)),
    Cell(Position3D("(foo+baz)", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 3.14 + 9.42)),
    Cell(Position3D("(foo+baz)", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28 + 18.84)),
    Cell(Position3D("(qux+bar)", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 6.28)),
    Cell(Position3D("(qux+baz)", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 9.42)),
    Cell(Position3D("(qux+foo)", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 3.14)))

  val result7 = List(
    Cell(Position2D("(2|xyz+1|xyz)", "bar"), Content(ContinuousSchema(DoubleCodex), 12.56 + 6.28)),
    Cell(Position2D("(2|xyz+1|xyz)", "baz"), Content(ContinuousSchema(DoubleCodex), 18.84 + 9.42)),
    Cell(Position2D("(2|xyz+1|xyz)", "foo"), Content(ContinuousSchema(DoubleCodex), 6.28 + 3.14)),
    Cell(Position2D("(2|xyz-1|xyz)", "bar"), Content(ContinuousSchema(DoubleCodex), 12.56 - 6.28)),
    Cell(Position2D("(2|xyz-1|xyz)", "baz"), Content(ContinuousSchema(DoubleCodex), 18.84 - 9.42)),
    Cell(Position2D("(2|xyz-1|xyz)", "foo"), Content(ContinuousSchema(DoubleCodex), 6.28 - 3.14)),
    Cell(Position2D("(3|xyz+1|xyz)", "bar"), Content(ContinuousSchema(DoubleCodex), 18.84 + 6.28)),
    Cell(Position2D("(3|xyz+1|xyz)", "foo"), Content(ContinuousSchema(DoubleCodex), 9.42 + 3.14)),
    Cell(Position2D("(3|xyz+2|xyz)", "bar"), Content(ContinuousSchema(DoubleCodex), 18.84 + 12.56)),
    Cell(Position2D("(3|xyz+2|xyz)", "foo"), Content(ContinuousSchema(DoubleCodex), 9.42 + 6.28)),
    Cell(Position2D("(3|xyz-1|xyz)", "bar"), Content(ContinuousSchema(DoubleCodex), 18.84 - 6.28)),
    Cell(Position2D("(3|xyz-1|xyz)", "foo"), Content(ContinuousSchema(DoubleCodex), 9.42 - 3.14)),
    Cell(Position2D("(3|xyz-2|xyz)", "bar"), Content(ContinuousSchema(DoubleCodex), 18.84 - 12.56)),
    Cell(Position2D("(3|xyz-2|xyz)", "foo"), Content(ContinuousSchema(DoubleCodex), 9.42 - 6.28)),
    Cell(Position2D("(4|xyz+1|xyz)", "foo"), Content(ContinuousSchema(DoubleCodex), 12.56 + 3.14)),
    Cell(Position2D("(4|xyz+2|xyz)", "foo"), Content(ContinuousSchema(DoubleCodex), 12.56 + 6.28)),
    Cell(Position2D("(4|xyz+3|xyz)", "foo"), Content(ContinuousSchema(DoubleCodex), 12.56 + 9.42)),
    Cell(Position2D("(4|xyz-1|xyz)", "foo"), Content(ContinuousSchema(DoubleCodex), 12.56 - 3.14)),
    Cell(Position2D("(4|xyz-2|xyz)", "foo"), Content(ContinuousSchema(DoubleCodex), 12.56 - 6.28)),
    Cell(Position2D("(4|xyz-3|xyz)", "foo"), Content(ContinuousSchema(DoubleCodex), 12.56 - 9.42)))

  val result8 = List(
    Cell(Position3D("(2+1)", "bar", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 6.28)),
    Cell(Position3D("(2+1)", "baz", "xyz"), Content(ContinuousSchema(DoubleCodex), 18.84 + 9.42)),
    Cell(Position3D("(2+1)", "foo", "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28 + 3.14)),
    Cell(Position3D("(2-1)", "bar", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 - 6.28)),
    Cell(Position3D("(2-1)", "baz", "xyz"), Content(ContinuousSchema(DoubleCodex), 18.84 - 9.42)),
    Cell(Position3D("(2-1)", "foo", "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28 - 3.14)),
    Cell(Position3D("(3+1)", "bar", "xyz"), Content(ContinuousSchema(DoubleCodex), 18.84 + 6.28)),
    Cell(Position3D("(3+1)", "foo", "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42 + 3.14)),
    Cell(Position3D("(3+2)", "bar", "xyz"), Content(ContinuousSchema(DoubleCodex), 18.84 + 12.56)),
    Cell(Position3D("(3+2)", "foo", "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42 + 6.28)),
    Cell(Position3D("(3-1)", "bar", "xyz"), Content(ContinuousSchema(DoubleCodex), 18.84 - 6.28)),
    Cell(Position3D("(3-1)", "foo", "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42 - 3.14)),
    Cell(Position3D("(3-2)", "bar", "xyz"), Content(ContinuousSchema(DoubleCodex), 18.84 - 12.56)),
    Cell(Position3D("(3-2)", "foo", "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42 - 6.28)),
    Cell(Position3D("(4+1)", "foo", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 3.14)),
    Cell(Position3D("(4+2)", "foo", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 6.28)),
    Cell(Position3D("(4+3)", "foo", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 9.42)),
    Cell(Position3D("(4-1)", "foo", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 - 3.14)),
    Cell(Position3D("(4-2)", "foo", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 - 6.28)),
    Cell(Position3D("(4-3)", "foo", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 - 9.42)))

  val result9 = List(
    Cell(Position2D("(baz|xyz+bar|xyz)", 1), Content(ContinuousSchema(DoubleCodex), 9.42 + 6.28)),
    Cell(Position2D("(baz|xyz+bar|xyz)", 2), Content(ContinuousSchema(DoubleCodex), 18.84 + 12.56)),
    Cell(Position2D("(foo|xyz+bar|xyz)", 1), Content(ContinuousSchema(DoubleCodex), 3.14 + 6.28)),
    Cell(Position2D("(foo|xyz+bar|xyz)", 2), Content(ContinuousSchema(DoubleCodex), 6.28 + 12.56)),
    Cell(Position2D("(foo|xyz+bar|xyz)", 3), Content(ContinuousSchema(DoubleCodex), 9.42 + 18.84)),
    Cell(Position2D("(foo|xyz+baz|xyz)", 1), Content(ContinuousSchema(DoubleCodex), 3.14 + 9.42)),
    Cell(Position2D("(foo|xyz+baz|xyz)", 2), Content(ContinuousSchema(DoubleCodex), 6.28 + 18.84)),
    Cell(Position2D("(qux|xyz+bar|xyz)", 1), Content(ContinuousSchema(DoubleCodex), 12.56 + 6.28)),
    Cell(Position2D("(qux|xyz+baz|xyz)", 1), Content(ContinuousSchema(DoubleCodex), 12.56 + 9.42)),
    Cell(Position2D("(qux|xyz+foo|xyz)", 1), Content(ContinuousSchema(DoubleCodex), 12.56 + 3.14)))

  val result10 = List()

  val result11 = List(
    Cell(Position2D("(bar|2+bar|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 6.28)),
    Cell(Position2D("(bar|3+bar|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 18.84 + 6.28)),
    Cell(Position2D("(bar|3+bar|2)", "xyz"), Content(ContinuousSchema(DoubleCodex), 18.84 + 12.56)),
    Cell(Position2D("(baz|1+bar|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42 + 6.28)),
    Cell(Position2D("(baz|1+bar|2)", "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42 + 12.56)),
    Cell(Position2D("(baz|1+bar|3)", "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42 + 18.84)),
    Cell(Position2D("(baz|2+bar|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 18.84 + 6.28)),
    Cell(Position2D("(baz|2+bar|2)", "xyz"), Content(ContinuousSchema(DoubleCodex), 18.84 + 12.56)),
    Cell(Position2D("(baz|2+bar|3)", "xyz"), Content(ContinuousSchema(DoubleCodex), 18.84 + 18.84)),
    Cell(Position2D("(baz|2+baz|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 18.84 + 9.42)),
    Cell(Position2D("(foo|1+bar|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 3.14 + 6.28)),
    Cell(Position2D("(foo|1+bar|2)", "xyz"), Content(ContinuousSchema(DoubleCodex), 3.14 + 12.56)),
    Cell(Position2D("(foo|1+bar|3)", "xyz"), Content(ContinuousSchema(DoubleCodex), 3.14 + 18.84)),
    Cell(Position2D("(foo|1+baz|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 3.14 + 9.42)),
    Cell(Position2D("(foo|1+baz|2)", "xyz"), Content(ContinuousSchema(DoubleCodex), 3.14 + 18.84)),
    Cell(Position2D("(foo|2+bar|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28 + 6.28)),
    Cell(Position2D("(foo|2+bar|2)", "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28 + 12.56)),
    Cell(Position2D("(foo|2+bar|3)", "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28 + 18.84)),
    Cell(Position2D("(foo|2+baz|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28 + 9.42)),
    Cell(Position2D("(foo|2+baz|2)", "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28 + 18.84)),
    Cell(Position2D("(foo|2+foo|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28 + 3.14)),
    Cell(Position2D("(foo|3+bar|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42 + 6.28)),
    Cell(Position2D("(foo|3+bar|2)", "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42 + 12.56)),
    Cell(Position2D("(foo|3+bar|3)", "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42 + 18.84)),
    Cell(Position2D("(foo|3+baz|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42 + 9.42)),
    Cell(Position2D("(foo|3+baz|2)", "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42 + 18.84)),
    Cell(Position2D("(foo|3+foo|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42 + 3.14)),
    Cell(Position2D("(foo|3+foo|2)", "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42 + 6.28)),
    Cell(Position2D("(foo|4+bar|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 6.28)),
    Cell(Position2D("(foo|4+bar|2)", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 12.56)),
    Cell(Position2D("(foo|4+bar|3)", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 18.84)),
    Cell(Position2D("(foo|4+baz|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 9.42)),
    Cell(Position2D("(foo|4+baz|2)", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 18.84)),
    Cell(Position2D("(foo|4+foo|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 3.14)),
    Cell(Position2D("(foo|4+foo|2)", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 6.28)),
    Cell(Position2D("(foo|4+foo|3)", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 9.42)),
    Cell(Position2D("(qux|1+bar|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 6.28)),
    Cell(Position2D("(qux|1+bar|2)", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 12.56)),
    Cell(Position2D("(qux|1+bar|3)", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 18.84)),
    Cell(Position2D("(qux|1+baz|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 9.42)),
    Cell(Position2D("(qux|1+baz|2)", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 18.84)),
    Cell(Position2D("(qux|1+foo|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 3.14)),
    Cell(Position2D("(qux|1+foo|2)", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 6.28)),
    Cell(Position2D("(qux|1+foo|3)", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 9.42)),
    Cell(Position2D("(qux|1+foo|4)", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 12.56)))

  val result12 = List(Cell(Position1D("(baz+bar)"), Content(ContinuousSchema(DoubleCodex), 9.42 + 6.28 + 1)),
    Cell(Position1D("(foo+bar)"), Content(ContinuousSchema(DoubleCodex), 3.14 + 6.28 + 1)),
    Cell(Position1D("(foo+baz)"), Content(ContinuousSchema(DoubleCodex), 3.14 + 9.42 + 1)),
    Cell(Position1D("(qux+bar)"), Content(ContinuousSchema(DoubleCodex), 12.56 + 6.28 + 1)),
    Cell(Position1D("(qux+baz)"), Content(ContinuousSchema(DoubleCodex), 12.56 + 9.42 + 1)),
    Cell(Position1D("(qux+foo)"), Content(ContinuousSchema(DoubleCodex), 12.56 + 3.14 + 1)))

  val result13 = List(Cell(Position2D("(baz+bar)", 1), Content(ContinuousSchema(DoubleCodex), 9.42 + 6.28 + 1)),
    Cell(Position2D("(baz+bar)", 2), Content(ContinuousSchema(DoubleCodex), 18.84 + 12.56 + 1)),
    Cell(Position2D("(foo+bar)", 1), Content(ContinuousSchema(DoubleCodex), 3.14 + 6.28 + 1)),
    Cell(Position2D("(foo+bar)", 2), Content(ContinuousSchema(DoubleCodex), 6.28 + 12.56 + 1)),
    Cell(Position2D("(foo+bar)", 3), Content(ContinuousSchema(DoubleCodex), 9.42 + 18.84 + 1)),
    Cell(Position2D("(foo+baz)", 1), Content(ContinuousSchema(DoubleCodex), 3.14 + 9.42 + 1)),
    Cell(Position2D("(foo+baz)", 2), Content(ContinuousSchema(DoubleCodex), 6.28 + 18.84 + 1)),
    Cell(Position2D("(qux+bar)", 1), Content(ContinuousSchema(DoubleCodex), 12.56 + 6.28 + 1)),
    Cell(Position2D("(qux+baz)", 1), Content(ContinuousSchema(DoubleCodex), 12.56 + 9.42 + 1)),
    Cell(Position2D("(qux+foo)", 1), Content(ContinuousSchema(DoubleCodex), 12.56 + 3.14 + 1)))

  val result14 = List(
    Cell(Position2D("(2+1)", "bar"), Content(ContinuousSchema(DoubleCodex), 12.56 + 6.28 + 1)),
    Cell(Position2D("(2+1)", "baz"), Content(ContinuousSchema(DoubleCodex), 18.84 + 9.42 + 1)),
    Cell(Position2D("(2+1)", "foo"), Content(ContinuousSchema(DoubleCodex), 6.28 + 3.14 + 1)),
    Cell(Position2D("(2-1)", "bar"), Content(ContinuousSchema(DoubleCodex), 12.56 - 6.28 - 1)),
    Cell(Position2D("(2-1)", "baz"), Content(ContinuousSchema(DoubleCodex), 18.84 - 9.42 - 1)),
    Cell(Position2D("(2-1)", "foo"), Content(ContinuousSchema(DoubleCodex), 6.28 - 3.14 - 1)),
    Cell(Position2D("(3+1)", "bar"), Content(ContinuousSchema(DoubleCodex), 18.84 + 6.28 + 1)),
    Cell(Position2D("(3+1)", "foo"), Content(ContinuousSchema(DoubleCodex), 9.42 + 3.14 + 1)),
    Cell(Position2D("(3+2)", "bar"), Content(ContinuousSchema(DoubleCodex), 18.84 + 12.56 + 1)),
    Cell(Position2D("(3+2)", "foo"), Content(ContinuousSchema(DoubleCodex), 9.42 + 6.28 + 1)),
    Cell(Position2D("(3-1)", "bar"), Content(ContinuousSchema(DoubleCodex), 18.84 - 6.28 - 1)),
    Cell(Position2D("(3-1)", "foo"), Content(ContinuousSchema(DoubleCodex), 9.42 - 3.14 - 1)),
    Cell(Position2D("(3-2)", "bar"), Content(ContinuousSchema(DoubleCodex), 18.84 - 12.56 - 1)),
    Cell(Position2D("(3-2)", "foo"), Content(ContinuousSchema(DoubleCodex), 9.42 - 6.28 - 1)),
    Cell(Position2D("(4+1)", "foo"), Content(ContinuousSchema(DoubleCodex), 12.56 + 3.14 + 1)),
    Cell(Position2D("(4+2)", "foo"), Content(ContinuousSchema(DoubleCodex), 12.56 + 6.28 + 1)),
    Cell(Position2D("(4+3)", "foo"), Content(ContinuousSchema(DoubleCodex), 12.56 + 9.42 + 1)),
    Cell(Position2D("(4-1)", "foo"), Content(ContinuousSchema(DoubleCodex), 12.56 - 3.14 - 1)),
    Cell(Position2D("(4-2)", "foo"), Content(ContinuousSchema(DoubleCodex), 12.56 - 6.28 - 1)),
    Cell(Position2D("(4-3)", "foo"), Content(ContinuousSchema(DoubleCodex), 12.56 - 9.42 - 1)))

  val result15 = List(
    Cell(Position2D("(2+1)", "bar"), Content(ContinuousSchema(DoubleCodex), 12.56 + 6.28 + 1)),
    Cell(Position2D("(2+1)", "baz"), Content(ContinuousSchema(DoubleCodex), 18.84 + 9.42 + 1)),
    Cell(Position2D("(2+1)", "foo"), Content(ContinuousSchema(DoubleCodex), 6.28 + 3.14 + 1)),
    Cell(Position2D("(2-1)", "bar"), Content(ContinuousSchema(DoubleCodex), 12.56 - 6.28 - 1)),
    Cell(Position2D("(2-1)", "baz"), Content(ContinuousSchema(DoubleCodex), 18.84 - 9.42 - 1)),
    Cell(Position2D("(2-1)", "foo"), Content(ContinuousSchema(DoubleCodex), 6.28 - 3.14 - 1)),
    Cell(Position2D("(3+1)", "bar"), Content(ContinuousSchema(DoubleCodex), 18.84 + 6.28 + 1)),
    Cell(Position2D("(3+1)", "foo"), Content(ContinuousSchema(DoubleCodex), 9.42 + 3.14 + 1)),
    Cell(Position2D("(3+2)", "bar"), Content(ContinuousSchema(DoubleCodex), 18.84 + 12.56 + 1)),
    Cell(Position2D("(3+2)", "foo"), Content(ContinuousSchema(DoubleCodex), 9.42 + 6.28 + 1)),
    Cell(Position2D("(3-1)", "bar"), Content(ContinuousSchema(DoubleCodex), 18.84 - 6.28 - 1)),
    Cell(Position2D("(3-1)", "foo"), Content(ContinuousSchema(DoubleCodex), 9.42 - 3.14 - 1)),
    Cell(Position2D("(3-2)", "bar"), Content(ContinuousSchema(DoubleCodex), 18.84 - 12.56 - 1)),
    Cell(Position2D("(3-2)", "foo"), Content(ContinuousSchema(DoubleCodex), 9.42 - 6.28 - 1)),
    Cell(Position2D("(4+1)", "foo"), Content(ContinuousSchema(DoubleCodex), 12.56 + 3.14 + 1)),
    Cell(Position2D("(4+2)", "foo"), Content(ContinuousSchema(DoubleCodex), 12.56 + 6.28 + 1)),
    Cell(Position2D("(4+3)", "foo"), Content(ContinuousSchema(DoubleCodex), 12.56 + 9.42 + 1)),
    Cell(Position2D("(4-1)", "foo"), Content(ContinuousSchema(DoubleCodex), 12.56 - 3.14 - 1)),
    Cell(Position2D("(4-2)", "foo"), Content(ContinuousSchema(DoubleCodex), 12.56 - 6.28 - 1)),
    Cell(Position2D("(4-3)", "foo"), Content(ContinuousSchema(DoubleCodex), 12.56 - 9.42 - 1)))

  val result16 = List(Cell(Position2D("(baz+bar)", 1), Content(ContinuousSchema(DoubleCodex), 9.42 + 6.28 + 1)),
    Cell(Position2D("(baz+bar)", 2), Content(ContinuousSchema(DoubleCodex), 18.84 + 12.56 + 1)),
    Cell(Position2D("(foo+bar)", 1), Content(ContinuousSchema(DoubleCodex), 3.14 + 6.28 + 1)),
    Cell(Position2D("(foo+bar)", 2), Content(ContinuousSchema(DoubleCodex), 6.28 + 12.56 + 1)),
    Cell(Position2D("(foo+bar)", 3), Content(ContinuousSchema(DoubleCodex), 9.42 + 18.84 + 1)),
    Cell(Position2D("(foo+baz)", 1), Content(ContinuousSchema(DoubleCodex), 3.14 + 9.42 + 1)),
    Cell(Position2D("(foo+baz)", 2), Content(ContinuousSchema(DoubleCodex), 6.28 + 18.84 + 1)),
    Cell(Position2D("(qux+bar)", 1), Content(ContinuousSchema(DoubleCodex), 12.56 + 6.28 + 1)),
    Cell(Position2D("(qux+baz)", 1), Content(ContinuousSchema(DoubleCodex), 12.56 + 9.42 + 1)),
    Cell(Position2D("(qux+foo)", 1), Content(ContinuousSchema(DoubleCodex), 12.56 + 3.14 + 1)))

  val result17 = List(
    Cell(Position3D("(baz+bar)", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42 + 6.28 + 1)),
    Cell(Position3D("(baz+bar)", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 18.84 + 12.56 + 1)),
    Cell(Position3D("(foo+bar)", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 3.14 + 6.28 + 1)),
    Cell(Position3D("(foo+bar)", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28 + 12.56 + 1)),
    Cell(Position3D("(foo+bar)", 3, "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42 + 18.84 + 1)),
    Cell(Position3D("(foo+baz)", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 3.14 + 9.42 + 1)),
    Cell(Position3D("(foo+baz)", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28 + 18.84 + 1)),
    Cell(Position3D("(qux+bar)", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 6.28 + 1)),
    Cell(Position3D("(qux+baz)", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 9.42 + 1)),
    Cell(Position3D("(qux+foo)", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 3.14 + 1)))

  val result18 = List(
    Cell(Position2D("(2|xyz+1|xyz)", "bar"), Content(ContinuousSchema(DoubleCodex), 12.56 + 6.28 + 1)),
    Cell(Position2D("(2|xyz+1|xyz)", "baz"), Content(ContinuousSchema(DoubleCodex), 18.84 + 9.42 + 1)),
    Cell(Position2D("(2|xyz+1|xyz)", "foo"), Content(ContinuousSchema(DoubleCodex), 6.28 + 3.14 + 1)),
    Cell(Position2D("(2|xyz-1|xyz)", "bar"), Content(ContinuousSchema(DoubleCodex), 12.56 - 6.28 - 1)),
    Cell(Position2D("(2|xyz-1|xyz)", "baz"), Content(ContinuousSchema(DoubleCodex), 18.84 - 9.42 - 1)),
    Cell(Position2D("(2|xyz-1|xyz)", "foo"), Content(ContinuousSchema(DoubleCodex), 6.28 - 3.14 - 1)),
    Cell(Position2D("(3|xyz+1|xyz)", "bar"), Content(ContinuousSchema(DoubleCodex), 18.84 + 6.28 + 1)),
    Cell(Position2D("(3|xyz+1|xyz)", "foo"), Content(ContinuousSchema(DoubleCodex), 9.42 + 3.14 + 1)),
    Cell(Position2D("(3|xyz+2|xyz)", "bar"), Content(ContinuousSchema(DoubleCodex), 18.84 + 12.56 + 1)),
    Cell(Position2D("(3|xyz+2|xyz)", "foo"), Content(ContinuousSchema(DoubleCodex), 9.42 + 6.28 + 1)),
    Cell(Position2D("(3|xyz-1|xyz)", "bar"), Content(ContinuousSchema(DoubleCodex), 18.84 - 6.28 - 1)),
    Cell(Position2D("(3|xyz-1|xyz)", "foo"), Content(ContinuousSchema(DoubleCodex), 9.42 - 3.14 - 1)),
    Cell(Position2D("(3|xyz-2|xyz)", "bar"), Content(ContinuousSchema(DoubleCodex), 18.84 - 12.56 - 1)),
    Cell(Position2D("(3|xyz-2|xyz)", "foo"), Content(ContinuousSchema(DoubleCodex), 9.42 - 6.28 - 1)),
    Cell(Position2D("(4|xyz+1|xyz)", "foo"), Content(ContinuousSchema(DoubleCodex), 12.56 + 3.14 + 1)),
    Cell(Position2D("(4|xyz+2|xyz)", "foo"), Content(ContinuousSchema(DoubleCodex), 12.56 + 6.28 + 1)),
    Cell(Position2D("(4|xyz+3|xyz)", "foo"), Content(ContinuousSchema(DoubleCodex), 12.56 + 9.42 + 1)),
    Cell(Position2D("(4|xyz-1|xyz)", "foo"), Content(ContinuousSchema(DoubleCodex), 12.56 - 3.14 - 1)),
    Cell(Position2D("(4|xyz-2|xyz)", "foo"), Content(ContinuousSchema(DoubleCodex), 12.56 - 6.28 - 1)),
    Cell(Position2D("(4|xyz-3|xyz)", "foo"), Content(ContinuousSchema(DoubleCodex), 12.56 - 9.42 - 1)))

  val result19 = List(
    Cell(Position3D("(2+1)", "bar", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 6.28 + 1)),
    Cell(Position3D("(2+1)", "baz", "xyz"), Content(ContinuousSchema(DoubleCodex), 18.84 + 9.42 + 1)),
    Cell(Position3D("(2+1)", "foo", "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28 + 3.14 + 1)),
    Cell(Position3D("(2-1)", "bar", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 - 6.28 - 1)),
    Cell(Position3D("(2-1)", "baz", "xyz"), Content(ContinuousSchema(DoubleCodex), 18.84 - 9.42 - 1)),
    Cell(Position3D("(2-1)", "foo", "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28 - 3.14 - 1)),
    Cell(Position3D("(3+1)", "bar", "xyz"), Content(ContinuousSchema(DoubleCodex), 18.84 + 6.28 + 1)),
    Cell(Position3D("(3+1)", "foo", "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42 + 3.14 + 1)),
    Cell(Position3D("(3+2)", "bar", "xyz"), Content(ContinuousSchema(DoubleCodex), 18.84 + 12.56 + 1)),
    Cell(Position3D("(3+2)", "foo", "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42 + 6.28 + 1)),
    Cell(Position3D("(3-1)", "bar", "xyz"), Content(ContinuousSchema(DoubleCodex), 18.84 - 6.28 - 1)),
    Cell(Position3D("(3-1)", "foo", "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42 - 3.14 - 1)),
    Cell(Position3D("(3-2)", "bar", "xyz"), Content(ContinuousSchema(DoubleCodex), 18.84 - 12.56 - 1)),
    Cell(Position3D("(3-2)", "foo", "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42 - 6.28 - 1)),
    Cell(Position3D("(4+1)", "foo", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 3.14 + 1)),
    Cell(Position3D("(4+2)", "foo", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 6.28 + 1)),
    Cell(Position3D("(4+3)", "foo", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 9.42 + 1)),
    Cell(Position3D("(4-1)", "foo", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 - 3.14 - 1)),
    Cell(Position3D("(4-2)", "foo", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 - 6.28 - 1)),
    Cell(Position3D("(4-3)", "foo", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 - 9.42 - 1)))

  val result20 = List(
    Cell(Position2D("(baz|xyz+bar|xyz)", 1), Content(ContinuousSchema(DoubleCodex), 9.42 + 6.28 + 1)),
    Cell(Position2D("(baz|xyz+bar|xyz)", 2), Content(ContinuousSchema(DoubleCodex), 18.84 + 12.56 + 1)),
    Cell(Position2D("(foo|xyz+bar|xyz)", 1), Content(ContinuousSchema(DoubleCodex), 3.14 + 6.28 + 1)),
    Cell(Position2D("(foo|xyz+bar|xyz)", 2), Content(ContinuousSchema(DoubleCodex), 6.28 + 12.56 + 1)),
    Cell(Position2D("(foo|xyz+bar|xyz)", 3), Content(ContinuousSchema(DoubleCodex), 9.42 + 18.84 + 1)),
    Cell(Position2D("(foo|xyz+baz|xyz)", 1), Content(ContinuousSchema(DoubleCodex), 3.14 + 9.42 + 1)),
    Cell(Position2D("(foo|xyz+baz|xyz)", 2), Content(ContinuousSchema(DoubleCodex), 6.28 + 18.84 + 1)),
    Cell(Position2D("(qux|xyz+bar|xyz)", 1), Content(ContinuousSchema(DoubleCodex), 12.56 + 6.28 + 1)),
    Cell(Position2D("(qux|xyz+baz|xyz)", 1), Content(ContinuousSchema(DoubleCodex), 12.56 + 9.42 + 1)),
    Cell(Position2D("(qux|xyz+foo|xyz)", 1), Content(ContinuousSchema(DoubleCodex), 12.56 + 3.14 + 1)))

  val result21 = List()

  val result22 = List(
    Cell(Position2D("(bar|2+bar|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 6.28 + 1)),
    Cell(Position2D("(bar|3+bar|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 18.84 + 6.28 + 1)),
    Cell(Position2D("(bar|3+bar|2)", "xyz"), Content(ContinuousSchema(DoubleCodex), 18.84 + 12.56 + 1)),
    Cell(Position2D("(baz|1+bar|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42 + 6.28 + 1)),
    Cell(Position2D("(baz|1+bar|2)", "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42 + 12.56 + 1)),
    Cell(Position2D("(baz|1+bar|3)", "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42 + 18.84 + 1)),
    Cell(Position2D("(baz|2+bar|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 18.84 + 6.28 + 1)),
    Cell(Position2D("(baz|2+bar|2)", "xyz"), Content(ContinuousSchema(DoubleCodex), 18.84 + 12.56 + 1)),
    Cell(Position2D("(baz|2+bar|3)", "xyz"), Content(ContinuousSchema(DoubleCodex), 18.84 + 18.84 + 1)),
    Cell(Position2D("(baz|2+baz|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 18.84 + 9.42 + 1)),
    Cell(Position2D("(foo|1+bar|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 3.14 + 6.28 + 1)),
    Cell(Position2D("(foo|1+bar|2)", "xyz"), Content(ContinuousSchema(DoubleCodex), 3.14 + 12.56 + 1)),
    Cell(Position2D("(foo|1+bar|3)", "xyz"), Content(ContinuousSchema(DoubleCodex), 3.14 + 18.84 + 1)),
    Cell(Position2D("(foo|1+baz|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 3.14 + 9.42 + 1)),
    Cell(Position2D("(foo|1+baz|2)", "xyz"), Content(ContinuousSchema(DoubleCodex), 3.14 + 18.84 + 1)),
    Cell(Position2D("(foo|2+bar|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28 + 6.28 + 1)),
    Cell(Position2D("(foo|2+bar|2)", "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28 + 12.56 + 1)),
    Cell(Position2D("(foo|2+bar|3)", "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28 + 18.84 + 1)),
    Cell(Position2D("(foo|2+baz|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28 + 9.42 + 1)),
    Cell(Position2D("(foo|2+baz|2)", "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28 + 18.84 + 1)),
    Cell(Position2D("(foo|2+foo|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28 + 3.14 + 1)),
    Cell(Position2D("(foo|3+bar|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42 + 6.28 + 1)),
    Cell(Position2D("(foo|3+bar|2)", "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42 + 12.56 + 1)),
    Cell(Position2D("(foo|3+bar|3)", "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42 + 18.84 + 1)),
    Cell(Position2D("(foo|3+baz|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42 + 9.42 + 1)),
    Cell(Position2D("(foo|3+baz|2)", "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42 + 18.84 + 1)),
    Cell(Position2D("(foo|3+foo|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42 + 3.14 + 1)),
    Cell(Position2D("(foo|3+foo|2)", "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42 + 6.28 + 1)),
    Cell(Position2D("(foo|4+bar|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 6.28 + 1)),
    Cell(Position2D("(foo|4+bar|2)", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 12.56 + 1)),
    Cell(Position2D("(foo|4+bar|3)", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 18.84 + 1)),
    Cell(Position2D("(foo|4+baz|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 9.42 + 1)),
    Cell(Position2D("(foo|4+baz|2)", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 18.84 + 1)),
    Cell(Position2D("(foo|4+foo|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 3.14 + 1)),
    Cell(Position2D("(foo|4+foo|2)", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 6.28 + 1)),
    Cell(Position2D("(foo|4+foo|3)", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 9.42 + 1)),
    Cell(Position2D("(qux|1+bar|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 6.28 + 1)),
    Cell(Position2D("(qux|1+bar|2)", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 12.56 + 1)),
    Cell(Position2D("(qux|1+bar|3)", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 18.84 + 1)),
    Cell(Position2D("(qux|1+baz|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 9.42 + 1)),
    Cell(Position2D("(qux|1+baz|2)", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 18.84 + 1)),
    Cell(Position2D("(qux|1+foo|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 3.14 + 1)),
    Cell(Position2D("(qux|1+foo|2)", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 6.28 + 1)),
    Cell(Position2D("(qux|1+foo|3)", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 9.42 + 1)),
    Cell(Position2D("(qux|1+foo|4)", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 12.56 + 1)))

  val result23 = List(Cell(Position1D("(baz+bar)"), Content(ContinuousSchema(DoubleCodex), 9.42 + 1)),
    Cell(Position1D("(foo+bar)"), Content(ContinuousSchema(DoubleCodex), 3.14 + 1)),
    Cell(Position1D("(foo+baz)"), Content(ContinuousSchema(DoubleCodex), 3.14 + 2)),
    Cell(Position1D("(qux+bar)"), Content(ContinuousSchema(DoubleCodex), 12.56 + 1)),
    Cell(Position1D("(qux+baz)"), Content(ContinuousSchema(DoubleCodex), 12.56 + 2)))

  val result24 = List(Cell(Position2D("(baz+bar)", 1), Content(ContinuousSchema(DoubleCodex), 9.42 + 1)),
    Cell(Position2D("(baz+bar)", 2), Content(ContinuousSchema(DoubleCodex), 18.84 + 2)),
    Cell(Position2D("(foo+bar)", 1), Content(ContinuousSchema(DoubleCodex), 3.14 + 1)),
    Cell(Position2D("(foo+bar)", 2), Content(ContinuousSchema(DoubleCodex), 6.28 + 2)),
    Cell(Position2D("(foo+bar)", 3), Content(ContinuousSchema(DoubleCodex), 9.42 + 3)),
    Cell(Position2D("(foo+baz)", 1), Content(ContinuousSchema(DoubleCodex), 3.14 + 4)),
    Cell(Position2D("(foo+baz)", 2), Content(ContinuousSchema(DoubleCodex), 6.28 + 5)),
    Cell(Position2D("(qux+bar)", 1), Content(ContinuousSchema(DoubleCodex), 12.56 + 1)),
    Cell(Position2D("(qux+baz)", 1), Content(ContinuousSchema(DoubleCodex), 12.56 + 4)))

  val result25 = List(Cell(Position2D("(3+2)", "bar"), Content(ContinuousSchema(DoubleCodex), 18.84 + 1)),
    Cell(Position2D("(3+2)", "foo"), Content(ContinuousSchema(DoubleCodex), 9.42 + 3)),
    Cell(Position2D("(3-2)", "bar"), Content(ContinuousSchema(DoubleCodex), 18.84 - 1)),
    Cell(Position2D("(3-2)", "foo"), Content(ContinuousSchema(DoubleCodex), 9.42 - 3)),
    Cell(Position2D("(4+2)", "foo"), Content(ContinuousSchema(DoubleCodex), 12.56 + 3)),
    Cell(Position2D("(4-2)", "foo"), Content(ContinuousSchema(DoubleCodex), 12.56 - 3)))

  val result26 = List(Cell(Position2D("(3+2)", "bar"), Content(ContinuousSchema(DoubleCodex), 18.84 + 1)),
    Cell(Position2D("(3+2)", "foo"), Content(ContinuousSchema(DoubleCodex), 9.42 + 3)),
    Cell(Position2D("(3-2)", "bar"), Content(ContinuousSchema(DoubleCodex), 18.84 - 1)),
    Cell(Position2D("(3-2)", "foo"), Content(ContinuousSchema(DoubleCodex), 9.42 - 3)),
    Cell(Position2D("(4+2)", "foo"), Content(ContinuousSchema(DoubleCodex), 12.56 + 3)),
    Cell(Position2D("(4-2)", "foo"), Content(ContinuousSchema(DoubleCodex), 12.56 - 3)))

  val result27 = List(Cell(Position2D("(baz+bar)", 1), Content(ContinuousSchema(DoubleCodex), 9.42 + 1)),
    Cell(Position2D("(baz+bar)", 2), Content(ContinuousSchema(DoubleCodex), 18.84 + 2)),
    Cell(Position2D("(foo+bar)", 1), Content(ContinuousSchema(DoubleCodex), 3.14 + 1)),
    Cell(Position2D("(foo+bar)", 2), Content(ContinuousSchema(DoubleCodex), 6.28 + 2)),
    Cell(Position2D("(foo+bar)", 3), Content(ContinuousSchema(DoubleCodex), 9.42 + 3)),
    Cell(Position2D("(foo+baz)", 1), Content(ContinuousSchema(DoubleCodex), 3.14 + 4)),
    Cell(Position2D("(foo+baz)", 2), Content(ContinuousSchema(DoubleCodex), 6.28 + 5)),
    Cell(Position2D("(qux+bar)", 1), Content(ContinuousSchema(DoubleCodex), 12.56 + 1)),
    Cell(Position2D("(qux+baz)", 1), Content(ContinuousSchema(DoubleCodex), 12.56 + 4)))

  val result28 = List(Cell(Position3D("(baz+bar)", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42 + 1)),
    Cell(Position3D("(baz+bar)", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 18.84 + 2)),
    Cell(Position3D("(foo+bar)", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 3.14 + 1)),
    Cell(Position3D("(foo+bar)", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28 + 2)),
    Cell(Position3D("(foo+bar)", 3, "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42 + 3)),
    Cell(Position3D("(foo+baz)", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 3.14 + 4)),
    Cell(Position3D("(foo+baz)", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28 + 5)),
    Cell(Position3D("(qux+bar)", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 1)),
    Cell(Position3D("(qux+baz)", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 4)))

  val result29 = List(
    Cell(Position2D("(3|xyz+2|xyz)", "bar"), Content(ContinuousSchema(DoubleCodex), 18.84 + 1)),
    Cell(Position2D("(3|xyz+2|xyz)", "foo"), Content(ContinuousSchema(DoubleCodex), 9.42 + 3)),
    Cell(Position2D("(3|xyz-2|xyz)", "bar"), Content(ContinuousSchema(DoubleCodex), 18.84 - 1)),
    Cell(Position2D("(3|xyz-2|xyz)", "foo"), Content(ContinuousSchema(DoubleCodex), 9.42 - 3)),
    Cell(Position2D("(4|xyz+2|xyz)", "foo"), Content(ContinuousSchema(DoubleCodex), 12.56 + 3)),
    Cell(Position2D("(4|xyz-2|xyz)", "foo"), Content(ContinuousSchema(DoubleCodex), 12.56 - 3)))

  val result30 = List(
    Cell(Position3D("(3+2)", "bar", "xyz"), Content(ContinuousSchema(DoubleCodex), 18.84 + 1)),
    Cell(Position3D("(3+2)", "foo", "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42 + 3)),
    Cell(Position3D("(3-2)", "bar", "xyz"), Content(ContinuousSchema(DoubleCodex), 18.84 - 1)),
    Cell(Position3D("(3-2)", "foo", "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42 - 3)),
    Cell(Position3D("(4+2)", "foo", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 3)),
    Cell(Position3D("(4-2)", "foo", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 - 3)))

  val result31 = List(
    Cell(Position2D("(baz|xyz+bar|xyz)", 1), Content(ContinuousSchema(DoubleCodex), 9.42 + 1)),
    Cell(Position2D("(baz|xyz+bar|xyz)", 2), Content(ContinuousSchema(DoubleCodex), 18.84 + 2)),
    Cell(Position2D("(foo|xyz+bar|xyz)", 1), Content(ContinuousSchema(DoubleCodex), 3.14 + 1)),
    Cell(Position2D("(foo|xyz+bar|xyz)", 2), Content(ContinuousSchema(DoubleCodex), 6.28 + 2)),
    Cell(Position2D("(foo|xyz+bar|xyz)", 3), Content(ContinuousSchema(DoubleCodex), 9.42 + 3)),
    Cell(Position2D("(foo|xyz+baz|xyz)", 1), Content(ContinuousSchema(DoubleCodex), 3.14 + 4)),
    Cell(Position2D("(foo|xyz+baz|xyz)", 2), Content(ContinuousSchema(DoubleCodex), 6.28 + 5)),
    Cell(Position2D("(qux|xyz+bar|xyz)", 1), Content(ContinuousSchema(DoubleCodex), 12.56 + 1)),
    Cell(Position2D("(qux|xyz+baz|xyz)", 1), Content(ContinuousSchema(DoubleCodex), 12.56 + 4)))

  val result32 = List()

  val result33 = List(
    Cell(Position2D("(bar|2+bar|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 1)),
    Cell(Position2D("(bar|3+bar|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 18.84 + 1)),
    Cell(Position2D("(bar|3+bar|2)", "xyz"), Content(ContinuousSchema(DoubleCodex), 18.84 + 2)),
    Cell(Position2D("(baz|1+bar|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42 + 1)),
    Cell(Position2D("(baz|1+bar|2)", "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42 + 2)),
    Cell(Position2D("(baz|1+bar|3)", "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42 + 3)),
    Cell(Position2D("(baz|2+bar|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 18.84 + 1)),
    Cell(Position2D("(baz|2+bar|2)", "xyz"), Content(ContinuousSchema(DoubleCodex), 18.84 + 2)),
    Cell(Position2D("(baz|2+bar|3)", "xyz"), Content(ContinuousSchema(DoubleCodex), 18.84 + 3)),
    Cell(Position2D("(baz|2+baz|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 18.84 + 4)),
    Cell(Position2D("(foo|1+bar|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 3.14 + 1)),
    Cell(Position2D("(foo|1+bar|2)", "xyz"), Content(ContinuousSchema(DoubleCodex), 3.14 + 2)),
    Cell(Position2D("(foo|1+bar|3)", "xyz"), Content(ContinuousSchema(DoubleCodex), 3.14 + 3)),
    Cell(Position2D("(foo|1+baz|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 3.14 + 4)),
    Cell(Position2D("(foo|1+baz|2)", "xyz"), Content(ContinuousSchema(DoubleCodex), 3.14 + 5)),
    Cell(Position2D("(foo|2+bar|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28 + 1)),
    Cell(Position2D("(foo|2+bar|2)", "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28 + 2)),
    Cell(Position2D("(foo|2+bar|3)", "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28 + 3)),
    Cell(Position2D("(foo|2+baz|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28 + 4)),
    Cell(Position2D("(foo|2+baz|2)", "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28 + 5)),
    Cell(Position2D("(foo|3+bar|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42 + 1)),
    Cell(Position2D("(foo|3+bar|2)", "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42 + 2)),
    Cell(Position2D("(foo|3+bar|3)", "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42 + 3)),
    Cell(Position2D("(foo|3+baz|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42 + 4)),
    Cell(Position2D("(foo|3+baz|2)", "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42 + 5)),
    Cell(Position2D("(foo|4+bar|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 1)),
    Cell(Position2D("(foo|4+bar|2)", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 2)),
    Cell(Position2D("(foo|4+bar|3)", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 3)),
    Cell(Position2D("(foo|4+baz|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 4)),
    Cell(Position2D("(foo|4+baz|2)", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 5)),
    Cell(Position2D("(qux|1+bar|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 1)),
    Cell(Position2D("(qux|1+bar|2)", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 2)),
    Cell(Position2D("(qux|1+bar|3)", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 3)),
    Cell(Position2D("(qux|1+baz|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 4)),
    Cell(Position2D("(qux|1+baz|2)", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 5)))

  val result34 = List(Cell(Position1D("(baz+bar)"), Content(ContinuousSchema(DoubleCodex), 9.42 + 1 + 1)),
    Cell(Position1D("(foo+bar)"), Content(ContinuousSchema(DoubleCodex), 3.14 + 1 + 1)),
    Cell(Position1D("(foo+baz)"), Content(ContinuousSchema(DoubleCodex), 3.14 + 2 + 1)),
    Cell(Position1D("(qux+bar)"), Content(ContinuousSchema(DoubleCodex), 12.56 + 1 + 1)),
    Cell(Position1D("(qux+baz)"), Content(ContinuousSchema(DoubleCodex), 12.56 + 2 + 1)))

  val result35 = List(
    Cell(Position2D("(baz+bar)", 1), Content(ContinuousSchema(DoubleCodex), 9.42 + 1 + 1)),
    Cell(Position2D("(baz+bar)", 2), Content(ContinuousSchema(DoubleCodex), 18.84 + 2 + 1)),
    Cell(Position2D("(foo+bar)", 1), Content(ContinuousSchema(DoubleCodex), 3.14 + 1 + 1)),
    Cell(Position2D("(foo+bar)", 2), Content(ContinuousSchema(DoubleCodex), 6.28 + 2 + 1)),
    Cell(Position2D("(foo+bar)", 3), Content(ContinuousSchema(DoubleCodex), 9.42 + 3 + 1)),
    Cell(Position2D("(foo+baz)", 1), Content(ContinuousSchema(DoubleCodex), 3.14 + 4 + 1)),
    Cell(Position2D("(foo+baz)", 2), Content(ContinuousSchema(DoubleCodex), 6.28 + 5 + 1)),
    Cell(Position2D("(qux+bar)", 1), Content(ContinuousSchema(DoubleCodex), 12.56 + 1 + 1)),
    Cell(Position2D("(qux+baz)", 1), Content(ContinuousSchema(DoubleCodex), 12.56 + 4 + 1)))

  val result36 = List(Cell(Position2D("(3+2)", "bar"), Content(ContinuousSchema(DoubleCodex), 18.84 + 1 + 1)),
    Cell(Position2D("(3+2)", "foo"), Content(ContinuousSchema(DoubleCodex), 9.42 + 3 + 1)),
    Cell(Position2D("(3-2)", "bar"), Content(ContinuousSchema(DoubleCodex), 18.84 - 1 - 1)),
    Cell(Position2D("(3-2)", "foo"), Content(ContinuousSchema(DoubleCodex), 9.42 - 3 - 1)),
    Cell(Position2D("(4+2)", "foo"), Content(ContinuousSchema(DoubleCodex), 12.56 + 3 + 1)),
    Cell(Position2D("(4-2)", "foo"), Content(ContinuousSchema(DoubleCodex), 12.56 - 3 - 1)))

  val result37 = List(Cell(Position2D("(3+2)", "bar"), Content(ContinuousSchema(DoubleCodex), 18.84 + 1 + 1)),
    Cell(Position2D("(3+2)", "foo"), Content(ContinuousSchema(DoubleCodex), 9.42 + 3 + 1)),
    Cell(Position2D("(3-2)", "bar"), Content(ContinuousSchema(DoubleCodex), 18.84 - 1 - 1)),
    Cell(Position2D("(3-2)", "foo"), Content(ContinuousSchema(DoubleCodex), 9.42 - 3 - 1)),
    Cell(Position2D("(4+2)", "foo"), Content(ContinuousSchema(DoubleCodex), 12.56 + 3 + 1)),
    Cell(Position2D("(4-2)", "foo"), Content(ContinuousSchema(DoubleCodex), 12.56 - 3 - 1)))

  val result38 = List(Cell(Position2D("(baz+bar)", 1), Content(ContinuousSchema(DoubleCodex), 9.42 + 1 + 1)),
    Cell(Position2D("(baz+bar)", 2), Content(ContinuousSchema(DoubleCodex), 18.84 + 2 + 1)),
    Cell(Position2D("(foo+bar)", 1), Content(ContinuousSchema(DoubleCodex), 3.14 + 1 + 1)),
    Cell(Position2D("(foo+bar)", 2), Content(ContinuousSchema(DoubleCodex), 6.28 + 2 + 1)),
    Cell(Position2D("(foo+bar)", 3), Content(ContinuousSchema(DoubleCodex), 9.42 + 3 + 1)),
    Cell(Position2D("(foo+baz)", 1), Content(ContinuousSchema(DoubleCodex), 3.14 + 4 + 1)),
    Cell(Position2D("(foo+baz)", 2), Content(ContinuousSchema(DoubleCodex), 6.28 + 5 + 1)),
    Cell(Position2D("(qux+bar)", 1), Content(ContinuousSchema(DoubleCodex), 12.56 + 1 + 1)),
    Cell(Position2D("(qux+baz)", 1), Content(ContinuousSchema(DoubleCodex), 12.56 + 4 + 1)))

  val result39 = List(
    Cell(Position3D("(baz+bar)", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42 + 1 + 1)),
    Cell(Position3D("(baz+bar)", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 18.84 + 2 + 1)),
    Cell(Position3D("(foo+bar)", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 3.14 + 1 + 1)),
    Cell(Position3D("(foo+bar)", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28 + 2 + 1)),
    Cell(Position3D("(foo+bar)", 3, "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42 + 3 + 1)),
    Cell(Position3D("(foo+baz)", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 3.14 + 4 + 1)),
    Cell(Position3D("(foo+baz)", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28 + 5 + 1)),
    Cell(Position3D("(qux+bar)", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 1 + 1)),
    Cell(Position3D("(qux+baz)", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 4 + 1)))

  val result40 = List(
    Cell(Position2D("(3|xyz+2|xyz)", "bar"), Content(ContinuousSchema(DoubleCodex), 18.84 + 1 + 1)),
    Cell(Position2D("(3|xyz+2|xyz)", "foo"), Content(ContinuousSchema(DoubleCodex), 9.42 + 3 + 1)),
    Cell(Position2D("(3|xyz-2|xyz)", "bar"), Content(ContinuousSchema(DoubleCodex), 18.84 - 1 - 1)),
    Cell(Position2D("(3|xyz-2|xyz)", "foo"), Content(ContinuousSchema(DoubleCodex), 9.42 - 3 - 1)),
    Cell(Position2D("(4|xyz+2|xyz)", "foo"), Content(ContinuousSchema(DoubleCodex), 12.56 + 3 + 1)),
    Cell(Position2D("(4|xyz-2|xyz)", "foo"), Content(ContinuousSchema(DoubleCodex), 12.56 - 3 - 1)))

  val result41 = List(
    Cell(Position3D("(3+2)", "bar", "xyz"), Content(ContinuousSchema(DoubleCodex), 18.84 + 1 + 1)),
    Cell(Position3D("(3+2)", "foo", "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42 + 3 + 1)),
    Cell(Position3D("(3-2)", "bar", "xyz"), Content(ContinuousSchema(DoubleCodex), 18.84 - 1 - 1)),
    Cell(Position3D("(3-2)", "foo", "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42 - 3 - 1)),
    Cell(Position3D("(4+2)", "foo", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 3 + 1)),
    Cell(Position3D("(4-2)", "foo", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 - 3 - 1)))

  val result42 = List(
    Cell(Position2D("(baz|xyz+bar|xyz)", 1), Content(ContinuousSchema(DoubleCodex), 9.42 + 1 + 1)),
    Cell(Position2D("(baz|xyz+bar|xyz)", 2), Content(ContinuousSchema(DoubleCodex), 18.84 + 2 + 1)),
    Cell(Position2D("(foo|xyz+bar|xyz)", 1), Content(ContinuousSchema(DoubleCodex), 3.14 + 1 + 1)),
    Cell(Position2D("(foo|xyz+bar|xyz)", 2), Content(ContinuousSchema(DoubleCodex), 6.28 + 2 + 1)),
    Cell(Position2D("(foo|xyz+bar|xyz)", 3), Content(ContinuousSchema(DoubleCodex), 9.42 + 3 + 1)),
    Cell(Position2D("(foo|xyz+baz|xyz)", 1), Content(ContinuousSchema(DoubleCodex), 3.14 + 4 + 1)),
    Cell(Position2D("(foo|xyz+baz|xyz)", 2), Content(ContinuousSchema(DoubleCodex), 6.28 + 5 + 1)),
    Cell(Position2D("(qux|xyz+bar|xyz)", 1), Content(ContinuousSchema(DoubleCodex), 12.56 + 1 + 1)),
    Cell(Position2D("(qux|xyz+baz|xyz)", 1), Content(ContinuousSchema(DoubleCodex), 12.56 + 4 + 1)))

  val result43 = List()

  val result44 = List(
    Cell(Position2D("(bar|2+bar|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 1 + 1)),
    Cell(Position2D("(bar|3+bar|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 18.84 + 1 + 1)),
    Cell(Position2D("(bar|3+bar|2)", "xyz"), Content(ContinuousSchema(DoubleCodex), 18.84 + 2 + 1)),
    Cell(Position2D("(baz|1+bar|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42 + 1 + 1)),
    Cell(Position2D("(baz|1+bar|2)", "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42 + 2 + 1)),
    Cell(Position2D("(baz|1+bar|3)", "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42 + 3 + 1)),
    Cell(Position2D("(baz|2+bar|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 18.84 + 1 + 1)),
    Cell(Position2D("(baz|2+bar|2)", "xyz"), Content(ContinuousSchema(DoubleCodex), 18.84 + 2 + 1)),
    Cell(Position2D("(baz|2+bar|3)", "xyz"), Content(ContinuousSchema(DoubleCodex), 18.84 + 3 + 1)),
    Cell(Position2D("(baz|2+baz|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 18.84 + 4 + 1)),
    Cell(Position2D("(foo|1+bar|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 3.14 + 1 + 1)),
    Cell(Position2D("(foo|1+bar|2)", "xyz"), Content(ContinuousSchema(DoubleCodex), 3.14 + 2 + 1)),
    Cell(Position2D("(foo|1+bar|3)", "xyz"), Content(ContinuousSchema(DoubleCodex), 3.14 + 3 + 1)),
    Cell(Position2D("(foo|1+baz|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 3.14 + 4 + 1)),
    Cell(Position2D("(foo|1+baz|2)", "xyz"), Content(ContinuousSchema(DoubleCodex), 3.14 + 5 + 1)),
    Cell(Position2D("(foo|2+bar|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28 + 1 + 1)),
    Cell(Position2D("(foo|2+bar|2)", "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28 + 2 + 1)),
    Cell(Position2D("(foo|2+bar|3)", "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28 + 3 + 1)),
    Cell(Position2D("(foo|2+baz|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28 + 4 + 1)),
    Cell(Position2D("(foo|2+baz|2)", "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28 + 5 + 1)),
    Cell(Position2D("(foo|3+bar|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42 + 1 + 1)),
    Cell(Position2D("(foo|3+bar|2)", "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42 + 2 + 1)),
    Cell(Position2D("(foo|3+bar|3)", "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42 + 3 + 1)),
    Cell(Position2D("(foo|3+baz|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42 + 4 + 1)),
    Cell(Position2D("(foo|3+baz|2)", "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42 + 5 + 1)),
    Cell(Position2D("(foo|4+bar|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 1 + 1)),
    Cell(Position2D("(foo|4+bar|2)", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 2 + 1)),
    Cell(Position2D("(foo|4+bar|3)", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 3 + 1)),
    Cell(Position2D("(foo|4+baz|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 4 + 1)),
    Cell(Position2D("(foo|4+baz|2)", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 5 + 1)),
    Cell(Position2D("(qux|1+bar|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 1 + 1)),
    Cell(Position2D("(qux|1+bar|2)", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 2 + 1)),
    Cell(Position2D("(qux|1+bar|3)", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 3 + 1)),
    Cell(Position2D("(qux|1+baz|1)", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 4 + 1)),
    Cell(Position2D("(qux|1+baz|2)", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 + 5 + 1)))

  def plus[P <: Position](slice: Slice[P]) = Locate.PrependPairwiseSelectedStringToRemainder[P](slice, "(%1$s+%2$s)")

  def minus[P <: Position](slice: Slice[P]) = Locate.PrependPairwiseSelectedStringToRemainder[P](slice, "(%1$s-%2$s)")
}

object TestMatrixPairwise {

  case class PlusX[P <: Position, Q <: Position](pos: Locate.FromPairwiseCells[P, Q]) extends OperatorWithValue[P, Q] {
    type V = Double

    val plus = Plus(pos)

    def computeWithValue(left: Cell[P], right: Cell[P], ext: V): TraversableOnce[Cell[Q]] = {
      plus.compute(left, right).map {
        case Cell(pos, Content(_, DoubleValue(d))) => Cell(pos, Content(ContinuousSchema(DoubleCodex), d + ext))
      }
    }
  }

  case class MinusX[P <: Position, Q <: Position](pos: Locate.FromPairwiseCells[P, Q]) extends OperatorWithValue[P, Q] {
    type V = Double

    val minus = Minus(pos)

    def computeWithValue(left: Cell[P], right: Cell[P], ext: V): TraversableOnce[Cell[Q]] = {
      minus.compute(left, right).map {
        case Cell(pos, Content(_, DoubleValue(d))) => Cell(pos, Content(ContinuousSchema(DoubleCodex), d - ext))
      }
    }
  }
}

class TestScaldingMatrixPairwise extends TestMatrixPairwise {

  "A Matrix.pairwise" should "return its first over pairwise in 1D" in {
    toPipe(num1)
      .pairwise(Over(First), Lower, Plus(plus[Position1D](Over(First))), InMemory())
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first over pairwise in 2D" in {
    toPipe(num2)
      .pairwise(Over(First), Lower, Plus(plus[Position2D](Over(First))), Default())
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its first along pairwise in 2D" in {
    toPipe(num2)
      .pairwise(Along(First), Lower, List(Plus(plus[Position2D](Along(First))),
        Minus(minus[Position2D](Along(First)))), Default(Redistribute(123), Redistribute(321)))
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its second over pairwise in 2D" in {
    toPipe(num2)
      .pairwise(Over(Second), Lower, List(Plus(plus[Position2D](Over(Second))),
        Minus(minus[Position2D](Over(Second)))), Default(Redistribute(123), Reducers(321)))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its second along pairwise in 2D" in {
    toPipe(num2)
      .pairwise(Along(Second), Lower, Plus(plus[Position2D](Along(Second))),
        Default(Redistribute(123), Redistribute(654) |-> Reducers(321)))
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its first over pairwise in 3D" in {
    toPipe(num3)
      .pairwise(Over(First), Lower, Plus(plus[Position3D](Over(First))), Default(Reducers(123), Redistribute(321)))
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its first along pairwise in 3D" in {
    toPipe(num3)
      .pairwise(Along(First), Lower, List(Plus(plus[Position3D](Along(First))),
        Minus(minus[Position3D](Along(First)))), Default(Reducers(123), Reducers(321)))
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its second over pairwise in 3D" in {
    toPipe(num3)
      .pairwise(Over(Second), Lower, List(Plus(plus[Position3D](Over(Second))),
        Minus(minus[Position3D](Over(Second)))), Default(Reducers(123), Redistribute(654) |-> Reducers(321)))
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its second along pairwise in 3D" in {
    toPipe(num3)
      .pairwise(Along(Second), Lower, Plus(plus[Position3D](Along(Second))),
        Default(Redistribute(123) |-> Reducers(456), Redistribute(321)))
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its third over pairwise in 3D" in {
    toPipe(num3)
      .pairwise(Over(Third), Lower, List(Plus(plus[Position3D](Over(Third))), Minus(minus[Position3D](Over(Third)))),
        Default(Redistribute(123) |-> Reducers(456), Reducers(321)))
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its third along pairwise in 3D" in {
    toPipe(num3)
      .pairwise(Along(Third), Lower, Plus(plus[Position3D](Along(Third))),
        Default(Redistribute(123) |-> Reducers(456), Redistribute(654) |-> Reducers(321)))
      .toList.sortBy(_.position) shouldBe result11
  }

  "A Matrix.pairwiseWithValue" should "return its first over pairwise in 1D" in {
    toPipe(num1)
      .pairwiseWithValue(Over(First), Lower, TestMatrixPairwise.PlusX(plus[Position1D](Over(First))), ValuePipe(ext),
          Unbalanced(Reducers(123), Reducers(654)))
      .toList.sortBy(_.position) shouldBe result12
  }

  it should "return its first over pairwise in 2D" in {
    toPipe(num2)
      .pairwiseWithValue(Over(First), Lower, TestMatrixPairwise.PlusX(plus[Position2D](Over(First))), ValuePipe(ext),
        Unbalanced(Redistribute(123) |-> Reducers(456), Redistribute(654) |-> Reducers(321)))
      .toList.sortBy(_.position) shouldBe result13
  }

  it should "return its first along pairwise in 2D" in {
    toPipe(num2)
      .pairwiseWithValue(Along(First), Lower, List(TestMatrixPairwise.PlusX(plus[Position2D](Along(First))),
        TestMatrixPairwise.MinusX(minus[Position2D](Along(First)))), ValuePipe(ext), InMemory())
      .toList.sortBy(_.position) shouldBe result14
  }

  it should "return its second over pairwise in 2D" in {
    toPipe(num2)
      .pairwiseWithValue(Over(Second), Lower, List(TestMatrixPairwise.PlusX(plus[Position2D](Over(Second))),
        TestMatrixPairwise.MinusX(minus[Position2D](Over(Second)))), ValuePipe(ext), Default())
      .toList.sortBy(_.position) shouldBe result15
  }

  it should "return its second along pairwise in 2D" in {
    toPipe(num2)
      .pairwiseWithValue(Along(Second), Lower, TestMatrixPairwise.PlusX(plus[Position2D](Along(Second))),
        ValuePipe(ext), Default(Redistribute(123), Redistribute(321)))
      .toList.sortBy(_.position) shouldBe result16
  }

  it should "return its first over pairwise in 3D" in {
    toPipe(num3)
      .pairwiseWithValue(Over(First), Lower, TestMatrixPairwise.PlusX(plus[Position3D](Over(First))), ValuePipe(ext),
        Default(Redistribute(123), Reducers(321)))
      .toList.sortBy(_.position) shouldBe result17
  }

  it should "return its first along pairwise in 3D" in {
    toPipe(num3)
      .pairwiseWithValue(Along(First), Lower, List(TestMatrixPairwise.PlusX(plus[Position3D](Along(First))),
        TestMatrixPairwise.MinusX(minus[Position3D](Along(First)))), ValuePipe(ext),
          Default(Redistribute(123), Redistribute(654) |-> Reducers(321)))
      .toList.sortBy(_.position) shouldBe result18
  }

  it should "return its second over pairwise in 3D" in {
    toPipe(num3)
      .pairwiseWithValue(Over(Second), Lower, List(TestMatrixPairwise.PlusX(plus[Position3D](Over(Second))),
        TestMatrixPairwise.MinusX(minus[Position3D](Over(Second)))), ValuePipe(ext),
          Default(Reducers(321), Redistribute(321)))
      .toList.sortBy(_.position) shouldBe result19
  }

  it should "return its second along pairwise in 3D" in {
    toPipe(num3)
      .pairwiseWithValue(Along(Second), Lower, TestMatrixPairwise.PlusX(plus[Position3D](Along(Second))),
        ValuePipe(ext), Default(Reducers(123), Reducers(321)))
      .toList.sortBy(_.position) shouldBe result20
  }

  it should "return its third over pairwise in 3D" in {
    toPipe(num3)
      .pairwiseWithValue(Over(Third), Lower, List(TestMatrixPairwise.PlusX(plus[Position3D](Over(Third))),
        TestMatrixPairwise.MinusX(minus[Position3D](Over(Third)))), ValuePipe(ext),
          Default(Reducers(123), Redistribute(654) |-> Reducers(321)))
      .toList.sortBy(_.position) shouldBe result21
  }

  it should "return its third along pairwise in 3D" in {
    toPipe(num3)
      .pairwiseWithValue(Along(Third), Lower, TestMatrixPairwise.PlusX(plus[Position3D](Along(Third))),
        ValuePipe(ext), Default(Redistribute(123) |-> Reducers(456), Redistribute(321)))
      .toList.sortBy(_.position) shouldBe result22
  }

  "A Matrix.pairwiseBetween" should "return its first over pairwise in 1D" in {
    toPipe(num1)
      .pairwiseBetween(Over(First), Lower, toPipe(dataA), Plus(plus[Position1D](Over(First))),
        Default(Redistribute(123) |-> Reducers(456), Reducers(321)))
      .toList.sortBy(_.position) shouldBe result23
  }

  it should "return its first over pairwise in 2D" in {
    toPipe(num2)
      .pairwiseBetween(Over(First), Lower, toPipe(dataB), Plus(plus[Position2D](Over(First))),
        Default(Redistribute(123) |-> Reducers(456), Redistribute(654) |-> Reducers(321)))
      .toList.sortBy(_.position) shouldBe result24
  }

  it should "return its first along pairwise in 2D" in {
    toPipe(num2)
      .pairwiseBetween(Along(First), Lower, toPipe(dataC), List(Plus(plus[Position2D](Along(First))),
        Minus(minus[Position2D](Along(First)))), Unbalanced(Reducers(123), Reducers(321)))
      .toList.sortBy(_.position) shouldBe result25
  }

  it should "return its second over pairwise in 2D" in {
    toPipe(num2)
      .pairwiseBetween(Over(Second), Lower, toPipe(dataD), List(Plus(plus[Position2D](Over(Second))),
        Minus(minus[Position2D](Over(Second)))),
          Unbalanced(Redistribute(123) |-> Reducers(456), Redistribute(654) |-> Reducers(321)))
      .toList.sortBy(_.position) shouldBe result26
  }

  it should "return its second along pairwise in 2D" in {
    toPipe(num2)
      .pairwiseBetween(Along(Second), Lower, toPipe(dataE), Plus(plus[Position2D](Along(Second))), InMemory())
      .toList.sortBy(_.position) shouldBe result27
  }

  it should "return its first over pairwise in 3D" in {
    toPipe(num3)
      .pairwiseBetween(Over(First), Lower, toPipe(dataF), Plus(plus[Position3D](Over(First))), Default())
      .toList.sortBy(_.position) shouldBe result28
  }

  it should "return its first along pairwise in 3D" in {
    toPipe(num3)
      .pairwiseBetween(Along(First), Lower, toPipe(dataG), List(Plus(plus[Position3D](Along(First))),
        Minus(minus[Position3D](Along(First)))), Default(Redistribute(123), Redistribute(321)))
      .toList.sortBy(_.position) shouldBe result29
  }

  it should "return its second over pairwise in 3D" in {
    toPipe(num3)
      .pairwiseBetween(Over(Second), Lower, toPipe(dataH), List(Plus(plus[Position3D](Over(Second))),
        Minus(minus[Position3D](Over(Second)))), Default(Redistribute(123), Reducers(321)))
      .toList.sortBy(_.position) shouldBe result30
  }

  it should "return its second along pairwise in 3D" in {
    toPipe(num3)
      .pairwiseBetween(Along(Second), Lower, toPipe(dataI), Plus(plus[Position3D](Along(Second))),
        Default(Redistribute(123), Redistribute(654) |-> Reducers(321)))
      .toList.sortBy(_.position) shouldBe result31
  }

  it should "return its third over pairwise in 3D" in {
    toPipe(num3)
      .pairwiseBetween(Over(Third), Lower, toPipe(dataJ), List(Plus(plus[Position3D](Over(Third))),
        Minus(minus[Position3D](Over(Third)))), Default(Reducers(123), Redistribute(321)))
      .toList.sortBy(_.position) shouldBe result32
  }

  it should "return its third along pairwise in 3D" in {
    toPipe(num3)
      .pairwiseBetween(Along(Third), Lower, toPipe(dataK), Plus(plus[Position3D](Along(Third))),
        Default(Reducers(123), Reducers(321)))
      .toList.sortBy(_.position) shouldBe result33
  }

  "A Matrix.pairwiseBetweenWithValue" should "return its first over pairwise in 1D" in {
    toPipe(num1)
      .pairwiseBetweenWithValue(Over(First), Lower, toPipe(dataL),
        TestMatrixPairwise.PlusX(plus[Position1D](Over(First))), ValuePipe(ext),
          Default(Reducers(123), Redistribute(654) |-> Reducers(321)))
      .toList.sortBy(_.position) shouldBe result34
  }

  it should "return its first over pairwise in 2D" in {
    toPipe(num2)
      .pairwiseBetweenWithValue(Over(First), Lower, toPipe(dataM),
        TestMatrixPairwise.PlusX(plus[Position2D](Over(First))), ValuePipe(ext),
          Default(Redistribute(123) |-> Reducers(456), Redistribute(321)))
      .toList.sortBy(_.position) shouldBe result35
  }

  it should "return its first along pairwise in 2D" in {
    toPipe(num2)
      .pairwiseBetweenWithValue(Along(First), Lower, toPipe(dataN), List(
        TestMatrixPairwise.PlusX(plus[Position2D](Along(First))),
        TestMatrixPairwise.MinusX(minus[Position2D](Along(First)))), ValuePipe(ext),
          Default(Redistribute(123) |-> Reducers(456), Reducers(321)))
      .toList.sortBy(_.position) shouldBe result36
  }

  it should "return its second over pairwise in 2D" in {
    toPipe(num2)
      .pairwiseBetweenWithValue(Over(Second), Lower, toPipe(dataO), List(
        TestMatrixPairwise.PlusX(plus[Position2D](Over(Second))),
        TestMatrixPairwise.MinusX(minus[Position2D](Over(Second)))), ValuePipe(ext),
          Default(Redistribute(123) |-> Reducers(456), Redistribute(654) |-> Reducers(321)))
      .toList.sortBy(_.position) shouldBe result37
  }

  it should "return its second along pairwise in 2D" in {
    toPipe(num2)
      .pairwiseBetweenWithValue(Along(Second), Lower, toPipe(dataP),
        TestMatrixPairwise.PlusX(plus[Position2D](Along(Second))), ValuePipe(ext),
          Unbalanced(Reducers(123), Reducers(321)))
      .toList.sortBy(_.position) shouldBe result38
  }

  it should "return its first over pairwise in 3D" in {
    toPipe(num3)
      .pairwiseBetweenWithValue(Over(First), Lower, toPipe(dataQ),
        TestMatrixPairwise.PlusX(plus[Position3D](Over(First))), ValuePipe(ext),
          Unbalanced(Redistribute(123) |-> Reducers(456), Redistribute(654) |-> Reducers(321)))
      .toList.sortBy(_.position) shouldBe result39
  }

  it should "return its first along pairwise in 3D" in {
    toPipe(num3)
      .pairwiseBetweenWithValue(Along(First), Lower, toPipe(dataR), List(
        TestMatrixPairwise.PlusX(plus[Position3D](Along(First))),
        TestMatrixPairwise.MinusX(minus[Position3D](Along(First)))), ValuePipe(ext), InMemory())
      .toList.sortBy(_.position) shouldBe result40
  }

  it should "return its second over pairwise in 3D" in {
    toPipe(num3)
      .pairwiseBetweenWithValue(Over(Second), Lower, toPipe(dataS), List(
        TestMatrixPairwise.PlusX(plus[Position3D](Over(Second))),
        TestMatrixPairwise.MinusX(minus[Position3D](Over(Second)))), ValuePipe(ext), Default())
      .toList.sortBy(_.position) shouldBe result41
  }

  it should "return its second along pairwise in 3D" in {
    toPipe(num3)
      .pairwiseBetweenWithValue(Along(Second), Lower, toPipe(dataT),
        TestMatrixPairwise.PlusX(plus[Position3D](Along(Second))), ValuePipe(ext),
          Default(Redistribute(123), Redistribute(321)))
      .toList.sortBy(_.position) shouldBe result42
  }

  it should "return its third over pairwise in 3D" in {
    toPipe(num3)
      .pairwiseBetweenWithValue(Over(Third), Lower, toPipe(dataU), List(
        TestMatrixPairwise.PlusX(plus[Position3D](Over(Third))),
        TestMatrixPairwise.MinusX(minus[Position3D](Over(Third)))), ValuePipe(ext),
          Default(Redistribute(123), Reducers(321)))
      .toList.sortBy(_.position) shouldBe result43
  }

  it should "return its third along pairwise in 3D" in {
    toPipe(num3)
      .pairwiseBetweenWithValue(Along(Third), Lower, toPipe(dataV),
        TestMatrixPairwise.PlusX(plus[Position3D](Along(Third))), ValuePipe(ext),
          Default(Redistribute(123), Redistribute(654) |-> Reducers(321)))
      .toList.sortBy(_.position) shouldBe result44
  }

  it should "return empty data - InMemory" in {
    toPipe(num3)
      .pairwiseBetween(Along(Third), Lower, TypedPipe.empty, Plus(plus[Position3D](Along(Third))), InMemory())
      .toList.sortBy(_.position) shouldBe List()
  }

  it should "return empty data - Default" in {
    toPipe(num3)
      .pairwiseBetween(Along(Third), Lower, TypedPipe.empty, Plus(plus[Position3D](Along(Third))), Default())
      .toList.sortBy(_.position) shouldBe List()
  }
}

class TestSparkMatrixPairwise extends TestMatrixPairwise {

  "A Matrix.pairwise" should "return its first over pairwise in 1D" in {
    toRDD(num1)
      .pairwise(Over(First), Lower, Plus(plus[Position1D](Over(First))), Default())
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first over pairwise in 2D" in {
    toRDD(num2)
      .pairwise(Over(First), Lower, Plus(plus[Position2D](Over(First))), Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its first along pairwise in 2D" in {
    toRDD(num2)
      .pairwise(Along(First), Lower, List(Plus(plus[Position2D](Along(First))),
        Minus(minus[Position2D](Along(First)))), Default(Reducers(12), Reducers(23)))
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its second over pairwise in 2D" in {
    toRDD(num2)
      .pairwise(Over(Second), Lower, List(Plus(plus[Position2D](Over(Second))),
        Minus(minus[Position2D](Over(Second)))), Default())
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its second along pairwise in 2D" in {
    toRDD(num2)
      .pairwise(Along(Second), Lower, Plus(plus[Position2D](Along(Second))), Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its first over pairwise in 3D" in {
    toRDD(num3)
      .pairwise(Over(First), Lower, Plus(plus[Position3D](Over(First))), Default(Reducers(12), Reducers(23)))
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its first along pairwise in 3D" in {
    toRDD(num3)
      .pairwise(Along(First), Lower, List(Plus(plus[Position3D](Along(First))),
        Minus(minus[Position3D](Along(First)))), Default())
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its second over pairwise in 3D" in {
    toRDD(num3)
      .pairwise(Over(Second), Lower, List(Plus(plus[Position3D](Over(Second))),
        Minus(minus[Position3D](Over(Second)))), Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its second along pairwise in 3D" in {
    toRDD(num3)
      .pairwise(Along(Second), Lower, Plus(plus[Position3D](Along(Second))), Default(Reducers(12), Reducers(23)))
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its third over pairwise in 3D" in {
    toRDD(num3)
      .pairwise(Over(Third), Lower, List(Plus(plus[Position3D](Over(Third))), Minus(minus[Position3D](Over(Third)))),
        Default())
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its third along pairwise in 3D" in {
    toRDD(num3)
      .pairwise(Along(Third), Lower, Plus(plus[Position3D](Along(Third))), Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result11
  }

  "A Matrix.pairwiseWithValue" should "return its first over pairwise in 1D" in {
    toRDD(num1)
      .pairwiseWithValue(Over(First), Lower, TestMatrixPairwise.PlusX(plus[Position1D](Over(First))), ext,
        Default(Reducers(12), Reducers(23)))
      .toList.sortBy(_.position) shouldBe result12
  }

  it should "return its first over pairwise in 2D" in {
    toRDD(num2)
      .pairwiseWithValue(Over(First), Lower, TestMatrixPairwise.PlusX(plus[Position2D](Over(First))), ext, Default())
      .toList.sortBy(_.position) shouldBe result13
  }

  it should "return its first along pairwise in 2D" in {
    toRDD(num2)
      .pairwiseWithValue(Along(First), Lower, List(TestMatrixPairwise.PlusX(plus[Position2D](Along(First))),
        TestMatrixPairwise.MinusX(minus[Position2D](Along(First)))), ext, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result14
  }

  it should "return its second over pairwise in 2D" in {
    toRDD(num2)
      .pairwiseWithValue(Over(Second), Lower, List(TestMatrixPairwise.PlusX(plus[Position2D](Over(Second))),
        TestMatrixPairwise.MinusX(minus[Position2D](Over(Second)))), ext, Default(Reducers(12), Reducers(23)))
      .toList.sortBy(_.position) shouldBe result15
  }

  it should "return its second along pairwise in 2D" in {
    toRDD(num2)
      .pairwiseWithValue(Along(Second), Lower, TestMatrixPairwise.PlusX(plus[Position2D](Along(Second))), ext,
        Default())
      .toList.sortBy(_.position) shouldBe result16
  }

  it should "return its first over pairwise in 3D" in {
    toRDD(num3)
      .pairwiseWithValue(Over(First), Lower, TestMatrixPairwise.PlusX(plus[Position3D](Over(First))), ext,
        Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result17
  }

  it should "return its first along pairwise in 3D" in {
    toRDD(num3)
      .pairwiseWithValue(Along(First), Lower, List(TestMatrixPairwise.PlusX(plus[Position3D](Along(First))),
        TestMatrixPairwise.MinusX(minus[Position3D](Along(First)))), ext, Default(Reducers(12), Reducers(23)))
      .toList.sortBy(_.position) shouldBe result18
  }

  it should "return its second over pairwise in 3D" in {
    toRDD(num3)
      .pairwiseWithValue(Over(Second), Lower, List(TestMatrixPairwise.PlusX(plus[Position3D](Over(Second))),
        TestMatrixPairwise.MinusX(minus[Position3D](Over(Second)))), ext, Default())
      .toList.sortBy(_.position) shouldBe result19
  }

  it should "return its second along pairwise in 3D" in {
    toRDD(num3)
      .pairwiseWithValue(Along(Second), Lower, TestMatrixPairwise.PlusX(plus[Position3D](Along(Second))), ext,
        Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result20
  }

  it should "return its third over pairwise in 3D" in {
    toRDD(num3)
      .pairwiseWithValue(Over(Third), Lower, List(TestMatrixPairwise.PlusX(plus[Position3D](Over(Third))),
        TestMatrixPairwise.MinusX(minus[Position3D](Over(Third)))), ext, Default(Reducers(12), Reducers(23)))
      .toList.sortBy(_.position) shouldBe result21
  }

  it should "return its third along pairwise in 3D" in {
    toRDD(num3)
      .pairwiseWithValue(Along(Third), Lower, TestMatrixPairwise.PlusX(plus[Position3D](Along(Third))), ext, Default())
      .toList.sortBy(_.position) shouldBe result22
  }

  "A Matrix.pairwiseBetween" should "return its first over pairwise in 1D" in {
    toRDD(num1)
      .pairwiseBetween(Over(First), Lower, toRDD(dataA), Plus(plus[Position1D](Over(First))), Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result23
  }

  it should "return its first over pairwise in 2D" in {
    toRDD(num2)
      .pairwiseBetween(Over(First), Lower, toRDD(dataB), Plus(plus[Position2D](Over(First))),
        Default(Reducers(12), Reducers(23)))
      .toList.sortBy(_.position) shouldBe result24
  }

  it should "return its first along pairwise in 2D" in {
    toRDD(num2)
      .pairwiseBetween(Along(First), Lower, toRDD(dataC), List(Plus(plus[Position2D](Along(First))),
        Minus(minus[Position2D](Along(First)))), Default())
      .toList.sortBy(_.position) shouldBe result25
  }

  it should "return its second over pairwise in 2D" in {
    toRDD(num2)
      .pairwiseBetween(Over(Second), Lower, toRDD(dataD), List(Plus(plus[Position2D](Over(Second))),
        Minus(minus[Position2D](Over(Second)))), Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result26
  }

  it should "return its second along pairwise in 2D" in {
    toRDD(num2)
      .pairwiseBetween(Along(Second), Lower, toRDD(dataE), Plus(plus[Position2D](Along(Second))),
        Default(Reducers(12), Reducers(23)))
      .toList.sortBy(_.position) shouldBe result27
  }

  it should "return its first over pairwise in 3D" in {
    toRDD(num3)
      .pairwiseBetween(Over(First), Lower, toRDD(dataF), Plus(plus[Position3D](Over(First))), Default())
      .toList.sortBy(_.position) shouldBe result28
  }

  it should "return its first along pairwise in 3D" in {
    toRDD(num3)
      .pairwiseBetween(Along(First), Lower, toRDD(dataG), List(Plus(plus[Position3D](Along(First))),
        Minus(minus[Position3D](Along(First)))), Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result29
  }

  it should "return its second over pairwise in 3D" in {
    toRDD(num3)
      .pairwiseBetween(Over(Second), Lower, toRDD(dataH), List(Plus(plus[Position3D](Over(Second))),
        Minus(minus[Position3D](Over(Second)))), Default(Reducers(12), Reducers(23)))
      .toList.sortBy(_.position) shouldBe result30
  }

  it should "return its second along pairwise in 3D" in {
    toRDD(num3)
      .pairwiseBetween(Along(Second), Lower, toRDD(dataI), Plus(plus[Position3D](Along(Second))), Default())
      .toList.sortBy(_.position) shouldBe result31
  }

  it should "return its third over pairwise in 3D" in {
    toRDD(num3)
      .pairwiseBetween(Over(Third), Lower, toRDD(dataJ), List(Plus(plus[Position3D](Over(Third))),
        Minus(minus[Position3D](Over(Third)))), Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result32
  }

  it should "return its third along pairwise in 3D" in {
    toRDD(num3)
      .pairwiseBetween(Along(Third), Lower, toRDD(dataK), Plus(plus[Position3D](Along(Third))),
        Default(Reducers(12), Reducers(23)))
      .toList.sortBy(_.position) shouldBe result33
  }

  "A Matrix.pairwiseBetweenWithValue" should "return its first over pairwise in 1D" in {
    toRDD(num1)
      .pairwiseBetweenWithValue(Over(First), Lower, toRDD(dataL),
        TestMatrixPairwise.PlusX(plus[Position1D](Over(First))), ext, Default())
      .toList.sortBy(_.position) shouldBe result34
  }

  it should "return its first over pairwise in 2D" in {
    toRDD(num2)
      .pairwiseBetweenWithValue(Over(First), Lower, toRDD(dataM),
        TestMatrixPairwise.PlusX(plus[Position2D](Over(First))), ext, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result35
  }

  it should "return its first along pairwise in 2D" in {
    toRDD(num2)
      .pairwiseBetweenWithValue(Along(First), Lower, toRDD(dataN), List(
        TestMatrixPairwise.PlusX(plus[Position2D](Along(First))),
        TestMatrixPairwise.MinusX(minus[Position2D](Along(First)))), ext, Default(Reducers(12), Reducers(23)))
      .toList.sortBy(_.position) shouldBe result36
  }

  it should "return its second over pairwise in 2D" in {
    toRDD(num2)
      .pairwiseBetweenWithValue(Over(Second), Lower, toRDD(dataO), List(
        TestMatrixPairwise.PlusX(plus[Position2D](Over(Second))),
        TestMatrixPairwise.MinusX(minus[Position2D](Over(Second)))), ext, Default())
      .toList.sortBy(_.position) shouldBe result37
  }

  it should "return its second along pairwise in 2D" in {
    toRDD(num2)
      .pairwiseBetweenWithValue(Along(Second), Lower, toRDD(dataP),
        TestMatrixPairwise.PlusX(plus[Position2D](Along(Second))), ext, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result38
  }

  it should "return its first over pairwise in 3D" in {
    toRDD(num3)
      .pairwiseBetweenWithValue(Over(First), Lower, toRDD(dataQ),
        TestMatrixPairwise.PlusX(plus[Position3D](Over(First))), ext, Default(Reducers(12), Reducers(23)))
      .toList.sortBy(_.position) shouldBe result39
  }

  it should "return its first along pairwise in 3D" in {
    toRDD(num3)
      .pairwiseBetweenWithValue(Along(First), Lower, toRDD(dataR), List(
        TestMatrixPairwise.PlusX(plus[Position3D](Along(First))),
        TestMatrixPairwise.MinusX(minus[Position3D](Along(First)))), ext, Default())
      .toList.sortBy(_.position) shouldBe result40
  }

  it should "return its second over pairwise in 3D" in {
    toRDD(num3)
      .pairwiseBetweenWithValue(Over(Second), Lower, toRDD(dataS), List(
        TestMatrixPairwise.PlusX(plus[Position3D](Over(Second))),
        TestMatrixPairwise.MinusX(minus[Position3D](Over(Second)))), ext, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result41
  }

  it should "return its second along pairwise in 3D" in {
    toRDD(num3)
      .pairwiseBetweenWithValue(Along(Second), Lower, toRDD(dataT),
        TestMatrixPairwise.PlusX(plus[Position3D](Along(Second))), ext, Default(Reducers(12), Reducers(23)))
      .toList.sortBy(_.position) shouldBe result42
  }

  it should "return its third over pairwise in 3D" in {
    toRDD(num3)
      .pairwiseBetweenWithValue(Over(Third), Lower, toRDD(dataU), List(
        TestMatrixPairwise.PlusX(plus[Position3D](Over(Third))),
        TestMatrixPairwise.MinusX(minus[Position3D](Over(Third)))), ext, Default())
      .toList.sortBy(_.position) shouldBe result43
  }

  it should "return its third along pairwise in 3D" in {
    toRDD(num3)
      .pairwiseBetweenWithValue(Along(Third), Lower, toRDD(dataV),
        TestMatrixPairwise.PlusX(plus[Position3D](Along(Third))), ext, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result44
  }

  it should "return empty data - Default" in {
    toRDD(num3)
      .pairwiseBetween(Along(Third), Lower, toRDD(List.empty[Cell[Position3D]]),
        Plus(plus[Position3D](Along(Third))), Default())
      .toList.sortBy(_.position) shouldBe List()
  }
}

trait TestMatrixChange extends TestMatrix {

  val result1 = List(Cell(Position1D("bar"), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position1D("baz"), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position1D("foo"), Content(ContinuousSchema(DoubleCodex), 3.14)),
    Cell(Position1D("qux"), Content(OrdinalSchema(StringCodex), "12.56")))

  val result2 = List(Cell(Position2D("bar", 1), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position2D("bar", 2), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position2D("bar", 3), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position2D("baz", 1), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position2D("baz", 2), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position2D("foo", 1), Content(ContinuousSchema(DoubleCodex), 3.14)),
    Cell(Position2D("foo", 2), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position2D("foo", 3), Content(ContinuousSchema(DoubleCodex), 9.42)),
    Cell(Position2D("qux", 1), Content(OrdinalSchema(StringCodex), "12.56")))

  val result3 = List(Cell(Position2D("bar", 1), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position2D("bar", 2), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position2D("bar", 3), Content(ContinuousSchema(DoubleCodex), 19)),
    Cell(Position2D("baz", 1), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position2D("baz", 2), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position2D("foo", 1), Content(OrdinalSchema(StringCodex), "3.14")),
    Cell(Position2D("foo", 2), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position2D("foo", 3), Content(ContinuousSchema(DoubleCodex), 9.42)),
    Cell(Position2D("qux", 1), Content(OrdinalSchema(StringCodex), "12.56")))

  val result4 = List(Cell(Position2D("bar", 1), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position2D("bar", 2), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position2D("bar", 3), Content(ContinuousSchema(DoubleCodex), 19)),
    Cell(Position2D("baz", 1), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position2D("baz", 2), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position2D("foo", 1), Content(OrdinalSchema(StringCodex), "3.14")),
    Cell(Position2D("foo", 2), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position2D("foo", 3), Content(ContinuousSchema(DoubleCodex), 9.42)),
    Cell(Position2D("qux", 1), Content(OrdinalSchema(StringCodex), "12.56")))

  val result5 = List(Cell(Position2D("bar", 1), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position2D("bar", 2), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position2D("bar", 3), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position2D("baz", 1), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position2D("baz", 2), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position2D("foo", 1), Content(ContinuousSchema(DoubleCodex), 3.14)),
    Cell(Position2D("foo", 2), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position2D("foo", 3), Content(ContinuousSchema(DoubleCodex), 9.42)),
    Cell(Position2D("qux", 1), Content(OrdinalSchema(StringCodex), "12.56")))

  val result6 = List(Cell(Position3D("bar", 1, "xyz"), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("bar", 3, "xyz"), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position3D("baz", 1, "xyz"), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position3D("baz", 2, "xyz"), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position3D("foo", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 3.14)),
    Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position3D("foo", 3, "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42)),
    Cell(Position3D("qux", 1, "xyz"), Content(OrdinalSchema(StringCodex), "12.56")))

  val result7 = List(Cell(Position3D("bar", 1, "xyz"), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("bar", 3, "xyz"), Content(ContinuousSchema(DoubleCodex), 19)),
    Cell(Position3D("baz", 1, "xyz"), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position3D("baz", 2, "xyz"), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position3D("foo", 1, "xyz"), Content(OrdinalSchema(StringCodex), "3.14")),
    Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position3D("foo", 3, "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42)),
    Cell(Position3D("qux", 1, "xyz"), Content(OrdinalSchema(StringCodex), "12.56")))

  val result8 = List(Cell(Position3D("bar", 1, "xyz"), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("bar", 3, "xyz"), Content(ContinuousSchema(DoubleCodex), 19)),
    Cell(Position3D("baz", 1, "xyz"), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position3D("baz", 2, "xyz"), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position3D("foo", 1, "xyz"), Content(OrdinalSchema(StringCodex), "3.14")),
    Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position3D("foo", 3, "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42)),
    Cell(Position3D("qux", 1, "xyz"), Content(OrdinalSchema(StringCodex), "12.56")))

  val result9 = List(Cell(Position3D("bar", 1, "xyz"), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("bar", 3, "xyz"), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position3D("baz", 1, "xyz"), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position3D("baz", 2, "xyz"), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position3D("foo", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 3.14)),
    Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position3D("foo", 3, "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42)),
    Cell(Position3D("qux", 1, "xyz"), Content(OrdinalSchema(StringCodex), "12.56")))

  val result10 = List(Cell(Position3D("bar", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("bar", 3, "xyz"), Content(ContinuousSchema(DoubleCodex), 19)),
    Cell(Position3D("baz", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42)),
    Cell(Position3D("baz", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 19)),
    Cell(Position3D("foo", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 3.14)),
    Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position3D("foo", 3, "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42)),
    Cell(Position3D("qux", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)))

  val result11 = List(Cell(Position3D("bar", 1, "xyz"), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("bar", 3, "xyz"), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position3D("baz", 1, "xyz"), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position3D("baz", 2, "xyz"), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position3D("foo", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 3.14)),
    Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position3D("foo", 3, "xyz"), Content(NominalSchema(StringCodex), "9.42")),
    Cell(Position3D("foo", 4, "xyz"), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
    Cell(Position3D("qux", 1, "xyz"), Content(OrdinalSchema(StringCodex), "12.56")))
}

class TestScaldingMatrixChange extends TestMatrixChange {

  "A Matrix.change" should "return its first over data in 1D" in {
    toPipe(data1)
      .change(Over(First), "foo", ContinuousSchema(DoubleCodex), InMemory())
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first over data in 2D" in {
    toPipe(data2)
      .change(Over(First), "foo", ContinuousSchema(DoubleCodex), Default())
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its first along data in 2D" in {
    toPipe(data2)
      .change(Along(First), List(3, 4), ContinuousSchema(DoubleCodex), Default(Reducers(123)))
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its second over data in 2D" in {
    toPipe(data2)
      .change(Over(Second), List(3, 4), ContinuousSchema(DoubleCodex), Unbalanced(Reducers(123)))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its second along data in 2D" in {
    toPipe(data2)
      .change(Along(Second), "foo", ContinuousSchema(DoubleCodex), InMemory())
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its first over data in 3D" in {
    toPipe(data3)
      .change(Over(First), "foo", ContinuousSchema(DoubleCodex), Default())
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its first along data in 3D" in {
    toPipe(data3)
      .change(Along(First), List(Position2D(3, "xyz"), Position2D(4, "xyz")),
        ContinuousSchema(DoubleCodex), Default(Reducers(123)))
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its second over data in 3D" in {
    toPipe(data3)
      .change(Over(Second), List(3, 4), ContinuousSchema(DoubleCodex), Unbalanced(Reducers(123)))
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its second along data in 3D" in {
    toPipe(data3)
      .change(Along(Second), Position2D("foo", "xyz"), ContinuousSchema(DoubleCodex), InMemory())
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its third over data in 3D" in {
    toPipe(data3)
      .change(Over(Third), List("xyz"), ContinuousSchema(DoubleCodex), Default())
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its third along data in 3D" in {
    toPipe(data3)
      .change(Along(Third), Position2D("foo", 1), ContinuousSchema(DoubleCodex), Default(Reducers(123)))
      .toList.sortBy(_.position) shouldBe result11
  }

  it should "return with empty data - InMemory" in {
    toPipe(data3)
      .change(Over(First), List.empty[Position1D], ContinuousSchema(DoubleCodex), InMemory())
      .toList.sortBy(_.position) shouldBe data3.sortBy(_.position)
  }

  it should "return with empty data - Default" in {
    toPipe(data3)
      .change(Over(First), List.empty[Position1D], ContinuousSchema(DoubleCodex), Default())
      .toList.sortBy(_.position) shouldBe data3.sortBy(_.position)
  }
}

class TestSparkMatrixChange extends TestMatrixChange {

  "A Matrix.change" should "return its first over data in 1D" in {
    toRDD(data1)
      .change(Over(First), "foo", ContinuousSchema(DoubleCodex), Default())
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first over data in 2D" in {
    toRDD(data2)
      .change(Over(First), "foo", ContinuousSchema(DoubleCodex), Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its first along data in 2D" in {
    toRDD(data2)
      .change(Along(First), List(3, 4), ContinuousSchema(DoubleCodex), Default())
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its second over data in 2D" in {
    toRDD(data2)
      .change(Over(Second), List(3, 4), ContinuousSchema(DoubleCodex), Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its second along data in 2D" in {
    toRDD(data2)
      .change(Along(Second), "foo", ContinuousSchema(DoubleCodex), Default())
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its first over data in 3D" in {
    toRDD(data3)
      .change(Over(First), "foo", ContinuousSchema(DoubleCodex), Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its first along data in 3D" in {
    toRDD(data3)
      .change(Along(First), List(Position2D(3, "xyz"), Position2D(4, "xyz")), ContinuousSchema(DoubleCodex), Default())
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its second over data in 3D" in {
    toRDD(data3)
      .change(Over(Second), List(3, 4), ContinuousSchema(DoubleCodex), Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its second along data in 3D" in {
    toRDD(data3)
      .change(Along(Second), Position2D("foo", "xyz"), ContinuousSchema(DoubleCodex), Default())
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its third over data in 3D" in {
    toRDD(data3)
      .change(Over(Third), List("xyz"), ContinuousSchema(DoubleCodex), Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its third along data in 3D" in {
    toRDD(data3)
      .change(Along(Third), Position2D("foo", 1), ContinuousSchema(DoubleCodex), Default())
      .toList.sortBy(_.position) shouldBe result11
  }

  it should "return with empty data - Default" in {
    toRDD(data3)
      .change(Over(First), List.empty[Position1D], ContinuousSchema(DoubleCodex), Default())
      .toList.sortBy(_.position) shouldBe data3.sortBy(_.position)
  }
}

trait TestMatrixSet extends TestMatrix {

  val dataA = List(Cell(Position1D("foo"), Content(ContinuousSchema(DoubleCodex), 1)),
    Cell(Position1D("quxx"), Content(ContinuousSchema(DoubleCodex), 2)))

  val dataB = List(Cell(Position2D("foo", 2), Content(ContinuousSchema(DoubleCodex), 1)),
    Cell(Position2D("quxx", 5), Content(ContinuousSchema(DoubleCodex), 2)))

  val dataC = List(Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 1)),
    Cell(Position3D("quxx", 5, "abc"), Content(ContinuousSchema(DoubleCodex), 2)))

  val result1 = List(Cell(Position1D("bar"), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position1D("baz"), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position1D("foo"), Content(ContinuousSchema(DoubleCodex), 1)),
    Cell(Position1D("qux"), Content(OrdinalSchema(StringCodex), "12.56")))

  val result2 = List(Cell(Position1D("bar"), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position1D("baz"), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position1D("foo"), Content(ContinuousSchema(DoubleCodex), 1)),
    Cell(Position1D("qux"), Content(OrdinalSchema(StringCodex), "12.56")),
    Cell(Position1D("quxx"), Content(ContinuousSchema(DoubleCodex), 1)))

  val result3 = List(Cell(Position1D("bar"), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position1D("baz"), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position1D("foo"), Content(ContinuousSchema(DoubleCodex), 1)),
    Cell(Position1D("qux"), Content(OrdinalSchema(StringCodex), "12.56")),
    Cell(Position1D("quxx"), Content(ContinuousSchema(DoubleCodex), 2)))

  val result4 = List(Cell(Position2D("bar", 1), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position2D("bar", 2), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position2D("bar", 3), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position2D("baz", 1), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position2D("baz", 2), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position2D("foo", 1), Content(OrdinalSchema(StringCodex), "3.14")),
    Cell(Position2D("foo", 2), Content(ContinuousSchema(DoubleCodex), 1)),
    Cell(Position2D("foo", 3), Content(NominalSchema(StringCodex), "9.42")),
    Cell(Position2D("foo", 4), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
    Cell(Position2D("qux", 1), Content(OrdinalSchema(StringCodex), "12.56")))

  val result5 = List(Cell(Position2D("bar", 1), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position2D("bar", 2), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position2D("bar", 3), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position2D("baz", 1), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position2D("baz", 2), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position2D("foo", 1), Content(OrdinalSchema(StringCodex), "3.14")),
    Cell(Position2D("foo", 2), Content(ContinuousSchema(DoubleCodex), 1)),
    Cell(Position2D("foo", 3), Content(NominalSchema(StringCodex), "9.42")),
    Cell(Position2D("foo", 4), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
    Cell(Position2D("qux", 1), Content(OrdinalSchema(StringCodex), "12.56")),
    Cell(Position2D("quxx", 5), Content(ContinuousSchema(DoubleCodex), 1)))

  val result6 = List(Cell(Position2D("bar", 1), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position2D("bar", 2), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position2D("bar", 3), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position2D("baz", 1), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position2D("baz", 2), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position2D("foo", 1), Content(OrdinalSchema(StringCodex), "3.14")),
    Cell(Position2D("foo", 2), Content(ContinuousSchema(DoubleCodex), 1)),
    Cell(Position2D("foo", 3), Content(NominalSchema(StringCodex), "9.42")),
    Cell(Position2D("foo", 4), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
    Cell(Position2D("qux", 1), Content(OrdinalSchema(StringCodex), "12.56")),
    Cell(Position2D("quxx", 5), Content(ContinuousSchema(DoubleCodex), 2)))

  val result7 = List(Cell(Position3D("bar", 1, "xyz"), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("bar", 3, "xyz"), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position3D("baz", 1, "xyz"), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position3D("baz", 2, "xyz"), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position3D("foo", 1, "xyz"), Content(OrdinalSchema(StringCodex), "3.14")),
    Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 1)),
    Cell(Position3D("foo", 3, "xyz"), Content(NominalSchema(StringCodex), "9.42")),
    Cell(Position3D("foo", 4, "xyz"), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
    Cell(Position3D("qux", 1, "xyz"), Content(OrdinalSchema(StringCodex), "12.56")))

  val result8 = List(Cell(Position3D("bar", 1, "xyz"), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("bar", 3, "xyz"), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position3D("baz", 1, "xyz"), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position3D("baz", 2, "xyz"), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position3D("foo", 1, "xyz"), Content(OrdinalSchema(StringCodex), "3.14")),
    Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 1)),
    Cell(Position3D("foo", 3, "xyz"), Content(NominalSchema(StringCodex), "9.42")),
    Cell(Position3D("foo", 4, "xyz"), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
    Cell(Position3D("qux", 1, "xyz"), Content(OrdinalSchema(StringCodex), "12.56")),
    Cell(Position3D("quxx", 5, "abc"), Content(ContinuousSchema(DoubleCodex), 1)))

  val result9 = List(Cell(Position3D("bar", 1, "xyz"), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("bar", 3, "xyz"), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position3D("baz", 1, "xyz"), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position3D("baz", 2, "xyz"), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position3D("foo", 1, "xyz"), Content(OrdinalSchema(StringCodex), "3.14")),
    Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 1)),
    Cell(Position3D("foo", 3, "xyz"), Content(NominalSchema(StringCodex), "9.42")),
    Cell(Position3D("foo", 4, "xyz"), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
    Cell(Position3D("qux", 1, "xyz"), Content(OrdinalSchema(StringCodex), "12.56")),
    Cell(Position3D("quxx", 5, "abc"), Content(ContinuousSchema(DoubleCodex), 2)))
}

class TestScaldingMatrixSet extends TestMatrixSet {

  "A Matrix.set" should "return its updated data in 1D" in {
    toPipe(data1)
      .set(Cell(Position1D("foo"), Content(ContinuousSchema(DoubleCodex), 1)), Default())
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its updated and added data in 1D" in {
    toPipe(data1)
      .set(List("foo", "quxx")
        .map { case pos => Cell(Position1D(pos), Content(ContinuousSchema(DoubleCodex), 1)) }, Default(Reducers(123)))
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its matrix updated data in 1D" in {
    toPipe(data1)
      .set(toPipe(dataA), Default())
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its updated data in 2D" in {
    toPipe(data2)
      .set(Cell(Position2D("foo", 2), Content(ContinuousSchema(DoubleCodex), 1)), Default(Reducers(123)))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its updated and added data in 2D" in {
    toPipe(data2)
      .set(List(Position2D("foo", 2), Position2D("quxx", 5))
        .map { case pos => Cell(pos, Content(ContinuousSchema(DoubleCodex), 1)) }, Default())
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its matrix updated data in 2D" in {
    toPipe(data2)
      .set(toPipe(dataB), Default(Reducers(123)))
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its updated data in 3D" in {
    toPipe(data3)
      .set(Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 1)), Default())
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its updated and added data in 3D" in {
    toPipe(data3)
      .set(List(Position3D("foo", 2, "xyz"), Position3D("quxx", 5, "abc"))
        .map { case pos => Cell(pos, Content(ContinuousSchema(DoubleCodex), 1)) }, Default(Reducers(123)))
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its matrix updated data in 3D" in {
    toPipe(data3)
      .set(toPipe(dataC), Default())
      .toList.sortBy(_.position) shouldBe result9
  }
}

class TestSparkMatrixSet extends TestMatrixSet {

  "A Matrix.set" should "return its updated data in 1D" in {
    toRDD(data1)
      .set(Cell(Position1D("foo"), Content(ContinuousSchema(DoubleCodex), 1)), Default())
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its updated and added data in 1D" in {
    toRDD(data1)
      .set(List("foo", "quxx").map { case pos => Cell(Position1D(pos), Content(ContinuousSchema(DoubleCodex), 1)) },
        Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its matrix updated data in 1D" in {
    toRDD(data1)
      .set(toRDD(dataA), Default())
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its updated data in 2D" in {
    toRDD(data2)
      .set(Cell(Position2D("foo", 2), Content(ContinuousSchema(DoubleCodex), 1)), Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its updated and added data in 2D" in {
    toRDD(data2)
      .set(List(Position2D("foo", 2), Position2D("quxx", 5))
        .map { case pos => Cell(pos, Content(ContinuousSchema(DoubleCodex), 1)) }, Default())
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its matrix updated data in 2D" in {
    toRDD(data2)
      .set(toRDD(dataB), Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its updated data in 3D" in {
    toRDD(data3)
      .set(Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 1)), Default())
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its updated and added data in 3D" in {
    toRDD(data3)
      .set(List(Position3D("foo", 2, "xyz"), Position3D("quxx", 5, "abc"))
        .map { case pos => Cell(pos, Content(ContinuousSchema(DoubleCodex), 1)) }, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its matrix updated data in 3D" in {
    toRDD(data3)
      .set(toRDD(dataC), Default())
      .toList.sortBy(_.position) shouldBe result9
  }
}

trait TestMatrixTransform extends TestMatrix {

  val ext = Map(
    Position1D("foo") -> Map(Position1D("max.abs") -> Content(ContinuousSchema(DoubleCodex), 3.14),
      Position1D("mean") -> Content(ContinuousSchema(DoubleCodex), 3.14),
      Position1D("sd") -> Content(ContinuousSchema(DoubleCodex), 1)),
    Position1D("bar") -> Map(Position1D("max.abs") -> Content(ContinuousSchema(DoubleCodex), 6.28),
      Position1D("mean") -> Content(ContinuousSchema(DoubleCodex), 3.14),
      Position1D("sd") -> Content(ContinuousSchema(DoubleCodex), 2)),
    Position1D("baz") -> Map(Position1D("max.abs") -> Content(ContinuousSchema(DoubleCodex), 9.42),
      Position1D("mean") -> Content(ContinuousSchema(DoubleCodex), 3.14),
      Position1D("sd") -> Content(ContinuousSchema(DoubleCodex), 3)),
    Position1D("qux") -> Map(Position1D("max.abs") -> Content(ContinuousSchema(DoubleCodex), 12.56),
      Position1D("mean") -> Content(ContinuousSchema(DoubleCodex), 3.14),
      Position1D("sd") -> Content(ContinuousSchema(DoubleCodex), 4)))

  val result1 = List(Cell(Position1D("bar.ind"), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position1D("baz.ind"), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position1D("foo.ind"), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position1D("qux.ind"), Content(DiscreteSchema(LongCodex), 1)))

  val result2 = List(Cell(Position2D("bar.ind", 1), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position2D("bar.ind", 2), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position2D("bar.ind", 3), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position2D("bar=19", 3), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position2D("bar=6.28", 1), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position2D("baz.ind", 1), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position2D("baz.ind", 2), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position2D("baz=9.42", 1), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position2D("foo.ind", 1), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position2D("foo.ind", 2), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position2D("foo.ind", 3), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position2D("foo.ind", 4), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position2D("foo=3.14", 1), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position2D("foo=9.42", 3), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position2D("qux.ind", 1), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position2D("qux=12.56", 1), Content(DiscreteSchema(LongCodex), 1)))

  val result3 = List(Cell(Position3D("bar.ind", 1, "xyz"), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position3D("bar.ind", 2, "xyz"), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position3D("bar.ind", 3, "xyz"), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position3D("bar=19", 3, "xyz"), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position3D("bar=6.28", 1, "xyz"), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position3D("baz.ind", 1, "xyz"), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position3D("baz.ind", 2, "xyz"), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position3D("baz=9.42", 1, "xyz"), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position3D("foo.ind", 1, "xyz"), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position3D("foo.ind", 2, "xyz"), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position3D("foo.ind", 3, "xyz"), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position3D("foo.ind", 4, "xyz"), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position3D("foo=3.14", 1, "xyz"), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position3D("foo=9.42", 3, "xyz"), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position3D("qux.ind", 1, "xyz"), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position3D("qux=12.56", 1, "xyz"), Content(DiscreteSchema(LongCodex), 1)))

  val result4 = List(Cell(Position1D("bar.n"), Content(ContinuousSchema(DoubleCodex), 6.28 / 6.28)),
    Cell(Position1D("bar.s"), Content(ContinuousSchema(DoubleCodex), (6.28 - 3.14) / 2)),
    Cell(Position1D("baz.n"), Content(ContinuousSchema(DoubleCodex), 9.42 / 9.42)),
    Cell(Position1D("baz.s"), Content(ContinuousSchema(DoubleCodex), (9.42 - 3.14) / 3)),
    Cell(Position1D("foo.n"), Content(ContinuousSchema(DoubleCodex), 3.14 / 3.14)),
    Cell(Position1D("foo.s"), Content(ContinuousSchema(DoubleCodex), (3.14 - 3.14) / 1)),
    Cell(Position1D("qux.n"), Content(ContinuousSchema(DoubleCodex), 12.56 / 12.56)),
    Cell(Position1D("qux.s"), Content(ContinuousSchema(DoubleCodex), (12.56 - 3.14) / 4)))

  val result5 = List(Cell(Position2D("bar.n", 1), Content(ContinuousSchema(DoubleCodex), 6.28 / 6.28)),
    Cell(Position2D("bar.n", 2), Content(ContinuousSchema(DoubleCodex), 12.56 / 6.28)),
    Cell(Position2D("bar.n", 3), Content(ContinuousSchema(DoubleCodex), 18.84 / 6.28)),
    Cell(Position2D("baz.n", 1), Content(ContinuousSchema(DoubleCodex), 9.42 / 9.42)),
    Cell(Position2D("baz.n", 2), Content(ContinuousSchema(DoubleCodex), 18.84 / 9.42)),
    Cell(Position2D("foo.n", 1), Content(ContinuousSchema(DoubleCodex), 3.14 / 3.14)),
    Cell(Position2D("foo.n", 2), Content(ContinuousSchema(DoubleCodex), 6.28 / 3.14)),
    Cell(Position2D("foo.n", 3), Content(ContinuousSchema(DoubleCodex), 9.42 / 3.14)),
    Cell(Position2D("foo.n", 4), Content(ContinuousSchema(DoubleCodex), 12.56 / 3.14)),
    Cell(Position2D("qux.n", 1), Content(ContinuousSchema(DoubleCodex), 12.56 / 12.56)))

  val result6 = List(Cell(Position3D("bar.n", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28 / 6.28)),
    Cell(Position3D("bar.n", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 / 6.28)),
    Cell(Position3D("bar.n", 3, "xyz"), Content(ContinuousSchema(DoubleCodex), 18.84 / 6.28)),
    Cell(Position3D("baz.n", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42 / 9.42)),
    Cell(Position3D("baz.n", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 18.84 / 9.42)),
    Cell(Position3D("foo.n", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 3.14 / 3.14)),
    Cell(Position3D("foo.n", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28 / 3.14)),
    Cell(Position3D("foo.n", 3, "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42 / 3.14)),
    Cell(Position3D("foo.n", 4, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 / 3.14)),
    Cell(Position3D("qux.n", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56 / 12.56)))

  val result7 = List(Cell(Position2D("bar.ind", "ind"), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position2D("baz.ind", "ind"), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position2D("foo.ind", "ind"), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position2D("qux.ind", "ind"), Content(DiscreteSchema(LongCodex), 1)))

  val result8 = List(Cell(Position3D("bar.ind", 1, "ind"), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position3D("bar.ind", 2, "ind"), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position3D("bar.ind", 3, "ind"), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position3D("bar=19", 3, "bin"), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position3D("bar=6.28", 1, "bin"), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position3D("baz.ind", 1, "ind"), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position3D("baz.ind", 2, "ind"), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position3D("baz=9.42", 1, "bin"), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position3D("foo.ind", 1, "ind"), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position3D("foo.ind", 2, "ind"), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position3D("foo.ind", 3, "ind"), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position3D("foo.ind", 4, "ind"), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position3D("foo=3.14", 1, "bin"), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position3D("foo=9.42", 3, "bin"), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position3D("qux.ind", 1, "ind"), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position3D("qux=12.56", 1, "bin"), Content(DiscreteSchema(LongCodex), 1)))

  val result9 = List(Cell(Position4D("bar.ind", 1, "xyz", "ind"), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position4D("bar.ind", 2, "xyz", "ind"), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position4D("bar.ind", 3, "xyz", "ind"), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position4D("bar=19", 3, "xyz", "bin"), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position4D("bar=6.28", 1, "xyz", "bin"), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position4D("baz.ind", 1, "xyz", "ind"), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position4D("baz.ind", 2, "xyz", "ind"), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position4D("baz=9.42", 1, "xyz", "bin"), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position4D("foo.ind", 1, "xyz", "ind"), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position4D("foo.ind", 2, "xyz", "ind"), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position4D("foo.ind", 3, "xyz", "ind"), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position4D("foo.ind", 4, "xyz", "ind"), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position4D("foo=3.14", 1, "xyz", "bin"), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position4D("foo=9.42", 3, "xyz", "bin"), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position4D("qux.ind", 1, "xyz", "ind"), Content(DiscreteSchema(LongCodex), 1)),
    Cell(Position4D("qux=12.56", 1, "xyz", "bin"), Content(DiscreteSchema(LongCodex), 1)))

  val result10 = List(Cell(Position2D("bar.n", "nrm"), Content(ContinuousSchema(DoubleCodex), 6.28 / 6.28)),
    Cell(Position2D("bar.s", "std"), Content(ContinuousSchema(DoubleCodex), (6.28 - 3.14) / 2)),
    Cell(Position2D("baz.n", "nrm"), Content(ContinuousSchema(DoubleCodex), 9.42 / 9.42)),
    Cell(Position2D("baz.s", "std"), Content(ContinuousSchema(DoubleCodex), (9.42 - 3.14) / 3)),
    Cell(Position2D("foo.n", "nrm"), Content(ContinuousSchema(DoubleCodex), 3.14 / 3.14)),
    Cell(Position2D("foo.s", "std"), Content(ContinuousSchema(DoubleCodex), (3.14 - 3.14) / 1)),
    Cell(Position2D("qux.n", "nrm"), Content(ContinuousSchema(DoubleCodex), 12.56 / 12.56)),
    Cell(Position2D("qux.s", "std"), Content(ContinuousSchema(DoubleCodex), (12.56 - 3.14) / 4)))

  val result11 = List(Cell(Position3D("bar.n", 1, "nrm"), Content(ContinuousSchema(DoubleCodex), 6.28 / 6.28)),
    Cell(Position3D("bar.n", 2, "nrm"), Content(ContinuousSchema(DoubleCodex), 12.56 / 6.28)),
    Cell(Position3D("bar.n", 3, "nrm"), Content(ContinuousSchema(DoubleCodex), 18.84 / 6.28)),
    Cell(Position3D("baz.n", 1, "nrm"), Content(ContinuousSchema(DoubleCodex), 9.42 / 9.42)),
    Cell(Position3D("baz.n", 2, "nrm"), Content(ContinuousSchema(DoubleCodex), 18.84 / 9.42)),
    Cell(Position3D("foo.n", 1, "nrm"), Content(ContinuousSchema(DoubleCodex), 3.14 / 3.14)),
    Cell(Position3D("foo.n", 2, "nrm"), Content(ContinuousSchema(DoubleCodex), 6.28 / 3.14)),
    Cell(Position3D("foo.n", 3, "nrm"), Content(ContinuousSchema(DoubleCodex), 9.42 / 3.14)),
    Cell(Position3D("foo.n", 4, "nrm"), Content(ContinuousSchema(DoubleCodex), 12.56 / 3.14)),
    Cell(Position3D("qux.n", 1, "nrm"), Content(ContinuousSchema(DoubleCodex), 12.56 / 12.56)))

  val result12 = List(
    Cell(Position4D("bar.n", 1, "xyz", "nrm"), Content(ContinuousSchema(DoubleCodex), 6.28 / 6.28)),
    Cell(Position4D("bar.n", 2, "xyz", "nrm"), Content(ContinuousSchema(DoubleCodex), 12.56 / 6.28)),
    Cell(Position4D("bar.n", 3, "xyz", "nrm"), Content(ContinuousSchema(DoubleCodex), 18.84 / 6.28)),
    Cell(Position4D("baz.n", 1, "xyz", "nrm"), Content(ContinuousSchema(DoubleCodex), 9.42 / 9.42)),
    Cell(Position4D("baz.n", 2, "xyz", "nrm"), Content(ContinuousSchema(DoubleCodex), 18.84 / 9.42)),
    Cell(Position4D("foo.n", 1, "xyz", "nrm"), Content(ContinuousSchema(DoubleCodex), 3.14 / 3.14)),
    Cell(Position4D("foo.n", 2, "xyz", "nrm"), Content(ContinuousSchema(DoubleCodex), 6.28 / 3.14)),
    Cell(Position4D("foo.n", 3, "xyz", "nrm"), Content(ContinuousSchema(DoubleCodex), 9.42 / 3.14)),
    Cell(Position4D("foo.n", 4, "xyz", "nrm"), Content(ContinuousSchema(DoubleCodex), 12.56 / 3.14)),
    Cell(Position4D("qux.n", 1, "xyz", "nrm"), Content(ContinuousSchema(DoubleCodex), 12.56 / 12.56)))

  type W = Map[Position1D, Map[Position1D, Content]]

  def extractor[P <: Position](dim: Dimension, key: String)(implicit ev: PosDimDep[P, dim.type]) = {
    ExtractWithDimensionAndKey[P, Content](dim, key).andThenPresent(_.value.asDouble)
  }
}

class TestScaldingMatrixTransform extends TestMatrixTransform {

  "A Matrix.transform" should "return its transformed data in 1D" in {
    toPipe(data1)
      .transform(Indicator[Position1D]()
        .andThenRelocate(Locate.RenameDimension(First, "%1$s.ind")))
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its transformed data in 2D" in {
    toPipe(data2)
      .transform(List(
        Indicator[Position2D]().andThenRelocate(Locate.RenameDimension(First, "%1$s.ind")),
        Binarise[Position2D](Locate.RenameDimensionWithContent(First))))
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its transformed data in 3D" in {
    toPipe(data3)
      .transform(List(
        Indicator[Position3D]().andThenRelocate(Locate.RenameDimension(First, "%1$s.ind")),
        Binarise[Position3D](Locate.RenameDimensionWithContent(First))))
      .toList.sortBy(_.position) shouldBe result3
  }

  "A Matrix.transformWithValue" should "return its transformed data in 1D" in {
    toPipe(num1)
      .transformWithValue(List(
        Normalise(extractor[Position1D](First, "max.abs"))
          .andThenRelocate(Locate.RenameDimension(First, "%1$s.n")),
        Standardise(extractor[Position1D](First, "mean"), extractor[Position1D](First, "sd"))
          .andThenRelocate(Locate.RenameDimension(First, "%1$s.s"))),
        ValuePipe(ext))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its transformed data in 2D" in {
    toPipe(num2)
      .transformWithValue(
        Normalise(extractor[Position2D](First, "max.abs"))
          .andThenRelocate(Locate.RenameDimension(First, "%1$s.n")),
        ValuePipe(ext))
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its transformed data in 3D" in {
    toPipe(num3)
      .transformWithValue(
        Normalise(extractor[Position3D](First, "max.abs"))
          .andThenRelocate(Locate.RenameDimension(First, "%1$s.n")),
        ValuePipe(ext))
      .toList.sortBy(_.position) shouldBe result6
  }

  "A Matrix.transformAndExpand" should "return its transformed data in 1D" in {
    toPipe(data1)
      .transform(Indicator[Position1D]()
        .andThenRelocate(Locate.RenameDimension(First, "%1$s.ind"))
        .andThenRelocate(c => c.position.append("ind").toOption))
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its transformed data in 2D" in {
    toPipe(data2)
      .transform(List(
        Indicator[Position2D]()
          .andThenRelocate(Locate.RenameDimension(First, "%1$s.ind"))
          .andThenRelocate(c => c.position.append("ind").toOption),
        Binarise[Position2D](Locate.RenameDimensionWithContent(First))
          .andThenRelocate(c => c.position.append("bin").toOption)))
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its transformed data in 3D" in {
    toPipe(data3)
      .transform(List(
        Indicator[Position3D]()
          .andThenRelocate(Locate.RenameDimension(First, "%1$s.ind"))
          .andThenRelocate(c => c.position.append("ind").toOption),
        Binarise[Position3D](Locate.RenameDimensionWithContent(First))
          .andThenRelocate(c => c.position.append("bin").toOption)))
      .toList.sortBy(_.position) shouldBe result9
  }

  "A Matrix.transformAndExpandWithValue" should "return its transformed data in 1D" in {
    toPipe(num1)
      .transformWithValue(List(
        Normalise(extractor[Position1D](First, "max.abs"))
          .andThenRelocate(Locate.RenameDimension(First, "%1$s.n"))
          .andThenRelocateWithValue((c, _) => c.position.append("nrm").toOption),
        Standardise(extractor[Position1D](First, "mean"), extractor[Position1D](First, "sd"))
          .andThenRelocate(Locate.RenameDimension(First, "%1$s.s"))
          .andThenRelocateWithValue((c, _) => c.position.append("std").toOption)),
        ValuePipe(ext))
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its transformed data in 2D" in {
    toPipe(num2)
      .transformWithValue(
        Normalise(extractor[Position2D](First, "max.abs"))
          .andThenRelocate(Locate.RenameDimension(First, "%1$s.n"))
          .andThenRelocateWithValue((c, _) => c.position.append("nrm").toOption),
        ValuePipe(ext))
      .toList.sortBy(_.position) shouldBe result11
  }

  it should "return its transformed data in 3D" in {
    toPipe(num3)
      .transformWithValue(
        Normalise(extractor[Position3D](First, "max.abs"))
          .andThenRelocate(Locate.RenameDimension(First, "%1$s.n"))
          .andThenRelocateWithValue((c, _) => c.position.append("nrm").toOption),
        ValuePipe(ext))
      .toList.sortBy(_.position) shouldBe result12
  }
}

class TestSparkMatrixTransform extends TestMatrixTransform {

  "A Matrix.transform" should "return its transformed data in 1D" in {
    toRDD(data1)
      .transform(Indicator[Position1D]()
        .andThenRelocate(Locate.RenameDimension(First, "%1$s.ind")))
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its transformed data in 2D" in {
    toRDD(data2)
      .transform(List(
        Indicator[Position2D]().andThenRelocate(Locate.RenameDimension(First, "%1$s.ind")),
        Binarise[Position2D](Locate.RenameDimensionWithContent(First))))
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its transformed data in 3D" in {
    toRDD(data3)
      .transform(List(
        Indicator[Position3D]().andThenRelocate(Locate.RenameDimension(First, "%1$s.ind")),
        Binarise[Position3D](Locate.RenameDimensionWithContent(First))))
      .toList.sortBy(_.position) shouldBe result3
  }

  "A Matrix.transformWithValue" should "return its transformed data in 1D" in {
    toRDD(num1)
      .transformWithValue(List(
        Normalise(extractor[Position1D](First, "max.abs"))
          .andThenRelocate(Locate.RenameDimension(First, "%1$s.n")),
        Standardise(extractor[Position1D](First, "mean"), extractor[Position1D](First, "sd"))
          .andThenRelocate(Locate.RenameDimension(First, "%1$s.s"))), ext)
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its transformed data in 2D" in {
    toRDD(num2)
      .transformWithValue(
        Normalise(extractor[Position2D](First, "max.abs"))
          .andThenRelocate(Locate.RenameDimension(First, "%1$s.n")), ext)
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its transformed data in 3D" in {
    toRDD(num3)
      .transformWithValue(
        Normalise(extractor[Position3D](First, "max.abs"))
          .andThenRelocate(Locate.RenameDimension(First, "%1$s.n")), ext)
      .toList.sortBy(_.position) shouldBe result6
  }

  "A Matrix.transformAndExpand" should "return its transformed data in 1D" in {
    toRDD(data1)
      .transform(Indicator[Position1D]()
        .andThenRelocate(Locate.RenameDimension(First, "%1$s.ind"))
        .andThenRelocate(c => c.position.append("ind").toOption))
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its transformed data in 2D" in {
    toRDD(data2)
      .transform(List(
        Indicator[Position2D]()
          .andThenRelocate(Locate.RenameDimension(First, "%1$s.ind"))
          .andThenRelocate(c => c.position.append("ind").toOption),
        Binarise[Position2D](Locate.RenameDimensionWithContent(First))
          .andThenRelocate(c => c.position.append("bin").toOption)))
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its transformed data in 3D" in {
    toRDD(data3)
      .transform(List(
        Indicator[Position3D]()
          .andThenRelocate(Locate.RenameDimension(First, "%1$s.ind"))
          .andThenRelocate(c => c.position.append("ind").toOption),
        Binarise[Position3D](Locate.RenameDimensionWithContent(First))
          .andThenRelocate(c => c.position.append("bin").toOption)))
      .toList.sortBy(_.position) shouldBe result9
  }

  "A Matrix.transformAndExpandWithValue" should "return its transformed data in 1D" in {
    toRDD(num1)
      .transformWithValue(List(
        Normalise(extractor[Position1D](First, "max.abs"))
          .andThenRelocate(Locate.RenameDimension(First, "%1$s.n"))
          .andThenRelocateWithValue((c, _) => c.position.append("nrm").toOption),
        Standardise(extractor[Position1D](First, "mean"), extractor[Position1D](First, "sd"))
          .andThenRelocate(Locate.RenameDimension(First, "%1$s.s"))
          .andThenRelocateWithValue((c, _) => c.position.append("std").toOption)), ext)
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its transformed data in 2D" in {
    toRDD(num2)
      .transformWithValue(
        Normalise(extractor[Position2D](First, "max.abs"))
          .andThenRelocate(Locate.RenameDimension(First, "%1$s.n"))
          .andThenRelocateWithValue((c, _) => c.position.append("nrm").toOption), ext)
      .toList.sortBy(_.position) shouldBe result11
  }

  it should "return its transformed data in 3D" in {
    toRDD(num3)
      .transformWithValue(
        Normalise(extractor[Position3D](First, "max.abs"))
          .andThenRelocate(Locate.RenameDimension(First, "%1$s.n"))
          .andThenRelocateWithValue((c, _) => c.position.append("nrm").toOption), ext)
      .toList.sortBy(_.position) shouldBe result12
  }
}

trait TestMatrixSlide extends TestMatrix {

  val ext = Map("one" -> 1, "two" -> 2)

  val result1 = List(Cell(Position1D("1*(bar-baz)"), Content(ContinuousSchema(DoubleCodex), 6.28 - 9.42)),
    Cell(Position1D("1*(baz-foo)"), Content(ContinuousSchema(DoubleCodex), 9.42 - 3.14)),
    Cell(Position1D("1*(foo-qux)"), Content(ContinuousSchema(DoubleCodex), 3.14 - 12.56)),
    Cell(Position1D("2*(bar-baz)"), Content(ContinuousSchema(DoubleCodex), 2 * (6.28 - 9.42))),
    Cell(Position1D("2*(baz-foo)"), Content(ContinuousSchema(DoubleCodex), 2 * (9.42 - 3.14))),
    Cell(Position1D("2*(foo-qux)"), Content(ContinuousSchema(DoubleCodex), 2 * (3.14 - 12.56))))

  val result2 = List(Cell(Position2D("bar", "1*(1-2)"), Content(ContinuousSchema(DoubleCodex), 6.28 - 12.56)),
    Cell(Position2D("bar", "1*(2-3)"), Content(ContinuousSchema(DoubleCodex), 12.56 - 18.84)),
    Cell(Position2D("baz", "1*(1-2)"), Content(ContinuousSchema(DoubleCodex), 9.42 - 18.84)),
    Cell(Position2D("foo", "1*(1-2)"), Content(ContinuousSchema(DoubleCodex), 3.14 - 6.28)),
    Cell(Position2D("foo", "1*(2-3)"), Content(ContinuousSchema(DoubleCodex), 6.28 - 9.42)),
    Cell(Position2D("foo", "1*(3-4)"), Content(ContinuousSchema(DoubleCodex), 9.42 - 12.56)))

  val result3 = List(Cell(Position2D(1, "1*(baz-bar)"), Content(ContinuousSchema(DoubleCodex), 9.42 - 6.28)),
    Cell(Position2D(1, "1*(foo-baz)"), Content(ContinuousSchema(DoubleCodex), 3.14 - 9.42)),
    Cell(Position2D(1, "1*(qux-foo)"), Content(ContinuousSchema(DoubleCodex), 12.56 - 3.14)),
    Cell(Position2D(2, "1*(baz-bar)"), Content(ContinuousSchema(DoubleCodex), 18.84 - 12.56)),
    Cell(Position2D(2, "1*(foo-baz)"), Content(ContinuousSchema(DoubleCodex), 6.28 - 18.84)),
    Cell(Position2D(3, "1*(foo-bar)"), Content(ContinuousSchema(DoubleCodex), 9.42 - 18.84)))

  val result4 = List(Cell(Position2D(1, "1*(baz-bar)"), Content(ContinuousSchema(DoubleCodex), 9.42 - 6.28)),
    Cell(Position2D(1, "1*(foo-baz)"), Content(ContinuousSchema(DoubleCodex), 3.14 - 9.42)),
    Cell(Position2D(1, "1*(qux-foo)"), Content(ContinuousSchema(DoubleCodex), 12.56 - 3.14)),
    Cell(Position2D(2, "1*(baz-bar)"), Content(ContinuousSchema(DoubleCodex), 18.84 - 12.56)),
    Cell(Position2D(2, "1*(foo-baz)"), Content(ContinuousSchema(DoubleCodex), 6.28 - 18.84)),
    Cell(Position2D(3, "1*(foo-bar)"), Content(ContinuousSchema(DoubleCodex), 9.42 - 18.84)))

  val result5 = List(Cell(Position2D("bar", "1*(1-2)"), Content(ContinuousSchema(DoubleCodex), 6.28 - 12.56)),
    Cell(Position2D("bar", "1*(2-3)"), Content(ContinuousSchema(DoubleCodex), 12.56 - 18.84)),
    Cell(Position2D("baz", "1*(1-2)"), Content(ContinuousSchema(DoubleCodex), 9.42 - 18.84)),
    Cell(Position2D("foo", "1*(1-2)"), Content(ContinuousSchema(DoubleCodex), 3.14 - 6.28)),
    Cell(Position2D("foo", "1*(2-3)"), Content(ContinuousSchema(DoubleCodex), 6.28 - 9.42)),
    Cell(Position2D("foo", "1*(3-4)"), Content(ContinuousSchema(DoubleCodex), 9.42 - 12.56)))

  val result6 = List(
    Cell(Position2D("bar", "1*(1|xyz-2|xyz)"), Content(ContinuousSchema(DoubleCodex), 6.28 - 12.56)),
    Cell(Position2D("bar", "1*(2|xyz-3|xyz)"), Content(ContinuousSchema(DoubleCodex), 12.56 - 18.84)),
    Cell(Position2D("baz", "1*(1|xyz-2|xyz)"), Content(ContinuousSchema(DoubleCodex), 9.42 - 18.84)),
    Cell(Position2D("foo", "1*(1|xyz-2|xyz)"), Content(ContinuousSchema(DoubleCodex), 3.14 - 6.28)),
    Cell(Position2D("foo", "1*(2|xyz-3|xyz)"), Content(ContinuousSchema(DoubleCodex), 6.28 - 9.42)),
    Cell(Position2D("foo", "1*(3|xyz-4|xyz)"), Content(ContinuousSchema(DoubleCodex), 9.42 - 12.56)))

  val result7 = List(
    Cell(Position3D(1, "xyz", "1*(baz-bar)"), Content(ContinuousSchema(DoubleCodex), 9.42 - 6.28)),
    Cell(Position3D(1, "xyz", "1*(foo-baz)"), Content(ContinuousSchema(DoubleCodex), 3.14 - 9.42)),
    Cell(Position3D(1, "xyz", "1*(qux-foo)"), Content(ContinuousSchema(DoubleCodex), 12.56 - 3.14)),
    Cell(Position3D(2, "xyz", "1*(baz-bar)"), Content(ContinuousSchema(DoubleCodex), 18.84 - 12.56)),
    Cell(Position3D(2, "xyz", "1*(foo-baz)"), Content(ContinuousSchema(DoubleCodex), 6.28 - 18.84)),
    Cell(Position3D(3, "xyz", "1*(foo-bar)"), Content(ContinuousSchema(DoubleCodex), 9.42 - 18.84)))

  val result8 = List(
    Cell(Position2D(1, "1*(baz|xyz-bar|xyz)"), Content(ContinuousSchema(DoubleCodex), 9.42 - 6.28)),
    Cell(Position2D(1, "1*(foo|xyz-baz|xyz)"), Content(ContinuousSchema(DoubleCodex), 3.14 - 9.42)),
    Cell(Position2D(1, "1*(qux|xyz-foo|xyz)"), Content(ContinuousSchema(DoubleCodex), 12.56 - 3.14)),
    Cell(Position2D(2, "1*(baz|xyz-bar|xyz)"), Content(ContinuousSchema(DoubleCodex), 18.84 - 12.56)),
    Cell(Position2D(2, "1*(foo|xyz-baz|xyz)"), Content(ContinuousSchema(DoubleCodex), 6.28 - 18.84)),
    Cell(Position2D(3, "1*(foo|xyz-bar|xyz)"), Content(ContinuousSchema(DoubleCodex), 9.42 - 18.84)))

  val result9 = List(
    Cell(Position3D("bar", "xyz", "1*(1-2)"), Content(ContinuousSchema(DoubleCodex), 6.28 - 12.56)),
    Cell(Position3D("bar", "xyz", "1*(2-3)"), Content(ContinuousSchema(DoubleCodex), 12.56 - 18.84)),
    Cell(Position3D("baz", "xyz", "1*(1-2)"), Content(ContinuousSchema(DoubleCodex), 9.42 - 18.84)),
    Cell(Position3D("foo", "xyz", "1*(1-2)"), Content(ContinuousSchema(DoubleCodex), 3.14 - 6.28)),
    Cell(Position3D("foo", "xyz", "1*(2-3)"), Content(ContinuousSchema(DoubleCodex), 6.28 - 9.42)),
    Cell(Position3D("foo", "xyz", "1*(3-4)"), Content(ContinuousSchema(DoubleCodex), 9.42 - 12.56)))

  val result10 = List(
    Cell(Position2D("xyz", "1*(bar|1-bar|2)"), Content(ContinuousSchema(DoubleCodex), 6.28 - 12.56)),
    Cell(Position2D("xyz", "1*(bar|2-bar|3)"), Content(ContinuousSchema(DoubleCodex), 12.56 - 18.84)),
    Cell(Position2D("xyz", "1*(bar|3-baz|1)"), Content(ContinuousSchema(DoubleCodex), 18.84 - 9.42)),
    Cell(Position2D("xyz", "1*(baz|1-baz|2)"), Content(ContinuousSchema(DoubleCodex), 9.42 - 18.84)),
    Cell(Position2D("xyz", "1*(baz|2-foo|1)"), Content(ContinuousSchema(DoubleCodex), 18.84 - 3.14)),
    Cell(Position2D("xyz", "1*(foo|1-foo|2)"), Content(ContinuousSchema(DoubleCodex), 3.14 - 6.28)),
    Cell(Position2D("xyz", "1*(foo|2-foo|3)"), Content(ContinuousSchema(DoubleCodex), 6.28 - 9.42)),
    Cell(Position2D("xyz", "1*(foo|3-foo|4)"), Content(ContinuousSchema(DoubleCodex), 9.42 - 12.56)),
    Cell(Position2D("xyz", "1*(foo|4-qux|1)"), Content(ContinuousSchema(DoubleCodex), 12.56 - 12.56)))

  val result11 = List()

  val result12 = List(Cell(Position1D("1*(baz-bar)"), Content(ContinuousSchema(DoubleCodex), 9.42 - 6.28)),
    Cell(Position1D("1*(foo-baz)"), Content(ContinuousSchema(DoubleCodex), 3.14 - 9.42)),
    Cell(Position1D("1*(qux-foo)"), Content(ContinuousSchema(DoubleCodex), 12.56 - 3.14)),
    Cell(Position1D("2*(baz-bar)"), Content(ContinuousSchema(DoubleCodex), 2 * (9.42 - 6.28))),
    Cell(Position1D("2*(foo-baz)"), Content(ContinuousSchema(DoubleCodex), 2 * (3.14 - 9.42))),
    Cell(Position1D("2*(qux-foo)"), Content(ContinuousSchema(DoubleCodex), 2 * (12.56 - 3.14))))

  val result13 = List(Cell(Position2D("bar", "1*(1-2)"), Content(ContinuousSchema(DoubleCodex), 6.28 - 12.56)),
    Cell(Position2D("bar", "1*(2-3)"), Content(ContinuousSchema(DoubleCodex), 12.56 - 18.84)),
    Cell(Position2D("baz", "1*(1-2)"), Content(ContinuousSchema(DoubleCodex), 9.42 - 18.84)),
    Cell(Position2D("foo", "1*(1-2)"), Content(ContinuousSchema(DoubleCodex), 3.14 - 6.28)),
    Cell(Position2D("foo", "1*(2-3)"), Content(ContinuousSchema(DoubleCodex), 6.28 - 9.42)),
    Cell(Position2D("foo", "1*(3-4)"), Content(ContinuousSchema(DoubleCodex), 9.42 - 12.56)))

  val result14 = List(Cell(Position2D(1, "1*(bar-baz)"), Content(ContinuousSchema(DoubleCodex), 6.28 - 9.42)),
    Cell(Position2D(1, "1*(baz-foo)"), Content(ContinuousSchema(DoubleCodex), 9.42 - 3.14)),
    Cell(Position2D(1, "1*(foo-qux)"), Content(ContinuousSchema(DoubleCodex), 3.14 - 12.56)),
    Cell(Position2D(2, "1*(bar-baz)"), Content(ContinuousSchema(DoubleCodex), 12.56 - 18.84)),
    Cell(Position2D(2, "1*(baz-foo)"), Content(ContinuousSchema(DoubleCodex), 18.84 - 6.28)),
    Cell(Position2D(3, "1*(bar-foo)"), Content(ContinuousSchema(DoubleCodex), 18.84 - 9.42)))

  val result15 = List(Cell(Position2D(1, "1*(baz-bar)"), Content(ContinuousSchema(DoubleCodex), 9.42 - 6.28)),
    Cell(Position2D(1, "1*(foo-baz)"), Content(ContinuousSchema(DoubleCodex), 3.14 - 9.42)),
    Cell(Position2D(1, "1*(qux-foo)"), Content(ContinuousSchema(DoubleCodex), 12.56 - 3.14)),
    Cell(Position2D(2, "1*(baz-bar)"), Content(ContinuousSchema(DoubleCodex), 18.84 - 12.56)),
    Cell(Position2D(2, "1*(foo-baz)"), Content(ContinuousSchema(DoubleCodex), 6.28 - 18.84)),
    Cell(Position2D(3, "1*(foo-bar)"), Content(ContinuousSchema(DoubleCodex), 9.42 - 18.84)))

  val result16 = List(Cell(Position2D("bar", "1*(2-1)"), Content(ContinuousSchema(DoubleCodex), 12.56 - 6.28)),
    Cell(Position2D("bar", "1*(3-2)"), Content(ContinuousSchema(DoubleCodex), 18.84 - 12.56)),
    Cell(Position2D("baz", "1*(2-1)"), Content(ContinuousSchema(DoubleCodex), 18.84 - 9.42)),
    Cell(Position2D("foo", "1*(2-1)"), Content(ContinuousSchema(DoubleCodex), 6.28 - 3.14)),
    Cell(Position2D("foo", "1*(3-2)"), Content(ContinuousSchema(DoubleCodex), 9.42 - 6.28)),
    Cell(Position2D("foo", "1*(4-3)"), Content(ContinuousSchema(DoubleCodex), 12.56 - 9.42)))

  val result17 = List(
    Cell(Position2D("bar", "1*(1|xyz-2|xyz)"), Content(ContinuousSchema(DoubleCodex), 6.28 - 12.56)),
    Cell(Position2D("bar", "1*(2|xyz-3|xyz)"), Content(ContinuousSchema(DoubleCodex), 12.56 - 18.84)),
    Cell(Position2D("baz", "1*(1|xyz-2|xyz)"), Content(ContinuousSchema(DoubleCodex), 9.42 - 18.84)),
    Cell(Position2D("foo", "1*(1|xyz-2|xyz)"), Content(ContinuousSchema(DoubleCodex), 3.14 - 6.28)),
    Cell(Position2D("foo", "1*(2|xyz-3|xyz)"), Content(ContinuousSchema(DoubleCodex), 6.28 - 9.42)),
    Cell(Position2D("foo", "1*(3|xyz-4|xyz)"), Content(ContinuousSchema(DoubleCodex), 9.42 - 12.56)))

  val result18 = List(
    Cell(Position3D(1, "xyz", "1*(bar-baz)"), Content(ContinuousSchema(DoubleCodex), 6.28 - 9.42)),
    Cell(Position3D(1, "xyz", "1*(baz-foo)"), Content(ContinuousSchema(DoubleCodex), 9.42 - 3.14)),
    Cell(Position3D(1, "xyz", "1*(foo-qux)"), Content(ContinuousSchema(DoubleCodex), 3.14 - 12.56)),
    Cell(Position3D(2, "xyz", "1*(bar-baz)"), Content(ContinuousSchema(DoubleCodex), 12.56 - 18.84)),
    Cell(Position3D(2, "xyz", "1*(baz-foo)"), Content(ContinuousSchema(DoubleCodex), 18.84 - 6.28)),
    Cell(Position3D(3, "xyz", "1*(bar-foo)"), Content(ContinuousSchema(DoubleCodex), 18.84 - 9.42)))

  val result19 = List(
    Cell(Position2D(1, "1*(baz|xyz-bar|xyz)"), Content(ContinuousSchema(DoubleCodex), 9.42 - 6.28)),
    Cell(Position2D(1, "1*(foo|xyz-baz|xyz)"), Content(ContinuousSchema(DoubleCodex), 3.14 - 9.42)),
    Cell(Position2D(1, "1*(qux|xyz-foo|xyz)"), Content(ContinuousSchema(DoubleCodex), 12.56 - 3.14)),
    Cell(Position2D(2, "1*(baz|xyz-bar|xyz)"), Content(ContinuousSchema(DoubleCodex), 18.84 - 12.56)),
    Cell(Position2D(2, "1*(foo|xyz-baz|xyz)"), Content(ContinuousSchema(DoubleCodex), 6.28 - 18.84)),
    Cell(Position2D(3, "1*(foo|xyz-bar|xyz)"), Content(ContinuousSchema(DoubleCodex), 9.42 - 18.84)))

  val result20 = List(
    Cell(Position3D("bar", "xyz", "1*(2-1)"), Content(ContinuousSchema(DoubleCodex), 12.56 - 6.28)),
    Cell(Position3D("bar", "xyz", "1*(3-2)"), Content(ContinuousSchema(DoubleCodex), 18.84 - 12.56)),
    Cell(Position3D("baz", "xyz", "1*(2-1)"), Content(ContinuousSchema(DoubleCodex), 18.84 - 9.42)),
    Cell(Position3D("foo", "xyz", "1*(2-1)"), Content(ContinuousSchema(DoubleCodex), 6.28 - 3.14)),
    Cell(Position3D("foo", "xyz", "1*(3-2)"), Content(ContinuousSchema(DoubleCodex), 9.42 - 6.28)),
    Cell(Position3D("foo", "xyz", "1*(4-3)"), Content(ContinuousSchema(DoubleCodex), 12.56 - 9.42)))

  val result21 = List(
    Cell(Position2D("xyz", "1*(bar|1-bar|2)"), Content(ContinuousSchema(DoubleCodex), 6.28 - 12.56)),
    Cell(Position2D("xyz", "1*(bar|2-bar|3)"), Content(ContinuousSchema(DoubleCodex), 12.56 - 18.84)),
    Cell(Position2D("xyz", "1*(bar|3-baz|1)"), Content(ContinuousSchema(DoubleCodex), 18.84 - 9.42)),
    Cell(Position2D("xyz", "1*(baz|1-baz|2)"), Content(ContinuousSchema(DoubleCodex), 9.42 - 18.84)),
    Cell(Position2D("xyz", "1*(baz|2-foo|1)"), Content(ContinuousSchema(DoubleCodex), 18.84 - 3.14)),
    Cell(Position2D("xyz", "1*(foo|1-foo|2)"), Content(ContinuousSchema(DoubleCodex), 3.14 - 6.28)),
    Cell(Position2D("xyz", "1*(foo|2-foo|3)"), Content(ContinuousSchema(DoubleCodex), 6.28 - 9.42)),
    Cell(Position2D("xyz", "1*(foo|3-foo|4)"), Content(ContinuousSchema(DoubleCodex), 9.42 - 12.56)),
    Cell(Position2D("xyz", "1*(foo|4-qux|1)"), Content(ContinuousSchema(DoubleCodex), 12.56 - 12.56)))

  val result22 = List()
}

object TestMatrixSlide {

  case class Delta[P <: Position, S <: Position with ExpandablePosition, R <: Position with ExpandablePosition](
    times: Int) extends Window[P, S, R, S#M] {
    type I = Option[Double]
    type T = (Option[Double], R)
    type O = (Double, R, R)

    def prepare(cell: Cell[P]): I = cell.content.value.asDouble

    def initialise(rem: R, in: I): (T, TraversableOnce[O]) = ((in, rem), None)

    def update(rem: R, in: I, t: T): (T, TraversableOnce[O]) = {
     ((in, rem), (in, t._1) match {
       case (Some(dc), Some(dt)) => Some((dc - dt, rem, t._2))
       case _ => None
     })
    }

    def present(pos: S, out: O): TraversableOnce[Cell[S#M]] = {
      Some(Cell[S#M](pos.append(times + "*(" + out._2.toShortString("|") + "-" + out._3.toShortString("|") + ")"),
        Content(ContinuousSchema(DoubleCodex), times * out._1)))
    }
  }

  case class DeltaWithValue[P <: Position, S <: Position with ExpandablePosition, R <: Position with ExpandablePosition](
    key: String) extends WindowWithValue[P, S, R, S#M] {
    type V = Map[String, Int]
    type I = Option[Double]
    type T = (Option[Double], R)
    type O = (Double, R, R)

    def prepareWithValue(cell: Cell[P], ext: V): I = cell.content.value.asDouble

    def initialise(rem: R, in: I): (T, TraversableOnce[O]) = ((in, rem), None)

    def update(rem: R, in: I, t: T): (T, TraversableOnce[O]) = {
     ((in, rem), (in, t._1) match {
       case (Some(dc), Some(dt)) => Some((dc - dt, rem, t._2))
       case _ => None
     })
    }

    def presentWithValue(pos: S, out: O, ext: V): TraversableOnce[Cell[S#M]] = {
      Some(Cell[S#M](pos.append(ext(key) + "*(" + out._2.toShortString("|") + "-" + out._3.toShortString("|") + ")"),
        Content(ContinuousSchema(DoubleCodex), ext(key) * out._1)))
    }
  }
}

class TestScaldingMatrixSlide extends TestMatrixSlide {

  "A Matrix.slide" should "return its first along derived data in 1D" in {
    toPipe(num1)
      .slide(Along(First), List(TestMatrixSlide.Delta[Position1D, Position0D, Position1D](1),
        TestMatrixSlide.Delta[Position1D, Position0D, Position1D](2)), false, Default())
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first over derived data in 2D" in {
    toPipe(num2)
      .slide(Over(First), TestMatrixSlide.Delta[Position2D, Position1D, Position1D](1), false,
        Default(Redistribute(123)))
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its first along derived data in 2D" in {
    toPipe(num2)
      .slide(Along(First), TestMatrixSlide.Delta[Position2D, Position1D, Position1D](1), true, Default(Reducers(123)))
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its second over derived data in 2D" in {
    toPipe(num2)
      .slide(Over(Second), TestMatrixSlide.Delta[Position2D, Position1D, Position1D](1), true,
        Default(Redistribute(123) |-> Reducers(123)))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its second along derived data in 2D" in {
    toPipe(num2)
      .slide(Along(Second), TestMatrixSlide.Delta[Position2D, Position1D, Position1D](1), false, Default())
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its first over derived data in 3D" in {
    toPipe(num3)
      .slide(Over(First), TestMatrixSlide.Delta[Position3D, Position1D, Position2D](1), false,
        Default(Redistribute(123)))
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its first along derived data in 3D" in {
    toPipe(num3)
      .slide(Along(First), TestMatrixSlide.Delta[Position3D, Position2D, Position1D](1), true,
        Default(Reducers(123)))
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its second over derived data in 3D" in {
    toPipe(num3)
      .slide(Over(Second), TestMatrixSlide.Delta[Position3D, Position1D, Position2D](1), true,
        Default(Redistribute(123) |-> Reducers(123)))
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its second along derived data in 3D" in {
    toPipe(num3)
      .slide(Along(Second), TestMatrixSlide.Delta[Position3D, Position2D, Position1D](1), false, Default())
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its third over derived data in 3D" in {
    toPipe(num3)
      .slide(Over(Third), TestMatrixSlide.Delta[Position3D, Position1D, Position2D](1), false,
        Default(Redistribute(123)))
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its third along derived data in 3D" in {
    toPipe(num3)
      .slide(Along(Third), TestMatrixSlide.Delta[Position3D, Position2D, Position1D](1), true,
        Default(Reducers(123)))
      .toList.sortBy(_.position) shouldBe result11
  }

  "A Matrix.slideWithValue" should "return its first along derived data in 1D" in {
    toPipe(num1)
      .slideWithValue(Along(First), List(TestMatrixSlide.DeltaWithValue[Position1D, Position0D, Position1D]("one"),
        TestMatrixSlide.DeltaWithValue[Position1D, Position0D, Position1D]("two")), ValuePipe(ext), true,
          Default(Redistribute(123)))
      .toList.sortBy(_.position) shouldBe result12
  }

  it should "return its first over derived data in 2D" in {
    toPipe(num2)
      .slideWithValue(Over(First), TestMatrixSlide.DeltaWithValue[Position2D, Position1D, Position1D]("one"),
        ValuePipe(ext), false, Default(Redistribute(123) |-> Reducers(123)))
      .toList.sortBy(_.position) shouldBe result13
  }

  it should "return its first along derived data in 2D" in {
    toPipe(num2)
      .slideWithValue(Along(First), TestMatrixSlide.DeltaWithValue[Position2D, Position1D, Position1D]("one"),
        ValuePipe(ext), false, Default())
      .toList.sortBy(_.position) shouldBe result14
  }

  it should "return its second over derived data in 2D" in {
    toPipe(num2)
      .slideWithValue(Over(Second), TestMatrixSlide.DeltaWithValue[Position2D, Position1D, Position1D]("one"),
        ValuePipe(ext), true, Default(Redistribute(123)))
      .toList.sortBy(_.position) shouldBe result15
  }

  it should "return its second along derived data in 2D" in {
    toPipe(num2)
      .slideWithValue(Along(Second), TestMatrixSlide.DeltaWithValue[Position2D, Position1D, Position1D]("one"),
        ValuePipe(ext), true, Default(Reducers(123)))
      .toList.sortBy(_.position) shouldBe result16
  }

  it should "return its first over derived data in 3D" in {
    toPipe(num3)
      .slideWithValue(Over(First), TestMatrixSlide.DeltaWithValue[Position3D, Position1D, Position2D]("one"),
        ValuePipe(ext), false, Default(Redistribute(123) |-> Reducers(123)))
      .toList.sortBy(_.position) shouldBe result17
  }

  it should "return its first along derived data in 3D" in {
    toPipe(num3)
      .slideWithValue(Along(First), TestMatrixSlide.DeltaWithValue[Position3D, Position2D, Position1D]("one"),
        ValuePipe(ext), false, Default())
      .toList.sortBy(_.position) shouldBe result18
  }

  it should "return its second over derived data in 3D" in {
    toPipe(num3)
      .slideWithValue(Over(Second), TestMatrixSlide.DeltaWithValue[Position3D, Position1D, Position2D]("one"),
        ValuePipe(ext), true, Default(Redistribute(123)))
      .toList.sortBy(_.position) shouldBe result19
  }

  it should "return its second along derived data in 3D" in {
    toPipe(num3)
      .slideWithValue(Along(Second), TestMatrixSlide.DeltaWithValue[Position3D, Position2D, Position1D]("one"),
        ValuePipe(ext), true, Default(Reducers(123)))
      .toList.sortBy(_.position) shouldBe result20
  }

  it should "return its third over derived data in 3D" in {
    toPipe(num3)
      .slideWithValue(Over(Third), TestMatrixSlide.DeltaWithValue[Position3D, Position1D, Position2D]("one"),
        ValuePipe(ext), false, Default(Redistribute(123) |-> Reducers(123)))
      .toList.sortBy(_.position) shouldBe result21
  }

  it should "return its third along derived data in 3D" in {
    toPipe(num3)
      .slideWithValue(Along(Third), TestMatrixSlide.DeltaWithValue[Position3D, Position2D, Position1D]("one"),
        ValuePipe(ext), false, Default())
      .toList.sortBy(_.position) shouldBe result22
  }
}

class TestSparkMatrixSlide extends TestMatrixSlide {

  "A Matrix.slide" should "return its first along derived data in 1D" in {
    toRDD(num1)
      .slide(Along(First), List(TestMatrixSlide.Delta[Position1D, Position0D, Position1D](1),
        TestMatrixSlide.Delta[Position1D, Position0D, Position1D](2)), false, Default())
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first over derived data in 2D" in {
    toRDD(num2)
      .slide(Over(First), TestMatrixSlide.Delta[Position2D, Position1D, Position1D](1), false, Default())
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its first along derived data in 2D" in {
    toRDD(num2)
      .slide(Along(First), TestMatrixSlide.Delta[Position2D, Position1D, Position1D](1), true, Default())
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its second over derived data in 2D" in {
    toRDD(num2)
      .slide(Over(Second), TestMatrixSlide.Delta[Position2D, Position1D, Position1D](1), true, Default())
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its second along derived data in 2D" in {
    toRDD(num2)
      .slide(Along(Second), TestMatrixSlide.Delta[Position2D, Position1D, Position1D](1), false, Default())
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its first over derived data in 3D" in {
    toRDD(num3)
      .slide(Over(First), TestMatrixSlide.Delta[Position3D, Position1D, Position2D](1), false, Default())
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its first along derived data in 3D" in {
    toRDD(num3)
      .slide(Along(First), TestMatrixSlide.Delta[Position3D, Position2D, Position1D](1), true, Default())
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its second over derived data in 3D" in {
    toRDD(num3)
      .slide(Over(Second), TestMatrixSlide.Delta[Position3D, Position1D, Position2D](1), true, Default())
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its second along derived data in 3D" in {
    toRDD(num3)
      .slide(Along(Second), TestMatrixSlide.Delta[Position3D, Position2D, Position1D](1), false, Default())
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its third over derived data in 3D" in {
    toRDD(num3)
      .slide(Over(Third), TestMatrixSlide.Delta[Position3D, Position1D, Position2D](1), false, Default())
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its third along derived data in 3D" in {
    toRDD(num3)
      .slide(Along(Third), TestMatrixSlide.Delta[Position3D, Position2D, Position1D](1), true, Default())
      .toList.sortBy(_.position) shouldBe result11
  }

  "A Matrix.slideWithValue" should "return its first along derived data in 1D" in {
    toRDD(num1)
      .slideWithValue(Along(First), List(TestMatrixSlide.DeltaWithValue[Position1D, Position0D, Position1D]("one"),
        TestMatrixSlide.DeltaWithValue[Position1D, Position0D, Position1D]("two")), ext, true, Default())
      .toList.sortBy(_.position) shouldBe result12
  }

  it should "return its first over derived data in 2D" in {
    toRDD(num2)
      .slideWithValue(Over(First), TestMatrixSlide.DeltaWithValue[Position2D, Position1D, Position1D]("one"),
        ext, false, Default())
      .toList.sortBy(_.position) shouldBe result13
  }

  it should "return its first along derived data in 2D" in {
    toRDD(num2)
      .slideWithValue(Along(First), TestMatrixSlide.DeltaWithValue[Position2D, Position1D, Position1D]("one"),
        ext, false, Default())
      .toList.sortBy(_.position) shouldBe result14
  }

  it should "return its second over derived data in 2D" in {
    toRDD(num2)
      .slideWithValue(Over(Second), TestMatrixSlide.DeltaWithValue[Position2D, Position1D, Position1D]("one"),
        ext, true, Default())
      .toList.sortBy(_.position) shouldBe result15
  }

  it should "return its second along derived data in 2D" in {
    toRDD(num2)
      .slideWithValue(Along(Second), TestMatrixSlide.DeltaWithValue[Position2D, Position1D, Position1D]("one"),
        ext, true, Default())
      .toList.sortBy(_.position) shouldBe result16
  }

  it should "return its first over derived data in 3D" in {
    toRDD(num3)
      .slideWithValue(Over(First), TestMatrixSlide.DeltaWithValue[Position3D, Position1D, Position2D]("one"),
        ext, false, Default())
      .toList.sortBy(_.position) shouldBe result17
  }

  it should "return its first along derived data in 3D" in {
    toRDD(num3)
      .slideWithValue(Along(First), TestMatrixSlide.DeltaWithValue[Position3D, Position2D, Position1D]("one"),
        ext, false, Default())
      .toList.sortBy(_.position) shouldBe result18
  }

  it should "return its second over derived data in 3D" in {
    toRDD(num3)
      .slideWithValue(Over(Second), TestMatrixSlide.DeltaWithValue[Position3D, Position1D, Position2D]("one"),
        ext, true, Default())
      .toList.sortBy(_.position) shouldBe result19
  }

  it should "return its second along derived data in 3D" in {
    toRDD(num3)
      .slideWithValue(Along(Second), TestMatrixSlide.DeltaWithValue[Position3D, Position2D, Position1D]("one"),
        ext, true, Default())
      .toList.sortBy(_.position) shouldBe result20
  }

  it should "return its third over derived data in 3D" in {
    toRDD(num3)
      .slideWithValue(Over(Third), TestMatrixSlide.DeltaWithValue[Position3D, Position1D, Position2D]("one"),
        ext, false, Default())
      .toList.sortBy(_.position) shouldBe result21
  }

  it should "return its third along derived data in 3D" in {
    toRDD(num3)
      .slideWithValue(Along(Third), TestMatrixSlide.DeltaWithValue[Position3D, Position2D, Position1D]("one"),
        ext, false, Default())
      .toList.sortBy(_.position) shouldBe result22
  }
}

trait TestMatrixFill extends TestMatrix {

  val result1 = List(Cell(Position2D("bar", 1), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position2D("bar", 2), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position2D("bar", 3), Content(ContinuousSchema(DoubleCodex), 18.84)),
    Cell(Position2D("bar", 4), Content(ContinuousSchema(DoubleCodex), 0)),
    Cell(Position2D("baz", 1), Content(ContinuousSchema(DoubleCodex), 9.42)),
    Cell(Position2D("baz", 2), Content(ContinuousSchema(DoubleCodex), 18.84)),
    Cell(Position2D("baz", 3), Content(ContinuousSchema(DoubleCodex), 0)),
    Cell(Position2D("baz", 4), Content(ContinuousSchema(DoubleCodex), 0)),
    Cell(Position2D("foo", 1), Content(ContinuousSchema(DoubleCodex), 3.14)),
    Cell(Position2D("foo", 2), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position2D("foo", 3), Content(ContinuousSchema(DoubleCodex), 9.42)),
    Cell(Position2D("foo", 4), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position2D("qux", 1), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position2D("qux", 2), Content(ContinuousSchema(DoubleCodex), 0)),
    Cell(Position2D("qux", 3), Content(ContinuousSchema(DoubleCodex), 0)),
    Cell(Position2D("qux", 4), Content(ContinuousSchema(DoubleCodex), 0)))

  val result2 = List(Cell(Position3D("bar", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("bar", 3, "xyz"), Content(ContinuousSchema(DoubleCodex), 18.84)),
    Cell(Position3D("bar", 4, "xyz"), Content(ContinuousSchema(DoubleCodex), 0)),
    Cell(Position3D("baz", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42)),
    Cell(Position3D("baz", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 18.84)),
    Cell(Position3D("baz", 3, "xyz"), Content(ContinuousSchema(DoubleCodex), 0)),
    Cell(Position3D("baz", 4, "xyz"), Content(ContinuousSchema(DoubleCodex), 0)),
    Cell(Position3D("foo", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 3.14)),
    Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position3D("foo", 3, "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42)),
    Cell(Position3D("foo", 4, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("qux", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("qux", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 0)),
    Cell(Position3D("qux", 3, "xyz"), Content(ContinuousSchema(DoubleCodex), 0)),
    Cell(Position3D("qux", 4, "xyz"), Content(ContinuousSchema(DoubleCodex), 0)))

  val result3 = List(Cell(Position2D("bar", 1), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position2D("bar", 2), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position2D("bar", 3), Content(ContinuousSchema(DoubleCodex), 18.84)),
    Cell(Position2D("bar", 4), Content(ContinuousSchema(DoubleCodex), (6.28 + 12.56 + 18.84) / 3)),
    Cell(Position2D("baz", 1), Content(ContinuousSchema(DoubleCodex), 9.42)),
    Cell(Position2D("baz", 2), Content(ContinuousSchema(DoubleCodex), 18.84)),
    Cell(Position2D("baz", 3), Content(ContinuousSchema(DoubleCodex), (9.42 + 18.84) / 2)),
    Cell(Position2D("baz", 4), Content(ContinuousSchema(DoubleCodex), (9.42 + 18.84) / 2)),
    Cell(Position2D("foo", 1), Content(ContinuousSchema(DoubleCodex), 3.14)),
    Cell(Position2D("foo", 2), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position2D("foo", 3), Content(ContinuousSchema(DoubleCodex), 9.42)),
    Cell(Position2D("foo", 4), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position2D("qux", 1), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position2D("qux", 2), Content(ContinuousSchema(DoubleCodex), (12.56) / 1)),
    Cell(Position2D("qux", 3), Content(ContinuousSchema(DoubleCodex), (12.56) / 1)),
    Cell(Position2D("qux", 4), Content(ContinuousSchema(DoubleCodex), (12.56) / 1)))

  val result4 = List(Cell(Position2D("bar", 1), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position2D("bar", 2), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position2D("bar", 3), Content(ContinuousSchema(DoubleCodex), 18.84)),
    Cell(Position2D("bar", 4), Content(ContinuousSchema(DoubleCodex), (12.56) / 1)),
    Cell(Position2D("baz", 1), Content(ContinuousSchema(DoubleCodex), 9.42)),
    Cell(Position2D("baz", 2), Content(ContinuousSchema(DoubleCodex), 18.84)),
    Cell(Position2D("baz", 3), Content(ContinuousSchema(DoubleCodex), (9.42 + 18.84) / 2)),
    Cell(Position2D("baz", 4), Content(ContinuousSchema(DoubleCodex), (12.56) / 1)),
    Cell(Position2D("foo", 1), Content(ContinuousSchema(DoubleCodex), 3.14)),
    Cell(Position2D("foo", 2), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position2D("foo", 3), Content(ContinuousSchema(DoubleCodex), 9.42)),
    Cell(Position2D("foo", 4), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position2D("qux", 1), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position2D("qux", 2), Content(ContinuousSchema(DoubleCodex), (6.28 + 12.56 + 18.84) / 3)),
    Cell(Position2D("qux", 3), Content(ContinuousSchema(DoubleCodex), (9.42 + 18.84) / 2)),
    Cell(Position2D("qux", 4), Content(ContinuousSchema(DoubleCodex), (12.56) / 1)))

  val result5 = List(Cell(Position2D("bar", 1), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position2D("bar", 2), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position2D("bar", 3), Content(ContinuousSchema(DoubleCodex), 18.84)),
    Cell(Position2D("bar", 4), Content(ContinuousSchema(DoubleCodex), (12.56) / 1)),
    Cell(Position2D("baz", 1), Content(ContinuousSchema(DoubleCodex), 9.42)),
    Cell(Position2D("baz", 2), Content(ContinuousSchema(DoubleCodex), 18.84)),
    Cell(Position2D("baz", 3), Content(ContinuousSchema(DoubleCodex), (9.42 + 18.84) / 2)),
    Cell(Position2D("baz", 4), Content(ContinuousSchema(DoubleCodex), (12.56) / 1)),
    Cell(Position2D("foo", 1), Content(ContinuousSchema(DoubleCodex), 3.14)),
    Cell(Position2D("foo", 2), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position2D("foo", 3), Content(ContinuousSchema(DoubleCodex), 9.42)),
    Cell(Position2D("foo", 4), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position2D("qux", 1), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position2D("qux", 2), Content(ContinuousSchema(DoubleCodex), (6.28 + 12.56 + 18.84) / 3)),
    Cell(Position2D("qux", 3), Content(ContinuousSchema(DoubleCodex), (9.42 + 18.84) / 2)),
    Cell(Position2D("qux", 4), Content(ContinuousSchema(DoubleCodex), (12.56) / 1)))

  val result6 = List(Cell(Position2D("bar", 1), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position2D("bar", 2), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position2D("bar", 3), Content(ContinuousSchema(DoubleCodex), 18.84)),
    Cell(Position2D("bar", 4), Content(ContinuousSchema(DoubleCodex), (6.28 + 12.56 + 18.84) / 3)),
    Cell(Position2D("baz", 1), Content(ContinuousSchema(DoubleCodex), 9.42)),
    Cell(Position2D("baz", 2), Content(ContinuousSchema(DoubleCodex), 18.84)),
    Cell(Position2D("baz", 3), Content(ContinuousSchema(DoubleCodex), (9.42 + 18.84) / 2)),
    Cell(Position2D("baz", 4), Content(ContinuousSchema(DoubleCodex), (9.42 + 18.84) / 2)),
    Cell(Position2D("foo", 1), Content(ContinuousSchema(DoubleCodex), 3.14)),
    Cell(Position2D("foo", 2), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position2D("foo", 3), Content(ContinuousSchema(DoubleCodex), 9.42)),
    Cell(Position2D("foo", 4), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position2D("qux", 1), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position2D("qux", 2), Content(ContinuousSchema(DoubleCodex), (12.56) / 1)),
    Cell(Position2D("qux", 3), Content(ContinuousSchema(DoubleCodex), (12.56) / 1)),
    Cell(Position2D("qux", 4), Content(ContinuousSchema(DoubleCodex), (12.56) / 1)))

  val result7 = List(Cell(Position3D("bar", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("bar", 3, "xyz"), Content(ContinuousSchema(DoubleCodex), 18.84)),
    Cell(Position3D("bar", 4, "xyz"), Content(ContinuousSchema(DoubleCodex), (6.28 + 12.56 + 18.84) / 3)),
    Cell(Position3D("baz", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42)),
    Cell(Position3D("baz", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 18.84)),
    Cell(Position3D("baz", 3, "xyz"), Content(ContinuousSchema(DoubleCodex), (9.42 + 18.84) / 2)),
    Cell(Position3D("baz", 4, "xyz"), Content(ContinuousSchema(DoubleCodex), (9.42 + 18.84) / 2)),
    Cell(Position3D("foo", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 3.14)),
    Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position3D("foo", 3, "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42)),
    Cell(Position3D("foo", 4, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("qux", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("qux", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), (12.56) / 1)),
    Cell(Position3D("qux", 3, "xyz"), Content(ContinuousSchema(DoubleCodex), (12.56) / 1)),
    Cell(Position3D("qux", 4, "xyz"), Content(ContinuousSchema(DoubleCodex), (12.56) / 1)))

  val result8 = List(Cell(Position3D("bar", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("bar", 3, "xyz"), Content(ContinuousSchema(DoubleCodex), 18.84)),
    Cell(Position3D("bar", 4, "xyz"), Content(ContinuousSchema(DoubleCodex), (12.56) / 1)),
    Cell(Position3D("baz", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42)),
    Cell(Position3D("baz", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 18.84)),
    Cell(Position3D("baz", 3, "xyz"), Content(ContinuousSchema(DoubleCodex), (9.42 + 18.84) / 2)),
    Cell(Position3D("baz", 4, "xyz"), Content(ContinuousSchema(DoubleCodex), (12.56) / 1)),
    Cell(Position3D("foo", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 3.14)),
    Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position3D("foo", 3, "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42)),
    Cell(Position3D("foo", 4, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("qux", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("qux", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), (6.28 + 12.56 + 18.84) / 3)),
    Cell(Position3D("qux", 3, "xyz"), Content(ContinuousSchema(DoubleCodex), (9.42 + 18.84) / 2)),
    Cell(Position3D("qux", 4, "xyz"), Content(ContinuousSchema(DoubleCodex), (12.56) / 1)))

  val result9 = List(Cell(Position3D("bar", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("bar", 3, "xyz"), Content(ContinuousSchema(DoubleCodex), 18.84)),
    Cell(Position3D("bar", 4, "xyz"), Content(ContinuousSchema(DoubleCodex), (12.56) / 1)),
    Cell(Position3D("baz", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42)),
    Cell(Position3D("baz", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 18.84)),
    Cell(Position3D("baz", 3, "xyz"), Content(ContinuousSchema(DoubleCodex), (9.42 + 18.84) / 2)),
    Cell(Position3D("baz", 4, "xyz"), Content(ContinuousSchema(DoubleCodex), (12.56) / 1)),
    Cell(Position3D("foo", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 3.14)),
    Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position3D("foo", 3, "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42)),
    Cell(Position3D("foo", 4, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("qux", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("qux", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), (6.28 + 12.56 + 18.84) / 3)),
    Cell(Position3D("qux", 3, "xyz"), Content(ContinuousSchema(DoubleCodex), (9.42 + 18.84) / 2)),
    Cell(Position3D("qux", 4, "xyz"), Content(ContinuousSchema(DoubleCodex), (12.56) / 1)))

  val result10 = List(Cell(Position3D("bar", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("bar", 3, "xyz"), Content(ContinuousSchema(DoubleCodex), 18.84)),
    Cell(Position3D("bar", 4, "xyz"), Content(ContinuousSchema(DoubleCodex), (6.28 + 12.56 + 18.84) / 3)),
    Cell(Position3D("baz", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42)),
    Cell(Position3D("baz", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 18.84)),
    Cell(Position3D("baz", 3, "xyz"), Content(ContinuousSchema(DoubleCodex), (9.42 + 18.84) / 2)),
    Cell(Position3D("baz", 4, "xyz"), Content(ContinuousSchema(DoubleCodex), (9.42 + 18.84) / 2)),
    Cell(Position3D("foo", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 3.14)),
    Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position3D("foo", 3, "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42)),
    Cell(Position3D("foo", 4, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("qux", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("qux", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), (12.56) / 1)),
    Cell(Position3D("qux", 3, "xyz"), Content(ContinuousSchema(DoubleCodex), (12.56) / 1)),
    Cell(Position3D("qux", 4, "xyz"), Content(ContinuousSchema(DoubleCodex), (12.56) / 1)))

  val result11 = List(Cell(Position3D("bar", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("bar", 3, "xyz"), Content(ContinuousSchema(DoubleCodex), 18.84)),
    Cell(Position3D("bar", 4, "xyz"), Content(ContinuousSchema(DoubleCodex),
      (3.14 + 2 * 6.28 + 2 * 9.42 + 3 * 12.56 + 2 * 18.84) / 10)),
    Cell(Position3D("baz", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42)),
    Cell(Position3D("baz", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 18.84)),
    Cell(Position3D("baz", 3, "xyz"), Content(ContinuousSchema(DoubleCodex),
      (3.14 + 2 * 6.28 + 2 * 9.42 + 3 * 12.56 + 2 * 18.84) / 10)),
    Cell(Position3D("baz", 4, "xyz"), Content(ContinuousSchema(DoubleCodex),
      (3.14 + 2 * 6.28 + 2 * 9.42 + 3 * 12.56 + 2 * 18.84) / 10)),
    Cell(Position3D("foo", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 3.14)),
    Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position3D("foo", 3, "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42)),
    Cell(Position3D("foo", 4, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("qux", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("qux", 2, "xyz"), Content(ContinuousSchema(DoubleCodex),
      (3.14 + 2 * 6.28 + 2 * 9.42 + 3 * 12.56 + 2 * 18.84) / 10)),
    Cell(Position3D("qux", 3, "xyz"), Content(ContinuousSchema(DoubleCodex),
      (3.14 + 2 * 6.28 + 2 * 9.42 + 3 * 12.56 + 2 * 18.84) / 10)),
    Cell(Position3D("qux", 4, "xyz"), Content(ContinuousSchema(DoubleCodex),
      (3.14 + 2 * 6.28 + 2 * 9.42 + 3 * 12.56 + 2 * 18.84) / 10)))

  val result12 = List(Cell(Position3D("bar", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position3D("bar", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("bar", 3, "xyz"), Content(ContinuousSchema(DoubleCodex), 18.84)),
    Cell(Position3D("baz", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42)),
    Cell(Position3D("baz", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 18.84)),
    Cell(Position3D("foo", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 3.14)),
    Cell(Position3D("foo", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position3D("foo", 3, "xyz"), Content(ContinuousSchema(DoubleCodex), 9.42)),
    Cell(Position3D("foo", 4, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("qux", 1, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)))
}

class TestScaldingMatrixFill extends TestMatrixFill {

  "A Matrix.fill" should "return its filled data in 2D" in {
    toPipe(num2)
      .fillHomogeneous(Content(ContinuousSchema(DoubleCodex), 0), Default())
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its filled data in 3D" in {
    toPipe(num3)
      .fillHomogeneous(Content(ContinuousSchema(DoubleCodex), 0), Default(Reducers(123)))
      .toList.sortBy(_.position) shouldBe result2
  }

  "A Matrix.fill" should "return its first over filled data in 2D" in {
    val cells = toPipe(num2)

    cells
      .fillHeterogeneous(Over(First), cells.summarise(Over(First), Mean[Position2D, Position1D]()), InMemory())
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its first along filled data in 2D" in {
    val cells = toPipe(num2)

    cells
      .fillHeterogeneous(Along(First), cells.summarise(Along(First), Mean[Position2D, Position1D]()),
        InMemory(Reducers(123)))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its second over filled data in 2D" in {
    val cells = toPipe(num2)

    cells
      .fillHeterogeneous(Over(Second), cells.summarise(Over(Second), Mean[Position2D, Position1D]()), Default())
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its second along filled data in 2D" in {
    val cells = toPipe(num2)

    cells
      .fillHeterogeneous(Along(Second), cells.summarise(Along(Second), Mean[Position2D, Position1D]()),
        Default(Reducers(123)))
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its first over filled data in 3D" in {
    val cells = toPipe(num3)

    cells
      .fillHeterogeneous(Over(First), cells.summarise(Over(First), Mean[Position3D, Position1D]()), InMemory())
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its first along filled data in 3D" in {
    val cells = toPipe(num3)

    cells
      .fillHeterogeneous(Along(First), cells.summarise(Along(First), Mean[Position3D, Position2D]()),
        InMemory(Reducers(123)))
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its second over filled data in 3D" in {
    val cells = toPipe(num3)

    cells
      .fillHeterogeneous(Over(Second), cells.summarise(Over(Second), Mean[Position3D, Position1D]()), Default())
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its second along filled data in 3D" in {
    val cells = toPipe(num3)

    cells
      .fillHeterogeneous(Along(Second), cells.summarise(Along(Second), Mean[Position3D, Position2D]()),
        Default(Reducers(123)))
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its third over filled data in 3D" in {
    val cells = toPipe(num3)

    cells
      .fillHeterogeneous(Over(Third), cells.summarise(Over(Third), Mean[Position3D, Position1D]()), InMemory())
      .toList.sortBy(_.position) shouldBe result11
  }

  it should "return its third along filled data in 3D" in {
    val cells = toPipe(num3)

    cells
      .fillHeterogeneous(Along(Third), cells.summarise(Along(Third), Mean[Position3D, Position2D]()),
        InMemory(Reducers(123)))
      .toList.sortBy(_.position) shouldBe result12
  }

  it should "return empty data - InMemory" in {
    toPipe(num3)
      .fillHeterogeneous(Along(Third), TypedPipe.empty, InMemory())
      .toList.sortBy(_.position) shouldBe List()
  }

  it should "return empty data - Default" in {
    toPipe(num3)
      .fillHeterogeneous(Along(Third), TypedPipe.empty, Default())
      .toList.sortBy(_.position) shouldBe List()
  }
}

class TestSparkMatrixFill extends TestMatrixFill {

  "A Matrix.fill" should "return its filled data in 2D" in {
    toRDD(num2)
      .fillHomogeneous(Content(ContinuousSchema(DoubleCodex), 0), Default())
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its filled data in 3D" in {
    toRDD(num3)
      .fillHomogeneous(Content(ContinuousSchema(DoubleCodex), 0), Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result2
  }

  "A Matrix.fill" should "return its first over filled data in 2D" in {
    val cells = toRDD(num2)

    cells
      .fillHeterogeneous(Over(First), cells.summarise(Over(First), Mean[Position2D, Position1D]()), Default())
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its first along filled data in 2D" in {
    val cells = toRDD(num2)

    cells
      .fillHeterogeneous(Along(First), cells.summarise(Along(First), Mean[Position2D, Position1D]()),
        Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its second over filled data in 2D" in {
    val cells = toRDD(num2)

    cells
      .fillHeterogeneous(Over(Second), cells.summarise(Over(Second), Mean[Position2D, Position1D]()),
        Default(Reducers(12), Reducers(23)))
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its second along filled data in 2D" in {
    val cells = toRDD(num2)

    cells
      .fillHeterogeneous(Along(Second), cells.summarise(Along(Second), Mean[Position2D, Position1D]()), Default())
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its first over filled data in 3D" in {
    val cells = toRDD(num3)

    cells
      .fillHeterogeneous(Over(First), cells.summarise(Over(First), Mean[Position3D, Position1D]()),
        Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its first along filled data in 3D" in {
    val cells = toRDD(num3)

    cells
      .fillHeterogeneous(Along(First), cells.summarise(Along(First), Mean[Position3D, Position2D]()),
        Default(Reducers(12), Reducers(23)))
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its second over filled data in 3D" in {
    val cells = toRDD(num3)

    cells
      .fillHeterogeneous(Over(Second), cells.summarise(Over(Second), Mean[Position3D, Position1D]()), Default())
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its second along filled data in 3D" in {
    val cells = toRDD(num3)

    cells
      .fillHeterogeneous(Along(Second), cells.summarise(Along(Second), Mean[Position3D, Position2D]()),
        Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its third over filled data in 3D" in {
    val cells = toRDD(num3)

    cells
      .fillHeterogeneous(Over(Third), cells.summarise(Over(Third), Mean[Position3D, Position1D]()),
        Default(Reducers(12), Reducers(23)))
      .toList.sortBy(_.position) shouldBe result11
  }

  it should "return its third along filled data in 3D" in {
    val cells = toRDD(num3)

    cells
      .fillHeterogeneous(Along(Third), cells.summarise(Along(Third), Mean[Position3D, Position2D]()), Default())
      .toList.sortBy(_.position) shouldBe result12
  }

  it should "return empty data - Default" in {
    toRDD(num3)
      .fillHeterogeneous(Along(Third), toRDD(List.empty[Cell[Position2D]]), Default())
      .toList.sortBy(_.position) shouldBe List()
  }
}

trait TestMatrixRename extends TestMatrix {

  val ext = ".new"

  val result1 = List(Cell(Position1D("bar.new"), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position1D("baz.new"), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position1D("foo.new"), Content(OrdinalSchema(StringCodex), "3.14")),
    Cell(Position1D("qux.new"), Content(OrdinalSchema(StringCodex), "12.56")))

  val result2 = List(Cell(Position2D("bar.new", 1), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position2D("bar.new", 2), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position2D("bar.new", 3), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position2D("baz.new", 1), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position2D("baz.new", 2), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position2D("foo.new", 1), Content(OrdinalSchema(StringCodex), "3.14")),
    Cell(Position2D("foo.new", 2), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position2D("foo.new", 3), Content(NominalSchema(StringCodex), "9.42")),
    Cell(Position2D("foo.new", 4), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
    Cell(Position2D("qux.new", 1), Content(OrdinalSchema(StringCodex), "12.56")))

  val result3 = List(Cell(Position2D("bar", "1.new"), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position2D("bar", "2.new"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position2D("bar", "3.new"), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position2D("baz", "1.new"), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position2D("baz", "2.new"), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position2D("foo", "1.new"), Content(OrdinalSchema(StringCodex), "3.14")),
    Cell(Position2D("foo", "2.new"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position2D("foo", "3.new"), Content(NominalSchema(StringCodex), "9.42")),
    Cell(Position2D("foo", "4.new"), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
    Cell(Position2D("qux", "1.new"), Content(OrdinalSchema(StringCodex), "12.56")))

  val result4 = List(Cell(Position3D("bar.new", 1, "xyz"), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position3D("bar.new", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("bar.new", 3, "xyz"), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position3D("baz.new", 1, "xyz"), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position3D("baz.new", 2, "xyz"), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position3D("foo.new", 1, "xyz"), Content(OrdinalSchema(StringCodex), "3.14")),
    Cell(Position3D("foo.new", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position3D("foo.new", 3, "xyz"), Content(NominalSchema(StringCodex), "9.42")),
    Cell(Position3D("foo.new", 4, "xyz"), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
    Cell(Position3D("qux.new", 1, "xyz"), Content(OrdinalSchema(StringCodex), "12.56")))

  val result5 = List(Cell(Position3D("bar", "1.new", "xyz"), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position3D("bar", "2.new", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("bar", "3.new", "xyz"), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position3D("baz", "1.new", "xyz"), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position3D("baz", "2.new", "xyz"), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position3D("foo", "1.new", "xyz"), Content(OrdinalSchema(StringCodex), "3.14")),
    Cell(Position3D("foo", "2.new", "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position3D("foo", "3.new", "xyz"), Content(NominalSchema(StringCodex), "9.42")),
    Cell(Position3D("foo", "4.new", "xyz"), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
    Cell(Position3D("qux", "1.new", "xyz"), Content(OrdinalSchema(StringCodex), "12.56")))

  val result6 = List(Cell(Position3D("bar", 1, "xyz.new"), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position3D("bar", 2, "xyz.new"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("bar", 3, "xyz.new"), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position3D("baz", 1, "xyz.new"), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position3D("baz", 2, "xyz.new"), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position3D("foo", 1, "xyz.new"), Content(OrdinalSchema(StringCodex), "3.14")),
    Cell(Position3D("foo", 2, "xyz.new"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position3D("foo", 3, "xyz.new"), Content(NominalSchema(StringCodex), "9.42")),
    Cell(Position3D("foo", 4, "xyz.new"), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
    Cell(Position3D("qux", 1, "xyz.new"), Content(OrdinalSchema(StringCodex), "12.56")))

  val result7 = List(Cell(Position1D("bar.new"), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position1D("baz.new"), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position1D("foo.new"), Content(OrdinalSchema(StringCodex), "3.14")),
    Cell(Position1D("qux.new"), Content(OrdinalSchema(StringCodex), "12.56")))

  val result8 = List(Cell(Position2D("bar.new", 1), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position2D("bar.new", 2), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position2D("bar.new", 3), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position2D("baz.new", 1), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position2D("baz.new", 2), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position2D("foo.new", 1), Content(OrdinalSchema(StringCodex), "3.14")),
    Cell(Position2D("foo.new", 2), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position2D("foo.new", 3), Content(NominalSchema(StringCodex), "9.42")),
    Cell(Position2D("foo.new", 4), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
    Cell(Position2D("qux.new", 1), Content(OrdinalSchema(StringCodex), "12.56")))

  val result9 = List(Cell(Position2D("bar", "1.new"), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position2D("bar", "2.new"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position2D("bar", "3.new"), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position2D("baz", "1.new"), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position2D("baz", "2.new"), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position2D("foo", "1.new"), Content(OrdinalSchema(StringCodex), "3.14")),
    Cell(Position2D("foo", "2.new"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position2D("foo", "3.new"), Content(NominalSchema(StringCodex), "9.42")),
    Cell(Position2D("foo", "4.new"), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
    Cell(Position2D("qux", "1.new"), Content(OrdinalSchema(StringCodex), "12.56")))

  val result10 = List(Cell(Position3D("bar.new", 1, "xyz"), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position3D("bar.new", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("bar.new", 3, "xyz"), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position3D("baz.new", 1, "xyz"), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position3D("baz.new", 2, "xyz"), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position3D("foo.new", 1, "xyz"), Content(OrdinalSchema(StringCodex), "3.14")),
    Cell(Position3D("foo.new", 2, "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position3D("foo.new", 3, "xyz"), Content(NominalSchema(StringCodex), "9.42")),
    Cell(Position3D("foo.new", 4, "xyz"), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
    Cell(Position3D("qux.new", 1, "xyz"), Content(OrdinalSchema(StringCodex), "12.56")))

  val result11 = List(Cell(Position3D("bar", "1.new", "xyz"), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position3D("bar", "2.new", "xyz"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("bar", "3.new", "xyz"), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position3D("baz", "1.new", "xyz"), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position3D("baz", "2.new", "xyz"), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position3D("foo", "1.new", "xyz"), Content(OrdinalSchema(StringCodex), "3.14")),
    Cell(Position3D("foo", "2.new", "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position3D("foo", "3.new", "xyz"), Content(NominalSchema(StringCodex), "9.42")),
    Cell(Position3D("foo", "4.new", "xyz"), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
    Cell(Position3D("qux", "1.new", "xyz"), Content(OrdinalSchema(StringCodex), "12.56")))

  val result12 = List(Cell(Position3D("bar", 1, "xyz.new"), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position3D("bar", 2, "xyz.new"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("bar", 3, "xyz.new"), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position3D("baz", 1, "xyz.new"), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position3D("baz", 2, "xyz.new"), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position3D("foo", 1, "xyz.new"), Content(OrdinalSchema(StringCodex), "3.14")),
    Cell(Position3D("foo", 2, "xyz.new"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position3D("foo", 3, "xyz.new"), Content(NominalSchema(StringCodex), "9.42")),
    Cell(Position3D("foo", 4, "xyz.new"), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
    Cell(Position3D("qux", 1, "xyz.new"), Content(OrdinalSchema(StringCodex), "12.56")))
}

object TestMatrixRename {

  def renamer[P <: Position](dim: Dimension)(cell: Cell[P]): Option[P] = {
    Some(cell.position.update(dim, cell.position(dim).toShortString + ".new"))
  }

  def renamerWithValue[P <: Position](dim: Dimension)(cell: Cell[P], ext: String): Option[P] = {
    Some(cell.position.update(dim, cell.position(dim).toShortString + ext))
  }
}

class TestScaldingMatrixRename extends TestMatrixRename {

  "A Matrix.relocate" should "return its first renamed data in 1D" in {
    toPipe(data1)
      .relocate(TestMatrixRename.renamer(First))
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first renamed data in 2D" in {
    toPipe(data2)
      .relocate(TestMatrixRename.renamer(First))
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its second renamed data in 2D" in {
    toPipe(data2)
      .relocate(TestMatrixRename.renamer(Second))
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its first renamed data in 3D" in {
    toPipe(data3)
      .relocate(TestMatrixRename.renamer(First))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its second renamed data in 3D" in {
    toPipe(data3)
      .relocate(TestMatrixRename.renamer(Second))
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its third renamed data in 3D" in {
    toPipe(data3)
      .relocate(TestMatrixRename.renamer(Third))
      .toList.sortBy(_.position) shouldBe result6
  }

  "A Matrix.renameWithValue" should "return its first renamed data in 1D" in {
    toPipe(data1)
      .relocateWithValue(TestMatrixRename.renamerWithValue(First), ValuePipe(ext))
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its first renamed data in 2D" in {
    toPipe(data2)
      .relocateWithValue(TestMatrixRename.renamerWithValue(First), ValuePipe(ext))
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its second renamed data in 2D" in {
    toPipe(data2)
      .relocateWithValue(TestMatrixRename.renamerWithValue(Second), ValuePipe(ext))
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its first renamed data in 3D" in {
    toPipe(data3)
      .relocateWithValue(TestMatrixRename.renamerWithValue(First), ValuePipe(ext))
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its second renamed data in 3D" in {
    toPipe(data3)
      .relocateWithValue(TestMatrixRename.renamerWithValue(Second), ValuePipe(ext))
      .toList.sortBy(_.position) shouldBe result11
  }

  it should "return its third renamed data in 3D" in {
    toPipe(data3)
      .relocateWithValue(TestMatrixRename.renamerWithValue(Third), ValuePipe(ext))
      .toList.sortBy(_.position) shouldBe result12
  }
}

class TestSparkMatrixRename extends TestMatrixRename {

  "A Matrix.rename" should "return its first renamed data in 1D" in {
    toRDD(data1)
      .relocate(TestMatrixRename.renamer(First))
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first renamed data in 2D" in {
    toRDD(data2)
      .relocate(TestMatrixRename.renamer(First))
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its second renamed data in 2D" in {
    toRDD(data2)
      .relocate(TestMatrixRename.renamer(Second))
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its first renamed data in 3D" in {
    toRDD(data3)
      .relocate(TestMatrixRename.renamer(First))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its second renamed data in 3D" in {
    toRDD(data3)
      .relocate(TestMatrixRename.renamer(Second))
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its third renamed data in 3D" in {
    toRDD(data3)
      .relocate(TestMatrixRename.renamer(Third))
      .toList.sortBy(_.position) shouldBe result6
  }

  "A Matrix.renameWithValue" should "return its first renamed data in 1D" in {
    toRDD(data1)
      .relocateWithValue(TestMatrixRename.renamerWithValue(First), ext)
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its first renamed data in 2D" in {
    toRDD(data2)
      .relocateWithValue(TestMatrixRename.renamerWithValue(First), ext)
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its second renamed data in 2D" in {
    toRDD(data2)
      .relocateWithValue(TestMatrixRename.renamerWithValue(Second), ext)
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its first renamed data in 3D" in {
    toRDD(data3)
      .relocateWithValue(TestMatrixRename.renamerWithValue(First), ext)
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its second renamed data in 3D" in {
    toRDD(data3)
      .relocateWithValue(TestMatrixRename.renamerWithValue(Second), ext)
      .toList.sortBy(_.position) shouldBe result11
  }

  it should "return its third renamed data in 3D" in {
    toRDD(data3)
      .relocateWithValue(TestMatrixRename.renamerWithValue(Third), ext)
      .toList.sortBy(_.position) shouldBe result12
  }
}

trait TestMatrixSquash extends TestMatrix {

  val ext = "ext"

  val result1 = List(Cell(Position1D(1), Content(OrdinalSchema(StringCodex), "12.56")),
    Cell(Position1D(2), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position1D(3), Content(NominalSchema(StringCodex), "9.42")),
    Cell(Position1D(4), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))

  val result2 = List(Cell(Position1D("bar"), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position1D("baz"), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position1D("foo"), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
    Cell(Position1D("qux"), Content(OrdinalSchema(StringCodex), "12.56")))

  val result3 = List(Cell(Position2D(1, "xyz"), Content(OrdinalSchema(StringCodex), "12.56")),
    Cell(Position2D(2, "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position2D(3, "xyz"), Content(NominalSchema(StringCodex), "9.42")),
    Cell(Position2D(4, "xyz"), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))

  val result4 = List(Cell(Position2D("bar", "xyz"), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position2D("baz", "xyz"), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position2D("foo", "xyz"), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
    Cell(Position2D("qux", "xyz"), Content(OrdinalSchema(StringCodex), "12.56")))

  val result5 = List(Cell(Position2D("bar", 1), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position2D("bar", 2), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position2D("bar", 3), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position2D("baz", 1), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position2D("baz", 2), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position2D("foo", 1), Content(OrdinalSchema(StringCodex), "3.14")),
    Cell(Position2D("foo", 2), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position2D("foo", 3), Content(NominalSchema(StringCodex), "9.42")),
    Cell(Position2D("foo", 4), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
    Cell(Position2D("qux", 1), Content(OrdinalSchema(StringCodex), "12.56")))

  val result6 = List(Cell(Position1D(1), Content(OrdinalSchema(StringCodex), "12.56")),
    Cell(Position1D(2), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position1D(3), Content(NominalSchema(StringCodex), "9.42")),
    Cell(Position1D(4), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))

  val result7 = List(Cell(Position1D("bar"), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position1D("baz"), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position1D("foo"), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
    Cell(Position1D("qux"), Content(OrdinalSchema(StringCodex), "12.56")))

  val result8 = List(Cell(Position2D(1, "xyz"), Content(OrdinalSchema(StringCodex), "12.56")),
    Cell(Position2D(2, "xyz"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position2D(3, "xyz"), Content(NominalSchema(StringCodex), "9.42")),
    Cell(Position2D(4, "xyz"), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))

  val result9 = List(Cell(Position2D("bar", "xyz"), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position2D("baz", "xyz"), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position2D("foo", "xyz"), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
    Cell(Position2D("qux", "xyz"), Content(OrdinalSchema(StringCodex), "12.56")))

  val result10 = List(Cell(Position2D("bar", 1), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position2D("bar", 2), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position2D("bar", 3), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position2D("baz", 1), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position2D("baz", 2), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position2D("foo", 1), Content(OrdinalSchema(StringCodex), "3.14")),
    Cell(Position2D("foo", 2), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position2D("foo", 3), Content(NominalSchema(StringCodex), "9.42")),
    Cell(Position2D("foo", 4), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
    Cell(Position2D("qux", 1), Content(OrdinalSchema(StringCodex), "12.56")))
}

object TestMatrixSquash {

  case class PreservingMaxPositionWithValue[P <: Position]() extends SquasherWithValue[P] {
    type V = String

    val squasher = PreservingMaxPosition[P]()

    type T = squasher.T

    def prepareWithValue(cell: Cell[P], dim: Dimension, ext: V): T = squasher.prepare(cell, dim)

    def reduce(lt: T, rt: T): T = squasher.reduce(lt, rt)

    def presentWithValue(t: T, ext: V): Option[Content] = if (ext == "ext") squasher.present(t) else None
  }
}

class TestScaldingMatrixSquash extends TestMatrixSquash {

  "A Matrix.squash" should "return its first squashed data in 2D" in {
    toPipe(data2)
      .squash(First, PreservingMaxPosition[Position2D](), Default())
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its second squashed data in 2D" in {
    toPipe(data2)
      .squash(Second, PreservingMaxPosition[Position2D](), Default(Reducers(123)))
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its first squashed data in 3D" in {
    toPipe(data3)
      .squash(First, PreservingMaxPosition[Position3D](), Default())
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its second squashed data in 3D" in {
    toPipe(data3)
      .squash(Second, PreservingMaxPosition[Position3D](), Default(Reducers(123)))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its third squashed data in 3D" in {
    toPipe(data3)
      .squash(Third, PreservingMaxPosition[Position3D](), Default())
      .toList.sortBy(_.position) shouldBe result5
  }

  "A Matrix.squashWithValue" should "return its first squashed data in 2D" in {
    toPipe(data2)
      .squashWithValue(First, TestMatrixSquash.PreservingMaxPositionWithValue[Position2D](), ValuePipe(ext),
        Default(Reducers(123)))
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its second squashed data in 2D" in {
    toPipe(data2)
      .squashWithValue(Second, TestMatrixSquash.PreservingMaxPositionWithValue[Position2D](), ValuePipe(ext),
        Default())
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its first squashed data in 3D" in {
    toPipe(data3)
      .squashWithValue(First, TestMatrixSquash.PreservingMaxPositionWithValue[Position3D](), ValuePipe(ext),
        Default(Reducers(123)))
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its second squashed data in 3D" in {
    toPipe(data3)
      .squashWithValue(Second, TestMatrixSquash.PreservingMaxPositionWithValue[Position3D](), ValuePipe(ext),
        Default())
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its third squashed data in 3D" in {
    toPipe(data3)
      .squashWithValue(Third, TestMatrixSquash.PreservingMaxPositionWithValue[Position3D](), ValuePipe(ext),
        Default(Reducers(123)))
      .toList.sortBy(_.position) shouldBe result10
  }
}

class TestSparkMatrixSquash extends TestMatrixSquash {

  "A Matrix.squash" should "return its first squashed data in 2D" in {
    toRDD(data2)
      .squash(First, PreservingMaxPosition[Position2D](), Default())
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its second squashed data in 2D" in {
    toRDD(data2)
      .squash(Second, PreservingMaxPosition[Position2D](), Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its first squashed data in 3D" in {
    toRDD(data3)
      .squash(First, PreservingMaxPosition[Position3D](), Default())
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its second squashed data in 3D" in {
    toRDD(data3)
      .squash(Second, PreservingMaxPosition[Position3D](), Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its third squashed data in 3D" in {
    toRDD(data3)
      .squash(Third, PreservingMaxPosition[Position3D](), Default())
      .toList.sortBy(_.position) shouldBe result5
  }

  "A Matrix.squashWithValue" should "return its first squashed data in 2D" in {
    toRDD(data2)
      .squashWithValue(First, TestMatrixSquash.PreservingMaxPositionWithValue[Position2D](), ext,
        Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its second squashed data in 2D" in {
    toRDD(data2)
      .squashWithValue(Second, TestMatrixSquash.PreservingMaxPositionWithValue[Position2D](), ext, Default())
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its first squashed data in 3D" in {
    toRDD(data3)
      .squashWithValue(First, TestMatrixSquash.PreservingMaxPositionWithValue[Position3D](), ext,
        Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its second squashed data in 3D" in {
    toRDD(data3)
      .squashWithValue(Second, TestMatrixSquash.PreservingMaxPositionWithValue[Position3D](), ext, Default())
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its third squashed data in 3D" in {
    toRDD(data3)
      .squashWithValue(Third, TestMatrixSquash.PreservingMaxPositionWithValue[Position3D](), ext,
        Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result10
  }
}

trait TestMatrixMelt extends TestMatrix {

  val result1 = List(Cell(Position1D("1.bar"), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position1D("1.baz"), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position1D("1.foo"), Content(OrdinalSchema(StringCodex), "3.14")),
    Cell(Position1D("1.qux"), Content(OrdinalSchema(StringCodex), "12.56")),
    Cell(Position1D("2.bar"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position1D("2.baz"), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position1D("2.foo"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position1D("3.bar"), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position1D("3.foo"), Content(NominalSchema(StringCodex), "9.42")),
    Cell(Position1D("4.foo"), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))

  val result2 = List(Cell(Position1D("bar.1"), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position1D("bar.2"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position1D("bar.3"), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position1D("baz.1"), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position1D("baz.2"), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position1D("foo.1"), Content(OrdinalSchema(StringCodex), "3.14")),
    Cell(Position1D("foo.2"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position1D("foo.3"), Content(NominalSchema(StringCodex), "9.42")),
    Cell(Position1D("foo.4"), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
    Cell(Position1D("qux.1"), Content(OrdinalSchema(StringCodex), "12.56")))

  val result3 = List(Cell(Position2D(1, "xyz.bar"), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position2D(1, "xyz.baz"), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position2D(1, "xyz.foo"), Content(OrdinalSchema(StringCodex), "3.14")),
    Cell(Position2D(1, "xyz.qux"), Content(OrdinalSchema(StringCodex), "12.56")),
    Cell(Position2D(2, "xyz.bar"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position2D(2, "xyz.baz"), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position2D(2, "xyz.foo"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position2D(3, "xyz.bar"), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position2D(3, "xyz.foo"), Content(NominalSchema(StringCodex), "9.42")),
    Cell(Position2D(4, "xyz.foo"), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))))

  val result4 = List(Cell(Position2D("bar", "xyz.1"), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position2D("bar", "xyz.2"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position2D("bar", "xyz.3"), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position2D("baz", "xyz.1"), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position2D("baz", "xyz.2"), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position2D("foo", "xyz.1"), Content(OrdinalSchema(StringCodex), "3.14")),
    Cell(Position2D("foo", "xyz.2"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position2D("foo", "xyz.3"), Content(NominalSchema(StringCodex), "9.42")),
    Cell(Position2D("foo", "xyz.4"), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
    Cell(Position2D("qux", "xyz.1"), Content(OrdinalSchema(StringCodex), "12.56")))

  val result5 = List(Cell(Position2D("bar.xyz", 1), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position2D("bar.xyz", 2), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position2D("bar.xyz", 3), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position2D("baz.xyz", 1), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position2D("baz.xyz", 2), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position2D("foo.xyz", 1), Content(OrdinalSchema(StringCodex), "3.14")),
    Cell(Position2D("foo.xyz", 2), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position2D("foo.xyz", 3), Content(NominalSchema(StringCodex), "9.42")),
    Cell(Position2D("foo.xyz", 4), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
    Cell(Position2D("qux.xyz", 1), Content(OrdinalSchema(StringCodex), "12.56")))
}

class TestScaldingMatrixMelt extends TestMatrixMelt {

  "A Matrix.melt" should "return its first melted data in 2D" in {
    toPipe(data2)
      .melt(First, Second)
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its second melted data in 2D" in {
    toPipe(data2)
      .melt(Second, First)
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its first melted data in 3D" in {
    toPipe(data3)
      .melt(First, Third)
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its second melted data in 3D" in {
    toPipe(data3)
      .melt(Second, Third)
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its third melted data in 3D" in {
    toPipe(data3)
      .melt(Third, First)
      .toList.sortBy(_.position) shouldBe result5
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

  val result1 = List(Cell(Position2D("bar", "abc"), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position2D("baz", "abc"), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position2D("foo", "abc"), Content(OrdinalSchema(StringCodex), "3.14")),
    Cell(Position2D("qux", "abc"), Content(OrdinalSchema(StringCodex), "12.56")))

  val result2 = List(Cell(Position3D("bar", "abc", "def"), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position3D("baz", "abc", "def"), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position3D("foo", "abc", "def"), Content(OrdinalSchema(StringCodex), "3.14")),
    Cell(Position3D("qux", "abc", "def"), Content(OrdinalSchema(StringCodex), "12.56")))

  val result3 = List(Cell(Position4D("bar", "abc", "def", "ghi"), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position4D("baz", "abc", "def", "ghi"), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position4D("foo", "abc", "def", "ghi"), Content(OrdinalSchema(StringCodex), "3.14")),
    Cell(Position4D("qux", "abc", "def", "ghi"), Content(OrdinalSchema(StringCodex), "12.56")))

  val result4 = List(
    Cell(Position5D("bar", "abc", "def", "ghi", "jkl"), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position5D("baz", "abc", "def", "ghi", "jkl"), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position5D("foo", "abc", "def", "ghi", "jkl"), Content(OrdinalSchema(StringCodex), "3.14")),
    Cell(Position5D("qux", "abc", "def", "ghi", "jkl"), Content(OrdinalSchema(StringCodex), "12.56")))

  val result5 = List(Cell(Position3D("bar", 1, "abc"), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position3D("bar", 2, "abc"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("bar", 3, "abc"), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position3D("baz", 1, "abc"), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position3D("baz", 2, "abc"), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position3D("foo", 1, "abc"), Content(OrdinalSchema(StringCodex), "3.14")),
    Cell(Position3D("foo", 2, "abc"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position3D("foo", 3, "abc"), Content(NominalSchema(StringCodex), "9.42")),
    Cell(Position3D("foo", 4, "abc"), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
    Cell(Position3D("qux", 1, "abc"), Content(OrdinalSchema(StringCodex), "12.56")))

  val result6 = List(Cell(Position4D("bar", 1, "abc", "def"), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position4D("bar", 2, "abc", "def"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position4D("bar", 3, "abc", "def"), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position4D("baz", 1, "abc", "def"), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position4D("baz", 2, "abc", "def"), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position4D("foo", 1, "abc", "def"), Content(OrdinalSchema(StringCodex), "3.14")),
    Cell(Position4D("foo", 2, "abc", "def"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position4D("foo", 3, "abc", "def"), Content(NominalSchema(StringCodex), "9.42")),
    Cell(Position4D("foo", 4, "abc", "def"), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
    Cell(Position4D("qux", 1, "abc", "def"), Content(OrdinalSchema(StringCodex), "12.56")))

  val result7 = List(
    Cell(Position5D("bar", 1, "abc", "def", "ghi"), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position5D("bar", 2, "abc", "def", "ghi"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position5D("bar", 3, "abc", "def", "ghi"), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position5D("baz", 1, "abc", "def", "ghi"), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position5D("baz", 2, "abc", "def", "ghi"), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position5D("foo", 1, "abc", "def", "ghi"), Content(OrdinalSchema(StringCodex), "3.14")),
    Cell(Position5D("foo", 2, "abc", "def", "ghi"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position5D("foo", 3, "abc", "def", "ghi"), Content(NominalSchema(StringCodex), "9.42")),
    Cell(Position5D("foo", 4, "abc", "def", "ghi"), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
    Cell(Position5D("qux", 1, "abc", "def", "ghi"), Content(OrdinalSchema(StringCodex), "12.56")))

  val result8 = List(Cell(Position4D("bar", 1, "xyz", "abc"), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position4D("bar", 2, "xyz", "abc"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position4D("bar", 3, "xyz", "abc"), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position4D("baz", 1, "xyz", "abc"), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position4D("baz", 2, "xyz", "abc"), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position4D("foo", 1, "xyz", "abc"), Content(OrdinalSchema(StringCodex), "3.14")),
    Cell(Position4D("foo", 2, "xyz", "abc"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position4D("foo", 3, "xyz", "abc"), Content(NominalSchema(StringCodex), "9.42")),
    Cell(Position4D("foo", 4, "xyz", "abc"), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
    Cell(Position4D("qux", 1, "xyz", "abc"), Content(OrdinalSchema(StringCodex), "12.56")))

  val result9 = List(
    Cell(Position5D("bar", 1, "xyz", "abc", "def"), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position5D("bar", 2, "xyz", "abc", "def"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position5D("bar", 3, "xyz", "abc", "def"), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position5D("baz", 1, "xyz", "abc", "def"), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position5D("baz", 2, "xyz", "abc", "def"), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position5D("foo", 1, "xyz", "abc", "def"), Content(OrdinalSchema(StringCodex), "3.14")),
    Cell(Position5D("foo", 2, "xyz", "abc", "def"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position5D("foo", 3, "xyz", "abc", "def"), Content(NominalSchema(StringCodex), "9.42")),
    Cell(Position5D("foo", 4, "xyz", "abc", "def"), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
    Cell(Position5D("qux", 1, "xyz", "abc", "def"), Content(OrdinalSchema(StringCodex), "12.56")))

  val result10 = List(Cell(Position2D("bar", "abc"), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position2D("baz", "abc"), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position2D("foo", "abc"), Content(OrdinalSchema(StringCodex), "3.14")),
    Cell(Position2D("qux", "abc"), Content(OrdinalSchema(StringCodex), "12.56")))

  val result11 = List(Cell(Position3D("bar", "abc", "def"), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position3D("baz", "abc", "def"), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position3D("foo", "abc", "def"), Content(OrdinalSchema(StringCodex), "3.14")),
    Cell(Position3D("qux", "abc", "def"), Content(OrdinalSchema(StringCodex), "12.56")))

  val result12 = List(Cell(Position4D("bar", "abc", "def", "ghi"), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position4D("baz", "abc", "def", "ghi"), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position4D("foo", "abc", "def", "ghi"), Content(OrdinalSchema(StringCodex), "3.14")),
    Cell(Position4D("qux", "abc", "def", "ghi"), Content(OrdinalSchema(StringCodex), "12.56")))

  val result13 = List(
    Cell(Position5D("bar", "abc", "def", "ghi", "jkl"), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position5D("baz", "abc", "def", "ghi", "jkl"), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position5D("foo", "abc", "def", "ghi", "jkl"), Content(OrdinalSchema(StringCodex), "3.14")),
    Cell(Position5D("qux", "abc", "def", "ghi", "jkl"), Content(OrdinalSchema(StringCodex), "12.56")))

  val result14 = List(Cell(Position3D("bar", 1, "abc"), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position3D("bar", 2, "abc"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position3D("bar", 3, "abc"), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position3D("baz", 1, "abc"), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position3D("baz", 2, "abc"), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position3D("foo", 1, "abc"), Content(OrdinalSchema(StringCodex), "3.14")),
    Cell(Position3D("foo", 2, "abc"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position3D("foo", 3, "abc"), Content(NominalSchema(StringCodex), "9.42")),
    Cell(Position3D("foo", 4, "abc"), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
    Cell(Position3D("qux", 1, "abc"), Content(OrdinalSchema(StringCodex), "12.56")))

  val result15 = List(Cell(Position4D("bar", 1, "abc", "def"), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position4D("bar", 2, "abc", "def"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position4D("bar", 3, "abc", "def"), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position4D("baz", 1, "abc", "def"), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position4D("baz", 2, "abc", "def"), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position4D("foo", 1, "abc", "def"), Content(OrdinalSchema(StringCodex), "3.14")),
    Cell(Position4D("foo", 2, "abc", "def"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position4D("foo", 3, "abc", "def"), Content(NominalSchema(StringCodex), "9.42")),
    Cell(Position4D("foo", 4, "abc", "def"), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
    Cell(Position4D("qux", 1, "abc", "def"), Content(OrdinalSchema(StringCodex), "12.56")))

  val result16 = List(
    Cell(Position5D("bar", 1, "abc", "def", "ghi"), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position5D("bar", 2, "abc", "def", "ghi"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position5D("bar", 3, "abc", "def", "ghi"), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position5D("baz", 1, "abc", "def", "ghi"), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position5D("baz", 2, "abc", "def", "ghi"), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position5D("foo", 1, "abc", "def", "ghi"), Content(OrdinalSchema(StringCodex), "3.14")),
    Cell(Position5D("foo", 2, "abc", "def", "ghi"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position5D("foo", 3, "abc", "def", "ghi"), Content(NominalSchema(StringCodex), "9.42")),
    Cell(Position5D("foo", 4, "abc", "def", "ghi"), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
    Cell(Position5D("qux", 1, "abc", "def", "ghi"), Content(OrdinalSchema(StringCodex), "12.56")))

  val result17 = List(Cell(Position4D("bar", 1, "xyz", "abc"), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position4D("bar", 2, "xyz", "abc"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position4D("bar", 3, "xyz", "abc"), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position4D("baz", 1, "xyz", "abc"), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position4D("baz", 2, "xyz", "abc"), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position4D("foo", 1, "xyz", "abc"), Content(OrdinalSchema(StringCodex), "3.14")),
    Cell(Position4D("foo", 2, "xyz", "abc"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position4D("foo", 3, "xyz", "abc"), Content(NominalSchema(StringCodex), "9.42")),
    Cell(Position4D("foo", 4, "xyz", "abc"), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
    Cell(Position4D("qux", 1, "xyz", "abc"), Content(OrdinalSchema(StringCodex), "12.56")))

  val result18 = List(
    Cell(Position5D("bar", 1, "xyz", "abc", "def"), Content(OrdinalSchema(StringCodex), "6.28")),
    Cell(Position5D("bar", 2, "xyz", "abc", "def"), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position5D("bar", 3, "xyz", "abc", "def"), Content(OrdinalSchema(LongCodex), 19)),
    Cell(Position5D("baz", 1, "xyz", "abc", "def"), Content(OrdinalSchema(StringCodex), "9.42")),
    Cell(Position5D("baz", 2, "xyz", "abc", "def"), Content(DiscreteSchema(LongCodex), 19)),
    Cell(Position5D("foo", 1, "xyz", "abc", "def"), Content(OrdinalSchema(StringCodex), "3.14")),
    Cell(Position5D("foo", 2, "xyz", "abc", "def"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position5D("foo", 3, "xyz", "abc", "def"), Content(NominalSchema(StringCodex), "9.42")),
    Cell(Position5D("foo", 4, "xyz", "abc", "def"), Content(DateSchema(DateCodex("yyyy-MM-dd hh:mm:ss")),
      (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))),
    Cell(Position5D("qux", 1, "xyz", "abc", "def"), Content(OrdinalSchema(StringCodex), "12.56")))
}

object TestMatrixExpand {

  type PwE = Position with ExpandablePosition

  def expander1D[P <: PwE](cell: Cell[P]): Option[P#M] = Some(cell.position.append("abc"))
  def expander2D[P <: PwE, Q <: PwE](cell: Cell[P])(implicit ev: P#M =:= Q): Option[Q#M] = {
    Some(cell.position.append("abc").append("def"))
  }
  def expander3D[P <: PwE, Q <: PwE, R <: PwE](cell: Cell[P])(implicit ev1: P#M =:= Q, ev2: Q#M =:= R): Option[R#M] = {
    Some(cell.position.append("abc").append("def").append("ghi"))
  }
  def expander4D[P <: PwE, Q <: PwE, R <: PwE, S <: PwE](cell: Cell[P])(implicit ev1: P#M =:= Q, ev2: Q#M =:= R,
    ev3: R#M =:= S): Option[S#M] = Some(cell.position.append("abc").append("def").append("ghi").append("jkl"))

  val expand1D2D = expander1D[Position1D] _
  val expand1D3D = expander2D[Position1D, Position2D] _
  val expand1D4D = expander3D[Position1D, Position2D, Position3D] _
  val expand1D5D = expander4D[Position1D, Position2D, Position3D, Position4D] _

  val expand2D3D = expander1D[Position2D] _
  val expand2D4D = expander2D[Position2D, Position3D] _
  val expand2D5D = expander3D[Position2D, Position3D, Position4D] _

  val expand3D4D = expander1D[Position3D] _
  val expand3D5D = expander2D[Position3D, Position4D] _

  def expander1DWithValue[P <: PwE](cell: Cell[P], ext: String): Option[P#M] = Some(cell.position.append(ext))
  def expander2DWithValue[P <: PwE, Q <: PwE](cell: Cell[P], ext: String)(implicit ev: P#M =:= Q): Option[Q#M] = {
    Some(cell.position.append(ext).append("def"))
  }
  def expander3DWithValue[P <: PwE, Q <: PwE, R <: PwE](cell: Cell[P], ext: String)(implicit ev1: P#M =:= Q,
    ev2: Q#M =:= R): Option[R#M] = Some(cell.position.append(ext).append("def").append("ghi"))
  def expander4DWithValue[P <: PwE, Q <: PwE, R <: PwE, S <: PwE](cell: Cell[P], ext: String)(implicit ev1: P#M =:= Q,
    ev2: Q#M =:= R, ev3: R#M =:= S): Option[S#M] = {
    Some(cell.position.append(ext).append("def").append("ghi").append("jkl"))
  }

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

class TestScaldingMatrixExpand extends TestMatrixExpand {

  "A Matrix.expand" should "return its 1D expanded data in 1D" in {
    toPipe(data1)
      .relocate(TestMatrixExpand.expand1D2D)
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its 2D expanded data in 1D" in {
    toPipe(data1)
      .relocate(TestMatrixExpand.expand1D3D)
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its 3D expanded data in 1D" in {
    toPipe(data1)
      .relocate(TestMatrixExpand.expand1D4D)
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its 4D expanded data in 1D" in {
    toPipe(data1)
      .relocate(TestMatrixExpand.expand1D5D)
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its 1D expanded data in 2D" in {
    toPipe(data2)
      .relocate(TestMatrixExpand.expand2D3D)
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its 2D expanded data in 2D" in {
    toPipe(data2)
      .relocate(TestMatrixExpand.expand2D4D)
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its 3D expanded data in 2D" in {
    toPipe(data2)
      .relocate(TestMatrixExpand.expand2D5D)
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its 1D expanded data in 3D" in {
    toPipe(data3)
      .relocate(TestMatrixExpand.expand3D4D)
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its 2D expanded data in 3D" in {
    toPipe(data3)
      .relocate(TestMatrixExpand.expand3D5D)
      .toList.sortBy(_.position) shouldBe result9
  }

  "A Matrix.expandWithValue" should "return its 1D expanded data in 1D" in {
    toPipe(data1)
      .relocateWithValue(TestMatrixExpand.expand1D2DWithValue, ValuePipe(ext))
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its 2D expanded data in 1D" in {
    toPipe(data1)
      .relocateWithValue(TestMatrixExpand.expand1D3DWithValue, ValuePipe(ext))
      .toList.sortBy(_.position) shouldBe result11
  }

  it should "return its 3D expanded data in 1D" in {
    toPipe(data1)
      .relocateWithValue(TestMatrixExpand.expand1D4DWithValue, ValuePipe(ext))
      .toList.sortBy(_.position) shouldBe result12
  }

  it should "return its 4D expanded data in 1D" in {
    toPipe(data1)
      .relocateWithValue(TestMatrixExpand.expand1D5DWithValue, ValuePipe(ext))
      .toList.sortBy(_.position) shouldBe result13
  }

  it should "return its 1D expanded data in 2D" in {
    toPipe(data2)
      .relocateWithValue(TestMatrixExpand.expand2D3DWithValue, ValuePipe(ext))
      .toList.sortBy(_.position) shouldBe result14
  }

  it should "return its 2D expanded data in 2D" in {
    toPipe(data2)
      .relocateWithValue(TestMatrixExpand.expand2D4DWithValue, ValuePipe(ext))
      .toList.sortBy(_.position) shouldBe result15
  }

  it should "return its 3D expanded data in 2D" in {
    toPipe(data2)
      .relocateWithValue(TestMatrixExpand.expand2D5DWithValue, ValuePipe(ext))
      .toList.sortBy(_.position) shouldBe result16
  }

  it should "return its 1D expanded data in 3D" in {
    toPipe(data3)
      .relocateWithValue(TestMatrixExpand.expand3D4DWithValue, ValuePipe(ext))
      .toList.sortBy(_.position) shouldBe result17
  }

  it should "return its 2D expanded data in 3D" in {
    toPipe(data3)
      .relocateWithValue(TestMatrixExpand.expand3D5DWithValue, ValuePipe(ext))
      .toList.sortBy(_.position) shouldBe result18
  }
}

class TestSparkMatrixExpand extends TestMatrixExpand {

  "A Matrix.expand" should "return its 1D expanded data in 1D" in {
    toRDD(data1)
      .relocate(TestMatrixExpand.expand1D2D)
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its 2D expanded data in 1D" in {
    toRDD(data1)
      .relocate(TestMatrixExpand.expand1D3D)
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its 3D expanded data in 1D" in {
    toRDD(data1)
      .relocate(TestMatrixExpand.expand1D4D)
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its 4D expanded data in 1D" in {
    toRDD(data1)
      .relocate(TestMatrixExpand.expand1D5D)
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its expanded 1D data in 2D" in {
    toRDD(data2)
      .relocate(TestMatrixExpand.expand2D3D)
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its expanded 2D data in 2D" in {
    toRDD(data2)
      .relocate(TestMatrixExpand.expand2D4D)
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its expanded 3D data in 2D" in {
    toRDD(data2)
      .relocate(TestMatrixExpand.expand2D5D)
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its expanded 1D data in 3D" in {
    toRDD(data3)
      .relocate(TestMatrixExpand.expand3D4D)
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its expanded 2D data in 3D" in {
    toRDD(data3)
      .relocate(TestMatrixExpand.expand3D5D)
      .toList.sortBy(_.position) shouldBe result9
  }

  "A Matrix.expandWithValue" should "return its 1D expanded data in 1D" in {
    toRDD(data1)
      .relocateWithValue(TestMatrixExpand.expand1D2DWithValue, ext)
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its 2D expanded data in 1D" in {
    toRDD(data1)
      .relocateWithValue(TestMatrixExpand.expand1D3DWithValue, ext)
      .toList.sortBy(_.position) shouldBe result11
  }

  it should "return its 3D expanded data in 1D" in {
    toRDD(data1)
      .relocateWithValue(TestMatrixExpand.expand1D4DWithValue, ext)
      .toList.sortBy(_.position) shouldBe result12
  }

  it should "return its 4D expanded data in 1D" in {
    toRDD(data1)
      .relocateWithValue(TestMatrixExpand.expand1D5DWithValue, ext)
      .toList.sortBy(_.position) shouldBe result13
  }

  it should "return its 1D expanded data in 2D" in {
    toRDD(data2)
      .relocateWithValue(TestMatrixExpand.expand2D3DWithValue, ext)
      .toList.sortBy(_.position) shouldBe result14
  }

  it should "return its 2D expanded data in 2D" in {
    toRDD(data2)
      .relocateWithValue(TestMatrixExpand.expand2D4DWithValue, ext)
      .toList.sortBy(_.position) shouldBe result15
  }

  it should "return its 3D expanded data in 2D" in {
    toRDD(data2)
      .relocateWithValue(TestMatrixExpand.expand2D5DWithValue, ext)
      .toList.sortBy(_.position) shouldBe result16
  }

  it should "return its 1D expanded data in 3D" in {
    toRDD(data3)
      .relocateWithValue(TestMatrixExpand.expand3D4DWithValue, ext)
      .toList.sortBy(_.position) shouldBe result17
  }

  it should "return its 2D expanded data in 3D" in {
    toRDD(data3)
      .relocateWithValue(TestMatrixExpand.expand3D5DWithValue, ext)
      .toList.sortBy(_.position) shouldBe result18
  }
}

trait TestMatrixPermute extends TestMatrix {

  val dataA = List(Cell(Position2D(1, 3), Content(ContinuousSchema(DoubleCodex), 3.14)),
    Cell(Position2D(2, 2), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position2D(3, 1), Content(ContinuousSchema(DoubleCodex), 9.42)))

  val dataB = List(Cell(Position3D(1, 2, 3), Content(ContinuousSchema(DoubleCodex), 3.14)),
    Cell(Position3D(2, 2, 2), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position3D(3, 2, 1), Content(ContinuousSchema(DoubleCodex), 9.42)))

  val dataC = List(Cell(Position4D(1, 2, 3, 4), Content(ContinuousSchema(DoubleCodex), 3.14)),
    Cell(Position4D(2, 2, 2, 2), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position4D(1, 1, 4, 4), Content(ContinuousSchema(DoubleCodex), 9.42)),
    Cell(Position4D(4, 1, 3, 2), Content(ContinuousSchema(DoubleCodex), 12.56)))

  val dataD = List(Cell(Position5D(1, 2, 3, 4, 5), Content(ContinuousSchema(DoubleCodex), 3.14)),
    Cell(Position5D(2, 2, 2, 2, 2), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position5D(1, 1, 3, 5, 5), Content(ContinuousSchema(DoubleCodex), 9.42)),
    Cell(Position5D(4, 4, 4, 1, 1), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position5D(5, 4, 3, 2, 1), Content(ContinuousSchema(DoubleCodex), 18.84)))

  val result1 = List(Cell(Position2D(1, 3), Content(ContinuousSchema(DoubleCodex), 9.42)),
    Cell(Position2D(2, 2), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position2D(3, 1), Content(ContinuousSchema(DoubleCodex), 3.14)))

  val result2 = List(Cell(Position3D(2, 1, 3), Content(ContinuousSchema(DoubleCodex), 9.42)),
    Cell(Position3D(2, 2, 2), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position3D(2, 3, 1), Content(ContinuousSchema(DoubleCodex), 3.14)))

  val result3 = List(Cell(Position4D(2, 2, 2, 2), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position4D(2, 3, 4, 1), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position4D(4, 3, 1, 2), Content(ContinuousSchema(DoubleCodex), 3.14)),
    Cell(Position4D(4, 4, 1, 1), Content(ContinuousSchema(DoubleCodex), 9.42)))

  val result4 = List(Cell(Position5D(1, 4, 4, 1, 4), Content(ContinuousSchema(DoubleCodex), 12.56)),
    Cell(Position5D(2, 2, 2, 2, 2), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position5D(2, 4, 5, 1, 3), Content(ContinuousSchema(DoubleCodex), 18.84)),
    Cell(Position5D(4, 2, 1, 5, 3), Content(ContinuousSchema(DoubleCodex), 3.14)),
    Cell(Position5D(5, 1, 1, 5, 3), Content(ContinuousSchema(DoubleCodex), 9.42)))
}

class TestScaldingMatrixPermute extends TestMatrixPermute {

  "A Matrix.permute" should "return its permutation in 2D" in {
    toPipe(dataA)
      .permute(Second, First)
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its permutation in 3D" in {
    toPipe(dataB)
      .permute(Second, Third, First)
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its permutation in 4D" in {
    toPipe(dataC)
      .permute(Fourth, Third, First, Second)
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its permutation in 5D" in {
    toPipe(dataD)
      .permute(Fourth, Second, First, Fifth, Third)
      .toList.sortBy(_.position) shouldBe result4
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

trait TestMatrixToVector extends TestMatrix {

  val separator = ":"

  val result1 = data2
    .map { case Cell(Position2D(f, s), c) => Cell(Position1D(f.toShortString + separator + s.toShortString), c) }
    .sortBy(_.position)

  val result2 = data3
    .map {
      case Cell(Position3D(f, s, t), c) =>
        Cell(Position1D(f.toShortString + separator + s.toShortString + separator + t.toShortString), c)
    }
    .sortBy(_.position)
}

class TestScaldingMatrixToVector extends TestMatrixToVector {

  "A Matrix.toVector" should "return its vector for 2D" in {
    toPipe(data2)
      .toVector(separator)
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its permutation vector for 3D" in {
    toPipe(data3)
      .toVector(separator)
      .toList.sortBy(_.position) shouldBe result2
  }
}

class TestSparkMatrixToVector extends TestMatrixToVector {

  "A Matrix.toVector" should "return its vector for 2D" in {
    toRDD(data2)
      .toVector(separator)
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its permutation vector for 3D" in {
    toRDD(data3)
      .toVector(separator)
      .toList.sortBy(_.position) shouldBe result2
  }
}

trait TestMatrixMaterialise extends TestMatrix {

  val data = List(("a", "one", Content(ContinuousSchema(DoubleCodex), 3.14)),
    ("a", "two", Content(NominalSchema(StringCodex), "foo")),
    ("a", "three", Content(DiscreteSchema(LongCodex), 42)),
    ("b", "one", Content(ContinuousSchema(DoubleCodex), 6.28)),
    ("b", "two", Content(DiscreteSchema(LongCodex), 123)),
    ("b", "three", Content(ContinuousSchema(DoubleCodex), 9.42)),
    ("c", "two", Content(NominalSchema(StringCodex), "bar")),
    ("c", "three", Content(ContinuousSchema(DoubleCodex), 12.56)))

  val result = List(Cell(Position2D("a", "one"), Content(ContinuousSchema(DoubleCodex), 3.14)),
    Cell(Position2D("a", "two"), Content(NominalSchema(StringCodex), "foo")),
    Cell(Position2D("a", "three"), Content(DiscreteSchema(LongCodex), 42)),
    Cell(Position2D("b", "one"), Content(ContinuousSchema(DoubleCodex), 6.28)),
    Cell(Position2D("b", "two"), Content(DiscreteSchema(LongCodex), 123)),
    Cell(Position2D("b", "three"), Content(ContinuousSchema(DoubleCodex), 9.42)),
    Cell(Position2D("c", "two"), Content(NominalSchema(StringCodex), "bar")),
    Cell(Position2D("c", "three"), Content(ContinuousSchema(DoubleCodex), 12.56)))
}

class TestScaldingMatrixMaterialise extends TestMatrixMaterialise {

  "A Matrix.materialise" should "return its list" in {
    LV2C2TPM2(data)
      .materialise(Default(Execution()))
      .sortBy(_.position) shouldBe result.sortBy(_.position)
  }
}

class TestSparkMatrixMaterialise extends TestMatrixMaterialise {

  "A Matrix.materialise" should "return its list" in {
    LV2C2RDDM2(data)
      .materialise(Default())
      .sortBy(_.position) shouldBe result.sortBy(_.position)
  }
}

trait TestMatrixToText extends TestMatrix {

  val result1 = data1.map(_.toString(":", true, true)).sorted

  val result2 = data2.map(_.toString("|", false, false)).sorted

  val result3 = data3.map(_.toString("/", false, true)).sorted
}

class TestScaldingMatrixToText extends TestMatrixToText {

  "A Matrix.toText" should "return its strings for 1D" in {
    toPipe(data1)
      .toText(Cell.toString(":", true, true))
      .toList.sorted shouldBe result1
  }

  "A Matrix.toText" should "return its strings for 2D" in {
    toPipe(data2)
      .toText(Cell.toString("|", false, false))
      .toList.sorted shouldBe result2
  }

  "A Matrix.toText" should "return its strings for 3D" in {
    toPipe(data3)
      .toText(Cell.toString("/", false, true))
      .toList.sorted shouldBe result3
  }
}

class TestSparkMatrixToText extends TestMatrixToText {

  "A Matrix.toText" should "return its strings for 1D" in {
    toRDD(data1)
      .toText(Cell.toString(":", true, true))
      .toList.sorted shouldBe result1
  }

  "A Matrix.toText" should "return its strings for 2D" in {
    toRDD(data2)
      .toText(Cell.toString("|", false, false))
      .toList.sorted shouldBe result2
  }

  "A Matrix.toText" should "return its strings for 3D" in {
    toRDD(data3)
      .toText(Cell.toString("/", false, true))
      .toList.sorted shouldBe result3
  }
}

