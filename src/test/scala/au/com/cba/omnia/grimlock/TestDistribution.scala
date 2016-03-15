// Copyright 2016 Commonwealth Bank of Australia
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
import au.com.cba.omnia.grimlock.framework.distribution._
import au.com.cba.omnia.grimlock.framework.position._

import au.com.cba.omnia.grimlock.scalding.Matrix._

import au.com.cba.omnia.grimlock.spark.Matrix._

import com.twitter.scalding.typed.{ TypedPipe, ValuePipe }

trait TestDistribution extends TestGrimlock {

  val data1 = List(Cell(Position1D("foo"), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position1D("bar"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position1D("bar"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position1D("qux"), Content(OrdinalSchema[String](), "3.14")))

  val data2 = List(Cell(Position2D("row1", "col1"), Content(NominalSchema[String](), "a")),
    Cell(Position2D("row1", "col3"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position2D("row1", "col4"), Content(NominalSchema[String](), "b")),
    Cell(Position2D("row2", "col1"), Content(NominalSchema[String](), "a")),
    Cell(Position2D("row2", "col2"), Content(NominalSchema[String](), "b")),
    Cell(Position2D("row2", "col4"), Content(DiscreteSchema[Long](), 2)),
    Cell(Position2D("row3", "col2"), Content(NominalSchema[String](), "b")),
    Cell(Position2D("row3", "col3"), Content(NominalSchema[String](), "a")),
    Cell(Position2D("row3", "col4"), Content(DiscreteSchema[Long](), 3)),
    Cell(Position2D("row4", "col1"), Content(DiscreteSchema[Long](), 4)),
    Cell(Position2D("row4", "col2"), Content(NominalSchema[String](), "a")),
    Cell(Position2D("row4", "col3"), Content(NominalSchema[String](), "b")))

  val data3 = List(Cell(Position3D("row1", "col1", "dep1"), Content(NominalSchema[String](), "a")),
    Cell(Position3D("row1", "col3", "dep1"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position3D("row1", "col4", "dep1"), Content(NominalSchema[String](), "b")),
    Cell(Position3D("row2", "col1", "dep1"), Content(NominalSchema[String](), "a")),
    Cell(Position3D("row2", "col2", "dep1"), Content(NominalSchema[String](), "b")),
    Cell(Position3D("row2", "col4", "dep1"), Content(DiscreteSchema[Long](), 2)),
    Cell(Position3D("row3", "col2", "dep1"), Content(NominalSchema[String](), "b")),
    Cell(Position3D("row3", "col3", "dep1"), Content(NominalSchema[String](), "a")),
    Cell(Position3D("row3", "col4", "dep1"), Content(DiscreteSchema[Long](), 3)),
    Cell(Position3D("row4", "col1", "dep1"), Content(DiscreteSchema[Long](), 4)),
    Cell(Position3D("row4", "col2", "dep1"), Content(NominalSchema[String](), "a")),
    Cell(Position3D("row4", "col3", "dep1"), Content(NominalSchema[String](), "b")),
    Cell(Position3D("row1", "col1", "dep2"), Content(NominalSchema[String](), "a")),
    Cell(Position3D("row2", "col2", "dep2"), Content(NominalSchema[String](), "b")),
    Cell(Position3D("row3", "col3", "dep2"), Content(NominalSchema[String](), "b")),
    Cell(Position3D("row4", "col4", "dep2"), Content(NominalSchema[String](), "a")))

  val result1 = List(Cell(Position2D("bar", "6.28"), Content(DiscreteSchema[Long](), 2)),
    Cell(Position2D("foo", "3.14"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position2D("qux", "3.14"), Content(DiscreteSchema[Long](), 1)))

  val result2 = List(Cell(Position1D("3.14"), Content(DiscreteSchema[Long](), 2)),
    Cell(Position1D("6.28"), Content(DiscreteSchema[Long](), 2)))

  val result3 = List(Cell(Position2D("row1", "a"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position2D("row1", "b"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position2D("row2", "a"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position2D("row2", "b"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position2D("row3", "a"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position2D("row3", "b"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position2D("row4", "a"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position2D("row4", "b"), Content(DiscreteSchema[Long](), 1)))

  val result4 = List(Cell(Position2D("col1", "a"), Content(DiscreteSchema[Long](), 2)),
    Cell(Position2D("col2", "a"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position2D("col2", "b"), Content(DiscreteSchema[Long](), 2)),
    Cell(Position2D("col3", "a"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position2D("col3", "b"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position2D("col4", "b"), Content(DiscreteSchema[Long](), 1)))

  val result5 = List(Cell(Position2D("col1", "4"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position2D("col1", "a"), Content(DiscreteSchema[Long](), 2)),
    Cell(Position2D("col2", "a"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position2D("col2", "b"), Content(DiscreteSchema[Long](), 2)),
    Cell(Position2D("col3", "1"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position2D("col3", "a"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position2D("col3", "b"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position2D("col4", "2"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position2D("col4", "3"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position2D("col4", "b"), Content(DiscreteSchema[Long](), 1)))

  val result6 = List(Cell(Position2D("row1", "1"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position2D("row1", "a"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position2D("row1", "b"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position2D("row2", "2"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position2D("row2", "a"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position2D("row2", "b"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position2D("row3", "3"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position2D("row3", "a"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position2D("row3", "b"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position2D("row4", "4"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position2D("row4", "a"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position2D("row4", "b"), Content(DiscreteSchema[Long](), 1)))

  val result7 = List(Cell(Position2D("row1", "1"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position2D("row1", "a"), Content(DiscreteSchema[Long](), 2)),
    Cell(Position2D("row1", "b"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position2D("row2", "2"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position2D("row2", "a"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position2D("row2", "b"), Content(DiscreteSchema[Long](), 2)),
    Cell(Position2D("row3", "3"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position2D("row3", "a"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position2D("row3", "b"), Content(DiscreteSchema[Long](), 2)),
    Cell(Position2D("row4", "4"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position2D("row4", "a"), Content(DiscreteSchema[Long](), 2)),
    Cell(Position2D("row4", "b"), Content(DiscreteSchema[Long](), 1)))

  val result8 = List(Cell(Position3D("col1", "dep1", "4"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position3D("col1", "dep1", "a"), Content(DiscreteSchema[Long](), 2)),
    Cell(Position3D("col1", "dep2", "a"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position3D("col2", "dep1", "a"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position3D("col2", "dep1", "b"), Content(DiscreteSchema[Long](), 2)),
    Cell(Position3D("col2", "dep2", "b"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position3D("col3", "dep1", "1"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position3D("col3", "dep1", "a"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position3D("col3", "dep1", "b"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position3D("col3", "dep2", "b"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position3D("col4", "dep1", "2"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position3D("col4", "dep1", "3"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position3D("col4", "dep1", "b"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position3D("col4", "dep2", "a"), Content(DiscreteSchema[Long](), 1)))

  val result9 = List(Cell(Position2D("col1", "a"), Content(DiscreteSchema[Long](), 3)),
    Cell(Position2D("col2", "a"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position2D("col2", "b"), Content(DiscreteSchema[Long](), 3)),
    Cell(Position2D("col3", "a"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position2D("col3", "b"), Content(DiscreteSchema[Long](), 2)),
    Cell(Position2D("col4", "a"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position2D("col4", "b"), Content(DiscreteSchema[Long](), 1)))

  val result10 = List(Cell(Position3D("row1", "dep1", "a"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position3D("row1", "dep1", "b"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position3D("row1", "dep2", "a"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position3D("row2", "dep1", "a"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position3D("row2", "dep1", "b"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position3D("row2", "dep2", "b"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position3D("row3", "dep1", "a"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position3D("row3", "dep1", "b"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position3D("row3", "dep2", "b"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position3D("row4", "dep1", "a"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position3D("row4", "dep1", "b"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position3D("row4", "dep2", "a"), Content(DiscreteSchema[Long](), 1)))

  val result11 = List(Cell(Position2D("dep1", "a"), Content(DiscreteSchema[Long](), 4)),
    Cell(Position2D("dep1", "b"), Content(DiscreteSchema[Long](), 4)),
    Cell(Position2D("dep2", "a"), Content(DiscreteSchema[Long](), 2)),
    Cell(Position2D("dep2", "b"), Content(DiscreteSchema[Long](), 2)))

  val result12 = List(Cell(Position3D("row1", "col1", "a"), Content(DiscreteSchema[Long](), 2)),
    Cell(Position3D("row1", "col4", "b"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position3D("row2", "col1", "a"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position3D("row2", "col2", "b"), Content(DiscreteSchema[Long](), 2)),
    Cell(Position3D("row3", "col2", "b"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position3D("row3", "col3", "a"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position3D("row3", "col3", "b"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position3D("row4", "col2", "a"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position3D("row4", "col3", "b"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position3D("row4", "col4", "a"), Content(DiscreteSchema[Long](), 1)))
}

class TestScaldingHistogram extends TestDistribution {

  "A histogram" should "return its first over in 1D" in {
    toPipe(data1)
      .histogram(Over(First), Locate.AppendContentString[Position1D](), true, Default())
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first along in 1D" in {
    toPipe(data1)
      .histogram(Along(First), Locate.AppendContentString[Position0D](), true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its first over in 2D" in {
    toPipe(data2)
      .histogram(Over(First), Locate.AppendContentString[Position1D](), true, Default())
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its first along in 2D" in {
    toPipe(data2)
      .histogram(Along(First), Locate.AppendContentString[Position1D](), true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its second over in 2D" in {
    toPipe(data2)
      .histogram(Over(Second), Locate.AppendContentString[Position1D](), false, Default())
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its second along in 2D" in {
    toPipe(data2)
      .histogram(Along(Second), Locate.AppendContentString[Position1D](), false, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its first over in 3D" in {
    toPipe(data3)
      .histogram(Over(First), Locate.AppendContentString[Position1D](), false, Default())
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its first along in 3D" in {
    toPipe(data3)
      .histogram(Along(First), Locate.AppendContentString[Position2D](), false, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its second over in 3D" in {
    toPipe(data3)
      .histogram(Over(Second), Locate.AppendContentString[Position1D](), true, Default())
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its second along in 3D" in {
    toPipe(data3)
      .histogram(Along(Second), Locate.AppendContentString[Position2D](), true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its third over in 3D" in {
    toPipe(data3)
      .histogram(Over(Third), Locate.AppendContentString[Position1D](), true, Default())
      .toList.sortBy(_.position) shouldBe result11
  }

  it should "return its third along in 3D" in {
    toPipe(data3)
      .histogram(Along(Third), Locate.AppendContentString[Position2D](), true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result12
  }
}

class TestSparkHistogram extends TestDistribution {

  "A histogram" should "return its first over in 1D" in {
    toRDD(data1)
      .histogram(Over(First), Locate.AppendContentString[Position1D](), true, Default())
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first along in 1D" in {
    toRDD(data1)
      .histogram(Along(First), Locate.AppendContentString[Position0D](), true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its first over in 2D" in {
    toRDD(data2)
      .histogram(Over(First), Locate.AppendContentString[Position1D](), true, Default())
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its first along in 2D" in {
    toRDD(data2)
      .histogram(Along(First), Locate.AppendContentString[Position1D](), true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its second over in 2D" in {
    toRDD(data2)
      .histogram(Over(Second), Locate.AppendContentString[Position1D](), false, Default())
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its second along in 2D" in {
    toRDD(data2)
      .histogram(Along(Second), Locate.AppendContentString[Position1D](), false, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its first over in 3D" in {
    toRDD(data3)
      .histogram(Over(First), Locate.AppendContentString[Position1D](), false, Default())
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its first along in 3D" in {
    toRDD(data3)
      .histogram(Along(First), Locate.AppendContentString[Position2D](), false, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its second over in 3D" in {
    toRDD(data3)
      .histogram(Over(Second), Locate.AppendContentString[Position1D](), true, Default())
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its second along in 3D" in {
    toRDD(data3)
      .histogram(Along(Second), Locate.AppendContentString[Position2D](), true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its third over in 3D" in {
    toRDD(data3)
      .histogram(Over(Third), Locate.AppendContentString[Position1D](), true, Default())
      .toList.sortBy(_.position) shouldBe result11
  }

  it should "return its third along in 3D" in {
    toRDD(data3)
      .histogram(Along(Third), Locate.AppendContentString[Position2D](), true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result12
  }
}

trait TestQuantile extends TestGrimlock {

  val probs = List(0.2, 0.4, 0.6, 0.8)

  val data1 = List(Cell(Position1D("foo"), Content(ContinuousSchema[Double](), 3.14)))

  val data2 = List(Cell(Position1D("foo"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position1D("bar"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position1D("baz"), Content(ContinuousSchema[Double](), 9.42)))

  val data3 = List(Cell(Position1D("foo"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position1D("bar"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position1D("baz"), Content(ContinuousSchema[Double](), 3.14)))

  val data4 = List(Cell(Position2D("row1", "col1"), Content(DiscreteSchema[Long](), 2)),
    Cell(Position2D("row2", "col1"), Content(DiscreteSchema[Long](), 3)),
    Cell(Position2D("row3", "col1"), Content(DiscreteSchema[Long](), 4)),
    Cell(Position2D("row4", "col1"), Content(DiscreteSchema[Long](), 5)),
    Cell(Position2D("row5", "col1"), Content(DiscreteSchema[Long](), 4)),
    Cell(Position2D("row6", "col1"), Content(DiscreteSchema[Long](), 4)),
    Cell(Position2D("row7", "col1"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position2D("row8", "col1"), Content(DiscreteSchema[Long](), 4)),
    Cell(Position2D("row1", "col2"), Content(DiscreteSchema[Long](), 42)),
    Cell(Position2D("row2", "col2"), Content(DiscreteSchema[Long](), 42)),
    Cell(Position2D("row3", "col2"), Content(DiscreteSchema[Long](), 42)),
    Cell(Position2D("row4", "col2"), Content(DiscreteSchema[Long](), 42)),
    Cell(Position2D("row5", "col2"), Content(DiscreteSchema[Long](), 42)),
    Cell(Position2D("row6", "col2"), Content(DiscreteSchema[Long](), 42)),
    Cell(Position2D("row7", "col2"), Content(DiscreteSchema[Long](), 42)),
    Cell(Position2D("row8", "col2"), Content(DiscreteSchema[Long](), 42)))

  val data5 = data4.map(_.relocate(_.position.permute(List(Second, First))))

  val data6 = List(Cell(Position1D("foo"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position1D("bar"), Content(NominalSchema[String](), "6.28")),
    Cell(Position1D("baz"), Content(ContinuousSchema[Double](), 9.42)))

  val result1 = List(Cell(Position1D("quantile=0.200000"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position1D("quantile=0.400000"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position1D("quantile=0.600000"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position1D("quantile=0.800000"), Content(ContinuousSchema[Double](), 3.14)))

  val result2 = List(Cell(Position1D("quantile=0.200000"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position1D("quantile=0.400000"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position1D("quantile=0.600000"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position1D("quantile=0.800000"), Content(ContinuousSchema[Double](), 9.42)))

  val result3 = List(Cell(Position1D("quantile=0.200000"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position1D("quantile=0.400000"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position1D("quantile=0.600000"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position1D("quantile=0.800000"), Content(ContinuousSchema[Double](), 6.28)))

  val result4 = List(Cell(Position1D("quantile=0.200000"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position1D("quantile=0.400000"), Content(ContinuousSchema[Double](), 3.768)),
    Cell(Position1D("quantile=0.600000"), Content(ContinuousSchema[Double](), 5.652)),
    Cell(Position1D("quantile=0.800000"), Content(ContinuousSchema[Double](), 7.536)))

  val result5 = List(Cell(Position1D("quantile=0.200000"), Content(ContinuousSchema[Double](), 3.454)),
    Cell(Position1D("quantile=0.400000"), Content(ContinuousSchema[Double](), 5.338)),
    Cell(Position1D("quantile=0.600000"), Content(ContinuousSchema[Double](), 7.222)),
    Cell(Position1D("quantile=0.800000"), Content(ContinuousSchema[Double](), 9.106)))

  val result6 = List(Cell(Position1D("quantile=0.200000"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position1D("quantile=0.400000"), Content(ContinuousSchema[Double](), 5.024)),
    Cell(Position1D("quantile=0.600000"), Content(ContinuousSchema[Double](), 7.536)),
    Cell(Position1D("quantile=0.800000"), Content(ContinuousSchema[Double](), 9.42)))

  val result7 = List(Cell(Position1D("quantile=0.200000"), Content(ContinuousSchema[Double](), 4.396)),
    Cell(Position1D("quantile=0.400000"), Content(ContinuousSchema[Double](), 5.652)),
    Cell(Position1D("quantile=0.600000"), Content(ContinuousSchema[Double](), 6.908)),
    Cell(Position1D("quantile=0.800000"), Content(ContinuousSchema[Double](), 8.164)))

  val result8 = List(Cell(Position1D("quantile=0.200000"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position1D("quantile=0.400000"), Content(ContinuousSchema[Double](), 5.233333)),
    Cell(Position1D("quantile=0.600000"), Content(ContinuousSchema[Double](), 7.326667)),
    Cell(Position1D("quantile=0.800000"), Content(ContinuousSchema[Double](), 9.42)))

  val result9 = List(Cell(Position1D("quantile=0.200000"), Content(ContinuousSchema[Double](), 3.2185)),
    Cell(Position1D("quantile=0.400000"), Content(ContinuousSchema[Double](), 5.2595)),
    Cell(Position1D("quantile=0.600000"), Content(ContinuousSchema[Double](), 7.3005)),
    Cell(Position1D("quantile=0.800000"), Content(ContinuousSchema[Double](), 9.3415)))

  val result10 = List(Cell(Position2D("col1", "quantile=0.200000"), Content(ContinuousSchema[Double](), 2.0)),
    Cell(Position2D("col1", "quantile=0.400000"), Content(ContinuousSchema[Double](), 4.0)),
    Cell(Position2D("col1", "quantile=0.600000"), Content(ContinuousSchema[Double](), 4.0)),
    Cell(Position2D("col1", "quantile=0.800000"), Content(ContinuousSchema[Double](), 4.0)),
    Cell(Position2D("col2", "quantile=0.200000"), Content(ContinuousSchema[Double](), 42.0)),
    Cell(Position2D("col2", "quantile=0.400000"), Content(ContinuousSchema[Double](), 42.0)),
    Cell(Position2D("col2", "quantile=0.600000"), Content(ContinuousSchema[Double](), 42.0)),
    Cell(Position2D("col2", "quantile=0.800000"), Content(ContinuousSchema[Double](), 42.0)))

  val result11 = List(Cell(Position2D("col1", "quantile=0.200000"), Content(ContinuousSchema[Double](), 2.0)),
    Cell(Position2D("col1", "quantile=0.400000"), Content(ContinuousSchema[Double](), 3.0)),
    Cell(Position2D("col1", "quantile=0.600000"), Content(ContinuousSchema[Double](), 4.0)),
    Cell(Position2D("col1", "quantile=0.800000"), Content(ContinuousSchema[Double](), 4.0)),
    Cell(Position2D("col2", "quantile=0.200000"), Content(ContinuousSchema[Double](), 42.0)),
    Cell(Position2D("col2", "quantile=0.400000"), Content(ContinuousSchema[Double](), 42.0)),
    Cell(Position2D("col2", "quantile=0.600000"), Content(ContinuousSchema[Double](), 42.0)),
    Cell(Position2D("col2", "quantile=0.800000"), Content(ContinuousSchema[Double](), 42.0)))

  val result12 = List(Cell(Position2D("col1", "quantile=0.200000"), Content(ContinuousSchema[Double](), 1.6)),
    Cell(Position2D("col1", "quantile=0.400000"), Content(ContinuousSchema[Double](), 3.2)),
    Cell(Position2D("col1", "quantile=0.600000"), Content(ContinuousSchema[Double](), 4.0)),
    Cell(Position2D("col1", "quantile=0.800000"), Content(ContinuousSchema[Double](), 4.0)),
    Cell(Position2D("col2", "quantile=0.200000"), Content(ContinuousSchema[Double](), 42.0)),
    Cell(Position2D("col2", "quantile=0.400000"), Content(ContinuousSchema[Double](), 42.0)),
    Cell(Position2D("col2", "quantile=0.600000"), Content(ContinuousSchema[Double](), 42.0)),
    Cell(Position2D("col2", "quantile=0.800000"), Content(ContinuousSchema[Double](), 42.0)))

  val result13 = List(Cell(Position2D("col1", "quantile=0.200000"), Content(ContinuousSchema[Double](), 2.1)),
    Cell(Position2D("col1", "quantile=0.400000"), Content(ContinuousSchema[Double](), 3.7)),
    Cell(Position2D("col1", "quantile=0.600000"), Content(ContinuousSchema[Double](), 4.0)),
    Cell(Position2D("col1", "quantile=0.800000"), Content(ContinuousSchema[Double](), 4.0)),
    Cell(Position2D("col2", "quantile=0.200000"), Content(ContinuousSchema[Double](), 42.0)),
    Cell(Position2D("col2", "quantile=0.400000"), Content(ContinuousSchema[Double](), 42.0)),
    Cell(Position2D("col2", "quantile=0.600000"), Content(ContinuousSchema[Double](), 42.0)),
    Cell(Position2D("col2", "quantile=0.800000"), Content(ContinuousSchema[Double](), 42.0)))

  val result14 = List(Cell(Position2D("col1", "quantile=0.200000"), Content(ContinuousSchema[Double](), 1.8)),
    Cell(Position2D("col1", "quantile=0.400000"), Content(ContinuousSchema[Double](), 3.6)),
    Cell(Position2D("col1", "quantile=0.600000"), Content(ContinuousSchema[Double](), 4.0)),
    Cell(Position2D("col1", "quantile=0.800000"), Content(ContinuousSchema[Double](), 4.2)),
    Cell(Position2D("col2", "quantile=0.200000"), Content(ContinuousSchema[Double](), 42.0)),
    Cell(Position2D("col2", "quantile=0.400000"), Content(ContinuousSchema[Double](), 42.0)),
    Cell(Position2D("col2", "quantile=0.600000"), Content(ContinuousSchema[Double](), 42.0)),
    Cell(Position2D("col2", "quantile=0.800000"), Content(ContinuousSchema[Double](), 42.0)))

  val result15 = List(Cell(Position2D("col1", "quantile=0.200000"), Content(ContinuousSchema[Double](), 2.4)),
    Cell(Position2D("col1", "quantile=0.400000"), Content(ContinuousSchema[Double](), 3.8)),
    Cell(Position2D("col1", "quantile=0.600000"), Content(ContinuousSchema[Double](), 4.0)),
    Cell(Position2D("col1", "quantile=0.800000"), Content(ContinuousSchema[Double](), 4.0)),
    Cell(Position2D("col2", "quantile=0.200000"), Content(ContinuousSchema[Double](), 42.0)),
    Cell(Position2D("col2", "quantile=0.400000"), Content(ContinuousSchema[Double](), 42.0)),
    Cell(Position2D("col2", "quantile=0.600000"), Content(ContinuousSchema[Double](), 42.0)),
    Cell(Position2D("col2", "quantile=0.800000"), Content(ContinuousSchema[Double](), 42.0)))

  val result16 = List(Cell(Position2D("col1", "quantile=0.200000"), Content(ContinuousSchema[Double](), 2.0)),
    Cell(Position2D("col1", "quantile=0.400000"), Content(ContinuousSchema[Double](), 3.666667)),
    Cell(Position2D("col1", "quantile=0.600000"), Content(ContinuousSchema[Double](), 4.0)),
    Cell(Position2D("col1", "quantile=0.800000"), Content(ContinuousSchema[Double](), 4.0)),
    Cell(Position2D("col2", "quantile=0.200000"), Content(ContinuousSchema[Double](), 42.0)),
    Cell(Position2D("col2", "quantile=0.400000"), Content(ContinuousSchema[Double](), 42.0)),
    Cell(Position2D("col2", "quantile=0.600000"), Content(ContinuousSchema[Double](), 42.0)),
    Cell(Position2D("col2", "quantile=0.800000"), Content(ContinuousSchema[Double](), 42.0)))

  val result17 = List(Cell(Position2D("col1", "quantile=0.200000"), Content(ContinuousSchema[Double](), 2.025)),
    Cell(Position2D("col1", "quantile=0.400000"), Content(ContinuousSchema[Double](), 3.675)),
    Cell(Position2D("col1", "quantile=0.600000"), Content(ContinuousSchema[Double](), 4.0)),
    Cell(Position2D("col1", "quantile=0.800000"), Content(ContinuousSchema[Double](), 4.0)),
    Cell(Position2D("col2", "quantile=0.200000"), Content(ContinuousSchema[Double](), 42.0)),
    Cell(Position2D("col2", "quantile=0.400000"), Content(ContinuousSchema[Double](), 42.0)),
    Cell(Position2D("col2", "quantile=0.600000"), Content(ContinuousSchema[Double](), 42.0)),
    Cell(Position2D("col2", "quantile=0.800000"), Content(ContinuousSchema[Double](), 42.0)))

  val result18 = List(Cell(Position1D("quantile=0.200000"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position1D("quantile=0.400000"), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position1D("quantile=0.600000"), Content(ContinuousSchema[Double](), 9.42)))
}

object TestQuantile {

  def name[S <: Position with ExpandablePosition] = {
    (pos: S, value: Double) => pos.append("quantile=%f".format(value)).toOption
  }
}

class TestScaldingQuantile extends TestQuantile {

  "A quantile" should "return its first along 1 value in 1D" in {
    toPipe(data1)
      .quantile(Along(First), probs, Quantile.Type1, TestQuantile.name[Position0D], true, true, Default())
      .toList.sortBy(_.position) shouldBe result1

    toPipe(data1)
      .quantile(Along(First), probs, Quantile.Type2, TestQuantile.name[Position0D], true, true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result1

    toPipe(data1)
      .quantile(Along(First), probs, Quantile.Type3, TestQuantile.name[Position0D], true, true, Default())
      .toList.sortBy(_.position) shouldBe result1

    toPipe(data1)
      .quantile(Along(First), probs, Quantile.Type4, TestQuantile.name[Position0D], true, true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result1

    toPipe(data1)
      .quantile(Along(First), probs, Quantile.Type5, TestQuantile.name[Position0D], true, true, Default())
      .toList.sortBy(_.position) shouldBe result1

    toPipe(data1)
      .quantile(Along(First), probs, Quantile.Type6, TestQuantile.name[Position0D], true, true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result1

    toPipe(data1)
      .quantile(Along(First), probs, Quantile.Type7, TestQuantile.name[Position0D], true, true, Default())
      .toList.sortBy(_.position) shouldBe result1

    toPipe(data1)
      .quantile(Along(First), probs, Quantile.Type8, TestQuantile.name[Position0D], true, true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result1

    toPipe(data1)
      .quantile(Along(First), probs, Quantile.Type9, TestQuantile.name[Position0D], true, true, Default())
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first along 3 values in 1D" in {
    toPipe(data2)
      .quantile(Along(First), probs, Quantile.Type1, TestQuantile.name[Position0D], true, true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result2

    toPipe(data2)
      .quantile(Along(First), probs, Quantile.Type2, TestQuantile.name[Position0D], true, true, Default())
      .toList.sortBy(_.position) shouldBe result2

    toPipe(data2)
      .quantile(Along(First), probs, Quantile.Type3, TestQuantile.name[Position0D], true, true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result3

    toPipe(data2)
      .quantile(Along(First), probs, Quantile.Type4, TestQuantile.name[Position0D], true, true, Default())
      .toList.sortBy(_.position) shouldBe result4

    toPipe(data2)
      .quantile(Along(First), probs, Quantile.Type5, TestQuantile.name[Position0D], true, true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result5

    toPipe(data2)
      .quantile(Along(First), probs, Quantile.Type6, TestQuantile.name[Position0D], true, true, Default())
      .toList.sortBy(_.position) shouldBe result6

    toPipe(data2)
      .quantile(Along(First), probs, Quantile.Type7, TestQuantile.name[Position0D], true, true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result7

    toPipe(data2)
      .quantile(Along(First), probs, Quantile.Type8, TestQuantile.name[Position0D], true, true, Default())
      .toList.sortBy(_.position) shouldBe result8

    toPipe(data2)
      .quantile(Along(First), probs, Quantile.Type9, TestQuantile.name[Position0D], true, true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its first along 3 equal values in 1D" in {
    toPipe(data3)
      .quantile(Along(First), probs, Quantile.Type1, TestQuantile.name[Position0D], true, true, Default())
      .toList.sortBy(_.position) shouldBe result1

    toPipe(data3)
      .quantile(Along(First), probs, Quantile.Type2, TestQuantile.name[Position0D], true, true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result1

    toPipe(data3)
      .quantile(Along(First), probs, Quantile.Type3, TestQuantile.name[Position0D], true, true, Default())
      .toList.sortBy(_.position) shouldBe result1

    toPipe(data3)
      .quantile(Along(First), probs, Quantile.Type4, TestQuantile.name[Position0D], true, true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result1

    toPipe(data3)
      .quantile(Along(First), probs, Quantile.Type5, TestQuantile.name[Position0D], true, true, Default())
      .toList.sortBy(_.position) shouldBe result1

    toPipe(data3)
      .quantile(Along(First), probs, Quantile.Type6, TestQuantile.name[Position0D], true, true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result1

    toPipe(data3)
      .quantile(Along(First), probs, Quantile.Type7, TestQuantile.name[Position0D], true, true, Default())
      .toList.sortBy(_.position) shouldBe result1

    toPipe(data3)
      .quantile(Along(First), probs, Quantile.Type8, TestQuantile.name[Position0D], true, true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result1

    toPipe(data3)
      .quantile(Along(First), probs, Quantile.Type9, TestQuantile.name[Position0D], true, true, Default())
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first along values in 2D" in {
    toPipe(data4)
      .quantile(Along(First), probs, Quantile.Type1, TestQuantile.name[Position1D], true, true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result10

    toPipe(data4)
      .quantile(Along(First), probs, Quantile.Type2, TestQuantile.name[Position1D], true, true, Default())
      .toList.sortBy(_.position) shouldBe result10

    toPipe(data4)
      .quantile(Along(First), probs, Quantile.Type3, TestQuantile.name[Position1D], true, true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result11

    toPipe(data4)
      .quantile(Along(First), probs, Quantile.Type4, TestQuantile.name[Position1D], true, true, Default())
      .toList.sortBy(_.position) shouldBe result12

    toPipe(data4)
      .quantile(Along(First), probs, Quantile.Type5, TestQuantile.name[Position1D], true, true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result13

    toPipe(data4)
      .quantile(Along(First), probs, Quantile.Type6, TestQuantile.name[Position1D], true, true, Default())
      .toList.sortBy(_.position) shouldBe result14

    toPipe(data4)
      .quantile(Along(First), probs, Quantile.Type7, TestQuantile.name[Position1D], true, true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result15

    toPipe(data4)
      .quantile(Along(First), probs, Quantile.Type8, TestQuantile.name[Position1D], true, true, Default())
      .toList.sortBy(_.position) shouldBe result16

    toPipe(data4)
      .quantile(Along(First), probs, Quantile.Type9, TestQuantile.name[Position1D], true, true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result17
  }

  it should "return its first over values in 2D" in {
    toPipe(data5)
      .quantile(Over(First), probs, Quantile.Type1, TestQuantile.name[Position1D], true, true, Default())
      .toList.sortBy(_.position) shouldBe result10

    toPipe(data5)
      .quantile(Over(First), probs, Quantile.Type2, TestQuantile.name[Position1D], true, true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result10

    toPipe(data5)
      .quantile(Over(First), probs, Quantile.Type3, TestQuantile.name[Position1D], true, true, Default())
      .toList.sortBy(_.position) shouldBe result11

    toPipe(data5)
      .quantile(Over(First), probs, Quantile.Type4, TestQuantile.name[Position1D], true, true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result12

    toPipe(data5)
      .quantile(Over(First), probs, Quantile.Type5, TestQuantile.name[Position1D], true, true, Default())
      .toList.sortBy(_.position) shouldBe result13

    toPipe(data5)
      .quantile(Over(First), probs, Quantile.Type6, TestQuantile.name[Position1D], true, true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result14

    toPipe(data5)
      .quantile(Over(First), probs, Quantile.Type7, TestQuantile.name[Position1D], true, true, Default())
      .toList.sortBy(_.position) shouldBe result15

    toPipe(data5)
      .quantile(Over(First), probs, Quantile.Type8, TestQuantile.name[Position1D], true, true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result16

    toPipe(data5)
      .quantile(Over(First), probs, Quantile.Type9, TestQuantile.name[Position1D], true, true, Default())
      .toList.sortBy(_.position) shouldBe result17
  }

  it should "return its second along values in 2D" in {
    toPipe(data5)
      .quantile(Along(Second), probs, Quantile.Type1, TestQuantile.name[Position1D], true, true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result10

    toPipe(data5)
      .quantile(Along(Second), probs, Quantile.Type2, TestQuantile.name[Position1D], true, true, Default())
      .toList.sortBy(_.position) shouldBe result10

    toPipe(data5)
      .quantile(Along(Second), probs, Quantile.Type3, TestQuantile.name[Position1D], true, true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result11

    toPipe(data5)
      .quantile(Along(Second), probs, Quantile.Type4, TestQuantile.name[Position1D], true, true, Default())
      .toList.sortBy(_.position) shouldBe result12

    toPipe(data5)
      .quantile(Along(Second), probs, Quantile.Type5, TestQuantile.name[Position1D], true, true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result13

    toPipe(data5)
      .quantile(Along(Second), probs, Quantile.Type6, TestQuantile.name[Position1D], true, true, Default())
      .toList.sortBy(_.position) shouldBe result14

    toPipe(data5)
      .quantile(Along(Second), probs, Quantile.Type7, TestQuantile.name[Position1D], true, true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result15

    toPipe(data5)
      .quantile(Along(Second), probs, Quantile.Type8, TestQuantile.name[Position1D], true, true, Default())
      .toList.sortBy(_.position) shouldBe result16

    toPipe(data5)
      .quantile(Along(Second), probs, Quantile.Type9, TestQuantile.name[Position1D], true, true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result17
  }

  it should "return its second over values in 2D" in {
    toPipe(data4)
      .quantile(Over(Second), probs, Quantile.Type1, TestQuantile.name[Position1D], true, true, Default())
      .toList.sortBy(_.position) shouldBe result10

    toPipe(data4)
      .quantile(Over(Second), probs, Quantile.Type2, TestQuantile.name[Position1D], true, true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result10

    toPipe(data4)
      .quantile(Over(Second), probs, Quantile.Type3, TestQuantile.name[Position1D], true, true, Default())
      .toList.sortBy(_.position) shouldBe result11

    toPipe(data4)
      .quantile(Over(Second), probs, Quantile.Type4, TestQuantile.name[Position1D], true, true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result12

    toPipe(data4)
      .quantile(Over(Second), probs, Quantile.Type5, TestQuantile.name[Position1D], true, true, Default())
      .toList.sortBy(_.position) shouldBe result13

    toPipe(data4)
      .quantile(Over(Second), probs, Quantile.Type6, TestQuantile.name[Position1D], true, true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result14

    toPipe(data4)
      .quantile(Over(Second), probs, Quantile.Type7, TestQuantile.name[Position1D], true, true, Default())
      .toList.sortBy(_.position) shouldBe result15

    toPipe(data4)
      .quantile(Over(Second), probs, Quantile.Type8, TestQuantile.name[Position1D], true, true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result16

    toPipe(data4)
      .quantile(Over(Second), probs, Quantile.Type9, TestQuantile.name[Position1D], true, true, Default())
      .toList.sortBy(_.position) shouldBe result17
  }

  it should "return with non-numeric data" in {
    val res1 = toPipe(data6)
      .quantile(Along(First), probs, Quantile.Type1, TestQuantile.name[Position0D], false, true, Default(Reducers(12)))
      .toList.sortBy(_.position)
    res1(0) shouldBe result18(0)
    res1(1) shouldBe result18(1)
    res1(2) shouldBe result18(2)
    res1(3).position shouldBe Position1D("quantile=0.800000")
    res1(3).content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Some(0)

    toPipe(data6)
      .quantile(Along(First), probs, Quantile.Type1, TestQuantile.name[Position0D], false, false, Default())
      .toList.sortBy(_.position) shouldBe result18
  }
}

class TestSparkQuantile extends TestQuantile {

  "A quantile" should "return its first along 1 value in 1D" in {
    toRDD(data1)
      .quantile(Along(First), probs, Quantile.Type1, TestQuantile.name[Position0D], true, true, Default())
      .toList.sortBy(_.position) shouldBe result1

    toRDD(data1)
      .quantile(Along(First), probs, Quantile.Type2, TestQuantile.name[Position0D], true, true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result1

    toRDD(data1)
      .quantile(Along(First), probs, Quantile.Type3, TestQuantile.name[Position0D], true, true, Default())
      .toList.sortBy(_.position) shouldBe result1

    toRDD(data1)
      .quantile(Along(First), probs, Quantile.Type4, TestQuantile.name[Position0D], true, true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result1

    toRDD(data1)
      .quantile(Along(First), probs, Quantile.Type5, TestQuantile.name[Position0D], true, true, Default())
      .toList.sortBy(_.position) shouldBe result1

    toRDD(data1)
      .quantile(Along(First), probs, Quantile.Type6, TestQuantile.name[Position0D], true, true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result1

    toRDD(data1)
      .quantile(Along(First), probs, Quantile.Type7, TestQuantile.name[Position0D], true, true, Default())
      .toList.sortBy(_.position) shouldBe result1

    toRDD(data1)
      .quantile(Along(First), probs, Quantile.Type8, TestQuantile.name[Position0D], true, true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result1

    toRDD(data1)
      .quantile(Along(First), probs, Quantile.Type9, TestQuantile.name[Position0D], true, true, Default())
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first along 3 values in 1D" in {
    toRDD(data2)
      .quantile(Along(First), probs, Quantile.Type1, TestQuantile.name[Position0D], true, true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result2

    toRDD(data2)
      .quantile(Along(First), probs, Quantile.Type2, TestQuantile.name[Position0D], true, true, Default())
      .toList.sortBy(_.position) shouldBe result2

    toRDD(data2)
      .quantile(Along(First), probs, Quantile.Type3, TestQuantile.name[Position0D], true, true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result3

    toRDD(data2)
      .quantile(Along(First), probs, Quantile.Type4, TestQuantile.name[Position0D], true, true, Default())
      .toList.sortBy(_.position) shouldBe result4

    toRDD(data2)
      .quantile(Along(First), probs, Quantile.Type5, TestQuantile.name[Position0D], true, true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result5

    toRDD(data2)
      .quantile(Along(First), probs, Quantile.Type6, TestQuantile.name[Position0D], true, true, Default())
      .toList.sortBy(_.position) shouldBe result6

    toRDD(data2)
      .quantile(Along(First), probs, Quantile.Type7, TestQuantile.name[Position0D], true, true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result7

    toRDD(data2)
      .quantile(Along(First), probs, Quantile.Type8, TestQuantile.name[Position0D], true, true, Default())
      .toList.sortBy(_.position) shouldBe result8

    toRDD(data2)
      .quantile(Along(First), probs, Quantile.Type9, TestQuantile.name[Position0D], true, true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its first along 3 equal values in 1D" in {
    toRDD(data3)
      .quantile(Along(First), probs, Quantile.Type1, TestQuantile.name[Position0D], true, true, Default())
      .toList.sortBy(_.position) shouldBe result1

    toRDD(data3)
      .quantile(Along(First), probs, Quantile.Type2, TestQuantile.name[Position0D], true, true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result1

    toRDD(data3)
      .quantile(Along(First), probs, Quantile.Type3, TestQuantile.name[Position0D], true, true, Default())
      .toList.sortBy(_.position) shouldBe result1

    toRDD(data3)
      .quantile(Along(First), probs, Quantile.Type4, TestQuantile.name[Position0D], true, true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result1

    toRDD(data3)
      .quantile(Along(First), probs, Quantile.Type5, TestQuantile.name[Position0D], true, true, Default())
      .toList.sortBy(_.position) shouldBe result1

    toRDD(data3)
      .quantile(Along(First), probs, Quantile.Type6, TestQuantile.name[Position0D], true, true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result1

    toRDD(data3)
      .quantile(Along(First), probs, Quantile.Type7, TestQuantile.name[Position0D], true, true, Default())
      .toList.sortBy(_.position) shouldBe result1

    toRDD(data3)
      .quantile(Along(First), probs, Quantile.Type8, TestQuantile.name[Position0D], true, true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result1

    toRDD(data3)
      .quantile(Along(First), probs, Quantile.Type9, TestQuantile.name[Position0D], true, true, Default())
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first along values in 2D" in {
    toRDD(data4)
      .quantile(Along(First), probs, Quantile.Type1, TestQuantile.name[Position1D], true, true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result10

    toRDD(data4)
      .quantile(Along(First), probs, Quantile.Type2, TestQuantile.name[Position1D], true, true, Default())
      .toList.sortBy(_.position) shouldBe result10

    toRDD(data4)
      .quantile(Along(First), probs, Quantile.Type3, TestQuantile.name[Position1D], true, true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result11

    toRDD(data4)
      .quantile(Along(First), probs, Quantile.Type4, TestQuantile.name[Position1D], true, true, Default())
      .toList.sortBy(_.position) shouldBe result12

    toRDD(data4)
      .quantile(Along(First), probs, Quantile.Type5, TestQuantile.name[Position1D], true, true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result13

    toRDD(data4)
      .quantile(Along(First), probs, Quantile.Type6, TestQuantile.name[Position1D], true, true, Default())
      .toList.sortBy(_.position) shouldBe result14

    toRDD(data4)
      .quantile(Along(First), probs, Quantile.Type7, TestQuantile.name[Position1D], true, true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result15

    toRDD(data4)
      .quantile(Along(First), probs, Quantile.Type8, TestQuantile.name[Position1D], true, true, Default())
      .toList.sortBy(_.position) shouldBe result16

    toRDD(data4)
      .quantile(Along(First), probs, Quantile.Type9, TestQuantile.name[Position1D], true, true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result17
  }

  it should "return its first over values in 2D" in {
    toRDD(data5)
      .quantile(Over(First), probs, Quantile.Type1, TestQuantile.name[Position1D], true, true, Default())
      .toList.sortBy(_.position) shouldBe result10

    toRDD(data5)
      .quantile(Over(First), probs, Quantile.Type2, TestQuantile.name[Position1D], true, true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result10

    toRDD(data5)
      .quantile(Over(First), probs, Quantile.Type3, TestQuantile.name[Position1D], true, true, Default())
      .toList.sortBy(_.position) shouldBe result11

    toRDD(data5)
      .quantile(Over(First), probs, Quantile.Type4, TestQuantile.name[Position1D], true, true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result12

    toRDD(data5)
      .quantile(Over(First), probs, Quantile.Type5, TestQuantile.name[Position1D], true, true, Default())
      .toList.sortBy(_.position) shouldBe result13

    toRDD(data5)
      .quantile(Over(First), probs, Quantile.Type6, TestQuantile.name[Position1D], true, true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result14

    toRDD(data5)
      .quantile(Over(First), probs, Quantile.Type7, TestQuantile.name[Position1D], true, true, Default())
      .toList.sortBy(_.position) shouldBe result15

    toRDD(data5)
      .quantile(Over(First), probs, Quantile.Type8, TestQuantile.name[Position1D], true, true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result16

    toRDD(data5)
      .quantile(Over(First), probs, Quantile.Type9, TestQuantile.name[Position1D], true, true, Default())
      .toList.sortBy(_.position) shouldBe result17
  }

  it should "return its second along values in 2D" in {
    toRDD(data5)
      .quantile(Along(Second), probs, Quantile.Type1, TestQuantile.name[Position1D], true, true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result10

    toRDD(data5)
      .quantile(Along(Second), probs, Quantile.Type2, TestQuantile.name[Position1D], true, true, Default())
      .toList.sortBy(_.position) shouldBe result10

    toRDD(data5)
      .quantile(Along(Second), probs, Quantile.Type3, TestQuantile.name[Position1D], true, true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result11

    toRDD(data5)
      .quantile(Along(Second), probs, Quantile.Type4, TestQuantile.name[Position1D], true, true, Default())
      .toList.sortBy(_.position) shouldBe result12

    toRDD(data5)
      .quantile(Along(Second), probs, Quantile.Type5, TestQuantile.name[Position1D], true, true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result13

    toRDD(data5)
      .quantile(Along(Second), probs, Quantile.Type6, TestQuantile.name[Position1D], true, true, Default())
      .toList.sortBy(_.position) shouldBe result14

    toRDD(data5)
      .quantile(Along(Second), probs, Quantile.Type7, TestQuantile.name[Position1D], true, true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result15

    toRDD(data5)
      .quantile(Along(Second), probs, Quantile.Type8, TestQuantile.name[Position1D], true, true, Default())
      .toList.sortBy(_.position) shouldBe result16

    toRDD(data5)
      .quantile(Along(Second), probs, Quantile.Type9, TestQuantile.name[Position1D], true, true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result17
  }

  it should "return its second over values in 2D" in {
    toRDD(data4)
      .quantile(Over(Second), probs, Quantile.Type1, TestQuantile.name[Position1D], true, true, Default())
      .toList.sortBy(_.position) shouldBe result10

    toRDD(data4)
      .quantile(Over(Second), probs, Quantile.Type2, TestQuantile.name[Position1D], true, true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result10

    toRDD(data4)
      .quantile(Over(Second), probs, Quantile.Type3, TestQuantile.name[Position1D], true, true, Default())
      .toList.sortBy(_.position) shouldBe result11

    toRDD(data4)
      .quantile(Over(Second), probs, Quantile.Type4, TestQuantile.name[Position1D], true, true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result12

    toRDD(data4)
      .quantile(Over(Second), probs, Quantile.Type5, TestQuantile.name[Position1D], true, true, Default())
      .toList.sortBy(_.position) shouldBe result13

    toRDD(data4)
      .quantile(Over(Second), probs, Quantile.Type6, TestQuantile.name[Position1D], true, true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result14

    toRDD(data4)
      .quantile(Over(Second), probs, Quantile.Type7, TestQuantile.name[Position1D], true, true, Default())
      .toList.sortBy(_.position) shouldBe result15

    toRDD(data4)
      .quantile(Over(Second), probs, Quantile.Type8, TestQuantile.name[Position1D], true, true, Default(Reducers(12)))
      .toList.sortBy(_.position) shouldBe result16

    toRDD(data4)
      .quantile(Over(Second), probs, Quantile.Type9, TestQuantile.name[Position1D], true, true, Default())
      .toList.sortBy(_.position) shouldBe result17
  }

  it should "return with non-numeric data" in {
    val res1 = toRDD(data6)
      .quantile(Along(First), probs, Quantile.Type1, TestQuantile.name[Position0D], false, true, Default(Reducers(12)))
      .toList.sortBy(_.position)
    res1(0) shouldBe result18(0)
    res1(1) shouldBe result18(1)
    res1(2) shouldBe result18(2)
    res1(3).position shouldBe Position1D("quantile=0.800000")
    res1(3).content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Some(0)

    toRDD(data6)
      .quantile(Along(First), probs, Quantile.Type1, TestQuantile.name[Position0D], false, false, Default())
      .toList.sortBy(_.position) shouldBe result18
  }
}

