// Copyright 2015,2016 Commonwealth Bank of Australia
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
import au.com.cba.omnia.grimlock.framework.position._

class TestExtractWithKey extends TestGrimlock {

  val cell = Cell(Position2D("abc", "def"), Content(ContinuousSchema[Double](), 1.0))
  val ext = Map(Position1D("ghi") -> 3.14)

  "A ExtractWithKey" should "extract with key" in {
    ExtractWithKey[Position2D, Double]("ghi").extract(cell, ext) shouldBe (Some(3.14))
  }

  it should "extract with missing key" in {
    ExtractWithKey[Position2D, Double]("jkl").extract(cell, ext) shouldBe (None)
  }

  it should "extract and present" in {
    ExtractWithKey[Position2D, Double]("ghi")
      .andThenPresent((d: Double) => Some(d * 2)).extract(cell, ext) shouldBe (Some(6.28))
  }
}

class TestExtractWithDimension extends TestGrimlock {

  val cell = Cell(Position2D("abc", "def"), Content(ContinuousSchema[Double](), 1.0))
  val ext = Map(Position1D("abc") -> 3.14)

  "A ExtractWithDimension" should "extract with First" in {
    ExtractWithDimension[Position2D, Double](First).extract(cell, ext) shouldBe (Some(3.14))
  }

  it should "extract with Second" in {
    ExtractWithDimension[Position2D, Double](Second).extract(cell, ext) shouldBe (None)
  }

  it should "extract and present" in {
    ExtractWithDimension[Position2D, Double](First)
      .andThenPresent((d: Double) => Some(d * 2)).extract(cell, ext) shouldBe (Some(6.28))
  }
}

class TestExtractWithDimensionAndKey extends TestGrimlock {

  val cell = Cell(Position2D("abc", "def"), Content(ContinuousSchema[Double](), 1.0))
  val ext = Map(Position1D("abc") -> Map(Position1D(123) -> 3.14))

  "A ExtractWithDimensionAndKey" should "extract with First" in {
    ExtractWithDimensionAndKey[Position2D, Double](First, 123)
      .extract(cell, ext) shouldBe (Some(3.14))
  }

  it should "extract with missing key" in {
    ExtractWithDimensionAndKey[Position2D, Double](First, 456)
      .extract(cell, ext) shouldBe (None)
  }

  it should "extract with Second" in {
    ExtractWithDimensionAndKey[Position2D, Double](Second, 123)
      .extract(cell, ext) shouldBe (None)
  }

  it should "extract with Second and missing key" in {
    ExtractWithDimensionAndKey[Position2D, Double](Second, 456)
      .extract(cell, ext) shouldBe (None)
  }

  it should "extract and present" in {
    ExtractWithDimensionAndKey[Position2D, Double](First, 123)
      .andThenPresent((d: Double) => Some(d * 2)).extract(cell, ext) shouldBe (Some(6.28))
  }
}

class TestExtractWithPosition extends TestGrimlock {

  val cell1 = Cell(Position2D("abc", "def"), Content(ContinuousSchema[Double](), 1.0))
  val cell2 = Cell(Position2D("cba", "fed"), Content(ContinuousSchema[Double](), 1.0))
  val ext = Map(Position2D("abc", "def") -> 3.14)

  "A ExtractWithPosition" should "extract with key" in {
    ExtractWithPosition[Position2D, Double]().extract(cell1, ext) shouldBe (Some(3.14))
  }

  it should "extract with missing position" in {
    ExtractWithPosition[Position2D, Double]().extract(cell2, ext) shouldBe (None)
  }

  it should "extract and present" in {
    ExtractWithPosition[Position2D, Double]()
      .andThenPresent((d: Double) => Some(d * 2)).extract(cell1, ext) shouldBe (Some(6.28))
  }
}

class TestExtractWithPositionAndKey extends TestGrimlock {

  val cell1 = Cell(Position2D("abc", "def"), Content(ContinuousSchema[Double](), 1.0))
  val cell2 = Cell(Position2D("cba", "fed"), Content(ContinuousSchema[Double](), 1.0))
  val ext = Map(Position2D("abc", "def") -> Map(Position1D("xyz") -> 3.14))

  "A ExtractWithPositionAndKey" should "extract with key" in {
    ExtractWithPositionAndKey[Position2D, Double]("xyz").extract(cell1, ext) shouldBe (Some(3.14))
  }

  it should "extract with missing position" in {
    ExtractWithPositionAndKey[Position2D, Double]("xyz").extract(cell2, ext) shouldBe (None)
  }

  it should "extract with missing key" in {
    ExtractWithPositionAndKey[Position2D, Double]("abc").extract(cell1, ext) shouldBe (None)
  }

  it should "extract with missing position and key" in {
    ExtractWithPositionAndKey[Position2D, Double]("abc").extract(cell2, ext) shouldBe (None)
  }

  it should "extract and present" in {
    ExtractWithPositionAndKey[Position2D, Double]("xyz")
      .andThenPresent((d: Double) => Some(d * 2)).extract(cell1, ext) shouldBe (Some(6.28))
  }
}

class TestExtractWithSelected extends TestGrimlock {

  val cell = Cell(Position2D("abc", "def"), Content(ContinuousSchema[Double](), 1.0))
  val ext = Map(Position1D("abc") -> 3.14)

  "A ExtractWithSelected" should "extract with Over" in {
    ExtractWithSelected[Position2D, Double](Over(First))
      .extract(cell, ext) shouldBe (Some(3.14))
  }

  it should "extract with Along" in {
    ExtractWithSelected[Position2D, Double](Along(First))
      .extract(cell, ext) shouldBe (None)
  }

  it should "extract and present" in {
    ExtractWithSelected[Position2D, Double](Over(First))
      .andThenPresent((d: Double) => Some(d * 2)).extract(cell, ext) shouldBe (Some(6.28))
  }
}

class TestExtractWithSelectedAndKey extends TestGrimlock {

  val cell = Cell(Position2D("abc", "def"), Content(ContinuousSchema[Double](), 1.0))
  val ext = Map(Position1D("abc") -> Map(Position1D("xyz") -> 3.14))

  "A ExtractWithSelectedAndKey" should "extract with Over" in {
    ExtractWithSelectedAndKey[Position2D, Double](Over(First), "xyz")
      .extract(cell, ext) shouldBe (Some(3.14))
  }

  it should "extract with Along" in {
    ExtractWithSelectedAndKey[Position2D, Double](Along(First), "xyz")
      .extract(cell, ext) shouldBe (None)
  }

  it should "extract with missing key" in {
    ExtractWithSelectedAndKey[Position2D, Double](Over(First), "abc")
      .extract(cell, ext) shouldBe (None)
  }

  it should "extract with Along and missing key" in {
    ExtractWithSelectedAndKey[Position2D, Double](Along(First), "abc")
      .extract(cell, ext) shouldBe (None)
  }

  it should "extract and present" in {
    ExtractWithSelectedAndKey[Position2D, Double](Over(First), "xyz")
      .andThenPresent((d: Double) => Some(d * 2)).extract(cell, ext) shouldBe (Some(6.28))
  }
}

class TestExtractWithSlice extends TestGrimlock {

  val cell1 = Cell(Position2D("abc", "def"), Content(ContinuousSchema[Double](), 1.0))
  val cell2 = Cell(Position2D("cba", "def"), Content(ContinuousSchema[Double](), 1.0))
  val cell3 = Cell(Position2D("abc", "fed"), Content(ContinuousSchema[Double](), 1.0))
  val ext = Map(Position1D("abc") -> Map(Position1D("def") -> 3.14))

  "A ExtractWithSlice" should "extract with Over" in {
    ExtractWithSlice[Position2D, Double](Over(First))
      .extract(cell1, ext) shouldBe (Some(3.14))
  }

  it should "extract with Along" in {
    ExtractWithSlice[Position2D, Double](Along(First))
      .extract(cell1, ext) shouldBe (None)
  }

  it should "extract with missing selected" in {
    ExtractWithSlice[Position2D, Double](Over(First))
      .extract(cell2, ext) shouldBe (None)
  }

  it should "extract with missing remaider" in {
    ExtractWithSlice[Position2D, Double](Over(First))
      .extract(cell3, ext) shouldBe (None)
  }

  it should "extract and present" in {
    ExtractWithSlice[Position2D, Double](Over(First))
      .andThenPresent((d: Double) => Some(d * 2)).extract(cell1, ext) shouldBe (Some(6.28))
  }
}

