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
import au.com.cba.omnia.grimlock.framework.position._

class TestExtractWithDimension extends TestGrimlock {

  val cell = Cell(Position2D("abc", "def"), Content(ContinuousSchema[Codex.DoubleCodex](), 1))
  val ext = Map(Position1D("abc") -> 3.14)

  "A ExtractWithDimension" should "extract with First" in {
    ExtractWithDimension[Dimension.First, Position2D, Double](First).extract(cell, ext) shouldBe (Some(3.14))
  }

  it should "extract with Second" in {
    ExtractWithDimension[Dimension.Second, Position2D, Double](Second).extract(cell, ext) shouldBe (None)
  }

  it should "extract and present" in {
    ExtractWithDimension[Dimension.First, Position2D, Double](First)
      .andThenPresent((d: Double) => Some(d * 2)).extract(cell, ext) shouldBe (Some(6.28))
  }
}

class TestExtractWithKey extends TestGrimlock {

  val cell = Cell(Position2D("abc", "def"), Content(ContinuousSchema[Codex.DoubleCodex](), 1))
  val ext = Map(Position1D("ghi") -> 3.14)

  "A ExtractWithKey" should "extract with key" in {
    ExtractWithKey[Position2D, String, Double]("ghi").extract(cell, ext) shouldBe (Some(3.14))
  }

  it should "extract with missing key" in {
    ExtractWithKey[Position2D, String, Double]("jkl").extract(cell, ext) shouldBe (None)
  }

  it should "extract and present" in {
    ExtractWithKey[Position2D, String, Double]("ghi")
      .andThenPresent((d: Double) => Some(d * 2)).extract(cell, ext) shouldBe (Some(6.28))
  }
}

class TestExtractWithPosition extends TestGrimlock {

  val cell1 = Cell(Position2D("abc", "def"), Content(ContinuousSchema[Codex.DoubleCodex](), 1))
  val cell2 = Cell(Position2D("cba", "fed"), Content(ContinuousSchema[Codex.DoubleCodex](), 1))
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

class TestExtractWithDimensionAndKey extends TestGrimlock {

  val cell = Cell(Position2D("abc", "def"), Content(ContinuousSchema[Codex.DoubleCodex](), 1))
  val ext = Map(Position1D("abc") -> Map(Position1D(123) -> 3.14))

  "A ExtractWithDimensionAndKey" should "extract with First" in {
    ExtractWithDimensionAndKey[Dimension.First, Position2D, Int, Double](First, 123)
      .extract(cell, ext) shouldBe (Some(3.14))
  }

  "A ExtractWithDimensionAndKey" should "extract with missing key" in {
    ExtractWithDimensionAndKey[Dimension.First, Position2D, Int, Double](First, 456)
      .extract(cell, ext) shouldBe (None)
  }

  it should "extract with Second" in {
    ExtractWithDimensionAndKey[Dimension.Second, Position2D, Int, Double](Second, 123)
      .extract(cell, ext) shouldBe (None)
  }

  it should "extract with Second and missing key" in {
    ExtractWithDimensionAndKey[Dimension.Second, Position2D, Int, Double](Second, 456)
      .extract(cell, ext) shouldBe (None)
  }

  it should "extract and present" in {
    ExtractWithDimensionAndKey[Dimension.First, Position2D, Int, Double](First, 123)
      .andThenPresent((d: Double) => Some(d * 2)).extract(cell, ext) shouldBe (Some(6.28))
  }
}

class TestExtractWithSelected extends TestGrimlock {

  val cell = Cell(Position2D("abc", "def"), Content(ContinuousSchema[Codex.DoubleCodex](), 1))
  val ext = Map(Position1D("abc") -> 3.14)

  "A ExtractWithSelected" should "extract with Over" in {
    ExtractWithSelected[Dimension.First, Position2D, Over[Position2D, Dimension.First], Double](Over(First))
      .extract(cell, ext) shouldBe (Some(3.14))
  }

  it should "extract with Along" in {
    ExtractWithSelected[Dimension.First, Position2D, Along[Position2D, Dimension.First], Double](Along(First))
      .extract(cell, ext) shouldBe (None)
  }

  it should "extract and present" in {
    ExtractWithSelected[Dimension.First, Position2D, Over[Position2D, Dimension.First], Double](Over(First))
      .andThenPresent((d: Double) => Some(d * 2)).extract(cell, ext) shouldBe (Some(6.28))
  }
}

class TestExtractWithSlice extends TestGrimlock {

  val cell1 = Cell(Position2D("abc", "def"), Content(ContinuousSchema[Codex.DoubleCodex](), 1))
  val cell2 = Cell(Position2D("cba", "def"), Content(ContinuousSchema[Codex.DoubleCodex](), 1))
  val cell3 = Cell(Position2D("abc", "fed"), Content(ContinuousSchema[Codex.DoubleCodex](), 1))
  val ext = Map(Position1D("abc") -> Map(Position1D("def") -> 3.14))

  "A ExtractWithSlice" should "extract with Over" in {
    ExtractWithSlice[Dimension.First, Position2D, Over[Position2D, Dimension.First], Double](Over(First))
      .extract(cell1, ext) shouldBe (Some(3.14))
  }

  it should "extract with Along" in {
    ExtractWithSlice[Dimension.First, Position2D, Along[Position2D, Dimension.First], Double](Along(First))
      .extract(cell1, ext) shouldBe (None)
  }

  it should "extract with missing selected" in {
    ExtractWithSlice[Dimension.First, Position2D, Over[Position2D, Dimension.First], Double](Over(First))
      .extract(cell2, ext) shouldBe (None)
  }

  it should "extract with missing remaider" in {
    ExtractWithSlice[Dimension.First, Position2D, Over[Position2D, Dimension.First], Double](Over(First))
      .extract(cell3, ext) shouldBe (None)
  }

  it should "extract and present" in {
    ExtractWithSlice[Dimension.First, Position2D, Over[Position2D, Dimension.First], Double](Over(First))
      .andThenPresent((d: Double) => Some(d * 2)).extract(cell1, ext) shouldBe (Some(6.28))
  }
}

class TestExtractWithSliceAndKey extends TestGrimlock {

  val cell1 = Cell(Position2D("abc", "def"), Content(ContinuousSchema[Codex.DoubleCodex](), 1))
  val cell2 = Cell(Position2D("cba", "def"), Content(ContinuousSchema[Codex.DoubleCodex](), 1))
  val ext = Map(Position1D("abc") -> Map(Position1D(123) -> 3.14))

  "A ExtractWithSliceAndKey" should "extract with Over" in {
    ExtractWithSliceAndKey[Dimension.First, Position2D, Over[Position2D, Dimension.First], Int, Double](Over(First),
      123).extract(cell1, ext) shouldBe (Some(3.14))
  }

  it should "extract with Along" in {
    ExtractWithSliceAndKey[Dimension.First, Position2D, Along[Position2D, Dimension.First], Int, Double](Along(First),
      123).extract(cell1, ext) shouldBe (None)
  }

  it should "extract with missing selected" in {
    ExtractWithSliceAndKey[Dimension.First, Position2D, Over[Position2D, Dimension.First], Int, Double](Over(First),
      123).extract(cell2, ext) shouldBe (None)
  }

  it should "extract with missing key" in {
    ExtractWithSliceAndKey[Dimension.First, Position2D, Over[Position2D, Dimension.First], Int, Double](Over(First),
      456).extract(cell1, ext) shouldBe (None)
  }

  it should "extract and present" in {
    ExtractWithSliceAndKey[Dimension.First, Position2D, Over[Position2D, Dimension.First], Int, Double](Over(First),
      123).andThenPresent((d: Double) => Some(d * 2)).extract(cell1, ext) shouldBe (Some(6.28))
  }
}

