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
import au.com.cba.omnia.grimlock.framework.sample._

import au.com.cba.omnia.grimlock.library.sample._

import scala.util._

trait TestSample extends TestGrimlock {

  val con = Content(ContinuousSchema[Codex.DoubleCodex](), 3.14)

  def toCell[P <: Position](pos: P): Cell[P] = Cell(pos, con)
}

class TestRandomSample extends TestSample {

  "A RandomSample" should "select 25% correctly" in {
    val obj = RandomSample[Position1D](0.25, new Random(123))

    (1 to 10000).map { case i => if (obj.select(toCell(Position1D(i)))) 1 else 0 }.sum shouldBe 2500 +- 50
  }

  it should "select 50% correctly" in {
    val obj = RandomSample[Position1D](0.5, new Random(123))

    (1 to 10000).map { case i => if (obj.select(toCell(Position1D(i)))) 1 else 0 }.sum shouldBe 5000 +- 50
  }

  it should "select 75% correctly" in {
    val obj = RandomSample[Position1D](0.75, new Random(123))

    (1 to 10000).map { case i => if (obj.select(toCell(Position1D(i)))) 1 else 0 }.sum shouldBe 7500 +- 50
  }
}

class TestHashSample extends TestSample {

  "A HashSample" should "select 25% correctly" in {
    val obj = HashSample[Position2D](Second, 1, 4)

    (1 to 10000).map { case i => if (obj.select(toCell(Position2D(2 * i, i)))) 1 else 0 }.sum shouldBe 2500 +- 50
  }

  it should "select 50% correctly" in {
    val obj = HashSample[Position2D](Second, 5, 10)

    (1 to 10000).map { case i => if (obj.select(toCell(Position2D(2 * i, i)))) 1 else 0 }.sum shouldBe 5000 +- 50
  }

  it should "select 75% correctly" in {
    val obj = HashSample[Position2D](Second, 75, 100)

    (1 to 10000).map { case i => if (obj.select(toCell(Position2D(2 * i, i)))) 1 else 0 }.sum shouldBe 7500 +- 50
  }
}

class TestHashSampleToSize extends TestSample {

  "A HashSampleToSize" should "select 25% correctly" in {
    val obj = HashSampleToSize(Second,
      ExtractWithKey[Position2D, Content](Second.toString).andThenPresent(_.value.asDouble), 2500)
    val ext = Map(Position1D(Second.toString) -> Content(DiscreteSchema[Codex.LongCodex](), 10000))

    (1 to 10000).map {
      case i => if (obj.selectWithValue(toCell(Position2D(2 * i, i)), ext)) 1 else 0
    }.sum shouldBe 2500 +- 50
  }

  it should "select 50% correctly" in {
    val obj = HashSampleToSize(Second,
      ExtractWithKey[Position2D, Content](Second.toString).andThenPresent(_.value.asDouble), 5000)
    val ext = Map(Position1D(Second.toString) -> Content(DiscreteSchema[Codex.LongCodex](), 10000))

    (1 to 10000).map {
      case i => if (obj.selectWithValue(toCell(Position2D(2 * i, i)), ext)) 1 else 0
    }.sum shouldBe 5000 +- 50
  }

  it should "select 75% correctly" in {
    val obj = HashSampleToSize(Second,
      ExtractWithKey[Position2D, Content](Second.toString).andThenPresent(_.value.asDouble), 7500)
    val ext = Map(Position1D(Second.toString) -> Content(DiscreteSchema[Codex.LongCodex](), 10000))

    (1 to 10000).map {
      case i => if (obj.selectWithValue(toCell(Position2D(2 * i, i)), ext)) 1 else 0
    }.sum shouldBe 7500 +- 50
  }
}

class TestAndThenSampler extends TestSample {

  "A AndThenSampler" should "select correctly" in {
    val obj = HashSample[Position2D](Second, 1, 4).andThen(HashSample(First, 1, 4))
    val res = (1 to 10000).flatMap { case i => if (obj.select(toCell(Position2D(i, i)))) Some((i,i)) else None }

    res.map(_._1).distinct.length shouldBe 2500 +- 50
    res.map(_._2).distinct.length shouldBe 2500 +- 50
  }
}

class TestAndThenSamplerWithValue extends TestSample {

  "A AndThenSamplerWithValue" should "select correctly" in {
    val obj = HashSampleToSize(Second,
      ExtractWithKey[Position2D, Content](Second.toString).andThenPresent(_.value.asDouble), 2500)
      .andThenWithValue(HashSample(First, 1, 4))
    val ext = Map(Position1D(Second.toString) -> Content(DiscreteSchema[Codex.LongCodex](), 10000))
    val res = (1 to 10000).flatMap {
      case i => if (obj.selectWithValue(toCell(Position2D(i, i)), ext)) Some((i,i)) else None
    }

    res.map(_._1).distinct.length shouldBe 625 +- 50
    res.map(_._2).distinct.length shouldBe 625 +- 50
  }
}

