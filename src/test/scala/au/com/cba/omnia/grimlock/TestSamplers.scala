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
import au.com.cba.omnia.grimlock.encoding._
import au.com.cba.omnia.grimlock.position._
import au.com.cba.omnia.grimlock.sample._

import org.scalatest._

import scala.util._

class TestRandomSample extends FlatSpec with Matchers {

  "A RandomSample" should "select 25% correctly" in {
    val obj = RandomSample(0.25, new Random(123))

    (1 to 10000).map { case i => if (obj.select(Position1D(i))) 1 else 0 }.sum should be (2500 +- 50)
  }

  it should "select 50% correctly" in {
    val obj = RandomSample(0.5, new Random(123))

    (1 to 10000).map { case i => if (obj.select(Position1D(i))) 1 else 0 }.sum should be (5000 +- 50)
  }

  it should "select 75% correctly" in {
    val obj = RandomSample(0.75, new Random(123))

    (1 to 10000).map { case i => if (obj.select(Position1D(i))) 1 else 0 }.sum should be (7500 +- 50)
  }
}

class TestHashSample extends FlatSpec with Matchers {

  "A HashSample" should "select 25% correctly" in {
    val obj = HashSample(Second, 1, 4)

    (1 to 10000).map { case i => if (obj.select(Position2D(2 * i, i))) 1 else 0 }.sum should be (2500 +- 50)
  }

  it should "select 50% correctly" in {
    val obj = HashSample(Second, 5, 10)

    (1 to 10000).map { case i => if (obj.select(Position2D(2 * i, i))) 1 else 0 }.sum should be (5000 +- 50)
  }

  it should "select 75% correctly" in {
    val obj = HashSample(Second, 75, 100)

    (1 to 10000).map { case i => if (obj.select(Position2D(2 * i, i))) 1 else 0 }.sum should be (7500 +- 50)
  }
}

class TestHashSampleToSize extends FlatSpec with Matchers {

  "A HashSampleToSize" should "select 25% correctly" in {
    val obj = HashSampleToSize(Second, 2500)
    val ext = Map(Position1D(Second.toString) -> Content(DiscreteSchema[Codex.LongCodex](), 10000))

    (1 to 10000).map { case i => if (obj.select(Position2D(2 * i, i), ext)) 1 else 0 }.sum should be (2500 +- 50)
  }

  it should "select 50% correctly" in {
    val obj = HashSampleToSize(Second, 5000)
    val ext = Map(Position1D(Second.toString) -> Content(DiscreteSchema[Codex.LongCodex](), 10000))

    (1 to 10000).map { case i => if (obj.select(Position2D(2 * i, i), ext)) 1 else 0 }.sum should be (5000 +- 50)
  }

  it should "select 75% correctly" in {
    val obj = HashSampleToSize(Second, 7500)
    val ext = Map(Position1D(Second.toString) -> Content(DiscreteSchema[Codex.LongCodex](), 10000))

    (1 to 10000).map { case i => if (obj.select(Position2D(2 * i, i), ext)) 1 else 0 }.sum should be (7500 +- 50)
  }
}

class TestAndThenSampler extends FlatSpec with Matchers {

  "A AndThenSampler" should "select correctly" in {
    val obj = AndThenSampler(HashSample(Second, 1, 4), HashSample(First, 1, 4))
    val ext = Map(Position1D(Second.toString) -> Content(DiscreteSchema[Codex.LongCodex](), 10000))
    val res = (1 to 10000).flatMap { case i => if (obj.select(Position2D(i, i), ext)) Some((i,i)) else None }

    res.map(_._1).distinct.length should be (2500 +- 50)
    res.map(_._2).distinct.length should be (2500 +- 50)
  }
}

class TestAndThenSamplerWithValue extends FlatSpec with Matchers {

  "A AndThenSamplerWithValue" should "select correctly" in {
    val obj = AndThenSamplerWithValue(HashSampleToSize(Second, 2500), HashSample(First, 1, 4))
    val ext = Map(Position1D(Second.toString) -> Content(DiscreteSchema[Codex.LongCodex](), 10000))
    val res = (1 to 10000).flatMap { case i => if (obj.select(Position2D(i, i), ext)) Some((i,i)) else None }

    res.map(_._1).distinct.length should be (625 +- 50)
    res.map(_._2).distinct.length should be (625 +- 50)
  }
}

