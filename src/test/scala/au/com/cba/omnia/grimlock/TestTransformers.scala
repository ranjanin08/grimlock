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
import au.com.cba.omnia.grimlock.transform._
import au.com.cba.omnia.grimlock.utility._

import org.scalatest._

trait TestTransformers {
  def getLongContent(value: Long): Content = Content(DiscreteSchema[Codex.LongCodex](), value)
  def getDoubleContent(value: Double): Content = Content(ContinuousSchema[Codex.DoubleCodex](), value)
  def getStringContent(value: String): Content = Content(NominalSchema[Codex.StringCodex](), value)
}

class TestIndicator extends FlatSpec with Matchers with TestTransformers {

  val cell = Cell(Position2D("foo", "bar"), getDoubleContent(3.1415))

  "An Indicator" should "present" in {
    Indicator(First).present(cell) should be (Collection(Position2D("foo", "bar"), getLongContent(1)))
    Indicator(Second).present(cell) should be (Collection(Position2D("foo", "bar"), getLongContent(1)))
  }

  it should "present with name" in {
    Indicator(First, "%1$s-%2$s.ind").present(cell) should
      be (Collection(Position2D("foo-3.1415.ind", "bar"), getLongContent(1)))
    Indicator(Second, "%1$s-%2$s.ind").present(cell) should
      be (Collection(Position2D("foo", "bar-3.1415.ind"), getLongContent(1)))
  }
}

class TestBinarise extends FlatSpec with Matchers with TestTransformers {

  val cell = Cell(Position2D("foo", "bar"), getStringContent("rules"))

  "A Binarise" should "present" in {
    Binarise(First).present(cell) should be (Collection(Position2D("foo=rules", "bar"), getLongContent(1)))
    Binarise(Second).present(cell) should be (Collection(Position2D("foo", "bar=rules"), getLongContent(1)))
  }

  it should "present with name" in {
    Binarise(First, "%1$s.%2$s").present(cell) should
      be (Collection(Position2D("foo.rules", "bar"), getLongContent(1)))
    Binarise(Second, "%1$s.%2$s").present(cell) should
      be (Collection(Position2D("foo", "bar.rules"), getLongContent(1)))
  }

  it should "not present a numerical" in {
    Binarise(First).present(Cell(Position1D("foo"), getDoubleContent(3.1415))) should be (Collection())
  }
}

class TestNormalise extends FlatSpec with Matchers with TestTransformers {

  val cell = Cell(Position2D("foo", "bar"), getDoubleContent(3.1415))
  val ext = Map(Position1D("foo") -> Map(Position1D("const") -> getDoubleContent(6.283)),
    Position1D("bar") -> Map(Position1D("const") -> getDoubleContent(-1.57075)))

  "An Normalise" should "present" in {
    Normalise(First, "const").present(cell, ext) should
      be (Collection(Position2D("foo", "bar"), getDoubleContent(0.5)))
    Normalise(Second, "const").present(cell, ext) should
      be (Collection(Position2D("foo", "bar"), getDoubleContent(-2)))
  }

  it should "present with name" in {
    Normalise(First, "const", "%1$s-%2$s.norm").present(cell, ext) should
      be (Collection(Position2D("foo-3.1415.norm", "bar"), getDoubleContent(0.5)))
    Normalise(Second, "const", "%1$s-%2$s.norm").present(cell, ext) should
      be (Collection(Position2D("foo", "bar-3.1415.norm"), getDoubleContent(-2)))
  }

  it should "not present with missing key" in {
    Normalise(First, "not.there").present(cell, ext) should be (Collection())
  }

  it should "not present with missing value" in {
    Normalise(First, "const").present(Cell(Position1D("baz"), getDoubleContent(3.1415)), ext) should be (Collection())
  }

  it should "not present with a categorical" in {
    Normalise(First, "const").present(Cell(Position1D("foo"), getStringContent("bar")), ext) should be (Collection())
  }
}

class TestStandardise extends FlatSpec with Matchers with TestTransformers {

  val cell = Cell(Position2D("foo", "bar"), getDoubleContent(3.1415))
  val ext = Map(
    Position1D("foo") -> Map(Position1D("mean") -> getDoubleContent(0.75),
      Position1D("sd") -> getDoubleContent(1.25)),
    Position1D("bar") -> Map(Position1D("mean") -> getDoubleContent(-0.75),
      Position1D("sd") -> getDoubleContent(0.75)))

  "A Standardise" should "present" in {
    Standardise(First, "mean", "sd").present(cell, ext) should
      be (Collection(Position2D("foo", "bar"), getDoubleContent((3.1415 - 0.75) / 1.25)))
    Standardise(Second, "mean", "sd").present(cell, ext) should
      be (Collection(Position2D("foo", "bar"), getDoubleContent((3.1415 + 0.75) / 0.75)))
  }

  it should "present with name" in {
    Standardise(First, "mean", "sd", "%1$s-%2$s.std").present(cell, ext) should
      be (Collection(Position2D("foo-3.1415.std", "bar"), getDoubleContent((3.1415 - 0.75) / 1.25)))
    Standardise(Second, "mean", "sd", "%1$s-%2$s.std").present(cell, ext) should
      be (Collection(Position2D("foo", "bar-3.1415.std"), getDoubleContent((3.1415 + 0.75) / 0.75)))
  }

  it should "present with threshold" in {
    Standardise(First, "mean", "sd", 1.0).present(cell, ext) should
      be (Collection(Position2D("foo", "bar"), getDoubleContent((3.1415 - 0.75) / 1.25)))
    Standardise(Second, "mean", "sd", 1.0).present(cell, ext) should
      be (Collection(Position2D("foo", "bar"), getDoubleContent(0)))
  }

  it should "present with N" in {
    Standardise(First, "mean", "sd", 2).present(cell, ext) should
      be (Collection(Position2D("foo", "bar"), getDoubleContent((3.1415 - 0.75) / (2 * 1.25))))
    Standardise(Second, "mean", "sd", 2).present(cell, ext) should
      be (Collection(Position2D("foo", "bar"), getDoubleContent((3.1415 + 0.75) / (2 * 0.75))))
  }

  it should "present with name and threshold" in {
    Standardise(First, "mean", "sd", "%1$s-%2$s.std", 1.0).present(cell, ext) should
      be (Collection(Position2D("foo-3.1415.std", "bar"), getDoubleContent((3.1415 - 0.75) / 1.25)))
    Standardise(Second, "mean", "sd", "%1$s-%2$s.std", 1.0).present(cell, ext) should
      be (Collection(Position2D("foo", "bar-3.1415.std"), getDoubleContent(0)))
  }

  it should "present with name and N" in {
    Standardise(First, "mean", "sd", "%1$s-%2$s.std", 2).present(cell, ext) should
      be (Collection(Position2D("foo-3.1415.std", "bar"), getDoubleContent((3.1415 - 0.75) / (2 * 1.25))))
    Standardise(Second, "mean", "sd", "%1$s-%2$s.std", 2).present(cell, ext) should
      be (Collection(Position2D("foo", "bar-3.1415.std"), getDoubleContent((3.1415 + 0.75) / (2 * 0.75))))
  }

  it should "present with threshold and N" in {
    Standardise(First, "mean", "sd", 1.0, 2).present(cell, ext) should
      be (Collection(Position2D("foo", "bar"), getDoubleContent((3.1415 - 0.75) / (2 * 1.25))))
    Standardise(Second, "mean", "sd", 1.0, 2).present(cell, ext) should
      be (Collection(Position2D("foo", "bar"), getDoubleContent(0)))
  }

  it should "present with name, threshold and N" in {
    Standardise(First, "mean", "sd", "%1$s-%2$s.std", 1.0, 2).present(cell, ext) should
      be (Collection(Position2D("foo-3.1415.std", "bar"), getDoubleContent((3.1415 - 0.75) / (2 * 1.25))))
    Standardise(Second, "mean", "sd", "%1$s-%2$s.std", 1.0, 2).present(cell, ext) should
      be (Collection(Position2D("foo", "bar-3.1415.std"), getDoubleContent(0)))
  }

  it should "not present with missing key" in {
    Standardise(First, "not.there", "sd").present(cell, ext) should be (Collection())
    Standardise(First, "mean", "not.there").present(cell, ext) should be (Collection())
  }

  it should "not present with missing value" in {
    Standardise(First, "mean", "sd").present(Cell(Position1D("baz"), getDoubleContent(3.1415)), ext) should
      be (Collection())
  }

  it should "not present with a categorical" in {
    Standardise(First, "mean", "sd").present(Cell(Position1D("foo"), getStringContent("bar")), ext) should
      be (Collection())
  }
}

class TestClamp extends FlatSpec with Matchers with TestTransformers {

  val cell = Cell(Position3D("foo", "bar", "baz"), getDoubleContent(3.1415))
  val ext = Map(
    Position1D("foo") -> Map(Position1D("min") -> getDoubleContent(0),
      Position1D("max") -> getDoubleContent(6.283)),
    Position1D("bar") -> Map(Position1D("min") -> getDoubleContent(0),
      Position1D("max") -> getDoubleContent(1.57075)),
    Position1D("baz") -> Map(Position1D("min") -> getDoubleContent(4.71225),
      Position1D("max") -> getDoubleContent(6.283)))

  "A Clamp" should "present" in {
    Clamp(First, "min", "max").present(cell, ext) should
      be (Collection(Position3D("foo", "bar", "baz"), getDoubleContent(3.1415)))
    Clamp(Second, "min", "max").present(cell, ext) should
      be (Collection(Position3D("foo", "bar", "baz"), getDoubleContent(1.57075)))
    Clamp(Third, "min", "max").present(cell, ext) should
      be (Collection(Position3D("foo", "bar", "baz"), getDoubleContent(4.71225)))
  }

  it should "present with name" in {
    Clamp(First, "min", "max", "%1$s-%2$s.std").present(cell, ext) should
      be (Collection(Position3D("foo-3.1415.std", "bar", "baz"), getDoubleContent(3.1415)))
    Clamp(Second, "min", "max", "%1$s-%2$s.std").present(cell, ext) should
      be (Collection(Position3D("foo", "bar-3.1415.std", "baz"), getDoubleContent(1.57075)))
    Clamp(Third, "min", "max", "%1$s-%2$s.std").present(cell, ext) should
      be (Collection(Position3D("foo", "bar", "baz-3.1415.std"), getDoubleContent(4.71225)))
  }

  it should "not present with missing key" in {
    Clamp(First, "not.there", "max").present(cell, ext) should be (Collection())
    Clamp(First, "min", "not.there").present(cell, ext) should be (Collection())
  }

  it should "not present with missing value" in {
    Clamp(First, "min", "max").present(Cell(Position1D("abc"), getDoubleContent(3.1415)), ext) should
      be (Collection())
  }

  it should "not present with a categorical" in {
    Clamp(First, "min", "max").present(Cell(Position1D("foo"), getStringContent("bar")), ext) should
      be (Collection())
  }
}

class TestIdf extends FlatSpec with Matchers with TestTransformers {

  val cell = Cell(Position1D("foo"), getLongContent(1))
  val ext = Map(Position1D("foo") -> getLongContent(2), Position1D("bar") -> getLongContent(2))

  "An Idf" should "present" in {
    Idf(First).present(cell, ext) should be (Collection(Position1D("foo"), getDoubleContent(0)))
  }

  it should "present with name" in {
    Idf(First, name="%1$s-%2$s.idf").present(cell, ext) should
      be (Collection(Position1D("foo-1.idf"), getDoubleContent(0)))
  }

  it should "present with key" in {
    Idf(First, key="bar").present(cell, ext) should be (Collection(Position1D("foo"), getDoubleContent(0)))
  }

  it should "present with function" in {
    Idf(First, Idf.Transform(math.log10, 0)).present(cell, ext) should
      be (Collection(Position1D("foo"), getDoubleContent(math.log10(2))))
  }

  it should "present with name and key" in {
    Idf(First, "bar", "%1$s-%2$s.idf").present(cell, ext) should
      be (Collection(Position1D("foo-1.idf"), getDoubleContent(0)))
  }

  it should "present with name and function" in {
    Idf(First, name="%1$s-%2$s.idf", Idf.Transform(math.log10, 0)).present(cell, ext) should
      be (Collection(Position1D("foo-1.idf"), getDoubleContent(math.log10(2))))
  }

  it should "present with key and function" in {
    Idf(First, key="bar", Idf.Transform(math.log10, 0)).present(cell, ext) should
      be (Collection(Position1D("foo"), getDoubleContent(math.log10(2))))
  }

  it should "present with name, key and function" in {
    Idf(First, "bar", "%1$s-%2$s.idf", Idf.Transform(math.log10, 0)).present(cell, ext) should
      be (Collection(Position1D("foo-1.idf"), getDoubleContent(math.log10(2))))
  }

  it should "not present with missing key" in {
    Idf(First, key="not.there").present(cell, ext) should be (Collection())
  }

  it should "not present with missing value" in {
    Idf(First).present(Cell(Position1D("abc"), getLongContent(1)), ext) should be (Collection())
  }

  it should "not present with a categorical" in {
    Idf(First).present(Cell(Position1D("foo"), getStringContent("bar")), ext) should be (Collection())
  }
}

class TestBooleanTf extends FlatSpec with Matchers with TestTransformers {

  val cell = Cell(Position2D("foo", "bar"), getLongContent(3))

  "A BooleanTf" should "present" in {
    BooleanTf(First).present(cell) should be (Collection(Position2D("foo", "bar"), getDoubleContent(1)))
    BooleanTf(Second).present(cell) should be (Collection(Position2D("foo", "bar"), getDoubleContent(1)))
  }

  it should "present with name" in {
    BooleanTf(First, "%1$s-%2$s.btf").present(cell) should
      be (Collection(Position2D("foo-3.btf", "bar"), getDoubleContent(1)))
    BooleanTf(Second, "%1$s-%2$s.btf").present(cell) should
      be (Collection(Position2D("foo", "bar-3.btf"), getDoubleContent(1)))
  }

  it should "not present with a categorical" in {
    BooleanTf(First).present(Cell(Position2D("foo", "bar"), getStringContent("baz"))) should be (Collection())
  }
}

class TestLogarithmicTf extends FlatSpec with Matchers with TestTransformers {

  val cell = Cell(Position2D("foo", "bar"), getLongContent(3))

  "A LogarithmicTf" should "present" in {
    LogarithmicTf(First).present(cell) should
      be (Collection(Position2D("foo", "bar"), getDoubleContent(1 + math.log(3))))
    LogarithmicTf(Second).present(cell) should
      be (Collection(Position2D("foo", "bar"), getDoubleContent(1 + math.log(3))))
  }

  it should "present with name" in {
    LogarithmicTf(First, "%1$s-%2$s.ltf").present(cell) should
      be (Collection(Position2D("foo-3.ltf", "bar"), getDoubleContent(1 + math.log(3))))
    LogarithmicTf(Second, "%1$s-%2$s.ltf").present(cell) should
      be (Collection(Position2D("foo", "bar-3.ltf"), getDoubleContent(1 + math.log(3))))
  }

  it should "present with log" in {
    LogarithmicTf(First, math.log10 _).present(cell) should
      be (Collection(Position2D("foo", "bar"), getDoubleContent(1 + math.log10(3))))
    LogarithmicTf(Second, math.log10 _).present(cell) should
      be (Collection(Position2D("foo", "bar"), getDoubleContent(1 + math.log10(3))))
  }

  it should "present with name and log" in {
    LogarithmicTf(First, "%1$s-%2$s.ltf", math.log10 _).present(cell) should
      be (Collection(Position2D("foo-3.ltf", "bar"), getDoubleContent(1 + math.log10(3))))
    LogarithmicTf(Second, "%1$s-%2$s.ltf", math.log10 _).present(cell) should
      be (Collection(Position2D("foo", "bar-3.ltf"), getDoubleContent(1 + math.log10(3))))
  }

  it should "not present with a categorical" in {
    LogarithmicTf(First).present(Cell(Position2D("foo", "bar"), getStringContent("baz"))) should be (Collection())
  }
}

class TestAugmentedTf extends FlatSpec with Matchers with TestTransformers {

  val cell = Cell(Position2D("foo", "bar"), getLongContent(1))
  val ext = Map(Position1D("foo") -> getLongContent(2), Position1D("bar") -> getLongContent(2))

  "An AugmentedTf" should "present" in {
    AugmentedTf(First).present(cell, ext) should
      be (Collection(Position2D("foo", "bar"), getDoubleContent(0.5 + 0.5 * 1 / 2)))
    AugmentedTf(Second).present(cell, ext) should
      be (Collection(Position2D("foo", "bar"), getDoubleContent(0.5 + 0.5 * 1 / 2)))
  }

  it should "present with name" in {
    AugmentedTf(First, "%1$s-%2$s.atf").present(cell, ext) should
      be (Collection(Position2D("foo-1.atf", "bar"), getDoubleContent(0.5 + 0.5 * 1 / 2)))
    AugmentedTf(Second, "%1$s-%2$s.atf").present(cell, ext) should
      be (Collection(Position2D("foo", "bar-1.atf"), getDoubleContent(0.5 + 0.5 * 1 / 2)))
  }

  it should "not present with missing value" in {
    AugmentedTf(First).present(Cell(Position2D("abc", "bar"), getLongContent(1)), ext) should be (Collection())
  }

  it should "not present with a categorical" in {
    AugmentedTf(First).present(Cell(Position2D("foo", "bar"), getStringContent("baz")), ext) should be (Collection())
  }
}

class TestTfIdf extends FlatSpec with Matchers with TestTransformers {

  val cell = Cell(Position2D("foo", "bar"), getDoubleContent(1.5))
  val ext = Map(Position1D("foo") -> getDoubleContent(2), Position1D("bar") -> getDoubleContent(2),
    Position1D("baz") -> getDoubleContent(2))

  "A TfIdf" should "present" in {
    TfIdf(First).present(cell, ext) should be (Collection(Position2D("foo", "bar"), getDoubleContent(3)))
    TfIdf(Second).present(cell, ext) should be (Collection(Position2D("foo", "bar"), getDoubleContent(3)))
  }

  it should "present with name" in {
    TfIdf(First, name="%1$s-%2$s.tfidf").present(cell, ext) should
      be (Collection(Position2D("foo-1.5.tfidf", "bar"), getDoubleContent(3)))
    TfIdf(Second, name="%1$s-%2$s.tfidf").present(cell, ext) should
      be (Collection(Position2D("foo", "bar-1.5.tfidf"), getDoubleContent(3)))
  }

  it should "present with key" in {
    TfIdf(First, key="baz").present(cell, ext) should be (Collection(Position2D("foo", "bar"), getDoubleContent(3)))
    TfIdf(Second, key="baz").present(cell, ext) should be (Collection(Position2D("foo", "bar"), getDoubleContent(3)))
  }

  it should "present with name and key" in {
    TfIdf(First, "baz", "%1$s-%2$s.tfidf").present(cell, ext) should
      be (Collection(Position2D("foo-1.5.tfidf", "bar"), getDoubleContent(3)))
    TfIdf(Second, "baz", "%1$s-%2$s.tfidf").present(cell, ext) should
      be (Collection(Position2D("foo", "bar-1.5.tfidf"), getDoubleContent(3)))
  }

  it should "not present with missing key" in {
    TfIdf(First, key="not.there").present(cell, ext) should be (Collection())
  }

  it should "not present with missing value" in {
    TfIdf(First).present(Cell(Position2D("abc", "bar"), getDoubleContent(1.5)), ext) should be (Collection())
  }

  it should "not present with a categorical" in {
    TfIdf(First).present(Cell(Position2D("foo", "bar"), getStringContent("baz")), ext) should be (Collection())
  }
}

class TestAdd extends FlatSpec with Matchers with TestTransformers {

  val cell = Cell(Position1D("foo"), getDoubleContent(1))
  val ext = Map(Position1D("foo") -> getDoubleContent(2), Position1D("bar") -> getDoubleContent(2))

  "An Add" should "present" in {
    Add(First).present(cell, ext) should be (Collection(Position1D("foo"), getDoubleContent(3)))
  }

  it should "present with name" in {
    Add(First, name="%1$s-%2$s.idf").present(cell, ext) should
      be (Collection(Position1D("foo-1.0.idf"), getDoubleContent(3)))
  }

  it should "present with key" in {
    Add(First, key="bar").present(cell, ext) should be (Collection(Position1D("foo"), getDoubleContent(3)))
  }

  it should "present with name and key" in {
    Add(First, "bar", "%1$s-%2$s.idf").present(cell, ext) should
      be (Collection(Position1D("foo-1.0.idf"), getDoubleContent(3)))
  }

  it should "not present with missing key" in {
    Add(First, key="not.there").present(cell, ext) should be (Collection())
  }

  it should "not present with missing value" in {
    Add(First).present(Cell(Position1D("abc"), getDoubleContent(1)), ext) should be (Collection())
  }

  it should "not present with a categorical" in {
    Add(First).present(Cell(Position1D("foo"), getStringContent("bar")), ext) should be (Collection())
  }
}

class TestSubtract extends FlatSpec with Matchers with TestTransformers {

  val cell = Cell(Position1D("foo"), getDoubleContent(1))
  val ext = Map(Position1D("foo") -> getDoubleContent(2), Position1D("bar") -> getDoubleContent(2))

  "A Subtract" should "present" in {
    Subtract(First).present(cell, ext) should be (Collection(Position1D("foo"), getDoubleContent(-1)))
  }

  it should "present with name" in {
    Subtract(First, name="%1$s-%2$s.idf").present(cell, ext) should
      be (Collection(Position1D("foo-1.0.idf"), getDoubleContent(-1)))
  }

  it should "present with key" in {
    Subtract(First, key="bar").present(cell, ext) should be (Collection(Position1D("foo"), getDoubleContent(-1)))
  }

  it should "present with inverse" in {
    Subtract(First, true).present(cell, ext) should be (Collection(Position1D("foo"), getDoubleContent(1)))
  }

  it should "present with name and key" in {
    Subtract(First, "bar", "%1$s-%2$s.idf").present(cell, ext) should
      be (Collection(Position1D("foo-1.0.idf"), getDoubleContent(-1)))
  }

  it should "present with name and inverse" in {
    Subtract(First, name="%1$s-%2$s.idf", true).present(cell, ext) should
      be (Collection(Position1D("foo-1.0.idf"), getDoubleContent(1)))
  }

  it should "present with key and inverse" in {
    Subtract(First, key="bar", true).present(cell, ext) should be (Collection(Position1D("foo"), getDoubleContent(1)))
  }

  it should "present with name, key and inverse" in {
    Subtract(First, "bar", "%1$s-%2$s.idf", true).present(cell, ext) should
      be (Collection(Position1D("foo-1.0.idf"), getDoubleContent(1)))
  }

  it should "not present with missing key" in {
    Subtract(First, key="not.there").present(cell, ext) should be (Collection())
  }

  it should "not present with missing value" in {
    Subtract(First).present(Cell(Position1D("abc"), getDoubleContent(1)), ext) should be (Collection())
  }

  it should "not present with a categorical" in {
    Subtract(First).present(Cell(Position1D("foo"), getStringContent("bar")), ext) should be (Collection())
  }
}

class TestMultiply extends FlatSpec with Matchers with TestTransformers {

  val cell = Cell(Position1D("foo"), getDoubleContent(1))
  val ext = Map(Position1D("foo") -> getDoubleContent(2), Position1D("bar") -> getDoubleContent(2))

  "A Multiply" should "present" in {
    Multiply(First).present(cell, ext) should be (Collection(Position1D("foo"), getDoubleContent(2)))
  }

  it should "present with name" in {
    Multiply(First, name="%1$s-%2$s.idf").present(cell, ext) should
      be (Collection(Position1D("foo-1.0.idf"), getDoubleContent(2)))
  }

  it should "present with key" in {
    Multiply(First, key="bar").present(cell, ext) should be (Collection(Position1D("foo"), getDoubleContent(2)))
  }

  it should "present with name and key" in {
    Multiply(First, "bar", "%1$s-%2$s.idf").present(cell, ext) should
      be (Collection(Position1D("foo-1.0.idf"), getDoubleContent(2)))
  }

  it should "not present with missing key" in {
    Multiply(First, key="not.there").present(cell, ext) should be (Collection())
  }

  it should "not present with missing value" in {
    Multiply(First).present(Cell(Position1D("abc"), getDoubleContent(1)), ext) should be (Collection())
  }

  it should "not present with a categorical" in {
    Multiply(First).present(Cell(Position1D("foo"), getStringContent("bar")), ext) should be (Collection())
  }
}

class TestFraction extends FlatSpec with Matchers with TestTransformers {

  val cell = Cell(Position1D("foo"), getDoubleContent(1))
  val ext = Map(Position1D("foo") -> getDoubleContent(2), Position1D("bar") -> getDoubleContent(2))

  "A Fraction" should "present" in {
    Fraction(First).present(cell, ext) should be (Collection(Position1D("foo"), getDoubleContent(0.5)))
  }

  it should "present with name" in {
    Fraction(First, name="%1$s-%2$s.idf").present(cell, ext) should
      be (Collection(Position1D("foo-1.0.idf"), getDoubleContent(0.5)))
  }

  it should "present with key" in {
    Fraction(First, key="bar").present(cell, ext) should be (Collection(Position1D("foo"), getDoubleContent(0.5)))
  }

  it should "present with inverse" in {
    Fraction(First, true).present(cell, ext) should be (Collection(Position1D("foo"), getDoubleContent(2)))
  }

  it should "present with name and key" in {
    Fraction(First, "bar", "%1$s-%2$s.idf").present(cell, ext) should
      be (Collection(Position1D("foo-1.0.idf"), getDoubleContent(0.5)))
  }

  it should "present with name and inverse" in {
    Fraction(First, name="%1$s-%2$s.idf", true).present(cell, ext) should
      be (Collection(Position1D("foo-1.0.idf"), getDoubleContent(2)))
  }

  it should "present with key and inverse" in {
    Fraction(First, key="bar", true).present(cell, ext) should be (Collection(Position1D("foo"), getDoubleContent(2)))
  }

  it should "present with name, key and inverse" in {
    Fraction(First, "bar", "%1$s-%2$s.idf", true).present(cell, ext) should
      be (Collection(Position1D("foo-1.0.idf"), getDoubleContent(2)))
  }

  it should "not present with missing key" in {
    Fraction(First, key="not.there").present(cell, ext) should be (Collection())
  }

  it should "not present with missing value" in {
    Fraction(First).present(Cell(Position1D("abc"), getDoubleContent(1)), ext) should be (Collection())
  }

  it should "not present with a categorical" in {
    Fraction(First).present(Cell(Position1D("foo"), getStringContent("bar")), ext) should be (Collection())
  }
}

class TestPower extends FlatSpec with Matchers with TestTransformers {

  val cell = Cell(Position1D("foo"), getDoubleContent(3.1415))

  "A Power" should "present" in {
    Power(First, 2).present(cell) should be (Collection(Position1D("foo"), getDoubleContent(3.1415 * 3.1415)))
  }

  it should "present with name" in {
    Power(First, 2, "%1$s-%2$s.pwr").present(cell) should
      be (Collection(Position1D("foo-3.1415.pwr"), getDoubleContent(3.1415 * 3.1415)))
  }
}

class TestSquareRoot extends FlatSpec with Matchers with TestTransformers {

  val cell = Cell(Position1D("foo"), getDoubleContent(3.1415))

  "A SquareRoot" should "present" in {
    SquareRoot(First).present(cell) should be (Collection(Position1D("foo"), getDoubleContent(math.sqrt(3.1415))))
  }

  it should "present with name" in {
    SquareRoot(First, "%1$s-%2$s.sqr").present(cell) should
      be (Collection(Position1D("foo-3.1415.sqr"), getDoubleContent(math.sqrt(3.1415))))
  }
}

class TestCut extends FlatSpec with Matchers with TestTransformers {

  val cell = Cell(Position1D("foo"), getDoubleContent(3.1415))
  val ext = Map(Position1D("foo") -> List[Double](0,1,2,3,4,5))

  "A Cut" should "present" in {
    Cut(First).present(cell, ext) should be (Collection(Position1D("foo"), Content(OrdinalSchema[Codex.StringCodex](
      List("(0.0,1.0]","(1.0,2.0]","(2.0,3.0]","(3.0,4.0]","(4.0,5.0]")), "(3.0,4.0]")))
  }

  it should "present with name" in {
    Cut(First, "%1$s.cut").present(cell, ext) should be (Collection(Position1D("foo.cut"),
        Content(OrdinalSchema[Codex.StringCodex](List("(0.0,1.0]","(1.0,2.0]","(2.0,3.0]","(3.0,4.0]","(4.0,5.0]")),
          "(3.0,4.0]")))
  }

  it should "not present with missing bins" in {
    Cut(First).present(cell, Map()) should be (Collection())
  }
}

class TestScaldingCutRules extends FlatSpec with Matchers with TestTransformers {

  import com.twitter.scalding.typed.ValuePipe

  val stats = ValuePipe(Map(Position1D("foo") -> Map(Position1D("min") -> getDoubleContent(0),
    Position1D("max") -> getDoubleContent(5), Position1D("count") -> getDoubleContent(25),
    Position1D("sd") -> getDoubleContent(2), Position1D("skewness") -> getDoubleContent(2))))

  "A fixed" should "cut" in {
    ScaldingCutRules.fixed(stats, "min", "max", 5) should
      be (ValuePipe(Map(Position1D("foo") -> List[Double](-0.005,1,2,3,4,5))))
  }

  it should "not cut with missing values" in {
    ScaldingCutRules.fixed(stats, "not.there", "max", 5) should be (ValuePipe(Map()))
    ScaldingCutRules.fixed(stats, "min", "not.there", 5) should be (ValuePipe(Map()))
  }

  "A squareRootChoice" should "cut" in {
    ScaldingCutRules.squareRootChoice(stats, "count", "min", "max") should
      be (ValuePipe(Map(Position1D("foo") -> List[Double](-0.005,1,2,3,4,5))))
  }

  it should "not cut with missing values" in {
    ScaldingCutRules.squareRootChoice(stats, "not.there", "min", "max") should be (ValuePipe(Map()))
    ScaldingCutRules.squareRootChoice(stats, "count", "not.there", "max") should be (ValuePipe(Map()))
    ScaldingCutRules.squareRootChoice(stats, "count", "min", "not.there") should be (ValuePipe(Map()))
  }

  "A sturgesFormula" should "cut" in {
    // math.ceil(log2(25) + 1) = 6
    val vals = -0.005 +: (0.0 to 5.0 by (5.0 / 6)).tail

    ScaldingCutRules.sturgesFormula(stats, "count", "min", "max") should be (ValuePipe(Map(Position1D("foo") -> vals)))
  }

  it should "not cut with missing values" in {
    ScaldingCutRules.sturgesFormula(stats, "not.there", "min", "max") should be (ValuePipe(Map()))
    ScaldingCutRules.sturgesFormula(stats, "count", "not.there", "max") should be (ValuePipe(Map()))
    ScaldingCutRules.sturgesFormula(stats, "count", "min", "not.there") should be (ValuePipe(Map()))
  }

  "A riceRule" should "cut" in {
    // math.ceil(2 * math.pow(25, 1.0 / 3.0)) = 6
    val vals = -0.005 +: (0.0 to 5.0 by (5.0 / 6)).tail

    ScaldingCutRules.riceRule(stats, "count", "min", "max") should be (ValuePipe(Map(Position1D("foo") -> vals)))
  }

  it should "not cut with missing values" in {
    ScaldingCutRules.riceRule(stats, "not.there", "min", "max") should be (ValuePipe(Map()))
    ScaldingCutRules.riceRule(stats, "count", "not.there", "max") should be (ValuePipe(Map()))
    ScaldingCutRules.riceRule(stats, "count", "min", "not.there") should be (ValuePipe(Map()))
  }

  "A doanesFormula" should "cut" in {
    // math.round(1 + log2(25) + log2(1 + math.abs(2) / math.sqrt((6.0 * 23.0) / (26.0 * 28.0)))) = 8
    val vals = -0.005 +: (0.0 to 5.0 by (5.0 / 8)).tail

    ScaldingCutRules.doanesFormula(stats, "count", "min", "max", "skewness") should
      be (ValuePipe(Map(Position1D("foo") -> vals)))
  }

  it should "not cut with missing values" in {
    ScaldingCutRules.doanesFormula(stats, "not.there", "min", "max", "skewness") should be (ValuePipe(Map()))
    ScaldingCutRules.doanesFormula(stats, "count", "not.there", "max", "skewness") should be (ValuePipe(Map()))
    ScaldingCutRules.doanesFormula(stats, "count", "min", "not.there", "skewness") should be (ValuePipe(Map()))
    ScaldingCutRules.doanesFormula(stats, "count", "min", "max", "not.there") should be (ValuePipe(Map()))
  }

  "A scottsNormalReferenceRule" should "cut" in {
    // math.ceil((5.0 - 0) / (3.5 * 2 / math.pow(25, 1.0 / 3.0))) = 3
    val vals = -0.005 +: (0.0 to 5.0 by (5.0 / 3)).tail

    ScaldingCutRules.scottsNormalReferenceRule(stats, "count", "min", "max", "sd") should
      be (ValuePipe(Map(Position1D("foo") -> vals)))
  }

  it should "not cut with missing values" in {
    ScaldingCutRules.scottsNormalReferenceRule(stats, "not.there", "min", "max", "sd") should be (ValuePipe(Map()))
    ScaldingCutRules.scottsNormalReferenceRule(stats, "count", "not.there", "max", "sd") should be (ValuePipe(Map()))
    ScaldingCutRules.scottsNormalReferenceRule(stats, "count", "min", "not.there", "sd") should be (ValuePipe(Map()))
    ScaldingCutRules.scottsNormalReferenceRule(stats, "count", "min", "max", "not.there") should be (ValuePipe(Map()))
  }

  "A breaks" should "cut" in {
    ScaldingCutRules.breaks(Map("foo" -> List[Double](0,1,2,3,4,5))) should
      be (ValuePipe(Map(Position1D("foo") -> List[Double](0,1,2,3,4,5))))
  }
}

class TestSparkCutRules extends FlatSpec with Matchers with TestTransformers {

  val stats = Map(Position1D("foo") -> Map(Position1D("min") -> getDoubleContent(0),
    Position1D("max") -> getDoubleContent(5), Position1D("count") -> getDoubleContent(25),
    Position1D("sd") -> getDoubleContent(2), Position1D("skewness") -> getDoubleContent(2)))

  "A fixed" should "cut" in {
    SparkCutRules.fixed(stats, "min", "max", 5) should
      be (Map(Position1D("foo") -> List[Double](-0.005,1,2,3,4,5)))
  }

  it should "not cut with missing values" in {
    SparkCutRules.fixed(stats, "not.there", "max", 5) should be (Map())
    SparkCutRules.fixed(stats, "min", "not.there", 5) should be (Map())
  }

  "A squareRootChoice" should "cut" in {
    SparkCutRules.squareRootChoice(stats, "count", "min", "max") should
      be (Map(Position1D("foo") -> List[Double](-0.005,1,2,3,4,5)))
  }

  it should "not cut with missing values" in {
    SparkCutRules.squareRootChoice(stats, "not.there", "min", "max") should be (Map())
    SparkCutRules.squareRootChoice(stats, "count", "not.there", "max") should be (Map())
    SparkCutRules.squareRootChoice(stats, "count", "min", "not.there") should be (Map())
  }

  "A sturgesFormula" should "cut" in {
    // math.ceil(log2(25) + 1) = 6
    val vals = -0.005 +: (0.0 to 5.0 by (5.0 / 6)).tail

    SparkCutRules.sturgesFormula(stats, "count", "min", "max") should be (Map(Position1D("foo") -> vals))
  }

  it should "not cut with missing values" in {
    SparkCutRules.sturgesFormula(stats, "not.there", "min", "max") should be (Map())
    SparkCutRules.sturgesFormula(stats, "count", "not.there", "max") should be (Map())
    SparkCutRules.sturgesFormula(stats, "count", "min", "not.there") should be (Map())
  }

  "A riceRule" should "cut" in {
    // math.ceil(2 * math.pow(25, 1.0 / 3.0)) = 6
    val vals = -0.005 +: (0.0 to 5.0 by (5.0 / 6)).tail

    SparkCutRules.riceRule(stats, "count", "min", "max") should be (Map(Position1D("foo") -> vals))
  }

  it should "not cut with missing values" in {
    SparkCutRules.riceRule(stats, "not.there", "min", "max") should be (Map())
    SparkCutRules.riceRule(stats, "count", "not.there", "max") should be (Map())
    SparkCutRules.riceRule(stats, "count", "min", "not.there") should be (Map())
  }

  "A doanesFormula" should "cut" in {
    // math.round(1 + log2(25) + log2(1 + math.abs(2) / math.sqrt((6.0 * 23.0) / (26.0 * 28.0)))) = 8
    val vals = -0.005 +: (0.0 to 5.0 by (5.0 / 8)).tail

    SparkCutRules.doanesFormula(stats, "count", "min", "max", "skewness") should be (Map(Position1D("foo") -> vals))
  }

  it should "not cut with missing values" in {
    SparkCutRules.doanesFormula(stats, "not.there", "min", "max", "skewness") should be (Map())
    SparkCutRules.doanesFormula(stats, "count", "not.there", "max", "skewness") should be (Map())
    SparkCutRules.doanesFormula(stats, "count", "min", "not.there", "skewness") should be (Map())
    SparkCutRules.doanesFormula(stats, "count", "min", "max", "not.there") should be (Map())
  }

  "A scottsNormalReferenceRule" should "cut" in {
    // math.ceil((5.0 - 0) / (3.5 * 2 / math.pow(25, 1.0 / 3.0))) = 3
    val vals = -0.005 +: (0.0 to 5.0 by (5.0 / 3)).tail

    SparkCutRules.scottsNormalReferenceRule(stats, "count", "min", "max", "sd") should
      be (Map(Position1D("foo") -> vals))
  }

  it should "not cut with missing values" in {
    SparkCutRules.scottsNormalReferenceRule(stats, "not.there", "min", "max", "sd") should be (Map())
    SparkCutRules.scottsNormalReferenceRule(stats, "count", "not.there", "max", "sd") should be (Map())
    SparkCutRules.scottsNormalReferenceRule(stats, "count", "min", "not.there", "sd") should be (Map())
    SparkCutRules.scottsNormalReferenceRule(stats, "count", "min", "max", "not.there") should be (Map())
  }

  "A breaks" should "cut" in {
    SparkCutRules.breaks(Map("foo" -> List[Double](0,1,2,3,4,5))) should
      be (Map(Position1D("foo") -> List[Double](0,1,2,3,4,5)))
  }
}

class TestAndThenTransformer extends FlatSpec with Matchers with TestTransformers {

  val cell = Cell(Position1D("foo"), getStringContent("rules"))

  "An AndThenTransformer" should "present" in {
    AndThenTransformer(Binarise(First), Indicator(First, "%1$s.ind")).present(cell) should
      be (Collection(List(Cell(Position1D("foo=rules.ind"), getLongContent(1)))))
  }
}

class TestAndThenTransformerWithValue extends FlatSpec with Matchers with TestTransformers {

  val cell = Cell(Position1D("foo"), getDoubleContent(3.1415))
  val ext = Map(Position1D("foo") -> Map(Position1D("min") -> getDoubleContent(0),
    Position1D("max") -> getDoubleContent(2), Position1D("max.abs") -> getDoubleContent(3)))

  "An AndThenTransformerWithValue" should "present" in {
    AndThenTransformerWithValue(Clamp(First, "min", "max"), Normalise(First, "max.abs")).present(cell, ext) should
      be (Collection(List(Cell(Position1D("foo"), getDoubleContent(2.0 / 3.0)))))
  }
}

class TestCombinationTransformer extends FlatSpec with Matchers with TestTransformers {

  val cell = Cell(Position1D("foo"), getStringContent("rules"))

  "A CombinationTransformer" should "present" in {
    CombinationTransformer(List(Binarise(First), Indicator(First, "%1$s.ind"))).present(cell) should
      be (Collection(List(Cell(Position1D("foo=rules"), getLongContent(1)),
        Cell(Position1D("foo.ind"), getLongContent(1)))))
  }
}

class TestCombinationTransformerWithValue extends FlatSpec with Matchers with TestTransformers {

  val cell = Cell(Position1D("foo"), getDoubleContent(3.1415))
  val ext = Map(Position1D("foo") -> Map(Position1D("min") -> getDoubleContent(0),
    Position1D("max") -> getDoubleContent(2), Position1D("max.abs") -> getDoubleContent(3)))

  "A CombinationTransformerWithValue" should "present" in {
    CombinationTransformerWithValue(List(Clamp(First, "min", "max"), Normalise(First, "max.abs")))
      .present(cell, ext) should be (Collection(List(Cell(Position1D("foo"), getDoubleContent(2.0)),
        Cell(Position1D("foo"), getDoubleContent(3.1415 / 3)))))
  }
}

