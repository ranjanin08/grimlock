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
import au.com.cba.omnia.grimlock.framework.transform._
import au.com.cba.omnia.grimlock.framework.utility._

import au.com.cba.omnia.grimlock.library.transform._

trait TestTransformers extends TestGrimlock {
  def getLongContent(value: Long): Content = Content(DiscreteSchema[Codex.LongCodex](), value)
  def getDoubleContent(value: Double): Content = Content(ContinuousSchema[Codex.DoubleCodex](), value)
  def getStringContent(value: String): Content = Content(NominalSchema[Codex.StringCodex](), value)
}

class TestIndicator extends TestTransformers {

  val cell = Cell(Position2D("foo", "bar"), getDoubleContent(3.1415))

  "An Indicator" should "present" in {
    Indicator().present(cell) shouldBe Collection(Position2D("foo", "bar"), getLongContent(1))
  }

  it should "present with name" in {
    Indicator().andThenRename(Transformer.rename(First, "%1$s-%2$s.ind")).present(cell) shouldBe
      Collection(List(Cell(Position2D("foo-3.1415.ind", "bar"), getLongContent(1))))
    Indicator().andThenRename(Transformer.rename(Second, "%1$s-%2$s.ind")).present(cell) shouldBe
      Collection(List(Cell(Position2D("foo", "bar-3.1415.ind"), getLongContent(1))))
  }
}

class TestBinarise extends TestTransformers {

  val cell = Cell(Position2D("foo", "bar"), getStringContent("rules"))

  "A Binarise" should "present" in {
    Binarise(First).present(cell) shouldBe Collection(Position2D("foo=rules", "bar"), getLongContent(1))
    Binarise(Second).present(cell) shouldBe Collection(Position2D("foo", "bar=rules"), getLongContent(1))
  }

  it should "present with name" in {
    Binarise(First, "%1$s.%2$s").present(cell) shouldBe Collection(Position2D("foo.rules", "bar"), getLongContent(1))
    Binarise(Second, "%1$s.%2$s").present(cell) shouldBe Collection(Position2D("foo", "bar.rules"), getLongContent(1))
  }

  it should "not present a numerical" in {
    Binarise(First).present(Cell(Position1D("foo"), getDoubleContent(3.1415))) shouldBe Collection()
  }
}

class TestNormalise extends TestTransformers {

  val cell = Cell(Position2D("foo", "bar"), getDoubleContent(3.1415))
  val ext = Map(Position1D("foo") -> Map(Position1D("const") -> getDoubleContent(6.283)),
    Position1D("bar") -> Map(Position1D("const") -> getDoubleContent(-1.57075)))

  "An Normalise" should "present" in {
    Normalise(First, "const").presentWithValue(cell, ext) shouldBe
      Collection(Position2D("foo", "bar"), getDoubleContent(0.5))
    Normalise(Second, "const").presentWithValue(cell, ext) shouldBe
      Collection(Position2D("foo", "bar"), getDoubleContent(-2))
  }

  it should "present with name" in {
    Normalise(First, "const")
      .andThenRenameWithValue(TransformerWithValue.rename(First, "%1$s-%2$s.norm"))
      .presentWithValue(cell, ext) shouldBe Collection(List(Cell(Position2D("foo-3.1415.norm", "bar"),
        getDoubleContent(0.5))))
    Normalise(Second, "const")
      .andThenRenameWithValue(TransformerWithValue.rename(Second, "%1$s-%2$s.norm"))
      .presentWithValue(cell, ext) shouldBe Collection(List(Cell(Position2D("foo", "bar-3.1415.norm"),
        getDoubleContent(-2))))
  }

  it should "not present with missing key" in {
    Normalise(First, "not.there").presentWithValue(cell, ext) shouldBe Collection()
  }

  it should "not present with missing value" in {
    Normalise(First, "const").presentWithValue(Cell(Position1D("baz"), getDoubleContent(3.1415)), ext) shouldBe
      Collection()
  }

  it should "not present with a categorical" in {
    Normalise(First, "const").presentWithValue(Cell(Position1D("foo"), getStringContent("bar")), ext) shouldBe
      Collection()
  }
}

class TestStandardise extends TestTransformers {

  val cell = Cell(Position2D("foo", "bar"), getDoubleContent(3.1415))
  val ext = Map(
    Position1D("foo") -> Map(Position1D("mean") -> getDoubleContent(0.75), Position1D("sd") -> getDoubleContent(1.25)),
    Position1D("bar") -> Map(Position1D("mean") -> getDoubleContent(-0.75), Position1D("sd") -> getDoubleContent(0.75)))

  "A Standardise" should "present" in {
    Standardise(First, "mean", "sd").presentWithValue(cell, ext) shouldBe Collection(Position2D("foo", "bar"),
      getDoubleContent((3.1415 - 0.75) / 1.25))
    Standardise(Second, "mean", "sd").presentWithValue(cell, ext) shouldBe Collection(Position2D("foo", "bar"),
      getDoubleContent((3.1415 + 0.75) / 0.75))
  }

  it should "present with name" in {
    Standardise(First, "mean", "sd")
      .andThenRenameWithValue(TransformerWithValue.rename(First, "%1$s-%2$s.std"))
      .presentWithValue(cell, ext) shouldBe Collection(List(Cell(Position2D("foo-3.1415.std", "bar"),
        getDoubleContent((3.1415 - 0.75) / 1.25))))
    Standardise(Second, "mean", "sd")
      .andThenRenameWithValue(TransformerWithValue.rename(Second, "%1$s-%2$s.std"))
      .presentWithValue(cell, ext) shouldBe Collection(List(Cell(Position2D("foo", "bar-3.1415.std"),
        getDoubleContent((3.1415 + 0.75) / 0.75))))
  }

  it should "present with threshold" in {
    Standardise(First, "mean", "sd", 1.0).presentWithValue(cell, ext) shouldBe Collection(Position2D("foo", "bar"),
      getDoubleContent((3.1415 - 0.75) / 1.25))
    Standardise(Second, "mean", "sd", 1.0).presentWithValue(cell, ext) shouldBe Collection(Position2D("foo", "bar"),
      getDoubleContent(0))
  }

  it should "present with N" in {
    Standardise(First, "mean", "sd", n=2).presentWithValue(cell, ext) shouldBe Collection(Position2D("foo", "bar"),
      getDoubleContent((3.1415 - 0.75) / (2 * 1.25)))
    Standardise(Second, "mean", "sd", n=2).presentWithValue(cell, ext) shouldBe Collection(Position2D("foo", "bar"),
      getDoubleContent((3.1415 + 0.75) / (2 * 0.75)))
  }

  it should "present with name and threshold" in {
    Standardise(First, "mean", "sd", 1.0)
      .andThenRenameWithValue(TransformerWithValue.rename(First, "%1$s-%2$s.std"))
      .presentWithValue(cell, ext) shouldBe Collection(List(Cell(Position2D("foo-3.1415.std", "bar"),
        getDoubleContent((3.1415 - 0.75) / 1.25))))
    Standardise(Second, "mean", "sd", 1.0)
      .andThenRenameWithValue(TransformerWithValue.rename(Second, "%1$s-%2$s.std"))
      .presentWithValue(cell, ext) shouldBe Collection(List(Cell(Position2D("foo", "bar-3.1415.std"),
        getDoubleContent(0))))
  }

  it should "present with name and N" in {
    Standardise(First, "mean", "sd", n=2)
      .andThenRenameWithValue(TransformerWithValue.rename(First, "%1$s-%2$s.std"))
      .presentWithValue(cell, ext) shouldBe Collection(List(Cell(Position2D("foo-3.1415.std", "bar"),
        getDoubleContent((3.1415 - 0.75) / (2 * 1.25)))))
    Standardise(Second, "mean", "sd", n=2)
      .andThenRenameWithValue(TransformerWithValue.rename(Second, "%1$s-%2$s.std"))
      .presentWithValue(cell, ext) shouldBe Collection(List(Cell(Position2D("foo", "bar-3.1415.std"),
        getDoubleContent((3.1415 + 0.75) / (2 * 0.75)))))
  }

  it should "present with threshold and N" in {
    Standardise(First, "mean", "sd", 1.0, 2).presentWithValue(cell, ext) shouldBe Collection(Position2D("foo", "bar"),
      getDoubleContent((3.1415 - 0.75) / (2 * 1.25)))
    Standardise(Second, "mean", "sd", 1.0, 2).presentWithValue(cell, ext) shouldBe Collection(Position2D("foo", "bar"),
      getDoubleContent(0))
  }

  it should "present with name, threshold and N" in {
    Standardise(First, "mean", "sd", 1.0, 2)
      .andThenRenameWithValue(TransformerWithValue.rename(First, "%1$s-%2$s.std"))
      .presentWithValue(cell, ext) shouldBe Collection(List(Cell(Position2D("foo-3.1415.std", "bar"),
        getDoubleContent((3.1415 - 0.75) / (2 * 1.25)))))
    Standardise(Second, "mean", "sd", 1.0, 2)
      .andThenRenameWithValue(TransformerWithValue.rename(Second, "%1$s-%2$s.std"))
      .presentWithValue(cell, ext) shouldBe Collection(List(Cell(Position2D("foo", "bar-3.1415.std"),
        getDoubleContent(0))))
  }

  it should "not present with missing key" in {
    Standardise(First, "not.there", "sd").presentWithValue(cell, ext) shouldBe Collection()
    Standardise(First, "mean", "not.there").presentWithValue(cell, ext) shouldBe Collection()
  }

  it should "not present with missing value" in {
    Standardise(First, "mean", "sd")
      .presentWithValue(Cell(Position1D("baz"), getDoubleContent(3.1415)), ext) shouldBe Collection()
  }

  it should "not present with a categorical" in {
    Standardise(First, "mean", "sd")
      .presentWithValue(Cell(Position1D("foo"), getStringContent("bar")), ext) shouldBe Collection()
  }
}

class TestClamp extends TestTransformers {

  val cell = Cell(Position3D("foo", "bar", "baz"), getDoubleContent(3.1415))
  val ext = Map(
    Position1D("foo") -> Map(Position1D("min") -> getDoubleContent(0), Position1D("max") -> getDoubleContent(6.283)),
    Position1D("bar") -> Map(Position1D("min") -> getDoubleContent(0), Position1D("max") -> getDoubleContent(1.57075)),
    Position1D("baz") -> Map(Position1D("min") -> getDoubleContent(4.71225),
      Position1D("max") -> getDoubleContent(6.283)))

  "A Clamp" should "present" in {
    Clamp(First, "min", "max").presentWithValue(cell, ext) shouldBe Collection(Position3D("foo", "bar", "baz"),
      getDoubleContent(3.1415))
    Clamp(Second, "min", "max").presentWithValue(cell, ext) shouldBe Collection(Position3D("foo", "bar", "baz"),
      getDoubleContent(1.57075))
    Clamp(Third, "min", "max").presentWithValue(cell, ext) shouldBe Collection(Position3D("foo", "bar", "baz"),
      getDoubleContent(4.71225))
  }

  it should "present with name" in {
    Clamp(First, "min", "max")
      .andThenRenameWithValue(TransformerWithValue.rename(First, "%1$s-%2$s.std"))
      .presentWithValue(cell, ext) shouldBe Collection(List(Cell(Position3D("foo-3.1415.std", "bar", "baz"),
        getDoubleContent(3.1415))))
    Clamp(Second, "min", "max")
      .andThenRenameWithValue(TransformerWithValue.rename(Second, "%1$s-%2$s.std"))
      .presentWithValue(cell, ext) shouldBe Collection(List(Cell(Position3D("foo", "bar-3.1415.std", "baz"),
        getDoubleContent(1.57075))))
    Clamp(Third, "min", "max")
      .andThenRenameWithValue(TransformerWithValue.rename(Third, "%1$s-%2$s.std"))
      .presentWithValue(cell, ext) shouldBe Collection(List(Cell(Position3D("foo", "bar", "baz-3.1415.std"),
        getDoubleContent(4.71225))))
  }

  it should "not present with missing key" in {
    Clamp(First, "not.there", "max").presentWithValue(cell, ext) shouldBe Collection()
    Clamp(First, "min", "not.there").presentWithValue(cell, ext) shouldBe Collection()
  }

  it should "not present with missing value" in {
    Clamp(First, "min", "max").presentWithValue(Cell(Position1D("abc"), getDoubleContent(3.1415)), ext) shouldBe
      Collection()
  }

  it should "not present with a categorical" in {
    Clamp(First, "min", "max").presentWithValue(Cell(Position1D("foo"), getStringContent("bar")), ext) shouldBe
      Collection()
  }
}

class TestIdf extends TestTransformers {

  val cell = Cell(Position1D("foo"), getLongContent(1))
  val ext = Map(Position1D("foo") -> getLongContent(2), Position1D("bar") -> getLongContent(2))

  "An Idf" should "present" in {
    Idf(First).presentWithValue(cell, ext) shouldBe Collection(Position1D("foo"), getDoubleContent(0))
  }

  it should "present with name" in {
    Idf(First)
      .andThenRenameWithValue(TransformerWithValue.rename(First, "%1$s-%2$s.idf"))
      .presentWithValue(cell, ext) shouldBe Collection(List(Cell(Position1D("foo-1.idf"), getDoubleContent(0))))
  }

  it should "present with key" in {
    Idf("bar").presentWithValue(cell, ext) shouldBe Collection(Position1D("foo"), getDoubleContent(0))
  }

  it should "present with function" in {
    Idf(First, Idf.Transform(math.log10, 0)).presentWithValue(cell, ext) shouldBe Collection(Position1D("foo"),
      getDoubleContent(math.log10(2)))
  }

  it should "present with name and key" in {
    Idf("bar")
      .andThenRenameWithValue(TransformerWithValue.rename(First, "%1$s-%2$s.idf"))
      .presentWithValue(cell, ext) shouldBe Collection(List(Cell(Position1D("foo-1.idf"), getDoubleContent(0))))
  }

  it should "present with name and function" in {
    Idf(First, Idf.Transform(math.log10, 0))
      .andThenRenameWithValue(TransformerWithValue.rename(First, "%1$s-%2$s.idf"))
      .presentWithValue(cell, ext) shouldBe Collection(List(Cell(Position1D("foo-1.idf"),
        getDoubleContent(math.log10(2)))))
  }

  it should "present with key and function" in {
    Idf("bar", Idf.Transform(math.log10, 0)).presentWithValue(cell, ext) shouldBe Collection(Position1D("foo"),
      getDoubleContent(math.log10(2)))
  }

  it should "present with name, key and function" in {
    Idf("bar", Idf.Transform(math.log10, 0))
      .andThenRenameWithValue(TransformerWithValue.rename(First, "%1$s-%2$s.idf"))
      .presentWithValue(cell, ext) shouldBe Collection(List(Cell(Position1D("foo-1.idf"),
        getDoubleContent(math.log10(2)))))
  }

  it should "not present with missing key" in {
    Idf("not.there").presentWithValue(cell, ext) shouldBe Collection()
  }

  it should "not present with missing value" in {
    Idf(First).presentWithValue(Cell(Position1D("abc"), getLongContent(1)), ext) shouldBe Collection()
  }

  it should "not present with a categorical" in {
    Idf(First).presentWithValue(Cell(Position1D("foo"), getStringContent("bar")), ext) shouldBe Collection()
  }
}

class TestBooleanTf extends TestTransformers {

  val cell = Cell(Position2D("foo", "bar"), getLongContent(3))

  "A BooleanTf" should "present" in {
    BooleanTf().present(cell) shouldBe Collection(Position2D("foo", "bar"), getDoubleContent(1))
  }

  it should "present with name" in {
    BooleanTf().andThenRename(Transformer.rename(First, "%1$s-%2$s.btf")).present(cell) shouldBe
      Collection(List(Cell(Position2D("foo-3.btf", "bar"), getDoubleContent(1))))
    BooleanTf().andThenRename(Transformer.rename(Second, "%1$s-%2$s.btf")).present(cell) shouldBe
      Collection(List(Cell(Position2D("foo", "bar-3.btf"), getDoubleContent(1))))
  }

  it should "not present with a categorical" in {
    BooleanTf().present(Cell(Position2D("foo", "bar"), getStringContent("baz"))) shouldBe Collection()
  }
}

class TestLogarithmicTf extends TestTransformers {

  val cell = Cell(Position2D("foo", "bar"), getLongContent(3))

  "A LogarithmicTf" should "present" in {
    LogarithmicTf().present(cell) shouldBe Collection(Position2D("foo", "bar"), getDoubleContent(1 + math.log(3)))
  }

  it should "present with name" in {
    LogarithmicTf().andThenRename(Transformer.rename(First, "%1$s-%2$s.ltf")).present(cell) shouldBe
      Collection(List(Cell(Position2D("foo-3.ltf", "bar"), getDoubleContent(1 + math.log(3)))))
    LogarithmicTf().andThenRename(Transformer.rename(Second, "%1$s-%2$s.ltf")).present(cell) shouldBe
      Collection(List(Cell(Position2D("foo", "bar-3.ltf"), getDoubleContent(1 + math.log(3)))))
  }

  it should "present with log" in {
    LogarithmicTf(math.log10 _).present(cell) shouldBe Collection(Position2D("foo", "bar"),
      getDoubleContent(1 + math.log10(3)))
  }

  it should "present with name and log" in {
    LogarithmicTf(math.log10 _).andThenRename(Transformer.rename(First, "%1$s-%2$s.ltf")).present(cell) shouldBe
      Collection(List(Cell(Position2D("foo-3.ltf", "bar"), getDoubleContent(1 + math.log10(3)))))
    LogarithmicTf(math.log10 _).andThenRename(Transformer.rename(Second, "%1$s-%2$s.ltf")).present(cell) shouldBe
      Collection(List(Cell(Position2D("foo", "bar-3.ltf"), getDoubleContent(1 + math.log10(3)))))
  }

  it should "not present with a categorical" in {
    LogarithmicTf().present(Cell(Position2D("foo", "bar"), getStringContent("baz"))) shouldBe Collection()
  }
}

class TestAugmentedTf extends TestTransformers {

  val cell = Cell(Position2D("foo", "bar"), getLongContent(1))
  val ext = Map(Position1D("foo") -> getLongContent(2), Position1D("bar") -> getLongContent(2))

  "An AugmentedTf" should "present" in {
    AugmentedTf(First).presentWithValue(cell, ext) shouldBe Collection(Position2D("foo", "bar"),
      getDoubleContent(0.5 + 0.5 * 1 / 2))
    AugmentedTf(Second).presentWithValue(cell, ext) shouldBe Collection(Position2D("foo", "bar"),
      getDoubleContent(0.5 + 0.5 * 1 / 2))
  }

  it should "present with name" in {
    AugmentedTf(First)
      .andThenRenameWithValue(TransformerWithValue.rename(First, "%1$s-%2$s.atf"))
      .presentWithValue(cell, ext) shouldBe Collection(List(Cell(Position2D("foo-1.atf", "bar"),
        getDoubleContent(0.5 + 0.5 * 1 / 2))))
    AugmentedTf(Second)
      .andThenRenameWithValue(TransformerWithValue.rename(Second, "%1$s-%2$s.atf"))
      .presentWithValue(cell, ext) shouldBe Collection(List(Cell(Position2D("foo", "bar-1.atf"),
        getDoubleContent(0.5 + 0.5 * 1 / 2))))
  }

  it should "not present with missing value" in {
    AugmentedTf(First).presentWithValue(Cell(Position2D("abc", "bar"), getLongContent(1)), ext) shouldBe Collection()
  }

  it should "not present with a categorical" in {
    AugmentedTf(First).presentWithValue(Cell(Position2D("foo", "bar"), getStringContent("baz")), ext) shouldBe
      Collection()
  }
}

class TestTfIdf extends TestTransformers {

  val cell = Cell(Position2D("foo", "bar"), getDoubleContent(1.5))
  val ext = Map(Position1D("foo") -> getDoubleContent(2), Position1D("bar") -> getDoubleContent(2),
    Position1D("baz") -> getDoubleContent(2))

  "A TfIdf" should "present" in {
    TfIdf(First).presentWithValue(cell, ext) shouldBe Collection(Position2D("foo", "bar"), getDoubleContent(3))
    TfIdf(Second).presentWithValue(cell, ext) shouldBe Collection(Position2D("foo", "bar"), getDoubleContent(3))
  }

  it should "present with name" in {
    TfIdf(First)
      .andThenRenameWithValue(TransformerWithValue.rename(First, "%1$s-%2$s.tfidf"))
      .presentWithValue(cell, ext) shouldBe Collection(List(Cell(Position2D("foo-1.5.tfidf", "bar"),
        getDoubleContent(3))))
    TfIdf(Second)
      .andThenRenameWithValue(TransformerWithValue.rename(Second, "%1$s-%2$s.tfidf"))
      .presentWithValue(cell, ext) shouldBe Collection(List(Cell(Position2D("foo", "bar-1.5.tfidf"),
        getDoubleContent(3))))
  }

  it should "present with key" in {
    TfIdf("baz").presentWithValue(cell, ext) shouldBe Collection(Position2D("foo", "bar"), getDoubleContent(3))
  }

  it should "present with name and key" in {
    TfIdf("baz")
      .andThenRenameWithValue(TransformerWithValue.rename(First, "%1$s-%2$s.tfidf"))
      .presentWithValue(cell, ext) shouldBe Collection(List(Cell(Position2D("foo-1.5.tfidf", "bar"),
        getDoubleContent(3))))
    TfIdf("baz")
      .andThenRenameWithValue(TransformerWithValue.rename(Second, "%1$s-%2$s.tfidf"))
      .presentWithValue(cell, ext) shouldBe Collection(List(Cell(Position2D("foo", "bar-1.5.tfidf"),
        getDoubleContent(3))))
  }

  it should "not present with missing key" in {
    TfIdf("not.there").presentWithValue(cell, ext) shouldBe Collection()
  }

  it should "not present with missing value" in {
    TfIdf(First).presentWithValue(Cell(Position2D("abc", "bar"), getDoubleContent(1.5)), ext) shouldBe Collection()
  }

  it should "not present with a categorical" in {
    TfIdf(First).presentWithValue(Cell(Position2D("foo", "bar"), getStringContent("baz")), ext) shouldBe Collection()
  }
}

class TestAdd extends TestTransformers {

  val cell = Cell(Position1D("foo"), getDoubleContent(1))
  val ext = Map(Position1D("foo") -> getDoubleContent(2), Position1D("bar") -> getDoubleContent(2))

  "An Add" should "present" in {
    Add(First).presentWithValue(cell, ext) shouldBe Collection(Position1D("foo"), getDoubleContent(3))
  }

  it should "present with name" in {
    Add(First)
      .andThenRenameWithValue(TransformerWithValue.rename(First, "%1$s-%2$s.idf"))
      .presentWithValue(cell, ext) shouldBe Collection(List(Cell(Position1D("foo-1.0.idf"), getDoubleContent(3))))
  }

  it should "present with key" in {
    Add("bar").presentWithValue(cell, ext) shouldBe Collection(Position1D("foo"), getDoubleContent(3))
  }

  it should "present with name and key" in {
    Add("bar")
      .andThenRenameWithValue(TransformerWithValue.rename(First, "%1$s-%2$s.idf"))
      .presentWithValue(cell, ext) shouldBe Collection(List(Cell(Position1D("foo-1.0.idf"), getDoubleContent(3))))
  }

  it should "not present with missing key" in {
    Add("not.there").presentWithValue(cell, ext) shouldBe Collection()
  }

  it should "not present with missing value" in {
    Add(First).presentWithValue(Cell(Position1D("abc"), getDoubleContent(1)), ext) shouldBe Collection()
  }

  it should "not present with a categorical" in {
    Add(First).presentWithValue(Cell(Position1D("foo"), getStringContent("bar")), ext) shouldBe Collection()
  }
}

class TestSubtract extends TestTransformers {

  val cell = Cell(Position1D("foo"), getDoubleContent(1))
  val ext = Map(Position1D("foo") -> getDoubleContent(2), Position1D("bar") -> getDoubleContent(2))

  "A Subtract" should "present" in {
    Subtract(First).presentWithValue(cell, ext) shouldBe Collection(Position1D("foo"), getDoubleContent(-1))
  }

  it should "present with name" in {
    Subtract(First)
      .andThenRenameWithValue(TransformerWithValue.rename(First, "%1$s-%2$s.idf"))
      .presentWithValue(cell, ext) shouldBe Collection(List(Cell(Position1D("foo-1.0.idf"), getDoubleContent(-1))))
  }

  it should "present with key" in {
    Subtract("bar").presentWithValue(cell, ext) shouldBe Collection(Position1D("foo"), getDoubleContent(-1))
  }

  it should "present with inverse" in {
    Subtract(First, true).presentWithValue(cell, ext) shouldBe Collection(Position1D("foo"), getDoubleContent(1))
  }

  it should "present with name and key" in {
    Subtract("bar")
      .andThenRenameWithValue(TransformerWithValue.rename(First, "%1$s-%2$s.idf"))
      .presentWithValue(cell, ext) shouldBe Collection(List(Cell(Position1D("foo-1.0.idf"), getDoubleContent(-1))))
  }

  it should "present with name and inverse" in {
    Subtract(First, true)
      .andThenRenameWithValue(TransformerWithValue.rename(First, "%1$s-%2$s.idf"))
      .presentWithValue(cell, ext) shouldBe Collection(List(Cell(Position1D("foo-1.0.idf"), getDoubleContent(1))))
  }

  it should "present with key and inverse" in {
    Subtract("bar", true).presentWithValue(cell, ext) shouldBe Collection(Position1D("foo"), getDoubleContent(1))
  }

  it should "present with name, key and inverse" in {
    Subtract("bar", true)
      .andThenRenameWithValue(TransformerWithValue.rename(First, "%1$s-%2$s.idf"))
      .presentWithValue(cell, ext) shouldBe Collection(List(Cell(Position1D("foo-1.0.idf"), getDoubleContent(1))))
  }

  it should "not present with missing key" in {
    Subtract("not.there").presentWithValue(cell, ext) shouldBe Collection()
  }

  it should "not present with missing value" in {
    Subtract(First).presentWithValue(Cell(Position1D("abc"), getDoubleContent(1)), ext) shouldBe Collection()
  }

  it should "not present with a categorical" in {
    Subtract(First).presentWithValue(Cell(Position1D("foo"), getStringContent("bar")), ext) shouldBe Collection()
  }
}

class TestMultiply extends TestTransformers {

  val cell = Cell(Position1D("foo"), getDoubleContent(1))
  val ext = Map(Position1D("foo") -> getDoubleContent(2), Position1D("bar") -> getDoubleContent(2))

  "A Multiply" should "present" in {
    Multiply(First).presentWithValue(cell, ext) shouldBe Collection(Position1D("foo"), getDoubleContent(2))
  }

  it should "present with name" in {
    Multiply(First)
      .andThenRenameWithValue(TransformerWithValue.rename(First, "%1$s-%2$s.idf"))
      .presentWithValue(cell, ext) shouldBe Collection(List(Cell(Position1D("foo-1.0.idf"), getDoubleContent(2))))
  }

  it should "present with key" in {
    Multiply("bar").presentWithValue(cell, ext) shouldBe Collection(Position1D("foo"), getDoubleContent(2))
  }

  it should "present with name and key" in {
    Multiply("bar")
      .andThenRenameWithValue(TransformerWithValue.rename(First, "%1$s-%2$s.idf"))
      .presentWithValue(cell, ext) shouldBe Collection(List(Cell(Position1D("foo-1.0.idf"), getDoubleContent(2))))
  }

  it should "not present with missing key" in {
    Multiply("not.there").presentWithValue(cell, ext) shouldBe Collection()
  }

  it should "not present with missing value" in {
    Multiply(First).presentWithValue(Cell(Position1D("abc"), getDoubleContent(1)), ext) shouldBe Collection()
  }

  it should "not present with a categorical" in {
    Multiply(First).presentWithValue(Cell(Position1D("foo"), getStringContent("bar")), ext) shouldBe Collection()
  }
}

class TestFraction extends TestTransformers {

  val cell = Cell(Position1D("foo"), getDoubleContent(1))
  val ext = Map(Position1D("foo") -> getDoubleContent(2), Position1D("bar") -> getDoubleContent(2))

  "A Fraction" should "present" in {
    Fraction(First).presentWithValue(cell, ext) shouldBe Collection(Position1D("foo"), getDoubleContent(0.5))
  }

  it should "present with name" in {
    Fraction(First)
      .andThenRenameWithValue(TransformerWithValue.rename(First, "%1$s-%2$s.idf"))
      .presentWithValue(cell, ext) shouldBe Collection(List(Cell(Position1D("foo-1.0.idf"), getDoubleContent(0.5))))
  }

  it should "present with key" in {
    Fraction("bar").presentWithValue(cell, ext) shouldBe Collection(Position1D("foo"), getDoubleContent(0.5))
  }

  it should "present with inverse" in {
    Fraction(First, true).presentWithValue(cell, ext) shouldBe Collection(Position1D("foo"), getDoubleContent(2))
  }

  it should "present with name and key" in {
    Fraction("bar")
      .andThenRenameWithValue(TransformerWithValue.rename(First, "%1$s-%2$s.idf"))
      .presentWithValue(cell, ext) shouldBe Collection(List(Cell(Position1D("foo-1.0.idf"), getDoubleContent(0.5))))
  }

  it should "present with name and inverse" in {
    Fraction(First, true)
      .andThenRenameWithValue(TransformerWithValue.rename(First, "%1$s-%2$s.idf"))
      .presentWithValue(cell, ext) shouldBe Collection(List(Cell(Position1D("foo-1.0.idf"), getDoubleContent(2))))
  }

  it should "present with key and inverse" in {
    Fraction("bar", true).presentWithValue(cell, ext) shouldBe Collection(Position1D("foo"), getDoubleContent(2))
  }

  it should "present with name, key and inverse" in {
    Fraction("bar", true)
      .andThenRenameWithValue(TransformerWithValue.rename(First, "%1$s-%2$s.idf"))
      .presentWithValue(cell, ext) shouldBe Collection(List(Cell(Position1D("foo-1.0.idf"), getDoubleContent(2))))
  }

  it should "not present with missing key" in {
    Fraction("not.there").presentWithValue(cell, ext) shouldBe Collection()
  }

  it should "not present with missing value" in {
    Fraction(First).presentWithValue(Cell(Position1D("abc"), getDoubleContent(1)), ext) shouldBe Collection()
  }

  it should "not present with a categorical" in {
    Fraction(First).presentWithValue(Cell(Position1D("foo"), getStringContent("bar")), ext) shouldBe Collection()
  }
}

class TestPower extends TestTransformers {

  val cell = Cell(Position1D("foo"), getDoubleContent(3.1415))

  "A Power" should "present" in {
    Power(2).present(cell) shouldBe Collection(Position1D("foo"), getDoubleContent(3.1415 * 3.1415))
  }

  it should "present with name" in {
    Power(2).andThenRename(Transformer.rename(First, "%1$s-%2$s.pwr")).present(cell) shouldBe
      Collection(List(Cell(Position1D("foo-3.1415.pwr"), getDoubleContent(3.1415 * 3.1415))))
  }
}

class TestSquareRoot extends TestTransformers {

  val cell = Cell(Position1D("foo"), getDoubleContent(3.1415))

  "A SquareRoot" should "present" in {
    SquareRoot().present(cell) shouldBe Collection(Position1D("foo"), getDoubleContent(math.sqrt(3.1415)))
  }

  it should "present with name" in {
    SquareRoot().andThenRename(Transformer.rename(First, "%1$s-%2$s.sqr")).present(cell) shouldBe
      Collection(List(Cell(Position1D("foo-3.1415.sqr"), getDoubleContent(math.sqrt(3.1415)))))
  }
}

class TestCut extends TestTransformers {

  val cell = Cell(Position1D("foo"), getDoubleContent(3.1415))
  val ext = Map(Position1D("foo") -> List[Double](0,1,2,3,4,5))

  "A Cut" should "present" in {
    Cut(First).presentWithValue(cell, ext) shouldBe Collection(Position1D("foo"),
      Content(OrdinalSchema[Codex.StringCodex](List("(0.0,1.0]", "(1.0,2.0]", "(2.0,3.0]", "(3.0,4.0]", "(4.0,5.0]")),
        "(3.0,4.0]"))
  }

  it should "present with name" in {
    Cut(First)
      .andThenRenameWithValue(TransformerWithValue.rename(First, "%1$s.cut"))
      .presentWithValue(cell, ext) shouldBe Collection(List(Cell(Position1D("foo.cut"),
        Content(OrdinalSchema[Codex.StringCodex](List("(0.0,1.0]", "(1.0,2.0]", "(2.0,3.0]", "(3.0,4.0]",
          "(4.0,5.0]")), "(3.0,4.0]"))))
  }

  it should "not present with missing bins" in {
    Cut(First).presentWithValue(cell, Map()) shouldBe Collection()
  }
}

class TestScaldingCutRules extends TestTransformers {

  import au.com.cba.omnia.grimlock.scalding.transform.CutRules

  import com.twitter.scalding.typed.ValuePipe

  val stats = ValuePipe(Map(Position1D("foo") -> Map(Position1D("min") -> getDoubleContent(0),
    Position1D("max") -> getDoubleContent(5), Position1D("count") -> getDoubleContent(25),
    Position1D("sd") -> getDoubleContent(2), Position1D("skewness") -> getDoubleContent(2))))

  "A fixed" should "cut" in {
    CutRules.fixed(stats, "min", "max", 5) shouldBe ValuePipe(Map(Position1D("foo") -> List[Double](-0.005,1,2,3,4,5)))
  }

  it should "not cut with missing values" in {
    CutRules.fixed(stats, "not.there", "max", 5) shouldBe ValuePipe(Map())
    CutRules.fixed(stats, "min", "not.there", 5) shouldBe ValuePipe(Map())
  }

  "A squareRootChoice" should "cut" in {
    CutRules.squareRootChoice(stats, "count", "min", "max") shouldBe
      ValuePipe(Map(Position1D("foo") -> List[Double](-0.005,1,2,3,4,5)))
  }

  it should "not cut with missing values" in {
    CutRules.squareRootChoice(stats, "not.there", "min", "max") shouldBe ValuePipe(Map())
    CutRules.squareRootChoice(stats, "count", "not.there", "max") shouldBe ValuePipe(Map())
    CutRules.squareRootChoice(stats, "count", "min", "not.there") shouldBe ValuePipe(Map())
  }

  "A sturgesFormula" should "cut" in {
    // math.ceil(log2(25) + 1) = 6
    val vals = -0.005 +: (0.0 to 5.0 by (5.0 / 6)).tail

    CutRules.sturgesFormula(stats, "count", "min", "max") shouldBe ValuePipe(Map(Position1D("foo") -> vals))
  }

  it should "not cut with missing values" in {
    CutRules.sturgesFormula(stats, "not.there", "min", "max") shouldBe ValuePipe(Map())
    CutRules.sturgesFormula(stats, "count", "not.there", "max") shouldBe ValuePipe(Map())
    CutRules.sturgesFormula(stats, "count", "min", "not.there") shouldBe ValuePipe(Map())
  }

  "A riceRule" should "cut" in {
    // math.ceil(2 * math.pow(25, 1.0 / 3.0)) = 6
    val vals = -0.005 +: (0.0 to 5.0 by (5.0 / 6)).tail

    CutRules.riceRule(stats, "count", "min", "max") shouldBe ValuePipe(Map(Position1D("foo") -> vals))
  }

  it should "not cut with missing values" in {
    CutRules.riceRule(stats, "not.there", "min", "max") shouldBe ValuePipe(Map())
    CutRules.riceRule(stats, "count", "not.there", "max") shouldBe ValuePipe(Map())
    CutRules.riceRule(stats, "count", "min", "not.there") shouldBe ValuePipe(Map())
  }

  "A doanesFormula" should "cut" in {
    // math.round(1 + log2(25) + log2(1 + math.abs(2) / math.sqrt((6.0 * 23.0) / (26.0 * 28.0)))) = 8
    val vals = -0.005 +: (0.0 to 5.0 by (5.0 / 8)).tail

    CutRules.doanesFormula(stats, "count", "min", "max", "skewness") shouldBe ValuePipe(Map(Position1D("foo") -> vals))
  }

  it should "not cut with missing values" in {
    CutRules.doanesFormula(stats, "not.there", "min", "max", "skewness") shouldBe ValuePipe(Map())
    CutRules.doanesFormula(stats, "count", "not.there", "max", "skewness") shouldBe ValuePipe(Map())
    CutRules.doanesFormula(stats, "count", "min", "not.there", "skewness") shouldBe ValuePipe(Map())
    CutRules.doanesFormula(stats, "count", "min", "max", "not.there") shouldBe ValuePipe(Map())
  }

  "A scottsNormalReferenceRule" should "cut" in {
    // math.ceil((5.0 - 0) / (3.5 * 2 / math.pow(25, 1.0 / 3.0))) = 3
    val vals = -0.005 +: (0.0 to 5.0 by (5.0 / 3)).tail

    CutRules.scottsNormalReferenceRule(stats, "count", "min", "max", "sd") shouldBe
      ValuePipe(Map(Position1D("foo") -> vals))
  }

  it should "not cut with missing values" in {
    CutRules.scottsNormalReferenceRule(stats, "not.there", "min", "max", "sd") shouldBe ValuePipe(Map())
    CutRules.scottsNormalReferenceRule(stats, "count", "not.there", "max", "sd") shouldBe ValuePipe(Map())
    CutRules.scottsNormalReferenceRule(stats, "count", "min", "not.there", "sd") shouldBe ValuePipe(Map())
    CutRules.scottsNormalReferenceRule(stats, "count", "min", "max", "not.there") shouldBe ValuePipe(Map())
  }

  "A breaks" should "cut" in {
    CutRules.breaks(Map("foo" -> List[Double](0,1,2,3,4,5))) shouldBe
      ValuePipe(Map(Position1D("foo") -> List[Double](0,1,2,3,4,5)))
  }
}

class TestSparkCutRules extends TestTransformers {

  import au.com.cba.omnia.grimlock.spark.transform.CutRules

  val stats = Map(Position1D("foo") -> Map(Position1D("min") -> getDoubleContent(0),
    Position1D("max") -> getDoubleContent(5), Position1D("count") -> getDoubleContent(25),
    Position1D("sd") -> getDoubleContent(2), Position1D("skewness") -> getDoubleContent(2)))

  "A fixed" should "cut" in {
    CutRules.fixed(stats, "min", "max", 5) shouldBe Map(Position1D("foo") -> List[Double](-0.005,1,2,3,4,5))
  }

  it should "not cut with missing values" in {
    CutRules.fixed(stats, "not.there", "max", 5) shouldBe Map()
    CutRules.fixed(stats, "min", "not.there", 5) shouldBe Map()
  }

  "A squareRootChoice" should "cut" in {
    CutRules.squareRootChoice(stats, "count", "min", "max") shouldBe
      Map(Position1D("foo") -> List[Double](-0.005,1,2,3,4,5))
  }

  it should "not cut with missing values" in {
    CutRules.squareRootChoice(stats, "not.there", "min", "max") shouldBe Map()
    CutRules.squareRootChoice(stats, "count", "not.there", "max") shouldBe Map()
    CutRules.squareRootChoice(stats, "count", "min", "not.there") shouldBe Map()
  }

  "A sturgesFormula" should "cut" in {
    // math.ceil(log2(25) + 1) = 6
    val vals = -0.005 +: (0.0 to 5.0 by (5.0 / 6)).tail

    CutRules.sturgesFormula(stats, "count", "min", "max") shouldBe Map(Position1D("foo") -> vals)
  }

  it should "not cut with missing values" in {
    CutRules.sturgesFormula(stats, "not.there", "min", "max") shouldBe Map()
    CutRules.sturgesFormula(stats, "count", "not.there", "max") shouldBe Map()
    CutRules.sturgesFormula(stats, "count", "min", "not.there") shouldBe Map()
  }

  "A riceRule" should "cut" in {
    // math.ceil(2 * math.pow(25, 1.0 / 3.0)) = 6
    val vals = -0.005 +: (0.0 to 5.0 by (5.0 / 6)).tail

    CutRules.riceRule(stats, "count", "min", "max") shouldBe Map(Position1D("foo") -> vals)
  }

  it should "not cut with missing values" in {
    CutRules.riceRule(stats, "not.there", "min", "max") shouldBe Map()
    CutRules.riceRule(stats, "count", "not.there", "max") shouldBe Map()
    CutRules.riceRule(stats, "count", "min", "not.there") shouldBe Map()
  }

  "A doanesFormula" should "cut" in {
    // math.round(1 + log2(25) + log2(1 + math.abs(2) / math.sqrt((6.0 * 23.0) / (26.0 * 28.0)))) = 8
    val vals = -0.005 +: (0.0 to 5.0 by (5.0 / 8)).tail

    CutRules.doanesFormula(stats, "count", "min", "max", "skewness") shouldBe Map(Position1D("foo") -> vals)
  }

  it should "not cut with missing values" in {
    CutRules.doanesFormula(stats, "not.there", "min", "max", "skewness") shouldBe Map()
    CutRules.doanesFormula(stats, "count", "not.there", "max", "skewness") shouldBe Map()
    CutRules.doanesFormula(stats, "count", "min", "not.there", "skewness") shouldBe Map()
    CutRules.doanesFormula(stats, "count", "min", "max", "not.there") shouldBe Map()
  }

  "A scottsNormalReferenceRule" should "cut" in {
    // math.ceil((5.0 - 0) / (3.5 * 2 / math.pow(25, 1.0 / 3.0))) = 3
    val vals = -0.005 +: (0.0 to 5.0 by (5.0 / 3)).tail

    CutRules.scottsNormalReferenceRule(stats, "count", "min", "max", "sd") shouldBe Map(Position1D("foo") -> vals)
  }

  it should "not cut with missing values" in {
    CutRules.scottsNormalReferenceRule(stats, "not.there", "min", "max", "sd") shouldBe Map()
    CutRules.scottsNormalReferenceRule(stats, "count", "not.there", "max", "sd") shouldBe Map()
    CutRules.scottsNormalReferenceRule(stats, "count", "min", "not.there", "sd") shouldBe Map()
    CutRules.scottsNormalReferenceRule(stats, "count", "min", "max", "not.there") shouldBe Map()
  }

  "A breaks" should "cut" in {
    CutRules.breaks(Map("foo" -> List[Double](0,1,2,3,4,5))) shouldBe
      Map(Position1D("foo") -> List[Double](0,1,2,3,4,5))
  }
}

class TestAndThenTransformer extends TestTransformers {

  val cell = Cell(Position1D("foo"), getStringContent("rules"))

  "An AndThenTransformer" should "present" in {
    Binarise[Position1D](First).andThen(Indicator().andThenRename(Transformer.rename(First, "%1$s.ind")))
      .present(cell) shouldBe Collection(List(Cell(Position1D("foo=rules.ind"), getLongContent(1))))
  }
}

class TestAndThenTransformerWithValue extends TestTransformers {

  val cell = Cell(Position1D("foo"), getDoubleContent(3.1415))
  val ext = Map(Position1D("foo") -> Map(Position1D("min") -> getDoubleContent(0),
    Position1D("max") -> getDoubleContent(2), Position1D("max.abs") -> getDoubleContent(3)))

  "An AndThenTransformerWithValue" should "present" in {
    Clamp[Position1D, String](First, "min", "max")
      .andThenWithValue(Normalise(First, "max.abs"))
      .presentWithValue(cell, ext) shouldBe Collection(List(Cell(Position1D("foo"), getDoubleContent(2.0 / 3.0))))
  }
}

