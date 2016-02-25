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
import au.com.cba.omnia.grimlock.framework.encoding._
import au.com.cba.omnia.grimlock.framework.position._

import au.com.cba.omnia.grimlock.library.transform._

trait TestTransformers extends TestGrimlock {
  def getLongContent(value: Long): Content = Content(DiscreteSchema[Long](), value)
  def getDoubleContent(value: Double): Content = Content(ContinuousSchema[Double](), value)
  def getStringContent(value: String): Content = Content(NominalSchema[String](), value)

  def extractor[P <: Position](dim: Dimension, key: String)(
    implicit ev: PosDimDep[P, dim.type]): Extract[P, Map[Position1D, Map[Position1D, Content]], Double] = {
    ExtractWithDimensionAndKey[P, Content](dim, key).andThenPresent(_.value.asDouble)
  }

  def dimExtractor[P <: Position](dim: Dimension)(
    implicit ev: PosDimDep[P, dim.type]): Extract[P, Map[Position1D, Content], Double] = {
    ExtractWithDimension[P, Content](dim).andThenPresent(_.value.asDouble)
  }

  def keyExtractor[P <: Position](key: String): Extract[P, Map[Position1D, Content], Double] = {
    ExtractWithKey[P, Content](key).andThenPresent(_.value.asDouble)
  }
}

class TestIndicator extends TestTransformers {

  val cell = Cell(Position2D("foo", "bar"), getDoubleContent(3.1415))

  "An Indicator" should "present" in {
    Indicator().present(cell) shouldBe List(Cell(Position2D("foo", "bar"), getLongContent(1)))
  }

  it should "present with name" in {
    Indicator[Position2D]()
      .andThenRelocate(Locate.RenameDimension(First, "%1$s.ind"))
      .present(cell).toList shouldBe List(Cell(Position2D("foo.ind", "bar"), getLongContent(1)))
    Indicator[Position2D]()
      .andThenRelocate(Locate.RenameDimension(Second, "%1$s.ind"))
      .present(cell).toList shouldBe List(Cell(Position2D("foo", "bar.ind"), getLongContent(1)))
  }
}

class TestBinarise extends TestTransformers {

  val cell = Cell(Position2D("foo", "bar"), getStringContent("rules"))

  "A Binarise" should "present" in {
    Binarise[Position2D](Locate.RenameDimensionWithContent(First))
      .present(cell) shouldBe List(Cell(Position2D("foo=rules", "bar"), getLongContent(1)))
    Binarise[Position2D](Locate.RenameDimensionWithContent(Second))
      .present(cell) shouldBe List(Cell(Position2D("foo", "bar=rules"), getLongContent(1)))
  }

  it should "present with name" in {
    Binarise[Position2D](Locate.RenameDimensionWithContent(First, "%1$s.%2$s"))
      .present(cell) shouldBe List(Cell(Position2D("foo.rules", "bar"), getLongContent(1)))
    Binarise[Position2D](Locate.RenameDimensionWithContent(Second, "%1$s.%2$s"))
      .present(cell) shouldBe List(Cell(Position2D("foo", "bar.rules"), getLongContent(1)))
  }

  it should "not present a numerical" in {
    Binarise[Position1D](Locate.RenameDimensionWithContent(First))
      .present(Cell(Position1D("foo"), getDoubleContent(3.1415))) shouldBe List()
  }
}

class TestNormalise extends TestTransformers {

  val cell = Cell(Position2D("foo", "bar"), getDoubleContent(3.1415))
  val ext = Map(Position1D("foo") -> Map(Position1D("const") -> getDoubleContent(6.283)),
    Position1D("bar") -> Map(Position1D("const") -> getDoubleContent(-1.57075)))

  "An Normalise" should "present" in {
    Normalise(extractor[Position2D](First, "const"))
      .presentWithValue(cell, ext) shouldBe List(Cell(Position2D("foo", "bar"), getDoubleContent(0.5)))
    Normalise(extractor[Position2D](Second, "const"))
      .presentWithValue(cell, ext) shouldBe List(Cell(Position2D("foo", "bar"), getDoubleContent(-2)))
  }

  it should "present with name" in {
    Normalise(extractor[Position2D](First, "const"))
      .andThenRelocate(Locate.RenameDimension(First, "%1$s.norm"))
      .presentWithValue(cell, ext).toList shouldBe List(Cell(Position2D("foo.norm", "bar"),
        getDoubleContent(0.5)))
    Normalise(extractor[Position2D](Second, "const"))
      .andThenRelocate(Locate.RenameDimension(Second, "%1$s.norm"))
      .presentWithValue(cell, ext).toList shouldBe List(Cell(Position2D("foo", "bar.norm"),
        getDoubleContent(-2)))
  }

  it should "not present with missing key" in {
    Normalise(extractor[Position2D](First, "not.there"))
      .presentWithValue(cell, ext) shouldBe List()
  }

  it should "not present with missing value" in {
    Normalise(extractor[Position1D](First, "const"))
      .presentWithValue(Cell(Position1D("baz"), getDoubleContent(3.1415)), ext) shouldBe List()
  }

  it should "not present with a categorical" in {
    Normalise(extractor[Position1D](First, "const"))
      .presentWithValue(Cell(Position1D("foo"), getStringContent("bar")), ext) shouldBe List()
  }
}

class TestStandardise extends TestTransformers {

  val cell = Cell(Position2D("foo", "bar"), getDoubleContent(3.1415))
  val ext = Map(
    Position1D("foo") -> Map(Position1D("mean") -> getDoubleContent(0.75), Position1D("sd") -> getDoubleContent(1.25)),
    Position1D("bar") -> Map(Position1D("mean") -> getDoubleContent(-0.75), Position1D("sd") -> getDoubleContent(0.75)))

  "A Standardise" should "present" in {
    Standardise(extractor[Position2D](First, "mean"), extractor[Position2D](First, "sd"))
      .presentWithValue(cell, ext) shouldBe List(Cell(Position2D("foo", "bar"),
        getDoubleContent((3.1415 - 0.75) / 1.25)))
    Standardise(extractor[Position2D](Second, "mean"), extractor[Position2D](Second, "sd"))
      .presentWithValue(cell, ext) shouldBe List(Cell(Position2D("foo", "bar"),
        getDoubleContent((3.1415 + 0.75) / 0.75)))
  }

  it should "present with name" in {
    Standardise(extractor[Position2D](First, "mean"), extractor[Position2D](First, "sd"))
      .andThenRelocate(Locate.RenameDimension(First, "%1$s.std"))
      .presentWithValue(cell, ext).toList shouldBe List(Cell(Position2D("foo.std", "bar"),
        getDoubleContent((3.1415 - 0.75) / 1.25)))
    Standardise(extractor[Position2D](Second, "mean"), extractor[Position2D](Second, "sd"))
      .andThenRelocate(Locate.RenameDimension(Second, "%1$s.std"))
      .presentWithValue(cell, ext).toList shouldBe List(Cell(Position2D("foo", "bar.std"),
        getDoubleContent((3.1415 + 0.75) / 0.75)))
  }

  it should "present with threshold" in {
    Standardise(extractor[Position2D](First, "mean"), extractor[Position2D](First, "sd"), 1.0)
      .presentWithValue(cell, ext) shouldBe List(Cell(Position2D("foo", "bar"),
        getDoubleContent((3.1415 - 0.75) / 1.25)))
    Standardise(extractor[Position2D](Second, "mean"), extractor[Position2D](Second, "sd"), 1.0)
      .presentWithValue(cell, ext) shouldBe List(Cell(Position2D("foo", "bar"), getDoubleContent(0)))
  }

  it should "present with N" in {
    Standardise(extractor[Position2D](First, "mean"), extractor[Position2D](First, "sd"), n=2)
      .presentWithValue(cell, ext) shouldBe List(Cell(Position2D("foo", "bar"),
        getDoubleContent((3.1415 - 0.75) / (2 * 1.25))))
    Standardise(extractor[Position2D](Second, "mean"), extractor[Position2D](Second, "sd"), n=2)
      .presentWithValue(cell, ext) shouldBe List(Cell(Position2D("foo", "bar"),
        getDoubleContent((3.1415 + 0.75) / (2 * 0.75))))
  }

  it should "present with name and threshold" in {
    Standardise(extractor[Position2D](First, "mean"), extractor[Position2D](First, "sd"), 1.0)
      .andThenRelocate(Locate.RenameDimension(First, "%1$s.std"))
      .presentWithValue(cell, ext).toList shouldBe List(Cell(Position2D("foo.std", "bar"),
        getDoubleContent((3.1415 - 0.75) / 1.25)))
    Standardise(extractor[Position2D](Second, "mean"), extractor[Position2D](Second, "sd"), 1.0)
      .andThenRelocate(Locate.RenameDimension(Second, "%1$s.std"))
      .presentWithValue(cell, ext).toList shouldBe List(Cell(Position2D("foo", "bar.std"),
        getDoubleContent(0)))
  }

  it should "present with name and N" in {
    Standardise(extractor[Position2D](First, "mean"), extractor[Position2D](First, "sd"), n=2)
      .andThenRelocate(Locate.RenameDimension(First, "%1$s.std"))
      .presentWithValue(cell, ext).toList shouldBe List(Cell(Position2D("foo.std", "bar"),
        getDoubleContent((3.1415 - 0.75) / (2 * 1.25))))
    Standardise(extractor[Position2D](Second, "mean"), extractor[Position2D](Second, "sd"), n=2)
      .andThenRelocate(Locate.RenameDimension(Second, "%1$s.std"))
      .presentWithValue(cell, ext).toList shouldBe List(Cell(Position2D("foo", "bar.std"),
        getDoubleContent((3.1415 + 0.75) / (2 * 0.75))))
  }

  it should "present with threshold and N" in {
    Standardise(extractor[Position2D](First, "mean"), extractor[Position2D](First, "sd"), 1.0, 2)
      .presentWithValue(cell, ext) shouldBe List(Cell(Position2D("foo", "bar"),
        getDoubleContent((3.1415 - 0.75) / (2 * 1.25))))
    Standardise(extractor[Position2D](Second, "mean"), extractor[Position2D](Second, "sd"), 1.0, 2)
      .presentWithValue(cell, ext) shouldBe List(Cell(Position2D("foo", "bar"), getDoubleContent(0)))
  }

  it should "present with name, threshold and N" in {
    Standardise(extractor[Position2D](First, "mean"), extractor[Position2D](First, "sd"), 1.0, 2)
      .andThenRelocate(Locate.RenameDimension(First, "%1$s.std"))
      .presentWithValue(cell, ext).toList shouldBe List(Cell(Position2D("foo.std", "bar"),
        getDoubleContent((3.1415 - 0.75) / (2 * 1.25))))
    Standardise(extractor[Position2D](Second, "mean"), extractor[Position2D](Second, "sd"), 1.0, 2)
      .andThenRelocate(Locate.RenameDimension(Second, "%1$s.std"))
      .presentWithValue(cell, ext).toList shouldBe List(Cell(Position2D("foo", "bar.std"), getDoubleContent(0)))
  }

  it should "not present with missing key" in {
    Standardise(extractor[Position2D](First, "not.there"), extractor[Position2D](First, "sd"))
      .presentWithValue(cell, ext) shouldBe List()
    Standardise(extractor[Position2D](First, "mean"), extractor[Position2D](First, "not.there"))
      .presentWithValue(cell, ext) shouldBe List()
  }

  it should "not present with missing value" in {
    Standardise(extractor[Position1D](First, "mean"), extractor[Position1D](First, "sd"))
      .presentWithValue(Cell(Position1D("baz"), getDoubleContent(3.1415)), ext) shouldBe List()
  }

  it should "not present with a categorical" in {
    Standardise(extractor[Position1D](First, "mean"), extractor[Position1D](First, "sd"))
      .presentWithValue(Cell(Position1D("foo"), getStringContent("bar")), ext) shouldBe List()
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
    Clamp(extractor[Position3D](First, "min"), extractor[Position3D](First, "max"))
      .presentWithValue(cell, ext) shouldBe List(Cell(Position3D("foo", "bar", "baz"), getDoubleContent(3.1415)))
    Clamp(extractor[Position3D](Second, "min"), extractor[Position3D](Second, "max"))
      .presentWithValue(cell, ext) shouldBe List(Cell(Position3D("foo", "bar", "baz"), getDoubleContent(1.57075)))
    Clamp(extractor[Position3D](Third, "min"), extractor[Position3D](Third, "max"))
      .presentWithValue(cell, ext) shouldBe List(Cell(Position3D("foo", "bar", "baz"), getDoubleContent(4.71225)))
  }

  it should "present with name" in {
    Clamp(extractor[Position3D](First, "min"), extractor[Position3D](First, "max"))
      .andThenRelocate(Locate.RenameDimension(First, "%1$s.std"))
      .presentWithValue(cell, ext).toList shouldBe List(Cell(Position3D("foo.std", "bar", "baz"),
        getDoubleContent(3.1415)))
    Clamp(extractor[Position3D](Second, "min"), extractor[Position3D](Second, "max"))
      .andThenRelocate(Locate.RenameDimension(Second, "%1$s.std"))
      .presentWithValue(cell, ext).toList shouldBe List(Cell(Position3D("foo", "bar.std", "baz"),
        getDoubleContent(1.57075)))
    Clamp(extractor[Position3D](Third, "min"), extractor[Position3D](Third, "max"))
      .andThenRelocate(Locate.RenameDimension(Third, "%1$s.std"))
      .presentWithValue(cell, ext).toList shouldBe List(Cell(Position3D("foo", "bar", "baz.std"),
        getDoubleContent(4.71225)))
  }

  it should "not present with missing key" in {
    Clamp(extractor[Position3D](First, "not.there"), extractor[Position3D](First, "max"))
      .presentWithValue(cell, ext) shouldBe List()
    Clamp(extractor[Position3D](First, "min"), extractor[Position3D](First, "not.there"))
      .presentWithValue(cell, ext) shouldBe List()
  }

  it should "not present with missing value" in {
    Clamp(extractor[Position1D](First, "min"), extractor[Position1D](First, "max"))
      .presentWithValue(Cell(Position1D("abc"), getDoubleContent(3.1415)), ext) shouldBe List()
  }

  it should "not present with a categorical" in {
    Clamp(extractor[Position1D](First, "min"), extractor[Position1D](First, "max"))
      .presentWithValue(Cell(Position1D("foo"), getStringContent("bar")), ext) shouldBe List()
  }
}

class TestIdf extends TestTransformers {

  val cell = Cell(Position1D("foo"), getLongContent(1))
  val ext = Map(Position1D("foo") -> getLongContent(2), Position1D("bar") -> getLongContent(2))

  "An Idf" should "present" in {
    Idf(dimExtractor[Position1D](First))
      .presentWithValue(cell, ext) shouldBe List(Cell(Position1D("foo"), getDoubleContent(0)))
  }

  it should "present with name" in {
    Idf(dimExtractor[Position1D](First))
      .andThenRelocate(Locate.RenameDimension(First, "%1$s.idf"))
      .presentWithValue(cell, ext).toList shouldBe List(Cell(Position1D("foo.idf"), getDoubleContent(0)))
  }

  it should "present with key" in {
    Idf(keyExtractor[Position1D]("bar"))
      .presentWithValue(cell, ext) shouldBe List(Cell(Position1D("foo"), getDoubleContent(0)))
  }

  it should "present with function" in {
    Idf(dimExtractor[Position1D](First), (df, n) => math.log10(n / df))
      .presentWithValue(cell, ext) shouldBe List(Cell(Position1D("foo"), getDoubleContent(math.log10(2))))
  }

  it should "present with name and key" in {
    Idf(keyExtractor[Position1D]("bar"))
      .andThenRelocate(Locate.RenameDimension(First, "%1$s.idf"))
      .presentWithValue(cell, ext).toList shouldBe List(Cell(Position1D("foo.idf"), getDoubleContent(0)))
  }

  it should "present with name and function" in {
    Idf(dimExtractor[Position1D](First), (df, n) => math.log10(n / df))
      .andThenRelocate(Locate.RenameDimension(First, "%1$s.idf"))
      .presentWithValue(cell, ext).toList shouldBe List(Cell(Position1D("foo.idf"), getDoubleContent(math.log10(2))))
  }

  it should "present with key and function" in {
    Idf(keyExtractor[Position1D]("bar"), (df, n) => math.log10(n / df))
      .presentWithValue(cell, ext) shouldBe List(Cell(Position1D("foo"), getDoubleContent(math.log10(2))))
  }

  it should "present with name, key and function" in {
    Idf(keyExtractor[Position1D]("bar"), (df, n) => math.log10(n / df))
      .andThenRelocate(Locate.RenameDimension(First, "%1$s.idf"))
      .presentWithValue(cell, ext).toList shouldBe List(Cell(Position1D("foo.idf"), getDoubleContent(math.log10(2))))
  }

  it should "not present with missing key" in {
    Idf(keyExtractor[Position1D]("not.there"))
      .presentWithValue(cell, ext) shouldBe List()
  }

  it should "not present with missing value" in {
    Idf(dimExtractor[Position1D](First))
      .presentWithValue(Cell(Position1D("abc"), getLongContent(1)), ext) shouldBe List()
  }

  it should "not present with a categorical" in {
    Idf(dimExtractor[Position1D](First))
      .presentWithValue(Cell(Position1D("foo"), getStringContent("bar")), ext) shouldBe List()
  }
}

class TestBooleanTf extends TestTransformers {

  val cell = Cell(Position2D("foo", "bar"), getLongContent(3))

  "A BooleanTf" should "present" in {
    BooleanTf().present(cell) shouldBe List(Cell(Position2D("foo", "bar"), getDoubleContent(1)))
  }

  it should "present with name" in {
    BooleanTf[Position2D]()
      .andThenRelocate(Locate.RenameDimension(First, "%1$s.btf"))
      .present(cell).toList shouldBe List(Cell(Position2D("foo.btf", "bar"), getDoubleContent(1)))
    BooleanTf[Position2D]()
      .andThenRelocate(Locate.RenameDimension(Second, "%1$s.btf"))
      .present(cell).toList shouldBe List(Cell(Position2D("foo", "bar.btf"), getDoubleContent(1)))
  }

  it should "not present with a categorical" in {
    BooleanTf().present(Cell(Position2D("foo", "bar"), getStringContent("baz"))) shouldBe List()
  }
}

class TestLogarithmicTf extends TestTransformers {

  val cell = Cell(Position2D("foo", "bar"), getLongContent(3))

  "A LogarithmicTf" should "present" in {
    LogarithmicTf().present(cell) shouldBe List(Cell(Position2D("foo", "bar"), getDoubleContent(1 + math.log(3))))
  }

  it should "present with name" in {
    LogarithmicTf[Position2D]()
      .andThenRelocate(Locate.RenameDimension(First, "%1$s.ltf"))
      .present(cell).toList shouldBe List(Cell(Position2D("foo.ltf", "bar"), getDoubleContent(1 + math.log(3))))
    LogarithmicTf[Position2D]()
      .andThenRelocate(Locate.RenameDimension(Second, "%1$s.ltf"))
      .present(cell).toList shouldBe List(Cell(Position2D("foo", "bar.ltf"), getDoubleContent(1 + math.log(3))))
  }

  it should "present with log" in {
    LogarithmicTf(math.log10 _).present(cell) shouldBe List(Cell(Position2D("foo", "bar"),
      getDoubleContent(1 + math.log10(3))))
  }

  it should "present with name and log" in {
    LogarithmicTf[Position2D](math.log10 _)
      .andThenRelocate(Locate.RenameDimension(First, "%1$s.ltf"))
      .present(cell).toList shouldBe List(Cell(Position2D("foo.ltf", "bar"), getDoubleContent(1 + math.log10(3))))
    LogarithmicTf[Position2D](math.log10 _)
      .andThenRelocate(Locate.RenameDimension(Second, "%1$s.ltf"))
      .present(cell).toList shouldBe List(Cell(Position2D("foo", "bar.ltf"), getDoubleContent(1 + math.log10(3))))
  }

  it should "not present with a categorical" in {
    LogarithmicTf().present(Cell(Position2D("foo", "bar"), getStringContent("baz"))) shouldBe List()
  }
}

class TestAugmentedTf extends TestTransformers {

  val cell = Cell(Position2D("foo", "bar"), getLongContent(1))
  val ext = Map(Position1D("foo") -> getLongContent(2), Position1D("bar") -> getLongContent(2))
  val ext2 = Map(Position1D("foo") -> Map(Position1D("baz") -> getLongContent(2)),
    Position1D("bar") -> Map(Position1D("baz") -> getLongContent(2)))

  "An AugmentedTf" should "present" in {
    AugmentedTf(dimExtractor[Position2D](First))
      .presentWithValue(cell, ext) shouldBe List(Cell(Position2D("foo", "bar"), getDoubleContent(0.5 + 0.5 * 1 / 2)))
    AugmentedTf(extractor[Position2D](Second, "baz"))
      .presentWithValue(cell, ext2) shouldBe List(Cell(Position2D("foo", "bar"), getDoubleContent(0.5 + 0.5 * 1 / 2)))
  }

  it should "present with name" in {
    AugmentedTf(extractor[Position2D](First, "baz"))
      .andThenRelocate(Locate.RenameDimension(First, "%1$s.atf"))
      .presentWithValue(cell, ext2).toList shouldBe List(Cell(Position2D("foo.atf", "bar"),
        getDoubleContent(0.5 + 0.5 * 1 / 2)))
    AugmentedTf(dimExtractor[Position2D](Second))
      .andThenRelocate(Locate.RenameDimension(Second, "%1$s.atf"))
      .presentWithValue(cell, ext).toList shouldBe List(Cell(Position2D("foo", "bar.atf"),
        getDoubleContent(0.5 + 0.5 * 1 / 2)))
  }

  it should "not present with missing value" in {
    AugmentedTf(dimExtractor[Position2D](First))
      .presentWithValue(Cell(Position2D("abc", "bar"), getLongContent(1)), ext) shouldBe List()
  }

  it should "not present with a categorical" in {
    AugmentedTf(dimExtractor[Position2D](First))
      .presentWithValue(Cell(Position2D("foo", "bar"), getStringContent("baz")), ext) shouldBe List()
  }
}

class TestTfIdf extends TestTransformers {

  val cell = Cell(Position2D("foo", "bar"), getDoubleContent(1.5))
  val ext = Map(Position1D("foo") -> getDoubleContent(2), Position1D("bar") -> getDoubleContent(2),
    Position1D("baz") -> getDoubleContent(2))

  "A TfIdf" should "present" in {
    TfIdf(dimExtractor[Position2D](First))
      .presentWithValue(cell, ext) shouldBe List(Cell(Position2D("foo", "bar"), getDoubleContent(3)))
    TfIdf(dimExtractor[Position2D](Second))
      .presentWithValue(cell, ext) shouldBe List(Cell(Position2D("foo", "bar"), getDoubleContent(3)))
  }

  it should "present with name" in {
    TfIdf(dimExtractor[Position2D](First))
      .andThenRelocate(Locate.RenameDimension(First, "%1$s.tfidf"))
      .presentWithValue(cell, ext).toList shouldBe List(Cell(Position2D("foo.tfidf", "bar"), getDoubleContent(3)))
    TfIdf(dimExtractor[Position2D](Second))
      .andThenRelocate(Locate.RenameDimension(Second, "%1$s.tfidf"))
      .presentWithValue(cell, ext).toList shouldBe List(Cell(Position2D("foo", "bar.tfidf"), getDoubleContent(3)))
  }

  it should "present with key" in {
    TfIdf(keyExtractor[Position2D]("baz"))
      .presentWithValue(cell, ext) shouldBe List(Cell(Position2D("foo", "bar"), getDoubleContent(3)))
  }

  it should "present with name and key" in {
    TfIdf(keyExtractor[Position2D]("baz"))
      .andThenRelocate(Locate.RenameDimension(First, "%1$s.tfidf"))
      .presentWithValue(cell, ext).toList shouldBe List(Cell(Position2D("foo.tfidf", "bar"), getDoubleContent(3)))
    TfIdf(keyExtractor[Position2D]("baz"))
      .andThenRelocate(Locate.RenameDimension(Second, "%1$s.tfidf"))
      .presentWithValue(cell, ext).toList shouldBe List(Cell(Position2D("foo", "bar.tfidf"), getDoubleContent(3)))
  }

  it should "not present with missing key" in {
    TfIdf(keyExtractor[Position2D]("not.there"))
      .presentWithValue(cell, ext) shouldBe List()
  }

  it should "not present with missing value" in {
    TfIdf(dimExtractor[Position2D](First))
      .presentWithValue(Cell(Position2D("abc", "bar"), getDoubleContent(1.5)), ext) shouldBe List()
  }

  it should "not present with a categorical" in {
    TfIdf(dimExtractor[Position2D](First))
      .presentWithValue(Cell(Position2D("foo", "bar"), getStringContent("baz")), ext) shouldBe List()
  }
}

class TestAdd extends TestTransformers {

  val cell = Cell(Position1D("foo"), getDoubleContent(1))
  val ext = Map(Position1D("foo") -> getDoubleContent(2), Position1D("bar") -> getDoubleContent(2))

  "An Add" should "present" in {
    Add(dimExtractor[Position1D](First))
      .presentWithValue(cell, ext) shouldBe List(Cell(Position1D("foo"), getDoubleContent(3)))
  }

  it should "present with name" in {
    Add(dimExtractor[Position1D](First))
      .andThenRelocate(Locate.RenameDimension(First, "%1$s.idf"))
      .presentWithValue(cell, ext).toList shouldBe List(Cell(Position1D("foo.idf"), getDoubleContent(3)))
  }

  it should "present with key" in {
    Add(keyExtractor[Position1D]("bar"))
      .presentWithValue(cell, ext) shouldBe List(Cell(Position1D("foo"), getDoubleContent(3)))
  }

  it should "present with name and key" in {
    Add(keyExtractor[Position1D]("bar"))
      .andThenRelocate(Locate.RenameDimension(First, "%1$s.idf"))
      .presentWithValue(cell, ext).toList shouldBe List(Cell(Position1D("foo.idf"), getDoubleContent(3)))
  }

  it should "not present with missing key" in {
    Add(keyExtractor[Position1D]("not.there"))
      .presentWithValue(cell, ext) shouldBe List()
  }

  it should "not present with missing value" in {
    Add(dimExtractor[Position1D](First))
      .presentWithValue(Cell(Position1D("abc"), getDoubleContent(1)), ext) shouldBe List()
  }

  it should "not present with a categorical" in {
    Add(dimExtractor[Position1D](First))
      .presentWithValue(Cell(Position1D("foo"), getStringContent("bar")), ext) shouldBe List()
  }
}

class TestSubtract extends TestTransformers {

  val cell = Cell(Position1D("foo"), getDoubleContent(1))
  val ext = Map(Position1D("foo") -> getDoubleContent(2), Position1D("bar") -> getDoubleContent(2))

  "A Subtract" should "present" in {
    Subtract(dimExtractor[Position1D](First))
      .presentWithValue(cell, ext) shouldBe List(Cell(Position1D("foo"), getDoubleContent(-1)))
  }

  it should "present with name" in {
    Subtract(dimExtractor[Position1D](First))
      .andThenRelocate(Locate.RenameDimension(First, "%1$s.idf"))
      .presentWithValue(cell, ext).toList shouldBe List(Cell(Position1D("foo.idf"), getDoubleContent(-1)))
  }

  it should "present with key" in {
    Subtract(keyExtractor[Position1D]("bar"))
      .presentWithValue(cell, ext) shouldBe List(Cell(Position1D("foo"), getDoubleContent(-1)))
  }

  it should "present with inverse" in {
    Subtract(dimExtractor[Position1D](First), true)
      .presentWithValue(cell, ext) shouldBe List(Cell(Position1D("foo"), getDoubleContent(1)))
  }

  it should "present with name and key" in {
    Subtract(keyExtractor[Position1D]("bar"))
      .andThenRelocate(Locate.RenameDimension(First, "%1$s.idf"))
      .presentWithValue(cell, ext).toList shouldBe List(Cell(Position1D("foo.idf"), getDoubleContent(-1)))
  }

  it should "present with name and inverse" in {
    Subtract(dimExtractor[Position1D](First), true)
      .andThenRelocate(Locate.RenameDimension(First, "%1$s.idf"))
      .presentWithValue(cell, ext).toList shouldBe List(Cell(Position1D("foo.idf"), getDoubleContent(1)))
  }

  it should "present with key and inverse" in {
    Subtract(keyExtractor[Position1D]("bar"), true)
      .presentWithValue(cell, ext) shouldBe List(Cell(Position1D("foo"), getDoubleContent(1)))
  }

  it should "present with name, key and inverse" in {
    Subtract(keyExtractor[Position1D]("bar"), true)
      .andThenRelocate(Locate.RenameDimension(First, "%1$s.idf"))
      .presentWithValue(cell, ext).toList shouldBe List(Cell(Position1D("foo.idf"), getDoubleContent(1)))
  }

  it should "not present with missing key" in {
    Subtract(keyExtractor[Position1D]("not.there"))
      .presentWithValue(cell, ext) shouldBe List()
  }

  it should "not present with missing value" in {
    Subtract(dimExtractor[Position1D](First))
      .presentWithValue(Cell(Position1D("abc"), getDoubleContent(1)), ext) shouldBe List()
  }

  it should "not present with a categorical" in {
    Subtract(dimExtractor[Position1D](First))
      .presentWithValue(Cell(Position1D("foo"), getStringContent("bar")), ext) shouldBe List()
  }
}

class TestMultiply extends TestTransformers {

  val cell = Cell(Position1D("foo"), getDoubleContent(1))
  val ext = Map(Position1D("foo") -> getDoubleContent(2), Position1D("bar") -> getDoubleContent(2))

  "A Multiply" should "present" in {
    Multiply(dimExtractor[Position1D](First))
      .presentWithValue(cell, ext) shouldBe List(Cell(Position1D("foo"), getDoubleContent(2)))
  }

  it should "present with name" in {
    Multiply(dimExtractor[Position1D](First))
      .andThenRelocate(Locate.RenameDimension(First, "%1$s.idf"))
      .presentWithValue(cell, ext).toList shouldBe List(Cell(Position1D("foo.idf"), getDoubleContent(2)))
  }

  it should "present with key" in {
    Multiply(keyExtractor[Position1D]("bar"))
      .presentWithValue(cell, ext) shouldBe List(Cell(Position1D("foo"), getDoubleContent(2)))
  }

  it should "present with name and key" in {
    Multiply(keyExtractor[Position1D]("bar"))
      .andThenRelocate(Locate.RenameDimension(First, "%1$s.idf"))
      .presentWithValue(cell, ext).toList shouldBe List(Cell(Position1D("foo.idf"), getDoubleContent(2)))
  }

  it should "not present with missing key" in {
    Multiply(keyExtractor[Position1D]("not.there"))
      .presentWithValue(cell, ext) shouldBe List()
  }

  it should "not present with missing value" in {
    Multiply(dimExtractor[Position1D](First))
      .presentWithValue(Cell(Position1D("abc"), getDoubleContent(1)), ext) shouldBe List()
  }

  it should "not present with a categorical" in {
    Multiply(dimExtractor[Position1D](First))
      .presentWithValue(Cell(Position1D("foo"), getStringContent("bar")), ext) shouldBe List()
  }
}

class TestFraction extends TestTransformers {

  val cell = Cell(Position1D("foo"), getDoubleContent(1))
  val ext = Map(Position1D("foo") -> getDoubleContent(2), Position1D("bar") -> getDoubleContent(2))

  "A Fraction" should "present" in {
    Fraction(dimExtractor[Position1D](First))
      .presentWithValue(cell, ext) shouldBe List(Cell(Position1D("foo"), getDoubleContent(0.5)))
  }

  it should "present with name" in {
    Fraction(dimExtractor[Position1D](First))
      .andThenRelocate(Locate.RenameDimension(First, "%1$s.idf"))
      .presentWithValue(cell, ext).toList shouldBe List(Cell(Position1D("foo.idf"), getDoubleContent(0.5)))
  }

  it should "present with key" in {
    Fraction(keyExtractor[Position1D]("bar"))
      .presentWithValue(cell, ext) shouldBe List(Cell(Position1D("foo"), getDoubleContent(0.5)))
  }

  it should "present with inverse" in {
    Fraction(dimExtractor[Position1D](First), true)
      .presentWithValue(cell, ext) shouldBe List(Cell(Position1D("foo"), getDoubleContent(2)))
  }

  it should "present with name and key" in {
    Fraction(keyExtractor[Position1D]("bar"))
      .andThenRelocate(Locate.RenameDimension(First, "%1$s.idf"))
      .presentWithValue(cell, ext).toList shouldBe List(Cell(Position1D("foo.idf"), getDoubleContent(0.5)))
  }

  it should "present with name and inverse" in {
    Fraction(dimExtractor[Position1D](First), true)
      .andThenRelocate(Locate.RenameDimension(First, "%1$s.idf"))
      .presentWithValue(cell, ext).toList shouldBe List(Cell(Position1D("foo.idf"), getDoubleContent(2)))
  }

  it should "present with key and inverse" in {
    Fraction(keyExtractor[Position1D]("bar"), true)
      .presentWithValue(cell, ext) shouldBe List(Cell(Position1D("foo"), getDoubleContent(2)))
  }

  it should "present with name, key and inverse" in {
    Fraction(keyExtractor[Position1D]("bar"), true)
      .andThenRelocate(Locate.RenameDimension(First, "%1$s.idf"))
      .presentWithValue(cell, ext).toList shouldBe List(Cell(Position1D("foo.idf"), getDoubleContent(2)))
  }

  it should "not present with missing key" in {
    Fraction(keyExtractor[Position1D]("not.there"))
      .presentWithValue(cell, ext) shouldBe List()
  }

  it should "not present with missing value" in {
    Fraction(dimExtractor[Position1D](First))
      .presentWithValue(Cell(Position1D("abc"), getDoubleContent(1)), ext) shouldBe List()
  }

  it should "not present with a categorical" in {
    Fraction(dimExtractor[Position1D](First))
      .presentWithValue(Cell(Position1D("foo"), getStringContent("bar")), ext) shouldBe List()
  }
}

class TestPower extends TestTransformers {

  val cell = Cell(Position1D("foo"), getDoubleContent(3.1415))

  "A Power" should "present" in {
    Power(2).present(cell) shouldBe List(Cell(Position1D("foo"), getDoubleContent(3.1415 * 3.1415)))
  }

  it should "present with name" in {
    Power[Position1D](2)
      .andThenRelocate(Locate.RenameDimension(First, "%1$s.pwr"))
      .present(cell).toList shouldBe List(Cell(Position1D("foo.pwr"), getDoubleContent(3.1415 * 3.1415)))
  }
}

class TestSquareRoot extends TestTransformers {

  val cell = Cell(Position1D("foo"), getDoubleContent(3.1415))

  "A SquareRoot" should "present" in {
    SquareRoot().present(cell) shouldBe List(Cell(Position1D("foo"), getDoubleContent(math.sqrt(3.1415))))
  }

  it should "present with name" in {
    SquareRoot[Position1D]()
      .andThenRelocate(Locate.RenameDimension(First, "%1$s.sqr"))
      .present(cell).toList shouldBe List(Cell(Position1D("foo.sqr"), getDoubleContent(math.sqrt(3.1415))))
  }
}

class TestCut extends TestTransformers {

  val cell = Cell(Position1D("foo"), getDoubleContent(3.1415))
  val ext = Map(Position1D("foo") -> List[Double](0,1,2,3,4,5))

  val binExtractor = ExtractWithDimension[Position1D, List[Double]](First)

  "A Cut" should "present" in {
    Cut(binExtractor).presentWithValue(cell, ext) shouldBe List(Cell(Position1D("foo"),
      Content(OrdinalSchema[String](
        Set("(0.0,1.0]", "(1.0,2.0]", "(2.0,3.0]", "(3.0,4.0]", "(4.0,5.0]")), "(3.0,4.0]")))
  }

  it should "present with name" in {
    Cut(binExtractor)
      .andThenRelocate(Locate.RenameDimension(First, "%1$s.cut"))
      .presentWithValue(cell, ext).toList shouldBe List(Cell(Position1D("foo.cut"),
        Content(OrdinalSchema[String](
          Set("(0.0,1.0]", "(1.0,2.0]", "(2.0,3.0]", "(3.0,4.0]", "(4.0,5.0]")), "(3.0,4.0]")))
  }

  it should "not present with missing bins" in {
    Cut(binExtractor).presentWithValue(cell, Map()) shouldBe List()
  }
}

class TestCompare extends TestTransformers {

  val cell = Cell(Position1D("foo"), getDoubleContent(3.1415))

  def equ(v: Double) = (cell: Cell[Position1D]) => {
    cell.content.value.asDouble.map(_ == v).getOrElse(false)
  }

  "A Compare" should "present" in {
    Compare(equ(3.1415)).present(cell) shouldBe List(Cell(Position1D("foo"),
      Content(NominalSchema[Boolean](), true)))
    Compare(equ(3.3)).present(cell) shouldBe List(Cell(Position1D("foo"),
      Content(NominalSchema[Boolean](), false)))
  }

  it should "present with name" in {
    Compare(equ(3.1415))
      .andThenRelocate(Locate.RenameDimension(First, "%1$s.cmp"))
      .present(cell).toList shouldBe List(Cell(Position1D("foo.cmp"), Content(NominalSchema[Boolean](), true)))
    Compare(equ(3.3))
      .andThenRelocate(Locate.RenameDimension(First, "%1$s.cmp"))
      .present(cell).toList shouldBe List(Cell(Position1D("foo.cmp"), Content(NominalSchema[Boolean](), false)))
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
    Binarise[Position1D](Locate.RenameDimensionWithContent(First))
      .andThen(Indicator()
      .andThenRelocate(Locate.RenameDimension(First, "%1$s.ind")))
      .present(cell).toList shouldBe List(Cell(Position1D("foo=rules.ind"), getLongContent(1)))
  }
}

class TestAndThenTransformerWithValue extends TestTransformers {

  val cell = Cell(Position1D("foo"), getDoubleContent(3.1415))
  val ext = Map(Position1D("foo") -> Map(Position1D("min") -> getDoubleContent(0),
    Position1D("max") -> getDoubleContent(2), Position1D("max.abs") -> getDoubleContent(3)))

  "An AndThenTransformerWithValue" should "present" in {
    Clamp(extractor[Position1D](First, "min"), extractor[Position1D](First, "max"))
      .andThenWithValue(Normalise(extractor[Position1D](First, "max.abs")))
      .presentWithValue(cell, ext).toList shouldBe List(Cell(Position1D("foo"), getDoubleContent(2.0 / 3.0)))
  }
}

class TestWithPrepareTransformer extends TestTransformers {

  val str = Cell(Position1D("x"), getDoubleContent(2))
  val dbl = Cell(Position1D("y"), getDoubleContent(4))
  val lng = Cell(Position1D("z"), getDoubleContent(8))

  val ext = Map(Position1D("x") -> getDoubleContent(3),
    Position1D("y") -> getDoubleContent(4),
    Position1D("z") -> getDoubleContent(5))

  def prepare(cell: Cell[Position1D]): Content = {
    cell.content.value match {
      case d: DoubleValue if (d.value == 2) => cell.content
      case d: DoubleValue if (d.value == 4) => getStringContent("not.supported")
      case d: DoubleValue if (d.value == 8) => getLongContent(8)
    }
  }

  def prepareWithValue(cell: Cell[Position1D], ext: Map[Position1D, Content]): Content = {
    (cell.content.value, ext(cell.position).value) match {
      case (c: DoubleValue, d: DoubleValue) if (c.value == 2) => getDoubleContent(2 * d.value)
      case (c: DoubleValue, _) if (c.value == 4) => getStringContent("not.supported")
      case (c: DoubleValue, d: DoubleValue) if (c.value == 8) => getLongContent(d.value.toLong)
    }
  }

  "A Transformer" should "withPrepare correctly" in {
    val obj = Power[Position1D](2).withPrepare(prepare)

    obj.present(str) shouldBe (List(Cell(str.position, getDoubleContent(4))))
    obj.present(dbl) shouldBe (List())
    obj.present(lng) shouldBe (List(Cell(lng.position, getDoubleContent(64))))
  }

  it should "withPrepareWithValue correctly (without value)" in {
    val obj = Multiply[Position1D, Map[Position1D, Content]](
      ExtractWithPosition().andThenPresent(_.value.asDouble)).withPrepare(prepare)

    obj.presentWithValue(str, ext) shouldBe (List(Cell(str.position, getDoubleContent(2 * 3))))
    obj.presentWithValue(dbl, ext) shouldBe (List())
    obj.presentWithValue(lng, ext) shouldBe (List(Cell(lng.position, getDoubleContent(5 * 8))))
  }

  it should "withPrepareWithVaue correctly" in {
    val obj = Multiply[Position1D, Map[Position1D, Content]](
      ExtractWithPosition().andThenPresent(_.value.asDouble)).withPrepareWithValue(prepareWithValue)

    obj.presentWithValue(str, ext) shouldBe (List(Cell(str.position, getDoubleContent(2 * 3 * 3))))
    obj.presentWithValue(dbl, ext) shouldBe (List())
    obj.presentWithValue(lng, ext) shouldBe (List(Cell(lng.position, getDoubleContent(5 * 5))))
  }
}

class TestAndThenMutateTransformer extends TestTransformers {

  val str = Cell(Position1D("x"), getDoubleContent(2))
  val dbl = Cell(Position1D("y"), getDoubleContent(4))
  val lng = Cell(Position1D("z"), getDoubleContent(8))

  val ext = Map(Position1D("x") -> getDoubleContent(3),
    Position1D("y") -> getDoubleContent(4),
    Position1D("z") -> getDoubleContent(5))

  def mutate(cell: Cell[Position1D]): Content = {
    cell.position match {
      case Position1D(StringValue("x", _)) => cell.content
      case Position1D(StringValue("y", _)) => getStringContent("not.supported")
      case Position1D(StringValue("z", _)) => getLongContent(8)
    }
  }

  def mutateWithValue(cell: Cell[Position1D], ext: Map[Position1D, Content]): Content = {
    (cell.position, ext(cell.position).value) match {
      case (Position1D(StringValue("x", _)), DoubleValue(d, _)) => getDoubleContent(2 * d)
      case (Position1D(StringValue("y", _)), _) => getStringContent("not.supported")
      case (Position1D(StringValue("z", _)), DoubleValue(d, _)) => getLongContent(d.toLong)
    }
  }

  "A Transfomer" should "andThenMutate correctly" in {
    val obj = Power[Position1D](2).andThenMutate(mutate)

    obj.present(str).toList shouldBe (List(Cell(str.position, getDoubleContent(4))))
    obj.present(dbl).toList shouldBe (List(Cell(dbl.position, getStringContent("not.supported"))))
    obj.present(lng).toList shouldBe (List(Cell(lng.position, getLongContent(8))))
  }

  it should "andThenMutateWithValue correctly (without value)" in {
    val obj = Multiply[Position1D, Map[Position1D, Content]](
      ExtractWithPosition().andThenPresent(_.value.asDouble)).andThenMutate(mutate)

    obj.presentWithValue(str, ext).toList shouldBe (List(Cell(str.position, getDoubleContent(2 * 3))))
    obj.presentWithValue(dbl, ext).toList shouldBe (List(Cell(dbl.position, getStringContent("not.supported"))))
    obj.presentWithValue(lng, ext).toList shouldBe (List(Cell(lng.position, getLongContent(8))))
  }

  it should "andThenMutateWithValue correctly" in {
    val obj = Multiply[Position1D, Map[Position1D, Content]](
      ExtractWithPosition().andThenPresent(_.value.asDouble)).andThenMutateWithValue(mutateWithValue)

    obj.presentWithValue(str, ext).toList shouldBe (List(Cell(str.position, getDoubleContent(2 * 3))))
    obj.presentWithValue(dbl, ext).toList shouldBe (List(Cell(dbl.position, getStringContent("not.supported"))))
    obj.presentWithValue(lng, ext).toList shouldBe (List(Cell(lng.position, getLongContent(5))))
  }
}

