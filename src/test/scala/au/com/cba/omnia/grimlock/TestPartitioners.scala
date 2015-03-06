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
import au.com.cba.omnia.grimlock.partition._
import au.com.cba.omnia.grimlock.position._
import au.com.cba.omnia.grimlock.utility._

import com.twitter.scalding._
import com.twitter.scalding.bdd._

import java.util.Date

import org.scalatest._

import scala.collection.mutable

trait TestHashPartitioners {

  // In scalding REPL:
  //
  // import au.com.cba.omnia.grimlock.encoding._
  //
  // List(1, 3, 4).map { case i => math.abs(LongValue(i).hashCode % 10) }
  // ->  List(4, 9, 0)
  //
  // List("b", "a", "c").map { case s => math.abs(StringValue(s).hashCode % 10) }
  // ->  List(6, 8, 0)

  val dfmt = new java.text.SimpleDateFormat("yyyy-MM-dd")

  val pos1 = Position2D(1, "b")
  val pos2 = Position2D(3, "a")
  val pos3 = Position2D(4, "c")
}

class TestBinaryHashSplit extends FlatSpec with Matchers with TestHashPartitioners {

  "A BinaryHashSplit" should "assign left on the first dimension" in {
    BinaryHashSplit(First, 5, "left", "right", 10).assign(pos1) should be (Collection("left"))
  }

  it should "assign left on the first dimension when on boundary" in {
    BinaryHashSplit(First, 4, "left", "right", 10).assign(pos1) should be (Collection("left"))
  }

  it should "assign right on the first dimension" in {
    BinaryHashSplit(First, 5, "left", "right", 10).assign(pos2) should be (Collection("right"))
  }

  it should "assign left on the second dimension" in {
    BinaryHashSplit(Second, 7, "left", "right", 10).assign(pos1) should be (Collection("left"))
  }

  it should "assign left on the second dimension when on boundary" in {
    BinaryHashSplit(Second, 6, "left", "right", 10).assign(pos1) should be (Collection("left"))
  }

  it should "assign right on the second dimension" in {
    BinaryHashSplit(Second, 7, "left", "right", 10).assign(pos2) should be (Collection("right"))
  }
}

class TestTernaryHashSplit extends FlatSpec with Matchers with TestHashPartitioners {

  "A TernaryHashSplit" should "assign left on the first dimension" in {
    TernaryHashSplit(First, 3, 7, "left", "middle", "right", 10).assign(pos3) should be (Collection("left"))
  }

  it should "assign left on the first dimension when on boundary" in {
    TernaryHashSplit(First, 4, 7, "left", "middle", "right", 10).assign(pos3) should be (Collection("left"))
  }

  it should "assign middle on the first dimension" in {
    TernaryHashSplit(First, 3, 7, "left", "middle", "right", 10).assign(pos1) should be (Collection("middle"))
  }

  it should "assign middle on the first dimension when on boundary" in {
    TernaryHashSplit(First, 3, 4, "left", "middle", "right", 10).assign(pos1) should be (Collection("middle"))
  }

  it should "assign right on the first dimension" in {
    TernaryHashSplit(First, 4, 7, "left", "middle", "right", 10).assign(pos2) should be (Collection("right"))
  }

  it should "assign left on the second dimension" in {
    TernaryHashSplit(Second, 3, 7, "left", "middle", "right", 10).assign(pos3) should be (Collection("left"))
  }

  it should "assign left on the second dimension when on boundary" in {
    TernaryHashSplit(Second, 6, 7, "left", "middle", "right", 10).assign(pos3) should be (Collection("left"))
  }

  it should "assign middle on the second dimension" in {
    TernaryHashSplit(Second, 3, 7, "left", "middle", "right", 10).assign(pos1) should be (Collection("middle"))
  }

  it should "assign middle on the second dimension when on boundary" in {
    TernaryHashSplit(Second, 3, 8, "left", "middle", "right", 10).assign(pos1) should be (Collection("middle"))
  }

  it should "assign right on the second dimension" in {
    TernaryHashSplit(Second, 3, 7, "left", "middle", "right", 10).assign(pos2) should be (Collection("right"))
  }
}

class TestHashSplit extends FlatSpec with Matchers with TestHashPartitioners {

  val map1: Map[String, (Int, Int)] = Map("lower.left" -> ((0, 4)), "upper.left" -> ((1, 5)), "right" -> ((6, 10)))
  val map2: Map[String, (Int, Int)] = Map("lower.left" -> ((0, 6)), "upper.left" -> ((1, 7)), "right" -> ((7, 10)))

  "A HashSplit" should "assign both left on the first dimension" in {
    HashSplit(First, map1, 10).assign(pos1) should be (Collection(List("lower.left", "upper.left")))
  }

  it should "assign right on the first dimension" in {
    HashSplit(First, map1, 10).assign(pos2) should be (Collection("right"))
  }

  it should "assign none on the first dimension" in {
    HashSplit(First, map1, 10).assign(pos3) should be (Collection())
  }

  it should "assign both left on the second dimension" in {
    HashSplit(Second, map2, 10).assign(pos1) should be (Collection(List("lower.left", "upper.left")))
  }

  it should "assign right on the second dimension" in {
    HashSplit(Second, map2, 10).assign(pos2) should be (Collection("right"))
  }

  it should "assign none on the second dimension" in {
    HashSplit(Second, map2, 10).assign(pos3) should be (Collection())
  }
}

trait TestDatePartitioners {

  val dfmt = new java.text.SimpleDateFormat("yyyy-MM-dd")

  val pos1 = Position2D(1, DateValue(dfmt.parse("2004-01-01"), DateCodex))
  val pos2 = Position2D(2, DateValue(dfmt.parse("2006-01-01"), DateCodex))
  val pos3 = Position2D(3, DateValue(dfmt.parse("2007-01-01"), DateCodex))
}

class TestBinaryDateSplit extends FlatSpec with Matchers with TestDatePartitioners {

  "A BinaryDateSplit" should "assign none on the first dimension" in {
    BinaryDateSplit(First, dfmt.parse("2005-01-01"), "left", "right").assign(pos1) should be (Collection())
  }

  it should "assign left on the second dimension" in {
    BinaryDateSplit(Second, dfmt.parse("2005-01-01"), "left", "right").assign(pos1) should be (Collection("left"))
  }

  it should "assign left on the second dimension when on boundary" in {
    BinaryDateSplit(Second, dfmt.parse("2004-01-01"), "left", "right").assign(pos1) should be (Collection("left"))
  }

  it should "assign right on the second dimension" in {
    BinaryDateSplit(Second, dfmt.parse("2005-01-01"), "left", "right").assign(pos2) should be (Collection("right"))
  }
}

class TestTernaryDateSplit extends FlatSpec with Matchers with TestDatePartitioners {

  "A TernaryDateSplit" should "assign none on the first dimension" in {
    TernaryDateSplit(First, dfmt.parse("2005-01-01"), dfmt.parse("2006-06-30"), "left", "middle", "right")
      .assign(pos1) should be (Collection())
  }

  it should "assign left on the second dimension" in {
    TernaryDateSplit(Second, dfmt.parse("2005-01-01"), dfmt.parse("2006-06-30"), "left", "middle", "right")
      .assign(pos1) should be (Collection("left"))
  }

  it should "assign left on the second dimension when on boundary" in {
    TernaryDateSplit(Second, dfmt.parse("2004-01-01"), dfmt.parse("2006-06-30"), "left", "middle", "right")
      .assign(pos1) should be (Collection("left"))
  }

  it should "assign middle on the second dimension" in {
    TernaryDateSplit(Second, dfmt.parse("2005-01-01"), dfmt.parse("2006-01-01"), "left", "middle", "right")
      .assign(pos2) should be (Collection("middle"))
  }

  it should "assign middle on the second dimension when on boundary" in {
    TernaryDateSplit(Second, dfmt.parse("2005-01-01"), dfmt.parse("2006-01-01"), "left", "middle", "right")
      .assign(pos2) should be (Collection("middle"))
  }

  it should "assign right on the second dimension" in {
    TernaryDateSplit(Second, dfmt.parse("2005-01-01"), dfmt.parse("2006-06-30"), "left", "middle", "right")
      .assign(pos3) should be (Collection("right"))
  }
}

class TestDateSplit extends FlatSpec with Matchers with TestDatePartitioners {

  val map1: Map[String, (Date, Date)] = Map("lower.left" -> ((dfmt.parse("2003-01-01"), dfmt.parse("2005-01-01"))),
    "upper.left" -> ((dfmt.parse("2003-06-30"), dfmt.parse("2005-06-30"))),
    "right" -> ((dfmt.parse("2006-06-30"), dfmt.parse("2008-01-01"))))

  "A DateSplit" should "assign none on the first dimension" in {
    DateSplit(First, map1).assign(pos1) should be (Collection())
  }

  it should "assign both left on the second dimension" in {
    DateSplit(Second, map1).assign(pos1) should be (Collection(List("lower.left", "upper.left")))
  }

  it should "assign right on the second dimension" in {
    DateSplit(Second, map1).assign(pos3) should be (Collection("right"))
  }

  it should "assign none on the second dimension" in {
    DateSplit(Second, map1).assign(pos2) should be (Collection())
  }
}

class TypedPartitions extends WordSpec with Matchers with TBddDsl {

  val data = List(("train", Cell(Position1D("fid:A"), Content(ContinuousSchema[Codex.LongCodex](), 1))),
    ("train", Cell(Position1D("fid:B"), Content(ContinuousSchema[Codex.LongCodex](), 2))),
    ("train", Cell(Position1D("fid:C"), Content(ContinuousSchema[Codex.LongCodex](), 3))),
    ("test", Cell(Position1D("fid:A"), Content(ContinuousSchema[Codex.LongCodex](), 4))),
    ("test", Cell(Position1D("fid:B"), Content(ContinuousSchema[Codex.LongCodex](), 5))),
    ("valid", Cell(Position1D("fid:B"), Content(ContinuousSchema[Codex.LongCodex](), 6))))

  def double(key: String, pipe: TypedPipe[Cell[Position1D]]): TypedPipe[Cell[Position1D]] = {
    pipe
      .flatMap {
        case c => c.content.value.asLong.map {
          case v => Cell(c.position, Content(ContinuousSchema[Codex.LongCodex](), 2 * v))
        }
      }
  }

  "A Partitions" should {
    "return its keys" in {
      Given {
        data
      } When {
        parts: TypedPipe[(String, Cell[Position1D])] =>
          new Partitions(parts).keys()
      } Then {
        buffer: mutable.Buffer[String] =>
          buffer.toList.sorted shouldBe List("test", "train", "valid")
      }
    }
    "get a partition's data" in {
      Given {
        data
      } When {
        parts: TypedPipe[(String, Cell[Position1D])] =>
          new Partitions(parts).get("train")
      } Then {
        buffer: mutable.Buffer[Cell[Position1D]] =>
          buffer.toList.sortBy(_.position.toShortString("|")) shouldBe
            List(Cell(Position1D("fid:A"), Content(ContinuousSchema[Codex.LongCodex](), 1)),
              Cell(Position1D("fid:B"), Content(ContinuousSchema[Codex.LongCodex](), 2)),
              Cell(Position1D("fid:C"), Content(ContinuousSchema[Codex.LongCodex](), 3)))
      }
    }
    "add new data" in {
      Given {
        data
      } And {
        List(Cell(Position1D("fid:A"), Content(ContinuousSchema[Codex.LongCodex](), 8)),
             Cell(Position1D("fid:C"), Content(ContinuousSchema[Codex.LongCodex](), 9)))
      } When {
        (parts: TypedPipe[(String, Cell[Position1D])], pipe: TypedPipe[Cell[Position1D]]) =>
          new Partitions(parts).add("xyz", pipe)
      } Then {
        buffer: mutable.Buffer[(String, Cell[Position1D])] =>
          buffer.toList.sortBy(_._2.content.value.toShortString) shouldBe data ++
            List(("xyz", Cell(Position1D("fid:A"), Content(ContinuousSchema[Codex.LongCodex](), 8))),
              ("xyz", Cell(Position1D("fid:C"), Content(ContinuousSchema[Codex.LongCodex](), 9))))
      }
    }
    "remove a partition's data" in {
      Given {
        data
      } When {
        parts: TypedPipe[(String, Cell[Position1D])] =>
          new Partitions(parts).remove("train")
      } Then {
        buffer: mutable.Buffer[(String, Cell[Position1D])] =>
          buffer.toList.sortBy(_._2.content.value.toShortString) shouldBe
            List(("test", Cell(Position1D("fid:A"), Content(ContinuousSchema[Codex.LongCodex](), 4))),
              ("test", Cell(Position1D("fid:B"), Content(ContinuousSchema[Codex.LongCodex](), 5))),
              ("valid", Cell(Position1D("fid:B"), Content(ContinuousSchema[Codex.LongCodex](), 6))))
      }
    }
    "foreach should apply to selected partitions" in {
      Given {
        data
      } When {
        parts: TypedPipe[(String, Cell[Position1D])] =>
          new Partitions(parts).foreach(List("test", "valid", "not.there"), double)
      } Then {
        buffer: mutable.Buffer[(String, Cell[Position1D])] =>
          buffer.toList.sortBy(_._2.toString("|", false)) shouldBe
            List(("test", Cell(Position1D("fid:A"), Content(ContinuousSchema[Codex.LongCodex](), 8))),
              ("test", Cell(Position1D("fid:B"), Content(ContinuousSchema[Codex.LongCodex](), 10))),
              ("valid", Cell(Position1D("fid:B"), Content(ContinuousSchema[Codex.LongCodex](), 12))))
      }
    }
  }
}

