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

trait TestSlice extends TestGrimlock {
  val dfmt = new java.text.SimpleDateFormat("yyyy-MM-dd")
  val con1 = Content(ContinuousSchema[Codex.LongCodex](), 1)
  val con2 = Content(ContinuousSchema[Codex.LongCodex](), 2)
}

trait TestSlicePosition1D extends TestSlice {
  val pos1 = Position1D(1)
  val pos2 = Position1D(-1)
}

class TestOverPosition1D extends TestSlicePosition1D {

  val over = Over[Position1D, First.type](First)

  "A Over[Position1D]" should "return a Position1D for the selected dimension" in {
    over.selected(pos1) shouldBe Position1D(pos1(First))
  }

  it should "return a Position0D for the remainder" in {
    over.remainder(pos1) shouldBe pos1.remove(First)
  }

  it should "throw an exception for an invalid dimension" in {
    a [IndexOutOfBoundsException] should be thrownBy { Over(Second).selected(pos1) }
    a [IndexOutOfBoundsException] should be thrownBy { Over(Third).selected(pos1) }
    a [IndexOutOfBoundsException] should be thrownBy { Over(Fourth).selected(pos1) }
    a [IndexOutOfBoundsException] should be thrownBy { Over(Fifth).selected(pos1) }

    a [UnsupportedOperationException] should be thrownBy { Over(Second).remainder(pos1) }
    a [UnsupportedOperationException] should be thrownBy { Over(Third).remainder(pos1) }
    a [UnsupportedOperationException] should be thrownBy { Over(Fourth).remainder(pos1) }
    a [UnsupportedOperationException] should be thrownBy { Over(Fifth).remainder(pos1) }
  }

  it should "return a map" in {
    over.toMap(Cell(pos1, con1)) shouldBe Map(over.selected(pos1) -> con1)
  }

  it should "combine two maps" in {
    over.combineMaps(pos1, over.toMap(Cell(pos1, con1)), over.toMap(Cell(pos2, con2))) shouldBe
      Map(over.selected(pos1) -> con1, over.selected(pos2) -> con2)
    over.combineMaps(pos1, over.toMap(Cell(pos1, con1)), over.toMap(Cell(pos1, con1))) shouldBe
      Map(over.selected(pos1) -> con1)
  }
}

class TestAlongPosition1D extends TestSlicePosition1D {

  val along = Along[Position1D, First.type](First)

  "A Along[Position1D]" should "return a Position0D for the selected dimension" in {
    along.selected(pos1) shouldBe pos1.remove(First)
  }

  it should "return a Position1D for the remainder" in {
    along.remainder(pos1) shouldBe Position1D(pos1(First))
  }

  it should "throw an exception for an invalid dimension" in {
    a [UnsupportedOperationException] should be thrownBy { Along(Second).selected(pos1) }
    a [UnsupportedOperationException] should be thrownBy { Along(Third).selected(pos1) }
    a [UnsupportedOperationException] should be thrownBy { Along(Fourth).selected(pos1) }
    a [UnsupportedOperationException] should be thrownBy { Along(Fifth).selected(pos1) }

    a [IndexOutOfBoundsException] should be thrownBy { Along(Second).remainder(pos1) }
    a [IndexOutOfBoundsException] should be thrownBy { Along(Third).remainder(pos1) }
    a [IndexOutOfBoundsException] should be thrownBy { Along(Fourth).remainder(pos1) }
    a [IndexOutOfBoundsException] should be thrownBy { Along(Fifth).remainder(pos1) }
  }

  it should "throw an exception for returning a map" in {
    a [Exception] should be thrownBy { along.toMap(Cell(pos1, con1)) }
  }

  it should "throw an exception for combining two maps" in {
    a [Exception] should be thrownBy { along.combineMaps(pos1, Map(Position0D() -> con1), Map(Position0D() -> con2)) }
    a [Exception] should be thrownBy { along.combineMaps(pos1, Map(Position0D() -> con1), Map(Position0D() -> con1)) }
  }
}

trait TestSlicePosition2D extends TestSlice {
  val pos1 = Position2D(2, "a")
  val pos2 = Position2D(-2, "z")
}

class TestOverPosition2D extends TestSlicePosition2D {

  val list = List((Over[Position2D, First.type](First), First), (Over[Position2D, Second.type](Second), Second))

  "A Over[Position2D]" should "return a Position1D for the selected dimension" in {
    list.map { case (o, d) => o.selected(pos1) shouldBe Position1D(pos1(d)) }
  }

  it should "return a Position1D for the remainder" in {
    list.map { case (o, d) => o.remainder(pos1) shouldBe pos1.remove(d) }
  }

  it should "throw an exception for an invalid dimension" in {
    a [IndexOutOfBoundsException] should be thrownBy { Over(Third).selected(pos1) }
    a [IndexOutOfBoundsException] should be thrownBy { Over(Fourth).selected(pos1) }
    a [IndexOutOfBoundsException] should be thrownBy { Over(Fifth).selected(pos1) }
  }

  it should "return a map" in {
    list.map {
      case (o, _) => o.toMap(Cell(pos1, con1)) shouldBe Map(o.selected(pos1) -> Map(o.remainder(pos1) -> con1))
    }
  }

  it should "combine two maps" in {
    list.map {
      case (o, _) =>
        o.combineMaps(pos1, o.toMap(Cell(pos1, con1)), o.toMap(Cell(pos2, con2))) shouldBe
          Map(o.selected(pos1) -> Map(o.remainder(pos1) -> con1), o.selected(pos2) -> Map(o.remainder(pos2) -> con2))
        o.combineMaps(pos1, o.toMap(Cell(pos1, con1)), o.toMap(Cell(pos1, con1))) shouldBe
          Map(o.selected(pos1) -> Map(o.remainder(pos1) -> con1))
    }
  }
}

class TestAlongPosition2D extends TestSlicePosition2D {

  val list = List((Along[Position2D, First.type](First), First), (Along[Position2D, Second.type](Second), Second))

  "A Along[Position2D]" should "return a Position1D for the selected dimension" in {
    list.map { case (a, d) => a.selected(pos1) shouldBe pos1.remove(d) }
  }

  it should "return a Position1D for the remainder" in {
    list.map { case (a, d) => a.remainder(pos1) shouldBe Position1D(pos1(d)) }
  }

  it should "throw an exception for an invalid dimension" in {
    a [IndexOutOfBoundsException] should be thrownBy { Along(Third).remainder(pos1) }
    a [IndexOutOfBoundsException] should be thrownBy { Along(Fourth).remainder(pos1) }
    a [IndexOutOfBoundsException] should be thrownBy { Along(Fifth).remainder(pos1) }
  }

  it should "return a map" in {
    list.map {
      case (a, _) => a.toMap(Cell(pos1, con1)) shouldBe Map(a.selected(pos1) -> Map(a.remainder(pos1) -> con1))
    }
  }

  it should "combine two maps" in {
    list.map {
      case (a, _) =>
        a.combineMaps(pos1, a.toMap(Cell(pos1, con1)), a.toMap(Cell(pos2, con2))) shouldBe
          Map(a.selected(pos1) -> Map(a.remainder(pos1) -> con1), a.selected(pos2) -> Map(a.remainder(pos2) -> con2))
        a.combineMaps(pos1, a.toMap(Cell(pos1, con1)), a.toMap(Cell(pos1, con1))) shouldBe
          Map(a.selected(pos1) -> Map(a.remainder(pos1) -> con1))
    }
  }
}

trait TestSlicePosition3D extends TestSlice {
  val pos1 = Position3D(3, "b", DateValue(dfmt.parse("2001-01-01"), DateCodex))
  val pos2 = Position3D(-3, "y", DateValue(dfmt.parse("1999-01-01"), DateCodex))
}

class TestOverPosition3D extends TestSlicePosition3D {

  val list = List((Over[Position3D, First.type](First), First), (Over[Position3D, Second.type](Second), Second),
    (Over[Position3D, Third.type](Third), Third))

  "A Over[Position3D]" should "return a Position1D for the selected dimension" in {
    list.map { case (o, d) => o.selected(pos1) shouldBe Position1D(pos1(d)) }
  }

  it should "return a Position2D for the remainder" in {
    list.map { case (o, d) => o.remainder(pos1) shouldBe pos1.remove(d) }
  }

  it should "throw an exception for an invalid dimension" in {
    a [IndexOutOfBoundsException] should be thrownBy { Over(Fourth).selected(pos1) }
    a [IndexOutOfBoundsException] should be thrownBy { Over(Fifth).selected(pos1) }
  }

  it should "return a map" in {
    list.map {
      case (o, _) => o.toMap(Cell(pos1, con1)) shouldBe Map(o.selected(pos1) -> Map(o.remainder(pos1) -> con1))
    }
  }

  it should "combine two maps" in {
    list.map {
      case (o, _) =>
        o.combineMaps(pos1, o.toMap(Cell(pos1, con1)), o.toMap(Cell(pos2, con2))) shouldBe
          Map(o.selected(pos1) -> Map(o.remainder(pos1) -> con1), o.selected(pos2) -> Map(o.remainder(pos2) -> con2))
        o.combineMaps(pos1, o.toMap(Cell(pos1, con1)), o.toMap(Cell(pos1, con1))) shouldBe
          Map(o.selected(pos1) -> Map(o.remainder(pos1) -> con1))
    }
  }
}

class TestAlongPosition3D extends TestSlicePosition3D {

  val list = List((Along[Position3D, First.type](First), First), (Along[Position3D, Second.type](Second), Second),
    (Along[Position3D, Third.type](Third), Third))

  "A Along[Position3D]" should "return a Position2D for the selected dimension" in {
    list.map { case (a, d) => a.selected(pos1) shouldBe pos1.remove(d) }
  }

  it should "return a Position1D for the remainder" in {
    list.map { case (a, d) => a.remainder(pos1) shouldBe Position1D(pos1(d)) }
  }

  it should "throw an exception for an invalid dimension" in {
    a [IndexOutOfBoundsException] should be thrownBy { Along(Fourth).remainder(pos1) }
    a [IndexOutOfBoundsException] should be thrownBy { Along(Fifth).remainder(pos1) }
  }

  it should "return a map" in {
    list.map {
      case (a, _) => a.toMap(Cell(pos1, con1)) shouldBe Map(a.selected(pos1) -> Map(a.remainder(pos1) -> con1))
    }
  }

  it should "combine two maps" in {
    list.map {
      case (a, _) =>
        a.combineMaps(pos1, a.toMap(Cell(pos1, con1)), a.toMap(Cell(pos2, con2))) shouldBe
          Map(a.selected(pos1) -> Map(a.remainder(pos1) -> con1), a.selected(pos2) -> Map(a.remainder(pos2) -> con2))
        a.combineMaps(pos1, a.toMap(Cell(pos1, con1)), a.toMap(Cell(pos1, con1))) shouldBe
          Map(a.selected(pos1) -> Map(a.remainder(pos1) -> con1))
    }
  }
}

trait TestSlicePosition4D extends TestSlice {
  val pos1 = Position4D(4, "c", DateValue(dfmt.parse("2002-01-01"), DateCodex), "foo")
  val pos2 = Position4D(-4, "x", DateValue(dfmt.parse("1998-01-01"), DateCodex), "oof")
}

class TestOverPosition4D extends TestSlicePosition4D {

  val list = List((Over[Position4D, First.type](First), First), (Over[Position4D, Second.type](Second), Second),
    (Over[Position4D, Third.type](Third), Third), (Over[Position4D, Fourth.type](Fourth), Fourth))

  "A Over[Position4D]" should "return a Position1D for the selected dimension" in {
    list.map { case (o, d) => o.selected(pos1) shouldBe Position1D(pos1(d)) }
  }

  it should "return a Position3D for the remainder" in {
    list.map { case (o, d) => o.remainder(pos1) shouldBe pos1.remove(d) }
  }

  it should "throw an exception for an invalid dimension" in {
    a [IndexOutOfBoundsException] should be thrownBy { Over(Fifth).selected(pos1) }
  }

  it should "return a map" in {
    list.map {
      case (o, _) => o.toMap(Cell(pos1, con1)) shouldBe Map(o.selected(pos1) -> Map(o.remainder(pos1) -> con1))
    }
  }

  it should "combine two maps" in {
    list.map {
      case (o, _) =>
        o.combineMaps(pos1, o.toMap(Cell(pos1, con1)), o.toMap(Cell(pos2, con2))) shouldBe
          Map(o.selected(pos1) -> Map(o.remainder(pos1) -> con1), o.selected(pos2) -> Map(o.remainder(pos2) -> con2))
        o.combineMaps(pos1, o.toMap(Cell(pos1, con1)), o.toMap(Cell(pos1, con1))) shouldBe
          Map(o.selected(pos1) -> Map(o.remainder(pos1) -> con1))
    }
  }
}

class TestAlongPosition4D extends TestSlicePosition4D {

  val list = List((Along[Position4D, First.type](First), First), (Along[Position4D, Second.type](Second), Second),
    (Along[Position4D, Third.type](Third), Third), (Along[Position4D, Fourth.type](Fourth), Fourth))

  "A Along[Position4D]" should "return a Position3D for the selected dimension" in {
    list.map { case (a, d) => a.selected(pos1) shouldBe pos1.remove(d) }
  }

  it should "return a Position1D for the remainder" in {
    list.map { case (a, d) => a.remainder(pos1) shouldBe Position1D(pos1(d)) }
  }

  it should "throw an exception for an invalid dimension" in {
    a [IndexOutOfBoundsException] should be thrownBy { Along(Fifth).remainder(pos1) }
  }

  it should "return a map" in {
    list.map {
      case (a, _) => a.toMap(Cell(pos1, con1)) shouldBe Map(a.selected(pos1) -> Map(a.remainder(pos1) -> con1))
    }
  }

  it should "combine two maps" in {
    list.map {
      case (a, _) =>
        a.combineMaps(pos1, a.toMap(Cell(pos1, con1)), a.toMap(Cell(pos2, con2))) shouldBe
          Map(a.selected(pos1) -> Map(a.remainder(pos1) -> con1), a.selected(pos2) -> Map(a.remainder(pos2) -> con2))
        a.combineMaps(pos1, a.toMap(Cell(pos1, con1)), a.toMap(Cell(pos1, con1))) shouldBe
          Map(a.selected(pos1) -> Map(a.remainder(pos1) -> con1))
    }
  }
}

trait TestSlicePosition5D extends TestSlice {
  val pos1 = Position5D(5, "d", DateValue(dfmt.parse("2003-01-01"), DateCodex), "bar", 3.1415)
  val pos2 = Position5D(-5, "w", DateValue(dfmt.parse("1997-01-01"), DateCodex), "rab", -3.1415)
}

class TestOverPosition5D extends TestSlicePosition5D {

  val list = List((Over[Position5D, First.type](First), First), (Over[Position5D, Second.type](Second), Second),
    (Over[Position5D, Third.type](Third), Third), (Over[Position5D, Fourth.type](Fourth), Fourth),
    (Over[Position5D, Fifth.type](Fifth), Fifth))

  "A Over[Position5D]" should "return a Position1D for the selected dimension" in {
    list.map { case (o, d) => o.selected(pos1) shouldBe Position1D(pos1(d)) }
  }

  it should "return a Position4D for the remainder" in {
    list.map { case (o, d) => o.remainder(pos1) shouldBe pos1.remove(d) }
  }

  it should "return a map" in {
    list.map {
      case (o, _) => o.toMap(Cell(pos1, con1)) shouldBe Map(o.selected(pos1) -> Map(o.remainder(pos1) -> con1))
    }
  }

  it should "combine two maps" in {
    list.map {
      case (o, _) =>
        o.combineMaps(pos1, o.toMap(Cell(pos1, con1)), o.toMap(Cell(pos2, con2))) shouldBe
          Map(o.selected(pos1) -> Map(o.remainder(pos1) -> con1), o.selected(pos2) -> Map(o.remainder(pos2) -> con2))
        o.combineMaps(pos1, o.toMap(Cell(pos1, con1)), o.toMap(Cell(pos1, con1))) shouldBe
          Map(o.selected(pos1) -> Map(o.remainder(pos1) -> con1))
    }
  }
}

class TestAlongPosition5D extends TestSlicePosition5D {

  val list = List((Along[Position5D, First.type](First), First), (Along[Position5D, Second.type](Second), Second),
    (Along[Position5D, Third.type](Third), Third), (Along[Position5D, Fourth.type](Fourth), Fourth),
    (Along[Position5D, Fifth.type](Fifth), Fifth))

  "A Along[Position5D]" should "return a Position4D for the selected dimension" in {
    list.map { case (a, d) => a.selected(pos1) shouldBe pos1.remove(d) }
  }

  it should "return a Position1D for the remainder" in {
    list.map { case (a, d) => a.remainder(pos1) shouldBe Position1D(pos1(d)) }
  }

  it should "return a map" in {
    list.map {
      case (a, _) => a.toMap(Cell(pos1, con1)) shouldBe Map(a.selected(pos1) -> Map(a.remainder(pos1) -> con1))
    }
  }

  it should "combine two maps" in {
    list.map {
      case (a, _) =>
        a.combineMaps(pos1, a.toMap(Cell(pos1, con1)), a.toMap(Cell(pos2, con2))) shouldBe
          Map(a.selected(pos1) -> Map(a.remainder(pos1) -> con1), a.selected(pos2) -> Map(a.remainder(pos2) -> con2))
        a.combineMaps(pos1, a.toMap(Cell(pos1, con1)), a.toMap(Cell(pos1, con1))) shouldBe
          Map(a.selected(pos1) -> Map(a.remainder(pos1) -> con1))
    }
  }
}

