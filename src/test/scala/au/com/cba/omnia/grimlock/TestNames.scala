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

import au.com.cba.omnia.grimlock._
import au.com.cba.omnia.grimlock.encoding._
import au.com.cba.omnia.grimlock.position._

import au.com.cba.omnia.grimlock.ScaldingNames._
import au.com.cba.omnia.grimlock.SparkNames._

import com.twitter.scalding._
import com.twitter.scalding.bdd._

trait TestNames extends TestGrimlock {

  val data = List((Position1D("fid:A"), 0L), (Position1D("fid:AA"), 1L), (Position1D("fid:B"), 2L),
    (Position1D("fid:C"), 3L), (Position1D("fid:D"), 4L), (Position1D("fid:E"), 5L))

  val result1 = data

  val result2 = List((Position1D("fid:A"), 0), (Position1D("fid:AA"), 1))

  val result3 = List()

  val result4 = List((Position1D("fid:B"), 0L), (Position1D("fid:C"), 1L), (Position1D("fid:D"), 2L),
    (Position1D("fid:E"), 3L))

  val result5 = data

  val result6 = List((Position1D("fid:AA"), 0))

  val result7 = List((Position1D("fid:AA"), 0), (Position1D("fid:B"), 1))

  val result8 = data.map {
    case (p @ Position1D(StringValue("fid:C")), _) => (p, 123)
    case x => x
  }

  val result9 = data.map {
    case (p @ Position1D(StringValue("fid:C")), _) => (p, 123)
    case (p @ Position1D(StringValue("fid:D")), _) => (p, 456)
    case x => x
  }

  val result10 = List((Position1D("fid:A"), 1L), (Position1D("fid:AA"), 2L), (Position1D("fid:B"), 3L),
    (Position1D("fid:C"), 0L), (Position1D("fid:D"), 4L), (Position1D("fid:E"), 5L))

  val result11 = List((Position1D("fid:A"), 0L), (Position1D("fid:AA"), 1L), (Position1D("fid:B"), 2L),
    (Position1D("fid:C"), 5L), (Position1D("fid:D"), 3L), (Position1D("fid:E"), 4L))
}

class TypedScaldingNames extends TestNames with TBddDsl {

  "A Names" should "renumber" in {
    Given {
      data.map { case (p, i) => (p, i + 5) }
    } When {
      names: TypedPipe[(Position1D, Long)] =>
        names.renumber()
    } Then {
      _.toList.sortBy(_._1) shouldBe result1
    }
  }

  it should "slice and keep by regular expression" in {
    Given {
      data
    } When {
      names: TypedPipe[(Position1D, Long)] =>
        names.slice("fid:A.*".r, true, "|")
    } Then {
      _.toList.sortBy(_._1) shouldBe result2
    }
  }

  it should "slice and keep nothing by regular expression" in {
    Given {
      data
    } When {
      names: TypedPipe[(Position1D, Long)] =>
        names.slice("not.there.*".r, true, "|")
    } Then {
      _.toList.sortBy(_._1) shouldBe result3
    }
  }

  it should "slice and remove by regular expression" in {
    Given {
      data
    } When {
      names: TypedPipe[(Position1D, Long)] =>
        names.slice("fid:A.*".r, false, "|")
    } Then {
      _.toList.sortBy(_._1) shouldBe result4
    }
  }

  it should "slice and remove nothing by regular expression" in {
    Given {
      data
    } When {
      names: TypedPipe[(Position1D, Long)] =>
        names.slice("not.there.*".r, false, "|")
    } Then {
      _.toList.sortBy(_._1) shouldBe result5
    }
  }

  it should "slice and keep by single name" in {
    Given {
      data
    } When {
      names: TypedPipe[(Position1D, Long)] =>
        names.slice("fid:AA", true)
    } Then {
      _.toList.sortBy(_._1) shouldBe result6
    }
  }

  it should "slice and keep by multiple names" in {
    Given {
      data
    } When {
      names: TypedPipe[(Position1D, Long)] =>
        names.slice(List("fid:AA", "fid:B", "not.there"), true)
    } Then {
      _.toList.sortBy(_._1) shouldBe result7
    }
  }

  it should "set a single name" in {
    Given {
      data
    } When {
      names: TypedPipe[(Position1D, Long)] =>
        names.set("fid:C", 123)
    } Then {
      _.toList.sortBy(_._1) shouldBe result8
    }
  }

  it should "set multiple names" in {
    Given {
      data
    } When {
      names: TypedPipe[(Position1D, Long)] =>
        names.set(Map("fid:C" -> 123, "fid:D" -> 456, "not.there" -> 789))
    } Then {
      _.toList.sortBy(_._1) shouldBe result9
    }
  }

  it should "move a name to front" in {
    Given {
      data
    } When {
      names: TypedPipe[(Position1D, Long)] =>
        names.moveToFront("fid:C")
    } Then {
      _.toList.sortBy(_._1) shouldBe result10
    }
  }

  it should "move a name to back" in {
    Given {
      data
    } When {
      names: TypedPipe[(Position1D, Long)] =>
        names.moveToBack("fid:C")
    } Then {
      _.toList.sortBy(_._1) shouldBe result11
    }
  }
}

class TestSparkNames extends TestNames {

  "A Names" should "renumber" in {
    toRDD(data)
      .map { case (p, i) =>  (p, i + 5) }
      .renumber()
      .toList.sortBy(_._1) shouldBe result1
  }

  it should "slice and keep by regular expression" in {
    toRDD(data)
      .slice("fid:A.*".r, true, "|")
      .toList.sortBy(_._1) shouldBe result2
  }

  it should "slice and keep nothing by regular expression" in {
    toRDD(data)
      .slice("not.there.*".r, true, "|")
      .toList.sortBy(_._1) shouldBe result3
  }

  it should "slice and remove by regular expression" in {
    toRDD(data)
      .slice("fid:A.*".r, false, "|")
      .toList.sortBy(_._1) shouldBe result4
  }

  it should "slice and remove nothing by regular expression" in {
    toRDD(data)
      .slice("not.there.*".r, false, "|")
      .toList.sortBy(_._1) shouldBe result5
  }

  it should "slice and keep by single name" in {
    toRDD(data)
      .slice("fid:AA", true)
      .toList.sortBy(_._1) shouldBe result6
  }

  it should "slice and keep by multiple names" in {
    toRDD(data)
      .slice(List("fid:AA", "fid:B", "not.there"), true)
      .toList.sortBy(_._1) shouldBe result7
  }

  it should "set a single name" in {
    toRDD(data)
      .set("fid:C", 123)
      .toList.sortBy(_._1) shouldBe result8
  }

  it should "set multiple names" in {
    toRDD(data)
      .set(Map("fid:C" -> 123, "fid:D" -> 456, "not.there" -> 789))
      .toList.sortBy(_._1) shouldBe result9
  }

  it should "move a name to front" in {
    toRDD(data)
      .moveToFront("fid:C")
      .toList.sortBy(_._1) shouldBe result10
  }

  it should "move a name to back" in {
    toRDD(data)
      .moveToBack("fid:C")
      .toList.sortBy(_._1) shouldBe result11
  }
}

