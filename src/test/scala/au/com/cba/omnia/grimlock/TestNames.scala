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

import org.apache.spark.rdd._

import org.scalatest._

import scala.collection.mutable

trait TestNames {

  val data = List((Position1D("fid:A"), 0L),
    (Position1D("fid:AA"), 1L),
    (Position1D("fid:B"), 2L),
    (Position1D("fid:C"), 3L),
    (Position1D("fid:D"), 4L),
    (Position1D("fid:E"), 5L))
}

class TypedScaldingNames extends WordSpec with Matchers with TBddDsl with TestNames {

  "A Names" should {
    "renumber" in {
      Given {
        data.map { case (p, i) => (p, i + 5) }
      } When {
        names: TypedPipe[(Position1D, Long)] =>
          names.renumber()
      } Then {
        buffer: mutable.Buffer[(Position1D, Long)] =>
          buffer.toList shouldBe data
      }
    }

    "slice and keep by regular expression" in {
      Given {
        data
      } When {
        names: TypedPipe[(Position1D, Long)] =>
          names.slice("fid:A.*".r, true, "|")
      } Then {
        buffer: mutable.Buffer[(Position1D, Long)] =>
          buffer.toList shouldBe List((Position1D("fid:A"), 0), (Position1D("fid:AA"), 1))
      }
    }

    "slice and keep nothing by regular expression" in {
      Given {
        data
      } When {
        names: TypedPipe[(Position1D, Long)] =>
          names.slice("not.there.*".r, true, "|")
      } Then {
        buffer: mutable.Buffer[(Position1D, Long)] =>
          buffer.toList shouldBe List()
      }
    }

    "slice and remove by regular expression" in {
      Given {
        data
      } When {
        names: TypedPipe[(Position1D, Long)] =>
          names.slice("fid:A.*".r, false, "|")
      } Then {
        buffer: mutable.Buffer[(Position1D, Long)] =>
          buffer.toList shouldBe List((Position1D("fid:B"), 0L), (Position1D("fid:C"), 1L),
            (Position1D("fid:D"), 2L), (Position1D("fid:E"), 3L))
      }
    }

    "slice and remove nothing by regular expression" in {
      Given {
        data
      } When {
        names: TypedPipe[(Position1D, Long)] =>
          names.slice("not.there.*".r, false, "|")
      } Then {
        buffer: mutable.Buffer[(Position1D, Long)] =>
          buffer.toList shouldBe data
      }
    }

    "slice and keep by single name" in {
      Given {
        data
      } When {
        names: TypedPipe[(Position1D, Long)] =>
          names.slice("fid:AA", true)
      } Then {
        buffer: mutable.Buffer[(Position1D, Long)] =>
          buffer.toList shouldBe List((Position1D("fid:AA"), 0))
      }
    }

    "slice and keep by multiple names" in {
      Given {
        data
      } When {
        names: TypedPipe[(Position1D, Long)] =>
          names.slice(List("fid:AA", "fid:B", "not.there"), true)
      } Then {
        buffer: mutable.Buffer[(Position1D, Long)] =>
          buffer.toList shouldBe List((Position1D("fid:AA"), 0), (Position1D("fid:B"), 1))
      }
    }

    "set a single name" in {
      Given {
        data
      } When {
        names: TypedPipe[(Position1D, Long)] =>
          names.set("fid:C", 123)
      } Then {
        buffer: mutable.Buffer[(Position1D, Long)] =>
          buffer.toList shouldBe data.map {
            case (p @ Position1D(StringValue("fid:C")), _) => (p, 123)
            case x => x
          }
      }
    }

    "set multiple names" in {
      Given {
        data
      } When {
        names: TypedPipe[(Position1D, Long)] =>
          names.set(Map("fid:C" -> 123, "fid:D" -> 456, "not.there" -> 789))
      } Then {
        buffer: mutable.Buffer[(Position1D, Long)] =>
          buffer.toList shouldBe data.map {
            case (p @ Position1D(StringValue("fid:C")), _) => (p, 123)
            case (p @ Position1D(StringValue("fid:D")), _) => (p, 456)
            case x => x
          }
      }
    }

    "move a name to front" in {
      Given {
        data
      } When {
        names: TypedPipe[(Position1D, Long)] =>
          names.moveToFront("fid:C")
      } Then {
        buffer: mutable.Buffer[(Position1D, Long)] =>
          buffer.toList shouldBe List((Position1D("fid:A"), 1L), (Position1D("fid:AA"), 2L),
            (Position1D("fid:B"), 3L), (Position1D("fid:C"), 0L), (Position1D("fid:D"), 4L),
            (Position1D("fid:E"), 5L))
      }
    }

    "move a name to back" in {
      Given {
        data
      } When {
        names: TypedPipe[(Position1D, Long)] =>
          names.moveToBack("fid:C")
      } Then {
        buffer: mutable.Buffer[(Position1D, Long)] =>
          buffer.toList shouldBe List((Position1D("fid:A"), 0L), (Position1D("fid:AA"), 1L),
            (Position1D("fid:B"), 2L), (Position1D("fid:C"), 5L), (Position1D("fid:D"), 3L),
            (Position1D("fid:E"), 4L))
      }
    }
  }
}

class TestSparkNames extends FlatSpec with Matchers with TestNames {

  "A Names" should "renumber" in {
    TestSpark.spark
      .parallelize(data)
      .map { case (p, i) =>  (p, i + 5) }
      .renumber()
      .toLocalIterator
      .toList should be (data)
  }

  it should "slice and keep by regular expression" in {
    TestSpark.spark
      .parallelize(data)
      .slice("fid:A.*".r, true, "|")
      .toLocalIterator
      .toList should be (List((Position1D("fid:A"), 0), (Position1D("fid:AA"), 1)))
  }

  it should "slice and keep nothing by regular expression" in {
    TestSpark.spark
      .parallelize(data)
      .slice("not.there.*".r, true, "|")
      .toLocalIterator
      .toList should be (List())
  }

  it should "slice and remove by regular expression" in {
    TestSpark.spark
      .parallelize(data)
      .slice("fid:A.*".r, false, "|")
      .toLocalIterator
      .toList should be (List((Position1D("fid:B"), 0L), (Position1D("fid:C"), 1L), (Position1D("fid:D"), 2L),
        (Position1D("fid:E"), 3L)))
  }

  it should "slice and remove nothing by regular expression" in {
    TestSpark.spark
      .parallelize(data)
      .slice("not.there.*".r, false, "|")
      .toLocalIterator
      .toList should be (data)
  }

  it should "slice and keep by single name" in {
    TestSpark.spark
      .parallelize(data)
      .slice("fid:AA", true)
      .toLocalIterator
      .toList should be (List((Position1D("fid:AA"), 0)))
  }

  it should "slice and keep by multiple names" in {
    TestSpark.spark
      .parallelize(data)
      .slice(List("fid:AA", "fid:B", "not.there"), true)
      .toLocalIterator
      .toList should be (List((Position1D("fid:AA"), 0), (Position1D("fid:B"), 1)))
  }

  it should "set a single name" in {
    TestSpark.spark
      .parallelize(data)
      .set("fid:C", 123)
      .toLocalIterator
      .toList should be (data.map {
        case (p @ Position1D(StringValue("fid:C")), _) => (p, 123)
        case x => x
      })
  }

  it should "set multiple names" in {
    TestSpark.spark
      .parallelize(data)
      .set(Map("fid:C" -> 123, "fid:D" -> 456, "not.there" -> 789))
      .toLocalIterator
      .toList should be (data.map {
        case (p @ Position1D(StringValue("fid:C")), _) => (p, 123)
        case (p @ Position1D(StringValue("fid:D")), _) => (p, 456)
        case x => x
      })
  }

  it should "move a name to front" in {
    TestSpark.spark
      .parallelize(data)
      .moveToFront("fid:C")
      .toLocalIterator
      .toList should be (List((Position1D("fid:A"), 1L), (Position1D("fid:AA"), 2L), (Position1D("fid:B"), 3L),
        (Position1D("fid:C"), 0L), (Position1D("fid:D"), 4L), (Position1D("fid:E"), 5L)))
  }

  it should "move a name to back" in {
    TestSpark.spark
      .parallelize(data)
      .moveToBack("fid:C")
      .toLocalIterator
      .toList should be (List((Position1D("fid:A"), 0L), (Position1D("fid:AA"), 1L), (Position1D("fid:B"), 2L),
        (Position1D("fid:C"), 5L), (Position1D("fid:D"), 3L), (Position1D("fid:E"), 4L)))
  }
}

