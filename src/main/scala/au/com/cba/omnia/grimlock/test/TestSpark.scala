// Copyright 2014-2015 Commonwealth Bank of Australia
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

package au.com.cba.omnia.grimlock.test

import au.com.cba.omnia.grimlock.framework._
import au.com.cba.omnia.grimlock.framework.content._
import au.com.cba.omnia.grimlock.framework.content.metadata._
import au.com.cba.omnia.grimlock.framework.encoding._
import au.com.cba.omnia.grimlock.framework.pairwise._
import au.com.cba.omnia.grimlock.framework.partition._
import au.com.cba.omnia.grimlock.framework.position._
import au.com.cba.omnia.grimlock.framework.sample._
import au.com.cba.omnia.grimlock.framework.Type._
import au.com.cba.omnia.grimlock.framework.utility._
import au.com.cba.omnia.grimlock.framework.window._

import au.com.cba.omnia.grimlock.library.aggregate._
import au.com.cba.omnia.grimlock.library.pairwise._
import au.com.cba.omnia.grimlock.library.partition._
import au.com.cba.omnia.grimlock.library.squash._
import au.com.cba.omnia.grimlock.library.transform._
import au.com.cba.omnia.grimlock.library.window._

import au.com.cba.omnia.grimlock.spark.content.Contents._
import au.com.cba.omnia.grimlock.spark.Matrix._
import au.com.cba.omnia.grimlock.spark.Nameable._
import au.com.cba.omnia.grimlock.spark.Names._
import au.com.cba.omnia.grimlock.spark.partition.Partitions._
import au.com.cba.omnia.grimlock.spark.position.Positions._
import au.com.cba.omnia.grimlock.spark.position.PositionDistributable._
import au.com.cba.omnia.grimlock.spark.transform._
import au.com.cba.omnia.grimlock.spark.Types._

import org.apache.spark._
import org.apache.spark.rdd._

object TestSparkReader {
  def read4TupleDataAddDate(file: String)(implicit sc: SparkContext): RDD[Cell[Position3D]] = {
    def hashDate(v: String) = {
      val cal = java.util.Calendar.getInstance()
      cal.setTime(DateCodex.fromValue(DateCodex.decode("2014-05-14").get))
      cal.add(java.util.Calendar.DATE, -(v.hashCode % 21)) // Generate 3 week window prior to date
      DateValue(cal.getTime(), DateCodex)
    }

    sc.textFile(file)
      .flatMap {
        _.trim.split(java.util.regex.Pattern.quote("|"), 4) match {
          case Array(i, f, e, v) =>
            val schema = e match {
              case StringCodex.name => NominalSchema[Codex.StringCodex]()
              case _ => scala.util.Try(v.toLong).toOption match {
                case Some(_) => ContinuousSchema[Codex.LongCodex](Long.MinValue, Long.MaxValue)
                case None => ContinuousSchema[Codex.DoubleCodex](Double.MinValue, Double.MaxValue)
              }
            }

            schema.decode(v).map { case c => Cell(Position3D(i, f, hashDate(v)), c) }
          case _ => None
        }
      }
  }
}

object TestSpark1 {

  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val data = TestSparkReader.read4TupleDataAddDate(args(1))

    data
      .save("./tmp.spark/dat1.out", descriptive=true)

    data
      .set(Position3D("iid:1548763", "fid:Y", DateCodex.decode("2014-04-26").get),
        Content(ContinuousSchema[Codex.LongCodex](), 1234))
      .slice(Over(First), "iid:1548763", true)
      .save("./tmp.spark/dat2.out", descriptive=true)

    load3D("smallInputfile.txt")
      .save("./tmp.spark/dat3.out", descriptive=true)
  }
}

object TestSpark2 {

  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val data = TestSparkReader.read4TupleDataAddDate(args(1))

    (data.names(Over(First)) ++ data.names(Over(Second)) ++ data.names(Over(Third)))
      .renumber
      .save("./tmp.spark/nm0.out", descriptive=true)

    data
      .names(Over(Second))
      .moveToFront("fid:Z")
      .save("./tmp.spark/nm1.out", descriptive=true)

    data
      .names(Over(Second))
      .slice("fid:M", false)
      .save("./tmp.spark/nm2.out", descriptive=true)

    data
      .names(Over(Second))
      .set(Map("fid:A" -> 100L, "fid:C" -> 200L))
      .save("./tmp.spark/nm3.out", descriptive=true)

    data
      .names(Over(Second))
      .moveToBack("fid:B")
      .save("./tmp.spark/nm4.out", descriptive=true)

    data
      .names(Over(Second))
      .slice(""".*[BCD]$""".r, true, "")
      .save("./tmp.spark/nm5.out", descriptive=true)
  }
}


object TestSpark {

  def mainX(args : Array[String]) {
    if (args.length == 2 && args(0) == "spark") doTest(args(1))
  }

  case class Sample500() extends Sampler with Select {
    def select[P <: Position](cell: Cell[P]): Boolean = cell.content.value gtr 500
  }

  def doTest(master: String) = {
    val spark = new SparkContext(master, "Test Spark", new SparkConf())

    val data = load2D("exampleInput.txt")(spark)

    data
      .sample(Sample500())
      .save("./tmp.spark/flt1.txt")

    data
      .names(Over(First))
      .save("./tmp.spark/nm1.txt")

    data
      .names(Along(First))
      .moveToFront("fid:C")
      .moveToBack("fid:B")
      .save("./tmp.spark/nm2.txt")

    data
      .domain()
      .save("./tmp.spark/dmn.txt")

    spark.stop()
  }
}

