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

import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.rdd.RDD

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

object TestSpark3 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val data = TestSparkReader.read4TupleDataAddDate(args(1))

    (data.types(Over(First)) ++ data.types(Over(Second)) ++ data.types(Over(Third)))
      .save("./tmp.spark/typ1.out", descriptive=true)

    (data.types(Over(First), true) ++ data.types(Over(Second), true) ++ data.types(Over(Third), true))
      .save("./tmp.spark/typ2.out", descriptive=true)
  }
}

class TestSpark4 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val data = TestSparkReader.read4TupleDataAddDate(args(1))

    data
      .slice(Over(Second), "fid:B", true)
      .save("./tmp.spark/scl0.out", descriptive=true)

    data
      .slice(Over(Second), List("fid:A", "fid:B"), true)
      .slice(Over(First), "iid:0221707", true)
      .save("./tmp.spark/scl1.out", descriptive=true)

    val rem = List("fid:B", "fid:D", "fid:F", "fid:H", "fid:J", "fid:L", "fid:N",
                   "fid:P", "fid:R", "fid:T", "fid:V", "fid:X", "fid:Z")
    data
      .slice(Over(Second), data.names(Over(Second)).slice(rem, false), false)
      .save("./tmp.spark/scl2.out", descriptive=true)
  }
}

class TestSpark5 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val data = TestSparkReader.read4TupleDataAddDate(args(1))

    data
      .slice(Over(Second), List("fid:A", "fid:B"), true)
      .slice(Over(First), "iid:0221707", true)
      .squash(Third, PreservingMaxPosition())
      .save("./tmp.spark/sqs1.out", descriptive=true)

    data
      .squash(Third, PreservingMaxPosition())
      .save("./tmp.spark/sqs2.out", descriptive=true)

    data
      .slice(Over(First), List("iid:0064402", "iid:0066848", "iid:0076357", "iid:0216406", "iid:0221707", "iid:0262443",
                               "iid:0364354", "iid:0375226", "iid:0444510", "iid:1004305"), true)
      .squash(Third, PreservingMaxPosition())
      .saveAsCSV(Over(First), "./tmp.spark/sqs3.out")

    data
      .slice(Over(First), List("iid:0064402", "iid:0066848", "iid:0076357", "iid:0216406", "iid:0221707", "iid:0262443",
                               "iid:0364354", "iid:0375226", "iid:0444510", "iid:1004305"), true)
      .slice(Over(Second), List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"), true)
      .squash(Third, PreservingMaxPosition())
      .saveAsCSV(Over(First), "./tmp.spark/sqs4.out")
  }
}

class TestSpark6 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val data = TestSparkReader.read4TupleDataAddDate(args(1))

    data
      .which((c: Cell[Position3D]) => c.content.schema.kind.isSpecialisationOf(Numerical))
      .save("./tmp.spark/whc1.out", descriptive=true)

    data
      .which((c: Cell[Position3D]) => ! c.content.value.isInstanceOf[StringValue])
      .save("./tmp.spark/whc2.out", descriptive=true)

    data
      .get(data.which((c: Cell[Position3D]) =>
        (c.content.value equ 666) || (c.content.value leq 11.0) || (c.content.value equ "KQUPKFEH")))
      .save("./tmp.spark/whc3.out", descriptive=true)

    data
      .which((c: Cell[Position3D]) => c.content.value.isInstanceOf[LongValue])
      .save("./tmp.spark/whc4.out", descriptive=true)

    TestSparkReader.read4TupleDataAddDate(args(1))
      .slice(Over(First), List("iid:0064402", "iid:0066848", "iid:0076357", "iid:0216406", "iid:0221707", "iid:0262443",
                               "iid:0364354", "iid:0375226", "iid:0444510", "iid:1004305"), true)
      .slice(Over(Second), List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"), true)
      .squash(Third, PreservingMaxPosition())
      .summariseAndExpand(Along(First), List(Count("count"), Mean("mean"), Min("min"), Max("max"), MaxAbs("max.abs")))
      .which(Over(Second), List(("count", (c: Cell[Position2D]) => c.content.value leq 2),
                                ("min", (c: Cell[Position2D]) => c.content.value equ 107)))
      .save("./tmp.spark/whc5.out", descriptive=true)
  }
}

class TestSpark7 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val data = TestSparkReader.read4TupleDataAddDate(args(1))

    data
      .get(Position3D("iid:1548763", "fid:Y", DateCodex.decode("2014-04-26").get))
      .save("./tmp.spark/get1.out", descriptive=true)

    data
      .get(List(Position3D("iid:1548763", "fid:Y", DateCodex.decode("2014-04-26").get),
                Position3D("iid:1303823", "fid:A", DateCodex.decode("2014-05-05").get)))
      .save("./tmp.spark/get2.out", descriptive=true)
  }
}

class TestSpark8 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val data = TestSparkReader.read4TupleDataAddDate(args(1))

    data
      .slice(Over(Second), "fid:B", true)
      .squash(Third, PreservingMaxPosition())
      .unique
      .save("./tmp.spark/uniq.out", descriptive=true)

    load2D("mutualInputfile.txt")
      .unique(Over(Second))
      .save("./tmp.spark/uni2.out")

    data
      .slice(Over(Second), List("fid:A", "fid:B", "fid:Y", "fid:Z"), true)
      .slice(Over(First), List("iid:0221707", "iid:0364354"), true)
      .squash(Third, PreservingMaxPosition())
      .saveAsCSV(Over(Second), "./tmp.spark/test.csv")
      .saveAsCSV(Over(First), "./tmp.spark/tset.csv", writeHeader=false, separator=",")

    data
      .slice(Over(Second), List("fid:A", "fid:B", "fid:Y", "fid:Z"), true)
      .slice(Over(First), List("iid:0221707", "iid:0364354"), true)
      .squash(Third, PreservingMaxPosition())
      .permute(Second, First)
      .save("./tmp.spark/trs1.out", descriptive=true)

    data
      .slice(Over(Second), List("fid:A", "fid:B", "fid:Y", "fid:Z"), true)
      .slice(Over(First), List("iid:0221707", "iid:0364354"), true)
      .squash(Third, PreservingMaxPosition())
      .save("./tmp.spark/data.txt")
  }
}

class TestSpark9 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val data = TestSparkReader.read4TupleDataAddDate(args(1))

    case class StringPartitioner(dim: Dimension) extends Partitioner with Assign {
      type T = String

      def assign[P <: Position](pos: P): Collection[T] = {
        Collection(List(pos(dim) match {
          case StringValue("fid:A") => "training"
          case StringValue("fid:B") => "testing"
        }, "scoring"))
      }
    }

    val prt1 = data
      .slice(Over(Second), List("fid:A", "fid:B"), true)
      .slice(Over(First), List("iid:0221707", "iid:0364354"), true)
      .squash(Third, PreservingMaxPosition())
      .partition(StringPartitioner(Second))

    prt1
      .save("./tmp.spark/prt1.out", descriptive=true)

    case class IntTuplePartitioner(dim: Dimension) extends Partitioner with Assign {
      type T = (Int, Int, Int)

      def assign[P <: Position](pos: P): Collection[T] = {
        Collection(List(pos(dim) match {
          case StringValue("fid:A") => (1, 0, 0)
          case StringValue("fid:B") => (0, 1, 0)
        }, (0, 0, 1)))
      }
    }

    data
      .slice(Over(Second), List("fid:A", "fid:B"), true)
      .slice(Over(First), List("iid:0221707", "iid:0364354"), true)
      .squash(Third, PreservingMaxPosition())
      .partition(IntTuplePartitioner(Second))
      .save("./tmp.spark/prt2.out", descriptive=true)

    prt1
      .get("training")
      .save("./tmp.spark/train.out", descriptive=true)

    prt1
      .get("testing")
      .save("./tmp.spark/test.out", descriptive=true)

    prt1
      .get("scoring")
      .save("./tmp.spark/score.out", descriptive=true)
  }
}

class TestSpark10 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val data = TestSparkReader.read4TupleDataAddDate(args(1))

    data
      .summariseAndExpand(Over(Second), Mean("mean", strict=true, nan=true))
      .saveAsCSV(Over(Second), "./tmp.spark/agg1.csv")

    data
      .slice(Over(First), List("iid:0064402", "iid:0066848", "iid:0076357", "iid:0216406", "iid:0221707", "iid:0262443",
                               "iid:0364354", "iid:0375226", "iid:0444510", "iid:1004305"), true)
      .squash(Third, PreservingMaxPosition())
      .summariseAndExpand(Along(Second), Count("count"))
      .saveAsCSV(Over(Second), "./tmp.spark/agg2.csv")

    data
      .slice(Over(First), List("iid:0064402", "iid:0066848", "iid:0076357", "iid:0216406", "iid:0221707", "iid:0262443",
                               "iid:0364354", "iid:0375226", "iid:0444510", "iid:1004305"), true)
      .squash(Third, PreservingMaxPosition())
      .summariseAndExpand(Along(First), List(Count("count"), Moments("mean", "sd", "skewness", "kurtosis"), Min("min"),
        Max("max"), MaxAbs("max.abs")))
      .saveAsCSV(Over(Second), "./tmp.spark/agg3.csv")
  }
}

class TestSpark11 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val data = TestSparkReader.read4TupleDataAddDate(args(1))

    data
      .slice(Over(Second), List("fid:A", "fid:B", "fid:Y", "fid:Z"), true)
      .slice(Over(First), List("iid:0221707", "iid:0364354"), true)
      .transform(Indicator(Second, "%1$s.ind"))
      .save("./tmp.spark/trn2.out", descriptive=true)

    data
      .slice(Over(Second), List("fid:A", "fid:B", "fid:Y", "fid:Z"), true)
      .slice(Over(First), List("iid:0221707", "iid:0364354"), true)
      .squash(Third, PreservingMaxPosition())
      .transform(Binarise(Second))
      .saveAsCSV(Over(Second), "./tmp.spark/trn3.out")
  }
}

class TestSpark12 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val data = TestSparkReader.read4TupleDataAddDate(args(1))
      .slice(Over(Second), List("fid:A", "fid:B", "fid:Y", "fid:Z"), true)
      .slice(Over(First), List("iid:0221707", "iid:0364354"), true)

    data
      .squash(Third, PreservingMaxPosition())
      .fill(Content(ContinuousSchema[Codex.LongCodex](), 0))
      .saveAsCSV(Over(Second), "./tmp.spark/fll1.out")

    data
      .fill(Content(ContinuousSchema[Codex.LongCodex](), 0))
      .save("./tmp.spark/fll3.out", descriptive=true)
  }
}

class TestSpark13 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val all = TestSparkReader.read4TupleDataAddDate(args(1))
    val data = all
      .slice(Over(First), List("iid:0064402", "iid:0066848", "iid:0076357", "iid:0216406", "iid:0221707", "iid:0262443",
                               "iid:0364354", "iid:0375226", "iid:0444510", "iid:1004305"), true)
      .slice(Over(Second), List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"), true)
      .squash(Third, PreservingMaxPosition())

    val inds = data
      .transform(Indicator(Second, "%1$s.ind"))
      .fill(Content(ContinuousSchema[Codex.LongCodex](), 0))

    data
      .join(Over(First), inds)
      .fill(Content(ContinuousSchema[Codex.LongCodex](), 0))
      .saveAsCSV(Over(Second), "./tmp.spark/fll2.out")

    data
      .fill(Over(Second), all.summarise(Over(Second), Mean(strict=true, nan=true)))
      .join(Over(First), inds)
      .saveAsCSV(Over(Second), "./tmp.spark/fll4.out")
  }
}

class TestSpark14 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val data = TestSparkReader.read4TupleDataAddDate(args(1))
      .slice(Over(Second), List("fid:A", "fid:B", "fid:Y", "fid:Z"), true)
      .slice(Over(First), List("iid:0221707", "iid:0364354"), true)

    data
      .change(Over(Second), "fid:A", NominalSchema[Codex.LongCodex]())
      .save("./tmp.spark/chg1.out", descriptive=true)
  }
}

class TestSpark15 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val data = TestSparkReader.read4TupleDataAddDate(args(1))

    data
      .slice(Over(Second), List("fid:A", "fid:C", "fid:E", "fid:G"), true)
      .slice(Over(First), List("iid:0221707", "iid:0364354"), true)
      .summariseAndExpand(Along(Third), Sum("sum"))
      .melt(Third, Second)
      .saveAsCSV(Over(Second), "./tmp.spark/rsh1.out")

    val inds = data
      .slice(Over(First), List("iid:0064402", "iid:0066848", "iid:0076357", "iid:0216406", "iid:0221707", "iid:0262443",
                               "iid:0364354", "iid:0375226", "iid:0444510", "iid:1004305"), true)
      .slice(Over(Second), List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"), true)
      .squash(Third, PreservingMaxPosition())
      .transform(Indicator(Second, "%1$s.ind"))
      .saveAsCSV(Over(Second), "./tmp.spark/trn1.csv")

    data
      .slice(Over(First), List("iid:0064402", "iid:0066848", "iid:0076357", "iid:0216406", "iid:0221707", "iid:0262443",
                               "iid:0364354", "iid:0375226", "iid:0444510", "iid:1004305"), true)
      .slice(Over(Second), List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"), true)
      .squash(Third, PreservingMaxPosition())
      .join(Over(First), inds)
      .saveAsCSV(Over(Second), "./tmp.spark/jn1.csv")
  }
}

class TestSpark16 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val data = TestSparkReader.read4TupleDataAddDate(args(1))

    case class HashSample() extends Sampler with Select {
      def select[P <: Position](cell: Cell[P]): Boolean = (cell.position(First).toString.hashCode % 25) == 0
    }

    data
      .sample(HashSample())
      .save("./tmp.spark/smp1.out")
  }
}

class TestSpark17 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val data = TestSparkReader.read4TupleDataAddDate(args(1))
      .slice(Over(First), List("iid:0064402", "iid:0066848", "iid:0076357", "iid:0216406", "iid:0221707", "iid:0262443",
                               "iid:0364354", "iid:0375226", "iid:0444510", "iid:1004305"), true)
      .slice(Over(Second), List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"), true)
      .squash(Third, PreservingMaxPosition())

    val stats = data
      .summariseAndExpand(Along(First), List(Count("count"), Mean("mean"), Min("min"), Max("max"), MaxAbs("max.abs")))
      .toMap(Over(First))

    data
      .transformWithValue(Normalise(Second, "max.abs"), stats)
      .saveAsCSV(Over(Second), "./tmp.spark/trn6.csv")

    case class Sample500() extends Sampler with Select {
      def select[P <: Position](cell: Cell[P]): Boolean = cell.content.value gtr 500
    }

    data
      .sample(Sample500())
      .saveAsCSV(Over(Second), "./tmp.spark/flt1.csv")

    case class RemoveGreaterThanMean(dim: Dimension) extends Sampler with SelectWithValue {
      type V = Map[Position1D, Map[Position1D, Content]]

      def select[P <: Position](cell: Cell[P], ext: V): Boolean = {
        if (cell.content.schema.kind.isSpecialisationOf(Numerical)) {
          cell.content.value leq ext(Position1D(cell.position(dim)))(Position1D("mean")).value
        } else {
          true
        }
      }
    }

    data
      .sampleWithValue(RemoveGreaterThanMean(Second), stats)
      .saveAsCSV(Over(Second), "./tmp.spark/flt2.csv")
  }
}

class TestSpark18 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val data = TestSparkReader.read4TupleDataAddDate(args(1))
      .slice(Over(First), List("iid:0064402", "iid:0066848", "iid:0076357", "iid:0216406", "iid:0221707", "iid:0262443",
                               "iid:0364354", "iid:0375226", "iid:0444510", "iid:1004305"), true)
      .slice(Over(Second), List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"), true)
      .squash(Third, PreservingMaxPosition())

    val stats = data
      .summariseAndExpand(Along(First), List(Count("count"), Mean("mean"), Min("min"), Max("max"), MaxAbs("max.abs")))

    val rem = stats
      .which(Over(Second), "count", (c: Cell[Position2D]) => c.content.value leq 2)
      .names(Over(First))

    data
      .slice(Over(Second), rem, false)
      .saveAsCSV(Over(Second), "./tmp.spark/flt3.csv")
  }
}

class TestSpark19 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val raw = TestSparkReader.read4TupleDataAddDate(args(1))
      .slice(Over(First), List("iid:0064402", "iid:0066848", "iid:0076357", "iid:0216406", "iid:0221707", "iid:0262443",
                               "iid:0364354", "iid:0375226", "iid:0444510", "iid:1004305"), true)
      .slice(Over(Second), List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"), true)
      .squash(Third, PreservingMaxPosition())

    case class CustomPartition[S: Ordering](dim: Dimension, left: S, right: S) extends Partitioner with Assign {
      type T = S

      val bhs = BinaryHashSplit(dim, 7, left, right, base=10)
      def assign[P <: Position](pos: P): Collection[T] = {
        if (pos(dim).toShortString == "iid:0364354") {
          Collection(right)
        } else {
          bhs.assign(pos)
        }
      }
    }

    val parts = raw
      .partition(CustomPartition(First, "train", "test"))

    val stats = parts
      .get("train")
      .summariseAndExpand(Along(First), List(Count("count"), MaxAbs("max.abs")))

    val rem = stats
      .which((c: Cell[Position2D]) => (c.position(Second) equ "count") && (c.content.value leq 2))
      .names(Over(First))

    def cb(key: String, pipe: RDD[Cell[Position2D]]): RDD[Cell[Position2D]] = {
      pipe
        .slice(Over(Second), rem, false)
        .transformWithValue(List(Indicator(Second, "%1$s.ind"), Binarise(Second), Normalise(Second, "max.abs")),
          stats.toMap(Over(First)))
        .fill(Content(ContinuousSchema[Codex.LongCodex](), 0))
        .saveAsCSV(Over(Second), "./tmp.spark/pln_" + key + ".csv")
    }

    parts
      .forEach(List("train", "test"), cb)
  }
}

class TestSpark20 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())

    load3DWithDictionary("./ivoryInputfile1.txt", Dictionary.read("./dict.txt"))
      .save("./tmp.spark/ivr1.out")
  }
}

class TestSpark21 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val data = TestSparkReader.read4TupleDataAddDate(args(1))

    data
      .shape()
      .save("./tmp.spark/siz0.out")

    data
      .size(First)
      .save("./tmp.spark/siz1.out")

    data
      .size(Second)
      .save("./tmp.spark/siz2.out")

    data
      .size(Third)
      .save("./tmp.spark/siz3.out")
  }
}

class TestSpark22 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val data = load2D("numericInputfile.txt")

    case class Diff() extends Windower with Initialise {
      type T = Cell[Position]

      def initialise[P <: Position, D <: Dimension](slice: Slice[P, D])(cell: Cell[slice.S], rem: slice.R): T = {
        Cell(rem, cell.content)
      }
      def present[P <: Position, D <: Dimension](slice: Slice[P, D])(cell: Cell[slice.S], rem: slice.R,
        t: T): (T, Collection[Cell[slice.S#M]]) = {
        (Cell(rem, cell.content), (cell.content.value.asDouble, t.content.value.asDouble) match {
          case (Some(c), Some(l)) =>
            Collection(cell.position.append(rem.toShortString("") + "-" + t.position.toShortString("")),
              Content(ContinuousSchema[Codex.DoubleCodex](), c - l))
          case _ => Collection[Cell[slice.S#M]]()
        })
      }
    }

    data
      .window(Over(First), Diff())
      .save("./tmp.spark/dif1.out")

    data
      .window(Over(Second), Diff())
      .permute(Second, First)
      .save("./tmp.spark/dif2.out")
  }
}

class TestSpark23 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val data = load2D("somePairwise.txt")

    case class DiffSquared() extends Operator with Compute {
      def compute[P <: Position, D <: Dimension](slice: Slice[P, D])(left: Cell[slice.S], right: Cell[slice.S],
        rem: slice.R): Collection[Cell[slice.R#M]] = {
        val xc = left.position.toShortString("")
        val yc = right.position.toShortString("")

        (xc < yc && xc != yc) match {
          case true => Collection(rem.append("(" + xc + "-" + yc + ")^2"),
            Content(ContinuousSchema[Codex.DoubleCodex](),
              math.pow(left.content.value.asLong.get - right.content.value.asLong.get, 2)))
          case false => Collection[Cell[slice.R#M]]
        }
      }
    }

    data
      .pairwise(Over(Second), DiffSquared())
      .save("./tmp.spark/pws1.out")
  }
}

class TestSpark24 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())

    // see http://www.mathsisfun.com/data/correlation.html for data
    val schema = List(("day", NominalSchema[Codex.StringCodex]()),
                      ("temperature", ContinuousSchema[Codex.DoubleCodex]()),
                      ("sales", DiscreteSchema[Codex.LongCodex]()))
    val data = readTable("somePairwise2.txt", schema, separator="|")

    data
      .correlation(Over(Second))
      .save("./tmp.spark/pws2.out")

    val schema2 = List(("day", NominalSchema[Codex.StringCodex]()),
                       ("temperature", ContinuousSchema[Codex.DoubleCodex]()),
                       ("sales", DiscreteSchema[Codex.LongCodex]()),
                       ("neg.sales", DiscreteSchema[Codex.LongCodex]()))
    val data2 = readTable("somePairwise3.txt", schema2, separator="|")

    data2
      .correlation(Over(Second))
      .save("./tmp.spark/pws3.out")
  }
}

class TestSpark25 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())

    load2D("mutualInputfile.txt")
      .mutualInformation(Over(Second))
      .save("./tmp.spark/mi.out")
  }
}

class TestSpark26 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val left = load2D("algebraInputfile1.txt")
    val right = load2D("algebraInputfile2.txt")

    left
      .pairwiseBetween(Over(First), right, Times(comparer=All))
      .save("./tmp.spark/alg.out")
  }
}

class TestSpark27 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())

    // http://www.statisticshowto.com/moving-average/
    load2D("simMovAvgInputfile.txt", first=LongCodex)
      .window(Over(Second), SimpleMovingAverage(5))
      .save("./tmp.spark/sma1.out")

    load2D("simMovAvgInputfile.txt", first=LongCodex)
      .window(Over(Second), SimpleMovingAverage(5, all=true))
      .save("./tmp.spark/sma2.out")

    load2D("simMovAvgInputfile.txt", first=LongCodex)
      .window(Over(Second), CenteredMovingAverage(2))
      .save("./tmp.spark/tma.out")

    load2D("simMovAvgInputfile.txt", first=LongCodex)
      .window(Over(Second), WeightedMovingAverage(5))
      .save("./tmp.spark/wma1.out")

    load2D("simMovAvgInputfile.txt", first=LongCodex)
      .window(Over(Second), WeightedMovingAverage(5, all=true))
      .save("./tmp.spark/wma2.out")

    // http://stackoverflow.com/questions/11074665/how-to-calculate-the-cumulative-average-for-some-numbers
    load1D("cumMovAvgInputfile.txt")
      .window(Along(First), CumulativeMovingAverage())
      .save("./tmp.spark/cma.out")

    // http://www.incrediblecharts.com/indicators/exponential_moving_average.php
    load1D("expMovAvgInputfile.txt")
      .window(Along(First), ExponentialMovingAverage(0.33))
      .save("./tmp.spark/ema.out")
  }
}

class TestSpark28 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val data = List
      .range(0, 16)
      .flatMap { case i => List(("iid:" + i, "fid:A", Content(ContinuousSchema[Codex.LongCodex](), i)),
                                ("iid:" + i, "fid:B", Content(NominalSchema[Codex.StringCodex](), i.toString))) }

    val stats = data
      .summariseAndExpand(Along(First), List(Count("count"), Min("min"), Max("max"), Moments("mean", "sd", "skewness")))
      .toMap(Over(First))

    data
      .transformWithValue(Cut(Second), CutRules.fixed(stats, "min", "max", 4))
      .save("./tmp.spark/cut1.out")

    data
      .transformWithValue(Cut(Second, "%s.square"), CutRules.squareRootChoice(stats, "count", "min", "max"))
      .save("./tmp.spark/cut2.out")

    data
      .transformWithValue(Cut(Second, "%s.sturges"), CutRules.sturgesFormula(stats, "count", "min", "max"))
      .save("./tmp.spark/cut3.out")

    data
      .transformWithValue(Cut(Second, "%s.rice"), CutRules.riceRule(stats, "count", "min", "max"))
      .save("./tmp.spark/cut4.out")

    data
      .transformWithValue(Cut(Second, "%s.doane"), CutRules.doanesFormula(stats, "count", "min", "max", "skewness"))
      .save("./tmp.spark/cut5.out")

    data
      .transformWithValue(Cut(Second, "%s.scott"),
        CutRules.scottsNormalReferenceRule(stats, "count", "min", "max", "sd"))
      .save("./tmp.spark/cut6.out")

    data
      .transformWithValue(Cut(Second, "%s.break"), CutRules.breaks(Map("fid:A" -> List(-1, 4, 8, 12, 16))))
      .save("./tmp.spark/cut7.out")
  }
}

class TestSpark29 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val schema = DiscreteSchema[Codex.LongCodex]()
    val data = List(("mod:123", "iid:A", Content(schema, 1)),
      ("mod:123", "iid:B", Content(schema, 1)),
      ("mod:123", "iid:C", Content(schema, 0)),
      ("mod:123", "iid:D", Content(schema, 1)),
      ("mod:123", "iid:E", Content(schema, 1)),
      ("mod:123", "iid:G", Content(schema, 0)),
      ("mod:123", "iid:H", Content(schema, 1)),
      ("mod:456", "iid:A", Content(schema, 1)),
      ("mod:456", "iid:B", Content(schema, 1)),
      ("mod:456", "iid:C", Content(schema, 1)),
      ("mod:456", "iid:E", Content(schema, 1)),
      ("mod:456", "iid:F", Content(schema, 0)),
      ("mod:456", "iid:G", Content(schema, 1)),
      ("mod:456", "iid:H", Content(schema, 0)))

    data
      .gini(Over(First))
      .save("./tmp.spark/gini.out")

  data
    .map { case (a, b, c) => (b, a, c) }
    .gini(Along(First))
    .save("./tmp.spark/inig.out")
  }
}

