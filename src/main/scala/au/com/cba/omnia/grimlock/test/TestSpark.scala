// Copyright 2014,2015 Commonwealth Bank of Australia
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
import au.com.cba.omnia.grimlock.framework.aggregate._
import au.com.cba.omnia.grimlock.framework.content._
import au.com.cba.omnia.grimlock.framework.content.metadata._
import au.com.cba.omnia.grimlock.framework.encoding._
import au.com.cba.omnia.grimlock.framework.pairwise._
import au.com.cba.omnia.grimlock.framework.partition._
import au.com.cba.omnia.grimlock.framework.position._
import au.com.cba.omnia.grimlock.framework.sample._
import au.com.cba.omnia.grimlock.framework.transform._
import au.com.cba.omnia.grimlock.framework.Type._
import au.com.cba.omnia.grimlock.framework.window._

import au.com.cba.omnia.grimlock.library.aggregate._
import au.com.cba.omnia.grimlock.library.pairwise._
import au.com.cba.omnia.grimlock.library.partition._
import au.com.cba.omnia.grimlock.library.squash._
import au.com.cba.omnia.grimlock.library.transform._
import au.com.cba.omnia.grimlock.library.window._

import au.com.cba.omnia.grimlock.spark.content.Contents._
import au.com.cba.omnia.grimlock.spark.Matrix._
import au.com.cba.omnia.grimlock.spark.Matrixable._
import au.com.cba.omnia.grimlock.spark.Names._
import au.com.cba.omnia.grimlock.spark.partition.Partitions._
import au.com.cba.omnia.grimlock.spark.position.Positions._
import au.com.cba.omnia.grimlock.spark.position.PositionDistributable._
import au.com.cba.omnia.grimlock.spark.Predicateable._
import au.com.cba.omnia.grimlock.spark.transform._
import au.com.cba.omnia.grimlock.spark.Types._

import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.rdd.RDD

object TestSparkReader {
  def load4TupleDataAddDate(file: String)(implicit sc: SparkContext): RDD[Cell[Position3D]] = {
    def hashDate(v: String) = {
      val cal = java.util.Calendar.getInstance()
      cal.setTime(DateCodex().fromValue(DateCodex().decode("2014-05-14").get))
      cal.add(java.util.Calendar.DATE, -(v.hashCode % 21)) // Generate 3 week window prior to date
      DateValue(cal.getTime(), DateCodex())
    }

    sc.textFile(file)
      .flatMap {
        _.trim.split(java.util.regex.Pattern.quote("|"), 4) match {
          case Array(i, f, e, v) =>
            val schema = e match {
              case StringCodex.name => NominalSchema(StringCodex)
              case _ => scala.util.Try(v.toLong).toOption match {
                case Some(_) => ContinuousSchema[Codex.LongCodex](LongCodex, Long.MinValue, Long.MaxValue)
                case None => ContinuousSchema[Codex.DoubleCodex](DoubleCodex, Double.MinValue, Double.MaxValue)
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
    val data = TestSparkReader.load4TupleDataAddDate(args(1) + "/someInputfile3.txt")

    data
      .saveAsText("./tmp.spark/dat1.out", Cell.toString(descriptive = true))
      .toUnit

    data
      .set(Cell(Position3D("iid:1548763", "fid:Y", DateCodex().decode("2014-04-26").get),
        Content(ContinuousSchema(LongCodex), 1234)))
      .slice(Over(First), "iid:1548763", true)
      .saveAsText("./tmp.spark/dat2.out", Cell.toString(descriptive = true))
      .toUnit

    loadText(args(1) + "/smallInputfile.txt", Cell.parse3D(third = DateCodex()))
      .data
      .saveAsText("./tmp.spark/dat3.out", Cell.toString(descriptive = true))
      .toUnit
  }
}

object TestSpark2 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val data = TestSparkReader.load4TupleDataAddDate(args(1) + "/someInputfile3.txt")

    (data.names(Over(First)) ++ data.names(Over(Second)) ++ data.names(Over(Third)))
      .number
      .saveAsText("./tmp.spark/nm0.out", Name.toString(descriptive = true))
      .toUnit

    data
      .names(Over(Second))
      .number
      .moveToFront("fid:Z")
      .saveAsText("./tmp.spark/nm1.out", Name.toString(descriptive = true))
      .toUnit

    data
      .names(Over(Second))
      .number
      .slice("fid:M", false)
      .saveAsText("./tmp.spark/nm2.out", Name.toString(descriptive = true))
      .toUnit

    data
      .names(Over(Second))
      .number
      .set(Map("fid:A" -> 100L, "fid:C" -> 200L))
      .saveAsText("./tmp.spark/nm3.out", Name.toString(descriptive = true))
      .toUnit

    data
      .names(Over(Second))
      .number
      .moveToBack("fid:B")
      .saveAsText("./tmp.spark/nm4.out", Name.toString(descriptive = true))
      .toUnit

    data
      .names(Over(Second))
      .number
      .slice(""".*[BCD]$""".r, true, "")
      .saveAsText("./tmp.spark/nm5.out", Name.toString(descriptive = true))
      .toUnit
  }
}

object TestSpark3 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val data = TestSparkReader.load4TupleDataAddDate(args(1) + "/someInputfile3.txt")

    (data.types(Over(First)) ++ data.types(Over(Second)) ++ data.types(Over(Third)))
      .saveAsText("./tmp.spark/typ1.out", Type.toString(descriptive = true))
      .toUnit

    (data.types(Over(First), true) ++ data.types(Over(Second), true) ++ data.types(Over(Third), true))
      .saveAsText("./tmp.spark/typ2.out", Type.toString(descriptive = true))
      .toUnit
  }
}

object TestSpark4 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val data = TestSparkReader.load4TupleDataAddDate(args(1) + "/someInputfile3.txt")

    data
      .slice(Over(Second), "fid:B", true)
      .saveAsText("./tmp.spark/scl0.out", Cell.toString(descriptive = true))
      .toUnit

    data
      .slice(Over(Second), List("fid:A", "fid:B"), true)
      .slice(Over(First), "iid:0221707", true)
      .saveAsText("./tmp.spark/scl1.out", Cell.toString(descriptive = true))
      .toUnit

    val rem = List("fid:B", "fid:D", "fid:F", "fid:H", "fid:J", "fid:L", "fid:N",
                   "fid:P", "fid:R", "fid:T", "fid:V", "fid:X", "fid:Z")
    data
      .slice(Over(Second), data.names(Over(Second)).slice(rem, false), false)
      .saveAsText("./tmp.spark/scl2.out", Cell.toString(descriptive = true))
      .toUnit
  }
}

object TestSpark5 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val data = TestSparkReader.load4TupleDataAddDate(args(1) + "/someInputfile3.txt")

    data
      .slice(Over(Second), List("fid:A", "fid:B"), true)
      .slice(Over(First), "iid:0221707", true)
      .squash(Third, PreservingMaxPosition[Position3D]())
      .saveAsText("./tmp.spark/sqs1.out", Cell.toString(descriptive = true))
      .toUnit

    data
      .squash(Third, PreservingMaxPosition[Position3D]())
      .saveAsText("./tmp.spark/sqs2.out", Cell.toString(descriptive = true))
      .toUnit

    data
      .slice(Over(First), List("iid:0064402", "iid:0066848", "iid:0076357", "iid:0216406", "iid:0221707", "iid:0262443",
                               "iid:0364354", "iid:0375226", "iid:0444510", "iid:1004305"), true)
      .squash(Third, PreservingMaxPosition[Position3D]())
      .saveAsCSV(Over(First), "./tmp.spark/sqs3.out")
      .toUnit

    data
      .slice(Over(First), List("iid:0064402", "iid:0066848", "iid:0076357", "iid:0216406", "iid:0221707", "iid:0262443",
                               "iid:0364354", "iid:0375226", "iid:0444510", "iid:1004305"), true)
      .slice(Over(Second), List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"), true)
      .squash(Third, PreservingMaxPosition[Position3D]())
      .saveAsCSV(Over(First), "./tmp.spark/sqs4.out")
      .toUnit
  }
}

object TestSpark6 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val data = TestSparkReader.load4TupleDataAddDate(args(1) + "/someInputfile3.txt")

    data
      .which((c: Cell[Position3D]) => c.content.schema.kind.isSpecialisationOf(Numerical))
      .saveAsText("./tmp.spark/whc1.out", Position.toString(descriptive = true))
      .toUnit

    data
      .which((c: Cell[Position3D]) => ! c.content.value.isInstanceOf[StringValue])
      .saveAsText("./tmp.spark/whc2.out", Position.toString(descriptive = true))
      .toUnit

    data
      .get(data.which((c: Cell[Position3D]) =>
        (c.content.value equ 666) || (c.content.value leq 11.0) || (c.content.value equ "KQUPKFEH")))
      .saveAsText("./tmp.spark/whc3.out", Cell.toString(descriptive = true))
      .toUnit

    data
      .which((c: Cell[Position3D]) => c.content.value.isInstanceOf[LongValue])
      .saveAsText("./tmp.spark/whc4.out", Position.toString(descriptive = true))
      .toUnit

    val aggregators: List[Aggregator[Position2D, Position1D, Position2D]] = List(
      Count().andThenExpand(_.position.append("count")),
      Mean().andThenExpand(_.position.append("mean")),
      Min().andThenExpand(_.position.append("min")),
      Max().andThenExpand(_.position.append("max")),
      MaxAbs().andThenExpand(_.position.append("max.abs")))

    TestSparkReader.load4TupleDataAddDate(args(1) + "/someInputfile3.txt")
      .slice(Over(First), List("iid:0064402", "iid:0066848", "iid:0076357", "iid:0216406", "iid:0221707", "iid:0262443",
                               "iid:0364354", "iid:0375226", "iid:0444510", "iid:1004305"), true)
      .slice(Over(Second), List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"), true)
      .squash(Third, PreservingMaxPosition[Position3D]())
      .summarise(Along(First), aggregators)
      .whichPositions(Over(Second), List(("count", (c: Cell[Position2D]) => c.content.value leq 2),
                                         ("min", (c: Cell[Position2D]) => c.content.value equ 107)))
      .saveAsText("./tmp.spark/whc5.out", Position.toString(descriptive = true))
      .toUnit
  }
}

object TestSpark7 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val data = TestSparkReader.load4TupleDataAddDate(args(1) + "/someInputfile3.txt")

    data
      .get(Position3D("iid:1548763", "fid:Y", DateCodex().decode("2014-04-26").get))
      .saveAsText("./tmp.spark/get1.out", Cell.toString(descriptive = true))
      .toUnit

    data
      .get(List(Position3D("iid:1548763", "fid:Y", DateCodex().decode("2014-04-26").get),
                Position3D("iid:1303823", "fid:A", DateCodex().decode("2014-05-05").get)))
      .saveAsText("./tmp.spark/get2.out", Cell.toString(descriptive = true))
      .toUnit
  }
}

object TestSpark8 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val data = TestSparkReader.load4TupleDataAddDate(args(1) + "/someInputfile3.txt")

    data
      .slice(Over(Second), "fid:B", true)
      .squash(Third, PreservingMaxPosition[Position3D]())
      .unique()
      .saveAsText("./tmp.spark/uniq.out", Content.toString(descriptive = true))
      .toUnit

    loadText(args(1) + "/mutualInputfile.txt", Cell.parse2D())
      .data
      .uniquePositions(Over[Position2D, Dimension.Second](Second))
      .map { case (p, c) => Cell(p, c) }
      .saveAsText("./tmp.spark/uni2.out")
      .toUnit

    data
      .slice(Over(Second), List("fid:A", "fid:B", "fid:Y", "fid:Z"), true)
      .slice(Over(First), List("iid:0221707", "iid:0364354"), true)
      .squash(Third, PreservingMaxPosition[Position3D]())
      .saveAsCSV(Over(Second), "./tmp.spark/test.csv")
      .saveAsCSV(Over(First), "./tmp.spark/tset.csv", writeHeader = false, separator = ",")
      .toUnit

    data
      .slice(Over(Second), List("fid:A", "fid:B", "fid:Y", "fid:Z"), true)
      .slice(Over(First), List("iid:0221707", "iid:0364354"), true)
      .squash(Third, PreservingMaxPosition[Position3D]())
      .permute(Second, First)
      .saveAsText("./tmp.spark/trs1.out", Cell.toString(descriptive = true))
      .toUnit

    data
      .slice(Over(Second), List("fid:A", "fid:B", "fid:Y", "fid:Z"), true)
      .slice(Over(First), List("iid:0221707", "iid:0364354"), true)
      .squash(Third, PreservingMaxPosition[Position3D]())
      .saveAsText("./tmp.spark/data.txt")
      .toUnit
  }
}

object TestSpark9 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val data = TestSparkReader.load4TupleDataAddDate(args(1) + "/someInputfile3.txt")

    case class StringPartitioner(dim: Dimension) extends Partitioner[Position2D, String] {
      def assign(cell: Cell[Position2D]): TraversableOnce[String] = {
        List(cell.position(dim) match {
          case StringValue("fid:A") => "training"
          case StringValue("fid:B") => "testing"
        }, "scoring")
      }
    }

    val prt1 = data
      .slice(Over(Second), List("fid:A", "fid:B"), true)
      .slice(Over(First), List("iid:0221707", "iid:0364354"), true)
      .squash(Third, PreservingMaxPosition[Position3D]())
      .split(StringPartitioner(Second))

    prt1
      .saveAsText("./tmp.spark/prt1.out", Partition.toString(descriptive = true))
      .toUnit

    case class IntTuplePartitioner(dim: Dimension) extends Partitioner[Position2D, (Int, Int, Int)] {
      def assign(cell: Cell[Position2D]): TraversableOnce[(Int, Int, Int)] = {
        List(cell.position(dim) match {
          case StringValue("fid:A") => (1, 0, 0)
          case StringValue("fid:B") => (0, 1, 0)
        }, (0, 0, 1))
      }
    }

    data
      .slice(Over(Second), List("fid:A", "fid:B"), true)
      .slice(Over(First), List("iid:0221707", "iid:0364354"), true)
      .squash(Third, PreservingMaxPosition[Position3D]())
      .split[(Int, Int, Int), IntTuplePartitioner](IntTuplePartitioner(Second))
      .saveAsText("./tmp.spark/prt2.out", Partition.toString(descriptive = true))
      .toUnit

    prt1
      .get("training")
      .saveAsText("./tmp.spark/train.out", Cell.toString(descriptive = true))
      .toUnit

    prt1
      .get("testing")
      .saveAsText("./tmp.spark/test.out", Cell.toString(descriptive = true))
      .toUnit

    prt1
      .get("scoring")
      .saveAsText("./tmp.spark/score.out", Cell.toString(descriptive = true))
      .toUnit
  }
}

object TestSpark10 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val data = TestSparkReader.load4TupleDataAddDate(args(1) + "/someInputfile3.txt")

    data
      .summarise(Over(Second), Mean[Position3D, Position1D](true, true).andThenExpand(_.position.append("mean")))
      .saveAsCSV(Over(Second), "./tmp.spark/agg1.csv")
      .toUnit

    data
      .slice(Over(First), List("iid:0064402", "iid:0066848", "iid:0076357", "iid:0216406", "iid:0221707", "iid:0262443",
                               "iid:0364354", "iid:0375226", "iid:0444510", "iid:1004305"), true)
      .squash(Third, PreservingMaxPosition[Position3D]())
      .summarise(Along(Second), Count[Position2D, Position1D]().andThenExpand(_.position.append("count")))
      .saveAsCSV(Over(Second), "./tmp.spark/agg2.csv")
      .toUnit

    val aggregators: List[Aggregator[Position2D, Position1D, Position2D]] = List(
      Count().andThenExpand(_.position.append("count")),
      Mean().andThenExpand(_.position.append("mean")),
      StandardDeviation().andThenExpand(_.position.append("sd")),
      Skewness().andThenExpand(_.position.append("skewness")),
      Kurtosis().andThenExpand(_.position.append("kurtosis")),
      Min().andThenExpand(_.position.append("min")),
      Max().andThenExpand(_.position.append("max")),
      MaxAbs().andThenExpand(_.position.append("max.abs")))

    data
      .slice(Over(First), List("iid:0064402", "iid:0066848", "iid:0076357", "iid:0216406", "iid:0221707", "iid:0262443",
                               "iid:0364354", "iid:0375226", "iid:0444510", "iid:1004305"), true)
      .squash(Third, PreservingMaxPosition[Position3D]())
      .summarise(Along(First), aggregators)
      .saveAsCSV(Over(Second), "./tmp.spark/agg3.csv")
      .toUnit
  }
}

object TestSpark11 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val data = TestSparkReader.load4TupleDataAddDate(args(1) + "/someInputfile3.txt")

    data
      .slice(Over(Second), List("fid:A", "fid:B", "fid:Y", "fid:Z"), true)
      .slice(Over(First), List("iid:0221707", "iid:0364354"), true)
      .transform(Indicator[Position3D]().andThenRename(Transformer.rename(Second, "%1$s.ind")))
      .saveAsText("./tmp.spark/trn2.out", Cell.toString(descriptive = true))
      .toUnit

    data
      .slice(Over(Second), List("fid:A", "fid:B", "fid:Y", "fid:Z"), true)
      .slice(Over(First), List("iid:0221707", "iid:0364354"), true)
      .squash(Third, PreservingMaxPosition[Position3D]())
      .transform(Binarise[Position2D](Binarise.rename(Second)))
      .saveAsCSV(Over(Second), "./tmp.spark/trn3.out")
      .toUnit
  }
}

object TestSpark12 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val data = TestSparkReader.load4TupleDataAddDate(args(1) + "/someInputfile3.txt")
      .slice(Over(Second), List("fid:A", "fid:B", "fid:Y", "fid:Z"), true)
      .slice(Over(First), List("iid:0221707", "iid:0364354"), true)

    data
      .squash(Third, PreservingMaxPosition[Position3D]())
      .fillHomogeneous(Content(ContinuousSchema(LongCodex), 0))
      .saveAsCSV(Over(Second), "./tmp.spark/fll1.out")
      .toUnit

    data
      .fillHomogeneous(Content(ContinuousSchema(LongCodex), 0))
      .saveAsText("./tmp.spark/fll3.out", Cell.toString(descriptive = true))
      .toUnit
  }
}

object TestSpark13 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val all = TestSparkReader.load4TupleDataAddDate(args(1) + "/someInputfile3.txt")
    val data = all
      .slice(Over(First), List("iid:0064402", "iid:0066848", "iid:0076357", "iid:0216406", "iid:0221707", "iid:0262443",
                               "iid:0364354", "iid:0375226", "iid:0444510", "iid:1004305"), true)
      .slice(Over(Second), List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"), true)
      .squash(Third, PreservingMaxPosition[Position3D]())

    val inds = data
      .transform(Indicator[Position2D]().andThenRename(Transformer.rename(Second, "%1$s.ind")))
      .fillHomogeneous(Content(ContinuousSchema(LongCodex), 0))

    data
      .join(Over(First), inds)
      .fillHomogeneous(Content(ContinuousSchema(LongCodex), 0))
      .saveAsCSV(Over(Second), "./tmp.spark/fll2.out")
      .toUnit

    data
      .fillHeterogeneous(Over[Position2D, Dimension.Second](Second),
        all.summarise(Over(Second), Mean[Position3D, Position1D](true, true)))
      .join(Over(First), inds)
      .saveAsCSV(Over(Second), "./tmp.spark/fll4.out")
      .toUnit
  }
}

object TestSpark14 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val data = TestSparkReader.load4TupleDataAddDate(args(1) + "/someInputfile3.txt")
      .slice(Over(Second), List("fid:A", "fid:B", "fid:Y", "fid:Z"), true)
      .slice(Over(First), List("iid:0221707", "iid:0364354"), true)

    data
      .change(Over(Second), "fid:A", NominalSchema(LongCodex))
      .saveAsText("./tmp.spark/chg1.out", Cell.toString(descriptive = true))
      .toUnit
  }
}

object TestSpark15 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val data = TestSparkReader.load4TupleDataAddDate(args(1) + "/someInputfile3.txt")

    data
      .slice(Over(Second), List("fid:A", "fid:C", "fid:E", "fid:G"), true)
      .slice(Over(First), List("iid:0221707", "iid:0364354"), true)
      .summarise(Along(Third), Sum[Position3D, Position2D]().andThenExpand(_.position.append("sum")))
      .melt(Third, Second)
      .saveAsCSV(Over(Second), "./tmp.spark/rsh1.out")
      .toUnit

    val inds = data
      .slice(Over(First), List("iid:0064402", "iid:0066848", "iid:0076357", "iid:0216406", "iid:0221707", "iid:0262443",
                               "iid:0364354", "iid:0375226", "iid:0444510", "iid:1004305"), true)
      .slice(Over(Second), List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"), true)
      .squash(Third, PreservingMaxPosition[Position3D]())
      .transform(Indicator[Position2D]().andThenRename(Transformer.rename(Second, "%1$s.ind")))
      .saveAsCSV(Over(Second), "./tmp.spark/trn1.csv")

    data
      .slice(Over(First), List("iid:0064402", "iid:0066848", "iid:0076357", "iid:0216406", "iid:0221707", "iid:0262443",
                               "iid:0364354", "iid:0375226", "iid:0444510", "iid:1004305"), true)
      .slice(Over(Second), List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"), true)
      .squash(Third, PreservingMaxPosition[Position3D]())
      .join(Over(First), inds)
      .saveAsCSV(Over(Second), "./tmp.spark/jn1.csv")
      .toUnit
  }
}

object TestSpark16 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val data = TestSparkReader.load4TupleDataAddDate(args(1) + "/someInputfile3.txt")

    case class HashSample() extends Sampler[Position3D] {
      def select(cell: Cell[Position3D]): Boolean = (cell.position(First).toString.hashCode % 25) == 0
    }

    data
      .sample(HashSample())
      .saveAsText("./tmp.spark/smp1.out")
      .toUnit
  }
}

object TestSpark17 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val data = TestSparkReader.load4TupleDataAddDate(args(1) + "/someInputfile3.txt")
      .slice(Over(First), List("iid:0064402", "iid:0066848", "iid:0076357", "iid:0216406", "iid:0221707", "iid:0262443",
                               "iid:0364354", "iid:0375226", "iid:0444510", "iid:1004305"), true)
      .slice(Over(Second), List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"), true)
      .squash(Third, PreservingMaxPosition[Position3D]())

    val aggregators: List[Aggregator[Position2D, Position1D, Position2D]] = List(
      Count().andThenExpand(_.position.append("count")),
      Mean().andThenExpand(_.position.append("mean")),
      Min().andThenExpand(_.position.append("min")),
      Max().andThenExpand(_.position.append("max")),
      MaxAbs().andThenExpand(_.position.append("max.abs")))

    val stats = data
      .summarise(Along(First), aggregators)
      .toMap(Over(First))

    data
      .transformWithValue(Normalise(ExtractWithDimensionAndKey[Dimension.Second, Position2D, String, Content](Second,
        "max.abs").andThenPresent(_.value.asDouble)), stats)
      .saveAsCSV(Over(Second), "./tmp.spark/trn6.csv")
      .toUnit

    case class Sample500() extends Sampler[Position2D] {
      def select(cell: Cell[Position2D]): Boolean = cell.content.value gtr 500
    }

    data
      .sample(Sample500())
      .saveAsCSV(Over(Second), "./tmp.spark/flt1.csv")
      .toUnit

    case class RemoveGreaterThanMean(dim: Dimension) extends SamplerWithValue[Position2D] {
      type V = Map[Position1D, Map[Position1D, Content]]

      def selectWithValue(cell: Cell[Position2D], ext: V): Boolean = {
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
      .toUnit
  }
}

object TestSpark18 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val data = TestSparkReader.load4TupleDataAddDate(args(1) + "/someInputfile3.txt")
      .slice(Over(First), List("iid:0064402", "iid:0066848", "iid:0076357", "iid:0216406", "iid:0221707", "iid:0262443",
                               "iid:0364354", "iid:0375226", "iid:0444510", "iid:1004305"), true)
      .slice(Over(Second), List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"), true)
      .squash(Third, PreservingMaxPosition[Position3D]())

    val aggregators: List[Aggregator[Position2D, Position1D, Position2D]] = List(
      Count().andThenExpand(_.position.append("count")),
      Mean().andThenExpand(_.position.append("mean")),
      Min().andThenExpand(_.position.append("min")),
      Max().andThenExpand(_.position.append("max")),
      MaxAbs().andThenExpand(_.position.append("max.abs")))

    val stats = data
      .summarise(Along(First), aggregators)

    val rem = stats
      .whichPositions(Over(Second), ("count", (c: Cell[Position2D]) => c.content.value leq 2))
      .names(Over(First))

    data
      .slice(Over(Second), rem, false)
      .saveAsCSV(Over(Second), "./tmp.spark/flt3.csv")
      .toUnit
  }
}

object TestSpark19 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val raw = TestSparkReader.load4TupleDataAddDate(args(1) + "/someInputfile3.txt")
      .slice(Over(First), List("iid:0064402", "iid:0066848", "iid:0076357", "iid:0216406", "iid:0221707", "iid:0262443",
                               "iid:0364354", "iid:0375226", "iid:0444510", "iid:1004305"), true)
      .slice(Over(Second), List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"), true)
      .squash(Third, PreservingMaxPosition[Position3D]())

    case class CustomPartition(dim: Dimension, left: String, right: String) extends Partitioner[Position2D, String] {
      val bhs = BinaryHashSplit[Position2D, String](dim, 7, left, right, base = 10)

      def assign(cell: Cell[Position2D]): TraversableOnce[String] = {
        if (cell.position(dim).toShortString == "iid:0364354") {
          Some(right)
        } else {
          bhs.assign(cell)
        }
      }
    }

    val parts = raw
      .split(CustomPartition(First, "train", "test"))

    val aggregators: List[Aggregator[Position2D, Position1D, Position2D]] = List(
      Count().andThenExpand(_.position.append("count")),
      MaxAbs().andThenExpand(_.position.append("max.abs")))

    val stats = parts
      .get("train")
      .summarise(Along(First), aggregators)

    val rem = stats
      .which((c: Cell[Position2D]) => (c.position(Second) equ "count") && (c.content.value leq 2))
      .names(Over(First))

    type W = Map[Position1D, Map[Position1D, Content]]

    val transforms: List[TransformerWithValue[Position2D, Position2D] { type V >: W }] = List(
      Indicator().andThenRename(Transformer.rename(Second, "%1$s.ind")),
      Binarise(Binarise.rename(Second)),
      Normalise(ExtractWithDimensionAndKey[Dimension.Second, Position2D, String, Content](Second, "max.abs")
        .andThenPresent(_.value.asDouble)))

    def cb(key: String, pipe: RDD[Cell[Position2D]]): RDD[Cell[Position2D]] = {
      pipe
        .slice(Over(Second), rem, false)
        .transformWithValue(transforms, stats.toMap(Over[Position2D, Dimension.First](First)))
        .fillHomogeneous(Content(ContinuousSchema(LongCodex), 0))
        .saveAsCSV(Over(Second), "./tmp.spark/pln_" + key + ".csv")
    }

    parts
      .forEach(cb, List("train", "test"))
      .toUnit
  }
}

object TestSpark20 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())

    val (dictionary, _) = Dictionary.load(args(1) + "/dict.txt")

    loadText(args(1) + "/ivoryInputfile1.txt", Cell.parse3DWithDictionary(dictionary, Second, third = DateCodex()))
      .data
      .saveAsText("./tmp.spark/ivr1.out")
      .toUnit
  }
}

object TestSpark21 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val data = TestSparkReader.load4TupleDataAddDate(args(1) + "/someInputfile3.txt")

    data
      .shape()
      .saveAsText("./tmp.spark/siz0.out")
      .toUnit

    data
      .size(First)
      .saveAsText("./tmp.spark/siz1.out")
      .toUnit

    data
      .size(Second)
      .saveAsText("./tmp.spark/siz2.out")
      .toUnit

    data
      .size(Third)
      .saveAsText("./tmp.spark/siz3.out")
      .toUnit
  }
}

object TestSpark22 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val (data, _) = loadText(args(1) + "/numericInputfile.txt", Cell.parse2D())

    case class Diff() extends Window[Position1D, Position1D, Position2D] {
      type T = Cell[Position]

      def initialise(cell: Cell[Position1D], rem: Position1D): (T, TraversableOnce[Cell[Position2D]]) = {
        (Cell(rem, cell.content), None)
      }

      def present(cell: Cell[Position1D], rem: Position1D, t: T): (T, TraversableOnce[Cell[Position2D]]) = {
        (Cell(rem, cell.content), (cell.content.value.asDouble, t.content.value.asDouble) match {
          case (Some(c), Some(l)) =>
            Some(Cell(cell.position.append(rem.toShortString("") + "-" + t.position.toShortString("")),
              Content(ContinuousSchema(DoubleCodex), c - l)))
          case _ => None
        })
      }
    }

    data
      .slide(Over(First), Diff())
      .saveAsText("./tmp.spark/dif1.out")
      .toUnit

    data
      .slide(Over(Second), Diff())
      .permute(Second, First)
      .saveAsText("./tmp.spark/dif2.out")
      .toUnit
  }
}

object TestSpark23 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val (data, _) = loadText(args(1) + "/somePairwise.txt", Cell.parse2D())

    case class DiffSquared() extends Operator[Position1D, Position1D, Position2D] {
      def compute(left: Cell[Position1D], reml: Position1D, right: Cell[Position1D],
        remr: Position1D): TraversableOnce[Cell[Position2D]] = {
        val xc = left.position.toShortString("")
        val yc = right.position.toShortString("")

        (reml == remr) match {
          case true => Some(Cell(remr.append("(" + xc + "-" + yc + ")^2"),
            Content(ContinuousSchema(DoubleCodex),
              math.pow(left.content.value.asLong.get - right.content.value.asLong.get, 2))))
          case false => None
        }
      }
    }

    data
      .pairwise(Over(Second), Upper, DiffSquared())
      .saveAsText("./tmp.spark/pws1.out")
      .toUnit
  }
}

object TestSpark24 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())

    // see http://www.mathsisfun.com/data/correlation.html for data
    val schema = List(("day", NominalSchema(StringCodex)),
                      ("temperature", ContinuousSchema(DoubleCodex)),
                      ("sales", DiscreteSchema(LongCodex)))
    val (data, _) = loadText(args(1) + "/somePairwise2.txt", Cell.parseTable(schema, separator = "|"))

    data
      .correlation(Over(Second))
      .saveAsText("./tmp.spark/pws2.out")
      .toUnit

    val schema2 = List(("day", NominalSchema(StringCodex)),
                       ("temperature", ContinuousSchema(DoubleCodex)),
                       ("sales", DiscreteSchema(LongCodex)),
                       ("neg.sales", DiscreteSchema(LongCodex)))
    val (data2, _) = loadText(args(1) + "/somePairwise3.txt", Cell.parseTable(schema2, separator = "|"))

    data2
      .correlation(Over(Second))
      .saveAsText("./tmp.spark/pws3.out")
      .toUnit
  }
}

object TestSpark25 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())

    loadText(args(1) + "/mutualInputfile.txt", Cell.parse2D())
      .data
      .mutualInformation(Over(Second))
      .saveAsText("./tmp.spark/mi.out")
      .toUnit
  }
}

object TestSpark26 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val (left, _) = loadText(args(1) + "/algebraInputfile1.txt", Cell.parse2D())
    val (right, _) = loadText(args(1) + "/algebraInputfile2.txt", Cell.parse2D())

    left
      .pairwiseBetween(Over(First), All, right, Times(Locate.OperatorString[Position1D, Position1D]("(%1$s*%2$s)")))
      .saveAsText("./tmp.spark/alg.out")
      .toUnit
  }
}

object TestSpark27 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())

    // http://www.statisticshowto.com/moving-average/
    loadText(args(1) + "/simMovAvgInputfile.txt", Cell.parse2D(first = LongCodex))
      .data
      .slide(Over(Second), SimpleMovingAverage(5, Locate.WindowDimension[Position1D, Position1D](First)))
      .saveAsText("./tmp.spark/sma1.out")
      .toUnit

    loadText(args(1) + "/simMovAvgInputfile.txt", Cell.parse2D(first = LongCodex))
      .data
      .slide(Over(Second), SimpleMovingAverage(5, Locate.WindowDimension[Position1D, Position1D](First), all = true))
      .saveAsText("./tmp.spark/sma2.out")
      .toUnit

    loadText(args(1) + "/simMovAvgInputfile.txt", Cell.parse2D(first = LongCodex))
      .data
      .slide(Over(Second), CenteredMovingAverage(2, Locate.WindowDimension[Position1D, Position1D](First)))
      .saveAsText("./tmp.spark/tma.out")
      .toUnit

    loadText(args(1) + "/simMovAvgInputfile.txt", Cell.parse2D(first = LongCodex))
      .data
      .slide(Over(Second), WeightedMovingAverage(5, Locate.WindowDimension[Position1D, Position1D](First)))
      .saveAsText("./tmp.spark/wma1.out")
      .toUnit

    loadText(args(1) + "/simMovAvgInputfile.txt", Cell.parse2D(first = LongCodex))
      .data
      .slide(Over(Second), WeightedMovingAverage(5, Locate.WindowDimension[Position1D, Position1D](First), all = true))
      .saveAsText("./tmp.spark/wma2.out")
      .toUnit

    // http://stackoverflow.com/questions/11074665/how-to-calculate-the-cumulative-average-for-some-numbers
    loadText(args(1) + "/cumMovAvgInputfile.txt", Cell.parse1D())
      .data
      .slide(Along(First), CumulativeMovingAverage(Locate.WindowDimension[Position0D, Position1D](First)))
      .saveAsText("./tmp.spark/cma.out")
      .toUnit

    // http://www.incrediblecharts.com/indicators/exponential_moving_average.php
    loadText(args(1) + "/expMovAvgInputfile.txt", Cell.parse1D())
      .data
      .slide(Along(First), ExponentialMovingAverage(0.33, Locate.WindowDimension[Position0D, Position1D](First)))
      .saveAsText("./tmp.spark/ema.out")
      .toUnit
  }
}

object TestSpark28 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val data = List
      .range(0, 16)
      .flatMap { case i => List(("iid:" + i, "fid:A", Content(ContinuousSchema(LongCodex), i)),
                                ("iid:" + i, "fid:B", Content(NominalSchema(StringCodex), i.toString))) }

    val aggregators: List[Aggregator[Position2D, Position1D, Position2D]] = List(
      Count().andThenExpand(_.position.append("count")),
      Min().andThenExpand(_.position.append("min")),
      Max().andThenExpand(_.position.append("max")),
      Mean().andThenExpand(_.position.append("mean")),
      StandardDeviation().andThenExpand(_.position.append("sd")),
      Skewness().andThenExpand(_.position.append("skewness")))

    val stats = data
      .summarise(Along(First), aggregators)
      .toMap(Over(First))

    val extractor = ExtractWithDimension[Dimension.Second, Position2D, List[Double]](Second)

    data
      .transformWithValue(Cut(extractor), CutRules.fixed(stats, "min", "max", 4))
      .saveAsText("./tmp.spark/cut1.out")
      .toUnit

    data
      .transformWithValue(Cut(extractor).andThenRenameWithValue(TransformerWithValue.rename(Second, "%s.square")),
        CutRules.squareRootChoice(stats, "count", "min", "max"))
      .saveAsText("./tmp.spark/cut2.out")
      .toUnit

    data
      .transformWithValue(Cut(extractor).andThenRenameWithValue(TransformerWithValue.rename(Second, "%s.sturges")),
        CutRules.sturgesFormula(stats, "count", "min", "max"))
      .saveAsText("./tmp.spark/cut3.out")
      .toUnit

    data
      .transformWithValue(Cut(extractor).andThenRenameWithValue(TransformerWithValue.rename(Second, "%s.rice")),
        CutRules.riceRule(stats, "count", "min", "max"))
      .saveAsText("./tmp.spark/cut4.out")
      .toUnit

    data
      .transformWithValue(Cut(extractor).andThenRenameWithValue(TransformerWithValue.rename(Second, "%s.doane")),
        CutRules.doanesFormula(stats, "count", "min", "max", "skewness"))
      .saveAsText("./tmp.spark/cut5.out")
      .toUnit

    data
      .transformWithValue(Cut(extractor).andThenRenameWithValue(TransformerWithValue.rename(Second, "%s.scott")),
        CutRules.scottsNormalReferenceRule(stats, "count", "min", "max", "sd"))
      .saveAsText("./tmp.spark/cut6.out")
      .toUnit

    data
      .transformWithValue(Cut(extractor).andThenRenameWithValue(TransformerWithValue.rename(Second, "%s.break")),
        CutRules.breaks(Map("fid:A" -> List(-1, 4, 8, 12, 16))))
      .saveAsText("./tmp.spark/cut7.out")
      .toUnit
  }
}

object TestSpark29 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val schema = DiscreteSchema(LongCodex)
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
      .saveAsText("./tmp.spark/gini.out")
      .toUnit

    data
      .map { case (a, b, c) => (b, a, c) }
      .gini(Along(First))
      .saveAsText("./tmp.spark/inig.out")
      .toUnit
  }
}

object TestSpark30 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val schema = DiscreteSchema(LongCodex)
    val data = List(("iid:A", Content(schema, 0)),
      ("iid:B", Content(schema, 1)),
      ("iid:C", Content(schema, 2)),
      ("iid:D", Content(schema, 3)),
      ("iid:E", Content(schema, 4)),
      ("iid:F", Content(schema, 5)),
      ("iid:G", Content(schema, 6)),
      ("iid:H", Content(schema, 7)))

    data
      .stream("Rscript", "double.R", "|", Cell.parse2D("#", StringCodex, LongCodex))
      .data
      .saveAsText("./tmp.spark/strm.out")
      .toUnit
  }
}

object TestSpark31 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())

    val (data, errors) = loadText(args(1) + "/badInputfile.txt", Cell.parse3D(third = DateCodex()))

    data
      .saveAsText("./tmp.spark/yok.out", Cell.toString(descriptive = true))
      .toUnit
    errors.saveAsTextFile("./tmp.spark/nok.out")
  }
}

