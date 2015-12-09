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
import au.com.cba.omnia.grimlock.spark.partition.Partitions._
import au.com.cba.omnia.grimlock.spark.position.Positions._
import au.com.cba.omnia.grimlock.spark.position.PositionDistributable._
import au.com.cba.omnia.grimlock.spark.Predicateable._
import au.com.cba.omnia.grimlock.spark.transform._
import au.com.cba.omnia.grimlock.spark.Types._

import au.com.cba.omnia.grimlock.test.TestSparkReader._

import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.rdd.RDD

import scala.io.Source

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
    val tool = "spark"
    val path = args(1)

    val data = load4TupleDataAddDate(path + "/someInputfile3.txt")

    data
      .saveAsText(s"./tmp.${tool}/dat1.out", Cell.toString(descriptive = true))
      .toUnit

    data
      .set(Cell(Position3D("iid:1548763", "fid:Y", DateCodex().decode("2014-04-26").get),
        Content(ContinuousSchema(LongCodex), 1234)))
      .slice(Over(First), "iid:1548763", true)
      .saveAsText(s"./tmp.${tool}/dat2.out", Cell.toString(descriptive = true))
      .toUnit

    loadText(path + "/smallInputfile.txt", Cell.parse3D(third = DateCodex()))
      .data
      .saveAsText(s"./tmp.${tool}/dat3.out", Cell.toString(descriptive = true))
      .toUnit
  }
}

object TestSpark2 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val tool = "spark"
    val path = args(1)

    val data = load4TupleDataAddDate(path + "/someInputfile3.txt")

    (data.names(Over(First)) ++ data.names(Over(Second)) ++ data.names(Over(Third)))
      .saveAsText(s"./tmp.${tool}/nm0.out", Position.toString(descriptive = true))
      .toUnit

    data
      .names(Over(Second))
      .slice("fid:M", false)
      .saveAsText(s"./tmp.${tool}/nm2.out", Position.toString(descriptive = true))
      .toUnit

    data
      .names(Over(Second))
      .slice(""".*[BCD]$""".r, true, "")
      .saveAsText(s"./tmp.${tool}/nm5.out", Position.toString(descriptive = true))
      .toUnit
  }
}

object TestSpark3 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val tool = "spark"
    val path = args(1)

    val data = load4TupleDataAddDate(path + "/someInputfile3.txt")

    (data.types(Over(First)) ++ data.types(Over(Second)) ++ data.types(Over(Third)))
      .saveAsText(s"./tmp.${tool}/typ1.out", Type.toString(descriptive = true))
      .toUnit

    (data.types(Over(First), true) ++ data.types(Over(Second), true) ++ data.types(Over(Third), true))
      .saveAsText(s"./tmp.${tool}/typ2.out", Type.toString(descriptive = true))
      .toUnit
  }
}

object TestSpark4 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val tool = "spark"
    val path = args(1)

    val data = load4TupleDataAddDate(path + "/someInputfile3.txt")

    data
      .slice(Over(Second), "fid:B", true)
      .saveAsText(s"./tmp.${tool}/scl0.out", Cell.toString(descriptive = true))
      .toUnit

    data
      .slice(Over(Second), List("fid:A", "fid:B"), true)
      .slice(Over(First), "iid:0221707", true)
      .saveAsText(s"./tmp.${tool}/scl1.out", Cell.toString(descriptive = true))
      .toUnit

    val rem = List("fid:B", "fid:D", "fid:F", "fid:H", "fid:J", "fid:L", "fid:N",
                   "fid:P", "fid:R", "fid:T", "fid:V", "fid:X", "fid:Z")
    data
      .slice(Over(Second), data.names(Over(Second)).slice(rem, false), false)
      .saveAsText(s"./tmp.${tool}/scl2.out", Cell.toString(descriptive = true))
      .toUnit
  }
}

object TestSpark5 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val tool = "spark"
    val path = args(1)

    val data = load4TupleDataAddDate(path + "/someInputfile3.txt")

    data
      .slice(Over(Second), List("fid:A", "fid:B"), true)
      .slice(Over(First), "iid:0221707", true)
      .squash(Third, PreservingMaxPosition[Position3D]())
      .saveAsText(s"./tmp.${tool}/sqs1.out", Cell.toString(descriptive = true))
      .toUnit

    data
      .squash(Third, PreservingMaxPosition[Position3D]())
      .saveAsText(s"./tmp.${tool}/sqs2.out", Cell.toString(descriptive = true))
      .toUnit

    data
      .slice(Over(First), List("iid:0064402", "iid:0066848", "iid:0076357", "iid:0216406", "iid:0221707", "iid:0262443",
                               "iid:0364354", "iid:0375226", "iid:0444510", "iid:1004305"), true)
      .squash(Third, PreservingMaxPosition[Position3D]())
      .saveAsCSV(Over(Second), s"./tmp.${tool}/sqs3.out")
      .toUnit

    data
      .slice(Over(First), List("iid:0064402", "iid:0066848", "iid:0076357", "iid:0216406", "iid:0221707", "iid:0262443",
                               "iid:0364354", "iid:0375226", "iid:0444510", "iid:1004305"), true)
      .slice(Over(Second), List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"), true)
      .squash(Third, PreservingMaxPosition[Position3D]())
      .saveAsCSV(Over(Second), s"./tmp.${tool}/sqs4.out")
      .toUnit
  }
}

object TestSpark6 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val tool = "spark"
    val path = args(1)

    val data = load4TupleDataAddDate(path + "/someInputfile3.txt")

    data
      .which((c: Cell[Position3D]) => c.content.schema.kind.isSpecialisationOf(Numerical))
      .saveAsText(s"./tmp.${tool}/whc1.out", Position.toString(descriptive = true))
      .toUnit

    data
      .which((c: Cell[Position3D]) => ! c.content.value.isInstanceOf[StringValue])
      .saveAsText(s"./tmp.${tool}/whc2.out", Position.toString(descriptive = true))
      .toUnit

    data
      .get(data.which((c: Cell[Position3D]) =>
        (c.content.value equ 666) || (c.content.value leq 11.0) || (c.content.value equ "KQUPKFEH")))
      .saveAsText(s"./tmp.${tool}/whc3.out", Cell.toString(descriptive = true))
      .toUnit

    data
      .which((c: Cell[Position3D]) => c.content.value.isInstanceOf[LongValue])
      .saveAsText(s"./tmp.${tool}/whc4.out", Position.toString(descriptive = true))
      .toUnit

    val aggregators: List[Aggregator[Position2D, Position1D, Position2D]] = List(
      Count().andThenExpand(_.position.append("count")),
      Mean().andThenExpand(_.position.append("mean")),
      Min().andThenExpand(_.position.append("min")),
      Max().andThenExpand(_.position.append("max")),
      MaxAbs().andThenExpand(_.position.append("max.abs")))

    load4TupleDataAddDate(path + "/someInputfile3.txt")
      .slice(Over(First), List("iid:0064402", "iid:0066848", "iid:0076357", "iid:0216406", "iid:0221707", "iid:0262443",
                               "iid:0364354", "iid:0375226", "iid:0444510", "iid:1004305"), true)
      .slice(Over(Second), List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"), true)
      .squash(Third, PreservingMaxPosition[Position3D]())
      .summarise(Along(First), aggregators)
      .whichByPositions(Over(Second), List(("count", (c: Cell[Position2D]) => c.content.value leq 2),
                                           ("min", (c: Cell[Position2D]) => c.content.value equ 107)))
      .saveAsText(s"./tmp.${tool}/whc5.out", Position.toString(descriptive = true))
      .toUnit
  }
}

object TestSpark7 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val tool = "spark"
    val path = args(1)

    val data = load4TupleDataAddDate(path + "/someInputfile3.txt")

    data
      .get(Position3D("iid:1548763", "fid:Y", DateCodex().decode("2014-04-26").get))
      .saveAsText(s"./tmp.${tool}/get1.out", Cell.toString(descriptive = true))
      .toUnit

    data
      .get(List(Position3D("iid:1548763", "fid:Y", DateCodex().decode("2014-04-26").get),
                Position3D("iid:1303823", "fid:A", DateCodex().decode("2014-05-05").get)))
      .saveAsText(s"./tmp.${tool}/get2.out", Cell.toString(descriptive = true))
      .toUnit
  }
}

object TestSpark8 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val tool = "spark"
    val path = args(1)

    val data = load4TupleDataAddDate(path + "/someInputfile3.txt")

    data
      .slice(Over(Second), "fid:B", true)
      .squash(Third, PreservingMaxPosition[Position3D]())
      .unique()
      .saveAsText(s"./tmp.${tool}/uniq.out", Content.toString(descriptive = true))
      .toUnit

    loadText(path + "/mutualInputfile.txt", Cell.parse2D())
      .data
      .uniqueByPositions(Over(Second))
      .map { case (p, c) => Cell(p, c) }
      .saveAsText(s"./tmp.${tool}/uni2.out")
      .toUnit

    data
      .slice(Over(Second), List("fid:A", "fid:B", "fid:Y", "fid:Z"), true)
      .slice(Over(First), List("iid:0221707", "iid:0364354"), true)
      .squash(Third, PreservingMaxPosition[Position3D]())
      .saveAsCSV(Over(First), s"./tmp.${tool}/test.csv")
      .saveAsCSV(Over(Second), s"./tmp.${tool}/tset.csv", writeHeader = false, separator = ",")
      .toUnit

    data
      .slice(Over(Second), List("fid:A", "fid:B", "fid:Y", "fid:Z"), true)
      .slice(Over(First), List("iid:0221707", "iid:0364354"), true)
      .squash(Third, PreservingMaxPosition[Position3D]())
      .permute(Second, First)
      .saveAsText(s"./tmp.${tool}/trs1.out", Cell.toString(descriptive = true))
      .toUnit

    data
      .slice(Over(Second), List("fid:A", "fid:B", "fid:Y", "fid:Z"), true)
      .slice(Over(First), List("iid:0221707", "iid:0364354"), true)
      .squash(Third, PreservingMaxPosition[Position3D]())
      .saveAsText(s"./tmp.${tool}/data.txt")
      .toUnit
  }
}

object TestSpark9 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val tool = "spark"
    val path = args(1)

    val data = load4TupleDataAddDate(path + "/someInputfile3.txt")

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
      .saveAsText(s"./tmp.${tool}/prt1.out", Partition.toString(descriptive = true))
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
      .split(IntTuplePartitioner(Second))
      .saveAsText(s"./tmp.${tool}/prt2.out", Partition.toString(descriptive = true))
      .toUnit

    prt1
      .get("training")
      .saveAsText(s"./tmp.${tool}/train.out", Cell.toString(descriptive = true))
      .toUnit

    prt1
      .get("testing")
      .saveAsText(s"./tmp.${tool}/test.out", Cell.toString(descriptive = true))
      .toUnit

    prt1
      .get("scoring")
      .saveAsText(s"./tmp.${tool}/score.out", Cell.toString(descriptive = true))
      .toUnit
  }
}

object TestSpark10 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val tool = "spark"
    val path = args(1)

    val data = load4TupleDataAddDate(path + "/someInputfile3.txt")

    data
      .summarise(Over(Second), Mean[Position3D, Position1D](true, true).andThenExpand(_.position.append("mean")))
      .saveAsCSV(Over(First), s"./tmp.${tool}/agg1.csv")
      .toUnit

    data
      .slice(Over(First), List("iid:0064402", "iid:0066848", "iid:0076357", "iid:0216406", "iid:0221707", "iid:0262443",
                               "iid:0364354", "iid:0375226", "iid:0444510", "iid:1004305"), true)
      .squash(Third, PreservingMaxPosition[Position3D]())
      .summarise(Along(Second), Count[Position2D, Position1D]().andThenExpand(_.position.append("count")))
      .saveAsCSV(Over(First), s"./tmp.${tool}/agg2.csv")
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
      .saveAsCSV(Over(First), s"./tmp.${tool}/agg3.csv")
      .toUnit
  }
}

object TestSpark11 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val tool = "spark"
    val path = args(1)

    val data = load4TupleDataAddDate(path + "/someInputfile3.txt")

    data
      .slice(Over(Second), List("fid:A", "fid:B", "fid:Y", "fid:Z"), true)
      .slice(Over(First), List("iid:0221707", "iid:0364354"), true)
      .transform(Indicator[Position3D]().andThenRename(Transformer.rename(Second, "%1$s.ind")))
      .saveAsText(s"./tmp.${tool}/trn2.out", Cell.toString(descriptive = true))
      .toUnit

    data
      .slice(Over(Second), List("fid:A", "fid:B", "fid:Y", "fid:Z"), true)
      .slice(Over(First), List("iid:0221707", "iid:0364354"), true)
      .squash(Third, PreservingMaxPosition[Position3D]())
      .transform(Binarise[Position2D](Binarise.rename(Second)))
      .saveAsCSV(Over(First), s"./tmp.${tool}/trn3.out")
      .toUnit
  }
}

object TestSpark12 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val tool = "spark"
    val path = args(1)

    val data = load4TupleDataAddDate(path + "/someInputfile3.txt")
      .slice(Over(Second), List("fid:A", "fid:B", "fid:Y", "fid:Z"), true)
      .slice(Over(First), List("iid:0221707", "iid:0364354"), true)

    data
      .squash(Third, PreservingMaxPosition[Position3D]())
      .fillHomogeneous(Content(ContinuousSchema(LongCodex), 0))
      .saveAsCSV(Over(First), s"./tmp.${tool}/fll1.out")
      .toUnit

    data
      .fillHomogeneous(Content(ContinuousSchema(LongCodex), 0))
      .saveAsText(s"./tmp.${tool}/fll3.out", Cell.toString(descriptive = true))
      .toUnit
  }
}

object TestSpark13 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val tool = "spark"
    val path = args(1)

    val all = load4TupleDataAddDate(path + "/someInputfile3.txt")
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
      .saveAsCSV(Over(First), s"./tmp.${tool}/fll2.out")
      .toUnit

    data
      .fillHeterogeneous(Over(Second), all.summarise(Over(Second), Mean[Position3D, Position1D](true, true)))
      .join(Over(First), inds)
      .saveAsCSV(Over(First), s"./tmp.${tool}/fll4.out")
      .toUnit
  }
}

object TestSpark14 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val tool = "spark"
    val path = args(1)

    val data = load4TupleDataAddDate(path + "/someInputfile3.txt")
      .slice(Over(Second), List("fid:A", "fid:B", "fid:Y", "fid:Z"), true)
      .slice(Over(First), List("iid:0221707", "iid:0364354"), true)

    data
      .change(Over(Second), "fid:A", NominalSchema(LongCodex))
      .saveAsText(s"./tmp.${tool}/chg1.out", Cell.toString(descriptive = true))
      .toUnit
  }
}

object TestSpark15 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val tool = "spark"
    val path = args(1)

    val data = load4TupleDataAddDate(path + "/someInputfile3.txt")

    data
      .slice(Over(Second), List("fid:A", "fid:C", "fid:E", "fid:G"), true)
      .slice(Over(First), List("iid:0221707", "iid:0364354"), true)
      .summarise(Along(Third), Sum[Position3D, Position2D]().andThenExpand(_.position.append("sum")))
      .melt(Third, Second)
      .saveAsCSV(Over(First), s"./tmp.${tool}/rsh1.out")
      .toUnit

    val inds = data
      .slice(Over(First), List("iid:0064402", "iid:0066848", "iid:0076357", "iid:0216406", "iid:0221707", "iid:0262443",
                               "iid:0364354", "iid:0375226", "iid:0444510", "iid:1004305"), true)
      .slice(Over(Second), List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"), true)
      .squash(Third, PreservingMaxPosition[Position3D]())
      .transform(Indicator[Position2D]().andThenRename(Transformer.rename(Second, "%1$s.ind")))
      .saveAsCSV(Over(First), s"./tmp.${tool}/trn1.csv")

    data
      .slice(Over(First), List("iid:0064402", "iid:0066848", "iid:0076357", "iid:0216406", "iid:0221707", "iid:0262443",
                               "iid:0364354", "iid:0375226", "iid:0444510", "iid:1004305"), true)
      .slice(Over(Second), List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"), true)
      .squash(Third, PreservingMaxPosition[Position3D]())
      .join(Over(First), inds)
      .saveAsCSV(Over(First), s"./tmp.${tool}/jn1.csv")
      .toUnit
  }
}

object TestSpark16 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val tool = "spark"
    val path = args(1)

    val data = load4TupleDataAddDate(path + "/someInputfile3.txt")

    case class HashSample() extends Sampler[Position3D] {
      def select(cell: Cell[Position3D]): Boolean = (cell.position(First).toString.hashCode % 25) == 0
    }

    data
      .subset(HashSample())
      .saveAsText(s"./tmp.${tool}/smp1.out")
      .toUnit
  }
}

object TestSpark17 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val tool = "spark"
    val path = args(1)

    val data = load4TupleDataAddDate(path + "/someInputfile3.txt")
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
      .compact(Over(First))

    data
      .transformWithValue(Normalise(ExtractWithDimensionAndKey[Dimension.Second, Position2D, String, Content](Second,
        "max.abs").andThenPresent(_.value.asDouble)), stats)
      .saveAsCSV(Over(First), s"./tmp.${tool}/trn6.csv")
      .toUnit

    case class Sample500() extends Sampler[Position2D] {
      def select(cell: Cell[Position2D]): Boolean = cell.content.value gtr 500
    }

    data
      .subset(Sample500())
      .saveAsCSV(Over(First), s"./tmp.${tool}/flt1.csv")
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
      .subsetWithValue(RemoveGreaterThanMean(Second), stats)
      .saveAsCSV(Over(First), s"./tmp.${tool}/flt2.csv")
      .toUnit
  }
}

object TestSpark18 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val tool = "spark"
    val path = args(1)

    val data = load4TupleDataAddDate(path + "/someInputfile3.txt")
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
      .whichByPositions(Over(Second), ("count", (c: Cell[Position2D]) => c.content.value leq 2))
      .names(Over(First))

    data
      .slice(Over(Second), rem, false)
      .saveAsCSV(Over(First), s"./tmp.${tool}/flt3.csv")
      .toUnit
  }
}

object TestSpark19 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val tool = "spark"
    val path = args(1)

    val raw = load4TupleDataAddDate(path + "/someInputfile3.txt")
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
        .transformWithValue(transforms, stats.compact(Over(First)))
        .fillHomogeneous(Content(ContinuousSchema(LongCodex), 0))
        .saveAsCSV(Over(First), s"./tmp.${tool}/pln_" + key + ".csv")
    }

    parts
      .forEach(List("train", "test"), cb)
      .toUnit
  }
}

object TestSpark20 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val tool = "spark"
    val path = args(1)

    val (dictionary, _) = Dictionary.load(Source.fromFile(path + "/dict.txt"))

    loadText(path + "/ivoryInputfile1.txt", Cell.parse3DWithDictionary(dictionary, Second, third = DateCodex()))
      .data
      .saveAsText(s"./tmp.${tool}/ivr1.out")
      .toUnit
  }
}

object TestSpark21 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val tool = "spark"
    val path = args(1)

    val data = load4TupleDataAddDate(path + "/someInputfile3.txt")

    data
      .shape()
      .saveAsText(s"./tmp.${tool}/siz0.out")
      .toUnit

    data
      .size(First)
      .saveAsText(s"./tmp.${tool}/siz1.out")
      .toUnit

    data
      .size(Second)
      .saveAsText(s"./tmp.${tool}/siz2.out")
      .toUnit

    data
      .size(Third)
      .saveAsText(s"./tmp.${tool}/siz3.out")
      .toUnit
  }
}

object TestSpark22 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val tool = "spark"
    val path = args(1)

    val (data, _) = loadText(path + "/numericInputfile.txt", Cell.parse2D())

    case class Diff() extends Window[Position2D, Position1D, Position1D, Position2D] {
      type I = Option[Double]
      type T = (Option[Double], Position1D)
      type O = (Double, Position1D, Position1D)

      def prepare(cell: Cell[Position2D]): I = cell.content.value.asDouble

      def initialise(rem: Position1D, in: I): (T, TraversableOnce[O]) = ((in, rem), None)

      def update(rem: Position1D, in: I, t: T): (T, TraversableOnce[O]) = {
        ((in, rem), (in, t._1) match {
          case (Some(c), Some(l)) => Some((c - l, rem,  t._2))
          case _ => None
        })
      }

      def present(pos: Position1D, out: O): TraversableOnce[Cell[Position2D]] = {
        Some(Cell(pos.append(out._2.toShortString("") + "-" + out._3.toShortString("")),
          Content(ContinuousSchema(DoubleCodex), out._1)))
      }
    }

    data
      .slide(Over(First), Diff())
      .saveAsText(s"./tmp.${tool}/dif1.out")
      .toUnit

    data
      .slide(Over(Second), Diff())
      .permute(Second, First)
      .saveAsText(s"./tmp.${tool}/dif2.out")
      .toUnit
  }
}

object TestSpark23 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val tool = "spark"
    val path = args(1)

    val (data, _) = loadText(path + "/somePairwise.txt", Cell.parse2D())

    case class DiffSquared() extends Operator[Position2D, Position2D] {
      def compute(left: Cell[Position2D], right: Cell[Position2D]): TraversableOnce[Cell[Position2D]] = {
        val xc = left.position(Second).toShortString
        val yc = right.position(Second).toShortString

        (left.position(First) == right.position(First)) match {
          case true => Some(Cell(right.position.remove(Second).append("(" + xc + "-" + yc + ")^2"),
            Content(ContinuousSchema(DoubleCodex),
              math.pow(left.content.value.asLong.get - right.content.value.asLong.get, 2))))
          case false => None
        }
      }
    }

    data
      .pairwise(Over(Second), Upper, DiffSquared())
      .saveAsText(s"./tmp.${tool}/pws1.out")
      .toUnit
  }
}

object TestSpark24 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val tool = "spark"
    val path = args(1)

    // see http://www.mathsisfun.com/data/correlation.html for data

    val schema = List(("day", NominalSchema(StringCodex)),
                      ("temperature", ContinuousSchema(DoubleCodex)),
                      ("sales", DiscreteSchema(LongCodex)))
    val (data, _) = loadText(path + "/somePairwise2.txt", Cell.parseTable(schema, separator = "|"))

    data
      .correlation(Over(Second))
      .saveAsText(s"./tmp.${tool}/pws2.out")
      .toUnit

    val schema2 = List(("day", NominalSchema(StringCodex)),
                       ("temperature", ContinuousSchema(DoubleCodex)),
                       ("sales", DiscreteSchema(LongCodex)),
                       ("neg.sales", DiscreteSchema(LongCodex)))
    val (data2, _) = loadText(path + "/somePairwise3.txt", Cell.parseTable(schema2, separator = "|"))

    data2
      .correlation(Over(Second))
      .saveAsText(s"./tmp.${tool}/pws3.out")
      .toUnit
  }
}

object TestSpark25 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val tool = "spark"
    val path = args(1)

    loadText(path + "/mutualInputfile.txt", Cell.parse2D())
      .data
      .mutualInformation(Over(Second))
      .saveAsText(s"./tmp.${tool}/mi.out")
      .toUnit
  }
}

object TestSpark26 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val tool = "spark"
    val path = args(1)

    val (left, _) = loadText(path + "/algebraInputfile1.txt", Cell.parse2D())
    val (right, _) = loadText(path + "/algebraInputfile2.txt", Cell.parse2D())

    left
      .pairwiseBetween(Over(First), All, right, Times(Locate.OperatorString[Position2D](Over(First), "(%1$s*%2$s)")))
      .saveAsText(s"./tmp.${tool}/alg.out")
      .toUnit
  }
}

object TestSpark27 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val tool = "spark"
    val path = args(1)

    // http://www.statisticshowto.com/moving-average/

    loadText(path + "/simMovAvgInputfile.txt", Cell.parse2D(first = LongCodex))
      .data
      .slide(Over(Second), SimpleMovingAverage[Position2D, Position1D, Position1D, Position2D](5,
        Locate.WindowDimension[Position1D, Position1D](First)))
      .saveAsText(s"./tmp.${tool}/sma1.out")
      .toUnit

    loadText(path + "/simMovAvgInputfile.txt", Cell.parse2D(first = LongCodex))
      .data
      .slide(Over(Second), SimpleMovingAverage[Position2D, Position1D, Position1D, Position2D](5,
        Locate.WindowDimension[Position1D, Position1D](First), all = true))
      .saveAsText(s"./tmp.${tool}/sma2.out")
      .toUnit

    loadText(path + "/simMovAvgInputfile.txt", Cell.parse2D(first = LongCodex))
      .data
      .slide(Over(Second), CenteredMovingAverage[Position2D, Position1D, Position1D, Position2D](2,
        Locate.WindowDimension[Position1D, Position1D](First)))
      .saveAsText(s"./tmp.${tool}/tma.out")
      .toUnit

    loadText(path + "/simMovAvgInputfile.txt", Cell.parse2D(first = LongCodex))
      .data
      .slide(Over(Second), WeightedMovingAverage[Position2D, Position1D, Position1D, Position2D](5,
        Locate.WindowDimension[Position1D, Position1D](First)))
      .saveAsText(s"./tmp.${tool}/wma1.out")
      .toUnit

    loadText(path + "/simMovAvgInputfile.txt", Cell.parse2D(first = LongCodex))
      .data
      .slide(Over(Second), WeightedMovingAverage[Position2D, Position1D, Position1D, Position2D](5,
        Locate.WindowDimension[Position1D, Position1D](First), all = true))
      .saveAsText(s"./tmp.${tool}/wma2.out")
      .toUnit

    // http://stackoverflow.com/questions/11074665/how-to-calculate-the-cumulative-average-for-some-numbers

    loadText(path + "/cumMovAvgInputfile.txt", Cell.parse1D())
      .data
      .slide(Along(First), CumulativeMovingAverage[Position1D, Position0D, Position1D, Position1D](
        Locate.WindowDimension[Position0D, Position1D](First)))
      .saveAsText(s"./tmp.${tool}/cma.out")
      .toUnit

    // http://www.incrediblecharts.com/indicators/exponential_moving_average.php

    loadText(path + "/expMovAvgInputfile.txt", Cell.parse1D())
      .data
      .slide(Along(First), ExponentialMovingAverage[Position1D, Position0D, Position1D, Position1D](0.33,
        Locate.WindowDimension[Position0D, Position1D](First)))
      .saveAsText(s"./tmp.${tool}/ema.out")
      .toUnit
  }
}

object TestSpark28 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val tool = "spark"

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
      .compact(Over(First))

    val extractor = ExtractWithDimension[Dimension.Second, Position2D, List[Double]](Second)

    data
      .transformWithValue(Cut(extractor), CutRules.fixed(stats, "min", "max", 4))
      .saveAsText(s"./tmp.${tool}/cut1.out")
      .toUnit

    data
      .transformWithValue(Cut(extractor).andThenRenameWithValue(TransformerWithValue.rename(Second, "%s.square")),
        CutRules.squareRootChoice(stats, "count", "min", "max"))
      .saveAsText(s"./tmp.${tool}/cut2.out")
      .toUnit

    data
      .transformWithValue(Cut(extractor).andThenRenameWithValue(TransformerWithValue.rename(Second, "%s.sturges")),
        CutRules.sturgesFormula(stats, "count", "min", "max"))
      .saveAsText(s"./tmp.${tool}/cut3.out")
      .toUnit

    data
      .transformWithValue(Cut(extractor).andThenRenameWithValue(TransformerWithValue.rename(Second, "%s.rice")),
        CutRules.riceRule(stats, "count", "min", "max"))
      .saveAsText(s"./tmp.${tool}/cut4.out")
      .toUnit

    data
      .transformWithValue(Cut(extractor).andThenRenameWithValue(TransformerWithValue.rename(Second, "%s.doane")),
        CutRules.doanesFormula(stats, "count", "min", "max", "skewness"))
      .saveAsText(s"./tmp.${tool}/cut5.out")
      .toUnit

    data
      .transformWithValue(Cut(extractor).andThenRenameWithValue(TransformerWithValue.rename(Second, "%s.scott")),
        CutRules.scottsNormalReferenceRule(stats, "count", "min", "max", "sd"))
      .saveAsText(s"./tmp.${tool}/cut6.out")
      .toUnit

    data
      .transformWithValue(Cut(extractor).andThenRenameWithValue(TransformerWithValue.rename(Second, "%s.break")),
        CutRules.breaks(Map("fid:A" -> List(-1, 4, 8, 12, 16))))
      .saveAsText(s"./tmp.${tool}/cut7.out")
      .toUnit
  }
}

object TestSpark29 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val tool = "spark"

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
      .saveAsText(s"./tmp.${tool}/gini.out")
      .toUnit

    data
      .map { case (a, b, c) => (b, a, c) }
      .gini(Along(First))
      .saveAsText(s"./tmp.${tool}/inig.out")
      .toUnit
  }
}

object TestSpark30 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val tool = "spark"

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
      .stream("Rscript double.R", List("double.R"), Cell.toString("|", false, true),
        Cell.parse2D("#", StringCodex, LongCodex))
      .data
      .saveAsText(s"./tmp.${tool}/strm.out")
      .toUnit
  }
}

object TestSpark31 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val tool = "spark"
    val path = args(1)

    val (data, errors) = loadText(path + "/badInputfile.txt", Cell.parse3D(third = DateCodex()))

    data
      .saveAsText(s"./tmp.${tool}/yok.out", Cell.toString(descriptive = true))
      .toUnit
    errors.saveAsTextFile(s"./tmp.${tool}/nok.out")
  }
}

object TestSpark32 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val tool = "spark"

    List(("a", Content(ContinuousSchema(DoubleCodex), 3.14)),
         ("b", Content(DiscreteSchema(LongCodex), 42)),
         ("c", Content(NominalSchema(StringCodex), "foo")))
      .saveAsIV(s"./tmp.${tool}/iv1.out")
      .toUnit

    List(("a", "d", Content(ContinuousSchema(DoubleCodex), 3.14)),
         ("b", "c", Content(DiscreteSchema(LongCodex), 42)),
         ("c", "b", Content(NominalSchema(StringCodex), "foo")))
      .saveAsIV(s"./tmp.${tool}/iv2.out")
      .toUnit

    List(("a", "d", "c", Content(ContinuousSchema(DoubleCodex), 3.14)),
         ("b", "c", "d", Content(DiscreteSchema(LongCodex), 42)),
         ("c", "b", "e", Content(NominalSchema(StringCodex), "foo")))
      .saveAsIV(s"./tmp.${tool}/iv3.out")
      .toUnit

    List(("a", "d", "c", "d", Content(ContinuousSchema(DoubleCodex), 3.14)),
         ("b", "c", "d", "e", Content(DiscreteSchema(LongCodex), 42)),
         ("c", "b", "e", "f", Content(NominalSchema(StringCodex), "foo")))
      .saveAsIV(s"./tmp.${tool}/iv4.out")
      .toUnit

    List(("a", "d", "c", "d", "e", Content(ContinuousSchema(DoubleCodex), 3.14)),
         ("b", "c", "d", "e", "f", Content(DiscreteSchema(LongCodex), 42)),
         ("c", "b", "e", "f", "g", Content(NominalSchema(StringCodex), "foo")))
      .saveAsIV(s"./tmp.${tool}/iv5.out")
      .toUnit

    List(("a", "d", "c", "d", "e", "f", Content(ContinuousSchema(DoubleCodex), 3.14)),
         ("b", "c", "d", "e", "f", "g", Content(DiscreteSchema(LongCodex), 42)),
         ("c", "b", "e", "f", "g", "h", Content(NominalSchema(StringCodex), "foo")))
      .saveAsIV(s"./tmp.${tool}/iv6.out")
      .toUnit

    List(("a", "d", "c", "d", "e", "f", "g", Content(ContinuousSchema(DoubleCodex), 3.14)),
         ("b", "c", "d", "e", "f", "g", "h", Content(DiscreteSchema(LongCodex), 42)),
         ("c", "b", "e", "f", "g", "h", "i", Content(NominalSchema(StringCodex), "foo")))
      .saveAsIV(s"./tmp.${tool}/iv7.out")
      .toUnit

    List(("a", "d", "c", "d", "e", "f", "g", "h", Content(ContinuousSchema(DoubleCodex), 3.14)),
         ("b", "c", "d", "e", "f", "g", "h", "i", Content(DiscreteSchema(LongCodex), 42)),
         ("c", "b", "e", "f", "g", "h", "i", "j", Content(NominalSchema(StringCodex), "foo")))
      .saveAsIV(s"./tmp.${tool}/iv8.out")
      .toUnit

    List(("a", "d", "c", "d", "e", "f", "g", "h", "i", Content(ContinuousSchema(DoubleCodex), 3.14)),
         ("b", "c", "d", "e", "f", "g", "h", "i", "j", Content(DiscreteSchema(LongCodex), 42)),
         ("c", "b", "e", "f", "g", "h", "i", "j", "k", Content(NominalSchema(StringCodex), "foo")))
      .saveAsIV(s"./tmp.${tool}/iv9.out")
      .toUnit
  }
}

object TestSpark33 {
  def main(args: Array[String]) {
    implicit val spark = new SparkContext(args(0), "Test Spark", new SparkConf())
    val tool = "spark"

    val data = List(("a", "one", Content(ContinuousSchema(DoubleCodex), 3.14)),
      ("a", "two", Content(NominalSchema(StringCodex), "foo")),
      ("a", "three", Content(DiscreteSchema(LongCodex), 42)),
      ("b", "one", Content(ContinuousSchema(DoubleCodex), 6.28)),
      ("b", "two", Content(DiscreteSchema(LongCodex), 123)),
      ("b", "three", Content(ContinuousSchema(DoubleCodex), 9.42)),
      ("c", "two", Content(NominalSchema(StringCodex), "bar")),
      ("c", "three", Content(ContinuousSchema(DoubleCodex), 12.56)))

    val labels = spark.parallelize(
      List(Cell(Position1D("a"), Content(DiscreteSchema(LongCodex), 1)),
        Cell(Position1D("b"), Content(DiscreteSchema(LongCodex), 2))))

    val importance = spark.parallelize(
      List(Cell(Position1D("a"), Content(ContinuousSchema(DoubleCodex), 0.5)),
        Cell(Position1D("b"), Content(ContinuousSchema(DoubleCodex), 0.75))))

    data
      .saveAsVW(Over(First), s"./tmp.${tool}/vw0.out", tag=false)
      .toUnit

    data
      .saveAsVW(Over(First), s"./tmp.${tool}/vw1.out", tag=true)
      .toUnit

    data
      .saveAsVWWithLabels(Over(First), s"./tmp.${tool}/vw2.out", labels, tag=false)
      .toUnit

    data
      .saveAsVWWithImportance(Over(First), s"./tmp.${tool}/vw3.out", importance, tag=true)
      .toUnit

    data
      .saveAsVWWithLabelsAndImportance(Over(First), s"./tmp.${tool}/vw4.out", labels, importance, tag=false)
      .toUnit
  }
}

