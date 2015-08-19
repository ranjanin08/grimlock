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
import au.com.cba.omnia.grimlock.framework.utility._
import au.com.cba.omnia.grimlock.framework.window._

import au.com.cba.omnia.grimlock.library.aggregate._
import au.com.cba.omnia.grimlock.library.pairwise._
import au.com.cba.omnia.grimlock.library.partition._
import au.com.cba.omnia.grimlock.library.squash._
import au.com.cba.omnia.grimlock.library.transform._
import au.com.cba.omnia.grimlock.library.window._

import au.com.cba.omnia.grimlock.scalding.content.Contents._
import au.com.cba.omnia.grimlock.scalding.Matrix._
import au.com.cba.omnia.grimlock.scalding.Nameable._
import au.com.cba.omnia.grimlock.scalding.Names._
import au.com.cba.omnia.grimlock.scalding.partition.Partitions._
import au.com.cba.omnia.grimlock.scalding.position.Positions._
import au.com.cba.omnia.grimlock.scalding.position.PositionDistributable._
import au.com.cba.omnia.grimlock.scalding.transform._
import au.com.cba.omnia.grimlock.scalding.Types._

import cascading.flow.FlowDef
import com.twitter.scalding.{ Args, Job, Mode, TypedPsv }
import com.twitter.scalding.TDsl.sourceToTypedPipe
import com.twitter.scalding.typed.{ IterablePipe, TypedPipe, ValuePipe }

object TestScaldingReader {
  def load4TupleDataAddDate(file: String)(implicit flow: FlowDef, mode: Mode): TypedPipe[Cell[Position3D]] = {
    def hashDate(v: String) = {
      val cal = java.util.Calendar.getInstance()
      cal.setTime(DateCodex().fromValue(DateCodex().decode("2014-05-14").get))
      cal.add(java.util.Calendar.DATE, -(v.hashCode % 21)) // Generate 3 week window prior to date
      DateValue(cal.getTime(), DateCodex())
    }

    (TypedPsv[(String, String, String, String)](file))
      .flatMap {
        case (i, f, e, v) =>
          val schema = e match {
            case StringCodex.name => NominalSchema(StringCodex)
            case _ => scala.util.Try(v.toLong).toOption match {
              case Some(_) => ContinuousSchema[Codex.LongCodex](LongCodex, Long.MinValue, Long.MaxValue)
              case None => ContinuousSchema[Codex.DoubleCodex](DoubleCodex, Double.MinValue, Double.MaxValue)
            }
          }

          schema.decode(v).map { case c => Cell(Position3D(i, f, hashDate(v)), c) }
      }
  }
}

class TestScalding1(args : Args) extends Job(args) {

  val data = TestScaldingReader.load4TupleDataAddDate(args("path") + "/someInputfile3.txt")

  data
    .save("./tmp.scalding/dat1.out", descriptive = true)

  data
    .set(Position3D("iid:1548763", "fid:Y", DateCodex().decode("2014-04-26").get),
      Content(ContinuousSchema(LongCodex), 1234))
    .slice(Over(First), "iid:1548763", true)
    .save("./tmp.scalding/dat2.out", descriptive = true)

  loadText(args("path") + "/smallInputfile.txt", Cell.parse3D(third = DateCodex()))
    .save("./tmp.scalding/dat3.out", descriptive = true)
}

class TestScalding2(args : Args) extends Job(args) {

  val data = TestScaldingReader.load4TupleDataAddDate(args("path") + "/someInputfile3.txt")

  (data.names(Over(First)) ++ data.names(Over(Second)) ++ data.names(Over(Third)))
    .number
    .save("./tmp.scalding/nm0.out", descriptive = true)

  data
    .names(Over(Second))
    .number
    .moveToFront("fid:Z")
    .save("./tmp.scalding/nm1.out", descriptive = true)

  data
    .names(Over(Second))
    .number
    .slice("fid:M", false)
    .save("./tmp.scalding/nm2.out", descriptive = true)

  data
    .names(Over(Second))
    .number
    .set(Map("fid:A" -> 100L, "fid:C" -> 200L))
    .save("./tmp.scalding/nm3.out", descriptive = true)

  data
    .names(Over(Second))
    .number
    .moveToBack("fid:B")
    .save("./tmp.scalding/nm4.out", descriptive = true)

  data
    .names(Over(Second))
    .number
    .slice(""".*[BCD]$""".r, true, "")
    .save("./tmp.scalding/nm5.out", descriptive = true)
}

class TestScalding3(args : Args) extends Job(args) {

  val data = TestScaldingReader.load4TupleDataAddDate(args("path") + "/someInputfile3.txt")

  (data.types(Over(First)) ++ data.types(Over(Second)) ++ data.types(Over(Third)))
    .save("./tmp.scalding/typ1.out", descriptive = true)

  (data.types(Over(First), true) ++ data.types(Over(Second), true) ++ data.types(Over(Third), true))
    .save("./tmp.scalding/typ2.out", descriptive = true)
}

class TestScalding4(args : Args) extends Job(args) {

  val data = TestScaldingReader.load4TupleDataAddDate(args("path") + "/someInputfile3.txt")

  data
    .slice(Over(Second), "fid:B", true)
    .save("./tmp.scalding/scl0.out", descriptive = true)

  data
    .slice(Over(Second), List("fid:A", "fid:B"), true)
    .slice(Over(First), "iid:0221707", true)
    .save("./tmp.scalding/scl1.out", descriptive = true)

  val rem = List("fid:B", "fid:D", "fid:F", "fid:H", "fid:J", "fid:L", "fid:N",
                 "fid:P", "fid:R", "fid:T", "fid:V", "fid:X", "fid:Z")
  data
    .slice(Over(Second), data.names(Over(Second)).slice(rem, false), false)
    .save("./tmp.scalding/scl2.out", descriptive = true)
}

class TestScalding5(args : Args) extends Job(args) {

  val data = TestScaldingReader.load4TupleDataAddDate(args("path") + "/someInputfile3.txt")

  data
    .slice(Over(Second), List("fid:A", "fid:B"), true)
    .slice(Over(First), "iid:0221707", true)
    .squash(Third, PreservingMaxPosition[Position3D]())
    .save("./tmp.scalding/sqs1.out", descriptive = true)

  data
    .squash(Third, PreservingMaxPosition[Position3D]())
    .save("./tmp.scalding/sqs2.out", descriptive = true)

  data
    .slice(Over(First), List("iid:0064402", "iid:0066848", "iid:0076357", "iid:0216406", "iid:0221707", "iid:0262443",
                             "iid:0364354", "iid:0375226", "iid:0444510", "iid:1004305"), true)
    .squash(Third, PreservingMaxPosition[Position3D]())
    .saveAsCSV(Over(First), "./tmp.scalding/sqs3.out")

  data
    .slice(Over(First), List("iid:0064402", "iid:0066848", "iid:0076357", "iid:0216406", "iid:0221707", "iid:0262443",
                             "iid:0364354", "iid:0375226", "iid:0444510", "iid:1004305"), true)
    .slice(Over(Second), List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"), true)
    .squash(Third, PreservingMaxPosition[Position3D]())
    .saveAsCSV(Over(First), "./tmp.scalding/sqs4.out")
}

class TestScalding6(args : Args) extends Job(args) {

  val data = TestScaldingReader.load4TupleDataAddDate(args("path") + "/someInputfile3.txt")

  data
    .which((c: Cell[Position3D]) => c.content.schema.kind.isSpecialisationOf(Numerical))
    .save("./tmp.scalding/whc1.out", descriptive = true)

  data
    .which((c: Cell[Position3D]) => ! c.content.value.isInstanceOf[StringValue])
    .save("./tmp.scalding/whc2.out", descriptive = true)

  data
    .get(data.which((c: Cell[Position3D]) =>
      (c.content.value equ 666) || (c.content.value leq 11.0) || (c.content.value equ "KQUPKFEH")))
    .save("./tmp.scalding/whc3.out", descriptive = true)

  data
    .which((c: Cell[Position3D]) => c.content.value.isInstanceOf[LongValue])
    .save("./tmp.scalding/whc4.out", descriptive = true)

  val aggregators: List[Aggregator[Position2D, Position1D, Position2D]] = List(
    Count().andThenExpand(_.position.append("count")),
    Mean().andThenExpand(_.position.append("mean")),
    Min().andThenExpand(_.position.append("min")),
    Max().andThenExpand(_.position.append("max")),
    MaxAbs().andThenExpand(_.position.append("max.abs")))

  TestScaldingReader.load4TupleDataAddDate(args("path") + "/someInputfile3.txt")
    .slice(Over(First), List("iid:0064402", "iid:0066848", "iid:0076357", "iid:0216406", "iid:0221707", "iid:0262443",
                             "iid:0364354", "iid:0375226", "iid:0444510", "iid:1004305"), true)
    .slice(Over(Second), List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"), true)
    .squash(Third, PreservingMaxPosition[Position3D]())
    .summarise(Along(First), aggregators)
    .which(Over(Second), List(("count", (c: Cell[Position2D]) => c.content.value leq 2),
                              ("min", (c: Cell[Position2D]) => c.content.value equ 107)))
    .save("./tmp.scalding/whc5.out", descriptive = true)
}

class TestScalding7(args : Args) extends Job(args) {

  val data = TestScaldingReader.load4TupleDataAddDate(args("path") + "/someInputfile3.txt")

  data
    .get(Position3D("iid:1548763", "fid:Y", DateCodex().decode("2014-04-26").get))
    .save("./tmp.scalding/get1.out", descriptive = true)

  data
    .get(List(Position3D("iid:1548763", "fid:Y", DateCodex().decode("2014-04-26").get),
              Position3D("iid:1303823", "fid:A", DateCodex().decode("2014-05-05").get)))
    .save("./tmp.scalding/get2.out", descriptive = true)
}

class TestScalding8(args : Args) extends Job(args) {

  val data = TestScaldingReader.load4TupleDataAddDate(args("path") + "/someInputfile3.txt")

  data
    .slice(Over(Second), "fid:B", true)
    .squash(Third, PreservingMaxPosition[Position3D]())
    .unique()
    .save("./tmp.scalding/uniq.out", descriptive = true)

  loadText(args("path") + "/mutualInputfile.txt", Cell.parse2D())
    .unique(Over[Position2D, Dimension.Second](Second))
    .save("./tmp.scalding/uni2.out")

  data
    .slice(Over(Second), List("fid:A", "fid:B", "fid:Y", "fid:Z"), true)
    .slice(Over(First), List("iid:0221707", "iid:0364354"), true)
    .squash(Third, PreservingMaxPosition[Position3D]())
    .saveAsCSV(Over(Second), "./tmp.scalding/test.csv")
    .saveAsCSV(Over(First), "./tmp.scalding/tset.csv", writeHeader = false, separator = ",")

  data
    .slice(Over(Second), List("fid:A", "fid:B", "fid:Y", "fid:Z"), true)
    .slice(Over(First), List("iid:0221707", "iid:0364354"), true)
    .squash(Third, PreservingMaxPosition[Position3D]())
    .permute(Second, First)
    .save("./tmp.scalding/trs1.out", descriptive = true)

  data
    .slice(Over(Second), List("fid:A", "fid:B", "fid:Y", "fid:Z"), true)
    .slice(Over(First), List("iid:0221707", "iid:0364354"), true)
    .squash(Third, PreservingMaxPosition[Position3D]())
    .save("./tmp.scalding/data.txt")
}

class TestScalding9(args : Args) extends Job(args) {

  val data = TestScaldingReader.load4TupleDataAddDate(args("path") + "/someInputfile3.txt")

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
    .save("./tmp.scalding/prt1.out", descriptive = true)

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
    .save("./tmp.scalding/prt2.out", descriptive = true)

  prt1
    .get("training")
    .save("./tmp.scalding/train.out", descriptive = true)

  prt1
    .get("testing")
    .save("./tmp.scalding/test.out", descriptive = true)

  prt1
    .get("scoring")
    .save("./tmp.scalding/score.out", descriptive = true)
}

class TestScalding10(args : Args) extends Job(args) {

  val data = TestScaldingReader.load4TupleDataAddDate(args("path") + "/someInputfile3.txt")

  data
    .summarise(Over(Second), Mean[Position3D, Position1D](true, true).andThenExpand(_.position.append("mean")))
    .saveAsCSV(Over(Second), "./tmp.scalding/agg1.csv")

  data
    .slice(Over(First), List("iid:0064402", "iid:0066848", "iid:0076357", "iid:0216406", "iid:0221707", "iid:0262443",
                             "iid:0364354", "iid:0375226", "iid:0444510", "iid:1004305"), true)
    .squash(Third, PreservingMaxPosition[Position3D]())
    .summarise(Along(Second), Count[Position2D, Position1D]().andThenExpand(_.position.append("count")))
    .saveAsCSV(Over(Second), "./tmp.scalding/agg2.csv")

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
    .saveAsCSV(Over(Second), "./tmp.scalding/agg3.csv")
}

class TestScalding11(args : Args) extends Job(args) {

  val data = TestScaldingReader.load4TupleDataAddDate(args("path") + "/someInputfile3.txt")

  data
    .slice(Over(Second), List("fid:A", "fid:B", "fid:Y", "fid:Z"), true)
    .slice(Over(First), List("iid:0221707", "iid:0364354"), true)
    .transform(Indicator[Position3D]() andThenRename Transformer.rename(Second, "%1$s.ind"))
    .save("./tmp.scalding/trn2.out", descriptive = true)

  data
    .slice(Over(Second), List("fid:A", "fid:B", "fid:Y", "fid:Z"), true)
    .slice(Over(First), List("iid:0221707", "iid:0364354"), true)
    .squash(Third, PreservingMaxPosition[Position3D]())
    .transform(Binarise[Position2D](Binarise.rename(Second)))
    .saveAsCSV(Over(Second), "./tmp.scalding/trn3.out")
}

class TestScalding12(args : Args) extends Job(args) {

  val data = TestScaldingReader.load4TupleDataAddDate(args("path") + "/someInputfile3.txt")
    .slice(Over(Second), List("fid:A", "fid:B", "fid:Y", "fid:Z"), true)
    .slice(Over(First), List("iid:0221707", "iid:0364354"), true)

  data
    .squash(Third, PreservingMaxPosition[Position3D]())
    .fill(Content(ContinuousSchema(LongCodex), 0))
    .saveAsCSV(Over(Second), "./tmp.scalding/fll1.out")

  data
    .fill(Content(ContinuousSchema(LongCodex), 0))
    .save("./tmp.scalding/fll3.out", descriptive = true)
}

class TestScalding13(args : Args) extends Job(args) {

  val all = TestScaldingReader.load4TupleDataAddDate(args("path") + "/someInputfile3.txt")
  val data = all
    .slice(Over(First), List("iid:0064402", "iid:0066848", "iid:0076357", "iid:0216406", "iid:0221707", "iid:0262443",
                             "iid:0364354", "iid:0375226", "iid:0444510", "iid:1004305"), true)
    .slice(Over(Second), List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"), true)
    .squash(Third, PreservingMaxPosition[Position3D]())

  val inds = data
    .transform(Indicator[Position2D]().andThenRename(Transformer.rename(Second, "%1$s.ind")))
    .fill(Content(ContinuousSchema(LongCodex), 0))

  data
    .join(Over(First), inds)
    .fill(Content(ContinuousSchema(LongCodex), 0))
    .saveAsCSV(Over(Second), "./tmp.scalding/fll2.out")

  data
    .fill(Over[Position2D, Dimension.Second](Second),
      all.summarise(Over(Second), Mean[Position3D, Position1D](true, true)))
    .join(Over(First), inds)
    .saveAsCSV(Over(Second), "./tmp.scalding/fll4.out")
}

class TestScalding14(args : Args) extends Job(args) {

  val data = TestScaldingReader.load4TupleDataAddDate(args("path") + "/someInputfile3.txt")
    .slice(Over(Second), List("fid:A", "fid:B", "fid:Y", "fid:Z"), true)
    .slice(Over(First), List("iid:0221707", "iid:0364354"), true)

  data
    .change(Over(Second), "fid:A", NominalSchema(LongCodex))
    .save("./tmp.scalding/chg1.out", descriptive = true)
}

class TestScalding15(args : Args) extends Job(args) {

  val data = TestScaldingReader.load4TupleDataAddDate(args("path") + "/someInputfile3.txt")

  data
    .slice(Over(Second), List("fid:A", "fid:C", "fid:E", "fid:G"), true)
    .slice(Over(First), List("iid:0221707", "iid:0364354"), true)
    .summarise(Along(Third), Sum[Position3D, Position2D]().andThenExpand(_.position.append("sum")))
    .melt(Third, Second)
    .saveAsCSV(Over(Second), "./tmp.scalding/rsh1.out")

  val inds = data
    .slice(Over(First), List("iid:0064402", "iid:0066848", "iid:0076357", "iid:0216406", "iid:0221707", "iid:0262443",
                             "iid:0364354", "iid:0375226", "iid:0444510", "iid:1004305"), true)
    .slice(Over(Second), List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"), true)
    .squash(Third, PreservingMaxPosition[Position3D]())
    .transform(Indicator[Position2D]().andThenRename(Transformer.rename(Second, "%1$s.ind")))
    .saveAsCSV(Over(Second), "./tmp.scalding/trn1.csv")

  data
    .slice(Over(First), List("iid:0064402", "iid:0066848", "iid:0076357", "iid:0216406", "iid:0221707", "iid:0262443",
                             "iid:0364354", "iid:0375226", "iid:0444510", "iid:1004305"), true)
    .slice(Over(Second), List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"), true)
    .squash(Third, PreservingMaxPosition[Position3D]())
    .join(Over(First), inds)
    .saveAsCSV(Over(Second), "./tmp.scalding/jn1.csv")
}

class TestScalding16(args : Args) extends Job(args) {

  val data = TestScaldingReader.load4TupleDataAddDate(args("path") + "/someInputfile3.txt")

  case class HashSample() extends Sampler[Position3D] {
    def select(cell: Cell[Position3D]): Boolean = (cell.position(First).toString.hashCode % 25) == 0
  }

  data
    .sample(HashSample())
    .save("./tmp.scalding/smp1.out")
}

class TestScalding17(args : Args) extends Job(args) {

  val data = TestScaldingReader.load4TupleDataAddDate(args("path") + "/someInputfile3.txt")
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
    .saveAsCSV(Over(Second), "./tmp.scalding/trn6.csv")

  case class Sample500() extends Sampler[Position2D] {
    def select(cell: Cell[Position2D]): Boolean = cell.content.value gtr 500
  }

  data
    .sample(Sample500())
    .saveAsCSV(Over(Second), "./tmp.scalding/flt1.csv")

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
    .saveAsCSV(Over(Second), "./tmp.scalding/flt2.csv")
}

class TestScalding18(args : Args) extends Job(args) {

  val data = TestScaldingReader.load4TupleDataAddDate(args("path") + "/someInputfile3.txt")
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
    .which(Over(Second), "count", (c: Cell[Position2D]) => c.content.value leq 2)
    .names(Over(First))

  data
    .slice(Over(Second), rem, false)
    .saveAsCSV(Over(Second), "./tmp.scalding/flt3.csv")
}

class TestScalding19(args : Args) extends Job(args) {

  val raw = TestScaldingReader.load4TupleDataAddDate(args("path") + "/someInputfile3.txt")
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

  def cb(key: String, pipe: TypedPipe[Cell[Position2D]]): TypedPipe[Cell[Position2D]] = {
    pipe
      .slice(Over(Second), rem, false)
      .transformWithValue(transforms, stats.toMap(Over[Position2D, Dimension.First](First)))
      .fill(Content(ContinuousSchema(LongCodex), 0))
      .saveAsCSV(Over(Second), "./tmp.scalding/pln_" + key + ".csv")
  }

  parts
    .forEach(List("train", "test"), cb)
}

class TestScalding20(args : Args) extends Job(args) {

  loadText(args("path") + "/ivoryInputfile1.txt",
      Cell.parse3DWithDictionary(Dictionary.load(args("path") + "/dict.txt"), Second, third = DateCodex()))
    .save("./tmp.scalding/ivr1.out")
}

class TestScalding21(args : Args) extends Job(args) {

  val data = TestScaldingReader.load4TupleDataAddDate(args("path") + "/someInputfile3.txt")

  data
    .shape()
    .save("./tmp.scalding/siz0.out")

  data
    .size(First)
    .save("./tmp.scalding/siz1.out")

  data
    .size(Second)
    .save("./tmp.scalding/siz2.out")

  data
    .size(Third)
    .save("./tmp.scalding/siz3.out")
}

class TestScalding22(args : Args) extends Job(args) {

  val data = loadText(args("path") + "/numericInputfile.txt", Cell.parse2D())

  case class Diff() extends Window[Position1D, Position1D, Position2D] {
    type T = Cell[Position1D]

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
    .save("./tmp.scalding/dif1.out")

  data
    .slide(Over(Second), Diff())
    .permute(Second, First)
    .save("./tmp.scalding/dif2.out")
}

class TestScalding23(args : Args) extends Job(args) {

  val data = loadText(args("path") + "/somePairwise.txt", Cell.parse2D())

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
    .save("./tmp.scalding/pws1.out")
}

class TestScalding24(args: Args) extends Job(args) {

  // see http://www.mathsisfun.com/data/correlation.html for data

  val schema = List(("day", NominalSchema(StringCodex)),
                    ("temperature", ContinuousSchema(DoubleCodex)),
                    ("sales", DiscreteSchema(LongCodex)))
  val data = loadText(args("path") + "/somePairwise2.txt", Cell.parseTable(schema, separator = "|"))

  data
    .correlation(Over(Second))
    .save("./tmp.scalding/pws2.out")

  val schema2 = List(("day", NominalSchema(StringCodex)),
                     ("temperature", ContinuousSchema(DoubleCodex)),
                     ("sales", DiscreteSchema(LongCodex)),
                     ("neg.sales", DiscreteSchema(LongCodex)))
  val data2 = loadText(args("path") + "/somePairwise3.txt", Cell.parseTable(schema2, separator = "|"))

  data2
    .correlation(Over(Second))
    .save("./tmp.scalding/pws3.out")
}

class TestScalding25(args: Args) extends Job(args) {

  loadText(args("path") + "/mutualInputfile.txt", Cell.parse2D())
    .mutualInformation(Over(Second))
    .save("./tmp.scalding/mi.out")
}

class TestScalding26(args: Args) extends Job(args) {

  val left = loadText(args("path") + "/algebraInputfile1.txt", Cell.parse2D())
  val right = loadText(args("path") + "/algebraInputfile2.txt", Cell.parse2D())

  left
    .pairwiseBetween(Over(First), All, right, Times(Locate.OperatorString[Position1D, Position1D]("(%1$s*%2$s)")))
    .save("./tmp.scalding/alg.out")
}

class TestScalding27(args: Args) extends Job(args) {

  // http://www.statisticshowto.com/moving-average/
  loadText(args("path") + "/simMovAvgInputfile.txt", Cell.parse2D(first = LongCodex))
    .slide(Over(Second), SimpleMovingAverage(5, Locate.WindowDimension[Position1D, Position1D](First)))
    .save("./tmp.scalding/sma1.out")

  loadText(args("path") + "/simMovAvgInputfile.txt", Cell.parse2D(first = LongCodex))
    .slide(Over(Second), SimpleMovingAverage(5, Locate.WindowDimension[Position1D, Position1D](First), all = true))
    .save("./tmp.scalding/sma2.out")

  loadText(args("path") + "/simMovAvgInputfile.txt", Cell.parse2D(first = LongCodex))
    .slide(Over(Second), CenteredMovingAverage(2, Locate.WindowDimension[Position1D, Position1D](First)))
    .save("./tmp.scalding/tma.out")

  loadText(args("path") + "/simMovAvgInputfile.txt", Cell.parse2D(first = LongCodex))
    .slide(Over(Second), WeightedMovingAverage(5, Locate.WindowDimension[Position1D, Position1D](First)))
    .save("./tmp.scalding/wma1.out")

  loadText(args("path") + "/simMovAvgInputfile.txt", Cell.parse2D(first = LongCodex))
    .slide(Over(Second), WeightedMovingAverage(5, Locate.WindowDimension[Position1D, Position1D](First), all = true))
    .save("./tmp.scalding/wma2.out")

  // http://stackoverflow.com/questions/11074665/how-to-calculate-the-cumulative-average-for-some-numbers
  loadText(args("path") + "/cumMovAvgInputfile.txt", Cell.parse1D())
    .slide(Along(First), CumulativeMovingAverage(Locate.WindowDimension[Position0D, Position1D](First)))
    .save("./tmp.scalding/cma.out")

  // http://www.incrediblecharts.com/indicators/exponential_moving_average.php
  loadText(args("path") + "/expMovAvgInputfile.txt", Cell.parse1D())
    .slide(Along(First), ExponentialMovingAverage(0.33, Locate.WindowDimension[Position0D, Position1D](First)))
    .save("./tmp.scalding/ema.out")
}

class TestScalding28(args: Args) extends Job(args) {

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
    .save("./tmp.scalding/cut1.out")

  data
    .transformWithValue(Cut(extractor).andThenRenameWithValue(TransformerWithValue.rename(Second, "%s.square")),
      CutRules.squareRootChoice(stats, "count", "min", "max"))
    .save("./tmp.scalding/cut2.out")

  data
    .transformWithValue(Cut(extractor).andThenRenameWithValue(TransformerWithValue.rename(Second, "%s.sturges")),
      CutRules.sturgesFormula(stats, "count", "min", "max"))
    .save("./tmp.scalding/cut3.out")

  data
    .transformWithValue(Cut(extractor).andThenRenameWithValue(TransformerWithValue.rename(Second, "%s.rice")),
      CutRules.riceRule(stats, "count", "min", "max"))
    .save("./tmp.scalding/cut4.out")

  data
    .transformWithValue(Cut(extractor).andThenRenameWithValue(TransformerWithValue.rename(Second, "%s.doane")),
      CutRules.doanesFormula(stats, "count", "min", "max", "skewness"))
    .save("./tmp.scalding/cut5.out")

  data
    .transformWithValue(Cut(extractor).andThenRenameWithValue(TransformerWithValue.rename(Second, "%s.scott")),
      CutRules.scottsNormalReferenceRule(stats, "count", "min", "max", "sd"))
    .save("./tmp.scalding/cut6.out")

  data
    .transformWithValue(Cut(extractor).andThenRenameWithValue(TransformerWithValue.rename(Second, "%s.break")),
      CutRules.breaks(Map("fid:A" -> List(-1, 4, 8, 12, 16))))
    .save("./tmp.scalding/cut7.out")
}

class TestScalding29(args: Args) extends Job(args) {

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
    .save("./tmp.scalding/gini.out")

  data
    .map { case (a, b, c) => (b, a, c) }
    .gini(Along(First))
    .save("./tmp.scalding/inig.out")
}

class TestScalding30(args: Args) extends Job(args) {

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
    .save("./tmp.scalding/strm.out")
}

