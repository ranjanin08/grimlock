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
      cal.setTime(DateCodex.fromValue(DateCodex.decode("2014-05-14").get))
      cal.add(java.util.Calendar.DATE, -(v.hashCode % 21)) // Generate 3 week window prior to date
      DateValue(cal.getTime(), DateCodex)
    }

    (TypedPsv[(String, String, String, String)](file))
      .flatMap {
        case (i, f, e, v) =>
          val schema = e match {
            case StringCodex.name => NominalSchema[Codex.StringCodex]()
            case _ => scala.util.Try(v.toLong).toOption match {
              case Some(_) => ContinuousSchema[Codex.LongCodex](Long.MinValue, Long.MaxValue)
              case None => ContinuousSchema[Codex.DoubleCodex](Double.MinValue, Double.MaxValue)
            }
          }

          schema.decode(v).map { case c => Cell(Position3D(i, f, hashDate(v)), c) }
      }
  }
}

class TestScalding1(args : Args) extends Job(args) {

  val data = TestScaldingReader.load4TupleDataAddDate(args("path") + "/someInputfile3.txt")

  data
    .save("./tmp.scalding/dat1.out", descriptive=true)

  data
    .set(Position3D("iid:1548763", "fid:Y", DateCodex.decode("2014-04-26").get),
      Content(ContinuousSchema[Codex.LongCodex](), 1234))
    .slice(Over(First), "iid:1548763", true)
    .save("./tmp.scalding/dat2.out", descriptive=true)

  load3D(args("path") + "/smallInputfile.txt")
    .save("./tmp.scalding/dat3.out", descriptive=true)
}

class TestScalding2(args : Args) extends Job(args) {

  val data = TestScaldingReader.load4TupleDataAddDate(args("path") + "/someInputfile3.txt")

  (data.names(Over(First)) ++ data.names(Over(Second)) ++ data.names(Over(Third)))
    .renumber
    .save("./tmp.scalding/nm0.out", descriptive=true)

  data
    .names(Over(Second))
    .moveToFront("fid:Z")
    .save("./tmp.scalding/nm1.out", descriptive=true)

  data
    .names(Over(Second))
    .slice("fid:M", false)
    .save("./tmp.scalding/nm2.out", descriptive=true)

  data
    .names(Over(Second))
    .set(Map("fid:A" -> 100L, "fid:C" -> 200L))
    .save("./tmp.scalding/nm3.out", descriptive=true)

  data
    .names(Over(Second))
    .moveToBack("fid:B")
    .save("./tmp.scalding/nm4.out", descriptive=true)

  data
    .names(Over(Second))
    .slice(""".*[BCD]$""".r, true, "")
    .save("./tmp.scalding/nm5.out", descriptive=true)
}

class TestScalding3(args : Args) extends Job(args) {

  val data = TestScaldingReader.load4TupleDataAddDate(args("path") + "/someInputfile3.txt")

  (data.types(Over(First)) ++ data.types(Over(Second)) ++ data.types(Over(Third)))
    .save("./tmp.scalding/typ1.out", descriptive=true)

  (data.types(Over(First), true) ++ data.types(Over(Second), true) ++ data.types(Over(Third), true))
    .save("./tmp.scalding/typ2.out", descriptive=true)
}

class TestScalding4(args : Args) extends Job(args) {

  val data = TestScaldingReader.load4TupleDataAddDate(args("path") + "/someInputfile3.txt")

  data
    .slice(Over(Second), "fid:B", true)
    .save("./tmp.scalding/scl0.out", descriptive=true)

  data
    .slice(Over(Second), List("fid:A", "fid:B"), true)
    .slice(Over(First), "iid:0221707", true)
    .save("./tmp.scalding/scl1.out", descriptive=true)

  val rem = List("fid:B", "fid:D", "fid:F", "fid:H", "fid:J", "fid:L", "fid:N",
                 "fid:P", "fid:R", "fid:T", "fid:V", "fid:X", "fid:Z")
  data
    .slice(Over(Second), data.names(Over(Second)).slice(rem, false), false)
    .save("./tmp.scalding/scl2.out", descriptive=true)
}

class TestScalding5(args : Args) extends Job(args) {

  val data = TestScaldingReader.load4TupleDataAddDate(args("path") + "/someInputfile3.txt")

  data
    .slice(Over(Second), List("fid:A", "fid:B"), true)
    .slice(Over(First), "iid:0221707", true)
    .squash(Third, PreservingMaxPosition())
    .save("./tmp.scalding/sqs1.out", descriptive=true)

  data
    .squash(Third, PreservingMaxPosition())
    .save("./tmp.scalding/sqs2.out", descriptive=true)

  data
    .slice(Over(First), List("iid:0064402", "iid:0066848", "iid:0076357", "iid:0216406", "iid:0221707", "iid:0262443",
                             "iid:0364354", "iid:0375226", "iid:0444510", "iid:1004305"), true)
    .squash(Third, PreservingMaxPosition())
    .saveAsCSV(Over(First), "./tmp.scalding/sqs3.out")

  data
    .slice(Over(First), List("iid:0064402", "iid:0066848", "iid:0076357", "iid:0216406", "iid:0221707", "iid:0262443",
                             "iid:0364354", "iid:0375226", "iid:0444510", "iid:1004305"), true)
    .slice(Over(Second), List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"), true)
    .squash(Third, PreservingMaxPosition())
    .saveAsCSV(Over(First), "./tmp.scalding/sqs4.out")
}

class TestScalding6(args : Args) extends Job(args) {

  val data = TestScaldingReader.load4TupleDataAddDate(args("path") + "/someInputfile3.txt")

  data
    .which((c: Cell[Position3D]) => c.content.schema.kind.isSpecialisationOf(Numerical))
    .save("./tmp.scalding/whc1.out", descriptive=true)

  data
    .which((c: Cell[Position3D]) => ! c.content.value.isInstanceOf[StringValue])
    .save("./tmp.scalding/whc2.out", descriptive=true)

  data
    .get(data.which((c: Cell[Position3D]) =>
      (c.content.value equ 666) || (c.content.value leq 11.0) || (c.content.value equ "KQUPKFEH")))
    .save("./tmp.scalding/whc3.out", descriptive=true)

  data
    .which((c: Cell[Position3D]) => c.content.value.isInstanceOf[LongValue])
    .save("./tmp.scalding/whc4.out", descriptive=true)

  TestScaldingReader.load4TupleDataAddDate(args("path") + "/someInputfile3.txt")
    .slice(Over(First), List("iid:0064402", "iid:0066848", "iid:0076357", "iid:0216406", "iid:0221707", "iid:0262443",
                             "iid:0364354", "iid:0375226", "iid:0444510", "iid:1004305"), true)
    .slice(Over(Second), List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"), true)
    .squash(Third, PreservingMaxPosition())
    .summariseAndExpand(Along(First), List(Count("count"), Mean("mean"), Min("min"), Max("max"), MaxAbs("max.abs")))
    .which(Over(Second), List(("count", (c: Cell[Position2D]) => c.content.value leq 2),
                              ("min", (c: Cell[Position2D]) => c.content.value equ 107)))
    .save("./tmp.scalding/whc5.out", descriptive=true)
}

class TestScalding7(args : Args) extends Job(args) {

  val data = TestScaldingReader.load4TupleDataAddDate(args("path") + "/someInputfile3.txt")

  data
    .get(Position3D("iid:1548763", "fid:Y", DateCodex.decode("2014-04-26").get))
    .save("./tmp.scalding/get1.out", descriptive=true)

  data
    .get(List(Position3D("iid:1548763", "fid:Y", DateCodex.decode("2014-04-26").get),
              Position3D("iid:1303823", "fid:A", DateCodex.decode("2014-05-05").get)))
    .save("./tmp.scalding/get2.out", descriptive=true)
}

class TestScalding8(args : Args) extends Job(args) {

  val data = TestScaldingReader.load4TupleDataAddDate(args("path") + "/someInputfile3.txt")

  data
    .slice(Over(Second), "fid:B", true)
    .squash(Third, PreservingMaxPosition())
    .unique
    .save("./tmp.scalding/uniq.out", descriptive=true)

  load2D(args("path") + "/mutualInputfile.txt")
    .unique(Over(Second))
    .save("./tmp.scalding/uni2.out")

  data
    .slice(Over(Second), List("fid:A", "fid:B", "fid:Y", "fid:Z"), true)
    .slice(Over(First), List("iid:0221707", "iid:0364354"), true)
    .squash(Third, PreservingMaxPosition())
    .saveAsCSV(Over(Second), "./tmp.scalding/test.csv")
    .saveAsCSV(Over(First), "./tmp.scalding/tset.csv", writeHeader=false, separator=",")

  data
    .slice(Over(Second), List("fid:A", "fid:B", "fid:Y", "fid:Z"), true)
    .slice(Over(First), List("iid:0221707", "iid:0364354"), true)
    .squash(Third, PreservingMaxPosition())
    .permute(Second, First)
    .save("./tmp.scalding/trs1.out", descriptive=true)

  data
    .slice(Over(Second), List("fid:A", "fid:B", "fid:Y", "fid:Z"), true)
    .slice(Over(First), List("iid:0221707", "iid:0364354"), true)
    .squash(Third, PreservingMaxPosition())
    .save("./tmp.scalding/data.txt")
}

class TestScalding9(args : Args) extends Job(args) {

  val data = TestScaldingReader.load4TupleDataAddDate(args("path") + "/someInputfile3.txt")

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
    .save("./tmp.scalding/prt1.out", descriptive=true)

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
    .save("./tmp.scalding/prt2.out", descriptive=true)

  prt1
    .get("training")
    .save("./tmp.scalding/train.out", descriptive=true)

  prt1
    .get("testing")
    .save("./tmp.scalding/test.out", descriptive=true)

  prt1
    .get("scoring")
    .save("./tmp.scalding/score.out", descriptive=true)
}

class TestScalding10(args : Args) extends Job(args) {

  val data = TestScaldingReader.load4TupleDataAddDate(args("path") + "/someInputfile3.txt")

  data
    .summariseAndExpand(Over(Second), Mean("mean", strict=true, nan=true))
    .saveAsCSV(Over(Second), "./tmp.scalding/agg1.csv")

  data
    .slice(Over(First), List("iid:0064402", "iid:0066848", "iid:0076357", "iid:0216406", "iid:0221707", "iid:0262443",
                             "iid:0364354", "iid:0375226", "iid:0444510", "iid:1004305"), true)
    .squash(Third, PreservingMaxPosition())
    .summariseAndExpand(Along(Second), Count("count"))
    .saveAsCSV(Over(Second), "./tmp.scalding/agg2.csv")

  data
    .slice(Over(First), List("iid:0064402", "iid:0066848", "iid:0076357", "iid:0216406", "iid:0221707", "iid:0262443",
                             "iid:0364354", "iid:0375226", "iid:0444510", "iid:1004305"), true)
    .squash(Third, PreservingMaxPosition())
    .summariseAndExpand(Along(First), List(Count("count"), Moments("mean", "sd", "skewness", "kurtosis"), Min("min"),
      Max("max"), MaxAbs("max.abs")))
    .saveAsCSV(Over(Second), "./tmp.scalding/agg3.csv")
}

class TestScalding11(args : Args) extends Job(args) {

  val data = TestScaldingReader.load4TupleDataAddDate(args("path") + "/someInputfile3.txt")

  data
    .slice(Over(Second), List("fid:A", "fid:B", "fid:Y", "fid:Z"), true)
    .slice(Over(First), List("iid:0221707", "iid:0364354"), true)
    .transform(Indicator(Second, name="%1$s.ind"))
    .save("./tmp.scalding/trn2.out", descriptive=true)

  data
    .slice(Over(Second), List("fid:A", "fid:B", "fid:Y", "fid:Z"), true)
    .slice(Over(First), List("iid:0221707", "iid:0364354"), true)
    .squash(Third, PreservingMaxPosition())
    .transform(Binarise(Second))
    .saveAsCSV(Over(Second), "./tmp.scalding/trn3.out")
}

class TestScalding12(args : Args) extends Job(args) {

  val data = TestScaldingReader.load4TupleDataAddDate(args("path") + "/someInputfile3.txt")
    .slice(Over(Second), List("fid:A", "fid:B", "fid:Y", "fid:Z"), true)
    .slice(Over(First), List("iid:0221707", "iid:0364354"), true)

  data
    .squash(Third, PreservingMaxPosition())
    .fill(Content(ContinuousSchema[Codex.LongCodex](), 0))
    .saveAsCSV(Over(Second), "./tmp.scalding/fll1.out")

  data
    .fill(Content(ContinuousSchema[Codex.LongCodex](), 0))
    .save("./tmp.scalding/fll3.out", descriptive=true)
}

class TestScalding13(args : Args) extends Job(args) {

  val all = TestScaldingReader.load4TupleDataAddDate(args("path") + "/someInputfile3.txt")
  val data = all
    .slice(Over(First), List("iid:0064402", "iid:0066848", "iid:0076357", "iid:0216406", "iid:0221707", "iid:0262443",
                             "iid:0364354", "iid:0375226", "iid:0444510", "iid:1004305"), true)
    .slice(Over(Second), List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"), true)
    .squash(Third, PreservingMaxPosition())

  val inds = data
    .transform(Indicator(Second, name="%1$s.ind"))
    .fill(Content(ContinuousSchema[Codex.LongCodex](), 0))

  data
    .join(Over(First), inds)
    .fill(Content(ContinuousSchema[Codex.LongCodex](), 0))
    .saveAsCSV(Over(Second), "./tmp.scalding/fll2.out")

  data
    .fill(Over(Second), all.summarise(Over(Second), Mean(strict=true, nan=true)))
    .join(Over(First), inds)
    .saveAsCSV(Over(Second), "./tmp.scalding/fll4.out")
}

class TestScalding14(args : Args) extends Job(args) {

  val data = TestScaldingReader.load4TupleDataAddDate(args("path") + "/someInputfile3.txt")
    .slice(Over(Second), List("fid:A", "fid:B", "fid:Y", "fid:Z"), true)
    .slice(Over(First), List("iid:0221707", "iid:0364354"), true)

  data
    .change(Over(Second), "fid:A", NominalSchema[Codex.LongCodex]())
    .save("./tmp.scalding/chg1.out", descriptive=true)
}

class TestScalding15(args : Args) extends Job(args) {

  val data = TestScaldingReader.load4TupleDataAddDate(args("path") + "/someInputfile3.txt")

  data
    .slice(Over(Second), List("fid:A", "fid:C", "fid:E", "fid:G"), true)
    .slice(Over(First), List("iid:0221707", "iid:0364354"), true)
    .summariseAndExpand(Along(Third), Sum("sum"))
    .melt(Third, Second)
    .saveAsCSV(Over(Second), "./tmp.scalding/rsh1.out")

  val inds = data
    .slice(Over(First), List("iid:0064402", "iid:0066848", "iid:0076357", "iid:0216406", "iid:0221707", "iid:0262443",
                             "iid:0364354", "iid:0375226", "iid:0444510", "iid:1004305"), true)
    .slice(Over(Second), List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"), true)
    .squash(Third, PreservingMaxPosition())
    .transform(Indicator(Second, name="%1$s.ind"))
    .saveAsCSV(Over(Second), "./tmp.scalding/trn1.csv")

  data
    .slice(Over(First), List("iid:0064402", "iid:0066848", "iid:0076357", "iid:0216406", "iid:0221707", "iid:0262443",
                             "iid:0364354", "iid:0375226", "iid:0444510", "iid:1004305"), true)
    .slice(Over(Second), List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"), true)
    .squash(Third, PreservingMaxPosition())
    .join(Over(First), inds)
    .saveAsCSV(Over(Second), "./tmp.scalding/jn1.csv")
}

class TestScalding16(args : Args) extends Job(args) {

  val data = TestScaldingReader.load4TupleDataAddDate(args("path") + "/someInputfile3.txt")

  case class HashSample() extends Sampler with Select {
    def select[P <: Position](cell: Cell[P]): Boolean = (cell.position(First).toString.hashCode % 25) == 0
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
    .squash(Third, PreservingMaxPosition())

  val stats = data
    .summariseAndExpand(Along(First), List(Count("count"), Mean("mean"), Min("min"), Max("max"), MaxAbs("max.abs")))
    .toMap(Over(First))

  data
    .transformWithValue(Normalise(Second, key="max.abs"), stats)
    .saveAsCSV(Over(Second), "./tmp.scalding/trn6.csv")

  case class Sample500() extends Sampler with Select {
    def select[P <: Position](cell: Cell[P]): Boolean = cell.content.value gtr 500
  }

  data
    .sample(Sample500())
    .saveAsCSV(Over(Second), "./tmp.scalding/flt1.csv")

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
    .saveAsCSV(Over(Second), "./tmp.scalding/flt2.csv")
}

class TestScalding18(args : Args) extends Job(args) {

  val data = TestScaldingReader.load4TupleDataAddDate(args("path") + "/someInputfile3.txt")
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
    .saveAsCSV(Over(Second), "./tmp.scalding/flt3.csv")
}

class TestScalding19(args : Args) extends Job(args) {

  val raw = TestScaldingReader.load4TupleDataAddDate(args("path") + "/someInputfile3.txt")
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

  def cb(key: String, pipe: TypedPipe[Cell[Position2D]]): TypedPipe[Cell[Position2D]] = {
    pipe
      .slice(Over(Second), rem, false)
      .transformWithValue(List(Indicator(Second, name="%1$s.ind"), Binarise(Second), Normalise(Second, key="max.abs")),
        stats.toMap(Over(First)))
      .fill(Content(ContinuousSchema[Codex.LongCodex](), 0))
      .saveAsCSV(Over(Second), "./tmp.scalding/pln_" + key + ".csv")
  }

  parts
    .forEach(List("train", "test"), cb)
}

class TestScalding20(args : Args) extends Job(args) {

  load3DWithDictionary(args("path") + "/ivoryInputfile1.txt", Dictionary.load(args("path") + "/dict.txt"))
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

  val data = load2D(args("path") + "/numericInputfile.txt")

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
    .save("./tmp.scalding/dif1.out")

  data
    .window(Over(Second), Diff())
    .permute(Second, First)
    .save("./tmp.scalding/dif2.out")
}

class TestScalding23(args : Args) extends Job(args) {

  val data = load2D(args("path") + "/somePairwise.txt")

  case class DiffSquared() extends Operator with Compute {
    def compute[P <: Position, D <: Dimension](slice: Slice[P, D])(left: Cell[slice.S], right: Cell[slice.S],
      rem: slice.R): Collection[Cell[slice.R#M]] = {
      val xc = left.position.toShortString("")
      val yc = right.position.toShortString("")

      (xc < yc && xc != yc) match {
        case true => Collection(rem.append("(" + xc + "-" + yc + ")^2"), Content(ContinuousSchema[Codex.DoubleCodex](),
          math.pow(left.content.value.asLong.get - right.content.value.asLong.get, 2)))
        case false => Collection[Cell[slice.R#M]]
      }
    }
  }

  data
    .pairwise(Over(Second), DiffSquared())
    .save("./tmp.scalding/pws1.out")
}

class TestScalding24(args: Args) extends Job(args) {

  // see http://www.mathsisfun.com/data/correlation.html for data

  val schema = List(("day", NominalSchema[Codex.StringCodex]()),
                    ("temperature", ContinuousSchema[Codex.DoubleCodex]()),
                    ("sales", DiscreteSchema[Codex.LongCodex]()))
  val data = loadTable(args("path") + "/somePairwise2.txt", schema, separator="|")

  data
    .correlation(Over(Second))
    .save("./tmp.scalding/pws2.out")

  val schema2 = List(("day", NominalSchema[Codex.StringCodex]()),
                     ("temperature", ContinuousSchema[Codex.DoubleCodex]()),
                     ("sales", DiscreteSchema[Codex.LongCodex]()),
                     ("neg.sales", DiscreteSchema[Codex.LongCodex]()))
  val data2 = loadTable(args("path") + "/somePairwise3.txt", schema2, separator="|")

  data2
    .correlation(Over(Second))
    .save("./tmp.scalding/pws3.out")
}

class TestScalding25(args: Args) extends Job(args) {

  load2D(args("path") + "/mutualInputfile.txt")
    .mutualInformation(Over(Second))
    .save("./tmp.scalding/mi.out")
}

class TestScalding26(args: Args) extends Job(args) {

  val left = load2D(args("path") + "/algebraInputfile1.txt")
  val right = load2D(args("path") + "/algebraInputfile2.txt")

  left
    .pairwiseBetween(Over(First), right, Times(comparer=All))
    .save("./tmp.scalding/alg.out")
}

class TestScalding27(args: Args) extends Job(args) {

  // http://www.statisticshowto.com/moving-average/
  load2D(args("path") + "/simMovAvgInputfile.txt", first=LongCodex)
    .window(Over(Second), SimpleMovingAverage(5))
    .save("./tmp.scalding/sma1.out")

  load2D(args("path") + "/simMovAvgInputfile.txt", first=LongCodex)
    .window(Over(Second), SimpleMovingAverage(5, all=true))
    .save("./tmp.scalding/sma2.out")

  load2D(args("path") + "/simMovAvgInputfile.txt", first=LongCodex)
    .window(Over(Second), CenteredMovingAverage(2))
    .save("./tmp.scalding/tma.out")

  load2D(args("path") + "/simMovAvgInputfile.txt", first=LongCodex)
    .window(Over(Second), WeightedMovingAverage(5))
    .save("./tmp.scalding/wma1.out")

  load2D(args("path") + "/simMovAvgInputfile.txt", first=LongCodex)
    .window(Over(Second), WeightedMovingAverage(5, all=true))
    .save("./tmp.scalding/wma2.out")

  // http://stackoverflow.com/questions/11074665/how-to-calculate-the-cumulative-average-for-some-numbers
  load1D(args("path") + "/cumMovAvgInputfile.txt")
    .window(Along(First), CumulativeMovingAverage())
    .save("./tmp.scalding/cma.out")

  // http://www.incrediblecharts.com/indicators/exponential_moving_average.php
  load1D(args("path") + "/expMovAvgInputfile.txt")
    .window(Along(First), ExponentialMovingAverage(0.33))
    .save("./tmp.scalding/ema.out")
}

class TestScalding28(args: Args) extends Job(args) {

  val data = List
    .range(0, 16)
    .flatMap { case i => List(("iid:" + i, "fid:A", Content(ContinuousSchema[Codex.LongCodex](), i)),
                              ("iid:" + i, "fid:B", Content(NominalSchema[Codex.StringCodex](), i.toString))) }

  val stats = data
    .summariseAndExpand(Along(First), List(Count("count"), Min("min"), Max("max"), Moments("mean", "sd", "skewness")))
    .toMap(Over(First))

  data
    .transformWithValue(Cut(Second), CutRules.fixed(stats, "min", "max", 4))
    .save("./tmp.scalding/cut1.out")

  data
    .transformWithValue(Cut(Second, "%s.square"), CutRules.squareRootChoice(stats, "count", "min", "max"))
    .save("./tmp.scalding/cut2.out")

  data
    .transformWithValue(Cut(Second, "%s.sturges"), CutRules.sturgesFormula(stats, "count", "min", "max"))
    .save("./tmp.scalding/cut3.out")

  data
    .transformWithValue(Cut(Second, "%s.rice"), CutRules.riceRule(stats, "count", "min", "max"))
    .save("./tmp.scalding/cut4.out")

  data
    .transformWithValue(Cut(Second, "%s.doane"),
      CutRules.doanesFormula(stats, "count", "min", "max", "skewness"))
    .save("./tmp.scalding/cut5.out")

  data
    .transformWithValue(Cut(Second, "%s.scott"),
      CutRules.scottsNormalReferenceRule(stats, "count", "min", "max", "sd"))
    .save("./tmp.scalding/cut6.out")

  data
    .transformWithValue(Cut(Second, "%s.break"), CutRules.breaks(Map("fid:A" -> List(-1, 4, 8, 12, 16))))
    .save("./tmp.scalding/cut7.out")
}

class TestScalding29(args: Args) extends Job(args) {

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
    .save("./tmp.scalding/gini.out")

  data
    .map { case (a, b, c) => (b, a, c) }
    .gini(Along(First))
    .save("./tmp.scalding/inig.out")
}

class TestScalding30(args: Args) extends Job(args) {

  val schema = DiscreteSchema[Codex.LongCodex]()
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

