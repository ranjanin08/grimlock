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
import au.com.cba.omnia.grimlock.framework.derive._
import au.com.cba.omnia.grimlock.framework.encoding._
import au.com.cba.omnia.grimlock.framework.pairwise._
import au.com.cba.omnia.grimlock.framework.partition._
import au.com.cba.omnia.grimlock.framework.position._
import au.com.cba.omnia.grimlock.framework.sample._
import au.com.cba.omnia.grimlock.framework.Type._
import au.com.cba.omnia.grimlock.framework.utility._

import au.com.cba.omnia.grimlock.library.derive._
import au.com.cba.omnia.grimlock.library.pairwise._
import au.com.cba.omnia.grimlock.library.partition._
import au.com.cba.omnia.grimlock.library.reduce._
import au.com.cba.omnia.grimlock.library.squash._
import au.com.cba.omnia.grimlock.library.transform._

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
import com.twitter.scalding.typed.{ IterablePipe, TypedPipe }

object TestReader {
  def read4TupleDataAddDate(file: String)(implicit flow: FlowDef, mode: Mode): TypedPipe[Cell[Position3D]] = {
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

class Test1(args : Args) extends Job(args) {

  val data = TestReader.read4TupleDataAddDate(args("input"))

  data
    .persist("./tmp/dat1.out", descriptive=true)

  data
    .set(Position3D("iid:1548763", "fid:Y", DateCodex.decode("2014-04-26").get),
      Content(ContinuousSchema[Codex.LongCodex](), 1234))
    .slice(Over(First), "iid:1548763", true)
    .persist("./tmp/dat2.out", descriptive=true)

  read3D("smallInputfile.txt")
    .persist("./tmp/dat3.out", descriptive=true)
}

class Test2(args : Args) extends Job(args) {

  val data = TestReader.read4TupleDataAddDate(args("input"))

  (data.names(Over(First)) ++ data.names(Over(Second)) ++ data.names(Over(Third)))
    .groupAll
    .values
    .renumber
    .persist("./tmp/nm0.out", descriptive=true)

  data
    .names(Over(Second))
    .moveToFront("fid:Z")
    .persist("./tmp/nm1.out", descriptive=true)

  data
    .names(Over(Second))
    .slice("fid:M", false)
    .persist("./tmp/nm2.out", descriptive=true)

  data
    .names(Over(Second))
    .set(Map("fid:A" -> 100L, "fid:C" -> 200L))
    .persist("./tmp/nm3.out", descriptive=true)

  data
    .names(Over(Second))
    .moveToBack("fid:B")
    .persist("./tmp/nm4.out", descriptive=true)

  data
    .names(Over(Second))
    .slice(""".*[BCD]$""".r, true, "")
    .persist("./tmp/nm5.out", descriptive=true)
}

class Test3(args : Args) extends Job(args) {

  val data = TestReader.read4TupleDataAddDate(args("input"))

  (data.types(Over(First)) ++ data.types(Over(Second)) ++ data.types(Over(Third)))
    .persist("./tmp/typ1.out", descriptive=true)

  (data.types(Over(First), true) ++ data.types(Over(Second), true) ++ data.types(Over(Third), true))
    .persist("./tmp/typ2.out", descriptive=true)
}

class Test4(args : Args) extends Job(args) {

  val data = TestReader.read4TupleDataAddDate(args("input"))

  data
    .slice(Over(Second), "fid:B", true)
    .persist("./tmp/scl0.out", descriptive=true)

  data
    .slice(Over(Second), List("fid:A", "fid:B"), true)
    .slice(Over(First), "iid:0221707", true)
    .persist("./tmp/scl1.out", descriptive=true)

  val rem = List("fid:B", "fid:D", "fid:F", "fid:H", "fid:J", "fid:L", "fid:N",
                 "fid:P", "fid:R", "fid:T", "fid:V", "fid:X", "fid:Z")
  data
    .slice(Over(Second), data.names(Over(Second)).slice(rem, false), false)
    .persist("./tmp/scl2.out", descriptive=true)
}

class Test5(args : Args) extends Job(args) {

  val data = TestReader.read4TupleDataAddDate(args("input"))

  data
    .slice(Over(Second), List("fid:A", "fid:B"), true)
    .slice(Over(First), "iid:0221707", true)
    .squash(Third, PreservingMaxPosition())
    .persist("./tmp/sqs1.out", descriptive=true)

  data
    .squash(Third, PreservingMaxPosition())
    .persist("./tmp/sqs2.out", descriptive=true)

  data
    .slice(Over(First), List("iid:0064402", "iid:0066848", "iid:0076357", "iid:0216406", "iid:0221707", "iid:0262443",
                             "iid:0364354", "iid:0375226", "iid:0444510", "iid:1004305"), true)
    .squash(Third, PreservingMaxPosition())
    .persistAsCSV(Over(First), "./tmp/sqs3.out")

  data
    .slice(Over(First), List("iid:0064402", "iid:0066848", "iid:0076357", "iid:0216406", "iid:0221707", "iid:0262443",
                             "iid:0364354", "iid:0375226", "iid:0444510", "iid:1004305"), true)
    .slice(Over(Second), List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"), true)
    .squash(Third, PreservingMaxPosition())
    .persistAsCSV(Over(First), "./tmp/sqs4.out")
}

class Test6(args : Args) extends Job(args) {

  val data = TestReader.read4TupleDataAddDate(args("input"))

  data
    .which((c: Cell[Position3D]) => c.content.schema.kind.isSpecialisationOf(Numerical))
    .persist("./tmp/whc1.out", descriptive=true)

  data
    .which((c: Cell[Position3D]) => ! c.content.value.isInstanceOf[StringValue])
    .persist("./tmp/whc2.out", descriptive=true)

  data
    .get(data.which((c: Cell[Position3D]) =>
      (c.content.value equ 666) || (c.content.value leq 11.0) || (c.content.value equ "KQUPKFEH")))
    .persist("./tmp/whc3.out", descriptive=true)

  data
    .which((c: Cell[Position3D]) => c.content.value.isInstanceOf[LongValue])
    .persist("./tmp/whc4.out", descriptive=true)

  TestReader.read4TupleDataAddDate(args("input"))
    .slice(Over(First), List("iid:0064402", "iid:0066848", "iid:0076357", "iid:0216406", "iid:0221707", "iid:0262443",
                             "iid:0364354", "iid:0375226", "iid:0444510", "iid:1004305"), true)
    .slice(Over(Second), List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"), true)
    .squash(Third, PreservingMaxPosition())
    .reduceAndExpand(Along(First), List(Count("count"), Mean("mean"), Min("min"), Max("max"), MaxAbs("max.abs")))
    .which(Over(Second), List(("count", (c: Cell[Position2D]) => c.content.value leq 2),
                              ("min", (c: Cell[Position2D]) => c.content.value equ 107)))
    .persist("./tmp/whc5.out", descriptive=true)
}

class Test7(args : Args) extends Job(args) {

  val data = TestReader.read4TupleDataAddDate(args("input"))

  data
    .get(Position3D("iid:1548763", "fid:Y", DateCodex.decode("2014-04-26").get))
    .persist("./tmp/get1.out", descriptive=true)

  data
    .get(List(Position3D("iid:1548763", "fid:Y", DateCodex.decode("2014-04-26").get),
              Position3D("iid:1303823", "fid:A", DateCodex.decode("2014-05-05").get)))
    .persist("./tmp/get2.out", descriptive=true)
}

class Test8(args : Args) extends Job(args) {

  val data = TestReader.read4TupleDataAddDate(args("input"))

  data
    .slice(Over(Second), "fid:B", true)
    .squash(Third, PreservingMaxPosition())
    .unique
    .persist("./tmp/uniq.out", descriptive=true)

  read2D("mutualInputfile.txt")
    .unique(Over(Second))
    .persist("./tmp/uni2.out")

  data
    .slice(Over(Second), List("fid:A", "fid:B", "fid:Y", "fid:Z"), true)
    .slice(Over(First), List("iid:0221707", "iid:0364354"), true)
    .squash(Third, PreservingMaxPosition())
    .persistAsCSV(Over(Second), "./tmp/test.csv")
    .persistAsCSV(Over(First), "./tmp/tset.csv", writeHeader=false, separator=",")

  data
    .slice(Over(Second), List("fid:A", "fid:B", "fid:Y", "fid:Z"), true)
    .slice(Over(First), List("iid:0221707", "iid:0364354"), true)
    .squash(Third, PreservingMaxPosition())
    .permute(Second, First)
    .persist("./tmp/trs1.out", descriptive=true)

  data
    .slice(Over(Second), List("fid:A", "fid:B", "fid:Y", "fid:Z"), true)
    .slice(Over(First), List("iid:0221707", "iid:0364354"), true)
    .squash(Third, PreservingMaxPosition())
    .persist("./tmp/data.txt")
}

class Test9(args : Args) extends Job(args) {

  val data = TestReader.read4TupleDataAddDate(args("input"))

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
    .persist("./tmp/prt1.out", descriptive=true)

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
    .persist("./tmp/prt2.out", descriptive=true)

  prt1
    .get("training")
    .persist("./tmp/train.out", descriptive=true)

  prt1
    .get("testing")
    .persist("./tmp/test.out", descriptive=true)

  prt1
    .get("scoring")
    .persist("./tmp/score.out", descriptive=true)
}

class Test10(args : Args) extends Job(args) {

  val data = TestReader.read4TupleDataAddDate(args("input"))

  data
    .reduceAndExpand(Over(Second), Mean("mean", strict=true, nan=true))
    .persistAsCSV(Over(Second), "./tmp/agg1.csv")

  data
    .slice(Over(First), List("iid:0064402", "iid:0066848", "iid:0076357", "iid:0216406", "iid:0221707", "iid:0262443",
                             "iid:0364354", "iid:0375226", "iid:0444510", "iid:1004305"), true)
    .squash(Third, PreservingMaxPosition())
    .reduceAndExpand(Along(Second), Count("count"))
    .persistAsCSV(Over(Second), "./tmp/agg2.csv")

  data
    .slice(Over(First), List("iid:0064402", "iid:0066848", "iid:0076357", "iid:0216406", "iid:0221707", "iid:0262443",
                             "iid:0364354", "iid:0375226", "iid:0444510", "iid:1004305"), true)
    .squash(Third, PreservingMaxPosition())
    .reduceAndExpand(Along(First), List(Count("count"), Moments("mean", "sd", "skewness", "kurtosis"), Min("min"),
      Max("max"), MaxAbs("max.abs")))
    .persistAsCSV(Over(Second), "./tmp/agg3.csv")
}

class Test11(args : Args) extends Job(args) {

  val data = TestReader.read4TupleDataAddDate(args("input"))

  data
    .slice(Over(Second), List("fid:A", "fid:B", "fid:Y", "fid:Z"), true)
    .slice(Over(First), List("iid:0221707", "iid:0364354"), true)
    .transform(Indicator(Second, name="%1$s.ind"))
    .persist("./tmp/trn2.out", descriptive=true)

  data
    .slice(Over(Second), List("fid:A", "fid:B", "fid:Y", "fid:Z"), true)
    .slice(Over(First), List("iid:0221707", "iid:0364354"), true)
    .squash(Third, PreservingMaxPosition())
    .transform(Binarise(Second))
    .persistAsCSV(Over(Second), "./tmp/trn3.out")
}

class Test12(args : Args) extends Job(args) {

  val data = TestReader.read4TupleDataAddDate(args("input"))
    .slice(Over(Second), List("fid:A", "fid:B", "fid:Y", "fid:Z"), true)
    .slice(Over(First), List("iid:0221707", "iid:0364354"), true)

  data
    .squash(Third, PreservingMaxPosition())
    .fillHomogenous(Content(ContinuousSchema[Codex.LongCodex](), 0))
    .persistAsCSV(Over(Second), "./tmp/fll1.out")

  data
    .fillHomogenous(Content(ContinuousSchema[Codex.LongCodex](), 0))
    .persist("./tmp/fll3.out", descriptive=true)
}

class Test13(args : Args) extends Job(args) {

  val all = TestReader.read4TupleDataAddDate(args("input"))
  val data = all
    .slice(Over(First), List("iid:0064402", "iid:0066848", "iid:0076357", "iid:0216406", "iid:0221707", "iid:0262443",
                             "iid:0364354", "iid:0375226", "iid:0444510", "iid:1004305"), true)
    .slice(Over(Second), List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"), true)
    .squash(Third, PreservingMaxPosition())

  val inds = data
    .transform(Indicator(Second, name="%1$s.ind"))
    .fillHomogenous(Content(ContinuousSchema[Codex.LongCodex](), 0))

  data
    .join(Over(First), inds)
    .fillHomogenous(Content(ContinuousSchema[Codex.LongCodex](), 0))
    .persistAsCSV(Over(Second), "./tmp/fll2.out")

  data
    .fillHetrogenous(Over(Second), all.reduce(Over(Second), Mean(strict=true, nan=true)))
    .join(Over(First), inds)
    .persistAsCSV(Over(Second), "./tmp/fll4.out")
}

class Test14(args : Args) extends Job(args) {

  val data = TestReader.read4TupleDataAddDate(args("input"))
    .slice(Over(Second), List("fid:A", "fid:B", "fid:Y", "fid:Z"), true)
    .slice(Over(First), List("iid:0221707", "iid:0364354"), true)

  data
    .change(Over(Second), "fid:A", NominalSchema[Codex.LongCodex]())
    .persist("./tmp/chg1.out", descriptive=true)
}

class Test15(args : Args) extends Job(args) {

  val data = TestReader.read4TupleDataAddDate(args("input"))

  data
    .slice(Over(Second), List("fid:A", "fid:C", "fid:E", "fid:G"), true)
    .slice(Over(First), List("iid:0221707", "iid:0364354"), true)
    .reduceAndExpand(Along(Third), Sum("sum"))
    .melt(Third, Second)
    .persistAsCSV(Over(Second), "./tmp/rsh1.out")

  val inds = data
    .slice(Over(First), List("iid:0064402", "iid:0066848", "iid:0076357", "iid:0216406", "iid:0221707", "iid:0262443",
                             "iid:0364354", "iid:0375226", "iid:0444510", "iid:1004305"), true)
    .slice(Over(Second), List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"), true)
    .squash(Third, PreservingMaxPosition())
    .transform(Indicator(Second, name="%1$s.ind"))
    .persistAsCSV(Over(Second), "./tmp/trn1.csv")

  data
    .slice(Over(First), List("iid:0064402", "iid:0066848", "iid:0076357", "iid:0216406", "iid:0221707", "iid:0262443",
                             "iid:0364354", "iid:0375226", "iid:0444510", "iid:1004305"), true)
    .slice(Over(Second), List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"), true)
    .squash(Third, PreservingMaxPosition())
    .join(Over(First), inds)
    .persistAsCSV(Over(Second), "./tmp/jn1.csv")
}

class Test16(args : Args) extends Job(args) {

  val data = TestReader.read4TupleDataAddDate(args("input"))

  case class HashSample() extends Sampler with Select {
    def select[P <: Position](pos: P): Boolean = (pos(First).toString.hashCode % 25) == 0
  }

  data
    .sample(HashSample())
    .persist("./tmp/smp1.out")
}

class Test17(args : Args) extends Job(args) {

  val data = TestReader.read4TupleDataAddDate(args("input"))
    .slice(Over(First), List("iid:0064402", "iid:0066848", "iid:0076357", "iid:0216406", "iid:0221707", "iid:0262443",
                             "iid:0364354", "iid:0375226", "iid:0444510", "iid:1004305"), true)
    .slice(Over(Second), List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"), true)
    .squash(Third, PreservingMaxPosition())

  val stats = data
    .reduceAndExpand(Along(First), List(Count("count"), Mean("mean"), Min("min"), Max("max"), MaxAbs("max.abs")))
    .toMap(Over(First))

  data
    .transformWithValue(Normalise(Second, key="max.abs"), stats)
    .persistAsCSV(Over(Second), "./tmp/trn6.csv")

  data
    .refine((c: Cell[Position2D]) => c.content.value gtr 500)
    .persistAsCSV(Over(Second), "./tmp/flt1.csv")

  def removeGreaterThanMean(c: Cell[Position2D], ext: Map[Position1D, Map[Position1D, Content]]): Boolean = {
    if (c.content.schema.kind.isSpecialisationOf(Numerical)) {
      c.content.value leq ext(Position1D(c.position(Second)))(Position1D("mean")).value
    } else {
      true
    }
  }

  data
    .refineWithValue(removeGreaterThanMean, stats)
    .persistAsCSV(Over(Second), "./tmp/flt2.csv")
}

class Test18(args : Args) extends Job(args) {

  val data = TestReader.read4TupleDataAddDate(args("input"))
    .slice(Over(First), List("iid:0064402", "iid:0066848", "iid:0076357", "iid:0216406", "iid:0221707", "iid:0262443",
                             "iid:0364354", "iid:0375226", "iid:0444510", "iid:1004305"), true)
    .slice(Over(Second), List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"), true)
    .squash(Third, PreservingMaxPosition())

  val stats = data
    .reduceAndExpand(Along(First), List(Count("count"), Mean("mean"), Min("min"), Max("max"), MaxAbs("max.abs")))

  val rem = stats
    .which(Over(Second), "count", (c: Cell[Position2D]) => c.content.value leq 2)
    .names(Over(First))

  data
    .slice(Over(Second), rem, false)
    .persistAsCSV(Over(Second), "./tmp/flt3.csv")
}

class Test19(args : Args) extends Job(args) {

  val raw = TestReader.read4TupleDataAddDate(args("input"))
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
    .reduceAndExpand(Along(First), List(Count("count"), MaxAbs("max.abs")))

  val rem = stats
    .which((c: Cell[Position2D]) => (c.position(Second) equ "count") && (c.content.value leq 2))
    .names(Over(First))

  def cb(key: String, pipe: TypedPipe[Cell[Position2D]]): TypedPipe[Cell[Position2D]] = {
    pipe
      .slice(Over(Second), rem, false)
      .transformWithValue(List(Indicator(Second, name="%1$s.ind"), Binarise(Second), Normalise(Second, key="max.abs")),
        stats.toMap(Over(First)))
      .fillHomogenous(Content(ContinuousSchema[Codex.LongCodex](), 0))
      .persistAsCSV(Over(Second), "./tmp/pln_" + key + ".csv")
  }

  parts
    .forEach(List("train", "test"), cb)
}

class Test20(args : Args) extends Job(args) {

  read3DWithDictionary("./ivoryInputfile1.txt", Dictionary.read("./dict.txt"))
    .persist("./tmp/ivr1.out")
}

class Test21(args : Args) extends Job(args) {

  val data = TestReader.read4TupleDataAddDate(args("input"))

  data
    .shape()
    .persist("./tmp/siz0.out")

  data
    .size(First)
    .persist("./tmp/siz1.out")

  data
    .size(Second)
    .persist("./tmp/siz2.out")

  data
    .size(Third)
    .persist("./tmp/siz3.out")
}

class Test22(args : Args) extends Job(args) {

  val data = read2D("numericInputfile.txt")

  case class Diff() extends Deriver with Initialise {
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
    .derive(Over(First), Diff())
    .persist("./tmp/dif1.out")

  data
    .derive(Over(Second), Diff())
    .permute(Second, First)
    .persist("./tmp/dif2.out")
}

class Test23(args : Args) extends Job(args) {

  val data = read2D("somePairwise.txt")

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
    .persist("./tmp/pws1.out")
}

class Test24(args: Args) extends Job(args) {

  // see http://www.mathsisfun.com/data/correlation.html for data

  val schema = List(("day", NominalSchema[Codex.StringCodex]()),
                    ("temperature", ContinuousSchema[Codex.DoubleCodex]()),
                    ("sales", DiscreteSchema[Codex.LongCodex]()))
  val data = readTable("somePairwise2.txt", schema, separator="|")

  data
    .correlation(Over(Second))
    .persist("./tmp/pws2.out")

  val schema2 = List(("day", NominalSchema[Codex.StringCodex]()),
                     ("temperature", ContinuousSchema[Codex.DoubleCodex]()),
                     ("sales", DiscreteSchema[Codex.LongCodex]()),
                     ("neg.sales", DiscreteSchema[Codex.LongCodex]()))
  val data2 = readTable("somePairwise3.txt", schema2, separator="|")

  data2
    .correlation(Over(Second))
    .persist("./tmp/pws3.out")
}

class Test25(args: Args) extends Job(args) {

  read2D("mutualInputfile.txt")
    .mutualInformation(Over(Second))
    .persist("./tmp/mi.out")
}

class Test26(args: Args) extends Job(args) {

  val left = read2D("algebraInputfile1.txt")
  val right = read2D("algebraInputfile2.txt")

  left
    .pairwiseBetween(Over(First), right, Times(comparer=All))
    .persist("./tmp/alg.out")
}

class Test27(args: Args) extends Job(args) {

  // http://www.statisticshowto.com/moving-average/
  read2D("simMovAvgInputfile.txt", first=LongCodex)
    .derive(Over(Second), SimpleMovingAverage(5))
    .persist("./tmp/sma1.out")

  read2D("simMovAvgInputfile.txt", first=LongCodex)
    .derive(Over(Second), SimpleMovingAverage(5, all=true))
    .persist("./tmp/sma2.out")

  read2D("simMovAvgInputfile.txt", first=LongCodex)
    .derive(Over(Second), CenteredMovingAverage(2))
    .persist("./tmp/tma.out")

  read2D("simMovAvgInputfile.txt", first=LongCodex)
    .derive(Over(Second), WeightedMovingAverage(5))
    .persist("./tmp/wma1.out")

  read2D("simMovAvgInputfile.txt", first=LongCodex)
    .derive(Over(Second), WeightedMovingAverage(5, all=true))
    .persist("./tmp/wma2.out")

  // http://stackoverflow.com/questions/11074665/how-to-calculate-the-cumulative-average-for-some-numbers
  read1D("cumMovAvgInputfile.txt")
    .derive(Along(First), CumulativeMovingAverage())
    .persist("./tmp/cma.out")

  // http://www.incrediblecharts.com/indicators/exponential_moving_average.php
  read1D("expMovAvgInputfile.txt")
    .derive(Along(First), ExponentialMovingAverage(0.33))
    .persist("./tmp/ema.out")
}

class Test28(args: Args) extends Job(args) {

  val data = List
    .range(0, 16)
    .flatMap { case i => List(("iid:" + i, "fid:A", Content(ContinuousSchema[Codex.LongCodex](), i)),
                              ("iid:" + i, "fid:B", Content(NominalSchema[Codex.StringCodex](), i.toString))) }

  val stats = data
    .reduceAndExpand(Along(First), List(Count("count"), Min("min"), Max("max"), Moments("mean", "sd", "skewness")))
    .toMap(Over(First))

  data
    .transformWithValue(Cut(Second), CutRules.fixed(stats, "min", "max", 4))
    .persist("./tmp/cut1.out")

  data
    .transformWithValue(Cut(Second, "%s.square"), CutRules.squareRootChoice(stats, "count", "min", "max"))
    .persist("./tmp/cut2.out")

  data
    .transformWithValue(Cut(Second, "%s.sturges"), CutRules.sturgesFormula(stats, "count", "min", "max"))
    .persist("./tmp/cut3.out")

  data
    .transformWithValue(Cut(Second, "%s.rice"), CutRules.riceRule(stats, "count", "min", "max"))
    .persist("./tmp/cut4.out")

  data
    .transformWithValue(Cut(Second, "%s.doane"),
      CutRules.doanesFormula(stats, "count", "min", "max", "skewness"))
    .persist("./tmp/cut5.out")

  data
    .transformWithValue(Cut(Second, "%s.scott"),
      CutRules.scottsNormalReferenceRule(stats, "count", "min", "max", "sd"))
    .persist("./tmp/cut6.out")

  data
    .transformWithValue(Cut(Second, "%s.break"), CutRules.breaks(Map("fid:A" -> List(-1, 4, 8, 12, 16))))
    .persist("./tmp/cut7.out")
}

