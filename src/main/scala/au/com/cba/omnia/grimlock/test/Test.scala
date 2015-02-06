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

import au.com.cba.omnia.grimlock._
import au.com.cba.omnia.grimlock.content._
import au.com.cba.omnia.grimlock.content.Contents._
import au.com.cba.omnia.grimlock.content.metadata._
import au.com.cba.omnia.grimlock.derive._
import au.com.cba.omnia.grimlock.encoding._
import au.com.cba.omnia.grimlock.Matrix._
import au.com.cba.omnia.grimlock.Names._
import au.com.cba.omnia.grimlock.pairwise._
import au.com.cba.omnia.grimlock.partition._
import au.com.cba.omnia.grimlock.partition.Partitions._
import au.com.cba.omnia.grimlock.position._
import au.com.cba.omnia.grimlock.position.Positions._
import au.com.cba.omnia.grimlock.reduce._
import au.com.cba.omnia.grimlock.sample._
import au.com.cba.omnia.grimlock.squash._
import au.com.cba.omnia.grimlock.transform._
import au.com.cba.omnia.grimlock.Type._
import au.com.cba.omnia.grimlock.Types._
import au.com.cba.omnia.grimlock.utility.Miscellaneous.Collection

import cascading.flow.FlowDef
import com.twitter.scalding._
import com.twitter.scalding.TDsl._, Dsl._
import com.twitter.scalding.typed.IterablePipe

object TestReader {
  def read4TupleDataAddDate(file: String)(implicit flow: FlowDef, mode: Mode): TypedPipe[(Position3D, Content)] = {
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

          schema.decode(v).map { case c => (Position3D(i, f, hashDate(v)), c) }
      }
  }
}

class Test1(args : Args) extends Job(args) {

  val data = TestReader.read4TupleDataAddDate(args("input"))

  data
    .persistFile("./tmp/dat1.out", descriptive=true)

  data
    .set(Position3D("iid:1548763", "fid:Y", DateCodex.decode("2014-04-26").get),
      Content(ContinuousSchema[Codex.LongCodex](), 1234))
    .slice(Over(First), "iid:1548763", true)
    .persistFile("./tmp/dat2.out", descriptive=true)

  read3DFile("smallInputfile.txt")
    .persistFile("./tmp/dat3.out", descriptive=true)
}

class Test2(args : Args) extends Job(args) {

  val data = TestReader.read4TupleDataAddDate(args("input"))

  (data.names(Over(First)) ++ data.names(Over(Second)) ++ data.names(Over(Third)))
    .groupAll
    .values
    .renumber
    .persistFile("./tmp/nm0.out", descriptive=true)

  data
    .names(Over(Second))
    .moveToFront("fid:Z")
    .persistFile("./tmp/nm1.out", descriptive=true)

  data
    .names(Over(Second))
    .slice("fid:M", false)
    .persistFile("./tmp/nm2.out", descriptive=true)

  data
    .names(Over(Second))
    .set(Map("fid:A" -> 100L, "fid:C" -> 200L))
    .persistFile("./tmp/nm3.out", descriptive=true)

  data
    .names(Over(Second))
    .moveToBack("fid:B")
    .persistFile("./tmp/nm4.out", descriptive=true)

  data
    .names(Over(Second))
    .slice(""".*[BCD]$""".r, true, "")
    .persistFile("./tmp/nm5.out", descriptive=true)
}

class Test3(args : Args) extends Job(args) {

  val data = TestReader.read4TupleDataAddDate(args("input"))

  (data.types(Over(First)) ++ data.types(Over(Second)) ++ data.types(Over(Third)))
    .persistFile("./tmp/typ1.out", descriptive=true)

  (data.types(Over(First), true) ++ data.types(Over(Second), true) ++ data.types(Over(Third), true))
    .persistFile("./tmp/typ2.out", descriptive=true)
}

class Test4(args : Args) extends Job(args) {

  val data = TestReader.read4TupleDataAddDate(args("input"))

  data
    .slice(Over(Second), "fid:B", true)
    .persistFile("./tmp/scl0.out", descriptive=true)

  data
    .slice(Over(Second), List("fid:A", "fid:B"), true)
    .slice(Over(First), "iid:0221707", true)
    .persistFile("./tmp/scl1.out", descriptive=true)

  val rem = List("fid:B", "fid:D", "fid:F", "fid:H", "fid:J", "fid:L", "fid:N",
                 "fid:P", "fid:R", "fid:T", "fid:V", "fid:X", "fid:Z")
  data
    .slice(Over(Second), data.names(Over(Second)).slice(rem, false), false)
    .persistFile("./tmp/scl2.out", descriptive=true)
}

class Test5(args : Args) extends Job(args) {

  val data = TestReader.read4TupleDataAddDate(args("input"))

  data
    .slice(Over(Second), List("fid:A", "fid:B"), true)
    .slice(Over(First), "iid:0221707", true)
    .squash(Third, PreservingMaxPosition())
    .persistFile("./tmp/sqs1.out", descriptive=true)

  data
    .squash(Third, PreservingMaxPosition())
    .persistFile("./tmp/sqs2.out", descriptive=true)

  data
    .slice(Over(First), List("iid:0064402", "iid:0066848", "iid:0076357", "iid:0216406", "iid:0221707", "iid:0262443",
                             "iid:0364354", "iid:0375226", "iid:0444510", "iid:1004305"), true)
    .squash(Third, PreservingMaxPosition())
    .writeCSV(Over(First), "./tmp/sqs3.out")

  data
    .slice(Over(First), List("iid:0064402", "iid:0066848", "iid:0076357", "iid:0216406", "iid:0221707", "iid:0262443",
                             "iid:0364354", "iid:0375226", "iid:0444510", "iid:1004305"), true)
    .slice(Over(Second), List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"), true)
    .squash(Third, PreservingMaxPosition())
    .writeCSV(Over(First), "./tmp/sqs4.out")
}

class Test6(args : Args) extends Job(args) {

  val data = TestReader.read4TupleDataAddDate(args("input"))

  data
    .which((p: Position, c: Content) => c.schema.kind.isSpecialisationOf(Numerical))
    .persistFile("./tmp/whc1.out", descriptive=true)

  data
    .which((p: Position, c: Content) => ! c.value.isInstanceOf[StringValue])
    .persistFile("./tmp/whc2.out", descriptive=true)

  data
    .get(data.which((p: Position, c: Content) => (c.value equ 666) || (c.value leq 11.0) || (c.value equ "KQUPKFEH")))
    .persistFile("./tmp/whc3.out", descriptive=true)

  data
    .which((p: Position, c: Content) => c.value.isInstanceOf[LongValue])
    .persistFile("./tmp/whc4.out", descriptive=true)

  TestReader.read4TupleDataAddDate(args("input"))
    .slice(Over(First), List("iid:0064402", "iid:0066848", "iid:0076357", "iid:0216406", "iid:0221707", "iid:0262443",
                             "iid:0364354", "iid:0375226", "iid:0444510", "iid:1004305"), true)
    .slice(Over(Second), List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"), true)
    .squash(Third, PreservingMaxPosition())
    .reduceAndExpand(Along(First), List(Count("count"), Mean("mean"), Min("min"), Max("max"), MaxAbs("max.abs")))
    .which(Over(Second), List(("count", (pos: Position, con: Content) => con.value leq 2),
                              ("min", (pos: Position, con: Content) => con.value equ 107)))
    .persistFile("./tmp/whc5.out", descriptive=true)
}

class Test7(args : Args) extends Job(args) {

  val data = TestReader.read4TupleDataAddDate(args("input"))

  data
    .get(Position3D("iid:1548763", "fid:Y", DateCodex.decode("2014-04-26").get))
    .persistFile("./tmp/get1.out", descriptive=true)

  data
    .get(List(Position3D("iid:1548763", "fid:Y", DateCodex.decode("2014-04-26").get),
              Position3D("iid:1303823", "fid:A", DateCodex.decode("2014-05-05").get)))
    .persistFile("./tmp/get2.out", descriptive=true)
}

class Test8(args : Args) extends Job(args) {

  val data = TestReader.read4TupleDataAddDate(args("input"))

  data
    .slice(Over(Second), "fid:B", true)
    .squash(Third, PreservingMaxPosition())
    .unique
    .persistFile("./tmp/uniq.out", descriptive=true)

  read2DFile("mutualInputfile.txt")
    .unique(Over(Second))
    .persistFile("./tmp/uni2.out")

  data
    .slice(Over(Second), List("fid:A", "fid:B", "fid:Y", "fid:Z"), true)
    .slice(Over(First), List("iid:0221707", "iid:0364354"), true)
    .squash(Third, PreservingMaxPosition())
    .writeCSV(Over(Second), "./tmp/test.csv")
    .writeCSV(Over(First), "./tmp/tset.csv", writeHeader=false, separator=",")

  data
    .slice(Over(Second), List("fid:A", "fid:B", "fid:Y", "fid:Z"), true)
    .slice(Over(First), List("iid:0221707", "iid:0364354"), true)
    .squash(Third, PreservingMaxPosition())
    .permute(Second, First)
    .persistFile("./tmp/trs1.out", descriptive=true)

  data
    .slice(Over(Second), List("fid:A", "fid:B", "fid:Y", "fid:Z"), true)
    .slice(Over(First), List("iid:0221707", "iid:0364354"), true)
    .squash(Third, PreservingMaxPosition())
    .persistFile("./tmp/data.txt")
}

class Test9(args : Args) extends Job(args) {

  val data = TestReader.read4TupleDataAddDate(args("input"))

  case class StringPartitioner(dim: Dimension) extends Partitioner with Assign {
    type T = String

    def assign[P <: Position](pos: P): Collection[T] = {
      Some(Right(List(pos.get(dim) match {
        case StringValue("fid:A") => "training"
        case StringValue("fid:B") => "testing"
      }, "scoring")))
    }
  }

  val prt1 = data
    .slice(Over(Second), List("fid:A", "fid:B"), true)
    .slice(Over(First), List("iid:0221707", "iid:0364354"), true)
    .squash(Third, PreservingMaxPosition())
    .partition(StringPartitioner(Second))

  prt1
    .persistFile("./tmp/prt1.out", descriptive=true)

  case class IntTuplePartitioner(dim: Dimension) extends Partitioner with Assign {
    type T = (Int, Int, Int)

    def assign[P <: Position](pos: P): Collection[T] = {
      Some(Right(List(pos.get(dim) match {
        case StringValue("fid:A") => (1, 0, 0)
        case StringValue("fid:B") => (0, 1, 0)
      }, (0, 0, 1))))
    }
  }

  data
    .slice(Over(Second), List("fid:A", "fid:B"), true)
    .slice(Over(First), List("iid:0221707", "iid:0364354"), true)
    .squash(Third, PreservingMaxPosition())
    .partition(IntTuplePartitioner(Second))
    .persistFile("./tmp/prt2.out", descriptive=true)

  prt1
    .get("training")
    .persistFile("./tmp/train.out", descriptive=true)

  prt1
    .get("testing")
    .persistFile("./tmp/test.out", descriptive=true)

  prt1
    .get("scoring")
    .persistFile("./tmp/score.out", descriptive=true)
}

class Test10(args : Args) extends Job(args) {

  val data = TestReader.read4TupleDataAddDate(args("input"))

  data
    .reduceAndExpand(Over(Second), Mean("mean", strict=true, nan=true))
    .writeCSV(Over(Second), "./tmp/agg1.csv")

  data
    .slice(Over(First), List("iid:0064402", "iid:0066848", "iid:0076357", "iid:0216406", "iid:0221707", "iid:0262443",
                             "iid:0364354", "iid:0375226", "iid:0444510", "iid:1004305"), true)
    .squash(Third, PreservingMaxPosition())
    .reduceAndExpand(Along(Second), Count("count"))
    .writeCSV(Over(Second), "./tmp/agg2.csv")

  data
    .slice(Over(First), List("iid:0064402", "iid:0066848", "iid:0076357", "iid:0216406", "iid:0221707", "iid:0262443",
                             "iid:0364354", "iid:0375226", "iid:0444510", "iid:1004305"), true)
    .squash(Third, PreservingMaxPosition())
    .reduceAndExpand(Along(First), List(Count("count"), Moments("mean", "sd", "skewness", "kurtosis"), Min("min"),
      Max("max"), MaxAbs("max.abs")))
    .writeCSV(Over(Second), "./tmp/agg3.csv")
}

class Test11(args : Args) extends Job(args) {

  val data = TestReader.read4TupleDataAddDate(args("input"))

  data
    .slice(Over(Second), List("fid:A", "fid:B", "fid:Y", "fid:Z"), true)
    .slice(Over(First), List("iid:0221707", "iid:0364354"), true)
    .transform(Indicator(Second, name="%1$s.ind"))
    .persistFile("./tmp/trn2.out", descriptive=true)

  data
    .slice(Over(Second), List("fid:A", "fid:B", "fid:Y", "fid:Z"), true)
    .slice(Over(First), List("iid:0221707", "iid:0364354"), true)
    .squash(Third, PreservingMaxPosition())
    .transform(Binarise(Second))
    .writeCSV(Over(Second), "./tmp/trn3.out")
}

class Test12(args : Args) extends Job(args) {

  val data = TestReader.read4TupleDataAddDate(args("input"))
    .slice(Over(Second), List("fid:A", "fid:B", "fid:Y", "fid:Z"), true)
    .slice(Over(First), List("iid:0221707", "iid:0364354"), true)

  data
    .squash(Third, PreservingMaxPosition())
    .fill(Content(ContinuousSchema[Codex.LongCodex](), 0))
    .writeCSV(Over(Second), "./tmp/fll1.out")

  data
    .fill(Content(ContinuousSchema[Codex.LongCodex](), 0))
    .persistFile("./tmp/fll3.out", descriptive=true)
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
    .fill(Content(ContinuousSchema[Codex.LongCodex](), 0))

  data
    .join(Over(First), inds)
    .fill(Content(ContinuousSchema[Codex.LongCodex](), 0))
    .writeCSV(Over(Second), "./tmp/fll2.out")

  data
    .fill(Over(Second), all.reduceAndExpand(Over(Second), Mean("mean", strict=true, nan=true)).permute(Second, First))
    .join(Over(First), inds)
    .writeCSV(Over(Second), "./tmp/fll4.out")
}

class Test14(args : Args) extends Job(args) {

  val data = TestReader.read4TupleDataAddDate(args("input"))
    .slice(Over(Second), List("fid:A", "fid:B", "fid:Y", "fid:Z"), true)
    .slice(Over(First), List("iid:0221707", "iid:0364354"), true)

  data
    .change(Over(Second), "fid:A", NominalSchema[Codex.LongCodex]())
    .persistFile("./tmp/chg1.out", descriptive=true)
}

class Test15(args : Args) extends Job(args) {

  val data = TestReader.read4TupleDataAddDate(args("input"))

  data
    .slice(Over(Second), List("fid:A", "fid:C", "fid:E", "fid:G"), true)
    .slice(Over(First), List("iid:0221707", "iid:0364354"), true)
    .reduceAndExpand(Along(Third), Sum("sum"))
    .melt(Third, Second)
    .writeCSV(Over(Second), "./tmp/rsh1.out")

  val inds = data
    .slice(Over(First), List("iid:0064402", "iid:0066848", "iid:0076357", "iid:0216406", "iid:0221707", "iid:0262443",
                             "iid:0364354", "iid:0375226", "iid:0444510", "iid:1004305"), true)
    .slice(Over(Second), List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"), true)
    .squash(Third, PreservingMaxPosition())
    .transform(Indicator(Second, name="%1$s.ind"))
    .writeCSV(Over(Second), "./tmp/trn1.csv")

  data
    .slice(Over(First), List("iid:0064402", "iid:0066848", "iid:0076357", "iid:0216406", "iid:0221707", "iid:0262443",
                             "iid:0364354", "iid:0375226", "iid:0444510", "iid:1004305"), true)
    .slice(Over(Second), List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"), true)
    .squash(Third, PreservingMaxPosition())
    .join(Over(First), inds)
    .writeCSV(Over(Second), "./tmp/jn1.csv")
}

class Test16(args : Args) extends Job(args) {

  val data: Matrix3D = TestReader.read4TupleDataAddDate(args("input"))

  case class HashSample() extends Sampler with Select {
    def select[P <: Position](pos: P): Boolean = (pos.get(First).toString.hashCode % 25) == 0
  }

  data
    .sample(HashSample())
    .persistFile("./tmp/smp1.out")
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
    .writeCSV(Over(Second), "./tmp/trn6.csv")

  data
    .refine((pos: Position2D, con: Content) => con.value gtr 500)
    .writeCSV(Over(Second), "./tmp/flt1.csv")

  def removeGreaterThanMean(pos: Position2D, con: Content, ext: Map[Position1D, Map[Position1D, Content]]): Boolean = {
    if (con.schema.kind.isSpecialisationOf(Numerical)) {
      con.value leq ext(Position1D(pos.get(Second)))(Position1D("mean")).value
    } else {
      true
    }
  }

  data
    .refineWithValue(removeGreaterThanMean, stats)
    .writeCSV(Over(Second), "./tmp/flt2.csv")
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
    .which(Over(Second), "count", (pos: Position, con: Content) => con.value leq 2)
    .names(Over(First))

  data
    .slice(Over(Second), rem, false)
    .writeCSV(Over(Second), "./tmp/flt3.csv")
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
      if (pos.get(dim).toShortString == "iid:0364354") {
        Some(Left(right))
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
    .which((pos: Position, con: Content) => (pos.get(Second) equ "count") && (con.value leq 2))
    .names(Over(First))

  for (p <- List("train", "test")) {
    parts
      .get(p)
      .slice(Over(Second), rem, false)
      .transformWithValue(List(Indicator(Second, name="%1$s.ind"), Binarise(Second), Normalise(Second, key="max.abs")),
        stats.toMap(Over(First)))
      .fill(Content(ContinuousSchema[Codex.LongCodex](), 0))
      .writeCSV(Over(Second), "./tmp/pln_" + p + ".csv")
  }
}

class Test20(args : Args) extends Job(args) {

  read3DFileWithDictionary("./ivoryInputfile1.txt", Dictionary.read("./dict.txt"))
    .persistFile("./tmp/ivr1.out")
}

class Test21(args : Args) extends Job(args) {

  val data = TestReader.read4TupleDataAddDate(args("input"))

  data
    .shape()
    .persistFile("./tmp/siz0.out")

  data
    .size(First)
    .persistFile("./tmp/siz1.out")

  data
    .size(Second)
    .persistFile("./tmp/siz2.out")

  data
    .size(Third)
    .persistFile("./tmp/siz3.out")
}

class Test22(args : Args) extends Job(args) {

  val data = read2DFile("numericInputfile.txt")

  case class Diff() extends Deriver with Initialise {
    type T = Cell[Position]

    def initialise[P <: Position, D <: Dimension](sel: Slice[P, D]#S, rem: Slice[P, D]#R, con: Content): T = (rem, con)
    def present[P <: Position, D <: Dimension](sel: Slice[P, D]#S, rem: Slice[P, D]#R, con: Content,
      t: T): (T, CellCollection[sel.M]) = {
      ((rem, con), (con.value.asDouble, t._2.value.asDouble) match {
        case (Some(c), Some(l)) => Some(Left((sel.append(rem.toShortString("") + "-" + t._1.toShortString("")),
          Content(ContinuousSchema[Codex.DoubleCodex](), c - l))))
        case _ => None
      })
    }
  }

  data
    .derive(Over(First), Diff())
    .persistFile("./tmp/dif1.out")

  data
    .derive(Over(Second), Diff())
    .permute(Second, First)
    .persistFile("./tmp/dif2.out")
}

class Test23(args : Args) extends Job(args) {

  val data = read2DFile("somePairwise.txt")

  case class DiffSquared() extends Operator with Compute {
    def compute[P <: Position, D <: Dimension](slice: Slice[P, D], leftPos: Slice[P, D]#S, leftCon: Content,
      rightPos: Slice[P, D]#S, rightCon: Content, rem: Slice[P, D]#R): Option[Cell[rem.M]] = {
      val xc = leftPos.toShortString("")
      val yc = rightPos.toShortString("")

      (xc < yc && xc != yc) match {
        case true => Some((rem.append("(" + xc + "-" + yc + ")^2"), Content(ContinuousSchema[Codex.DoubleCodex](),
          math.pow(leftCon.value.asLong.get - rightCon.value.asLong.get, 2))))
        case false => None
      }
    }
  }

  data
    .pairwise(Over(Second), DiffSquared())
    .persistFile("./tmp/pws1.out")
}

class Test24(args: Args) extends Job(args) {

  // see http://www.mathsisfun.com/data/correlation.html for data

  val schema = List(("day", NominalSchema[Codex.StringCodex]()),
                    ("temperature", ContinuousSchema[Codex.DoubleCodex]()),
                    ("sales", DiscreteSchema[Codex.LongCodex]()))
  val data = readTable("somePairwise2.txt", schema, separator="\\|")

  data
    .correlation(Over(Second))
    .persistFile("./tmp/pws2.out")

  val schema2 = List(("day", NominalSchema[Codex.StringCodex]()),
                     ("temperature", ContinuousSchema[Codex.DoubleCodex]()),
                     ("sales", DiscreteSchema[Codex.LongCodex]()),
                     ("neg.sales", DiscreteSchema[Codex.LongCodex]()))
  val data2 = readTable("somePairwise3.txt", schema2, separator="\\|")

  data2
    .correlation(Over(Second))
    .persistFile("./tmp/pws3.out")
}

class Test25(args: Args) extends Job(args) {

  read2DFile("mutualInputfile.txt")
    .mutualInformation(Over(Second))
    .persistFile("./tmp/mi.out")
}

class Test26(args: Args) extends Job(args) {

  val left = read2DFile("algebraInputfile1.txt")
  val right = read2DFile("algebraInputfile2.txt")

  left
    .pairwiseBetween(Over(First), right, Times(comparer=All))
    .persistFile("./tmp/alg.out")
}

class Test27(args: Args) extends Job(args) {

  // http://www.statisticshowto.com/moving-average/
  read2DFile("simMovAvgInputfile.txt", first=LongCodex)
    .derive(Over(Second), SimpleMovingAverage(5))
    .persistFile("./tmp/sma1.out")

  read2DFile("simMovAvgInputfile.txt", first=LongCodex)
    .derive(Over(Second), SimpleMovingAverage(5, all=true))
    .persistFile("./tmp/sma2.out")

  read2DFile("simMovAvgInputfile.txt", first=LongCodex)
    .derive(Over(Second), CenteredMovingAverage(2))
    .persistFile("./tmp/tma.out")

  read2DFile("simMovAvgInputfile.txt", first=LongCodex)
    .derive(Over(Second), WeightedMovingAverage(5))
    .persistFile("./tmp/wma1.out")

  read2DFile("simMovAvgInputfile.txt", first=LongCodex)
    .derive(Over(Second), WeightedMovingAverage(5, all=true))
    .persistFile("./tmp/wma2.out")

  // http://stackoverflow.com/questions/11074665/how-to-calculate-the-cumulative-average-for-some-numbers
  read1DFile("cumMovAvgInputfile.txt")
    .derive(Along(First), CumulativeMovingAverage())
    .persistFile("./tmp/cma.out")

  // http://www.incrediblecharts.com/indicators/exponential_moving_average.php
  read1DFile("expMovAvgInputfile.txt")
    .derive(Along(First), ExponentialMovingAverage(0.33))
    .persistFile("./tmp/ema.out")
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
    .transformWithValue(Cut(Second), Cut.fixed(stats, "min", "max", 4))
    .persistFile("./tmp/cut1.out")

  data
    .transformWithValue(Cut(Second, "%s.square"), Cut.squareRootChoice(stats, "count", "min", "max"))
    .persistFile("./tmp/cut2.out")

  data
    .transformWithValue(Cut(Second, "%s.sturges"), Cut.sturgesFormula(stats, "count", "min", "max"))
    .persistFile("./tmp/cut3.out")

  data
    .transformWithValue(Cut(Second, "%s.rice"), Cut.riceRule(stats, "count", "min", "max"))
    .persistFile("./tmp/cut4.out")

  data
    .transformWithValue(Cut(Second, "%s.doane"), Cut.doanesFormula(stats, "count", "min", "max", "skewness"))
    .persistFile("./tmp/cut5.out")

  data
    .transformWithValue(Cut(Second, "%s.scott"), Cut.scottsNormalReferenceRule(stats, "count", "min", "max", "sd"))
    .persistFile("./tmp/cut6.out")

  data
    .transformWithValue(Cut(Second, "%s.break"), Cut.breaks(Map("fid:A" -> List(-1, 4, 8, 12, 16))))
    .persistFile("./tmp/cut7.out")
}

