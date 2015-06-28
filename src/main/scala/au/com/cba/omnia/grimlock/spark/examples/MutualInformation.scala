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

package au.com.cba.omnia.grimlock.spark.examples

import au.com.cba.omnia.grimlock.framework._
import au.com.cba.omnia.grimlock.framework.aggregate._
import au.com.cba.omnia.grimlock.framework.content._
import au.com.cba.omnia.grimlock.framework.content.metadata._
import au.com.cba.omnia.grimlock.framework.encoding._
import au.com.cba.omnia.grimlock.framework.pairwise._
import au.com.cba.omnia.grimlock.framework.position._
import au.com.cba.omnia.grimlock.framework.transform._
import au.com.cba.omnia.grimlock.framework.utility._

import au.com.cba.omnia.grimlock.library.aggregate._
import au.com.cba.omnia.grimlock.library.pairwise._
import au.com.cba.omnia.grimlock.library.squash._

import au.com.cba.omnia.grimlock.spark.Matrix._

import org.apache.spark.{ SparkConf, SparkContext }

// Simple bucketing implementation. For numerical values it generates categorical values that are the rounded up
// value. All other values are passed through.
case class CeilingBucketing() extends Transformer[Position2D, Position2D] {
  def present(cell: Cell[Position2D]): Collection[Cell[Position2D]] = {
    val con = (cell.content.schema.kind.isSpecialisationOf(Type.Numerical), cell.content.value.asDouble) match {
      case (true, Some(d)) => Content(NominalSchema[Codex.LongCodex](), math.ceil(d).toLong)
      case _ => cell.content
    }

    Collection(cell.position, con)
  }
}

object MutualInformation {

  def main(args: Array[String]) {
    // Define implicit context for reading.
    implicit val spark = new SparkContext(args(0), "Grimlock Spark Demo", new SparkConf())

    // Path to data files
    val path = if (args.length > 1) args(1) else "../../data"
    val output = "spark"

    // Read the data.
    // 1/ Read the data using the supplied dictionary. This returns a 3D matrix (instance x feature x date).
    // 2/ Squash the 3rd dimension, keeping values with minimum (earlier) coordinates. The result is a 2D matrix
    //    (instance x feature).
    // 3/ Bucket all continuous variables by rounding them.
    val data = load3DWithDictionary(s"${path}/exampleMutual.txt", Dictionary.load(s"${path}/exampleDictionary.txt"))
      .squash(Third, PreservingMinPosition[Position3D]())
      .transform[Position2D, CeilingBucketing](CeilingBucketing())

    // Compute sum of marginal entropy
    // 1/ Compute the marginal entropy over the features (second dimension).
    // 2/ Compute pairwise sum of marginal entropies for all upper triangular values.
    val mhist = data
      .expand((c: Cell[Position2D]) => c.position.append(c.content.value.toShortString))
      .summarise[Dimension.First, Position2D, Count[Position3D, Position2D]](Along(First), Count())

    val mcount = mhist
      .summarise[Dimension.First, Position1D, Sum[Position2D, Position1D]](Over(First), Sum())
      .toMap()

    type W = Map[Position1D, Content]

    val marginal = mhist
      .summariseWithValue[Dimension.First, Position2D, AggregatorWithValue[Position2D, Position1D, Position2D] { type V >: W }, W](Over(First),
        Entropy(ExtractWithDimension[Dimension.First, Position2D, Content](First).andThenPresent(_.value.asDouble))
          .andThenExpandWithValue((c: Cell[Position1D], e: W) => c.position.append("marginal")), mcount)
      .pairwise[Dimension.First, Position2D, Plus[Position1D, Position1D]](Over(First), Upper, Plus(name = "%s,%s"))

    // Compute joint entropy
    // 1/ Generate pairwise values for all upper triangular values.
    // 2/ Compute entropy over pairwise values. Negate the result for easy reduction below.
    val jhist = data
      .pairwise[Dimension.Second, Position2D, Concatenate[Position1D, Position1D]](Over(Second), Upper,
        Concatenate(name = "%s,%s"))
      .expand((c: Cell[Position2D]) => c.position.append(c.content.value.toShortString))
      .summarise[Dimension.Second, Position2D, Count[Position3D, Position2D]](Along(Second), Count())

    val jcount = jhist
      .summarise[Dimension.First, Position1D, Sum[Position2D, Position1D]](Over(First), Sum())
      .toMap()

    val joint = jhist
      .summariseWithValue[Dimension.First, Position2D, AggregatorWithValue[Position2D, Position1D, Position2D] { type V >: W }, W](Over(First),
        Entropy(ExtractWithDimension[Dimension.First, Position2D, Content](First).andThenPresent(_.value.asDouble), negate = true)
          .andThenExpandWithValue((c: Cell[Position1D], e: W) => c.position.append("joint")), jcount)

    // Generate mutual information
    // 1/ Sum marginal and negated joint entropy
    // 2/ Persist mutual information.
    (marginal ++ joint)
      .summarise[Dimension.First, Position1D, Sum[Position2D, Position1D]](Over(First), Sum())
      .save(s"./demo.${output}/mi.out")
  }
}

