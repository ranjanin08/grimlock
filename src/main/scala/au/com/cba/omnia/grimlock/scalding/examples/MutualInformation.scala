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

package au.com.cba.omnia.grimlock.scalding.examples

import au.com.cba.omnia.grimlock.framework._
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

import au.com.cba.omnia.grimlock.scalding.Matrix._

import com.twitter.scalding.{ Args, Job }

// Simple bucketing implementation. For numerical values it generates categorical values that are the rounded up
// value. All other values are passed through.
case class CeilingBucketing() extends Transformer with Present {
  def present[P <: Position](cell: Cell[P]): Collection[Cell[P]] = {
    val con = (cell.content.schema.kind.isSpecialisationOf(Type.Numerical), cell.content.value.asDouble) match {
      case (true, Some(d)) => Content(NominalSchema[Codex.LongCodex](), math.ceil(d).toLong)
      case _ => cell.content
    }

    Collection(cell.position, con)
  }
}

class MutualInformation(args: Args) extends Job(args) {

  // Path to data files, output folder
  val path = args.getOrElse("path", "../../data")
  val output = "scalding"

  // Read the data.
  // 1/ Read the data using the supplied dictionary. This returns a 3D matrix (instance x feature x date).
  // 2/ Squash the 3rd dimension, keeping values with minimum (earlier) coordinates. The result is a 2D matrix
  //    (instance x feature).
  // 3/ Bucket all continuous variables by rounding them.
  val data = load3DWithDictionary(s"${path}/exampleMutual.txt", Dictionary.load(s"${path}/exampleDictionary.txt"))
    .squash(Third, PreservingMinPosition())
    .transform(CeilingBucketing())

  // Compute sum of marginal entropy
  // 1/ Compute the marginal entropy over the features (second dimension).
  // 2/ Compute pairwise sum of marginal entropies for all upper triangular values.
  val marginal = data
    .summariseAndExpand(Over(Second), Entropy("marginal"))
    .pairwise(Over(First), Plus(name="%s,%s", comparer=Upper))

  // Compute joint entropy
  // 1/ Generate pairwise values for all upper triangular values.
  // 2/ Compute entropy over pairwise values. Negate the result for easy reduction below.
  val joint = data
    .pairwise(Over(Second), Concatenate(name="%s,%s", comparer=Upper))
    .summariseAndExpand(Over(First), Entropy("joint", strict=true, nan=true, all=false, negate=true))

  // Generate mutual information
  // 1/ Sum marginal and negated joint entropy
  // 2/ Persist mutual information.
  (marginal ++ joint)
    .summarise(Over(First), Sum())
    .save(s"./demo.${output}/mi.out")
}

