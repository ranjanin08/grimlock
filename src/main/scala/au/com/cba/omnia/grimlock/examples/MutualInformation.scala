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

package au.com.cba.omnia.grimlock.examples

import au.com.cba.omnia.grimlock._
import au.com.cba.omnia.grimlock.content._
import au.com.cba.omnia.grimlock.content.metadata._
import au.com.cba.omnia.grimlock.encoding._
import au.com.cba.omnia.grimlock.Matrix._
import au.com.cba.omnia.grimlock.pairwise._
import au.com.cba.omnia.grimlock.position._
import au.com.cba.omnia.grimlock.reduce._
import au.com.cba.omnia.grimlock.transform._
import au.com.cba.omnia.grimlock.squash._

import com.twitter.scalding._

// Simple bucketing implementation. For continuous values it generates the rounded up value. All other values are
// passed through.
case class CeilingBucketing() extends Transformer with Present {
  def present[P <: Position with ModifiablePosition](pos: P, con: Content): CellCollection[pos.S] = {
    val c = (con.schema.kind.isSpecialisationOf(Type.Continuous), con.value.asDouble) match {
      case (true, Some(d)) => Content(DiscreteSchema[Codex.LongCodex](), math.ceil(d).toLong)
      case _ => con
    }

    Some(Left((pos.asInstanceOf[pos.S], c)))
  }
}

class MutualInformation(args : Args) extends Job(args) {
  // Read the data.
  // 1/ Read the data using the supplied dictionary. This returns a 3D matrix (instance x feature x date).
  // 2/ Squash the 3rd dimension, keeping values with minimum (earlier) coordinates. The result is a 2D matrix
  //    (instance x feature).
  // 3/ Bucket all continuous variables by rounding them.
  val data = read3DWithDictionary("exampleMIData.txt", Dictionary.read("exampleDictionary.txt"))
    .squash(Third, PreservingMinPosition())
    .transform(CeilingBucketing())

  // Compute sum of marginal entropy
  // 1/ Compute the marginal entropy over the features (second dimension).
  // 2/ Compute pairwise sum of marginal entropies for all upper triangular values.
  val marginal = data
    .reduceAndExpand(Over(Second), Entropy("marginal"))
    .pairwise(Over(First), Plus(name="%s,%s", comparer=Upper))

  // Compute joint entropy
  // 1/ Generate pairwise values for all upper triangular values.
  // 2/ Compute entropy over pairwise values. Negate the result for easy reduction below.
  val joint = data
    .pairwise(Over(Second), Concatenate(name="%s,%s", comparer=Upper))
    .reduceAndExpand(Over(First), Entropy("joint", strict=true, nan=true, negate=true))

  // Generate mutual information
  // 1/ Sum marginal and negated joint entropy
  // 2/ Persist mutual information.
  (marginal ++ joint)
    .reduce(Over(First), Sum())
    .persist("./demo/mi.out")
}

