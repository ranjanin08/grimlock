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
import au.com.cba.omnia.grimlock.framework.position._
import au.com.cba.omnia.grimlock.framework.transform._

import au.com.cba.omnia.grimlock.library.aggregate._
import au.com.cba.omnia.grimlock.library.transform._

import au.com.cba.omnia.grimlock.scalding.Matrix._

import com.twitter.scalding.{ Args, Job }

class Scoring(args: Args) extends Job(args) {

  // Path to data files, output folder
  val path = args.getOrElse("path", "../../data")
  val output = "scalding"

  // Read the data. This returns a 2D matrix (instance x feature).
  val data = load2D(s"${path}/exampleInput.txt")
  // Read the statistics from the PipelineDataPreparation example.
  val stats = load2D(s"./demo.${output}/stats.out").toMap(Over(First))
  // Read externally learned weights.
  val weights = load1D(s"${path}/exampleWeights.txt").toMap(Over(First))

  // Define type of statistics map.
  type S = Map[Position1D, Map[Position1D, Content]]

  // Define extract object to get data out of statistics map.
  def extractStat(key: String): Extract[Position2D, S, Double] = {
    ExtractWithDimensionAndKey[Dimension.Second, Position2D, String, Content](Second, key)
      .andThenPresent(_.value.asDouble)
  }

  // For the data do:
  //  1/ Create indicators, binarise categorical, and clamp & standardise numerical features;
  //  2/ Compute the scored (as a weighted sum);
  //  3/ Save the results.
  val transforms: List[TransformerWithValue[Position2D, Position2D] { type V >: S }] = List(
    Indicator().andThenRename(Transformer.rename(Second, "%1$s.ind")),
    Binarise(Second),
    Clamp(extractStat("min"), extractStat("max"))
      .andThenWithValue(Standardise(extractStat("mean"), extractStat("sd"))))

  // Type of the weights map.
  type W = Map[Position1D, Content]

  // Define extract object to get data out of weights map.
  val extractWeight = ExtractWithDimension[Dimension.Second, Position2D, Content](Second)
    .andThenPresent(_.value.asDouble)

  data
    .transformWithValue(transforms, stats)
    .summariseWithValue(Over(First), WeightedSum[Position2D, Position1D, W](extractWeight), weights)
    .save(s"./demo.${output}/scores.out")
}

