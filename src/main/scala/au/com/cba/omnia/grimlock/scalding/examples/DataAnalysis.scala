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

package au.com.cba.omnia.grimlock.scalding.examples

import au.com.cba.omnia.grimlock.framework._
import au.com.cba.omnia.grimlock.framework.aggregate._
import au.com.cba.omnia.grimlock.framework.position._

import au.com.cba.omnia.grimlock.library.aggregate._

import au.com.cba.omnia.grimlock.scalding.Matrix._

import com.twitter.scalding.{ Args, Job }

class DataAnalysis(args: Args) extends Job(args) {

  // Path to data files, output folder
  val path = args.getOrElse("path", "../../data")
  val output = "scalding"

  // Read the data. This returns a 2D matrix (instance x feature).
  val data = load2D(s"${path}/exampleInput.txt")

  // Define moments to compute.
  val moments: List[Aggregator[Position1D, Position0D, Position1D]] = List(
    Mean().andThenExpand(_.position.append("mean")),
    StandardDeviation().andThenExpand(_.position.append("sd")),
    Skewness().andThenExpand(_.position.append("skewness")),
    Kurtosis().andThenExpand(_.position.append("kurtosis")))

  // For the instances:
  //  1/ Compute the number of features for each instance;
  //  2/ Save the counts;
  //  3/ Compute the moments of the counts;
  //  4/ Save the moments.
  data
    .summarise(Over(First), Count[Position2D, Position1D]())
    .save(s"./demo.${output}/feature_count.out")
    .summarise(Along(First), moments)
    .save(s"./demo.${output}/feature_density.out")

  // For the features:
  //  1/ Compute the number of instance that have a value for each features;
  //  2/ Save the counts;
  //  3/ Compute the moments of the counts;
  //  4/ Save the moments.
  data
    .summarise(Over(Second), Count[Position2D, Position1D]())
    .save(s"./demo.${output}/instance_count.out")
    .summarise(Along(First), moments)
    .save(s"./demo.${output}/instance_density.out")
}

