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

package au.com.cba.omnia.grimlock.spark.examples

import au.com.cba.omnia.grimlock.framework._
import au.com.cba.omnia.grimlock.framework.content._
import au.com.cba.omnia.grimlock.framework.position._

import au.com.cba.omnia.grimlock.library.aggregate._
import au.com.cba.omnia.grimlock.library.squash._
import au.com.cba.omnia.grimlock.library.transform._

import au.com.cba.omnia.grimlock.spark.Matrix._

import org.apache.spark.{ SparkConf, SparkContext }

object Conditional {

  def main(args: Array[String]) {
    // Define implicit context for reading.
    implicit val spark = new SparkContext(args(0), "Grimlock Spark Demo", new SparkConf())

    // Path to data files, output folder
    val path = if (args.length > 1) args(1) else "../../data"
    val output = "spark"

    // Read the data.
    // 1/ Read the data (ignoring errors), this returns a 2D matrix (row x feature).
    val (data, _) = loadText(s"${path}/exampleConditional.txt", Cell.parse2D())

    // Generate 3D matrix (hair color x eye color x gender)
    // 1/ Reshape matrix expanding it with hair color.
    // 2/ Reshape matrix expanding it with eye color.
    // 3/ Reshape matrix expanding it with gender.
    // 4/ Melt the remaining 'value' column of the second dimension into the row key (first) dimension.
    // 5/ Squash the first dimension (row ids + value). As there is only one value for each
    //    hair/eye/gender triplet, any squash function can be used.
    val heg = data
      .reshape(Second, "hair", "missing")
      .reshape(Second, "eye", "missing")
      .reshape(Second, "gender", "missing")
      .melt(Second, First)
      .squash(First, PreservingMaxPosition[Position4D]())

    // Define an extractor for getting data out of the gender count (gcount) map.
    def extractor = ExtractWithDimension[Position2D, Content](Second).andThenPresent(_.value.asDouble)

    // Get the gender counts. Sum out hair and eye color.
    val gcount = heg
      .summarise(Along(First), Sum[Position3D, Position2D]())
      .summarise(Along(First), Sum[Position2D, Position1D]())
      .compact()

    // Get eye color conditional on gender.
    // 1/ Sum out hair color.
    // 2/ Divide each element by the gender's count to get conditional distribution.
    heg
      .summarise(Along(First), Sum[Position3D, Position2D]())
      .transformWithValue(Fraction(extractor), gcount)
      .saveAsText(s"./demo.${output}/eye.out")
      .toUnit

    // Get hair color conditional on gender.
    // 1/ Sum out eye color.
    // 2/ Divide each element by the gender's count to get conditional distribution.
    heg
      .summarise(Along(Second), Sum[Position3D, Position2D]())
      .transformWithValue(Fraction(extractor), gcount)
      .saveAsText(s"./demo.${output}/hair.out")
      .toUnit
  }
}

