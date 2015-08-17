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
import au.com.cba.omnia.grimlock.framework.content.metadata._
import au.com.cba.omnia.grimlock.framework.encoding._
import au.com.cba.omnia.grimlock.framework.position._
import au.com.cba.omnia.grimlock.framework.utility._
import au.com.cba.omnia.grimlock.framework.window._

import au.com.cba.omnia.grimlock.spark.Matrix._

import org.apache.spark.{ SparkConf, SparkContext }

// Simple gradient feature genertor
case class Gradient(dim: Dimension) extends Window[Position2D, Position1D, Position3D] {
  type T = Cell[Position1D]

  // Initialise state to the remainder coordinates (contains the date) and the content.
  def initialise(cell: Cell[Position2D], rem: Position1D): (T, TraversableOnce[Cell[Position3D]]) = {
    (Cell(rem, cell.content), None)
  }

  val DayInMillis = 1000 * 60 * 60 * 24
  val separator = ""

  // For each new cell, output the difference with the previous cell (contained in `t`).
  def present(cell: Cell[Position2D], rem: Position1D, t: T): (T, TraversableOnce[Cell[Position3D]]) = {
    // Get current date from `rem` and previous date from `t` and compute number of days between the dates.
    val days = rem(dim).asDate.flatMap {
      case dc => t.position(dim).asDate.map { case dt => (dc.getTime - dt.getTime) / DayInMillis }
    }

    // Get the difference in current value and previous value.
    val delta = cell.content.value.asDouble.flatMap { case dc => t.content.value.asDouble.map { case dt => dc - dt } }

    // Generate cell containing the gradient (delta / days).
    val grad = days.flatMap {
      case td => delta.map {
        case vd => Cell(cell.position.append(t.position.toShortString(separator) + ".to." +
          rem.toShortString(separator)), Content(ContinuousSchema(DoubleCodex), vd / td))
      }
    }

    // Update state to be current `rem` and `con`, and output the gradient.
    (Cell(rem, cell.content), grad)
  }
}

object DerivedData {

  def main(args: Array[String]) {
    // Define implicit context for reading.
    implicit val spark = new SparkContext(args(0), "Grimlock Spark Demo", new SparkConf())

    // Path to data files, output folder
    val path = if (args.length > 1) args(1) else "../../data"
    val output = "spark"

    // Generate gradient features:
    // 1/ Read the data as 3D matrix (instance x feature x date).
    // 2/ Compute gradients along the date axis. The result is a 3D matrix (instance x feature x gradient).
    // 3/ Melt third dimension (gradients) into second dimension. The result is a 2D matrix (instance x
    //    feature.from.gradient)
    // 4/ Persist 2D gradient features.
    loadText(s"${path}/exampleDerived.txt", Cell.parse3D(third = DateCodex()))
      .slide(Along(Third), Gradient(First))
      .melt(Third, Second, ".from.")
      .save(s"./demo.${output}/gradient.out")
  }
}

