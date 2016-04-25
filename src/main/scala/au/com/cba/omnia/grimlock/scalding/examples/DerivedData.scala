// Copyright 2014,2015,2016 Commonwealth Bank of Australia
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
import au.com.cba.omnia.grimlock.framework.position._
import au.com.cba.omnia.grimlock.framework.window._

import au.com.cba.omnia.grimlock.scalding.environment._
import au.com.cba.omnia.grimlock.scalding.Matrix._

import com.twitter.scalding.{ Args, Job }

// Simple gradient feature genertor
case class Gradient(dim: Dimension) extends Window[Position3D, Position2D, Position1D, Position3D] {
  type I = (Option[Long], Option[Double])
  type T = (Option[Long], Option[Double], Position1D)
  type O = (Option[Double], Position1D, Position1D)

  val DayInMillis = 1000 * 60 * 60 * 24
  val separator = ""

  // Prepare the sliding window, the state is the time and the value.
  def prepare(cell: Cell[Position3D]): I = {
    (cell.position(dim).asDate.map { case d => d.getTime }, cell.content.value.asDouble)
  }

  // Initialise state to the time, value and remainder coordinates.
  def initialise(rem: Position1D, in: I): (T, TraversableOnce[O]) = ((in._1, in._2, rem), None)

  // For each new cell, output the difference with the previous cell (contained in `t`).
  def update(rem: Position1D, in: I, t: T): (T, TraversableOnce[O]) = {
    // Get current date from `in` and previous date from `t` and compute number of days between the dates.
    val days = in._1.flatMap { case dc => t._1.map { case dt => (dc - dt) / DayInMillis } }

    // Get the difference between current and previous values.
    val delta = in._2.flatMap { case dc => t._2.map { case dt => dc - dt } }

    // Generate the gradient (delta / days).
    val grad = days.flatMap { case td => delta.map { case vd => vd / td } }

    // Update state to be current `in` and `rem`, and output the gradient.
    ((in._1, in._2, rem), Some((grad, rem, t._3)))
  }

  // If a gradient is available, output a cell for it.
  def present(pos: Position2D, out: O): TraversableOnce[Cell[Position3D]] = {
    out._1.map {
      case grad => Cell(pos.append(out._3.toShortString(separator) + ".to." +
        out._2.toShortString(separator)), Content(ContinuousSchema[Double](), grad))
    }
  }
}

class DerivedData(args: Args) extends Job(args) {

  // Define implicit context.
  implicit val ctx = Context()

  // Path to data files, output folder
  val path = args.getOrElse("path", "../../data")
  val output = "scalding"

  // Generate gradient features:
  // 1/ Read the data as 3D matrix (instance x feature x date).
  // 2/ Proceed with only the data (ignoring errors).
  // 3/ Compute gradients along the date axis. The result is a 3D matrix (instance x feature x gradient).
  // 4/ Melt third dimension (gradients) into second dimension. The result is a 2D matrix (instance x
  //    feature.from.gradient)
  // 5/ Persist 2D gradient features.
  loadText(s"${path}/exampleDerived.txt", Cell.parse3D(third = DateCodec()))
    .data
    .slide(Along(Third), Gradient(Third))
    .melt(Third, Second, Value.concatenate(".from."))
    .saveAsText(s"./demo.${output}/gradient.out")
    .toUnit
}

