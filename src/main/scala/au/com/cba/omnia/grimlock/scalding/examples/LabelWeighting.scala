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
import au.com.cba.omnia.grimlock.framework.content._
import au.com.cba.omnia.grimlock.framework.content.metadata._
import au.com.cba.omnia.grimlock.framework.encoding._
import au.com.cba.omnia.grimlock.framework.position._
import au.com.cba.omnia.grimlock.framework.transform._
import au.com.cba.omnia.grimlock.framework.utility._

import au.com.cba.omnia.grimlock.library.aggregate._
import au.com.cba.omnia.grimlock.library.transform._

import au.com.cba.omnia.grimlock.scalding.Matrix._

import com.twitter.scalding.{ Args, Job }

// Simple transformer that adds weight to a label.
case class AddWeight() extends TransformerWithValue[Position2D, Position3D] {
  type V = Map[Position1D, Content]

  // Adding the weight is a straight forward lookup by the value of the content. Also return this cell
  // (cell.position.append("label"), cell.content) so no additional join is needed with the original label data.
  def presentWithValue(cell: Cell[Position2D], ext: V): Collection[Cell[Position3D]] = {
    Collection(List(Cell(cell.position.append("label"), cell.content),
      Cell(cell.position.append("weight"), ext(Position1D(cell.content.value.toShortString)))))
  }
}

class LabelWeighting(args: Args) extends Job(args) {

  // Path to data files, output folder
  val path = args.getOrElse("path", "../../data")
  val output = "scalding"

  // Read labels and melt the date into the instance id to generate a 1D matrix.
  val labels = load2DWithSchema(s"${path}/exampleLabels.txt", ContinuousSchema(DoubleCodex))
    .melt(Second, First, ":")

  // Compute histogram over the label values.
  val histogram = labels
    .expand((c: Cell[Position1D]) => c.position.append(c.content.value.toShortString))
    .summarise(Along(First), Count[Position2D, Position1D]())

  // Compute the total number of labels and store result in a Map.
  val sum = labels
    .size(First)
    .toMap(Over(First))

  // Define extract object to get data out of sum/min map.
  def extractor(key: String): Extract[Position1D, Map[Position1D, Content], Double] = {
    ExtractWithKey[Position1D, String, Content](key).andThenPresent(_.value.asDouble)
  }

  // Compute the ratio of (total number of labels) / (count for each label).
  val ratio = histogram
    .transformWithValue(Fraction(extractor(First.toString), true), sum)

  // Find the minimum ratio, and store the result as a Map.
  val min = ratio
    .summarise(Along(First), Min[Position1D, Position0D]().andThenExpand(_.position.append("min")))
    .toMap(Over(First))

  // Divide the ratio by the minimum ratio, and store the result as a Map.
  val weights = ratio
    .transformWithValue(Fraction(extractor("min")), min)
    .toMap(Over(First))

  // Re-read labels and add the computed weight.
  load2DWithSchema(s"${path}/exampleLabels.txt", ContinuousSchema(DoubleCodex))
    .transformWithValue(AddWeight(), weights)
    .save(s"./demo.${output}/weighted.out")
}

