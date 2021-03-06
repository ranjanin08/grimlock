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

package au.com.cba.omnia.grimlock.spark.examples

import au.com.cba.omnia.grimlock.framework._
import au.com.cba.omnia.grimlock.framework.content._
import au.com.cba.omnia.grimlock.framework.content.metadata._
import au.com.cba.omnia.grimlock.framework.encoding._
import au.com.cba.omnia.grimlock.framework.position._
import au.com.cba.omnia.grimlock.framework.transform._

import au.com.cba.omnia.grimlock.library.aggregate._
import au.com.cba.omnia.grimlock.library.transform._

import au.com.cba.omnia.grimlock.spark.environment._
import au.com.cba.omnia.grimlock.spark.Matrix._

import org.apache.spark.{ SparkConf, SparkContext }

// Simple transformer that adds weight to a label.
case class AddWeight() extends TransformerWithValue[Position2D, Position3D] {
  type V = Map[Position1D, Content]

  // Adding the weight is a straight forward lookup by the value of the content. Also return this cell
  // (cell.position.append("label"), cell.content) so no additional join is needed with the original label data.
  def presentWithValue(cell: Cell[Position2D], ext: V): TraversableOnce[Cell[Position3D]] = {
    List(Cell(cell.position.append("label"), cell.content),
      Cell(cell.position.append("weight"), ext(Position1D(cell.content.value.toShortString))))
  }
}

object LabelWeighting {

  def main(args: Array[String]) {
    // Define implicit context.
    implicit val ctx = Context(new SparkContext(args(0), "Grimlock Spark Demo", new SparkConf()))

    // Path to data files, output folder
    val path = if (args.length > 1) args(1) else "../../data"
    val output = "spark"

    // Read labels and melt the date into the instance id to generate a 1D matrix.
    val labels = loadText(s"${path}/exampleLabels.txt",
      Cell.parse2DWithSchema(Content.parser(DoubleCodec, ContinuousSchema[Double]())))
      .data // Keep only the data (ignoring errors).
      .melt(Second, First, Value.concatenate(":"))

    // Compute histogram over the label values.
    val histogram = labels
      .histogram(Along(First), Locate.AppendContentString[Position0D](), false)

    // Compute the total number of labels and compact result into a Map.
    val sum = labels
      .size(First)
      .compact(Over(First))

    // Define extract object to get data out of sum/min map.
    def extractor(key: String) = ExtractWithKey[Position1D, Content](key).andThenPresent(_.value.asDouble)

    // Compute the ratio of (total number of labels) / (count for each label).
    val ratio = histogram
      .transformWithValue(Fraction(extractor(First.toString), true), sum)

    // Find the minimum ratio, and compact the result into a Map.
    val min = ratio
      .summarise(Along(First), Min[Position1D, Position0D]().andThenRelocate(_.position.append("min").toOption))
      .compact(Over(First))

    // Divide the ratio by the minimum ratio, and compact the result into a Map.
    val weights = ratio
      .transformWithValue(Fraction(extractor("min")), min)
      .compact(Over(First))

    // Re-read labels and add the computed weight.
    loadText(s"${path}/exampleLabels.txt",
      Cell.parse2DWithSchema(Content.parser(DoubleCodec, ContinuousSchema[Double]())))
      .data // Keep only the data (ignoring errors).
      .transformWithValue(AddWeight(), weights)
      .saveAsText(s"./demo.${output}/weighted.out")
      .toUnit
  }
}

