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
import au.com.cba.omnia.grimlock.framework.aggregate._
import au.com.cba.omnia.grimlock.framework.content._
import au.com.cba.omnia.grimlock.framework.partition._
import au.com.cba.omnia.grimlock.framework.position._
import au.com.cba.omnia.grimlock.framework.transform._

import au.com.cba.omnia.grimlock.library.aggregate._
import au.com.cba.omnia.grimlock.library.transform._

import au.com.cba.omnia.grimlock.spark.Matrix._
import au.com.cba.omnia.grimlock.spark.partition.Partitions._
import au.com.cba.omnia.grimlock.spark.position.PositionDistributable._
import au.com.cba.omnia.grimlock.spark.position.Positions._

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd.RDD

// Define a custom partition. If the instance is 'iid:0364354' or 'iid:0216406' then assign it to the right (test)
// partition. In all other cases assing it to the left (train) partition.
case class CustomPartition(dim: Dimension, left: String, right: String) extends Partitioner[Position2D, String] {
  def assign(cell: Cell[Position2D]): TraversableOnce[String] = {
    if (cell.position(dim).toShortString == "iid:0364354" || cell.position(dim).toShortString == "iid:0216406") {
      Some(right)
    } else {
      Some(left)
    }
  }
}

object PipelineDataPreparation {

  def main(args: Array[String]) {
    // Define implicit context for loading data.
    implicit val spark = new SparkContext(args(0), "Grimlock Spark Demo", new SparkConf())

    // Path to data files, output folder
    val path = if (args.length > 1) args(1) else "../../data"
    val output = "spark"

    // Read the data (ignoring errors). This returns a 2D matrix (instance x feature).
    val (data, _) = loadText(s"${path}/exampleInput.txt", Cell.parse2D())

    // Perform a split of the data into a training and test set.
    val parts = data
      .split(CustomPartition(First, "train", "test"))

    // Get the training data
    val train = parts
      .get("train")

    // Define descriptive statistics to be computed on the training data.
    val dstats: List[Aggregator[Position2D, Position1D, Position2D]] = List(
      Count().andThenRelocate(_.position.append("count").toOption),
      Mean().andThenRelocate(_.position.append("mean").toOption),
      StandardDeviation().andThenRelocate(_.position.append("sd").toOption),
      Skewness().andThenRelocate(_.position.append("skewness").toOption),
      Kurtosis().andThenRelocate(_.position.append("kurtosis").toOption),
      Min().andThenRelocate(_.position.append("min").toOption),
      Max().andThenRelocate(_.position.append("max").toOption),
      MaxAbs().andThenRelocate(_.position.append("max.abs").toOption))

    // Compute descriptive statistics on the training data.
    val descriptive = train
      .summarise(Along(First), dstats)

    // Compute histogram on the categorical features in the training data.
    val histogram = train
      .histogram(Along(First), Locate.AppendDimensionAndContentString[Position1D](First))

    // Compute the counts for each categorical features.
    val counts = histogram
      .summarise(Over(First), Sum[Position2D, Position1D]())
      .compact()

    // Define type of the counts map.
    type W = Map[Position1D, Content]

    // Define extractor to extract counts from the map.
    val extractCount = ExtractWithDimension[Position2D, Content](First).andThenPresent(_.value.asDouble)

    // Define summary statisics to compute on the histogram.
    val sstats: List[AggregatorWithValue[Position2D, Position1D, Position2D] { type V >: W }] = List(
      Count().andThenRelocate(_.position.append("num.cat").toOption),
      Entropy(extractCount).andThenRelocate(_.position.append("entropy").toOption),
      FrequencyRatio().andThenRelocate(_.position.append("freq.ratio").toOption))

    // Compute summary statisics on the histogram.
    val summary = histogram
      .summariseWithValue(Over(First), sstats, counts)

    // Combine all statistics and write result to file
    val stats = (descriptive ++ histogram ++ summary)
      .saveAsText(s"./demo.${output}/stats.out")

    // Determine which features to filter based on statistics. In this case remove all features that occur for 2 or
    // fewer instances. These are removed first to prevent indicator features from being created.
    val rem1 = stats
      .which(cell => (cell.position(Second) equ "count") && (cell.content.value leq 2))
      .names(Over(First))

    // Also remove constant features (standard deviation is 0, or 1 category). These are removed after indicators have
    // been created.
    val rem2 = stats
      .which(cell => ((cell.position(Second) equ "sd") && (cell.content.value equ 0)) ||
          ((cell.position(Second) equ "num.cat") && (cell.content.value equ 1)))
      .names(Over(First))

    // Finally remove categoricals for which an individual category has only 1 value. These are removed after binarized
    // features have been created.
    val rem3 = stats
      .which(cell => (cell.position(Second) like ".*=.*".r) && (cell.content.value equ 1))
      .names(Over(Second))

    // Define type of statistics map.
    type S = Map[Position1D, Map[Position1D, Content]]

    // Define extract object to get data out of statistics map.
    def extractStat(key: String) = ExtractWithDimensionAndKey[Position2D, Content](Second, key)
      .andThenPresent(_.value.asDouble)

    // List of transformations to apply to each partition.
    val transforms: List[TransformerWithValue[Position2D, Position2D] { type V >: S }] = List(
      Clamp(extractStat("min"), extractStat("max"))
        .andThenWithValue(Standardise(extractStat("mean"), extractStat("sd"))),
      Binarise(Locate.RenameDimensionWithContent(Second)))

    // For each partition:
    //  1/  Remove sparse features;
    //  2/  Create indicator features;
    //  3a/ Remove constant features;
    //  3b/ Clamp features to min/max value of the training data and standardise, binarise categorical features;
    //  3c/ Remove sparse category features;
    //  4a/ Combine preprocessed data sets;
    //  4b/ Optionally fill the matrix (note: this is expensive);
    //  4c/ Save the result as pipe separated CSV for use in modelling.
    def prepare(key: String, partition: RDD[Cell[Position2D]]): RDD[Cell[Position2D]] = {
      val d = partition
        .slice(Over(Second), rem1, false)

      val ind = d
        .transform(Indicator[Position2D]().andThenRelocate(Locate.RenameDimension(Second, "%1$s.ind")))

      val csb = d
        .slice(Over(Second), rem2, false)
        .transformWithValue(transforms, stats.compact(Over(First)))
        .slice(Over(Second), rem3, false)

      (ind ++ csb)
        //.fillHomogeneous(Content(ContinuousSchema(DoubleCodex), 0))
        .saveAsCSV(Over(First), s"./demo.${output}/${key}.csv")
    }

    // Prepare each partition.
    parts
      .forEach(List("train", "test"), prepare)
      .toUnit
  }
}

