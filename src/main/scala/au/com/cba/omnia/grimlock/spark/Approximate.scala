// Copyright 2015 Commonwealth Bank of Australia
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

package au.com.cba.omnia.grimlock.spark.distribution

import au.com.cba.omnia.grimlock.framework._
import au.com.cba.omnia.grimlock.framework.content._
import au.com.cba.omnia.grimlock.framework.content.metadata._
import au.com.cba.omnia.grimlock.framework.distribution.{ ApproximateDistribution => BaseApproximateDistribution, _ }
import au.com.cba.omnia.grimlock.framework.encoding._
import au.com.cba.omnia.grimlock.framework.position._
import au.com.cba.omnia.grimlock.framework.utility._

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object ApproximateDistribution extends BaseApproximateDistribution {
  type U[A] = RDD[A]
  type E[B] = B

  import au.com.cba.omnia.grimlock.spark.SparkImplicits._

  type HistogramTuners = TP2
  def histogram[P <: Position, Q <: Position, T <: Tuner](matrix: U[Cell[P]], slice: Slice[P],
    name: Locate.FromCell[P, Q], all: Boolean, tuner: T = Default())(implicit ev1: PosExpDep[slice.S, Q],
      ev2: ClassTag[Q], ev3: HistogramTuners#V[T]): U[Cell[Q]] = {
    matrix
      .collect { case c if (all || c.content.schema.kind.isSpecialisationOf(Type.Categorical)) => (name(c), 1L) }
      .tunedReduce(tuner.parameters, _ + _)
      .map { case (p, s) => Cell(p, Content(DiscreteSchema(LongCodex), s)) }
  }

  type QuantileTuners = TP1
  def quantile[P <: Position, S <: Position with ExpandablePosition, Q <: Position, W, T <: Tuner](matrix: U[Cell[P]],
    slice: Slice[P], probs: List[Double], quantiser: Quantile.Quantiser,
      name: Locate.FromSelectedAndOutput[S, Double, Q], count: Extract[P, W, Long], value: E[W], tuner: T = Default())(
        implicit ev1: slice.S =:= S, ev2: PosExpDep[slice.S, Q], ev3: slice.R =!= Position0D, ev4: ClassTag[slice.S],
          ev5: QuantileTuners#V[T]): U[Cell[Q]] = {
    val q = Quantile[P, S, Q, W](probs, count, quantiser, name)

    matrix
      .collect {
        case c if (c.content.schema.kind.isSpecialisationOf(Type.Numerical)) =>
          (slice.selected(c.position), q.prepare(c, value))
      }
      .groupByKey
      .flatMap {
        case (p, itr) =>
          val lst = itr
            .toList
            .sortBy { case (v, _) => v }
          val (t, c) = q.initialise(lst.head)

          lst
            .tail
            .scanLeft((t, List[q.O]())) { case ((t, _), i) => q.update(i, t, c) }
            .flatMap { case (_, o) => o.map(q.present(p, _)) }
      }
  }
}

