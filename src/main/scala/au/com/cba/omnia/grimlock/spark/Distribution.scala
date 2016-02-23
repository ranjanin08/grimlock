// Copyright 2015,2016 Commonwealth Bank of Australia
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

import au.com.cba.omnia.grimlock.framework.{
  Cell,
  Default,
  Extract,
  Locate,
  Tuner,
  Type
}
import au.com.cba.omnia.grimlock.framework.content._
import au.com.cba.omnia.grimlock.framework.content.metadata._
import au.com.cba.omnia.grimlock.framework.distribution.{ ApproximateDistribution => FwApproximateDistribution, _ }
import au.com.cba.omnia.grimlock.framework.position._
import au.com.cba.omnia.grimlock.framework.utility._

import au.com.cba.omnia.grimlock.spark._

import scala.reflect.ClassTag

trait ApproximateDistribution[P <: Position] extends FwApproximateDistribution[P] { self: Matrix[P] =>

  import SparkImplicits._

  type HistogramTuners = TP2
  def histogram[S <: Position with ExpandablePosition, Q <: Position, T <: Tuner](slice: Slice[P],
    name: Locate.FromSelectedAndContent[S, Q], filter: Boolean, tuner: T = Default())(
      implicit ev1: PosExpDep[slice.S, Q], ev2: slice.S =:= S, ev3: ClassTag[Q],
        ev4: HistogramTuners#V[T]): U[Cell[Q]] = {
    data
      .filter { case c => (!filter || c.content.schema.kind.isSpecialisationOf(Type.Categorical)) }
      .flatMap { case c => name(slice.selected(c.position), c.content).map((_, 1L)) }
      .tunedReduce(tuner.parameters, _ + _)
      .map { case (p, s) => Cell(p, Content(DiscreteSchema[Long](), s)) }
  }

  type QuantileTuners = TP1
  def quantile[S <: Position with ExpandablePosition, Q <: Position, W, T <: Tuner](slice: Slice[P],
    probs: List[Double], quantiser: Quantile.Quantiser, name: Locate.FromSelectedAndOutput[S, Double, Q],
      count: Extract[P, W, Long], value: E[W], filter: Boolean, nan: Boolean, tuner: T = Default())(
        implicit ev1: slice.S =:= S, ev2: PosExpDep[slice.S, Q], ev3: slice.R =!= Position0D, ev4: ClassTag[slice.S],
          ev5: QuantileTuners#V[T]): U[Cell[Q]] = {
    val q = QuantileImpl[P, S, Q, W](probs, count, quantiser, name, nan)

    data
      .collect {
        case c if (!filter || c.content.schema.kind.isSpecialisationOf(Type.Numerical)) =>
          val (double, count) = q.prepare(c, value)

          ((slice.selected(c.position), count), double)
      }
      .groupByKey
      .flatMap {
        case ((p, count), itr) =>
          val lst = itr.toList.sorted
          val first = lst.head
          val (t, c, o) = q.initialise(first, count)

          o.flatMap(q.present(p, _)) ++ lst
            .tail
            .scanLeft((t, List[q.O]())) { case ((t, _), i) => q.update(i, t, c) }
            .flatMap { case (_, o) => o.flatMap(q.present(p, _)) }
      }
  }
}

