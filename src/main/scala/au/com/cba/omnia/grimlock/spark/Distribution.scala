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

import au.com.cba.omnia.grimlock.spark._

import scala.reflect.ClassTag

import shapeless.=:!=

trait ApproximateDistribution[P <: Position with CompactablePosition] extends FwApproximateDistribution[P] {
  self: Matrix[P] =>

  import SparkImplicits._

  type HistogramTuners[T] = TP2[T]
  def histogram[S <: Position with ExpandablePosition, Q <: Position, T <: Tuner : HistogramTuners](slice: Slice[P],
    name: Locate.FromSelectedAndContent[S, Q], filter: Boolean, tuner: T = Default())(
      implicit ev1: PosExpDep[S, Q], ev2: slice.S =:= S, ev3: ClassTag[Q]): U[Cell[Q]] = {
    data
      .filter { case c => (!filter || c.content.schema.kind.isSpecialisationOf(Type.Categorical)) }
      .flatMap { case c => name(slice.selected(c.position), c.content).map((_, 1L)) }
      .tunedReduce(tuner.parameters, _ + _)
      .map { case (p, s) => Cell(p, Content(DiscreteSchema[Long](), s)) }
  }

  type QuantileTuners[T] = TP2[T]
  def quantile[S <: Position with ExpandablePosition, Q <: Position, T <: Tuner : QuantileTuners](slice: Slice[P],
    probs: List[Double], quantiser: Quantile.Quantiser, name: Locate.FromSelectedAndOutput[S, Double, Q],
      filter: Boolean, nan: Boolean, tuner: T = Default())(implicit ev1: slice.S =:= S, ev2: PosExpDep[S, Q],
        ev3: slice.R =:!= Position0D, ev4: ClassTag[slice.S]): U[Cell[Q]] = {
    val q = QuantileImpl[P, S, Q](probs, quantiser, name, nan)

    val prep = data
      .collect {
        case c if (!filter || c.content.schema.kind.isSpecialisationOf(Type.Numerical)) =>
          (slice.selected(c.position), q.prepare(c))
      }

    prep
      .tunedJoin(tuner.parameters, prep.map { case (s, _) => (s, 1L) }.tunedReduce(tuner.parameters, _ + _))
      .map { case (s, (d, c)) => ((s, c), d) }
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

  type CountMapQuantilesTuners[T] = TP2[T]
  def countMapQuantiles[S <: Position with ExpandablePosition, Q <: Position, T <: Tuner : CountMapQuantilesTuners](
    slice: Slice[P], probs: List[Double], quantiser: Quantile.Quantiser,
      name: Locate.FromSelectedAndOutput[S, Double, Q], filter: Boolean, nan: Boolean, tuner: T = Default())(
        implicit ev1: slice.S =:= S, ev2: PosExpDep[S, Q], ev3: slice.R =:!= Position0D,
          ev4: ClassTag[slice.S]): U[Cell[Q]] = {
    data
      .flatMap {
        case c =>
          if (!filter || c.content.schema.kind.isSpecialisationOf(Type.Numerical)) {
            Some((slice.selected(c.position), CountMap.from(c.content.value.asDouble.getOrElse(Double.NaN))))
          } else {
            None
          }
      }
      .tunedReduce(tuner.parameters, CountMap.reduce)
      .flatMap { case (pos, t) => CountMap.toCells[S, Q](t, probs, pos, quantiser, name, nan) }
  }

  type TDigestQuantilesTuners[T] = TP2[T]
  def tDigestQuantiles[S <: Position with ExpandablePosition, Q <: Position, T <: Tuner : TDigestQuantilesTuners](
    slice: Slice[P], probs: List[Double], compression: Double, name: Locate.FromSelectedAndOutput[S, Double, Q],
      filter: Boolean, nan: Boolean, tuner: T = Default())(implicit ev1: slice.S =:= S, ev2: PosExpDep[S, Q],
        ev3: slice.R =:!= Position0D, ev4: ClassTag[slice.S]): U[Cell[Q]] = {
    data
      .flatMap {
        case c =>
          if (!filter || c.content.schema.kind.isSpecialisationOf(Type.Numerical)) {
            c.content.value.asDouble.flatMap {
              case d => TDigest.from(d, compression).map(td => (slice.selected(c.position), td))
            }
          } else {
            None
          }
      }
      .tunedReduce(tuner.parameters, TDigest.reduce)
      .flatMap { case (pos, t) => TDigest.toCells[S, Q](t, probs, pos, name, nan) }
  }

  type UniformQuantilesTuners[T] = TP2[T]
  def uniformQuantiles[S <: Position with ExpandablePosition, Q <: Position, T <: Tuner : UniformQuantilesTuners](
    slice: Slice[P], count: Long, name: Locate.FromSelectedAndOutput[S, Double, Q], filter: Boolean, nan: Boolean,
      tuner: T = Default())(implicit ev1: slice.S =:= S, ev2: PosExpDep[S, Q], ev3: slice.R =:!= Position0D,
        ev4: ClassTag[slice.S]): U[Cell[Q]] = {
    data
      .flatMap {
        case c =>
          if (!filter || c.content.schema.kind.isSpecialisationOf(Type.Numerical)) {
            c.content.value.asDouble.map(d =>(slice.selected(c.position), StreamingHistogram.from(d, count)))
          } else {
            None
          }
      }
      .tunedReduce(tuner.parameters, StreamingHistogram.reduce)
      .flatMap { case (pos, t) => StreamingHistogram.toCells[S, Q](t, count, pos, name, nan) }
  }
}

