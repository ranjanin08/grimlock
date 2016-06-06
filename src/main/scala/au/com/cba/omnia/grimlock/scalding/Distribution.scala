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

package au.com.cba.omnia.grimlock.scalding.distribution

import au.com.cba.omnia.grimlock.framework.{
  Cell,
  Default,
  Extract,
  InMemory,
  Locate,
  NoParameters,
  Reducers,
  Tuner,
  Type
}
import au.com.cba.omnia.grimlock.framework.content._
import au.com.cba.omnia.grimlock.framework.content.metadata._
import au.com.cba.omnia.grimlock.framework.distribution.{ ApproximateDistribution => FwApproximateDistribution, _ }
import au.com.cba.omnia.grimlock.framework.position._
import au.com.cba.omnia.grimlock.framework.utility.UnionTypes._

import au.com.cba.omnia.grimlock.scalding._

import scala.reflect.ClassTag

import shapeless.=:!=

trait ApproximateDistribution[
  P <: Position[P] with ReduceablePosition[P, _] with CompactablePosition[P]
] extends FwApproximateDistribution[P] { self: Matrix[P] =>

  import ScaldingImplicits._

  type HistogramTuners[T] = TP2[T]
  def histogram[
    S <: Position[S] with ExpandablePosition[S, _],
    R <: Position[R] with ExpandablePosition[R, _],
    Q <: Position[Q],
    T <: Tuner : HistogramTuners
  ](
    slice: Slice[P, S, R],
    name: Locate.FromSelectedAndContent[S, Q],
    filter: Boolean,
    tuner: T = Default()
  )(implicit
    ev1: PosExpDep[S, Q],
    ev2: ClassTag[Q]
  ): U[Cell[Q]] = {
    data
      .filter { case c => (!filter || c.content.schema.kind.isSpecialisationOf(Type.Categorical)) }
      .flatMap { case c => name(slice.selected(c.position), c.content) }
      .asKeys
      .tuneReducers(tuner.parameters)
      .size
      .map { case (p, s) => Cell(p, Content(DiscreteSchema[Long](), s)) }
  }

  type QuantileTuners[T] = T In OneOf[InMemory[NoParameters]]#
    Or[InMemory[Reducers]]#
    Or[Default[NoParameters]]#
    Or[Default[Reducers]]
  def quantile[
    S <: Position[S] with ExpandablePosition[S, _],
    R <: Position[R] with ExpandablePosition[R, _],
    Q <: Position[Q],
    T <: Tuner : QuantileTuners
  ](
    slice: Slice[P, S, R],
    probs: List[Double],
    quantiser: Quantile.Quantiser,
    name: Locate.FromSelectedAndOutput[S, Double, Q],
    filter: Boolean,
    nan: Boolean,
    tuner: T = Default()
  )(implicit
    ev1: PosExpDep[S, Q],
    ev2: R =:!= Position0D,
    ev4: ClassTag[S]
  ): U[Cell[Q]] = {
    val q = QuantileImpl[P, S, Q](probs, quantiser, name, nan)

    val prep = data
      .collect {
        case c if (!filter || c.content.schema.kind.isSpecialisationOf(Type.Numerical)) =>
          (slice.selected(c.position), q.prepare(c))
      }
      .group

    val grouped = (tuner match {
      case InMemory(_) =>
        prep
          .flatMapWithValue(prep.size.map { case (k, v) => Map(k -> v) }.sum) {
            case ((s, d), c) => c.map { case m => ((s, m(s)), d) }
          }
      case _ =>
        prep
          .tunedJoin(tuner, tuner.parameters, prep.size)
          .map { case (s, (d, c)) => ((s, c), d) }
      })
      .group

    val tuned = tuner.parameters match {
      case Reducers(reducers) => grouped.withReducers(reducers)
      case _ => grouped
    }

    tuned
      .sorted
      .mapGroup {
        case ((_, count), itr) =>
          val first = itr.next
          val (t, c, o) = q.initialise(first, count)

          o.toIterator ++ itr
            .scanLeft((t, List[q.O]())) { case ((t, _), i) => q.update(i, t, c) }
            .flatMap { case (_, o) => o }
      }
      .flatMap { case ((p, _), o) => q.present(p, o) }
  }

  type CountMapQuantilesTuners[T] = TP2[T]
  def countMapQuantiles[
    S <: Position[S] with ExpandablePosition[S, _],
    R <: Position[R] with ExpandablePosition[R, _],
    Q <: Position[Q],
    T <: Tuner : CountMapQuantilesTuners
  ](
    slice: Slice[P, S, R],
    probs: List[Double],
    quantiser: Quantile.Quantiser,
    name: Locate.FromSelectedAndOutput[S, Double, Q],
    filter: Boolean,
    nan: Boolean,
    tuner: T = Default()
  )(implicit
    ev1: PosExpDep[S, Q],
    ev2: R =:!= Position0D,
    ev3: ClassTag[S]
  ): U[Cell[Q]] = {
    data
      .flatMap {
        case c =>
          if (!filter || c.content.schema.kind.isSpecialisationOf(Type.Numerical)) {
            Some((slice.selected(c.position), CountMap.from(c.content.value.asDouble.getOrElse(Double.NaN))))
          } else {
            None
          }
      }
      .group
      .tuneReducers(tuner.parameters)
      .reduce(CountMap.reduce)
      .flatMap { case (pos, t) => CountMap.toCells[S, Q](t, probs, pos, quantiser, name, nan) }
  }

  type TDigestQuantilesTuners[T] = TP2[T]
  def tDigestQuantiles[
    S <: Position[S] with ExpandablePosition[S, _],
    R <: Position[R] with ExpandablePosition[R, _],
    Q <: Position[Q],
    T <: Tuner : TDigestQuantilesTuners
  ](
    slice: Slice[P, S, R],
    probs: List[Double],
    compression: Double,
    name: Locate.FromSelectedAndOutput[S, Double, Q],
    filter: Boolean,
    nan: Boolean,
    tuner: T = Default()
  )(implicit
    ev1: PosExpDep[S, Q],
    ev2: R =:!= Position0D,
    ev3: ClassTag[S]
  ): U[Cell[Q]] = {
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
      .group
      .tuneReducers(tuner.parameters)
      .reduce(TDigest.reduce)
      .flatMap { case (pos, t) => TDigest.toCells[S, Q](t, probs, pos, name, nan) }
  }

  type UniformQuantilesTuners[T] = TP2[T]
  def uniformQuantiles[
    S <: Position[S] with ExpandablePosition[S, _],
    R <: Position[R] with ExpandablePosition[R, _],
    Q <: Position[Q],
    T <: Tuner : UniformQuantilesTuners
  ](
    slice: Slice[P, S, R],
    count: Long,
    name: Locate.FromSelectedAndOutput[S, Double, Q],
    filter: Boolean,
    nan: Boolean,
    tuner: T = Default()
  )(implicit
    ev1: PosExpDep[S, Q],
    ev2: R =:!= Position0D,
    ev4: ClassTag[S]
  ): U[Cell[Q]] = {
    data
      .flatMap {
        case c =>
          if (!filter || c.content.schema.kind.isSpecialisationOf(Type.Numerical)) {
            c.content.value.asDouble.map(d =>(slice.selected(c.position), StreamingHistogram.from(d, count)))
          } else {
            None
          }
      }
      .group
      .tuneReducers(tuner.parameters)
      .reduce(StreamingHistogram.reduce)
      .flatMap { case (pos, t) => StreamingHistogram.toCells[S, Q](t, count, pos, name, nan) }
  }
}

