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

trait ApproximateDistribution[P <: Position with CompactablePosition] extends FwApproximateDistribution[P] {
  self: Matrix[P] =>

  import ScaldingImplicits._

  type HistogramTuners[T] = TP2[T]
  def histogram[S <: Position with ExpandablePosition, Q <: Position, T <: Tuner : HistogramTuners](slice: Slice[P],
    name: Locate.FromSelectedAndContent[S, Q], filter: Boolean, tuner: T = Default())(
      implicit ev1: PosExpDep[slice.S, Q], ev2: slice.S =:= S, ev3: ClassTag[Q]): U[Cell[Q]] = {
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
  def quantile[S <: Position with ExpandablePosition, Q <: Position, T <: Tuner : QuantileTuners](slice: Slice[P],
    probs: List[Double], quantiser: Quantile.Quantiser, name: Locate.FromSelectedAndOutput[S, Double, Q],
      filter: Boolean, nan: Boolean, tuner: T = Default())(implicit ev1: slice.S =:= S, ev2: PosExpDep[slice.S, Q],
        ev3: slice.R =:!= Position0D, ev4: ClassTag[slice.S]): U[Cell[Q]] = {
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
}

