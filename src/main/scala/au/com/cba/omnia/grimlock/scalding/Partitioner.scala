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

package au.com.cba.omnia.grimlock.scalding.partition

import au.com.cba.omnia.grimlock.framework.{ Cell, Default, NoParameters, Reducers, Tuner, Sequence2 }
import au.com.cba.omnia.grimlock.framework.partition.{ Partitions => FwPartitions }
import au.com.cba.omnia.grimlock.framework.position._
import au.com.cba.omnia.grimlock.framework.utility.UnionTypes._

import au.com.cba.omnia.grimlock.scalding._

import com.twitter.scalding.typed.{ Grouped, TypedPipe }

import scala.reflect.ClassTag

/**
 * Rich wrapper around a `TypedPipe[(I, Cell[P])]`.
 *
 * @param data The `TypedPipe[(I, Cell[P])]`.
 */
case class Partitions[P <: Position, I: Ordering](data: TypedPipe[(I, Cell[P])]) extends FwPartitions[P, I]
  with Persist[(I, Cell[P])] {
  def add(id: I, partition: U[Cell[P]]): U[(I, Cell[P])] = data ++ (partition.map { case c => (id, c) })

  type ForAllTuners[T] = T In OneOf[Default[Execution]]#Or[Default[Sequence2[Reducers, Execution]]]
  def forAll[Q <: Position, T <: Tuner : ForAllTuners](fn: (I, U[Cell[P]]) => U[Cell[Q]], exclude: List[I], tuner: T)(
    implicit ev1: ClassTag[I]): U[(I, Cell[Q])] = {
    val (context, identifiers) = tuner.parameters match {
      case Execution(ctx) => (ctx, ids(Default()))
      case Sequence2(r @ Reducers(_), Execution(ctx)) => (ctx, ids(Default(r)))
    }

    val keys = identifiers
      .collect { case i if !exclude.contains(i) => List(i) }
      .sum
      .getExecution
      .waitFor(context.config, context.mode)
      .getOrElse(throw new Exception("unable to get ids list"))

    forEach(keys, fn)
  }

  def forEach[Q <: Position](ids: List[I], fn: (I, U[Cell[P]]) => U[Cell[Q]]): U[(I, Cell[Q])] = {
    ids
      .map { case i => fn(i, get(i)).map { case c => (i, c) } }
      .reduce[U[(I, Cell[Q])]]((x, y) => x ++ y)
  }

  def get(id: I): U[Cell[P]] = data.collect { case (i, c) if (id == i) => c }

  type IdsTuners[T] = T In OneOf[Default[NoParameters]]#Or[Default[Reducers]]
  def ids[T <: Tuner : IdsTuners](tuner: T = Default())(implicit ev1: ClassTag[I]): U[I] = {
    val keys = Grouped(data.map { case (i, _) => (i, ()) })

    tuner.parameters match {
      case Reducers(reducers) => keys.withReducers(reducers).sum.keys
      case _ => keys.sum.keys
    }
  }

  def merge(ids: List[I]): U[Cell[P]] = data.collect { case (i, c) if (ids.contains(i)) => c }

  def remove(id: I): U[(I, Cell[P])] = data.filter { case (i, _) => i != id }

  def saveAsText(file: String, writer: TextWriter)(implicit ctx: C): U[(I, Cell[P])] = saveText(file, writer)
}

/** Companion object for the Scalding `Partitions` class. */
object Partitions {
  /** Conversion from `TypedPipe[(I, Cell[P])]` to a Scalding `Partitions`. */
  implicit def TPTC2TPP[P <: Position, I: Ordering](data: TypedPipe[(I, Cell[P])]): Partitions[P, I] = Partitions(data)
}

