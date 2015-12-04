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

package au.com.cba.omnia.grimlock.library.squash

import au.com.cba.omnia.grimlock.framework._
import au.com.cba.omnia.grimlock.framework.content._
import au.com.cba.omnia.grimlock.framework.encoding._
import au.com.cba.omnia.grimlock.framework.position._
import au.com.cba.omnia.grimlock.framework.squash._

private[squash] trait PreservingPosition[P <: Position] extends Squasher[P] {
  type T = (Value, Content)

  def prepare(cell: Cell[P], dim: Dimension): T = (cell.position(dim), cell.content)

  def present(t: T): Option[Content] = Some(t._2)
}

/** Reduce two cells preserving the cell with maximal value for the coordinate of the dimension being squashed. */
case class PreservingMaxPosition[P <: Position]() extends PreservingPosition[P] {
  def reduce(lt: T, rt: T): T = if (Value.Ordering.compare(lt._1, rt._1) > 0) { lt } else { rt }
}

/** Reduce two cells preserving the cell with minimal value for the coordinate of the dimension being squashed. */
case class PreservingMinPosition[P <: Position]() extends PreservingPosition[P] {
  def reduce(lt: T, rt: T): T = if (Value.Ordering.compare(lt._1, rt._1) < 0) { lt } else { rt }
}

/** Reduce two cells preserving the cell whose coordinate matches `keep`. */
case class KeepSlice[P <: Position, V](keep: V)(implicit ev: Valueable[V]) extends Squasher[P] {
  type T = Option[Content]

  def prepare(cell: Cell[P], dim: Dimension): T = if (cell.position(dim) equ keep) { Some(cell.content) } else { None }

  def reduce(lt: T, rt: T): T = if (lt.isEmpty) { rt } else { lt }

  def present(t: T): Option[Content] = t
}

