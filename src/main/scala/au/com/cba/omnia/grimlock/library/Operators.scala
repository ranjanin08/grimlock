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

package au.com.cba.omnia.grimlock.library.pairwise

import au.com.cba.omnia.grimlock.framework._
import au.com.cba.omnia.grimlock.framework.content._
import au.com.cba.omnia.grimlock.framework.content.metadata._
import au.com.cba.omnia.grimlock.framework.pairwise._
import au.com.cba.omnia.grimlock.framework.position._

/** Convenience trait for operators that apply to `Double` values. */
trait DoubleOperator[P <: Position, Q <: Position] extends Operator[P, Q] {
  /** Function to extract result position. */
  val pos: Locate.FromPairwiseCells[P, Q]

  /** Indicator if pairwise operator `f()` should be called as `f(l, r)` or as `f(r, l)`. */
  val inverse: Boolean

  /**
   * Indicate if the cell is selected as part of the sample.
   *
   * @param left  The left cell to compute with.
   * @param right The right cell to compute with.
   */
  def compute(left: Cell[P], right: Cell[P]): TraversableOnce[Cell[Q]] = {
    (pos(left, right), left.content.value.asDouble, right.content.value.asDouble) match {
      case (Some(p), Some(l), Some(r)) => Some[Cell[Q]](Cell(p,
        Content(ContinuousSchema[Double](), if (inverse) compute(r, l) else compute(l, r))))
      case _ => None
    }
  }

  protected def compute(l: Double, r: Double): Double
}

/** Add two values. */
case class Plus[P <: Position, Q <: Position](pos: Locate.FromPairwiseCells[P, Q]) extends DoubleOperator[P, Q] {
  val inverse: Boolean = false
  protected def compute(l: Double, r: Double) = l + r
}

/** Subtract two values. */
case class Minus[P <: Position, Q <: Position](pos: Locate.FromPairwiseCells[P, Q],
  inverse: Boolean = false) extends DoubleOperator[P, Q] {
  protected def compute(l: Double, r: Double) = l - r
}

/** Multiply two values. */
case class Times[P <: Position, Q <: Position](pos: Locate.FromPairwiseCells[P, Q]) extends DoubleOperator[P, Q] {
  val inverse: Boolean = false
  protected def compute(l: Double, r: Double) = l * r
}

/** Divide two values. */
case class Divide[P <: Position, Q <: Position](pos: Locate.FromPairwiseCells[P, Q],
  inverse: Boolean = false) extends DoubleOperator[P, Q] {
  protected def compute(l: Double, r: Double) = l / r
}

/**
 * Concatenate two cells.
 *
 * @param pos   Function to extract result position.
 * @param value Pattern for the new (string) value of the pairwise contents. Use `%[12]$``s` for the string
 *              representations of the content.
 */
case class Concatenate[P <: Position, Q <: Position](pos: Locate.FromPairwiseCells[P, Q],
  value: String = "%1$s,%2$s") extends Operator[P, Q] {
  def compute(left: Cell[P], right: Cell[P]): TraversableOnce[Cell[Q]] = {
    pos(left, right) match {
      case Some(p) => Some[Cell[Q]](Cell(p, Content(NominalSchema[String](),
        value.format(left.content.value.toShortString, right.content.value.toShortString))))
      case None => None
    }
  }
}

