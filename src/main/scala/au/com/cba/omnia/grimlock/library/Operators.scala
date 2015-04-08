// Copyright 2014-2015 Commonwealth Bank of Australia
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

package au.com.cba.omnia.grimlock.pairwise

import au.com.cba.omnia.grimlock._
import au.com.cba.omnia.grimlock.content._
import au.com.cba.omnia.grimlock.content.metadata._
import au.com.cba.omnia.grimlock.encoding._
import au.com.cba.omnia.grimlock.position._
import au.com.cba.omnia.grimlock.utility._

/** Convenience trait for operators that apply to `Double` values. */
trait DoubleOperator { self: Operator with Compute =>
  /**
   * Pattern for the new name of the pairwise coordinate. Use `%[12]$``s` for the string representations of the
   * coordinate.
   */
  val name: String

  /** Separator to use when writing positions to string. */
  val separator: String

  /** Indicator if pairwise operator `f()` should be called as `f(l, r)` or as `f(r, l)`. */
  val inverse: Boolean

  /** Comparer object defining which pairwise operations should be computed. */
  val comparer: Comparer

  /**
   * Indicate if the cell is selected as part of the sample.
   *
   * @param slice Encapsulates the dimension(s) along which to compute.
   * @param left  The selected left cell to compute with.
   * @param right The selected right cell to compute with.
   * @param rem   The remaining coordinates.
   *
   * @note The return value is a `Collection` to allow, for example, upper or lower triangular matrices to be returned
   *       (this can be done by comparing the selected coordinates)
   */
  def compute[P <: Position, D <: Dimension](slice: Slice[P, D])(left: Cell[slice.S], right: Cell[slice.S],
    rem: slice.R): Collection[Cell[slice.R#M]] = {
    val coordinate = name.format(left.position.toShortString(separator), right.position.toShortString(separator))

    (comparer.check(left.position, right.position), left.content.value.asDouble, right.content.value.asDouble) match {
      case (true, Some(l), Some(r)) => Collection(rem.prepend(coordinate),
        Content(ContinuousSchema[Codex.DoubleCodex](), if (inverse) compute(r, l) else compute(l, r)))
      case _ => Collection[Cell[slice.R#M]]()
    }
  }

  protected def compute(l: Double, r: Double): Double
}

/** Add two values. */
case class Plus(name: String = "(%1$s+%2$s)", separator: String = "|", comparer: Comparer = Lower) extends Operator
  with Compute with DoubleOperator {
  val inverse: Boolean = false
  protected def compute(l: Double, r: Double) = l + r
}

/** Subtract two values. */
case class Minus(name: String = "(%1$s-%2$s)", separator: String = "|", inverse: Boolean = false,
  comparer: Comparer = Lower) extends Operator with Compute with DoubleOperator {
  protected def compute(l: Double, r: Double) = l - r
}

/** Multiply two values. */
case class Times(name: String = "(%1$s*%2$s)", separator: String = "|", comparer: Comparer = Lower) extends Operator
  with Compute with DoubleOperator {
  val inverse: Boolean = false
  protected def compute(l: Double, r: Double) = l * r
}

/** Divide two values. */
case class Divide(name: String = "(%1$s/%2$s)", separator: String = "|", inverse: Boolean = false,
  comparer: Comparer = Lower) extends Operator with Compute with DoubleOperator {
  protected def compute(l: Double, r: Double) = l / r
}

/**
 * Concatenate two cells.
 *
 * @param name      Pattern for the new name of the pairwise coordinate. Use `%[12]$``s` for the string representations
 *                  of the coordinate.
 * @param value     Pattern for the new (string) value of the pairwise contents. Use `%[12]$``s` for the string
 *                  representations of the content.
 * @param separator Separator to use when writing positions to string.
 * @param comparer  Comparer object defining which pairwise operations should be computed.
 */
case class Concatenate(name: String = "(%1$s,%2$s)", value: String = "%1$s,%2$s", separator: String = "|",
  comparer: Comparer = Lower) extends Operator with Compute {
  def compute[P <: Position, D <: Dimension](slice: Slice[P, D])(left: Cell[slice.S], right: Cell[slice.S],
    rem: slice.R): Collection[Cell[slice.R#M]] = {
    comparer.check(left.position, right.position) match {
      case true =>
        val coordinate = name.format(left.position.toShortString(separator), right.position.toShortString(separator))
        val content = value.format(left.content.value.toShortString, right.content.value.toShortString)

        Collection(rem.prepend(coordinate), Content(NominalSchema[Codex.StringCodex](), content))
      case false => Collection[Cell[slice.R#M]]()
    }
  }
}

