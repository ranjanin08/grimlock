// Copyright 2014 Commonwealth Bank of Australia
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
import au.com.cba.omnia.grimlock.Matrix.Cell
import au.com.cba.omnia.grimlock.position._

/** Convenience trait for operators that apply to `Double` values. */
trait DoubleOperator { self: Operator with Compute =>
  /**
   * Pattern for the new name of the pairwise coordinate. Use `%[12]$``s` for
   * the string representations of the coordinate.
   */
  val name: String

  /** Separator to use when writing positions to string. */
  val separator: String

  /**
   * Indicator if pairwise operator `f()` should be called as `f(l, r)` or as
   * `f(r, l)`.
   */
  val inverse: Boolean

  /** Comparer object defining which pairwise operations should be computed. */
  val comparer: Comparer

  /**
   * Indicate if the cell is selected as part of the sample.
   *
   * @param slice Encapsulates the dimension(s) along which to compute.
   * @param lp    The selected left cell position to compute for.
   * @param lc    The contents of the left cell position to compute with.
   * @param rp    The selected right cell position to compute for.
   * @param rc    The contents of the right cell position to compute with.
   * @param rem   The remaining coordinates.
   *
   * @note The return value is an `Option` to allow, for example, upper
   *       or lower triangular matrices to be returned (this can be done by
   *       comparing the selected coordinates)
   */
  def compute[P <: Position, D <: Dimension](slice: Slice[P, D],
    lp: Slice[P, D]#S, lc: Content, rp: Slice[P, D]#S, rc: Content,
      rem: Slice[P, D]#R): Option[Cell[rem.M]] = {
    val coordinate = name.format(lp.toShortString(separator),
      rp.toShortString(separator))

    (comparer.check(lp, rp), lc.value.asDouble, rc.value.asDouble) match {
        case (true, Some(l), Some(r)) =>
          Some((rem.prepend(coordinate),
            Content(ContinuousSchema[Codex.DoubleCodex](),
              if (inverse) compute(r, l) else compute(l, r))))
        case _ => None
      }
  }

  protected def compute(l: Double, r: Double): Double
}

/** Add two values. */
case class Plus(name: String = "(%s+%s)", separator: String = "|",
  comparer: Comparer = Lower) extends Operator with Compute
  with DoubleOperator {
  val inverse: Boolean = false
  protected def compute(l: Double, r: Double) = l + r
}

/** Subtract two values. */
case class Minus(name: String = "(%s-%s)", separator: String = "|",
  inverse: Boolean = false, comparer: Comparer = Lower) extends Operator
  with Compute with DoubleOperator {
  protected def compute(l: Double, r: Double) = l - r
}

/** Multiply two values. */
case class Times(name: String = "(%s*%s)", separator: String = "|",
  comparer: Comparer = Lower) extends Operator with Compute
  with DoubleOperator {
  val inverse: Boolean = false
  protected def compute(l: Double, r: Double) = l * r
}

/** Divide two values. */
case class Divide(name: String = "(%s/%s)", separator: String = "|",
  inverse: Boolean = false, comparer: Comparer = Lower) extends Operator
  with Compute with DoubleOperator {
  protected def compute(l: Double, r: Double) = l / r
}

/**
 * Concatenate two cells.
 *
 * @param name      Pattern for the new name of the pairwise coordinate. Use
 *                  `%[12]$``s` for the string representations of the
 *                  coordinate.
 * @param value     Pattern for the new (string) value of the pairwise contents.
 *                  Use `%[12]$``s` for the string representations of the
 *                  content.
 * @param separator Separator to use when writing positions to string.
 * @param inverse   Indicator if pairwise operator `f()` should be called as
 *                  `f(l, r)` or as `f(r, l)`.
 * @param comparer  Comparer object defining which pairwise operations should
 *                  be computed.
 */
case class Concatenate(name: String = "(%s,%s)", value: String = "%s,%s",
  separator: String = "|", inverse: Boolean = false,
  comparer: Comparer = Lower) extends Operator with Compute {
  def compute[P <: Position, D <: Dimension](slice: Slice[P, D],
    lp: Slice[P, D]#S, lc: Content, rp: Slice[P, D]#S, rc: Content,
      rem: Slice[P, D]#R): Option[Cell[rem.M]] = {
    val coordinate = name.format(lp.toShortString(separator),
      rp.toShortString(separator))
    val content = value.format(lc.value.toShortString,
      rc.value.toShortString)

    comparer.check(lp, rp) match {
      case true => Some((rem.prepend(coordinate),
        Content(NominalSchema[Codex.StringCodex](), content)))
      case false => None
    }
  }
}

