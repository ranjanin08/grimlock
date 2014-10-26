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

case class Multiply() extends Operator with Compute {
  def compute[P <: Position with ModifyablePosition, D <: Dimension](
    slice: Slice[P, D], left: Cell[P], right: Cell[P]): Option[Cell[P#S]] = {
    (slice.selected(left._1).compare(slice.selected(right._1)) > 0,
      left._2.value.asDouble, right._2.value.asDouble) match {
        case (true, Some(l), Some(r)) =>
          Some((left._1.merge(right._1, "(%s*%s)"),
            Content(ContinuousSchema[Codex.DoubleCodex](), l * r)))
        case _ => None
      }
  }
}

