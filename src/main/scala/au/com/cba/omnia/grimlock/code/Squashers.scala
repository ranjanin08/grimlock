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

package au.com.cba.omnia.grimlock.squash

import au.com.cba.omnia.grimlock.content._
import au.com.cba.omnia.grimlock.encoding._
import au.com.cba.omnia.grimlock.Matrix.Cell
import au.com.cba.omnia.grimlock.position._

/**
 * Reduce two cells preserving the cell with maximal value for the coordinate
 * of the dimension being squashed.
 */
case class PreservingMaxPosition() extends Squasher with Reduce {
  def reduce[P <: Position](dim: Dimension, xpos: P, xcon: Content, ypos: P,
    ycon: Content): Cell[P] = {
    (Value.Ordering.compare(xpos.get(dim), ypos.get(dim)) > 0) match {
      case true => (xpos, xcon)
      case false => (ypos, ycon)
    }
  }
}

/**
 * Reduce two cells preserving the cell with minimal value for the coordinate
 * of the dimension being squashed.
 */
case class PreservingMinPosition() extends Squasher with Reduce {
  def reduce[P <: Position](dim: Dimension, xpos: P, xcon: Content, ypos: P,
    ycon: Content): Cell[P] = {
    (Value.Ordering.compare(xpos.get(dim), ypos.get(dim)) < 0) match {
      case true => (xpos, xcon)
      case false => (ypos, ycon)
    }
  }
}

