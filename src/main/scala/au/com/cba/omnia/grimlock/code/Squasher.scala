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
import au.com.cba.omnia.grimlock.Matrix.Cell
import au.com.cba.omnia.grimlock.position._

/** Base trait for squashing. */
trait Squasher {
}

/** Base trait for reducing two cells. */
trait Reduce { self: Squasher =>
  /**
   * Reduce two cells.
   *
   * @param dim The dimension along which to squash.
   * @param x   The first cell to reduce.
   * @param y   The second cell to reduce.
   */
  def reduce[P <: Position](dim: Dimension, x: Cell[P], y: Cell[P]): Cell[P]
}

/** Base trait for reducing two cells with a user provided value. */
trait ReduceWithValue { self: Squasher =>
  /** Type of the external value. */
  type V

  /**
   * Reduce two cells with a user supplied value.
   *
   * @param dim The dimension along which to squash.
   * @param x   The first cell to reduce.
   * @param y   The second cell to reduce.
   * @param ext The user define the value.
   */
  def reduce[P <: Position](dim: Dimension, x: Cell[P], y: Cell[P],
    ext: V): Cell[P]
}

/**
 * Convenience trait for a squasher that selects with or without using a
 * user supplied value.
 */
trait ReduceAndWithValue extends Reduce with ReduceWithValue { self: Squasher =>
  type V = Any

  def reduce[P <: Position](dim: Dimension, x: Cell[P], y: Cell[P], ext: V) = {
    reduce(dim, x, y)
  }
}

