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

package au.com.cba.omnia.grimlock.library.sample

import au.com.cba.omnia.grimlock.framework._
import au.com.cba.omnia.grimlock.framework.content._
import au.com.cba.omnia.grimlock.framework.position._
import au.com.cba.omnia.grimlock.framework.sample._

import scala.util.Random

/**
 * Randomly sample to a ratio.
 *
 * @param ratio The sampling ratio.
 * @param rnd   The random number generator.
 *
 * @note This randomly samples ignoring the position.
 */
case class RandomSample[P <: Position](ratio: Double, rnd: Random = new Random()) extends Sampler[P] {
  def select(cell: Cell[P]): Boolean = rnd.nextDouble() < ratio
}

/**
 * Sample based on the hash code of a dimension.
 *
 * @param dim   The dimension to sample from.
 * @param ratio The sample ratio (relative to `base`).
 * @param base  The base of the sampling ratio.
 */
case class HashSample[P <: Position](dim: Dimension, ratio: Int, base: Int) extends Sampler[P] {
  def select(cell: Cell[P]): Boolean = math.abs(cell.position(dim).hashCode % base) < ratio
}

/**
 * Sample to a defined size based on the hash code of a dimension.
 *
 * @param dim   The dimension to sample from.
 * @param count Object that will extract, for `cell`, its corresponding number of values.
 * @param size  The size to sample to.
 */
case class HashSampleToSize[P <: Position, W](dim: Dimension, count: Extract[P, W, Double],
  size: Long) extends SamplerWithValue[P] {
  type V = W

  def selectWithValue(cell: Cell[P], ext: V): Boolean = {
    count.extract(cell, ext) match {
      case Some(cnt) => math.abs(cell.position(dim).hashCode % cnt) < size
      case None => false
    }
  }
}

