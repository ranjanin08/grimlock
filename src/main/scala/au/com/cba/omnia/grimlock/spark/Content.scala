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

package au.com.cba.omnia.grimlock.content

import au.com.cba.omnia.grimlock._
import au.com.cba.omnia.grimlock.encoding._
import au.com.cba.omnia.grimlock.content.metadata._

import org.apache.spark.rdd._

/**
 * Rich wrapper around a `RDD[Content]`.
 *
 * @param data The `RDD[Content]`.
 */
class SparkContents(val data: RDD[Content]) extends Contents with SparkPersist[Content] {
  type U[A] = RDD[A]
}

/** Companion object for the `SparkContents` class. */
object SparkContents {
  /** Converts a `RDD[Content]` to a `Contents`. */
  implicit def RDDC2C(data: RDD[Content]): SparkContents = new SparkContents(data)
}

