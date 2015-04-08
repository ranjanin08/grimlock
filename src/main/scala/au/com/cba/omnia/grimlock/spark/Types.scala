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

package au.com.cba.omnia.grimlock

import au.com.cba.omnia.grimlock.position._

import org.apache.spark.rdd._

/**
 * Rich wrapper around a `RDD[(Position, Type)]`.
 *
 * @param data `RDD[(Position, Type)]`.
 *
 * @note This class represents the variable type along the dimensions of a matrix.
 */
class SparkTypes[P <: Position](val data: RDD[(P, Type)]) extends Types[P] with SparkPersist[(P, Type)] {
  type U[A] = RDD[A]
}

/** Companion object for the `SparkTypes` class. */
object SparkTypes {
  /** Conversion from `RDD[(Position, Type)]` to a `SparkTypes`. */
  implicit def RDDPT2T[P <: Position](data: RDD[(P, Type)]): SparkTypes[P] = new SparkTypes(data)
}

