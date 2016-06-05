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

package au.com.cba.omnia.grimlock.spark

import au.com.cba.omnia.grimlock.framework.{ Type, Types => FwTypes }
import au.com.cba.omnia.grimlock.framework.position._

import org.apache.spark.rdd.RDD

/**
 * Rich wrapper around a `RDD[(Position, Type)]`.
 *
 * @param data `RDD[(Position, Type)]`.
 *
 * @note This class represents the variable type along the dimensions of a matrix.
 */
case class Types[P <: Position[P]](data: RDD[(P, Type)]) extends FwTypes[P] with Persist[(P, Type)] {
  def saveAsText(file: String, writer: TextWriter)(implicit ctx: C): U[(P, Type)] = saveText(file, writer)
}

/** Companion object for the Spark `Types` class. */
object Types {
  /** Conversion from `RDD[(Position, Type)]` to a `Types`. */
  implicit def RDDPT2RDDT[P <: Position[P]](data: RDD[(P, Type)]): Types[P] = Types(data)
}

