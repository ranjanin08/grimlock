// Copyright 2014,2015 Commonwealth Bank of Australia
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

package au.com.cba.omnia.grimlock.scalding

import au.com.cba.omnia.grimlock.framework.{ Type, Types => BaseTypes }
import au.com.cba.omnia.grimlock.framework.position._

import com.twitter.scalding.typed.TypedPipe

/**
 * Rich wrapper around a `TypedPipe[(Position, Type)]`.
 *
 * @param data `TypedPipe[(Position, Type)]`.
 *
 * @note This class represents the variable type along the dimensions of a matrix.
 */
class Types[P <: Position](val data: TypedPipe[(P, Type)]) extends BaseTypes[P] with Persist[(P, Type)] {
  type U[A] = TypedPipe[A]
}

/** Companion object for the Scalding `Types` class. */
object Types {
  /** Conversion from `TypedPipe[(Position, Type)]` to a `Types`. */
  implicit def TPPT2TPT[P <: Position](data: TypedPipe[(P, Type)]): Types[P] = new Types(data)
}

