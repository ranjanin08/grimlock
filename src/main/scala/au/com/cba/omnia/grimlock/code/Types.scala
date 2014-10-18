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

package au.com.cba.omnia.grimlock

import au.com.cba.omnia.grimlock.position._

import cascading.flow.FlowDef
import com.twitter.scalding._
import com.twitter.scalding.TDsl._, Dsl._

/**
 * Base class for variable types.
 *
 * @param base Optional parent variable type.
 * @param name Name of the variable type.
 */
case class Type(base: Option[Type], name: String) {
  /**
   * Check if `this` is a specialisation of `that`.
   *
   * @param that Variable type to check against.
   */
  def isSpecialisationOf(that: Type): Boolean = {
    base match {
      case Some(vt) => (this == that) || vt.isSpecialisationOf(that)
      case None => (this == that)
    }
  }

  /** Returns the most general super type of `this`. */
  def getGeneralisation(): Type = {
    base match {
      case Some(vt) => vt.getGeneralisation()
      case None => this
    }
  }

  /** Returns the name of `this` type. */
  override def toString = name.capitalize

  /** Converts the type to a consise (terse) string. */
  def toShortString = name
}

/** Compantion object to `Type` class. */
object Type {
  /** Type for when the type is mixed. */
  val Mixed = Type(None, "mixed")
  /** Type for numeric types. */
  val Numerical = Type(None, "numerical")
  /** Type for continuous types. */
  val Continuous = Type(Some(Numerical), "continuous")
  /** Type for discrete types. */
  val Discrete = Type(Some(Numerical), "discrete")
  /** Type for categorical types. */
  val Categorical = Type(None, "categorical")
  /** Type for nominal types. */
  val Nominal = Type(Some(Categorical), "nominal")
  /** Type for ordinal types. */
  val Ordinal = Type(Some(Categorical), "ordinal")
  /** Type for date types. */
  val Date = Type(None, "date")
  /** Type for event types. */
  val Event = Type(None, "event")
}

/**
 * Rich wrapper around a `TypedPipe[(Position, Type)]`.
 *
 * @param data `TypedPipe[(Position, Type)]`.
 *
 * @note This class represents the variable type along the dimensions of
 *       a matrix.
 */
class Types[P <: Position](data: TypedPipe[(P, Type)]) {
  /**
   * Persist `Types` to disk.
   *
   * @param file        Name of the output file.
   * @param separator   Separator to use between position and type.
   * @param descriptive Indicates if the output should be descriptive.
   *
   * @return A Scalding `TypedPipe[(P, Type)]` which is this objects's data.
   */
  def persist(file: String, separator: String = "|",
    descriptive: Boolean = false)(implicit flow: FlowDef,
      mode: Mode): TypedPipe[(P, Type)] = {
    data
      .map {
        case (p, t) => descriptive match {
          case true => p.toString + separator + t.toString
          case false => p.toShortString(separator) + separator + t.name
        }
      }
      .toPipe('line)
      .write(TextLine(file))

    data
  }
}

object Types {
  /** Conversion from `TypedPipe[(Position, Type)]` to a `Types`. */
  implicit def TPPT2T[P <: Position](data: TypedPipe[(P, Type)]): Types[P] = {
    new Types(data)
  }
}

