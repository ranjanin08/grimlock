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

import com.twitter.scalding._

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
 * @note This class represents the variable type along the dimensions of a matrix.
 */
class Types[P <: Position](protected val data: TypedPipe[(P, Type)]) extends Persist[(P, Type)] {
  protected def toString(t: (P, Type), separator: String, descriptive: Boolean): String = {
    descriptive match {
      case true => t._1.toString + separator + t._2.toString
      case false => t._1.toShortString(separator) + separator + t._2.name
    }
  }
}

object Types {
  /** Conversion from `TypedPipe[(Position, Type)]` to a `Types`. */
  implicit def TPPT2T[P <: Position](data: TypedPipe[(P, Type)]): Types[P] = new Types(data)
}

