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

package au.com.cba.omnia.grimlock.utility

import au.com.cba.omnia.grimlock.content._
import au.com.cba.omnia.grimlock.Matrix.Cell
import au.com.cba.omnia.grimlock.position._

/**
 * This code is a carbon copy of an answer on stackoverflow:
 *
 *   http://stackoverflow.com/questions/6909053/enforce-type-difference
 *
 * It can be used to ensure that the types of arguments are not equal.
 */
sealed class =!=[A, B]

/**
 * This code is a carbon copy of an answer on stackoverflow:
 *
 *   http://stackoverflow.com/questions/6909053/enforce-type-difference
 *
 * It can be used to ensure that the types of arguments are not equal.
 */
trait LowerPriorityImplicits {
  /** do not call explicitly! */
  implicit def equal[A]: =!=[A, A] = sys.error("should not be called")
}

/**
 * This code is a carbon copy of an answer on stackoverflow:
 *
 *   http://stackoverflow.com/questions/6909053/enforce-type-difference
 *
 * It can be used to ensure that the types of arguments are not equal.
 */
object =!= extends LowerPriorityImplicits {
  /** do not call explicitly! */
  implicit def nequal[A, B]: =!=[A, B] = new =!=[A, B]
}

/** Collection of zero, one or more objects. */
case class Collection[T](data: Option[Either[T, List[T]]]) {
  /** Check if the collection is empty. */
  def isEmpty(): Boolean = data.isEmpty

  /** Transforms a `Collection[T]` to a `List[T]`. */
  def toList(): List[T] = {
    flatten(data.map {
      case Left(t) => List(t)
      case Right(tl) => tl
    })
  }

  /** Transforms a `Collection[T]` together with a single `U` to a `List[(T, U)]`. */
  def toList[U](u: U): List[(T, U)] = {
    flatten(data.map {
      case Left(t) => List((t, u))
      case Right(tl) => tl.map { case t => (t, u) }
    })
  }

  private def flatten[V](tlo: Option[List[V]]): List[V] = {
    tlo match {
      case Some(tl) => tl
      case None => List()
    }
  }
}

object Collection {
  /** Create an empty collection. */
  def apply[T](): Collection[T] = Collection(Option.empty[Either[T, List[T]]])

  /**
   * Create a collection with a single object.
   *
   * @param t The object to wrap in a collection.
   */
  def apply[T](t: T): Collection[T] = Collection(Some(Left(t)))

  /**
   * Create a collection with a list of objects.
   *
   * @param t The list of objects to wrap in a collection.
   */
  def apply[T](t: List[T]): Collection[T] = Collection(Some(Right(t)))

  /**
   * Create a collection from a position and content.
   *
   * @param pos The position.
   * @param con The content.
   */
  def apply[P <: Position](pos: P, con: Content): Collection[Cell[P]] = Collection(Some(Left((pos, con))))

  /** Create an empty collection. */
  def empty[T](): Collection[T] = Collection[T]()
}

/** Base trait for ecaping special characters in a string. */
trait Escape {
  /**
   * Escape a string.
   *
   * @param str     The string to escape.
   * @param special The special character to escape.
   *
   * @return The escaped string.
   */
  def escape(str: String, special: String): String
}

/**
 * Escape a string by enclosing it in quotes.
 *
 * @param all   Indicator if all strings should be quoted.
 * @param quote The quoting character to use.
 */
case class Quote(all: Boolean = false, quote: String = "\"") extends Escape {
  def escape(str: String, special: String): String = {
    (all || str.contains(special)) match {
      case true => quote + str + quote
      case false => str
    }
  }
}

/**
 * Escape a string by replacing the special character.
 *
 * @param pattern The escape pattern to use. Use `%1$``s` to substitute the special character.
 */
case class Replace(pattern: String = "\\%1$s") extends Escape {
  def escape(str: String, special: String): String = str.replaceAllLiterally(special, pattern.format(special))
}

