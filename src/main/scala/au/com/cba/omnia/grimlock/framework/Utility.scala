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

package au.com.cba.omnia.grimlock.framework.utility

import au.com.cba.omnia.grimlock.framework._
import au.com.cba.omnia.grimlock.framework.content._
import au.com.cba.omnia.grimlock.framework.position._

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

  private def flatten[V](tlo: Option[List[V]]): List[V] = tlo.getOrElse(List())
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
  def apply[P <: Position](pos: P, con: Content): Collection[Cell[P]] = Collection(Some(Left(Cell(pos, con))))

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
    if (all || str.contains(special)) { quote + str + quote } else { str }
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

/** Class that ensures three types are different. */
sealed class Distinct3[A, B, C]

/** Companion object that ensures three types are different. */
object Distinct3 {
  /** Implicit enforcing three types are different. */
  implicit def nequal[A, B, C](implicit ev1: A =!= B, ev2: A =!= C,
    ev3: B =!= C): Distinct3[A, B, C] = new Distinct3[A, B, C]
}

/** Class that ensures four types are different. */
sealed class Distinct4[A, B, C, D]

/** Companion object that ensures four types are different. */
object Distinct4 {
  /** Shorthand for `Distinct3`. */
  type D3[A, B, C] = Distinct3[A, B, C]

  /** Implicit enforcing four types are different. */
  implicit def nequal[A, B, C, D](implicit ev1: D3[A, B, C], ev2: D3[A, B, D], ev3: D3[A, C, D],
    ev4: D3[B, C, D]): Distinct4[A, B, C, D] = new Distinct4[A, B, C, D]
}

/** Class that ensures five types are different. */
sealed class Distinct5[A, B, C, D, E]

/** Companion object that ensures five types are different. */
object Distinct5 {
  /** Shorthand for `Distinct4`. */
  type D4[A, B, C, D] = Distinct4[A, B, C, D]

  /** Implicit enforcing five types are different. */
  implicit def nequal[A, B, C, D, E](implicit ev1: D4[A, B, C, D], ev2: D4[A, B, C, E], ev3: D4[A, B, D, E],
    ev4: D4[A, C, D, E], ev5: D4[B, C, D, E]): Distinct5[A, B, C, D, E] = new Distinct5[A, B, C, D, E]
}

/** Class that ensures six types are different. */
sealed class Distinct6[A, B, C, D, E, F]

/** Companion object that ensures six types are different. */
object Distinct6 {
  /** Shorthand for `Distinct5`. */
  type D5[A, B, C, D, E] = Distinct5[A, B, C, D, E]

  /** Implicit enforcing six types are different. */
  implicit def nequal[A, B, C, D, E, F](implicit ev1: D5[A, B, C, D, E], ev2: D5[A, B, C, D, F],
    ev3: D5[A, B, C, E, F], ev4: D5[A, B, D, E, F], ev5: D5[A, C, D, E, F],
    ev6: D5[B, C, D, E, F]): Distinct6[A, B, C, D, E, F] = new Distinct6[A, B, C, D, E, F]
}

/** Class that ensures seven types are different. */
sealed class Distinct7[A, B, C, D, E, F, G]

/** Companion object that ensures seven types are different. */
object Distinct7 {
  /** Shorthand for `Distinct6`. */
  type D6[A, B, C, D, E, F] = Distinct6[A, B, C, D, E, F]

  /** Implicit enforcing seven types are different. */
  implicit def nequal[A, B, C, D, E, F, G](implicit ev1: D6[A, B, C, D, E, F], ev2: D6[A, B, C, D, E, G],
    ev3: D6[A, B, C, D, F, G], ev4: D6[A, B, C, E, F, G], ev5: D6[A, B, D, E, F, G], ev6: D6[A, C, D, E, F, G],
    ev7: D6[B, C, D, E, F, G]): Distinct7[A, B, C, D, E, F, G] = new Distinct7[A, B, C, D, E, F, G]
}

/** Class that ensures eight types are different. */
sealed class Distinct8[A, B, C, D, E, F, G, H]

/** Companion object that ensures eight types are different. */
object Distinct8 {
  /** Shorthand for `Distinct7`. */
  type D7[A, B, C, D, E, F, G] = Distinct7[A, B, C, D, E, F, G]

  /** Implicit enforcing eight types are different. */
  implicit def nequal[A, B, C, D, E, F, G, H](implicit ev1: D7[A, B, C, D, E, F, G],
    ev2: D7[A, B, C, D, E, F, H], ev3: D7[A, B, C, D, E, G, H], ev4: D7[A, B, C, D, F, G, H],
    ev5: D7[A, B, C, E, F, G, H], ev6: D7[A, B, D, E, F, G, H], ev7: D7[A, C, D, E, F, G, H],
    ev8: D7[B, C, D, E, F, G, H]): Distinct8[A, B, C, D, E, F, G, H] = new Distinct8[A, B, C, D, E, F, G, H]
}

/** Class that ensures nine types are different. */
sealed class Distinct9[A, B, C, D, E, F, G, H, I]

/** Companion object that ensures nine types are different. */
object Distinct9 {
  /** Shorthand for `Distinct8`. */
  type D8[A, B, C, D, E, F, G, H] = Distinct8[A, B, C, D, E, F, G, H]

  /** Implicit enforcing nine types are different. */
  implicit def nequal[A, B, C, D, E, F, G, H, I](implicit ev1: D8[A, B, C, D, E, F, G, H],
    ev2: D8[A, B, C, D, E, F, G, I], ev3: D8[A, B, C, D, E, F, H, I], ev4: D8[A, B, C, D, E, G, H, I],
    ev5: D8[A, B, C, D, F, G, H, I], ev6: D8[A, B, C, E, F, G, H, I], ev7: D8[A, B, D, E, F, G, H, I],
    ev8: D8[A, C, D, E, F, G, H, I], ev9: D8[B, C, D, E, F, G, H, I]): Distinct9[A, B, C, D, E, F, G, H, I] =
    new Distinct9[A, B, C, D, E, F, G, H, I]
}

/** Base trait for specifying `B` should be one of a number of types. */
trait OneOf {
  /** Check if `B` conforms to the set of allowed types. */
  type L[B]
}

/** Companion object to `OneOf` trait. */
object OneOf {
  private type Not[A] = A => Nothing
  private type NotNot[A] = Not[Not[A]]

  private type Or2[R, S] = Not[Not[R] with Not[S]]
  private type Or3[R, S, T] = Not[Not[R] with Not[S] with Not[T]]
  private type Or4[R, S, T, U] = Not[Not[R] with Not[S] with Not[T] with Not[U]]
  private type Or5[R, S, T, U, V] = Not[Not[R] with Not[S] with Not[T] with Not[U] with Not[V]]
  private type Or6[R, S, T, U, V, W] = Not[Not[R] with Not[S] with Not[T] with Not[U] with Not[V] with Not[W]]
  private type Or7[R, S, T, U, V, W, X] = Not[Not[R] with Not[S] with Not[T] with Not[U] with Not[V] with Not[W] with Not[X]]
  private type Or8[R, S, T, U, V, W, X, Y] = Not[Not[R] with Not[S] with Not[T] with Not[U] with Not[V] with Not[W] with Not[X] with Not[Y]]
  private type Or9[R, S, T, U, V, W, X, Y, Z] = Not[Not[R] with Not[S] with Not[T] with Not[U] with Not[V] with Not[W] with Not[X] with Not[Y] with Not[Z]]

  /** Type constraint to ensure `B` is either `R` or `S`. */
  type OneOf2[R, S] = OneOf { type L[B] = NotNot[B] <:< Or2[R, S] }

  /** Type constraint to ensure `B` is either `R`, `S` or `T`. */
  type OneOf3[R, S, T] = OneOf { type L[B] = NotNot[B] <:< Or3[R, S, T] }

  /** Type constraint to ensure `B` is either `R`, `S`, `T` or `U`. */
  type OneOf4[R, S, T, U] = OneOf { type L[B] = NotNot[B] <:< Or4[R, S, T, U] }

  /** Type constraint to ensure `B` is either `R`, `S`, `T`, `U` or `V`. */
  type OneOf5[R, S, T, U, V] = OneOf { type L[B] = NotNot[B] <:< Or5[R, S, T, U, V] }

  /** Type constraint to ensure `B` is either `R`, `S`, `T`, `U`, `V` or `W`. */
  type OneOf6[R, S, T, U, V, W] = OneOf { type L[B] = NotNot[B] <:< Or6[R, S, T, U, V, W] }

  /** Type constraint to ensure `B` is either `R`, `S`, `T`, `U`, `V`, `W` or `X`. */
  type OneOf7[R, S, T, U, V, W, X] = OneOf { type L[B] = NotNot[B] <:< Or7[R, S, T, U, V, W, X] }

  /** Type constraint to ensure `B` is either `R`, `S`, `T`, `U`, `V`, `W`, `X` or `Y`. */
  type OneOf8[R, S, T, U, V, W, X, Y] = OneOf { type L[B] = NotNot[B] <:< Or8[R, S, T, U, V, W, X, Y] }

  /** Type constraint to ensure `B` is either `R`, `S`, `T`, `U`, `V`, `W, `X`, `Y` or `Z`. */
  type OneOf9[R, S, T, U, V, W, X, Y, Z] = OneOf { type L[B] = NotNot[B] <:< Or9[R, S, T, U, V, W, X, Y, Z] }
}

