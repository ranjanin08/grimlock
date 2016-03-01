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

package au.com.cba.omnia.grimlock.framework.utility

/** Base trait for ecaping special characters in a string. */
trait Escape {
  /** The special character to escape. */
  val special: String

  /**
   * Escape a string.
   *
   * @param str The string to escape.
    * @return The escaped string.
   */
  def escape(str: String): String
}

/**
 * Escape a string by enclosing it in quotes.
 *
 * @param special The special character to quote.
 * @param quote   The quoting character to use.
 * @param all     Indicator if all strings should be quoted.
 */
case class Quote(special: String, quote: String = "\"", all: Boolean = false) extends Escape {
  def escape(str: String): String = {
    if (all || str.contains(special)) { quote + str + quote } else { str }
  }
}

/**
 * Escape a string by replacing the special character.
 *
 * @param special The special character to replace.
 * @param pattern The escape pattern to use. Use `%1$``s` to substitute the special character.
 */
case class Replace(special: String, pattern: String = "\\%1$s") extends Escape {
  def escape(str: String): String = str.replaceAllLiterally(special, pattern.format(special))
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

trait UnionTypes {
  type Not[A] = A => Nothing
  type NotNot[A] = Not[Not[A]]

  trait Disjunction {
    self =>
    type D
    type Or[S] = Disjunction {
      type D = self.D with Not[S]
    }
  }

  type IsA[T] = {
    type Or[S] = (Disjunction {type D = Not[T]})#Or[S]
  }

  type Contains[S, T <: Disjunction] = NotNot[S] <:< Not[T#D]
  type In[S, T <: Disjunction] = Contains[S, T]
}

object UnionTypes extends UnionTypes
