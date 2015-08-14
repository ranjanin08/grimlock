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

/** Base trait for specifying `X` should be one of a number of types. */
trait OneOf {
  /** Verify that `X` belongs to the set of allowed types. */
  type V[X]
}

/** Companion object to `OneOf` trait. */
object OneOf {
  private type Not[X] = X => Nothing
  private type NotNot[X] = Not[Not[X]]

  private type Or2[A, B] = Not[Not[A] with Not[B]]
  private type Or3[A, B, C] = Not[Not[A] with Not[B] with Not[C]]
  private type Or4[A, B, C, D] = Not[Not[A] with Not[B] with Not[C] with Not[D]]
  private type Or5[A, B, C, D, E] = Not[Not[A] with Not[B] with Not[C] with Not[D] with Not[E]]
  private type Or6[A, B, C, D, E, F] = Not[Not[A] with Not[B] with Not[C] with Not[D] with Not[E] with Not[F]]
  private type Or7[A, B, C, D, E, F, G] = Not[Not[A] with Not[B] with Not[C] with Not[D] with Not[E] with Not[F] with Not[G]]
  private type Or8[A, B, C, D, E, F, G, H] = Not[Not[A] with Not[B] with Not[C] with Not[D] with Not[E] with Not[F] with Not[G] with Not[H]]
  private type Or9[A, B, C, D, E, F, G, H, I] = Not[Not[A] with Not[B] with Not[C] with Not[D] with Not[E] with Not[F] with Not[G] with Not[H] with Not[I]]
  private type Or10[A, B, C, D, E, F, G, H, I, J] = Not[Not[A] with Not[B] with Not[C] with Not[D] with Not[E] with Not[F] with Not[G] with Not[H] with Not[I] with Not[J]]
  private type Or11[A, B, C, D, E, F, G, H, I, J, K] = Not[Not[A] with Not[B] with Not[C] with Not[D] with Not[E] with Not[F] with Not[G] with Not[H] with Not[I] with Not[J] with Not[K]]
  private type Or12[A, B, C, D, E, F, G, H, I, J, K, L] = Not[Not[A] with Not[B] with Not[C] with Not[D] with Not[E] with Not[F] with Not[G] with Not[H] with Not[I] with Not[J] with Not[K] with Not[L]]
  private type Or13[A, B, C, D, E, F, G, H, I, J, K, L, M] = Not[Not[A] with Not[B] with Not[C] with Not[D] with Not[E] with Not[F] with Not[G] with Not[H] with Not[I] with Not[J] with Not[K] with Not[L] with Not[M]]
  private type Or14[A, B, C, D, E, F, G, H, I, J, K, L, M, N] = Not[Not[A] with Not[B] with Not[C] with Not[D] with Not[E] with Not[F] with Not[G] with Not[H] with Not[I] with Not[J] with Not[K] with Not[L] with Not[M] with Not[N]]
  private type Or15[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O] = Not[Not[A] with Not[B] with Not[C] with Not[D] with Not[E] with Not[F] with Not[G] with Not[H] with Not[I] with Not[J] with Not[K] with Not[L] with Not[M] with Not[N] with Not[O]]

  /** Type constraint to ensure `X` is `A`. */
  type OneOf1[A] = OneOf { type V[X] = X <:< A }

  /** Type constraint to ensure `X` is either `A` or `B`. */
  type OneOf2[A, B] = OneOf { type V[X] = NotNot[X] <:< Or2[A, B] }

  /** Type constraint to ensure `X` is either `A`, `B` or `C`. */
  type OneOf3[A, B, C] = OneOf { type V[X] = NotNot[X] <:< Or3[A, B, C] }

  /** Type constraint to ensure `X` is either `A`, `B`, `C` or `D`. */
  type OneOf4[A, B, C, D] = OneOf { type V[X] = NotNot[X] <:< Or4[A, B, C, D] }

  /** Type constraint to ensure `X` is either `A`, `B`, `C`, `D` or `E`. */
  type OneOf5[A, B, C, D, E] = OneOf { type V[X] = NotNot[X] <:< Or5[A, B, C, D, E] }

  /** Type constraint to ensure `X` is either `A`, `B`, `C`, `D`, `E` or `F`. */
  type OneOf6[A, B, C, D, E, F] = OneOf { type V[X] = NotNot[X] <:< Or6[A, B, C, D, E, F] }

  /** Type constraint to ensure `X` is either `A`, `B`, `C`, `D`, `E`, `F` or `G`. */
  type OneOf7[A, B, C, D, E, F, G] = OneOf { type V[X] = NotNot[X] <:< Or7[A, B, C, D, E, F, G] }

  /** Type constraint to ensure `X` is either `A`, `B`, `C`, `D`, `E`, `F`, `G` or `H`. */
  type OneOf8[A, B, C, D, E, F, G, H] = OneOf { type V[X] = NotNot[X] <:< Or8[A, B, C, D, E, F, G, H] }

  /** Type constraint to ensure `X` is either `A`, `B`, `C`, `D`, `E`, `F, `G`, `H` or `I`. */
  type OneOf9[A, B, C, D, E, F, G, H, I] = OneOf { type V[X] = NotNot[X] <:< Or9[A, B, C, D, E, F, G, H, I] }

  /** Type constraint to ensure `X` is either `A`, `B`, `C`, `D`, `E`, `F, `G`, `H`, `I` or `J`. */
  type OneOf10[A, B, C, D, E, F, G, H, I, J] = OneOf { type V[X] = NotNot[X] <:< Or10[A, B, C, D, E, F, G, H, I, J] }

  /** Type constraint to ensure `X` is either `A`, `B`, `C`, `D`, `E`, `F, `G`, `H`, `I`, `J` or `K`. */
  type OneOf11[A, B, C, D, E, F, G, H, I, J, K] = OneOf { type V[X] = NotNot[X] <:< Or11[A, B, C, D, E, F, G, H, I, J, K] }

  /** Type constraint to ensure `X` is either `A`, `B`, `C`, `D`, `E`, `F, `G`, `H`, `I`, `J`, `K` or `L`. */
  type OneOf12[A, B, C, D, E, F, G, H, I, J, K, L] = OneOf { type V[X] = NotNot[X] <:< Or12[A, B, C, D, E, F, G, H, I, J, K, L] }

  /** Type constraint to ensure `X` is either `A`, `B`, `C`, `D`, `E`, `F, `G`, `H`, `I`, `J`, `K`, `L` or `M`. */
  type OneOf13[A, B, C, D, E, F, G, H, I, J, K, L, M] = OneOf { type V[X] = NotNot[X] <:< Or13[A, B, C, D, E, F, G, H, I, J, K, L, M] }

  /** Type constraint to ensure `X` is either `A`, `B`, `C`, `D`, `E`, `F, `G`, `H`, `I`, `J`, `K`, `L`, `M` or `N`. */
  type OneOf14[A, B, C, D, E, F, G, H, I, J, K, L, M, N] = OneOf { type V[X] = NotNot[X] <:< Or14[A, B, C, D, E, F, G, H, I, J, K, L, M, N] }

  /** Type constraint to ensure `X` is either `A`, `B`, `C`, `D`, `E`, `F, `G`, `H`, `I`, `J`, `K`, `L`, `M`, `N` or `O`. */
  type OneOf15[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O] = OneOf { type V[X] = NotNot[X] <:< Or15[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O] }
}

