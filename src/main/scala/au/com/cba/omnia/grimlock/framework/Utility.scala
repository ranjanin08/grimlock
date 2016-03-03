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

import shapeless._
import shapeless.ops.hlist._

/** Type class that ensures three types are different. */
trait Distinct[P <: Product] {

}

object Distinct {
  implicit def default[P <: Product](implicit gen: Generic.Aux[P, HNil]) = new Distinct[P] {}

  implicit def isDistinct[P <: Product, L <: HList, C <: Coproduct](implicit
                                                                   gen: Generic.Aux[P, L],
                                                                   toCoproduct: ToCoproduct.Aux[L, C],
                                                                   toSum: ToSum.Aux[L, C]
                                                                  ) = new Distinct[P] {}
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

  type OneOf[T] = {
    type Or[S] = (Disjunction {type D = Not[T]})#Or[S]
  }




  type Contains[S, T <: Disjunction] = NotNot[S] <:< Not[T#D]
  type In[S, T <: Disjunction] = Contains[S, T]
  type Is[S, T] = Contains[S, OneOf[T]#Or[Nothing]]

}

object UnionTypes extends UnionTypes
