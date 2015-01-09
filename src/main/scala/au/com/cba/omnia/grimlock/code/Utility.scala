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

/** Collection of simple utility functions. */
object Miscellaneous {
  private def flatten[T](tlo: Option[List[T]]): List[T] = {
    tlo match {
      case Some(tl) => tl
      case None => List()
    }
  }

  type Collection[T] = Option[Either[T, List[T]]]

  /** Transforms a `Collection[T]` to a `List[T]`. */
  def mapFlatten[T](c: Collection[T]): List[T] = {
    flatten(c.map {
      case Left(t) => List(t)
      case Right(tl) => tl
    })
  }

  /** Transforms a `Collection[T]` together with a single `U` to a `List[(T, U)]`. */
  def mapFlatten[T, U](c: Collection[T], u: U): List[(T, U)] = {
    flatten(c.map {
      case Left(t) => List((t, u))
      case Right(tl) => tl.map { case t => (t, u) }
    })
  }
}

