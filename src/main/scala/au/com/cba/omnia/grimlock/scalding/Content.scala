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

package au.com.cba.omnia.grimlock.scalding.content

import au.com.cba.omnia.grimlock.framework.content.{ Contents => FwContents, IndexedContents => FwIndexedContents, _ }
import au.com.cba.omnia.grimlock.framework.position._

import au.com.cba.omnia.grimlock.scalding._

import com.twitter.scalding.typed.TypedPipe

/**
 * Rich wrapper around a `TypedPipe[Content]`.
 *
 * @param data The `TypedPipe[Content]`.
 */
case class Contents(data: TypedPipe[Content]) extends FwContents with Persist[Content] {
  def saveAsText(file: String, writer: TextWriter)(implicit ctx: C): U[Content] = saveText(file, writer)
}

/** Companion object for the Scalding `Contents` class. */
object Contents {
  /** Converts a `TypedPipe[Content]` to a `Contents`. */
  implicit def TPC2TPC(data: TypedPipe[Content]): Contents = Contents(data)
}

/**
 * Rich wrapper around a `TypedPipe[(P, Content]`.
 *
 * @param data The `TypedPipe[(P, Content]`.
 */
case class IndexedContents[P <: Position](
  data: TypedPipe[(P, Content)]
) extends FwIndexedContents[P] with Persist[(P, Content)] {
  def saveAsText(file: String, writer: TextWriter)(implicit ctx: C): U[(P, Content)] = saveText(file, writer)
}

/** Companion object for the Scalding `IndexedContents` class. */
object IndexedContents {
  /** Converts a `TypedPipe[(P, Content)]` to a `IndexedContents`. */
  implicit def TPIC2TPC[P <: Position](data: TypedPipe[(P, Content)]): IndexedContents[P] = IndexedContents[P](data)
}

