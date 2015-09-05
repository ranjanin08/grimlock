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

package au.com.cba.omnia.grimlock.scalding.content

import au.com.cba.omnia.grimlock.framework.content.{ Contents => BaseContents, _ }

import au.com.cba.omnia.grimlock.scalding._

import cascading.flow.FlowDef
import com.twitter.scalding.Mode
import com.twitter.scalding.typed.TypedPipe

/**
 * Rich wrapper around a `TypedPipe[Content]`.
 *
 * @param data The `TypedPipe[Content]`.
 */
class Contents(val data: TypedPipe[Content]) extends BaseContents with Persist[Content] {
  type U[A] = TypedPipe[A]

  def saveAsText(file: String, writer: TextWriter = Content.toString())(implicit flow: FlowDef,
    mode: Mode): U[Content] = saveText(file, writer)
}

/** Companion object for the Scalding `Contents` class. */
object Contents {
  /** Converts a `TypedPipe[Content]` to a `Contents`. */
  implicit def TPC2TPC(data: TypedPipe[Content]): Contents = new Contents(data)
}

