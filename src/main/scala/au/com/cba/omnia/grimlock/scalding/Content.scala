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

package au.com.cba.omnia.grimlock.content

import au.com.cba.omnia.grimlock._
import au.com.cba.omnia.grimlock.encoding._
import au.com.cba.omnia.grimlock.content.metadata._

import com.twitter.scalding._

/**
 * Rich wrapper around a `TypedPipe[Content]`.
 *
 * @param data The `TypedPipe[Content]`.
 */
class ScaldingContents(val data: TypedPipe[Content]) extends Contents with ScaldingPersist[Content] {
  type U[A] = TypedPipe[A]
}

/** Companion object for the `ScaldingContents` class. */
object ScaldingContents {
  /** Converts a `TypedPipe[Content]` to a `Contents`. */
  implicit def TPC2TPC(data: TypedPipe[Content]): ScaldingContents = new ScaldingContents(data)
}

