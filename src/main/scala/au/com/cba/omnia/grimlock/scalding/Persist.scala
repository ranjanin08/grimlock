// Copyright 2015 Commonwealth Bank of Australia
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

package au.com.cba.omnia.grimlock.scalding

import au.com.cba.omnia.grimlock.framework.{ Persist => BasePersist }

import cascading.flow.FlowDef
import com.twitter.scalding.{ Mode, TextLine }
import com.twitter.scalding.typed.{ TypedPipe, TypedSink }

/** Trait for peristing a Scalding `TypedPipe`. */
trait Persist[T] extends BasePersist[T] {
  /** The data to persist. */
  val data: TypedPipe[T]

  /**
   * Persist to disk.
   *
   * @param file        Name of the output file.
   * @param separator   Separator to use between the fields.
   * @param descriptive Indicates if the output should be descriptive.
   *
   * @return A Scalding `TypedPipe[T]` which is this object's data.
   */
  def save(file: String, separator: String = "|", descriptive: Boolean = false)(implicit flow: FlowDef,
    mode: Mode): TypedPipe[T] = {
    data
      .map { case t => toString(t, separator, descriptive) }
      .write(TypedSink(TextLine(file)))

    data
  }
}

