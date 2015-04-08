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

package au.com.cba.omnia.grimlock

import cascading.flow.FlowDef
import com.twitter.scalding._
import com.twitter.scalding.typed.TypedSink

/** Trait for peristing a Scalding `TypedPipe`. */
trait ScaldingPersist[T] extends Persist[T] {
  /** The data to persist. */
  val data: TypedPipe[T]

  /**
   * Persist to a sink.
   *
   * @param sink        Sink to write to.
   * @param separator   Separator to use between the fields.
   * @param descriptive Indicates if the output should be descriptive.
   *
   * @return A Scalding `TypedPipe[T]` which is this object's data.
   */
  def persist(sink: TypedSink[String], separator: String = "|", descriptive: Boolean = false)(implicit flow: FlowDef,
    mode: Mode): TypedPipe[T] = {
    data
      .map { case t => toString(t, separator, descriptive) }
      .write(sink)

    data
  }

  /**
   * Persist to disk.
   *
   * @param file        Name of the output file.
   * @param separator   Separator to use between the fields.
   * @param descriptive Indicates if the output should be descriptive.
   *
   * @return A Scalding `TypedPipe[T]` which is this object's data.
   */
  def persistFile(file: String, separator: String = "|", descriptive: Boolean = false)(implicit flow: FlowDef,
    mode: Mode): TypedPipe[T] = persist(TypedSink(TextLine(file)), separator, descriptive)
}

