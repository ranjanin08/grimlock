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

import cascading.flow.FlowDef
import com.twitter.scalding.{ Mode, TextLine }
import com.twitter.scalding.typed.{ TypedPipe, TypedSink }
import com.twitter.scrooge.ThriftStruct

import au.com.cba.omnia.ebenezer.scrooge.ParquetScroogeSource

import au.com.cba.omnia.grimlock.framework.{ Persist => BasePersist }

/** Trait for peristing a Scalding `TypedPipe`. */
trait Persist[T] extends BasePersist[T] {
  /** The data to persist. */
  val data: TypedPipe[T]

  /**
   * Persist to disk.
   *
   * @param file   Name of the output file.
   * @param writer Writer that converts `T` to string.
   *
   * @return A Scalding `TypedPipe[T]` which is this object's data.
   */
  def saveAsText(file: String, writer: TextWriter)(implicit flow: FlowDef, mode: Mode): TypedPipe[T]

  protected def saveText(file: String, writer: TextWriter)(implicit flow: FlowDef, mode: Mode): TypedPipe[T] = {
    data
      .flatMap(writer(_))
      .write(TypedSink(TextLine(file)))

    data
  }

  /**
   * Persist to disk as parquet file.
   *
   * @param file        Name of the output file.
   * @param f           Function that converts type T to thrift struct of type U.
   *
   * @return A Scalding `TypedPipe[T]` which is this object's data.
   */
  def saveAsParquet[U <: ThriftStruct](file: String, f: T => U)(implicit flow: FlowDef,
                                                                mode: Mode, m: Manifest[U]): TypedPipe[T] = {
    data
      .map(f(_))
      .write(ParquetScroogeSource[U](file))

    data
  }
}

