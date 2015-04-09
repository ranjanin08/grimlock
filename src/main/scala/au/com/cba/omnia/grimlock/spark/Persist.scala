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

import org.apache.spark.rdd._

/** Trait for peristing a Spark `RDD`. */
trait SparkPersist[T] extends Persist[T] with java.io.Serializable {
  /** The data to persist. */
  val data: RDD[T]

  /**
   * Persist to disk.
   *
   * @param file        Name of the output file.
   * @param separator   Separator to use between the fields.
   * @param descriptive Indicates if the output should be descriptive.
   *
   * @return A Spark `RDD[T]` which is this object's data.
   */
  def persist(file: String, separator: String = "|", descriptive: Boolean = false): RDD[T] = {
    data
      .map(toString(_, separator, descriptive))
      .saveAsTextFile(file)

    data
  }
}

