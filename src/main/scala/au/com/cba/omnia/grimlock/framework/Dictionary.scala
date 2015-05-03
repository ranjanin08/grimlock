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

package au.com.cba.omnia.grimlock.framework.content.metadata

object Dictionary {
  /**
   * Placeholder for reading in a dictionary.
   *
   * @param file File containing dictionary data.
   *
   * @return A dictionary object.
   */
  def load(file: String, separator: String = "|"): Map[String, Schema] = {
    (for (line <- scala.io.Source.fromFile(file).getLines()) yield {
      val parts = line.split(java.util.regex.Pattern.quote(separator))
      (parts(0), Schema.fromString(parts(1), parts(2)).get)
    }).toMap
  }
}

