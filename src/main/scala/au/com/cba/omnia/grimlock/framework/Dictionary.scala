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

package au.com.cba.omnia.grimlock.framework.content.metadata

import scala.io.Source

object Dictionary {
  /**
   * Load a dictionary from file.
   *
   * @param source    Source to read dictionary data from.
   * @param separator Separator for splitting dictionary into data fields.
   * @param key       Index (into data fields) for schema key.
   * @param encoding  Index (into data fields) for the encoding identifier.
   * @param schema    Index (into data fields) for the schema identifier
   *
   * @return A tuple consisting of the dictionary object and an iterator containing parse errors.
   */
  def load(source: Source, separator: String = "|", key: Int = 0, encoding: Int = 1,
    schema: Int = 2): (Map[String, Schema], Iterator[String]) = {
    val result = source
      .getLines()
      .map {
        case line =>
          val parts = line.split(java.util.regex.Pattern.quote(separator))

          List(key, encoding, schema).exists(_ >= parts.length) match {
            case true => Right("unable to parse: '" + line + "'")
            case false =>
              Schema.fromString(parts(encoding), parts(schema)) match {
                case Some(s) => Left((parts(key), s))
                case None => Right("unable to decode '" + line + "'")
              }
          }
      }

    (result.collect { case Left(entry) => entry }.toMap, result.collect { case Right(error) => error })
  }
}

