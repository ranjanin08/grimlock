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

package au.com.cba.omnia.grimlock.framework

import au.com.cba.omnia.grimlock.framework.content._
import au.com.cba.omnia.grimlock.framework.content.metadata._
import au.com.cba.omnia.grimlock.framework.encoding._
import au.com.cba.omnia.grimlock.framework.position._

import java.util.regex.Pattern

import scala.reflect.ClassTag

/**
 * Cell in a matrix.
 *
 * @param position The position of the cell in the matri.
 * @param content  The contents of the cell.
 */
case class Cell[P <: Position](position: P, content: Content) {
  /**
   * Relocate this cell.
   *
   * @param relocator Function that returns the new position for this cell.
   */
  def relocate[Q <: Position](relocator: (Cell[P]) => Q): Cell[Q] = Cell(relocator(this), content)

  /**
   * Mutate the content of this cell.
   *
   * @param mutator Function that returns the new content for this cell.
   */
  def mutate(mutator: (Cell[P]) => Content): Cell[P] = Cell(position, mutator(this))

  /**
   * Return string representation of a cell.
   *
   * @param separator   The separator to use between various fields.
   * @param descriptive Indicator if descriptive string is required or not.
   * @param schema      Indicator if schema is required or not (only used if descriptive is `false`).
   */
  def toString(separator: String, descriptive: Boolean, schema: Boolean = true): String = {
    (descriptive, schema) match {
      case (true, _) => position.toString + separator + content.toString
      case (false, true) => position.toShortString(separator) + separator + content.toShortString(separator)
      case (false, false) => position.toShortString(separator) + separator + content.toShortString()
    }
  }
}

/** Companion object to the Cell class. */
object Cell {
  /**
   * Parse a line into a `Cell[Position1D]`.
   *
   * @param separator The column separator.
   * @param first     The codex for decoding the first dimension.
   * @param line      The line to parse.
   */
  def parse1D(separator: String = "|", first: Codex = StringCodex)(
    line: String): Option[Either[Cell[Position1D], String]] = {
    line.trim.split(Pattern.quote(separator), 4) match {
      case Array(r, t, e, v) =>
        Schema.fromString(e, t).flatMap {
          case s => (s.decode(v), first.decode(r)) match {
            case (Some(con), Some(c1)) => Some(Left(Cell(Position1D(c1), con)))
            case _ => Some(Right("Unable to decode: '" + line + "'"))
          }
        }
      case _ => Some(Right("Unable to split: '" + line + "'"))
    }
  }

  /**
   * Parse a line data into a `Cell[Position1D]` with a dictionary.
   *
   * @param dict      The dictionary describing the features in the data.
   * @param separator The column separator.
   * @param first     The codex for decoding the first dimension.
   * @param line      The line to parse.
   */
  def parse1DWithDictionary(dict: Map[String, Schema], separator: String = "|", first: Codex = StringCodex)(
    line: String): Option[Either[Cell[Position1D], String]] = {
    line.trim.split(Pattern.quote(separator), 2) match {
      case Array(e, v) =>
        dict.get(e) match {
          case Some(schema) =>
            (schema.decode(v), first.decode(e)) match {
              case (Some(con), Some(c1)) => Some(Left(Cell(Position1D(c1), con)))
              case _ => Some(Right("Unable to decode: '" + line + "'"))
            }
          case _ => Some(Right("Missing schema for: '" + line + "'"))
        }
      case _ => Some(Right("Unable to split: '" + line + "'"))
    }
  }

  /**
   * Parse a line into a `Cell[Position1D]` with a schema.
   *
   * @param schema    The schema for decoding the data.
   * @param separator The column separator.
   * @param first     The codex for decoding the first dimension.
   * @param line      The line to parse.
   */
  def parse1DWithSchema(schema: Schema, separator: String = "|", first: Codex = StringCodex)(
    line: String): Option[Either[Cell[Position1D], String]] = {
    line.trim.split(Pattern.quote(separator), 2) match {
      case Array(e, v) =>
        (schema.decode(v), first.decode(e)) match {
          case (Some(con), Some(c1)) => Some(Left(Cell(Position1D(c1), con)))
          case _ => Some(Right("Unable to decode: '" + line + "'"))
        }
      case _ => Some(Right("Unable to split: '" + line + "'"))
    }
  }

  /**
   * Parse a line into a `Cell[Position2D]`.
   *
   * @param separator The column separator.
   * @param first     The codex for decoding the first dimension.
   * @param second    The codex for decoding the second dimension.
   * @param line      The line to parse.
   */
  def parse2D(separator: String = "|", first: Codex = StringCodex, second: Codex = StringCodex)(
    line: String): Option[Either[Cell[Position2D], String]] = {
    line.trim.split(Pattern.quote(separator), 5) match {
      case Array(r, c, t, e, v) =>
        Schema.fromString(e, t).flatMap {
          case s => (s.decode(v), first.decode(r), second.decode(c)) match {
            case (Some(con), Some(c1), Some(c2)) => Some(Left(Cell(Position2D(c1, c2), con)))
            case _ => Some(Right("Unable to decode: '" + line + "'"))
          }
        }
      case _ => Some(Right("Unable to split: '" + line + "'"))
    }
  }

  /**
   * Parse a line into a `Cell[Position2D]` with a dictionary.
   *
   * @param dict      The dictionary describing the features in the data.
   * @param dim       The dimension on which to apply the dictionary.
   * @param separator The column separator.
   * @param first     The codex for decoding the first dimension.
   * @param second    The codex for decoding the second dimension.
   * @param line      The line to parse.
   */
  def parse2DWithDictionary[D <: Dimension](dict: Map[String, Schema], dim: D, separator: String = "|",
    first: Codex = StringCodex, second: Codex = StringCodex)(line: String)(
      implicit ev: PosDimDep[Position2D, D]): Option[Either[Cell[Position2D], String]] = {
    line.trim.split(Pattern.quote(separator), 3) match {
      case Array(e, a, v) =>
        (dim match {
          case First => dict.get(e)
          case Second => dict.get(a)
        }) match {
          case Some(schema) =>
            (schema.decode(v), first.decode(e), second.decode(a)) match {
              case (Some(con), Some(c1), Some(c2)) => Some(Left(Cell(Position2D(c1, c2), con)))
              case _ => Some(Right("Unable to decode: '" + line + "'"))
            }
          case _ => Some(Right("Missing schema for: '" + line + "'"))
        }
      case _ => Some(Right("Unable to split: '" + line + "'"))
    }
  }

  /**
   * Parse a line into a `Cell[Position2D]` with a schema.
   *
   * @param schema    The schema for decoding the data.
   * @param separator The column separator.
   * @param first     The codex for decoding the first dimension.
   * @param second    The codex for decoding the second dimension.
   * @param line      The line to parse.
   */
  def parse2DWithSchema(schema: Schema, separator: String = "|", first: Codex = StringCodex,
    second: Codex = StringCodex)(line: String): Option[Either[Cell[Position2D], String]] = {
    line.trim.split(Pattern.quote(separator), 3) match {
      case Array(e, a, v) =>
        (schema.decode(v), first.decode(e), second.decode(a)) match {
          case (Some(con), Some(c1), Some(c2)) => Some(Left(Cell(Position2D(c1, c2), con)))
          case _ => Some(Right("Unable to decode: '" + line + "'"))
        }
      case _ => Some(Right("Unable to split: '" + line + "'"))
    }
  }

  /**
   * Parse a line into a `Cell[Position3D]`.
   *
   * @param separator The column separator.
   * @param first     The codex for decoding the first dimension.
   * @param second    The codex for decoding the second dimension.
   * @param third     The codex for decoding the third dimension.
   * @param line      The line to parse.
   */
  def parse3D(separator: String = "|", first: Codex = StringCodex, second: Codex = StringCodex,
    third: Codex = StringCodex)(line: String): Option[Either[Cell[Position3D], String]] = {
    line.trim.split(Pattern.quote(separator), 6) match {
      case Array(r, c, d, t, e, v) =>
        Schema.fromString(e, t).flatMap {
          case s => (s.decode(v), first.decode(r), second.decode(c), third.decode(d)) match {
            case (Some(con), Some(c1), Some(c2), Some(c3)) => Some(Left(Cell(Position3D(c1, c2, c3), con)))
            case _ => Some(Right("Unable to decode: '" + line + "'"))
          }
        }
      case _ => Some(Right("Unable to split: '" + line + "'"))
    }
  }

  /**
   * Parse a line into a `Cell[Position3D]` with a dictionary.
   *
   * @param dict      The dictionary describing the features in the data.
   * @param dim       The dimension on which to apply the dictionary.
   * @param separator The column separator.
   * @param first     The codex for decoding the first dimension.
   * @param second    The codex for decoding the second dimension.
   * @param third     The codex for decoding the third dimension.
   * @param line      The line to parse.
   */
  def parse3DWithDictionary[D <: Dimension](dict: Map[String, Schema], dim: D, separator: String = "|",
    first: Codex = StringCodex, second: Codex = StringCodex, third: Codex = StringCodex)(line: String)(
      implicit ev: PosDimDep[Position3D, D]): Option[Either[Cell[Position3D], String]] = {
    line.trim.split(Pattern.quote(separator), 4) match {
      case Array(e, a, t, v) =>
        (dim match {
          case First => dict.get(e)
          case Second => dict.get(a)
          case Third => dict.get(t)
        }) match {
          case Some(schema) =>
            (schema.decode(v), first.decode(e), second.decode(a), third.decode(t)) match {
              case (Some(con), Some(c1), Some(c2), Some(c3)) => Some(Left(Cell(Position3D(c1, c2, c3), con)))
              case _ => Some(Right("Unable to decode: '" + line + "'"))
            }
          case _ => Some(Right("Missing schema for: '" + line + "'"))
        }
      case _ => Some(Right("Unable to split: '" + line + "'"))
    }
  }

  /**
   * Parse a line into a `Cell[Position3D]` with a schema.
   *
   * @param schema    The schema for decoding the data.
   * @param separator The column separator.
   * @param first     The codex for decoding the first dimension.
   * @param second    The codex for decoding the second dimension.
   * @param third     The codex for decoding the third dimension.
   * @param line      The line to parse.
   */
  def parse3DWithSchema(schema: Schema, separator: String = "|", first: Codex = StringCodex,
    second: Codex = StringCodex, third: Codex = StringCodex)(
      line: String): Option[Either[Cell[Position3D], String]] = {
    line.trim.split(Pattern.quote(separator), 4) match {
      case Array(e, a, t, v) =>
        (schema.decode(v), first.decode(e), second.decode(a), third.decode(t)) match {
          case (Some(con), Some(c1), Some(c2), Some(c3)) => Some(Left(Cell(Position3D(c1, c2, c3), con)))
          case _ => Some(Right("Unable to decode: '" + line + "'"))
        }
      case _ => Some(Right("Unable to split: '" + line + "'"))
    }
  }

  /**
   * Parse a line into a `Cell[Position4D]`.
   *
   * @param separator The column separator.
   * @param first     The codex for decoding the first dimension.
   * @param second    The codex for decoding the second dimension.
   * @param third     The codex for decoding the third dimension.
   * @param fourth    The codex for decoding the fourth dimension.
   * @param line      The line to parse.
   */
  def parse4D(separator: String = "|", first: Codex = StringCodex, second: Codex = StringCodex,
    third: Codex = StringCodex, fourth: Codex = StringCodex)(
      line: String): Option[Either[Cell[Position4D], String]] = {
    line.trim.split(Pattern.quote(separator), 7) match {
      case Array(a, b, c, d, t, e, v) =>
        Schema.fromString(e, t).flatMap {
          case s => (s.decode(v), first.decode(a), second.decode(b), third.decode(c), fourth.decode(d)) match {
            case (Some(con), Some(c1), Some(c2), Some(c3), Some(c4)) =>
              Some(Left(Cell(Position4D(c1, c2, c3, c4), con)))
            case _ => Some(Right("Unable to decode: '" + line + "'"))
          }
        }
      case _ => Some(Right("Unable to split: '" + line + "'"))
    }
  }

  /**
   * Parse a line into a `Cell[Position4D]` with a dictionary.
   *
   * @param dict      The dictionary describing the features in the data.
   * @param dim       The dimension on which to apply the dictionary.
   * @param separator The column separator.
   * @param first     The codex for decoding the first dimension.
   * @param second    The codex for decoding the second dimension.
   * @param third     The codex for decoding the third dimension.
   * @param fourth    The codex for decoding the fourth dimension.
   * @param line      The line to parse.
   */
  def parse4DWithDictionary[D <: Dimension](dict: Map[String, Schema], dim: D, separator: String = "|",
    first: Codex = StringCodex, second: Codex = StringCodex, third: Codex = StringCodex, fourth: Codex = StringCodex)(
      line: String)(implicit ev: PosDimDep[Position4D, D]): Option[Either[Cell[Position4D], String]] = {
    line.trim.split(Pattern.quote(separator), 5) match {
      case Array(a, b, c, d, v) =>
        (dim match {
          case First => dict.get(a)
          case Second => dict.get(b)
          case Third => dict.get(c)
          case Fourth => dict.get(d)
        }) match {
          case Some(schema) =>
            (schema.decode(v), first.decode(a), second.decode(b), third.decode(c), fourth.decode(d)) match {
              case (Some(con), Some(c1), Some(c2), Some(c3), Some(c4)) =>
                Some(Left(Cell(Position4D(c1, c2, c3, c4), con)))
              case _ => Some(Right("Unable to decode: '" + line + "'"))
            }
          case _ => Some(Right("Missing schema for: '" + line + "'"))
        }
      case _ => Some(Right("Unable to split: '" + line + "'"))
    }
  }

  /**
   * Parse a line into a `Cell[Position4D]` with a schema.
   *
   * @param schema    The schema for decoding the data.
   * @param separator The column separator.
   * @param first     The codex for decoding the first dimension.
   * @param second    The codex for decoding the second dimension.
   * @param third     The codex for decoding the third dimension.
   * @param fourth    The codex for decoding the fourth dimension.
   * @param line      The line to parse.
   */
  def parse4DWithSchema(schema: Schema, separator: String = "|", first: Codex = StringCodex,
    second: Codex = StringCodex, third: Codex = StringCodex, fourth: Codex = StringCodex)(
      line: String): Option[Either[Cell[Position4D], String]] = {
    line.trim.split(Pattern.quote(separator), 5) match {
      case Array(a, b, c, d, v) =>
        (schema.decode(v), first.decode(a), second.decode(b), third.decode(c), fourth.decode(d)) match {
          case (Some(con), Some(c1), Some(c2), Some(c3), Some(c4)) => Some(Left(Cell(Position4D(c1, c2, c3, c4), con)))
          case _ => Some(Right("Unable to decode: '" + line + "'"))
        }
      case _ => Some(Right("Unable to split: '" + line + "'"))
    }
  }

  /**
   * Parse a line into a `Cell[Position5D]`.
   *
   * @param separator The column separator.
   * @param first     The codex for decoding the first dimension.
   * @param second    The codex for decoding the second dimension.
   * @param third     The codex for decoding the third dimension.
   * @param fourth    The codex for decoding the fourth dimension.
   * @param fifth     The codex for decoding the fifth dimension.
   * @param line      The line to parse.
   */
  def parse5D(separator: String = "|", first: Codex = StringCodex, second: Codex = StringCodex,
    third: Codex = StringCodex, fourth: Codex = StringCodex, fifth: Codex = StringCodex)(
      line: String): Option[Either[Cell[Position5D], String]] = {
    line.trim.split(Pattern.quote(separator), 8) match {
      case Array(a, b, c, d, f, t, e, v) =>
        Schema.fromString(e, t).flatMap {
          case s => (s.decode(v), first.decode(a), second.decode(b), third.decode(c), fourth.decode(d),
            fifth.decode(f)) match {
              case (Some(con), Some(c1), Some(c2), Some(c3), Some(c4), Some(c5)) =>
                Some(Left(Cell(Position5D(c1, c2, c3, c4, c5), con)))
              case _ => Some(Right("Unable to decode: '" + line + "'"))
            }
        }
      case _ => Some(Right("Unable to split: '" + line + "'"))
    }
  }

  /**
   * Parse a line into a `Cell[Position5D]` with a dictionary.
   *
   * @param dict      The dictionary describing the features in the data.
   * @param dim       The dimension on which to apply the dictionary.
   * @param separator The column separator.
   * @param first     The codex for decoding the first dimension.
   * @param second    The codex for decoding the second dimension.
   * @param third     The codex for decoding the third dimension.
   * @param fourth    The codex for decoding the fourth dimension.
   * @param fifth     The codex for decoding the fifth dimension.
   * @param line      The line to parse.
   */
  def parse5DWithDictionary[D <: Dimension](dict: Map[String, Schema], dim: D, separator: String = "|",
    first: Codex = StringCodex, second: Codex = StringCodex, third: Codex = StringCodex, fourth: Codex = StringCodex,
    fifth: Codex = StringCodex)(line: String)(
      implicit ev: PosDimDep[Position5D, D]): Option[Either[Cell[Position5D], String]] = {
    line.trim.split(Pattern.quote(separator), 6) match {
      case Array(a, b, c, d, e, v) =>
        (dim match {
          case First => dict.get(a)
          case Second => dict.get(b)
          case Third => dict.get(c)
          case Fourth => dict.get(d)
          case Fifth => dict.get(e)
        }) match {
          case Some(schema) =>
            (schema.decode(v), first.decode(a), second.decode(b), third.decode(c), fourth.decode(d),
              fifth.decode(e)) match {
                case (Some(con), Some(c1), Some(c2), Some(c3), Some(c4), Some(c5)) =>
                  Some(Left(Cell(Position5D(c1, c2, c3, c4, c5), con)))
                case _ => Some(Right("Unable to decode: '" + line + "'"))
              }
          case _ => Some(Right("Missing schema for: '" + line + "'"))
        }
      case _ => Some(Right("Unable to split: '" + line + "'"))
    }
  }

  /**
   * Parse a line into a `Cell[Position5D]` with a schema.
   *
   * @param schema    The schema for decoding the data.
   * @param separator The column separator.
   * @param first     The codex for decoding the first dimension.
   * @param second    The codex for decoding the second dimension.
   * @param third     The codex for decoding the third dimension.
   * @param fourth    The codex for decoding the fourth dimension.
   * @param fifth     The codex for decoding the fifth dimension.
   * @param line      The line to parse.
   */
  def parse5DWithSchema(schema: Schema, separator: String = "|", first: Codex = StringCodex,
    second: Codex = StringCodex, third: Codex = StringCodex, fourth: Codex = StringCodex, fifth: Codex = StringCodex)(
      line: String): Option[Either[Cell[Position5D], String]] = {
    line.trim.split(Pattern.quote(separator), 6) match {
      case Array(a, b, c, d, e, v) =>
        (schema.decode(v), first.decode(a), second.decode(b), third.decode(c), fourth.decode(d),
          fifth.decode(e)) match {
            case (Some(con), Some(c1), Some(c2), Some(c3), Some(c4), Some(c5)) =>
              Some(Left(Cell(Position5D(c1, c2, c3, c4, c5), con)))
            case _ => Some(Right("Unable to decode: '" + line + "'"))
          }
      case _ => Some(Right("Unable to split: '" + line + "'"))
    }
  }

  /**
   * Parse a line into a `List[Cell[Position2D]]` with column definitions.
   *
   * @param columns   `List[(String, Schema)]` describing each column in the table.
   * @param pkeyIndex Index (into `columns`) describing which column is the primary key.
   * @param separator The column separator.
   * @param line      The line to parse.
   */
  def parseTable(columns: List[(String, Schema)], pkeyIndex: Int = 0, separator: String = "\u0001")(
    line: String): List[Either[Cell[Position2D], String]] = {
    val parts = line.trim.split(Pattern.quote(separator), columns.length)

    (parts.length == columns.length) match {
      case true =>
        val pkey = parts(pkeyIndex)

        columns.zipWithIndex.flatMap {
          case ((name, schema), idx) if (idx != pkeyIndex) =>
            schema.decode(parts(idx)) match {
              case Some(con) => Some(Left(Cell(Position2D(pkey, name), con)))
              case _ => Some(Right("Unable to decode: '" + line + "'"))
            }
          case _ => None
        }
      case _ => List(Right("Unable to split: '" + line + "'"))
    }
  }

  /**
   * Return function that returns a string representation of a cell.
   *
   * @param separator   The separator to use between various fields.
   * @param descriptive Indicator if descriptive string is required or not.
   * @param schema      Indicator if schema is required or not (only used if descriptive is `false`).
   */
  def toString[P <: Position](separator: String = "|", descriptive: Boolean = false,
    schema: Boolean = true): (Cell[P]) => TraversableOnce[String] = {
    (t: Cell[P]) => Some(t.toString(separator, descriptive, schema))
  }
}

