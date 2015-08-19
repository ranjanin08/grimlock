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

import au.com.cba.omnia.grimlock.framework.aggregate._
import au.com.cba.omnia.grimlock.framework.content._
import au.com.cba.omnia.grimlock.framework.content.metadata._
import au.com.cba.omnia.grimlock.framework.encoding._
import au.com.cba.omnia.grimlock.framework.pairwise._
import au.com.cba.omnia.grimlock.framework.partition._
import au.com.cba.omnia.grimlock.framework.position._
import au.com.cba.omnia.grimlock.framework.sample._
import au.com.cba.omnia.grimlock.framework.squash._
import au.com.cba.omnia.grimlock.framework.transform._
import au.com.cba.omnia.grimlock.framework.utility._
import au.com.cba.omnia.grimlock.framework.utility.OneOf._
import au.com.cba.omnia.grimlock.framework.window._

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
   * Return string representation of a cell.
   *
   * @param separator   The separator to use between various fields.
   * @param descriptive Indicator if descriptive string is required or not.
   */
  def toString(separator: String, descriptive: Boolean): String = {
    descriptive match {
      case true => position.toString + separator + content.toString
      case false => position.toShortString(separator) + separator + content.toShortString(separator)
    }
  }
}

/** Companion object to the Cell class. */
object Cell {
  /**
   * Parse a line into a `Option[Cell[Position1D]]`.
   *
   * @param separator The column separator.
   * @param first     The codex for decoding the first dimension.
   * @param line      The line to parse.
   */
  def parse1D(separator: String = "|", first: Codex = StringCodex)(line: String): Option[Cell[Position1D]] = {
    line.trim.split(Pattern.quote(separator), 4) match {
      case Array(r, t, e, v) =>
        Schema.fromString(e, t).flatMap {
          case s => (s.decode(v), first.decode(r)) match {
            case (Some(con), Some(c1)) => Some(Cell(Position1D(c1), con))
            case _ => None
          }
        }
      case _ => None
    }
  }

  /**
   * Parse a line data into a `Option[Cell[Position1D]]` with a dictionary.
   *
   * @param dict      The dictionary describing the features in the data.
   * @param separator The column separator.
   * @param first     The codex for decoding the first dimension.
   * @param line      The line to parse.
   */
  def parse1DWithDictionary(dict: Map[String, Schema], separator: String = "|", first: Codex = StringCodex)(
    line: String): Option[Cell[Position1D]] = {
    line.trim.split(Pattern.quote(separator), 2) match {
      case Array(e, v) =>
        (dict(e).decode(v), first.decode(e)) match {
          case (Some(con), Some(c1)) => Some(Cell(Position1D(c1), con))
          case _ => None
        }
      case _ => None
    }
  }

  /**
   * Parse a line into a `Option[Cell[Position1D]]` with a schema.
   *
   * @param schema    The schema for decoding the data.
   * @param separator The column separator.
   * @param first     The codex for decoding the first dimension.
   * @param line      The line to parse.
   */
  def parse1DWithSchema(schema: Schema, separator: String = "|", first: Codex = StringCodex)(
    line: String): Option[Cell[Position1D]] = {
    line.trim.split(Pattern.quote(separator), 2) match {
      case Array(e, v) =>
        (schema.decode(v), first.decode(e)) match {
          case (Some(con), Some(c1)) => Some(Cell(Position1D(c1), con))
          case _ => None
        }
      case _ => None
    }
  }

  /**
   * Parse a line into a `Option[Cell[Position2D]]`.
   *
   * @param separator The column separator.
   * @param first     The codex for decoding the first dimension.
   * @param second    The codex for decoding the second dimension.
   * @param line      The line to parse.
   */
  def parse2D(separator: String = "|", first: Codex = StringCodex, second: Codex = StringCodex)(
    line: String): Option[Cell[Position2D]] = {
    line.trim.split(Pattern.quote(separator), 5) match {
      case Array(r, c, t, e, v) =>
        Schema.fromString(e, t).flatMap {
          case s => (s.decode(v), first.decode(r), second.decode(c)) match {
            case (Some(con), Some(c1), Some(c2)) => Some(Cell(Position2D(c1, c2), con))
            case _ => None
          }
        }
      case _ => None
    }
  }

  /**
   * Parse a line into a `Option[Cell[Position2D]]` with a dictionary.
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
      implicit ev: PosDimDep[Position2D, D]): Option[Cell[Position2D]] = {
    line.trim.split(Pattern.quote(separator), 3) match {
      case Array(e, a, v) =>
        val s = dim match {
          case First => dict(e)
          case Second => dict(a)
        }

        (s.decode(v), first.decode(e), second.decode(a)) match {
          case (Some(con), Some(c1), Some(c2)) => Some(Cell(Position2D(c1, c2), con))
          case _ => None
        }
      case _ => None
    }
  }

  /**
   * Parse a line into a `Option[Cell[Position2D]]` with a schema.
   *
   * @param schema    The schema for decoding the data.
   * @param separator The column separator.
   * @param first     The codex for decoding the first dimension.
   * @param second    The codex for decoding the second dimension.
   * @param line      The line to parse.
   */
  def parse2DWithSchema(schema: Schema, separator: String = "|", first: Codex = StringCodex,
    second: Codex = StringCodex)(line: String): Option[Cell[Position2D]] = {
    line.trim.split(Pattern.quote(separator), 3) match {
      case Array(e, a, v) =>
        (schema.decode(v), first.decode(e), second.decode(a)) match {
          case (Some(con), Some(c1), Some(c2)) => Some(Cell(Position2D(c1, c2), con))
          case _ => None
        }
      case _ => None
    }
  }

  /**
   * Parse a line into a `Option[Cell[Position3D]]`.
   *
   * @param separator The column separator.
   * @param first     The codex for decoding the first dimension.
   * @param second    The codex for decoding the second dimension.
   * @param third     The codex for decoding the third dimension.
   * @param line      The line to parse.
   */
  def parse3D(separator: String = "|", first: Codex = StringCodex, second: Codex = StringCodex,
    third: Codex = StringCodex)(line: String): Option[Cell[Position3D]] = {
    line.trim.split(Pattern.quote(separator), 6) match {
      case Array(r, c, d, t, e, v) =>
        Schema.fromString(e, t).flatMap {
          case s => (s.decode(v), first.decode(r), second.decode(c), third.decode(d)) match {
            case (Some(con), Some(c1), Some(c2), Some(c3)) => Some(Cell(Position3D(c1, c2, c3), con))
            case _ => None
          }
        }
      case _ => None
    }
  }

  /**
   * Parse a line into a `Option[Cell[Position3D]]` with a dictionary.
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
      implicit ev: PosDimDep[Position3D, D]): Option[Cell[Position3D]] = {
    line.trim.split(Pattern.quote(separator), 4) match {
      case Array(e, a, t, v) =>
        val s = dim match {
          case First => dict(e)
          case Second => dict(a)
          case Third => dict(t)
        }

        (s.decode(v), first.decode(e), second.decode(a), third.decode(t)) match {
          case (Some(con), Some(c1), Some(c2), Some(c3)) => Some(Cell(Position3D(c1, c2, c3), con))
          case _ => None
        }
      case _ => None
    }
  }

  /**
   * Parse a line into a `Option[Cell[Position3D]]` with a schema.
   *
   * @param schema    The schema for decoding the data.
   * @param separator The column separator.
   * @param first     The codex for decoding the first dimension.
   * @param second    The codex for decoding the second dimension.
   * @param third     The codex for decoding the third dimension.
   * @param line      The line to parse.
   */
  def parse3DWithSchema(schema: Schema, separator: String = "|", first: Codex = StringCodex,
    second: Codex = StringCodex, third: Codex = StringCodex)(line: String): Option[Cell[Position3D]] = {
    line.trim.split(Pattern.quote(separator), 4) match {
      case Array(e, a, t, v) =>
        (schema.decode(v), first.decode(e), second.decode(a), third.decode(t)) match {
          case (Some(con), Some(c1), Some(c2), Some(c3)) => Some(Cell(Position3D(c1, c2, c3), con))
          case _ => None
        }
      case _ => None
    }
  }

  /**
   * Parse a line into a `Option[Cell[Position4D]]`.
   *
   * @param separator The column separator.
   * @param first     The codex for decoding the first dimension.
   * @param second    The codex for decoding the second dimension.
   * @param third     The codex for decoding the third dimension.
   * @param fourth    The codex for decoding the fourth dimension.
   * @param line      The line to parse.
   */
  def parse4D(separator: String = "|", first: Codex = StringCodex, second: Codex = StringCodex,
    third: Codex = StringCodex, fourth: Codex = StringCodex)(line: String): Option[Cell[Position4D]] = {
    line.trim.split(Pattern.quote(separator), 7) match {
      case Array(a, b, c, d, t, e, v) =>
        Schema.fromString(e, t).flatMap {
          case s => (s.decode(v), first.decode(a), second.decode(b), third.decode(c), fourth.decode(d)) match {
            case (Some(con), Some(c1), Some(c2), Some(c3), Some(c4)) => Some(Cell(Position4D(c1, c2, c3, c4), con))
            case _ => None
          }
        }
      case _ => None
    }
  }

  /**
   * Parse a line into a `Option[Cell[Position4D]]` with a dictionary.
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
      line: String)(implicit ev: PosDimDep[Position4D, D]): Option[Cell[Position4D]] = {
    line.trim.split(Pattern.quote(separator), 5) match {
      case Array(a, b, c, d, v) =>
        val s = dim match {
          case First => dict(a)
          case Second => dict(b)
          case Third => dict(c)
          case Fourth => dict(d)
        }

        (s.decode(v), first.decode(a), second.decode(b), third.decode(c), fourth.decode(d)) match {
          case (Some(con), Some(c1), Some(c2), Some(c3), Some(c4)) => Some(Cell(Position4D(c1, c2, c3, c4), con))
          case _ => None
        }
      case _ => None
    }
  }

  /**
   * Parse a line into a `Option[Cell[Position4D]]` with a schema.
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
      line: String): Option[Cell[Position4D]] = {
    line.trim.split(Pattern.quote(separator), 5) match {
      case Array(a, b, c, d, v) =>
        (schema.decode(v), first.decode(a), second.decode(b), third.decode(c), fourth.decode(d)) match {
          case (Some(con), Some(c1), Some(c2), Some(c3), Some(c4)) => Some(Cell(Position4D(c1, c2, c3, c4), con))
          case _ => None
        }
      case _ => None
    }
  }

  /**
   * Parse a line into a `Option[Cell[Position5D]]`.
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
      line: String): Option[Cell[Position5D]] = {
    line.trim.split(Pattern.quote(separator), 8) match {
      case Array(a, b, c, d, f, t, e, v) =>
        Schema.fromString(e, t).flatMap {
          case s => (s.decode(v), first.decode(a), second.decode(b), third.decode(c), fourth.decode(d),
            fifth.decode(f)) match {
              case (Some(con), Some(c1), Some(c2), Some(c3), Some(c4), Some(c5)) =>
                Some(Cell(Position5D(c1, c2, c3, c4, c5), con))
              case _ => None
            }
        }
      case _ => None
    }
  }

  /**
   * Parse a line into a `Option[Cell[Position5D]]` with a dictionary.
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
      fifth: Codex = StringCodex)(line: String)(implicit ev: PosDimDep[Position5D, D]): Option[Cell[Position5D]] = {
    line.trim.split(Pattern.quote(separator), 6) match {
      case Array(a, b, c, d, e, v) =>
        val s = dim match {
          case First => dict(a)
          case Second => dict(b)
          case Third => dict(c)
          case Fourth => dict(d)
          case Fifth => dict(e)
        }

        (s.decode(v), first.decode(a), second.decode(b), third.decode(c), fourth.decode(d), fifth.decode(e)) match {
          case (Some(con), Some(c1), Some(c2), Some(c3), Some(c4), Some(c5)) =>
            Some(Cell(Position5D(c1, c2, c3, c4, c5), con))
          case _ => None
        }
      case _ => None
    }
  }

  /**
   * Parse a line into a `Option[Cell[Position5D]]` with a schema.
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
      line: String): Option[Cell[Position5D]] = {
    line.trim.split(Pattern.quote(separator), 6) match {
      case Array(a, b, c, d, e, v) =>
        (schema.decode(v), first.decode(a), second.decode(b), third.decode(c), fourth.decode(d),
          fifth.decode(e)) match {
            case (Some(con), Some(c1), Some(c2), Some(c3), Some(c4), Some(c5)) =>
              Some(Cell(Position5D(c1, c2, c3, c4, c5), con))
            case _ => None
          }
      case _ => None
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
    line: String): List[Cell[Position2D]] = {
    val parts = line.trim.split(Pattern.quote(separator), columns.length)
    val pkey = parts(pkeyIndex)

    columns.zipWithIndex.flatMap {
      case ((name, schema), idx) if (idx != pkeyIndex) =>
        schema.decode(parts(idx).trim).map { case c => Cell(Position2D(pkey, name), c) }
      case _ => None
    }
  }
}

/** Base trait for matrix operations. */
trait Matrix[P <: Position] extends Persist[Cell[P]] {
  /** Type of the underlying data structure. */
  type U[_]

  /** Type of 'wrapper' around user defined data. */
  type E[_]

  /** Self-type of a specific implementation of this API. */
  type S <: Matrix[P]

  /** Predicate used in, for example, the `which` methods of a matrix for finding content. */
  type Predicate = Cell[P] => Boolean

  /** Specifies tuners permitted on a call to `change`. */
  type ChangeTuners <: OneOf

  /**
   * Change the variable type of `positions` in a matrix.
   *
   * @param slice     Encapsulates the dimension(s) to change.
   * @param positions The position(s) within the dimension(s) to change.
   * @param schema    The schema to change to.
   * @param tuner     The tuner for the job.
   *
   * @return A `U[Cell[P]]' with the changed contents.
   */
  def change[D <: Dimension, I, T <: Tuner](slice: Slice[P, D], positions: I, schema: Schema, tuner: T)(
    implicit ev1: PosDimDep[P, D], ev2: PositionDistributable[I, slice.S, U], ev3: ClassTag[slice.S],
      ev4: ChangeTuners#V[T]): U[Cell[P]]

  /** Specifies tuners permitted on a call to `domain`. */
  type DomainTuners <: OneOf

  /**
   * Return all possible positions of a matrix.
   *
   * @param tuner The tuner for the job.
   */
  def domain[T <: Tuner](tuner: T)(implicit ev: DomainTuners#V[T]): U[P]

  /** Specifies tuners permitted on a call to `get`. */
  type GetTuners <: OneOf

  /**
   * Return contents of a matrix at `positions`.
   *
   * @param positions The positions for which to get the contents.
   * @param tuner     The tuner for the job.
   *
   * @return A `U[Cell[P]]' of the `positions` together with their content.
   */
  def get[I, T <: Tuner](positions: I, tuner: T)(implicit ev1: PositionDistributable[I, P, U], ev2: ClassTag[P],
    ev3: GetTuners#V[T]): U[Cell[P]]

  /** Specifies tuners permitted on a call to `join`. */
  type JoinTuners <: OneOf

  /**
   * Join two matrices.
   *
   * @param slice Encapsulates the dimension(s) along which to join.
   * @param that  The matrix to join with.
   * @param tuner The tuner for the job.
   *
   * @return A `U[Cell[P]]` consisting of the inner-join of the two matrices.
   */
  // TODO: Add inner/left/right/outer join functionality?
  def join[D <: Dimension, T <: Tuner](slice: Slice[P, D], that: S, tuner: T)(implicit ev1: PosDimDep[P, D],
    ev2: P =!= Position1D, ev3: ClassTag[slice.S], ev4: JoinTuners#V[T]): U[Cell[P]]

  /** Specifies tuners permitted on a call to `names`. */
  type NamesTuners <: OneOf

  /**
   * Returns the distinct position(s) (or names) for a given `slice`.
   *
   * @param slice Encapsulates the dimension(s) for which the names are to be returned.
   * @param tuner The tuner for the job.
   *
   * @return A `U[(slice.S, Long)]` of the distinct position(s) together with a unique index.
   */
  def names[D <: Dimension, T <: Tuner](slice: Slice[P, D], tuner: T)(implicit ev1: PosDimDep[P, D],
    ev2: slice.S =!= Position0D, ev3: ClassTag[slice.S], ev4: NamesTuners#V[T]): U[slice.S]

  /** Specifies tuners permitted on a call to `pairwise` functions. */
  type PairwiseTuners <: OneOf

  /**
   * Compute pairwise values between all pairs of values given a slice.
   *
   * @param slice     Encapsulates the dimension(s) along which to compute values.
   * @param comparer  Defines which element the pairwise operations should apply to.
   * @param operators The pairwise operators to apply.
   * @param tuner     The tuner for the job.
   *
   * @return A `U[Cell[slice.R#M]]` where the content contains the pairwise values.
   */
  def pairwise[D <: Dimension, Q <: Position, F, T <: Tuner](slice: Slice[P, D], comparer: Comparer, operators: F,
    tuner: T)(implicit ev1: PosDimDep[P, D], ev2: Operable[F, slice.S, slice.R, Q], ev3: slice.S =!= Position0D,
      ev4: ClassTag[slice.S], ev5: ClassTag[slice.R], ev6: PairwiseTuners#V[T]): U[Cell[Q]]

  /**
   * Compute pairwise values between all pairs of values given a slice with a user supplied value.
   *
   * @param slice     Encapsulates the dimension(s) along which to compute values.
   * @param comparer  Defines which element the pairwise operations should apply to.
   * @param operators The pairwise operators to apply.
   * @param value     The user supplied value.
   * @param tuner     The tuner for the job.
   *
   * @return A `U[Cell[slice.R#M]]` where the content contains the pairwise values.
   */
  def pairwiseWithValue[D <: Dimension, Q <: Position, F, W, T <: Tuner](slice: Slice[P, D], comparer: Comparer,
    operators: F, value: E[W], tuner: T)(implicit ev1: PosDimDep[P, D],
      ev2: OperableWithValue[F, slice.S, slice.R, Q, W], ev3: slice.S =!= Position0D, ev4: ClassTag[slice.S],
        ev5: ClassTag[slice.R], ev6: PairwiseTuners#V[T]): U[Cell[Q]]

  /**
   * Compute pairwise values between all values of this and that given a slice.
   *
   * @param slice     Encapsulates the dimension(s) along which to compute values.
   * @param comparer  Defines which element the pairwise operations should apply to.
   * @param that      Other matrix to compute pairwise values with.
   * @param operators The pairwise operators to apply.
   * @param tuner     The tuner for the job.
   *
   * @return A `U[Cell[slice.R#M]]` where the content contains the pairwise values.
   */
  def pairwiseBetween[D <: Dimension, Q <: Position, F, T <: Tuner](slice: Slice[P, D], comparer: Comparer, that: S,
    operators: F, tuner: T)(implicit ev1: PosDimDep[P, D], ev2: Operable[F, slice.S, slice.R, Q],
      ev3: slice.S =!= Position0D, ev4: ClassTag[slice.S], ev5: ClassTag[slice.R], ev6: PairwiseTuners#V[T]): U[Cell[Q]]

  /**
   * Compute pairwise values between all values of this and that given a slice with a user supplied value.
   *
   * @param slice     Encapsulates the dimension(s) along which to compute values.
   * @param comparer  Defines which element the pairwise operations should apply to.
   * @param that      Other matrix to compute pairwise values with.
   * @param operators The pairwise operators to apply.
   * @param value     The user supplied value.
   * @param tuner     The tuner for the job.
   *
   * @return A `U[Cell[slice.R#M]]` where the content contains the pairwise values.
   */
  def pairwiseBetweenWithValue[D <: Dimension, Q <: Position, F, W, T <: Tuner](slice: Slice[P, D], comparer: Comparer,
    that: S, operators: F, value: E[W], tuner: T)(implicit ev1: PosDimDep[P, D],
      ev2: OperableWithValue[F, slice.S, slice.R, Q, W], ev3: slice.S =!= Position0D, ev4: ClassTag[slice.S],
        ev5: ClassTag[slice.R], ev6: PairwiseTuners#V[T]): U[Cell[Q]]

  /**
   * Rename the coordinates of a dimension.
   *
   * @param renamer Function that renames coordinates.
   *
   * @return A `U[Cell[P]]` where the position has been renamed.
   */
  def rename(renamer: (Cell[P]) => P): U[Cell[P]]

  /**
   * Rename the coordinates of a dimension using user a suplied value.
   *
   * @param renamer Function that renames coordinates.
   * @param value   A `E` holding a user supplied value.
   *
   * @return A `U[Cell[P]]` where the position has been renamed.
   */
  def renameWithValue[W](renamer: (Cell[P], W) => P, value: E[W]): U[Cell[P]]

  /**
   * Sample a matrix according to some `sampler`. It keeps only those cells for which `sampler` returns true.
   *
   * @param samplers Sampling function(s).
   *
   * @return A `U[Cell[P]]` with the sampled cells.
   */
  def sample[F](samplers: F)(implicit ev: Sampleable[F, P]): U[Cell[P]]

  /**
   * Sample a matrix according to some `sampler` using a user supplied value. It keeps only those cells for which
   * `sampler` returns true.
   *
   * @param samplers Sampling function(s).
   * @param value    A `E` holding a user supplied value.
   *
   * @return A `U[Cell[P]]` with the sampled cells.
   */
  def sampleWithValue[F, W](samplers: F, value: E[W])(implicit ev: SampleableWithValue[F, P, W]): U[Cell[P]]

  /** Specifies tuners permitted on a call to `set` functions. */
  type SetTuners <: OneOf

  /**
   * Set `value` as the content for all `positions` in a matrix.
   *
   * @param positions The positions for which to set the contents.
   * @param value     The value to set.
   * @param tuner     The tuner for the job.
   *
   * @return A `U[Cell[P]]' where the `positions` have `value` as their content.
   */
  def set[I, T <: Tuner](positions: I, value: Content, tuner: T)(implicit ev1: PositionDistributable[I, P, U],
    ev2: ClassTag[P], ev3: SetTuners#V[T]): U[Cell[P]]

  /**
   * Set the `values` in a matrix.
   *
   * @param values The values to set.
   * @param tuner  The tuner for the job.
   *
   * @return A `U[Cell[P]]' with the `values` set.
   */
  def set[M, T <: Tuner](values: M, tuner: T)(implicit ev1: Matrixable[M, P, U], ev2: ClassTag[P],
    ev3: SetTuners#V[T]): U[Cell[P]]

  /** Specifies tuners permitted on a call to `shape`. */
  type ShapeTuners <: OneOf

  /**
   * Returns the shape of the matrix.
   *
   * @param tuner The tuner for the job.
   *
   * @return A `U[Cell[Position1D]]`. The position consists of a string value with the name of the dimension
   *         (`dim.toString`). The content has the actual size in it as a discrete variable.
   */
  def shape[T <: Tuner](tuner: T)(implicit ev: ShapeTuners#V[T]): U[Cell[Position1D]]

  /** Specifies tuners permitted on a call to `shape`. */
  type SizeTuners <: OneOf

  /**
   * Returns the size of the matrix in dimension `dim`.
   *
   * @param dim      The dimension for which to get the size.
   * @param distinct Indicates if each coordinate in dimension `dim` occurs only once. If this is the case, then
   *                 enabling this flag has better run-time performance.
   *
   * @return A `U[Cell[Position1D]]`. The position consists of a string value with the name of the dimension
   *         (`dim.toString`). The content has the actual size in it as a discrete variable.
   */
  def size[D <: Dimension, T <: Tuner](dim: D, distinct: Boolean = false, tuner: T)(implicit ev1: PosDimDep[P, D],
    ev2: SizeTuners#V[T]): U[Cell[Position1D]]

  /** Specifies tuners permitted on a call to `slice`. */
  type SliceTuners <: OneOf

  /**
   * Slice a matrix.
   *
   * @param slice     Encapsulates the dimension(s) to slice.
   * @param positions The position(s) within the dimension(s) to slice.
   * @param keep      Indicates if the `positions` should be kept or removed.
   * @param tuner     The tuner for the job.
   *
   * @return A `U[Cell[P]]' of the remaining content.
   */
  def slice[D <: Dimension, I, T <: Tuner](slice: Slice[P, D], positions: I, keep: Boolean, tuner: T)(
    implicit ev1: PosDimDep[P, D], ev2: PositionDistributable[I, slice.S, U], ev3: ClassTag[slice.S],
      ev4: SliceTuners#V[T]): U[Cell[P]]

  /** Specifies tuners permitted on a call to `slide` functions. */
  type SlideTuners <: OneOf

  /**
   * Create window based derived data.
   *
   * @param slice     Encapsulates the dimension(s) to slide over.
   * @param windows   The window functions to apply to the content.
   * @param ascending Indicator if the data should be sorted ascending or descending.
   * @param tuner     The tuner for the job.
   *
   * @return A `U[Cell[slice.S#M]]` with the derived data.
   */
  def slide[D <: Dimension, Q <: Position, F, T <: Tuner](slice: Slice[P, D], windows: F, ascending: Boolean = true,
    tuner: T)(implicit ev1: PosDimDep[P, D], ev2: Windowable[F, slice.S, slice.R, Q], ev3: slice.R =!= Position0D,
    ev4: ClassTag[slice.S], ev5: ClassTag[slice.R], ev6: SlideTuners#V[T]): U[Cell[Q]]

  /**
   * Create window based derived data with a user supplied value.
   *
   * @param slice     Encapsulates the dimension(s) to slide over.
   * @param windows   The window functions to apply to the content.
   * @param value     A `E` holding a user supplied value.
   * @param ascending Indicator if the data should be sorted ascending or descending.
   * @param tuner     The tuner for the job.
   *
   * @return A `U[Cell[slice.S#M]]` with the derived data.
   */
  def slideWithValue[D <: Dimension, Q <: Position, F, W, T <: Tuner](slice: Slice[P, D], windows: F, value: E[W],
    ascendig: Boolean = true, tuner: T)(implicit ev1: PosDimDep[P, D],
      ev2: WindowableWithValue[F, slice.S, slice.R, Q, W], ev3: slice.R =!= Position0D, ev4: ClassTag[slice.S],
        ev5: ClassTag[slice.R], ev6: SlideTuners#V[T]): U[Cell[Q]]

  /**
   * Partition a matrix according to `partitioner`.
   *
   * @param partitioners Assigns each position to zero, one or more partition(s).
   *
   * @return A `U[(Q, Cell[P])]` where `Q` is the partition for the corresponding tuple.
   */
  def split[Q, F](partitioners: F)(implicit ev: Partitionable[F, P, Q]): U[(Q, Cell[P])]

  /**
   * Partition a matrix according to `partitioner` using a user supplied value.
   *
   * @param partitioners Assigns each position to zero, one or more partition(s).
   * @param value        A `ValuePipe` holding a user supplied value.
   *
   * @return A `U[(Q, Cell[P])]` where `Q` is the partition for the corresponding tuple.
   */
  def splitWithValue[Q, F, W](partitioners: F, value: E[W])(
    implicit ev: PartitionableWithValue[F, P, Q, W]): U[(Q, Cell[P])]

  /**
   * Stream this matrix through `command` and apply `script`.
   *
   * @param command   The command to stream (pipe) the data through.
   * @param script    The script to apply to the data.
   * @param separator The separator to convert a cell to string.
   * @param parser    Function that parses the resulting string back to a cell.
   *
   * @return a `U[Cell[Q]]` with the new data.
   *
   * @note The `command` must be installed on each node of the cluster. Also, `script` must be a single self
   *       contained script. Lastly, `parser` functions are provided on the `Cell` object.
   */
  def stream[Q <: Position](command: String, script: String, separator: String,
    parser: String => TraversableOnce[Cell[Q]]): U[Cell[Q]]

  /** Specifies tuners permitted on a call to `summarise` functions. */
  type SummariseTuners <: OneOf

  /**
   * Summarise a matrix and return the aggregates.
   *
   * @param slice       Encapsulates the dimension(s) along which to aggregate.
   * @param aggregators The aggregator(s) to apply to the data.
   * @param tuner       The tuner for the job.
   *
   * @return A `U[Cell[Q]]` with the aggregates.
   */
  def summarise[D <: Dimension, Q <: Position, F, T <: Tuner](slice: Slice[P, D], aggregators: F, tuner: T)(
    implicit ev1: PosDimDep[P, D], ev2: Aggregatable[F, P, slice.S, Q], ev3: ClassTag[slice.S],
      ev4: SummariseTuners#V[T]): U[Cell[Q]]

  /**
   * Summarise a matrix, using a user supplied value, and return the aggregates.
   *
   * @param slice       Encapsulates the dimension(s) along which to aggregate.
   * @param aggregators The aggregator(s) to apply to the data.
   * @param value       A `E` holding a user supplied value.
   * @param tuner       The tuner for the job.
   *
   * @return A `U[Cell[Q]]` with the aggregates.
   */
  def summariseWithValue[D <: Dimension, Q <: Position, F, W, T <: Tuner](slice: Slice[P, D], aggregators: F,
    value: E[W], tuner: T)(implicit ev1: PosDimDep[P, D], ev2: AggregatableWithValue[F, P, slice.S, Q, W],
      ev3: ClassTag[slice.S], ev4: SummariseTuners#V[T]): U[Cell[Q]]

  /** Specifies tuners permitted on a call to `toMap` functions. */
  type ToMapTuners <: OneOf

  /**
   * Convert a matrix to an in-memory `Map`.
   *
   * @return A `E[Map[P, Content]]` containing the Map representation of this matrix.
   *
   * @note Avoid using this for very large matrices.
   */
  def toMap()(implicit ev: ClassTag[P]): E[Map[P, Content]]

  /**
   * Convert a matrix to an in-memory `Map`.
   *
   * @param slice Encapsulates the dimension(s) along which to convert.
   * @param tuner The tuner for the job.
   *
   * @return A `E[Map[slice.S, Slice.C]]` containing the Map representation of this matrix.
   *
   * @note Avoid using this for very large matrices.
   */
  def toMap[D <: Dimension, T <: Tuner](slice: Slice[P, D], tuner: T)(implicit ev1: PosDimDep[P, D],
    ev2: slice.S =!= Position0D, ev3: ClassTag[slice.S], ev4: ToMapTuners#V[T]): E[Map[slice.S, slice.C]]

  /**
   * Transform the content of a matrix.
   *
   * @param transformers The transformer(s) to apply to the content.
   *
   * @return A `U[Cell[Q]]` with the transformed cells.
   */
  def transform[Q <: Position, F](transformers: F)(implicit ev: Transformable[F, P, Q]): U[Cell[Q]]

  /**
   * Transform the content of a matrix using a user supplied value.
   *
   * @param transformers The transformer(s) to apply to the content.
   * @param value        A `E` holding a user supplied value.
   *
   * @return A `U[Cell[P]]` with the transformed cells.
   */
  def transformWithValue[Q <: Position, F, W](transformers: F, value: E[W])(
    implicit ev: TransformableWithValue[F, P, Q, W]): U[Cell[Q]]

  /** Specifies tuners permitted on a call to `types`. */
  type TypesTuners <: OneOf

  /**
   * Returns the variable type of the content(s) for a given `slice`.
   *
   * @param slice    Encapsulates the dimension(s) for this the types are to be returned.
   * @param specific Indicates if the most specific type should be returned, or it's generalisation (default).
   * @param tuner    The tuner for the job.
   *
   * @return A `U[(slice.S, Type)]` of the distinct position(s) together with their type.
   *
   * @see [[Types]]
   */
  def types[D <: Dimension, T <: Tuner](slice: Slice[P, D], specific: Boolean = false, tuner: T)(
    implicit ev1: PosDimDep[P, D], ev2: slice.S =!= Position0D, ev3: ClassTag[slice.S],
      ev4: TypesTuners#V[T]): U[(slice.S, Type)]

  /** Specifies tuners permitted on a call to `unique` functions. */
  type UniqueTuners <: OneOf

  /**
   * Return the unique (distinct) contents of an entire matrix.
   *
   * @param tuner The tuner for the job.
   */
  def unique[T <: Tuner](tuner: T)(implicit ev: UniqueTuners#V[T]): U[Content]

  /**
   * Return the unique (distinct) contents along a dimension.
   *
   * @param slice Encapsulates the dimension(s) along which to join.
   * @param tuner The tuner for the job.
   *
   * @return A `U[Cell[slice.S]]` consisting of the unique values.
   */
  // TODO: Should this return a Cell? Coordinates are no longer unique in the matrix.
  def unique[D <: Dimension, T <: Tuner](slice: Slice[P, D], tuner: T)(implicit ev1: slice.S =!= Position0D,
    ev2: UniqueTuners#V[T]): U[Cell[slice.S]]

  /** Specifies tuners permitted on a call to `which` functions. */
  type WhichTuners <: OneOf

  /**
   * Query the contents of a matrix and return the positions of those that match the predicate.
   *
   * @param predicate The predicate used to filter the contents.
   *
   * @return A `U[P]' of the positions for which the content matches `predicate`.
   */
  def which(predicate: Predicate)(implicit ev: ClassTag[P]): U[P]

  /**
   * Query the contents of the `positions` of a matrix and return the positions of those that match the predicate.
   *
   * @param slice     Encapsulates the dimension(s) to query.
   * @param positions The position(s) within the dimension(s) to query.
   * @param predicate The predicate used to filter the contents.
   * @param tuner     The tuner for the job.
   *
   * @return A `U[P]' of the positions for which the content matches `predicate`.
   */
  def which[D <: Dimension, I, T <: Tuner](slice: Slice[P, D], positions: I, predicate: Predicate, tuner: T)(
    implicit ev1: PosDimDep[P, D], ev2: PositionDistributable[I, slice.S, U], ev3: ClassTag[slice.S], ev4: ClassTag[P],
      ev5: WhichTuners#V[T]): U[P]

  /**
   * Query the contents of one of more positions of a matrix and return the positions of those that match the
   * corresponding predicates.
   *
   * @param slice   Encapsulates the dimension(s) to query.
   * @param pospred The list of position(s) within the dimension(s) to query together with the predicates used to
   *                filter the contents.
   * @param tuner   The tuner for the job.
   *
   * @return A `U[P]' of the positions for which the content matches predicates.
   */
  def which[D <: Dimension, I, T <: Tuner](slice: Slice[P, D], pospred: List[(I, Predicate)], tuner: T)(
    implicit ev1: PosDimDep[P, D], ev2: PositionDistributable[I, slice.S, U], ev3: ClassTag[slice.S], ev4: ClassTag[P],
      ev5: WhichTuners#V[T]): U[P]

  protected type TP1 = OneOf1[Default[NoParameters.type]]
  protected type TP2 = OneOf2[Default[NoParameters.type], Default[Reducers]]
  protected type TP3 = OneOf3[Default[NoParameters.type], Default[Reducers], Default[Sequence2[Reducers, Reducers]]]

  protected def toString(t: Cell[P], separator: String, descriptive: Boolean): String = {
    t.toString(separator, descriptive)
  }

  protected implicit def PositionOrdering[T <: Position] = Position.Ordering[T]()

  // TODO: Add more compile-time type checking
  // TODO: Add label join operations
  // TODO: Add read/write[CSV|Hive|VW|LibSVM] operations
  // TODO: Add statistics/dictionary into memory (from HDFS) operations
  // TODO: Is there a way not to use asInstanceOf[] as much?
  // TODO: Add machine learning operations (SVD/finding cliques/etc.) - use Spark instead?
}

/** Base trait for methods that reduce the number of dimensions or that can be filled. */
trait ReduceableMatrix[P <: Position with ReduceablePosition] { self: Matrix[P] =>
  /** Specifies tuners permitted on a call to `fill` with hetrogeneous data. */
  type FillHeterogeneousTuners <: OneOf

  /**
   * Fill a matrix with `values` for a given `slice`.
   *
   * @param slice  Encapsulates the dimension(s) on which to fill.
   * @param values The content to fill a matrix with.
   * @param tuner  The tuner for the job.
   *
   * @return A `U[Cell[P]]` where all missing values have been filled in.
   *
   * @note This joins `values` onto this matrix, as such it can be used for imputing missing values.
   */
  // TODO: Should missing positions (in `values`) be filtered out?
  def fill[D <: Dimension, Q <: Position, T <: Tuner](slice: Slice[P, D], values: U[Cell[Q]], tuner: T)(
    implicit ev1: PosDimDep[P, D], ev2: ClassTag[P], ev3: ClassTag[slice.S], ev4: slice.S =:= Q,
      ev5: FillHeterogeneousTuners#V[T]): U[Cell[P]]

  /** Specifies tuners permitted on a call to `fill` with homogeneous data. */
  type FillHomogeneousTuners <: OneOf

  /**
   * Fill a matrix with `value`.
   *
   * @param value The content to fill a matrix with.
   * @param tuner The tuner for the job.
   *
   * @return A `U[Cell[P]]` where all missing values have been filled in.
   */
  def fill[T <: Tuner](value: Content, tuner: T)(implicit ev1: ClassTag[P], ev2: FillHomogeneousTuners#V[T]): U[Cell[P]]

  /**
   * Melt one dimension of a matrix into another.
   *
   * @param dim       The dimension to melt
   * @param into      The dimension to melt into
   * @param separator The separator to use in the melt dimension
   *
   * @return A `U[Cell[P#L]]` with one fewer dimension.
   *
   * @note A melt coordinate is always a string value constructed from the string representation of the `dim` and
   *       `into` coordinates.
   */
  def melt[D <: Dimension, G <: Dimension](dim: D, into: G, separator: String = ".")(implicit ev1: PosDimDep[P, D],
    ev2: PosDimDep[P, G], ne: D =!= G): U[Cell[P#L]]

  /** Specifies tuners permitted on a call to `squash` functions. */
  type SquashTuners <: OneOf

  /**
   * Squash a dimension of a matrix.
   *
   * @param dim      The dimension to squash.
   * @param squasher The squasher that reduces two cells.
   * @param tuner    The tuner for the job.
   *
   * @return A `U[Cell[P#L]]` with the dimension `dim` removed.
   */
  def squash[D <: Dimension, F, T <: Tuner](dim: D, squasher: F, tuner: T)(implicit ev1: PosDimDep[P, D],
    ev2: Squashable[F, P], ev3: SquashTuners#V[T]): U[Cell[P#L]]

  /**
   * Squash a dimension of a matrix with a user supplied value.
   *
   * @param dim      The dimension to squash.
   * @param squasher The squasher that reduces two cells.
   * @param value    The user supplied value.
   * @param tuner    The tuner for the job.
   *
   * @return A `U[Cell[P#L]]` with the dimension `dim` removed.
   */
  def squashWithValue[D <: Dimension, F, W, T <: Tuner](dim: D, squasher: F, value: E[W], tuner: T)(
    implicit ev1: PosDimDep[P, D], ev2: SquashableWithValue[F, P, W], ev3: SquashTuners#V[T]): U[Cell[P#L]]
}

/** Base trait for methods that expands the number of dimension of a matrix. */
trait ExpandableMatrix[P <: Position with ExpandablePosition] { self: Matrix[P] =>
  /**
   * Expand a matrix with extra dimension(s).
   *
   * @param expander A function that expands each position with extra dimension(s).
   *
   * @return A `U[Cell[Q]]` with extra dimension(s) added.
   */
  def expand[Q <: Position](expander: Cell[P] => Q)(implicit ev: PosExpDep[P, Q]): U[Cell[Q]]

  /**
   * Expand a matrix with extra dimension(s) using a user supplied value.
   *
   * @param expander A function that expands each position with extra dimension(s).
   * @param value    A `E` holding a user supplied value.
   *
   * @return A `U[Cell[Q]]` with extra dimension(s) added.
   */
  def expandWithValue[Q <: Position, W](expander: (Cell[P], W) => Q, value: E[W])(
    implicit ev: PosExpDep[P, Q]): U[Cell[Q]]
}

/** Type class for transforming a type `T` into a `U[Cell[P]]`. */
trait Matrixable[T, P <: Position, U[_]] {
  /**
   * Returns a `U[Cell[P]]` for type `T`.
   *
   * @param t Object that can be converted to a `U[Cell[P]]`.
   */
  def convert(t: T): U[Cell[P]]
}

