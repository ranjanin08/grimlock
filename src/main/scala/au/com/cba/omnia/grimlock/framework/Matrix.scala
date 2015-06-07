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
  def parse1D(separator: String, first: Codex)(line: String): Option[Cell[Position1D]] = {
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
  def parse1DWithDictionary(dict: Map[String, Schema], separator: String, first: Codex)(
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
  def parse1DWithSchema(schema: Schema, separator: String, first: Codex)(line: String): Option[Cell[Position1D]] = {
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
  def parse2D(separator: String, first: Codex, second: Codex)(line: String): Option[Cell[Position2D]] = {
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
  def parse2DWithDictionary[D <: Dimension](dict: Map[String, Schema], dim: D, separator: String, first: Codex,
    second: Codex)(line: String)(implicit ev: PosDimDep[Position2D, D]): Option[Cell[Position2D]] = {
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
  def parse2DWithSchema(schema: Schema, separator: String, first: Codex, second: Codex)(
    line: String): Option[Cell[Position2D]] = {
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
  def parse3D(separator: String, first: Codex, second: Codex, third: Codex)(line: String): Option[Cell[Position3D]] = {
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
  def parse3DWithDictionary[D <: Dimension](dict: Map[String, Schema], dim: D, separator: String, first: Codex,
    second: Codex, third: Codex)(line: String)(implicit ev: PosDimDep[Position3D, D]): Option[Cell[Position3D]] = {
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
  def parse3DWithSchema(schema: Schema, separator: String, first: Codex, second: Codex, third: Codex)(
    line: String): Option[Cell[Position3D]] = {
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
   * Parse a line into a `List[Cell[Position2D]]` with column definitions.
   *
   * @param columns   `List[(String, Schema)]` describing each column in the table.
   * @param pkeyIndex Index (into `columns`) describing which column is the primary key.
   * @param separator The column separator.
   * @param line      The line to parse.
   */
  def parseTable(columns: List[(String, Schema)], pkeyIndex: Int, separator: String)(
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

  /**
   * Change the variable type of `positions` in a matrix.
   *
   * @param slice     Encapsulates the dimension(s) to change.
   * @param positions The position(s) within the dimension(s) to change.
   * @param schema    The schema to change to.
   *
   * @return A `U[Cell[P]]' with the changed contents.
   */
  def change[T, D <: Dimension](slice: Slice[P, D], positions: T, schema: Schema)(implicit ev1: PosDimDep[P, D],
    ev2: Nameable[T, P, slice.S, D, U], ev3: ClassTag[slice.S]): U[Cell[P]]

  /** Return all possible positions of a matrix. */
  def domain(): U[P]

  /**
   * Return contents of a matrix at `positions`.
   *
   * @param positions The positions for which to get the contents.
   *
   * @return A `U[Cell[P]]' of the `positions` together with their content.
   */
  def get[T](positions: T)(implicit ev1: PositionDistributable[T, P, U], ev2: ClassTag[P]): U[Cell[P]]

  /**
   * Join two matrices.
   *
   * @param slice Encapsulates the dimension(s) along which to join.
   * @param that  The matrix to join with.
   *
   * @return A `U[Cell[P]]` consisting of the inner-join of the two matrices.
   */
  // TODO: Add inner/left/right/outer join functionality?
  def join[D <: Dimension](slice: Slice[P, D], that: S)(implicit ev1: PosDimDep[P, D], ev2: P =!= Position1D,
    ev3: ClassTag[slice.S]): U[Cell[P]]

  /**
   * Returns the distinct position(s) (or names) for a given `slice`.
   *
   * @param slice Encapsulates the dimension(s) for which the names are to be returned.
   *
   * @return A `U[(Slice.S, Long)]` of the distinct position(s) together with a unique index.
   *
   * @note The position(s) are returned with an index so the return value can be used in various `save` methods. The
   *       index itself is unique for each position but no ordering is defined.
   *
   * @see [[Names]]
   */
  def names[D <: Dimension](slice: Slice[P, D])(implicit ev1: PosDimDep[P, D], ev2: slice.S =!= Position0D,
    ev3: ClassTag[slice.S]): U[(slice.S, Long)]

  /**
   * Compute pairwise values between all pairs of values given a slice.
   *
   * @param slice     Encapsulates the dimension(s) along which to compute values.
   * @param operators The pairwise operators to apply.
   *
   * @return A `U[Cell[slice.R#M]]` where the content contains the pairwise values.
   */
  def pairwise[D <: Dimension, Q <: Position, T](slice: Slice[P, D], operators: T)(implicit ev1: PosDimDep[P, D],
    ev2: Operable[T, slice.S, slice.R, Q], ev3: slice.S =!= Position0D, ev4: ClassTag[slice.S],
    ev5: ClassTag[slice.R]): U[Cell[Q]]

  /**
   * Compute pairwise values between all pairs of values given a slice with a user supplied value.
   *
   * @param slice     Encapsulates the dimension(s) along which to compute values.
   * @param operators The pairwise operators to apply.
   * @param value     The user supplied value.
   *
   * @return A `U[Cell[slice.R#M]]` where the content contains the pairwise values.
   */
  def pairwiseWithValue[D <: Dimension, Q <: Position, T, W](slice: Slice[P, D], operators: T, value: E[W])(
    implicit ev1: PosDimDep[P, D], ev2: OperableWithValue[T, slice.S, slice.R, Q, W], ev3: slice.S =!= Position0D,
    ev4: ClassTag[slice.S], ev5: ClassTag[slice.R]): U[Cell[Q]]

  /**
   * Compute pairwise values between all values of this and that given a slice.
   *
   * @param slice     Encapsulates the dimension(s) along which to compute values.
   * @param that      Other matrix to compute pairwise values with.
   * @param operators The pairwise operators to apply.
   *
   * @return A `U[Cell[slice.R#M]]` where the content contains the pairwise values.
   */
  def pairwiseBetween[D <: Dimension, Q <: Position, T](slice: Slice[P, D], that: S, operators: T)(
    implicit ev1: PosDimDep[P, D], ev2: Operable[T, slice.S, slice.R, Q], ev3: slice.S =!= Position0D,
    ev4: ClassTag[slice.S], ev5: ClassTag[slice.R]): U[Cell[Q]]

  /**
   * Compute pairwise values between all values of this and that given a slice with a user supplied value.
   *
   * @param slice     Encapsulates the dimension(s) along which to compute values.
   * @param that      Other matrix to compute pairwise values with.
   * @param operators The pairwise operators to apply.
   * @param value     The user supplied value.
   *
   * @return A `U[Cell[slice.R#M]]` where the content contains the pairwise values.
   */
  def pairwiseBetweenWithValue[D <: Dimension, Q <: Position, T, W](slice: Slice[P, D], that: S, operators: T,
    value: E[W])(implicit ev1: PosDimDep[P, D], ev2: OperableWithValue[T, slice.S, slice.R, Q, W],
    ev3: slice.S =!= Position0D, ev4: ClassTag[slice.S], ev5: ClassTag[slice.R]): U[Cell[Q]]

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
  def renameWithValue[V](renamer: (Cell[P], V) => P, value: E[V]): U[Cell[P]]

  /**
   * Sample a matrix according to some `sampler`. It keeps only those cells for which `sampler` returns true.
   *
   * @param samplers Sampling function(s).
   *
   * @return A `U[Cell[P]]` with the sampled cells.
   */
  def sample[T](samplers: T)(implicit ev: Sampleable[T, P]): U[Cell[P]]

  /**
   * Sample a matrix according to some `sampler` using a user supplied value. It keeps only those cells for which
   * `sampler` returns true.
   *
   * @param samplers Sampling function(s).
   * @param value    A `E` holding a user supplied value.
   *
   * @return A `U[Cell[P]]` with the sampled cells.
   */
  def sampleWithValue[T, W](samplers: T, value: E[W])(implicit ev: SampleableWithValue[T, P, W]): U[Cell[P]]

  /**
   * Set `value` as the content for all `positions` in a matrix.
   *
   * @param positions The positions for which to set the contents.
   * @param value     The value to set.
   *
   * @return A `U[Cell[P]]' where the `positions` have `value` as their content.
   */
  def set[T](positions: T, value: Content)(implicit ev1: PositionDistributable[T, P, U], ev2: ClassTag[P]): U[Cell[P]]

  /**
   * Set the `values` in a matrix.
   *
   * @param values The values to set.
   *
   * @return A `U[Cell[P]]' with the `values` set.
   */
  def set[T](values: T)(implicit ev1: Matrixable[T, P, U], ev2: ClassTag[P]): U[Cell[P]]

  /**
   * Returns the shape of the matrix.
   *
   * @return A `U[Cell[Position1D]]`. The position consists of a string value with the name of the dimension
   *         (`dim.toString`). The content has the actual size in it as a discrete variable.
   */
  def shape(): U[Cell[Position1D]]

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
  def size[D <: Dimension](dim: D, distinct: Boolean = false)(implicit ev: PosDimDep[P, D]): U[Cell[Position1D]]

  /**
   * Slice a matrix.
   *
   * @param slice     Encapsulates the dimension(s) to slice.
   * @param positions The position(s) within the dimension(s) to slice.
   * @param keep      Indicates if the `positions` should be kept or removed.
   *
   * @return A `U[Cell[P]]' of the remaining content.
   */
  def slice[T, D <: Dimension](slice: Slice[P, D], positions: T, keep: Boolean)(implicit ev1: PosDimDep[P, D],
    ev2: Nameable[T, P, slice.S, D, U], ev3: ClassTag[slice.S]): U[Cell[P]]

  /**
   * Create window based derived data.
   *
   * @param slice   Encapsulates the dimension(s) to slide over.
   * @param windows The window functions to apply to the content.
   *
   * @return A `U[Cell[slice.S#M]]` with the derived data.
   */
  def slide[D <: Dimension, Q <: Position, T](slice: Slice[P, D], windows: T)(implicit ev1: PosDimDep[P, D],
    ev2: Windowable[T, slice.S, slice.R, Q], ev3: slice.R =!= Position0D, ev4: ClassTag[slice.S],
    ev5: ClassTag[slice.R]): U[Cell[Q]]

  /**
   * Create window based derived data with a user supplied value.
   *
   * @param slice   Encapsulates the dimension(s) to slide over.
   * @param windows The window functions to apply to the content.
   * @param value   A `E` holding a user supplied value.
   *
   * @return A `U[Cell[slice.S#M]]` with the derived data.
   */
  def slideWithValue[D <: Dimension, Q <: Position, T, W](slice: Slice[P, D], windows: T, value: E[W])(
    implicit ev1: PosDimDep[P, D], ev2: WindowableWithValue[T, slice.S, slice.R, Q, W], ev3: slice.R =!= Position0D,
    ev4: ClassTag[slice.S], ev5: ClassTag[slice.R]): U[Cell[Q]]

  /**
   * Partition a matrix according to `partitioner`.
   *
   * @param partitioners Assigns each position to zero, one or more partition(s).
   *
   * @return A `U[(I, Cell[P])]` where `I` is the partition for the corresponding tuple.
   */
  def split[I, T](partitioners: T)(implicit ev: Partitionable[T, P, I]): U[(I, Cell[P])]

  /**
   * Partition a matrix according to `partitioner` using a user supplied value.
   *
   * @param partitioners Assigns each position to zero, one or more partition(s).
   * @param value        A `ValuePipe` holding a user supplied value.
   *
   * @return A `U[(I, Cell[P])]` where `I` is the partition for the corresponding tuple.
   */
  def splitWithValue[I: Ordering, T, W](partitioners: T, value: E[W])(
    implicit ev: PartitionableWithValue[T, P, I, W]): U[(I, Cell[P])]

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
    parser: String => Option[Cell[Q]]): U[Cell[Q]]

  /**
   * Summarise a matrix and return the aggregates with an expanded position.
   *
   * @param slice       Encapsulates the dimension(s) along which to aggregate.
   * @param aggregators The aggregator(s) to apply to the data.
   *
   * @return A `U[Cell[slice.S#M]]` with the aggregates.
   *
   * @note If the `slice` is an `Over` then the returned position will be a `Position2D` since `Slice.S` for `Over` is
   *       a `Position1D` and that expands to `Position2D`. Analogously, if the `slice` is an `Along` then the returned
   *       position will be equal to `P`.
   */
  def summariseAndExpand[T, D <: Dimension](slice: Slice[P, D], aggregators: T)(implicit ev1: PosDimDep[P, D],
    ev2: AggregatableMultiple[T], ev3: ClassTag[slice.S]): U[Cell[slice.S#M]]

  /**
   * Summarise a matrix, using a user supplied value, and return the aggregates with an expanded position.
   *
   * @param slice       Encapsulates the dimension(s) along which to aggregate.
   * @param aggregators The aggregator(s) to apply to the data.
   * @param value       A `E` holding a user supplied value.
   *
   * @return A `U[Cell[slice.S#M]]` with the aggregates.
   *
   * @note If the `slice` is an `Over` then the returned position will be a `Position2D` since `Slice.S` for `Over` is
   *       a `Position1D` and that expands to `Position2D`. Analogously, if the `slice` is an `Along` then the returned
   *       position will be equal to `P`.
   */
  def summariseAndExpandWithValue[T, D <: Dimension, V](slice: Slice[P, D], aggregators: T, value: E[V])(
    implicit ev1: PosDimDep[P, D], ev2: AggregatableMultipleWithValue[T, V], ev3: ClassTag[slice.S]): U[Cell[slice.S#M]]

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
   *
   * @return A `E[Map[Slice.S, Slice.C]]` containing the Map representation of this matrix.
   *
   * @note Avoid using this for very large matrices.
   */
  def toMap[D <: Dimension](slice: Slice[P, D])(implicit ev1: PosDimDep[P, D], ev2: slice.S =!= Position0D,
    ev3: ClassTag[slice.S]): E[Map[slice.S, slice.C]]

  /**
   * Transform the content of a matrix.
   *
   * @param transformers The transformer(s) to apply to the content.
   *
   * @return A `U[Cell[Q]]` with the transformed cells.
   */
  def transform[Q <: Position, T](transformers: T)(implicit ev1: Transformable[T, P, Q],
    ev2: ExpPosDep[P, Q]): U[Cell[Q]]

  /**
   * Transform the content of a matrix using a user supplied value.
   *
   * @param transformers The transformer(s) to apply to the content.
   * @param value        A `E` holding a user supplied value.
   *
   * @return A `U[Cell[P]]` with the transformed cells.
   */
  def transformWithValue[Q <: Position, T, W](transformers: T, value: E[W])(
    implicit ev1: TransformableWithValue[T, P, Q, W], ev2: ExpPosDep[P, Q]): U[Cell[Q]]

  /**
   * Returns the variable type of the content(s) for a given `slice`.
   *
   * @param slice    Encapsulates the dimension(s) for this the types are to be returned.
   * @param specific Indicates if the most specific type should be returned, or it's generalisation (default).
   *
   * @return A `U[(Slice.S, Type)]` of the distinct position(s) together with their type.
   *
   * @see [[Types]]
   */
  def types[D <: Dimension](slice: Slice[P, D], specific: Boolean = false)(implicit ev1: PosDimDep[P, D],
    ev2: slice.S =!= Position0D, ev3: ClassTag[slice.S]): U[(slice.S, Type)]

  /** Return the unique (distinct) contents of an entire matrix. */
  def unique(): U[Content]

  /**
   * Return the unique (distinct) contents along a dimension.
   *
   * @param slice Encapsulates the dimension(s) along which to join.
   *
   * @return A `U[Cell[slice.S]]` consisting of the unique values.
   */
  // TODO: Should this return a Cell? Coordinates are no longer unique in the matrix.
  def unique[D <: Dimension](slice: Slice[P, D])(implicit ev: slice.S =!= Position0D): U[Cell[slice.S]]

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
   *
   * @return A `U[P]' of the positions for which the content matches `predicate`.
   */
  def which[T, D <: Dimension](slice: Slice[P, D], positions: T, predicate: Predicate)(implicit ev1: PosDimDep[P, D],
    ev2: Nameable[T, P, slice.S, D, U], ev3: ClassTag[slice.S], ev4: ClassTag[P]): U[P]

  /**
   * Query the contents of one of more positions of a matrix and return the positions of those that match the
   * corresponding predicates.
   *
   * @param slice   Encapsulates the dimension(s) to query.
   * @param pospred The list of position(s) within the dimension(s) to query together with the predicates used to
   *                filter the contents.
   *
   * @return A `U[P]' of the positions for which the content matches predicates.
   */
  def which[T, D <: Dimension](slice: Slice[P, D], pospred: List[(T, Predicate)])(implicit ev1: PosDimDep[P, D],
    ev2: Nameable[T, P, slice.S, D, U], ev3: ClassTag[slice.S], ev4: ClassTag[P]): U[P]

  protected def toString(t: Cell[P], separator: String, descriptive: Boolean): String = {
    t.toString(separator, descriptive)
  }

  protected implicit def PositionOrdering[T <: Position] = Position.Ordering[T]

  // TODO: Add more compile-time type checking
  // TODO: Add label join operations
  // TODO: Add read/write[CSV|Hive|VW|LibSVM] operations
  // TODO: Add statistics/dictionary into memory (from HDFS) operations
  // TODO: Is there a way not to use asInstanceOf[] as much?
  // TODO: Add machine learning operations (SVD/finding cliques/etc.) - use Spark instead?
}

/** Base trait for methods that reduce the number of dimensions or that can be filled. */
trait ReduceableMatrix[P <: Position with ReduceablePosition] { self: Matrix[P] =>
  /**
   * Fill a matrix with `values` for a given `slice`.
   *
   * @param slice  Encapsulates the dimension(s) on which to fill.
   * @param values The content to fill a matrix with.
   *
   * @return A `U[Cell[P]]` where all missing values have been filled in.
   *
   * @note This joins `values` onto this matrix, as such it can be used for imputing missing values.
   */
  def fill[D <: Dimension, Q <: Position](slice: Slice[P, D], values: U[Cell[Q]])(implicit ev1: PosDimDep[P, D],
    ev2: ClassTag[P], ev3: ClassTag[slice.S], ev4: slice.S =:= Q): U[Cell[P]]

  /**
   * Fill a matrix with `value`.
   *
   * @param value The content to fill a matrix with.
   *
   * @return A `U[Cell[P]]` where all missing values have been filled in.
   */
  def fill(value: Content)(implicit ev: ClassTag[P]): U[Cell[P]]

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
  def melt[D <: Dimension, F <: Dimension](dim: D, into: F, separator: String = ".")(implicit ev1: PosDimDep[P, D],
    ev2: PosDimDep[P, F], ne: D =!= F): U[Cell[P#L]]

  /**
   * Summarise a matrix.
   *
   * @param slice      Encapsulates the dimension(s) along which to aggregate.
   * @param aggregator The aggregator to apply to the data.
   *
   * @return A `U[Cell[slice.S]]` with the aggregates.
   */
  def summarise[D <: Dimension](slice: Slice[P, D], aggregator: Aggregator with Prepare with PresentSingle)(
    implicit ev1: PosDimDep[P, D], ev2: ClassTag[slice.S]): U[Cell[slice.S]]

  /**
   * Summarise a matrix, using a user supplied value.
   *
   * @param slice      Encapsulates the dimension(s) along which to aggregate.
   * @param aggregator The aggregator to apply to the data.
   * @param value      A `E` holding a user supplied value.
   *
   * @return A `U[Cell[slice.S]]` with the aggregates.
   */
  def summariseWithValue[D <: Dimension, W](slice: Slice[P, D],
    aggregator: Aggregator with PrepareWithValue with PresentSingleWithValue { type V >: W }, value: E[W])(
      implicit ev1: PosDimDep[P, D], ev2: ClassTag[slice.S]): U[Cell[slice.S]]

  /**
   * Squash a dimension of a matrix.
   *
   * @param dim      The dimension to squash.
   * @param squasher The squasher that reduces two cells.
   *
   * @return A `U[Cell[P#L]]` with the dimension `dim` removed.
   */
  def squash[D <: Dimension, T](dim: D, squasher: T)(implicit ev1: PosDimDep[P, D], ev2: Squashable[T, P]): U[Cell[P#L]]

  /**
   * Squash a dimension of a matrix with a user supplied value.
   *
   * @param dim      The dimension to squash.
   * @param squasher The squasher that reduces two cells.
   * @param value    The user supplied value.
   *
   * @return A `U[Cell[P#L]]` with the dimension `dim` removed.
   */
  def squashWithValue[D <: Dimension, T, W](dim: D, squasher: T, value: E[W])(implicit ev1: PosDimDep[P, D],
    ev2: SquashableWithValue[T, P, W]): U[Cell[P#L]]
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
  def expand[Q <: Position](expander: Cell[P] => Q)(implicit ev: ExpPosDep[P, Q]): U[Cell[Q]]

  /**
   * Expand a matrix with extra dimension(s) using a user supplied value.
   *
   * @param expander A function that expands each position with extra dimension(s).
   * @param value    A `E` holding a user supplied value.
   *
   * @return A `U[Cell[Q]]` with extra dimension(s) added.
   */
  def expandWithValue[Q <: Position, V](expander: (Cell[P], V) => Q, value: E[V])(
    implicit ev: ExpPosDep[P, Q]): U[Cell[Q]]
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

