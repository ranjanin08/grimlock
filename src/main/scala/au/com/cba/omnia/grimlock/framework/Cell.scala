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

package au.com.cba.omnia.grimlock.framework

import au.com.cba.omnia.grimlock.framework.content._
import au.com.cba.omnia.grimlock.framework.encoding._
import au.com.cba.omnia.grimlock.framework.position._

import com.twitter.scrooge.ThriftStruct

import java.util.regex.Pattern

import org.apache.hadoop.io.Writable

import scala.util.Try

import shapeless._
import shapeless.ops.hlist._
import shapeless.ops.traversable.FromTraversable

private trait PosFromArgs extends DepFn0 with Serializable

private object PosFromArgs {
  type Aux[Out0] = PosFromArgs { type Out = Out0 }

  def apply(implicit p: PosFromArgs): Aux[p.Out] = p

  implicit def position1D[A <: Value]: Aux[(A :: HNil) => Position1D] =
    new PosFromArgs {
      type Out = (A :: HNil) => Position1D

      def apply(): Out = { case a :: HNil => Position1D(a) }
    }

  implicit def position2D[A <: Value, B <: Value]: Aux[(A :: B :: HNil) => Position2D] =
    new PosFromArgs {
      type Out = (A :: B :: HNil) => Position2D

      def apply(): Out = { case a :: b :: HNil => Position2D(a, b) }
    }

  implicit def position3D[A <: Value, B <: Value, C <: Value]: Aux[(A :: B :: C :: HNil) => Position3D] =
    new PosFromArgs {
      type Out = (A :: B :: C :: HNil) => Position3D

      def apply(): Out = { case a :: b :: c :: HNil => Position3D(a, b, c) }
    }

  implicit def position4D[A <: Value, B <: Value, C <: Value, D <: Value]: Aux[(A :: B :: C :: D :: HNil) => Position4D] =
    new PosFromArgs {
      type Out = (A :: B :: C :: D :: HNil) => Position4D

      def apply(): Out = { case a :: b :: c :: d :: HNil => Position4D(a, b, c, d) }
    }

  implicit def position5D[A <: Value, B <: Value, C <: Value, D <: Value, E <: Value]: Aux[(A :: B :: C :: D :: E :: HNil) => Position5D] =
    new PosFromArgs {
      type Out = (A :: B :: C :: D :: E :: HNil) => Position5D

      def apply(): Out = { case a :: b :: c :: d :: e :: HNil => Position5D(a, b, c, d, e) }
    }
}

private trait CellFactory[P <: Product] {
  type Out

  def apply(cds: P, pos: Array[String], value: String, decoder: Content.Parser): Out
}

private object CellFactory {
  type Aux[P <: Product, Out0] = CellFactory[P] { type Out = Out0 }

  object toFns extends Poly1 {
    implicit def default[C <: Codec]: Case.Aux[C, String => Option[Value]] = at[C]((c: C) => (s: String) => c.decode(s))
  }

  object Sequence extends Poly2 {
    implicit def caseSome[L <: HList, T](implicit prep: Prepend[L, T :: HNil]) = at[Option[L], Option[T]] {
      case (a, v) =>
        for {
          acc <- a
          value <- v
        } yield (acc :+ value)
    }
  }

  implicit def makeCell[R <: Position, P <: Product, L <: HList, CO <: HList, MC <: HList, AO <: HList, FO <: HList](
    implicit gen: Generic.Aux[P, L], cm: ConstMapper.Aux[String, L, CO], tr: FromTraversable[CO],
      ma: Mapper.Aux[toFns.type, L, MC], za: ZipApply.Aux[MC, CO, AO],
        f: LeftFolder.Aux[AO, Option[HNil.type], Sequence.type, Option[FO]],
          p: PosFromArgs.Aux[FO => R]): Aux[P, Option[Cell[R]]] = new CellFactory[P] {
    type Out = Option[Cell[R]]

    def apply(cds: P, pos: Array[String], value: String, decoder: Content.Parser): Out = {
      for {
        rawPositionsHList <- tr(pos)
        positionOptions = gen.to(cds) map toFns zipApply rawPositionsHList
        optionPositions <- (positionOptions.foldLeft(Option(HNil))(Sequence))
        position = p()(optionPositions)
        content <- decoder(value)
      } yield Cell(position, content)
    }
  }
}

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
   * @param codec       Indicator if codec is required or not.
   * @param schema      Indicator if schema is required or not.
   */
  def toShortString(separator: String, codec: Boolean = true, schema: Boolean = true): String = {
    position.toShortString(separator) + separator + content.toShortString(separator, codec, schema)
  }
}

/** Companion object to the Cell class. */
object Cell {
  /** Predicate used in, for example, the `which` methods of a matrix for finding content. */
  type Predicate[P <: Position] = Cell[P] => Boolean

  /** Type for parsing a string into either a `Cell[P]` or an error message. */
  type TextParser[P <: Position] = (String) => TraversableOnce[Either[String, Cell[P]]]

  /** Type for parsing a key value tuple into either a `Cell[P]` or an error message. */
  type SequenceParser[K <: Writable, V <: Writable, P <: Position] = (K, V) => TraversableOnce[Either[String, Cell[P]]]

  /** Type for parsing Parquet data. */
  type ParquetParser[T <: ThriftStruct, P <: Position] = (T) => TraversableOnce[Either[String, Cell[P]]]

  /**
   * Parse a line into a `Cell[Position1D]`.
   *
   * @param separator The column separator.
   * @param first     The codec for decoding the first dimension.
   * @param line      The line to parse.
   */
  def parse1D(separator: String = "|", first: Codec = StringCodec)(
    line: String): Option[Either[String, Cell[Position1D]]] = {
    parseXD[Position1D, Tuple1[Codec]](1, separator, Tuple1(first), line)
  }

  /**
   * Parse a line data into a `Cell[Position1D]` with a dictionary.
   *
   * @param dict      The dictionary describing the features in the data.
   * @param separator The column separator.
   * @param first     The codec for decoding the first dimension.
   * @param line      The line to parse.
   */
  def parse1DWithDictionary(dict: Map[String, Content.Parser], separator: String = "|", first: Codec = StringCodec)(
    line: String): Option[Either[String, Cell[Position1D]]] = {
    parseXDWithDictionary[Position1D, Tuple1[Codec], First.type](1, First, dict, separator, Tuple1(first), line)
  }

  /**
   * Parse a line into a `Cell[Position1D]` with a schema.
   *
   * @param schema    The schema for decoding the data.
   * @param separator The column separator.
   * @param first     The codec for decoding the first dimension.
   * @param line      The line to parse.
   */
  def parse1DWithSchema(schema: Content.Parser, separator: String = "|", first: Codec = StringCodec)(
    line: String): Option[Either[String, Cell[Position1D]]] = {
    parseXDWithSchema[Position1D, Tuple1[Codec]](1, schema, separator, Tuple1(first), line)
  }

  /**
   * Parse a line into a `Cell[Position2D]`.
   *
   * @param separator The column separator.
   * @param first     The codec for decoding the first dimension.
   * @param second    The codec for decoding the second dimension.
   * @param line      The line to parse.
   */
  def parse2D(separator: String = "|", first: Codec = StringCodec, second: Codec = StringCodec)(
    line: String): Option[Either[String, Cell[Position2D]]] = {
    parseXD[Position2D, (Codec, Codec)](2, separator, (first, second), line)
  }

  /**
   * Parse a line into a `Cell[Position2D]` with a dictionary.
   *
   * @param dict      The dictionary describing the features in the data.
   * @param dim       The dimension on which to apply the dictionary.
   * @param separator The column separator.
   * @param first     The codec for decoding the first dimension.
   * @param second    The codec for decoding the second dimension.
   * @param line      The line to parse.
   */
  def parse2DWithDictionary[D <: Dimension](dict: Map[String, Content.Parser], dim: D, separator: String = "|",
    first: Codec = StringCodec, second: Codec = StringCodec)(line: String)(
      implicit ev:PosDimDep[Position2D, D]): Option[Either[String, Cell[Position2D]]] = {
    parseXDWithDictionary[Position2D, (Codec, Codec), D](2, dim, dict, separator, (first, second), line)
  }

  /**
   * Parse a line into a `Cell[Position2D]` with a schema.
   *
   * @param schema    The schema for decoding the data.
   * @param separator The column separator.
   * @param first     The codec for decoding the first dimension.
   * @param second    The codec for decoding the second dimension.
   * @param line      The line to parse.
   */
  def parse2DWithSchema(schema: Content.Parser, separator: String = "|", first: Codec = StringCodec,
    second: Codec = StringCodec)(line: String): Option[Either[String, Cell[Position2D]]] = {
    parseXDWithSchema[Position2D, (Codec,Codec)](2, schema, separator, (first, second), line)
  }

  /**
   * Parse a line into a `Cell[Position3D]`.
   *
   * @param separator The column separator.
   * @param first     The codec for decoding the first dimension.
   * @param second    The codec for decoding the second dimension.
   * @param third     The codec for decoding the third dimension.
   * @param line      The line to parse.
   */
  def parse3D(separator: String = "|", first: Codec = StringCodec, second: Codec = StringCodec,
    third: Codec = StringCodec)(line: String): Option[Either[String, Cell[Position3D]]] = {
    parseXD[Position3D, (Codec, Codec, Codec)](3, separator, (first, second, third), line)
  }

  /**
   * Parse a line into a `Cell[Position3D]` with a dictionary.
   *
   * @param dict      The dictionary describing the features in the data.
   * @param dim       The dimension on which to apply the dictionary.
   * @param separator The column separator.
   * @param first     The codec for decoding the first dimension.
   * @param second    The codec for decoding the second dimension.
   * @param third     The codec for decoding the third dimension.
   * @param line      The line to parse.
   */
  def parse3DWithDictionary[D <: Dimension](dict: Map[String, Content.Parser], dim: D, separator: String = "|",
    first: Codec = StringCodec, second: Codec = StringCodec, third: Codec = StringCodec)(line: String)(
      implicit ev: PosDimDep[Position3D, D]): Option[Either[String, Cell[Position3D]]] = {
    parseXDWithDictionary[Position3D, (Codec, Codec, Codec), D](3, dim, dict, separator, (first, second, third), line)
  }

  /**
   * Parse a line into a `Cell[Position3D]` with a schema.
   *
   * @param schema    The schema for decoding the data.
   * @param separator The column separator.
   * @param first     The codec for decoding the first dimension.
   * @param second    The codec for decoding the second dimension.
   * @param third     The codec for decoding the third dimension.
   * @param line      The line to parse.
   */
  def parse3DWithSchema(schema: Content.Parser, separator: String = "|", first: Codec = StringCodec,
    second: Codec = StringCodec, third: Codec = StringCodec)(
      line: String): Option[Either[String, Cell[Position3D]]] = {
    parseXDWithSchema[Position3D, (Codec,Codec, Codec)](3, schema, separator, (first, second, third), line)
  }

  /**
   * Parse a line into a `Cell[Position4D]`.
   *
   * @param separator The column separator.
   * @param first     The codec for decoding the first dimension.
   * @param second    The codec for decoding the second dimension.
   * @param third     The codec for decoding the third dimension.
   * @param fourth    The codec for decoding the fourth dimension.
   * @param line      The line to parse.
   */
  def parse4D(separator: String = "|", first: Codec = StringCodec, second: Codec = StringCodec,
    third: Codec = StringCodec, fourth: Codec = StringCodec)(
      line: String): Option[Either[String, Cell[Position4D]]] = {
    parseXD[Position4D, (Codec, Codec, Codec, Codec)](4, separator, (first, second, third, fourth), line)
  }

  /**
   * Parse a line into a `Cell[Position4D]` with a dictionary.
   *
   * @param dict      The dictionary describing the features in the data.
   * @param dim       The dimension on which to apply the dictionary.
   * @param separator The column separator.
   * @param first     The codec for decoding the first dimension.
   * @param second    The codec for decoding the second dimension.
   * @param third     The codec for decoding the third dimension.
   * @param fourth    The codec for decoding the fourth dimension.
   * @param line      The line to parse.
   */
  def parse4DWithDictionary[D <: Dimension](dict: Map[String, Content.Parser], dim: D, separator: String = "|",
    first: Codec = StringCodec, second: Codec = StringCodec, third: Codec = StringCodec, fourth: Codec = StringCodec)(
      line: String)(implicit ev: PosDimDep[Position4D, D]): Option[Either[String, Cell[Position4D]]] = {
    parseXDWithDictionary[Position4D, (Codec, Codec, Codec, Codec), D](4, dim, dict, separator,
      (first, second, third, fourth), line)
  }

  /**
   * Parse a line into a `Cell[Position4D]` with a schema.
   *
   * @param schema    The schema for decoding the data.
   * @param separator The column separator.
   * @param first     The codec for decoding the first dimension.
   * @param second    The codec for decoding the second dimension.
   * @param third     The codec for decoding the third dimension.
   * @param fourth    The codec for decoding the fourth dimension.
   * @param line      The line to parse.
   */
  def parse4DWithSchema(schema: Content.Parser, separator: String = "|", first: Codec = StringCodec,
    second: Codec = StringCodec, third: Codec = StringCodec, fourth: Codec = StringCodec)(
      line: String): Option[Either[String, Cell[Position4D]]] = {
    parseXDWithSchema[Position4D, (Codec,Codec, Codec, Codec)](4, schema, separator,
      (first, second, third, fourth), line)
  }

  /**
   * Parse a line into a `Cell[Position5D]`.
   *
   * @param separator The column separator.
   * @param first     The codec for decoding the first dimension.
   * @param second    The codec for decoding the second dimension.
   * @param third     The codec for decoding the third dimension.
   * @param fourth    The codec for decoding the fourth dimension.
   * @param fifth     The codec for decoding the fifth dimension.
   * @param line      The line to parse.
   */
  def parse5D(separator: String = "|", first: Codec = StringCodec, second: Codec = StringCodec,
    third: Codec = StringCodec, fourth: Codec = StringCodec, fifth: Codec = StringCodec)(
      line: String): Option[Either[String, Cell[Position5D]]] = {
    parseXD[Position5D, (Codec, Codec, Codec, Codec, Codec)](5, separator, (first, second, third, fourth, fifth), line)
  }

  /**
   * Parse a line into a `Cell[Position5D]` with a dictionary.
   *
   * @param dict      The dictionary describing the features in the data.
   * @param dim       The dimension on which to apply the dictionary.
   * @param separator The column separator.
   * @param first     The codec for decoding the first dimension.
   * @param second    The codec for decoding the second dimension.
   * @param third     The codec for decoding the third dimension.
   * @param fourth    The codec for decoding the fourth dimension.
   * @param fifth     The codec for decoding the fifth dimension.
   * @param line      The line to parse.
   */
  def parse5DWithDictionary[D <: Dimension](dict: Map[String, Content.Parser], dim: D, separator: String = "|",
    first: Codec = StringCodec, second: Codec = StringCodec, third: Codec = StringCodec, fourth: Codec = StringCodec,
      fifth: Codec = StringCodec)(line: String)(
        implicit ev: PosDimDep[Position5D, D]): Option[Either[String, Cell[Position5D]]] = {
    parseXDWithDictionary[Position5D, (Codec, Codec, Codec, Codec, Codec), D](5, dim, dict, separator,
      (first, second, third, fourth, fifth), line)
  }

  /**
   * Parse a line into a `Cell[Position5D]` with a schema.
   *
   * @param schema    The schema for decoding the data.
   * @param separator The column separator.
   * @param first     The codec for decoding the first dimension.
   * @param second    The codec for decoding the second dimension.
   * @param third     The codec for decoding the third dimension.
   * @param fourth    The codec for decoding the fourth dimension.
   * @param fifth     The codec for decoding the fifth dimension.
   * @param line      The line to parse.
   */
  def parse5DWithSchema(schema: Content.Parser, separator: String = "|", first: Codec = StringCodec,
    second: Codec = StringCodec, third: Codec = StringCodec, fourth: Codec = StringCodec, fifth: Codec = StringCodec)(
      line: String): Option[Either[String, Cell[Position5D]]] = {
    parseXDWithSchema[Position5D, (Codec, Codec,Codec, Codec, Codec)](5, schema, separator,
      (first, second, third, fourth, fifth), line)
  }

  /**
   * Parse a line into a `List[Cell[Position2D]]` with column definitions.
   *
   * @param columns   `List[(String, Content.Parser)]` describing each column in the table.
   * @param pkeyIndex Index (into `columns`) describing which column is the primary key.
   * @param separator The column separator.
   * @param line      The line to parse.
   */
  def parseTable(columns: List[(String, Content.Parser)], pkeyIndex: Int = 0, separator: String = "\u0001")(
    line: String): List[Either[String, Cell[Position2D]]] = {
    val parts = line.trim.split(Pattern.quote(separator), columns.length)

    (parts.length == columns.length) match {
      case true =>
        val pkey = parts(pkeyIndex)

        columns.zipWithIndex.flatMap {
          case ((name, decoder), idx) if (idx != pkeyIndex) =>
            decoder(parts(idx)) match {
              case Some(con) => Some(Right(Cell(Position2D(pkey, name), con)))
              case _ => Some(Left("Unable to decode: '" + line + "'"))
            }
          case _ => None
        }
      case _ => List(Left("Unable to split: '" + line + "'"))
    }
  }

  /**
   * Return function that returns a string representation of a cell.
   *
   * @param descriptive Indicator if descriptive string is required or not.
   * @param separator   The separator to use between various fields (only used if descriptive is `false`).
   * @param codec       Indicator if codec is required or not (only used if descriptive is `false`).
   * @param schema      Indicator if schema is required or not (only used if descriptive is `false`).
   */
  def toString[P <: Position](descriptive: Boolean = false, separator: String = "|",
    codec: Boolean = true, schema: Boolean = true): (Cell[P]) => TraversableOnce[String] = {
    (t: Cell[P]) => List(if (descriptive) { t.toString } else { t.toShortString(separator, codec, schema) })
  }

  private def parseXD[R <: Position, P <: Product](split: Int, separator: String = "|", codecs: P, line: String)(
    implicit mc: CellFactory.Aux[P, Option[Cell[R]]]): Option[Either[String, Cell[R]]] =
    line.trim.split(Pattern.quote(separator), split + 3).splitAt(split) match {
      case (pos, Array(c, s, v)) =>
        mc(codecs, pos, v, (v: String) => Content.fromShortString(c + separator + s + separator + v, separator))
          .map(Right(_)) orElse Some(Left("Unable to decode: '" + line + "'"))
      case _ => Some(Left("Unable to split: '" + line + "'"))
    }

  private def parseXDWithSchema[R <: Position, P <: Product](split: Int, schema: Content.Parser,
    separator: String = "|", codecs: P, line: String)(
      implicit mc: CellFactory.Aux[P, Option[Cell[R]]]): Option[Either[String, Cell[R]]] = {
    line.trim.split(Pattern.quote(separator), split + 1).splitAt(split) match {
      case (pos, Array(v)) =>
        mc(codecs, pos, v, schema)
          .map(Right(_)) orElse Some(Left("Unable to decode: '" + line + "'"))
      case _ => Some(Left("Unable to split: '" + line + "'"))
    }
  }

  private def parseXDWithDictionary[R <: Position, P <: Product, D <: Dimension](split: Int, dim: D,
    dict: Map[String, Content.Parser], separator: String = "|", codecs: P, line: String)(
      implicit mc: CellFactory.Aux[P, Option[Cell[R]]]): Option[Either[String, Cell[R]]] = {
    def getD(positions: Array[String]): Option[Content.Parser] = for {
      pos <- Try(positions(dim.index)).toOption
      decoder <- dict.get(pos)
    } yield decoder

    line.trim.split(Pattern.quote(separator), split + 1).splitAt(split) match {
      case (pos, Array(v)) => getD(pos) match {
        case Some(decoder) =>
          mc(codecs, pos, v, decoder)
            .map(Right(_)) orElse Some(Left("Unable to decode: '" + line + "'"))
        case _ => Some(Left("Missing schema for: '" + line + "'"))
      }
      case _ => Some(Left("Unable to split: '" + line + "'"))
    }
  }
}

