// Copyright 2014 Commonwealth Bank of Australia
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

import au.com.cba.omnia.grimlock.content._
import au.com.cba.omnia.grimlock.content.encoding._
import au.com.cba.omnia.grimlock.content.metadata._
import au.com.cba.omnia.grimlock.content.metadata.Dictionary._
import au.com.cba.omnia.grimlock.content.variable._
import au.com.cba.omnia.grimlock.derive._
import au.com.cba.omnia.grimlock.Matrix._
import au.com.cba.omnia.grimlock.pairwise._
import au.com.cba.omnia.grimlock.partition._
import au.com.cba.omnia.grimlock.position._
import au.com.cba.omnia.grimlock.reduce._
import au.com.cba.omnia.grimlock.sample._
import au.com.cba.omnia.grimlock.squash._
import au.com.cba.omnia.grimlock.transform._
import au.com.cba.omnia.grimlock.utility.{ =!=, Miscellaneous => Misc }

import cascading.flow.FlowDef
import com.twitter.scalding._
import com.twitter.scalding.TDsl._, Dsl._
import com.twitter.scalding.typed.IterablePipe

/**
 * Rich wrapper around a `TypedPipe[(`[[position.Position]]`,
 * `[[content.Content]]`)]`.
 *
 * @param data `TypedPipe[(`[[position.Position]]`, `[[content.Content]]`)]`.
 */
trait Matrix[P <: Position] {
  protected val data: TypedPipe[Cell[P]]

  protected implicit def PositionOrdering[T <: Position] = {
    new Ordering[T] { def compare(l: T, r: T) = l.compare(r) }
  }

  /**
   * Returns the distinct [[position.Position]](s) (or names) for a given
   * `slice`.
   *
   * @param slice Encapsulates the dimension(s) for which the names are to
   *        be returned.
   *
   * @return A Scalding `TypedPipe[(`[[Slice.S]]`, Long)]` of the distinct
   *         [[position.Position]](s) together with a unique index.
   *
   * @note The [[position.Position]](s) are returned with an index so the
   *       return value can be used in various `write` methods. The index
   *       itself is unique for each [[position.Position]] but no ordering
   *       is defined.
   *
   * @see [[Names]], [[Matrix2D]]
   */
  def names[D <: Dimension](slice: Slice[P, D])(
    implicit ev: PosDimDep[P, D]): TypedPipe[(slice.S, Long)] = {
    Names.number(data.map { case (p, c) => slice.selected(p) }.distinct)
  }

  /**
   * Returns the [[content.variable.Type]] of the [[content.Content]](s)
   * for a given `slice`.
   *
   * @param slice    Encapsulates the dimension(s) for this the types are
   *                 to be returned.
   * @param specific Indicates if the most specific type should be returned,
   *                 or it's generalisation (default).
   *
   * @return A Scalding `TypedPipe[(`[[Slice.S]]`,
   *         `[[content.variable.Type]]`)]` of the distinct
   *         [[position.Position]](s) together with their type.
   *
   * @see [[Types]], [[content.variable.Type]]
   */
  def types[D <: Dimension](slice: Slice[P, D], specific: Boolean = false)(
    implicit ev: PosDimDep[P, D]): TypedPipe[(slice.S, Type)] = {
    data
      .map { case (p, c) => (slice.selected(p), c.schema.kind) }
      .groupBy { case (p, t) => p }
      .reduce[(slice.S, Type)] {
        case ((lp, lt), (rp, rt)) =>
          (lp,
            if (lt == rt) {
              lt
            } else if (!specific && lt.isSpecialisationOf(rt)) {
              rt
            } else if (!specific && rt.isSpecialisationOf(lt)) {
              lt
            } else {
              Type.Mixed
            })
      }
      .values
      .map { case (p, t) => (p, if (specific) t else t.getGeneralisation()) }
  }

  /**
   * Returns the size of the [[Matrix]] in dimension `dim`.
   *
   * @param dim      The [[position.Dimension]] for which to get the size.
   * @param distinct Indicates if the [[position.coordinate.Coordinate]]s in
   *                 dimension `dim` are unique. If this is the case, then
   *                 enabling this flag has better run-time performance.
   *
   * @return A Scalding `TypedPipe[(`[[position.Position1D]]`,
   *         `[[content.Content]]`)]`. The [[position.Position1D]] consists
   *         a [[position.coordinate.StringCoordinate]] with the name of the
   *         dimension (`dim.toString`). The [[content.Content]] has the
   *         actual size in it as a [[content.variable.Type.Discrete]]
   *         variable.
   */
  def size[D <: Dimension](dim: D, distinct: Boolean = false)(
    implicit ev: PosDimDep[P, D]): TypedPipe[Cell[Position1D]] = {
    val coords = data.map { case (p, c) => p.get(dim) }
    val dist = distinct match {
      case true => coords
      case false => coords.distinct
    }

    dist
      .map { case _ => 1L }
      .sum
      .map {
        case sum => (Position1D(dim.toString),
          Content(DiscreteSchema[Codex.LongCodex](), sum))
      }
  }

  /**
   * Returns the shape of the [[Matrix]].
   *
   * @return A Scalding `TypedPipe[(`[[position.Position1D]]`,
   *         `[[content.Content]]`)]`. The [[position.Position1D]] consists
   *         a [[position.coordinate.StringCoordinate]] with the name of the
   *         dimension (`dim.toString`). The [[content.Content]] has the
   *         actual size in it as a [[content.variable.Type.Discrete]]
   *         variable.
   */
  def shape(): TypedPipe[Cell[Position1D]] = {
    data
      .flatMap { case (p, c) => p.coordinates.map(_.toString).zipWithIndex }
      .distinct
      .groupBy { case (s, i) => i }
      .size
      .map {
        case (i, s) => (Position1D(Dimension.All(i).toString),
          Content(DiscreteSchema[Codex.LongCodex](), s))
      }
  }

  /**
   * Slice a [[Matrix]].
   *
   * @param slice     Encapsulates the dimension(s) to slice.
   * @param positions The [[position.Position]](s) within the dimension(s)
   *                  to slice.
   * @param keep      Indicates if the `positions` should be kept or removed.
   *
   * @return A Scalding `TypedPipe[(P, `[[content.Content]]`)]' of the
   *         remaining content.
   *
   * @see [[Nameable]]
   */
  def slice[T, D <: Dimension](slice: Slice[P, D], positions: T,
    keep: Boolean)(implicit ev1: Nameable[T, P, slice.S, D],
      ev2: PosDimDep[P, D]): TypedPipe[Cell[P]] = {
    val pos = ev1.convert(this, slice, positions)

    val wanted = keep match {
      case true => pos
      case false =>
        names(slice)
          .groupBy { case (p, i) => p }
          .leftJoin(pos.groupBy { case (p, i) => p })
          .flatMap {
            case (p, (lpi, rpi)) => rpi match {
              case Some(_) => None
              case None => Some(lpi)
            }
          }
    }

    data
      .groupBy { case (p, c) => slice.selected(p) }
      .join(wanted.groupBy { case (p, i) => p })
      .map { case (_, ((p, c), _)) => (p, c) }
  }

  /**
   * Predicate used in, for example, the `which` methods of a [[Matrix]]
   * for finding content.
   */
  type Predicate = (P, Content) => Boolean

  /**
   * Query the [[content.Content]]s of a [[Matrix]] and return the
   * [[position.Position]]s of those that match the predicate.
   *
   * @param predicate The predicate used to filter the contents.
   *
   * @return A Scalding `TypedPipe[(P)]' of the [[position.Position]]s for
   *         which the [[content.Content]] matches `predicate`.
   *
   * @see [[content.Content]]
   */
  def which(predicate: Predicate): TypedPipe[P] = {
    data.collect { case (p, c) if predicate(p, c) => p }
  }

  /**
   * Query the [[content.Content]]s of the `positions` of a [[Matrix]] and
   * return the [[position.Position]]s of those that match the predicate.
   *
   * @param slice     Encapsulates the dimension(s) to query.
   * @param positions The [[position.Position]](s) within the dimension(s)
   *                  to query.
   * @param predicate The predicate used to filter the contents.
   *
   * @return A Scalding `TypedPipe[(P)]' of the [[position.Position]]s for
   *         which the [[content.Content]] matches `predicate`.
   *
   * @see [[content.Content]], [[Nameable]]
   */
  def which[T, D <: Dimension](slice: Slice[P, D], positions: T,
    predicate: Predicate)(implicit ev1: Nameable[T, P, slice.S, D],
      ev2: PosDimDep[P, D]): TypedPipe[P] = {
    data
      .groupBy { case (p, c) => slice.selected(p) }
      .join(ev1.convert(this, slice, positions).groupBy { case (p, i) => p })
      .collect { case (s, ((p, c), pi)) if predicate(p, c) => p }
  }
  // TODO: Add def which(slice: Slice[P], List[(T, Predicate)]) version

  /**
   * Return contents of a [[Matrix]] at `positions`.
   *
   * @param positions The positions for which to get the contents.
   *
   * @return A Scalding `TypedPipe[(P, `[[content.Content]]`)]' of the
   *         `positions` together with their [[content.Content]].
   *
   * @see [[position.PositionPipeable]]
   */
  def get[T](positions: T)(
    implicit ev: PositionPipeable[T, P]): TypedPipe[Cell[P]] = {
    data
      .groupBy { case (p, c) => p }
      .leftJoin(ev.convert(positions).groupBy { case p => p })
      .flatMap {
        case (_, ((p, c), po)) => po match {
          case Some(_) => Some((p, c))
          case None => None
        }
      }
  }

  /**
   * Convert a [[Matrix]] to an in-memory `Map`.
   *
   * @param slice Encapsulates the dimension(s) along which to convert.
   *
   * @return A Scalding `ValuePipe[Map[`[[Slice.S]], [[Slice.C]]`]]` containing
   *         the Map representation of this [[Matrix]].
   *
   * @note Avoid using this for very large matrices.
   */
  def toMap[D <: Dimension](slice: Slice[P, D])(
    implicit ev: PosDimDep[P, D]): ValuePipe[Map[slice.S, slice.C]] = {
    data
      .map { case (p, c) => (p, slice.toMap(p, c)) }
      .groupBy { case (p, m) => slice.selected(p) }
      .reduce[(P, Map[slice.S, slice.C])] {
        case ((lp, lm), (rp, rm)) => (lp, slice.combineMaps(lp, lm, rm))
      }
      .map { case (_, (_, m)) => m }
      .sum(new com.twitter.algebird.Semigroup[Map[slice.S, slice.C]] {
        def plus(l: Map[slice.S, slice.C],
          r: Map[slice.S, slice.C]): Map[slice.S, slice.C] = {
          l ++ r
        }
      })
  }

  /**
   * Reduce a [[Matrix]] and return the reductions with an expanded
   * [[position.Position]].
   *
   * @param slice    Encapsulates the dimension(s) along which to reduce.
   * @param reducers The reducer(s) to apply to the data.
   *
   * @return A Scalding `TypedPipe[(`[[position.ExpandablePosition.M]]`,
   *         `[[content.Content]]`)]`. Where the
   *         [[position.ExpandablePosition.M]] is relative to [[Slice.S]].
   *
   * @note If the `slice` is an [[Over]] then the returned
   *       [[position.Position]] will be a [[position.Position2D]] since
   *       [[Slice.S]] for [[Over]] is a [[position.Position1D]] and that
   *       expands to [[position.Position2D]]. Analogously, if the `slice`
   *       is an [[Along]] then the returned [[position.Position]] will be
   *       equal to `P`.
   *
   * @see [[reduce.ReducerableMultiple]], [[Slice]],
   *      [[position.ExpandablePosition]]
   */
  def reduceAndExpand[T, D <: Dimension](slice: Slice[P, D],
    reducers: T)(implicit ev1: ReducerableMultiple[T],
      ev2: PosDimDep[P, D]): TypedPipe[Cell[slice.S#M]] = {
    val reducer = ev1.convert(reducers)

    data
      .map { case (p, c) => (slice.selected(p), reducer.prepare(slice, p, c)) }
      .groupBy { case (p, t) => p }
      .reduce[(slice.S, reducer.T)] {
        case ((lp, lt), (rp, rt)) => (lp, reducer.reduce(lt, rt))
      }
      .values
      .flatMap {
        case (p, t) => Misc.mapFlatten(reducer.presentMultiple(p, t))
      }
  }

  /**
   * Reduce a [[Matrix]], using a user supplied value, and return the
   * reductions with an expanded [[position.Position]].
   *
   * @param slice    Encapsulates the dimension(s) along which to reduce.
   * @param reducers The reducer(s) to apply to the data.
   * @param value    A `ValuePipe` holding a user supplied value.
   *
   * @return A Scalding `TypedPipe[(`[[position.ExpandablePosition.M]]`,
   *         `[[content.Content]]`)]`. Where the
   *         [[position.ExpandablePosition.M]] is relative to [[Slice.S]].
   *
   * @note If the `slice` is an [[Over]] then the returned
   *       [[position.Position]] will be a [[position.Position2D]] since
   *       [[Slice.S]] for [[Over]] is a [[position.Position1D]] and that
   *       expands to [[position.Position2D]]. Analogously, if the `slice`
   *       is an [[Along]] then the returned [[position.Position]] will be
   *       equal to `P`.
   *
   * @see [[reduce.ReducerableMultiple]], [[Slice]],
   *      [[position.ExpandablePosition]]
   */
  def reduceAndExpandWithValue[T, D <: Dimension, V](slice: Slice[P, D],
    reducers: T, value: ValuePipe[V])(
      implicit ev1: ReducerableMultipleWithValue[T, V],
      ev2: PosDimDep[P, D]): TypedPipe[Cell[slice.S#M]] = {
    val reducer = ev1.convert(reducers)

    data
      .leftCross(value)
      .map {
        case ((p, c), vo) => (slice.selected(p),
          reducer.prepare(slice, p, c, vo.get.asInstanceOf[reducer.V]))
      }
      .groupBy { case (p, t) => p }
      .reduce[(slice.S, reducer.T)] {
        case ((lp, lt), (rp, rt)) => (lp, reducer.reduce(lt, rt))
      }
      .values
      .flatMap {
        case (p, t) => Misc.mapFlatten(reducer.presentMultiple(p, t))
      }
  }

  /**
   * Partition a [[Matrix]] according to `partitioner`.
   *
   * @param partitioner Assigns each [[position.Position]] to zero, one or
   *                    more partitions.
   *
   * @return A Scalding `TypedPipe[(T, (P, `[[content.Content]]`))]` where
   *         `T` is the partition for the corresponding tuple.
   *
   * @see [[partition.Partitioner]]
   */
  def partition[S: Ordering](partitioner: Partitioner with Assign { type T = S }): TypedPipe[(S, Cell[P])] = {
    data.flatMap {
      case (p, c) => Misc.mapFlatten(partitioner.assign(p), (p, c))
    }
  }

  /**
   * Partition a [[Matrix]] according to `partitioner` using a user supplied
   * value.
   *
   * @param partitioner Assigns each [[position.Position]] to zero, one or
   *                    more partitions.
   * @param value       A `ValuePipe` holding a user supplied value.
   *
   * @return A Scalding `TypedPipe[(T, (P, `[[content.Content]]`))]` where
   *         `T` is the partition for the corresponding tuple.
   *
   * @see [[partition.Partitioner]]
   */
  def partitionWithValue[S: Ordering, W](partitioner: Partitioner with AssignWithValue { type V >: W; type T = S },
    value: ValuePipe[W]): TypedPipe[(S, Cell[P])] = {
    data.flatMapWithValue(value) {
      case ((p, c), vo) => Misc.mapFlatten(
        partitioner.assign(p, vo.get), (p, c))
    }
  }

  /**
   * Refine (filter) a [[Matrix]] according to some function `f`. It keeps
   * only those cells for which `f` returns true.
   *
   * @param f Filtering function.
   *
   * @return A Scalding `TypedPipe[(P, `[[content.Content]]`)]`.
   */
  def refine(f: (P, Content) => Boolean): TypedPipe[Cell[P]] = {
    data.filter { case (p, c) => f(p, c) }
  }

  /**
   * Refine (filter) a [[Matrix]] according to some function `f` using a
   * user supplied value. It keeps only those cells for which `f` returns
   * true.
   *
   * @param f     Filtering function.
   * @param value A `ValuePipe` holding a user supplied value.
   *
   * @return A Scalding `TypedPipe[(P, `[[content.Content]]`)]`.
   */
  def refineWithValue[V](f: (P, Content, V) => Boolean,
    value: ValuePipe[V]): TypedPipe[Cell[P]] = {
    data.filterWithValue(value) { case ((p, c), vo) => f(p, c, vo.get) }
  }

  /**
   * Sample a [[Matrix]] according to some function `f`. It keeps only those
   * cells for which `f` returns true.
   *
   * @param sampler Sampling function.
   *
   * @return A Scalding `TypedPipe[(P, `[[content.Content]]`)]`.
   */
  def sample(sampler: Sampler with Select): TypedPipe[Cell[P]] = {
    data.filter { case (p, c) => sampler.select(p) }
  }

  /**
   * Sample a [[Matrix]] according to some function `f` using a user supplied
   * value. It keeps only those cells for which `f` returns true.
   *
   * @param sampler Sampling function.
   * @param value A `ValuePipe` holding a user supplied value.
   *
   * @return A Scalding `TypedPipe[(P, `[[content.Content]]`)]`.
   */
  def sampleWithValue[W](sampler: Sampler with SelectWithValue { type V >: W },
    value: ValuePipe[W]): TypedPipe[Cell[P]] = {
    data.filterWithValue(value) {
      case ((p, c), vo) => sampler.select(p, vo.get)
    }
  }

  /**
   * Return all possible [[position.Position]]s of a [[Matrix]].
   *
   * @return A Scalding `TypedPipe[P]`
   *
   * @see [[position.PositionPipe]]
   */
  def domain(): TypedPipe[P]

  /**
   * Join two matrices.
   *
   * @param slice Encapsulates the dimension(s) along which to join.
   * @param that  The [[Matrix]] to join with.
   *
   * @return A Scalding `TypedPipe[(P, `[[content.Content]]`)]` consisting of
   *         the inner-join of the two matrices.
   */
  def join[D <: Dimension](slice: Slice[P, D], that: Matrix[P])(
    implicit ev: PosDimDep[P, D]): TypedPipe[Cell[P]] = {
    val keep = names(slice)
      .groupBy { case (p, i) => p }
      .join(that.names(slice).groupBy { case (p, i) => p })

    data
      .groupBy { case (p, c) => slice.selected(p) }
      .join(keep)
      .values
      .map { case ((p, c), pi) => (p, c) } ++
      that
      .data
      .groupBy { case (p, c) => slice.selected(p) }
      .join(keep)
      .values
      .map { case ((p, c), pi) => (p, c) }
  }

  /**
   * Return the unique (distinct) [[content.Content]]s of a [[Matrix]].
   *
   * @see [[content.ContentPipe]]
   */
  def unique(): TypedPipe[Content] = {
    data
      .map { case (p, c) => c.toShortString("|") }
      .distinct
      .flatMap {
        case s =>
          val parts = s.split("\\|")

          Schema.fromString(parts(1), parts(0)).flatMap {
            case s => s.decode(parts(2))
          }
      }
  }
  // TODO: Add def unique(slice: Slice): TypedPipe[Cell[slice.S]]

  /**
   * Persist a [[Matrix]] to disk.
   *
   * @param file        Name of the output file.
   * @param separator   Separator to use between [[position.Position]] and
   *                    [[content.Content]].
   * @param descriptive Indicates if the output should be descriptive.
   *
   * @return A Scalding `TypedPipe[(P, `[[content.Content]]`)]` which is
   *         this [[Matrix]].
   */
  def persist(file: String, separator: String = "|",
    descriptive: Boolean = false)(implicit flow: FlowDef,
      mode: Mode): TypedPipe[Cell[P]] = {
    data
      .map {
        case (p, c) => descriptive match {
          case true => p.toString + separator + c.toString
          case false => p.toShortString(separator) + separator +
            c.toShortString(separator)
        }
      }
      .toPipe('line)
      .write(TextLine(file))

    data
  }

  // TODO: Add more compile-time type checking
  // TODO: Add label join operations
  // TODO: Add read/write[CSV|Hive|VW|LibSVM] operations
  // TODO: Add (pairwise) distance operations
  // TODO: Add statistics/dictionary into memory (from HDFS) operations
  // TODO: Is there a way not to use asInstanceOf[]?

  // TODO: Add matrix algebra operations (add/sub/div/mul - traditional &
  //       element wise) - use Spark instead?
  // TODO: Add machine learning operations (SVD/bucketing/finding
  //       cliques/etc.) - use Spark instead?
}

/** Define operations that modify a [[Matrix]]. */
trait ModifyableMatrix[P <: Position with ModifyablePosition] {
  self: Matrix[P] =>

  /**
   * Change the [[content.variable.Type]] of `positions` in a [[Matrix]].
   *
   * @param slice     Encapsulates the dimension(s) to change.
   * @param positions The [[position.Position]](s) within the dimension(s)
   *                  to change.
   * @param schema    The [[content.metadata.Schema]] to change to.
   *
   * @return A Scalding `TypedPipe[(P, `[[content.Content]]`)]' of the
   *         changed contents.
   *
   * @see [[Nameable]], [[content.variable.Type]]
   */
  def change[T, D <: Dimension](slice: Slice[P, D], positions: T,
    schema: Schema)(implicit ev1: Nameable[T, P, slice.S, D],
      ev2: PosDimDep[P, D]): TypedPipe[Cell[P]] = {
    data
      .groupBy { case (p, c) => slice.selected(p) }
      .leftJoin(ev1.convert(this, slice, positions).groupBy {
        case (p, i) => p
      })
      .values
      .flatMap {
        case ((p, c), po) => po match {
          case Some(_) => schema.decode(c.value.toShortString).map {
            case con => (p, con)
          }
          case None => Some((p, c))
        }
      }
  }

  /**
   * Set `value` as the [[content.Content]] for all `positions` in a
   * [[Matrix]].
   *
   * @param positions The positions for which to set the contents.
   * @param value     The value to set.
   *
   * @return A Scalding `TypedPipe[(P, `[[content.Content]]`)]' where
   *         the `positions` have `value` as their [[content.Content]].
   *
   * @see [[position.PositionPipeable]]
   */
  def set[T](positions: T, value: Content)(implicit ev: PositionPipeable[T, P],
    flow: FlowDef, mode: Mode): TypedPipe[Cell[P]] = {
    set((positions, value))
  }

  /**
   * Set the `values` in a [[Matrix]].
   *
   * @param values The values to set.
   *
   * @return A Scalding `TypedPipe[(P, `[[content.Content]]`)]' with
   *         the `values` set.
   *
   * @see [[Matrixable]]
   */
  def set[T](values: T)(implicit ev: Matrixable[T, P], flow: FlowDef,
    mode: Mode): TypedPipe[Cell[P]] = {
    data
      .groupBy { case (p, c) => p }
      .leftJoin(ev.convert(values).groupBy { case (p, c) => p })
      .map {
        case (_, ((p, c), co)) => co match {
          case Some((sp, sc)) => (sp, sc)
          case None => (p, c)
        }
      }
  }

  /**
   * Transform the [[content.Content]] of a [[Matrix]].
   *
   * @param transformers The transformer(s) to apply to the [[content.Content]].
   *
   * @return A Scalding `TypedPipe[(`[[position.Position.S]]`,
   *         `[[content.Content]]`)]`
   *
   * @see [[transform.Transformable]], [[transform.Transformer]]
   */
  def transform[T](transformers: T)(
    implicit ev: Transformable[T]): TypedPipe[Cell[P#S]] = {
    val t = ev.convert(transformers)

    data.flatMap { case (p, c) => Misc.mapFlatten(t.present(p, c)) }
  }
  /**
   * Transform the [[content.Content]] of a [[Matrix]] using a user
   * supplied value.
   *
   * @param transformers The transformer(s) to apply to the [[content.Content]].
   * @param value        A `ValuePipe` holding a user supplied value.
   *
   * @return A Scalding `TypedPipe[(`[[position.Position.S]]`,
   *         `[[content.Content]]`)]`
   *
   * @see [[transform.Transformable]], [[transform.Transformer]]
   */
  def transformWithValue[T, V](transformers: T, value: ValuePipe[V])(
    implicit ev: TransformableWithValue[T, V]): TypedPipe[Cell[P#S]] = {
    val t = ev.convert(transformers)

    data.flatMapWithValue(value) {
      case ((p, c), vo) => Misc.mapFlatten(t.present(p, c,
        vo.get.asInstanceOf[t.V]))
    }
  }

  /**
   * Create window based derived data.
   *
   * @param slice   Encapsulates the dimension(s) to derive over.
   * @param deriver The [[derive.Deriver]] to apply to the [[content.Content]].
   *
   * @return A Scalding `TypedPipe[(`[[position.Position.S]]`,
   *         `[[content.Content]]`)]` with the derived data.
   *
   * @see [[derive.Deriver]]
   */
  def derive[D <: Dimension](slice: Slice[P, D],
    deriver: Deriver with Initialise)(
      implicit ev: PosDimDep[P, D]): TypedPipe[Cell[P#S]] = {
    data
      .groupBy { case (p, c) => slice.selected(p) }
      .sortBy { case (p, c) => slice.remainder(p) }
      .scanLeft(
        Option.empty[(deriver.T, Option[Either[Cell[P#S], List[Cell[P#S]]]])]) {
          case (None, curr) => Some((deriver.initialise(curr), None))
          case (Some((t, c)), curr) => Some(deriver.present(curr, t))
        }
      .flatMap {
        case (p, Some((t, oe @ Some(_)))) => Misc.mapFlatten(oe)
        case _ => List()
      }
  }
  /**
   * Create window based derived data with a user supplied value.
   *
   * @param slice   Encapsulates the dimension(s) to derive over.
   * @param deriver The [[derive.Deriver]] to apply to the [[content.Content]].
   * @param value   A `ValuePipe` holding a user supplied value.
   *
   * @return A Scalding `TypedPipe[(`[[position.Position.S]]`,
   *         `[[content.Content]]`)]` with the derived data.
   *
   * @see [[derive.Deriver]]
   */
  def deriveWithValue[D <: Dimension, W](slice: Slice[P, D], deriver: Deriver with InitialiseWithValue { type V >: W }, value: ValuePipe[W])(
    implicit ev: PosDimDep[P, D]): TypedPipe[Cell[P#S]] = {
    data
      .leftCross(value)
      .groupBy { case ((p, c), vo) => slice.selected(p) }
      .sortBy { case ((p, c), vo) => slice.remainder(p) }
      .scanLeft(
        Option.empty[(deriver.T, Option[Either[Cell[P#S], List[Cell[P#S]]]])]) {
          case (None, (curr, vo)) =>
            Some((deriver.initialise(curr, vo.get.asInstanceOf[deriver.V]), None))
          case (Some((t, c)), (curr, vo)) => Some(deriver.present(curr, t))
        }
      .flatMap {
        case (p, Some((t, oe @ Some(_)))) => Misc.mapFlatten(oe)
        case _ => List()
      }
  }

  /**
   * Fill a [[Matrix]] with `value`.
   *
   * @param value The [[content.Content]] to fill a [[Matrix]] with.
   *
   * @return A Scalding `TypedPipe[(P`, `[[content.Content]]`)]` where all
   *         missing values have been filled in.
   */
  def fill(value: Content): TypedPipe[Cell[P]] = {
    domain
      .groupBy { case p => p }
      .leftJoin(data.groupBy { case (p, c) => p })
      .map {
        case (p, (_, co)) => co match {
          case Some((_, c)) => (p, c)
          case None => (p, value)
        }
      }
  }

  /**
   * Fill a [[Matrix]] with `values` for a given `slice`.
   *
   * @param slice  Encapsulates the dimension(s) on which to fill.
   * @param values The [[content.Content]] to fill a [[Matrix]] with.
   *
   * @return A Scalding `TypedPipe[(P`, `[[content.Content]]`)]` where all
   *         missing values have been filled in.
   *
   * @note This joins `values` onto this [[Matrix]], as such it can
   *       be used for imputing missing values.
   */
  def fill[D <: Dimension](slice: Slice[P, D], values: TypedPipe[Cell[P]])(
    implicit ev: PosDimDep[P, D]): TypedPipe[Cell[P]] = {
    val dense = domain
      .groupBy { case p => slice.selected(p) }
      .join(values.groupBy { case (p, c) => slice.selected(p) })
      .map { case (_, (p, (_, c))) => (p, c) }

    dense
      .groupBy { case (p, c) => p }
      .leftJoin(data.groupBy { case (p, c) => p })
      .map {
        case (p, ((_, fc), co)) => co match {
          case Some((_, c)) => (p, c)
          case None => (p, fc)
        }
      }
  }

  /**
   * Rename the [[position.coordinate.Coordinate]]s of a dimension.
   *
   * @param dim     The [[position.Dimension]] to rename.
   * @param renamer Function that renames [[position.coordinate.Coordinate]]s.
   *
   * @return A Scalding `TypedPipe[(`[[position.Position.S]]`,
   *         `[[content.Content]]`)]` where the dimension `dim` has been
   *         renamed.
   */
  def rename[D <: Dimension](dim: D,
    renamer: (Dimension, P, Content) => P#S)(
      implicit ev: PosDimDep[P, D]): TypedPipe[Cell[P#S]] = {
    data.map { case (p, c) => (renamer(dim, p, c), c) }
  }

  /**
   * Rename the [[position.coordinate.Coordinate]]s of a dimension using
   * user a suplied value.
   *
   * @param dim     The [[position.Dimension]] to rename.
   * @param renamer Function that renames [[position.coordinate.Coordinate]]s.
   * @param value   A `ValuePipe` holding a user supplied value.
   *
   * @return A Scalding `TypedPipe[(`[[position.Position.S]]`,
   *         `[[content.Content]]`)]` where the dimension `dim` has been
   *         renamed.
   */
  def renameWithValue[D <: Dimension, V](dim: D,
    renamer: (Dimension, P, Content, V) => P#S, value: ValuePipe[V])(
      implicit ev: PosDimDep[P, D]): TypedPipe[Cell[P#S]] = {
    data.mapWithValue(value) {
      case ((p, c), vo) => (renamer(dim, p, c, vo.get), c)
    }
  }

  private def pairwise[D <: Dimension](slice: Slice[P, D],
    inverse: Slice[P, D])(implicit ev: PosDimDep[P, D]) = {
    val inverse = slice.inverse
    val wanted = names(slice).map { case (p, i) => p }

    val pairs = wanted
      .cross(wanted)
      .cross(names(inverse))
      .map { case ((l, r), (o, i)) => (l, r, o) }

    val values = data
      .groupBy { case (p, c) => (slice.selected(p), inverse.selected(p)) }

    pairs
      .groupBy { case (l, r, o) => (l, o) }
      .join(values)
      .map { case (_, ((l, r, o), (p, c))) => ((l, r, o), (p, c)) }
      .groupBy { case ((l, r, o), (p, c)) => (r, o) }
      .join(values)
  }

  /**
   * Compute pairwise values between all pairs of values given a [[Slice]].
   *
   * @param slice    Encapsulates the dimension(s) along which to compute
   *                 values.
   * @param operator The [[operate.Operator]] to apply.
   *
   * @return A Scalding `TypedPipe[(`[[position.Position.S]]`,
   *         `[[content.Content]]`)]` where the [[content.Content]] contains
   *         the pairwise value.
   */
  def pairwise[D <: Dimension](slice: Slice[P, D], operator: Operator with Compute)(implicit ev: PosDimDep[P, D]): TypedPipe[Cell[P#S]] = {
    pairwise(slice, slice.inverse)
      .flatMap { case (_, ((_, x), y)) => operator.compute(slice, x, y) }
  }

  /**
   * Compute pairwise values between all pairs of values given a [[Slice]]
   * with a user supplied value.
   *
   * @param slice    Encapsulates the dimension(s) along which to compute
   *                 values.
   * @param operator The [[operate.Operator]] to apply.
   * @param value    The user supplied value.
   *
   * @return A Scalding `TypedPipe[(`[[position.Position.S]]`,
   *         `[[content.Content]]`)]` where the [[content.Content]] contains
   *         the pairwise value.
   */
  def pairwiseWithValue[D <: Dimension, W](slice: Slice[P, D],
    operator: Operator with ComputeWithValue { type V >: W },
    value: ValuePipe[W])(implicit ev: PosDimDep[P, D]): TypedPipe[Cell[P#S]] = {
    pairwise(slice, slice.inverse)
      .flatMapWithValue(value) {
        case ((_, ((_, x), y)), vo) => operator.compute(slice, x, y, vo.get)
      }
  }
}

/** Define operations that reduce a [[Matrix]]'s dimensions. */
trait ReduceableMatrix[P <: Position with ReduceablePosition] {
  self: Matrix[P] =>

  /**
   * Squash a dimension of a [[Matrix]].
   *
   * @param dim      The [[position.Dimension]] to squash.
   * @param squasher The [[squash.Squasher]] that reduces two cells.
   *
   * @return A Scalding `TypedPipe[('[[position.ReduceablePosition.L]]`,
   *         `[[content.Content]]`)]`.
   *
   * @see [[position.ReduceablePosition]]
   */
  def squash[D <: Dimension](dim: D, squasher: Squasher with Reduce)(
    implicit ev: PosDimDep[P, D]): TypedPipe[Cell[P#L]] = {
    data
      .groupBy { case (p, c) => p.remove(dim) }
      .reduce[Cell[P]] {
        case ((xp, xc), (yp, yc)) => squasher.reduce(dim, (xp, xc), (yp, yc))
      }
      .map { case (p, (_, c)) => (p, c) }
  }

  /**
   * Squash a dimension of a [[Matrix]] with a user supplied value.
   *
   * @param dim      The [[position.Dimension]] to squash.
   * @param squasher The [[squash.Squasher]] that reduces two cells.
   * @param value    The user supplied value.
   *
   * @return A Scalding `TypedPipe[('[[position.ReduceablePosition.L]]`,
   *         `[[content.Content]]`)]`.
   *
   * @see [[position.ReduceablePosition]]
   */
  def squashWithValue[D <: Dimension, V](dim: D,
    squasher: Squasher with ReduceWithValue, value: ValuePipe[V])(
      implicit ev: PosDimDep[P, D]): TypedPipe[Cell[P#L]] = {
    data
      .leftCross(value)
      .groupBy { case ((p, c), vo) => p.remove(dim) }
      .reduce[(Cell[P], Option[V])] {
        case (((xp, xc), xvo), ((yp, yc), yvo)) => (squasher.reduce(dim,
          (xp, xc), (yp, yc), xvo.get.asInstanceOf[squasher.V]), xvo)
      }
      .map { case (p, ((_, c), _)) => (p, c) }
  }

  /**
   * Melt one dimension of a [[Matrix]] into another.
   *
   * @param dim       The [[position.Dimension]] to melt
   * @param into      The [[position.Dimension]] to melt into
   * @param separator The separator to use in the melt dimension
   *
   * @return A Scalding `TypedPipe[('[[position.ReduceablePosition.L]]`,
   *         `[[content.Content]]`)]`.
   *
   * @note A melt [[position.coordinate.Coordinate]] is always a
   *       [[position.coordinate.StringCoordinate]] constructed from
   *       the string representation of the `dim` and `into`
   *       [[position.coordinate.Coordinate]]s.
   *
   * @see [[position.ReduceablePosition]]
   */
  def melt[D <: Dimension, E <: Dimension](dim: D, into: E,
    separator: String = ".")(implicit ev1: PosDimDep[P, D],
      ev2: PosDimDep[P, E], ne: D =!= E): TypedPipe[Cell[P#L]] = {
    data.map { case (p, c) => (p.melt(dim, into, separator), c) }
  }

  /**
   * Reduce a [[Matrix]].
   *
   * @param slice   Encapsulates the dimension(s) along which to reduce.
   * @param reducer The reducer to apply to the data.
   *
   * @return A Scalding `TypedPipe[(`[[Slice.S]]`, `[[content.Content]]`)]`.
   *
   * @see [[reduce.Reducer]], [[Slice]]
   */
  def reduce[D <: Dimension](slice: Slice[P, D],
    reducer: Reducer with Prepare with PresentSingle)(
      implicit ev: PosDimDep[P, D]): TypedPipe[Cell[slice.S]] = {
    data
      .map { case (p, c) => (slice.selected(p), reducer.prepare(slice, p, c)) }
      .groupBy { case (p, t) => p }
      .reduce[(slice.S, reducer.T)] {
        case ((lp, lt), (rp, rt)) => (lp, reducer.reduce(lt, rt))
      }
      .values
      .flatMap { case (p, t) => reducer.presentSingle(p, t) }
  }

  /**
   * Reduce a [[Matrix]], using a user supplied value.
   *
   * @param slice   Encapsulates the dimension(s) along which to reduce.
   * @param reducer The reducer to apply to the data.
   * @param value   A `ValuePipe` holding a user supplied value.
   *
   * @return A Scalding `TypedPipe[(`[[Slice.S]]`, `[[content.Content]]`)]`.
   *
   * @see [[reduce.Reducer]], [[Slice]]
   */
  def reduceWithValue[D <: Dimension, W](slice: Slice[P, D],
    reducer: Reducer with PrepareWithValue with PresentSingle { type V >: W },
    value: ValuePipe[W])(
      implicit ev: PosDimDep[P, D]): TypedPipe[Cell[slice.S]] = {
    data
      .leftCross(value)
      .map {
        case ((p, c), vo) => (slice.selected(p),
          reducer.prepare(slice, p, c, vo.get))
      }
      .groupBy { case (p, t) => p }
      .reduce[(slice.S, reducer.T)] {
        case ((lp, lt), (rp, rt)) => (lp, reducer.reduce(lt, rt))
      }
      .values
      .flatMap { case (p, t) => reducer.presentSingle(p, t) }
  }
  // TODO: Add support for aggregating over multiple dimensions? This can be
  //       done analogous to 'shape()'. That is, given a list of (Slice,
  //       Reducer) first flatMap over the data creating a prepared version
  //       for each slice. The rest should be as is.
}

/** Define operations that expand a [[Matrix]]'s dimensions. */
trait ExpandableMatrix[P <: Position with ExpandablePosition] {
  self: Matrix[P] =>

  /**
   * Transform the [[content.Content]] of a [[Matrix]] and return the
   * transformations with an expanded [[position.Position]].
   *
   * @param transformers The transformer(s) to apply to the [[content.Content]].
   *
   * @return A Scalding `TypedPipe[(`[[position.ExpandablePosition.M]]`,
   *         `[[content.Content]]`)]`
   *
   * @see [[transform.TransformableExpanded]], [[transform.Transformer]]
   */
  def transformAndExpand[T](transformers: T)(
    implicit ev: TransformableExpanded[T]): TypedPipe[Cell[P#M]] = {
    val t = ev.convert(transformers)

    data.flatMap { case (p, c) => Misc.mapFlatten(t.present(p, c)) }
  }

  /**
   * Transform the [[content.Content]] of a [[Matrix]] using a user supplied
   * value, and return the transformations with an expanded
   * [[position.Position]].
   *
   * @param transformers The transformer(s) to apply to the [[content.Content]].
   * @param value        A `ValuePipe` holding a user supplied value.
   *
   * @return A Scalding `TypedPipe[(`[[position.ExpandablePosition.M]]`,
   *         `[[content.Content]]`)]`
   *
   * @see [[transform.TransformableExpandedWithValue]],
   *      [[transform.Transformer]]
   */
  def transformAndExpandWithValue[T, V](transformers: T, value: ValuePipe[V])(
    implicit ev: TransformableExpandedWithValue[T, V]): TypedPipe[Cell[P#M]] = {
    val t = ev.convert(transformers)

    data.flatMapWithValue(value) {
      case ((p, c), vo) => Misc.mapFlatten(t.present(p, c,
        vo.get.asInstanceOf[t.V]))
    }
  }

  /**
   * Expand a [[Matrix]] with an extra dimension.
   *
   * @param expander A function that expands each position with 1 dimension.
   *
   * @return A Scalding `TypedPipe[(`[[position.ExpandablePosition.M]]`,
   *         `[[content.Content]]`)]`
   */
  def expand(expander: (P, Content) => P#M): TypedPipe[Cell[P#M]] = {
    data.map { case (p, c) => (expander(p, c), c) }
  }

  /**
   * Expand a [[Matrix]] with an extra dimension using a user supplied value.
   *
   * @param expander A function that expands each position with 1 dimension.
   * @param value    A `ValuePipe` holding a user supplied value.
   *
   * @return A Scalding `TypedPipe[(`[[position.ExpandablePosition.M]]`,
   *         `[[content.Content]]`)]`
   */
  def expandWithValue[V](expander: (P, Content, V) => P#M,
    value: ValuePipe[V]): TypedPipe[Cell[P#M]] = {
    data.mapWithValue(value) {
      case ((p, c), vo) => (expander(p, c, vo.get), c)
    }
  }
}

object Matrix {
  type Cell[P <: Position] = (P, Content)

  /**
   * Read column oriented, pipe separated matrix data into a
   * `TypedPipe[(`[[position.Position2D]]`, `[[content.Content]]`)]`.
   *
   * @param file The file to read from.
   *
   * @note The returned [[position.Position2D]] consists of 2
   *       [[position.coordinate.StringCoordinate]]s.
   *
   * @see [[Matrix2D]]
   */
  def read2D(file: String, first: Codex with CoordinateCodex = StringCodex,
    second: Codex with CoordinateCodex = StringCodex)(implicit flow: FlowDef,
      mode: Mode): TypedPipe[Cell[Position2D]] = {
    (TypedPsv[(String, String, String, String, String)](file))
      .flatMap {
        case (r, c, t, e, v) =>
          Schema.fromString(e, t).flatMap {
            case s => (s.decode(v), first.read(r), second.read(c)) match {
              case (Some(con), Some(c1), Some(c2)) =>
                Some((Position2D(c1, c2), con))
              case _ => None
            }
          }
      }
  }

  /**
   * Read column oriented, pipe separated data into a
   * `TypedPipe[(`[[position.Position2D]]`, `[[content.Content]]`)]`.
   *
   * @param file   The file to read from.
   * @param dict   The dictionary describing the features in the data.
   * @param dim    The dimension on which to apply the dictionary.
   * @param first  The [[content.encoding.Codex]] for decoding the first
   *               dimension.
   * @param second The [[content.encoding.Codex]] for decoding the second
   *               dimension.
   *
   * @see [[Matrix2D]]
   */
  def read2DWithDictionary[D <: Dimension](file: String, dict: Dictionary,
    dim: D = Second, first: Codex with CoordinateCodex = StringCodex,
    second: Codex with CoordinateCodex = StringCodex)(
      implicit ev: PosDimDep[Position2D, D], flow: FlowDef,
      mode: Mode): TypedPipe[Cell[Position2D]] = {
    (TypedPsv[(String, String, String)](file))
      .flatMap {
        case (e, a, v) =>
          val s = dim match {
            case First => dict(e)
            case Second => dict(a)
          }

          (s.decode(v), first.read(e), second.read(a)) match {
            case (Some(con), Some(c1), Some(c2)) =>
              Some((Position2D(c1, c2), con))
            case _ => None
          }
      }
  }

  /**
   * Read column oriented, pipe separated matrix data into a
   * `TypedPipe[(`[[position.Position3D]]`, `[[content.Content]]`)]`.
   *
   * @param file The file to read from.
   *
   * @note The returned [[position.Position3D]] consists of 3
   *       [[position.coordinate.StringCoordinate]]s.
   *
   * @see [[Matrix3D]]
   */
  def read3D(file: String, first: Codex with CoordinateCodex = StringCodex,
    second: Codex with CoordinateCodex = StringCodex,
    third: Codex with CoordinateCodex = StringCodex)(implicit flow: FlowDef,
      mode: Mode): TypedPipe[Cell[Position3D]] = {
    (TypedPsv[(String, String, String, String, String, String)](file))
      .flatMap {
        case (r, c, d, t, e, v) =>
          Schema.fromString(e, t).flatMap {
            case s => (s.decode(v), first.read(r), second.read(c),
              third.read(d)) match {
                case (Some(con), Some(c1), Some(c2), Some(c3)) =>
                  Some((Position3D(c1, c2, c3), con))
                case _ => None
              }
          }
      }
  }

  /**
   * Read column oriented, pipe separated data into a
   * `TypedPipe[(`[[position.Position3D]]`, `[[content.Content]]`)]`.
   *
   * @param file   The file to read from.
   * @param dict   The dictionary describing the features in the data.
   * @param dim    The dimension on which to apply the dictionary.
   * @param first  The [[content.encoding.Codex]] for decoding the first
   *               dimension.
   * @param second The [[content.encoding.Codex]] for decoding the second
   *               dimension.
   * @param third  The [[content.encoding.Codex]] for decoding the third
   *               dimension.
   *
   * @see [[Matrix3D]]
   */
  def read3DWithDictionary[D <: Dimension](file: String, dict: Dictionary,
    dim: D = Second, first: Codex with CoordinateCodex = StringCodex,
    second: Codex with CoordinateCodex = StringCodex,
    third: Codex with CoordinateCodex = DateCodex)(
      implicit ev: PosDimDep[Position3D, D], flow: FlowDef,
      mode: Mode): TypedPipe[Cell[Position3D]] = {
    (TypedPsv[(String, String, String, String)](file))
      .flatMap {
        case (e, a, t, v) =>
          val s = dim match {
            case First => dict(e)
            case Second => dict(a)
            case Third => dict(t)
          }

          (s.decode(v), first.read(e), second.read(a), third.read(t)) match {
            case (Some(con), Some(c1), Some(c2), Some(c3)) =>
              Some((Position3D(c1, c2, c3), con))
            case _ => None
          }
      }
  }

  /**
   * Read tabled data into a `TypedPipe[(`[[position.Position2D]]`,
   * `[[content.Content]]`)]`.
   *
   * @param table     The file (table) to read from.
   * @param columns   `List` of `(String, `[[content.metadata.Schema]]`)`
   *                  tuples describing each column in the table.
   * @param pkeyIndex Index (into columns) describing which column is the
   *                  primary key.
   * @param separator The column separator.
   *
   * @note The returned [[position.Position2D]] consists of 2
   *       [[position.coordinate.StringCoordinate]]s. The first
   *       [[position.coordinate.StringCoordinate]] is the contents of the
   *       primary key column. The second
   *       [[position.coordinate.StringCoordinate]] is the name of the column.
   *
   * @see [[Matrix2D]]
   */
  def readTable(table: String, columns: List[(String, Schema)],
    pkeyIndex: Int = 0, separator: String = "\01")(implicit flow: FlowDef,
      mode: Mode): TypedPipe[Cell[Position2D]] = {
    TypedPipe.from(TextLine(table))
      .flatMap {
        case line =>
          val parts = line.trim.split(separator, columns.length).toList
          val pkey = parts(pkeyIndex)

          columns.zipWithIndex.flatMap {
            case ((name, schema), idx) if (idx != pkeyIndex) =>
              schema.decode(parts(idx).trim).map {
                case c => (Position2D(pkey, name), c)
              }
            case _ => None
          }
      }
  }

  /**
   * Read label data (instance id|date|value) into a
   * `TypedPipe[(`[[position.Position2D]]`, `[[content.Content]]`)]`.
   *
   * @param file The label file to read from.
   *
   * @note The returned [[position.Position2D]] consists of; instance id
   *       ([[position.coordinate.StringCoordinate]]) and data
   *       ([[position.coordinate.DateCoordinate]]). The
   *       [[content.variable.Type]] is [[content.variable.Type.Continuous]].
   *
   * @see [[Matrix2D]]
   */
  def readLabels(file: String, schema: Schema,
    first: Codex with CoordinateCodex = StringCodex,
    second: Codex with CoordinateCodex = DateCodex)(implicit flow: FlowDef,
      mode: Mode): TypedPipe[Cell[Position2D]] = {
    (TypedPsv[(String, String, String)](file))
      .flatMap {
        case (i, d, l) =>
          (schema.decode(l), first.read(i), second.read(d)) match {
            case (Some(con), Some(c1), Some(c2)) =>
              Some((Position2D(c1, c2), con))
            case _ => None
          }
      }
  }

  /**
   * Conversion from `TypedPipe[(`[[position.Position1D]]`,
   * `[[content.Content]]`)]` to a [[Matrix1D]].
   */
  implicit def typedPipePosition1DContent(
    data: TypedPipe[Cell[Position1D]]): Matrix1D = new Matrix1D(data)
  /**
   * Conversion from `TypedPipe[(`[[position.Position2D]]`,
   * `[[content.Content]]`)]` to a [[Matrix2D]].
   */
  implicit def typedPipePosition2DContent(
    data: TypedPipe[Cell[Position2D]]): Matrix2D = new Matrix2D(data)
  /**
   * Conversion from `TypedPipe[(`[[position.Position3D]]`,
   * `[[content.Content]]`)]` to a [[Matrix3D]].
   */
  implicit def typedPipePosition3DContent(
    data: TypedPipe[Cell[Position3D]]): Matrix3D = new Matrix3D(data)
  /**
   * Conversion from `TypedPipe[(`[[position.Position4D]]`,
   * `[[content.Content]]`)]` to a [[Matrix4D]].
   */
  implicit def typedPipePosition4DContent(
    data: TypedPipe[Cell[Position4D]]): Matrix4D = new Matrix4D(data)
  /**
   * Conversion from `TypedPipe[(`[[position.Position5D]]`,
   * `[[content.Content]]`)]` to a [[Matrix5D]].
   */
  implicit def typedPipePosition5DContent(
    data: TypedPipe[Cell[Position5D]]): Matrix5D = new Matrix5D(data)
}

/**
 * Rich wrapper around a `TypedPipe[(`[[position.Position1D]]`,
 * `[[content.Content]]`)]`.
 *
 * @param data `TypedPipe[(`[[position.Position1D]]`, `[[content.Content]]`)]`.
 */
class Matrix1D(val data: TypedPipe[Cell[Position1D]]) extends Matrix[Position1D]
  with ModifyableMatrix[Position1D] with ExpandableMatrix[Position1D] {
  def domain(): TypedPipe[Position1D] = {
    names(Over(First)).map { case (p, i) => p }
  }
}

/**
 * Rich wrapper around a `TypedPipe[(`[[position.Position2D]]`,
 * `[[content.Content]]`)]`.
 *
 * @param data `TypedPipe[(`[[position.Position2D]]`, `[[content.Content]]`)]`.
 */
class Matrix2D(val data: TypedPipe[Cell[Position2D]]) extends Matrix[Position2D]
  with ModifyableMatrix[Position2D] with ReduceableMatrix[Position2D]
  with ExpandableMatrix[Position2D] {
  def domain(): TypedPipe[Position2D] = {
    names(Over(First))
      .map { case (Position1D(c), i) => c }
      .cross(names(Over(Second)).map { case (Position1D(c), i) => c })
      .map { case (c1, c2) => Position2D(c1, c2) }
  }

  /**
   * Permute the order of the [[position.coordinate.Coordinate]]s in a
   * [[position.Position]].
   *
   * @param first  [[position.Dimension]] used for the first
   *               [[position.coordinate.Coordinate]].
   * @param second [[position.Dimension]] used for the second
   *               [[position.coordinate.Coordinate]].
   *
   * @see [[position.coordinate.Coordinate]], [[position.Position]],
   *      [[position.Dimension]]
   */
  def permute[D <: Dimension, E <: Dimension](first: D, second: E)(
    implicit ev1: PosDimDep[Position2D, D], ev2: PosDimDep[Position2D, E],
    ne: D =!= E): TypedPipe[Cell[Position2D]] = {
    data.map { case (p, c) => (p.permute(List(first, second)), c) }
  }

  def correlation[D <: Dimension](slice: Slice[Position2D, D])(
    implicit ev: PosDimDep[Position2D, D]): TypedPipe[Cell[Position1D]] = {
    implicit def typedPipeSliceSMContent(
      data: TypedPipe[Cell[slice.S#M]]): Matrix2D = {
      new Matrix2D(data.asInstanceOf[TypedPipe[Cell[Position2D]]])
    }

    implicit def typedPipeSliceSContent(
      data: TypedPipe[Cell[slice.S]]): Matrix1D = {
      new Matrix1D(data.asInstanceOf[TypedPipe[Cell[Position1D]]])
    }

    val mean = data
      .reduceAndExpand(slice, Moments(only = List(1)))
      .toMap(Over(First))

    val centered = data
      .transformWithValue(Subtract(slice.dimension, "mean"), mean)

    val squared = centered
      .transform(Square(slice.dimension))
      .reduceAndExpand(slice, Sum())
      .pairwise(slice.inverse, Multiply())
      .transform(SquareRoot(slice.dimension))
      .toMap(Over(First))

    centered
      .pairwise(slice, Multiply())
      .reduce(slice, Sum())
      .transformWithValue(Divide(First, "sum"), squared)
  }

  /**
   * Persist a [[Matrix2D]] as a CSV file.
   *
   * @param slice         Encapsulates the dimension that makes up the columns.
   * @param file          File to write to.
   * @param separator     Column separator to use.
   * @param writeHeader   Indicator of the header should be written to a
   *                      separate file.
   * @param headerPostfix Postfix for the header file name.
   * @param writeRowId    Indicator if row names should be written.
   * @param rowId         Column name of row names.
   *
   * @return A `TypedPipe[(`[[position.Position2D]]`,
   *         `[[content.Content]]`)]`; that is it returns `data`.
   */
  def writeCSV[D <: Dimension](slice: Slice[Position2D, D], file: String,
    separator: String = "|", writeHeader: Boolean = true,
    headerPostfix: String = ".header", writeRowId: Boolean = true,
    rowId: String = "id")(implicit ev: PosDimDep[Position2D, D],
      flow: FlowDef, mode: Mode): TypedPipe[Cell[Position2D]] = {
    writeCSVWithNames(slice, file, names(slice), separator, writeHeader,
      headerPostfix, writeRowId, rowId)
  }

  /**
   * Persist a [[Matrix2D]] as a CSV file.
   *
   * @param slice         Encapsulates the dimension that makes up the columns.
   * @param file          File to write to.
   * @param names         The names to use for the columns (according to their
   *                      ordering).
   * @param separator     Column separator to use.
   * @param writeHeader   Indicator of the header should be written to a
   *                      separate file.
   * @param headerPostfix Postfix for the header file name.
   * @param writeRowId    Indicator if row names should be written.
   * @param rowId         Column name of row names.
   *
   * @return A `TypedPipe[(`[[position.Position2D]]`,
   *         `[[content.Content]]`)]`; that is it returns `data`.
   *
   * @note If `names` contains a subset of the columns, then only those columns
   *       get persisted to file.
   */
  def writeCSVWithNames[T, D <: Dimension](slice: Slice[Position2D, D],
    file: String, names: T, separator: String = "|",
    writeHeader: Boolean = true, headerPostfix: String = ".header",
    writeRowId: Boolean = true,
    rowId: String = "id")(implicit ev1: Nameable[T, Position2D, slice.S, D],
      ev2: PosDimDep[Position2D, D], flow: FlowDef,
      mode: Mode): TypedPipe[Cell[Position2D]] = {
    // Note: Usage of .toShortString should be safe as data is written
    //       as string anyways. It does assume that all indices have
    //       unique short string representations.
    val columns = ev1.convert(this, slice, names)
      .map { List(_) }
      .sum
      .map { _.sortBy(_._2).map(_._1.toShortString("")) }

    if (writeHeader) {
      columns
        .map {
          case lst =>
            if (writeRowId) rowId + separator + lst.mkString(separator)
            else lst.mkString(separator)
        }
        .toPipe('line)
        .write(new TextLine(file + headerPostfix))
    }

    // TODO: Escape separator in values
    data
      .groupBy { case (p, c) => slice.remainder(p).toShortString("") }
      .mapValues {
        case (p, c) => Map(slice.selected(p).toShortString("") ->
          c.value.toShortString)
      }
      .sum
      .flatMapWithValue(columns) {
        case ((key, values), optCols) => optCols.map {
          case cols => (key, cols.map { case c => values.getOrElse(c, "") })
        }
      }
      .map {
        case (i, lst) =>
          if (writeRowId) i + separator + lst.mkString(separator)
          else lst.mkString(separator)
      }
      .toPipe('line)
      .write(new TextLine(file))

    data
  }

  def writeVW[D <: Dimension](slice: Slice[Position2D, D],
    labels: TypedPipe[Cell[Position1D]], file: String,
    postfix: String = ".dict", separator: String = ":")(
      implicit ev: PosDimDep[Position2D, D], flow: FlowDef,
      mode: Mode): TypedPipe[Cell[Position2D]] = {
    writeVWWithNames(slice, labels, file, names(Along(slice.dimension)),
      postfix, separator)
  }

  def writeVWWithNames[D <: Dimension](slice: Slice[Position2D, D],
    labels: TypedPipe[Cell[Position1D]], file: String,
    names: TypedPipe[(Position1D, Long)], postfix: String = ".dict",
    separator: String = ":")(implicit ev: PosDimDep[Position2D, D],
      flow: FlowDef, mode: Mode): TypedPipe[Cell[Position2D]] = {

    val dict = names
      .groupBy { case (p, i) => p }

    dict
      .map { case (_, (p, i)) => p.toShortString(separator) + separator + i }
      .toPipe('line)
      .write(new TextLine(file + postfix))

    data
      .groupBy { case (p, c) => slice.remainder(p).asInstanceOf[Position1D] }
      .join(dict)
      .map {
        case (_, ((p, c), (_, i))) =>
          (p, " " + i + ":" + c.value.toShortString)
      }
      .groupBy { case (p, ics) => slice.selected(p).asInstanceOf[Position1D] }
      .reduce[(Position2D, String)] { case ((p, ls), (_, rs)) => (p, ls + rs) }
      .join(labels.groupBy { case (p, c) => p })
      .map {
        case (p, ((_, ics), (_, l))) =>
          l.value.toShortString + " " + p.toShortString(separator) + "|" + ics
      }
      .toPipe('line)
      .write(new TextLine(file))

    data
  }

  /**
   * Persist a [[Matrix2D]] as a LDA file.
   *
   * @param slice     Encapsulates the dimension that makes up the columns.
   * @param file      File to write to.
   * @param postfix   Postfix for the dictionary file name.
   * @param separator Column separator to use in dictionary file.
   * @param addId     Indicator if each line should start with the row id
   *                  followed by `separator`.
   *
   * @return A `TypedPipe[(`[[position.Position2D]]`,
   *         `[[content.Content]]`)]`; that is it returns `data`.
   */
  def writeLDA[D <: Dimension](slice: Slice[Position2D, D], file: String,
    postfix: String = ".dict", separator: String = "|",
    addId: Boolean = false)(implicit ev: PosDimDep[Position2D, D],
      flow: FlowDef, mode: Mode): TypedPipe[Cell[Position2D]] = {
    writeLDAWithNames(slice, file, names(Along(slice.dimension)), postfix,
      separator, addId)
  }

  /**
   * Persist a [[Matrix2D]] as a LDA file.
   *
   * @param slice     Encapsulates the dimension that makes up the columns.
   * @param file      File to write to.
   * @param names     The names to use for the columns (according to their
   *                  ordering).
   * @param postfix   Postfix for the dictionary file name.
   * @param separator Column separator to use in dictionary file.
   * @param addId     Indicator if each line should start with the row id
   *                  followed by `separator`.
   *
   * @return A `TypedPipe[(`[[position.Position2D]]`,
   *         `[[content.Content]]`)]`; that is it returns `data`.
   *
   * @note If `names` contains a subset of the columns, then only those columns
   *       get persisted to file.
   */
  def writeLDAWithNames[D <: Dimension](slice: Slice[Position2D, D],
    file: String, names: TypedPipe[(Position1D, Long)],
    postfix: String = ".dict", separator: String = "|",
    addId: Boolean = false)(implicit ev: PosDimDep[Position2D, D],
      flow: FlowDef, mode: Mode): TypedPipe[Cell[Position2D]] = {
    val dict = names
      .groupBy { case (p, i) => p }

    dict
      .map { case (_, (p, i)) => p.toShortString(separator) + separator + i }
      .toPipe('line)
      .write(new TextLine(file + postfix))

    data
      .groupBy { case (p, c) => slice.remainder(p).asInstanceOf[Position1D] }
      .join(dict)
      .map {
        case (_, ((p, c), (_, i))) =>
          (p, " " + i + ":" + c.value.toShortString, 1L)
      }
      .groupBy { case (p, ics, m) => slice.selected(p) }
      .reduce[(Position2D, String, Long)] {
        case ((p, ls, lm), (_, rs, rm)) => (p, ls + rs, lm + rm)
      }
      .map {
        case (p, (_, ics, m)) =>
          if (addId) p.toShortString(separator) + separator + m + ics
          else m + ics
      }
      .toPipe('line)
      .write(new TextLine(file))

    data
  }

  /**
   * Persist a [[Matrix2D]] as sparse matrix file (index, index, value).
   *
   * @param file      File to write to.
   * @param postfixI  Postfix for the row dictionary file name.
   * @param postfixJ  Postfix for the column dictionary file name.
   * @param separator Column separator to use in dictionary file.
   *
   * @return A `TypedPipe[(`[[position.Position2D]]`,
   *         `[[content.Content]]`)]`; that is it returns `data`.
   *
   * @note R's slam package has a simple triplet matrx format (which in turn is
   *       used by the tm package). This format should be compatible.
   */
  def writeIJV(file: String, postfixI: String = ".dict.i",
    postfixJ: String = ".dict.j", separator: String = "|")(
      implicit flow: FlowDef, mode: Mode): TypedPipe[Cell[Position2D]] = {
    writeIJVWithNames(file, names(Over(First)), names(Over(Second)),
      postfixI, postfixJ, separator)
  }

  /**
   * Persist a [[Matrix2D]] as sparse matrix file (index, index, value).
   *
   * @param file      File to write to.
   * @param namesI    The names to use for the rows (according to their
   *                  ordering).
   * @param namesJ    The names to use for the columns (according to their
   *                  ordering).
   * @param postfixI  Postfix for the row dictionary file name.
   * @param postfixJ  Postfix for the column dictionary file name.
   * @param separator Column separator to use in dictionary file.
   *
   * @return A `TypedPipe[(`[[position.Position2D]]`,
   *         `[[content.Content]]`)]`; that is it returns `data`.
   *
   * @note If `names` contains a subset of the columns, then only those columns
   *       get persisted to file.
   * @note R's slam package has a simple triplet matrx format (which in turn is
   *       used by the tm package). This format should be compatible.
   */
  def writeIJVWithNames(file: String, namesI: TypedPipe[(Position1D, Long)],
    namesJ: TypedPipe[(Position1D, Long)], postfixI: String = ".dict.i",
    postfixJ: String = ".dict.j", separator: String = "|")(
      implicit flow: FlowDef, mode: Mode): TypedPipe[Cell[Position2D]] = {
    val dictI = namesI.groupBy { case (p, i) => p }
    val dictJ = namesJ.groupBy { case (p, j) => p }

    dictI
      .map { case (_, (p, i)) => p.toShortString(separator) + separator + i }
      .toPipe('line)
      .write(new TextLine(file + postfixI))

    dictJ
      .map { case (_, (p, j)) => p.toShortString(separator) + separator + j }
      .toPipe('line)
      .write(new TextLine(file + postfixJ))

    data
      .groupBy { case (p, c) => p.remove(Second) }
      .join(dictI)
      .values
      .groupBy { case ((p, c), pi) => p.remove(First) }
      .join(dictJ)
      .map {
        case (_, (((_, c), (_, i)), (_, j))) =>
          i + separator + j + separator + c.value.toShortString
      }
      .toPipe('line)
      .write(new TextLine(file))

    data
  }
}

/**
 * Rich wrapper around a `TypedPipe[(`[[position.Position3D]]`,
 * `[[content.Content]]`)]`.
 *
 * @param data `TypedPipe[(`[[position.Position3D]]`, `[[content.Content]]`)]`.
 */
class Matrix3D(val data: TypedPipe[Cell[Position3D]]) extends Matrix[Position3D]
  with ModifyableMatrix[Position3D] with ReduceableMatrix[Position3D]
  with ExpandableMatrix[Position3D] {
  def domain(): TypedPipe[Position3D] = {
    names(Over(First))
      .map { case (Position1D(c), i) => c }
      .cross(names(Over(Second)).map { case (Position1D(c), i) => c })
      .cross(names(Over(Third)).map { case (Position1D(c), i) => c })
      .map { case ((c1, c2), c3) => Position3D(c1, c2, c3) }
  }

  /**
   * Permute the order of the [[position.coordinate.Coordinate]]s in a
   * [[position.Position]].
   *
   * @param first  [[position.Dimension]] used for the first
   *               [[position.coordinate.Coordinate]].
   * @param second [[position.Dimension]] used for the second
   *               [[position.coordinate.Coordinate]].
   * @param third  [[position.Dimension]] used for the third
   *               [[position.coordinate.Coordinate]].
   *
   * @see [[position.coordinate.Coordinate]], [[position.Position]],
   *      [[position.Dimension]]
   */
  def permute[D <: Dimension, E <: Dimension, F <: Dimension](first: D,
    second: E, third: F)(implicit ev1: PosDimDep[Position3D, D],
      ev2: PosDimDep[Position3D, E], ev3: PosDimDep[Position3D, F],
      ne1: D =!= E, ne2: D =!= F, ne3: E =!= F): TypedPipe[Cell[Position3D]] = {
    data.map { case (p, c) => (p.permute(List(first, second, third)), c) }
  }
}

/**
 * Rich wrapper around a `TypedPipe[(`[[position.Position4D]]`,
 * `[[content.Content]]`)]`.
 *
 * @param data `TypedPipe[(`[[position.Position4D]]`, `[[content.Content]]`)]`.
 */
class Matrix4D(val data: TypedPipe[Cell[Position4D]]) extends Matrix[Position4D]
  with ModifyableMatrix[Position4D] with ReduceableMatrix[Position4D]
  with ExpandableMatrix[Position4D] {
  def domain(): TypedPipe[Position4D] = {
    names(Over(First))
      .map { case (Position1D(c), i) => c }
      .cross(names(Over(Second)).map { case (Position1D(c), i) => c })
      .cross(names(Over(Third)).map { case (Position1D(c), i) => c })
      .cross(names(Over(Fourth)).map { case (Position1D(c), i) => c })
      .map { case (((c1, c2), c3), c4) => Position4D(c1, c2, c3, c4) }
  }

  /**
   * Permute the order of the [[position.coordinate.Coordinate]]s in a
   * [[position.Position]].
   *
   * @param first  [[position.Dimension]] used for the first
   *               [[position.coordinate.Coordinate]].
   * @param second [[position.Dimension]] used for the second
   *               [[position.coordinate.Coordinate]].
   * @param third  [[position.Dimension]] used for the third
   *               [[position.coordinate.Coordinate]].
   * @param fourth [[position.Dimension]] used for the fourth
   *               [[position.coordinate.Coordinate]].
   *
   * @see [[position.coordinate.Coordinate]], [[position.Position]],
   *      [[position.Dimension]]
   */
  def permute[D <: Dimension, E <: Dimension, F <: Dimension, G <: Dimension](
    first: D, second: E, third: F, fourth: G)(
      implicit ev1: PosDimDep[Position4D, D], ev2: PosDimDep[Position4D, E],
      ev3: PosDimDep[Position4D, F], ev4: PosDimDep[Position4D, G],
      ne1: D =!= E, ne2: D =!= F, ne3: D =!= G, ne4: E =!= F, ne5: E =!= G,
      ne6: F =!= G): TypedPipe[Cell[Position4D]] = {
    data.map {
      case (p, c) => (p.permute(List(first, second, third, fourth)), c)
    }
  }
}

/**
 * Rich wrapper around a `TypedPipe[(`[[position.Position5D]]`,
 * `[[content.Content]]`)]`.
 *
 * @param data `TypedPipe[(`[[position.Position5D]]`, `[[content.Content]]`)]`.
 */
class Matrix5D(val data: TypedPipe[Cell[Position5D]]) extends Matrix[Position5D]
  with ModifyableMatrix[Position5D] with ReduceableMatrix[Position5D] {
  def domain(): TypedPipe[Position5D] = {
    names(Over(First))
      .map { case (Position1D(c), i) => c }
      .cross(names(Over(Second)).map { case (Position1D(c), i) => c })
      .cross(names(Over(Third)).map { case (Position1D(c), i) => c })
      .cross(names(Over(Fourth)).map { case (Position1D(c), i) => c })
      .cross(names(Over(Fifth)).map { case (Position1D(c), i) => c })
      .map { case ((((c1, c2), c3), c4), c5) => Position5D(c1, c2, c3, c4, c5) }
  }

  /**
   * Permute the order of the [[position.coordinate.Coordinate]]s in a
   * [[position.Position]].
   *
   * @param first  [[position.Dimension]] used for the first
   *               [[position.coordinate.Coordinate]].
   * @param second [[position.Dimension]] used for the second
   *               [[position.coordinate.Coordinate]].
   * @param third  [[position.Dimension]] used for the third
   *               [[position.coordinate.Coordinate]].
   * @param fourth [[position.Dimension]] used for the fourth
   *               [[position.coordinate.Coordinate]].
   * @param fifth  [[position.Dimension]] used for the fifth
   *               [[position.coordinate.Coordinate]].
   *
   * @see [[position.coordinate.Coordinate]], [[position.Position]],
   *      [[position.Dimension]]
   */
  def permute[D <: Dimension, E <: Dimension, F <: Dimension, G <: Dimension, H <: Dimension](
    first: D, second: E, third: F, fourth: G, fifth: H)(
      implicit ev1: PosDimDep[Position5D, D], ev2: PosDimDep[Position5D, E],
      ev3: PosDimDep[Position5D, F], ev4: PosDimDep[Position5D, G],
      ev5: PosDimDep[Position5D, H], ne1: D =!= E, ne2: D =!= F, ne3: D =!= G,
      ne4: D =!= H, ne5: E =!= F, ne6: E =!= G, ne7: E =!= H, ne8: F =!= G,
      ne9: F =!= H, ne10: G =!= H): TypedPipe[Cell[Position5D]] = {
    data.map {
      case (p, c) => (p.permute(List(first, second, third, fourth, fifth)), c)
    }
  }
}

/**
 * Type class for transforming a type `T` into a `TypedPipe[(P,
 * `[[content.Content]]`)]`.
 */
trait Matrixable[T, P <: Position] {
  /**
   * Returns a `TypedPipe[(P, `[[content.Content]]`)]` for type `T`.
   *
   * @param t Object that can be converted to a `TypedPipe[(P,
   *          `[[content.Content]]`)]`.
   */
  def convert(t: T): TypedPipe[Cell[P]]
}

/** Companion object for the [[Matrixable]] type class. */
object Matrixable {
  /**
   * Converts a `TypedPipe[(P, `[[content.Content]]`)]` into a
   * `TypedPipe[(P, `[[content.Content]]`)]`; that is, it is a
   * pass through.
   */
  implicit def MatrixMatrixable[P <: Position]: Matrixable[TypedPipe[Cell[P]], P] = {
    new Matrixable[TypedPipe[Cell[P]], P] {
      def convert(t: TypedPipe[Cell[P]]): TypedPipe[Cell[P]] = t
    }
  }
  /**
   * Converts a `(`[[position.PositionPipeable]]`, `[[content.Content]]`)]`
   * into a `TypedPipe[(P, `[[content.Content]]`)]`.
   */
  implicit def PositionPipeableContentTupleMatrixable[T, P <: Position](
    implicit ev: PositionPipeable[T, P], flow: FlowDef,
    mode: Mode): Matrixable[(T, Content), P] = {
    new Matrixable[(T, Content), P] {
      def convert(t: (T, Content)): TypedPipe[Cell[P]] = {
        ev.convert(t._1).map { case p => (p, t._2) }
      }
    }
  }
  /**
   * Converts a `List(P, `[[content.Content]]`)]` into a
   * `TypedPipe[(P, `[[content.Content]]`)]`.
   */
  implicit def ListCellMatrixable[P <: Position](implicit flow: FlowDef,
    mode: Mode): Matrixable[List[Cell[P]], P] = {
    new Matrixable[List[Cell[P]], P] {
      def convert(t: List[Cell[P]]): TypedPipe[Cell[P]] = {
        new IterablePipe(t, flow, mode)
      }
    }
  }
  /**
   * Converts a `(P, `[[content.Content]]`)` tuple into a
   * `TypedPipe[(P, `[[content.Content]]`)]`.
   */
  implicit def CellMatrixable[P <: Position](implicit flow: FlowDef,
    mode: Mode): Matrixable[Cell[P], P] = {
    new Matrixable[Cell[P], P] {
      def convert(t: Cell[P]): TypedPipe[Cell[P]] = {
        new IterablePipe(List(t), flow, mode)
      }
    }
  }
}

