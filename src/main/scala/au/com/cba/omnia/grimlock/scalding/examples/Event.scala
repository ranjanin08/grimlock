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

package au.com.cba.omnia.grimlock.scalding.examples

import au.com.cba.omnia.grimlock.framework._
import au.com.cba.omnia.grimlock.framework.content._
import au.com.cba.omnia.grimlock.framework.content.metadata._
import au.com.cba.omnia.grimlock.framework.encoding._
import au.com.cba.omnia.grimlock.framework.nlp._
import au.com.cba.omnia.grimlock.framework.position._
import au.com.cba.omnia.grimlock.framework.transform._
import au.com.cba.omnia.grimlock.framework.utility._

import au.com.cba.omnia.grimlock.library.aggregate._
import au.com.cba.omnia.grimlock.library.transform._

import au.com.cba.omnia.grimlock.scalding.Matrix._

import cascading.flow.FlowDef
import com.twitter.scalding.{ Args, Job, Mode, TextLine }
import com.twitter.scalding.TDsl.sourceToTypedPipe
import com.twitter.scalding.typed.TypedPipe

// Define a simple event (structured) data type. It has an id, a type, a start time and duration. It applies to one or
// more instances and has a detailed information field.
case class ExampleEvent(
  eventId: String,
  eventType: String,
  startTime: java.util.Date,
  duration: Long,
  instances: List[String],
  details: String) extends Event

object ExampleEvent {
  // Function to read a file with event data.
  def load(file: String)(implicit flow: FlowDef, mode: Mode): TypedPipe[Cell[Position1D]] = {
    val es = EventSchema[ExampleEventCodex]()
    TextLine(file)
      .flatMap { case s => es.decode(s).map { case e => Cell(Position1D(es.codex.fromValue(e.value).eventId), e) } }
  }

  // Define a type and implicit so schema construction looks simple.
  type ExampleEventCodex = ExampleEventCodex.type
  implicit val EC: ExampleEventCodex = ExampleEventCodex
}

// Define a codex for dealing with the example event. Note that comparison, for this example, is simply comparison
// on the event id.
case object ExampleEventCodex extends EventCodex {
  val name = "example.event"

  type T = ExampleEvent
  type V = EventValue[T]

  def toValue(value: T): V = EventValue(value, this)
  def fromValue(value: Value): T = value.asInstanceOf[V].value
  def compare(x: Value, y: Value): Option[Int] = {
    (x.asEvent, y.asEvent) match {
      case (Some(ExampleEvent(l, _, _, _, _, _)), Some(ExampleEvent(r, _, _, _, _, _))) => Some(l.compare(r))
      case _ => None
    }
  }

  protected def fromString(str: String): T = {
    val dfmt = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val parts = str.split("#")

    ExampleEvent(parts(0), parts(1), dfmt.parse(parts(2)), parts(3).toLong, parts(4).split(",").toList, parts(5))
  }
  protected def toString(value: T): String = {
    val dfmt = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    value.eventId + "#" + value.eventType + "#" + dfmt.format(value.startTime) + "#" + value.duration.toString +
      value.instances.mkString(",") + "#" + value.details
  }
}

// Transformer for denormalising events; that is, create a separate cell in the matrix for each (event, instance) pair.
// Assumes that the initial position is 1D with event id (as is the output from `read` above).
case class Denormalise() extends Transformer[Position1D, Position2D] {
  def present(cell: Cell[Position1D]): Collection[Cell[Position2D]] = {
    cell.content match {
      case Content(_, EventValue(ExampleEvent(_, _, _, _, instances, _), _)) =>
        Collection(for { iid <- instances } yield { Cell(cell.position.append(iid), cell.content) })
      case _ => Collection[Cell[Position2D]]()
    }
  }
}

// For each event, get the details out. Split the details string, apply filtering, and (optinally) add ngrams. Then
// simply return the count for each term (word or ngram) in the document (i.e. event).
case class WordCounts(minLength: Long = Long.MinValue, ngrams: Int = 1, separator: String = "_",
  stopwords: List[String] = Stopwords.English) extends Transformer[Position2D, Position3D] {
  def present(cell: Cell[Position2D]): Collection[Cell[Position3D]] = {
    cell.content match {
      case Content(_, EventValue(ExampleEvent(_, _, _, _, _, details), _)) =>
        // Get words from details. Optionally filter by length and/or stopwords.
        val words = details
          .toLowerCase
          .split("""[ ,!.?;:"'#)($+></\\=~_&-@\[\]%`{}]+""")
          .toList
          .filterNot {
            case word => word.isEmpty || word.exists(Character.isDigit) ||
              word.length < minLength || stopwords.contains(word)
          }
        // Get terms from words. Optionally add ngrams.
        val terms = (ngrams > 1) match {
          case false => words
          case _ => words ++ words.sliding(ngrams).map(_.mkString(separator)).toList
        }

        // Return the term and it's count in the document.
        Collection(terms
          .groupBy(identity)
          .map {
            case (k, v) => Cell(cell.position.append(k), Content(DiscreteSchema[Codex.LongCodex](), v.size))
          }
          .toList)
      case _ => Collection[Cell[Position3D]]()
    }
  }
}

// Simple tf-idf example (input data is same as tf-idf example here: http://en.wikipedia.org/wiki/Tf%E2%80%93idf).
class InstanceCentricTfIdf(args: Args) extends Job(args) {

  // Path to data files, output folder
  val path = args.getOrElse("path", "../../data")
  val output = "scalding"

  // Read event data, then de-normalises the events and return a 2D matrix (event id x instance id).
  val data = ExampleEvent.load(s"${path}/exampleEvents.txt")
    .transform(Denormalise())

  // For each event, append the word counts to the 3D matrix. The result is a 3D matrix (event id x instance id x word
  // count). Then aggregate out the event id. The result is a 2D matrix (instance x word count) where the counts are
  // the sums over all events.
  val tf = data
    .transform(WordCounts(stopwords = List()))
    .summarise(Along(First), Sum[Position3D, Position2D]())

  // Get the number of instances (i.e. documents)
  val n = tf
    .size(First)
    .toMap(Over(First))

  // Define extractor to get data out of map.
  val extractN = ExtractWithKey[Position1D, String, Content](First.toString)
    .andThenPresent(_.value.asDouble)

  // Using the number of documents, compute Idf:
  //  1/ Compute document frequency;
  //  2/ Apply Idf transformation (using document count);
  //  3/ Save as Map for use in Tf-Idf below.
  val idf = tf
    .summarise(Along(First), Count[Position2D, Position1D]())
    .transformWithValue(Idf(extractN, (df: Double, n: Double) => math.log10(n / df)), n)
    .toMap(Over(First))

  // Define extractor to get data out of idf map.
  val extractIdf = ExtractWithDimension[Dimension.Second, Position2D, Content](Second)
    .andThenPresent(_.value.asDouble)

  // Apply TfIdf to the term frequency matrix with the Idf values, then save the results to file.
  //
  // Uncomment one of the 3 lines below to try different tf-idf versions.
  val tfIdf = tf
    //.transform(BooleanTf[Position2D]())
    //.transform(LogarithmicTf[Position2D]())
    //.transformWithValue(AugmentedTf(ExtractWithDimension[Dimension.First, Position2D, Content](First)
    //    .andThenPresent(_.value.asDouble)),
    //  tf.summarise(Along(Second), Max[Position2D, Position1D]()).toMap(Over(First)))
    .transformWithValue(TfIdf(extractIdf), idf)
    .save(s"./demo.${output}/tfidf_entity.out")
}

