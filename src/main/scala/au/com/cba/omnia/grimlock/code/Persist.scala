package au.com.cba.omnia.grimlock

import cascading.flow.FlowDef
import com.twitter.scalding._
import com.twitter.scalding.typed.TypedSink

trait Persist[T] {
  protected val data: TypedPipe[T]

  /**
   * Persist to disk.
   *
   * @param file        Name of the output file.
   * @param separator   Separator to use between the fields.
   * @param descriptive Indicates if the output should be descriptive.
   *
   * @return A Scalding `TypedPipe[T]` which is this object's data.
   */
  def persistFile(file: String, separator: String = "|", descriptive: Boolean = false)(implicit flow: FlowDef,
    mode: Mode): TypedPipe[T] = persist(TypedSink(TextLine(file)), separator, descriptive)

  /**
   * Persist to a sink.
   *
   * @param sink        Sink to write to.
   * @param separator   Separator to use between the fields.
   * @param descriptive Indicates if the output should be descriptive.
   *
   * @return A Scalding `TypedPipe[T]` which is this object's data.
   */
  def persist(sink: TypedSink[String], separator: String = "|", descriptive: Boolean = false)(implicit flow: FlowDef,
    mode: Mode): TypedPipe[T] = {
    data
      .map { case t => toString(t, separator, descriptive) }
      .write(sink)

    data
  }

  protected def toString(t: T, separator: String, descriptive: Boolean): String
}

