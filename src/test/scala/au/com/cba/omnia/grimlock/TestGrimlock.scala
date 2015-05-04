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

import au.com.cba.omnia.grimlock.framework.position._

import org.apache.log4j.{ Level, Logger }
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd._

import org.scalatest._

import scala.reflect.ClassTag

trait TestGrimlock extends FlatSpec with Matchers {
  def toRDD[T](list: List[T])(implicit ev: ClassTag[T]): RDD[T] = TestGrimlock.spark.parallelize(list)

  implicit def toList[T](rdd: RDD[T]): List[T] = rdd.toLocalIterator.toList

  implicit def PositionOrdering[T <: Position] = Position.Ordering[T]

  implicit val sc = TestGrimlock.spark
}

object TestGrimlock {

  val spark = new SparkContext("local", "Test Spark", new SparkConf())

  Logger.getRootLogger().setLevel(Level.WARN);
}

