// Copyright 2016 Commonwealth Bank of Australia
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

package au.com.cba.omnia.grimlock.spark.environment

import au.com.cba.omnia.grimlock.framework.environment.{
  Context => FwContext,
  DistributedData => FwDistributedData,
  Environment => FwEnvironment,
  UserData => FwUserData
}

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Spark operating context state.
 *
 * @param context The Spark context.
 */
case class Context(context: SparkContext) extends FwContext

trait DistributedData extends FwDistributedData {
  type U[X] = RDD[X]
}

trait UserData extends FwUserData {
  type E[X] = X
}

trait Environment extends FwEnvironment {
  type C = Context
}

