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

package commbank.grimlock.spark.transform

import commbank.grimlock.framework.position.Position

import commbank.grimlock.spark.environment.UserData

import commbank.grimlock.library.transform.{ CutRules => FwCutRules }

import shapeless.nat._1

/** Implement cut rules for Spark. */
object CutRules extends FwCutRules with UserData {
  def fixed(
    ext: E[Stats],
    min: Position[_1],
    max: Position[_1],
    k: Long
  ): E[Map[Position[_1], List[Double]]] = fixedFromStats(ext, min, max, k)

  def squareRootChoice(
    ext: E[Stats],
    count: Position[_1],
    min: Position[_1],
    max: Position[_1]
  ): E[Map[Position[_1], List[Double]]] = squareRootChoiceFromStats(ext, count, min, max)

  def sturgesFormula(
    ext: E[Stats],
    count: Position[_1],
    min: Position[_1],
    max: Position[_1]
  ): E[Map[Position[_1], List[Double]]] = sturgesFormulaFromStats(ext, count, min, max)

  def riceRule(
    ext: E[Stats],
    count: Position[_1],
    min: Position[_1],
    max: Position[_1]
  ): E[Map[Position[_1], List[Double]]] = riceRuleFromStats(ext, count, min, max)

  def doanesFormula(
    ext: E[Stats],
    count: Position[_1],
    min: Position[_1],
    max: Position[_1],
    skewness: Position[_1]
  ): E[Map[Position[_1], List[Double]]] = doanesFormulaFromStats(ext, count, min, max, skewness)

  def scottsNormalReferenceRule(
    ext: E[Stats],
    count: Position[_1],
    min: Position[_1],
    max: Position[_1],
    sd: Position[_1]
  ): E[Map[Position[_1], List[Double]]] = scottsNormalReferenceRuleFromStats(ext, count, min, max, sd)

  def breaks[P <% Position[_1]](range: Map[P, List[Double]]): E[Map[Position[_1], List[Double]]] = breaksFromMap(range)
}

