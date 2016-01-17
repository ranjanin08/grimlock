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

package au.com.cba.omnia.grimlock.spark.transform

import au.com.cba.omnia.grimlock.framework.position._

import au.com.cba.omnia.grimlock.library.transform.{ CutRules => BaseCutRules }

/** Implement cut rules for Spark. */
object CutRules extends BaseCutRules {
  type E[A] = A

  def fixed(ext: Stats, min: Positionable[Position1D], max: Positionable[Position1D],
    k: Long): Map[Position1D, List[Double]] = fixedFromStats(ext, min, max, k)

  def squareRootChoice(ext: Stats, count: Positionable[Position1D], min: Positionable[Position1D],
    max: Positionable[Position1D]): Map[Position1D, List[Double]] = squareRootChoiceFromStats(ext, count, min, max)

  def sturgesFormula(ext: Stats, count: Positionable[Position1D], min: Positionable[Position1D],
    max: Positionable[Position1D]): Map[Position1D, List[Double]] = sturgesFormulaFromStats(ext, count, min, max)

  def riceRule(ext: Stats, count: Positionable[Position1D], min: Positionable[Position1D],
    max: Positionable[Position1D]): Map[Position1D, List[Double]] = riceRuleFromStats(ext, count, min, max)

  def doanesFormula(ext: Stats, count: Positionable[Position1D], min: Positionable[Position1D],
    max: Positionable[Position1D], skewness: Positionable[Position1D]): Map[Position1D, List[Double]] = {
    doanesFormulaFromStats(ext, count, min, max, skewness)
  }

  def scottsNormalReferenceRule(ext: Stats, count: Positionable[Position1D], min: Positionable[Position1D],
    max: Positionable[Position1D], sd: Positionable[Position1D]): Map[Position1D, List[Double]] = {
    scottsNormalReferenceRuleFromStats(ext, count, min, max, sd)
  }

  def breaks[P <% Positionable[Position1D]](range: Map[P, List[Double]]): Map[Position1D, List[Double]] = {
    breaksFromMap(range)
  }
}

