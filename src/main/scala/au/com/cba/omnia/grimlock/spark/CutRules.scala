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

package au.com.cba.omnia.grimlock.transform

import au.com.cba.omnia.grimlock.encoding._

/** Implement cut rules using spark. */
object SparkCutRules extends CutRules {
  type E[A] = A

  def fixed[V: Valueable, W: Valueable](ext: Stats, min: V, max: W, k: Long): Cut#V = fixedFromStats(ext, min, max, k)

  def squareRootChoice[V: Valueable, W: Valueable, X: Valueable](ext: Stats, count: V, min: W, max: X): Cut#V = {
    squareRootChoiceFromStats(ext, count, min, max)
  }

  def sturgesFormula[V: Valueable, W: Valueable, X: Valueable](ext: Stats, count: V, min: W, max: X): Cut#V = {
    sturgesFormulaFromStats(ext, count, min, max)
  }

  def riceRule[V: Valueable, W: Valueable, X: Valueable](ext: Stats, count: V, min: W, max: X): Cut#V = {
    riceRuleFromStats(ext, count, min, max)
  }

  def doanesFormula[V: Valueable, W: Valueable, X: Valueable, Y: Valueable](ext: Stats, count: V, min: W, max: X,
    skewness: Y): Cut#V = doanesFormulaFromStats(ext, count, min, max, skewness)

  def scottsNormalReferenceRule[V: Valueable, W: Valueable, X: Valueable, Y: Valueable](ext: Stats, count: V, min: W,
    max: X, sd: Y): Cut#V = scottsNormalReferenceRuleFromStats(ext, count, min, max, sd)

  def breaks[V: Valueable](range: Map[V, List[Double]]): Cut#V = breaksFromMap(range)
}

