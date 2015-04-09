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

import com.twitter.scalding._
import com.twitter.scalding.typed.LiteralValue

/** Implement cut rules using scalding. */
object ScaldingCutRules extends CutRules {
  type E[A] = ValuePipe[A]

  def fixed[V: Valueable, W: Valueable](ext: ValuePipe[Stats], min: V, max: W, k: Long): ValuePipe[Cut#V] = {
    ext.map(stats => fixedFromStats(stats, min, max, k))
  }

  def squareRootChoice[V: Valueable, W: Valueable, X: Valueable](ext: ValuePipe[Stats], count: V, min: W,
    max: X): ValuePipe[Cut#V] = ext.map { case stats => squareRootChoiceFromStats(stats, count, min, max) }

  def sturgesFormula[V: Valueable, W: Valueable, X: Valueable](ext: ValuePipe[Stats], count: V, min: W,
    max: X): ValuePipe[Cut#V] = ext.map { case stats => sturgesFormulaFromStats(stats, count, min, max) }

  def riceRule[V: Valueable, W: Valueable, X: Valueable](ext: ValuePipe[Stats], count: V, min: W,
    max: X): ValuePipe[Cut#V] = ext.map { case stats => riceRuleFromStats(stats, count, min, max) }

  def doanesFormula[V: Valueable, W: Valueable, X: Valueable, Y: Valueable](ext: ValuePipe[Stats], count: V, min: W,
    max: X, skewness: Y): ValuePipe[Cut#V] = {
    ext.map { case stats => doanesFormulaFromStats(stats, count, min, max, skewness) }
  }

  def scottsNormalReferenceRule[V: Valueable, W: Valueable, X: Valueable, Y: Valueable](ext: ValuePipe[Stats],
    count: V, min: W, max: X, sd: Y): ValuePipe[Cut#V] = {
    ext.map { case stats => scottsNormalReferenceRuleFromStats(stats, count, min, max, sd) }
  }

  def breaks[V: Valueable](range: Map[V, List[Double]]): ValuePipe[Cut#V] = new LiteralValue(breaksFromMap(range))
}

