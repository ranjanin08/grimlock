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

package au.com.cba.omnia.grimlock.scalding.transform

import au.com.cba.omnia.grimlock.framework.position._

import au.com.cba.omnia.grimlock.scalding.environment._

import au.com.cba.omnia.grimlock.library.transform.{ CutRules => FwCutRules }

import com.twitter.scalding.typed.LiteralValue

/** Implement cut rules using Scalding. */
object CutRules extends FwCutRules with UserData {
  def fixed(ext: E[Stats], min: Positionable[Position1D], max: Positionable[Position1D],
    k: Long): E[Map[Position1D, List[Double]]] = ext.map(stats => fixedFromStats(stats, min, max, k))

  def squareRootChoice(ext: E[Stats], count: Positionable[Position1D], min: Positionable[Position1D],
    max: Positionable[Position1D]): E[Map[Position1D, List[Double]]] = {
    ext.map { case stats => squareRootChoiceFromStats(stats, count, min, max) }
  }

  def sturgesFormula(ext: E[Stats], count: Positionable[Position1D], min: Positionable[Position1D],
    max: Positionable[Position1D]): E[Map[Position1D, List[Double]]] = {
    ext.map { case stats => sturgesFormulaFromStats(stats, count, min, max) }
  }

  def riceRule(ext: E[Stats], count: Positionable[Position1D], min: Positionable[Position1D],
    max: Positionable[Position1D]): E[Map[Position1D, List[Double]]] = {
    ext.map { case stats => riceRuleFromStats(stats, count, min, max) }
  }

  def doanesFormula(ext: E[Stats], count: Positionable[Position1D], min: Positionable[Position1D],
    max: Positionable[Position1D], skewness: Positionable[Position1D]): E[Map[Position1D, List[Double]]] = {
    ext.map { case stats => doanesFormulaFromStats(stats, count, min, max, skewness) }
  }

  def scottsNormalReferenceRule(ext: E[Stats], count: Positionable[Position1D], min: Positionable[Position1D],
    max: Positionable[Position1D], sd: Positionable[Position1D]): E[Map[Position1D, List[Double]]] = {
    ext.map { case stats => scottsNormalReferenceRuleFromStats(stats, count, min, max, sd) }
  }

  def breaks[P <% Positionable[Position1D]](range: Map[P, List[Double]]): E[Map[Position1D, List[Double]]] = {
    new LiteralValue(breaksFromMap(range))
  }
}

