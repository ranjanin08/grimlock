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

import au.com.cba.omnia.grimlock.Type._

import org.scalatest._

class TestMixedType extends FlatSpec with Matchers {

  "A Mixed" should "return its short name" in {
    Mixed.toShortString should be ("mixed")
  }

  it should "return its name" in {
    Mixed.toString should be ("Mixed")
  }

  it should "return correct generalised type" in {
    Mixed.getGeneralisation should be (Mixed)
  }

  it should "match correct specialisation" in {
    Mixed.isSpecialisationOf(Mixed) should be (true)
  }

  it should "not match incorrect specialisation" in {
    Mixed.isSpecialisationOf(Numerical) should be (false)
  }
}

class TestNumericalType extends FlatSpec with Matchers {

  "A Numerical" should "return its short name" in {
    Numerical.toShortString should be ("numerical")
  }

  it should "return its name" in {
    Numerical.toString should be ("Numerical")
  }

  it should "return correct generalised type" in {
    Numerical.getGeneralisation should be (Numerical)
  }

  it should "match correct specialisation" in {
    Numerical.isSpecialisationOf(Numerical) should be (true)
  }

  it should "not match incorrect specialisation" in {
    Numerical.isSpecialisationOf(Continuous) should be (false)
  }
}

class TestContinuousType extends FlatSpec with Matchers {

  "A Continuous" should "return its short name" in {
    Continuous.toShortString should be ("continuous")
  }

  it should "return its name" in {
    Continuous.toString should be ("Continuous")
  }

  it should "return correct generalised type" in {
    Continuous.getGeneralisation should be (Numerical)
  }

  it should "match correct specialisation" in {
    Continuous.isSpecialisationOf(Continuous) should be (true)
    Continuous.isSpecialisationOf(Numerical) should be (true)
  }

  it should "not match incorrect specialisation" in {
    Continuous.isSpecialisationOf(Mixed) should be (false)
  }
}

class TestDiscreteType extends FlatSpec with Matchers {

  "A Discrete" should "return its short name" in {
    Discrete.toShortString should be ("discrete")
  }

  it should "return its name" in {
    Discrete.toString should be ("Discrete")
  }

  it should "return correct generalised type" in {
    Discrete.getGeneralisation should be (Numerical)
  }

  it should "match correct specialisation" in {
    Discrete.isSpecialisationOf(Discrete) should be (true)
    Discrete.isSpecialisationOf(Numerical) should be (true)
  }

  it should "not match incorrect specialisation" in {
    Discrete.isSpecialisationOf(Mixed) should be (false)
  }
}

class TestCategoricalType extends FlatSpec with Matchers {

  "A Categorical" should "return its short name" in {
    Categorical.toShortString should be ("categorical")
  }

  it should "return its name" in {
    Categorical.toString should be ("Categorical")
  }

  it should "return correct generalised type" in {
    Categorical.getGeneralisation should be (Categorical)
  }

  it should "match correct specialisation" in {
    Categorical.isSpecialisationOf(Categorical) should be (true)
  }

  it should "not match incorrect specialisation" in {
    Categorical.isSpecialisationOf(Continuous) should be (false)
  }
}

class TestNominalType extends FlatSpec with Matchers {

  "A Nominal" should "return its short name" in {
    Nominal.toShortString should be ("nominal")
  }

  it should "return its name" in {
    Nominal.toString should be ("Nominal")
  }

  it should "return correct generalised type" in {
    Nominal.getGeneralisation should be (Categorical)
  }

  it should "match correct specialisation" in {
    Nominal.isSpecialisationOf(Nominal) should be (true)
    Nominal.isSpecialisationOf(Categorical) should be (true)
  }

  it should "not match incorrect specialisation" in {
    Nominal.isSpecialisationOf(Mixed) should be (false)
  }
}

class TestOrdinalType extends FlatSpec with Matchers {

  "A Ordinal" should "return its short name" in {
    Ordinal.toShortString should be ("ordinal")
  }

  it should "return its name" in {
    Ordinal.toString should be ("Ordinal")
  }

  it should "return correct generalised type" in {
    Ordinal.getGeneralisation should be (Categorical)
  }

  it should "match correct specialisation" in {
    Ordinal.isSpecialisationOf(Ordinal) should be (true)
    Ordinal.isSpecialisationOf(Categorical) should be (true)
  }

  it should "not match incorrect specialisation" in {
    Ordinal.isSpecialisationOf(Mixed) should be (false)
  }
}

class TestDateType extends FlatSpec with Matchers {

  "A Date" should "return its short name" in {
    Date.toShortString should be ("date")
  }

  it should "return its name" in {
    Date.toString should be ("Date")
  }

  it should "return correct generalised type" in {
    Date.getGeneralisation should be (Date)
  }

  it should "match correct specialisation" in {
    Date.isSpecialisationOf(Date) should be (true)
  }

  it should "not match incorrect specialisation" in {
    Date.isSpecialisationOf(Numerical) should be (false)
  }
}

class TestEventType extends FlatSpec with Matchers {

  "A Event" should "return its short name" in {
    Event.toShortString should be ("event")
  }

  it should "return its name" in {
    Event.toString should be ("Event")
  }

  it should "return correct generalised type" in {
    Event.getGeneralisation should be (Event)
  }

  it should "match correct specialisation" in {
    Event.isSpecialisationOf(Event) should be (true)
  }

  it should "not match incorrect specialisation" in {
    Event.isSpecialisationOf(Numerical) should be (false)
  }
}

