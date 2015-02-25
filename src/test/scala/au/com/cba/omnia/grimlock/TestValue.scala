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

import au.com.cba.omnia.grimlock.content._
import au.com.cba.omnia.grimlock.encoding._

import org.scalatest._

class TestDateValue extends FlatSpec with Matchers {

  val dfmt = new java.text.SimpleDateFormat("dd/MM/yyyy")
  val date2001 = dfmt.parse("01/01/2001")
  val date2002 = dfmt.parse("01/01/2002")
  val dv2001 = DateValue(date2001, DateCodex)
  val dv2002 = DateValue(date2002, DateCodex)

  "A DateValue" should "return its short string" in {
    dv2001.toShortString should be ("2001-01-01")
  }

  it should "return a date" in {
    dv2001.asDate should be (Some(date2001))
    dv2001.asDate should be (Some(dfmt.parse("01/01/2001")))
  }

  it should "not return a string" in {
    dv2001.asString should be (None)
  }

  it should "not return a double" in {
    dv2001.asDouble should be (None)
  }

  it should "not return a long" in {
    dv2001.asLong should be (None)
  }

  it should "not return a boolean" in {
    dv2001.asBoolean should be (None)
  }

  it should "not return a event" in {
    dv2001.asEvent should be (None)
  }

  it should "equal itself" in {
    dv2001.equ(dv2001) should be (true)
    dv2001.equ(DateValue(date2001, DateCodex)) should be (true)
  }

  it should "not equal another date" in {
    dv2001.equ(dv2002) should be (false)
  }

  it should "not equal another value" in {
    dv2001.equ("a") should be (false)
    dv2001.equ(2) should be (false)
    dv2001.equ(2.0) should be (false)
    dv2001.equ(false) should be (false)
  }

  it should "match a matching pattern" in {
    dv2001.like("^20.*".r) should be (true)
  }

  it should "not match a non-existing pattern" in {
    dv2001.like("^2002.*".r) should be (false)
  }

  it should "identify a smaller value calling lss" in {
    dv2001.lss(dv2002) should be (true)
  }

  it should "not identify an equal value calling lss" in {
    dv2001.lss(dv2001) should be (false)
  }

  it should "not identify a greater value calling lss" in {
    dv2002.lss(dv2001) should be (false)
  }

  it should "not identify another value calling lss" in {
    dv2001.lss(2) should be (false)
  }

  it should "identify a smaller value calling leq" in {
    dv2001.leq(dv2002) should be (true)
  }

  it should "identify an equal value calling leq" in {
    dv2001.leq(dv2001) should be (true)
  }

  it should "not identify a greater value calling leq" in {
    dv2002.leq(dv2001) should be (false)
  }

  it should "not identify another value calling leq" in {
    dv2001.leq(2) should be (false)
  }

  it should "not identify a smaller value calling gtr" in {
    dv2001.gtr(dv2002) should be (false)
  }

  it should "not identify an equal value calling gtr" in {
    dv2001.gtr(dv2001) should be (false)
  }

  it should "identify a greater value calling gtr" in {
    dv2002.gtr(dv2001) should be (true)
  }

  it should "not identify another value calling gtr" in {
    dv2001.gtr(2) should be (false)
  }

  it should "not identify a smaller value calling geq" in {
    dv2001.geq(dv2002) should be (false)
  }

  it should "identify an equal value calling geq" in {
    dv2001.geq(dv2001) should be (true)
  }

  it should "identify a greater value calling geq" in {
    dv2002.geq(dv2001) should be (true)
  }

  it should "not identify another value calling geq" in {
    dv2001.geq(2) should be (false)
  }
}

class TestStringValue extends FlatSpec with Matchers {

  val foo = "foo"
  val bar = "bar"
  val dvfoo = StringValue(foo)
  val dvbar = StringValue(bar)

  "A StringValue" should "return its short string" in {
    dvfoo.toShortString should be (foo)
  }

  it should "not return a date" in {
    dvfoo.asDate should be (None)
  }

  it should "return a string" in {
    dvfoo.asString should be (Some(foo))
    dvfoo.asString should be (Some("foo"))
  }

  it should "not return a double" in {
    dvfoo.asDouble should be (None)
  }

  it should "not return a long" in {
    dvfoo.asLong should be (None)
  }

  it should "not return a boolean" in {
    dvfoo.asBoolean should be (None)
  }

  it should "not return a event" in {
    dvfoo.asEvent should be (None)
  }

  it should "equal itself" in {
    dvfoo.equ(dvfoo) should be (true)
    dvfoo.equ(StringValue(foo)) should be (true)
  }

  it should "not equal another string" in {
    dvfoo.equ(dvbar) should be (false)
  }

  it should "not equal another value" in {
    dvfoo.equ(2) should be (false)
    dvfoo.equ(2.0) should be (false)
    dvfoo.equ(false) should be (false)
  }

  it should "match a matching pattern" in {
    dvfoo.like("^f..".r) should be (true)
  }

  it should "not match a non-existing pattern" in {
    dvfoo.like("^b..".r) should be (false)
  }

  it should "identify a smaller value calling lss" in {
    dvbar.lss(dvfoo) should be (true)
  }

  it should "not identify an equal value calling lss" in {
    dvbar.lss(dvbar) should be (false)
  }

  it should "not identify a greater value calling lss" in {
    dvfoo.lss(dvbar) should be (false)
  }

  it should "not identify another value calling lss" in {
    dvbar.lss(2) should be (false)
  }

  it should "identify a smaller value calling leq" in {
    dvbar.leq(dvfoo) should be (true)
  }

  it should "identify an equal value calling leq" in {
    dvbar.leq(dvbar) should be (true)
  }

  it should "not identify a greater value calling leq" in {
    dvfoo.leq(dvbar) should be (false)
  }

  it should "not identify another value calling leq" in {
    dvbar.leq(2) should be (false)
  }

  it should "not identify a smaller value calling gtr" in {
    dvbar.gtr(dvfoo) should be (false)
  }

  it should "not identify an equal value calling gtr" in {
    dvbar.gtr(dvbar) should be (false)
  }

  it should "identify a greater value calling gtr" in {
    dvfoo.gtr(dvbar) should be (true)
  }

  it should "not identify another value calling gtr" in {
    dvbar.gtr(2) should be (false)
  }

  it should "not identify a smaller value calling geq" in {
    dvbar.geq(dvfoo) should be (false)
  }

  it should "identify an equal value calling geq" in {
    dvbar.geq(dvbar) should be (true)
  }

  it should "identify a greater value calling geq" in {
    dvfoo.geq(dvbar) should be (true)
  }

  it should "not identify another value calling geq" in {
    dvbar.geq(2) should be (false)
  }
}

class TestDoubleValue extends FlatSpec with Matchers {

  val one = 1.0
  val pi = 3.14
  val dvone = DoubleValue(one)
  val dvpi = DoubleValue(pi)

  "A DoubleValue" should "return its short string" in {
    dvone.toShortString should be ("1.0")
  }

  it should "not return a date" in {
    dvone.asDate should be (None)
  }

  it should "not return a string" in {
    dvone.asString should be (None)
  }

  it should "return a double" in {
    dvone.asDouble should be (Some(one))
    dvone.asDouble should be (Some(1.0))
  }

  it should "not return a long" in {
    dvone.asLong should be (None)
  }

  it should "not return a boolean" in {
    dvone.asBoolean should be (None)
  }

  it should "not return a event" in {
    dvone.asEvent should be (None)
  }

  it should "equal itself" in {
    dvone.equ(dvone) should be (true)
    dvone.equ(DoubleValue(one)) should be (true)
  }

  it should "not equal another double" in {
    dvone.equ(dvpi) should be (false)
  }

  it should "not equal another value" in {
    dvone.equ("a") should be (false)
    dvone.equ(false) should be (false)
  }

  it should "match a matching pattern" in {
    dvone.like("..0$".r) should be (true)
  }

  it should "not match a non-existing pattern" in {
    dvone.like("^3...".r) should be (false)
  }

  it should "identify a smaller value calling lss" in {
    dvone.lss(dvpi) should be (true)
  }

  it should "not identify an equal value calling lss" in {
    dvone.lss(dvone) should be (false)
  }

  it should "not identify a greater value calling lss" in {
    dvpi.lss(dvone) should be (false)
  }

  it should "not identify another value calling lss" in {
    dvone.lss("a") should be (false)
  }

  it should "identify a smaller value calling leq" in {
    dvone.leq(dvpi) should be (true)
  }

  it should "identify an equal value calling leq" in {
    dvone.leq(dvone) should be (true)
  }

  it should "not identify a greater value calling leq" in {
    dvpi.leq(dvone) should be (false)
  }

  it should "not identify another value calling leq" in {
    dvone.leq("a") should be (false)
  }

  it should "not identify a smaller value calling gtr" in {
    dvone.gtr(dvpi) should be (false)
  }

  it should "not identify an equal value calling gtr" in {
    dvone.gtr(dvpi) should be (false)
  }

  it should "identify a greater value calling gtr" in {
    dvpi.gtr(dvone) should be (true)
  }

  it should "not identify another value calling gtr" in {
    dvone.gtr("a") should be (false)
  }

  it should "not identify a smaller value calling geq" in {
    dvone.geq(dvpi) should be (false)
  }

  it should "identify an equal value calling geq" in {
    dvone.geq(dvone) should be (true)
  }

  it should "identify a greater value calling geq" in {
    dvpi.geq(dvone) should be (true)
  }

  it should "not identify another value calling geq" in {
    dvone.geq("a") should be (false)
  }
}

class TestLongValue extends FlatSpec with Matchers {

  val one = 1
  val two = 2
  val dvone = LongValue(one)
  val dvtwo = LongValue(two)

  "A LongValue" should "return its short string" in {
    dvone.toShortString should be ("1")
  }

  it should "not return a date" in {
    dvone.asDate should be (None)
  }

  it should "not return a string" in {
    dvone.asString should be (None)
  }

  it should "return a double" in {
    dvone.asDouble should be (Some(one))
    dvone.asDouble should be (Some(1.0))
  }

  it should "return a long" in {
    dvone.asLong should be (Some(one))
    dvone.asLong should be (Some(1))
  }

  it should "not return a boolean" in {
    dvone.asBoolean should be (None)
  }

  it should "not return a event" in {
    dvone.asEvent should be (None)
  }

  it should "equal itself" in {
    dvone.equ(dvone) should be (true)
    dvone.equ(LongValue(one)) should be (true)
  }

  it should "not equal another double" in {
    dvone.equ(dvtwo) should be (false)
  }

  it should "not equal another value" in {
    dvone.equ("a") should be (false)
    dvone.equ(false) should be (false)
  }

  it should "match a matching pattern" in {
    dvone.like("^1$".r) should be (true)
  }

  it should "not match a non-existing pattern" in {
    dvone.like("^3...".r) should be (false)
  }

  it should "identify a smaller value calling lss" in {
    dvone.lss(dvtwo) should be (true)
  }

  it should "not identify an equal value calling lss" in {
    dvone.lss(dvone) should be (false)
  }

  it should "not identify a greater value calling lss" in {
    dvtwo.lss(dvone) should be (false)
  }

  it should "not identify another value calling lss" in {
    dvone.lss("a") should be (false)
  }

  it should "identify a smaller value calling leq" in {
    dvone.leq(dvtwo) should be (true)
  }

  it should "identify an equal value calling leq" in {
    dvone.leq(dvone) should be (true)
  }

  it should "not identify a greater value calling leq" in {
    dvtwo.leq(dvone) should be (false)
  }

  it should "not identify another value calling leq" in {
    dvone.leq("a") should be (false)
  }

  it should "not identify a smaller value calling gtr" in {
    dvone.gtr(dvtwo) should be (false)
  }

  it should "not identify an equal value calling gtr" in {
    dvone.gtr(dvtwo) should be (false)
  }

  it should "identify a greater value calling gtr" in {
    dvtwo.gtr(dvone) should be (true)
  }

  it should "not identify another value calling gtr" in {
    dvone.gtr("a") should be (false)
  }

  it should "not identify a smaller value calling geq" in {
    dvone.geq(dvtwo) should be (false)
  }

  it should "identify an equal value calling geq" in {
    dvone.geq(dvone) should be (true)
  }

  it should "identify a greater value calling geq" in {
    dvtwo.geq(dvone) should be (true)
  }

  it should "not identify another value calling geq" in {
    dvone.geq("a") should be (false)
  }
}

class TestBooleanValue extends FlatSpec with Matchers {

  val pos = true
  val neg = false
  val dvpos = BooleanValue(pos)
  val dvneg = BooleanValue(neg)

  "A BooleanValue" should "return its short string" in {
    dvpos.toShortString should be ("true")
  }

  it should "not return a date" in {
    dvpos.asDate should be (None)
  }

  it should "not return a string" in {
    dvpos.asString should be (None)
  }

  it should "not return a double" in {
    dvpos.asDouble should be (None)
  }

  it should "not return a long" in {
    dvpos.asLong should be (None)
  }

  it should "return a boolean" in {
    dvpos.asBoolean should be (Some(pos))
    dvpos.asBoolean should be (Some(true))
  }

  it should "not return a event" in {
    dvpos.asEvent should be (None)
  }

  it should "equal itself" in {
    dvpos.equ(dvpos) should be (true)
    dvpos.equ(BooleanValue(pos)) should be (true)
  }

  it should "not equal another boolean" in {
    dvpos.equ(dvneg) should be (false)
  }

  it should "not equal another value" in {
    dvpos.equ("a") should be (false)
    dvpos.equ(2) should be (false)
    dvpos.equ(2.0) should be (false)
  }

  it should "match a matching pattern" in {
    dvpos.like("^tr..".r) should be (true)
  }

  it should "not match a non-existing pattern" in {
    dvpos.like("^fal..".r) should be (false)
  }

  it should "identify a smaller value calling lss" in {
    dvneg.lss(dvpos) should be (true)
  }

  it should "not identify an equal value calling lss" in {
    dvneg.lss(dvneg) should be (false)
  }

  it should "not identify a greater value calling lss" in {
    dvpos.lss(dvneg) should be (false)
  }

  it should "not identify another value calling lss" in {
    dvneg.lss(2) should be (false)
  }

  it should "identify a smaller value calling leq" in {
    dvneg.leq(dvpos) should be (true)
  }

  it should "identify an equal value calling leq" in {
    dvneg.leq(dvneg) should be (true)
  }

  it should "not identify a greater value calling leq" in {
    dvpos.leq(dvneg) should be (false)
  }

  it should "not identify another value calling leq" in {
    dvneg.leq(2) should be (false)
  }

  it should "not identify a smaller value calling gtr" in {
    dvneg.gtr(dvpos) should be (false)
  }

  it should "not identify an equal value calling gtr" in {
    dvneg.gtr(dvneg) should be (false)
  }

  it should "identify a greater value calling gtr" in {
    dvpos.gtr(dvneg) should be (true)
  }

  it should "not identify another value calling gtr" in {
    dvneg.gtr(2) should be (false)
  }

  it should "not identify a smaller value calling geq" in {
    dvneg.geq(dvpos) should be (false)
  }

  it should "identify an equal value calling geq" in {
    dvneg.geq(dvneg) should be (true)
  }

  it should "identify a greater value calling geq" in {
    dvpos.geq(dvneg) should be (true)
  }

  it should "not identify another value calling geq" in {
    dvneg.geq(2) should be (false)
  }
}

