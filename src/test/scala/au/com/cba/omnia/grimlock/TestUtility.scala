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
import au.com.cba.omnia.grimlock.content.metadata._
import au.com.cba.omnia.grimlock.encoding._
import au.com.cba.omnia.grimlock.position._
import au.com.cba.omnia.grimlock.utility._

import org.scalatest._

class TestCollection extends FlatSpec with Matchers {

  "A Collection" should "create an empty collection" in {
    Collection[String]() should be (Collection[String](None))
    Collection.empty[String] should be (Collection[String](None))
  }

  it should "create a collection with a single entry" in {
    Collection("foo") should be (Collection(Some(Left("foo"))))
  }

  it should "create a collection with a multiple entries" in {
    Collection(List("foo", "bar")) should be (Collection(Some(Right(List("foo", "bar")))))
  }

  it should "create a collection with a cell" in {
    Collection(Position1D("foo"), Content(NominalSchema[Codex.StringCodex](), "bar")) should be
      (Collection(Some(Left((Position1D("foo"), Content(NominalSchema[Codex.StringCodex](), "bar"))))))
  }

  it should "identify an empty collection" in {
    Collection[String]().isEmpty should be (true)
  }

  it should "identify a non-empty collection" in {
    Collection("foo").isEmpty should be (false)
    Collection(List("foo", "bar")).isEmpty should be (false)
  }

  it should "return an empty list" in {
    Collection[String]().toList should be (List())
  }

  it should "return a single entry list" in {
    Collection("foo").toList should be (List("foo"))
  }

  it should "return a multi entry list" in {
    Collection(List("foo", "bar")).toList should be (List("foo", "bar"))
  }

  it should "return an empty list with value" in {
    Collection[String]().toList(3.14) should be (List())
  }

  it should "return a single entry list with value" in {
    Collection("foo").toList(3.14) should be (List(("foo", 3.14)))
  }

  it should "return a multi entry list with value" in {
    Collection(List("foo", "bar")).toList(3.14) should be (List(("foo", 3.14), ("bar", 3.14)))
  }
}

class TestQuote extends FlatSpec with Matchers {

  "A Quote" should "escape a special character" in {
    Quote().escape("foo,bar,baz", ",") should be ("\"foo,bar,baz\"")
  }

  it should "not escape regular characters" in {
    Quote().escape("foo;bar;baz", ",") should be ("foo;bar;baz")
  }

  it should "always escape a special character" in {
    Quote(true).escape("foo,bar,baz", ",") should be ("\"foo,bar,baz\"")
  }

  it should "always escape regular characters" in {
    Quote(true).escape("foo;bar;baz", ",") should be ("\"foo;bar;baz\"")
  }
}

class TestReplace extends FlatSpec with Matchers {

  "A Replace" should "escape a special character" in {
    Replace().escape("foo,bar,baz", ",") should be ("foo\\,bar\\,baz")
  }

  it should "not escape regular characters" in {
    Replace().escape("foo;bar;baz", ",") should be ("foo;bar;baz")
  }

  it should "substitute a special character" in {
    Replace("|").escape("foo,bar,baz", ",") should be ("foo|bar|baz")
  }

  it should "not substitute regular characters" in {
    Replace("|").escape("foo;bar;baz", ",") should be ("foo;bar;baz")
  }
}

