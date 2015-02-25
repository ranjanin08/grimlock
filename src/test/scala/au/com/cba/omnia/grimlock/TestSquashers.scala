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
import au.com.cba.omnia.grimlock.squash._

import org.scalatest._

trait TestSquashers {

  val dfmt = new java.text.SimpleDateFormat("yyyy-MM-dd")
  val pos1 = Position3D(1, "b", DateValue(dfmt.parse("2001-01-01"), DateCodex))
  val pos2 = Position3D(2, "a", DateValue(dfmt.parse("2002-01-01"), DateCodex))
  val con = Content(ContinuousSchema[Codex.LongCodex](), 123)
}

class TestPreservingMaxPosition extends FlatSpec with Matchers with TestSquashers {

  "A PreservingMaxPosition" should "return the second cell for the first dimension when greater" in {
    PreservingMaxPosition().reduce(First, pos1, con, pos2, con) should be ((pos2, con))
  }

  it should "return the first cell for the first dimension when greater" in {
    PreservingMaxPosition().reduce(First, pos2, con, pos1, con) should be ((pos2, con))
  }

  it should "return the first cell for the first dimension when equal" in {
    PreservingMaxPosition().reduce(First, pos2, con, pos2, con) should be ((pos2, con))
  }

  it should "return the first cell for the second dimension when greater" in {
    PreservingMaxPosition().reduce(Second, pos1, con, pos2, con) should be ((pos1, con))
  }

  it should "return the second cell for the second dimension when greater" in {
    PreservingMaxPosition().reduce(Second, pos2, con, pos1, con) should be ((pos1, con))
  }

  it should "return the first cell for the second dimension when equal" in {
    PreservingMaxPosition().reduce(Second, pos1, con, pos1, con) should be ((pos1, con))
  }

  it should "return the second cell for the third dimension when greater" in {
    PreservingMaxPosition().reduce(Third, pos1, con, pos2, con) should be ((pos2, con))
  }

  it should "return the first cell for the third dimension when greater" in {
    PreservingMaxPosition().reduce(Third, pos2, con, pos1, con) should be ((pos2, con))
  }

  it should "return the first cell for the third dimension when equal" in {
    PreservingMaxPosition().reduce(Third, pos2, con, pos2, con) should be ((pos2, con))
  }
}

class TestPreservingMinPosition extends FlatSpec with Matchers with TestSquashers {

  "A PreservingMinPosition" should "return the first cell for the first dimension when less" in {
    PreservingMinPosition().reduce(First, pos1, con, pos2, con) should be ((pos1, con))
  }

  it should "return the second cell for the first dimension when less" in {
    PreservingMinPosition().reduce(First, pos2, con, pos1, con) should be ((pos1, con))
  }

  it should "return the first cell for the first dimension when equal" in {
    PreservingMinPosition().reduce(First, pos1, con, pos1, con) should be ((pos1, con))
  }

  it should "return the second cell for the second dimension when less" in {
    PreservingMinPosition().reduce(Second, pos1, con, pos2, con) should be ((pos2, con))
  }

  it should "return the first cell for the second dimension when less" in {
    PreservingMinPosition().reduce(Second, pos2, con, pos1, con) should be ((pos2, con))
  }

  it should "return the first cell for the second dimension when equal" in {
    PreservingMinPosition().reduce(Second, pos2, con, pos2, con) should be ((pos2, con))
  }

  it should "return the first cell for the third dimension when less" in {
    PreservingMinPosition().reduce(Third, pos1, con, pos2, con) should be ((pos1, con))
  }

  it should "return the second cell for the third dimension when less" in {
    PreservingMinPosition().reduce(Third, pos2, con, pos1, con) should be ((pos1, con))
  }

  it should "return the first cell for the third dimension when equal" in {
    PreservingMinPosition().reduce(Third, pos1, con, pos1, con) should be ((pos1, con))
  }
}

