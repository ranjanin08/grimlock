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

package au.com.cba.omnia.grimlock.framework.position

/** Base trait for dimensions of a matrix. */
sealed trait Dimension {
  /** Index of the dimension (starting at 0). */
  val index: Int
}

/** Type for first dimension. */
case object First extends Dimension { val index = 0 }
/** Type for second dimension. */
case object Second extends Dimension { val index = 1 }
/** Type for third dimension. */
case object Third extends Dimension { val index = 2 }
/** Type for fourth dimension. */
case object Fourth extends Dimension { val index = 3 }
/** Type for fifth dimension. */
case object Fifth extends Dimension { val index = 4 }
/** Type for sixth dimension. */
case object Sixth extends Dimension { val index = 5 }
/** Type for seventh dimension. */
case object Seventh extends Dimension { val index = 6 }
/** Type for eighth dimension. */
case object Eighth extends Dimension { val index = 7 }
/** Type for ninth dimension. */
case object Ninth extends Dimension { val index = 8 }
/** Type for last dimension. */
case object Last extends Dimension { val index = -1 }

/** Trait for capturing the dependency between a position and a dimension. */
trait PosDimDep[A <: Position, B <: Dimension] extends java.io.Serializable

/**
 * Companion object of `Dimension` trait. This defines the (implicit) permitted dependencies between position and
 * dimension.
 */
object Dimension {
  /** List of all available dimensions ordered by index. */
  val All = List(First, Second, Third, Fourth, Fifth, Sixth, Seventh, Eighth, Ninth)

  /** Define dependency between `Position1D` and `First`. */
  implicit object P1D1 extends PosDimDep[Position1D, First.type]
  /** Define dependency between `Position1D` and `Last`. */
  implicit object P1DL extends PosDimDep[Position1D, Last.type]

  /** Define dependency between `Position2D` and `First`. */
  implicit object P2D1 extends PosDimDep[Position2D, First.type]
  /** Define dependency between `Position2D` and `Second`. */
  implicit object P2D2 extends PosDimDep[Position2D, Second.type]
  /** Define dependency between `Position2D` and `Last`. */
  implicit object P2DL extends PosDimDep[Position2D, Last.type]

  /** Define dependency between `Position3D` and `First`. */
  implicit object P3D1 extends PosDimDep[Position3D, First.type]
  /** Define dependency between `Position3D` and `Second`. */
  implicit object P3D2 extends PosDimDep[Position3D, Second.type]
  /** Define dependency between `Position3D` and `Third`. */
  implicit object P3D3 extends PosDimDep[Position3D, Third.type]
  /** Define dependency between `Position3D` and `Last`. */
  implicit object P3DL extends PosDimDep[Position3D, Last.type]

  /** Define dependency between `Position4D` and `First`. */
  implicit object P4D1 extends PosDimDep[Position4D, First.type]
  /** Define dependency between `Position4D` and `Second`. */
  implicit object P4D2 extends PosDimDep[Position4D, Second.type]
  /** Define dependency between `Position4D` and `Third`. */
  implicit object P4D3 extends PosDimDep[Position4D, Third.type]
  /** Define dependency between `Position4D` and `Fourth`. */
  implicit object P4D4 extends PosDimDep[Position4D, Fourth.type]
  /** Define dependency between `Position4D` and `Last`. */
  implicit object P4DL extends PosDimDep[Position4D, Last.type]

  /** Define dependency between `Position5D` and `First`. */
  implicit object P5D1 extends PosDimDep[Position5D, First.type]
  /** Define dependency between `Position5D` and `Second`. */
  implicit object P5D2 extends PosDimDep[Position5D, Second.type]
  /** Define dependency between `Position5D` and `Third`. */
  implicit object P5D3 extends PosDimDep[Position5D, Third.type]
  /** Define dependency between `Position5D` and `Fourth`. */
  implicit object P5D4 extends PosDimDep[Position5D, Fourth.type]
  /** Define dependency between `Position5D` and `Fifth`. */
  implicit object P5D5 extends PosDimDep[Position5D, Fifth.type]
  /** Define dependency between `Position5D` and `Last`. */
  implicit object P5DL extends PosDimDep[Position5D, Last.type]

  /** Define dependency between `Position6D` and `First`. */
  implicit object P6D1 extends PosDimDep[Position6D, First.type]
  /** Define dependency between `Position6D` and `Second`. */
  implicit object P6D2 extends PosDimDep[Position6D, Second.type]
  /** Define dependency between `Position6D` and `Third`. */
  implicit object P6D3 extends PosDimDep[Position6D, Third.type]
  /** Define dependency between `Position6D` and `Fourth`. */
  implicit object P6D4 extends PosDimDep[Position6D, Fourth.type]
  /** Define dependency between `Position6D` and `Fifth`. */
  implicit object P6D5 extends PosDimDep[Position6D, Fifth.type]
  /** Define dependency between `Position6D` and `Sixth`. */
  implicit object P6D6 extends PosDimDep[Position6D, Sixth.type]
  /** Define dependency between `Position6D` and `Last`. */
  implicit object P6DL extends PosDimDep[Position6D, Last.type]

  /** Define dependency between `Position7D` and `First`. */
  implicit object P7D1 extends PosDimDep[Position7D, First.type]
  /** Define dependency between `Position7D` and `Second`. */
  implicit object P7D2 extends PosDimDep[Position7D, Second.type]
  /** Define dependency between `Position7D` and `Third`. */
  implicit object P7D3 extends PosDimDep[Position7D, Third.type]
  /** Define dependency between `Position7D` and `Fourth`. */
  implicit object P7D4 extends PosDimDep[Position7D, Fourth.type]
  /** Define dependency between `Position7D` and `Fifth`. */
  implicit object P7D5 extends PosDimDep[Position7D, Fifth.type]
  /** Define dependency between `Position7D` and `Sixth`. */
  implicit object P7D6 extends PosDimDep[Position7D, Sixth.type]
  /** Define dependency between `Position7D` and `Seventh`. */
  implicit object P7D7 extends PosDimDep[Position7D, Seventh.type]
  /** Define dependency between `Position7D` and `Last`. */
  implicit object P7DL extends PosDimDep[Position7D, Last.type]

  /** Define dependency between `Position8D` and `First`. */
  implicit object P8D1 extends PosDimDep[Position8D, First.type]
  /** Define dependency between `Position8D` and `Second`. */
  implicit object P8D2 extends PosDimDep[Position8D, Second.type]
  /** Define dependency between `Position8D` and `Third`. */
  implicit object P8D3 extends PosDimDep[Position8D, Third.type]
  /** Define dependency between `Position8D` and `Fourth`. */
  implicit object P8D4 extends PosDimDep[Position8D, Fourth.type]
  /** Define dependency between `Position8D` and `Fifth`. */
  implicit object P8D5 extends PosDimDep[Position8D, Fifth.type]
  /** Define dependency between `Position8D` and `Sixth`. */
  implicit object P8D6 extends PosDimDep[Position8D, Sixth.type]
  /** Define dependency between `Position8D` and `Seventh`. */
  implicit object P8D7 extends PosDimDep[Position8D, Seventh.type]
  /** Define dependency between `Position8D` and `Eighth`. */
  implicit object P8D8 extends PosDimDep[Position8D, Eighth.type]
  /** Define dependency between `Position8D` and `Last`. */
  implicit object P8DL extends PosDimDep[Position8D, Last.type]

  /** Define dependency between `Position9D` and `First`. */
  implicit object P9D1 extends PosDimDep[Position9D, First.type]
  /** Define dependency between `Position9D` and `Second`. */
  implicit object P9D2 extends PosDimDep[Position9D, Second.type]
  /** Define dependency between `Position9D` and `Third`. */
  implicit object P9D3 extends PosDimDep[Position9D, Third.type]
  /** Define dependency between `Position9D` and `Fourth`. */
  implicit object P9D4 extends PosDimDep[Position9D, Fourth.type]
  /** Define dependency between `Position9D` and `Fifth`. */
  implicit object P9D5 extends PosDimDep[Position9D, Fifth.type]
  /** Define dependency between `Position9D` and `Sixth`. */
  implicit object P9D6 extends PosDimDep[Position9D, Sixth.type]
  /** Define dependency between `Position9D` and `Seventh`. */
  implicit object P9D7 extends PosDimDep[Position9D, Seventh.type]
  /** Define dependency between `Position9D` and `Eighth`. */
  implicit object P9D8 extends PosDimDep[Position9D, Eighth.type]
  /** Define dependency between `Position9D` and `Ninth`. */
  implicit object P9D9 extends PosDimDep[Position9D, Ninth.type]
  /** Define dependency between `Position9D` and `Last`. */
  implicit object P9DL extends PosDimDep[Position9D, Last.type]
}

