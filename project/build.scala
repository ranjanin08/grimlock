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

import sbt._, Keys._
import sbtassembly.AssemblyKeys._

import au.com.cba.omnia.uniform.assembly.UniformAssemblyPlugin._
import au.com.cba.omnia.uniform.core.standard.StandardProjectPlugin.uniform
import au.com.cba.omnia.uniform.dependency.UniformDependencyPlugin._
import au.com.cba.omnia.uniform.thrift.UniformThriftPlugin._

object build extends Build {
  lazy val grimlock = Project(
                              id       = "grimlock",
                              base     = file("."),
                              settings = uniform.project("grimlock", "au.com.cba.omnia.grimlock")
                                          ++ uniformDependencySettings
                                          ++ uniformThriftSettings
                                          ++ strictDependencySettings
                                          ++ dependencies
                                          ++ uniformAssemblySettings
                                          ++ uniform.docSettings("https://github.com/CommBank/grimlock")
                                          ++ uniform.ghsettings
                                          ++ Seq(
                                              test in assembly          :=  {},
                                              parallelExecution in Test := false
                                            )
                              )

  lazy val dependencies: List[Setting[_]] = List(libraryDependencies <++=
    scalaVersion.apply(scalaVersion => {
     (depend.hadoopClasspath
        ++ depend.scalding()
        ++ depend.parquet()
        ++ depend.omnia("ebenezer","0.18.5-20150814060245-db099a2")
        ++ Seq(
            noHadoop("com.twitter"        % "parquet-scrooge"     % depend.versions.parquet)
              exclude("com.twitter", "scrooge-core_2.9.2"),
            noHadoop("org.apache.spark"   %% "spark-core"         % "1.5.0")
              exclude("com.twitter", "chill-java")
              exclude("com.twitter", "chill_2.11"),
            "org.scalatest"               %% "scalatest"          % "2.2.4" % "test"
     )
      )
    }),
    dependencyOverrides ++= Set(
      "org.scala-lang.modules" %% "scala-parser-combinators"      % "1.0.3",
      "org.scala-lang.modules" %% "scala-xml"                     % "1.0.3"
    )
  )
}

