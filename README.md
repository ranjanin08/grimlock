grimlock
========

[![Build Status](https://travis-ci.org/CommBank/grimlock.svg?branch=master)](https://travis-ci.org/CommBank/grimlock)
[![Gitter](https://badges.gitter.im/Join Chat.svg)](https://gitter.im/CommBank/grimlock?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Overview
--------

Grimlock is a library for performing data-science and machine learning related data preparation, aggregation, manipulation and querying tasks. It can be used for such tasks as:

* Normalisation/Standardisation/Bucketing of numeric variables;
* Binarisation of categorical variables;
* Creating indicator variables;
* Computing statistics in all dimensions;
* Generating data for a variety of machine learning tools;
* Partitioning and sampling of data;
* Derived features such as gradients and moving averages;
* Text analysis (tf-idf/LDA);
* Computing pairwise distances.

Grimlock has default implementations for many of the above tasks. It also has a number of useful properties:

* Supports wide variety of variable types;
* Is easily extensible;
* Can operate in multiple dimensions (currently up to 9);
* Supports heterogeneous data;
* Can be used in the Scalding/Spark REPL;
* Supports basic as well as structured data types.

Documentation
-------------

[Scaladoc](https://commbank.github.io/grimlock/latest/api/index.html)

[Github Pages](https://commbank.github.io/grimlock/index.html)

Concepts
--------

### Data Structures

The basic data structure in grimlock is a N-dimensional sparse __Matrix__ (N=1..9). Each __Cell__ in matrix consists of a __Position__ and __Content__.

```
          Matrix
            ^ 1
            |
            | M
  Cell(Position, Content)
```

The position is, essentially, a list of N coordinates (__Value__). The content consists of a __Schema__ together with a __Value__. The value contains the actual value of the cell, while the schema defines what type of variable is in the cell, and (optionally) what it's legal values are.

```
   Position              Content
       ^ 1                  ^ 1
       |                    |
       | N           +------+------+
     Value           | 1           | 1
                  Schema         Value
```

Lastly, a __Codec__ can be used to parse and write the basic data types used in the values.

```
  Value
    ^ 1
    |
    | 1
  Codec
```

### Working with Dimensions

Grimlock supports performing operations along all directions of the matrix. This is realised through a __Slice__. There are two realisations of Slice: __Along__ and __Over__. Both are constructed with a single dimension (__shapeless.Nat__), but differ in how the dimension is interpreted. When using Over, all data in the matrix is grouped by the dimension and operations, such as aggregation, are applied to the resulting groups. When using Along, the data is group by all dimensions *except* the dimension used when constructing the Slice. The differences between Over and Along are graphically presented below for a three dimensional matrix. Note that in 2 dimensions, Along and Over are each other's inverse.

```
        Over(_2)          Along(_3)

     +----+------+      +-----------+
    /    /|     /|     /     _     /|
   /    / |    / |    /    /|_|   / |
  +----+------+  |   +-----------+  |
  |    |  |   |  |   |   /_/ /   |  |
  |    |  +   |  +   |   |_|/    |  +
  |    | /    | /    |           | /
  |    |/     |/     |           |/
  +----+------+      +----+------+
```

### Data Format

The basic data format used by grimlock (though others are supported) is a column-oriented pipe separated file (each row is a single cell). The first N fields are the coordinates, optionally followed by the variable type and codec (again pipe separated). If the variable type and codec are omitted from the data then they have to be provided by a __Dictionary__. The last field of each row is the value.

In the example below the first field is a coordinate identifying an instance, the second field is a coordinate identifying a feature. The third and fourth columns are the codec and variable type respectively. The last column has the actual value.

```
> head <path to>/grimlock/examples/src/main/scala/commbank/grimlock/data/exampleInput.txt
iid:0064402|fid:B|string|nominal|H
iid:0064402|fid:E|long|continuous|219
iid:0064402|fid:H|string|nominal|C
iid:0066848|fid:A|long|continuous|371
iid:0066848|fid:B|string|nominal|H
iid:0066848|fid:C|long|continuous|259
iid:0066848|fid:D|string|nominal|F
iid:0066848|fid:E|long|continuous|830
iid:0066848|fid:F|string|nominal|G
iid:0066848|fid:H|string|nominal|B
...
```

If the type and codec were omitted then the data would look as follows:

```
iid:0064402|fid:B|H
iid:0064402|fid:E|219
iid:0064402|fid:H|C
iid:0066848|fid:A|371
iid:0066848|fid:B|H
iid:0066848|fid:C|259
iid:0066848|fid:D|F
iid:0066848|fid:E|830
iid:0066848|fid:F|G
iid:0066848|fid:H|B
...
```

An external dictionary will then have to be provided to correctly decode and validate the values:

```
fid:A|long|continuous
fid:B|string|nominal
fid:C|long|continuous
fid:D|string|nominal
fid:E|long|continuous
fid:F|string|nominal
fid:H|string|nominal
...
```

Usage - Scalding
-----

### Setting up REPL

The examples below are executed in the Scalding REPL. Note that the Scalding REPL only works in [local](https://github.com/twitter/scalding/issues/1195) mode for scala 2.11. To use grimlock in the REPL follow the following steps:

1. Install Scalding; follow [these](https://github.com/twitter/scalding/wiki/Getting-Started) instructions.
2. Check out tag (0.13.1); git checkout 0.13.1.
3. Update `project/Build.scala` of the scalding project:
    * Update the `scalaVersion` under `sharedSettings` to `scalaVersion := "2.11.5"`;
    * For the module `scaldingRepl`, comment out `skip in compile := !isScala210x(scalaVersion.value),`;
    * For the module `scaldingRepl`, add `grimlock` as a dependency:
        + Under the `libraryDependencies` add,
            `"commbank.grimlock" %% "grimlock-core" % "<version-string>"`;
    * Optionally uncomment `test in assembly := {}`.
4. Update `project/plugins.sbt` to add the 'commbank-ext' repository.

    ```
    resolvers ++= Seq(
      "jgit-repo" at "http://download.eclipse.org/jgit/maven",
      "sonatype-releases"  at "https://oss.sonatype.org/content/repositories/releases"
      "commbank-ext" at "http://commbank.artifactoryonline.com/commbank/ext-releases-local-ivy"
    )
    ```
5. Start REPL; ./sbt scalding-repl/console.

After the last command, the console should appear as follows:

```
> ./sbt scalding-repl/console
...
[info] Starting scala interpreter...
[info] 
import com.twitter.scalding._
import com.twitter.scalding.ReplImplicits._
import com.twitter.scalding.ReplImplicitContext._
Welcome to Scala version 2.11.5 (Java HotSpot(TM) 64-Bit Server VM, Java 1.7.0_75).
Type in expressions to have them evaluated.
Type :help for more information.

scala>
```

Note, for readability, the REPL info is suppressed from now on.

### Getting started

When at the Scalding REPL console, the first step is to import grimlock's functionality (be sure to press ctrl-D after the last import statement):

```
> scala> :paste
// Entering paste mode (ctrl-D to finish)

import commbank.grimlock.framework._
import commbank.grimlock.framework.aggregate._
import commbank.grimlock.framework.content._
import commbank.grimlock.framework.position._
import commbank.grimlock.library.aggregate._
import commbank.grimlock.scalding.environment._
import commbank.grimlock.scalding.environment.Context._
import commbank.grimlock.scalding.Matrix._

import shapeless.nat.{ _0, _1, _2 }

```

Next, for convenience, set up grimlock's Context as an implicit:

```
scala> implicit val context = Context()
```

The next step is to read in data (be sure to change <path to> to the correct path to the grimlock repo):

```
scala> val (data, _) = loadText(context, "<path to>/grimlock/examples/src/main/scala/commbank/grimlock/data/exampleInput.txt", Cell.parse2D())
```

The returned `data` is a 2 dimensional matrix. To investigate it's content Scalding's `dump` command can be used in the REPL, use grimlock's `saveAsText` API for writing to disk:

```
scala> data.dump
Cell(Position(StringValue(iid:0064402,StringCodec),StringValue(fid:B,StringCodec)),Content(NominalSchema[String](),StringValue(H,StringCodec)))
Cell(Position(StringValue(iid:0064402,StringCodec),StringValue(fid:E,StringCodec)),Content(ContinuousSchema[Long](),LongValue(219,LongCodec)))
Cell(Position(StringValue(iid:0064402,StringCodec),StringValue(fid:H,StringCodec)),Content(NominalSchema[String](),StringValue(C,StringCodec)))
Cell(Position(StringValue(iid:0066848,StringCodec),StringValue(fid:A,StringCodec)),Content(ContinuousSchema[Long](),LongValue(371,LongCodec)))
Cell(Position(StringValue(iid:0066848,StringCodec),StringValue(fid:B,StringCodec)),Content(NominalSchema[String](),StringValue(H,StringCodec)))
Cell(Position(StringValue(iid:0066848,StringCodec),StringValue(fid:C,StringCodec)),Content(ContinuousSchema[Long](),LongValue(259,LongCodec)))
Cell(Position(StringValue(iid:0066848,StringCodec),StringValue(fid:D,StringCodec)),Content(NominalSchema[String](),StringValue(F,StringCodec)))
Cell(Position(StringValue(iid:0066848,StringCodec),StringValue(fid:E,StringCodec)),Content(ContinuousSchema[Long](),LongValue(830,LongCodec)))
Cell(Position(StringValue(iid:0066848,StringCodec),StringValue(fid:F,StringCodec)),Content(NominalSchema[String](),StringValue(G,StringCodec)))
Cell(Position(StringValue(iid:0066848,StringCodec),StringValue(fid:H,StringCodec)),Content(NominalSchema[String](),StringValue(B,StringCodec)))
...
```

The following shows a number of basic operations (get number of rows, get type of features, perform simple query):

```
scala> data.size(_1).dump
Cell(Position(StringValue(_1,StringCodec)),Content(DiscreteSchema[Long](),LongValue(9,LongCodec)))

scala> data.types(Over(_2))().dump
(Position(StringValue(fid:A,StringCodec)),Numerical)
(Position(StringValue(fid:B,StringCodec)),Categorical)
(Position(StringValue(fid:C,StringCodec)),Numerical)
(Position(StringValue(fid:D,StringCodec)),Categorical)
(Position(StringValue(fid:E,StringCodec)),Numerical)
(Position(StringValue(fid:F,StringCodec)),Categorical)
(Position(StringValue(fid:G,StringCodec)),Numerical)
(Position(StringValue(fid:H,StringCodec)),Categorical)

scala> data.which((cell: Cell[_2]) => (cell.content.value gtr 995) || (cell.content.value equ "F")).dump
Position(StringValue(iid:0066848,StringCodec),StringValue(fid:D,StringCodec))
Position(StringValue(iid:0216406,StringCodec),StringValue(fid:E,StringCodec))
Position(StringValue(iid:0444510,StringCodec),StringValue(fid:D,StringCodec))
```

Now for something a little more interesting. Let's compute the number of features for each instance and then compute the moments of the distribution of counts:

```
scala> val counts = data.summarise(Over(_1))(Count())

scala> counts.dump
Cell(Position(StringValue(iid:0064402,StringCodec)),Content(DiscreteSchema[Long](),LongValue(3,LongCodec)))
Cell(Position(StringValue(iid:0066848,StringCodec)),Content(DiscreteSchema[Long](),LongValue(7,LongCodec)))
Cell(Position(StringValue(iid:0216406,StringCodec)),Content(DiscreteSchema[Long](),LongValue(5,LongCodec)))
Cell(Position(StringValue(iid:0221707,StringCodec)),Content(DiscreteSchema[Long](),LongValue(4,LongCodec)))
Cell(Position(StringValue(iid:0262443,StringCodec)),Content(DiscreteSchema[Long](),LongValue(2,LongCodec)))
Cell(Position(StringValue(iid:0364354,StringCodec)),Content(DiscreteSchema[Long](),LongValue(5,LongCodec)))
Cell(Position(StringValue(iid:0375226,StringCodec)),Content(DiscreteSchema[Long](),LongValue(3,LongCodec)))
Cell(Position(StringValue(iid:0444510,StringCodec)),Content(DiscreteSchema[Long](),LongValue(5,LongCodec)))
Cell(Position(StringValue(iid:1004305,StringCodec)),Content(DiscreteSchema[Long](),LongValue(2,LongCodec)))

scala> val aggregators: List[Aggregator[_1, _0, _1]] = List(
     | Mean().andThenRelocate(_.position.append("mean").toOption),
     | StandardDeviation().andThenRelocate(_.position.append("sd").toOption),
     | Skewness().andThenRelocate(_.position.append("skewness").toOption),
     | Kurtosis().andThenRelocate(_.position.append("kurtosis").toOption))

scala> counts.summarise(Along(_1))(aggregators).dump
Cell(Position(StringValue(mean,StringCodec)),Content(ContinuousSchema[Double](),DoubleValue(4.0,DoubleCodec)))
Cell(Position(StringValue(sd,StringCodec)),Content(ContinuousSchema[Double](),DoubleValue(1.6583123951777,DoubleCodec)))
Cell(Position(StringValue(skewness,StringCodec)),Content(ContinuousSchema[Double](),DoubleValue(0.348873899490999,DoubleCodec)))
Cell(Position(StringValue(kurtosis,StringCodec)),Content(ContinuousSchema[Double](),DoubleValue(2.194214876033058,DoubleCodec)))
```

Computing the moments can also be achieved more concisely as follows:

```
scala> counts.summarise(Along(_1))(Moments(
     | _.append("mean").toOption,
     | _.append("sd").toOption,
     | _.append("skewness").toOption,
     | _.append("kurtosis").toOption)).dump

```

For more examples see [BasicOperations.scala](https://github.com/CommBank/grimlock/blob/master/examples/src/main/scala/commbank/grimlock/scalding/BasicOperations.scala), [Conditional.scala](https://github.com/CommBank/grimlock/blob/master/examples/src/main/scala/commbank/grimlock/scalding/Conditional.scala), [DataAnalysis.scala](https://github.com/CommBank/grimlock/blob/master/examples/src/main/scala/commbank/grimlock/scalding/DataAnalysis.scala), [DerivedData.scala](https://github.com/CommBank/grimlock/blob/master/examples/src/main/scala/commbank/grimlock/scalding/DerivedData.scala), [Ensemble.scala](https://github.com/CommBank/grimlock/blob/master/examples/src/main/scala/commbank/grimlock/scalding/Ensemble.scala), [Event.scala](https://github.com/CommBank/grimlock/blob/master/examples/src/main/scala/commbank/grimlock/scalding/Event.scala), [LabelWeighting.scala](https://github.com/CommBank/grimlock/blob/master/examples/src/main/scala/commbank/grimlock/scalding/LabelWeighting.scala), [MutualInformation.scala](https://github.com/CommBank/grimlock/blob/master/examples/src/main/scala/commbank/grimlock/scalding/MutualInformation.scala), [PipelineDataPreparation.scala](https://github.com/CommBank/grimlock/blob/master/examples/src/main/scala/commbank/grimlock/scalding/PipelineDataPreparation.scala) or [Scoring.scala](https://github.com/CommBank/grimlock/blob/master/examples/src/main/scala/commbank/grimlock/scalding/Scoring.scala).

Usage - Spark
-----

### Setting up REPL

The examples below are executed in the Spark REPL. To use grimlock in the REPL follow the following steps:

1. Download the latest source code release for Spark from [here](http://spark.apache.org/downloads.html).
2. Compile Spark for Scala 2.11, see [here](http://spark.apache.org/docs/latest/building-spark.html#building-for-scala-211). Note that if `mvn` (maven) is not installed then one might be able to use the `build/mvn` binary in the build directory.
3. You can, optionally, suppress much of the console INFO output. Follow [these](http://stackoverflow.com/questions/28189408/how-to-reduce-the-verbosity-of-sparks-runtime-output) instructions.
4. Start REPL; ./bin/spark-shell --master local --jars <path to>/grimlock.jar

After the last command, the console should appear as follows:

```
> ./bin/spark-shell --master local --jars <path to>/grimlock.jar
...
Spark context available as sc.
SQL context available as sqlContext.
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 1.6.2
      /_/

Using Scala version 2.11.7 (OpenJDK 64-Bit Server VM, Java 1.7.0_101)
Type in expressions to have them evaluated.
Type :help for more information.

scala>
```

Note, for readability, the REPL info is suppressed from now on.

### Getting started

When at the Spark REPL console, the first step is to import grimlock's functionality (be sure to press ctrl-D after the last import statement):

```
> scala> :paste
// Entering paste mode (ctrl-D to finish)

import commbank.grimlock.framework._
import commbank.grimlock.framework.aggregate._
import commbank.grimlock.framework.content._
import commbank.grimlock.framework.position._
import commbank.grimlock.library.aggregate._
import commbank.grimlock.spark.environment._
import commbank.grimlock.spark.environment.Context._
import commbank.grimlock.spark.Matrix._

import shapeless.nat.{ _0, _1, _2 }

```

Next, for convenience, set up grimlock's Context as an implicit:

```
scala> implicit val context = Context(sc)
```

The next step is to read in data (be sure to change <path to> to the correct path to the grimlock repo):

```
scala> val (data, _) = loadText(context, "<path to>/grimlock/examples/src/main/scala/commbank/grimlock/data/exampleInput.txt", Cell.parse2D())
```

The returned `data` is a 2 dimensional matrix. To investigate it's content Spark's `foreach` command can be used in the REPL, use the grimlock's `saveAsText` API for writing to disk:

```
scala> data.foreach(println)
Cell(Position(StringValue(iid:0064402,StringCodec),StringValue(fid:B,StringCodec)),Content(NominalSchema[String](),StringValue(H,StringCodec)))
Cell(Position(StringValue(iid:0064402,StringCodec),StringValue(fid:E,StringCodec)),Content(ContinuousSchema[Long](),LongValue(219,LongCodec)))
Cell(Position(StringValue(iid:0064402,StringCodec),StringValue(fid:H,StringCodec)),Content(NominalSchema[String](),StringValue(C,StringCodec)))
Cell(Position(StringValue(iid:0066848,StringCodec),StringValue(fid:A,StringCodec)),Content(ContinuousSchema[Long](),LongValue(371,LongCodec)))
Cell(Position(StringValue(iid:0066848,StringCodec),StringValue(fid:B,StringCodec)),Content(NominalSchema[String](),StringValue(H,StringCodec)))
Cell(Position(StringValue(iid:0066848,StringCodec),StringValue(fid:C,StringCodec)),Content(ContinuousSchema[Long](),LongValue(259,LongCodec)))
Cell(Position(StringValue(iid:0066848,StringCodec),StringValue(fid:D,StringCodec)),Content(NominalSchema[String](),StringValue(F,StringCodec)))
Cell(Position(StringValue(iid:0066848,StringCodec),StringValue(fid:E,StringCodec)),Content(ContinuousSchema[Long](),LongValue(830,LongCodec)))
Cell(Position(StringValue(iid:0066848,StringCodec),StringValue(fid:F,StringCodec)),Content(NominalSchema[String](),StringValue(G,StringCodec)))
Cell(Position(StringValue(iid:0066848,StringCodec),StringValue(fid:H,StringCodec)),Content(NominalSchema[String](),StringValue(B,StringCodec)))
...
```

The following shows a number of basic operations (get number of rows, get type of features, perform simple query):

```
scala> data.size(_1).foreach(println)
Cell(Position(StringValue(_1,StringCodec)),Content(DiscreteSchema[Long](),LongValue(9,LongCodec)))

scala> data.types(Over(_2))().foreach(println)
(Position(StringValue(fid:A,StringCodec)),Numerical)
(Position(StringValue(fid:B,StringCodec)),Categorical)
(Position(StringValue(fid:C,StringCodec)),Numerical)
(Position(StringValue(fid:D,StringCodec)),Categorical)
(Position(StringValue(fid:E,StringCodec)),Numerical)
(Position(StringValue(fid:F,StringCodec)),Categorical)
(Position(StringValue(fid:G,StringCodec)),Numerical)
(Position(StringValue(fid:H,StringCodec)),Categorical)

scala> data.which((cell: Cell[_2]) => (cell.content.value gtr 995) || (cell.content.value equ "F")).foreach(println)
Position(StringValue(iid:0066848,StringCodec),StringValue(fid:D,StringCodec))
Position(StringValue(iid:0216406,StringCodec),StringValue(fid:E,StringCodec))
Position(StringValue(iid:0444510,StringCodec),StringValue(fid:D,StringCodec))
```

Now for something a little more interesting. Let's compute the number of features for each instance and then compute the moments of the distribution of counts:

```
scala> val counts = data.summarise(Over(_1))(Count())

scala> counts.foreach(println)
Cell(Position(StringValue(iid:0064402,StringCodec)),Content(DiscreteSchema[Long](),LongValue(3,LongCodec)))
Cell(Position(StringValue(iid:0066848,StringCodec)),Content(DiscreteSchema[Long](),LongValue(7,LongCodec)))
Cell(Position(StringValue(iid:0216406,StringCodec)),Content(DiscreteSchema[Long](),LongValue(5,LongCodec)))
Cell(Position(StringValue(iid:0221707,StringCodec)),Content(DiscreteSchema[Long](),LongValue(4,LongCodec)))
Cell(Position(StringValue(iid:0262443,StringCodec)),Content(DiscreteSchema[Long](),LongValue(2,LongCodec)))
Cell(Position(StringValue(iid:0364354,StringCodec)),Content(DiscreteSchema[Long](),LongValue(5,LongCodec)))
Cell(Position(StringValue(iid:0375226,StringCodec)),Content(DiscreteSchema[Long](),LongValue(3,LongCodec)))
Cell(Position(StringValue(iid:0444510,StringCodec)),Content(DiscreteSchema[Long](),LongValue(5,LongCodec)))
Cell(Position(StringValue(iid:1004305,StringCodec)),Content(DiscreteSchema[Long](),LongValue(2,LongCodec)))

scala> val aggregators: List[Aggregator[_1, _0, _1]] = List(
     | Mean().andThenRelocate(_.position.append("mean").toOption),
     | StandardDeviation().andThenRelocate(_.position.append("sd").toOption),
     | Skewness().andThenRelocate(_.position.append("skewness").toOption),
     | Kurtosis().andThenRelocate(_.position.append("kurtosis").toOption))

scala> counts.summarise(Along(_1))(aggregators).foreach(println)
Cell(Position(StringValue(mean,StringCodec)),Content(ContinuousSchema[Double](),DoubleValue(4.0,DoubleCodec)))
Cell(Position(StringValue(sd,StringCodec)),Content(ContinuousSchema[Double](),DoubleValue(1.6583123951777,DoubleCodec)))
Cell(Position(StringValue(skewness,StringCodec)),Content(ContinuousSchema[Double](),DoubleValue(0.348873899490999,DoubleCodec)))
Cell(Position(StringValue(kurtosis,StringCodec)),Content(ContinuousSchema[Double](),DoubleValue(2.194214876033058,DoubleCodec)))
```

Computing the moments can also be achieved more concisely as follows:

```
scala> counts.summarise(Along(_1))(Moments(
     | _.append("mean").toOption,
     | _.append("sd").toOption,
     | _.append("skewness").toOption,
     | _.append("kurtosis").toOption)).foreach(println)
```

For more examples see [BasicOperations.scala](https://github.com/CommBank/grimlock/blob/master/examples/src/main/scala/commbank/grimlock/spark/BasicOperations.scala), [Conditional.scala](https://github.com/CommBank/grimlock/blob/master/examples/src/main/scala/commbank/grimlock/spark/Conditional.scala), [DataAnalysis.scala](https://github.com/CommBank/grimlock/blob/master/examples/src/main/scala/commbank/grimlock/spark/DataAnalysis.scala), [DerivedData.scala](https://github.com/CommBank/grimlock/blob/master/examples/src/main/scala/commbank/grimlock/spark/DerivedData.scala), [Ensemble.scala](https://github.com/CommBank/grimlock/blob/master/examples/src/main/scala/commbank/grimlock/spark/Ensemble.scala), [Event.scala](https://github.com/CommBank/grimlock/blob/master/examples/src/main/scala/commbank/grimlock/spark/Event.scala), [LabelWeighting.scala](https://github.com/CommBank/grimlock/blob/master/examples/src/main/scala/commbank/grimlock/spark/LabelWeighting.scala), [MutualInformation.scala](https://github.com/CommBank/grimlock/blob/master/examples/src/main/scala/commbank/grimlock/spark/MutualInformation.scala), [PipelineDataPreparation.scala](https://github.com/CommBank/grimlock/blob/master/examples/src/main/scala/commbank/grimlock/spark/PipelineDataPreparation.scala) or [Scoring.scala](https://github.com/CommBank/grimlock/blob/master/examples/src/main/scala/commbank/grimlock/spark/Scoring.scala).

Acknowledgement
---------------
We would like to thank the YourKit team for their support in providing us with their excellent [Java Profiler](https://www.yourkit.com/java/profiler/index.jsp). ![YourKit Logo](yk_logo.png)

