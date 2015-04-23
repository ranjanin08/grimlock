Grimlock
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
* Can operate in multiple dimensions (currently up to 5);
* Supports hetrogeneous data;
* Can be used in the Scalding REPL (with simple symlink);
* Supports basic as well as structured data types.

Concepts
--------

### Data Structures

The basic data structure in Grimlock is a N-dimensional sparse __Matrix__ (N=1..5). Each cell in matrix consists of a __Position__ and __Content__ tuple.

```
         Matrix
           ^ 1
           |
           | M
  (Position, Content)
```

The position is, essentialy, a list of N coordinates (__Value__). The content consists of a __Schema__ together with a __Value__. The value contains the actual value of the cell, while the schema defines what type of variable is in the cell, and what it's legal values are.

```
   Position              Content
       ^ 1                  ^ 1
       |                    |
       | N           +------+------+
     Value           | 1           | 1
                  Schema         Value
```

Lastly, the __Codex__ singleton objects can be used to parse and write the basic data types used in both coordinates and values.

```
  Schema       Value
     ^ 1         ^ 1
     |           |
     | 1         | 1
   Codex       Codex
```

### Working with Dimensions

Grimlock supports performing operations along all directions of the matrix.  This is realised through a __Slice__. There are two realisations of Slice: __Along__ and __Over__. Both are constructed with a single __Dimension__, but differ in how the dimension is interpreted. When using Over, all data in the matrix is grouped by the dimension and operations, such as aggregation, are applied to the resulting groups. When using Along, the data is group by all dimensions *except* the dimension used when constructing the Slice. The differences between Over and Along are graphically presented below for a dimensional matrix. Note that in 2 dimensions, Along and Over are each other's inverse.

```
      Over(Second)       Along(Third)

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

The basic data format used by Grimlock (though others are supported) is a row-oriented pipe separated file (each row is a single cell). The first N fields are the coordinates, optionally followed by the variable type and codex (again pipe separated). If the variable type and codex are omitted from the data then they have to be provided by a __Dictionary__. The last field of each row is the value.

In the example below the first field is a coordinate identifying an instance, the second field is a coordinate identifying a feature. The third and fourth columns are the variable type and codex respectively. The last column has the actual value.

```
> head src/main/scala/au/com/cba/omnia/grimlock/examples/exampleInput.txt
iid:0064402|fid:B|nominal|string|H
iid:0064402|fid:E|continuous|long|219
iid:0064402|fid:H|nominal|string|C
iid:0066848|fid:A|continuous|long|371
iid:0066848|fid:B|nominal|string|H
iid:0066848|fid:C|continuous|long|259
iid:0066848|fid:D|nominal|string|F
iid:0066848|fid:E|continuous|long|830
iid:0066848|fid:F|nominal|string|G
iid:0066848|fid:H|nominal|string|B
...
```

If the type and codex were omitted then the data would look as follows:

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

Usage
-----

### Setting up REPL

The examples below are executed in the Scalding REPL. To use Grimlock in the REPL follow the following steps:

1. Install Scalding; follow [these](https://github.com/twitter/scalding/wiki/Getting-Started) instructions.
2. Check out tag (0.12.0); git checkout 0.12.0.
3. In scalding-repl/src/main/scala add symlinks to Grimlock's framework, library and scalding folders.
4. Start REPL; ./sbt scalding-repl/console.

After the last command, the console should appear as follows:

```
> ./sbt scalding-repl/console
[info] Loading project definition from <path to>/scalding/project
[info] Set current project to scalding (in build file:<path to>/scalding/)
[info] Formatting 2 Scala sources {file:<path to>/scalding/}scalding-repl(compile) ...
[info] Compiling 2 Scala sources to <path to>/scalding/scalding-repl/target/scala-2.10/classes...
[warn] there were 7 feature warning(s); re-run with -feature for details
[warn] one warning found
[info] Starting scala interpreter...
[info] 
import com.twitter.scalding._
import com.twitter.scalding.ReplImplicits._
import com.twitter.scalding.ReplImplicitContext._
Welcome to Scala version 2.10.4 (Java HotSpot(TM) 64-Bit Server VM, Java 1.6.0_65).
Type in expressions to have them evaluated.
Type :help for more information.

scala>
```

Note, for readability, the REPL info is supressed from now on.

### Getting started

When at the Scalding REPL console, the first step is to import Grimlock's functionality (be sure to press ctrl-D after the last import statement):

```
> scala> :paste
// Entering paste mode (ctrl-D to finish)

import au.com.cba.omnia.grimlock.framework._
import au.com.cba.omnia.grimlock.framework.content._
import au.com.cba.omnia.grimlock.framework.position._
import au.com.cba.omnia.grimlock.framework.reduce._
import au.com.cba.omnia.grimlock.library.reduce._
import au.com.cba.omnia.grimlock.scalding.Matrix._

```

The next step is to read in data (be sure to change <path to> to the correct path to the Grimlock repo):

```
scala> val data = read2D("<path to>/grimlock/src/main/scala/au/com/cba/omnia/grimlock/examples/exampleInput.txt")
```

The returned `data` is a 2 dimensional matrix. To investigate it's content Scalding's `dump` command can be used in the REPL, use the matrix `save` API for writing to disk:

```
scala> data.dump
Cell(Position2D(StringValue(iid:0064402),StringValue(fid:B)),Content(NominalSchema[StringCodex](),StringValue(H)))
Cell(Position2D(StringValue(iid:0064402),StringValue(fid:E)),Content(ContinuousSchema[LongCodex](),LongValue(219)))
Cell(Position2D(StringValue(iid:0064402),StringValue(fid:H)),Content(NominalSchema[StringCodex](),StringValue(C)))
Cell(Position2D(StringValue(iid:0066848),StringValue(fid:A)),Content(ContinuousSchema[LongCodex](),LongValue(371)))
Cell(Position2D(StringValue(iid:0066848),StringValue(fid:B)),Content(NominalSchema[StringCodex](),StringValue(H)))
Cell(Position2D(StringValue(iid:0066848),StringValue(fid:C)),Content(ContinuousSchema[LongCodex](),LongValue(259)))
Cell(Position2D(StringValue(iid:0066848),StringValue(fid:D)),Content(NominalSchema[StringCodex](),StringValue(F)))
Cell(Position2D(StringValue(iid:0066848),StringValue(fid:E)),Content(ContinuousSchema[LongCodex](),LongValue(830)))
Cell(Position2D(StringValue(iid:0066848),StringValue(fid:F)),Content(NominalSchema[StringCodex](),StringValue(G)))
Cell(Position2D(StringValue(iid:0066848),StringValue(fid:H)),Content(NominalSchema[StringCodex](),StringValue(B)))
...
```

The following shows a number of basic operations (get number of rows, get type of features, perform simple query):

```
scala> data.size(First).dump
Cell(Position1D(StringValue(First)),Content(DiscreteSchema[LongCodex](),LongValue(9)))

scala> data.types(Over(Second)).dump
(Position1D(StringValue(fid:A)),Numerical)
(Position1D(StringValue(fid:B)),Categorical)
(Position1D(StringValue(fid:C)),Numerical)
(Position1D(StringValue(fid:D)),Categorical)
(Position1D(StringValue(fid:E)),Numerical)
(Position1D(StringValue(fid:F)),Categorical)
(Position1D(StringValue(fid:G)),Numerical)
(Position1D(StringValue(fid:H)),Categorical)

scala> data.which((cell: Cell[Position2D]) => (cell.content.value gtr 995) || (cell.content.value equ "F")).dump
Position2D(StringValue(iid:0066848),StringValue(fid:D))
Position2D(StringValue(iid:0216406),StringValue(fid:E))
Position2D(StringValue(iid:0444510),StringValue(fid:D))
```

Now for something a little more intersting. Let's compute the number of features for each instance and then compute the moments of the distribution of counts:

```
scala> val counts = data.reduce(Over(First), Count())

scala> counts.dump
Cell(Position1D(StringValue(iid:0064402)),Content(DiscreteSchema[LongCodex](),LongValue(3)))
Cell(Position1D(StringValue(iid:0066848)),Content(DiscreteSchema[LongCodex](),LongValue(7)))
Cell(Position1D(StringValue(iid:0216406)),Content(DiscreteSchema[LongCodex](),LongValue(5)))
Cell(Position1D(StringValue(iid:0221707)),Content(DiscreteSchema[LongCodex](),LongValue(4)))
Cell(Position1D(StringValue(iid:0262443)),Content(DiscreteSchema[LongCodex](),LongValue(2)))
Cell(Position1D(StringValue(iid:0364354)),Content(DiscreteSchema[LongCodex](),LongValue(5)))
Cell(Position1D(StringValue(iid:0375226)),Content(DiscreteSchema[LongCodex](),LongValue(3)))
Cell(Position1D(StringValue(iid:0444510)),Content(DiscreteSchema[LongCodex](),LongValue(5)))
Cell(Position1D(StringValue(iid:1004305)),Content(DiscreteSchema[LongCodex](),LongValue(2)))

scala> counts.reduceAndExpand(Along(First), Moments("mean", "sd", "skewness", "kurtosis")).dump
Cell(Position1D(StringValue(mean)),Content(ContinuousSchema[DoubleCodex](),DoubleValue(4.0,DoubleCodex)))
Cell(Position1D(StringValue(sd)),Content(ContinuousSchema[DoubleCodex](),DoubleValue(1.5634719199411433,DoubleCodex)))
Cell(Position1D(StringValue(skewness)),Content(ContinuousSchema[DoubleCodex](),DoubleValue(0.348873899490999,DoubleCodex)))
Cell(Position1D(StringValue(kurtosis)),Content(ContinuousSchema[DoubleCodex](),DoubleValue(-0.8057851239669427,DoubleCodex)))
```

For more examples see [Demo.scala](https://github.com/CommBank/grimlock/blob/master/src/main/scala/au/com/cba/omnia/grimlock/examples/Demo.scala), [Event.scala](https://github.com/CommBank/grimlock/blob/master/src/main/scala/au/com/cba/omnia/grimlock/examples/Event.scala), [MutualInformation.scala](https://github.com/CommBank/grimlock/blob/master/src/main/scala/au/com/cba/omnia/grimlock/examples/MutualInformation.scala) or [DerivedData.scala](https://github.com/CommBank/grimlock/blob/master/src/main/scala/au/com/cba/omnia/grimlock/examples/DerivedData.scala).

Documentation
-------------

[Scaladoc](https://commbank.github.io/grimlock/latest/api/index.html)

