#!/bin/bash
#
# Copyright 2014-2015 Commonwealth Bank of Australia
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -vx

JAR=grimlock.jar
NUM_TEST=29
DO_BUILD=true
DO_CLEANUP=true
DO_INIT=false
DO_LOCAL=true
DO_CLUSTER=false
DO_DEMO=true
DO_TEST=true
BASE_DIR="../../../../../../../../.."

if [ ${DO_BUILD} = "true" ]
then
  rm -f ${JAR}
  cd ${BASE_DIR}; ./sbt clean assembly; cd -
  cp ${BASE_DIR}/target/scala-2.10/grimlock*.jar ${JAR}
fi

if [ ${DO_DEMO} = "true" ]
then
  if [ ${DO_LOCAL} = "true" ]
  then
    if [ ${DO_INIT} = "true" ]
    then
      mkdir -p demo.scalding
    fi

    if [ ${DO_CLEANUP} = "true" ]
    then
      rm -rf demo.scalding/*
    fi

    export HADOOP_OPTS="-Dsun.io.serialization.extendedDebugInfo=true"; hadoop jar $JAR com.twitter.scalding.Tool \
      au.com.cba.omnia.grimlock.scalding.examples.BasicOperations --local --path ../data
    export HADOOP_OPTS="-Dsun.io.serialization.extendedDebugInfo=true"; hadoop jar $JAR com.twitter.scalding.Tool \
      au.com.cba.omnia.grimlock.scalding.examples.DataSciencePipelineWithFiltering --local --path ../data
    export HADOOP_OPTS="-Dsun.io.serialization.extendedDebugInfo=true"; hadoop jar $JAR com.twitter.scalding.Tool \
      au.com.cba.omnia.grimlock.scalding.examples.Scoring --local --path ../data
    export HADOOP_OPTS="-Dsun.io.serialization.extendedDebugInfo=true"; hadoop jar $JAR com.twitter.scalding.Tool \
      au.com.cba.omnia.grimlock.scalding.examples.DataQualityAndAnalysis --local --path ../data
    export HADOOP_OPTS="-Dsun.io.serialization.extendedDebugInfo=true"; hadoop jar $JAR com.twitter.scalding.Tool \
      au.com.cba.omnia.grimlock.scalding.examples.LabelWeighting --local --path ../data
    export HADOOP_OPTS="-Dsun.io.serialization.extendedDebugInfo=true"; hadoop jar $JAR com.twitter.scalding.Tool \
      au.com.cba.omnia.grimlock.scalding.examples.InstanceCentricTfIdf --local --path ../data
    export HADOOP_OPTS="-Dsun.io.serialization.extendedDebugInfo=true"; hadoop jar $JAR com.twitter.scalding.Tool \
      au.com.cba.omnia.grimlock.scalding.examples.MutualInformation --local --path ../data
    export HADOOP_OPTS="-Dsun.io.serialization.extendedDebugInfo=true"; hadoop jar $JAR com.twitter.scalding.Tool \
      au.com.cba.omnia.grimlock.scalding.examples.DerivedData --local --path ../data

    if [ -d "demo.old" ]
    then
      diff -r demo.scalding demo.old
    fi
  fi

  if [ ${DO_CLUSTER} = "true" ]
  then
    if [ ${DO_INIT} = "true" ]
    then
      hadoop fs -mkdir -p demo.scalding

      for f in $(ls ../data/*.txt)
      do
        hadoop fs -put $f
      done
    fi

    if [ ${DO_CLEANUP} = "true" ]
    then
      hadoop fs -rm -r -f 'demo.scalding/*'
    fi

    export HADOOP_OPTS="-Dsun.io.serialization.extendedDebugInfo=true"; hadoop jar $JAR com.twitter.scalding.Tool \
      au.com.cba.omnia.grimlock.scalding.examples.BasicOperations --hdfs --path .
    export HADOOP_OPTS="-Dsun.io.serialization.extendedDebugInfo=true"; hadoop jar $JAR com.twitter.scalding.Tool \
      au.com.cba.omnia.grimlock.scalding.examples.DataSciencePipelineWithFiltering --hdfs --path .
    export HADOOP_OPTS="-Dsun.io.serialization.extendedDebugInfo=true"; hadoop jar $JAR com.twitter.scalding.Tool \
      au.com.cba.omnia.grimlock.scalding.examples.Scoring --hdfs --path .
    export HADOOP_OPTS="-Dsun.io.serialization.extendedDebugInfo=true"; hadoop jar $JAR com.twitter.scalding.Tool \
      au.com.cba.omnia.grimlock.scalding.examples.DataQualityAndAnalysis --hdfs --path .
    export HADOOP_OPTS="-Dsun.io.serialization.extendedDebugInfo=true"; hadoop jar $JAR com.twitter.scalding.Tool \
      au.com.cba.omnia.grimlock.scalding.examples.LabelWeighting --hdfs --path .
    export HADOOP_OPTS="-Dsun.io.serialization.extendedDebugInfo=true"; hadoop jar $JAR com.twitter.scalding.Tool \
      au.com.cba.omnia.grimlock.scalding.examples.InstanceCentricTfIdf --hdfs --path .
    export HADOOP_OPTS="-Dsun.io.serialization.extendedDebugInfo=true"; hadoop jar $JAR com.twitter.scalding.Tool \
      au.com.cba.omnia.grimlock.scalding.examples.MutualInformation --hdfs --path .
    export HADOOP_OPTS="-Dsun.io.serialization.extendedDebugInfo=true"; hadoop jar $JAR com.twitter.scalding.Tool \
      au.com.cba.omnia.grimlock.scalding.examples.DerivedData --hdfs --path .
  fi
fi

if [ ${DO_TEST} = "true" ]
then
  if [ ${DO_LOCAL} = "true" ]
  then
    if [ ${DO_INIT} = "true" ]
    then
      mkdir -p tmp.scalding
    fi

    if [ ${DO_CLEANUP} = "true" ]
    then
      rm -rf tmp.scalding/*
    fi

    for i in $(seq 1 ${NUM_TEST})
    do
      export HADOOP_OPTS="-Dsun.io.serialization.extendedDebugInfo=true"; hadoop jar $JAR com.twitter.scalding.Tool \
        au.com.cba.omnia.grimlock.test.TestScalding${i} --local --input "someInputfile3.txt"
    done

    if [ -d "tmp.old" ]
    then
      diff -r tmp.scalding tmp.old
    fi
  fi

  if [ ${DO_CLUSTER} = "true" ]
  then
    if [ ${DO_INIT} = "true" ]
    then
      hadoop fs -mkdir -p tmp.scalding

      for f in $(ls *.txt)
      do
        hadoop fs -put $f
      done
    fi

    if [ ${DO_CLEANUP} = "true" ]
    then
      hadoop fs -rm -r -f 'tmp.scalding/*'
    fi

    for i in $(seq 1 ${NUM_TEST})
    do
      export HADOOP_OPTS="-Dsun.io.serialization.extendedDebugInfo=true"; \
        hadoop jar $JAR com.twitter.scalding.Tool au.com.cba.omnia.grimlock.test.TestScalding${i} --hdfs \
          --input "someInputfile3.txt"

      # --tool.graph
      #dot -Tps2 au.com.cba.omnia.grimlock.test.TestScalding${i}0.dot -o graph_${1}.ps
      #dot -Tps2 au.com.cba.omnia.grimlock.test.TestScalding${i}0_steps.dot -o graph_${i}_steps.ps
    done
  fi
fi

