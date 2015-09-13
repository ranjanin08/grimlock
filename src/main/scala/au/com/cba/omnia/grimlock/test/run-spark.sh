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
NUM_TEST=31
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
  cp ${BASE_DIR}/target/scala-2.11/grimlock*.jar ${JAR}
fi

if [ ${DO_DEMO} = "true" ]
then
  if [ ${DO_LOCAL} = "true" ]
  then
    if [ ${DO_INIT} = "true" ]
    then
      mkdir -p demo.spark
    fi

    if [ ${DO_CLEANUP} = "true" ]
    then
      rm -rf demo.spark/*
    fi

    $BASE_DIR/../spark-1.5.0/bin/spark-submit --master local \
      --class au.com.cba.omnia.grimlock.spark.examples.BasicOperations $JAR local ../data
    $BASE_DIR/../spark-1.5.0/bin/spark-submit --master local \
      --class au.com.cba.omnia.grimlock.spark.examples.Conditional $JAR local ../data
    $BASE_DIR/../spark-1.5.0/bin/spark-submit --master local \
      --class au.com.cba.omnia.grimlock.spark.examples.PipelineDataPreparation $JAR local ../data
    $BASE_DIR/../spark-1.5.0/bin/spark-submit --master local \
      --class au.com.cba.omnia.grimlock.spark.examples.Scoring $JAR local ../data
    $BASE_DIR/../spark-1.5.0/bin/spark-submit --master local \
      --class au.com.cba.omnia.grimlock.spark.examples.DataAnalysis $JAR local ../data
    $BASE_DIR/../spark-1.5.0/bin/spark-submit --master local \
      --class au.com.cba.omnia.grimlock.spark.examples.LabelWeighting $JAR local ../data
    $BASE_DIR/../spark-1.5.0/bin/spark-submit --master local \
      --class au.com.cba.omnia.grimlock.spark.examples.InstanceCentricTfIdf $JAR local ../data
    $BASE_DIR/../spark-1.5.0/bin/spark-submit --master local \
      --class au.com.cba.omnia.grimlock.spark.examples.MutualInformation $JAR local ../data
    $BASE_DIR/../spark-1.5.0/bin/spark-submit --master local \
      --class au.com.cba.omnia.grimlock.spark.examples.DerivedData $JAR local ../data
    $BASE_DIR/../spark-1.5.0/bin/spark-submit --master local \
      --class au.com.cba.omnia.grimlock.spark.examples.Ensemble $JAR local ../data

    if [ -d "demo.old" ]
    then
      set +x
      for f in $(ls demo.spark demo.old | sed '/:$/d' |sort | uniq)
      do
        echo $f
        cat demo.old/$f | sort | while read line; do
          echo $line | tr '|' '\n' | sort | awk '{line=line "|" $0} END {print line}'; done > demo.x
        cat demo.spark/$f/part* | sort | while read line; do
          echo $line | tr '|' '\n' | sort | awk '{line=line "|" $0} END {print line}'; done > demo.y
        diff demo.x demo.y
      done
      rm demo.x demo.y
      set -x
    fi
  fi
fi

if [ ${DO_TEST} = "true" ]
then
  if [ ${DO_LOCAL} = "true" ]
  then
    if [ ${DO_INIT} = "true" ]
    then
      mkdir -p tmp.spark
    fi

    if [ ${DO_CLEANUP} = "true" ]
    then
      rm -rf tmp.spark/*
    fi

    for i in $(seq 1 ${NUM_TEST})
    do
      $BASE_DIR/../spark-1.5.0/bin/spark-submit --master local \
        --class au.com.cba.omnia.grimlock.test.TestSpark${i} $JAR local .
    done

    if [ -d "tmp.old" ]
    then
      set +x
      for f in $(ls tmp.spark tmp.old | sed '/:$/d' |sort | uniq)
      do
        echo $f
        cat tmp.old/$f | sort | while read line; do
          echo $line | tr '|' '\n' | sort | awk '{line=line "|" $0} END {print line}'; done > tmp.x
        cat tmp.spark/$f/part* | sort | while read line; do
          echo $line | tr '|' '\n' | sort | awk '{line=line "|" $0} END {print line}'; done > tmp.y
        diff tmp.x tmp.y
      done
      rm tmp.x tmp.y
      set -x
    fi
  fi
fi

