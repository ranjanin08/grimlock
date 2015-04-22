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
NUM_TEST=2 #28
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
  cd ${BASE_DIR}; ./sbt clean assembly; cd -
  cp ${BASE_DIR}/target/scala-2.10/grimlock*.jar ${JAR}
fi

if [ ${DO_DEMO} = "true" ]
then
  if [ ${DO_LOCAL} = "true" ]
  then
    if [ ${DO_CLEANUP} = "true" ]
    then
      rm -rf demo.spark/*
    fi

    $BASE_DIR/../spark-1.3.0-bin-hadoop2.4/bin/spark-submit \
      --master local --class au.com.cba.omnia.grimlock.spark.examples.BasicOperations $JAR local
#    $BASE_DIR/../spark-1.3.0-bin-hadoop2.4/bin/spark-submit \
#      --master local --class au.com.cba.omnia.grimlock.spark.examples.DataSciencePipelineWithFiltering $JAR local
#    $BASE_DIR/../spark-1.3.0-bin-hadoop2.4/bin/spark-submit \
#      --master local --class au.com.cba.omnia.grimlock.spark.examples.Scoring $JAR local
    $BASE_DIR/../spark-1.3.0-bin-hadoop2.4/bin/spark-submit \
      --master local --class au.com.cba.omnia.grimlock.spark.examples.DataQualityAndAnalysis $JAR local
    $BASE_DIR/../spark-1.3.0-bin-hadoop2.4/bin/spark-submit \
      --master local --class au.com.cba.omnia.grimlock.spark.examples.LabelWeighting $JAR local
    $BASE_DIR/../spark-1.3.0-bin-hadoop2.4/bin/spark-submit \
      --master local --class au.com.cba.omnia.grimlock.spark.examples.InstanceCentricTfIdf $JAR local
#    $BASE_DIR/../spark-1.3.0-bin-hadoop2.4/bin/spark-submit \
#      --master local --class au.com.cba.omnia.grimlock.spark.examples.MutualInformation $JAR local
#    $BASE_DIR/../spark-1.3.0-bin-hadoop2.4/bin/spark-submit \
#      --master local --class au.com.cba.omnia.grimlock.spark.examples.DerivedData $JAR local

    if [ -d "demo.old" ]
    then
      diff -r demo.spark demo.old
    fi
  fi
fi

if [ ${DO_TEST} = "true" ]
then
  if [ ${DO_LOCAL} = "true" ]
  then
    if [ ${DO_CLEANUP} = "true" ]
    then
      rm -rf tmp.spark/*
    fi

    for i in $(seq 1 ${NUM_TEST})
    do
      $BASE_DIR/../spark-1.3.0-bin-hadoop2.4/bin/spark-submit \
        --master local --class au.com.cba.omnia.grimlock.test.TestSpark2 $JAR local "someInputfile3.txt"
    done

    if [ -d "tmp.old" ]
    then
      diff -r tmp.spark tmp.old
    fi
  fi
fi

