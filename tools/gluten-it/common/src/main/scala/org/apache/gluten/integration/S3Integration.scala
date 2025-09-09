/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gluten.integration

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

class S3Integration extends AnyFunSuite {
  // Configure Spark session with:
  //   spark.hadoop.fs.s3a.endpoint=http://localhost:9000 \
  //   spark.hadoop.fs.s3a.access.key=admin \
  //   spark.hadoop.fs.s3a.secret.key=admin123 \
  //   spark.hadoop.fs.s3a.path.style.access=true \
  //   spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem

  test("S3 read with Gluten") {
    withSparkSession { spark =>
      val irisDF = spark.read.parquet("s3a://gluten-it/iris.parquet")
      irisDF.createOrReplaceTempView("irisView")
      val irisSet = spark.sql("SELECT * from irisView where sepal_length > 5.0")
      
      assert(irisSet.count() == 118, "S3 read test failed")
    }
  }

  test("S3 write with Gluten") {
    withSparkSession { spark =>
      val testData = Seq((1, 10), (2, 20), (2, 30), (3, 40), (2, 50), (1, 70), (4, 60))
        .toDF("l_partkey", "l_quantity")
      testData.createOrReplaceTempView("lineitem")
      val result = spark.sql("select * from lineitem where l_partkey < 3")
      result.write.mode("overwrite").parquet("s3a://gluten-it/test-write")
      val partsDF = spark.read.parquet("s3a://gluten-it/test-write")
      val count = partsDF.count()

      assert(count == 5, s"S3 write test failed: expected 5 rows, got $count")
    }
  }
}