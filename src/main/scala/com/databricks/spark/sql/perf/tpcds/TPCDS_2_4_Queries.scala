/*
 * Copyright 2015 Databricks Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.databricks.spark.sql.perf.tpcds

import org.apache.commons.io.IOUtils

import com.databricks.spark.sql.perf.{Benchmark, ExecutionMode, Query}

/**
 * This implements the official TPCDS v2.4 queries with only cosmetic modifications.
 */
trait Tpcds_2_4_Queries extends Benchmark {

  import ExecutionMode._

  val queryNames = Seq(
    "q1"
  )

  val tpcds2_4Queries = queryNames.map { queryName =>
    val queryContent: String = IOUtils.toString(
      getClass().getClassLoader().getResourceAsStream(s"tpcds_2_4/$queryName.sql"))
    Query(queryName + "-v2.4", queryContent, description = "TPCDS 2.4 Query",
      executionMode = CollectResults)
  }

  val tpcds2_4QueriesMap = tpcds2_4Queries.map(q => q.name.split("-").get(0) -> q).toMap
}
