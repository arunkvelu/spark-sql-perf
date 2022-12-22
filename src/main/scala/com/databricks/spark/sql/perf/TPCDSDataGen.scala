package com.databricks.spark.sql.perf

import org.apache.spark.sql.SparkSession

case class DatagenConfig(
                          path: String = "s3a://",
                          scaleFactor: String = "1",
                          format: String = "parquet",
                          toolsDir: String = "/app/mount/tools",
                          skipDatagen: Boolean = false,
                          useDecimal: Boolean = true,
                          useDate: Boolean = true,
                          filterNull: Boolean = false,
                          shuffle: Boolean = true,
                          nonPartitionedTablesList: Seq[String] = Seq(),
                          partitionedTablesList: Seq[String] = Seq())

object TPCDSDataGen {

  def main(args: Array[String]): Unit = {

    val parser = new scopt.OptionParser[DatagenConfig]("dex-tpcds-data") {
      head("dex-tpcds-data", "0.1.0")
      opt[String]('p', "path")
        .action { (x, c) => c.copy(path = x) }
        .text("Path for generating TPC-DS data")
        .required()
      opt[String]('s', "scaleFactor")
        .action ((x, c) => c.copy(scaleFactor = x))
        .text("The size of the generated dataset in GBs")
        .required()
      opt[String]('f', "format")
        .action((x, c) => c.copy(format = x))
        .text("Format of the generated data e.g.: parquet, orc etc")
      opt[String]("toolsDir")
        .action((x, c) => c.copy(toolsDir = x))
        .text("Location of all files required to create data")
      opt[Boolean]("skipDatagen")
        .action((x, c) => c.copy(skipDatagen = x))
        .text("Skip datagen, external table generation only")
      opt[Boolean]("useDecimal")
        .action((x, c) => c.copy(useDecimal = x))
        .text("If false, float type will be used instead of decimal")
      opt[Boolean]("useDate")
        .action((x, c) => c.copy(useDate = x))
        .text("If false, string type will be used instead of date")
      opt[Boolean]("filterNull")
        .action((x, c) => c.copy(filterNull = x))
        .text("If true, rows with nulls in partition key will be thrown away")
      opt[Boolean]("shuffle")
        .action((x, c) => c.copy(shuffle = x))
        .text("If true, partitions will be coalesced into a single file during generation")
      opt[Seq[String]]("nonPartitionedTablesList")
        .valueName("<v1>,<v2>")
        .action((x, c) => c.copy(nonPartitionedTablesList = x))
        .text("\"\" means generate all non-partitioned tables")
      opt[Seq[String]]("partitionedTablesList")
        .valueName("<v1>,<v2>")
        .action((x, c) => c.copy(partitionedTablesList = x))
        .text("\"\" means generate all partitioned tables")
      help("help")
        .text("prints this usage text")
    }

    parser.parse(args, DatagenConfig()) match {
      case Some(config) =>
        runDatagen(config);
      case _ =>
      // arguments are bad, error message will have been displayed
    }
  }

  def runDatagen(datagenConfig: DatagenConfig) {

    val spark = SparkSession.builder
      .appName("DEXTPCDSDataGen")
      .enableHiveSupport().getOrCreate()

    try {

      val sparkContext = spark.sparkContext
      val sqlContext = spark.sqlContext

      // s3/abfs path to generate the data to.
      //val rootDir = s"s3a://dex-dev-us-west-2/dl2/performance-datasets/tpcds/sf$scaleFactor-$format/useDecimal=$useDecimal,useDate=$useDate,filterNull=$filterNull-dex"
      val rootDir = s"${datagenConfig.path}/sf${datagenConfig.scaleFactor}-${datagenConfig.format}/useDecimal=${datagenConfig.useDecimal},useDate=${datagenConfig.useDate},filterNull=${datagenConfig.filterNull}-dex"
      // name of database to be created.
      val databaseName = s"dex_tpcds_sf${datagenConfig.scaleFactor}" +
        s"""_${if (datagenConfig.useDecimal) "with" else "no"}decimal""" +
        s"""_${if (datagenConfig.useDate) "with" else "no"}date""" +
        s"""_${if (datagenConfig.filterNull) "no" else "with"}nulls"""

      // COMMAND ----------

      // Create the table schema with the specified parameters.
      import com.databricks.spark.sql.perf.tpcds.TPCDSTables
      val tables = new TPCDSTables(sqlContext,
        dsdgenDir = datagenConfig.toolsDir,
        scaleFactor = datagenConfig.scaleFactor,
        useDoubleForDecimal = !datagenConfig.useDecimal,
        useStringForDate = !datagenConfig.useDate)

      import org.apache.spark.deploy.SparkHadoopUtil
      // Limit the memory used by parquet writer
      SparkHadoopUtil.get.conf.set("parquet.memory.pool.ratio", "0.1")
      // Compress with snappy:
      sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
      // TPCDS has around 2000 dates.
      spark.conf.set("spark.sql.shuffle.partitions", "2000")
      // Don't write too huge files.
      sqlContext.setConf("spark.sql.files.maxRecordsPerFile", "20000000")

      val dsdgen_partitioned = 10000 // recommended for SF10000+.
      val dsdgen_nonpartitioned = 10 // small tables do not need much parallelism in generation.
      // COMMAND ----------

      // val tableNames = Array("") // Array("") = generate all.
      //val tableNames = Array("call_center", "catalog_page", "catalog_returns", "catalog_sales", "customer", "customer_address", "customer_demographics", "date_dim", "household_demographics", "income_band", "inventory", "item", "promotion", "reason", "ship_mode", "store", "store_returns", "store_sales", "time_dim", "warehouse", "web_page", "web_returns", "web_sales", "web_site") // all tables

      // generate all the small dimension tables
      import java.time.LocalDateTime

      val startTime = LocalDateTime.now()
      if (!datagenConfig.skipDatagen){
        println(s"$startTime - Generating non partitioned tables.")
        val nonPartitionedTables = if (datagenConfig.nonPartitionedTablesList.isEmpty) {
            Array("call_center", "catalog_page", "customer", "customer_address", "customer_demographics", "date_dim", "household_demographics", "income_band", "item", "promotion", "reason", "ship_mode", "store", "time_dim", "warehouse", "web_page", "web_site")
        }
        else {
            datagenConfig.nonPartitionedTablesList.toArray
        }
        nonPartitionedTables.foreach { t => {
          tables.genData(
            location = rootDir,
            format = datagenConfig.format,
            overwrite = true,
            partitionTables = true,
            clusterByPartitionColumns = datagenConfig.shuffle,
            filterOutNullPartitionValues = datagenConfig.filterNull,
            tableFilter = t,
            numPartitions = dsdgen_nonpartitioned)
        }
        }
        val endTime = LocalDateTime.now()
        println(s"${endTime} - Done generating non partitioned tables.")

        val startTimeD = LocalDateTime.now()
        println(s"$startTimeD - Generating partitioned tables.")

        // leave the biggest/potentially hardest tables to be generated last.
        val partitionedTables = if (datagenConfig.partitionedTablesList.isEmpty) {
            Array("catalog_sales", "store_sales", "inventory", "web_returns", "catalog_returns", "store_returns", "web_sales")
        } else {
            datagenConfig.partitionedTablesList.toArray
        }
        partitionedTables.foreach { t => {
          tables.genData(
            location = rootDir,
            format = datagenConfig.format,
            overwrite = true,
            partitionTables = true,
            clusterByPartitionColumns = datagenConfig.shuffle,
            filterOutNullPartitionValues = datagenConfig.filterNull,
            tableFilter = t,
            numPartitions = dsdgen_partitioned)
        }
        }
        val endTimeD = LocalDateTime.now()
        println(s"$endTimeD - Done generating partitioned tables.")
      }


      // COMMAND ----------

      // MAGIC %md
      // MAGIC Create database

      // COMMAND ----------

      sqlContext.sql(s"drop database if exists $databaseName cascade")
      sqlContext.sql(s"create database $databaseName")

      // COMMAND ----------

      sqlContext.sql(s"use $databaseName")

      // COMMAND ----------

      tables.createExternalTables(rootDir, datagenConfig.format, databaseName, overwrite = true, discoverPartitions = true)

      // COMMAND ----------

      // MAGIC %md
      // MAGIC Analyzing tables is needed only if cbo is to be used.

      // COMMAND ----------

      tables.analyzeTables(databaseName, analyzeColumns = true)
      //sqlContext.sql(s"ALTER TABLE $databaseName.store_sales RECOVER PARTITIONS")
      //sqlContext.sql(s"select count(1) from store_sales").show()
      //sqlContext.sql(s"describe formatted store_sales").show(1000,false)

    }
    finally {
      spark.stop()
    }

  }
}
