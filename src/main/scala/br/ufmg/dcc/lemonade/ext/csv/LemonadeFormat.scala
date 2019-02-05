package br.ufmg.dcc.lemonade.ext.csv

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.csv._
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{AnalysisException, SparkSession}

import scala.util.Try

/**
  */
class LemonadeFormat extends CSVFileFormat {
  override def buildReader(sparkSession: SparkSession, dataSchema: StructType,
                           partitionSchema: StructType,
                           requiredSchema: StructType, filters: Seq[Filter],
                           options: Map[String, String],
                           hadoopConf: Configuration): (PartitionedFile) => Iterator[InternalRow] = {
    CSVUtils.verifySchema(dataSchema)
    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    val parsedOptions = new CSVOptions(
      options,
      sparkSession.sessionState.conf.sessionLocalTimeZone,
      sparkSession.sessionState.conf.columnNameOfCorruptRecord)
    // Check a field requirement for corrupt records here to throw an exception in a driver side
    var f: StructField = null
    Try {
      val index = dataSchema.fieldIndex(parsedOptions.columnNameOfCorruptRecord)
      f = dataSchema(index)
    }

    if (f != null && (f.dataType != StringType || !f.nullable)) {
      throw new LemonadeAnalysisException(
        "The field for corrupt records must be string type and nullable")
    }

    if (requiredSchema.length == 1 &&
      requiredSchema.head.name == parsedOptions.columnNameOfCorruptRecord) {
      throw new LemonadeAnalysisException(
        "Since Spark 2.3, the queries from raw JSON/CSV files are disallowed when the\n" +
          "referenced columns only include the internal corrupt record column\n" +
          s"(named _corrupt_record by default). For example:\n" +
          "spark.read.schema(schema).csv(file).filter($\"_corrupt_record\".isNotNull).count()\n" +
          "and spark.read.schema(schema).csv(file).select(\"_corrupt_record\").show().\n" +
          "Instead, you can cache or save the parsed results and then send the same query.\n" +
          "For example, val df = spark.read.schema(schema).csv(file).cache() and then\n" +
          "df.filter($\"_corrupt_record\".isNotNull).count()."
      )
    }

    (file: PartitionedFile) => {
      val conf = broadcastedHadoopConf.value.value
      val parser = new LemonadeParser(
        StructType(dataSchema.filterNot(_.name == parsedOptions.columnNameOfCorruptRecord)),
        StructType(requiredSchema.filterNot(_.name == parsedOptions.columnNameOfCorruptRecord)),
        parsedOptions)
      CSVDataSource(parsedOptions).readFile(conf, file, parser, requiredSchema)
    }
  }
}

