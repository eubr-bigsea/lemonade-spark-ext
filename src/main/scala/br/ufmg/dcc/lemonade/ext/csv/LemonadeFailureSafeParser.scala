package br.ufmg.dcc.lemonade.ext.csv

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.execution.datasources.FailureSafeParser
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

import scala.util.Try

/**
  */
class LemonadeFailureSafeParser[IN](rawParser: IN => Seq[InternalRow],
                                    mode: ParseMode,
                                    schema: StructType,
                                    columnNameOfCorruptRecord: String){


  private var corruptFieldIndex: Int = -1
  private val actualSchema = StructType(schema.filterNot(_.name == columnNameOfCorruptRecord))
  private val resultRow = new GenericInternalRow(schema.length)
  private val nullResult = new GenericInternalRow(schema.length)

  {
    Try {
      corruptFieldIndex = schema.fieldIndex (columnNameOfCorruptRecord)
    }
  }

  def parse(input: IN): scala.Iterator[org.apache.spark.sql.catalyst.InternalRow] = {
    try {
      rawParser.apply(input).toIterator.map(row => toResultRow(Some(row), () => null))
    } catch {
      case e: BadRecordException => mode match {
        case PermissiveMode =>
          Iterator(toResultRow(e.partialResult(), e.record))
        case DropMalformedMode =>
          Iterator.empty
        case FailFastMode =>
          throw new SparkException("Malformed records are detected in record parsing. " +
            s"Parse Mode: ${FailFastMode.name}.", e)
      }
    }
  }
  private val toResultRow: (Option[InternalRow], () => UTF8String) => InternalRow = {
    if (corruptFieldIndex > -1) {
      (row, badRecord) => {
        var i = 0
        while (i < actualSchema.length) {
          val from = actualSchema(i)
          resultRow(schema.fieldIndex(from.name)) = row.map(_.get(i, from.dataType)).orNull
          i += 1
        }
        resultRow(corruptFieldIndex) = badRecord()
        resultRow
      }
    } else {
      (row, _) => row.getOrElse(nullResult)
    }
  }
}
