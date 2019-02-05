package br.ufmg.dcc.lemonade.ext.csv

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.unsafe.types.UTF8String

/**
  * Created by walter on 14/05/18.
  */
class LemonadeBadRecordException(var _record: UTF8String,
                                 var _partialRecord: Option[InternalRow], val cause:
                                 scala.Throwable, _rowNumber: Int) extends
  RuntimeException(cause) {
  def record = _record

  def partialRecord = _partialRecord

  def getRecord = _record

  def getPartialRecord = _partialRecord

  def getRowNumber = _rowNumber

}
