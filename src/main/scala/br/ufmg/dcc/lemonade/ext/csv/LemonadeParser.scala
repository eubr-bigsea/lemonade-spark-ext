package br.ufmg.dcc.lemonade.ext.csv

import com.univocity.parsers.csv.CsvParser
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.BadRecordException
import org.apache.spark.sql.execution.datasources.csv.{CSVOptions, UnivocityParser}
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

import scala.util.control.NonFatal

/**
  */
class LemonadeParser(schema: StructType, requiredSchema: StructType,
                     options: CSVOptions)
  extends UnivocityParser(schema, requiredSchema, options) {


  private var rowNumber: Int = 0
  private val tokenizer = new CsvParser(options.asParserSettings)
  private type ValueConverter = String => Any

  override def parse(input: String): InternalRow = convert(tokenizer.parseLine(input))

  private val row = new GenericInternalRow(requiredSchema.length)

  private def getCurrentInput: UTF8String = {
    UTF8String.fromString(tokenizer.getContext.currentParsedContent())
  }

  private val tokenIndexArr: Array[Int] = {
    requiredSchema.map(f => schema.indexOf(f)).toArray
  }
  private val valueConverters: Array[ValueConverter] =
    schema.map(f => makeConverter(f.name, f.dataType, f.nullable, options)).toArray


  private def convert(tokens: Array[String]): InternalRow = {

    if (tokens.length != schema.length) {
      val checkedTokens = if (schema.length > tokens.length) {
        tokens ++ new Array[String](schema.length - tokens.length)
      } else {
        tokens.take(schema.length)
      }
      def getPartialResult(): Option[InternalRow] = {
        try {
          Some(convert(checkedTokens))
        } catch {
          case _: BadRecordException => None
        }
      }
      // For records with less or more tokens than the schema, tries to return partial results
      // if possible.

      try {
        throw BadRecordException(
          () => getCurrentInput,
          () => getPartialResult(),
          new RuntimeException("Malformed CSV record"))
      } catch {
        case BadRecordException(currentInput, partial, cause) =>
          throw new LemonadeBadRecordException(
            currentInput(), partial(),
            new RuntimeException("Malformed CSV record", cause),
            rowNumber)
      }
    } else {
      try {
        var i = 0
        while (i < requiredSchema.length) {
          val from = this.tokenIndexArr(i)
          row(i) = valueConverters(from).apply(tokens(from))
          i += 1
        }
        rowNumber += 1
        row
      } catch {
        case NonFatal(e) =>
          throw new LemonadeBadRecordException(getCurrentInput, None, e, rowNumber)
      }
    }
  }

  //
  //  private[csv] object LemonadeParser {
  //    def parseStream(
  //                     inputStream: InputStream,
  //                     shouldDropHeader: Boolean,
  //                     parser: LemonadeParser,
  //                     schema: StructType): Iterator[InternalRow] = {
  //      val tokenizer = parser.tokenizer
  //      val safeParser = new LemonadeFailureSafeParser[Array[String]](
  //        input => Seq(parser.convert(input)),
  //        parser.options.parseMode,
  //        schema,
  //        parser.options.columnNameOfCorruptRecord)
  //      convertStream(inputStream, shouldDropHeader, tokenizer) { tokens =>
  //        safeParser.parse(tokens)
  //      }.flatten
  //    }
  //
  //    private def convertStream[T](
  //                                  inputStream: InputStream,
  //                                  shouldDropHeader: Boolean,
  //                                  tokenizer: CsvParser)(convert: Array[String] => T) = new Iterator[T] {
  //      tokenizer.beginParsing(inputStream)
  //      private var nextRecord = {
  //        if (shouldDropHeader) {
  //          tokenizer.parseNext()
  //        }
  //        tokenizer.parseNext()
  //      }
  //
  //      override def hasNext: Boolean = nextRecord != null
  //
  //      override def next(): T = {
  //        if (!hasNext) {
  //          throw new NoSuchElementException("End of stream")
  //        }
  //        val curRecord = convert(nextRecord)
  //        nextRecord = tokenizer.parseNext()
  //        curRecord
  //      }
  //    }
  //  }
  //
  //  def parseIterator(
  //                     lines: Iterator[String],
  //                     shouldDropHeader: Boolean,
  //                     parser: UnivocityParser,
  //                     schema: StructType): Iterator[InternalRow] = {
  //    val options = parser.options
  //
  //    val linesWithoutHeader = if (shouldDropHeader) {
  //      CSVUtils.dropHeaderLine(lines, options)
  //    } else {
  //      lines
  //    }
  //
  //    val filteredLines: Iterator[String] =
  //      CSVUtils.filterCommentAndEmpty(linesWithoutHeader, options)
  //
  //    val safeParser = new LemonadeFailureSafeParser[String](
  //      input => Seq(parser.parse(input)),
  //      parser.options.parseMode,
  //      schema,
  //      parser.options.columnNameOfCorruptRecord)
  //    filteredLines.flatMap(safeParser.parse)
  //  }
}