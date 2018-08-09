package com.spotify.sciocontrib.bigquery

import java.nio.ByteBuffer
import java.util

import com.google.common.io.BaseEncoding
import com.spotify.scio.bigquery.TableRow
import com.spotify.sciocontrib.bigquery.Implicits.AvroConversionException
import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord

import scala.collection.JavaConverters._

trait ToTableRow {
  private lazy val encodingPropName: String = "bigquery.bytes.encoder"
  private lazy val base64Encoding: BaseEncoding = BaseEncoding.base64Url()
  private lazy val hexEncoding: BaseEncoding = BaseEncoding.base16()

  def toTableRow[T <: IndexedRecord](record: T): TableRow = {
    val row = new TableRow

    record.getSchema.getFields.forEach { field =>
      Option(record.get(field.pos)).foreach { fieldValue =>
        row.set(field.name, toTableRowField(fieldValue, field))
      }
    }

    row
  }

  // scalastyle:off cyclomatic.complexity
  private def toTableRowField(fieldValue: Any, field: Schema.Field): Any = {
    fieldValue match {
      case x: CharSequence => x.toString
      case x: Enum[_] => x.name()
      case x: Number => x
      case x: Boolean => x
      case x: ByteBuffer =>
        Option(field.schema().getProp(encodingPropName)) match {
          case Some("BASE64") => base64Encoding.encode(toByteArray(x))
          case Some("HEX") => hexEncoding.encode(toByteArray(x))
          case Some(encoding) => throw AvroConversionException(s"Unsupported encoding $encoding")
          case None => base64Encoding.encode(toByteArray(x))
        }
      case x: util.Map[_, _] => toTableRowFromMap(x.asScala, field)
      case x: java.lang.Iterable[_] => toTableRowFromIterable(x.asScala, field)
      case x: IndexedRecord => toTableRow(x)
      case _ =>
        throw AvroConversionException(s"ToTableRow conversion failed: could not match $fieldValue")
    }
  }
  // scalastyle:on cyclomatic.complexity

  private def toTableRowFromIterable(iterable: Iterable[Any], field: Schema.Field): util.List[_] = {
    iterable.map { item =>
      if (item.isInstanceOf[Iterable[_]] || item.isInstanceOf[Map[_, _]]) {
        throw AvroConversionException(s"ToTableRow conversion failed for item $item: " +
          s"iterable and map types not supported")
      }
      toTableRowField(item, field)
    }.toList.asJava
  }

  private def toTableRowFromMap(map: Iterable[Any], field: Schema.Field): util.List[_] = {
    map.map { case (k, v) =>
      new TableRow()
        .set("key", toTableRowField(k, field))
        .set("value", toTableRowField(v, field))
    }.toList.asJava
  }

  private def toByteArray(buffer: ByteBuffer) = {
    val copy = buffer.asReadOnlyBuffer
    val bytes = new Array[Byte](copy.limit)
    copy.rewind
    copy.get(bytes)
    bytes
  }
}
