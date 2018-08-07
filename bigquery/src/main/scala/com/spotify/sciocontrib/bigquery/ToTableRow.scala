package com.spotify.sciocontrib.bigquery

import java.nio.ByteBuffer

import com.google.common.io.BaseEncoding
import com.spotify.scio.bigquery.TableRow
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, IndexedRecord}

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
  private def toTableRowField[T](o: T, field: Schema.Field): Object = {
    o match {
      case x: CharSequence => x.toString
      case x: Enum[_] => x.name()
      case x: GenericData.EnumSymbol => x.toString
      case x: Number => x
      case x: Boolean => x
      case x: ByteBuffer =>
        Option(field.schema().getProp(encodingPropName)) match {
          case Some("BASE64") => base64Encoding.encode(toByteArray(x))
          case Some("HEX") => hexEncoding.encode(toByteArray(x))
          case Some(encoding) => throw AvroConversionException(s"Unsupported encoding $encoding")
          case None => base64Encoding.encode(toByteArray(x))
        }
      case x: Iterable[_] => toTableRowFromIterable(x, field)
      case x: Map[_, _] => toTableRowFromMap(x, field)
      case x: IndexedRecord => toTableRow(x)
      case _ =>
        throw AvroConversionException(s"ToTableRow conversion failed: could not match $o")
    }
  }
  // scalastyle:on cyclomatic.complexity

  private def toTableRowFromIterable(iterable: Iterable[_], field: Schema.Field): Iterable[_] = {
    iterable.map { item =>
      if (item.isInstanceOf[Iterable[_]] || item.isInstanceOf[Map[_, _]]) {
        throw AvroConversionException(s"ToTableRow conversion failed for item $item: " +
          s"iterable and map types not supported")
      }
      toTableRowField(item, field)
    }
  }

  private def toTableRowFromMap(mapValue: Map[_, _], field: Schema.Field): Iterable[_] = {
    mapValue.map { case (k, v) =>
      new TableRow()
        .set("key", toTableRowField(k, field))
        .set("value", toTableRowField(v, field))
    }
  }

  private def toByteArray(buffer: ByteBuffer) = {
    val copy = buffer.asReadOnlyBuffer
    val bytes = new Array[Byte](copy.limit)
    copy.rewind
    copy.get(bytes)
    bytes
  }
}
