package com.spotify.sciocontrib.bigquery

import java.nio.ByteBuffer

import com.google.common.io.BaseEncoding
import com.google.protobuf.ByteString
import com.spotify.scio.bigquery.TableRow
import org.apache.avro.generic.GenericData
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._

class ToTableRowTest extends FlatSpec with Matchers with ToTableRow {
  val expectedOutput: TableRow = new TableRow()
    .set("booleanField", true)
    .set("intField", 1)
    .set("stringField", "someString")
    .set("longField", 1L)
    .set("doubleField", 1.0)
    .set("floatField", 1F)
    .set("bytesField", BaseEncoding.base64Url().encode("someBytes".getBytes))
    .set("unionField", "someUnion")
    .set("arrayField", List(new TableRow().set("nestedField", "nestedValue")))
    .set("mapField", List(new TableRow().set("key", "mapKey").set("value", 1.0D)))
    .set("enumField", Kind.FOO.toString)

  "ToTableRow" should "convert a SpecificRecord to TableRow" in {
    val specificRecord = AvroExample.newBuilder()
      .setBooleanField(true)
      .setStringField("someString")
      .setDoubleField(1.0)
      .setLongField(1L)
      .setIntField(1)
      .setFloatField(1F)
      .setBytesField(ByteBuffer.wrap(ByteString.copyFromUtf8("someBytes").toByteArray))
      .setArrayField(List(NestedAvro.newBuilder().setNestedField("nestedValue").build()).asJava)
      .setUnionField("someUnion")
      .setMapField(Map("mapKey" -> 1.0D).asJava
        .asInstanceOf[java.util.Map[java.lang.CharSequence, java.lang.Double]])
      .setEnumField(Kind.FOO)
      .build()

    toTableRow(specificRecord) shouldEqual expectedOutput
  }

  it should "convert a GenericRecord to TableRow" in {
    val nestedAvro = new GenericData.Record(NestedAvro.SCHEMA$)
    nestedAvro.put("nestedField", "nestedValue")

    val genericRecord = new GenericData.Record(AvroExample.SCHEMA$)
    genericRecord.put("booleanField", true)
    genericRecord.put("stringField", "someString")
    genericRecord.put("doubleField", 1.0)
    genericRecord.put("longField", 1L)
    genericRecord.put("intField", 1)
    genericRecord.put("floatField", 1F)
    genericRecord.put("bytesField",
      ByteBuffer.wrap(ByteString.copyFromUtf8("someBytes").toByteArray))
    genericRecord.put("arrayField", List(nestedAvro).asJava)
    genericRecord.put("unionField", "someUnion")
    genericRecord.put("mapField", Map("mapKey" -> 1.0D).asJava
      .asInstanceOf[java.util.Map[java.lang.CharSequence, java.lang.Double]])
    genericRecord.put("enumField", Kind.FOO)

    toTableRow(genericRecord) shouldEqual expectedOutput
  }
}
