package com.spotify.sciocontrib.bigquery

import com.google.api.services.bigquery.model.{TableFieldSchema, TableSchema}
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._

class ToSchemaTest extends FlatSpec with Matchers with ToSchema {

  "ToSchema" should "convert an Avro Schema to a BigQuery TableSchema" in {
    toBigQuerySchema(AvroExample.SCHEMA$) shouldEqual
      new TableSchema().setFields(List(
        new TableFieldSchema().setName("intField").setType("INTEGER").setMode("REQUIRED"),
        new TableFieldSchema().setName("stringField").setType("STRING").setMode("REQUIRED"),
        new TableFieldSchema().setName("booleanField").setType("BOOLEAN").setMode("REQUIRED"),
        new TableFieldSchema().setName("longField").setType("INTEGER").setMode("REQUIRED"),
        new TableFieldSchema().setName("doubleField").setType("FLOAT").setMode("REQUIRED"),
        new TableFieldSchema().setName("floatField").setType("FLOAT").setMode("REQUIRED"),
        new TableFieldSchema().setName("bytesField").setType("BYTES").setMode("REQUIRED"),
        new TableFieldSchema().setName("unionField").setType("STRING").setMode("NULLABLE"),
        new TableFieldSchema().setName("arrayField").setType("RECORD").setMode("REPEATED")
          .setFields(List(
            new TableFieldSchema().setName("nestedField").setMode("REQUIRED").setType("STRING"))
            .asJava),
        new TableFieldSchema().setName("mapField").setType("RECORD").setMode("REPEATED")
          .setFields(List(
            new TableFieldSchema().setName("key").setType("STRING").setMode("REQUIRED"),
            new TableFieldSchema().setName("value").setType("FLOAT").setMode("REQUIRED"))
            .asJava),
        new TableFieldSchema().setName("enumField").setType("STRING").setMode("REQUIRED")
      ).asJava)
  }
}
