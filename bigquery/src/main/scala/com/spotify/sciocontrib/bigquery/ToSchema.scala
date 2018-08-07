package com.spotify.sciocontrib.bigquery

import com.google.api.services.bigquery.model.{TableFieldSchema, TableSchema}
import org.apache.avro.Schema
import org.apache.avro.Schema.Type._

import scala.collection.JavaConverters._

trait ToSchema {
  def toBigQuerySchema(avroSchema: Schema): TableSchema = {
    val fields = getFieldSchemas(avroSchema)

    new TableSchema().setFields(fields.asJava)
  }

  private def getFieldSchemas(avroSchema: Schema): List[TableFieldSchema] = {
    avroSchema.getFields.asScala.map { field =>
      val tableField = new TableFieldSchema()
        .setName(field.name())
        .setDescription(field.doc())

      setFieldType(tableField, avroSchema)
      tableField
    }.toList
  }

  // scalastyle:off cyclomatic.complexity
  private def setFieldType(field: TableFieldSchema, schema: Schema): Unit = {
    val schemaType = schema.getType

    if (schemaType != UNION && Option(field.getMode).isEmpty) {
      field.setMode("REQUIRED")
    }

    schemaType match {
      case UNION => setFieldDataTypeFromUnion(field, schema)
      case STRING =>
      case ENUM =>
      case BYTES => field.setType("STRING")
      case INT =>
      case LONG => field.setType("INTEGER")
      case FLOAT =>
      case DOUBLE => field.setType("FLOAT")
      case BOOLEAN => field.setType("BOOLEAN")
      case ARRAY => setFieldDataTypeFromArray(field, schema)
      case RECORD =>
        field.setType("RECORD")
        field.setFields(getFieldSchemas(schema).asJava)
      case MAP => setFieldTypeFromMap(field, schema)
      case _ => throw AvroConversionException(s"Could not match type $schemaType")
    }
  }
  // scalastyle:on cyclomatic.complexity

  private def setFieldDataTypeFromUnion(field: TableFieldSchema, schema: Schema): Unit = {
    if (schema.getTypes.size() != 2) {
      throw AvroConversionException("Union fields with > 2 types not supported")
    }

    if (field.getMode.equals("REPEATED")) {
      throw AvroConversionException("Array of unions is not supported")
    }

    if (schema.getTypes.asScala.count(_.getType == NULL) != 1) {
      throw AvroConversionException("Union field must include null type")
    }

    field.setMode("NULLABLE")

    schema.getTypes.asScala.find(_.getType != NULL)
      .foreach { fieldType => setFieldType(field, fieldType) }
  }

  private def setFieldDataTypeFromArray(field: TableFieldSchema, schema: Schema): Unit = {
    if (field.getMode.equals("REPEATED")) {
      throw AvroConversionException("Array of arrays not supported")
    }

    field.setMode("REPEATED")
    setFieldType(field, schema.getElementType)
  }

  private def setFieldTypeFromMap(field: TableFieldSchema, schema: Schema): Unit = {
    if (field.getMode.equals("REPEATED")) {
      throw AvroConversionException("Array of maps not supported")
    }

    field.setMode("REPEATED")
    field.setType("RECORD")

    val keyField = new TableFieldSchema().setName("key").setType("STRING").setMode("REQUIRED")
    val valueField = new TableFieldSchema().setName("value")
    setFieldType(valueField, schema.getValueType)

    field.setFields(List(keyField, valueField).asJava)
  }
}
