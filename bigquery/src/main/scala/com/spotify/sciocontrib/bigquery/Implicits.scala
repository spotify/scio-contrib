package com.spotify.sciocontrib.bigquery

import com.google.api.services.bigquery.model.{TableReference, TableSchema}
import com.spotify.scio.bigquery.TableRow
import com.spotify.scio.io.Tap
import com.spotify.scio.values.SCollection
import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.{CreateDisposition, WriteDisposition}

import scala.concurrent.Future


object Implicits extends ToTableRow with ToSchema {
  case class AvroConversionException(
                                      private val message: String,
                                      private val cause: Throwable = null
                                    ) extends Exception(message, cause)

  implicit class AvroImplicits[T <: IndexedRecord](val self: SCollection[T]) {
    def saveAvroAsBigQuery(table: TableReference,
                       avroSchema: Schema,
                       writeDisposition: WriteDisposition,
                       createDisposition: CreateDisposition,
                       tableDescription: String): Future[Tap[TableRow]] = {
      val bqSchema: TableSchema = toBigQuerySchema(avroSchema)

      self
        .map(toTableRow)
        .saveAsBigQuery(table, bqSchema, writeDisposition, createDisposition, tableDescription)
    }
  }
}