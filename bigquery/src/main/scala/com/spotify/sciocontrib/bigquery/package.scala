
package com.spotify.sciocontrib

import com.google.api.services.bigquery.model.{TableReference, TableSchema}
import com.spotify.scio.bigquery.TableRow
import com.spotify.scio.io.Tap
import com.spotify.scio.values.SCollection
import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.{CreateDisposition, WriteDisposition}

import scala.concurrent.Future


package object bigquery extends ToTableRow with ToSchema {
  case class AvroConversionException(
                                        private val message: String,
                                        private val cause: Throwable = null
                                      ) extends Exception(message, cause)

  implicit class AvroImplicits[T <: IndexedRecord](val self: SCollection[T]) {
    implicit val recordToTableRow: T => TableRow = toTableRow

    def saveAsBigQuery(table: TableReference,
                       avroSchema: Schema,
                       writeDisposition: WriteDisposition,
                       createDisposition: CreateDisposition,
                       tableDescription: String)
                      (implicit t: T => TableRow): Future[Tap[TableRow]] = {
      val bqSchema: TableSchema = toBigQuerySchema(avroSchema)

      self
        .map(avroRecord => t.apply(avroRecord))
        .saveAsBigQuery(table, bqSchema, writeDisposition, createDisposition, tableDescription)
    }
  }
}