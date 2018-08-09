/*
 * Copyright 2018 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.sciocontrib.bigquery

import com.google.api.services.bigquery.model.{TableReference, TableSchema}
import com.spotify.scio.bigquery.TableRow
import com.spotify.scio.io.Tap
import com.spotify.scio.values.SCollection
import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.{CreateDisposition, WriteDisposition}

import scala.concurrent.Future

/**
  * Provides implicit helpers for SCollections interacting with BigQuery.
  */
object Implicits extends ToTableRow with ToTableSchema {
  case class AvroConversionException(
                                      private val message: String,
                                      private val cause: Throwable = null
                                    ) extends Exception(message, cause)

  implicit class AvroImplicits[T <: IndexedRecord](val self: SCollection[T]) {

    /**
      * Saves the provided SCollection[T] to BigQuery where T is a subtype of Indexed Record,
      * automatically converting T's [[org.apache.avro.Schema AvroSchema]] to BigQuery's
      * [[com.google.api.services.bigquery.model.TableSchema TableSchema]] and converting each
      * [[org.apache.avro.generic.IndexedRecord IndexedRecord]] into a
      * [[com.spotify.scio.bigquery.TableRow TableRow]].
      */
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