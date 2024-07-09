/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.xml

import org.apache.spark.SparkException
import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult, TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.ExprUtils
import org.apache.spark.sql.catalyst.xml.XmlOptions
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types._

object XmlUtils {
  /**
   * Sample XML dataset as configured by `samplingRatio`.
   */
  def sample(xml: Dataset[String], options: XmlOptions): Dataset[String] = {
    require(options.samplingRatio > 0,
      s"samplingRatio (${options.samplingRatio}) should be greater than 0")
    if (options.samplingRatio > 0.99) {
      xml
    } else {
      xml.sample(withReplacement = false, options.samplingRatio, 1)
    }
  }

  /**
   * Sample XML RDD as configured by `samplingRatio`.
   */
  def sample(xml: RDD[PortableDataStream], options: XmlOptions): RDD[PortableDataStream] = {
    require(options.samplingRatio > 0,
      s"samplingRatio (${options.samplingRatio}) should be greater than 0")
    if (options.samplingRatio > 0.99) {
      xml
    } else {
      xml.sample(withReplacement = false, options.samplingRatio, 1)
    }
  }
  /**
   * Check if the schema is valid for XML
   *
   * @param schema The schema to check.
   * @return
   * `TypeCheckSuccess` if the schema is valid
   * `DataTypeMismatch` with an error error if the schema is not valid
   */
  def innerCheckXmlSchema(schema: DataType): TypeCheckResult = {
    val isInvalid = schema.existsRecursively {
      // XML field names must be StringType
      case MapType(keyType, _, _) if !keyType.isInstanceOf[StringType] => true
      case _ => false
    }
    if (isInvalid) {
      TypeCheckFailure("""
        errorSubClass = "INVALID_XML_MAP_KEY_TYPE",
        messageParameters = Map("schema" -> toSQLType(schema))""")
    } else {
      TypeCheckSuccess
    }
  }

  def checkXmlSchema(schema: DataType): Unit = {
    innerCheckXmlSchema(schema) match {
      case TypeCheckFailure("INVALID_XML_MAP_KEY_TYPE") =>
        throw QueryCompilationErrors.invalidJsonSchema(schema)
      case TypeCheckSuccess =>
      case result =>
        throw new SparkException(s"Unknown type check result: $result.")
    }
  }
}
