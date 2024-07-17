package org.apache.spark.sql.execution.datasources

import org.apache.spark.{SparkThrowable, SparkThrowableHelper}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.xml.XmlOptions
import org.apache.spark.sql.execution.datasources.xml.XmlUtils.checkXmlSchema

import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.catalyst.xml.StaxXmlParser
import org.apache.spark.sql.catalyst.util.{AttributeNameParser, FailureSafeParser}
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.DataType.parseTypeWithFallback
import org.apache.spark.sql.errors.QueryCompilationErrors
import java.lang.reflect.Field
import scala.collection.JavaConverters._
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.SparkThrowableHelper
import org.apache.spark.sql.catalyst.analysis.UnresolvedFunction
import org.apache.spark.sql.catalyst.expressions.SchemaOfXml
import org.apache.spark.sql.catalyst.expressions.Expression

package object xml {

  // scalastyle:off line.size.limit
  /** Parses a column containing a XML string into the data type corresponding to the specified schema. Returns `null`,
    * in the case of an unparseable string.
    *
    * @param e
    *   a string column containing XML data.
    * @param schema
    *   the schema to use when parsing the XML string
    * @param options
    *   options to control how the XML is parsed. accepts the same options and the XML data source. See <a href=
    *   "https://spark.apache.org/docs/latest/sql-data-sources-xml.html#data-source-option"> Data Source Option</a> in
    *   the version you use.
    * @group xml_funcs
    * @since 4.0.0
    */
  // scalastyle:on line.size.limit
  def from_xml(e: Column, schema: StructType, options: java.util.Map[String, String]): Column =
    from_xml(e, lit(CharVarcharUtils.failIfHasCharVarchar(schema).sql), options.asScala.iterator)

  // scalastyle:off line.size.limit
  /** (Java-specific) Parses a column containing a XML string into a `StructType` with the specified schema. Returns
    * `null`, in the case of an unparseable string.
    *
    * @param e
    *   a string column containing XML data.
    * @param schema
    *   the schema as a DDL-formatted string.
    * @param options
    *   options to control how the XML is parsed. accepts the same options and the xml data source. See <a href=
    *   "https://spark.apache.org/docs/latest/sql-data-sources-xml.html#data-source-option"> Data Source Option</a> in
    *   the version you use.
    * @group xml_funcs
    * @since 4.0.0
    */
  // scalastyle:on line.size.limit
  def from_xml(e: Column, schema: String, options: java.util.Map[String, String]): Column = {
    val dataType =
      parseTypeWithFallback(
        schema,
        DataType.fromJson,
        errorMsg = s"Could not parse $schema",
        fallbackParser = DataType.fromDDL
      )
    val structType = dataType match {
      case t: StructType => t
      case _ => throw failedParsingStructTypeError(schema)
    }
    from_xml(e, structType, options)
  }

  /** Parses a XML string and infers its schema in DDL format.
    *
    * @param xml
    *   a XML string.
    * @group xml_funcs
    * @since 4.0.0
    */
  def schema_of_xml(xml: String): Column = schema_of_xml(lit(xml))

  /** Parses a XML string and infers its schema in DDL format.
    *
    * @param xml
    *   a foldable string column containing a XML string.
    * @group xml_funcs
    * @since 4.0.0
    */
  def schema_of_xml(xml: Column): Column = withExpr(new SchemaOfXml(xml.expr))

  // scalastyle:off line.size.limit

  /** Parses a XML string and infers its schema in DDL format using options.
    *
    * @param xml
    *   a foldable string column containing XML data.
    * @param options
    *   options to control how the xml is parsed. accepts the same options and the XML data source. See <a href=
    *   "https://spark.apache.org/docs/latest/sql-data-sources-xml.html#data-source-option"> Data Source Option</a> in
    *   the version you use.
    * @return
    *   a column with string literal containing schema in DDL format.
    * @group xml_funcs
    * @since 4.0.0
    */
  // scalastyle:on line.size.limit
  def schema_of_xml(xml: Column, options: java.util.Map[String, String]): Column = {
    withExpr(SchemaOfXml(xml.expr, options.asScala.toMap))
  }
  // scalastyle:off line.size.limit

  /** (Java-specific) Converts a column containing a `StructType` into a XML string with the specified schema. Throws an
    * exception, in the case of an unsupported type.
    *
    * @param e
    *   a column containing a struct.
    * @param options
    *   options to control how the struct column is converted into a XML string. It accepts the same options as the XML
    *   data source. See <a href= "https://spark.apache.org/docs/latest/sql-data-sources-xml.html#data-source-option">
    *   Data Source Option</a> in the version you use.
    * @group xml_funcs
    * @since 4.0.0
    */
  // scalastyle:on line.size.limit
  def to_xml(e: Column, options: java.util.Map[String, String]): Column =
    fnWithOptions("to_xml", options.asScala.iterator, e)

  /** Converts a column containing a `StructType` into a XML string with the specified schema. Throws an exception, in
    * the case of an unsupported type.
    *
    * @param e
    *   a column containing a struct.
    * @group xml_funcs
    * @since 4.0.0
    */
  def to_xml(e: Column): Column = to_xml(e, Map.empty[String, String].asJava)

  private def withExpr(expr: => Expression): Column = {
    Column(expr)
  }
  def failedParsingStructTypeError(raw: String): SparkRuntimeException = {
    new SparkRuntimeException(errorClass = "FAILED_PARSE_STRUCT_TYPE", messageParameters = Map("raw" -> s"'$raw'"))
  }
  class SparkRuntimeException private (
    message: String,
    cause: Option[Throwable],
    errorClass: Option[String],
    messageParameters: Map[String, String]
  ) extends RuntimeException(message, cause.orNull)
      with SparkThrowable {

    def this(
      errorClass: String,
      messageParameters: Map[String, String],
      cause: Throwable = null,
      summary: String = ""
    ) = {
      this(
        SparkThrowableHelper.getMessage(errorClass, messageParameters.map(_.toString()).toArray, summary),
        Option(cause),
        Option(errorClass),
        messageParameters
      )
    }

    override def getErrorClass: String = errorClass.orNull
  }
  def xmlRowTagRequiredError(optionName: String): Throwable = {
    new AnalysisException(
      errorClass = "XML_ROW_TAG_MISSING",
      messageParameters = Array(s"rowTag -> ${toSQLId(optionName)}")
    )
  }
  def toSQLId(parts: String): String = {
    toSQLId(AttributeNameParser.parseAttributeName(parts))
  }

  def toSQLId(parts: Seq[String]): String = {
    val cleaned = parts match {
      case Seq("__auto_generated_subquery_name", rest @ _*) if rest != Nil => rest
      case other => other
    }
    cleaned.map(quoteIdentifier).mkString(".")
  }
  // scalastyle:off line.size.limit
  def quoteIdentifier(name: String): String = {
    // Escapes back-ticks within the identifier name with double-back-ticks, and then quote the
    // identifier with back-ticks.
    "`" + name.replace("`", "``") + "`"
  }

  /** (Java-specific) Parses a column containing a XML string into a `StructType` with the specified schema. Returns
    * `null`, in the case of an unparseable string.
    *
    * @param e
    *   a string column containing XML data.
    * @param schema
    *   the schema to use when parsing the XML string
    * @group xml_funcs
    * @since 4.0.0
    */
  // scalastyle:on line.size.limit
  def from_xml(e: Column, schema: Column): Column = {
    from_xml(e, schema, Iterator.empty)
  }

  // scalastyle:off line.size.limit
  /** (Java-specific) Parses a column containing a XML string into a `StructType` with the specified schema. Returns
    * `null`, in the case of an unparseable string.
    *
    * @param e
    *   a string column containing XML data.
    * @param schema
    *   the schema to use when parsing the XML string
    * @param options
    *   options to control how the XML is parsed. accepts the same options and the XML data source. See <a href=
    *   "https://spark.apache.org/docs/latest/sql-data-sources-xml.html#data-source-option"> Data Source Option</a> in
    *   the version you use.
    * @group xml_funcs
    * @since 4.0.0
    */
  // scalastyle:on line.size.limit
  def from_xml(e: Column, schema: Column, options: java.util.Map[String, String]): Column =
    from_xml(e, schema, options.asScala.iterator)

  /** Parses a column containing a XML string into the data type corresponding to the specified schema. Returns `null`,
    * in the case of an unparseable string.
    *
    * @param e
    *   a string column containing XML data.
    * @param schema
    *   the schema to use when parsing the XML string
    *
    * @group xml_funcs
    * @since 4.0.0
    */
  def from_xml(e: Column, schema: StructType): Column =
    from_xml(e, schema, Map.empty[String, String].asJava)

  private def from_xml(e: Column, schema: Column, options: Iterator[(String, String)]): Column = {
    fnWithOptions("from_xml", options, e, schema)
  }

  /** Invoke a function with an options map as its last argument. If there are no options, its column is dropped.
    */
  private def fnWithOptions(name: String, options: Iterator[(String, String)], arguments: Column*): Column = {
    val augmentedArguments = if (options.hasNext) {
      val flattenedKeyValueIterator = options.flatMap { case (k, v) =>
        Iterator(lit(k), lit(v))
      }
      arguments :+ map(flattenedKeyValueIterator.toSeq: _*)
    } else {
      arguments
    }
    fn(name, augmentedArguments: _*)
  }
  private[sql] def fn(name: String, inputs: Column*): Column = {
    fn(name, isDistinct = false, ignoreNulls = false, inputs: _*)
  }

  private[sql] def fn(name: String, isDistinct: Boolean, inputs: Column*): Column = {
    fn(name, isDistinct = isDistinct, ignoreNulls = false, inputs: _*)
  }

  private[sql] def fn(name: String, isDistinct: Boolean, ignoreNulls: Boolean, inputs: Column*): Column = {
    Column {
      UnresolvedFunction(Seq(name), inputs.map(_.expr), isDistinct, ignoreNulls = ignoreNulls)
    }
  }
  implicit class DataFrameReaderExtensions(val dfr: DataFrameReader) extends AnyVal {

    /** Loads a XML file and returns the result as a `DataFrame`. See the documentation on the other overloaded `xml()`
      * method for more details.
      *
      * @since 4.0.0
      */
    def xml(path: String): DataFrame = {
      // This method ensures that calls that explicit need single argument works, see SPARK-16009
      xml(Seq(path): _*)
    }

    /** Loads XML files and returns the result as a `DataFrame`.
      *
      * This function will go through the input once to determine the input schema if `inferSchema` is enabled. To avoid
      * going through the entire data once, disable `inferSchema` option or specify the schema explicitly using
      * `schema`.
      *
      * You can find the XML-specific options for reading XML files in <a
      * href="https://spark.apache.org/docs/latest/sql-data-sources-xml.html#data-source-option"> Data Source Option</a>
      * in the version you use.
      *
      * @since 4.0.0
      */
    @scala.annotation.varargs
    def xml(paths: String*): DataFrame = {
      val userSpecifiedSchema = readField[Option[StructType]]("userSpecifiedSchema")
      userSpecifiedSchema.foreach(checkXmlSchema)
      dfr.format("xml").load(paths: _*)
    }

    /** Loads an `Dataset[String]` storing XML object and returns the result as a `DataFrame`.
      *
      * If the schema is not specified using `schema` function and `inferSchema` option is enabled, this function goes
      * through the input once to determine the input schema.
      *
      * @param xmlDataset
      *   input Dataset with one XML object per record
      * @since 4.0.0
      */
    def xml(xmlDataset: Dataset[String]): DataFrame = {
      val sparkSession = readField[SparkSession]("sparkSession")
      val extraOptions = readField[Map[String, String]]("extraOptions")
      val parsedOptions: XmlOptions = new XmlOptions(
        extraOptions.toMap,
        sparkSession.sessionState.conf.sessionLocalTimeZone,
        sparkSession.sessionState.conf.columnNameOfCorruptRecord
      )

      val userSpecifiedSchema = readField[Option[StructType]]("userSpecifiedSchema")
      userSpecifiedSchema.foreach(checkXmlSchema)

      val schema = userSpecifiedSchema
        .map {
          case s if !SQLConf.get.getConf(SQLConf.LEGACY_RESPECT_NULLABILITY_IN_TEXT_DATASET_CONVERSION) => s.asNullable
          case other => other
        }
        .getOrElse {
          TextInputXmlDataSource.inferFromDataset(xmlDataset, parsedOptions)
        }

      verifyColumnNameOfCorruptRecord(schema, parsedOptions.columnNameOfCorruptRecord)
      val actualSchema =
        StructType(schema.filterNot(_.name == parsedOptions.columnNameOfCorruptRecord))

      val parsed = xmlDataset.rdd.mapPartitions { iter =>
        val rawParser = new StaxXmlParser(actualSchema, parsedOptions)
        val parser = new FailureSafeParser[String](
          input => rawParser.parse(input),
          parsedOptions.parseMode,
          schema,
          parsedOptions.columnNameOfCorruptRecord
        )
        iter.flatMap(parser.parse)
      }
      sparkSession.internalCreateDataFrame(parsed, schema, isStreaming = xmlDataset.isStreaming)
    }
    private def readField[A](fieldName: String) = {
      val field: Field = dfr.getClass.getDeclaredField(fieldName)
      field.setAccessible(true)
      field.get(dfr).asInstanceOf[A]
    }

    /** A convenient function for schema validation in datasources supporting `columnNameOfCorruptRecord` as an option.
      */
    def verifyColumnNameOfCorruptRecord(schema: StructType, columnNameOfCorruptRecord: String): Unit = {
      schema.getFieldIndex(columnNameOfCorruptRecord).foreach { corruptFieldIndex =>
        val f = schema(corruptFieldIndex)
        if (!f.dataType.isInstanceOf[StringType] || !f.nullable) {
          throw QueryCompilationErrors.invalidFieldTypeForCorruptRecordError()
        }
      }
    }

  }
  implicit class DataFrameWriterExtensions[T](val dfw: DataFrameWriter[T]) extends AnyVal {

    /** Saves the content of the `DataFrame` in XML format at the specified path.
      *
      * You can find the XML-specific options for writing XML files in <a
      * href="https://spark.apache.org/docs/latest/sql-data-sources-xml.html#data-source-option"> Data Source Option</a>
      * in the version you use.
      *
      * @since 4.0.0
      */
    def xml(path: String): Unit = {
      dfw.format("xml").save(path)
    }
  }
}
