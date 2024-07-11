package org.apache.spark.sql.catalyst.xml

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.analysis.FunctionRegistryBase
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo, SchemaOfXml, StructsToXml, XmlToStructs}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag

import scala.reflect.ClassTag
import RegisterFunction._
import org.apache.spark.sql.catalyst.FunctionIdentifier
class XmlSparkSessionExtensions extends (SparkSessionExtensions => Unit) {

  override def apply(extensions: SparkSessionExtensions): Unit = {
    register[XmlToStructs](extensions, fromXmlFunctionName)
    register[StructsToXml](extensions, toXmlFunctionName)
    register[SchemaOfXml](extensions, schemaOfXmlFunctionName)
  }
  private def register[T <: Expression : ClassTag](extensions: SparkSessionExtensions, funcName: String): Unit = {
    val (name, (info, builder)) = expression[T](funcName)
    extensions.injectFunction(FunctionIdentifier(name), info, builder)
  }
}

object RegisterFunction {
  val FUNC_ALIAS = TreeNodeTag[String]("functionAliasName")

  /** Create a SQL function builder and corresponding `ExpressionInfo`.
    *
    * @param name
    *   The function name.
    * @param setAlias
    *   The alias name used in SQL representation string.
    * @param since
    *   The Spark version since the function is added.
    * @tparam T
    *   The actual expression class.
    * @return
    *   (function name, (expression information, function builder))
    */
  def expression[T <: Expression : ClassTag](
    name: String,
    setAlias: Boolean = false,
    since: Option[String] = None
  ): (String, (ExpressionInfo, Seq[Expression] => T)) = {
    val (expressionInfo, builder) = FunctionRegistryBase.build[T](name, since)
    val newBuilder = (expressions: Seq[Expression]) => {
      val expr = builder(expressions)
      if (setAlias) expr.setTagValue(FUNC_ALIAS, name)
      expr
    }
    (name, (expressionInfo, newBuilder))
  }

  val fromXmlFunctionName = "from_xml"
  val toXmlFunctionName = "to_xml"
  val schemaOfXmlFunctionName = "schema_of_xml"
}
