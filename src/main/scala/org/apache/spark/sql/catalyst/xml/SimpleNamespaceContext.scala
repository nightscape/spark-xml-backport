package org.apache.spark.sql.catalyst.xml

import javax.xml.namespace.NamespaceContext
import scala.collection.JavaConverters._

class SimpleNamespaceContext(prefixMappings: Map[String, String]) extends NamespaceContext {
  private val prefixToUri: Map[String, String] = prefixMappings
  private val uriToPrefix: Map[String, String] = prefixMappings.map(_.swap)

  override def getNamespaceURI(prefix: String): String = 
    prefixToUri.getOrElse(prefix, javax.xml.XMLConstants.NULL_NS_URI)

  override def getPrefix(namespaceURI: String): String = 
    uriToPrefix.get(namespaceURI).orNull

  override def getPrefixes(namespaceURI: String): java.util.Iterator[String] = 
    prefixToUri.keys.iterator.asJava
}
