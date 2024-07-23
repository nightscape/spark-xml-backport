package com.sun.xml.txw2.output

import org.apache.spark.sql.catalyst.xml.XmlOptions

import javax.xml.stream.XMLStreamException
import javax.xml.stream.XMLStreamWriter
import java.util
import java.util.Stack
/*
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * Copyright (c) 2005-2010 Oracle and/or its affiliates. All rights reserved.
 *
 * The contents of this file are subject to the terms of either the GNU
 * General Public License Version 2 only ("GPL") or the Common Development
 * and Distribution License("CDDL") (collectively, the "License").  You
 * may not use this file except in compliance with the License.  You can
 * obtain a copy of the License at
 * https://glassfish.dev.java.net/public/CDDL+GPL_1_1.html
 * or packager/legal/LICENSE.txt.  See the License for the specific
 * language governing permissions and limitations under the License.
 *
 * When distributing the software, include this License Header Notice in each
 * file and include the License file at packager/legal/LICENSE.txt.
 *
 * GPL Classpath Exception:
 * Oracle designates this particular file as subject to the "Classpath"
 * exception as provided by Oracle in the GPL Version 2 section of the License
 * file that accompanied this code.
 *
 * Modifications:
 * If applicable, add the following below the License Header, with the fields
 * enclosed by brackets [] replaced by your own identifying information:
 * "Portions Copyright [year] [name of copyright owner]"
 *
 * Contributor(s):
 * If you wish your version of this file to be governed by only the CDDL or
 * only the GPL Version 2, indicate your decision by adding "[Contributor]
 * elects to include this software in this distribution under the [CDDL or GPL
 * Version 2] license."  If you don't indicate a single choice of license, a
 * recipient has the option to distribute your version of this file under
 * either the CDDL, the GPL Version 2 or to extend the choice of license to
 * its licensees as provided above.  However, if you add GPL Version 2 code
 * and therefore, elected the GPL Version 2 license, then the option applies
 * only if the new code is made subject to such option by the copyright
 * holder.
 */

/** @author
  *   Kohsuke Kawaguchi
  */
object CustomXMLStreamWriter {
  private val SEEN_NOTHING = new AnyRef
  private val SEEN_ELEMENT = new AnyRef
  private val SEEN_DATA = new AnyRef
}

class CustomXMLStreamWriter(writer: XMLStreamWriter, options: XmlOptions) extends DelegatingXMLStreamWriter(writer) {
  private var state = CustomXMLStreamWriter.SEEN_NOTHING
  private val stateStack = new util.Stack[AnyRef]
  private var indentStep = options.indent
  private var depth = 0

  /** Return the current indent step.
    *
    * <p>Return the current indent step: each start tag will be indented by this number of spaces times the number of
    * ancestors that the element has.</p>
    *
    * @return
    *   The number of spaces in each indentation step, or 0 or less for no indentation.
    * @see
    *   #setIndentStep(int)
    * @deprecated
    *   Only return the length of the indent string.
    */
  def getIndentStep: Int = indentStep.length

  @throws[XMLStreamException]
  private def onStartElement(): Unit = {
    stateStack.push(CustomXMLStreamWriter.SEEN_ELEMENT)
    state = CustomXMLStreamWriter.SEEN_NOTHING
    if (depth > 0) super.writeCharacters(options.lineEnding)
    doIndent()
    depth += 1
  }

  @throws[XMLStreamException]
  private def onEndElement(): Unit = {
    depth -= 1
    if (state eq CustomXMLStreamWriter.SEEN_ELEMENT) {
      super.writeCharacters(options.lineEnding)
      doIndent()
    }
    state = stateStack.pop
  }

  @throws[XMLStreamException]
  private def onEmptyElement(): Unit = {
    state = CustomXMLStreamWriter.SEEN_ELEMENT
    if (depth > 0) super.writeCharacters(options.lineEnding)
    doIndent()
  }

  /** Print indentation for the current level.
    *
    * @exception
    *   org.xml.sax.SAXException If there is an error writing the indentation characters, or if a filter further down
    *   the chain raises an exception.
    */
  @throws[XMLStreamException]
  private def doIndent(): Unit = {
    if (depth > 0) for (i <- 0 until depth) {
      super.writeCharacters(indentStep)
    }
  }

  @throws[XMLStreamException]
  override def writeStartDocument(): Unit = {
    super.writeStartDocument()
    super.writeCharacters(options.lineEnding)
  }

  @throws[XMLStreamException]
  override def writeStartDocument(version: String): Unit = {
    super.writeStartDocument(version)
    super.writeCharacters(options.lineEnding)
  }

  @throws[XMLStreamException]
  override def writeStartDocument(encoding: String, version: String): Unit = {
    super.writeStartDocument(encoding, version)
    super.writeCharacters(options.lineEnding)
  }

  @throws[XMLStreamException]
  override def writeStartElement(localName: String): Unit = {
    onStartElement()
    super.writeStartElement(localName)
  }

  @throws[XMLStreamException]
  override def writeStartElement(namespaceURI: String, localName: String): Unit = {
    onStartElement()
    super.writeStartElement(namespaceURI, localName)
  }

  @throws[XMLStreamException]
  override def writeStartElement(prefix: String, localName: String, namespaceURI: String): Unit = {
    onStartElement()
    super.writeStartElement(prefix, localName, namespaceURI)
  }

  @throws[XMLStreamException]
  override def writeEmptyElement(namespaceURI: String, localName: String): Unit = {
    onEmptyElement()
    super.writeEmptyElement(namespaceURI, localName)
  }

  @throws[XMLStreamException]
  override def writeEmptyElement(prefix: String, localName: String, namespaceURI: String): Unit = {
    onEmptyElement()
    super.writeEmptyElement(prefix, localName, namespaceURI)
  }

  @throws[XMLStreamException]
  override def writeEmptyElement(localName: String): Unit = {
    onEmptyElement()
    super.writeEmptyElement(localName)
  }

  @throws[XMLStreamException]
  override def writeEndElement(): Unit = {
    onEndElement()
    super.writeEndElement()
  }

  @throws[XMLStreamException]
  override def writeCharacters(text: String): Unit = {
    state = CustomXMLStreamWriter.SEEN_DATA
    super.writeCharacters(text)
  }

  @throws[XMLStreamException]
  override def writeCharacters(text: Array[Char], start: Int, len: Int): Unit = {
    state = CustomXMLStreamWriter.SEEN_DATA
    super.writeCharacters(text, start, len)
  }

  @throws[XMLStreamException]
  override def writeCData(data: String): Unit = {
    state = CustomXMLStreamWriter.SEEN_DATA
    super.writeCData(data)
  }
}
