package com.robert.kafka.kclient.handlers;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

/**
 * This class converts the JSON string to w3c XML document, and then make it
 * available to be processed by any subclass.
 * 
 * @author Robert Lee
 * @since Aug 21, 2015
 *
 */
public abstract class DocumentMessageHandler extends SafelyMessageHandler {
	protected static Logger log = LoggerFactory
			.getLogger(DocumentMessageHandler.class);

	protected void doExecute(String message) {
		DocumentBuilderFactory dbfac = DocumentBuilderFactory.newInstance();
		try {
			DocumentBuilder docBuilder = dbfac.newDocumentBuilder();
			Document document = docBuilder.parse(new ByteArrayInputStream(
					message.getBytes()));

			doExecuteDocument(document);
		} catch (ParserConfigurationException ex) {
			throw new IllegalStateException(ex);
		} catch (SAXException ex) {
			throw new IllegalArgumentException("Malformed XML document", ex);
		} catch (IOException ioex) {
			throw new IllegalArgumentException("Failed to parse XML document",
					ioex);
		}
	}

	protected abstract void doExecuteDocument(Document document);
}
