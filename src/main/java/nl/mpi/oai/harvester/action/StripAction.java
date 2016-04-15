/*
 * Copyright (C) 2015, The Max Planck Institute for
 * Psycholinguistics.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, version 3 of the License.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * A copy of the GNU General Public License is included in the file
 * LICENSE-gpl-3.0.txt. If that file is missing, see
 * <http://www.gnu.org/licenses/>.
 */

package nl.mpi.oai.harvester.action;

import nl.mpi.oai.harvester.metadata.Metadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.util.ArrayList;
import java.util.List;

/**
 * This action corresponds to stripping off the OAI-PMH envelope surrounding
 * a harvested metadata record.
 * 
 * @author Lari Lampen (MPI-PL)
 */
public class StripAction implements Action {
    private static final Logger logger = LogManager.getLogger(StripAction.class);

    private final XPath xpath;
    private final DocumentBuilder db;

    public StripAction() throws ParserConfigurationException {
	XPathFactory xpf = XPathFactory.newInstance();
	xpath = xpf.newXPath();	
	DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
	db = dbf.newDocumentBuilder();
    }

    @Override
    public boolean perform(List<Metadata> records) {
        
        List<Metadata> newRecords = new ArrayList();
        for (Metadata record:records) {

            // Get the child nodes of the "metadata" tag;
            // that's the content of the response without the
            // OAI-PMH envelope.
            
            NodeList content = null;
            try {
                content = (NodeList) xpath.evaluate("//*[local-name()=" +
                                "'metadata' and parent::*[local-name()=" +
                                "'record']]/*",
                        record.getDoc(), XPathConstants.NODESET);
            } catch (XPathExpressionException ex) {
                logger.error(ex);
            }
            
            if ((content == null) || (content.getLength()==0)) {
                logger.warn("No content was found in this envelope["+record.getId()+"], it might contain only deleted records");
                continue;
            }

            for (int i=0;i<content.getLength();i++) {
                Document doc = db.newDocument();
                Node copy = doc.importNode(content.item(i), true);
                doc.appendChild(copy);
                String id = "";
                try {
                    id = (String) xpath.evaluate(
                        "parent::*[local-name()='metadata']/preceding-sibling::*[local-name()='header']/*[local-name()='identifier']",
                        content.item(i),XPathConstants.STRING);
                } catch (XPathExpressionException ex) {
                    logger.error(ex);
                }
                newRecords.add(new Metadata(
                            id, record.getPrefix(),
                            doc, record.getOrigin(), false, false)
                );
            }
        }
        records.clear();
        records.addAll(newRecords);
        return true;
    }

    @Override
    public String toString() {
	return "strip";
    }

    // All strip actions are equal.
    @Override
    public int hashCode() {
	return 1;
    }
    @Override
    public boolean equals(Object o) {
	if (o instanceof StripAction) {
	    return true;
	}
	return false;
    }

    @Override
    public Action clone() {
	try {
	    // All strip actions are the same. This is effectively a "deep"
	    // copy since it has its own XPath object.
	    return new StripAction();
	} catch (ParserConfigurationException ex) {
	    logger.error(ex);
	}
	return null;
    }
}
