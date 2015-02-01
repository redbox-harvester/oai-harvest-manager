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

package nl.mpi.oai.harvester;

import org.w3c.dom.Document;

/**
 * Metadata container <br><br>
 *
 * An object of this class represents metadata in XML document tree. While the
 * identifier associated with the metadata itself is immutable, clients to this
 * class can modify the XML document contained by invoking the getDoc and
 * setDoc methods.
 *
 * Apart from being well-formed, the metadata represented by an object of this
 * class is not constrained. This means that objects can hold metadata packaged
 * in an OAI envelope, the content of such an envelope, or an envelope containing
 * multiple metadata records.
 *
 * @author Lari Lampen (MPI-PL)
 */
public class Metadata {
    /** A unique identifier, such as the OAI-PMH record identifier. */
    private final String id;

    /** The OAI-PMH provider where this record originated. */
    private final Provider origin;

    // whether or not the metadata is packed in an OAI envelope
    private boolean isInEnvelope;
    // whether or not the metadata takes the form of a list of records
    private final boolean isList;

    /** The XML content of this record. */
    private Document doc;

    /**
     * Create a metadata record.
     * 
     * @param id unique identifier
     * @param doc XML tree corresponding to this record
     * @param origin endpoint information
     */
    public Metadata(String id, Document doc, Provider origin,
                    boolean isInEnvelope, boolean isList) {
        this.id           = id;
        this.doc          = doc;
        this.origin       = origin;
        this.isInEnvelope = isInEnvelope;
        this.isList       = isList;
    }

    /**
     * Modify the XML tree representation of this record in a way that does
     * not change its identity.
     * 
     * @param doc modified content of this record
     */
    public void setDoc(Document doc) {
	this.doc = doc;
    }

    /** Get this record's unique identifier. */
    public String getId() {
	return id;
    }

    /** Get the XML tree representing this record. */
    public Document getDoc() {
	return doc;
    }

    /** Get the provider from which this record was harvested. */
    public Provider getOrigin() {
        return origin;
    }

    /**
     * Check if the metadata is contained in an envelope
     *
     * @return true, iff the metadata is contained in an envelope
     */
    public boolean isInEnvelope (){
        return isInEnvelope;
    }

    /**
     * Check if the metadata takes the form of a list
     *
     * @return true, iff the metadata takes the form of a list
     */
    public boolean isList () {
        return isList;
    }

    /**
     * Remember whether or not the metadata is packaged in an envelope
     *
     * @param isInEnvelope whether or not the metadata is packaged in an envelope
     */
    public void setIsInEnvelope(boolean isInEnvelope) {
        this.isInEnvelope = isInEnvelope;
    }
}