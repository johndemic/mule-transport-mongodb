/*
 * $Id$
 * --------------------------------------------------------------------------------------
 * Copyright (c) MuleSource, Inc.  All rights reserved.  http://www.mulesource.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package org.mule.transport.mongodb;

import com.mongodb.Mongo;
import com.mongodb.MongoURI;
import org.mule.api.MuleContext;
import org.mule.api.MuleException;
import org.mule.api.lifecycle.InitialisationException;
import org.mule.transport.AbstractConnector;

/**
 * <code>MongoDBConnector</code>
 */
public class MongoDBConnector extends AbstractConnector {

    public static final String PROPERTY_FILENAME = "filename";
    public static final String PROPERTY_OBJECT_ID = "objectId";
    public static final String PROPERTY_CONTENT_TYPE = "contentType";

    public static final String MULE_MONGO_DISPATCH_MODE = "dispatch_mode";
    public static final String MULE_MONGO_WRITE_CONCERN = "write_concern";
    public static final String MULE_MONGO_UPDATE_QUERY = "update_query";
    public static final String MULE_MONGO_UPDATE_UPSERT = "update_upsert";
    public static final String MULE_MONGO_UPDATE_MULTI = "update_multi";

    String uri = "mongodb://localhost";
    Long pollingFrequency = 1000L;

    Mongo mongo;
    MongoURI mongoURI;

    public MongoDBConnector(MuleContext context) {
        super(context);
    }

    /* This constant defines the main transport protocol identifier */
    public static final String MONGODB = "mongodb";


    public void doInitialise() throws InitialisationException {
    }

    public void doConnect() throws Exception {
        logger.debug("Attempting to connect with MongoURI: " + uri);
        mongoURI = new MongoURI(uri);
        mongo = new Mongo(mongoURI);
    }

    public void doDisconnect() throws Exception {
    }


    public void doStart() throws MuleException {
    }

    public void doStop() throws MuleException {
    }

    public void doDispose() {
    }

    public String getProtocol() {
        return MONGODB;
    }

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public Long getPollingFrequency() {
        return pollingFrequency;
    }

    public void setPollingFrequency(Long pollingFrequency) {
        this.pollingFrequency = pollingFrequency;
    }


    public Mongo getMongo() {
        return mongo;
    }

    public void setPollingFrequency(String pollingFrequency) {
        this.pollingFrequency = Long.parseLong(pollingFrequency);
    }

    public MongoURI getMongoURI() {
        return mongoURI;
    }
}
