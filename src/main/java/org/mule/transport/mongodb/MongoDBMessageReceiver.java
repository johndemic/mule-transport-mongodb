/*
 * $Id: MessageReceiver.vm 11079 2008-02-27 15:52:01Z tcarlson $
 * --------------------------------------------------------------------------------------
 * Copyright (c) MuleSource, Inc.  All rights reserved.  http://www.mulesource.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package org.mule.transport.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import org.codehaus.jackson.map.ObjectMapper;
import org.mule.DefaultMuleMessage;
import org.mule.api.MuleException;
import org.mule.api.endpoint.InboundEndpoint;
import org.mule.api.lifecycle.CreateException;
import org.mule.api.service.Service;
import org.mule.api.transport.Connector;
import org.mule.api.transport.MessageAdapter;
import org.mule.transport.AbstractPollingMessageReceiver;
import org.mule.transport.ConnectException;

import java.util.ArrayList;
import java.util.List;

/**
 * <code>MongoDBMessageReceiver</code> periodically polls a collection and returns the result.
 */
public class MongoDBMessageReceiver extends AbstractPollingMessageReceiver {

    ObjectMapper mapper;

    public MongoDBMessageReceiver(Connector connector, Service service,
                                  InboundEndpoint endpoint)
            throws CreateException {

        super(connector, service, endpoint);
        mapper = new ObjectMapper();
    }

    public void doConnect() throws ConnectException {
    }

    public void doDisconnect() throws ConnectException {
    }


    public void poll() throws Exception {

        String destination = endpoint.getEndpointURI().toString().split("://")[1];
        if (!destination.startsWith("bucket:")) {
            logger.debug("Polling collection: " + destination);
            pollCollection(destination);
        } else {
            logger.debug("Polling bucket: " + destination);
            pollBucket(destination.split("bucket:")[1]);
        }
    }

    void pollCollection(String collection) throws Exception {
        List<DBObject> result = new ArrayList<DBObject>();

        BasicDBObject query = mapper.readValue((String) endpoint.getProperty("query"), BasicDBObject.class);

        DBCursor cursor = ((MongoDBConnector) connector).getDb().getCollection(collection).find(query);

        while (cursor.hasNext()) {
            result.add(cursor.next());
        }

        MessageAdapter adapter = connector.getMessageAdapter(result);
        routeMessage(new DefaultMuleMessage(adapter), endpoint.isSynchronous());
    }

    void pollBucket(String bucket) throws Exception {

        GridFS gridFS = new GridFS(((MongoDBConnector) connector).getDb(), bucket);
        BasicDBObject query = mapper.readValue((String) endpoint.getProperty("query"), BasicDBObject.class);

        List<GridFSDBFile> results =  gridFS.find(query);

        logger.debug(String.format("Query %s on bucket %s returned %d results", query, bucket, results.size()));

        MessageAdapter adapter = connector.getMessageAdapter(results);
        routeMessage(new DefaultMuleMessage(adapter), endpoint.isSynchronous());
    }


    @Override
    protected void doStart() throws MuleException {
        super.setFrequency(((MongoDBConnector) connector).pollingFrequency);
        super.doStart();
    }
}

