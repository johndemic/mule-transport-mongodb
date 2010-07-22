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

import com.mongodb.DB;
import com.mongodb.Mongo;
import org.mule.transport.AbstractConnector;
import org.mule.api.MuleException;
import org.mule.api.lifecycle.InitialisationException;

/**
 * <code>MongoDBConnector</code>
 */
public class MongoDBConnector extends AbstractConnector
{

    public static final String PROPERTY_FILENAME = "filename";

    String database;
    String hostname = "localhost";
    String port = "27017";
    String username;
    String password;
    Long pollingFrequency = 1000L;
    
    Mongo mongo;
    DB db;


    /* This constant defines the main transport protocol identifier */
    public static final String MONGODB = "mongodb";


    public void doInitialise() throws InitialisationException
    {
    }

    public void doConnect() throws Exception
    {
        mongo = new Mongo(hostname, Integer.parseInt(port));
        db = mongo.getDB(database);
    }

    public void doDisconnect() throws Exception
    {
        db.requestDone();
    }

   
    public void doStart() throws MuleException
    {
    }

    public void doStop() throws MuleException
    {
    }

    public void doDispose()
    {
    }

    public String getProtocol()
    {
        return MONGODB;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public DB getDb() {
        return db;
    }

   

    public void setPollingFrequency(String pollingFrequency) {
        this.pollingFrequency = Long.parseLong(pollingFrequency);
    }
}
