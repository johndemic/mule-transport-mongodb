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
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import org.mule.api.MuleContext;
import org.mule.api.MuleException;
import org.mule.api.lifecycle.InitialisationException;
import org.mule.api.registry.MuleRegistry;
import org.mule.transport.AbstractConnector;
import org.mule.util.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * <code>MongoDBConnector</code>
 */
public class MongoDBConnector extends AbstractConnector {

    public static final String PROPERTY_FILENAME = "filename";
    public static final String PROPERTY_OBJECT_ID = "objectId";

    public static final String MULE_MONGO_DISPATCH_MODE = "dispatch_mode";
    public static final String MULE_MONGO_WRITE_CONCERN = "write_concern";
    public static final String MULE_MONGO_UPDATE_QUERY = "update_query";
    public static final String MULE_MONGO_UPDATE_UPSERT = "update_upsert";
    public static final String MULE_MONGO_UPDATE_MULTI = "update_multi";

    public static final String MULE_MONGO_MONGO_REGISTRY_ID = "mongodb.mongo";
    public static final String MULE_MONGO_DATABASE_REGISTRY_ID = "mongodb.database";


    String database;
    String hostname = "localhost";
    String replicaSet;
    String port = "27017";
    String username;
    String password;
    Long pollingFrequency = 1000L;

    Mongo mongo;

    public MongoDBConnector(MuleContext context) {
        super(context);
    }

    /* This constant defines the main transport protocol identifier */
    public static final String MONGODB = "mongodb";


    public void doInitialise() throws InitialisationException {
    }

    public void doConnect() throws Exception {

        if (StringUtils.isBlank(replicaSet)) {
            mongo = new Mongo(hostname, Integer.parseInt(port));
        } else {
            if (!replicaSet.contains(",")) {
                throw new MongoException("A replicaSet must contain a list of address:port pairs separated by commas");
            }

            List<ServerAddress> addresses = new ArrayList<ServerAddress>();

            for (String addressString : replicaSet.split(",")) {

                String[] addressComponents = addressString.split(":");

                if (addressComponents.length != 2) {
                    throw new MongoException("A replicaSet must contain a list of address:port pairs separated by commas");
                }
                addresses.add(new ServerAddress(addressComponents[0], Integer.parseInt(addressComponents[1])));
            }
            mongo = new Mongo(addresses);
        }


        MuleRegistry registry = muleContext.getRegistry();

        /*
         ToDo I need to place the Mongo reference into the registry in order to get at it from GridFSFileLoader.
          The rub is that if multiple Mongo connectors are configured only the first will be available.  Need to
          find a better way to handle this.
          */
        if (registry.lookupObject("MULE_MONGO_MONGO_REGISTRY_ID") == null) {
            muleContext.getRegistry().registerObject(MULE_MONGO_MONGO_REGISTRY_ID, mongo);
            muleContext.getRegistry().registerObject(MULE_MONGO_DATABASE_REGISTRY_ID, database);
        } else {
            logger.warn("Mongo and DB objects are already registered");
        }
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

    public Mongo getMongo() {
        return mongo;
    }

    public void setPollingFrequency(String pollingFrequency) {
        this.pollingFrequency = Long.parseLong(pollingFrequency);
    }

    public void setReplicaSet(String replicaSet) {
        this.replicaSet = replicaSet;
    }
}
