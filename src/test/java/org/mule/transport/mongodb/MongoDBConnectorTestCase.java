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

import com.mongodb.BasicDBObject;
import org.mule.api.transport.Connector;
import org.mule.transport.AbstractConnectorTestCase;

public class MongoDBConnectorTestCase extends AbstractConnectorTestCase
{
    
    public Connector createConnector() throws Exception
    {

        MongoDBConnector c = new MongoDBConnector(muleContext);
        c.setUri("mongodb://localhost");
        c.setName("Test");
        return c;
    }

    public String getTestEndpointURI()
    {
       return "mongodb://stuff";
    }

    public Object getValidMessage() throws Exception
    {
        return new BasicDBObject();
    }

    public void testProperties() throws Exception
    {        
    }

}
