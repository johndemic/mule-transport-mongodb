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

import org.mule.api.endpoint.EndpointURI;
import org.mule.endpoint.MuleEndpointURI;
import org.mule.tck.AbstractMuleTestCase;

public class MongoDBEndpointTestCase extends AbstractMuleTestCase
{

    public void testValidEndpointURI() throws Exception
    {
        EndpointURI url = new MuleEndpointURI("mongodb://stuff", muleContext);
        url.initialise();
        assertEquals("mongodb", url.getScheme());
        assertEquals("mongodb://stuff", url.getAddress());       
    }

}
