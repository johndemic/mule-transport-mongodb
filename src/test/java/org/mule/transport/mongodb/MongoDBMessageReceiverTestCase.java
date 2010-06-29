/*
 * $Id: MessageReceiverTestCase.vm 11967 2008-06-05 20:32:19Z dfeist $
 * --------------------------------------------------------------------------------------
 * Copyright (c) MuleSource, Inc.  All rights reserved.  http://www.mulesource.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package org.mule.transport.mongodb;

import org.mule.api.MuleContext;
import org.mule.api.endpoint.EndpointBuilder;
import org.mule.api.endpoint.InboundEndpoint;
import org.mule.api.service.Service;
import org.mule.api.transport.MessageReceiver;
import org.mule.endpoint.EndpointURIEndpointBuilder;
import org.mule.endpoint.URIBuilder;
import org.mule.transport.AbstractMessageReceiverTestCase;

import com.mockobjects.dynamic.Mock;


public class MongoDBMessageReceiverTestCase extends AbstractMessageReceiverTestCase {
    public MessageReceiver getMessageReceiver() throws Exception {
        Mock mockService = new Mock(Service.class);
        mockService.expectAndReturn("getResponseRouter", null);
        return new MongoDBMessageReceiver(endpoint.getConnector(), (Service) mockService.proxy(), endpoint);
    }

    public InboundEndpoint getEndpoint() throws Exception {        
        return newInboundEndpoint(muleContext,"mongodb://stuff");
    }

    static InboundEndpoint newInboundEndpoint(MuleContext muleContext, String address) throws Exception {

        EndpointBuilder builder = newEndpointBuilder(muleContext, address);
        return muleContext.getRegistry().lookupEndpointFactory()
                .getInboundEndpoint(builder);
    }

    private static EndpointBuilder newEndpointBuilder(MuleContext muleContext, String address)
            throws Exception {
        EndpointBuilder builder = new EndpointURIEndpointBuilder(
                new URIBuilder(address), muleContext);


        final Mock mockService = new Mock(Service.class);
		mockService.expectAndReturn("getResponseRouter", null);
        
        MongoDBConnector connector = new MongoDBConnector();

        builder.setConnector(connector);
        return builder;
    }

}
