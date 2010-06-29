package org.mule.transport.mongodb;

import org.mule.api.MuleException;
import org.mule.api.endpoint.InboundEndpoint;
import org.mule.api.transport.MessageRequester;
import org.mule.transport.AbstractMessageRequesterFactory;


public class MongoDBMessageRequesterFactory extends AbstractMessageRequesterFactory {

    @Override
    public MessageRequester create(InboundEndpoint endpoint) throws MuleException {
        return new MongoDBMessageRequester(endpoint);
    }
}
