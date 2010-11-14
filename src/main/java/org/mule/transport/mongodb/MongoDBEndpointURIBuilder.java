package org.mule.transport.mongodb;

import org.mule.api.endpoint.MalformedEndpointException;
import org.mule.endpoint.AbstractEndpointURIBuilder;

import java.net.URI;
import java.util.Properties;


public class MongoDBEndpointURIBuilder extends AbstractEndpointURIBuilder {

    protected void setEndpoint(URI uri, Properties properties) throws MalformedEndpointException {
        address = uri.toString();
    }


}
