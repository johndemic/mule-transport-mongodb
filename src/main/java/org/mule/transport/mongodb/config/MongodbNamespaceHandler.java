/*
 * $Id: NamespaceHandler.vm 10621 2008-01-30 12:15:16Z dirk.olmes $
 * --------------------------------------------------------------------------------------
 * Copyright (c) MuleSource, Inc.  All rights reserved.  http://www.mulesource.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.transport.mongodb.config;

import org.mule.config.spring.factories.InboundEndpointFactoryBean;
import org.mule.config.spring.factories.OutboundEndpointFactoryBean;
import org.mule.config.spring.handlers.AbstractMuleNamespaceHandler;
import org.mule.config.spring.parsers.MuleDefinitionParser;
import org.mule.config.spring.parsers.assembly.configuration.PrefixValueMap;
import org.mule.config.spring.parsers.specific.MessageProcessorDefinitionParser;
import org.mule.config.spring.parsers.specific.endpoint.TransportEndpointDefinitionParser;
import org.mule.endpoint.URIBuilder;
import org.mule.transport.mongodb.MongoDBConnector;
import org.mule.transport.mongodb.transformer.GridFSDBFileToByteArrayTransformer;
import org.mule.transport.mongodb.transformer.GridFSDBFileToInputStreamTransformer;

/**
 * Registers a Bean Definition Parser for handling <code><mongodb:connector></code> elements
 * and supporting endpoint elements.
 */
public class MongodbNamespaceHandler extends AbstractMuleNamespaceHandler {

    public void init() {

        registerEndpointDefinitionParser("inbound-endpoint",
                new TransportEndpointDefinitionParser(MongoDBConnector.MONGODB,
                        TransportEndpointDefinitionParser.PROTOCOL, InboundEndpointFactoryBean.class,
                        TransportEndpointDefinitionParser.RESTRICTED_ENDPOINT_ATTRIBUTES,
                        new String[][]{new String[]{"collection"}, new String[]{"bucket"}}, new String[][]{}));

        registerEndpointDefinitionParser("outbound-endpoint",
                new TransportEndpointDefinitionParser(MongoDBConnector.MONGODB,
                        TransportEndpointDefinitionParser.PROTOCOL, OutboundEndpointFactoryBean.class,
                        TransportEndpointDefinitionParser.RESTRICTED_ENDPOINT_ATTRIBUTES,
                        new String[][]{new String[]{"collection"}, new String[]{"bucket"}}, new String[][]{}));

        registerBeanDefinitionParser("db-file-to-byte-array",
                        new MessageProcessorDefinitionParser(GridFSDBFileToByteArrayTransformer.class));

        registerBeanDefinitionParser("db-file-to-input-stream",
                        new MessageProcessorDefinitionParser(GridFSDBFileToInputStreamTransformer.class));

        registerConnectorDefinitionParser(MongoDBConnector.class);
    }

    protected void registerEndpointDefinitionParser(String element, MuleDefinitionParser parser) {
        parser.addAlias("collection", URIBuilder.PATH);
        parser.addAlias("bucket", URIBuilder.PATH);
        parser.addMapping("bucket", new PrefixValueMap("bucket" + ":"));

        registerBeanDefinitionParser(element, parser);
    }


}
