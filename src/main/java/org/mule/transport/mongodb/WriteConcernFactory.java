package org.mule.transport.mongodb;

import com.mongodb.WriteConcern;
import org.mule.api.MuleMessage;

public class WriteConcernFactory {

    public static WriteConcern getWriteConcern(MuleMessage message) {

        if (!message.getOutboundPropertyNames().contains(MongoDBConnector.MULE_MONGO_WRITE_CONCERN)) return null;

        String writeConcernString = message.getOutboundProperty(MongoDBConnector.MULE_MONGO_WRITE_CONCERN);

        switch (MongoDBWriteConcern.valueOf(writeConcernString)) {
            case NORMAL:
                return WriteConcern.NORMAL;
            case SAFE:
                return WriteConcern.SAFE;
            case REPLICAS_SAFE:
                return WriteConcern.REPLICAS_SAFE;
            case FSYNC_SAFE:
                return WriteConcern.FSYNC_SAFE;
            default:
                throw new MongoDBException(String.format(
                        "Could not resolve WriteConcern for %s, allowed values are " +
                                "NORMAL, SAFE, REPLICAS_SAFE and FSYNC_SAFE", writeConcernString));
        }

    }
}
