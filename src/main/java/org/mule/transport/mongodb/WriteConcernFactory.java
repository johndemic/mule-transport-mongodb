package org.mule.transport.mongodb;

import com.mongodb.WriteConcern;
import org.mule.api.MuleEvent;
import org.mule.api.MuleMessage;
import org.mule.util.StringUtils;

public class WriteConcernFactory {

    public static WriteConcern getWriteConcern(MuleEvent event) {

        MuleMessage message = event.getMessage();

        String writeConcernString = message.getOutboundProperty(MongoDBConnector.MULE_MONGO_WRITE_CONCERN, null);

        if (writeConcernString == null) {
            writeConcernString = (String) event.getEndpoint().getProperty("writeConcern");
        }

        if (StringUtils.isBlank(writeConcernString)) return null;

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
