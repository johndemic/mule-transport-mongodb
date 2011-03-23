package org.mule.transport.mongodb.transformer;

import com.mongodb.BasicDBObject;
import com.mongodb.Mongo;
import com.mongodb.gridfs.GridFS;
import org.bson.types.ObjectId;
import org.codehaus.jackson.map.ObjectMapper;
import org.mule.api.MuleMessage;
import org.mule.api.transformer.TransformerException;
import org.mule.transformer.AbstractMessageTransformer;
import org.mule.transport.mongodb.MongoDBConnector;
import org.mule.transport.mongodb.i18n.MongodbMessages;
import org.mule.util.StringUtils;

import java.io.IOException;

/**
 * <code>GridFSFileLoader</code> takes the payload of a <code>MuleMessage</code> and uses it as the filename to load
 * a file from GridFS.
 */
public class GridFSFileLoader extends AbstractMessageTransformer {

    public static final String DEFAULT_EXPRESSION = "#[message:payload]";

    String bucket;
    String query;
    String expression = DEFAULT_EXPRESSION;
    String connector;

    @Override
    public Object transformMessage(MuleMessage message, String outputEncoding) throws TransformerException {

        MongoDBConnector mongoConnector;

        if (endpoint != null) {
            mongoConnector = (MongoDBConnector) endpoint.getConnector();
        } else {
            if (this.connector == null) {
                throw new TransformerException(MongodbMessages.gridfsFileNoConnectorRef(), this);
            }
            mongoConnector = (MongoDBConnector) muleContext.getRegistry().lookupConnector(this.connector);
        }

        Mongo mongo = mongoConnector.getMongo();

        String parsedBucket = message.getMuleContext().getExpressionManager().parse(bucket, message);

        GridFS gridFS = new GridFS(mongo.getDB(mongoConnector.getDatabase()), parsedBucket);

        if (StringUtils.isNotBlank(query)) {
            String parsedQuery = message.getMuleContext().getExpressionManager().parse(
                    query, message);
            ObjectMapper mapper = new ObjectMapper();

            try {
                logger.debug(String.format("Querying for %s from GridFS bucket %s", parsedQuery, parsedBucket));
                BasicDBObject queryObject = mapper.readValue(parsedQuery, BasicDBObject.class);

                if (queryObject.containsField("_id")) {
                    message.setPayload(gridFS.findOne(ObjectId.massageToObjectId(queryObject.get("_id"))));
                } else {
                    message.setPayload(gridFS.findOne(queryObject));
                }
            } catch (IOException e) {
                throw new TransformerException(this, e);
            }
        } else {

            String filename;
            try {
                filename = message.getMuleContext().getExpressionManager().parse(
                        expression, message);


                logger.debug(String.format("Loading %s from GridFS bucket %s", filename, parsedBucket));

                message.setPayload(gridFS.findOne(filename));

            } catch (Exception e) {
                throw new TransformerException(this, e);
            }
        }
        return message;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public void setFileExpression(String expression) {
        this.expression = expression;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public void setConnector(String connector) {
        this.connector = connector;
    }
}
