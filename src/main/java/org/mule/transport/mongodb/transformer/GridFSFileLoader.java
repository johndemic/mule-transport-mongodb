package org.mule.transport.mongodb.transformer;

import com.mongodb.Mongo;
import com.mongodb.gridfs.GridFS;
import org.mule.api.MuleMessage;
import org.mule.api.transformer.TransformerException;
import org.mule.transformer.AbstractMessageTransformer;
import org.mule.transport.mongodb.MongoDBConnector;

/**
 * <code>GridFSFileLoader</code> takes the payload of a <code>MuleMessage</code> and uses it as the filename to load
 * a file from GridFS.
 */
public class GridFSFileLoader extends AbstractMessageTransformer {

    String bucket;

    @Override
    public Object transformMessage(MuleMessage message, String outputEncoding) throws TransformerException {
        Mongo mongo = message.getMuleContext().getRegistry().lookupObject(
                MongoDBConnector.MULE_MONGO_MONGO_REGISTRY_ID);

        String database = message.getMuleContext().getRegistry().lookupObject(
                MongoDBConnector.MULE_MONGO_DATABASE_REGISTRY_ID);

        String parsedBucket = message.getMuleContext().getExpressionManager().parse(bucket, message);

        GridFS gridFS = new GridFS(mongo.getDB(database), parsedBucket);

        String filename;
        try {
            filename = message.getMuleContext().getExpressionManager().parse(
                    message.getPayloadAsString(), message);
        } catch (Exception e) {
            throw new TransformerException(this, e);
        }

        logger.debug(String.format("Loading %s from GridFS bucket %s", filename, parsedBucket));

        message.setPayload(gridFS.findOne(filename));

        return message;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }
}
