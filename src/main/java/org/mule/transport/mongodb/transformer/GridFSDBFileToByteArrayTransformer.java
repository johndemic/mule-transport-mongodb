package org.mule.transport.mongodb.transformer;

import com.mongodb.gridfs.GridFSDBFile;
import org.mule.api.MuleMessage;
import org.mule.api.transformer.TransformerException;
import org.mule.transformer.AbstractMessageAwareTransformer;
import org.mule.transport.mongodb.MongoDBConnector;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Extracts the byte array from a GridFSDBFile
 */
public class GridFSDBFileToByteArrayTransformer extends AbstractMessageAwareTransformer {


    @Override
    public Object transform(MuleMessage message, String s) throws TransformerException {
        GridFSDBFile dbFile = (GridFSDBFile) message.getPayload();

        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try {
            dbFile.writeTo(stream);
        } catch (IOException e) {
            throw new TransformerException(this, e);
        }

        if (dbFile.getFilename() != null) {
            logger.debug("Propagating GridFSDBFile name: " + dbFile.getFilename());
            message.setProperty(MongoDBConnector.PROPERTY_FILENAME, dbFile.getFilename());
        }

        return stream.toByteArray();
    }


}
