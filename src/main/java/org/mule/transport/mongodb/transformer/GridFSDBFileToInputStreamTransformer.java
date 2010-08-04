package org.mule.transport.mongodb.transformer;

import com.mongodb.gridfs.GridFSDBFile;
import org.mule.api.MuleMessage;
import org.mule.api.transformer.TransformerException;
import org.mule.transformer.AbstractMessageAwareTransformer;
import org.mule.transport.mongodb.MongoDBConnector;

public class GridFSDBFileToInputStreamTransformer extends AbstractMessageAwareTransformer {


    @Override
    public Object transform(MuleMessage message, String s) throws TransformerException {
        GridFSDBFile dbFile = (GridFSDBFile) message.getPayload();

        if (dbFile.getFilename() != null) {
            logger.debug("Propagating GridFSDBFile name: " + dbFile.getFilename());
            message.setProperty(MongoDBConnector.PROPERTY_FILENAME, dbFile.getFilename());
        }

        return dbFile.getInputStream();
    }

}
