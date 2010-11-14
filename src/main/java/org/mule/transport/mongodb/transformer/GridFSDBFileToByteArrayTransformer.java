package org.mule.transport.mongodb.transformer;

import com.mongodb.gridfs.GridFSDBFile;
import org.mule.api.MuleMessage;
import org.mule.api.transformer.TransformerException;
import org.mule.transformer.AbstractMessageTransformer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Extracts the byte array from a GridFSDBFile
 */
public class GridFSDBFileToByteArrayTransformer extends AbstractMessageTransformer {

    @Override
    public Object transformMessage(MuleMessage message, String s) throws TransformerException {
        GridFSDBFile dbFile = (GridFSDBFile) message.getPayload();

        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try {
            dbFile.writeTo(stream);
        } catch (IOException e) {
            throw new TransformerException(this, e);
        }
        return stream.toByteArray();
    }
}
