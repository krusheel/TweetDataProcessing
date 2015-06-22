package org.freethinking.dataprocessing;

import java.io.File;
import java.io.IOException;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TweetFileReader {

    Logger logger = LoggerFactory.getLogger(TweetFileReader.class);
    JsonParser parser = null;
    private JsonFactory jsonFactory = new JsonFactory();
    private String fileName;

    public TweetFileReader(String fileName) throws IOException {
        this.fileName = fileName;
        parser = getFileParser();
        
        JsonToken token;
        token = parser.nextToken();
        if (token != JsonToken.START_ARRAY) {
            parser = null;
            logger.error("File should start with [");
        }
    }

    public String getNextTextFieldValue() throws IOException {

        if (parser == null)
            return null;

        try {

            JsonToken token;
            token = parser.nextToken();

            while ((token = parser.nextToken()) != JsonToken.END_ARRAY) {
                switch (token) {

                // Starts a new object
                case START_OBJECT:
                    break;

                // For each field-value pair
                case FIELD_NAME:
                    String field = parser.getCurrentName();
                    token = parser.nextToken();
                    if (field.equals("text") && (token == JsonToken.VALUE_STRING)) {
                        return parser.getText();
                    }
                    else if (token == JsonToken.START_ARRAY) {
                        parser.skipChildren();
                    }
                    else if (token == JsonToken.START_OBJECT) {
                        parser.skipChildren();
                    }
                    else {
                    }
                    break;

                // Do something with the field-value pairs
                case END_OBJECT:
                    break;
                }

            }
        } catch (JsonParseException e) {
            logger.error("Error while parsing");
        }
        return null;
    }

    private JsonParser getFileParser() throws IOException {

        try {
            parser = jsonFactory.createJsonParser(new File(fileName));
        } catch (IOException ex) {
            logger.error("Json file  {} not available ", fileName);
            throw ex;
        }

        return parser;
    }

}
