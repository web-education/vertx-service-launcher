package com.opendigitaleducation.launcher.logger;

import java.io.IOException;
import java.util.logging.FileHandler;

public class VideoEventsFileHandler extends FileHandler {

    public VideoEventsFileHandler() throws IOException, SecurityException {
    }

    public VideoEventsFileHandler(String pattern) throws IOException, SecurityException {
        super(pattern);
    }

    public VideoEventsFileHandler(String pattern, boolean append) throws IOException, SecurityException {
        super(pattern, append);
    }
}
