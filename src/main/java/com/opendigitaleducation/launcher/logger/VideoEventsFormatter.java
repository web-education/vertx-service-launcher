package com.opendigitaleducation.launcher.logger;

import io.vertx.core.impl.Utils;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

public class VideoEventsFormatter extends Formatter {

    @Override
    public String format(LogRecord record) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String dateString = format.format(new Date(record.getMillis()));

        StringBuilder builder = new StringBuilder();
        builder.append(dateString);
        builder.append(record.getMessage());
        builder.append(Utils.LINE_SEPARATOR);
        return builder.toString();
    }
}
