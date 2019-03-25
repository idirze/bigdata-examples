package com.idirze.bigdata.examples.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.*;


@Slf4j
public class CommandLineParserHelper {


    public static CommandLine parse(Options options, String... args) throws ParseException {
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            log.error("Unable to parse options: " + options, e);
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("spark-submit", options);
            throw e;
        }

        return cmd;
    }


}
