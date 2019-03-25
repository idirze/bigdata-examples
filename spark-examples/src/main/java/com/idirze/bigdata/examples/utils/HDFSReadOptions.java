package com.idirze.bigdata.examples.utils;

import lombok.Getter;
import lombok.ToString;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import java.io.Serializable;

@ToString
@Getter
public class HDFSReadOptions implements Serializable {

    private String jd;
    private String inputFile;

    public HDFSReadOptions(String[] args) throws Exception {
        Options options = new Options();

        options.addOption("i", "Input file", true, "path to text file.");
        options.addOption("jd", "jobDescription", true, "Job description.");

        CommandLine cmd = CommandLineParserHelper.parse(options, args);

        jd = cmd.getOptionValue("jd", "SampleHDFSRead [hbase sample read]");
        inputFile = cmd.getOptionValue("i");

    }
}