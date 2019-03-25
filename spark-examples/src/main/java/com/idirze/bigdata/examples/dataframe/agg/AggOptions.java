package com.idirze.bigdata.examples.dataframe.agg;

import lombok.Data;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(mixinStandardHelpOptions = true,
        description = "Prints usage help.")
@Data
public class AggOptions {

    @Option(names = "--count",
            description = "The number of times to repeat.",
            required = true)
    private int count;


}


