package de.hhu.bigdata.project;

import de.hhu.bigdata.project.tasks.*;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;

public class EntryPoint {

    public interface Options extends PipelineOptions {

        @Description("Input for the pipeline")
        @Validation.Required
        String getInput();

        void setInput(String input);
    }

    public static void main(String... args) {
        // Parse and create pipeline options
        PipelineOptionsFactory.register(Options.class);
        var options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(Options.class);

        // Create a new Apache Beam pipeline
        var pipeline = Pipeline.create(options);

        // Read input file
        var input = pipeline.apply(
                "Read all lines from input file",
                TextIO.read().from(options.getInput()));

//        Exe1.calculate(input);
//        Exe2a.calculate(input);
        Exe2b.calculate(input);
//        Exe3a.calculate(input);
//        Exe3b.calculate(input);
//        Exe4a.calculate(input);
//        Exe4b.calculate(input);

        pipeline.run().waitUntilFinish();
    }



}
