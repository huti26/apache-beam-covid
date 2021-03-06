package hutan;

import hutan.tasks.*;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.joda.time.DateTime;
import org.joda.time.Period;

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


        SumCasesByCounty.calculate(input);
        SumCasesBySex.calculate(input);
        FindTheTenDaysWithMostDeaths.calculate(input);
        SumRecoveredByAgeGroupAndDay.calculate(input);

        FindTopThreeTimeDifferenceMeansBetweenIllnessStartAndReportDateByCounty.calculate(input);
        SumDeathsOfPersonsUnderAgeOf80ByCounty.calculate(input);

        var start = DateTime.now();
        pipeline.run().waitUntilFinish();
        var end = DateTime.now();

        var period = new Period(start, end);
        System.out.println(period);


    }


}
