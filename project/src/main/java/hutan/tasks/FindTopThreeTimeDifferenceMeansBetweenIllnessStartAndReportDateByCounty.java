package hutan.tasks;

import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.Serializable;
import java.util.Comparator;

public class FindTopThreeTimeDifferenceMeansBetweenIllnessStartAndReportDateByCounty {

    public static PDone calculate(PCollection<String> input) {

        return input
                .apply("KV(17:istErkrankungsbeginn, KV(2:bundesland, KV(8:meldedatum,14:refDatum)))",
                        MapElements
                                .into(TypeDescriptors.kvs(
                                        TypeDescriptors.strings(),
                                        TypeDescriptors.kvs(
                                                TypeDescriptors.strings(),
                                                TypeDescriptors.kvs(
                                                        TypeDescriptors.strings(),
                                                        TypeDescriptors.strings()
                                                )
                                        )
                                ))
                                .via(line -> {
                                    var fields = line.split(",");
                                    return KV.of(fields[17], KV.of(fields[2], KV.of(fields[8], fields[14])));
                                }))
                .apply("Filter unknown starts of illness",
                        Filter.by(element -> element.getKey().startsWith("1")))
                .apply("Calculate difference between start of illness and report of illness: " +
                                "KV(17:istErkrankungsbeginn, KV(2:bundesland, KV(8:meldedatum,14:refDatum))) -> KV(bundesland, time_difference)",
                        MapElements
                                .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers()))
                                .via(element -> KV.of(
                                        element.getValue().getKey(),
                                        calculateDifferencesBetweenTwoDates(
                                                element.getValue().getValue().getValue(), // refDatum
                                                element.getValue().getValue().getKey() // meldeDatum
                                        )
                                )))
                .apply("Calculate average time difference between illness start and illness report",
                        Mean.perKey())
                .apply("Filter for top 3 counties with biggest average time difference between illness start and illness report",
                        Top.of(3, new CompareCount()))
                .apply("Extract key value pairs",
                        FlatMapElements
                                .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.doubles()))
                                .via(list -> list))
                .apply("String(bundesland,mean(time_difference))",
                        MapElements
                                .into(TypeDescriptors.strings())
                                .via(element -> element.getKey() + "," + element.getValue()))
                .apply("Write to file",
                        TextIO.write().to("pipeline_results/top_three_time_difference_means_between_ilness_and_reporting_date_by_county.csv").withoutSharding());

    }

    public static int calculateDifferencesBetweenTwoDates(String startDateString, String endDateString) {
        var formatter = DateTimeFormat.forPattern("yyyy/MM/dd HH:mm:ss");

        var startDate = formatter.parseDateTime(startDateString);
        var endDate = formatter.parseDateTime(endDateString);
        var result = Days.daysBetween(startDate, endDate);

        return result.getDays();
    }


    // Die CompareCount Klasse kann genutzt werden, um den numerischen
    // Wert zweier KV<String, Double> (Schlüssel-Wert Paare) zu vergleichen.
    private static class CompareCount implements Comparator<KV<String, Double>>, Serializable {

        @Override
        public int compare(KV<String, Double> left, KV<String, Double> right) {
            return Double.compare(left.getValue(), right.getValue());
        }
    }


}
