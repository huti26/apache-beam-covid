package de.hhu.bigdata.project.tasks;

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


public class Exe3a {

    public static PDone calculate(PCollection<String> input) {

        return input
                .apply("Extract fields 8:meldedatum & 14:refDatum & 17:istErkrankungsbeginn & 2:bundesland",
                        MapElements.into(TypeDescriptors.strings())
                                .via(line -> {
                                    var fields = line.split(",");
                                    var istErkrankungsbeginn = fields[17];
                                    var bundesland = fields[2];
                                    var meldedatum = fields[8];
                                    var refDatum = fields[14];
                                    return istErkrankungsbeginn + "," + bundesland + "," + meldedatum + "," + refDatum;
                                }))
                .apply("Filter unknown starts of illness", Filter.by(line -> line.startsWith("1")))
                .apply("Calculate difference between start of illness and report of illness",
                        MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers()))
                                .via(line -> {
                                    var fields = line.split(",");
                                    var bundesland = fields[1];
                                    var meldedatum = fields[2];
                                    var refDatum = fields[3];
                                    var difference = calculateDifferencesBetweenTwoDates(refDatum, meldedatum);
                                    return KV.of(bundesland, difference);
                                }))
                .apply(Mean.perKey())
                .apply(Top.of(3, new CompareCount()))
                .apply("Extract key value pairs",
                        FlatMapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.doubles())).via(list -> list))
                .apply("Convert key value pairs to strings",
                        MapElements.into(TypeDescriptors.strings()).via(element -> element.getKey() + "," + element.getValue()))
                .apply("Write to file",
                        TextIO.write().to("exe3a").withoutSharding());

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
