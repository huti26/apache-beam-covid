package de.hhu.bigdata.project.tasks;

import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptors;


public class Exe3b {

    public static PDone calculate(PCollection<String> input) {

        return input
                .apply("Extract fields 2:bundesland & 4:altersgruppe & 7:anzahlTodesfall & 13:neuerTodesfall",
                        MapElements.into(TypeDescriptors.strings())
                                .via(line -> {
                                    var fields = line.split(",");
                                    var bundesland = fields[2];
                                    var altersgruppe = fields[4];
                                    var anzahlTodesfall = fields[7];
                                    var neuerTodesfall = fields[13];
                                    return altersgruppe + "," + bundesland + "," + anzahlTodesfall + "," + neuerTodesfall;
                                }))
                .apply("Filter over 80 and unbekannt", Filter.by(line -> !line.startsWith("A80+") && !line.startsWith("unbekannt")))
                .apply("Remove altersgruppe, create KV to filter by neuerTodesfall",
                        MapElements.into(TypeDescriptors.kvs(TypeDescriptors.integers(),TypeDescriptors.strings()))
                                .via(line -> {
                                    var fields = line.split(",");
                                    var bundesland = fields[1];
                                    var anzahlTodesfall = fields[2];
                                    var neuerTodesfall = Integer.parseInt(fields[3]);
                                    return KV.of(neuerTodesfall,bundesland + "," + anzahlTodesfall);
                                }))
                .apply("Remove non new cases", Filter.by(element -> element.getKey() >= 0))
                .apply("Convert key value pairs to strings",
                        MapElements.into(TypeDescriptors.strings()).via(element -> element.getKey() + "," + element.getValue()))
                .apply("Remove neuerTodesfall, create KV to sum",
                        MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(),TypeDescriptors.integers()))
                                .via(line -> {
                                    var fields = line.split(",");
                                    var bundesland = fields[1];
                                    var anzahlTodesfall = Integer.parseInt(fields[2]);
                                    return KV.of(bundesland,anzahlTodesfall);
                                }))
                .apply("Sum the amount of cases", Sum.integersPerKey())
                .apply("Convert key value pairs to strings",
                        MapElements.into(TypeDescriptors.strings()).via(element -> element.getKey() + "," + element.getValue()))
                .apply("Write to file",
                        TextIO.write().to("exe3b").withoutSharding());

    }


}
