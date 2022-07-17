package hutan.tasks;

import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptors;


public class GroupRecoveredByAgeGroupAndDay {

    public static PDone calculate(PCollection<String> input) {

        // field "Datenstand" contains a ",", all fields afterwards must be accessed with that in mind
        var base = input.apply("Extract fields 16:anzahlGenesen & 4:altersgruppe & 8:meldedatum, 15:neuGenesen",
                MapElements.into(TypeDescriptors.kvs(TypeDescriptors.integers(), TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers())))
                        .via(line -> {
                            var fields = line.split(",");
                            var altersgruppe = fields[4];
                            var meldedatum = fields[8];
                            var anzahlGenesen = Integer.parseInt(fields[16]);
                            var neuGenesen = Integer.parseInt(fields[15]);

                            return KV.of(neuGenesen, KV.of(altersgruppe + "," + meldedatum, anzahlGenesen));
                        }));


        return base
                .apply("Remove non new cases", Filter.by(element -> element.getKey() >= 0))
                .apply("Unnest KV -> Remove neuerFall field",
                        MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers()))
                                .via(element -> KV.of(element.getValue().getKey(), element.getValue().getValue())))
                .apply(Sum.integersPerKey())
                .apply("Convert key value pairs to strings",
                        MapElements.into(TypeDescriptors.strings()).via(element -> element.getKey() + "," + element.getValue()))
                .apply("Write to file",
                        TextIO.write().to("pipeline_results/recovered_by_age_group.csv").withoutSharding());

    }


}
