package hutan.tasks;

import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.ArrayList;


public class GroupDeathsByAgeGroup {

    public static PDone calculate(PCollection<String> input) {

        // 2020/12/06
        // 0123456789
        return input
                .apply("Extract fields 8:meldedatum & 7:anzahlTodesfall & 4:altersgruppe & 13:neueTodesfall",
                        MapElements.into(TypeDescriptors.kvs(TypeDescriptors.integers(), TypeDescriptors.strings()))
                                .via(line -> {
                                    var fields = line.split(",");
                                    var meldedatum = fields[8];
                                    var altersgruppe = fields[4];
                                    var anzahlTodesfall = fields[7];
                                    var neuerTodesfall = Integer.parseInt(fields[13]);
                                    return KV.of(neuerTodesfall, meldedatum + "," + altersgruppe + "," + anzahlTodesfall);
                                }))
                .apply("Remove non new cases", Filter.by(element -> element.getKey() >= 0))
                .apply("Convert key value pairs to strings",
                        MapElements.into(TypeDescriptors.strings()).via(element -> element.getKey() + "," + element.getValue()))
                .apply("Extract fields 1:meldedatum & 2:anzahlTodesfall & 3:altersgruppe",
                        MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers()))
                                .via(line -> {
                                    var fields = line.split(",");
                                    var meldedatum = fields[1];
                                    var altersgruppe = fields[2];
                                    var anzahlTodesfall = Integer.parseInt(fields[3]);
                                    return KV.of(meldedatum + "," + altersgruppe, anzahlTodesfall);
                                }))
                .apply("Sum the amount of cases", Sum.integersPerKey())
                .apply("Convert key value pairs to strings",
                        MapElements.into(TypeDescriptors.strings()).via(element -> element.getKey() + "," + element.getValue()))
                .apply("Transform to K: Altersgruppe,MeldeJahr,MeldeMonat V: (K: MeldeTag V: Sum(AnzahlTodesfall))",
                        MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.kvs(TypeDescriptors.integers(), TypeDescriptors.integers())))
                                .via(line -> {
                                    var fields = line.split(",");
                                    var meldedatum = fields[0];
                                    var meldeJahrMonat = meldedatum.substring(0, 7);
                                    var meldeTag = Integer.parseInt(meldedatum.substring(8, 10));
                                    var altersgruppe = fields[1];
                                    var anzahlTodesfall = Integer.parseInt(fields[2]);
                                    return KV.of(altersgruppe + "," + meldeJahrMonat, KV.of(meldeTag, anzahlTodesfall));
                                }))
                .apply("Group days per agegroup,year,month", GroupByKey.create())
                .apply("Group on altersgruppe meldejahr",
                        MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.kvs(
                                TypeDescriptors.integers(), TypeDescriptors.iterables(
                                        TypeDescriptors.kvs(TypeDescriptors.integers(), TypeDescriptors.integers())
                                )
                        )))
                                .via(element -> {
                                            var ageGroupYearMonth = element.getKey();
                                            var ageGroupYearMonthSeperated = ageGroupYearMonth.split(",");
                                            var ageGroup = ageGroupYearMonthSeperated[0];
                                            var yearMonth = ageGroupYearMonthSeperated[1];
                                            var year = yearMonth.substring(0, 4);
                                            var month = Integer.parseInt(yearMonth.substring(5, 7));

                                            var dayIterable = element.getValue();

                                            return KV.of(ageGroup + "," + year, KV.of(month, dayIterable));
                                        }

                                )
                )
                .apply("Group days per agegroup,year", GroupByKey.create())
                .apply("Calculate cumsum",
                        FlatMapElements.into(TypeDescriptors.strings())
                                .via(element -> {
                                    var agegroupYear = element.getKey();
                                    var monthsIterable = element.getValue();

                                    // arrays start at 0, months start at 1
                                    // get month sizes
                                    var monthSizes = new int[13];
                                    for (var month :
                                            monthsIterable) {
                                        var monthNumber = month.getKey();
                                        var daysIterable = month.getValue();
                                        monthSizes[monthNumber] = calculateMonthSize(daysIterable);
                                    }


                                    // calculate cumsum for each month, starting with month 1, ending with month 12
                                    var dailyDeathsPerMonth = new int[13][];
                                    var lastMonthCumsum = 0;

                                    for (int currentMonth = 1; currentMonth <= 12; currentMonth++) {
                                        for (var month :
                                                monthsIterable) {
                                            var monthNumber = month.getKey();

                                            if (monthNumber == currentMonth) {
                                                var daysIterable = month.getValue();
                                                var currentMonthSize = monthSizes[monthNumber];

                                                var deaths = calculateMonthCumSum(
                                                        daysIterable,
                                                        lastMonthCumsum,
                                                        currentMonthSize
                                                );

                                                // cumsum per month is the value of the last date of a month
                                                lastMonthCumsum = deaths[currentMonthSize];

                                                dailyDeathsPerMonth[monthNumber] = deaths;

                                            }
                                        }
                                    }

                                    // create output by iterating through dailyDeathsPerMonth
                                    var output = new ArrayList<String>();

                                    for (int currentMonth = 1; currentMonth <= 12; currentMonth++) {
                                        var dailyDeaths = dailyDeathsPerMonth[currentMonth];

                                        if (dailyDeaths != null) {
                                            for (int day = 1; day < dailyDeaths.length; day++) {
                                                var dayFormatted = day < 10 ? "0" + day : String.valueOf(day);
                                                output.add(agegroupYear + "/" + currentMonth + "/" + dayFormatted + "," + dailyDeaths[day]);
                                            }
                                        }


                                    }

                                    return output;
                                })
                )

                .apply("Write to file",
                        TextIO.write().to("pipeline_results/deaths_by_age_group.csv").withoutSharding());


    }

    public static int calculateMonthSize(Iterable<KV<Integer, Integer>> days) {
        var monthSize = 0;
        for (var day :
                days) {
            var dayNumber = day.getKey();
            if (dayNumber > monthSize) {
                monthSize = dayNumber;
            }
        }
        return monthSize;
    }

    public static int[] calculateMonthCumSum(Iterable<KV<Integer, Integer>> daysIterable, int lastMonthCumSum, int monthSize) {

        // write iterable values to array
        // days start at 1, arrays start at 0, we keep deaths[1] = deathsOnDay1 & later skip the 0 field
        var deaths = new int[monthSize + 1];
        for (var dayDeathsPair : daysIterable) {
            var day = dayDeathsPair.getKey();
            var deathsOnDay = dayDeathsPair.getValue();
            deaths[day] = deathsOnDay;
        }

        // add last months cumsum to day 1 of new month
        deaths[1] += lastMonthCumSum;

        // cumsum & add to final list & start at 1 to ignore empty 0 field
        for (int day = 1; day < deaths.length; day++) {
            deaths[day] = deaths[day] + deaths[day - 1];
        }

        return deaths;
    }


}
