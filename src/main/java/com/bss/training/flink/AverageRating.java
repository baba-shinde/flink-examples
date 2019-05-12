package com.bss.training.flink;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class AverageRating {
    private static final Logger LOGGER = LoggerFactory.getLogger(AverageRating.class);
    private static AverageRating averageRating;

    static {
        averageRating = new AverageRating();
    }

    public static AverageRating getInstance() {
        return averageRating;
    }

    public static void main(String[] args) throws Exception {
        AverageRating rating = new AverageRating();
        String inputPath = args[0];
        rating.calculateAverageRating(inputPath);
    }

    public void calculateAverageRating(final String inputPath) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple3<Long, String, String>> movies = environment.readCsvFile(inputPath+"\\movies.csv")
                .ignoreFirstLine()
                .parseQuotedStrings('"')
                .ignoreInvalidLines()
                .types(Long.class, String.class, String.class);

        DataSet<Tuple2<Long, Double>> ratings = environment.readCsvFile(inputPath+"\\ratings.csv")
                .ignoreFirstLine()
                .includeFields(false, true, true, false)
                .parseQuotedStrings('"')
                .ignoreInvalidLines()
                .types(Long.class, Double.class);

        DataSet<Tuple3<String, String, Double>> joinedDs = movies.join(ratings)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple3<Long, String, String>, Tuple2<Long, Double>, Tuple3<String, String, Double>>() {
                    @Override
                    public Tuple3<String, String, Double> join(Tuple3<Long, String, String> mv, Tuple2<Long, Double> rt) throws Exception {
                        String movieName = mv.f1;
                        String genre = mv.f2.split("\\|")[0];
                        Double rating = rt.f1;

                        return new Tuple3<>(movieName, genre, rating);
                    }
                });

        List<Tuple2<String, Double>> tuples = joinedDs.groupBy(1)
                .reduceGroup(new GroupReduceFunction<Tuple3<String, String, Double>, Tuple2<String, Double>>() {
                    @Override
                    public void reduce(Iterable<Tuple3<String, String, Double>> iterable, Collector<Tuple2<String, Double>> collector) throws Exception {
                        String genre = null;
                        double totalScore = 0;
                        int count = 0;
                        for (Tuple3<String, String, Double> m : iterable){
                            genre = m.f1;
                            totalScore = totalScore + m.f2;
                            count++;
                        }
                        collector.collect(new Tuple2<>(genre, totalScore/count));
                    }
                }).collect();

        String res = tuples.stream()
                .sorted((r1, r2) -> Double.compare(r1.f1, r2.f1))
                .map(Objects::toString)
                .collect(Collectors.joining("\n"));

        System.out.println(res);

        //filteredMovies.writeAsText("G:\\apache-flink\\data-set\\ml-20m\\output\\");

        //environment.execute();
    }
}