package com.bss.training.flink;

import com.bss.training.flink.dto.Movie;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;

public class MovieFilter {
    private static final Logger LOGGER = LoggerFactory.getLogger(MovieFilter.class);

    public static void main(String[] args) throws Exception {
        MovieFilter movieFilter = new MovieFilter();
        String inputPath = args[0];
        String outputPath = args[1];
        movieFilter.doFilter(inputPath, outputPath);
    }

    public void doFilter(final String inputPath, final String outputPath) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple3<Long, String, String>> source = environment.readCsvFile(inputPath)
                .ignoreFirstLine()
                .parseQuotedStrings('"')
                .ignoreInvalidLines()
                .types(Long.class, String.class, String.class);

        DataSet<Movie> movieDataSet = source.map((line)->{
                    Movie movie = new Movie();
                    movie.setName(line.f1);
                    String[] ary = line.f2.split("\\|");
                    movie.setGenres(new HashSet(Arrays.asList(ary)));
            return movie;
        }/*new MapFunction<Tuple3<Long, String, String>, Object>() {
            @Override
            public Object map(Tuple3<Long, String, String> longStringStringTuple3) throws Exception {
                return null;
            }
        }*/
        );

        DataSet<Movie> filteredMovies = movieDataSet.filter(new FilterFunction<Movie>() {
            @Override
            public boolean filter(Movie movie) throws Exception {
                return movie.getGenres().contains("War");
            }
        });

        filteredMovies.writeAsText(outputPath);

        environment.execute();
    }
}
