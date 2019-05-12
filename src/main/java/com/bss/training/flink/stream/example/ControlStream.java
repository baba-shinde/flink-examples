/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bss.training.flink.stream.example;

import com.bss.training.flink.dto.GenereConfig;
import com.bss.training.flink.dto.MovieDetail;
import com.bss.training.flink.dto.MovieWithSingleGenre;
import com.google.gson.Gson;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Properties;

public class ControlStream {
	private static final Logger LOGGER = LoggerFactory.getLogger(ControlStream.class);
	private static Gson gson;

	static {
		gson = new Gson();
	}

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		Gson gson = new Gson();

		Properties props = new Properties();
		props.setProperty("zookeeper.connect", "localhost:2181"); // Zookeeper default host:port
		props.setProperty("bootstrap.servers", "localhost:9092"); // Broker default host:port
		//props.setProperty("group.id", "myGroup");                 // Consumer group ID
		//props.setProperty("auto.offset.reset", "earliest");       // Always read topic from start

		// create a Kafka consumer
		FlinkKafkaConsumer011<String> consumer =
				new FlinkKafkaConsumer011(
						"movies_data",
						new SimpleStringSchema(),
						props);

		FlinkKafkaConsumer011<String> controlConsumer =
				new FlinkKafkaConsumer011(
						"control_topic",
						new SimpleStringSchema(),
						props);


		// create Kafka consumer data source
		//DataStream<MovieDetail> movies = env.addSource(consumer);

		SingleOutputStreamOperator<GenereConfig> configSingleOutputStreamOperator = env.addSource(controlConsumer).map(new GenereConfigMap());

		env.addSource(consumer)
				.map(new MovieMap())
				.keyBy(new KeySelector<MovieWithSingleGenre, String>() {
					@Override
					public String getKey(MovieWithSingleGenre movieWithSingleGenre) throws Exception {
						return movieWithSingleGenre.getGenre();
					}
				})
				.connect(configSingleOutputStreamOperator.keyBy(new KeySelector<GenereConfig, String>() {
					@Override
					public String getKey(GenereConfig genereConfig) throws Exception {
						return genereConfig.getGenere();
					}
				}))
				.flatMap(new RichCoFlatMapFunction<MovieWithSingleGenre, GenereConfig, Tuple2<String, String>>() {
					private ValueStateDescriptor<Boolean> shouldProcess = new ValueStateDescriptor<Boolean>("genereConfig", Boolean.class);

					@Override
					public void flatMap1(MovieWithSingleGenre movieWithSingleGenre, Collector<Tuple2<String, String>> collector) throws Exception {
						Boolean check = getRuntimeContext().getState(shouldProcess).value();
						if (check != null && check) {
							collector.collect(new Tuple2(movieWithSingleGenre.getTitle(), movieWithSingleGenre.getGenre()));
						}
					}

					@Override
					public void flatMap2(GenereConfig genereConfig, Collector<Tuple2<String, String>> collector) throws Exception {
						getRuntimeContext().getState(shouldProcess).update(genereConfig.getAllowed());
					}
				})
				.print();

		//movies.print();

		env.execute("Flink Streaming Java API Skeleton");
	}

	private static class MovieMap implements MapFunction<String, MovieWithSingleGenre> {
		@Override
		public MovieWithSingleGenre map(String s) throws Exception {
			MovieDetail movieDetail = gson.fromJson(s, MovieDetail.class);
			MovieWithSingleGenre ret = new MovieWithSingleGenre();
			ret.setGenre(movieDetail.getGenres().get(0));
			ret.setTitle(movieDetail.getTitle());
			ret.setMovieId(movieDetail.getMovieId());

			return ret;
		}
	}

	private static class GenereConfigMap implements MapFunction<String, GenereConfig> {
		@Override
		public GenereConfig map(String s) throws Exception {
			return gson.fromJson(s, GenereConfig.class);
		}
	}
}
