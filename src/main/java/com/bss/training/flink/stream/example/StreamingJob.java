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

import com.bss.training.flink.dto.MovieDetail;
import com.google.gson.Gson;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class StreamingJob {
	private static final Logger LOGGER = LoggerFactory.getLogger(StreamingJob.class);
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

		// create Kafka consumer data source
		//DataStream<MovieDetail> movies = env.addSource(consumer);

		env.addSource(consumer)
				.map(new MovieMapper())
				.filter(new FilterFunction<MovieDetail>() {
					@Override
					public boolean filter(final MovieDetail movieDetail) throws Exception {
						if (movieDetail != null && movieDetail.getGenres() != null && movieDetail.getGenres().contains("Drama")) {
							return true;
						} else {
							LOGGER.warn("Movie is filterred out .... {}", movieDetail);
							return false;
						}
					}
				}).print();

		//movies.print();

		env.execute("Flink Streaming Java API Skeleton");
	}

	private static class MovieMapper implements MapFunction<String, MovieDetail> {
		@Override
		public MovieDetail map(String s) throws Exception {
			MovieDetail movieDetail = gson.fromJson(s, MovieDetail.class);
			//LOGGER.info("Movie detail object is created: {}", movieDetail);
			return movieDetail;
		}
	}
}
