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

package Flink_Ecommerce_java;

import Dezerializer.JSONValueDeserializationSchema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import dto.transaction;
import Functions.TransactionToGenericRecordMapper;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.formats.avro.utils.AvroKryoSerializerUtils;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.formats.avro.AvroDeserializationSchema;

import java.io.File;
import java.time.Duration;
import java.io.IOException;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.formats.parquet.avro.AvroParquetWriters;
import org.apache.avro.Schema;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.kafka.common.protocol.types.Field;


public class DataStreamJob {

	public static void main(String[] args) throws Exception {
		// Sets up the execution environment, which is the main entry point
		// to building Flink applications.
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// Ange Kafka topic
		String topic = "financial_transactions";

		// Skapa en instans av AvroKryoSerializerUtils
		AvroKryoSerializerUtils avroKryoSerializerUtil = new AvroKryoSerializerUtils();

		// Lägg till Avro-serialiserare om det behövs
		avroKryoSerializerUtil.addAvroSerializersIfRequired(env.getConfig(), GenericData.Record.class);

		Schema schema = new Schema.Parser().parse(new File("/opt/flink/schema/schema.avsc"));

		KafkaSource<transaction> source = KafkaSource.<transaction>builder()
				.setBootstrapServers("kafka:29092")
				.setTopics(topic)
				.setGroupId("flink-group")
				.setStartingOffsets(OffsetsInitializer.earliest())
				.setValueOnlyDeserializer(new JSONValueDeserializationSchema())
				.build();
		// Skapa en DataStream genom att lägga till källan
		DataStream<transaction> transactionStream = env.fromSource(source, WatermarkStrategy.noWatermarks(),"Kafka source" );

		// Skapa en MapFunction för att konvertera transaction till GenericRecord
		MapFunction<transaction, GenericRecord> mapper = new TransactionToGenericRecordMapper(schema.toString());

		// Tillämpa MapFunction på DataStream
		DataStream<GenericRecord> avroStream = transactionStream.map(mapper);

		String outputBasePath = "\\opt\\flink\\filer";

		final FileSink<GenericRecord> sink = FileSink
				.forBulkFormat(new Path(outputBasePath), AvroParquetWriters.forGenericRecord(schema))
				.build();

		avroStream.sinkTo(sink);

		// Starta Flink-applikationen
		env.execute("Simple Flink Kafka Consumer");
	}
}
