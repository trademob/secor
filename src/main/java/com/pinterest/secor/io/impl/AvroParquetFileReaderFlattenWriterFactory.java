package com.pinterest.secor.io.impl;

import com.pinterest.secor.common.LogFilePath;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.io.FileWriter;
import com.pinterest.secor.io.KeyValue;
import com.pinterest.secor.tools.SchemaFlattener;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AvroParquetFileReaderFlattenWriterFactory extends AvroParquetFileReaderWriterFactory {

    private static final Logger LOG = LoggerFactory.getLogger(AvroParquetFileReaderFlattenWriterFactory.class);
    public static final Map<String, SchemaFlattener.FlattenSchema> flattenSchemaCache = new ConcurrentHashMap<>();

    public AvroParquetFileReaderFlattenWriterFactory(SecorConfig config) {
        super(config);
    }

    @Override
    public FileWriter BuildFileWriter(LogFilePath logFilePath, CompressionCodec codec) throws Exception {
        return new FlattenAvroParquetFileWriter(logFilePath, codec);
    }


    protected class FlattenAvroParquetFileWriter extends AvroParquetFileWriter {

        public FlattenAvroParquetFileWriter(LogFilePath logFilePath,
                                            CompressionCodec codec) throws IOException {
            super(logFilePath, codec);
        }

        @Override
        protected ParquetWriter buildWriter(Path path, Schema schema, CompressionCodecName codecName) throws IOException {
            // push the flatten schema a separate cache (not in SecorSchemaRegistryClient to prevent name collision)
            flattenSchemaCache.put(topic, SchemaFlattener.flattenAvroSchema(schema));
            // build the writer
            return AvroParquetWriter.builder(path)
                    .withSchema(SchemaFlattener.flattenAvroSchema(flattenSchemaCache.get(topic).getSchema()).getSchema())
                    .withCompressionCodec(codecName)
                    .build();
        }

        @Override
        public void write(KeyValue keyValue) throws IOException {
            GenericRecord record = flattenRecord(schemaRegistryClient.decodeMessage(topic, keyValue.getValue()));

            LOG.trace("Writing record {}", record);
            if (record != null) {
                writer.write(record);
            }
        }

        private GenericRecord flattenRecord(final GenericRecord source) {
            final SchemaFlattener.FlattenSchema flatten = flattenSchemaCache.get(topic);

            GenericRecord record = new GenericData.Record(flatten.getSchema());

            for(SchemaFlattener.Column column: flatten.getColumns()) {
                record.put(column.generateName(), SchemaFlattener.extractValue(column, source));
            }

            return record;
        }

    }


}
