package com.pinterest.secor.tools;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.util.LinkedList;
import java.util.List;

public class SchemaFlattener {

    public static class Column {

        private final String[] hierarchy;
        private final Schema schema;
        private final String doc;
        private final Object defaultValue;

        public Column(final List<String> hierarchy,
                      final Schema schema,
                      final String doc,
                      final Object defautValue) {
            this.hierarchy = hierarchy.toArray(new String[0]);
            this.schema = schema;
            this.doc = doc;
            this.defaultValue = defautValue;
        }

        public Schema.Field generateField() {
            return new Schema.Field(generateName(), unionNullify(), doc, defaultValue);
        }

        private Schema unionNullify() {
            return Schema.createUnion(Schema.create(Schema.Type.NULL), schema);
        }

        public String generateName() {
            final StringBuilder sb = new StringBuilder();
            for(int i = 0; i < hierarchy.length; i++) {
                if(i != 0) {
                    sb.append("_");
                }
                sb.append(hierarchy[i]);
            }
            return sb.toString();
        }

        public String[] getHierarchy() {
            return hierarchy;
        }

    }

    public static class FlattenSchema {

        private final Schema schema;
        private final List<Column> columns;

        public FlattenSchema(final Schema schema, final List<Column> columns) {
            this.schema = schema;
            this.columns = columns;
        }

        public Schema getSchema() {
            return schema;
        }

        public List<Column> getColumns() {
            return columns;
        }
    }

    public static Object extractValue(final Column column, final GenericRecord source) {
        GenericRecord it = source;
        for(final String recordName: column.getHierarchy()) {
            final Object value = it.get(recordName);
            if(!GenericRecord.class.isInstance(value)) {
                return value;
            } else {
                it = (GenericRecord)value;
            }
        }
        return null;
    }

    public static FlattenSchema flattenAvroSchema(final Schema schema) {

        if(schema.getType() != Schema.Type.RECORD) {
            throw new IllegalArgumentException("Schema must be a record");
        }

        final LinkedList<String> hierarchy = new LinkedList<>();
        final LinkedList<Column> columns = new LinkedList<>();

        for(final Schema.Field field: schema.getFields()) {
            flattenAvroField(field, hierarchy, columns);
        }

        Schema flatten = Schema.createRecord(schema.getName(), schema.getDoc(), schema.getNamespace(), false);
        flatten.setFields(generateFields(columns));
        return new FlattenSchema(flatten, columns);
    }

    private static LinkedList<Schema.Field> generateFields(final List<Column> columns) {
        LinkedList<Schema.Field> fields = new LinkedList<>();
        for(Column column: columns) {
            fields.add(column.generateField());
        }
        return fields;
    }

    private static void flattenAvroSchema(Schema schema,
                                          List<String> hierarchy,
                                          List<Column> columns) {
        for(final Schema.Field field: schema.getFields()) {
            flattenAvroField(field, hierarchy, columns);
        }
    }

    private static void flattenAvroField(Schema.Field field,
                                         List<String> hierarchy,
                                         List<Column> columns) {
        final Schema schema = field.schema();

        if(schema.getType() == Schema.Type.RECORD) {
            // recurse
            for (final Schema.Field it : schema.getFields()) {
                flattenAvroField(it, copyAndAdd(hierarchy, field.name()), columns);
            }
        } else if(schema.getType() == Schema.Type.UNION) {
            // also recurse
            flattenAvroUnion(schema, copyAndAdd(hierarchy, field.name()), columns);

        } else {
            columns.add(new Column(copyAndAdd(hierarchy, field.name()), field.schema(), field.doc(), field.defaultVal()));
        }
    }

    private static <T> LinkedList<T> copyAndAdd(final List<T> list, final T element) {
        final LinkedList<T> ret = new LinkedList<>(list);
        ret.add(element);
        return ret;
    }

    private static void flattenAvroUnion(Schema schema,
                                         List<String> hierarchy,
                                         List<Column> columns) {

        for(Schema son: schema.getTypes()) {
            if(son.getType() != Schema.Type.NULL) {
                if(son.getType() == Schema.Type.RECORD) {
                    flattenAvroSchema(son, hierarchy, columns);
                } else {
                    columns.add(new Column(hierarchy, son, null, (Object)null));
                }
            }
        }

    }



}
