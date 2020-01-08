package de.witcom.kafka.connect.smt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.Date;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class Namespacefy<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC =
            "Lift record's name into the specified namespace."
                    + "<p/>"
                    + "This is mainly useful for jdbc source connectors, where namespace is null by default."
                    + "It allows easier integration with commons-avro definitions of these records.";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.NAMESPACE,
                    ConfigDef.Type.STRING,
                    "de.witcom.avro.message.connect",
                    ConfigDef.Importance.HIGH,
                    "Fully qualified namespace for the record.");

    private interface ConfigName {
        String NAMESPACE = "record.namespace";
    }

    private static final String PURPOSE = "add namespace to record";

    private String recordNamespace;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        recordNamespace = config.getString(ConfigName.NAMESPACE);
    }

    @Override
    public R apply(R record) {
        final Struct value = requireStruct(record.value(), PURPOSE);
        Schema updatedSchema = makeUpdatedSchema(record.valueSchema(), recordNamespace);
        final Struct updatedValue = new Struct(updatedSchema);

        for (Field field: updatedValue.schema().fields()) {
            updatedValue.put(field.name(), value.get(field));
        }

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                updatedSchema,
                updatedValue,
                record.timestamp()
        );
    }

    private Schema makeUpdatedSchema(Schema schema, String namespace) {
        final SchemaBuilder builder = SchemaBuilder.struct();
        builder.name(namespace+"."+schema.name());
        builder.version(schema.version());
        builder.doc(schema.doc());
        Map<String, String> params = schema.parameters();
        if (params != null) {
            builder.parameters(params);
        }

        for (Field field : schema.fields()) {
            builder.field(field.name(), field.schema());
        }
        return builder.build();
    }

    @Override
    public void close() {}

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

  protected abstract Schema operatingSchema(R record);

  protected abstract Object operatingValue(R record);

  protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

  public static class Key<R extends ConnectRecord<R>> extends Namespacefy<R> {

    @Override
    protected Schema operatingSchema(R record) {
      return record.keySchema();
    }

    @Override
    protected Object operatingValue(R record) {
      return record.key();
    }

    @Override
    protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
      return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
    }

  }

  public static class Value<R extends ConnectRecord<R>> extends Namespacefy<R> {

    @Override
    protected Schema operatingSchema(R record) {
      return record.valueSchema();
    }

    @Override
    protected Object operatingValue(R record) {
      return record.value();
    }

    @Override
    protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
      return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
    }

  }

}
