Kafka Connect SMT to add a Namespace-Prefix

This SMT supports adding a namespace prefix to the schema-name. Especially useful for JDBC-Source-connectors where the namespace is empty by default 

Properties:

|Name|Description|Type|Default|Importance|
|---|---|---|---|---|
|`record.namespace`| Namespace for schema-name | String | `de.witcom.avro.message.connect` | High |

Example on how to add to your connector:
```
transforms=addnamespace
transforms.addnamespace.type=de.witcom.kafka.connect.smt.Namespacefy$Value
transforms.addnamespace.record.namespace="my.super.namespace"
```

Lots borrowed from the Apache Kafka® `InsertField` SMT
