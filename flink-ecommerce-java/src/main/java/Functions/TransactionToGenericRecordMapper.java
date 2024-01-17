package Functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData;
import org.apache.avro.Schema;
import java.io.File;
import dto.transaction;

public class TransactionToGenericRecordMapper implements MapFunction<transaction, GenericRecord> {
    private transient Schema schema;
    private final String schemaString;

    public TransactionToGenericRecordMapper(String schemaString) {
        this.schemaString = schemaString;
    }

    @Override
    public GenericRecord map(transaction value) {
        if (schema == null) {
            schema = new Schema.Parser().parse(schemaString);
        }
        GenericRecord record = new GenericData.Record(schema);
        record.put("transactionId", value.getTransactionId());
        record.put("productId", value.getProductId());
        record.put("productName", value.getProductName());
        record.put("transactionDate", value.getTransactionDate());

        return record;
    }
}
