package Dezerializer;


import com.fasterxml.jackson.databind.ObjectMapper;
import dto.transaction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class JSONValueDeserializationSchema implements DeserializationSchema<transaction> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void open(InitializationContext context) throws Exception {
        DeserializationSchema.super.open(context);
    }

    @Override
    public transaction deserialize(byte[] bytes) throws IOException {
        return objectMapper.readValue(bytes,transaction.class);
    }

    @Override
    public boolean isEndOfStream(transaction transaction) {
        return false;
    }

    @Override
    public TypeInformation<transaction> getProducedType() {
        return TypeInformation.of(transaction.class);
    }
}
