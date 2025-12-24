package com.github.nexmark.flink.source;

import com.github.nexmark.flink.model.Event;
import com.github.nexmark.flink.utils.NexmarkUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class NexmarkEventDeserializationSchema implements DeserializationSchema<Event> {

    @Override
    public Event deserialize(byte[] message) throws IOException {
        if (message == null) {
            return null;
        }
        // 這行才是真正把 Kafka 的 bytes 轉成 Event 物件的關鍵
        return NexmarkUtils.MAPPER.readValue(message, Event.class);
    }

    @Override
    public boolean isEndOfStream(Event nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Event> getProducedType() {
        return TypeInformation.of(Event.class);
    }
}