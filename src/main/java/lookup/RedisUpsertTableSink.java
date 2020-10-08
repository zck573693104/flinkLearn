package lookup;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sinks.UpsertStreamTableSink;
import org.apache.flink.types.Row;

public class RedisUpsertTableSink implements UpsertStreamTableSink<Row> {

    private final String[] fieldNames;
    private final TypeInformation[] fieldTypes;

    private final String ip;
    private final int port;

    private final String password;

    private final long cacheMaxSize;
    private final long cacheExpireMs;
    private transient Cache<String, String> cache;


    //异步客户端
    private transient RedisAsyncCommands<String, String> asyncClient;
    private transient RedisClient redisClient;

    public RedisUpsertTableSink(String[] fieldNames, TypeInformation[] fieldTypes, String ip, int port, String password, long cacheMaxSize, long cacheExpireMs) {
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        this.ip = ip;
        this.port = port;
        this.password = password;
        this.cacheMaxSize = cacheMaxSize;
        this.cacheExpireMs = cacheExpireMs;
    }

    @Override
    public void setKeyFields(String[] strings) {

    }

    @Override
    public void setIsAppendOnly(Boolean aBoolean) {

    }

    @Override
    public TypeInformation<Row> getRecordType() {
        return null;
    }

 

    @Override
    public TableSink<Tuple2<Boolean, Row>> configure(String[] strings, TypeInformation<?>[] typeInformations) {
        return null;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        return null;
    }

    public static class Builder {
        private String[] fieldNames;
        private TypeInformation[] fieldTypes;

        private String ip;
        private String password;

        public Builder setPassword(String password) {
            this.password = password;
            return this;
        }

        private int port;

        private long cacheMaxSize;
        private long cacheExpireMs;


        public Builder setFieldNames(String[] fieldNames) {
            this.fieldNames = fieldNames;
            return this;
        }

        public Builder setFieldTypes(TypeInformation[] fieldTypes) {
            this.fieldTypes = fieldTypes;
            return this;
        }

        public Builder setIp(String ip) {
            this.ip = ip;
            return this;
        }

        public Builder setPort(int port) {
            this.port = port;
            return this;
        }

        public Builder setCacheMaxSize(long cacheMaxSize) {
            this.cacheMaxSize = cacheMaxSize;
            return this;
        }

        public Builder setCacheExpireMs(long cacheExpireMs) {
            this.cacheExpireMs = cacheExpireMs;
            return this;
        }


        public RedisUpsertTableSink build() {
            return new RedisUpsertTableSink(fieldNames, fieldTypes, ip, port, password, cacheExpireMs,cacheMaxSize);
        }
    }
}
