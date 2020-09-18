package source;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;

public class RedisRowInputFormat extends RichInputFormat<Row, InputSplit> implements ResultTypeQueryable<Row> {

    private RowTypeInfo rowTypeInfo;
    private Object[][] parameterValues;

    private String[] fieldNames;
    private List<String> resultList;
    private TypeInformation[] fieldTypes;

    private String ip;
    private int port;

    private String password;

    private long cacheMaxSize;
    private long cacheExpireMs;
    private transient Cache<String, String> cache;


    //异步客户端
    private transient RedisAsyncCommands<String, String> asyncClient;
    private transient RedisClient redisClient;

    public static RedisRowInputFormatBuilder buildRedisInputFormat() {
        return new RedisRowInputFormatBuilder();
    }


    public void openInputFormat() throws IOException {
        RedisURI redisUri = RedisURI.builder()
                .withHost(ip)
                .withPort(port)
                .withPassword(password)
                .withTimeout(Duration.of(10, ChronoUnit.SECONDS))
                .build();
        redisClient = RedisClient.create(redisUri);
        asyncClient = redisClient.connect().async();
    }


    public void closeInputFormat() throws IOException {
        if (redisClient != null)
            redisClient.shutdown();

    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return this.rowTypeInfo;
    }

    @Override
    public void configure(Configuration configuration) {

    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics baseStatistics) throws IOException {
        return baseStatistics;
    }

    @Override
    public InputSplit[] createInputSplits(int minNumSplits) throws IOException {
        if (this.parameterValues == null) {
            return new GenericInputSplit[]{new GenericInputSplit(0, 1)};
        } else {
            GenericInputSplit[] ret = new GenericInputSplit[this.parameterValues.length];

            for(int i = 0; i < ret.length; ++i) {
                ret[i] = new GenericInputSplit(i, ret.length);
            }

            return ret;
        }
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
        return new DefaultInputSplitAssigner(inputSplits);
    }

    @Override
    public void open(InputSplit inputSplit) throws IOException {
        try {
            if (inputSplit != null && this.parameterValues != null) {
                for (int i = 0; i < this.parameterValues[inputSplit.getSplitNumber()].length; ++i) {
                    Object param = this.parameterValues[inputSplit.getSplitNumber()][i];
                    if (param instanceof String) {
                        asyncClient.get((String) param);

                    }
                }
                asyncClient.get("a").thenAccept(value -> this.resultList.add(value));
            }

        } catch (Exception e) {

        }
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return false;
    }

    @Override
    public Row nextRecord(Row row) throws IOException {
        return null;
    }


    @Override
    public void close() throws IOException {
        if (redisClient != null)
            redisClient.shutdown();
    }


    public static class RedisRowInputFormatBuilder {

        private final RedisRowInputFormat format = new RedisRowInputFormat();

        private String[] fieldNames;
        private TypeInformation[] fieldTypes;

        private String ip;
        private String password;

        public RedisRowInputFormatBuilder setPassword(String password) {
            this.password = password;
            return this;
        }

        private int port;

        private long cacheMaxSize;
        private long cacheExpireMs;


        public RedisRowInputFormatBuilder setFieldNames(String[] fieldNames) {
            this.fieldNames = fieldNames;
            return this;
        }

        public RedisRowInputFormatBuilder setFieldTypes(TypeInformation[] fieldTypes) {
            this.fieldTypes = fieldTypes;
            return this;
        }

        public RedisRowInputFormatBuilder setIp(String ip) {
            this.ip = ip;
            return this;
        }

        public RedisRowInputFormatBuilder setPort(int port) {
            this.port = port;
            return this;
        }

        public RedisRowInputFormatBuilder setCacheMaxSize(long cacheMaxSize) {
            this.cacheMaxSize = cacheMaxSize;
            return this;
        }

        public RedisRowInputFormatBuilder setCacheExpireMs(long cacheExpireMs) {
            this.cacheExpireMs = cacheExpireMs;
            return this;
        }


        public RedisRowInputFormat build() {
            return format;
        }
    }
}
