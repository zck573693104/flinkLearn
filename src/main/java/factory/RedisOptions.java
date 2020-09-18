package factory;

import org.apache.flink.api.common.typeinfo.TypeInformation;

public class RedisOptions {


    private final String[] fieldNames;
    private final TypeInformation[] fieldTypes;

    private final String ip;
    private final int port;

    private final String password;

    private final long cacheMaxSize;
    private final long cacheExpireMs;


    public String[] getFieldNames() {
        return fieldNames;
    }

    public TypeInformation[] getFieldTypes() {
        return fieldTypes;
    }

    public String getIp() {
        return ip;
    }

    public int getPort() {
        return port;
    }

    public String getPassword() {
        return password;
    }

    public long getCacheMaxSize() {
        return cacheMaxSize;
    }

    public long getCacheExpireMs() {
        return cacheExpireMs;
    }

    public static Builder builder() {
        return new Builder();
    }


    public RedisOptions(String[] fieldNames, TypeInformation[] fieldTypes, String ip, int port, long cacheMaxSize, long cacheExpireMs, String password) {
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        this.ip = ip;
        this.port = port;
        this.cacheMaxSize = cacheMaxSize;
        this.cacheExpireMs = cacheExpireMs;
        this.password = password;
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


        public RedisOptions build() {
            return new RedisOptions(fieldNames, fieldTypes, ip, port, cacheMaxSize, cacheExpireMs,password);
        }
    }

}
