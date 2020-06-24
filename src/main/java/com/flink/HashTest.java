package com.flink;

import com.google.common.hash.Hashing;
import org.apache.commons.codec.digest.DigestUtils;

import java.nio.charset.StandardCharsets;

public class HashTest {

    public static String md5Test(String primaryKey) {
        return DigestUtils.md5Hex(primaryKey).substring(0, 4) + "_" + primaryKey;
    }

    public static String murmur3Test(String primaryKey) {
        return Hashing.murmur3_32().hashString(primaryKey, StandardCharsets.UTF_8).toString().substring(0, 4) +
            "_" + primaryKey;
    }
}