package com.bigdata.lineage.graph;

/**
 * 表级边：一条语句把 {@code from} 表读进 {@code to} 表。与字段级边同源于一次解析，
 * 分层只在表级图上算。
 */
public final class TableLink {

    private final String from;
    private final String to;
    private final String jobId;

    TableLink(String from, String to, String jobId) {
        this.from = from;
        this.to = to;
        this.jobId = jobId;
    }

    public String getFrom() {
        return from;
    }

    public String getTo() {
        return to;
    }

    public String getJobId() {
        return jobId;
    }
}
