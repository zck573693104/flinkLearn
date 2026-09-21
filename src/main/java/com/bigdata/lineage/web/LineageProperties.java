package com.bigdata.lineage.web;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** WebUI 的扫描配置，键见 {@code application.yml} 的 {@code lineage.*}。 */
@ConfigurationProperties(prefix = "lineage")
public class LineageProperties {

    /** 语料目录；相对路径按进程工作目录解析 */
    private String scanDir = "sql";

    /** 启动时是否扫一遍：关掉它能单独起服务验证静态页 */
    private boolean rescanOnStart = true;

    public String getScanDir() {
        return scanDir;
    }

    public void setScanDir(String scanDir) {
        this.scanDir = scanDir;
    }

    public boolean isRescanOnStart() {
        return rescanOnStart;
    }

    public void setRescanOnStart(boolean rescanOnStart) {
        this.rescanOnStart = rescanOnStart;
    }
}
