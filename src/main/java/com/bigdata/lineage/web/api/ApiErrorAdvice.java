package com.bigdata.lineage.web.api;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.io.IOException;

/** 把异常翻译成统一信封，同时把 HTTP 状态码对齐：前端看 body 也看得出是参数错还是服务错。 */
@Slf4j
@RestControllerAdvice(basePackages = "com.bigdata.lineage.web.api")
public class ApiErrorAdvice {

    /** 业务性可预期错误（表不存在、参数非法、目录找不到）都是 400 */
    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<ApiResponse> badRequest(IllegalArgumentException e) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(ApiResponse.fail(e.getMessage()));
    }

    /** 主动抛出的 IOException（重扫时目录不可读）算请求失败，不是服务坏了 */
    @ExceptionHandler(IOException.class)
    public ResponseEntity<ApiResponse> ioFailed(IOException e) {
        log.warn("读写失败", e);
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(ApiResponse.fail("目录读写失败：" + e.getMessage()));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiResponse> unexpected(Exception e) {
        log.error("接口处理失败", e);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(ApiResponse.fail("服务内部错误：" + e.getClass().getSimpleName()));
    }
}
