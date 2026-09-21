package com.bigdata.lineage.web.api;

/**
 * 所有端点共用的响应信封：{@code {success, data, message}}。
 *
 * <p>前端只认这一种形状，成功与否看 {@code success}，出错时 {@code message} 直接可展示，
 * 省掉一套按状态码分支的解析逻辑。HTTP 状态码仍由 {@link ApiErrorAdvice} 对齐。
 */
public final class ApiResponse {

    private final boolean success;
    private final Object data;
    private final String message;

    private ApiResponse(boolean success, Object data, String message) {
        this.success = success;
        this.data = data;
        this.message = message;
    }

    public static ApiResponse ok(Object data) {
        return new ApiResponse(true, data, null);
    }

    public static ApiResponse fail(String message) {
        return new ApiResponse(false, null, message);
    }

    public boolean isSuccess() {
        return success;
    }

    public Object getData() {
        return data;
    }

    public String getMessage() {
        return message;
    }
}
