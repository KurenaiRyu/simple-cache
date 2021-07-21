package io.github.natsusai.cache.core.exception;

/**
 * @author Kurenai
 * @since 2021-07-21 11:20
 */
public class CacheRunTimeException extends RuntimeException {

    public CacheRunTimeException() {
    }

    public CacheRunTimeException(String message) {
        super(message);
    }

    public CacheRunTimeException(String message, Throwable cause) {
        super(message, cause);
    }

    public CacheRunTimeException(Throwable cause) {
        super(cause);
    }

    public CacheRunTimeException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
