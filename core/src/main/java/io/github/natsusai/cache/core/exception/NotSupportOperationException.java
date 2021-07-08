package io.github.natsusai.cache.core.exception;

/**
 * 不支持操作异常
 * </p>
 * @author Kurenai
 * @since 2020-03-12 10:31
 */

public class NotSupportOperationException extends RuntimeException {
  public NotSupportOperationException() {
    super("Do not support this operation!");
  }

  public NotSupportOperationException(String message) {
    super(message);
  }

  public NotSupportOperationException(String message, Throwable cause) {
    super(message, cause);
  }

  public NotSupportOperationException(Throwable cause) {
    super(cause);
  }

  public NotSupportOperationException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
