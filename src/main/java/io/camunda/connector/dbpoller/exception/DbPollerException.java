package io.camunda.connector.dbpoller.exception;

public class DbPollerException extends RuntimeException {

    public DbPollerException(String message) {
        super(message);
    }

    public DbPollerException(String message, Throwable cause) {
        super(message, cause);
    }
}

