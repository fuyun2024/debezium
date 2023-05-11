package io.debezium.connector.oracle.logminer.processor;

@FunctionalInterface
public interface Handler<T> {
    void accept(T event, long eventsProcessed) throws InterruptedException;
}