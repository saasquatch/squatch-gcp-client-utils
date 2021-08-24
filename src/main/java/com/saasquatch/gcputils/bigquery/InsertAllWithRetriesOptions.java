package com.saasquatch.gcputils.bigquery;

import java.time.Duration;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import javax.annotation.Nonnull;

public final class InsertAllWithRetriesOptions {

  final int retryCount;
  final IntFunction<Duration> backoffFunction;
  final Consumer<Duration> insertAllElapsedTimeConsumer;

  private InsertAllWithRetriesOptions(int retryCount,
      @Nonnull IntFunction<Duration> backoffFunction,
      @Nonnull Consumer<Duration> insertAllElapsedTimeConsumer) {
    this.retryCount = retryCount;
    this.backoffFunction = backoffFunction;
    this.insertAllElapsedTimeConsumer = insertAllElapsedTimeConsumer;
  }

  public static final class Builder {

    private int retryCount = 1;
    private IntFunction<Duration> backoffFunction = ignored -> Duration.ZERO;
    private Consumer<Duration> insertAllElapsedTimeConsumer = ignored -> {
    };

    private Builder() {
    }

    public Builder setRetryCount(int retryCount) {
      if (retryCount < 1) {
        throw new IllegalArgumentException();
      }
      this.retryCount = retryCount;
      return this;
    }

    public Builder setBackoffFunction(@Nonnull IntFunction<Duration> backoffFunction) {
      this.backoffFunction = Objects.requireNonNull(backoffFunction);
      return this;
    }

    public Builder setInsertAllElapsedTimeConsumer(
        @Nonnull Consumer<Duration> insertAllElapsedTimeConsumer) {
      this.insertAllElapsedTimeConsumer = Objects.requireNonNull(insertAllElapsedTimeConsumer);
      return this;
    }

    public InsertAllWithRetriesOptions build() {
      return new InsertAllWithRetriesOptions(retryCount, backoffFunction,
          insertAllElapsedTimeConsumer);
    }

  }
}
