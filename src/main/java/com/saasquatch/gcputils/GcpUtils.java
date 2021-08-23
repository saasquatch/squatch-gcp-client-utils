package com.saasquatch.gcputils;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.Timestamp;
import com.google.common.util.concurrent.MoreExecutors;
import java.time.Instant;
import java.util.Date;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public final class GcpUtils {

  private GcpUtils() {
  }

  /**
   * Convert an {@link ApiFuture} to a {@link CompletableFuture}.
   */
  public static <T> CompletableFuture<T> toCompletableFuture(@Nonnull ApiFuture<T> apiFuture) {
    return toCompletableFuture(apiFuture, MoreExecutors.directExecutor());
  }

  /**
   * Convert an {@link ApiFuture} to a {@link CompletableFuture}.
   */
  public static <T> CompletableFuture<T> toCompletableFuture(@Nonnull ApiFuture<T> apiFuture,
      @Nonnull Executor executor) {
    Objects.requireNonNull(apiFuture);
    final CompletableFuture<T> cf = new CompletableFuture<>();
    ApiFutures.addCallback(apiFuture,
        new ApiFutureCallback<T>() {
          @Override
          public void onFailure(Throwable t) {
            cf.completeExceptionally(t);
          }

          @Override
          public void onSuccess(T result) {
            cf.complete(result);
          }
        },
        executor);
    return cf;
  }

  /**
   * Convert a possible {@link Timestamp} to a {@link Date}
   */
  public static Date getDate(@Nullable Object o) {
    if (o == null) {
      return null;
    } else if (o instanceof Instant) {
      return Date.from((Instant) o);
    } else if (o instanceof Date) {
      return (Date) o;
    } else if (o instanceof Timestamp) {
      return ((Timestamp) o).toDate();
    } else if (o instanceof com.google.protobuf.Timestamp) {
      return Timestamp.fromProto((com.google.protobuf.Timestamp) o).toDate();
    }
    throw new IllegalArgumentException(String.format(Locale.ROOT,
        "Unable to get Date from object[%s]: %s", o.getClass(), o));
  }

  public static Instant getInstant(@Nullable Object o) {
    if (o == null) {
      return null;
    } else if (o instanceof Instant) {
      return (Instant) o;
    } else if (o instanceof Date) {
      return ((Date) o).toInstant();
    } else if (o instanceof Timestamp) {
      final Timestamp timestamp = (Timestamp) o;
      return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
    } else if (o instanceof com.google.protobuf.Timestamp) {
      final com.google.protobuf.Timestamp timestamp = (com.google.protobuf.Timestamp) o;
      return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
    }
    throw new IllegalArgumentException(String.format(Locale.ROOT,
        "Unable to get Date from object[%s]: %s", o.getClass(), o));
  }

}
