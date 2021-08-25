package com.saasquatch.gcputils.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.TableId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.saasquatch.gcputils.internal.GUJson;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import javax.annotation.Nonnull;
import net.logstash.logback.marker.Markers;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BigQueryUtils {

  private static final Logger logger = LoggerFactory.getLogger(BigQueryUtils.class);

  /**
   * The recommended batch size for streaming insert requests.
   */
  public static final int STREAMING_INSERT_BATCH_SIZE = 500;
  private static final Set<String> PERSISTENT_ERROR_REASONS =
      ImmutableSet.of("invalid", "invalidQuery", "notImplemented");

  private BigQueryUtils() {
  }

  /**
   * When one entry in the insert all request is invalid, the errors can get spammed with "stopped"
   * errors. This method filters out all the "stopped" errors.
   */
  private static Map<Long, List<BigQueryError>> filterRelevantInsertErrors(
      @Nonnull Map<Long, List<BigQueryError>> insertErrors) {
    return insertErrors.entrySet().stream()
        .filter(errors -> errors.getValue().stream()
            .anyMatch(error -> !"stopped".equalsIgnoreCase(error.getReason())))
        .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  /**
   * Check whether the given {@link Map} of insert errors contains persistent errors that cannot be
   * retried.
   */
  private static boolean containsPersistentErrors(
      @Nonnull Map<Long, List<BigQueryError>> insertErrors) {
    return insertErrors.values().stream()
        .filter(Objects::nonNull)
        .flatMap(Collection::stream)
        .filter(Objects::nonNull)
        .map(BigQueryError::getReason)
        .filter(Objects::nonNull)
        .anyMatch(PERSISTENT_ERROR_REASONS::contains);
  }

  public static Publisher<Void> insertAllWithRetries(@Nonnull BigQuery bigQuery,
      @Nonnull InsertAllRequest insertAllRequest, @Nonnull InsertAllWithRetriesOptions options) {
    final TableId tableId = insertAllRequest.getTable();
    final BooleanSupplier action = () -> {
      final long t0 = System.nanoTime();
      final InsertAllResponse response;
      try {
        response = bigQuery.insertAll(insertAllRequest);
        // Do not record this in finally, because client side errors don't count
        options.insertAllElapsedTimeConsumer.accept(Duration.ofNanos(System.nanoTime() - t0));
      } catch (RuntimeException e) {
        if (StringUtils.containsAnyIgnoreCase(e.getMessage(),
            "timed out", "retrying limits", "502")) {
          logger.warn("Server side exception encountered during insertAll for table[{}]. "
              + "Attempting to retry.", tableId, e);
          return false;
        } else if (e.getCause() instanceof IOException) {
          logger.warn("Client side IOException encountered during insertAll for table[{}]. "
              + "Attempting to retry.", tableId, e);
          return false;
        }
        logger.error("Client side exception encountered during insertAll for table[{}]. "
            + "Not attempting to retry.", tableId, e);
        throw e;
      }
      final Map<Long, List<BigQueryError>> insertErrors = response.getInsertErrors();
      if (insertErrors == null || insertErrors.isEmpty()) {
        return true;
      }
      final Map<Long, List<BigQueryError>> relevantInsertErrors =
          filterRelevantInsertErrors(insertErrors);
      final String relevantInsertErrorsStringify = GUJson.stringify(relevantInsertErrors);
      if (containsPersistentErrors(insertErrors)) {
        final List<Map<String, Object>> rowsContent = insertAllRequest.getRows().stream()
            .map(InsertAllRequest.RowToInsert::getContent)
            .collect(ImmutableList.toImmutableList());
        logger.error(Markers.appendEntries(ImmutableMap.of(
                "rowsStringify", GUJson.stringify(rowsContent))),
            "Persistent error encountered during insertAll for table[{}]. Unable to retry. "
                + "insertErrors: {}", tableId, relevantInsertErrorsStringify);
        throw new RuntimeException(relevantInsertErrorsStringify);
      }
      logger.warn("Error encountered during insertAll for table[{}]. "
          + "Attempting to retry. insertErrors: {}", tableId, relevantInsertErrorsStringify);
      return false;
    };
    final int retryCount = options.retryCount;
    return Flowable.range(0, retryCount)
        .subscribeOn(Schedulers.io()).observeOn(Schedulers.io())
        .concatMap(retryIndex -> Flowable.fromCallable(action::getAsBoolean)
            .concatMap(insertResult -> {
              if (insertResult) {
                return Flowable.just(ObjectUtils.NULL);
              }
              final long backoffMillis = options.backoffFunction.apply(retryIndex).toMillis();
              if (backoffMillis == 0) {
                return Flowable.empty();
              }
              return Completable.timer(backoffMillis, TimeUnit.MILLISECONDS, Schedulers.io())
                  .toFlowable();
            }), 1)
        .take(1)
        .switchIfEmpty(Flowable.error(() -> new IllegalStateException(
            "insertAll failed after " + retryCount + " retries")))
        .ignoreElements()
        .toFlowable();
  }

}
