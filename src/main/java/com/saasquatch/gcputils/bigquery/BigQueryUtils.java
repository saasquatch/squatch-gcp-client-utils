package com.saasquatch.gcputils.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.StringUtils;

public final class BigQueryUtils {

  /**
   * The recommended batch size for streaming insert requests.
   */
  public static final int STREAMING_INSERT_BATCH_SIZE = 500;
  private static final Set<String> PERSISTENT_ERROR_REASONS =
      ImmutableSet.of("invalid", "invalidQuery", "notImplemented");

  private BigQueryUtils() {
  }

  /**
   * Check if a {@link RuntimeException} thrown by {@link BigQuery#insertAll(InsertAllRequest)} is a
   * client side invalid error that cannot be retried.
   */
  @SuppressWarnings("RedundantIfStatement")
  public static boolean isClientSideInvalidInsertAllException(@Nonnull RuntimeException e) {
    if (StringUtils.containsAnyIgnoreCase(e.getMessage(), "timed out", "retrying limits", "502")) {
      // Server side intermittent
      return false;
    } else if (e.getCause() instanceof IOException) {
      // Client side or server side IO related problem
      return false;
    }
    return true;
  }

  /**
   * When one entry in the insert all request is invalid, the errors can get spammed with "stopped"
   * errors. This method filters out all the "stopped" errors.
   */
  public static Map<Long, List<BigQueryError>> filterRelevantInsertErrors(
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
  public static boolean containsPersistentErrors(
      @Nonnull Map<Long, List<BigQueryError>> insertErrors) {
    return insertErrors.values().stream()
        .filter(Objects::nonNull)
        .flatMap(Collection::stream)
        .filter(Objects::nonNull)
        .map(BigQueryError::getReason)
        .filter(Objects::nonNull)
        .anyMatch(PERSISTENT_ERROR_REASONS::contains);
  }

}
