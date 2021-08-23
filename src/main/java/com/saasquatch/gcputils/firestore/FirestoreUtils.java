package com.saasquatch.gcputils.firestore;

import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.Query;
import com.google.cloud.firestore.QueryDocumentSnapshot;
import com.google.cloud.firestore.QuerySnapshot;
import com.google.cloud.firestore.WriteBatch;
import com.google.common.base.Utf8;
import com.saasquatch.gcputils.GcpUtils;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Maybe;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nonnull;
import org.reactivestreams.Publisher;

public final class FirestoreUtils {

  private FirestoreUtils() {
  }

  private static final int
      QUERY_BATCH_SIZE = 1000,
      WRITE_BATCH_SIZE = 500;

  public static boolean isValidId(@Nonnull String s) {
    return !isInvalidId(s);
  }

  public static boolean isInvalidId(@Nonnull String s) {
    return s.equals(".") || s.equals("..")
        || s.indexOf('/') >= 0
        || (s.startsWith("__") && s.endsWith("__"))
        || Utf8.encodedLength(s) > 1500;
  }

  /**
   * Get the results of a Query.
   */
  public static Publisher<QueryDocumentSnapshot> getQueryDocumentSnapshots(@Nonnull Query query) {
    return Flowable.fromPublisher(getQuerySnapshots(query, QUERY_BATCH_SIZE))
        .concatMapIterable(snapshot -> snapshot);
  }

  public static Publisher<QuerySnapshot> getQuerySnapshots(@Nonnull Query query,
      int queryBatchSize) {
    final AtomicReference<DocumentSnapshot> lastSnapshotRef = new AtomicReference<>();
    return Flowable.just(true) // placeholder
        .repeat()
        .concatMap(ignored -> {
          final DocumentSnapshot lastSnapshot = lastSnapshotRef.get();
          final Query q;
          if (lastSnapshot == null) {
            q = query.limit(queryBatchSize);
          } else {
            q = query.startAfter(lastSnapshot).limit(queryBatchSize);
          }
          return Flowable.fromCallable(q::get)
              .map(GcpUtils::toCompletableFuture)
              .concatMapMaybe(Maybe::fromCompletionStage)
              .map(snapshot -> {
                final List<QueryDocumentSnapshot> documents = snapshot.getDocuments();
                if (!documents.isEmpty()) {
                  lastSnapshotRef.set(documents.get(documents.size() - 1));
                }
                return snapshot;
              });
        }, 1)
        .takeUntil(snapshot -> {
          //noinspection Convert2MethodRef
          return snapshot.isEmpty();
        })
        .filter(snapshot -> !snapshot.isEmpty());
  }

  /**
   * Delete documents in batches. Note that this method does NOT delete sub-collections.
   *
   * @return a {@link Publisher} with the number of documents deleted
   */
  public static Publisher<Long> shallowDeleteDocumentReferences(
      @Nonnull Publisher<DocumentReference> documentReferences) {
    return Flowable.fromPublisher(documentReferences)
        .buffer(WRITE_BATCH_SIZE)
        .filter(refList -> !refList.isEmpty())
        .concatMap(refList -> {
          final Firestore firestore = refList.get(0).getFirestore();
          final WriteBatch writeBatch = firestore.batch();
          refList.forEach(writeBatch::delete);
          return Flowable.fromCallable(writeBatch::commit)
              .map(GcpUtils::toCompletableFuture)
              .concatMapCompletable(Completable::fromCompletionStage)
              .andThen(Flowable.just((long) refList.size()));
        })
        .reduce(0L, Math::addExact)
        .toFlowable();
  }

}
