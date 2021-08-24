package com.saasquatch.gcputils.auth;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public final class GoogleCredentialsAccessTokenGenerator {

  private final GoogleCredentials credentials;
  private final LoadingCache<AccessTokenCacheKey, AccessToken> accessTokenCache;

  private GoogleCredentialsAccessTokenGenerator(GoogleCredentials credentials, int cacheSize,
      Duration cacheDuration) {
    this.credentials = credentials;
    final Caffeine<Object, Object> cacheBuilder = Caffeine.newBuilder();
    if (cacheSize > 0) {
      cacheBuilder.maximumSize(cacheSize);
    }
    if (cacheDuration != null) {
      cacheBuilder.expireAfterWrite(cacheDuration.toMillis(), TimeUnit.MILLISECONDS);
    }
    this.accessTokenCache = cacheBuilder.build(this::doGetFreshToken);
  }

  public AccessToken getAccessToken(String... scopes) {
    return accessTokenCache.get(new AccessTokenCacheKey(null, ImmutableSet.copyOf(scopes)));
  }

  public AccessToken getDelegatedAccessToken(@Nonnull String delegateEmail, String... scopes) {
    return accessTokenCache.get(
        new AccessTokenCacheKey(delegateEmail, ImmutableSet.copyOf(scopes)));
  }

  private AccessToken doGetFreshToken(AccessTokenCacheKey cacheKey) {
    final GoogleCredentials credentialsToUse;
    if (cacheKey.delegateEmail == null) {
      credentialsToUse = credentials;
    } else {
      credentialsToUse = credentials.createDelegated(cacheKey.delegateEmail);
    }
    try {
      return credentialsToUse.createScoped(cacheKey.scopes).refreshAccessToken();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static final class Builder {

    private GoogleCredentials credentials;
    private int cacheSize;
    private Duration cacheDuration;

    private Builder() {
    }

    public Builder setCredentials(@Nonnull GoogleCredentials credentials) {
      this.credentials = Objects.requireNonNull(credentials);
      return this;
    }

    public Builder setCacheSize(int cacheSize) {
      this.cacheSize = cacheSize;
      return this;
    }

    public Builder setCacheDuration(@Nonnull Duration cacheDuration) {
      this.cacheDuration = Objects.requireNonNull(cacheDuration);
      return this;
    }

    public GoogleCredentialsAccessTokenGenerator build() {
      return new GoogleCredentialsAccessTokenGenerator(credentials, cacheSize, cacheDuration);
    }

  }

  private static final class AccessTokenCacheKey {

    final String delegateEmail;
    final Set<String> scopes;

    AccessTokenCacheKey(@Nullable String delegateEmail, @Nonnull Set<String> scopes) {
      this.delegateEmail = delegateEmail;
      this.scopes = scopes;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final AccessTokenCacheKey that = (AccessTokenCacheKey) o;
      return Objects.equals(delegateEmail, that.delegateEmail)
          && Objects.equals(scopes, that.scopes);
    }

    @Override
    public int hashCode() {
      return Objects.hash(delegateEmail, scopes);
    }
  }

}
