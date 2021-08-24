package com.saasquatch.gcputils.auth;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
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
  private final Cache<Set<String>, AccessToken> accessTokenCache;

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
    this.accessTokenCache = cacheBuilder.build();
  }

  public AccessToken getAccessToken(String... scopes) {
    final Set<String> scopesSet = ImmutableSet.copyOf(scopes);
    return accessTokenCache.get(scopesSet, ignored -> doGetFreshToken(null, scopesSet));
  }

  public AccessToken getDelegatedAccessToken(@Nonnull String delegateEmail, String... scopes) {
    final Set<String> scopesSet = ImmutableSet.copyOf(scopes);
    final Set<String> cacheKey = ImmutableSet.<String>builder()
        .add(delegateEmail).add(scopes).build();
    return accessTokenCache.get(cacheKey, ignored -> doGetFreshToken(delegateEmail, scopesSet));
  }

  private AccessToken doGetFreshToken(@Nullable String delegateEmail, Set<String> scopes) {
    final GoogleCredentials credentialsToUse;
    if (delegateEmail == null) {
      credentialsToUse = credentials;
    } else {
      credentialsToUse = credentials.createDelegated(delegateEmail);
    }
    try {
      return credentialsToUse.createScoped(scopes).refreshAccessToken();
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

}
