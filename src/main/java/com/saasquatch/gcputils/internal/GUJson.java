package com.saasquatch.gcputils.internal;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import java.io.IOException;
import java.io.UncheckedIOException;

public final class GUJson {

  public static final ObjectMapper MAPPER = new JsonMapper();

  private GUJson() {}

  public static String stringify(Object o) {
    try {
      return MAPPER.writeValueAsString(o);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

}
