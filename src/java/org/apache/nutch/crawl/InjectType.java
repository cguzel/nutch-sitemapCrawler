package org.apache.nutch.crawl;

import org.apache.avro.util.Utf8;

public enum InjectType {
  INJECT("y"),
  SITEMAP_INJECT("s");

  Utf8 type;

  private InjectType(String type) {
    this.type = new Utf8(type);
  }

  public Utf8 getTypeString() {
    return new Utf8(type);
  }

}