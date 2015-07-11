/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nutch.parse;

import java.io.*;
import java.net.MalformedURLException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.*;
import org.apache.nutch.metadata.Metadata;

/* An outgoing link from a page. */
public class Outlink implements Writable {

  private String toUrl;
  private String anchor;
  private Metadata metadata;

  public Outlink() {
  }

  public Outlink(String toUrl, String anchor) throws MalformedURLException {
    this.toUrl = toUrl;
    if (anchor == null)
      anchor = "";
    this.anchor = anchor;
    this.metadata = null;
  }

  public void readFields(DataInput in) throws IOException {
    toUrl = Text.readString(in);
    anchor = Text.readString(in);
    boolean hasMetadata = in.readBoolean();
    metadata.readFields(in);
  }

  /** Skips over one Outlink in the input. */
  public static void skip(DataInput in) throws IOException {
    Text.skip(in); // skip toUrl
    Text.skip(in); // skip anchor
    boolean hasMetadata = in.readBoolean();
    if (hasMetadata) {
      // skip metadata
      Metadata metadata = new Metadata();
      metadata.readFields(in);
    }
  }

  public void write(DataOutput out) throws IOException {
    Text.writeString(out, toUrl);
    Text.writeString(out, anchor);
    if (hasMetadata()) {
      out.writeBoolean(true);
      metadata.write(out);
    } else {
      out.writeBoolean(false);
    }
  }

  public static Outlink read(DataInput in) throws IOException {
    Outlink outlink = new Outlink();
    outlink.readFields(in);
    return outlink;
  }

  public String getToUrl() {
    return toUrl;
  }

  public String getAnchor() {
    return anchor;
  }

  public Metadata getMetadata() {
    return metadata;
  }

  public void setMetadata(Metadata metadata) {
    this.metadata = metadata;
  }

  public boolean hasMetadata() {
    return metadata != null && metadata.size()>0;
  }

  public boolean equals(Object o) {
    if (!(o instanceof Outlink))
      return false;
    Outlink other = (Outlink) o;
    return this.toUrl.equals(other.toUrl) && this.anchor.equals(other.anchor);
  }

  public String toString() {
    StringBuffer buffer = new StringBuffer("toUrl: ");
    buffer.append(toUrl);
    buffer.append(" anchor: ");
    buffer.append(anchor);
    if (hasMetadata()) {
      for (Map.Entry<String, String[]> e : metadata.getMetaData()) {
        buffer.append(" ");
        buffer.append(e.getKey());
        buffer.append(": ");
        buffer.append(e.getValue());
      }
    }
    return buffer.toString();
  }

}
