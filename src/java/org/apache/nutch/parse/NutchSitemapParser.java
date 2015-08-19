package org.apache.nutch.parse;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

import crawlercommons.sitemaps.*;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.metadata.Metadata;

import org.apache.nutch.storage.ParseStatus;
import org.apache.nutch.storage.WebPage;

public class NutchSitemapParser {

  private Configuration conf;

  private static Collection<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();

  static {
    FIELDS.add(WebPage.Field.BASE_URL);
  }

  public NutchSitemapParse getParse(String url, WebPage page) {
    NutchSitemapParse nutchSitemapParse = null;
    SiteMapParser parser = new SiteMapParser();

    AbstractSiteMap siteMap = null;
    String contentType = page.getContentType().toString();
    try {
      siteMap = parser
          .parseSiteMap(contentType, page.getContent().array(),
              new URL(url));
    } catch (UnknownFormatException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    Map<Outlink, Metadata> outlinkMap = null;
    Iterator i$;
    if (siteMap.isIndex()) {
      Collection<AbstractSiteMap> links = ((SiteMapIndex) siteMap)
          .getSitemaps();
      for (AbstractSiteMap siteMapIndex : links) {
        page.getSitemaps().put(new Utf8(siteMapIndex.getUrl().toString()),
            new Utf8("parser"));
      }

    } else {
      Collection<SiteMapURL> links = ((SiteMap) siteMap).getSiteMapUrls();
      outlinkMap = new HashMap<Outlink, Metadata>();

      for (SiteMapURL sitemapUrl : links) {
        Metadata metadata = new Metadata();
        metadata
            .add("changeFrequency", sitemapUrl.getChangeFrequency().name());
        metadata.add("lastModified", Long.toString(
            sitemapUrl.getLastModified().getTime()));
        metadata.add("priority", Double.toString(sitemapUrl.getPriority()));
        try {
          outlinkMap.put(
              new Outlink(sitemapUrl.getUrl().toString(), "sitemap.outlink"),
              metadata);
        } catch (MalformedURLException e) {
          e.printStackTrace();
        }
      }
    }
    ParseStatus status = ParseStatus.newBuilder().build();
    status.setMajorCode((int) ParseStatusCodes.SUCCESS);
    nutchSitemapParse = new NutchSitemapParse(outlinkMap, status);
    return nutchSitemapParse;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public Configuration getConf() {
    return conf;
  }

  public Collection<WebPage.Field> getFields() {
    return FIELDS;
  }
}
