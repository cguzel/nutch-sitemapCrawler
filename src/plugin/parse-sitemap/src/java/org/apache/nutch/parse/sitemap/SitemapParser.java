package org.apache.nutch.parse.sitemap;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

import crawlercommons.sitemaps.*;
import net.sourceforge.sitemaps.Sitemap;

import net.sourceforge.sitemaps.SitemapUrl;
import org.apache.avro.util.Utf8;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.Parse;

import org.apache.nutch.parse.ParseStatusCodes;
import org.apache.nutch.parse.Parser;
import org.apache.nutch.storage.ParseStatus;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.MimeUtil;

import javax.activation.MimeType;

	public class SitemapParser implements Parser {

		private Configuration conf;

		@Override
		public Parse getParse(String url, WebPage page) {
			Parse parse = null;
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

			Iterator i$;
			if (siteMap.isIndex()) {
				Collection<AbstractSiteMap> links = ((SiteMapIndex) siteMap).getSitemaps();
				Map<CharSequence,CharSequence> map= new HashMap<CharSequence, CharSequence>();
				for (AbstractSiteMap siteMapIndex : links) {
						page.getSitemaps().put(new Utf8(siteMapIndex.getUrl().toString()), new Utf8("parser"));
				}

			} else {
				Collection<SiteMapURL> links = ((SiteMap) siteMap).getSiteMapUrls();
				ArrayList<Outlink> outlinks = new ArrayList<Outlink>();

				for (SiteMapURL sitemapUrl : links) {
					try {
						outlinks.add(new Outlink(sitemapUrl.getUrl().toString(), "deneme"));
					} catch (MalformedURLException e) {
						e.printStackTrace();
					}
				}
				ParseStatus status = ParseStatus.newBuilder().build();
				status.setMajorCode((int) ParseStatusCodes.SUCCESS);
				parse = new Parse("","",outlinks.toArray(new Outlink[outlinks.size()]),status);

			}
			return parse;
		}


		public void setConf(Configuration conf) {
			this.conf = conf;
		}

		@Override
		public Configuration getConf() {
			return conf;
		}

		@Override
		public Collection<WebPage.Field> getFields() {
			return null;
		}
	}
