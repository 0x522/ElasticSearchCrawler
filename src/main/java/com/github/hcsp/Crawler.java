package com.github.hcsp;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.sql.SQLException;
import java.util.stream.Collectors;


public class Crawler extends Thread {


    CrawlerDao dao;

    public Crawler(CrawlerDao dao) {
        this.dao = dao;
    }

    @Override
    public void run() {
        try {
            String link;
            //如果数据库里加载下一个链接，如果不是null就进行循环
            while ((link = dao.getNextLinkAndThenDelete()) != null) {
                //查询数据库，如果链接被处理过了，就继续下一条链接
                if (!dao.isLinkProcessed(link)) {//链接没有被处理过
                    //如果是我们感兴趣的链接，就继续处理
                    if (isInterestingPage(link)) {
                        //打印当前链接
                        System.out.println(link);
                        //解析url并拿到返回的页面DOM
                        Document document = httpGetAndParseHtml(link);
                        //把拿到的url插入数据库未处理的表中
                        putAllUrlsFromPageIntoDatabase(document);
                        //如果是新闻链接，就存储到数据库
                        storeIntoDatabaseIfItIsNewsPage(document, link);
                        //把已经处理过的链接插入数据库处理完成的表中
                        dao.insertProcessedLinkIntoDatabase(link);
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void putAllUrlsFromPageIntoDatabase(Document document) throws SQLException {
        //遍历所有的a标签，并将其href属性添加到数据库中
        Elements links = document.select("a");
        for (Element aTag : links) {
            String href = aTag.attr("href");
            if (href.startsWith("//")) {
                href = "https:" + href;
            }
            if (!href.toLowerCase().startsWith("javascript")) {
                dao.insertToBeProcessedLinkIntoDatabase(href);
            }
        }
    }


    private void storeIntoDatabaseIfItIsNewsPage(Document document, String link) throws SQLException {
        Elements articleTags = document.select("article");
        if (!articleTags.isEmpty()) {
            for (Element articleTag : articleTags) {
                String title = articleTag.child(0).text();
                String content = articleTag.select("p").stream().map(Element::text).collect(Collectors.joining("\n"));
                dao.insertNewsIntoDatabase(title, content, link);
            }
        }
    }


    private static Document httpGetAndParseHtml(String link) throws IOException {
        CloseableHttpClient httpclient = HttpClients.createDefault();
        HttpGet httpGet = new HttpGet(link);
        httpGet.addHeader("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.125 Safari/537.36");
        try (CloseableHttpResponse response = httpclient.execute(httpGet)) {
//            System.out.println(response.getStatusLine());
            HttpEntity entity = response.getEntity();
            String html = EntityUtils.toString(entity);
            //使用jsoup把请求字符串解析成文档对象模型
            return Jsoup.parse(html);
        }
    }

    private static boolean isInterestingPage(String link) {
        return (isNewsPage(link) || isHomePage(link)) && isNotLoginPage(link);
    }

    private static boolean isNewsPage(String link) {
        return link.contains("news.sina.cn");
    }

    private static boolean isNotLoginPage(String link) {
        return !link.contains("passport.sina.cn");
    }

    private static boolean isHomePage(String link) {
        return "https://sina.cn".equals(link);
    }
}
