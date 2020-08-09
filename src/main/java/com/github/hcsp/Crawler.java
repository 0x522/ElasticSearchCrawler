package com.github.hcsp;

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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Crawler {
    public static void main(String[] args) throws IOException {
        //待处理的链接池子
        List<String> linkPool = new ArrayList<>();
        //已经被处理的链接池子
        Set<String> processedPool = new HashSet<>();
        linkPool.add("https://sina.cn");
        while (true) {
            if (linkPool.isEmpty()) {
                break;
            }
            //从数组列表的尾部拿数据最有效率，不用做元素移动
            String link = linkPool.remove(linkPool.size() - 1);

            //判断链接是否被处理过
            if (processedPool.contains(link)) {
                continue;
            }
            if (isInterestingPage(link)) {
                //这是我们感兴趣的，所以之后会被处理
                Document document = httpGetAndParseHtml(link);
                //选择所有的a标签
                //遍历所有的a标签，并将其href属性添加到未处理的链接池里
                Elements links = document.select("a");
                links.stream()
                        .map(aTag ->
                                aTag.attr("href"))
                        .forEach(linkPool::add);
//                for (Element aTag : links) {
//                    String href = aTag.attr("href");
//                    linkPool.add(href);
//                }
//                chooseAtagsAndStoreIntoDatabase(linkPool, document);
                storeIntoDatabaseIfItIsNewsPage(document, processedPool, link);
            } else {
                //这是我们不感兴趣的，查找下一个链接
                continue;
            }
        }
    }

    private static void storeIntoDatabaseIfItIsNewsPage(Document document, Set<String> processedPool, String link) {
        //假如这是一个新闻的详情页面，就存入数据库，否则就什么都不做
        Elements articleTags = document.select("article");
        if (!articleTags.isEmpty()) {
            for (Element articleTag : articleTags) {
                String title = articleTag.child(0).text();
                System.out.println(title);
            }
        }
        processedPool.add(link);
    }


    private static Document httpGetAndParseHtml(String link) throws IOException {
        CloseableHttpClient httpclient = HttpClients.createDefault();
        if (link.startsWith("//")) {
            link = "https:" + link;
            System.out.println(link);
        }
        HttpGet httpGet = new HttpGet(link);
        httpGet.addHeader("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36");
        try (CloseableHttpResponse response = httpclient.execute(httpGet)) {
            System.out.println(response.getStatusLine());
            HttpEntity entity = response.getEntity();
            String html = EntityUtils.toString(entity);
            //使用jsoup把请求字符串解析成文档对象模型
            return Jsoup.parse(html);
        }
    }

    public static boolean isInterestingPage(String link) {
        return (isNewsPage(link) || isHomePage(link)) && isNotLoginPage(link);
    }

    public static boolean isNewsPage(String link) {
        return link.contains("news.sina.cn");
    }

    public static boolean isNotLoginPage(String link) {
        return !link.contains("passport.sina.cn");
    }

    public static boolean isHomePage(String link) {
        return "https://sina.cn".equals(link);
    }
}
