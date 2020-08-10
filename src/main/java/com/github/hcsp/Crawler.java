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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class Crawler {
    public static final String USER_NAME = "root";
    public static final String PASSWORD = "root";

    @SuppressFBWarnings("DMI_CONSTANT_DB_PASSWORD")
    public static void main(String[] args) throws IOException, SQLException {
        //建立数据库链接
        Connection connection = DriverManager.getConnection("jdbc:h2:file:/E:/Crawler/news", USER_NAME, PASSWORD);
        while (true) {
            //从数据库加载即将被处理的链接并返回
            List<String> linkPool = loadUrlsFromDatabase(connection, "select links from LINKS_TO_BE_PROCESSED");
            if (linkPool.isEmpty()) {
                break;
            }
            //从数组列表的尾部拿数据最有效率，不用做元素移动
            String link = linkPool.remove(linkPool.size() - 1);

            //从池子中拿一个来处理，处理完后从数据库中删除
            deleteUrlsFromDatabase(connection, link);

            //查询数据库，如果链接没有被处理过，就处理它
            if (!linkIsProcessed(connection, link)) {
                //如果是我们感兴趣的链接，就继续
                if (isInterestingPage(link)) {
                    System.out.println(link);
                    //解析url并拿到返回的页面DOM
                    Document document = httpGetAndParseHtml(link);
                    putAllUrlsFromPageIntoDatabase(connection, document);
                    //如果是新闻链接，就存储到数据库
                    storeIntoDatabaseIfItIsNewsPage(document);
                    insertLinkIntoDatabase(connection, link, "insert into LINKS_ALREADY_PROCESSED (LINKS) values (?)");

                }
            }
        }
    }

    private static void putAllUrlsFromPageIntoDatabase(Connection connection, Document document) throws SQLException {
        //遍历所有的a标签，并将其href属性添加到数据库中
        Elements links = document.select("a");
        for (Element aTag : links) {
            String href = aTag.attr("href");
            insertLinkIntoDatabase(connection, href, "insert into LINKS_TO_BE_PROCESSED (LINKS) values (?)");
        }
    }

    private static void insertLinkIntoDatabase(Connection connection, String link, String sql) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, link);
            statement.executeUpdate();
        }
    }

    private static boolean linkIsProcessed(Connection connection, String link) throws SQLException {
        ResultSet resultSet = null;
        try (PreparedStatement statement = connection.prepareStatement("select LINKS from LINKS_ALREADY_PROCESSED where links = ?")) {
            statement.setString(1, link);
            resultSet = statement.executeQuery();
            while (resultSet.next()) {
                return true;
            }
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
        }
        return false;
    }

    private static void deleteUrlsFromDatabase(Connection connection, String link) throws SQLException {
        //从数据库里删除处理过的链接
        try (PreparedStatement statement = connection.prepareStatement("delete from LINKS_TO_BE_PROCESSED where links = ?")) {
            statement.setString(1, link);
            statement.executeUpdate();
        }
    }

    private static List<String> loadUrlsFromDatabase(Connection connection, String sql) throws SQLException {
        List<String> result = new ArrayList<>();
        try (PreparedStatement statement = connection.prepareStatement(sql); ResultSet resultSet = statement.executeQuery()) {
            //执行查询并拿到link结果集
            while (resultSet.next()) {
                //获取第一列的link链接并加入链接池中
                result.add(resultSet.getString(1));
            }
        }
        return result;
    }

    private static void storeIntoDatabaseIfItIsNewsPage(Document document) {
        Elements articleTags = document.select("article");
        if (!articleTags.isEmpty()) {
            for (Element articleTag : articleTags) {
                String title = articleTag.child(0).text();
                System.out.println(title);
            }
        }
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
