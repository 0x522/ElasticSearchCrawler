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

public class Crawler {
    public static final String USER_NAME = "root";
    public static final String PASSWORD = "root";

    @SuppressFBWarnings("DMI_CONSTANT_DB_PASSWORD")
    public static void main(String[] args) throws IOException, SQLException {
        //建立数据库链接
        Connection connection = DriverManager.getConnection("jdbc:h2:file:/E:/Crawler/news", USER_NAME, PASSWORD);
        String link;
        //如果数据库里加载下一个链接，如果不是null就进行循环
        while ((link = getNextLinkAndThenDelete(connection)) != null) {
            //查询数据库，如果链接被处理过了，就继续下一条链接
            if (linkIsProcessed(connection, link)) {
                continue;
            }
            //链接没有被处理过
            //如果是我们感兴趣的链接，就继续处理
            if (isInterestingPage(link)) {
                //打印当前链接
                System.out.println(link);
                //解析url并拿到返回的页面DOM
                Document document = httpGetAndParseHtml(link);
                //把拿到的url插入数据库未处理的表中
                putAllUrlsFromPageIntoDatabase(connection, document);
                //如果是新闻链接，就存储到数据库
                storeIntoDatabaseIfItIsNewsPage(document);
                //把已经处理过的链接插入数据库处理完成的表中
                insertLinkIntoDatabase(connection, link, "insert into LINKS_ALREADY_PROCESSED (LINKS) values (?)");
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

    private static String getNextLink(Connection connection, String sql) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(sql); ResultSet resultSet = statement.executeQuery()) {
            //执行查询并拿到link结果集
            while (resultSet.next()) {
                //获取第一列的link链接并加入链接池中
                return resultSet.getString(1);
            }
        }
        return null;
    }

    private static String getNextLinkAndThenDelete(Connection connection) throws SQLException {
        String link = getNextLink(connection, "select links from LINKS_TO_BE_PROCESSED limit 1");
        if (link != null) {
            deleteUrlsFromDatabase(connection, link);
        }
        return link;
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
