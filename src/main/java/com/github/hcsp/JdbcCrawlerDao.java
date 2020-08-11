package com.github.hcsp;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class JdbcCrawlerDao implements CrawlerDao {
    private static final String USER_NAME = "root";
    private static final String PASSWORD = "root";
    private final Connection connection;

    public JdbcCrawlerDao() {
        //构造函数中建立数据库链接
        try {
            this.connection = DriverManager.getConnection("jdbc:h2:file:/E:/Crawler/news", USER_NAME, PASSWORD);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public String getNextLink(String sql) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(sql); ResultSet resultSet = statement.executeQuery()) {
            //执行查询并拿到link结果集
            while (resultSet.next()) {
                //获取第一列的link链接并加入链接池中
                return resultSet.getString(1);
            }
        }
        return null;
    }

    public String getNextLinkAndThenDelete() throws SQLException {
        String link = getNextLink("select links from LINKS_TO_BE_PROCESSED limit 1");
        if (link != null) {
            deleteUrlsFromDatabase(link);
        }
        return link;
    }

    public void deleteUrlsFromDatabase(String link) throws SQLException {
        //从数据库里删除处理过的链接
        try (PreparedStatement statement = connection.prepareStatement("delete from LINKS_TO_BE_PROCESSED where links = ?")) {
            statement.setString(1, link);
            statement.executeUpdate();
        }
    }

    public void insertLinkIntoDatabase(String link, String sql) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, link);
            statement.executeUpdate();
        }
    }

    public void insertNewsIntoDatabase(String title, String content, String url) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("insert into NEWS (TITLE, CONTENT, URL, CREATED_AT, MODIFIED_AT) VALUES ( ?,?,?,now(),now() )")) {
            statement.setString(1, title);
            statement.setString(2, content);
            statement.setString(3, url);
            statement.executeUpdate();
        }
    }

    public boolean linkIsProcessed(String link) throws SQLException {
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
}
