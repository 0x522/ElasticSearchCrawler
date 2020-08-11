package com.github.hcsp;

import java.sql.SQLException;

public interface CrawlerDao {
    String getNextLink(String sql) throws SQLException;

    String getNextLinkAndThenDelete() throws SQLException;

    void deleteUrlsFromDatabase(String link) throws SQLException;

    void insertLinkIntoDatabase(String link, String sql) throws SQLException;

    void insertNewsIntoDatabase(String title, String content, String url) throws SQLException;

    boolean linkIsProcessed(String link) throws SQLException;
}
