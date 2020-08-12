package com.github.hcsp;

import java.sql.SQLException;

public interface CrawlerDao {
    String getNextLinkAndThenDelete() throws SQLException;

    void insertProcessedLinkIntoDatabase(String link) throws SQLException;

    void insertToBeProcessedLinkIntoDatabase(String link) throws SQLException;

    void insertNewsIntoDatabase(String title, String content, String url) throws SQLException;

    boolean isLinkProcessed(String link) throws SQLException;
}
