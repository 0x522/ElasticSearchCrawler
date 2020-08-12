package com.github.hcsp;

import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class MybatisCrawlerDao implements CrawlerDao {
    SqlSessionFactory sqlSessionFactory;

    public MybatisCrawlerDao() {
        try {
            String resource = "db/Mybatis/config.xml";
            InputStream inputStream = Resources.getResourceAsStream(resource);
            this.sqlSessionFactory = new SqlSessionFactoryBuilder().build(inputStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getNextLinkAndThenDelete() throws SQLException {
        try (SqlSession session = sqlSessionFactory.openSession(true)) {
            String url = session.selectOne("Crawler.selectNextAvailableLink");
            if (url != null) {
                session.delete("Crawler.deleteLink", url);
            }
            return url;
        }
    }

    @Override
    public void insertProcessedLinkIntoDatabase(String link) throws SQLException {
        Map<String, Object> map = new HashMap<>();
        map.put("tableName", "LINKS_ALREADY_PROCESSED");
        map.put("link", link);
        try (SqlSession session = sqlSessionFactory.openSession(true)) {
            session.insert("Crawler.insertLink", map);
        }

    }

    @Override
    public void insertToBeProcessedLinkIntoDatabase(String link) throws SQLException {
        Map<String, Object> map = new HashMap<>();
        map.put("tableName", "LINKS_TO_BE_PROCESSED");
        map.put("link", link);
        try (SqlSession session = sqlSessionFactory.openSession(true)) {
            session.insert("Crawler.insertLink", map);
        }

    }

    @Override
    public void insertNewsIntoDatabase(String title, String content, String url) throws SQLException {
        try (SqlSession session = sqlSessionFactory.openSession(true)) {
            session.insert("Crawler.insertNews", new News(title, content, url));
        }

    }

    @Override
    public boolean isLinkProcessed(String link) throws SQLException {
        try (SqlSession session = sqlSessionFactory.openSession()) {
            int count = session.selectOne("Crawler.selectLinkCount", link);
            return count != 0;
        }
    }
}
