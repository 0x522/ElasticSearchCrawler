package com.github.hcsp;

import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.List;
import java.util.Random;

public class MockDataGenerator {
    private static void mockData(SqlSessionFactory sqlSessionFactory, int mockCount) {
        try (SqlSession session = sqlSessionFactory.openSession(ExecutorType.BATCH)) {
            final List<News> currrentNews = session.selectList("Mock.selectNews");
            mockCount = mockCount - currrentNews.size();
            Random random = new Random();
            try {
                while (mockCount-- > 0) {
                    int index = random.nextInt(currrentNews.size());
                    News newsToBeInserted = currrentNews.get(index);
                    Instant time = newsToBeInserted.getCreatedAt();
                    time = time.minusSeconds(random.nextInt(3600 * 24 * 365));
                    newsToBeInserted.setCreatedAt(time);
                    newsToBeInserted.setModifiedAt(time);
                    session.insert("Mock.insertNews", newsToBeInserted);
                    System.out.println("left :" + mockCount);
                    if (mockCount % 2000 == 0) {
                        session.flushStatements();
                    }
                }
                session.commit();
            } catch (Exception e) {
                session.rollback();
                throw new RuntimeException(e);
            }
        }

    }

    public static void main(String[] args) {
        SqlSessionFactory sqlSessionFactory;
        try {
            String resource = "db/Mybatis/config.xml";
            InputStream inputStream = Resources.getResourceAsStream(resource);
            sqlSessionFactory = new SqlSessionFactoryBuilder().build(inputStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        mockData(sqlSessionFactory, 1000000);
    }
}
