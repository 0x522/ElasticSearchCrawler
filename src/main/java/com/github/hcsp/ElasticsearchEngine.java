package com.github.hcsp;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

@SuppressFBWarnings("DM_DEFAULT_ENCODING")
public class ElasticsearchEngine {
    public static void main(String[] args) throws IOException {
        while (true) {
            System.out.println("Please input a search keyword:");
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in));
            String keyword = bufferedReader.readLine();
            System.out.println("Your Keyword is: " + keyword);
            search(keyword);
        }
    }

    public static void search(String keyword) throws IOException {
        try (RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")))) {
            SearchRequest searchRequest = new SearchRequest("news");
            searchRequest.source(new SearchSourceBuilder().query(new MultiMatchQueryBuilder(keyword, "title", "content")));
            final SearchResponse result = client.search(searchRequest, RequestOptions.DEFAULT);
            result.getHits().forEach(hit -> System.out.println(hit.getSourceAsString()));
        }
    }
}
