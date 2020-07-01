package com.watermelon;

import com.alibaba.fastjson.JSON;
import com.watermelon.entity.User;
import com.watermelon.util.HtmlParseUtil;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@SpringBootTest
class ElasticSearchApplicationTests {

    @Autowired
    @Qualifier("getRestHighLevelClient")
    private RestHighLevelClient client;

    /**
     * 创建索引测试
     * @throws IOException
     */
    @Test
    void createIndex() throws IOException {
        //索引名称为test_index
        CreateIndexRequest request = new CreateIndexRequest("student");
        CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);
        System.out.println(response);
    }

    /**
     * 获取索引测试
     * @throws IOException
     */
    @Test
    void getIndex() throws IOException {
        SearchRequest request = new SearchRequest("test1");
        SearchResponse response = client.search(request,RequestOptions.DEFAULT);
        //获取索引，并将索引内的数据读取出来
        System.out.println("索引内doc：");
        for (SearchHit hit : response.getHits().getHits()){
            System.out.println(hit.getSourceAsString());
        }
        //输出数据条数
        System.out.println(response.getHits().getHits().length);
        System.out.println("数量："+response.getHits().getTotalHits());
        /*System.out.println(response.toString());*/
    }

    /**
     * 判断索引是否存在测试
     * @throws IOException
     */
    @Test
    void isIndexExists() throws IOException {
        GetIndexRequest request = new GetIndexRequest("test_index");
        Boolean isExists = client.indices().exists(request, RequestOptions.DEFAULT);
        System.out.println("test_index exists :"+isExists);
    }

    /**
     * 删除索引测试
     * @throws IOException
     */
    @Test
    void deleteIndex() throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest("test_index");
        AcknowledgedResponse response = client.indices().delete(request, RequestOptions.DEFAULT);
        System.out.println("delete response:"+response.isAcknowledged());
    }

    /**
     * 添加文档
     * @throws IOException
     */
    @Test
    void addDoc() throws IOException {
        User user = new User("啦啦啦大西瓜",20);
//        User user = new User("红豆",19);
//        User user = new User("墨黛",18);
        //创建请求
        IndexRequest request = new IndexRequest("test_index");
        //创建请求规则
        request.id("7");
        request.timeout(TimeValue.timeValueMillis(1000));
        //将数据放入请求
        request.source(JSON.toJSONString(user), XContentType.JSON);
        //发送请求
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);
        System.out.println(response);
        System.out.println(response.status());
    }

    /**
     * 获取文档(记录)，判断其是否存在
     * @throws IOException
     */
    @Test
    void isDocExists() throws IOException {
        GetRequest request = new GetRequest("test_index","1");
        //设置不反悔source的上下文
        request.fetchSourceContext(new FetchSourceContext(false));
        request.storedFields("_none_");

        boolean exists = client.exists(request,RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    /**
     * 获取文档(记录)的信息
     * @throws IOException
     */
    @Test
    void getDoc() throws IOException {
        GetRequest request = new GetRequest("test_index","1");
        GetResponse response = client.get(request,RequestOptions.DEFAULT);
        System.out.println(response.getSourceAsString());
        System.out.println("==========");
        System.out.println(response);
    }

    /**
     * 更新文档(记录)的信息
     * @throws IOException
     */
    @Test
    void updateDoc() throws IOException {
        //创建请求
        UpdateRequest request = new UpdateRequest("test_index","1");
        //设置请求
        request.timeout(TimeValue.timeValueMillis(1000));
        //将更新数据放入请求
        User user = new User("啦啦啦大西瓜",19);
        request.doc(JSON.toJSONString(user),XContentType.JSON);
        UpdateResponse response = client.update(request,RequestOptions.DEFAULT);
        System.out.println(response);
    }

    /**
     * 删除文档(记录)的信息
     * @throws IOException
     */
    @Test
    void deleteDoc() throws IOException {
        //创建请求
        DeleteRequest request = new DeleteRequest("test_index","1");
        request.timeout(TimeValue.timeValueMillis(1000));
        DeleteResponse response = client.delete(request,RequestOptions.DEFAULT);
        System.out.println(response);
        System.out.println("===========");
        System.out.println("status:"+response.status());
    }

    /**
     * 批量导入数据
     * @throws IOException
     */
    @Test
    void pourDoc() throws IOException {
        //创建请求
        BulkRequest request = new BulkRequest();
        request.timeout(TimeValue.timeValueMillis(10000));
        //伪造批量数据
        ArrayList<User> users = new ArrayList<>();
        users.add(new User("Alice",17));
        users.add(new User("Bayonetta",18));
        users.add(new User("Catherine",19));
        users.add(new User("Danna",18));
        users.add(new User("Ella",17));

        for (int i=0;i<users.size();i++){
            request.add(new IndexRequest("test_index")
                    .id((i+3)+"")
                    .source(JSON.toJSONString(users.get(i)),XContentType.JSON));
        }
        BulkResponse responses = client.bulk(request,RequestOptions.DEFAULT);
        System.out.println("isFailed: "+responses.hasFailures());
    }

    /**
     * 查询数据
     * @throws IOException
     */
    @Test
    void search() throws IOException {
        SearchRequest request = new SearchRequest("test_index");
        SearchSourceBuilder builder = new SearchSourceBuilder();
        //设置条件查询，TermQueryBuilder精确查询
        TermQueryBuilder queryBuilder = QueryBuilders.termQuery("age","20");
//        MatchAllQueryBuilder queryBuilder = QueryBuilders.matchAllQuery();
        builder.query(queryBuilder);
        builder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        request.source(builder);
        SearchResponse response = client.search(request,RequestOptions.DEFAULT);
        System.out.println(JSON.toJSONString(response.getHits()));
        System.out.println("===========");
        for (SearchHit hit : response.getHits().getHits()){
            System.out.println(hit.getSourceAsString());
        }

    }

    @Test
    void contextLoads() throws IOException {
//        Random random = new Random();
//        int a = random.nextInt(3)+1;
//        System.out.println(a);
        HtmlParseUtil.parse();
    }

}
