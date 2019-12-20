package com.company;

import com.couchbase.client.java.*;
import com.couchbase.client.java.auth.CertAuthenticator;
import com.couchbase.client.java.document.*;
import com.couchbase.client.java.document.json.*;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.couchbase.client.java.query.Select;
import com.couchbase.client.java.query.SimpleN1qlQuery;
import com.couchbase.client.java.query.Statement;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Comparator;
import java.time.*;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.CRC32;
import rx.Observable;
import static com.couchbase.client.java.query.Select.select;
import static com.couchbase.client.java.query.dsl.Expression.*;


public class Main {

    public static final long START_INDEX = 3;
    public static final long END_INDEX = 9;
    public static final long SLICE_START = 30;
    public static final long SLICE_END = 90;

    public static void main(String[] args) {

//        Cluster cluster = CouchbaseCluster.create("localhost:8091");
//        cluster.authenticate("thirdeye", "thirdeye");
//        Bucket bucket = cluster.openBucket("travel-sample");


      CouchbaseEnvironment env = DefaultCouchbaseEnvironment
          .builder()
          //.dnsSrvEnabled(true)
          .sslEnabled(true)
          .sslKeystoreFile("/export/content/lid/apps/thirdeye-webapp/i001/var/identity.p12")
          .sslKeystorePassword("work_around_jdk-6879539")
          .sslTruststoreFile("/etc/riddler/cacerts")
          .sslTruststorePassword("")
          .certAuthEnabled(true)
          .build();
//
//      String sssss = ".com";
//
//      try {
//        URI uri = new URI(sssss);
//        // The next workaround is to correctly parse urls that don't include any schema
//        if (uri.getHost() == null) {
//          uri = new URI("http://" + sssss);
//        }
//        String host = uri.getHost();
//        System.out.println(host);
//      } catch (URISyntaxException e) {
//
//      }

      //System.exit(1);

      List<String> hosts = new ArrayList<>();
//      hosts.add(null);
      hosts.add("http://localhost:8091");

        Cluster cluster = CouchbaseCluster.create(env, hosts);
        cluster.authenticate(CertAuthenticator.INSTANCE);

        Bucket bucket = cluster.openBucket("thirdeye-cache");

        bucket.upsert(JsonDocument.create("hi", JsonObject.create().put("hey", "hi")).);

/*      N1qlQueryResult result = bucket.query(
          N1qlQuery.simple("select * from `thirdeye-cache` where `bryan` = `ji`;")
      );

      System.out.println(result);*/

//      System.out.println(bucket.get("hi2"));
      //
//        bucket.upsert(JsonDocument.create("hi", JsonObject.create().put("third", "eye")));

        //System.out.println(bucket.get("hi"));
        //try {
            //Instant start = Instant.now();

        //final ExecutorService executor = Executors.newCachedThreadPool();

        //LocalDateTime d = LocalDateTime.now().plusYears(2);

        //Instant start = Instant.now();
//        for (int i = 1000; i > 0; i--) {
//            System.out.println(bucket.upsert(JsonDocument.create("lolol"+String.valueOf(i), JsonObject.create()
//               .put("date", d.plusDays(i).toString())
//               .put("index", String.valueOf(i))
//               .put("value", String.valueOf(i*10))
//               //.put("garbage", "garbage")
//            )));
//        }

        /*N1qlQueryResult result = bucket.query(
            N1qlQuery.simple("select * from `travel-sample` where date BETWEEN \"2022-10-02T14:40:10.400\" AND \"2023-07-03T10:10:18.615\"")
        );


        Instant end = Instant.now();

        System.out.println("Took " + Duration.between(start, end).toMillis() + " ms");

        for (N1qlQueryRow row : result) {
            System.out.println(row);
        }*/

        Instant start = Instant.now();

//        JsonDocument doc = JsonDocument.create("123456", 36000, JsonObject.create().put("name", "bryan"));
//
//        //((JsonObject)doc.content().get("dims")).put("123", "abc");
//
//        Observable<JsonDocument> a = bucket.async().get("123456");
//
//        a.subscribe(r -> System.out.println(r));
//
//        try {
//            Thread.sleep(100);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }

//        N1qlQueryResult r = bucket.query(N1qlQuery.simple("select * from `travel-sample` where asdasdsad = 123123123;"));
//
//        System.out.println(r.info().resultCount());

//        String dimensionKey = "2964793212";
//        String baseQuery = "SELECT time, `dims`.`" + dimensionKey + "` FROM `$bucket` WHERE metricId = $metricId AND time BETWEEN $start AND $end ORDER BY time ASC";
//
//        StringBuilder sb = new StringBuilder("SELECT time, `dims`.`");
//        sb.append(dimensionKey);
//        sb.append("` FROM `");
//        sb.append("travel-sample");
//        sb.append("` WHERE metricId = ");
//        sb.append(112869415);
//        sb.append(" AND time BETWEEN ");
//        sb.append(1568271600000L);
//        sb.append(" AND ");
//        sb.append(1568790000000L);
//        sb.append(" ORDER BY time ASC");
//
//        // NOTE: we subtract one from the end date because Couchabase's BETWEEN clause is inclusive on both sides
//        JsonObject parameters = JsonObject.create()
//            .put("bucket", "travel-sample")
//            .put("metricId", 112869415)
//            .put("start", 1568271600000L)
//            .put("end", 1568790000000L);

        //String query = "SELECT timestamp, `2810290851` FROM `travel-sample` WHERE metricId = 3572379 AND time BETWEEN 1566172800000 AND 1573977600000 ORDER BY time ASC";

//        String query = String.format("SELECT time, `%s` FROM `%s` WHERE metricId = %d AND time BETWEEN %d AND %d ORDER BY time ASC",
//            "2810290851", "travel-sample", 3572379, 1561618800000L, 1569481199999L);
//
//        Statement q = select("time", "999").from("travel-sample").where("me")

//        String query = "lol";
//
       //N1qlQueryResult queryResult = bucket.query(N1qlQuery.simple(query));
//
//        for (JsonObject error : queryResult.errors()) {
//            System.out.println(error.getString("msg"));
//        }

      //double a = bucket.get("123456").content().getDouble("metricId");

      //System.out.println(a);

//        System.out.println(queryResult);
//


        Instant end = Instant.now();

//      for (N1qlQueryRow row : queryResult) {
//        if (row == null)
//          System.out.println("wtf");
//        else
//          System.out.println(row);
//      }

        System.out.println("Took " + Duration.between(start, end).toMillis() + " ms");

        //JsonObject j = JsonObject.create()
        //    .put("name1", "jeffrey");

        //bucket.upsert(JsonDocument.create("123456", j));



        //System.out.println(Duration.between(a, b).toMillis());
            /*

            Map<String, String> testt = new HashMap<String, String>() {{
                put("a", "a");
                put("b", "b");
            }};

            JsonObject test = JsonObject.create()
                .put("name", "bryan3 :)")
                .put("map", testt);

            System.out.println(bucket.upsert(JsonDocument.create("maptest", test)));


            */

            //Instant end = Instant.now();

            //System.out.println(Duration.between(start, end));

            //Instant start = Instant.now();
            //JsonDocument r = bucket.get("test");

//            DocumentFragment df = bucket.lookupIn("abc123")
//                                    .get("metrics")
//                                    .execute();
//
//            System.out.println((Jsdf.content("metrics"));

            //DocumentFragment df2 = bucket.lookupIn("array_test3").get("123.times").get("123.values").execute();

            //List<Object> times = ((JsonArray)df.content("times")).toList();

            /*
            if (times.isEmpty()) {
                // make call to data source
                // write results to the cache async

            } else {
                Collection<MetricSlice> slices;
                if (((Number)times.get(0)).longValue() > SLICE_START) {
                    // some logic to fetch the slice from SLICE_START -> times.get(0) - granularity
                    // write results to cache async, can do async array_prepend
                }

                if (((Number)times.get(times.size() - 1)).longValue() < SLICE_END) {
                    // some logic to fetch the slice from  times.get(0) + granularity -> SLICE_END
                    // write results to cache async, can do async array_append
                }
            }

            List<Object> values = ((JsonArray)df.content("values")).toList();

            //System.out.println("indexOf: " + (end - start));

            long a = 10000000003L;
            long b = 10000000008L;

            // since there is a 1-1 mapping between "times" and "values", the starting
            // and ending indexes will the same for both. so, since we know that
            // timeseries data will always be in sorted ascending order, we can
            // use binarySearch on times array to find the target index.

            int start_idx = Collections.binarySearch(times, a, c);
            int end_idx = Collections.binarySearch(times, b, c);

            System.out.println(start_idx);
            System.out.println(end_idx);

            List<Long> real_list = times.subList(start_idx, end_idx + 1)
                                        .stream()
                                            .map(x -> ((Number)x).longValue())
                                                .collect(Collectors.toList());
            Instant end = Instant.now();

            */
        //} catch(Exception e) {
            //System.err.println(e.getMessage());
            //System.out.println("cache miss occurred, time slice < --- > was not found. fetching from data source");
        //} finally {
            //bucket.close();
            //cluster.disconnect();
        //}
    }
}
