package com.company;

import com.couchbase.client.java.*;
import com.couchbase.client.java.document.*;
import com.couchbase.client.java.document.json.*;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.couchbase.client.java.query.SimpleN1qlQuery;
import java.util.Comparator;
import java.time.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class Main {

    public static final long START_INDEX = 3;
    public static final long END_INDEX = 9;
    public static final long SLICE_START = 30;
    public static final long SLICE_END = 90;

    public static void main(String[] args) {
	// write your code here
        Cluster cluster = CouchbaseCluster.create();
        cluster.authenticate("thirdeye", "thirdeye");
        Bucket bucket = cluster.openBucket("travel-sample");

        Comparator<Object> c = new Comparator<Object>() {
            public int compare(Object u1, Object u2) {
                return Long.compare(((Number)u1).longValue(), ((Number)u2).longValue());
            }
        };

        //try {
            //Instant start = Instant.now();

        final ExecutorService executor = Executors.newCachedThreadPool();

        LocalDateTime d = LocalDateTime.now().plusYears(2);

        Instant start = Instant.now();
//        for (int i = 1000; i > 0; i--) {
//            System.out.println(bucket.upsert(JsonDocument.create("lolol"+String.valueOf(i), JsonObject.create()
//               .put("date", d.plusDays(i).toString())
//               .put("index", String.valueOf(i))
//               .put("value", String.valueOf(i*10))
//               //.put("garbage", "garbage")
//            )));
//        }

        N1qlQueryResult result = bucket.query(
            N1qlQuery.simple("select * from `travel-sample` where date BETWEEN \"2022-10-02T14:40:10.400\" AND \"2023-07-03T10:10:18.615\"")
        );


        Instant end = Instant.now();

        System.out.println("Took " + Duration.between(start, end).toMillis() + " ms");

        for (N1qlQueryRow row : result) {
            System.out.println(row);
        }

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
