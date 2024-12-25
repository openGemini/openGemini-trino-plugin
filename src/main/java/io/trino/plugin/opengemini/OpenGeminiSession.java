/* Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.opengemini;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.collect.cache.NonEvictableLoadingCache;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import org.apache.http.ssl.SSLContexts;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import javax.inject.Inject;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static io.trino.collect.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.plugin.opengemini.OpenGeminiColumn.FIELD_KIND;
import static io.trino.plugin.opengemini.OpenGeminiColumn.TAG_KIND;
import static io.trino.plugin.opengemini.OpenGeminiColumn.TIME_KIND;
import static io.trino.plugin.opengemini.OpenGeminiQLUtils.toDoubleQuoted;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Objects.requireNonNull;

public class OpenGeminiSession
{
    private static final Logger log = Logger.get(OpenGeminiSession.class);

    private static final String SCHEMA_CACHE_KEY = "schema";

    private final Supplier<InfluxDB> db;
    // "schema" -> Set<schame>
    private final NonEvictableLoadingCache<String, Set<String>> schemaCache;
    // schema name -> Set<table name>
    private final NonEvictableLoadingCache<String, Set<String>> tableCache;
    // schema name -> default rp name
    private final NonEvictableLoadingCache<String, String> rpCache;
    // {schema, table} -> List<OpenGeminiColumn>
    private final NonEvictableLoadingCache<OpenGeminiColumnKey, List<OpenGeminiColumn>> columnCache;

    private final int chunkSize;
    private final Duration chunkPollTimeout;

    @Inject
    public OpenGeminiSession(OpenGeminiConfig config)
    {
        db = Suppliers.memoize(connect(config));

        long expireMillis = config.getCacheExpireDuration().toMillis();
        schemaCache = buildNonEvictableCache(
                CacheBuilder.newBuilder().expireAfterWrite(expireMillis, TimeUnit.MILLISECONDS),
                CacheLoader.from(this::loadSchemaNames));
        tableCache = buildNonEvictableCache(
                CacheBuilder.newBuilder().expireAfterWrite(expireMillis, TimeUnit.MILLISECONDS),
                CacheLoader.from(this::loadTableNames));
        rpCache = buildNonEvictableCache(
                CacheBuilder.newBuilder().expireAfterWrite(expireMillis, TimeUnit.MILLISECONDS),
                CacheLoader.from(this::loadDefaultRpName));
        columnCache = buildNonEvictableCache(
                CacheBuilder.newBuilder().expireAfterWrite(expireMillis, TimeUnit.MILLISECONDS),
                CacheLoader.from(this::loadColumns));

        chunkSize = config.getChunkSize();
        chunkPollTimeout = config.getChunkPollTimeout();
    }

    public static Supplier<InfluxDB> connect(OpenGeminiConfig config)
    {
        return () -> {
            OkHttpClient.Builder client = new OkHttpClient.Builder()
                    .connectTimeout(config.getConnectTimeout().toMillis(), TimeUnit.MILLISECONDS)
                    .writeTimeout(config.getWriteTimeout().toMillis(), TimeUnit.MILLISECONDS)
                    .readTimeout(config.getReadTimeout().toMillis(), TimeUnit.MILLISECONDS)
                    .retryOnConnectionFailure(true);
            if (!config.getKeepalive()) {
                client.addNetworkInterceptor(chain -> {
                    // use http short connection
                    Request newRequest = chain.request().newBuilder().header("Connection", "close").build();
                    return chain.proceed(newRequest);
                });
            }

            String url = config.getEndpoint().toString();
            if (url.startsWith("https://")) {
                client.sslSocketFactory(defaultSslSocketFactory(), defaultTrustManager());
                client.hostnameVerifier(noopHostnameVerifier());
            }
            log.info("sessin connect, url: %s, user: %s, keepalive: %s", url, config.getUsername(), config.getKeepalive());
            return InfluxDBFactory.connect(url, config.getUsername(), config.getPassword(), client);
        };
    }

    public void writePoints(BatchPoints batchPoints)
    {
        db.get().write(batchPoints);
    }

    public OpenGeminiQueryResultIterator queryResultByChunk(String schema, String sql)
    {
        BlockingQueue<QueryResult> queue = new LinkedBlockingQueue<>();
        db.get().query(new Query(sql, schema), chunkSize, new Consumer<QueryResult>() {
            @Override
            public void accept(QueryResult result)
            {
                queue.add(result);
            }
        });
        return new OpenGeminiQueryResultIterator(queue, chunkPollTimeout);
    }

    public QueryResult execute(String schema, String sql)
    {
        return db.get().query(new Query(sql, schema));
    }

    private Set<String> loadSchemaNames(String key)
    {
        if (key.equals(SCHEMA_CACHE_KEY)) {
            QueryResult resp = db.get().query(new Query("show databases"));
            log.info("debug for load schema names, resp: %s", resp.toString());
            return extractFromResp(resp);
        }
        return Collections.emptySet();
    }

    public Set<String> getSchemaNames()
    {
        try {
            return schemaCache.get(SCHEMA_CACHE_KEY);
        }
        catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private Set<String> extractFromResp(QueryResult resp)
    {
        ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        extractSeriesFromResp(resp).forEach(s -> {
            s.getValues().forEach(value -> {
                builder.add(value.get(0).toString());
            });
        });
        return builder.build();
    }

    private List<QueryResult.Series> extractSeriesFromResp(QueryResult resp)
    {
        if (resp.getResults() == null || resp.getResults().isEmpty()) {
            return Collections.emptyList();
        }
        List<QueryResult.Series> series = resp.getResults().get(0).getSeries();
        if (series == null || series.isEmpty()) {
            return Collections.emptyList();
        }
        return series;
    }

    private Set<String> loadTableNames(String schema)
    {
        requireNonNull(schema, "schema is null");
        QueryResult resp = db.get().query(new Query("show measurements", schema));
        log.info("debug for load table names from %s, resp: %s", schema, resp.toString());
        return extractFromResp(resp);
    }

    public Set<String> getTableNames(String schema)
    {
        try {
            return tableCache.get(schema);
        }
        catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private String loadDefaultRpName(String schema)
    {
        requireNonNull(schema, "schema is null");
        QueryResult resp = db.get().query(new Query("show retention policies", schema));
        log.info("debug for load default rp name from %s, resp: %s", schema, resp.toString());
        for (QueryResult.Series s : extractSeriesFromResp(resp)) {
            List<String> columns = s.getColumns();
            int nameIndex = -1;
            int defaultIndex = -1;
            for (int i = 0; i < columns.size(); i++) {
                if (columns.get(i).toLowerCase(Locale.ENGLISH).equals("name")) {
                    nameIndex = i;
                }
                if (columns.get(i).toLowerCase(Locale.ENGLISH).equals("default")) {
                    defaultIndex = i;
                }
            }
            if (nameIndex == -1 || defaultIndex == -1) {
                log.error("no name column or no default column for schema %s when get default rp name", schema);
                return null;
            }
            for (List<Object> value : s.getValues()) {
                if (value.get(defaultIndex).toString().toLowerCase(Locale.ENGLISH).equals("true")) {
                    return value.get(nameIndex).toString();
                }
            }
        }
        return null;
    }

    public String getDefaultRpName(String schema)
    {
        try {
            return rpCache.get(schema);
        }
        catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private List<OpenGeminiColumn> loadColumns(OpenGeminiColumnKey columnKey)
    {
        String schema = columnKey.schema();
        String tableName = columnKey.tableName();

        ImmutableList.Builder<OpenGeminiColumn> columns = ImmutableList.builder();

        // default time column
        columns.add(new OpenGeminiColumn("time", TimestampWithTimeZoneType.TIMESTAMP_TZ_NANOS, TIME_KIND));

        // show tag keys
        Set<String> tagKeys = extractFromResp(execute(schema, "show tag keys from " + toDoubleQuoted(tableName)));
        for (String key : tagKeys) {
            columns.add(new OpenGeminiColumn(key, createUnboundedVarcharType(), TAG_KIND));
        }

        // show field keys
        extractSeriesFromResp(execute(schema, "show field keys from " + toDoubleQuoted(tableName))).forEach(s -> {
            s.getValues().forEach(value -> {
                String key = value.get(0).toString();
                String type = value.get(1).toString();
                switch (type) {
                    case "float":
                        columns.add(new OpenGeminiColumn(key, DoubleType.DOUBLE, FIELD_KIND));
                        break;
                    case "integer", "unsigned":
                        columns.add(new OpenGeminiColumn(key, BigintType.BIGINT, FIELD_KIND));
                        break;
                    case "string":
                        columns.add(new OpenGeminiColumn(key, createUnboundedVarcharType(), FIELD_KIND));
                        break;
                    case "boolean":
                        columns.add(new OpenGeminiColumn(key, BooleanType.BOOLEAN, FIELD_KIND));
                        break;
                    default:
                        log.warn("unsupport type: " + type + ", key: " + key);
                }
            });
        });
        log.info("debug for load columns, schema: %s, table: %s", schema, tableName);
        return columns.build();
    }

    public OpenGeminiTable getTable(String schema, String tableName)
    {
        requireNonNull(schema, "schema is null");
        requireNonNull(tableName, "tableName is null");
        try {
            return new OpenGeminiTable(tableName, columnCache.get(new OpenGeminiColumnKey(schema, tableName)));
        }
        catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private static X509TrustManager defaultTrustManager()
    {
        return new X509TrustManager() {
            public X509Certificate[] getAcceptedIssuers()
            {
                return new X509Certificate[0];
            }

            public void checkClientTrusted(X509Certificate[] certs, String authType) {}

            public void checkServerTrusted(X509Certificate[] certs, String authType) {}
        };
    }

    private static SSLSocketFactory defaultSslSocketFactory()
    {
        try {
            SSLContext sslContext = SSLContexts.createDefault();

            sslContext.init(null, new TrustManager[] {
                    defaultTrustManager()
            }, new SecureRandom());
            return sslContext.getSocketFactory();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static HostnameVerifier noopHostnameVerifier()
    {
        return new HostnameVerifier() {
            @Override
            public boolean verify(final String s, final SSLSession sslSession)
            {
                return true; //true 表示使用ssl方式，但是不校验ssl证书，建议使用这种方式
            }
        };
    }
}
