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

import io.airlift.log.Logger;
import io.airlift.units.Duration;
import org.influxdb.dto.QueryResult;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class OpenGeminiQueryResultIterator
{
    private static final Logger log = Logger.get(OpenGeminiQueryResultIterator.class);

    private final BlockingQueue<QueryResult> queue;
    private final Duration pollTimeout;

    private List<QueryResult.Result> results;
    private int resultCursor;

    private List<QueryResult.Series> series = new ArrayList<>();
    private int seriesCursor;

    private List<List<Object>> values = new ArrayList<>();
    private int valueCursor;

    private List<Object> row = new ArrayList<>();

    public OpenGeminiQueryResultIterator(BlockingQueue<QueryResult> queue, Duration timeout)
    {
        this.queue = queue;
        this.pollTimeout = timeout;

        results = poll();
        if (results != null && !results.isEmpty()) {
            series = nextResult().getSeries();
        }
        if (series != null && !series.isEmpty()) {
            values = nextSeries().getValues();
        }
    }

    // return one chunk results by queue.poll normally, otherwise return null
    private List<QueryResult.Result> poll()
    {
        QueryResult rs = null;
        try {
            rs = this.queue.poll(pollTimeout.toMillis(), TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException e) {
            log.error("exception while queue.poll: %s", e);
            throw new RuntimeException(e);
        }
        if (rs == null) {
            log.error("QueryResult from queue.poll is null");
            return null;
        }
        if (rs.hasError()) {
            String error = rs.getError();
            // all results have been polled if error is DONE
            if (error.equals("DONE")) {
                log.info("QueryResult from queue.poll DONE");
                return null;
            }
            log.error("QueryResult from queue.poll has error: %s", error);
            throw new RuntimeException(error);
        }
        return rs.getResults();
    }

    // return current QueryResult.Result from List<QueryResult.Result> and cursor++
    private QueryResult.Result nextResult()
    {
        QueryResult.Result result = results.get(resultCursor);
        if (result.hasError()) {
            throw new RuntimeException(result.getError());
        }
        resultCursor += 1;
        return result;
    }

    // return one row from Series.values and cursor++
    private List<Object> nextRow()
    {
        List<Object> row = values.get(valueCursor);
        valueCursor += 1;
        return row;
    }

    // return current QueryResult.Series from List<QueryResult.Series> and cursor++
    private QueryResult.Series nextSeries()
    {
        QueryResult.Series s = series.get(seriesCursor);
        seriesCursor += 1;
        return s;
    }

    public boolean hasNext()
    {
        if (values.isEmpty()) {
            return false;
        }

        if (valueCursor < values.size()) {
            row = nextRow();
            return true;
        }

        valueCursor = 0;
        if (seriesCursor < series.size()) {
            values = nextSeries().getValues();
            row = nextRow();
            return true;
        }

        seriesCursor = 0;
        if (resultCursor < results.size()) {
            series = nextResult().getSeries();
            values = nextSeries().getValues();
            row = nextRow();
            return true;
        }

        resultCursor = 0;
        results = poll();
        if (results != null && !results.isEmpty()) {
            series = nextResult().getSeries();
            values = nextSeries().getValues();
            row = nextRow();
            return true;
        }
        return false;
    }

    public List<Object> getRow()
    {
        return row;
    }
}
