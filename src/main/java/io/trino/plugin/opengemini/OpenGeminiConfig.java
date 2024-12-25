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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import io.trino.spi.function.Description;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

import java.net.URI;
import java.util.concurrent.TimeUnit;

public class OpenGeminiConfig
{
    private static final Duration DEFAULT_TIMEOUT = new Duration(10, TimeUnit.SECONDS);
    private static final Duration DEFAULT_CACHE_EXPIRE_DURATION = new Duration(60, TimeUnit.SECONDS);

    private URI endpoint;
    private String username;
    private String password;

    // used for OkHttpClient when connecting
    private Duration connectTimeout = DEFAULT_TIMEOUT;
    private Duration writeTimeout = DEFAULT_TIMEOUT;
    private Duration readTimeout = DEFAULT_TIMEOUT;

    // keepalive or not for OkHttpClient when connecting
    private boolean keepalive;

    // cache expire duration
    private Duration cacheExpireDuration = DEFAULT_CACHE_EXPIRE_DURATION;

    // used for chunk querying
    private int chunkSize;
    // used for poll chunk query result
    private Duration chunkPollTimeout = DEFAULT_TIMEOUT;

    @NotNull
    @Description("to connect database, like http://localhost:8086 or https://localhost:8086")
    public URI getEndpoint()
    {
        return endpoint;
    }

    @Config("opengemini.connect.endpoint")
    public OpenGeminiConfig setEndpoint(URI endpoint)
    {
        this.endpoint = endpoint;
        return this;
    }

    @NotBlank
    public String getUsername()
    {
        return username;
    }

    @Config("opengemini.connect.username")
    public OpenGeminiConfig setUsername(String username)
    {
        this.username = username;
        return this;
    }

    @NotBlank
    public String getPassword()
    {
        return password;
    }

    @Config("opengemini.connect.password")
    @ConfigSecuritySensitive
    public OpenGeminiConfig setPassword(String password)
    {
        this.password = password;
        return this;
    }

    @MinDuration("0s")
    public Duration getConnectTimeout()
    {
        return connectTimeout;
    }

    @Config("opengemini.connect.timeout")
    public OpenGeminiConfig setConnectTimeout(Duration timeout)
    {
        this.connectTimeout = timeout;
        return this;
    }

    @MinDuration("0s")
    public Duration getWriteTimeout()
    {
        return writeTimeout;
    }

    @Config("opengemini.write.timeout")
    public OpenGeminiConfig setWriteTimeout(Duration timeout)
    {
        this.writeTimeout = timeout;
        return this;
    }

    @MinDuration("0s")
    public Duration getReadTimeout()
    {
        return readTimeout;
    }

    @Config("opengemini.read.timeout")
    public OpenGeminiConfig setReadTimeout(Duration timeout)
    {
        this.readTimeout = timeout;
        return this;
    }

    public boolean getKeepalive()
    {
        return keepalive;
    }

    @Config("opengemini.connect.keepalive")
    public OpenGeminiConfig setKeepalive(boolean keepalive)
    {
        this.keepalive = keepalive;
        return this;
    }

    @MinDuration("0s")
    public Duration getCacheExpireDuration()
    {
        return cacheExpireDuration;
    }

    @Config("opengemini.cache.expire-duraion")
    public OpenGeminiConfig setCacheExpireDuration(Duration duration)
    {
        this.cacheExpireDuration = duration;
        return this;
    }

    @Min(0)
    public int getChunkSize()
    {
        return chunkSize;
    }

    @Config("opengemini.query.chunk-size")
    public OpenGeminiConfig setChunkSize(int size)
    {
        this.chunkSize = size;
        return this;
    }

    public Duration getChunkPollTimeout()
    {
        return chunkPollTimeout;
    }

    @Config("opengemini.query.chunk-poll-timeout")
    public OpenGeminiConfig setChunkPollTimeout(Duration timeout)
    {
        this.chunkPollTimeout = timeout;
        return this;
    }
}
