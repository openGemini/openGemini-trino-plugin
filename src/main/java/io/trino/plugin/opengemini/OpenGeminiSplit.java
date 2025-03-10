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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class OpenGeminiSplit
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = toIntExact(ClassLayout.parseClass(OpenGeminiSplit.class).instanceSize());

    private final boolean remotelyAccessible;
    private final List<HostAddress> addresses;

    @JsonCreator
    public OpenGeminiSplit(@JsonProperty("addresses") List<HostAddress> addresses)
    {
        this.remotelyAccessible = true;
        this.addresses = ImmutableList.copyOf(requireNonNull(addresses, "addresses is null"));
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        // only http or https is remotely accessible
        return remotelyAccessible;
    }

    @JsonProperty
    @Override
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + estimatedSizeOf(addresses, HostAddress::getRetainedSizeInBytes);
    }
}
