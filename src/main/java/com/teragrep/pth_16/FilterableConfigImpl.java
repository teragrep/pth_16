/*
 * Teragrep DPL Command-Line Executor (pth_16)
 * Copyright (C) 2026 Suomen Kanuuna Oy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 *
 * Additional permission under GNU Affero General Public License version 3
 * section 7
 *
 * If you modify this Program, or any covered work, by linking or combining it
 * with other code, such other code is not for that reason alone subject to any
 * of the requirements of the GNU Affero GPL version 3 as long as this Program
 * is the same Program as licensed from Suomen Kanuuna Oy without any additional
 * modifications.
 *
 * Supplemented terms under GNU Affero General Public License version 3
 * section 7
 *
 * Origin of the software must be attributed to Suomen Kanuuna Oy. Any modified
 * versions must be marked as "Modified version of" The Program.
 *
 * Names of the licensors and authors may not be used for publicity purposes.
 *
 * No rights are granted for use of trade names, trademarks, or service marks
 * which are in The Program if any.
 *
 * Licensee must indemnify licensors and authors for any liability that these
 * contractual assumptions impose on licensors and authors.
 *
 * To the extent this program is licensed as part of the Commercial versions of
 * Teragrep, the applicable Commercial License may apply to this file if you as
 * a licensee so wish it.
 */
package com.teragrep.pth_16;

import com.typesafe.config.*;

import java.time.Duration;
import java.time.Period;
import java.time.temporal.TemporalAmount;
import java.util.*;
import java.util.concurrent.TimeUnit;

public final class FilterableConfigImpl implements FilterableConfig {

    private final Config config;

    public FilterableConfigImpl(final Config config) {
        this.config = config;
    }

    @Override
    public ConfigObject root() {
        return config.root();
    }

    @Override
    public ConfigOrigin origin() {
        return config.origin();
    }

    @Override
    public Config withFallback(final ConfigMergeable other) {
        return config.withFallback(other);
    }

    @Override
    public Config resolve() {
        return config.resolve();
    }

    @Override
    public Config resolve(final ConfigResolveOptions options) {
        return config.resolve(options);
    }

    @Override
    public boolean isResolved() {
        return config.isResolved();
    }

    @Override
    public Config resolveWith(final Config source) {
        return config.resolveWith(source);
    }

    @Override
    public Config resolveWith(final Config source, final ConfigResolveOptions options) {
        return config.resolveWith(source, options);
    }

    @Override
    public void checkValid(final Config reference, final String ... restrictToPaths) {
        config.checkValid(reference, restrictToPaths);
    }

    @Override
    public boolean hasPath(final String path) {
        return config.hasPath(path);
    }

    @Override
    public boolean hasPathOrNull(final String path) {
        return config.hasPathOrNull(path);
    }

    @Override
    public boolean isEmpty() {
        return config.isEmpty();
    }

    @Override
    public Set<Map.Entry<String, ConfigValue>> entrySet() {
        return config.entrySet();
    }

    @Override
    public boolean getIsNull(final String path) {
        return config.getIsNull(path);
    }

    @Override
    public boolean getBoolean(final String path) {
        return config.getBoolean(path);
    }

    @Override
    public Number getNumber(final String path) {
        return config.getNumber(path);
    }

    @Override
    public int getInt(final String path) {
        return config.getInt(path);
    }

    @Override
    public long getLong(final String path) {
        return config.getLong(path);
    }

    @Override
    public double getDouble(final String path) {
        return config.getDouble(path);
    }

    @Override
    public String getString(final String path) {
        return config.getString(path);
    }

    @Override
    public <T extends Enum<T>> T getEnum(final Class<T> enumClass, final String path) {
        return config.getEnum(enumClass, path);
    }

    @Override
    public ConfigObject getObject(final String path) {
        return config.getObject(path);
    }

    @Override
    public Config getConfig(final String path) {
        return config.getConfig(path);
    }

    @Override
    public Object getAnyRef(final String path) {
        return config.getAnyRef(path);
    }

    @Override
    public ConfigValue getValue(final String path) {
        return config.getValue(path);
    }

    @Override
    public Long getBytes(final String path) {
        return config.getBytes(path);
    }

    @Override
    public ConfigMemorySize getMemorySize(final String path) {
        return config.getMemorySize(path);
    }

    @Deprecated
    @Override
    public Long getMilliseconds(final String path) {
        return config.getMilliseconds(path);
    }

    @Deprecated
    @Override
    public Long getNanoseconds(final String path) {
        return config.getNanoseconds(path);
    }

    @Override
    public long getDuration(final String path, final TimeUnit unit) {
        return config.getDuration(path, unit);
    }

    @Override
    public Duration getDuration(final String path) {
        return config.getDuration(path);
    }

    @Override
    public Period getPeriod(final String path) {
        return config.getPeriod(path);
    }

    @Override
    public TemporalAmount getTemporal(final String path) {
        return config.getTemporal(path);
    }

    @Override
    public ConfigList getList(final String path) {
        return config.getList(path);
    }

    @Override
    public List<Boolean> getBooleanList(final String path) {
        return config.getBooleanList(path);
    }

    @Override
    public List<Number> getNumberList(final String path) {
        return config.getNumberList(path);
    }

    @Override
    public List<Integer> getIntList(final String path) {
        return config.getIntList(path);
    }

    @Override
    public List<Long> getLongList(final String path) {
        return config.getLongList(path);
    }

    @Override
    public List<Double> getDoubleList(final String path) {
        return config.getDoubleList(path);
    }

    @Override
    public List<String> getStringList(final String path) {
        return config.getStringList(path);
    }

    @Override
    public <T extends Enum<T>> List<T> getEnumList(final Class<T> enumClass, final String path) {
        return config.getEnumList(enumClass, path);
    }

    @Override
    public List<? extends ConfigObject> getObjectList(final String path) {
        return config.getObjectList(path);
    }

    @Override
    public List<? extends Config> getConfigList(final String path) {
        return config.getConfigList(path);
    }

    @Override
    public List<? extends Object> getAnyRefList(final String path) {
        return config.getAnyRefList(path);
    }

    @Override
    public List<Long> getBytesList(final String path) {
        return config.getBytesList(path);
    }

    @Override
    public List<ConfigMemorySize> getMemorySizeList(final String path) {
        return config.getMemorySizeList(path);
    }

    @Deprecated
    @Override
    public List<Long> getMillisecondsList(final String path) {
        return config.getMillisecondsList(path);
    }

    @Deprecated
    @Override
    public List<Long> getNanosecondsList(final String path) {
        return config.getNanosecondsList(path);
    }

    @Override
    public List<Long> getDurationList(final String path, final TimeUnit unit) {
        return config.getDurationList(path, unit);
    }

    @Override
    public List<Duration> getDurationList(final String path) {
        return config.getDurationList(path);
    }

    @Override
    public Config withOnlyPath(final String path) {
        return config.withOnlyPath(path);
    }

    @Override
    public Config withoutPath(final String path) {
        return config.withoutPath(path);
    }

    @Override
    public Config atPath(final String path) {
        return config.atPath(path);
    }

    @Override
    public Config atKey(final String key) {
        return config.atKey(key);
    }

    @Override
    public Config withValue(final String path, final ConfigValue value) {
        return config.withValue(path, value);
    }

    @Override
    public Config startsWith(final Set<String> filters) {
        Config filteredConfig = ConfigFactory.empty();

        for (Map.Entry<String, ConfigValue> entry : config.entrySet()) {
            final String key = entry.getKey();

            for (final String filter : filters) {
                if (key.startsWith(filter)) {
                    filteredConfig = filteredConfig.withValue(key, entry.getValue());
                }
            }
        }
        return filteredConfig;

    }

    @Override
    public Config startsWith(final String ... filters) {
        Set<String> filterSet = new HashSet<>(Arrays.asList(filters));
        return startsWith(filterSet);
    }
}
