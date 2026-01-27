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

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.File;

import com.typesafe.config.ConfigValueFactory;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonReader;
import jakarta.json.JsonValue;

import java.io.FileReader;
import java.io.FileNotFoundException;
import java.util.Map;

public final class InterpreterSettingsConfig {

    private final File configFile;

    public InterpreterSettingsConfig(final File configFile) {
        this.configFile = configFile;
    }

    public Config config() throws FileNotFoundException {

        Config config = ConfigFactory.empty();

        try (JsonReader reader = Json.createReader(new FileReader(configFile))) {
            final JsonObject jsonObject = reader.readObject();

            if (!jsonObject.getValueType().equals(JsonValue.ValueType.OBJECT)) {
                throw new IllegalArgumentException("config does not contain a JSON object");
            }

            final String interpreterSettingsKey = "interpreterSettings";
            if (!jsonObject.containsKey(interpreterSettingsKey)) {
                throw new IllegalArgumentException(
                        "config does not contain a JSON object with key <" + interpreterSettingsKey + ">"
                );
            }

            final JsonObject interpreterSettings = jsonObject.getJsonObject(interpreterSettingsKey);

            final String sparkKey = "spark";
            if (!interpreterSettings.containsKey(sparkKey)) {
                throw new IllegalArgumentException(
                        "config does not contain a <" + interpreterSettingsKey + "> object with key <" + sparkKey + ">"
                );
            }

            final JsonObject sparkObject = interpreterSettings.getJsonObject(sparkKey);

            if (!sparkObject.getValueType().equals(JsonValue.ValueType.OBJECT)) {
                throw new IllegalArgumentException(
                        "config does not contain a <" + interpreterSettingsKey + "> object with key <" + sparkKey
                                + "> as a JSON object"
                );
            }

            final String propertieskey = "properties";
            if (!sparkObject.containsKey(propertieskey)) {
                throw new IllegalArgumentException(
                        "config does not contain a <" + interpreterSettingsKey + "> object with key <" + sparkKey
                                + "> with key <" + propertieskey + ">"
                );
            }

            final JsonObject propertiesObject = sparkObject.getJsonObject(propertieskey);

            if (!propertiesObject.getValueType().equals(JsonValue.ValueType.OBJECT)) {
                throw new IllegalArgumentException(
                        "config does not contain a <" + interpreterSettingsKey + "> object with key <" + sparkKey
                                + "> with key <" + propertieskey + "> as a JSON object"
                );
            }

            for (Map.Entry<String, JsonValue> entry : propertiesObject.entrySet()) {
                if (entry.getKey().startsWith("dpl.") || entry.getKey().startsWith("fs.s3a.")) {
                    final String key = entry.getKey();

                    final JsonValue entryValue = entry.getValue();

                    if (!entryValue.getValueType().equals(JsonValue.ValueType.OBJECT)) {
                        throw new IllegalArgumentException("json key <[" + key + "]> does not refer to a json object");
                    }

                    final JsonObject entryValueObject = entryValue.asJsonObject();

                    final String valueKey = "value";
                    if (!entryValueObject.containsKey(valueKey)) {
                        throw new IllegalArgumentException(
                                "json key <[" + key + "]> does not refer to a json object with a key named <" + valueKey
                                        + ">"
                        );
                    }

                    final JsonValue jsonValue = entryValueObject.get(valueKey);

                    switch (jsonValue.getValueType()) {
                        case STRING:
                            config = config
                                    .withValue(key, ConfigValueFactory.fromAnyRef(entryValueObject.getString("value")));
                            break;
                        case NULL:
                            config = config.withValue(key, ConfigValueFactory.fromAnyRef(null));
                            break;
                        case TRUE:
                            config = config.withValue(key, ConfigValueFactory.fromAnyRef(true));
                            break;
                        case FALSE:
                            config = config.withValue(key, ConfigValueFactory.fromAnyRef(false));
                            break;
                        case NUMBER:
                            config = config
                                    .withValue(key, ConfigValueFactory.fromAnyRef(entryValueObject.getInt("value")));
                            break;
                        default:
                            throw new IllegalArgumentException(
                                    "json key <[" + key + "]> has object with key <" + valueKey
                                            + "> that has unsupported value type" + jsonValue.getValueType()
                            );
                    }
                }
            }
        }
        return config;
    }
}
