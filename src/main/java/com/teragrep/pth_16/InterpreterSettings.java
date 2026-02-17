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

import java.io.File;

import jakarta.json.*;

import java.io.FileReader;
import java.io.FileNotFoundException;

public final class InterpreterSettings {

    private final File configFile;

    public InterpreterSettings(final File configFile) {
        this.configFile = configFile;
    }

    public InterpreterProperties interpreterProperties(final String interpreterName) throws FileNotFoundException {

        try (JsonReader reader = Json.createReader(new FileReader(configFile))) {
            final JsonStructure jsonStructure = reader.read();

            if (!jsonStructure.getValueType().equals(JsonValue.ValueType.OBJECT)) {
                throw new IllegalArgumentException("config does not contain a JSON object");
            }

            final JsonObject jsonObject = jsonStructure.asJsonObject();

            final String interpreterSettingsKey = "interpreterSettings";
            if (!jsonObject.containsKey(interpreterSettingsKey)) {
                throw new IllegalArgumentException(
                        "config does not contain a JSON object with key <" + interpreterSettingsKey + ">"
                );
            }

            final JsonObject interpreterSettings = jsonObject.getJsonObject(interpreterSettingsKey);

            if (!interpreterSettings.containsKey(interpreterName)) {
                throw new IllegalArgumentException(
                        "config does not contain a <" + interpreterSettingsKey + "> object with key <" + interpreterName
                                + ">"
                );
            }

            final JsonObject interpreterObject = interpreterSettings.getJsonObject(interpreterName);

            if (!interpreterObject.getValueType().equals(JsonValue.ValueType.OBJECT)) {
                throw new IllegalArgumentException(
                        "config does not contain a <" + interpreterSettingsKey + "> object with key <" + interpreterName
                                + "> as a JSON object"
                );
            }

            final String propertieskey = "properties";
            if (!interpreterObject.containsKey(propertieskey)) {
                throw new IllegalArgumentException(
                        "config does not contain a <" + interpreterSettingsKey + "> object with key <" + interpreterName
                                + "> with key <" + propertieskey + ">"
                );
            }

            final JsonObject propertiesObject = interpreterObject.getJsonObject(propertieskey);

            if (!propertiesObject.getValueType().equals(JsonValue.ValueType.OBJECT)) {
                throw new IllegalArgumentException(
                        "config does not contain a <" + interpreterSettingsKey + "> object with key <" + interpreterName
                                + "> with key <" + propertieskey + "> as a JSON object"
                );
            }

            return new InterpreterProperties(propertiesObject);
        }
    }
}
