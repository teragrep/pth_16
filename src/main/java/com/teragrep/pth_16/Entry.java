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
import com.typesafe.config.ConfigValue;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class Entry {

    public static void main(final String[] args) throws FileNotFoundException {

        final File configFile = new File("interpreter.json");

        final InterpreterSettings interpreterSettings = new InterpreterSettings(configFile);

        final InterpreterProperties sparkProperties = interpreterSettings.interpreterProperties("spark");

        final Config sparkInterpterConfig = sparkProperties.config();

        final FilterableConfig filterableConfig = new FilterableConfigImpl(sparkInterpterConfig);

        // dpl
        final Set<String> dplConfigFilters = new HashSet<>();
        dplConfigFilters.add("dpl.");
        dplConfigFilters.add("fs.s3a.");
        final Config dplConfig = filterableConfig.startsWith(dplConfigFilters);

        // spark
        final Set<String> sparkConfigFilters = new HashSet<>();
        sparkConfigFilters.add("spark.");
        final Config sparkConfig = filterableConfig.startsWith(sparkConfigFilters);
        final SparkConf sparkConf = new SparkConf();

        for (Map.Entry<String, ConfigValue> sparkConfigEntry : sparkConfig.entrySet()) {
            String key = sparkConfigEntry.getKey();
            // ignore spark-submit configs such as spark.submit.deployMode
            if (!key.startsWith("spark.submit.")) {
                sparkConf.set(key, sparkConfigEntry.getValue().unwrapped().toString());
            }
        }

        final String applicationName = "com.teragrep.pth_16.Entry";

        final String prompt;
        if (args.length > 0) {
            prompt = args[0];
        }
        else {
            prompt = "| makeresults count=1 | eval _raw=\"Welcome to Teragrep®\"";
        }

        final SparkSession sparkSession = SparkSession
                .builder()
                .config(sparkConf)
                .appName(applicationName)
                .getOrCreate();

        final Query query = new Query(sparkSession, dplConfig, prompt);

        final List<String> rows = query.run();

        for (String string : rows) {
            System.out.println(string);
        }
    }

}
