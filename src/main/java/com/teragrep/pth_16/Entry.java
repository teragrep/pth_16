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

import com.teragrep.pth_15.DPLExecutor;
import com.teragrep.pth_15.DPLExecutorFactory;
import com.teragrep.pth_15.DPLExecutorResult;
import com.typesafe.config.Config;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.FileNotFoundException;
import java.lang.reflect.InvocationTargetException;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;

public class Entry {

    public static void main(String[] args) throws FileNotFoundException {
        File configFile = new File("interpreter.json");

        InterpreterSettingsConfig interpreterSettingsConfig = new InterpreterSettingsConfig(configFile);

        Config config = interpreterSettingsConfig.getConfig();

        String applicationName = "com.teragrep.pth_16.Entry";
        String lines = "| makeresults count=1";
        String queryId = UUID.randomUUID().toString();
        String noteId = "";
        String paragraphId = "";

        BiConsumer<Dataset<Row>, Boolean> batchHandler = (rowDataset, aggsUsed) -> {
            rowDataset.show(false);
        };

        final DPLExecutor dplExecutor;
        try {
            dplExecutor = new DPLExecutorFactory("com.teragrep.pth_10.executor.DPLExecutorImpl", config).create();
        }
        catch (
                ClassNotFoundException | NoSuchMethodException | InvocationTargetException | InstantiationException
                | IllegalAccessException e
        ) {
            throw new RuntimeException("Error initializing DPLExecutor implementation", e);
        }

        SparkSession sparkSession = SparkSession.builder().appName(applicationName).getOrCreate();

        try {
            final DPLExecutorResult executorResult = dplExecutor
                    .interpret(batchHandler, sparkSession, queryId, noteId, paragraphId, lines);

            /*
            final InterpreterResult.Code code;
            if (executorResult.code().equals(DPLExecutorResult.Code.SUCCESS)) {
                code = Code.SUCCESS;
            } else if (executorResult.code().equals(DPLExecutorResult.Code.INCOMPLETE)) {
                code = DPLExecutorResult.Code.INCOMPLETE;
            } else if (executorResult.code().equals(DPLExecutorResult.Code.KEEP_PREVIOUS_RESULT)) {
                code = Code.KEEP_PREVIOUS_RESULT;
            } else {
                code = Code.ERROR;
            }
             */

            System.out
                    .println(
                            "executorResult code <" + executorResult.code() + "> message <" + executorResult.message()
                                    + ">"
                    );

        }
        catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

}
