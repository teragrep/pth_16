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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

final public class Query {

    private final Logger LOGGER = LoggerFactory.getLogger(Query.class);

    private final SparkSession sparkSession;
    private final Config dplConfig;
    private final String queryId;
    private final String noteId;
    private final String paragraphId;
    private final String prompt;

    public Query(final SparkSession sparkSession, final Config dplConfig, final String prompt) {
        this(
                sparkSession,
                dplConfig,
                UUID.randomUUID().toString(),
                "pth_16-notebook-" + UUID.randomUUID(),
                "pth_16-paragraph-" + UUID.randomUUID(),
                prompt
        );
    }

    public Query(
            final SparkSession sparkSession,
            final Config dplConfig,
            final String queryId,
            final String noteId,
            final String paragraphId,
            final String prompt
    ) {
        this.sparkSession = sparkSession;
        this.dplConfig = dplConfig;
        this.queryId = queryId;
        this.noteId = noteId;
        this.paragraphId = paragraphId;
        this.prompt = prompt;
    }

    public List<String> run() {
        final AtomicReference<List<String>> rows = new AtomicReference<>(new ArrayList<>());

        final BiConsumer<Dataset<Row>, Boolean> batchHandler = (rowDataset, aggsUsed) -> {
            rows.set(rowDataset.toJSON().collectAsList());
        };

        final DPLExecutor dplExecutor;
        try {
            dplExecutor = new DPLExecutorFactory("com.teragrep.pth_10.executor.DPLExecutorImpl", dplConfig).create();
        }
        catch (
                ClassNotFoundException | NoSuchMethodException | InvocationTargetException | InstantiationException
                | IllegalAccessException e
        ) {
            throw new RuntimeException("Error initializing DPLExecutor implementation", e);
        }

        try {
            final DPLExecutorResult executorResult = dplExecutor
                    .interpret(batchHandler, sparkSession, queryId, noteId, paragraphId, prompt);

            LOGGER.info("executorResult code <{}> message <{}>", executorResult.code(), executorResult.message());

        }
        catch (TimeoutException e) {
            throw new RuntimeException(e);
        }

        return rows.get();
    }
}
