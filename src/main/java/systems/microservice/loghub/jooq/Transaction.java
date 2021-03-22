/*
 * Copyright (C) 2020 Microservice Systems, Inc.
 * All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package systems.microservice.loghub.jooq;

import org.jooq.impl.DefaultDSLContext;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author Dmitry Kotlyarov
 * @since 1.0
 */
public class Transaction extends DefaultDSLContext implements AutoCloseable {
    private static final long serialVersionUID = 1L;

    protected final transient Database database;
    protected final transient Connector connector;
    protected final transient Connection connection;
    protected final transient Map<String, Object> context;
    protected final long begin;
    protected long end;
    protected boolean committed;
    protected boolean closed;

    protected Transaction(Database database, boolean readOnly) {
        this(new Init(database, database.acquire(readOnly)));
    }

    protected Transaction(Init init) {
        super(init.connection, init.database.getDialect(), init.database.getSettings());

        this.database = init.database;
        this.connector = init.connector;
        this.connection = init.connection;
        this.context = new LinkedHashMap<>(16);
        this.begin = System.currentTimeMillis();
        this.end = -1;
        this.committed = false;
        this.closed = false;
    }

    public Database getDatabase() {
        return database;
    }

    public Connection getConnection() {
        return connection;
    }

    public boolean isReadOnly() {
        try {
            return connection.isReadOnly();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public Map<String, Object> getContext() {
        return context;
    }

    public long getBegin() {
        return begin;
    }

    public long getEnd() {
        return end;
    }

    public boolean isCommitted() {
        return committed;
    }

    public boolean isClosed() {
        return closed;
    }

    public void commit() {
        if (!closed) {
            if (!committed) {
                try {
                    connection.commit();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
                committed = true;
            } else {
                throw new IllegalStateException("Transaction is committed");
            }
        } else {
            throw new IllegalStateException("Transaction is closed");
        }
    }

    @Override
    public void close() {
        if (!closed) {
            try {
                if (!committed) {
                    try {
                        connection.rollback();
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
            } finally {
                connector.release();
                closed = true;
            }
        }
    }

    protected static class Init {
        public final Database database;
        public final Connector connector;
        public final Connection connection;

        public Init(Database database, Connector connector) {
            this.database = database;
            this.connector = connector;
            this.connection = connector.getConnection();
        }
    }
}
