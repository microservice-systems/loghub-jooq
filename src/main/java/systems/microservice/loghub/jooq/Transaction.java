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
public class Transaction extends DefaultDSLContext {
    private static final long serialVersionUID = 1L;

    protected final transient Database database;
    protected final transient Connector connector;
    protected final transient Connection connection;
    protected final transient Map<String, Object> context;
    protected final long begin;
    protected long end;

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

    @SuppressWarnings("unchecked")
    public <T> T get(String key, Class<T> clazz) {
        return (T) context.get(key);
    }

    @SuppressWarnings("unchecked")
    public <T> T put(String key, T object) {
        return (T) context.put(key, object);
    }

    public long getBegin() {
        return begin;
    }

    public long getEnd() {
        return end;
    }

    public void commit() {
        try {
            connection.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void rollback() {
        try {
            connection.rollback();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    protected void close() {
        end = System.currentTimeMillis();
        connector.release();
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
