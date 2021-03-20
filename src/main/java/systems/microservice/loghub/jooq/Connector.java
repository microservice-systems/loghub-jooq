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

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Dmitry Kotlyarov
 * @since 1.0
 */
public class Connector {
    protected final Driver driver;
    protected final String url;
    protected final Properties properties;
    protected final AtomicBoolean closed;
    protected final AtomicBoolean ready;
    protected final AtomicReference<Connection> connection;

    protected Connector(Driver driver, String url, Properties properties, AtomicBoolean closed) {
        this.driver = driver;
        this.url = url;
        this.properties = properties;
        this.closed = closed;
        this.ready = new AtomicBoolean(true);
        this.connection = new AtomicReference<>(null);
    }

    public Driver getDriver() {
        return driver;
    }

    public String getUrl() {
        return url;
    }

    public Properties getProperties() {
        return properties;
    }

    public boolean isClosed() {
        return closed.get();
    }

    public boolean isReady() {
        return ready.get();
    }

    public Connection getConnection() {
        return connection.get();
    }

    protected boolean acquire(boolean readOnly) {
        if (!closed.get() && ready.compareAndSet(true, false)) {
            try {
                Connection c = connection.get();
                if ((c == null) || c.isClosed() || !c.isValid(10)) {
                    c = driver.connect(url, properties);
                    connection.set(c);
                }
                c.setAutoCommit(false);
                c.setReadOnly(readOnly);
                return true;
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        } else {
            return false;
        }
    }

    protected void release() {
        if (!ready.compareAndSet(false, true)) {
            throw new IllegalStateException("Connector is not acquired");
        }
    }
}
