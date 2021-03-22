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

import org.jooq.SQLDialect;
import org.jooq.conf.Settings;

import java.sql.Driver;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Dmitry Kotlyarov
 * @since 1.0
 */
public class Database implements AutoCloseable {
    protected final Driver driver;
    protected final String url;
    protected final Properties properties;
    protected final SQLDialect dialect;
    protected final Settings settings;
    protected final AtomicBoolean closed;
    protected final Connector[] connectors;

    public Database(Driver driver,
                    String url,
                    Properties properties,
                    SQLDialect dialect,
                    Settings settings,
                    int count) {
        this.driver = driver;
        this.url = url;
        this.properties = properties;
        this.dialect = dialect;
        this.settings = settings;
        this.closed = new AtomicBoolean(false);
        this.connectors = createConnectors(driver, url, properties, this.closed, count);
    }

    protected Database(Driver driver,
                       String url,
                       Properties properties,
                       SQLDialect dialect,
                       Settings settings,
                       AtomicBoolean closed,
                       Connector[] connectors) {
        this.driver = driver;
        this.url = url;
        this.properties = properties;
        this.dialect = dialect;
        this.settings = settings;
        this.closed = closed;
        this.connectors = connectors;
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

    public SQLDialect getDialect() {
        return dialect;
    }

    public Settings getSettings() {
        return settings;
    }

    public boolean isClosed() {
        return closed.get();
    }

    public int getCount() {
        return connectors.length;
    }

    protected Connector acquire(boolean readOnly) {
        while (!isClosed()) {
            for (Connector c : connectors) {
                if (c.acquire(readOnly)) {
                    return c;
                }
            }
            try {
                Thread.sleep(100L);
            } catch (InterruptedException e) {
            }
        }
        throw new IllegalStateException("Database is closed");
    }

    public void transactRead(Transact transact) {
        try (Transaction tx = new Transaction(this, true)) {
            transact.run(tx);
            tx.commit();
        }
    }

    public void transactWrite(Transact transact) {
        try (Transaction tx = new Transaction(this, false)) {
            transact.run(tx);
            tx.commit();
        }
    }

    @Override
    public void close() throws Exception {
        closed.set(true);
    }

    private static Connector[] createConnectors(Driver driver,
                                                String url,
                                                Properties properties,
                                                AtomicBoolean closed,
                                                int count) {
        Connector[] cs = new Connector[count];
        for (int i = 0; i < count; ++i) {
            cs[i] = new Connector(driver, url, properties, closed);
        }
        return cs;
    }
}
