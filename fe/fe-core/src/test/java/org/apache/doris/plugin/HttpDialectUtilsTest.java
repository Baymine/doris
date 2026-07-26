// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.plugin;

import org.apache.doris.common.Config;
import org.apache.doris.plugin.dialect.HttpDialectUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.net.SocketException;

public class HttpDialectUtilsTest {

    private int port;
    private SimpleHttpServer server;

    @Before
    public void setUp() throws Exception {
        port = findValidPort();
        server = new SimpleHttpServer(port);
        server.start("/api/v1/convert");
    }

    @After
    public void tearDown() {
        if (server != null) {
            server.stop();
        }
    }

    @Test
    public void testSqlConvert() {
        String originSql = "select * from t1 where \"k1\" = 1";
        String expectedSql = "select * from t1 where `k1` = 1";
        String[] features = new String[] {"ctas"};
        String targetURL = "http://127.0.0.1:" + port + "/api/v1/convert";
        String res = HttpDialectUtils.convertSql(targetURL, originSql, "presto", features, "{}");
        Assert.assertEquals(originSql, res);
        // test presto
        server.setResponse("{\"version\": \"v1\", \"data\": \"" + expectedSql + "\", \"code\": 0, \"message\": \"\"}");
        res = HttpDialectUtils.convertSql(targetURL, originSql, "presto", features, "{}");
        Assert.assertEquals(expectedSql, res);
        // test response version error
        server.setResponse("{\"version\": \"v2\", \"data\": \"" + expectedSql + "\", \"code\": 0, \"message\": \"\"}");
        res = HttpDialectUtils.convertSql(targetURL, originSql, "presto", features, "{}");
        Assert.assertEquals(originSql, res);
        // test response code error
        server.setResponse(
                "{\"version\": \"v1\", \"data\": \"" + expectedSql + "\", \"code\": 400, \"message\": \"\"}");
        res = HttpDialectUtils.convertSql(targetURL, originSql, "presto", features, "{}");
        Assert.assertEquals(originSql, res);
    }

    @Test
    public void testSqlConvertReadTimeout() {
        String originSql = "select * from t1 where \"k1\" = 1";
        String expectedSql = "select * from t1 where `k1` = 1";
        String[] features = new String[] {"ctas"};
        String targetURL = "http://127.0.0.1:" + port + "/api/v1/convert";
        int originReadTimeout = Config.sql_converter_read_timeout_ms;
        try {
            // server would return a valid conversion, but responds slower than the read timeout
            server.setResponse(
                    "{\"version\": \"v1\", \"data\": \"" + expectedSql + "\", \"code\": 0, \"message\": \"\"}");
            server.setResponseDelayMs(2000);
            Config.sql_converter_read_timeout_ms = 200;
            // read timeout triggers, convertSql should gracefully fall back to the origin statement
            String res = HttpDialectUtils.convertSql(targetURL, originSql, "presto", features, "{}");
            Assert.assertEquals(originSql, res);
        } finally {
            Config.sql_converter_read_timeout_ms = originReadTimeout;
            server.setResponseDelayMs(0);
        }
    }

    private static int findValidPort() {
        int port;
        while (true) {
            try (ServerSocket socket = new ServerSocket(0)) {
                socket.setReuseAddress(true);
                port = socket.getLocalPort();
                try (DatagramSocket datagramSocket = new DatagramSocket(port)) {
                    datagramSocket.setReuseAddress(true);
                    break;
                } catch (SocketException e) {
                    System.out.println("The port " + port + " is invalid and try another port.");
                }
            } catch (IOException e) {
                throw new IllegalStateException("Could not find a free TCP/IP port to start HTTP Server on");
            }
        }
        return port;
    }
}
