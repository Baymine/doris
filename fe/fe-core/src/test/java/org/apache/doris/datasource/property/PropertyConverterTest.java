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

package org.apache.doris.datasource.property;

import org.apache.doris.analysis.CreateCatalogStmt;
import org.apache.doris.analysis.DropCatalogStmt;
import org.apache.doris.backup.Repository;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Resource;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergExternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergGlueExternalCatalog;
import org.apache.doris.datasource.maxcompute.MaxComputeExternalCatalog;
import org.apache.doris.datasource.property.constants.CosProperties;
import org.apache.doris.datasource.property.constants.DLFProperties;
import org.apache.doris.datasource.property.constants.GCSProperties;
import org.apache.doris.datasource.property.constants.GlueProperties;
import org.apache.doris.datasource.property.constants.HMSProperties;
import org.apache.doris.datasource.property.constants.MinioProperties;
import org.apache.doris.datasource.property.constants.ObsProperties;
import org.apache.doris.datasource.property.constants.OssProperties;
import org.apache.doris.datasource.property.constants.S3Properties;
import org.apache.doris.meta.MetaContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.CreateRepositoryCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateResourceCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.utframe.TestWithFeService;

import com.aliyun.datalake.metastore.common.DataLakeConfig;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class PropertyConverterTest extends TestWithFeService {

    private final Set<String> checkSet = new HashSet<>();
    private final Map<String, String> expectedCredential = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("mock_db");
        useDatabase("mock_db");
        createTable("create table mock_tbl1 \n" + "(k1 int, k2 int) distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1');");

        List<String> withoutPrefix = ImmutableList.of("endpoint", "access_key", "secret_key");
        checkSet.addAll(withoutPrefix);
        checkSet.addAll(Arrays.asList(S3Properties.ENDPOINT, S3Properties.ACCESS_KEY, S3Properties.SECRET_KEY));
        expectedCredential.put("access_key", "akk");
        expectedCredential.put("secret_key", "skk");
    }

    @Test
    public void testS3SourcePropertiesConverter() throws Exception {
        String queryOld = "CREATE RESOURCE 'remote_s3'\n"
                + "PROPERTIES\n"
                + "(\n"
                + "   'type' = 's3',\n"
                + "   'AWS_ENDPOINT' = 's3.us-east-1.amazonaws.com',\n"
                + "   'AWS_REGION' = 'us-east-1',\n"
                + "   'AWS_ACCESS_KEY' = 'akk',\n"
                + "   'AWS_SECRET_KEY' = 'skk',\n"
                + "   'AWS_ROOT_PATH' = '/',\n"
                + "   'AWS_BUCKET' = 'bucket',\n"
                + "   's3_validity_check' = 'false'"
                + ");";
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalPlan = nereidsParser.parseSingle(queryOld);
        Assertions.assertTrue(logicalPlan instanceof CreateResourceCommand);
        CreateResourceCommand command = (CreateResourceCommand) logicalPlan;
        command.getInfo().validate();

        Assertions.assertEquals(command.getInfo().getProperties().size(), 8);
        Resource resource = Resource.fromCommand(command);
        // will add converted properties
        Assertions.assertEquals(resource.getCopiedProperties().size(), 20);

        String queryNew = "CREATE RESOURCE 'remote_new_s3'\n"
                + "PROPERTIES\n"
                + "(\n"
                + "   'type' = 's3',\n"
                + "   's3.endpoint' = 'http://s3.us-east-1.amazonaws.com',\n"
                + "   's3.region' = 'us-east-1',\n"
                + "   's3.access_key' = 'akk',\n"
                + "   's3.secret_key' = 'skk',\n"
                + "   's3.root.path' = '/',\n"
                + "   's3.bucket' = 'bucket',\n"
                + "   's3_validity_check' = 'false'"
                + ");";
        logicalPlan = nereidsParser.parseSingle(queryNew);
        Assertions.assertTrue(logicalPlan instanceof CreateResourceCommand);
        command = (CreateResourceCommand) logicalPlan;
        command.getInfo().validate();

        Assertions.assertEquals(command.getInfo().getProperties().size(), 8);
        Resource newResource = Resource.fromCommand(command);
        // will add converted properties
        Assertions.assertEquals(newResource.getCopiedProperties().size(), 14);

    }

    @Test
    public void testS3RepositoryPropertiesConverter() throws Exception {
        FeConstants.runningUnitTest = true;
        String s3Repo = "CREATE REPOSITORY `s3_repo`\n"
                + "WITH S3\n"
                + "ON LOCATION 's3://s3-repo'\n"
                + "PROPERTIES\n"
                + "(\n"
                + "    'AWS_ENDPOINT' = 'http://s3.us-east-1.amazonaws.com',\n"
                + "    'AWS_ACCESS_KEY' = 'akk',\n"
                + "    'AWS_SECRET_KEY'='skk',\n"
                + "    'AWS_REGION' = 'us-east-1'\n"
                + ");";
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalPlan = nereidsParser.parseSingle(s3Repo);
        Assertions.assertTrue(logicalPlan instanceof CreateRepositoryCommand);
        CreateRepositoryCommand command = (CreateRepositoryCommand) logicalPlan;
        command.validate();

        Assertions.assertEquals(command.getProperties().size(), 4);
        Repository repository = getRepository(command, "s3_repo");
        Assertions.assertEquals(4, repository.getRemoteFileSystem().getProperties().size());

        String s3RepoNew = "CREATE REPOSITORY `s3_repo_new`\n"
                + "WITH S3\n"
                + "ON LOCATION 's3://s3-repo'\n"
                + "PROPERTIES\n"
                + "(\n"
                + "    's3.endpoint' = 'http://s3.us-east-1.amazonaws.com',\n"
                + "    's3.access_key' = 'akk',\n"
                + "    's3.secret_key' = 'skk'\n"
                + ");";
        logicalPlan = nereidsParser.parseSingle(s3RepoNew);
        Assertions.assertTrue(logicalPlan instanceof CreateRepositoryCommand);
        command = (CreateRepositoryCommand) logicalPlan;
        command.validate();

        Assertions.assertEquals(command.getProperties().size(), 3);
        Repository repositoryNew = getRepository(command, "s3_repo_new");
        Assertions.assertEquals(3, repositoryNew.getRemoteFileSystem().getProperties().size());
    }

    private static Repository getRepository(CreateRepositoryCommand command, String name) throws DdlException {
        Env.getCurrentEnv().getBackupHandler().createRepository(command);
        return Env.getCurrentEnv().getBackupHandler().getRepoMgr().getRepo(name);
    }

    @Test
    public void testAWSOldCatalogPropertiesConverter() throws Exception {
        String queryOld = "create catalog hms_s3_old properties (\n"
                    + "    'type'='hms',\n"
                    + "    'hive.metastore.uris' = 'thrift://172.21.0.44:7004',\n"
                    + "    'AWS_ENDPOINT' = 's3.us-east-1.amazonaws.com',\n"
                    + "    'AWS_REGION' = 'us-east-1',\n"
                    + "    'AWS_ACCESS_KEY' = 'akk',\n"
                    + "    'AWS_SECRET_KEY' = 'skk'\n"
                    + ");";
        CreateCatalogStmt analyzedStmt = createStmt(queryOld);
        HMSExternalCatalog catalog = createAndGetCatalog(analyzedStmt, "hms_s3_old");
        Map<String, String> properties = catalog.getCatalogProperty().getProperties();
        Assertions.assertEquals(13, properties.size());

        Map<String, String> hdProps = catalog.getCatalogProperty().getHadoopProperties();
        Assertions.assertEquals(21, hdProps.size());
    }

    @Test
    public void testS3CatalogPropertiesConverter() throws Exception {
        String query = "create catalog hms_s3 properties (\n"
                    + "    'type'='hms',\n"
                    + "    'hive.metastore.uris' = 'thrift://172.21.0.1:7004',\n"
                    + "    's3.endpoint' = 's3.us-east-1.amazonaws.com',\n"
                    + "    's3.access_key' = 'akk',\n"
                    + "    's3.secret_key' = 'skk'\n"
                    + ");";
        CreateCatalogStmt analyzedStmt = createStmt(query);
        HMSExternalCatalog catalog = createAndGetCatalog(analyzedStmt, "hms_s3");
        Map<String, String> properties = catalog.getCatalogProperty().getProperties();
        Assertions.assertEquals(13, properties.size());

        Map<String, String> hdProps = catalog.getCatalogProperty().getHadoopProperties();
        Assertions.assertNull(hdProps.get("fs.s3.impl.disable.cache"));
        Assertions.assertEquals(21, hdProps.size());
    }

    @Test
    public void testOssHdfsProperties() throws Exception {
        String catalogName1 = "hms_oss_hdfs";
        String query1 = "create catalog " + catalogName1 + " properties (\n"
                + "    'type'='hms',\n"
                + "    'hive.metastore.uris' = 'thrift://172.21.0.1:7004',\n"
                + "    'oss.endpoint' = 'oss-cn-beijing.aliyuncs.com',\n"
                + "    'oss.hdfs.enabled' = 'true',\n"
                + "    'oss.access_key' = 'akk',\n"
                + "    'oss.secret_key' = 'skk'\n"
                + ");";
        String catalogName = "hms_oss_hdfs";
        CreateCatalogStmt analyzedStmt = createStmt(query1);
        HMSExternalCatalog catalog = createAndGetCatalog(analyzedStmt, catalogName);
        Map<String, String> hdProps = catalog.getCatalogProperty().getHadoopProperties();
        Assertions.assertEquals("com.aliyun.jindodata.oss.JindoOssFileSystem", hdProps.get("fs.oss.impl"));
        Assertions.assertEquals("cn-beijing.oss-dls.aliyuncs.com", hdProps.get("fs.oss.endpoint"));
    }

    @Test
    public void testDlfPropertiesConverter() throws Exception {
        String queryDlf1 = "create catalog hms_dlf1 properties (\n"
                + "    'type'='hms',\n"
                + "    'hive.metastore.type'='dlf',\n"
                + "    'dlf.proxy.mode' = 'DLF_ONLY',\n"
                + "    'dlf.endpoint' = 'dlf.cn-beijing.aliyuncs.com',\n"
                + "    'dlf.uid' = '20239444',\n"
                + "    'dlf.access_key' = 'akk',\n"
                + "    'dlf.secret_key' = 'skk',\n"
                + "    'dlf.region' = 'cn-beijing',\n"
                + "    'dlf.access.public' = 'false'\n"
                + ");";
        String catalogName = "hms_dlf1";
        CreateCatalogStmt analyzedStmt = createStmt(queryDlf1);
        HMSExternalCatalog catalog = createAndGetCatalog(analyzedStmt, catalogName);
        Map<String, String> properties = catalog.getCatalogProperty().getProperties();
        Assertions.assertEquals("hms", properties.get("type"));
        Assertions.assertEquals("dlf", properties.get(HMSProperties.HIVE_METASTORE_TYPE));
        Assertions.assertEquals("akk", properties.get(DataLakeConfig.CATALOG_ACCESS_KEY_ID));
        Assertions.assertEquals("skk", properties.get(DataLakeConfig.CATALOG_ACCESS_KEY_SECRET));
        Assertions.assertEquals("dlf.cn-beijing.aliyuncs.com", properties.get(DataLakeConfig.CATALOG_ENDPOINT));
        Assertions.assertEquals("cn-beijing", properties.get(DataLakeConfig.CATALOG_REGION_ID));
        Assertions.assertEquals("20239444", properties.get(DataLakeConfig.CATALOG_USER_ID));

        Map<String, String> hdProps = catalog.getCatalogProperty().getHadoopProperties();
        Assertions.assertEquals("akk", hdProps.get(OssProperties.ACCESS_KEY));
        Assertions.assertEquals("skk", hdProps.get(OssProperties.SECRET_KEY));
        Assertions.assertEquals("oss-cn-beijing-internal.aliyuncs.com",
                hdProps.get(OssProperties.ENDPOINT));

        String queryDlf2 = "create catalog hms_dlf2 properties (\n"
                + "    'type'='hms',\n"
                + "    'hive.metastore.type'='dlf',\n"
                + "    'dlf.catalog.endpoint' = 'dlf.cn-beijing.aliyuncs.com',\n"
                + "    'dlf.catalog.uid' = '20239444',\n"
                + "    'dlf.catalog.id' = 'catalogId',\n"
                + "    'dlf.catalog.proxyMode' = 'DLF_ONLY',\n"
                + "    'dlf.catalog.accessKeyId' = 'akk',\n"
                + "    'dlf.catalog.accessKeySecret' = 'skk',\n"
                + "    'dlf.catalog.region' = 'cn-beijing',\n"
                + "    'oss.hdfs.enabled' = 'true',\n"
                + "    'dlf.catalog.accessPublic' = 'true'\n"
                + ");";
        String catalogName2 = "hms_dlf2";
        CreateCatalogStmt analyzedStmt2 = createStmt(queryDlf2);
        HMSExternalCatalog catalog2 = createAndGetCatalog(analyzedStmt2, catalogName2);
        Map<String, String> properties2 = catalog2.getCatalogProperty().getProperties();
        Assertions.assertEquals("dlf", properties2.get(HMSProperties.HIVE_METASTORE_TYPE));
        Assertions.assertEquals("akk", properties2.get(DataLakeConfig.CATALOG_ACCESS_KEY_ID));
        Assertions.assertEquals("skk", properties2.get(DataLakeConfig.CATALOG_ACCESS_KEY_SECRET));
        Assertions.assertEquals("dlf.cn-beijing.aliyuncs.com", properties2.get(DataLakeConfig.CATALOG_ENDPOINT));
        Assertions.assertEquals("cn-beijing", properties2.get(DataLakeConfig.CATALOG_REGION_ID));
        Assertions.assertEquals("20239444", properties2.get(DataLakeConfig.CATALOG_USER_ID));

        Map<String, String> hdProps2 = catalog2.getCatalogProperty().getHadoopProperties();
        Assertions.assertEquals("akk", hdProps2.get(OssProperties.ACCESS_KEY));
        Assertions.assertEquals("skk", hdProps2.get(OssProperties.SECRET_KEY));
        Assertions.assertEquals("cn-beijing.oss-dls.aliyuncs.com", hdProps2.get(OssProperties.ENDPOINT));

        String queryDlfIceberg = "create catalog dlf_iceberg properties (\n"
                + "    'type'='iceberg',\n"
                + "    'iceberg.catalog.type'='dlf',\n"
                + "    'dlf.proxy.mode' = 'DLF_ONLY',\n"
                + "    'dlf.endpoint' = 'dlf.cn-beijing.aliyuncs.com',\n"
                + "    'dlf.uid' = '20239444',\n"
                + "    'dlf.access_key' = 'akk',\n"
                + "    'dlf.secret_key' = 'skk',\n"
                + "    'dlf.region' = 'cn-beijing'\n"
                + ");";
        String catalogName3 = "dlf_iceberg";
        CreateCatalogStmt analyzedStmt3 = createStmt(queryDlfIceberg);
        IcebergExternalCatalog catalog3 = createAndGetIcebergCatalog(analyzedStmt3, catalogName3);
        Map<String, String> properties3 = catalog3.getCatalogProperty().getProperties();
        Assertions.assertEquals("dlf", properties3.get(IcebergExternalCatalog.ICEBERG_CATALOG_TYPE));
        Assertions.assertEquals("akk", properties3.get(DataLakeConfig.CATALOG_ACCESS_KEY_ID));
        Assertions.assertEquals("skk", properties3.get(DataLakeConfig.CATALOG_ACCESS_KEY_SECRET));
        Assertions.assertEquals("dlf.cn-beijing.aliyuncs.com", properties3.get(DataLakeConfig.CATALOG_ENDPOINT));
        Assertions.assertEquals("cn-beijing", properties3.get(DataLakeConfig.CATALOG_REGION_ID));
        Assertions.assertEquals("20239444", properties3.get(DataLakeConfig.CATALOG_USER_ID));

        Map<String, String> hdProps3 = catalog3.getCatalogProperty().getHadoopProperties();
        Assertions.assertEquals("akk", hdProps3.get(OssProperties.ACCESS_KEY));
        Assertions.assertEquals("skk", hdProps3.get(OssProperties.SECRET_KEY));
        Assertions.assertEquals("oss-cn-beijing-internal.aliyuncs.com", hdProps3.get(OssProperties.ENDPOINT));
    }

    @Test
    public void testMcPropertiesConverter() throws Exception {
        String queryDlf1 = "create catalog hms_mc properties (\n"
                + "    'type'='max_compute',\n"
                + "    'mc.default.project' = 'project0',\n"
                + "    'mc.access_key' = 'ak',\n"
                + "    'mc.secret_key' = 'sk',\n"
                + "    'mc.endpoint' = 'http://service.cn-beijing-vpc.maxcompute.aliyun-inc.com/api' \n"
                + ");";
        String catalogName = "hms_mc";
        CreateCatalogStmt analyzedStmt = createStmt(queryDlf1);
        Env.getCurrentEnv().getCatalogMgr().createCatalog(analyzedStmt);
        MaxComputeExternalCatalog catalog = (MaxComputeExternalCatalog) Env.getCurrentEnv()
                .getCatalogMgr().getCatalog(catalogName);
        Map<String, String> properties = catalog.getCatalogProperty().getProperties();
        Assertions.assertEquals(properties.get("type"), "max_compute");
        Assertions.assertEquals(properties.get("mc.access_key"), "ak");
        Assertions.assertEquals(properties.get("mc.secret_key"), "sk");
        Assertions.assertEquals(properties.get("mc.endpoint"),
                "http://service.cn-beijing-vpc.maxcompute.aliyun-inc.com/api");
        Assertions.assertEquals(properties.get("mc.default.project"), "project0");
    }

    @Test
    public void testGlueCatalogPropertiesConverter() throws Exception {
        String queryOld = "create catalog hms_glue_old properties (\n"
                + "    'type'='hms',\n"
                + "    'hive.metastore.type'='glue',\n"
                + "    'hive.metastore.uris' = 'thrift://172.21.0.1:7004',\n"
                + "    'aws.glue.endpoint' = 'glue.us-east-1.amazonaws.com',\n"
                + "    'aws.glue.access-key' = 'akk',\n"
                + "    'aws.glue.secret-key' = 'skk',\n"
                + "    'aws.region' = 'us-east-1'\n"
                + ");";
        String catalogName = "hms_glue_old";
        CreateCatalogStmt analyzedStmt = createStmt(queryOld);
        HMSExternalCatalog catalog = createAndGetCatalog(analyzedStmt, catalogName);
        Map<String, String> properties = catalog.getProperties();
        Assertions.assertEquals(22, properties.size());
        Assertions.assertEquals("s3.us-east-1.amazonaws.com", properties.get(S3Properties.ENDPOINT));

        Map<String, String> hdProps = catalog.getCatalogProperty().getHadoopProperties();
        Assertions.assertEquals(30, hdProps.size());
        Assertions.assertNull(hdProps.get("fs.s3.impl.disable.cache"));

        String query = "create catalog hms_glue properties (\n"
                + "    'type'='hms',\n"
                + "    'hive.metastore.type'='glue',\n"
                + "    'hive.metastore.uris' = 'thrift://172.21.0.1:7004',\n"
                + "    'glue.endpoint' = 'glue.us-east-1.amazonaws.com.cn',\n"
                + "    'glue.access_key' = 'akk',\n"
                + "    'glue.secret_key' = 'skk'\n"
                + ");";
        catalogName = "hms_glue";
        CreateCatalogStmt analyzedStmtNew = createStmt(query);
        HMSExternalCatalog catalogNew = createAndGetCatalog(analyzedStmtNew, catalogName);
        Map<String, String> propertiesNew = catalogNew.getProperties();
        Assertions.assertEquals(22, propertiesNew.size());
        Assertions.assertEquals("s3.us-east-1.amazonaws.com.cn", propertiesNew.get(S3Properties.ENDPOINT));

        Map<String, String> hdPropsNew = catalogNew.getCatalogProperty().getHadoopProperties();
        Assertions.assertNull(hdPropsNew.get("fs.s3.impl.disable.cache"));
        Assertions.assertEquals(30, hdPropsNew.size());
    }

    @Test
    public void testS3CompatibleCatalogPropertiesConverter() throws Exception {
        String catalogName0 = "hms_cos";
        String query0 = "create catalog " + catalogName0 + " properties (\n"
                    + "    'type'='hms',\n"
                    + "    'hive.metastore.uris' = 'thrift://172.21.0.1:7004',\n"
                    + "    'cos.endpoint' = 'cos.ap-beijing.myqcloud.com',\n"
                    + "    'cos.access_key' = 'akk',\n"
                    + "    'cos.secret_key' = 'skk'\n"
                    + ");";
        testS3CompatibleCatalogProperties(catalogName0, CosProperties.COS_PREFIX,
                "cos.ap-beijing.myqcloud.com", query0, 13, 18);

        String catalogName1 = "hms_oss";
        String query1 = "create catalog " + catalogName1 + " properties (\n"
                + "    'type'='hms',\n"
                + "    'hive.metastore.uris' = 'thrift://172.21.0.1:7004',\n"
                + "    'oss.endpoint' = 'oss.oss-cn-beijing.aliyuncs.com',\n"
                + "    'oss.access_key' = 'akk',\n"
                + "    'oss.secret_key' = 'skk'\n"
                + ");";
        testS3CompatibleCatalogProperties(catalogName1, OssProperties.OSS_PREFIX,
                "oss.oss-cn-beijing.aliyuncs.com", query1, 13, 17);

        String catalogName2 = "hms_minio";
        String query2 = "create catalog " + catalogName2 + " properties (\n"
                + "    'type'='hms',\n"
                + "    'hive.metastore.uris' = 'thrift://172.21.0.1:7004',\n"
                + "    'minio.endpoint' = 'http://127.0.0.1',\n"
                + "    'minio.access_key' = 'akk',\n"
                + "    'minio.secret_key' = 'skk'\n"
                + ");";
        testS3CompatibleCatalogProperties(catalogName2, MinioProperties.MINIO_PREFIX,
                "http://127.0.0.1", query2, 13, 21);

        String catalogName3 = "hms_obs";
        String query3 = "create catalog hms_obs properties (\n"
                + "    'type'='hms',\n"
                + "    'hive.metastore.uris' = 'thrift://172.21.0.1:7004',\n"
                + "    'obs.endpoint' = 'obs.cn-north-4.myhuaweicloud.com',\n"
                + "    'obs.access_key' = 'akk',\n"
                + "    'obs.secret_key' = 'skk'\n"
                + ");";
        testS3CompatibleCatalogProperties(catalogName3, ObsProperties.OBS_PREFIX,
                "obs.cn-north-4.myhuaweicloud.com", query3, 13, 17);
    }

    private void testS3CompatibleCatalogProperties(String catalogName, String prefix,
                                                   String endpoint, String sql,
                                                   int catalogPropsSize, int bePropsSize) throws Exception {
        Env.getCurrentEnv().getCatalogMgr().dropCatalog(new DropCatalogStmt(true, catalogName));
        CreateCatalogStmt analyzedStmt = createStmt(sql);
        HMSExternalCatalog catalog = createAndGetCatalog(analyzedStmt, catalogName);
        Map<String, String> properties = catalog.getCatalogProperty().getProperties();
        Assertions.assertEquals(catalogPropsSize, properties.size());

        Map<String, String> hdProps = catalog.getCatalogProperty().getHadoopProperties();
        Assertions.assertEquals(bePropsSize, hdProps.size());
        Assertions.assertNull(hdProps.get(String.format("fs.%s.impl.disable.cache", prefix)));

        Map<String, String> expectedMetaProperties = new HashMap<>();
        expectedMetaProperties.put("endpoint", endpoint);
        expectedMetaProperties.put("AWS_ENDPOINT", endpoint);
        expectedMetaProperties.putAll(expectedCredential);
        checkExpectedProperties(prefix, properties, expectedMetaProperties);
    }

    private void checkExpectedProperties(String prefix, Map<String, String> properties,
                                         Map<String, String> expectedProperties) {
        properties.forEach((key, value) -> {
            if (key.startsWith(prefix)) {
                String keyToCheck = key.replace(prefix, "");
                if (checkSet.contains(keyToCheck)) {
                    Assertions.assertEquals(value, expectedProperties.get(keyToCheck));
                }
            }
        });
    }

    private static HMSExternalCatalog createAndGetCatalog(CreateCatalogStmt analyzedStmt, String name)
            throws UserException {
        Env.getCurrentEnv().getCatalogMgr().createCatalog(analyzedStmt);
        return (HMSExternalCatalog) Env.getCurrentEnv().getCatalogMgr().getCatalog(name);
    }

    private static IcebergExternalCatalog createAndGetIcebergCatalog(CreateCatalogStmt analyzedStmt, String name)
            throws UserException {
        Env.getCurrentEnv().getCatalogMgr().createCatalog(analyzedStmt);
        return (IcebergExternalCatalog) Env.getCurrentEnv().getCatalogMgr().getCatalog(name);
    }

    @Test
    public void testSerialization() throws Exception {
        MetaContext metaContext = new MetaContext();
        metaContext.setMetaVersion(FeMetaVersion.VERSION_CURRENT);
        metaContext.setThreadLocalInfo();
    }

    @Test
    public void testS3PropertiesConvertor() {
        // 1. AWS
        Map<String, String> origProp = Maps.newHashMap();
        origProp.put(S3Properties.Env.ACCESS_KEY, "ak");
        origProp.put(S3Properties.Env.SECRET_KEY, "sk");
        origProp.put(S3Properties.Env.ENDPOINT, "endpoint");
        origProp.put(S3Properties.Env.REGION, "region");
        origProp.put(PropertyConverter.USE_PATH_STYLE, "true");
        Map<String, String> beProperties = S3ClientBEProperties.getBeFSProperties(origProp);
        Assertions.assertEquals(5, beProperties.size());
        Assertions.assertEquals("ak", beProperties.get(S3Properties.Env.ACCESS_KEY));
        Assertions.assertEquals("sk", beProperties.get(S3Properties.Env.SECRET_KEY));
        Assertions.assertEquals("endpoint", beProperties.get(S3Properties.Env.ENDPOINT));
        Assertions.assertEquals("region", beProperties.get(S3Properties.Env.REGION));
        Assertions.assertEquals("true", beProperties.get(PropertyConverter.USE_PATH_STYLE));

        // 2. s3.
        origProp = Maps.newHashMap();
        origProp.put(S3Properties.ACCESS_KEY, "ak");
        origProp.put(S3Properties.SECRET_KEY, "sk");
        origProp.put(S3Properties.ENDPOINT, "endpoint");
        origProp.put(S3Properties.REGION, "region");
        origProp.put(PropertyConverter.USE_PATH_STYLE, "false");
        beProperties = S3ClientBEProperties.getBeFSProperties(origProp);
        Assertions.assertEquals(6, beProperties.size());
        Assertions.assertEquals("ak", beProperties.get(S3Properties.Env.ACCESS_KEY));
        Assertions.assertEquals("sk", beProperties.get(S3Properties.Env.SECRET_KEY));
        Assertions.assertEquals("endpoint", beProperties.get(S3Properties.Env.ENDPOINT));
        Assertions.assertEquals("region", beProperties.get(S3Properties.Env.REGION));
        Assertions.assertEquals("false", beProperties.get(PropertyConverter.USE_PATH_STYLE));

        // 3. minio.
        origProp = Maps.newHashMap();
        origProp.put(MinioProperties.ACCESS_KEY, "ak");
        origProp.put(MinioProperties.SECRET_KEY, "sk");
        origProp.put(MinioProperties.ENDPOINT, "endpoint");
        origProp.put(MinioProperties.REGION, "region");
        origProp.put(PropertyConverter.USE_PATH_STYLE, "false");
        beProperties = S3ClientBEProperties.getBeFSProperties(origProp);
        Assertions.assertEquals(6, beProperties.size());
        Assertions.assertEquals("ak", beProperties.get(S3Properties.Env.ACCESS_KEY));
        Assertions.assertEquals("sk", beProperties.get(S3Properties.Env.SECRET_KEY));
        Assertions.assertEquals("endpoint", beProperties.get(S3Properties.Env.ENDPOINT));
        Assertions.assertEquals("region", beProperties.get(S3Properties.Env.REGION));
        Assertions.assertEquals("false", beProperties.get(PropertyConverter.USE_PATH_STYLE));

        // 3.1 minio without region
        origProp = Maps.newHashMap();
        origProp.put(MinioProperties.ACCESS_KEY, "ak");
        origProp.put(MinioProperties.SECRET_KEY, "sk");
        origProp.put(MinioProperties.ENDPOINT, "endpoint");
        origProp.put(PropertyConverter.USE_PATH_STYLE, "false");
        beProperties = S3ClientBEProperties.getBeFSProperties(origProp);
        Assertions.assertEquals(6, beProperties.size());
        Assertions.assertEquals("ak", beProperties.get(S3Properties.Env.ACCESS_KEY));
        Assertions.assertEquals("sk", beProperties.get(S3Properties.Env.SECRET_KEY));
        Assertions.assertEquals("endpoint", beProperties.get(S3Properties.Env.ENDPOINT));
        Assertions.assertEquals(MinioProperties.DEFAULT_REGION, beProperties.get(S3Properties.Env.REGION));
        Assertions.assertEquals("false", beProperties.get(PropertyConverter.USE_PATH_STYLE));

        // 4. obs
        origProp = Maps.newHashMap();
        origProp.put(ObsProperties.ACCESS_KEY, "ak");
        origProp.put(ObsProperties.SECRET_KEY, "sk");
        origProp.put(ObsProperties.ENDPOINT, "endpoint");
        origProp.put(ObsProperties.REGION, "region");
        origProp.put(PropertyConverter.USE_PATH_STYLE, "false");
        beProperties = S3ClientBEProperties.getBeFSProperties(origProp);
        Assertions.assertEquals(6, beProperties.size());
        Assertions.assertEquals("ak", beProperties.get(S3Properties.Env.ACCESS_KEY));
        Assertions.assertEquals("sk", beProperties.get(S3Properties.Env.SECRET_KEY));
        Assertions.assertEquals("endpoint", beProperties.get(S3Properties.Env.ENDPOINT));
        Assertions.assertEquals("region", beProperties.get(S3Properties.Env.REGION));
        Assertions.assertEquals("false", beProperties.get(PropertyConverter.USE_PATH_STYLE));

        // 4. oss
        origProp = Maps.newHashMap();
        origProp.put(OssProperties.ACCESS_KEY, "ak");
        origProp.put(OssProperties.SECRET_KEY, "sk");
        origProp.put(OssProperties.ENDPOINT, "endpoint");
        origProp.put(OssProperties.REGION, "region");
        origProp.put(PropertyConverter.USE_PATH_STYLE, "false");
        beProperties = S3ClientBEProperties.getBeFSProperties(origProp);
        Assertions.assertEquals(6, beProperties.size());
        Assertions.assertEquals("ak", beProperties.get(S3Properties.Env.ACCESS_KEY));
        Assertions.assertEquals("sk", beProperties.get(S3Properties.Env.SECRET_KEY));
        Assertions.assertEquals("endpoint", beProperties.get(S3Properties.Env.ENDPOINT));
        Assertions.assertEquals("region", beProperties.get(S3Properties.Env.REGION));
        Assertions.assertEquals("false", beProperties.get(PropertyConverter.USE_PATH_STYLE));

        // 4. cos
        origProp = Maps.newHashMap();
        origProp.put(CosProperties.ACCESS_KEY, "ak");
        origProp.put(CosProperties.SECRET_KEY, "sk");
        origProp.put(CosProperties.ENDPOINT, "endpoint");
        origProp.put(CosProperties.REGION, "region");
        origProp.put(PropertyConverter.USE_PATH_STYLE, "false");
        beProperties = S3ClientBEProperties.getBeFSProperties(origProp);
        Assertions.assertEquals(6, beProperties.size());
        Assertions.assertEquals("ak", beProperties.get(S3Properties.Env.ACCESS_KEY));
        Assertions.assertEquals("sk", beProperties.get(S3Properties.Env.SECRET_KEY));
        Assertions.assertEquals("endpoint", beProperties.get(S3Properties.Env.ENDPOINT));
        Assertions.assertEquals("region", beProperties.get(S3Properties.Env.REGION));
        Assertions.assertEquals("false", beProperties.get(PropertyConverter.USE_PATH_STYLE));

        // 5. gs
        origProp = Maps.newHashMap();
        origProp.put(GCSProperties.ACCESS_KEY, "ak");
        origProp.put(GCSProperties.SECRET_KEY, "sk");
        origProp.put(GCSProperties.ENDPOINT, "endpoint");
        origProp.put(GCSProperties.REGION, "region");
        origProp.put(PropertyConverter.USE_PATH_STYLE, "false");
        beProperties = S3ClientBEProperties.getBeFSProperties(origProp);
        Assertions.assertEquals(6, beProperties.size());
        Assertions.assertEquals("ak", beProperties.get(S3Properties.Env.ACCESS_KEY));
        Assertions.assertEquals("sk", beProperties.get(S3Properties.Env.SECRET_KEY));
        Assertions.assertEquals("endpoint", beProperties.get(S3Properties.Env.ENDPOINT));
        Assertions.assertEquals("region", beProperties.get(S3Properties.Env.REGION));
        Assertions.assertEquals("false", beProperties.get(PropertyConverter.USE_PATH_STYLE));
    }

    @Test
    public void testMetaPropertiesConvertor() {
        // test region parser
        Assertions.assertNull(S3Properties.getRegionOfEndpoint("http://192.168.2.30:9099/com.region.test/dir"));
        Assertions.assertEquals("cn-beijing",
                S3Properties.getRegionOfEndpoint("http://dlf.cn-beijing.aliyuncs.com/com.region.test/dir"));
        Assertions.assertEquals("oss-cn-beijing",
                S3Properties.getRegionOfEndpoint("http://oss-cn-beijing.aliyuncs.com/com.region.test/dir"));
        Assertions.assertEquals("us-east-1",
                S3Properties.getRegionOfEndpoint("http://s3.us-east-1.amazonaws.com/com.region.test/dir"));

        //1. dlf
        Map<String, String> props = new HashMap<>();
        // iceberg.catalog.type
        props.put("type", "hms");
        props.put("hive.metastore.type", "dlf");
        props.put(DLFProperties.PROXY_MODE, "DLF_ONLY");
        props.put(DLFProperties.ENDPOINT, "dlf.cn-beijing.aliyuncs.com");
        props.put(DLFProperties.UID, "20239444");
        props.put(DLFProperties.ACCESS_KEY, "akk");
        props.put(DLFProperties.SECRET_KEY, "skk");
        props.put(DLFProperties.REGION, "cn-beijing");
        props.put(DLFProperties.ACCESS_PUBLIC, "false");
        Map<String, String> res = PropertyConverter.convertToMetaProperties(new HashMap<>(props));
        Assertions.assertEquals(26, res.size());
        Assertions.assertEquals("akk", res.get(S3Properties.Env.ACCESS_KEY));
        Assertions.assertEquals("skk", res.get(S3Properties.Env.SECRET_KEY));
        Assertions.assertEquals("akk", res.get(DataLakeConfig.CATALOG_ACCESS_KEY_ID));
        Assertions.assertEquals("skk", res.get(DataLakeConfig.CATALOG_ACCESS_KEY_SECRET));
        Assertions.assertEquals("dlf.cn-beijing.aliyuncs.com", res.get(DataLakeConfig.CATALOG_ENDPOINT));
        Assertions.assertEquals("oss-cn-beijing-internal.aliyuncs.com", res.get(S3Properties.Env.ENDPOINT));
        Assertions.assertEquals("cn-beijing", res.get(DataLakeConfig.CATALOG_REGION_ID));
        Assertions.assertEquals("oss-cn-beijing", res.get(S3Properties.Env.REGION));

        props.put(DLFProperties.ACCESS_PUBLIC, "true");
        res = PropertyConverter.convertToMetaProperties(new HashMap<>(props));
        Assertions.assertEquals(26, res.size());
        Assertions.assertEquals("oss-cn-beijing.aliyuncs.com", res.get(S3Properties.Env.ENDPOINT));
        Assertions.assertEquals("oss-cn-beijing", res.get(S3Properties.Env.REGION));

        props.put(OssProperties.OSS_HDFS_ENABLED, "true");
        res = PropertyConverter.convertToMetaProperties(new HashMap<>(props));
        Assertions.assertEquals(29, res.size());
        Assertions.assertEquals("com.aliyun.jindodata.oss.JindoOssFileSystem", res.get("fs.oss.impl"));
        Assertions.assertEquals("com.aliyun.jindodata.oss.OSS", res.get("fs.AbstractFileSystem.oss.impl"));
        Assertions.assertEquals("false", res.get(DataLakeConfig.CATALOG_CREATE_DEFAULT_DB));
        Assertions.assertEquals("cn-beijing", res.get(S3Properties.Env.REGION));

        // 2. glue
        Map<String, String> props2 = new HashMap<>();
        props2.put("hive.metastore.type", "glue");
        props2.put("aws.glue.endpoint", "glue.us-east-1.amazonaws.com");
        props2.put("aws.glue.access-key", "akk");
        props2.put("aws.glue.secret-key", "skk");
        props2.put("aws.region", "us-east-1");
        res = PropertyConverter.convertToMetaProperties(props2);
        Assertions.assertEquals(17, res.size());
        Assertions.assertEquals("akk", res.get(S3Properties.Env.ACCESS_KEY));
        Assertions.assertEquals("skk", res.get(S3Properties.Env.SECRET_KEY));
        Assertions.assertEquals("s3.us-east-1.amazonaws.com", res.get(S3Properties.Env.ENDPOINT));
        Assertions.assertEquals("us-east-1", res.get(S3Properties.Env.REGION));

        Map<String, String> props3 = new HashMap<>();
        props3.put("hive.metastore.type", "glue");
        props3.put(GlueProperties.ENDPOINT, "glue.us-east-1.amazonaws.com");
        props3.put(GlueProperties.ACCESS_KEY, "akk");
        props3.put(GlueProperties.SECRET_KEY, "skk");
        res = PropertyConverter.convertToMetaProperties(props3);
        Assertions.assertEquals(17, res.size());
        Assertions.assertEquals("akk", res.get(S3Properties.Env.ACCESS_KEY));
        Assertions.assertEquals("skk", res.get(S3Properties.Env.SECRET_KEY));
        Assertions.assertEquals("s3.us-east-1.amazonaws.com", res.get(S3Properties.Env.ENDPOINT));
        Assertions.assertEquals("us-east-1", res.get(S3Properties.Env.REGION));

        // 3. s3 env
        Map<String, String> props4 = new HashMap<>();
        props4.put("hive.metastore.type", "hms");
        props4.put(S3Properties.Env.ENDPOINT, "s3.us-west-2.amazonaws.com");
        props4.put(S3Properties.Env.ACCESS_KEY, "akk");
        props4.put(S3Properties.Env.SECRET_KEY, "skk");
        res = PropertyConverter.convertToMetaProperties(new HashMap<>(props4));
        Assertions.assertEquals(9, res.size());
        Assertions.assertEquals("akk", res.get(S3Properties.Env.ACCESS_KEY));
        Assertions.assertEquals("skk", res.get(S3Properties.Env.SECRET_KEY));
        Assertions.assertEquals("s3.us-west-2.amazonaws.com", res.get(S3Properties.Env.ENDPOINT));
        Assertions.assertEquals("us-west-2", res.get(S3Properties.Env.REGION));

        props4.put(S3Properties.Env.ENDPOINT, "http://172.23.56.19:9033");
        res = PropertyConverter.convertToMetaProperties(new HashMap<>(props4));
        Assertions.assertEquals(9, res.size());
        Assertions.assertEquals("http://172.23.56.19:9033", res.get(S3Properties.Env.ENDPOINT));
        Assertions.assertEquals("us-east-1", res.get(S3Properties.Env.REGION));

        props4.put(S3Properties.Env.REGION, "north");
        res = PropertyConverter.convertToMetaProperties(new HashMap<>(props4));
        Assertions.assertEquals(9, res.size());
        Assertions.assertEquals("north", res.get(S3Properties.Env.REGION));
    }

    @Test
    public void testGluePropertiesConvertor() throws Exception {
        Map<String, String> originProps = Maps.newHashMap();
        originProps.put(GlueProperties.ACCESS_KEY, "ak");
        originProps.put(GlueProperties.SECRET_KEY, "sk");
        originProps.put(GlueProperties.ENDPOINT, "https://glue.us-east-1.amazonaws.com");
        originProps.put("type", "iceberg");
        originProps.put("iceberg.catalog.type", "glue");

        Map<String, String> convertedProps = PropertyConverter.convertToMetaProperties(originProps);
        System.out.println(convertedProps);
        Assertions.assertEquals("com.amazonaws.glue.catalog.credentials.ConfigurationAWSCredentialsProvider2x",
                convertedProps.get(GlueProperties.CLIENT_CREDENTIALS_PROVIDER));
        Assertions.assertEquals("ak", convertedProps.get(GlueProperties.CLIENT_CREDENTIALS_PROVIDER_AK));
        Assertions.assertEquals("sk", convertedProps.get(GlueProperties.CLIENT_CREDENTIALS_PROVIDER_SK));

        String createIceGlue = "CREATE CATALOG iceglue PROPERTIES (\n"
                + "    \"type\"=\"iceberg\",\n"
                + "    \"iceberg.catalog.type\" = \"glue\",\n"
                + "    \"glue.endpoint\" = \"https://glue.us-east-1.amazonaws.com/\",\n"
                + "    \"glue.access_key\" = \"ak123\",\n"
                + "    \"glue.secret_key\" = \"sk123\"\n"
                + ");";
        CreateCatalogStmt analyzedStmt = createStmt(createIceGlue);
        IcebergExternalCatalog icebergExternalCatalog = createAndGetIcebergCatalog(analyzedStmt, "iceglue");
        Assertions.assertTrue(icebergExternalCatalog instanceof IcebergGlueExternalCatalog);
        IcebergGlueExternalCatalog glueCatalog = (IcebergGlueExternalCatalog) icebergExternalCatalog;

        PrintableMap<String, String> printableMap = new PrintableMap<>(glueCatalog.getProperties(), "=", true, true,
                true, true);
        printableMap.setAdditionalHiddenKeys(ExternalCatalog.HIDDEN_PROPERTIES);
        String result = printableMap.toString();
        System.out.println(result);
        Assertions.assertTrue(!result.contains(GlueProperties.CLIENT_CREDENTIALS_PROVIDER));
        Assertions.assertTrue(!result.contains(GlueProperties.CLIENT_CREDENTIALS_PROVIDER_AK));
        Assertions.assertTrue(!result.contains(GlueProperties.CLIENT_CREDENTIALS_PROVIDER_SK));
    }
}
