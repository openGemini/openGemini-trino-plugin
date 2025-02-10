[中文](./README_ZH.md) | [Englist](./README.md)

# trino-openGemini-plugin

This project is a plug-in for the distributed SQL engine Trino. With this plug-in, you can use Trino directly to access openGemini.

## Quick start

Please refer to the [Deploying Trino—Trino 399 Documentation](https://trinodb.github.io/docs.trino.io/399/installation/deployment.html). The following is a deployment example based on Ubuntu 24.04, Java 17.0.11, and Python 3.12.3.

1. Download [trino-server-399.tar.gz](https://repo1.maven.org/maven2/io/trino/trino-server/399/trino-server-399.tar.gz), and unzip.

   ```bash
   $ tar xf trino-server-399.tar.gz
   ```

2. Unzip `trino-opengemini-399.zip`. You can compile `trino-opengemini-399.zip` yourself, or [download](https://github.com/openGemini/openGemini-trino-plugin/releases/tag/v0.1.0) the pre-compiled `trino-opengemini-399.zip`.

   ```bash
   $ unzip trino-opengemini-399.zip
   ```

3. Put `trino-opengemini` in the `plugin` directory of trino-server and rename it to opengemini

   ```bash
   $ mv trino-opengemini-399 trino-server-399/plugin/opengemini
   ```

4. Config `trino-server-399/etc/config.properties` (For Reference Only)

   ```toml
   coordinator=true
   node-scheduler.include-coordinator=true
   http-server.http.port=8080
   discovery.uri=http://localhost:8080
   ```

5. Config `trino-server-399/etc/jvm.config` (For Reference Only)

   ```toml
   -server
   -Xmx4G
   -XX:InitialRAMPercentage=80
   -XX:MaxRAMPercentage=80
   -XX:G1HeapRegionSize=32M
   -XX:+ExplicitGCInvokesConcurrent
   -XX:+ExitOnOutOfMemoryError
   -XX:+HeapDumpOnOutOfMemoryError
   -XX:-OmitStackTraceInFastThrow
   -XX:ReservedCodeCacheSize=512M
   -XX:PerMethodRecompilationCutoff=10000
   -XX:PerBytecodeRecompilationCutoff=10000
   -Djdk.attach.allowAttachSelf=true
   -Djdk.nio.maxCachedBufferSize=2000000
   -XX:+UnlockDiagnosticVMOptions
   -XX:+UseAESCTRIntrinsics
   -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005
   ```

6. Config `trino-server-399/etc/log.properties`

   ```bash
   io.trino=INFO
   ```

7. Config `trino-server-399/etc/node.properties`

   ```bash
   node.environment=test
   node.id=1
   node.data-dir=/tmp/trino/logs
   ```

8. Config opengemini catalog，The following is an example:

   ```bash
   $ mkdir etc/catalog
   $ echo "connector.name=opengemini
   opengemini.connect.endpoint=http://127.0.0.1:8086
   opengemini.connect.username=xxxxxx
   opengemini.connect.password=xxxxxx
   opengemini.connect.timeout=300s" > etc/catalog/opengemini.properties
   ```

## Start trino server

Use `bin/launcher` to start `trino` server

```bash
$ bin/launcher start
```

The service log is similar to the following when successful: 

```bash
$ tail -30 /tmp/trino/logs/var/log/server.log
2025-02-08T16:48:58.050+0800    INFO    main    io.trino.server.PluginManager   -- Finished loading plugin /tmp/trino/logs/plugin/teradata-functions --
2025-02-08T16:48:58.050+0800    INFO    main    io.trino.server.PluginManager   -- Loading plugin /tmp/trino/logs/plugin/thrift --
2025-02-08T16:48:58.059+0800    INFO    main    io.trino.server.PluginManager   Installing io.trino.plugin.thrift.ThriftPlugin
2025-02-08T16:48:58.064+0800    INFO    main    io.trino.server.PluginManager   Registering connector trino-thrift
2025-02-08T16:48:58.064+0800    INFO    main    io.trino.server.PluginManager   -- Finished loading plugin /tmp/trino/logs/plugin/thrift --
2025-02-08T16:48:58.064+0800    INFO    main    io.trino.server.PluginManager   -- Loading plugin /tmp/trino/logs/plugin/tpcds --
2025-02-08T16:48:58.070+0800    INFO    main    io.trino.server.PluginManager   Installing io.trino.plugin.tpcds.TpcdsPlugin
2025-02-08T16:48:58.076+0800    INFO    main    io.trino.server.PluginManager   Registering connector tpcds
2025-02-08T16:48:58.077+0800    INFO    main    io.trino.server.PluginManager   -- Finished loading plugin /tmp/trino/logs/plugin/tpcds --
2025-02-08T16:48:58.077+0800    INFO    main    io.trino.server.PluginManager   -- Loading plugin /tmp/trino/logs/plugin/tpch --
2025-02-08T16:48:58.082+0800    INFO    main    io.trino.server.PluginManager   Installing io.trino.plugin.tpch.TpchPlugin
2025-02-08T16:48:58.088+0800    INFO    main    io.trino.server.PluginManager   Registering connector tpch
2025-02-08T16:48:58.089+0800    INFO    main    io.trino.server.PluginManager   -- Finished loading plugin /tmp/trino/logs/plugin/tpch --
2025-02-08T16:48:58.090+0800    INFO    main    io.trino.connector.StaticCatalogManager -- Loading catalog opengemini --
2025-02-08T16:48:58.541+0800    INFO    main    Bootstrap       PROPERTY                             DEFAULT     RUNTIME                DESCRIPTION
2025-02-08T16:48:58.541+0800    INFO    main    Bootstrap       opengemini.cache.expire-duraion      60.00s      60.00s
2025-02-08T16:48:58.541+0800    INFO    main    Bootstrap       opengemini.query.chunk-poll-timeout  10.00s      10.00s
2025-02-08T16:48:58.541+0800    INFO    main    Bootstrap       opengemini.query.chunk-size          0           0
2025-02-08T16:48:58.541+0800    INFO    main    Bootstrap       opengemini.connect.timeout           10.00s      3.00s
2025-02-08T16:48:58.541+0800    INFO    main    Bootstrap       opengemini.connect.endpoint          ----        http://127.0.0.1:8086
2025-02-08T16:48:58.541+0800    INFO    main    Bootstrap       opengemini.connect.keepalive         false       false
2025-02-08T16:48:58.541+0800    INFO    main    Bootstrap       opengemini.connect.password          [REDACTED]  [REDACTED]
2025-02-08T16:48:58.541+0800    INFO    main    Bootstrap       opengemini.read.timeout              10.00s      10.00s
2025-02-08T16:48:58.541+0800    INFO    main    Bootstrap       opengemini.connect.username          ----        admin
2025-02-08T16:48:58.541+0800    INFO    main    Bootstrap       opengemini.write.timeout             10.00s      10.00s
2025-02-08T16:48:58.643+0800    INFO    main    io.airlift.bootstrap.LifeCycleManager   Life cycle starting...
2025-02-08T16:48:58.643+0800    INFO    main    io.airlift.bootstrap.LifeCycleManager   Life cycle started
2025-02-08T16:48:58.644+0800    INFO    main    io.trino.connector.StaticCatalogManager -- Added catalog opengemini using connector opengemini --
2025-02-08T16:48:58.647+0800    INFO    main    io.trino.security.AccessControlManager  Using system access control: default
2025-02-08T16:48:58.702+0800    INFO    main    io.trino.server.Server  ======== SERVER STARTED ========

```

## How to query from openGemini

1. Download trino cli [trino-cli-399-executable.jar](https://repo1.maven.org/maven2/io/trino/trino-cli/399/trino-cli-399-executable.jar)，and make it executable

   ```bash
   $ chmod +x trino-cli-399-executable.jar
   ```

2. Connect trino server. The openGemini needs to run before.

   ```bash
   $ ./trino-cli-399-executable.jar 
   trino> show catalogs;
     Catalog
   ------------
    opengemini
    system
   (2 rows)
   
   Query 20250208_092335_00001_p3k9x, FINISHED, 1 node
   Splits: 4 total, 4 done (100.00%)
   0.19 [0 rows, 0B] [0 rows/s, 0B/s]
   
   trino> show schemas from opengemini;
          Schema
   --------------------
    information_schema
    stress
   (2 rows)
   
   Query 20250208_092530_00004_p3k9x, FINISHED, 1 node
   Splits: 4 total, 4 done (100.00%)
   0.35 [2 rows, 34B] [5 rows/s, 96B/s]
   
   trino> use opengemini.stress;
   USE
   trino:stress> show tables;
    Table
   -------
    m0
   (1 row)
   
   Query 20250208_092554_00008_p3k9x, FINISHED, 1 node
   Splits: 4 total, 4 done (100.00%)
   0.15 [1 rows, 18B] [6 rows/s, 123B/s]
   
   trino:stress> show columns from m0;
    Column |            Type             | Extra |  Comment
   --------+-----------------------------+-------+-----------
    time   | timestamp(9) with time zone |       | timestamp
    tag0   | varchar                     |       | tag
    tag1   | varchar                     |       | tag
    tag2   | varchar                     |       | tag
    v0     | double                      |       | field
    v01    | double                      |       | field
    v02    | double                      |       | field
   (7 rows)
   
   Query 20250208_092715_00010_p3k9x, FINISHED, 1 node
   Splits: 4 total, 4 done (100.00%)
   0.28 [7 rows, 422B] [24 rows/s, 1.47KB/s]
   
   trino:stress> select * from m0 where time > timestamp '2024-12-27 01:35:10 UTC' and time <= timestamp '2024-12-27 04:45:10 UTC' limit 10;
                  time                |  tag0  |  tag1  |  tag2  |   v0   |  v01   |  v02
   -----------------------------------+--------+--------+--------+--------+--------+--------
    2024-12-27 03:49:52.285251826 UTC | value0 | value0 | value0 | 1682.0 | 1682.0 | 1682.0
    2024-12-27 03:49:52.285253826 UTC | value1 | value0 | value0 | 8315.0 | 8315.0 | 8315.0
    2024-12-27 03:49:52.285254526 UTC | value2 | value0 | value0 | 7389.0 | 7389.0 | 7389.0
    2024-12-27 03:49:52.285255226 UTC | value3 | value0 | value0 | 7796.0 | 7796.0 | 7796.0
    2024-12-27 03:49:52.285255926 UTC | value4 | value0 | value0 | 3140.0 | 3140.0 | 3140.0
    2024-12-27 03:49:52.285256726 UTC | value5 | value0 | value0 | 3348.0 | 3348.0 | 3348.0
    2024-12-27 03:49:52.285257426 UTC | value6 | value0 | value0 | 8596.0 | 8596.0 | 8596.0
    2024-12-27 03:49:52.285258126 UTC | value7 | value0 | value0 | 2531.0 | 2531.0 | 2531.0
    2024-12-27 03:49:52.285258726 UTC | value8 | value0 | value0 | 4118.0 | 4118.0 | 4118.0
    2024-12-27 03:49:52.285259626 UTC | value9 | value0 | value0 | 1334.0 | 1334.0 | 1334.0
   (10 rows)
   
   Query 20250208_094453_00003_uwxgt, FINISHED, 1 node
   Splits: 1 total, 1 done (100.00%)
   0.89 [10 rows, 0B] [11 rows/s, 0B/s]
   
   trino:stress>
   ```

## Compile trino-openGemin

As a trino plugin, `trino-openGemin` wants to depend on `trino` when compiling. Taking the `trino-399` version as an example, the compilation steps are as follows: 

1. Download [trino-399](https://github.com/trinodb/trino/archive/refs/tags/399.zip) source code and unzip

   ```bash
   $ unzip trino-399.zip
   ```

2. Download trino-openGemini code from `https://github.com/openGemini/openGemini-trino-plugin/archive/refs/heads/main.zip`

   ```bash
   $ unzip openGemini-trino-plugin-main.zip
   ```

3. Move `trino-openGemini under` plugin，and rename `openGemini-trino-plugin-main` to `trino-opengemini`

   ```bash
   $ mv openGemini-trino-plugin-main trino-399/plugin/trino-opengemini
   ```

4. Add configuration `<module>plugin/trino-opengemini</module>` in `trino-399/pom.xml` file

   ```bash
   <modules>
       ......
       <module>plugin/trino-opengemini</module>
       ......
   </modules>
   ```

5. If the compilation process involves license, git and test checks, you can add the following content to trino-399/plugin/trino-opengemini/pom.xml

   ```bash
   <build>
   	<plugins>
       	<plugin>
                   <groupId>com.mycila</groupId>
                   <artifactId>license-maven-plugin</artifactId>
                   <version>3.0</version>
                   <configuration>
                       <skip>true</skip>
                   </configuration>
           </plugin>
   
           <plugin>
                   <groupId>pl.project13.maven</groupId>
                   <artifactId>git-commit-id-plugin</artifactId>
                   <version>4.0.5</version>
                   <configuration>
                       <skip>true</skip>
                   </configuration>
           </plugin>
   
           <plugin>
                   <groupId>org.apache.maven.plugins</groupId>
                   <artifactId>maven-surefire-plugin</artifactId>
                   <version>3.0.0-M5</version>
                   <configuration>
                       <skipTests>true</skipTests>
                   </configuration>
           </plugin>
   	</plugins>
   </build>
   ```

6. Complie `trino-opengemini`, refer to [trino-399 README](https://github.com/trinodb/trino/tree/399)，take IntelliJ IDEA, Java 17.0.4 and Maven 3.6.3 as an example:

   ![image](./images/openGemini-trino.png)

7. As shown in the above figure, the trino-399/plugin/trino-opengemini/target/trino-opengemini-399.zip generated by installation is the file required for trino to run.

