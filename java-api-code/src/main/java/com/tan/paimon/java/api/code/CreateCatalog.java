package com.tan.paimon.java.api.code;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;

public class CreateCatalog {

    static {
        System.setProperty("HADOOP_USER_NAME", "tanbs");
    }

    public static void createFilesystemCatalog(String path) {
        CatalogContext context = CatalogContext.create(new Path(path));
        Catalog catalog = CatalogFactory.createCatalog(context);
    }

    public static void createHiveCatalog() {
        // Paimon Hive catalog relies on Hive jars
        // You should add hive classpath or hive bundled jar.
        Options options = new Options();
        options.set("warehouse", "/user/hive/warehouse/paimon");
        options.set("metastore", "hive");
        options.set("uri", "thrift://hadoop102:9083");
        options.set("hive-conf-dir", "D:\\project\\bigdata\\source-code\\paimon-0.4.0-code\\java-api-code\\src\\main\\resources");
        CatalogContext context = CatalogContext.create(options);
        Catalog catalog = CatalogFactory.createCatalog(context);
    }

    public static void main(String[] args) {

//        String path = "hdfs://hadoop102:8020/paimon/api/java";
//        createFilesystemCatalog(path);

        createHiveCatalog();

    }

}
