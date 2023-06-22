package com.tan.paimon.java.api.code;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.options.Options;

public class CreateDatabase {

    static {
        System.setProperty("HADOOP_USER_NAME", "tanbs");
    }

    public static void main(String[] args) {

        Options options = new Options();
        options.set("warehouse", "/user/hive/warehouse/paimon");
        options.set("metastore", "hive");
        options.set("uri", "thrift://hadoop102:9083");
        options.set("hive-conf-dir", "D:\\project\\bigdata\\source-code\\paimon-0.4.0-code\\java-api-code\\src\\main\\resources");
        CatalogContext context = CatalogContext.create(options);
        Catalog catalog = CatalogFactory.createCatalog(context);

        try {
            // idea 出现 HADOOP_HOME 相关问题: https://blog.csdn.net/whs0329/article/details/121878162
            // catalog.createDatabase("my_db", false);

            // System.out.println(catalog.databaseExists("my_db"));

            // for (String database : catalog.listDatabases()) {
            //    System.out.println(database);
            // }

            catalog.dropDatabase("my_db", false, true);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
