package com.alibaba.datax.plugin.writer.sqlserverwriter;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

public final class SqlServerWriterUtil {

    public static String getWriteTemplate(String table, List<String> columnHolders, List<String> updateKeys, List<String> valueHolders, Boolean isBatch) {

        String writeDataSqlTemplate;
        if (!isBatch) {

            List<String> updateWhereSql = new ArrayList<String>();
            List<String> updateSetSql = new ArrayList<String>();
            for (String updateKey : updateKeys) {
                updateWhereSql.add(updateKey + " = ? ");

            }
            for (String column : columnHolders) {
                if (!updateKeys.contains(column)) {
                    updateSetSql.add(column + " = ? ");
                }
            }


            writeDataSqlTemplate = new StringBuilder()
                    .append("if exists (select 1 from ")
                    .append(table)
                    .append(" where ")
                    .append(StringUtils.join(updateWhereSql, " and "))
                    .append(" ) begin update ")
                    .append(table)
                    .append(" set ")
                    .append(StringUtils.join(updateSetSql, ","))
                    .append("  where ")
                    .append(StringUtils.join(updateWhereSql, " and "))
                    .append(" end else begin insert into ")
                    .append(table)
                    .append(" (")
                    .append(StringUtils.join(columnHolders, ","))
                    .append(") VALUES(")
                    .append(StringUtils.join(valueHolders, ","))
                    .append(") end ")
                    .toString();
        } else {

            writeDataSqlTemplate = new StringBuilder()
                    .append("INSERT INTO ")
                    .append(table)
                    .append(" (").append(StringUtils.join(columnHolders, ","))
                    .append(") VALUES(").append(StringUtils.join(valueHolders, ","))
                    .append(")")
                    .toString();
        }

        return writeDataSqlTemplate;
    }

}
