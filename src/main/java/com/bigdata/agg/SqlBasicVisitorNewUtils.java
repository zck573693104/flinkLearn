package com.bigdata.agg;

import io.github.melin.superior.common.StatementType;
import io.github.melin.superior.common.relational.Statement;
import io.github.melin.superior.common.relational.TableId;
import io.github.melin.superior.common.relational.dml.InsertTable;
import io.github.melin.superior.common.relational.dml.QueryStmt;
import lombok.Getter;
import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Getter
public class SqlBasicVisitorNewUtils {
    private Set<String> sourceTables = new HashSet<>();
    private String sinkTable;
    private Set<String> withTables = new HashSet<>();
    private static final List<SqlParser> PARSERS = new ArrayList<>(){{
        add(new SparkSqlHelpers());
        add(new PrestoSqlHelpers());
        add(new MysqlSqlHelpers());
    }};
    public static void main(String[] args) throws IOException {

        String fileName = "D:\\project\\bigdata-operation-cost\\oc-analysis\\src\\main\\subitem\\pms_emp_rpt_cust_cost_analysis\\dws_pms\\kernel\\job_insert_dws_pms_cust_cost_analysis_waybill_wide.sql";
        String sqls = FileUtils.readFileToString(new File(fileName), Charset.defaultCharset());
        SqlBasicVisitorNewUtils visitor = new SqlBasicVisitorNewUtils();
        sqlParser(sqls, visitor);
        visitor.getSourceTables().removeAll(visitor.getWithTables());
        System.out.println("Source tables: ");
        visitor.getSourceTables().forEach(System.out::println);
        System.out.println("Sink table: " + visitor.getSinkTable());
    }

    private static void sqlParser(String sqls, SqlBasicVisitorNewUtils visitor) {
        for (String sql : sqls.split(";")) {
            if (sql.trim().isEmpty()) {
                continue;
            }
            Statement statement = getStatement(sql);
            StatementType statementType = statement.getStatementType();
            List<TableId> inputTables = new ArrayList<>();
            if (statementType.equals(StatementType.INSERT)) {
                inputTables = ((InsertTable) statement).getQueryStmt().getInputTables();
            } else if (statementType.equals(StatementType.SELECT)) {
                inputTables = ((QueryStmt) statement).getInputTables();
            }
            inputTables.forEach(tableId -> visitor.getSourceTables().add(tableId.getFullTableName()));
        }
    }

    @NotNull
    private static Statement getStatement(String sql) {
        Statement statement = null;
        for (SqlParser parser : PARSERS) {
            try {
                statement = parser.parseStatement(sql);
                break; // 如果解析成功，跳出循环
            } catch (Exception e) {
                System.out.println(e);
            }
        }
        if (statement == null) {
            throw new RuntimeException("Failed to parse SQL with all available parsers");
        }
        return statement;
    }
}
