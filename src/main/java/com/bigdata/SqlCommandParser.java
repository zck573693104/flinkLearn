package com.bigdata;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Simple parser for determining the type of command and its parameters.
 */
public final class SqlCommandParser {

  private SqlCommandParser() {
    // private
  }

  public static Optional<SqlCommandCall> parse(String stmt) {
    // normalize
    stmt = stmt.trim();
    // remove ';' at the end
    if (stmt.endsWith(";")) {
      stmt = stmt.substring(0, stmt.length() - 1).trim();
    }

    // parse
    for (SqlCommand cmd : SqlCommand.values()) {
      final Matcher matcher = cmd.pattern.matcher(stmt);
      if (matcher.matches()) {
        final String[] groups = new String[matcher.groupCount()];
        for (int i = 0; i < groups.length; i++) {
          groups[i] = matcher.group(i + 1);
        }
        final String sql = stmt;
        return cmd.operandConverter.apply(groups)
                .map((operands) -> new SqlCommandCall(cmd, operands, sql));
      }
    }
    return Optional.empty();
  }

  // --------------------------------------------------------------------------------------------

  private static final Function<String[], Optional<String[]>> NO_OPERANDS =
          (operands) -> Optional.of(new String[0]);

  private static final Function<String[], Optional<String[]>> SINGLE_OPERAND =
          (operands) -> Optional.of(new String[]{operands[0]});

  private static final int DEFAULT_PATTERN_FLAGS = Pattern.CASE_INSENSITIVE | Pattern.DOTALL;

  /**
   * Supported SQL commands.
   */
  public enum SqlCommand {
    QUIT(
            "(QUIT|EXIT)",
            NO_OPERANDS),

    CLEAR(
            "CLEAR",
            NO_OPERANDS),

    HELP(
            "HELP",
            NO_OPERANDS),

    SHOW_CATALOGS(
            "SHOW\\s+CATALOGS",
            NO_OPERANDS),

    SHOW_DATABASES(
            "SHOW\\s+DATABASES",
            NO_OPERANDS),

    SHOW_TABLES(
            "SHOW\\s+TABLES",
            NO_OPERANDS),

    SHOW_FUNCTIONS(
            "SHOW\\s+FUNCTIONS",
            NO_OPERANDS),

    SHOW_MODULES(
            "SHOW\\s+MODULES",
            NO_OPERANDS),

    USE_CATALOG(
            "USE\\s+CATALOG\\s+(.*)",
            SINGLE_OPERAND),

    USE(
            "USE\\s+(?!CATALOG)(.*)",
            SINGLE_OPERAND),

    DESCRIBE(
            "DESCRIBE\\s+(.*)",
            SINGLE_OPERAND),

    EXPLAIN(
            "EXPLAIN\\s+(.*)",
            SINGLE_OPERAND),

    SELECT(
            "(SELECT.*)",
            SINGLE_OPERAND),

    INSERT_INTO(
            "(INSERT\\s+INTO.*)",
            SINGLE_OPERAND),

    INSERT_OVERWRITE(
            "(INSERT\\s+OVERWRITE.*)",
            SINGLE_OPERAND),
    BEGIN_STATEMENT_SET("(BEGIN\\s+STATEMENT\\s+SET.*)",SINGLE_OPERAND),

    END("(END)",SINGLE_OPERAND),

    CREATE_TABLE("(CREATE\\s+TABLE\\s+.*)", SINGLE_OPERAND),

    DROP_TABLE("(DROP\\s+TABLE\\s+.*)", SINGLE_OPERAND),

    CREATE_VIEW(
            "CREATE\\s+VIEW\\s+(\\S+)\\s+AS\\s+(.*)",
            (operands) -> {
              if (operands.length < 2) {
                return Optional.empty();
              }
              return Optional.of(new String[]{operands[0], operands[1]});
            }),

    CREATE_DATABASE(
            "(CREATE\\s+DATABASE\\s+.*)",
            SINGLE_OPERAND),

    DROP_DATABASE(
            "(DROP\\s+DATABASE\\s+.*)",
            SINGLE_OPERAND),

    DROP_VIEW(
            "DROP\\s+VIEW\\s+(.*)",
            SINGLE_OPERAND),

    ALTER_DATABASE(
            "(ALTER\\s+DATABASE\\s+.*)",
            SINGLE_OPERAND),

    ALTER_TABLE(
            "(ALTER\\s+TABLE\\s+.*)",
            SINGLE_OPERAND),

    SET(
            "SET(\\s+(\\S+)\\s*=(.*))?", // whitespace is only ignored on the left side of '='
            (operands) -> {
              if (operands.length < 3) {
                return Optional.empty();
              } else if (operands[0] == null) {
                return Optional.of(new String[0]);
              }
              return Optional.of(new String[]{operands[1], operands[2]});
            }),

    RESET(
            "RESET",
            NO_OPERANDS),

    SOURCE(
            "SOURCE\\s+(.*)",
            SINGLE_OPERAND);

    public final Pattern pattern;
    public final Function<String[], Optional<String[]>> operandConverter;

    SqlCommand(String matchingRegex, Function<String[], Optional<String[]>> operandConverter) {
      this.pattern = Pattern.compile(matchingRegex, DEFAULT_PATTERN_FLAGS);
      this.operandConverter = operandConverter;
    }

    @Override
    public String toString() {
      return super.toString().replace('_', ' ');
    }

    public boolean hasOperands() {
      return operandConverter != NO_OPERANDS;
    }
  }

  /**
   * Call of SQL command with operands and command type.
   */
  public static class SqlCommandCall {
    public final SqlCommand command;
    public final String[] operands;
    public final String sql;

    public SqlCommandCall(SqlCommand command, String[] operands, String sql) {
      this.command = command;
      this.operands = operands;
      this.sql = sql;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SqlCommandCall that = (SqlCommandCall) o;
      return command == that.command && Arrays.equals(operands, that.operands);
    }

    @Override
    public int hashCode() {
      int result = Objects.hash(command);
      result = 31 * result + Arrays.hashCode(operands);
      return result;
    }

    @Override
    public String toString() {
      return command + "(" + Arrays.toString(operands) + ")";
    }
  }
}
