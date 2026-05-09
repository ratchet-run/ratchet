package run.ratchet.testsuite.util;

final class DataSourceResources {

  private DataSourceResources() {}

  static String dataSourceClassName(String dbType) {
    return switch (dbType) {
      case "mysql" -> "com.mysql.cj.jdbc.MysqlDataSource";
      case "postgresql" -> "org.postgresql.ds.PGSimpleDataSource";
      default -> throw new IllegalArgumentException("Unsupported database type: " + dbType);
    };
  }

  static String driverCoordinates(String dbType) {
    return switch (dbType) {
      case "mysql" -> "com.mysql:mysql-connector-j";
      case "postgresql" -> "org.postgresql:postgresql";
      default -> throw new IllegalArgumentException("Unsupported database type: " + dbType);
    };
  }

  static String xml(String value) {
    return value
        .replace("&", "&amp;")
        .replace("\"", "&quot;")
        .replace("<", "&lt;")
        .replace(">", "&gt;");
  }
}
