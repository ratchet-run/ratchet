package run.ratchet.testsuite.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.junit.jupiter.api.Test;
import run.ratchet.testsuite.infra.JdbcDatabaseConfig;

class GlassFishDataSourceStrategyTest {

  @Test
  void exposesApplicationScopedJtaDatasourceName() {
    assertEquals("java:app/jdbc/RatchetDS", new GlassFishDataSourceStrategy().jtaDataSourceName());
  }

  @Test
  void rendersPostgresqlResourcesXmlWithGlassFishDtdAndEscapedValues() throws Exception {
    String xml =
        resourcesXml(
            new JdbcDatabaseConfig(
                "jdbc:postgresql://localhost:5432/ratchet?a=1&b=<two>",
                "test\"user",
                "pa&ss<word>",
                "org.postgresql.Driver",
                "postgresql"));

    assertTrue(xml.contains("<!DOCTYPE resources PUBLIC"));
    assertTrue(xml.contains("-//GlassFish.org//DTD GlassFish Application Server 3.1"));
    assertTrue(xml.contains("http://glassfish.org/dtds/glassfish-resources_1_5.dtd"));
    assertTrue(xml.contains("datasource-classname=\"org.postgresql.ds.PGSimpleDataSource\""));
    assertTrue(
        xml.contains("value=\"jdbc:postgresql://localhost:5432/ratchet?a=1&amp;b=&lt;two&gt;\""));
    assertTrue(xml.contains("value=\"test&quot;user\""));
    assertTrue(xml.contains("value=\"pa&amp;ss&lt;word&gt;\""));
    assertTrue(xml.contains("jndi-name=\"java:app/jdbc/RatchetDS\""));
    assertFalse(xml.contains("sslMode"));
    assertFalse(xml.contains("allowPublicKeyRetrieval"));
  }

  @Test
  void rendersMysqlResourcesXmlWithMysqlProperties() throws Exception {
    String xml =
        resourcesXml(
            new JdbcDatabaseConfig(
                "jdbc:mysql://localhost:3306/ratchet",
                "ratchet",
                "secret",
                "com.mysql.cj.jdbc.Driver",
                "mysql"));

    assertTrue(xml.contains("datasource-classname=\"com.mysql.cj.jdbc.MysqlDataSource\""));
    assertTrue(xml.contains("<property name=\"sslMode\" value=\"DISABLED\"/>"));
    assertTrue(xml.contains("<property name=\"allowPublicKeyRetrieval\" value=\"true\"/>"));
  }

  @Test
  void rejectsUnsupportedDatabaseTypeDuringXmlGeneration() {
    JdbcDatabaseConfig config =
        new JdbcDatabaseConfig("jdbc:h2:mem:ratchet", "ratchet", "secret", "org.h2.Driver", "h2");

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> resourcesXml(config));

    assertEquals("Unsupported database type: h2", exception.getMessage());
  }

  @Test
  void rejectsUnsupportedDatabaseTypeForDriverCoordinates() throws Exception {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> invokePrivate("driverCoordinates", "h2"));

    assertEquals("Unsupported database type: h2", exception.getMessage());
  }

  private static String resourcesXml(JdbcDatabaseConfig config) throws Exception {
    return invokePrivate("resourcesXml", config, glassFishDoctype());
  }

  private static String glassFishDoctype() throws Exception {
    Field doctype = GlassFishDataSourceStrategy.class.getDeclaredField("DOCTYPE");
    doctype.setAccessible(true);
    return (String) doctype.get(null);
  }

  private static String invokePrivate(String methodName, Object... args) throws Exception {
    Class<?>[] parameterTypes = new Class<?>[args.length];
    for (int i = 0; i < args.length; i++) {
      parameterTypes[i] = args[i].getClass();
    }

    Method method = GlassFishResources.class.getDeclaredMethod(methodName, parameterTypes);
    method.setAccessible(true);

    try {
      return (String) method.invoke(null, args);
    } catch (InvocationTargetException e) {
      Throwable cause = e.getCause();
      if (cause instanceof Exception exception) {
        throw exception;
      }
      throw e;
    }
  }
}
