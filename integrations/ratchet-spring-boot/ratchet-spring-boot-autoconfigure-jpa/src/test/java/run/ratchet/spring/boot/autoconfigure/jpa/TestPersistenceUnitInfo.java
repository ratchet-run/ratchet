/*
 * Copyright 2026 Ratchet Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package run.ratchet.spring.boot.autoconfigure.jpa;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import javax.sql.DataSource;
import org.springframework.orm.jpa.persistenceunit.SmartPersistenceUnitInfo;

/** Mutable test metadata backed by the Jakarta SPI rather than Spring's version-specific helper. */
final class TestPersistenceUnitInfo {
  private final SmartPersistenceUnitInfo info = mock(SmartPersistenceUnitInfo.class);
  private final List<String> managedClassNames = new ArrayList<>();
  private final List<String> managedPackages = new ArrayList<>();
  private final List<String> mappingFileNames = new ArrayList<>();
  private final List<URL> jarFileUrls = new ArrayList<>();
  private final Properties properties = new Properties();
  private ClassLoader classLoader = TestPersistenceUnitInfo.class.getClassLoader();
  private URL persistenceUnitRootUrl;
  private DataSource jtaDataSource;
  private DataSource nonJtaDataSource;

  TestPersistenceUnitInfo() {
    when(info.getManagedClassNames()).thenReturn(managedClassNames);
    when(info.getManagedPackages()).thenReturn(managedPackages);
    when(info.getMappingFileNames()).thenReturn(mappingFileNames);
    when(info.getJarFileUrls()).thenReturn(jarFileUrls);
    when(info.getProperties()).thenReturn(properties);
    when(info.getClassLoader()).thenAnswer(ignored -> classLoader);
    when(info.getPersistenceUnitRootUrl()).thenAnswer(ignored -> persistenceUnitRootUrl);
    when(info.getJtaDataSource()).thenAnswer(ignored -> jtaDataSource);
    when(info.getNonJtaDataSource()).thenAnswer(ignored -> nonJtaDataSource);
  }

  SmartPersistenceUnitInfo info() {
    return info;
  }

  void addManagedClassName(String managedClassName) {
    managedClassNames.add(managedClassName);
  }

  void addManagedPackage(String managedPackage) {
    managedPackages.add(managedPackage);
  }

  void addMappingFileName(String mappingFileName) {
    mappingFileNames.add(mappingFileName);
  }

  void addJarFileUrl(URL jarFileUrl) {
    jarFileUrls.add(jarFileUrl);
  }

  void addProperty(String name, String value) {
    properties.setProperty(name, value);
  }

  void setClassLoader(ClassLoader classLoader) {
    this.classLoader = classLoader;
  }

  void setJtaDataSource(DataSource jtaDataSource) {
    this.jtaDataSource = jtaDataSource;
  }

  void setNonJtaDataSource(DataSource nonJtaDataSource) {
    this.nonJtaDataSource = nonJtaDataSource;
  }

  void setPersistenceUnitName(String persistenceUnitName) {
    when(info.getPersistenceUnitName()).thenReturn(persistenceUnitName);
  }

  void setPersistenceUnitRootUrl(URL persistenceUnitRootUrl) {
    this.persistenceUnitRootUrl = persistenceUnitRootUrl;
  }
}
