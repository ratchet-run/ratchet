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
package run.ratchet.store.sqlserver;

import java.util.Set;
import run.ratchet.tck.store.schema.DialectTypeMapper;
import run.ratchet.tck.store.schema.ForeignKey;
import run.ratchet.tck.store.schema.LogicalType;
import run.ratchet.tck.store.schema.OnDeleteAction;

/**
 * SQL Server type/action acceptance for the schema conformance contract.
 *
 * <p>Column types are introspected through {@code DatabaseMetaData.getColumns}, which mssql-jdbc
 * reports as the lowercase native type name ({@code int}, {@code bigint}, {@code binary}, {@code
 * nvarchar}, {@code datetime2}, {@code bit}, …). The Ratchet SQL Server schema stores UUIDs as
 * {@code BINARY(16)} (canonical big-endian bytes, not native {@code UNIQUEIDENTIFIER}), JSON and
 * free TEXT columns as {@code NVARCHAR(MAX)}, and zoneless UTC timestamps as {@code DATETIME2}.
 *
 * <p>Like the MySQL mapper, {@link #supportsPartialIndexIntrospection()} stays {@code false}: SQL
 * Server exposes filtered-index predicates via {@code sys.indexes.filter_definition}, but its
 * canonical text form ({@code ([status]='PENDING')}) does not line up byte-for-byte with the
 * catalog's rendered predicate, so the partial-predicate test is skipped while index existence is
 * still verified.
 */
final class SqlserverDialectMapper implements DialectTypeMapper {

  @Override
  public String dialectName() {
    return "SQL Server";
  }

  @Override
  public Set<String> acceptedTypes(LogicalType logical) {
    return switch (logical) {
      case INT32 -> Set.of("int");
      case INT64 -> Set.of("bigint");
      case UUID -> Set.of("binary");
      // Bounded enum/identifier columns are VARCHAR(n); free-text and JSON columns are
      // NVARCHAR(MAX).
      case TEXT -> Set.of("varchar", "nvarchar", "char", "nchar");
      case CHAR_1 -> Set.of("char", "nchar");
      // Ratchet stores UTC instants in zoneless DATETIME2; DATETIMEOFFSET is also acceptable.
      case TIMESTAMP_TZ -> Set.of("datetime2", "datetimeoffset");
      case BOOLEAN -> Set.of("bit");
      case JSON -> Set.of("nvarchar");
    };
  }

  @Override
  public OnDeleteAction parseOnDelete(String introspectedValue) {
    if (introspectedValue == null) {
      return OnDeleteAction.NO_ACTION;
    }
    return switch (introspectedValue.toUpperCase()) {
      case "CASCADE" -> OnDeleteAction.CASCADE;
      case "RESTRICT" -> OnDeleteAction.RESTRICT;
      case "SET NULL" -> OnDeleteAction.SET_NULL;
      case "SET DEFAULT" -> OnDeleteAction.SET_DEFAULT;
      default -> OnDeleteAction.NO_ACTION;
    };
  }

  /**
   * SQL Server rejects two {@code ON DELETE CASCADE} foreign keys from one table to the same parent
   * ("multiple cascade paths"). {@code scheduler_workflow_condition} references {@code
   * scheduler_job} from both {@code parent_job_id} and {@code child_job_id}, so both are declared
   * {@code NO ACTION} and the store deletes condition rows explicitly. Accept that substitution for
   * those two FKs; every other FK is compared strictly.
   */
  @Override
  public boolean acceptsOnDelete(ForeignKey expected, OnDeleteAction actual) {
    boolean isWorkflowConditionFk =
        expected.name().equals("fk_workflow_parent") || expected.name().equals("fk_workflow_child");
    if (isWorkflowConditionFk
        && expected.onDelete() == OnDeleteAction.CASCADE
        && actual == OnDeleteAction.NO_ACTION) {
      return true;
    }
    return expected.onDelete() == actual;
  }

  @Override
  public boolean supportsPartialIndexIntrospection() {
    return false;
  }
}
