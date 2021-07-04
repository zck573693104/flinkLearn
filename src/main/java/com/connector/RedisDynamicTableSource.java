package com.connector;

import org.apache.flink.calcite.shaded.com.google.common.base.Preconditions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.*;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.util.List;

public class RedisDynamicTableSource implements LookupTableSource, ScanTableSource {
  private final ReadableConfig options;
  private final ResolvedSchema schema;
 
  public RedisDynamicTableSource(ReadableConfig options, ResolvedSchema schema) {
    this.options = options;
    this.schema = schema;
  }
 
  @Override
  public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
    Preconditions.checkArgument(context.getKeys().length == 1 && context.getKeys()[0].length == 1, "Redis source only supports lookup by single key");
    
    int fieldCount = schema.getColumnCount();
    if (fieldCount != 2) {
      throw new ValidationException("Redis source only supports 2 columns");
    }
 
    List<DataType> dataTypes = schema.getColumnDataTypes();
    for (int i = 0; i < fieldCount; i++) {
      if (!dataTypes.get(i).getLogicalType().getTypeRoot().equals(LogicalTypeRoot.VARCHAR)) {
        throw new ValidationException("Redis connector only supports STRING type");
      }
    }

    return TableFunctionProvider.of(new RedisRowDataLookupFunction(options));
  }
 
  @Override
  public DynamicTableSource copy() {
    return new RedisDynamicTableSource(options, schema);
  }
 
  @Override
  public String asSummaryString() {
    return "Redis Dynamic Table Source";
  }

  @Override
  public ChangelogMode getChangelogMode() {
    return ChangelogMode.insertOnly();
  }

  @Override
  public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
    return SourceFunctionProvider.of(new RedisSourceFunction(options),true);
  }
}