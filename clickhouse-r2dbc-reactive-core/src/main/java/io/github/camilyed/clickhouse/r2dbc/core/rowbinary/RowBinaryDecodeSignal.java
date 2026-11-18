package io.github.camilyed.clickhouse.r2dbc.core.rowbinary;

import io.github.camilyed.clickhouse.r2dbc.core.ColumnDescriptor;
import io.github.camilyed.clickhouse.r2dbc.core.DecodedRow;
import java.util.List;

sealed interface RowBinaryDecodeSignal {

  record Schema(List<ColumnDescriptor> columns) implements RowBinaryDecodeSignal {

    public Schema {
      columns = List.copyOf(columns);
    }
  }

  record Row(DecodedRow value) implements RowBinaryDecodeSignal {}
}
