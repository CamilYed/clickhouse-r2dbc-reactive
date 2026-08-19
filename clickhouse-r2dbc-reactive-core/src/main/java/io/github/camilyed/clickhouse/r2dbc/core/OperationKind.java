package io.github.camilyed.clickhouse.r2dbc.core;

/**
 * The kind of operation a {@link DriverObservationEvent} reports on — distinguishes a {@code
 * SELECT}/DDL/DML statement run via {@code Statement}/{@code Batch} from a streamed {@code INSERT}
 * run via the vendor {@code insertStreaming} extension, since the two have meaningfully different
 * event semantics (see {@link QueryCompletedEvent}'s Javadoc for {@code timeToFirstRow}).
 */
public enum OperationKind {

  /** A {@code SELECT}, DDL, or DML statement executed via {@code Statement} or {@code Batch}. */
  QUERY,

  /** A streamed {@code INSERT} executed via the vendor {@code insertStreaming} extension. */
  INSERT
}
