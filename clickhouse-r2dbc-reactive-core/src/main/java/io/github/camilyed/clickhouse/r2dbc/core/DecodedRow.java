package io.github.camilyed.clickhouse.r2dbc.core;

import java.util.Arrays;

/**
 * One decoded row's values, in wire (column) order — a compact snapshot, not a name-keyed lookup
 * structure. Name-based access is layered on top by a caller that also holds the matching {@link
 * ColumnDescriptor} list from the same {@link DecodedResult} (name→index lookup is once-per-result
 * work, not once-per-row work).
 *
 * <p>Deliberately a plain {@code Object[]} snapshot rather than a {@code Map<String, Object>} built
 * fresh per row: the {@code Map} shape measured at roughly 576 bytes/row and was the dominant
 * per-row allocation and latency cost in this driver's decode path — client-v2's own row
 * representation never needed a hash table either. See docs/PERFORMANCE.md's Phase 5 "Optimization phase"
 * section (hypothesis H1) for the measurements this replaces.
 *
 * <p><b>{@code values} is not defensively copied by this type</b> — copying it here would silently
 * reintroduce the per-row allocation this type exists to avoid. The producer ({@link
 * RowBinaryDecoder}) hands out a snapshot it owns and never mutates again; a caller must not mutate
 * {@link #values()}'s array contents either, or every other reader of the same {@code DecodedRow}
 * observes the change (there is no copy-on-read to protect them).
 */
public record DecodedRow(Object[] values) {

  @Override
  public boolean equals(final Object other) {
    if (!(other instanceof DecodedRow otherRow)) {
      return false;
    }
    return Arrays.equals(values, otherRow.values);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(values);
  }

  @Override
  public String toString() {
    return "DecodedRow" + Arrays.toString(values);
  }

  /** The value at {@code index} (0-based), in wire column order. */
  public Object valueAt(final int index) {
    return values[index];
  }
}
