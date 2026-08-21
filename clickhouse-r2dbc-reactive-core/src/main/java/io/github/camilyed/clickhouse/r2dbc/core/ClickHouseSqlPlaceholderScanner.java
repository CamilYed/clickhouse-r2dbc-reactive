package io.github.camilyed.clickhouse.r2dbc.core;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.jspecify.annotations.Nullable;

/**
 * Finds the distinct {@code {name:Type}} parameter placeholder names a ClickHouse SQL text
 * declares, in first-occurrence order — the implementation behind {@link
 * ClickHouseQuery#parameterNamesIn(String)}.
 *
 * <p>An earlier version of this scan was a single {@link java.util.regex.Pattern} applied to the
 * whole SQL text (roughly {@code \{([a-zA-Z_]\w*):[^}]+}}), which cannot distinguish a real
 * placeholder from the same placeholder-shaped text sitting inside a string literal or a comment —
 * regular expressions have no notion of "unless we're inside a quote right now", since that's
 * context that has to be tracked while scanning, not expressed as a pattern to match against a
 * fixed window of characters. Fixed here with a single left-to-right pass that tracks just enough
 * state to skip the spans a placeholder can never legitimately appear in — single-quoted string
 * literals, double-quoted/backtick-quoted identifiers, and every comment form ClickHouse's own
 * lexer recognizes — before ever looking for a placeholder. Verified against
 * clickhouse.com/docs/sql-reference/syntax's "Comments" section rather than assumed: ClickHouse
 * accepts {@code --}, {@code #!}, and {@code # } (hash-space) as line-comment starters, {@code //}
 * (two or more slashes) as a C-style line-comment starter, and — the one easy to get wrong — {@code
 * /*} ... {@code *}{@code /} block comments that <b>nest</b>, unlike standard SQL. Three ClickHouse
 * SQL text spots this project's own review and this class's own docs cross-check found broken by
 * the old regex now decode correctly: {@code SELECT '{not_a_parameter:String}'}, {@code SELECT 1 --
 * {not_a_parameter:UInt32}}, and a placeholder-shaped span inside a nested block comment no longer
 * register a bind parameter.
 *
 * <p><b>Performance is not the reason for this class — correctness is.</b> Both the old regex and
 * this scanner are O(sql length) with no pathological worst case (nothing here backtracks, and
 * {@link java.util.regex.Pattern}'s compiled DFA for this particular expression doesn't either), so
 * there was no complexity-class problem to fix. A throwaway, non-JMH timing sanity check
 * (single-JVM, warmed up, not a substitute for a real benchmark — this project's benchmark
 * infrastructure is deliberately on hold, see ROADMAP.md's Phase 8 "Deferred" list) comparing this
 * scanner against the old {@code Pattern}/{@code Matcher} on realistic input found the two within
 * noise of each other on a typical short query (regex fractionally faster, well under 1 microsecond
 * either way) and this scanner roughly 8% faster on a large SQL text with 200 placeholders — not a
 * result worth advertising as a win either way. Context-sensitive skipping being inexpressible as a
 * single regular expression is the actual justification for this class existing.
 *
 * <p>Deliberately still narrow: this is a scanner for exactly the spans that can hide a
 * placeholder-shaped false positive, not a SQL parser. It has no opinion on whether the surrounding
 * SQL is otherwise valid, and does not validate the {@code Type} half of a placeholder
 * (ClickHouse's own job, same as before).
 */
final class ClickHouseSqlPlaceholderScanner {

  private ClickHouseSqlPlaceholderScanner() {}

  static List<String> parameterNamesIn(final String sql) {
    final Set<String> names = new LinkedHashSet<>();
    final int length = sql.length();
    int i = 0;
    while (i < length) {
      final char c = sql.charAt(i);
      if (c == '\'' || c == '"' || c == '`') {
        i = skipQuoted(sql, i, c);
      } else if (startsLineComment(sql, i)) {
        i = skipLineComment(sql, i);
      } else if (startsBlockComment(sql, i)) {
        i = skipBlockComment(sql, i);
      } else if (c == '{') {
        final @Nullable Placeholder placeholder = tryParsePlaceholder(sql, i);
        if (placeholder == null) {
          i++;
        } else {
          names.add(placeholder.name());
          i = placeholder.endExclusive();
        }
      } else {
        i++;
      }
    }
    return List.copyOf(names);
  }

  /**
   * Advances past a quoted span opened by {@code quoteChar} at {@code start} (a string literal for
   * {@code '}, a quoted identifier for {@code "}/{@code `}) — a backslash escapes the next
   * character, and a doubled quote character escapes itself, matching ClickHouse's own lexer for
   * both forms. Returns {@code sql.length()} for an unterminated span (no closing quote before the
   * text ends) rather than throwing: this is a best-effort scan over SQL text, not a validator, and
   * the caller is going to send this exact text to ClickHouse regardless, which will reject
   * genuinely malformed SQL itself.
   */
  private static int skipQuoted(final String sql, final int start, final char quoteChar) {
    final int length = sql.length();
    int i = start + 1;
    while (i < length) {
      final char c = sql.charAt(i);
      if (c == '\\' && i + 1 < length) {
        i += 2;
      } else if (c == quoteChar) {
        if (i + 1 < length && sql.charAt(i + 1) == quoteChar) {
          i += 2;
        } else {
          return i + 1;
        }
      } else {
        i++;
      }
    }
    return length;
  }

  /**
   * True at a ClickHouse line-comment starter: {@code --}, {@code #!}, {@code # } (hash followed by
   * a space), or {@code //} — the exact set clickhouse.com/docs/sql-reference/syntax's "Comments"
   * section lists, not just the {@code --} the old regex implicitly assumed via its own ad-hoc
   * "rest of the query is free text" handling elsewhere. A bare {@code #} not followed by {@code !}
   * or a space is not a comment starter and is left for the main loop to skip as an ordinary
   * character, matching that same doc section.
   */
  private static boolean startsLineComment(final String sql, final int i) {
    final char c = sql.charAt(i);
    final boolean hasNext = i + 1 < sql.length();
    if (c == '-') {
      return hasNext && sql.charAt(i + 1) == '-';
    }
    if (c == '/') {
      return hasNext && sql.charAt(i + 1) == '/';
    }
    if (c == '#') {
      return hasNext && (sql.charAt(i + 1) == '!' || sql.charAt(i + 1) == ' ');
    }
    return false;
  }

  private static int skipLineComment(final String sql, final int start) {
    final int newline = sql.indexOf('\n', start + 2);
    return newline == -1 ? sql.length() : newline;
  }

  private static boolean startsBlockComment(final String sql, final int i) {
    return sql.charAt(i) == '/' && i + 1 < sql.length() && sql.charAt(i + 1) == '*';
  }

  /**
   * Advances past a block comment opened at {@code start}, tracking nesting depth so an inner
   * {@code /* ... *}{@code /} doesn't prematurely close the outer one — ClickHouse's own block
   * comments nest, unlike standard SQL's (confirmed against
   * clickhouse.com/docs/sql-reference/syntax before relying on it). Returns {@code sql.length()}
   * for an unterminated comment, same best-effort convention as {@link #skipQuoted}.
   */
  private static int skipBlockComment(final String sql, final int start) {
    final int length = sql.length();
    int depth = 1;
    int i = start + 2;
    while (i < length && depth > 0) {
      if (i + 1 < length && sql.charAt(i) == '/' && sql.charAt(i + 1) == '*') {
        depth++;
        i += 2;
      } else if (i + 1 < length && sql.charAt(i) == '*' && sql.charAt(i + 1) == '/') {
        depth--;
        i += 2;
      } else {
        i++;
      }
    }
    return i;
  }

  /**
   * Attempts to parse a placeholder starting exactly at {@code sql.charAt(openBrace) == '{'},
   * matching the same shape the previous regex required — {@code [a-zA-Z_]\w*} for the name,
   * immediately followed by {@code :}, then one or more characters that aren't {@code }}, then a
   * closing {@code }} — deliberately preserving that exact grammar rather than changing it, since
   * only the surrounding context-skipping behavior is what this class exists to fix.
   */
  private static @Nullable Placeholder tryParsePlaceholder(final String sql, final int openBrace) {
    final int length = sql.length();
    int i = openBrace + 1;
    if (i >= length || !isNameStart(sql.charAt(i))) {
      return null;
    }
    final int nameStart = i;
    i++;
    while (i < length && isNameChar(sql.charAt(i))) {
      i++;
    }
    final int nameEnd = i;
    if (i >= length || sql.charAt(i) != ':') {
      return null;
    }
    i++;
    final int typeStart = i;
    while (i < length && sql.charAt(i) != '}') {
      i++;
    }
    if (i >= length || i == typeStart) {
      return null;
    }
    return new Placeholder(sql.substring(nameStart, nameEnd), i + 1);
  }

  private static boolean isNameStart(final char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_';
  }

  private static boolean isNameChar(final char c) {
    return isNameStart(c) || (c >= '0' && c <= '9');
  }

  private record Placeholder(String name, int endExclusive) {}
}
