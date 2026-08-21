package io.github.camilyed.clickhouse.r2dbc.core;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Exercises {@link ClickHouseSqlPlaceholderScanner} directly and thoroughly — the two {@code
 * parameterNamesIn} cases in {@link ClickHouseQueryTest} cover the public-API happy path, this
 * class covers every context-skipping rule (string literals, quoted identifiers, every comment form
 * ClickHouse's own lexer accepts, nested block comments), the escaping rules within quoted spans,
 * malformed placeholder shapes that must not match, and boundary/unterminated-input cases — the
 * exact set of concerns this class exists to get right that a single regex could not express.
 */
class ClickHouseSqlPlaceholderScannerTest {

  @Test
  void shouldFindNothingInAnEmptyString() {
    // when
    final var names = ClickHouseSqlPlaceholderScanner.parameterNamesIn("");

    // then
    assertThat(names).isEmpty();
  }

  @Test
  void shouldFindNoParameterNamesInAPlainQueryWithNoPlaceholders() {
    // when
    final var names = ClickHouseSqlPlaceholderScanner.parameterNamesIn("SELECT 1");

    // then
    assertThat(names).isEmpty();
  }

  @Test
  void shouldFindASinglePlaceholderName() {
    // when
    final var names = ClickHouseSqlPlaceholderScanner.parameterNamesIn("SELECT {a:UInt32}");

    // then
    assertThat(names).containsExactly("a");
  }

  @Test
  void shouldFindPlaceholderNamesInFirstOccurrenceOrderWithoutDuplicates() {
    // when
    final var names =
        ClickHouseSqlPlaceholderScanner.parameterNamesIn(
            "SELECT * FROM t WHERE b = {b:String} AND a = {a:UInt32} AND b = {b:String}");

    // then
    assertThat(names).containsExactly("b", "a");
  }

  @Test
  void shouldAllowUnderscoreAndDigitsAfterTheFirstNameCharacter() {
    // when
    final var names = ClickHouseSqlPlaceholderScanner.parameterNamesIn("SELECT {_a1:UInt32}");

    // then
    assertThat(names).containsExactly("_a1");
  }

  @Test
  void shouldAllowParenthesesInsideTheTypeHalf() {
    // when
    final var names =
        ClickHouseSqlPlaceholderScanner.parameterNamesIn(
            "SELECT {a:Nullable(String)}, {b:Map(String, UInt8)}");

    // then
    assertThat(names).containsExactly("a", "b");
  }

  @Test
  void shouldNotTreatAPlaceholderShapeInsideASingleQuotedStringAsAParameter() {
    // when
    final var names =
        ClickHouseSqlPlaceholderScanner.parameterNamesIn("SELECT '{not_a_parameter:String}'");

    // then
    assertThat(names).isEmpty();
  }

  @Test
  void shouldFindARealPlaceholderAfterAStringLiteralContainingPlaceholderShapedText() {
    // when
    final var names =
        ClickHouseSqlPlaceholderScanner.parameterNamesIn(
            "SELECT '{not_a_parameter:String}', {a:UInt32}");

    // then
    assertThat(names).containsExactly("a");
  }

  @Test
  void shouldStayInsideAStringLiteralAcrossABackslashEscapedQuote() {
    // when
    final var names =
        ClickHouseSqlPlaceholderScanner.parameterNamesIn(
            "SELECT 'it\\'s {not_a_parameter:String}'");

    // then
    assertThat(names).isEmpty();
  }

  @Test
  void shouldStayInsideAStringLiteralAcrossADoubledQuoteEscape() {
    // when
    final var names =
        ClickHouseSqlPlaceholderScanner.parameterNamesIn("SELECT 'it''s {not_a_parameter:String}'");

    // then
    assertThat(names).isEmpty();
  }

  @Test
  void shouldNotTreatAPlaceholderShapeInsideADoubleQuotedIdentifierAsAParameter() {
    // when
    final var names =
        ClickHouseSqlPlaceholderScanner.parameterNamesIn(
            "SELECT \"col{not_a_parameter:String}\" FROM t");

    // then
    assertThat(names).isEmpty();
  }

  @Test
  void shouldNotTreatAPlaceholderShapeInsideABacktickQuotedIdentifierAsAParameter() {
    // when
    final var names =
        ClickHouseSqlPlaceholderScanner.parameterNamesIn(
            "SELECT `col{not_a_parameter:String}` FROM t");

    // then
    assertThat(names).isEmpty();
  }

  @Test
  void shouldNotThrowOnAnUnterminatedStringLiteral() {
    // when
    final var names =
        ClickHouseSqlPlaceholderScanner.parameterNamesIn("SELECT '{not_a_parameter:String}");

    // then
    assertThat(names).isEmpty();
  }

  @Test
  void shouldNotThrowOnAStringLiteralThatIsJustAnUnterminatedQuote() {
    // when
    final var names = ClickHouseSqlPlaceholderScanner.parameterNamesIn("'");

    // then
    assertThat(names).isEmpty();
  }

  @Test
  void shouldNotTreatAPlaceholderShapeInsideADashDashLineCommentAsAParameter() {
    // when
    final var names =
        ClickHouseSqlPlaceholderScanner.parameterNamesIn("SELECT 1 -- {not_a_parameter:UInt32}");

    // then
    assertThat(names).isEmpty();
  }

  @Test
  void shouldResumeScanningAfterADashDashLineCommentEndsAtANewline() {
    // when
    final var names =
        ClickHouseSqlPlaceholderScanner.parameterNamesIn(
            "SELECT 1 -- {not_a_parameter:UInt32}\nAND a = {a:UInt32}");

    // then
    assertThat(names).containsExactly("a");
  }

  @Test
  void shouldNotTreatASingleDashAsALineComment() {
    // when
    final var names =
        ClickHouseSqlPlaceholderScanner.parameterNamesIn("SELECT {a:UInt32}-{b:UInt32}");

    // then
    assertThat(names).containsExactly("a", "b");
  }

  @Test
  void shouldNotTreatAPlaceholderShapeInsideAHashBangLineCommentAsAParameter() {
    // when
    final var names =
        ClickHouseSqlPlaceholderScanner.parameterNamesIn("SELECT 1 #!{not_a_parameter:UInt32}");

    // then
    assertThat(names).isEmpty();
  }

  @Test
  void shouldNotTreatAPlaceholderShapeInsideAHashSpaceLineCommentAsAParameter() {
    // when
    final var names =
        ClickHouseSqlPlaceholderScanner.parameterNamesIn("SELECT 1 # {not_a_parameter:UInt32}");

    // then
    assertThat(names).isEmpty();
  }

  @Test
  void shouldNotTreatABareHashNotFollowedBySpaceOrBangAsALineComment() {
    // when
    final var names = ClickHouseSqlPlaceholderScanner.parameterNamesIn("SELECT 1#{a:UInt32}");

    // then
    assertThat(names).containsExactly("a");
  }

  @Test
  void shouldNotTreatAPlaceholderShapeInsideASlashSlashLineCommentAsAParameter() {
    // when
    final var names =
        ClickHouseSqlPlaceholderScanner.parameterNamesIn("SELECT 1 // {not_a_parameter:UInt32}");

    // then
    assertThat(names).isEmpty();
  }

  @Test
  void shouldNotTreatAPlaceholderShapeInsideABlockCommentAsAParameter() {
    // when
    final var names =
        ClickHouseSqlPlaceholderScanner.parameterNamesIn("SELECT /* {not_a_parameter:UInt32} */ 1");

    // then
    assertThat(names).isEmpty();
  }

  @Test
  void shouldFindARealPlaceholderAfterABlockComment() {
    // when
    final var names =
        ClickHouseSqlPlaceholderScanner.parameterNamesIn("SELECT /* comment */ {a:UInt32}");

    // then
    assertThat(names).containsExactly("a");
  }

  @Test
  void shouldTreatNestedBlockCommentsAsOneSpanMatchingClickHousesOwnNestingLexer() {
    // when
    final var names =
        ClickHouseSqlPlaceholderScanner.parameterNamesIn(
            "SELECT /* outer /* inner {not_a_parameter:UInt32} */ still comment */ {a:UInt32}");

    // then
    assertThat(names).containsExactly("a");
  }

  @Test
  void shouldNotThrowOnAnUnterminatedBlockComment() {
    // when
    final var names =
        ClickHouseSqlPlaceholderScanner.parameterNamesIn("SELECT /* {not_a_parameter:UInt32}");

    // then
    assertThat(names).isEmpty();
  }

  @Test
  void shouldNotMatchAPlaceholderWhoseNameStartsWithADigit() {
    // when
    final var names = ClickHouseSqlPlaceholderScanner.parameterNamesIn("SELECT {123abc:UInt32}");

    // then
    assertThat(names).isEmpty();
  }

  @Test
  void shouldNotMatchAPlaceholderWithAnEmptyName() {
    // when
    final var names = ClickHouseSqlPlaceholderScanner.parameterNamesIn("SELECT {:UInt32}");

    // then
    assertThat(names).isEmpty();
  }

  @Test
  void shouldNotMatchAPlaceholderMissingItsColon() {
    // when
    final var names = ClickHouseSqlPlaceholderScanner.parameterNamesIn("SELECT {a UInt32}");

    // then
    assertThat(names).isEmpty();
  }

  @Test
  void shouldNotMatchAPlaceholderWithAnEmptyType() {
    // when
    final var names = ClickHouseSqlPlaceholderScanner.parameterNamesIn("SELECT {a:}");

    // then
    assertThat(names).isEmpty();
  }

  @Test
  void shouldNotThrowOnAnUnterminatedPlaceholder() {
    // when
    final var names = ClickHouseSqlPlaceholderScanner.parameterNamesIn("SELECT {a:UInt32");

    // then
    assertThat(names).isEmpty();
  }

  @Test
  void shouldFindOnlyTheRealPlaceholdersInARealisticMixedQuery() {
    // given
    final String sql =
        "SELECT id, '{escaped:String}' AS literal_field -- {commented:UInt32}\n"
            + "FROM events\n"
            + "WHERE user_id = {user_id:UUID}\n"
            + "  AND status = {status:String} /* filter by status, e.g. {example:String} */\n"
            + "  AND created_at > {since:DateTime}";

    // when
    final var names = ClickHouseSqlPlaceholderScanner.parameterNamesIn(sql);

    // then
    assertThat(names).containsExactly("user_id", "status", "since");
  }
}
