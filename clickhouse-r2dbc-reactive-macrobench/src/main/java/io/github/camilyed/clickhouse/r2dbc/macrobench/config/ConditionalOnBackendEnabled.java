package io.github.camilyed.clickhouse.r2dbc.macrobench.config;

import io.github.camilyed.clickhouse.r2dbc.macrobench.backend.Backend;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.context.annotation.Conditional;

/**
 * Gates a {@code @Bean} method so it only runs when {@code benchmark.backend} selects the given
 * {@link Backend}, or is {@link Backend#DUAL}. Needed because isolated single-backend runs are
 * how this module's CPU/RSS/thread measurements stay trusted - an idle backend's own
 * threads/connections would otherwise still show up in process-level resource numbers even with
 * zero traffic reaching it. See {@link Backend}'s own Javadoc and ROADMAP.md's Phase 12.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Conditional(BackendEnabledCondition.class)
public @interface ConditionalOnBackendEnabled {

  /** The backend a {@code @Bean} method requires to be active before it runs. */
  Backend value();
}
