package io.github.camilyed.clickhouse.r2dbc.macrobench.config;

import io.github.camilyed.clickhouse.r2dbc.macrobench.backend.Backend;
import java.util.Map;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

/** Backs {@link ConditionalOnBackendEnabled} - see that annotation's Javadoc for the contract. */
final class BackendEnabledCondition implements Condition {

  private static final String BACKEND_PROPERTY = "benchmark.backend";
  private static final String DEFAULT_BACKEND = "dual";

  @Override
  public boolean matches(final ConditionContext context, final AnnotatedTypeMetadata metadata) {
    final Map<String, Object> attributes =
        metadata.getAnnotationAttributes(ConditionalOnBackendEnabled.class.getName());
    if (attributes == null) {
      return false;
    }
    final Backend required = (Backend) attributes.get("value");
    final Backend configured = configuredBackend(context);
    return configured == Backend.DUAL || configured == required;
  }

  private Backend configuredBackend(final ConditionContext context) {
    final String raw = context.getEnvironment().getProperty(BACKEND_PROPERTY, DEFAULT_BACKEND);
    return Backend.fromProperty(raw);
  }
}
