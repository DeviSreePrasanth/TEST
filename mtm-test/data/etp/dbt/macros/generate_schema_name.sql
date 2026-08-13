{#
    Override dbt's default schema naming.

    dbt's built-in behaviour concatenates the target schema with the model's custom schema
    (target_custom), which would produce datasets like `mtm_analytics_mtm_analytics` in
    Composer. Here a model's `+schema:` config is used verbatim, falling back to the
    target's default schema when a model doesn't set one. This keeps BigQuery dataset names
    identical across dev and prod, so the only thing that varies between environments is the
    GCP project.
#}

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}

{%- endmacro %}
