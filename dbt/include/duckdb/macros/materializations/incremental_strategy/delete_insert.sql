{% macro duckdb__get_delete_insert_merge_sql(target, source, unique_key, dest_columns, incremental_predicates) -%}

    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}

    {% if unique_key %}
        {% if unique_key is sequence and unique_key is not string %}
            {#-- Match the keys in a subquery so that unqualified columns in the
                 incremental predicates refer to the target table only --#}
            delete from {{target }} as DBT_INCREMENTAL_TARGET
            where exists (
                select 1
                from {{ source }} as DBT_INTERNAL_SOURCE
                where
                {% for key in unique_key %}
                    DBT_INTERNAL_SOURCE.{{ key }} = DBT_INCREMENTAL_TARGET.{{ key }}
                    {{ "and " if not loop.last}}
                {% endfor %}
            )
            {%- for predicate in incremental_predicates or [] %}
                and ({{ predicate }})
            {%- endfor -%};
        {% else %}
            delete from {{ target }}
            where (
                {{ unique_key }}) in (
                select ({{ unique_key }})
                from {{ source }}
            )
            {%- for predicate in incremental_predicates or [] %}
                and ({{ predicate }})
            {%- endfor -%};

        {% endif %}
    {% endif %}

    insert into {{ target }} ({{ dest_cols_csv }})
    (
        select {{ dest_cols_csv }}
        from {{ source }}
    )

{%- endmacro %}
