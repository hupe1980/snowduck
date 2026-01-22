from collections import OrderedDict

from sqlglot import exp
from sqlglot.dialects import DuckDB

from .context import DialectContext
from .preprocess import (
    preprocess_current_schema,
    preprocess_generator,
    preprocess_identifier,
    preprocess_info_schema,
    preprocess_semi_structured,
    preprocess_seq_functions,
    preprocess_system_calls,
    preprocess_variables,
)
from .transforms import (
    transform_copy,
    transform_create,
    transform_current_session_info,
    transform_describe,
    transform_lateral,
    transform_set,
    transform_show,
    transform_use,
)


class Dialect(DuckDB):
    _SQL_CACHE: "OrderedDict[tuple[str, str | None, str | None, str | None, str | None, str, str], str]" = OrderedDict()
    _SQL_CACHE_MAX = 1024

    def __init__(self, context: DialectContext):
        super().__init__()
        self._context = context

    @property
    def context(self) -> DialectContext:
        return self._context

    @classmethod
    def clear_cache(cls) -> None:
        cls._SQL_CACHE.clear()

    @classmethod
    def cache_size(cls) -> int:
        return len(cls._SQL_CACHE)

    @classmethod
    def _cache_key(
        cls, snowflake_sql: str, context: DialectContext
    ) -> tuple[str, str | None, str | None, str | None, str | None, str, str]:
        return (
            snowflake_sql,
            context.current_database,
            context.current_schema,
            context.current_role,
            context.current_warehouse,
            context.info_schema_manager.account_catalog_name,
            context.info_schema_manager.info_schema_name,
        )

    @classmethod
    def sql_with_cache(cls, expression: exp.Expression, dialect: "Dialect") -> str:
        snowflake_sql = expression.sql(dialect="snowflake")

        # Don't cache queries with variable substitutions (they can change frequently)
        has_variables = bool(
            expression.find(exp.Parameter) or expression.find(exp.Placeholder)
        )
        if has_variables:
            return expression.sql(dialect=dialect)

        key = cls._cache_key(snowflake_sql, dialect.context)
        cached = cls._SQL_CACHE.get(key)
        if cached is not None:
            cls._SQL_CACHE.move_to_end(key)
            return cached

        sql = expression.sql(dialect=dialect)
        cls._SQL_CACHE[key] = sql
        if len(cls._SQL_CACHE) > cls._SQL_CACHE_MAX:
            cls._SQL_CACHE.popitem(last=False)
        return sql

    class Generator(DuckDB.Generator):
        # Extend the TRANSFORMS dictionary to include the custom transformation
        TRANSFORMS = {
            **DuckDB.Generator.TRANSFORMS,  # Include existing DuckDB transforms
            exp.Show: lambda self, e: transform_show(e, context=self._context),
            exp.Create: lambda self, e: transform_create(e, context=self._context),
            exp.Describe: lambda self, e: transform_describe(e, context=self._context),
            exp.Use: lambda self, e: transform_use(e, context=self._context),
            exp.Select: lambda self, e: transform_current_session_info(
                e, context=self._context
            ),
            exp.Lateral: lambda self, e: transform_lateral(e, context=self._context),
            exp.Copy: lambda self, e: transform_copy(e, context=self._context),
            exp.Set: lambda self, e: transform_set(e, context=self._context),
        }

        def __init__(self, dialect: "Dialect", *args, **kwargs):
            super().__init__(*args, **kwargs)
            self._context = dialect.context

        def preprocess(self, expression: exp.Expression) -> exp.Expression:
            expression = expression.transform(
                preprocess_variables, context=self._context
            )
            expression = expression.transform(
                preprocess_identifier, context=self._context
            )
            expression = expression.transform(
                preprocess_info_schema, context=self._context
            )
            expression = expression.transform(
                preprocess_current_schema, context=self._context
            )
            expression = expression.transform(
                preprocess_system_calls, context=self._context
            )
            expression = expression.transform(
                preprocess_semi_structured, context=self._context
            )
            expression = expression.transform(
                preprocess_generator, context=self._context
            )
            expression = expression.transform(
                preprocess_seq_functions, context=self._context
            )
            return super().preprocess(expression)
