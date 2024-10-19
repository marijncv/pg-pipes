from typing import Iterator, Union

from dagster import (
    AssetExecutionContext,
    PipesTempFileContextInjector,
    PipesTempFileMessageReader,
    MaterializeResult,
    AssetCheckResult,
    asset,
    open_pipes_session,
)
import psycopg2

@asset
def some_pipes_asset(context: AssetExecutionContext) -> Iterator[Union[MaterializeResult, AssetCheckResult]]:
    with open_pipes_session(
        context=context,
        context_injector=PipesTempFileContextInjector(),
        message_reader=PipesTempFileMessageReader(),
    ) as pipes_session:

        context_path = pipes_session.context_injector_params["path"]
        messages_path = pipes_session.message_reader_params["path"]

        conn = psycopg2.connect(
            database="pg_pipes",
            host="/home/marijncv/.pgrx",
            port=28813
        )

        cursor = conn.cursor()

        cursor.execute(f"select pipes_session('{context_path}', '{messages_path}', 'select * from pg_tables');")

    # Yield any remaining results received from the external process.
    yield from pipes_session.get_results()