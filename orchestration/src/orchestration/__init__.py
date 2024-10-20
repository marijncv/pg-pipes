from typing import Iterator, Union
from pathlib import Path

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
def pg_pipes_asset(context: AssetExecutionContext) -> Iterator[Union[MaterializeResult, AssetCheckResult]]:
    with open_pipes_session(
        context=context,
        context_injector=PipesTempFileContextInjector(),
        message_reader=PipesTempFileMessageReader(),
    ) as pipes_session:

        context_path = pipes_session.context_injector_params["path"]
        messages_path = pipes_session.message_reader_params["path"]
        query = "SELECT * FROM pg_tables"

        conn = psycopg2.connect(
            database="pg_pipes",
            host=Path.home() / ".pgrx",
            port=28813
        )

        cursor = conn.cursor()

        # make sure the extension is enabled
        cursor.execute("drop extension if exists pg_pipes; create extension pg_pipes;")

        # start the pipes_session in postgres
        cursor.execute(f"select pipes_session('{context_path}', '{messages_path}', '{query}');")

    yield from pipes_session.get_results()