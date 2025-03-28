# Import packages
from snowflake.core import Root

# from conn_help import session
from conn_help_local import session


session = session()
database = session.get_current_database()
schema = session.get_current_schema()


def query_cortex_search_service(
    service_name,
    query,
    columns=["chunk", "language"],
    filter={"@eq": {"language": "English"}},
):
    """
    Query the selected cortex search service with the given query and retrieve context documents.
    Return the context documents as a string.

    Args:
        session (snowflake.snowpark.session.Session): A session object.
        database (str): A database to query.
        schema (str): A schema to query.
        query (str): The query to search the cortex search service with.

    Returns:
        str: The concatenated string of context documents.
    """
    root = Root(session)
    cortex_search_service = (
        root.databases[database].schemas[schema].cortex_search_services[service_name]
    )

    context_documents = cortex_search_service.search(
        query, columns=columns, filter=filter, limit=1
    )
    results = context_documents.results

    search_col = "chunk"

    context_str = ""
    for i, r in enumerate(results):
        context_str += f"Context document {i+1}: {r[search_col]} \n" + "\n"

    return context_str, results
