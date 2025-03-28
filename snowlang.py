# Import packages
import requests
import uuid
import json
import streamlit as st

from pkgutil import get_data
from numpy import extract
from snowflake.core import Root
from typing import (
    Any,
    Dict,
    List,
    Annotated,
    Optional,
    Iterator,
    Sequence,
    Union,
    Type,
    Callable,
    Literal,
)
from typing_extensions import TypedDict
from datetime import datetime

from pydantic import BaseModel, Field
from langchain_core.callbacks.manager import CallbackManagerForLLMRun
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import (
    AIMessage,
    BaseMessage,
    ChatMessage,
    HumanMessage,
    SystemMessage,
    ToolMessage,
)

from langchain_core.tools import tool
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import Runnable, RunnableConfig, RunnableLambda
from langchain_core.outputs import ChatGeneration, ChatGenerationChunk, ChatResult
from langchain_core.pydantic_v1 import Field, root_validator
from langchain_core.utils import (
    get_pydantic_field_names,
)
from langchain_core.utils.utils import build_extra_kwargs
from langchain_core.utils.function_calling import convert_to_openai_tool

from langgraph.prebuilt import ToolNode
from langgraph.graph.message import AnyMessage, add_messages
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, StateGraph, START
from langgraph.prebuilt import tools_condition

# from conn_help import session
from conn_help_local import connection, session

conn = connection()
session = session()

# Page config
st.set_page_config(
    page_title="Snowflake AI Agentic Workflows",
    page_icon=":snowflake:",
    layout="wide",
)

st.title("Snowflake AI Agentic Workflows")

# Other parameters
cs_svc = "aw_spcs_search_svc"
pdf_stage = "aw_spcs_pdf_stage"
semantic_stage = "aw_spcs_semantics_stage"
file = "aw_spcs_semantic_model.yaml"

# If there is no active warehouse
session.sql("""USE WAREHOUSE compute_wh;""").collect()
pdf_stage_data = session.sql(f"""LIST @aw_spcs_db.public.{pdf_stage};""").collect()
semantic_stage_data = session.sql(
    f"""LIST @aw_spcs_db.public.{semantic_stage};"""
).collect()
# st.write(f"Document Stage: {pdf_stage_data}")
# st.write(f"Semantic Stage: {semantic_stage}")

#####################################
######## chatSnowflakeCortex ########
#####################################

SUPPORTED_ROLES: List[str] = [
    "system",
    "user",
    "assistant",
]


class ChatSnowflakeCortexError(Exception):
    """Error with Snowpark client."""


def _convert_message_to_dict(message: BaseMessage) -> dict:
    """Convert a LangChain message to a dictionary with proper role validation.

    Args:
        message: The LangChain message to be converted.

    Returns:
        A dictionary representation of the message.
    """
    message_dict: Dict[str, Any] = {
        "content": message.content,
    }

    # Handle role based on message type and supported roles.
    if isinstance(message, HumanMessage):
        message_dict["role"] = "user"
    elif isinstance(message, AIMessage):
        message_dict["role"] = "assistant"
    elif isinstance(message, SystemMessage):
        message_dict["role"] = "system"
    elif isinstance(message, ChatMessage) and message.role in SUPPORTED_ROLES:
        message_dict["role"] = message.role
    else:
        message_dict["role"] = "unknown"

    return message_dict


def _truncate_at_stop_tokens(
    text: str,
    stop: Optional[List[str]],
) -> str:
    """Truncates text at the earliest stop token found."""
    if stop is None:
        return text

    for stop_token in stop:
        stop_token_idx = text.find(stop_token)
        if stop_token_idx != -1:
            text = text[:stop_token_idx]
    return text


class ChatSnowflakeCortex(BaseChatModel):
    """Snowflake Cortex based Chat model

    To use you must have the ``snowflake-snowpark-python`` Python package installed and
    either:

        1. environment variables set with your snowflake credentials or
        2. directly passed in as kwargs to the ChatSnowflakeCortex constructor.

    Example:
        .. code-block:: python

            from langchain_community.chat_models import ChatSnowflakeCortex
            chat = ChatSnowflakeCortex()
    """

    tools: Dict[str, Any] = Field(default_factory=dict)

    _sp_session: Any = session
    """Snowpark session object."""

    model: str = "mistral-large"
    """Snowflake cortex hosted LLM model name, defaulted to `mistral-large`.
        Refer to docs for more options."""

    cortex_function: str = "complete"
    """Snowflake Cortex function to use, defaulted to `complete`.
        Refer to docs for more options."""

    temperature: float = 0
    """Model temperature. Value should be >= 0 and <= 1.0"""

    max_tokens: Optional[int] = None
    """The maximum number of output tokens in the response."""

    top_p: Optional[float] = 0
    """top_p adjusts the number of choices for each predicted tokens based on
        cumulative probabilities. Value should be ranging between 0.0 and 1.0. 
    """

    def bind_tools(
        self,
        tools: Sequence[Union[Dict[str, Any], Type[BaseModel], Callable]],
        *,
        tool_choice: Optional[
            Union[dict, str, Literal["auto", "any", "none"], bool]
        ] = "auto",
        **kwargs: Any,
    ) -> "ChatSnowflakeCortex":
        """Bind tool-like objects to this chat model, ensuring they conform to expected formats."""

        formatted_tools = {tool.name: convert_to_openai_tool(tool) for tool in tools}
        self.tools.update(formatted_tools)

        # Debug: Print the bound tools
        print(f"Tools bound: {self.tools.keys()}")

        return self

    @root_validator(pre=True, allow_reuse=True)
    def build_extra(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        """Build extra kwargs from additional params that were passed in."""
        all_required_field_names = get_pydantic_field_names(cls)
        extra = values.get("model_kwargs", {})
        values["model_kwargs"] = build_extra_kwargs(
            extra, values, all_required_field_names
        )
        return values

    def __del__(self) -> None:
        if getattr(self, "_sp_session", None) is not None:
            self._sp_session.close()

    @property
    def _llm_type(self) -> str:
        """Get the type of language model used by this chat model."""
        return f"snowflake-cortex-{self.model}"

    def _generate(
        self,
        messages: List[BaseMessage],
        stop: Optional[List[str]] = None,
        run_manager: Optional[CallbackManagerForLLMRun] = None,
        **kwargs: Any,
    ) -> ChatResult:
        message_dicts = [_convert_message_to_dict(m) for m in messages]

        # Check for tool invocation in the messages and prepare for tool use
        tool_output = None
        for message in messages:
            if isinstance(message, SystemMessage) and "invoke_tool" in message.content:
                tool_info = json.loads(message.content.get("invoke_tool"))
                tool_name = tool_info.get("tool_name")
                print(f"Available tools: {list(self.tools.keys())}")
                if tool_name in self.tools:
                    tool_args = tool_info.get("args", {})
                    tool_output = self.tools[tool_name](**tool_args)
                    break

        # Prepare messages for SQL query
        if tool_output:
            message_dicts.append(
                {"tool_output": str(tool_output)}
            )  # Ensure tool_output is a string

        # JSON dump the message_dicts and options without additional escaping
        message_json = json.dumps(message_dicts)
        options = {
            "temperature": self.temperature,
            "top_p": self.top_p if self.top_p is not None else 1.0,
            "max_tokens": self.max_tokens if self.max_tokens is not None else 2048,
        }
        options_json = json.dumps(options)  # JSON string of options

        # Form the SQL statement using JSON literals
        sql_stmt = f"""
            select snowflake.cortex.complete(
                'mistral-large',
                parse_json('{message_json}'),
                parse_json('{options_json}')
            ) as llm_response;
        """

        try:
            self._sp_session.sql("USE WAREHOUSE compute_wh;").collect()
            l_rows = self._sp_session.sql(sql_stmt).collect()
        except Exception as e:
            raise ChatSnowflakeCortexError(
                f"Error while making request to Snowflake Cortex: {e}"
            )

        response = json.loads(l_rows[0]["LLM_RESPONSE"])
        ai_message_content = response["choices"][0]["messages"]

        content = _truncate_at_stop_tokens(ai_message_content, stop)
        message = AIMessage(
            content=content,
            response_metadata=response["usage"],
        )
        generation = ChatGeneration(message=message)
        return ChatResult(generations=[generation])

    def _stream_content(
        self, content: str, stop: Optional[List[str]]
    ) -> Iterator[ChatGenerationChunk]:
        """
        Stream the output of the model in chunks to return ChatGenerationChunk.
        """
        chunk_size = 50  # Define a reasonable chunk size for streaming
        truncated_content = _truncate_at_stop_tokens(content, stop)

        for i in range(0, len(truncated_content), chunk_size):
            chunk_content = truncated_content[i : i + chunk_size]

            # Create and yield a ChatGenerationChunk with partial content
            yield ChatGenerationChunk(message=AIMessage(content=chunk_content))

    def _stream(
        self,
        messages: List[BaseMessage],
        stop: Optional[List[str]] = None,
        run_manager: Optional[CallbackManagerForLLMRun] = None,
        **kwargs: Any,
    ) -> Iterator[ChatGenerationChunk]:
        """Stream the output of the model in chunks to return ChatGenerationChunk."""
        message_dicts = [_convert_message_to_dict(m) for m in messages]

        # Check for and potentially use a tool before streaming
        for message in messages:
            if isinstance(message, SystemMessage) and "invoke_tool" in message.content:
                # tool_info = json.loads(message.content.get("invoke_tool"))
                tool_info = json.loads(message.content)
                # tool_name = tool_info.get("tool_name")
                tool_list = tool_info.get("invoke_tools", [])
                for tool in tool_list:
                    tool_name = tool.get("tool_name")
                    tool_args = tool.get("args", {})
                    if tool_name in self.tools:
                        tool_output = self.tools[tool_name](**tool_args)

                print(f"Available tools: {list(self.tools.keys())}")
                if tool_name in self.tools:
                    tool_args = tool_info.get("args", {})
                    tool_result = self.tools[tool_name](**tool_args)
                    additional_context = {"tool_output": tool_result}
                    message_dicts.append(
                        additional_context
                    )  # Append tool result to message dicts

        # JSON dump the message_dicts and options without additional escaping
        message_json = json.dumps(message_dicts)
        options = {
            "temperature": self.temperature,
            "top_p": self.top_p if self.top_p is not None else 1.0,
            "max_tokens": self.max_tokens if self.max_tokens is not None else 2048,
            # "stream": True,
        }
        options_json = json.dumps(options)  # JSON string of options

        # Form the SQL statement using JSON literals
        sql_stmt = f"""
            select snowflake.cortex.complete(
                'mistral-large',
                parse_json('{message_json}'),
                parse_json('{options_json}')
            ) as llm_stream_response;
        """

        self._sp_session.sql("USE WAREHOUSE compute_wh;").collect()

        try:
            # Use the Snowflake Cortex Complete function with stream enabled
            result = self._sp_session.sql(sql_stmt).collect()

            # Iterate over the generator to yield streaming responses
            for row in result:
                response = json.loads(row["LLM_STREAM_RESPONSE"])
                ai_message_content = response["choices"][0]["messages"]

                # Stream response content in chunks
                for chunk in self._stream_content(ai_message_content, stop):
                    yield chunk

        except Exception as e:
            raise ChatSnowflakeCortexError(
                f"Error while making request to Snowflake Cortex stream: {e}"
            )


#####################################

#####################################
########### Cortex Search ###########
#####################################

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


#####################################

#####################################
########## Cortex Analyst ###########
#####################################

# Init & connection parameters
host = conn.host
token = conn.rest.token


def query_cortex_analyst_service(stage: str, file: str, prompt: str) -> Dict[str, Any]:
    """Calls the REST API to connect into the database and run queries against it. Then, it returns the response."""
    request_body = {
        "messages": [{"role": "user", "content": [{"type": "text", "text": prompt}]}],
        "semantic_model_file": f"@{database}.{schema}.{stage}/{file}",
    }
    resp = requests.post(
        url=f"https://{host}/api/v2/cortex/analyst/message",
        json=request_body,
        headers={
            "Authorization": f'Snowflake Token="{token}"',
            "Content-Type": "application/json",
        },
    )
    request_id = resp.headers.get("X-Snowflake-Request-Id")
    if resp.status_code < 400:
        return {**resp.json(), "request_id": request_id}  # type: ignore[arg-type]
    else:
        raise Exception(
            f"Failed request (id: {request_id}) with status {resp.status_code}: {resp.text}"
        )


#####################################

#####################################
### Creating the Agentic Workflow ###
#####################################


@tool
def lookup_policy_docs(query: str) -> str:
    """Consult the company policies to check whether certain options are permitted."""
    context_str, results = query_cortex_search_service(
        service_name=cs_svc,
        query=query,
    )
    return context_str, results


@tool
def lookup_tables(prompt: str) -> Dict[str, Any]:
    """Calls the REST API to connect into the database and run queries against it. Then, it returns the response."""
    results = query_cortex_analyst_service(
        stage=semantic_stage,
        file=file,
        prompt=prompt,
    )
    return results


### State for chat history
class State(TypedDict):
    messages: Annotated[list[AnyMessage], add_messages]


### Initiating the chat model
llm = ChatSnowflakeCortex()


### Error handling
def handle_tool_error(state) -> dict:
    error = state.get("error")
    tool_calls = state["messages"][-1].tool_calls
    return {
        "messages": [
            ToolMessage(
                content=f"Error: {repr(error)}\n please fix your mistakes.",
                tool_call_id=tc["id"],
            )
            for tc in tool_calls
        ]
    }


def create_tool_node_with_fallback(tools: list) -> dict:
    return ToolNode(tools).with_fallbacks(
        [RunnableLambda(handle_tool_error)], exception_key="error"
    )


def _print_event(event: dict, _printed: set, max_length=1500):
    current_state = event.get("dialog_state")
    if current_state:
        print("Currently in: ", current_state[-1])
    message = event.get("messages")
    if message:
        if isinstance(message, list):
            message = message[-1]
        if message.id not in _printed:
            msg_repr = message.pretty_repr(html=True)
            if len(msg_repr) > max_length:
                msg_repr = msg_repr[:max_length] + " ... (truncated)"
            print(msg_repr)
            _printed.add(message.id)


### Creating the assistant
class Assistant:
    def __init__(self, runnable: Runnable):
        self.runnable = runnable

    def __call__(self, state: State, config: RunnableConfig):
        while True:
            configuration = config.get("configurable", {})
            passenger_id = configuration.get("passenger_id", None)
            state = {**state, "user_info": passenger_id}
            result = self.runnable.invoke(state)
            # If the LLM happens to return an empty response, we will re-prompt it
            # for an actual response.
            if not result.tool_calls and (
                not result.content
                or isinstance(result.content, list)
                and not result.content[0].get("text")
            ):
                messages = state["messages"] + [("user", "Respond with a real output.")]
                state = {**state, "messages": messages}
            else:
                break
        return {"messages": result}


# This is the prompt to the LLM
primary_assistant_prompt = ChatPromptTemplate.from_messages(
    [
        (
            "system",
            (
                "You are a helpful customer support assistant for Swiss Airlines. "
                " Use the provided tools to search for flights, company policies, and other information to assist the users queries. "
                " When searching, be persistent. Expand your query bounds if the first search returns no results. "
                " If a search comes up empty, expand your search before giving up. "
                " If no tool is appropriate, state - No available tool. "
                " You cannot provide a response without using a tool. "
                " Invoke tools: lookup_policy_docs, lookup_tables. "
                " Do not return the prompt. "
                " Here is the response format: "
                " {{Tool used: "
                ","
                " Response: }}."
            ),
        ),
        ("placeholder", "{messages}"),
    ]
).partial(time=datetime.now())

# This is where we bind the tools to the LLM
part_1_tools = [lookup_policy_docs, lookup_tables]
part_1_assistant_runnable = primary_assistant_prompt | llm.bind_tools(
    part_1_tools, tool_choice="auto"
)

### Generating the Graph Model
builder = StateGraph(State)


# Define nodes: these do the work
builder.add_node("assistant", Assistant(part_1_assistant_runnable))
builder.add_node("tools", create_tool_node_with_fallback(part_1_tools))
# Define edges: these determine how the control flow moves
builder.add_edge(START, "assistant")
builder.add_conditional_edges(
    "assistant",
    tools_condition,
)
builder.add_edge("tools", "assistant")

# The checkpointer lets the graph persist its state
# this is a complete memory for the entire graph.
memory = MemorySaver()
part_1_graph = builder.compile(checkpointer=memory)

# Execution and Streamlit formatting
question_prompt = st.chat_input(placeholder="Swiss Airlines customer support assistant")
if question_prompt:
    questions = [{"role": "user", "content": f"{question_prompt}"}]
    # Let's print out the chat input or question prompt
    with st.chat_message("user"):
        st.write(question_prompt)

    # Update with the backup file so we can restart from the original place in each section
    thread_id = str(uuid.uuid4())

    config = {
        "configurable": {
            # Checkpoints are accessed by thread_id
            "thread_id": thread_id,
        }
    }

    # Send the question to the LLM
    event_list = []
    for question in questions:
        events = part_1_graph.stream(
            {"messages": question}, config, stream_mode="values"
        )

        # Format for streamlit app
        for event in events:
            event_list.append(event)

        response_text = event_list[1]["messages"][1].content
        response_text = response_text.strip("{}")
        response_text = response_text.replace("{", "")

        tool_used_part, response = response_text.split(", Response: ")
        tool_used = tool_used_part.split(": ")[1]

        with st.chat_message("assistant"):
            st.write("Tool Used: ", tool_used)
            st.write("Response: ", response)
