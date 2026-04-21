"""Utilities, including those to be used in the project Synapse Notebooks."""

import csv
from datetime import date, datetime
import importlib.util
from io import StringIO
import json
import os
from pathlib import Path
from tempfile import NamedTemporaryFile
from types import ModuleType
from typing import Any, Literal, Optional, overload

from IPython.display import display
import numpy as np
import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame, SparkSession
import pytz
import requests

from ..exceptions import TSVLoggedError
from .cache import YESTERDAY_DATE
from .logging import initialize_logging, log_invalid_fields, setup_tsv_logger
from .teams import send_alert

logger = initialize_logging()


def execute_vars_file(vars_file_path: str) -> None:
    """
    Safely read and execute Python code from a given variables file.

    Example:
    -------
    >>> ripple_variables = execute_vars_file(ripple_vars_file_path)  # doctest: +SKIP

    """
    if not os.path.isfile(vars_file_path):
        msg = f"Error: The file '{vars_file_path}' does not exist."
        raise FileNotFoundError(msg)

    # exec_scope = {}
    try:
        with open(vars_file_path, "r", encoding="utf-8") as file:
            exec(file.read(), globals())
            # file_content = file.read()
        # Define a restricted execution scope
        # exec_scope = {}
        # exec(file_content, {}, exec_scope)
        # return exec_scope  # Return executed variables safely
    except Exception as e:
        msg = f"Error executing file '{vars_file_path}': {e}"
        raise RuntimeError(msg) from e


def read_vars_file_as_module(filepath: str | Path) -> ModuleType:
    """Load a Python file as a module dynamically."""
    filepath = Path(filepath)

    # Create a module spec from the file
    spec = importlib.util.spec_from_file_location(filepath.stem, filepath)

    if not spec or not spec.loader:
        msg = f"Could not load module from {filepath}"
        raise ValueError(msg)

    # Create a new module based on the spec
    module = importlib.util.module_from_spec(spec)

    # Execute the module to populate it
    spec.loader.exec_module(module)

    return module


def fetch_and_save_api_data(
    url: str, headers: dict, data: dict, file_path: str
) -> None:
    """Make the REST API request."""
    # response = requests.get(url, headers=headers, data=data)
    response = requests.post(url, headers=headers, data=data)
    # Check the API response return code
    if response.status_code == requests.codes["okay"]:
        # Save the response to a csv file
        with open(file_path, "wb") as file:
            file.write(response.content)
        logger.info("Export successful! Data saved to %s", file_path)
    else:
        logger.info(
            "Failed to export data: %d - %s", response.status_code, response.text
        )


def _redcap_project_info(url: str, token: str) -> tuple[str, str]:
    """Given a token, get project title and PID."""
    response = requests.post(
        url,
        {
            "token": token,
            "content": "project",
            "format": "json",
            "returnFormat": "json",
        },
    )
    if response.status_code == requests.codes["okay"]:
        result = response.json()
        return result["project_title"], result["project_id"]
    response.raise_for_status()
    raise requests.HTTPError


@overload
def _fetch_api_data(
    url: str,
    headers: dict,
    data: dict | str,
    _index: Literal[0, 1, 3],
    spark: None = None,
    *,
    capture_invalid_fields: bool = False,
) -> pd.DataFrame | list[str]: ...
@overload
def _fetch_api_data(
    url: str,
    headers: dict,
    data: dict | str,
    _index: Literal[2],
    spark: SparkSession,
    *,
    capture_invalid_fields: bool = False,
) -> SparkDataFrame | list[str]: ...
def _fetch_api_data(  # noqa: PLR0911
    url: str,
    headers: dict,
    data: dict | str,
    _index: Literal[0, 1, 2, 3],
    spark: Optional[SparkSession] = None,
    *,
    capture_invalid_fields: bool = False,
) -> pd.DataFrame | SparkDataFrame | list[str]:
    """Handle various `fetch_api_data` functions."""
    try:
        response = requests.post(url, headers=headers, data=data)
        if response.status_code == requests.codes["okay"]:
            if response.text.strip():  # Check if the response content is not empty
                csv_data = StringIO(response.text)
                match _index:
                    case 3:
                        return pd.read_csv(
                            csv_data,
                            low_memory=False,
                            dtype={"customId": str, "mrn": str},
                        )
                    case 2:
                        if spark is None:
                            msg = "Spark session required for `fetch_api_data2`."
                            raise ValueError(msg)

                        with NamedTemporaryFile(
                            mode="w", suffix=".csv", delete=False, newline=""
                        ) as tmp_file:
                            tmp_file.write(response.text)
                            tmp_file_path = Path(tmp_file.name)
                        try:
                            return spark.read.csv(
                                str(tmp_file_path), header=True, inferSchema=True
                            )
                        finally:
                            tmp_file_path.unlink(missing_ok=True)
                    case 0 | 1 | _:
                        return pd.read_csv(csv_data, low_memory=False)
            logger.info("Empty response received from the API.")
            # Return an empty dataframe when the response is empty
            return pd.DataFrame()
        logger.info(
            "Failed to fetch data: %d - %s", response.status_code, response.text
        )
        invalid_fields_message = (
            'The following values in the parameter "fields" are not valid: '
        )
        invalid_fields = [
            _[1:-1]
            for _ in response.text.split(invalid_fields_message, 1)[1].split(",")
            if _
        ]
        new_invald_fields = log_invalid_fields(invalid_fields)
        if new_invald_fields:
            msg = "These fields from Curious don't exist in REDCap project"
            if isinstance(data, dict):
                project_title, pid = _redcap_project_info(url, data["token"])
                msg = f"{msg} {project_title} (PID {pid})"
            send_alert(
                f"{msg}: {new_invald_fields}",
                "Send webhook alerts to 🔴 MS Fabric - Failures",
            )
        if capture_invalid_fields and invalid_fields_message in response.text:
            return invalid_fields
        # Return an empty dataframe when the status code is not 200
        return pd.DataFrame()
    except Exception as e:
        logger.info("An error occurred: %s", e)
        # Return an empty dataframe in case of an exception
        return pd.DataFrame()


@overload
def fetch_api_data(
    url: str,
    headers: dict,
    data: dict | str,
    return_type: type[list],
    column: str = "record",
    *,
    capture_invalid_fields: bool = False,
) -> list[str]: ...


@overload
def fetch_api_data(
    url: str,
    headers: dict,
    data: dict | str,
    return_type: type[pd.DataFrame] = ...,
    column: str | None = None,
    *,
    capture_invalid_fields: Literal[True],
) -> pd.DataFrame: ...


@overload
def fetch_api_data(
    url: str,
    headers: dict,
    data: dict | str,
    return_type: type[pd.DataFrame] = ...,
    column: str | None = None,
    *,
    capture_invalid_fields: Literal[False] = False,
) -> pd.DataFrame: ...


def fetch_api_data(  # noqa: PLR0913
    url: str,
    headers: dict,
    data: dict | str,
    return_type: type[list | pd.DataFrame] = pd.DataFrame,
    column: str | None = None,
    *,
    capture_invalid_fields: bool = False,
) -> pd.DataFrame | list[str]:
    """Fetch REST API response data and load it into a Pandas Dataframe or list."""
    df = _fetch_api_data(
        url, headers, data, 0, capture_invalid_fields=capture_invalid_fields
    )
    if isinstance(df, list) or return_type is pd.DataFrame:
        return df
    if return_type is list:
        return df.get(column, pd.Series()).to_list()
    raise NotImplementedError


@overload
def fetch_api_data1(
    url: str, headers: dict, data: dict | str, *, capture_invalid_fields: Literal[True]
) -> Optional[pd.DataFrame | list[str]]: ...
@overload
def fetch_api_data1(
    url: str,
    headers: dict,
    data: dict | str,
    *,
    capture_invalid_fields: Literal[False] = False,
) -> Optional[pd.DataFrame]: ...
def fetch_api_data1(
    url: str, headers: dict, data: dict | str, *, capture_invalid_fields: bool = False
) -> Optional[pd.DataFrame | list[str]]:
    """Fetch REST API response data and load into it a Pandas Dataframe."""
    df = _fetch_api_data(
        url, headers, data, 1, capture_invalid_fields=capture_invalid_fields
    )
    if capture_invalid_fields and isinstance(df, list):
        return df
    return None if (isinstance(df, pd.DataFrame) and df.empty) else df


@overload
def fetch_api_data3(
    url: str, headers: dict, data: dict | str, *, capture_invalid_fields: Literal[True]
) -> pd.DataFrame | list[str]: ...
@overload
def fetch_api_data3(
    url: str, headers: dict, data: dict | str, *, capture_invalid_fields: Literal[False]
) -> pd.DataFrame: ...
def fetch_api_data3(
    url: str, headers: dict, data: dict | str, *, capture_invalid_fields: bool = False
) -> pd.DataFrame | list[str]:
    """
    Fetch REST API response data and load it into a Pandas Dataframe.

    Preserves leading zeros in the 'customId' column.
    """
    return _fetch_api_data(
        url, headers, data, 3, capture_invalid_fields=capture_invalid_fields
    )


@overload
def fetch_api_data2(
    url: str,
    headers: dict,
    data: dict | str,
    spark: SparkSession,
    *,
    capture_invalid_fields: Literal[True],
) -> SparkDataFrame | list[str]: ...
@overload
def fetch_api_data2(
    url: str,
    headers: dict,
    data: dict | str,
    spark: SparkSession,
    *,
    capture_invalid_fields: Literal[False],
) -> SparkDataFrame: ...
def fetch_api_data2(
    url: str,
    headers: dict,
    data: dict | str,
    spark: SparkSession,
    *,
    capture_invalid_fields: bool = False,
) -> SparkDataFrame | list[str]:
    """Fetch REST API response data and load it into a PySpark Dataframe."""
    return _fetch_api_data(
        url, headers, data, 2, spark, capture_invalid_fields=capture_invalid_fields
    )


def peek_into_dataframe(df: pd.DataFrame) -> None:
    """Display 10 rows from the Pandas DataFrame hosting the APIs reponses."""
    peek_into_dataframe2(df, 10)


def peek_into_dataframe2(df: pd.DataFrame, load_rows: int) -> None:
    """Peek into the Pandas dataframes hosting the APIs responses."""
    df.index.name = "Index"
    pd.set_option("display.max_columns", None)

    # Set timezone variable
    edt_timezone = pytz.timezone("America/New_York")
    now = datetime.now(edt_timezone)
    logger.info(
        "Date-Time data was last displayed: %s", now.strftime("%Y-%m-%d %I:%M:%S %p %Z")
    )

    # Apply alternating row colors
    def highlight_rows(row):
        return [
            "background-color: #f2f2f2"
            if row.name % 2 == 0
            else "background-color: white"
        ] * len(row)

    # Display the first 10 rows of the DataFrame in a formatted tabular form
    styled_df = (
        df.head(load_rows)
        .style.apply(highlight_rows, axis=1)
        .set_table_styles(
            [{"selector": "thead th", "props": [("font-weight", "bold")]}]
        )
    )
    display(styled_df)


def peek_into_file(file_path: str, load_rows: int = 10) -> None:
    """
    Display a specified number of rows from a CSV file as a Pandas DataFrame.

    Parameters
    ----------
    file_path
        The path of the CSV file to load.
    load_rows
        The number of rows to load from the CSV file. Default is 10.

    Raises
    ------
    FileNotFoundError
        If the file does not exist.
    ValueError
        If the file_path is not a valid string.

    Examples
    --------
    >>> df = peek_into_file('path/to/your/file.csv', load_rows)  # doctest: +SKIP

    """
    if not isinstance(file_path, str):
        msg = "The file_path must be a valid string."
        raise ValueError(msg)

    if not os.path.isfile(file_path):
        msg = f"The file '{file_path}' does not exist."
        raise FileNotFoundError(msg)

    try:
        # Load specified number of rows from CSV file into a PySpark DataFrame
        # df = spark.read.csv(file_path, header=True, inferSchema=True).limit(load_rows)
        # Load specified number of rows from CSV file into a Pandas DataFrame
        df = pd.read_csv(file_path, nrows=load_rows)
        # df = pd.read_csv(file_path).head(limit_rows)

        # Get current timestamp in the desired timezone
        # current_time = spark.sql("SELECT current_timestamp()").collect()[0][0]
        # current_time = datetime.datetime.now().time()
        # edt_timezone = datetime.now().astimezone().tzinfo
        # now = current_time.astimezone(edt_timezone)
        # print("Date-Time data was last displayed:",
        #       now.strftime("%Y-%m-%d %I:%M:%S %p %Z"))

        # Show the specified number of rows of the Spark DataFrame
        # df.show()
        # Print the specified number of rows of the Pandas DataFrame
        # pd.set_option("display.max_columns", None)  # Ensure all columns are visible
        # print(df.head(load_rows))
        # print(df)
        # print(df.to_string(index=False))
        display(df)

        # return df  # Optionally return the DataFrame for further use
    except Exception as e:
        msg = f"Error loading file '{file_path}': {e}"
        raise RuntimeError(msg) from e


def get_mindlogger_token(
    mindlogger_url: str, mindlogger_data: dict, mindlogger_headers: dict
) -> Optional[tuple[str, str]]:
    """
    Process the response to a POST request to the specified URL.

    Parameters
    ----------
    mindlogger_url
        The URL to send the POST request to.
    mindlogger_data
        The data to include in the POST request.
    mindlogger_headers
        The headers to include in the POST request.

    Returns
    -------
    tuple[str, str] or None
        A tuple containing (access_token, refresh_token), or None if unsuccessful.

    Raises
    ------
    RuntimeError
        If there is an error with the request or response.

    """
    try:
        # Sending the POST request
        response = requests.post(
            mindlogger_url, json=mindlogger_data, headers=mindlogger_headers
        )

        if response.status_code == requests.codes["okay"]:
            response_data = response.json()  # Convert response to JSON
            access_token = (
                response_data.get("result", {})
                .get("token", {})
                .get("accessToken", None)
            )  # Get access token
            refresh_token = (
                response_data.get("result", {})
                .get("token", {})
                .get("refreshToken", None)
            )  # Get refresh token
        else:
            logger.exception(
                "Failed to fetch data: %d - %s", response.status_code, response.text
            )
            response.raise_for_status()
            return None

        # return response_data
        return access_token, refresh_token

    except requests.RequestException as e:
        msg = f"Error sending request: {e}"
        raise RuntimeError(msg) from e
    except json.JSONDecodeError as e:
        msg = f"Error: Response is not valid JSON! {e}"
        raise RuntimeError(msg) from e


def print_module_variables(  # noqa: RUF100,T201
    module: ModuleType, exclude_prefix: str = "__", exclude_suffix: str = "_data"
) -> None:
    """
    Print all variable names and values from a given module.

    Excluding:
    - Built-in attributes (starting with "__")
    - Variables ending with a specific suffix (default: "_data")

    :param module: The imported module from which to print variables.
    :param exclude_prefix: Prefix to exclude (default: "__" for built-ins).
    :param exclude_suffix: Suffix to exclude (default: "_data").
    """
    print(f"{module} API pre-defined variables:\n")  # noqa: T201

    for var_name, var_value in vars(module).items():
        if not var_name.startswith(exclude_prefix) and not var_name.endswith(
            exclude_suffix
        ):
            print(f"{var_name}: {var_value}")  # noqa: T201

    print("\n")  # noqa: T201


def redcap_api_push(
    df: pd.DataFrame,
    token: str,
    url: str,
    headers: dict,
    force_auto_number: bool = False,
) -> int:
    """Push data to REDCap API."""
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_content = csv_buffer.getvalue()

    redcap_push_data = {
        "token": token,
        "content": "record",
        "action": "import",
        "format": "csv",
        "type": "eav",
        "csvDelimiter": "",
        "returnFormat": "csv",
        "forceAutoNumber": str(force_auto_number).lower(),
        "data": csv_content,
    }

    response = requests.post(url=url, headers=headers, data=redcap_push_data)

    if response.status_code == requests.codes["okay"]:
        return int(response.text)
    if response.status_code == requests.codes["gateway_timeout"]:
        logger.exception("Failed to push data with shape %s", df.shape)
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as http_error:
        logger.exception(response.text)
        raise TSVLoggedError(
            ",".join(df.get("mrn", default=pd.Series(dtype="int64"))),
            response.text,
            "redcap_api_push",
        ) from http_error
    return 0


def isnan(x: Any) -> bool:
    """Is np.nan?"""  # noqa: D400
    try:
        return np.isnan(x)
    except TypeError:
        return x == np.nan  # noqa: PLW0177


def new_curious_account(
    host: str, applet_id: str, record: dict, headers: dict[str, str]
) -> Optional[str]:
    """Create new account in Curious."""
    match record.get("accountType"):
        case "limited":
            account_type = "shell-account"
        case "full":
            account_type = "respondent"
        case _:
            msg = f"No valid account type specified: {record.get('accountType')}"
            raise ValueError(msg)
    curious_url = f"{host}/invitations/{applet_id}/{account_type}"
    new_record = {}
    for key, value in record.items():
        if isinstance(value, (list, set, tuple)) and len(value) == 1:
            new_record[key] = next(iter(value))
        elif not isnan(value):
            new_record[key] = value
    response = requests.post(curious_url, json=new_record, headers=headers)
    logger.info("Status Code: %d", response.status_code)
    response_body = response.json()
    logger.info("Response Body: %s", response_body)
    if response.status_code == requests.codes["okay"]:
        return (
            f"{record.get('accountType')} account created for "
            f"MRN {record.get('secretUserId')}."
        )
    if response.status_code == requests.codes["unprocessable"] or (
        response.status_code == requests.codes["bad"]
        and response_body.get("result", [])[-1].get("message") == "Non-unique value."
    ):
        breakpoint()
        return f"Account already exists for MRN {record.get('secretUserId')}"
    response.raise_for_status()
    return None


def get_redcap_event_names(
    endpoint_url: str, headers: dict, data: dict[str, str]
) -> dict[str, str]:
    """Fetch REST API response and return a dict mapping `{form: unique_event_name}`."""
    _data = {
        **{"content": "formEventMapping", "format": "csv", "returnFormat": "csv"},
        **data,
    }
    try:
        response = requests.post(endpoint_url, headers=headers, data=_data)

        if response.status_code == requests.codes["okay"]:
            if response.text.strip():
                csv_reader = csv.DictReader(StringIO(response.text))
                return {row["form"]: row["unique_event_name"] for row in csv_reader}
            logger.info("Empty response received from the API.")
            return {}
        logger.info(
            "Failed to fetch data: %d - %s", response.status_code, response.text
        )
        tsv_logger = setup_tsv_logger("mrn_error_log", "mrn_error_log.tsv")
        tsv_logger.exception(
            response.text, extra={"mrn": "", "attempt": "get_redcap_event_names"}
        )
        return {}
    except KeyError as e:
        logger.info("Required column not found: %s", e)
        return {}
    except Exception as e:
        logger.info("An error occurred: %s", e)
        return {}


_today = date.today()
"""Date representation of today."""

today = str(_today)
"""`YYYY-MM-DD` string date format of today."""


def yesterday_or_more_recent(date_str: str) -> bool:
    """Return truth value if a string date-time is yesterday or more recent."""
    return datetime.fromisoformat(date_str).date() >= YESTERDAY_DATE.date()


def create_tempory_file(extension: str = "csv") -> Path:
    """Create a temporary file, returning the path."""
    file = NamedTemporaryFile(
        mode="w", suffix=f".{extension}", delete=False, newline=""
    )
    filepath = Path(file.name)
    file.close()
    return filepath


def flat_to_eav(df_flat: pd.DataFrame) -> pd.DataFrame:
    """Convert flat DataFrame to EAV."""
    # Transform to EAV (Entity-Attribute-Value)
    df_eav = df_flat.melt(
        id_vars=["mrn"],  # The identifier column(s)
        var_name="field_name",  # Column name for field names
        value_name="value",  # Column name for values
    ).dropna(subset=["value"])

    return df_eav.sort_values(["mrn", "field_name"]).reset_index(drop=True)
