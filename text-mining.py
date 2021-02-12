"""
A web app that analyses call and email logs, returning some stats
"""

import logging
import os

import boto3
import pandas as pd
import streamlit as st
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth


logger = logging.getLogger(__name__)

# Connect to Elastic Search
host = os.getenv(
    "ELASTICSEARCH_HOST",
    "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxeu-west-1.es.amazonaws.com",
)


def get_es_client(host) -> Elasticsearch:
    region = os.getenv("AWS_DEFAULT_REGION", "eu-west-1")

    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(
        credentials.access_key,
        credentials.secret_key,
        region,
        "es",
        session_token=credentials.token,
    )
    logger.info("Getting elasticsearch client")
    es = Elasticsearch(
        hosts=[host],
        http_auth=awsauth,
        connection_class=RequestsHttpConnection,
    )
    return es


es = get_es_client(host)

# Configure page
st.set_page_config(page_title="Text Mining", page_icon=":octopus:", layout="wide")


def pick_date_range(index_key):
    date_start = st.date_input("Enter range start:", key="start{}".format(index_key))
    date_end = st.date_input("Enter range end:", key="end{}".format(index_key))
    date_start_formatted = date_start.isoformat()  # iso.format for ElasticSearch
    date_end_formatted = date_end.isoformat()
    return date_start_formatted, date_end_formatted


def get_user_input(key_input: str) -> list:
    """
    Gets user keyword input for the query.
    To perform the query, the input is stripped of whitespace
    and looks for a comma to separate terms

    :param key_input: String to set as key index to avoid errors when more than one input is present
    :return: User input without comma and white space
    :rtype: list
    """
    user_input = st.text_input("Write a word or phrase, separate by comma:", key=key_input)
    result = [x.strip() for x in user_input.split(",")]
    return result


def generate_graphs(histogram_data: dict, log_type: str) -> st.bar_chart:
    """
    Generates a histogram to show logs per day

    :param histogram_data: Dictionary containing the data needed for the histogram
    :param log_type: Updates the columns value to show if the graph shows logs or emails
    :return: A histogram
    :rtype: st.bar_chart
    """
    chart_data = pd.DataFrame.from_dict(histogram_data, orient="index", columns=[log_type])
    graph = st.bar_chart(chart_data)
    return graph


def percentage_of_total_logs(partial: int, total: int) -> float:
    """
    Calculates the percentage of logs containing the keyword
    out of all logs for the date range

    :param partial: The number of logs that match the query params
    :param total: The number of all logs
    :return: percentage of logs containing the keyword
    :rtype: int
    """
    return round((partial / total) * 100, 2)


def obtain_matching_logs(
    index_name: str, query_str: str, date_range: tuple, range_keyword: str, field_main: str
) -> dict:
    """
    Gets the logs that match the required output.

    :param str index_name: The index to be used 'transcriptions_index'
    :param str query_str: String to be queried
    :param tuple date_range: The range of dates to execute the query for,
                            uses the output of pick_date_range()
    :param str range_keyword: The field name for the range,
                            'created_at'/'called at' for emails and call logs respectively
    :param str field_main: The field name for the body of the text,
                            'body'/'utterance' for emails and call logs respectively
    :return: Data matching the criteria
    :rtype: dict
    """
    res = es.search(
        index=index_name,
        body={
            "_source": [range_keyword, field_main],
            "query": {
                "bool": {
                    "must": [
                        {
                            "query_string": {
                                "query": query_str,  # Obtained line 195
                            }
                        },
                        {
                            "range": {
                                range_keyword: {
                                    "gte": date_range[0],
                                    "lt": date_range[1],
                                }
                            }
                        },
                    ]
                }
            },
            "aggs": {
                "daily_logs": {"date_histogram": {"field": range_keyword, "fixed_interval": "1d"}}
            },
        },
    )
    histogram_data = {}
    for bucket in res["aggregations"]["daily_logs"]["buckets"]:
        histogram_data[bucket["key_as_string"]] = bucket["doc_count"]
    return histogram_data


def cardinality_matched_logs(
    index_name: str, query_str: str, date_range: tuple, range_keyword: str, field_main: str
) -> int:
    """
    Gets the number of logs that match the date range and string query.

    :param str index_name: The index to be used 'intercoms_index'/'transcriptions_index'
    :param str query_str: String to be queried
    :param tuple date_range: The range of dates to execute the query for,
                            uses the output of pick_date_range()
    :param str range_keyword: The field name for the range,
                            'created_at'/'called at' for emails and call logs respectively
    :param str field_main: The field name for the body of the text,
                            'body'/'utterance' for emails and call logs respectively
    :return: number of logs that match the date range
    :rtype: int

    """
    res = es.search(
        index=index_name,
        body={
            "query": {
                "bool": {
                    "must": [
                        {
                            "query_string": {
                                "query": query_str,
                            }
                        },
                        {
                            "range": {
                                range_keyword: {
                                    "gte": date_range[0],
                                    "lt": date_range[1],
                                }
                            }
                        },
                    ]
                }
            },
            "aggs": {"aggregations": {"cardinality": {"field": "{}.keyword".format(field_main)}}},
        },
    )
    matched_logs_for_range = res["aggregations"]["aggregations"]["value"]

    return matched_logs_for_range


def cardinality_all_logs(
    index_name: str, query_str: str, date_range: tuple, range_keyword: str, field_main: str
) -> int:
    """
    Gets the number of all logs that match the date range.

    :param str index_name: The index to be used 'intercoms_index'/'transcriptions_index'
    :param str query_str: String to be queried
    :param tuple date_range: The range of dates to execute the query for,
                            uses the output of pick_date_range()
    :param str range_keyword: The field name for the range,
                            'created_at'/'called at' for emails and call logs respectively
    :param str field_main: The field name for the body of the text,
                            'body'/'utterance' for emails and call logs respectively
    :return: number of all logs that match the date range
    :rtype: int
    """
    res = es.search(
        index=index_name,
        body={
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                range_keyword: {
                                    "gte": date_range[0],
                                    "lt": date_range[1],
                                }
                            }
                        }
                    ]
                }
            },
            "aggs": {"aggregations": {"cardinality": {"field": "{}.keyword".format(field_main)}}},
        },
    )
    total_logs_for_range = res["aggregations"]["aggregations"]["value"]
    return total_logs_for_range


def generate_content(input_keyword: str) -> None:
    """
    Main function that displays the graph and data

    :param: input_keyword: A string used for the data range
                            and user input functions that use it to generate unique keys
    """
    # Generate container for the graphs and data
    with st.beta_container():
        # Generate two layout columns
        col1, col2 = st.beta_columns(2)
        with col1:
            dates = pick_date_range(input_keyword)
            user_input = get_user_input(input_keyword)
            if user_input:
                # String for the query to search
                # Throws an error until the user enters values in the text field
                query_str = "{} and {}".format(user_input[0], user_input[1])

                # Get the matching call logs
                res = obtain_matching_logs(
                    "transcriptions_index", query_str, dates, "called_at", "utterance"
                )

                # Get number of matching call logs
                matching_call_logs = cardinality_matched_logs(
                    "transcriptions_index", query_str, dates, "called_at", "utterance"
                )

                # Get number of all call logs
                all_call_logs = cardinality_all_logs(
                    "transcriptions_index", query_str, dates, "called_at", "utterance"
                )

                # Get the matching email logs
                res2 = obtain_matching_logs(
                    "intercoms_index", query_str, dates, "created_at", "body"
                )

                # Get number of matching email logs
                matching_email_logs = cardinality_matched_logs(
                    "intercoms_index", query_str, dates, "created_at", "body"
                )

                # Get number of all email logs
                all_email_logs = cardinality_all_logs(
                    "intercoms_index", query_str, dates, "created_at", "body"
                )

                # Graph a histogram of matching call logs per day
                generate_graphs(res, "calls")

                # Graph a histogram of matching email logs per day
                generate_graphs(res2, "emails")

        with col2:
            # Show the number of logs and the keywords they match
            st.write(
                matching_call_logs,
                "call logs containing keywords:",
                "'{}'".format(user_input[0]),
                "and",
                "'{}'".format(user_input[1]),
            )
            # Show total percentage of call logs
            st.write(
                percentage_of_total_logs(matching_call_logs, all_call_logs),
                "percent of all call logs",
            )

            # Show the number of logs and the keywords they match
            st.write(
                matching_email_logs,
                "email logs containing keywords:",
                "'{}'".format(user_input[0]),
                "and",
                "'{}'".format(user_input[1]),
            )
            # Show total percentage of total email logs
            st.write(
                percentage_of_total_logs(matching_email_logs, all_email_logs),
                "percent of all email logs",
            )


generate_content("one")

