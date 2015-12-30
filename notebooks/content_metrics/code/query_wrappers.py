"""Wrapper function used to pull data needed for dashboard and reports."""

import collections
import datetime
import os
import numpy as np
import pandas as pd
import sys
sys.path.append("../..")
import util.bq_util as bqnb
import config as cfg
from queries import query_dict
import queries


def query_to_df(query, bq=None):
    result = bqnb.exec_and_return(query, bq)
    return result.rows


def fetch_all_dashboard_data(bq=None):
    """Fetch all data needed to support the content metrics dashboard and
       reports (from intermediate tables in BigQuery."""
    content_production = query_to_df(query_dict['production_queries'], bq)
    yt_logs = query_to_df(query_dict['yt_logs_query'], bq)
    request_logs_breakdown, request_logs_device = get_request_log_dfs()
    usage_dfs = get_usage_dfs(bq)
    new_learner_df = get_new_learners_this_month(bq)
    content_added, added_timecourse, total_content, total_timecourse = (
        construct_production_data(content_production))
    data = {}
    data['content_production'] = content_production
    data['yt_logs'] = yt_logs
    data['request_logs_breakdown'] = request_logs_breakdown
    data['request_logs_device'] = request_logs_device
    data['new_learner_df'] = new_learner_df
    data['content_added_this_month'] = content_added
    data['content_added_timecourse_df'] = added_timecourse
    data['total_content_available'] = total_content
    data['content_total_timecourse_df'] = total_timecourse
    return data, usage_dfs


def get_request_log_dfs(bq=None):
    """Pull data on visitors/sessions from request_logs_summary."""
    request_logs_type_breakdown = pd.concat(
        [query_to_df(query_dict['request_logs_type_breakdown'], bq),
         query_to_df(query_dict['core_request_logs_type_breakdown'], bq)])
    request_logs_all_types = pd.concat(
        [query_to_df(query_dict['request_logs_all_types'], bq),
         query_to_df(query_dict['core_request_logs_all_types'], bq)])
    request_logs_device = pd.concat(
        [query_to_df(query_dict['request_logs_device_query'], bq),
         query_to_df(query_dict['core_request_logs_device_query'], bq)])
    breakdown = pd.concat([request_logs_type_breakdown, request_logs_all_types])
    return breakdown, request_logs_device


def get_usage_dfs(bq):
    """Pull data on usage from content_metrics.learning_log_*."""
    usage_dfs = {}
    for content_type in cfg.C_TYPES + ["all"]:
        tmp_dfs = []
        for prefix in ["", "core_"]:
            query_name = "%susage_queries_%s" % (prefix, content_type)
            df = query_to_df(query_dict[query_name], bq)
            if df is not None:
                tmp_dfs.append(df)
            else:
                print "warning: failed to create df for %s" % query_name
                print query_dict[query_name]
        df = pd.concat(tmp_dfs)
        df["kind"] = content_type
        dates = []
        for i, row in df.iterrows():
            dates.append(datetime.datetime(
                int(row["year"]), int(row["month"]), 1).strftime("%Y-%m-%d"))
        df["date"] = dates
        df = df.rename(columns={"content_area": "Content Area"})
        df["num_learners"] = df["num_learners"].astype(float)
        df["num_content_learned"] = df["num_content_learned"].astype(float)
        df["avg_prop_completed"] = df["avg_prop_completed"].astype(float)
        df["TLT"] = df["TLT"].astype(float)
        usage_dfs["%s_usage" % content_type] = df
    return usage_dfs


def get_new_learners_this_month(bq, bust_cache=False):
    """Get table of learners that are new this month vs. returning."""
    if "new_learner_counts.csv" in os.listdir(".") and not bust_cache:
        return pd.read_csv("new_learner_counts.csv")
    data = collections.defaultdict(list)
    all_nodes = dict(cfg.COMPARISON_NODES, **cfg.SUMMARY_NODE)
    for content_type in cfg.C_TYPES + ["all"]:
        for node_name, node_data in all_nodes.iteritems():
            data["content_area"].append(node_name)
            data["content_type"].append(content_type)
            for last_month in [True, False]:
                try:
                    single = query_to_df(
                        queries.construct_new_learner_query(
                            content_type, node_data, last_month=last_month),
                        bq)
                    val = single.ix[0, 0]
                except:
                    val = np.nan
                if last_month:
                    data["returning_last_month"].append(val)
                else:
                    data["returning_all_time"].append(val)
    df = pd.DataFrame(data=data)
    df.to_csv("new_learner_counts.csv", index=False)
    return df


def get_content_counts(df, df_kind="new"):
    """Pull counts of the units of content produced by subject."""
    data = collections.defaultdict(list)
    for c, comparison_data in cfg.COMPARISON_NODES.iteritems():
        subject = comparison_data.get('subject', None)
        if subject is None:
            subject_list = comparison_data.get('subject_list', None)
        else:
            subject_list = [subject]
        domain = comparison_data.get('domain', None)
        if domain is None:
            rdf = df[df["subject"].isin(subject_list)]
        else:
            rdf = df[df["domain"] == domain]
        data["all"].append(len(rdf))
        data["Content Area"].append(c)
        for kind in cfg.P_TYPES:
            data[kind].append(len(rdf[rdf["kind"] == kind]))
    newdf = pd.DataFrame(data)
    return newdf


def get_content_production_timecourses(content_production):
    date_pairs = queries._create_date_intervals()
    added_dfs = []
    total_dfs = []
    for start_date, end_date in date_pairs:
        month_df = get_content_counts(content_production[(
            content_production["creation_date"] >= start_date) & (
            content_production["creation_date"] <= end_date)], df_kind="new")
        month_totals_df = get_content_counts(content_production[(
            content_production["creation_date"] >= "1970-05-16") & (
            content_production["creation_date"] <= end_date)], df_kind="total")
        month_df["date"] = start_date
        month_totals_df["date"] = start_date
        added_dfs.append(month_df)
        total_dfs.append(month_totals_df)
    return pd.concat(added_dfs), pd.concat(total_dfs)


def construct_production_data(content_production):
    total_available = get_content_counts(
        content_production, df_kind="all")
    this_month_production = content_production[(
        content_production["creation_date"] >= queries.LATEST_MONTH_START) & (
        content_production["creation_date"] <= queries.LATEST_MONTH_END)]
    content_added = get_content_counts(
        this_month_production, df_kind="new")
    added_timecourse, total_timecourse = (
        get_content_production_timecourses(content_production))
    return content_added, added_timecourse, total_available, total_timecourse
