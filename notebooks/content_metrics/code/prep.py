"""Module for performing basic post processing to generate data for content
metrics csv export and dashboard."""

import datetime
import numpy as np
import pandas as pd

import config as cfg
import queries


def construct_df(usage_dfs, PLOT_STATE):
    """Construct a reduced dataframe based on the desired plotting state."""
    df = usage_dfs["%s_usage" % PLOT_STATE["content_type"]]
    return df[(df["Content Area"].isin(PLOT_STATE["comparisons"])) &
              (df["date"] >= queries.ALL_TIME_START) &
              (df["date"] <= queries.LATEST_MONTH_END)]
   

def calc_yoy_growth(df, column):
    """Calculate YoY growth in the target column."""
    # only look at months/content_types with two years to look at
    keepers = (df.groupby(
        ["Content Area", "month"]).count()["year"] > 1).reset_index()
    keepers = keepers[keepers["year"]][["Content Area", "month"]]
    yoy_df = {col: [] for col in
              ["Content Area", "month", "year", "yoy_growth", "date", column]}
    if len(keepers) == 0:
        for key in yoy_df:
            yoy_df[key] = [np.nan]
    for i, row in keepers.iterrows():
        ca = row["Content Area"]
        m = row["month"]
        subset = df[(df["Content Area"] == ca) &
                    (df["month"] == m)].sort_values(by="year", ascending=False)
        percent_growth = (
            (subset[column].values[0]
             - subset[column].values[1]) / subset[column].values[1]) * 100
        yoy_df["Content Area"].append(ca)
        yoy_df["month"].append(m)
        yoy_df["year"].append(subset["year"].values[0])
        yoy_df[column].append(subset[column].values[0])
        yoy_df["yoy_growth"].append(percent_growth)
        yoy_df["date"].append(datetime.datetime(
            int(subset["year"].values[0]), int(m), 1).strftime("%Y-%m-%d"))
    output = pd.DataFrame(yoy_df).groupby(
        ["date", "Content Area", "month", "year"]
    )[[column, "yoy_growth"]].mean().unstack("Content Area")
    return output


def get_yts(yt_logs):
    yt = yt_logs[~pd.isnull(
        yt_logs["content_area"]
    )].rename(columns={"content_area": "Content Area"})
    yt = yt.groupby(["Content Area", "product"]).mean().unstack("product")
    yt.columns = ["VIDEO_" + '_'.join(
        [col[0], col[1].strip()]) for col in yt.columns.values]
    return yt.reset_index()


def prep_request_logs(request_logs_breakdown):
    rlog = request_logs_breakdown.groupby(
        ["content_area", "content_type"]).mean().unstack("content_type")
    rlog.columns = ['_'.join(
        [col[0], col[1].strip().upper()]) for col in rlog.columns.values]
    rlog["Content Area"] = rlog.index
    return rlog


def create_production_snapshot(total_content_available,
                               content_added_this_month):
    total_content_available = total_content_available.copy()
    total_content_available["kind"] = "Total"
    content_added_this_month = content_added_this_month.copy()
    content_added_this_month["kind"] = "New"
    df = pd.concat(
        [total_content_available,
         content_added_this_month]
        ).groupby(["Content Area", "kind"]).mean().unstack("kind")
    cols = [' '.join(
        [col[1], col[0].strip().upper()]) for col in df.columns.values]
    df.columns = cols
    df = df[[c for c in cols if "Total" in c] +
            [c for c in cols if "New" in c]]
    return df.reset_index()


def create_production_history(total_content_available,
                              content_added_this_month):
    total_content_available = total_content_available.copy()
    total_content_available["kind"] = "Total"
    content_added_this_month = content_added_this_month.copy()
    content_added_this_month["kind"] = "New"
    full_df = pd.concat(
        [total_content_available,
         content_added_this_month]
        ).groupby(["Content Area", "kind", "date"]).mean().unstack("kind")
    cols = [' '.join(
        [col[1], col[0].strip().upper()]) for col in full_df.columns.values]
    full_df.columns = cols
    full_df = full_df[[c for c in cols if "Total" in c] +
                      [c for c in cols if "New" in c]].reset_index()
    dfs = []
    for node in total_content_available['Content Area'].unique():
        tmp_df = full_df[full_df["Content Area"] == node].groupby(
            "date").sum().T
        tmp_df["METRIC"] = tmp_df.index
        tmp_df["CONTENT AREA"] = node
        dfs.append(tmp_df)
    df = pd.concat(dfs)
    df = df[["METRIC", "CONTENT AREA"] +
            [col for col in df.columns if col not in (
                "METRIC", "CONTENT_AREA")]]
    return df


def reorder_spreadsheet(df):
    cols = ["Content Area", "month", "year"]
    for content_type in ["all"] + cfg.C_TYPES:
        cols.extend([c for c in df.columns if content_type.upper() in c
                     and "prop_visits_learning" not in c])
    return df[cols]


def combine_usage_dfs(usage_dfs):
    dfs = []
    for content_type in ["all"] + cfg.C_TYPES:
        df = usage_dfs["%s_usage" % content_type].rename(
            columns={"kind": "content_type"})
        for column in ["TLT", "num_learners", "num_content_learned"]:
            yoy_df = calc_yoy_growth(df, column)
            yoy_df = yoy_df.stack("Content Area").reset_index()
            yoy_df = yoy_df.rename(columns={"yoy_growth":
                                            "%s_yoy_growth" % column})
            df = pd.merge(df, yoy_df, how="left")
        dfs.append(df)
    output = pd.concat(dfs)
    output = output[(output["date"] >= queries.ALL_TIME_START) &
                    (output["date"] <= queries.LATEST_MONTH_END)]
    output = output.groupby(
        ["Content Area", "month", "year", "content_type"]).mean().unstack(
            "content_type").sortlevel(1, axis=1)
    output.columns = ['_'.join([col[0], col[1].upper() + "_CONTENT"]
                              ).strip() for col in output.columns.values]
    output = output.reset_index().sort_values(by=["Content Area", "year", "month"])
    return output


def create_usage_snapshot(usage_dfs,
                          request_logs_breakdown,
                          yt_logs,
                          new_learner_df):
    output = combine_usage_dfs(usage_dfs)
    df = output[(output["month"] == queries.CURR_MONTH) &
                (output["year"] == queries.CURR_YEAR)]
    rlog = prep_request_logs(request_logs_breakdown)
    df = pd.merge(df, rlog, how="left")
    df = pd.merge(df, get_yts(yt_logs), how="left")
    new_learner = new_learner_df.groupby(
        ["content_area", "content_type"]).mean().unstack("content_type")
    new_learner.columns = ['_'.join([col[0], col[1].strip().upper()])
                           for col in new_learner.columns.values]
    new_learner["Content Area"] = new_learner.index
    df = pd.merge(df, new_learner, how="left").fillna(0)
    return reorder_spreadsheet(df)


def create_usage_history(usage_dfs):
    df = combine_usage_dfs(usage_dfs)
    return reorder_spreadsheet(df)


def analyze_new_learners(new_learner, usage_dfs, content_type, comparisons):
    df = new_learner[new_learner["content_type"] == content_type].groupby(
        "content_area").mean()
    usage = usage_dfs["%s_usage" % content_type]
    all_usage = usage_dfs["all_usage"]
    this_month = usage[usage["date"] == queries.LATEST_MONTH_START].groupby(
        "Content Area").mean()[["num_learners"]]
    d = datetime.datetime.strptime(
        queries.LATEST_MONTH_START, "%Y-%m-%d") - datetime.timedelta(days=1)
    total_last_month = all_usage[(all_usage["month"] == d.month) &
                                 (all_usage["year"] == d.year)]
    plotdf = this_month.join(df).loc[list(comparisons)]
    churndf = calc_churn(plotdf, total_last_month).loc[list(comparisons)]
    return plotdf, churndf

    
def calc_churn(plotdf, total_last_month):
    all_df = plotdf.join(total_last_month.groupby("Content Area").mean()[
        ["num_learners"]].rename(
            columns={"num_learners": "last_month_learners"}))
    all_df["current month: Totals"] = all_df["num_learners"]
    all_df["current month: Loss/Gain"] = (all_df["num_learners"] -
                                          all_df["last_month_learners"])
    all_df["last month: proportion churned"] = (
        all_df["last_month_learners"] - all_df["returning_last_month"]
    ) / all_df["last_month_learners"]
    all_df["last month: proportion returning"] = (
        all_df["returning_last_month"] / all_df["last_month_learners"])
    all_df["current month: proportion new"] = (
        all_df["num_learners"] - all_df["returning_all_time"]
    ) / all_df["num_learners"]
    all_df["current month: proportion returning from last_month"] = (
        all_df["returning_last_month"] / all_df["num_learners"])
    all_df["current month: proportion resurrected"] = (
        all_df["returning_all_time"] - all_df["returning_last_month"]
    ) / all_df["num_learners"]
    return all_df[[c for c in all_df.columns if ":" in c]]
    
