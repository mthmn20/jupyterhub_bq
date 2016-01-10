"""Module containing simple plotting wrappers for content metrics dashboard."""

import sys
sys.path.append("../..")
from util import viz

import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
from IPython.display import clear_output

from config import COLOR_MAP, COLORS, NODES


def construct_plotdf(input_df, comparisons, type_selection, kind):
    """Create and plot content production timecourse."""
    df = input_df[input_df["Content Area"].isin(comparisons)]
    column = type_selection[:-1] if type_selection.endswith("s") else "all"
    plot_timecourse(df, "%s Content Each Month (%s)" % (kind, type_selection),
                    "%s leaves" % (kind), column)
    

def plot_timecourse(
        df, title, yaxis, column, colors=None, ax=None, kind="line"):
    """Plot timecourse of content production or usage."""
    if ax is None:
        f, ax = plt.subplots(figsize=[12, 4])
    plotdf = df.groupby(["Content Area", "date"]).mean().unstack(
        "Content Area")[column]
    colors = [COLOR_MAP[node] for node in plotdf.columns.values]
    if len(colors) == 1:
        colors = colors[0]
    plotdf.plot(kind=kind, ax=ax, color=colors)
    ax.legend(loc=[1, .2])
    ax.set_title(title)
    ax.set_ylabel(yaxis)
    ax.set_xlabel("month")
    sns.despine()

    
def plot_yoy_growth(yoy_growth, column, ax=None):
    """Plot yoy growth for specified column."""
    yoy_growth = yoy_growth.copy()
    yoy_growth = yoy_growth.reset_index(["month", "year"])
    del yoy_growth["month"]
    del yoy_growth["year"]
    if ax is None:
        f, ax = plt.subplots(figsize=[10, 3])
    comparisons = [c[1] for c in yoy_growth.columns.values]
    colors = [COLOR_MAP[node] for node in comparisons]
    if len(colors) == 1:
        colors = colors[0]
    yoy_growth["yoy_growth"].plot(kind="bar", ax=ax, color=colors)
    sns.despine()
    ax.set_ylabel("Percent YoY Growth")
    ax.legend(loc=[1, .2])
    ax.set_title("Year over year growth: %s" % column)

    
def plot_youtube_snapshot(yt_logs, column, ax):
    """Plot youtube vs KA stats."""
    clear_output()
    temp = yt_logs.groupby(
        ['content_area', "product"]).mean()[[column]].unstack(
            "product").loc[NODES]
    temp["total"] = temp.sum(axis=1)
    kaindex = list(temp.columns.levels[1].values).index("KA")
    totals = temp.values[:, 2]
    kas = temp.values[:, kaindex]
    ax.bar(range(len(totals)), [i / 1000000 for i in totals], color=[
        c + (.1,) for c in COLORS])
    ax.bar(range(len(kas)), [i / 1000000 for i in kas], color=COLORS)
    ax.set_xticks(np.arange(len(kas)) + .5)
    ax.set_xticklabels(temp.index, rotation=90)
    ax.set_ylabel("million %s" % (column))
    sns.despine()
    plt.suptitle("KA usage out of total (KA + YT) usage")
    return temp


def make_yt_plot(yt_logs):
    f, axes = plt.subplots(1, 2, figsize=[12, 5])
    yt_views = plot_youtube_snapshot(yt_logs, "views", axes[0])
    yt_minutes = plot_youtube_snapshot(yt_logs, "minutes_watched", axes[1])
    return yt_views, yt_minutes


def plot_completion(usage_dfs, plot_state):
    """Plot stats on completion percentages for current month snapshot."""
    clear_output()
    f, axes = plt.subplots(2, 1, figsize=[8, 5])
    for i, content_type in enumerate(["video", "exercise"]):
        df = usage_dfs["%s_usage" % content_type]
        df = df[(df["date"] > "2014-01-01") & (
            df["Content Area"].isin(plot_state["comparisons"]))]
        df = df.groupby(["Content Area", "date"]).mean()["avg_prop_completed"]
        df.unstack("Content Area").plot(ax=axes[i])
        axes[i].set_ylim([0, 1])
        axes[i].set_title(content_type)
        axes[i].set_ylabel("%s completion stats" % content_type)
        axes[i].legend(loc=[1, .5])
        axes[i].set_xlabel("")
        sns.despine()

        
def plot_new_learner_props(plotdf):
    f, ax = plt.subplots(figsize=[8, 4])
    colors1 = [list(COLOR_MAP[c]) + [.1] for c in plotdf.index]
    colors2 = [list(COLOR_MAP[c]) + [.5] for c in plotdf.index]
    colors3 = [list(COLOR_MAP[c]) + [.9] for c in plotdf.index]
    plotdf["num_learners"].plot(kind="bar", color=colors1, ax=ax)
    (plotdf["returning_all_time"] + plotdf["returning_last_month"]).plot(
        kind="bar", color=colors2, ax=ax)
    plotdf["returning_last_month"].plot(kind="bar", color=colors3, ax=ax)
    ax.set_ylabel("Number of Learners")
    d = {"New learners": colors1[0],
         "Returning all time": colors2[0],
         "Returning last month": colors3[0]}
    viz.add_legend(d)
    sns.despine()

    
def plot_visitors(df, comparisons, content_type):
    df = df[(df["content_area"].isin(comparisons)) & (
        (df["content_type"] == content_type))]
    df = df.groupby("content_area").mean()[["num_sessions", "num_visitors"]]
    df = df.rename(columns={"num_sessions": "Number of Sessions",
                            "num_visitors": "Number of Visitors"})
    colors = [COLOR_MAP[c] for c in comparisons]
    if len(colors) == 1:
        colors = colors[0]
    df.T.plot(kind="bar", color=colors)
    sns.despine()
    return
