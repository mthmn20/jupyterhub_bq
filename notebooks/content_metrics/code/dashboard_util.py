"""Module containing miscellaneous utility classes and functions used to
to support widgets in the content_metrics dashboard."""

import sys
sys.path.append("../..")
from util import bq_util
from util import widgets
import ipywidgets
import json
import pandas as pd
import os

import config as cfg
import queries

# files to support exporting
exporter_names = ipywidgets.Dropdown(options=["content_production_history",
                                              "content_production_snapshot",
                                              "content_usage_history",
                                              "content_usage_snapshot"],
                                     value='content_production_history',
                                     width="275")

export_button = ipywidgets.Button(description="Create CSV Export",
                                  width="300px",
                                  button_style="warning")

            
def export(filename, df):
    """Create download link for csv export."""
    filename = "%s_%s.csv" % (filename, queries.LATEST_MONTH_END)
    csvdata = [",".join(list(df.columns))]
    for i, row in df.iterrows():
        csvdata.append(",".join([str(i).encode('utf-8') for i in row]))
    csv_output = "\n".join(csvdata)
    return widgets.create_export(filename, csv_output)


def create_production_widgets(content_type_callback, plot_type_callback):
    """Create widgets used for visualizing content production."""
    comparison_selector = ipywidgets.SelectMultiple(
        options=[c for c in cfg.NODES if c != "Core Academic"],
        selected_labels=["Physics", "Biology", "Chemistry"])
    comparison_selector.height = '270px'
    type_selector1 = widgets.create_toggle(
        ["videos", "exercises", "articles"], content_type_callback)
    type_selector2 = widgets.create_toggle(
        ["tutorials", "projects", "challenges"], content_type_callback)
    all_button = ipywidgets.Button(
        description="all", button_style="info", width="300px")
    all_button.on_click(content_type_callback)
    plot_selector = widgets.create_toggle(
        ["Total Content", "Added Content"],
        plot_type_callback, style="primary",
        button_width="300px", orientation="vertical")
    title = ipywidgets.HTML(
        value='<div class=VizTitle><h2> Content Production </h2> </div>')
    toggle_container = ipywidgets.VBox(
        children=[title, widgets.gap, type_selector1, type_selector2,
                  all_button, widgets.gap, plot_selector],
        width="300px")
    prod_container = ipywidgets.HBox(
        children=[toggle_container, comparison_selector])
    return prod_container, comparison_selector


def create_usage_widgets(
        content_type_callback, usage_type_callback, extra_buttons):
    """Create widgets used for visualizing content usage."""
    comparison_selector = ipywidgets.SelectMultiple(
        options=cfg.NODES, selected_labels=["Physics",
                                            "Biology",
                                            "Chemistry"], width="200px")
    comparison_selector.height = '270px'
    type_selector1 = widgets.create_toggle(
        ["all", "videos", "exercises"], content_type_callback)
    type_selector2 = widgets.create_toggle(
        ["articles", "talkthroughs", "scratchpads"], content_type_callback)
    plot_selector = widgets.create_toggle(
        ["Learners", "Learning Time", "Content Nodes Learned"],
        usage_type_callback, style="primary",
        button_width="300px", orientation="vertical")
    title = ipywidgets.HTML(
        value='<div class=VizTitle><h2> Content Usage </h2> </div>')
    toggle_container = ipywidgets.VBox(
        children=[title, widgets.gap, type_selector1, type_selector2,
                  widgets.gap, plot_selector],
        width="300px")
    extras = [ipywidgets.HTML(
        value='<div class=VizTitle><h4> Extras: </h4> </div>')]
    extras.extend(extra_buttons)
    extra_container = ipywidgets.VBox(extras, width="200px")
    usage_container = ipywidgets.HBox(
        children=[toggle_container, comparison_selector, extra_container])
    return usage_container, comparison_selector


def save_to_gcs(gcs_client, dataframe_dict, usage_dfs,
                gspath="ka_content_analytics/subject_metrics_output",
                localdir="local_data"):
    """Save content analytics data to GCS."""
    save_locally(dataframe_dict, usage_dfs, localdir)
    filenames = os.listdir(localdir)
    return bq_util.upload_files_to_gcs(gcs_client, filenames, localdir, gspath)

    
def load_from_gcs(gcs_client,
                  gcspath="ka_content_analytics/subject_metrics_output",
                  localdir="local_data"):
    """Load content analytics data from GCS."""
    files = bq_util.list_files_in_gcs_bucket(gcs_client, gcspath)
    for path in files:
        bq_util.download_files_from_gcs(gcs_client, '/'.join(
            ['ka_content_analytics', path]),
            localpath=os.path.join(localdir, os.path.basename(path)))
    dataframe_dict, usage_df = load_locally(localdir)
    return dataframe_dict, usage_df


def save_locally(dataframe_dict, usage_dfs, localdir="local_data"):
    """Save content analytics dataframes to local disk."""
    if not os.path.exists(localdir):
        os.mkdir(localdir)
    for name, df in dataframe_dict.iteritems():
        filename = os.path.join(localdir, name + ".csv")
        df.to_csv(filename, index=False, encoding="utf8")
    with open(os.path.join(localdir, "usage_keys.json"), "w") as f:
        json.dump(usage_dfs.keys(), f)
    for name, df in usage_dfs.iteritems():
        filename = os.path.join(localdir, name + ".csv")
        df.to_csv(filename, index=False, encoding="utf8")

        
def load_locally(localdir="local_data"):
    """Load content analytics dataframes from local disk."""
    with open(os.path.join(localdir, "usage_keys.json"), "r") as fjson:
        usage_keys = json.load(fjson)
    usage_dfs = {}
    for key in usage_keys:
        filename = os.path.join(localdir, key + '.csv')
        usage_dfs[key] = pd.read_csv(filename)
    other_files = os.listdir(localdir)
    remaining = [f for f in other_files if f.endswith(".csv") and
                 f not in [key + ".csv" for key in usage_keys]]
    dataframe_dict = {}
    for filename in remaining:
        dataframe_dict[filename[:-4]] = pd.read_csv(os.path.join(localdir,
                                                                filename))
    return dataframe_dict, usage_dfs
