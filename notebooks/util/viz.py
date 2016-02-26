"""General purpose functions for common plotting needs (w/ mpl and seaborn)."""

import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
sns.set_style("white")


def create_colored_df(df, cmap="hot", columns=None):
    """Create dataframe html object with colored cells."""
    if columns is None:
        columns = [col for col in df.columns if df[col].dtype in ("float64",
                                                                  "int64")]
    return df.style.background_gradient(cmap=cmap, low=.5, subset=columns)
    

def standard_plot(df, columns, colors=None, kind="line",
                  ylabel="", xlabel="", title="", ax=None):
    """Wrapper for standard pandas plots (line, bar, area)."""
    if ax is None:
        f, ax = plt.subplots(figsize=[8, 5])
    df[columns].plot(kind=kind, ax=ax)
    ax.legend(loc=[1, .6])
    ax.set_ylabel(ylabel)
    ax.set_xlabel(xlabel)
    ax.set_title(title)
    sns.despine()


def stacked_bar_chart(df, columns, yaxislabel="total", normalize=False,
                      ax=None, row_colors=None, cmap="PuBuGn"):
    """Create a stacked plot from a dataframe where each row is a bar on the
       x axis, and each column is a component of the stacked bar."""
    if ax is None:
        f, ax = plt.subplots(figsize=[8, 5])
    tdf = df.copy()[columns]
    tdf["total"] = tdf.sum(axis=1)
    if normalize:
        for c in columns:
            tdf[c] = tdf[c] / tdf["total"]
        tdf["total"] = 1
    colors = _get_colors(columns, row_colors=row_colors, cmap=cmap)
    for coln, col in enumerate(columns):
        ax = tdf["total"].plot(kind="bar", label=col, color=colors[coln], ax=ax)
        tdf["total"] = tdf["total"] - tdf[col]
    ax.set_ylabel("total")
    ax.legend(loc=[1, .6])
    sns.despine()
    return tdf


def clustered_bar_chart(df, groupingcol, xcol, ycol, ax=None,
                        xcolors=None, cmap="PuBuGn"):
    """Create a clustered bar plot where each """
    if ax is None:
        f, ax = plt.subplots(figsize=[8, 5])
    if xcolors is None:
        sns.barplot(
            y=ycol, x=xcol, hue=groupingcol, data=df, palette=cmap, ax=ax)
    else:
        df = df.groupby([groupingcol, xcol]).mean()[[ycol]].unstack(xcol)
        ind = np.arange(len(df.columns)) + .1  # the x locations for the groups
        xticks = np.arange(len(df.columns)) + .5
        bar_width = .8 / len(df.columns)
        colors = _get_colors(df.columns, xcolors)
        for colnum, column in enumerate(df.columns):
            xs = ind + (bar_width * colnum)
            ys = df[column]
            bars = ax.bar(xs, ys, width=bar_width, label=column[1])
            for colorn, color in enumerate(colors[colnum]):
                bars[colorn].set_color(color)
        ax.set_xticks(xticks)
        ax.set_xticklabels(df.index)
        ax.legend(loc=[1, .6])
    sns.despine()

    
def plot_overlapping_histograms(df, columns, bins=20, colors=None,
                                ax=None, alpha=.4):
    if ax is None:
        f, ax = plt.subplots(figsize=[8, 5])
    if colors is None:
        colors = sns.color_palette("Set2", len(columns))
    color_dict = {column: tuple(list(color) + [alpha])
                  for column, color in zip(columns, colors)}
    for col in columns:
        df[col].hist(bins=bins, alpha=alpha, color=color_dict[col], ax=ax)
    add_legend(color_dict)
    sns.despine()

    
def add_legend(color_dict, loc=(1, .6)):
    import matplotlib.patches as mpatches
    patches = []
    for key in color_dict.keys():
        patches.append(mpatches.Patch(color=color_dict[key], label=key))
    plt.legend(handles=patches, loc=loc)
    
    
def _get_colors(columns, row_colors=None, cmap="PuBuGn"):
    if row_colors:
        weights = np.arange(.1, 1, (1.0 / len(columns)))
        colors = []
        for w in weights:
            colors.append([tuple(list(color) + [w]) for color in row_colors])
    else:
        colors = sns.color_palette(cmap, len(columns))
    return colors
    
