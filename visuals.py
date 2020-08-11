import os
import seaborn as sns
import matplotlib.pyplot as plt


def gen_pie_plot(df, weight_col, colormap="Blues",
                 figsize=(15, 15), float_fmt="%.2f",
                 fontsize=20, title=None, save_figure=False, path="./pie_chart", use_cubehelix=False):
    f, ax = plt.subplots()
    if use_cubehelix:
        cm = sns.cubehelix_palette(n_colors=df.shape[0],
                                   start=.8,
                                   rot=.4,
                                   gamma=.4,
                                   hue=5, reverse=True)
    else:
        cm = sns.color_palette(colormap)
    df[weight_col].sort_values().plot(kind="pie", figsize=figsize,
                        autopct=float_fmt, ax=ax,
                        colors=cm, fontsize=fontsize)
    ax.set_title(title)
    if save_figure and path is not None:
        try:
            f.savefig(path)
        except(e):
            print("Could not save pie chart")
            print(e)


def pos_breakdown(weight_df, path="./images/pos_breakdown_{}.png"):
    # _ init paths
    longs_path, shorts_path = (None, None)

    # _ decide whether or not there are shorts
    shorts = weight_df[weight_df.weight < 0]
    shorts.loc[:, "weight"] = shorts.weight * -1

    longs = weight_df[weight_df.weight > 0]

    if shorts.shape[0] > 0:
        shorts_path = path.format("shorts")
        gen_pie_plot(shorts, weight_col="weight", use_cubehelix=True,
                     title="Shorts", path=shorts_path, save_figure=True)
        # _ only send back the filename for the templates
        shorts_path = os.path.basename(shorts_path)

    if longs.shape[0] > 0:
        longs_path = path.format("longs")
        gen_pie_plot(longs, weight_col="weight", use_cubehelix=True,
                     title="Longs", path=longs_path, save_figure=True)
        # _ only send back the filename for the templates
        longs_path = os.path.basename(longs_path)

    return (longs_path, shorts_path)


def cumulative_returns(agg_df, path="./images/cum_returns.png"):
    total_ret = agg_df.cumsum().dropna()

    f, ax = plt.subplots()
    total_ret.plot(figsize=(20, 10), ax=ax)

    ax.set_title("Portfolio vs Benchmark Returns (cumulative)")
    ax.set_ylabel("Returns")

    f.savefig(path)

    # _ send only the filename
    filename = os.path.basename(path)
    return filename


def scenario_returns(scenario_returns_df, pf_beta, bench_name, path="./images/scenario_returns.png"):
    f, ax = plt.subplots()
    scenario_returns_df.plot(kind="bar", figsize=(20, 10), ax=ax)
    ax.set_title('Stress Scenario returns for Mock Portfolio (pf beta: {:.2f} vs SP500)'.format(pf_beta))
    ax.set_xlabel("Scenarios (pct up/down)")
    ax.set_ylabel("Returns")
    f.savefig(path)

    # _ send only the filename
    filename = os.path.basename(path)
    return filename


def correlation_chart(cor_df, path="./images/rolling_corr.png"):
    f, ax = plt.subplots()
    cor_df.plot(ax=ax, figsize=(20, 10))
    ax.set_title("30-day rolling correlation portfolio vs SP500")
    ax.set_ylabel("Correlation")
    f.savefig(path)

    # _ send only the filename
    filename = os.path.basename(path)
    return filename


def single_pie(weight_df):
    f, ax = plt.subplots()
    weight_df["weight"].plot(kind="pie", figsize=(15,15), autopct="%.2f",
                   ax=ax, colors=sns.color_palette("Reds"),
                   fontsize=20)