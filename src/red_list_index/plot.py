import numpy as np
import polars as pl
import seaborn as sns
import matplotlib.pyplot as plt


class Plot:
    """
    Plots the global RLI by group over time using Seaborn and saves the plot to a file.
    Args:
        rli_df (pl.DataFrame): Input Polars DataFrame with columns 'group', 'year', 'rli', 'qn_05', 'qn_95'.
        filename (str): The filename to save the plot (default: 'rli.png').
    """

    def __init__(self, df):
        self.df = df

    def global_rli(self, filename="rli.png"):
        # Convert to pandas for Seaborn/Matplotlib
        rli_df = self.df.to_pandas()

        # Set Seaborn style
        sns.set(style="whitegrid")
        plt.figure(figsize=(8, 5))

        # Plot each group
        groups = rli_df["group"].unique()
        for group in groups:
            sub = rli_df[rli_df["group"] == group]
            plt.plot(sub["year"], sub["rli"], label=group, lw=0.5)  # No marker
            plt.fill_between(sub["year"], sub["qn_05"], sub["qn_95"], alpha=0.2)
        plt.xlabel("Year")
        plt.ylabel("RLI")
        plt.title("RLI by Group Over Time")
        plt.legend(loc="center left", bbox_to_anchor=(1, 0.5))
        plt.tight_layout()
        plt.savefig(filename, dpi=300)
        plt.close()  # Close the figure to avoid display in notebooks
