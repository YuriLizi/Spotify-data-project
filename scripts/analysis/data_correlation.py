import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import os
from scipy import stats


def analyze_duration_listens_correlation(input_csv, output_dir='correlation_results'):
    """
    Analyze the correlation between podcast duration and number of listens.

    Parameters:
    - input_csv: Path to the CSV file containing podcast data
    - output_dir: Directory to save the results
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Load the CSV data
    print(f"Loading data from {input_csv}...")
    df = pd.read_csv(input_csv)

    # Convert duration to minutes for better visualization
    df['duration_minutes'] = df['duration_seconds'] / 60

    # Calculate correlation
    correlation = df['duration_minutes'].corr(df['listens'])
    spearman_corr, p_value = stats.spearmanr(df['duration_minutes'], df['listens'], nan_policy='omit')

    print(f"Pearson correlation coefficient: {correlation:.4f}")
    print(f"Spearman rank correlation: {spearman_corr:.4f} (p-value: {p_value:.4f})")

    # Create a scatter plot with regression line
    plt.figure(figsize=(12, 8))

    # Create scatter plot with color-coded density
    scatter = sns.jointplot(
        x='duration_minutes',
        y='listens',
        data=df,
        kind='scatter',
        joint_kws={'alpha': 0.6, 's': 50},
        color='steelblue',
        height=10
    )

    # Add regression line
    scatter.ax_joint.set_title(f'Correlation between Podcast Duration and Listens (r = {correlation:.4f})',
                               fontsize=16, pad=20)
    scatter.ax_joint.set_xlabel('Duration (minutes)', fontsize=14)
    scatter.ax_joint.set_ylabel('Number of Listens', fontsize=14)

    # Add regression line
    sns.regplot(x='duration_minutes', y='listens', data=df, scatter=False,
                ax=scatter.ax_joint, color='red', line_kws={'linewidth': 2})

    # Save the figure
    plt.tight_layout()
    plt.savefig(f"{output_dir}/duration_listens_correlation.png", dpi=300)
    plt.close()

    # Create a heatmap for visual correlation
    plt.figure(figsize=(10, 8))

    # Select relevant columns for correlation analysis
    corr_columns = ['duration_seconds', 'duration_minutes', 'listens', 'likes', 'searches']
    corr_df = df[corr_columns].copy()

    # Calculate correlation matrix
    corr_matrix = corr_df.corr()

    # Create a heatmap
    sns.heatmap(corr_matrix, annot=True, cmap='coolwarm', vmin=-1, vmax=1, fmt='.2f', linewidths=0.5)
    plt.title('Correlation Matrix for Podcast Metrics', fontsize=16, pad=20)
    plt.tight_layout()
    plt.savefig(f"{output_dir}/correlation_heatmap.png", dpi=300)
    plt.close()

    # Create binned analysis to detect non-linear patterns
    print("Creating binned analysis...")

    # Create duration bins (in minutes)
    bins = [0, 15, 30, 45, 60, 90, 120, np.inf]
    labels = ['0-15', '15-30', '30-45', '45-60', '60-90', '90-120', '120+']
    df['duration_bin'] = pd.cut(df['duration_minutes'], bins=bins, labels=labels)

    # Calculate average listens per duration bin
    duration_group = df.groupby('duration_bin')['listens'].agg(['mean', 'median', 'count'])
    duration_group = duration_group.reset_index()

    # Save to CSV
    duration_group.to_csv(f"{output_dir}/duration_listens_by_bin.csv", index=False)

    # Create a bar plot for binned analysis
    plt.figure(figsize=(14, 8))

    # Plot mean listens per duration bin
    ax = sns.barplot(x='duration_bin', y='mean', data=duration_group, palette='viridis')

    # Annotate bars with count
    for i, row in enumerate(duration_group.itertuples()):
        ax.text(i, row.mean + 5, f'n={row.count}', ha='center', fontsize=10)

    plt.title('Average Listens by Podcast Duration', fontsize=16)
    plt.xlabel('Duration (minutes)', fontsize=14)
    plt.ylabel('Average Number of Listens', fontsize=14)
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.savefig(f"{output_dir}/avg_listens_by_duration.png", dpi=300)
    plt.close()

    print(f"Analysis complete! Results saved to {output_dir}/")

    return correlation, duration_group


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Analyze correlation between podcast duration and listens')
    parser.add_argument('--input_csv', help='Path to the input CSV file')
    parser.add_argument('--output_dir', default='correlation_results', help='Directory to save results')

    args = parser.parse_args()

    analyze_duration_listens_correlation(args.input_csv, args.output_dir)