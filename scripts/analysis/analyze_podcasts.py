import os
import argparse
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import re

def analyze_podcasts(input_csv, output_dir):


    os.makedirs(output_dir, exist_ok=True)

    # Load data
    df = pd.read_csv(input_csv)
    df["pub_date"] = pd.to_datetime(df["pub_date"], errors="coerce")

    # Remove rows with invalid or missing pub_date
    df = df[df["pub_date"].notna()]

    df["duration_seconds"] = pd.to_numeric(df["duration_seconds"], errors="coerce")
    df.fillna(0, inplace=True)

    # extract MamraMic#number format
    def extract_mamramic( title):
        match = re.search(r'(MamraMic#\d+)',title)
        return match.group(1) if match else title

    # Create a new column with the extracted format
    df['display_title'] = df['title'].apply(extract_mamramic)

    # Most Listen
    top_listens = df.sort_values("listens",ascending=False)[["title", "display_title", "listens"]].head(10)
    top_listens.to_csv(f"{output_dir}/top_listens.csv",index=False)

    plt.figure(figsize=(10, 6))
    sns.barplot(data=top_listens,x="listens", y="display_title",palette="Blues_d")
    plt.title("Top 10 Most Listened Podcasts")
    plt.tight_layout()
    plt.savefig(f"{output_dir}/most_listened.png")
    plt.close()

   #likes
    top_likes = df.sort_values("likes",ascending=False)[["title", "display_title","likes"]].head(10)
    top_likes.to_csv(f"{output_dir}/top_likes.csv",index=False)

    plt.figure(figsize=(10,6))
    sns.barplot(data=top_likes,x="likes",y="display_title",palette="Greens_d")
    plt.title("Top 10 Most Liked Podcasts")
    plt.tight_layout()
    plt.savefig(f"{output_dir}/most_liked.png")
    plt.close()

    #  Searched podcasts
    top_searches = df.sort_values("searches", ascending=False)[["title", "display_title", "searches"]].head(10)
    top_searches.to_csv(f"{output_dir}/top_searches.csv", index=False)

    plt.figure(figsize=(10, 6))
    sns.barplot(data=top_searches, x="searches", y="display_title", palette="Oranges_d")
    plt.title("Top 10 Most Searched Podcasts")
    plt.tight_layout()
    plt.savefig(f"{output_dir}/most_searched.png")
    plt.close()

    # time of release Frequency
    df_sorted = df.sort_values("pub_date")
    release_diffs = df_sorted["pub_date"].diff().dt.days.dropna()
    avg_release_gap = release_diffs.mean()
    with open(f"{output_dir}/release_frequency.txt","w") as f:
        f.write(f"Average number days between releases: {avg_release_gap:.2f}\n")

    # shortest longest average
    shortest = df.loc[df["duration_seconds"].idxmin()]
    longest = df.loc[df["duration_seconds"].idxmax()]
    average_duration = df["duration_seconds"].mean()
    median_duration = df["duration_seconds"].median()

    with open(f"{output_dir}/duration_stats.txt", "w") as f:
        f.write(f"Shortest podcast: {shortest['title']} ({shortest['duration_seconds']}s)\n")
        f.write(f"Longest podcast: {longest['title']} ({longest['duration_seconds']}s)\n")
        f.write(f"Average duration: {average_duration:.2f}s\n")
        f.write(f"Median duration: {median_duration:.2f}s\n")



    # Peak listening hour
    df["pub_hour"] = df["pub_date"].dt.hour
    hour_counts = df.groupby("pub_hour")["listens"].sum().sort_values(ascending=False)
    peak_hour = hour_counts.idxmax()

    hour_counts.to_csv(f"{output_dir}/listens_by_hour.csv")
    with open(f"{output_dir}/peak_hour.txt", "w") as f:
        f.write(f"Peak hour for podcast listening:{peak_hour}:00\n")

    plt.figure(figsize=(10, 6))
    sns.barplot(x=hour_counts.index,y=hour_counts.values, palette="Purples_d")
    plt.xlabel("Hour of Day")
    plt.ylabel("Total Listens")
    plt.title("Listening Activity by Hour")
    plt.tight_layout()
    plt.savefig(f"{output_dir}/listening_by_hour.png")
    plt.close()

    print(f"✅ Analysis complete. All results saved to: {output_dir}")


def main():
    parser = argparse.ArgumentParser(description="Analyze podcast CSV data and generate stats + visuals.")
    parser.add_argument("--input", "-i", required=True, help="Path to the input CSV file")
    parser.add_argument("--output", "-o", required=True, help="Output directory to save results")
    args = parser.parse_args()

    analyze_podcasts(args.input, args.output)


if __name__ == "__main__":
    main()
