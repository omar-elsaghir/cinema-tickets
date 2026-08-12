import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

# Set visual style
sns.set_theme(style="whitegrid")
RESULTS_DIR = "./my_analytics_results"

# ---------------------------------------------------------
# JOB 1: Distribution of Bookings
# ---------------------------------------------------------
def plot_job1_distribution():
    path = os.path.join(RESULTS_DIR, "total_bookings_per_event")
    if not os.path.exists(path):
        print(f"⚠️ Missing {path}")
        return

    df = pd.read_parquet(path)
    
    plt.figure(figsize=(10, 6))
    sns.histplot(data=df, x='total_bookings', bins=15, kde=True, color='purple')
    
    plt.title('Job 1: Distribution of Bookings Across All Events', fontsize=16, pad=15)
    plt.xlabel('Number of Tickets Booked', fontsize=12)
    plt.ylabel('Number of Events (Frequency)', fontsize=12)
    plt.tight_layout()
    plt.savefig('chart_job1_booking_distribution.png', dpi=300)
    print("✅ Saved Job 1 chart: 'chart_job1_booking_distribution.png'")

# ---------------------------------------------------------
# JOB 3: Top 10 Revenue Generating Events
# ---------------------------------------------------------
def plot_job3_revenue():
    path = os.path.join(RESULTS_DIR, "total_revenue_per_event")
    if not os.path.exists(path):
        return

    df = pd.read_parquet(path)
    
    # Sort by revenue descending and grab the top 10
    top10_df = df.sort_values('total_revenue', ascending=False).head(10)
    
    plt.figure(figsize=(12, 6))
    sns.barplot(data=top10_df, x='total_revenue', y='movie_title', palette='magma')
    
    plt.title('Job 3: Top 10 Highest Grossing Movies', fontsize=16, pad=15)
    plt.xlabel('Total Revenue ($)', fontsize=12)
    plt.ylabel('Movie Title', fontsize=12)
    plt.tight_layout()
    plt.savefig('chart_job3_top10_revenue.png', dpi=300)
    print("✅ Saved Job 3 chart: 'chart_job3_top10_revenue.png'")

# ---------------------------------------------------------
# JOB 5: Top 5 Most-Booked Events
# ---------------------------------------------------------
def plot_job5_top_events():
    path = os.path.join(RESULTS_DIR, "top5_events")
    if not os.path.exists(path):
        return

    df = pd.read_parquet(path)
    
    plt.figure(figsize=(10, 6))
    sns.barplot(data=df, x='total_bookings', y='movie_title', palette='viridis')
    
    plt.title('Job 5: Top 5 Most-Booked Movies', fontsize=16, pad=15)
    plt.xlabel('Total Tickets Booked', fontsize=12)
    plt.ylabel('Movie Title', fontsize=12)
    plt.tight_layout()
    plt.savefig('chart_job5_top_events.png', dpi=300)
    print("✅ Saved Job 5 chart: 'chart_job5_top_events.png'")

# ---------------------------------------------------------
# JOB 7: Booking Statistics by Date
# ---------------------------------------------------------
def plot_job7_bookings_by_date():
    path = os.path.join(RESULTS_DIR, "bookings_by_date")
    if not os.path.exists(path):
        return

    df = pd.read_parquet(path)
    df = df.sort_values('event_date')
    
    plt.figure(figsize=(10, 6))
    sns.lineplot(data=df, x='event_date', y='daily_revenue', marker='o', color='b', linewidth=2)
    
    plt.title('Job 7: Daily Revenue Over Time', fontsize=16, pad=15)
    plt.xlabel('Date', fontsize=12)
    plt.ylabel('Total Revenue ($)', fontsize=12)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('chart_job7_revenue_by_date.png', dpi=300)
    print("✅ Saved Job 7 chart: 'chart_job7_revenue_by_date.png'")

# ---------------------------------------------------------
# MAIN EXECUTION
# ---------------------------------------------------------
if __name__ == "__main__":
    print("📊 Generating comprehensive charts for all Odd Jobs...")
    plot_job1_distribution()
    plot_job3_revenue()
    plot_job5_top_events()
    plot_job7_bookings_by_date()
    print("🎉 All 4 charts generated! Check your folder.")