# -*- coding: utf-8 -*-
"""
===============================================================================
Cinema Ticket System - Comprehensive Batch Analytics Visualization
===============================================================================
"""

import os
import warnings
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

warnings.filterwarnings("ignore")

# Set visual style
sns.set_theme(style="whitegrid")
RESULTS_DIR = "./my_analytics_results"
CHARTS_DIR = "./charts"
os.makedirs(CHARTS_DIR, exist_ok=True)

def plot_job1_distribution():
    """Job 1: Distribution of Bookings Across All Events"""
    path = os.path.join(RESULTS_DIR, "total_bookings_per_event")
    if not os.path.exists(path): return
    df = pd.read_parquet(path)
    
    plt.figure(figsize=(10, 6))
    sns.histplot(data=df, x='total_bookings', bins=15, kde=True, color='purple')
    plt.title('Job 1: Distribution of Bookings Across All Events', fontsize=16, pad=15)
    plt.xlabel('Number of Tickets Booked', fontsize=12)
    plt.ylabel('Number of Events (Frequency)', fontsize=12)
    plt.tight_layout()
    out_path = os.path.join(CHARTS_DIR, 'chart_job1_booking_distribution.png')
    plt.savefig(out_path, dpi=300)
    plt.close()
    print(f"[SUCCESS] Saved Job 1 chart: '{out_path}'")

def plot_job2_occupancy():
    """Job 2: Top 10 Movies by Seat Occupancy Percentage"""
    path = os.path.join(RESULTS_DIR, "occupancy_percentage_per_event")
    if not os.path.exists(path): return
    df = pd.read_parquet(path)
    top10 = df.sort_values('occupancy_percentage', ascending=False).head(10)
    
    plt.figure(figsize=(12, 6))
    sns.barplot(data=top10, x='occupancy_percentage', y='movie_title', hue='movie_title', palette='crest', legend=False)
    plt.title('Job 2: Top 10 Movies by Seat Occupancy Percentage', fontsize=16, pad=15)
    plt.xlabel('Occupancy Percentage (%)', fontsize=12)
    plt.ylabel('Movie Title', fontsize=12)
    plt.tight_layout()
    out_path = os.path.join(CHARTS_DIR, 'chart_job2_occupancy_percentage.png')
    plt.savefig(out_path, dpi=300)
    plt.close()
    print(f"[SUCCESS] Saved Job 2 chart: '{out_path}'")

def plot_job3_revenue():
    """Job 3: Top 10 Highest Grossing Movies"""
    path = os.path.join(RESULTS_DIR, "total_revenue_per_event")
    if not os.path.exists(path): return
    df = pd.read_parquet(path)
    top10 = df.sort_values('total_revenue', ascending=False).head(10)
    
    plt.figure(figsize=(12, 6))
    sns.barplot(data=top10, x='total_revenue', y='movie_title', hue='movie_title', palette='magma', legend=False)
    plt.title('Job 3: Top 10 Highest Grossing Movies', fontsize=16, pad=15)
    plt.xlabel('Total Revenue ($)', fontsize=12)
    plt.ylabel('Movie Title', fontsize=12)
    plt.tight_layout()
    out_path = os.path.join(CHARTS_DIR, 'chart_job3_top10_revenue.png')
    plt.savefig(out_path, dpi=300)
    plt.close()
    print(f"[SUCCESS] Saved Job 3 chart: '{out_path}'")

def plot_job4_available_seats():
    """Job 4: Distribution of Available Seats Across Events"""
    path = os.path.join(RESULTS_DIR, "available_seats_per_event")
    if not os.path.exists(path): return
    df = pd.read_parquet(path)
    
    plt.figure(figsize=(10, 6))
    sns.histplot(data=df, x='available_seats', bins=15, kde=True, color='teal')
    plt.title('Job 4: Distribution of Available Seats Across Events', fontsize=16, pad=15)
    plt.xlabel('Available Seats', fontsize=12)
    plt.ylabel('Number of Events', fontsize=12)
    plt.tight_layout()
    out_path = os.path.join(CHARTS_DIR, 'chart_job4_available_seats.png')
    plt.savefig(out_path, dpi=300)
    plt.close()
    print(f"[SUCCESS] Saved Job 4 chart: '{out_path}'")

def plot_job5_top_events():
    """Job 5: Top 5 Most-Booked Movies"""
    path = os.path.join(RESULTS_DIR, "top5_events")
    if not os.path.exists(path): return
    df = pd.read_parquet(path)
    
    plt.figure(figsize=(10, 6))
    sns.barplot(data=df, x='total_bookings', y='movie_title', hue='movie_title', palette='viridis', legend=False)
    plt.title('Job 5: Top 5 Most-Booked Movies', fontsize=16, pad=15)
    plt.xlabel('Total Tickets Booked', fontsize=12)
    plt.ylabel('Movie Title', fontsize=12)
    plt.tight_layout()
    out_path = os.path.join(CHARTS_DIR, 'chart_job5_top_events.png')
    plt.savefig(out_path, dpi=300)
    plt.close()
    print(f"[SUCCESS] Saved Job 5 chart: '{out_path}'")

def plot_job6_category():
    """Job 6: Top 10 Event Categories / Genres by Total Bookings"""
    path = os.path.join(RESULTS_DIR, "bookings_by_category")
    if not os.path.exists(path): return
    df = pd.read_parquet(path)
    
    # Priority check for column name
    cat_col = 'category' if 'category' in df.columns else 'genre' if 'genre' in df.columns else df.columns[0]
    
    # Sort descending and take top 10 categories for clean readability
    top10_cat = df.sort_values('total_bookings', ascending=False).head(10)
    
    plt.figure(figsize=(12, 6))
    sns.barplot(data=top10_cat, x='total_bookings', y=cat_col, hue=cat_col, palette='rocket', legend=False)
    plt.title('Job 6: Top 10 Event Categories by Total Bookings', fontsize=16, pad=15)
    plt.xlabel('Total Bookings', fontsize=12)
    plt.ylabel('Category / Genre', fontsize=12)
    plt.tight_layout()
    out_path = os.path.join(CHARTS_DIR, 'chart_job6_bookings_by_category.png')
    plt.savefig(out_path, dpi=300)
    plt.close()
    print(f"[SUCCESS] Saved Job 6 chart: '{out_path}'")

def plot_job7_revenue_by_date():
    """Job 7: Daily Revenue Over Time"""
    path = os.path.join(RESULTS_DIR, "bookings_by_date")
    if not os.path.exists(path): return
    df = pd.read_parquet(path)
    df = df.sort_values('event_date')
    
    plt.figure(figsize=(10, 6))
    sns.lineplot(data=df, x='event_date', y='daily_revenue', marker='o', color='b', linewidth=2)
    plt.title('Job 7: Daily Revenue Over Time', fontsize=16, pad=15)
    plt.xlabel('Date', fontsize=12)
    plt.ylabel('Total Revenue ($)', fontsize=12)
    plt.xticks(rotation=45)
    plt.tight_layout()
    out_path = os.path.join(CHARTS_DIR, 'chart_job7_revenue_by_date.png')
    plt.savefig(out_path, dpi=300)
    plt.close()
    print(f"[SUCCESS] Saved Job 7 chart: '{out_path}'")

def plot_job8_top_users():
    """Job 8: Top 5 Users by Number of Bookings"""
    path = os.path.join(RESULTS_DIR, "top5_users")
    if not os.path.exists(path): return
    df = pd.read_parquet(path)
    name_col = 'name' if 'name' in df.columns else 'username' if 'username' in df.columns else 'user_id'
    
    plt.figure(figsize=(10, 6))
    sns.barplot(data=df, x='total_bookings', y=name_col, hue=name_col, palette='cubehelix', legend=False)
    plt.title('Job 8: Top 5 Users by Number of Bookings', fontsize=16, pad=15)
    plt.xlabel('Total Bookings', fontsize=12)
    plt.ylabel('User Name / ID', fontsize=12)
    plt.tight_layout()
    out_path = os.path.join(CHARTS_DIR, 'chart_job8_top_users.png')
    plt.savefig(out_path, dpi=300)
    plt.close()
    print(f"[SUCCESS] Saved Job 8 chart: '{out_path}'")

if __name__ == "__main__":
    print("[INFO] Generating visualizations for all 8 Batch Analytics Jobs...")
    plot_job1_distribution()
    plot_job2_occupancy()
    plot_job3_revenue()
    plot_job4_available_seats()
    plot_job5_top_events()
    plot_job6_category()
    plot_job7_revenue_by_date()
    plot_job8_top_users()
    print("[SUCCESS] All 8 charts successfully generated and saved in './charts/' directory!")