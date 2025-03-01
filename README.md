# YouTube Dashboard

A robust data pipeline and interactive dashboard for harvesting, transforming, and visualizing YouTube channel and video data using the YouTube Data API, Pandas, Streamlit, Altair, Plotly, and SQLAlchemy.

## Table of Contents

- [Overview](#overview)
- [Project Structure](#project-structure)
- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
  - [Data Extraction](#data-extraction)
  - [Data Transformation](#data-transformation)
  - [Dashboard](#dashboard)
- [Configuration](#configuration)
- [Contribution Guidelines](#contribution-guidelines)
- [License](#license)
- [Additional Resources](#additional-resources)

## Overview

This project implements a complete data pipeline for YouTube data:

1. **Extraction:** Uses the YouTube Data API to collect channel and video information.
2. **Transformation:** Processes and converts raw JSON data into a clean CSV file with proper formatting and additional computed fields (like engagement rates).
3. **Dashboard:** Displays interactive visualizations and analytics using Streamlit, Altair, and Plotly. The dashboard also provides a channel suggestion feature with PostgreSQL integration.

## Project Structure

```plaintext
├── config
│   ├── channel_id.py       # Contains YouTube channel IDs
│   └── path.py             # Contains file paths configuration
├── dashboard
│   └── dashboard.py        # Streamlit dashboard application
├── data
│   └── bronze
│       └── video
│           └── video_data.csv  # Transformed video data in CSV format
├── data_extract.py         # Main script for data extraction using the YouTube Data API
├── data_transform.py       # Main script for transforming raw JSON data to CSV
├── extract
│   ├── channel
│   │   └── channel_extract.py  # Channel-specific extraction logic
│   └── video
│       └── video_extract.py    # Video-specific extraction logic
├── poetry.lock             # Poetry dependency lock file
├── pyproject.toml          # Project configuration and dependencies (Poetry)
├── README.md               # This README file
├── requirements.txt        # Dependencies if using pip
└── transform
    ├── channel
    │   └── channel_transform.py  # Channel-specific transformation logic
    └── video
        └── video_transform.py    # Video-specific transformation logic
