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
│   ├── channel_id.py       # Contains YouTube channel IDs
│   └── path.py             # Contains file paths configuration
├── dashboard
│   └── dashboard.py        # Streamlit dashboard application
├── data
│   └── bronze
│       └── video
│           └── video_data.csv  # Transformed video data in CSV format
├── data_extract.py         # Main script for data extraction using the YouTube Data API
├── data_transform.py       # Main script for transforming raw JSON data to CSV
├── extract
│   ├── channel
│   │   └── channel_extract.py  # Channel-specific extraction logic
│   └── video
│       └── video_extract.py    # Video-specific extraction logic
├── poetry.lock             # Poetry dependency lock file
├── pyproject.toml          # Project configuration and dependencies (Poetry)
├── README.md               # This README file
├── requirements.txt        # Dependencies if using pip
└── transform
    ├── channel
    │   └── channel_transform.py  # Channel-specific transformation logic
    └── video
        └── video_transform.py    # Video-specific transformation logic
```

## Features

### Data Extraction:
- Harvests YouTube channel uploads and video details with error handling for API interactions.

### Data Transformation:
- Converts raw JSON into a well-structured DataFrame.
- Calculates additional metrics such as video duration in seconds, engagement rate, like ratio, and comment ratio.
-  Transform video and channel dataframe.

### Interactive Dashboard:
- Provides overviews for long and short videos.
- Displays monthly views, engagement rates, and channel-specific analytics.
- Features comparative analysis across months and years.
- Integrates with a PostgreSQL database to capture channel suggestions.

### Configurable and Modular:
- Organized into distinct modules for extraction, transformation, and dashboard rendering for ease of maintenance and scalability.

## Installation

### Clone the Repository:

```bash
git clone https://github.com/your-username/your-repo.git
cd your-repo
```

### Install Dependencies:

If you are using Poetry:

```bash
poetry install
```

Or with pip:

```bash
pip install -r requirements.txt
```

### Environment Variables:

Create a `.env` file in the root directory and add your PostgreSQL database credentials:

```env
DB_HOST=your_db_host
DB_DATABASE=your_db_name
DB_USER=your_db_user
DB_PASSWORD=your_db_password
```

### API Key:

Ensure you have a valid YouTube Data API v3 key. Update your configuration (e.g., in `config/channel_id.py` or wherever applicable) with your API key.

## Usage

### Data Extraction

Run the data extraction script to collect YouTube video data:

```bash
python data_extract.py
```

This script will:
- Connect to the YouTube Data API.
- Retrieve channel upload playlists.
- Harvest video IDs and details.
- Save the raw JSON data to the designated file path.

### Data Transformation

After extracting the data, transform the raw JSON into a CSV:

```bash
python data_transform.py
```

This process reads the JSON file, converts the data into a structured Pandas DataFrame, computes additional metrics (e.g., duration conversion and engagement rate), and saves the output as `video_data.csv` in the `data/bronze/video/` directory.

### Dashboard

Launch the interactive dashboard using Streamlit:

```bash
streamlit run dashboard/dashboard.py
```

The dashboard provides:
- An overview of key metrics.
- Time-series visualizations for video counts and view metrics.
- Detailed monthly comparisons of engagement metrics.
- A form to suggest channels that integrates with a PostgreSQL database.

## Configuration

### API Configuration:
- Update your API key and channel IDs in the appropriate configuration file(s) within the `config` directory.

### Database Setup:
- Ensure that your PostgreSQL database is running and the connection credentials are set in your `.env` file.

### File Paths:
- Adjust the file paths as needed in the code (e.g., where raw data is saved, CSV paths, etc.).

## Contribution Guidelines

Contributions are welcome! To contribute:

1. Fork the repository.
2. Create a new branch for your feature or bug fix.
3. Write tests for your changes.
4. Submit a pull request with a clear description of your changes.

Please adhere to the established coding style and include proper documentation in your commits.

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.

## Additional Resources

- [YouTube Data API v3 Documentation](https://developers.google.com/youtube/v3)
- [Streamlit Documentation](https://docs.streamlit.io/)
- [Pandas Documentation](https://pandas.pydata.org/docs/)
- [SQLAlchemy Documentation](https://www.sqlalchemy.org/)
- [Altair Documentation](https://altair-viz.github.io/)
- [Plotly Express Documentation](https://plotly.com/python/plotly-express/)
