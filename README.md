# Elasticsearch Documents Synchronization

This code provides a Python script for synchronizing data between two MVX Elasticsearch clusters. It retrieves documents from one cluster and indexes them into another cluster to ensure data consistency.

## Prerequisites

- Python (version 3.6 or higher)
- `elasticsearch` library (install using `pip install elasticsearch`)
- `python-dotenv` library (install using `pip install python-dotenv`)

## Setup

1. Clone this repository.

2. Create a `.env` file at the root of the project and add the following lines, replacing the placeholders with your Elasticsearch connection information:

   ```dotenv
   ES1_USERNAME=your_es1_username
   ES2_PASSWORD=your_es2_password
   
## Configuration

The script uses the following configuration parameters:

- `indices_name`: A list of index names to process.
- `batch_size`: The number of documents to retrieve per request (maximum 10000).
- `request_timeout`: Maximum timeout for a request in seconds.
- `max_request_retries`: Maximum number of request retries in case of an error.
- `request_interval`: Interval for batching bulk requests.
- `delay`: Delay between each request interval in seconds.
- `from_interval`: Offset interval from which to start the document search from the current timestamp.
- `offset`: Offset in seconds to add to the search interval of the second Elasticsearch cluster.

## Usage

1. Install the required libraries: `pip install elasticsearch`, `pip install python-dotenv`.
2. Update the configuration parameters in the script to match your Elasticsearch clusters.
3. Run the script: `python data_sync.py`.
4. The script will compare the document IDs between the two Elasticsearch clusters and index the missing documents from one cluster to another. 
5. The script will create log files named <index_name>_missing_documents.txt for each index processed, listing the missing document IDs.

## Functionality

1. Query Elasticsearch to retrieve documents based on the given time range and page size.
2. Compare document IDs between two Elasticsearch clusters.
3. Fetch missing documents from one cluster and index them into the other cluster.
4. Write the missing document IDs to log files.

The script performs these steps for each index specified in the `indices_name` list.

Note: The script uses multithreading to query the Elasticsearch clusters in parallel for better performance.

Please make sure to adjust the configuration parameters and review the code according to your specific requirements before running the script.

Feel free to contribute or provide feedback to improve the script.

