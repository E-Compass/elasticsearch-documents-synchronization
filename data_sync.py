from elasticsearch import Elasticsearch, TransportError, ConnectionTimeout, helpers
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
import os

# CONFIG
# List of index names to process
indices_name = ["accountsesdt", "tokens", "blocks", "receipts",
                "transactions", "miniblocks", "rounds",  "accountshistory",
                "scresults", "accountsesdthistory", "scdeploys", "logs", "operations"]

# Batch size for retrieving documents per request (max 10000)
batch_size = 5000

# Maximum timeout for a request in seconds
request_timeout = 30

# Maximum number of request retries in case of an error
max_request_retries = 20

# Interval for batching bulk requests
request_interval = 3600 * 12  # 12 hours

# Delay between each request interval in seconds
delay = 0.5

# Offset interval from which to start the document search from the current timestamp
from_interval = 3600 * 24  # One day

# Offset in seconds to add to the es2 search interval.
# Observers might be slightly out of sync, and the timestamp from es1 can be
# different from that of es2, but the documents can still be present in es2.
offset = 60

# Actual timestamp
actual_timestamp = int(time.time())
from_timestamp = actual_timestamp - from_interval

# Elasticsearch's connection configuration
load_dotenv()
es2_username = os.getenv("ES2_USERNAME")
es2_password = os.getenv("ES2_PASSWORD")

es1 = Elasticsearch("https://index.multiversx.com:443", request_timeout=request_timeout)
es2 = Elasticsearch("https://mvx-index.e-compass.io:443", request_timeout=request_timeout,
                    basic_auth=(es2_username, es2_password))


def query_documents(es, index, batch_start_timestamp, batch_end_timestamp, page_size, max_retries):
    """
    Query Elasticsearch to retrieve documents based on the given time range and page size.
    Returns a list of document IDs.
    """
    document_ids = []
    # Perform the search and measure the execution time
    start_time = time.time()

    # Construct the initial query
    query = {
        "range": {
            "timestamp": {
                "gte": batch_end_timestamp,
                "lt": batch_start_timestamp
            }
        }
    }
    sort_order = {
        "timestamp": "desc"
    }

    for attempt in range(max_retries):
        try:
            # Perform the initial search
            response = es.search(
                index=index,
                query=query,
                size=page_size,
                sort=sort_order,
                scroll="1m"
            )
            break
        except (TransportError, ConnectionError) as e:
            if isinstance(e, ConnectionTimeout):
                print("Request timed out. Retrying...")
                if attempt == max_retries - 1:
                    raise
                continue
            else:
                print(f"An error occurred during the Elasticsearch search request: {e}")
                return document_ids, time.time() - start_time

    scroll_id = response.get("_scroll_id")
    hits = response["hits"]["hits"]
    current_ids = [hit["_id"] for hit in hits]
    document_ids.extend(current_ids)

    # Continue scrolling until all documents are retrieved
    while len(hits) > 0:
        for attempt in range(max_retries):
            try:
                response = es.scroll(scroll_id=scroll_id, scroll="1m")
                break
            except (TransportError, ConnectionError) as e:
                if isinstance(e, ConnectionTimeout):
                    print("Scroll request timed out. Retrying...")
                    if attempt == max_retries - 1:
                        raise
                    continue
                else:
                    print(f"An error occurred during the Elasticsearch scroll request: {e}")
                    break

        hits = response["hits"]["hits"]
        current_ids = [hit["_id"] for hit in hits]
        document_ids.extend(current_ids)

    # Clear the scroll
    for attempt in range(max_retries):
        try:
            es.clear_scroll(scroll_id=scroll_id)
            break
        except (TransportError, ConnectionError) as e:
            if isinstance(e, ConnectionTimeout):
                print("Clear scroll request timed out. Retrying...")
                if attempt == max_retries - 1:
                    raise
                continue
            else:
                print(f"An error occurred when attempting to clear the Elasticsearch scroll: {e}")

    end_time = time.time()
    execution_time = end_time - start_time

    return document_ids, execution_time


def compare_document_ids(ids1, ids2):
    """
    Compare two lists of document IDs and return the IDs present in ids1 but not in ids2.
    """
    ids1_set = set(ids1)
    ids2_set = set(ids2)

    return list(ids1_set - ids2_set)


def parallel_query_documents(es_instance_1, es_instance_2, index_name_query, start_time, end_time, batch_sz):
    """
    Execute parallel queries to Elasticsearch instances to retrieve documents based on the given index name,
    time range and batch size.
    """
    with ThreadPoolExecutor(max_workers=2) as executor:
        future_es1_query = executor.submit(query_documents, es_instance_1, index_name_query,
                                           start_time, end_time, batch_sz, max_request_retries)
        future_es2_query = executor.submit(query_documents, es_instance_2, index_name_query,
                                           start_time + offset, end_time - offset, batch_sz, max_request_retries)

        result_es1_query, es1_execution_time = future_es1_query.result()
        result_es2_query, es2_execution_time = future_es2_query.result()

    return (result_es1_query, es1_execution_time), (result_es2_query, es2_execution_time)


def get_documents(es, index, doc_ids):
    """
    Retrieve documents from Elasticsearch based on the specified index and document IDs.
    """
    if not doc_ids:
        return []
    try:
        response = es.mget(index=index, ids=doc_ids)
        return [(doc['_id'], doc['_source']) for doc in response['docs'] if doc['found']]
    except Exception as e:
        print(f"An error occurred while getting the documents: {e}")
        return []


def bulk_index_documents(es, index, documents):
    """
    Bulk index documents into Elasticsearch on the specified index.
    """
    if not documents:
        return

    actions = []
    for doc in documents:
        action = {
            "_index": index,
            "_id": doc[0],
            "_source": doc[1]
        }
        actions.append(action)
        print(f"Preparing to index document with ID: {doc[0]}")
    try:
        response = helpers.bulk(es, actions)
        if response[1]:
            print(f"Errors occurred while bulk indexing: {response[1]}")
        else:
            print("Bulk indexing completed successfully.")

    except Exception as e:
        print(f"An error occurred while bulk indexing the documents: {e}")


def fetch_and_index_missing_documents(fetch_missing_ids, fetch_index_name, source_es, destination_es):
    """
    Fetch missing documents from the source and index them in the destination Elasticsearch instance.
    """
    missing_documents = get_documents(source_es, fetch_index_name, fetch_missing_ids)
    if missing_documents:
        # print(missing_documents)
        bulk_index_documents(destination_es, fetch_index_name, missing_documents)


for index_name in indices_name:
    # Open log file
    file_name = index_name + '_missing_documents.txt'
    with open(file_name, 'w') as file:
        current_timestamp = actual_timestamp
        while current_timestamp > from_timestamp:
            # Calculate the start and end timestamps for the current batch
            start_timestamp = current_timestamp
            end_timestamp = current_timestamp - request_interval

            # Query the clusters Elasticsearch in parallel
            (document_ids_es1, execution_time_es1), (document_ids_es2, execution_time_es2) \
                = parallel_query_documents(es1, es2, index_name, start_timestamp, end_timestamp, batch_size)

            # Compare the document IDs between es1 and es2
            missing_ids = compare_document_ids(document_ids_es1, document_ids_es2)

            # Convert timestamps to datetime objects
            start_datetime = datetime.fromtimestamp(start_timestamp)
            end_datetime = datetime.fromtimestamp(end_timestamp)

            print(f"\n################# {index_name} #####################")
            print(f"Interval {start_datetime} - {end_datetime}")

            print(f"Number of document in {index_name} in es1: {len(document_ids_es1)}")
            print(f"Number of document in {index_name} in es2: {len(document_ids_es2)}")
            # Print the missing document IDs for debugging
            print(f"Missing Document: {len(missing_ids)}")
            # Print the execution time for es1 and es2
            print(f"Execution time for {index_name} in es1: {execution_time_es1} seconds")
            print(f"Execution time for {index_name} in es2: {execution_time_es2} seconds")

            # Get and index missing documents in bulk
            fetch_and_index_missing_documents(missing_ids, index_name, es1, es2)

            # Write the missing document IDs on file
            for doc_id in missing_ids:
                file.write(doc_id + '\n')

            # Update the current timestamp
            current_timestamp -= request_interval

            # Sleep for the specified delay
            time.sleep(delay)
