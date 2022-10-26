"""
This lambda function streams a list of JSON objects that represent Braze
custom events and purchases, and sends them to Braze using the POST
/user/track endpoint.

POST /user/track: https://www.braze.com/docs/api/endpoints/user_data/post_user_track/
Event Object: https://www.braze.com/docs/api/objects_filters/event_object/
Purchase Object: https://www.braze.com/docs/api/objects_filters/purchase_object/

The objects must conform to the Braze object format mentioned above.

Prerequisites:
 - Braze REST API Key
"""

import json
import os
import boto3
import requests
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Generator, List, Optional
from urllib.parse import unquote_plus
from requests.exceptions import RequestException
from tenacity import (
    RetryCallState,
    retry,
    stop_after_attempt,  # type: ignore
    wait_exponential,  # type: ignore
    retry_if_exception_type  # type: ignore
)


CHUNK_SIZE_1_MB = 1024 * 1024 
THREADS = int(os.environ.get("THREADS", 15))
FUNCTION_TIME_LIMIT = 1000 * 60 * 3  # 3 minutes remaining from 15 minute timeout
MAX_RETRIES = 5

try:
    BRAZE_API_KEY = os.environ["BRAZE_API_KEY"]
    BRAZE_API_URL = os.environ["BRAZE_API_URL"]
except KeyError:
    print("ERROR: Braze API key or URL is missing. Cannot process the file")
    raise 


REQUEST_HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {BRAZE_API_KEY}",
    "X-Braze-Bulk": "true"
}

def lambda_handler(event, context):
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    object_key = unquote_plus(event['Records'][0]['s3']['object']['key'])
    byte_offset = event.get("byte_offset", 0)

    print(f"INFO: New Braze object import lambda invoked. Starting at byte {byte_offset}")

    s3_file = get_s3_file(bucket_name, object_key)
    processor = S3FileProcessor(s3_file, context, byte_offset)
    processor.process_file()

    print(f"INFO: Processed {format_bytes_read(processor.total_bytes_read)} of the current file")
    print(f"INFO: Imported {processor.processed_objects_count} objects")

    if not processor.is_finished():
        invoke_next_lambda(
            event,
            context.function_name,
            processor.total_bytes_read,
        )
    else:
        print(f"INFO: File {object_key} imported successfully")

    return {
        "objects_sent": processor.processed_objects_count,
        "bytes_read": processor.total_bytes_read,
        "is_finished": processor.is_finished()
    }
    

class S3FileProcessor:
    def __init__(
        self, 
        s3_file,  # boto3.s3.Object
        lambda_context,  # lambda context object: https://docs.aws.amazon.com/lambda/latest/dg/python-context.html
        byte_offset: int = 0,
    ) -> None:
        self.s3_file = s3_file
        self.lambda_context = lambda_context
        self.total_bytes_read = byte_offset
        self.valid_chunk_bytes_read = 0
        self.processed_objects_count = 0

    def process_file(self) -> None:
        all_parsed_objects, current_batch  = [], []
        for braze_object in self.get_objects_from_file_stream():
            current_batch.append(braze_object)

            if len(current_batch) == 75:
                all_parsed_objects.append(current_batch)
                current_batch = []
            
            if len(all_parsed_objects) == THREADS:
                self.send_objects_to_braze(all_parsed_objects)
                if self.should_terminate():
                    break
                all_parsed_objects = []
        else:
            if current_batch:
                all_parsed_objects.append(current_batch)
            self.send_objects_to_braze(all_parsed_objects)

        # Leftover bytes from the end of the file that include closing array
        # bracket and whitespace
        self.total_bytes_read += self.valid_chunk_bytes_read

    def get_objects_from_file_stream(self) -> Generator:
        current_object = ""
        current_bytes = 0
        opened = 0
        file_stream = self.s3_file.get(Range=f"bytes={self.total_bytes_read}-")["Body"]
        for byte_chunk in file_stream.iter_chunks(chunk_size=CHUNK_SIZE_1_MB):
            for line_as_bytes in byte_chunk.splitlines(keepends=True):
                current_bytes += len(line_as_bytes)
                line = line_as_bytes.decode('utf-8').strip()

                opened += line.count('{')
                opened -= line.count('}')

                if line and not opened:
                    if line[-1] == ',':
                        line = line[:-1]
                
                    is_single_line_array = line[0] == '[' and line[-1] == ']'
                    if line and not is_single_line_array and line[0] == '[':
                        line = line[1:]
                    if line and not is_single_line_array and line[-1] == ']':
                        line = line[:-1]

                # Not a valid object, but a valid line, such as opening 
                # array bracket on a single line
                if not line:
                    self.valid_chunk_bytes_read += current_bytes
                    current_bytes = 0
                    continue

                current_object += line
                try:
                    event_data = json.loads(current_object)
                    self.valid_chunk_bytes_read += current_bytes
                    current_bytes = 0
                    current_object = ""

                    if isinstance(event_data, list):
                        for event in event_data:
                            yield event
                    else:
                        yield event_data
                except json.JSONDecodeError:
                    pass
        

    def send_objects_to_braze(self, objects: List[List[Dict]]) -> None:
        sent_objects = send_object_chunks_to_braze(objects)
        self.processed_objects_count += sent_objects
        self.total_bytes_read += self.valid_chunk_bytes_read
        self.valid_chunk_bytes_read = 0


    def should_terminate(self) -> bool:
        return self.lambda_context.get_remaining_time_in_millis() < FUNCTION_TIME_LIMIT

    def is_finished(self) -> bool:
        return not self.processed_objects_count \
            or not self.total_bytes_read \
            or self.total_bytes_read >= self.s3_file.content_length


def get_s3_file(
    bucket_name: str, 
    object_key: str,
    session: Optional[boto3.Session] = None
):
    if session:
        return session.resource("s3").Object(bucket_name, object_key)  # type: ignore
    return boto3.resource("s3").Object(bucket_name, object_key)   # type: ignore


def send_object_chunks_to_braze(object_chunks: List[List[Dict]]) -> int:
    """Sends a batch of requests to the Braze API. Expects a list of 75 object
    chunks. Each chunk will be sent to the API in its own thread and it will be
    retried independently in case of an error.
    """
    sent = 0
    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        results = executor.map(send_objects_to_braze, object_chunks)
        for result in results:
            sent += result
    if sent:
        print(f"INFO: Successfully sent {sent} objects to Braze")
    return sent


def on_network_retry_error(state: RetryCallState):
    print(
        f"Retry attempt: {state.attempt_number}/{MAX_RETRIES}. Wait time: {state.idle_for}")


@retry(retry=retry_if_exception_type(RequestException),
       wait=wait_exponential(multiplier=5, min=5),
       stop=stop_after_attempt(MAX_RETRIES),
       after=on_network_retry_error,
       reraise=True)
def send_objects_to_braze(objects: List[Dict]) -> int:
    """Sends a chunk of 75 objects to Braze. Parses the response and prints
    whether any of the objects were not possible to be parsed by Braze. Raises
    an exception in case it encounters an error response.
    Return the number of objects that were successfully sent to Braze.

    :param objects: List of object dictionaries representing custom events
                    or purchases
    :returns: Number of successfully imported objects
    :raises: APIRetryError - if a retryable error occurred such as an unresponsive
                             server
             FatalAPIError - if a non-recoverable error occurred such as lack of
                             or invalid API key
    """
    if not objects:
        return 0

    events, purchases = [], []
    for candidate in objects:
        if 'price' in candidate and 'currency' in candidate:
            purchases.append(candidate)
        else:
            events.append(candidate)

    # print(f"DEBUG: Sending {len(events)} events and {len(purchases)} purchases")
    
    data = {}
    if events:
        data['events'] = events
    if purchases:
        data['purchases'] = purchases

    response = requests.post(
        f"{BRAZE_API_URL}/users/track",
        headers=REQUEST_HEADERS, 
        data=json.dumps(data)
    )

    response_msg = response.json()
    if response.status_code == 201 and 'errors' in response_msg:
        print(f"ERROR: Encountered errors processing some users: {response_msg.get('errors')}")

    if response.status_code == 400:
        print(f"ERROR: Encountered error for user chunk. {response.text}")
    
    if response.status_code == 429 or response.status_code >= 500:
        raise APIRetryError("Server error. Retrying..")

    if response.status_code > 400:
        raise FatalAPIError(response_msg.get('message', response.text))
    
    return response_msg.get('events_processed', 0) + response_msg.get('purchases_processed', 0)



def invoke_next_lambda(event: Dict, function_name: str, byte_offset: int) -> None:
    print("INFO: Invoking lambda to continue processing the file")
    event = {
        **event,
        "byte_offset": byte_offset
    }

    boto3.client("lambda").invoke(
        FunctionName=function_name,
        InvocationType="Event",
        Payload=json.dumps(event),
    )


def format_bytes_read(byte_count: int) -> str:
    if byte_count >= 10**9:
        return f"{byte_count / (10**9):,.1f} GB"
    if byte_count >= 10**6:
        return f"{byte_count / (10**6):,.1f} MB"
    if byte_count >= 10**3:
        return f"{byte_count / (10**3):,.1f} KB"
    return f"{byte_count} B"


class APIRetryError(RequestException):
    """Raised on 429 or 5xx server exception. If there are retries left, the
    API call will be made again after a delay."""
    pass


class FatalAPIError(Exception):
    """Raised when received an unexpected error from the server. Causes the
    execution to fail."""
    pass
