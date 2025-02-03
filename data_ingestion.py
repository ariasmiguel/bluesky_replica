import os
import json
import gzip
import websockets
import asyncio
import boto3
import clickhouse_connect
from datetime import datetime
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BlueskyIngestor:
    def __init__(self):
        self.ws_url = "wss://jetstream1.us-east.bsky.network"
        self.bucket_path = os.environ['BUCKET_PATH']
        self.max_messages = int(os.environ.get('MAX_MESSAGES', 1000))
        self.s3_client = boto3.client('s3')
        self.bucket_name = self.bucket_path.split('/')[2]
        self.bucket_prefix = '/'.join(self.bucket_path.split('/')[3:])

    def get_most_recent_file(self):
        """Get the most recent CSV file from S3 bucket"""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=self.bucket_prefix
            )
            
            if 'Contents' not in response:
                logger.error("No files found in bucket")
                return None

            files = [obj['Key'] for obj in response['Contents'] 
                    if obj['Key'].endswith('.csv.gz')]
            
            if not files:
                logger.error("No matching .csv.gz files found")
                return None

            return sorted(files)[-1]
        except Exception as e:
            logger.error(f"Error accessing S3: {e}")
            return None

    def extract_cursor(self, filename):
        """Extract timestamp from filename"""
        return Path(filename).stem.split('.')[0]

    async def process_messages(self, cursor):
        """Connect to WebSocket and process messages"""
        output_file = "output.json"
        messages = []
        
        async with websockets.connect(
            f"{self.ws_url}/subscribe?wantedCollections=app.*&cursor={cursor}"
        ) as websocket:
            while len(messages) < self.max_messages:
                try:
                    message = await websocket.recv()
                    messages.append(json.loads(message))
                except websockets.exceptions.ConnectionClosed:
                    break

        if messages:
            with open(output_file, 'w') as f:
                for msg in messages:
                    f.write(json.dumps(msg) + '\n')

        return len(messages), output_file

    def process_chunk(self, count, output_file):
        """Process message chunk and upload to S3"""
        try:
            # Get last timestamp
            with open(output_file, 'r') as f:
                lines = f.readlines()
                last_message = json.loads(lines[-1])
                last_value = str(last_message.get('time_us'))

            if not last_value:
                logger.error("Error: last_value is empty")
                return False, None

            # Rename files
            json_file = f"{last_value}.json"
            csv_file = f"{last_value}.csv"
            gz_file = f"{csv_file}.gz"
            
            os.rename(output_file, json_file)

            # Process with ClickHouse
            client = clickhouse_connect.get_client()
            result = client.query(
                "SELECT line as data FROM file($json_file, 'LineAsString')",
                parameters={'json_file': json_file}
            )

            # Save to CSV and compress
            with open(csv_file, 'w') as f:
                f.write('data\n')  # header
                for row in result.result_rows:
                    f.write(f"{row[0]}\n")

            with open(csv_file, 'rb') as f_in:
                with gzip.open(gz_file, 'wb') as f_out:
                    f_out.writelines(f_in)

            # Upload to S3
            s3_path = f"{self.bucket_prefix}/{gz_file}"
            self.s3_client.upload_file(gz_file, self.bucket_name, s3_path)

            # Cleanup
            for file in [json_file, csv_file, gz_file]:
                if os.path.exists(file):
                    os.remove(file)

            logger.info(f"Processed {count} messages")
            return True, last_value

        except Exception as e:
            logger.error(f"Error processing chunk: {e}")
            return False, None

    async def run(self):
        """Main ingestion loop"""
        has_more = True
        
        while has_more:
            most_recent_file = self.get_most_recent_file()
            if not most_recent_file:
                return

            cursor = self.extract_cursor(most_recent_file)
            logger.info(f"Extracted cursor: {cursor}")

            count, output_file = await self.process_messages(cursor)
            logger.info(f"Received {count} messages")

            has_more = count >= self.max_messages
            if has_more:
                success, _ = self.process_chunk(count, output_file)
                has_more = success

async def main():
    ingestor = BlueskyIngestor()
    await ingestor.run()

if __name__ == "__main__":
    asyncio.run(main())