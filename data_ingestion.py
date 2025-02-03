import os
import logging
import time
from datetime import datetime
import clickhouse_driver
from atproto import Client
from typing import List, Dict, Any

# Configure logging
logging.basicConfig(
    level=os.getenv('LOG_LEVEL', 'INFO'),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class BlueskyIngestion:
    def __init__(self):
        self.client = clickhouse_driver.Client(
            host=os.getenv('CLICKHOUSE_HOST', 'localhost'),
            port=int(os.getenv('CLICKHOUSE_PORT', 9000)),
            user=os.getenv('CLICKHOUSE_USER', 'default'),
            password=os.getenv('CLICKHOUSE_PASSWORD', ''),
            database=os.getenv('CLICKHOUSE_DATABASE', 'bluesky')
        )
        
        self.bsky = Client()
        self.batch_size = int(os.getenv('BATCH_SIZE', 1000))
        
    def connect_bluesky(self):
        """Authenticate with Bluesky"""
        self.bsky.login(
            os.getenv('BLUESKY_USERNAME'),
            os.getenv('BLUESKY_PASSWORD')
        )
    
    def process_posts(self, posts: List[Dict[Any, Any]]):
        """Process and insert posts into ClickHouse"""
        try:
            # Transform posts into the correct format
            values = [
                (
                    post['uri'],
                    post['author']['handle'],
                    post['record']['text'],
                    datetime.fromisoformat(post['record']['createdAt'].replace('Z', '+00:00')),
                    post.get('likeCount', 0),
                    post.get('repostCount', 0)
                )
                for post in posts
            ]
            
            # Insert into ClickHouse
            self.client.execute(
                '''
                INSERT INTO posts 
                (uri, author_handle, content, created_at, like_count, repost_count)
                VALUES
                ''',
                values
            )
            
        except Exception as e:
            logger.error(f"Error processing posts: {e}")
            raise

def main():
    ingestion = BlueskyIngestion()
    
    try:
        ingestion.connect_bluesky()
        logger.info("Connected to Bluesky successfully")
        
        # Main ingestion loop
        while True:
            # Implement your ingestion logic here
            # This is a placeholder for the actual implementation
            time.sleep(60)
            
    except Exception as e:
        logger.error(f"Fatal error in main loop: {e}")
        raise

if __name__ == "__main__":
    main()