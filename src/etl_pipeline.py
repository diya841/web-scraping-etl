# src/etl_pipeline.py

import requests
from bs4 import BeautifulSoup
import sqlite3
import pandas as pd
from datetime import datetime
import logging
import hashlib

class WebScraper:
    def __init__(self, db_name: str = "web_data.db"):
        self.db_name = db_name
        self.setup_logging()
        self.setup_database()
    
    def setup_logging(self):
        """Set up logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
    
    def setup_database(self):
        """Initialize SQLite database"""
        try:
            with sqlite3.connect(self.db_name) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS web_data (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        title TEXT,
                        content TEXT,
                        url TEXT UNIQUE,
                        timestamp TEXT,
                        data_hash TEXT UNIQUE
                    )
                ''')
                conn.commit()
                self.logger.info("Database setup completed successfully")
        except Exception as e:
            self.logger.error(f"Database setup failed: {str(e)}")
            raise
    def extract_data(self, url: str) -> dict:
        """Extract data from a single website"""
        try:
            self.logger.info(f"Extracting data from: {url}")
            
            # Add headers to avoid blocking
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()  # Raise exception for bad status codes
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract basic data
            data = {
                'url': url,
                'title': soup.title.string if soup.title else '',
                'content': soup.get_text()[:1000],  # Limit content size
                'timestamp': datetime.now().isoformat()
            }
            
            self.logger.info(f"Successfully extracted data from: {url}")
            return data
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to extract data from {url}: {str(e)}")
            return None
    
    def transform_data(self, data: dict) -> dict:
        """Clean and transform the extracted data"""
        if not data:
            return None
            
        try:
            # Clean the data
            cleaned_data = {
                'title': data['title'].strip(),
                'content': ' '.join(data['content'].split()),  # Clean whitespace
                'url': data['url'],
                'timestamp': data['timestamp']
            }
            
            # Generate hash for deduplication
            data_hash = hashlib.md5(
                f"{cleaned_data['url']}{cleaned_data['content'][:100]}".encode()
            ).hexdigest()
            
            cleaned_data['data_hash'] = data_hash
            
            self.logger.info(f"Successfully transformed data for: {data['url']}")
            return cleaned_data
            
        except Exception as e:
            self.logger.error(f"Transformation failed: {str(e)}")
            return None
    
    def load_data(self, data: dict) -> bool:
        """Load data into SQLite database"""
        if not data:
            return False
            
        try:
            with sqlite3.connect(self.db_name) as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    INSERT OR REPLACE INTO web_data 
                    (title, content, url, timestamp, data_hash)
                    VALUES (?, ?, ?, ?, ?)
                ''', (
                    data['title'],
                    data['content'],
                    data['url'],
                    data['timestamp'],
                    data['data_hash']
                ))
                
                conn.commit()
                self.logger.info(f"Successfully loaded data for: {data['url']}")
                return True
                
        except Exception as e:
            self.logger.error(f"Load operation failed: {str(e)}")
            return False
    
    def run_pipeline(self, url: str) -> bool:
        """Execute the complete ETL pipeline for a single URL"""
        try:
            # Extract
            raw_data = self.extract_data(url)
            if not raw_data:
                return False
            
            # Transform
            transformed_data = self.transform_data(raw_data)
            if not transformed_data:
                return False
            
            # Load
            success = self.load_data(transformed_data)
            return success
            
        except Exception as e:
            self.logger.error(f"Pipeline failed for {url}: {str(e)}")
            return False