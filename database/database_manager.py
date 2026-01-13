import sqlite3
import json
import hashlib
from pathlib import Path
from datetime import datetime
from contextlib import contextmanager
from typing import Optional, List, Dict, Any

from .schema import * 


class DatabaseManager:
    """
    Handles SQLite database connections, schema initialization, and data operations.
    """
    def __init__(self, db_path: str = "output/real_estate.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_schema()

    @contextmanager
    def get_connection(self):
        """
        Context manager for database connections.
        Ensures connections are closed and transactions committed/rolled back.
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row 
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()

    def _ensure_schema(self):
        """Initialize database schema on startup."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Create Tables
            cursor.execute(BDS_RAW_LISTINGS_TABLE) 
            cursor.execute(ONEHOUSING_RAW_LISTINGS_TABLE)
            cursor.execute(CLEANED_LISTINGS_TABLE)
            cursor.execute(METADATA_TABLE)

            # Create Indices
            for index_sql in INDICES:
                cursor.execute(index_sql)

    def _compute_content_hash(self, data: Dict[str, Any]) -> str:
        """Compute hash of content fields to detect changes."""
        hash_fields = [
            str(data.get('title', '')),
            str(data.get('short_address', '')),
            str(data.get('main_info', '')),
            str(data.get('description', '')),
            str(data.get('other_info', '')),
        ]
        content = '|'.join(hash_fields)
        return hashlib.sha256(content.encode()).hexdigest()

    def _compute_onehousing_content_hash(self, data: Dict[str, Any]) -> str:
        """Compute hash of OneHousing content fields to detect changes."""
        hash_fields = [
            str(data.get('listing_title', '')),
            str(data.get('total_price', '')),
            str(data.get('features', '')),
            str(data.get('property_description', '')),
        ]
        content = '|'.join(hash_fields)
        return hashlib.sha256(content.encode()).hexdigest()

    # --- BDS Raw Listings Operations ---
    
    def get_existing_listing_ids(self) -> set:
        """Get all existing listing IDs from the database."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT listing_id FROM bds_raw_listings")
            return {str(row[0]) for row in cursor.fetchall()}

    def check_listing_status(self, url: str, content_hash: str) -> str:
        """
        Check if a listing is NEW, DUPLICATE, or CHANGED.
        
        Returns:
            'NEW': URL doesn't exist in database
            'DUPLICATE': URL exists with same content hash
            'CHANGED': URL exists but content hash is different
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT content_hash FROM bds_raw_listings WHERE url = ? ORDER BY scraped_at DESC LIMIT 1",
                (url,)
            )
            result = cursor.fetchone()
            
            if result is None:
                return 'NEW'
            elif result[0] == content_hash:
                return 'DUPLICATE'
            else:
                return 'CHANGED'

    def insert_raw_listings_batch(self, listings: List[Dict[str, Any]], metadata_id: int) -> Dict[str, int]:
        """
        Insert a batch of raw listings into the database.
        
        Returns:
            Dictionary with counts: {'new': X, 'duplicate': Y, 'changed': Z}
        """
        stats = {'new': 0, 'duplicate': 0, 'changed': 0}
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            for listing in listings:
                content_hash = self._compute_content_hash(listing)
                status = self.check_listing_status(listing['url'], content_hash)
                
                if status == 'DUPLICATE':
                    stats['duplicate'] += 1
                    continue
                
                stats[status.lower()] += 1
                
                cursor.execute("""
                    INSERT INTO bds_raw_listings 
                    (listing_id, url, title, short_address, address_parts, latitude, longitude,
                     main_info, description, other_info, image_urls, content_hash, status, scraped_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    listing.get('id'),
                    listing.get('url'),
                    listing.get('title'),
                    listing.get('short_address'),
                    listing.get('address_parts'),
                    listing.get('latitude'),
                    listing.get('longitude'),
                    listing.get('main_info'),
                    listing.get('description'),
                    listing.get('other_info'),
                    listing.get('image_urls'),
                    content_hash,
                    status,
                    datetime.now().isoformat()
                ))
            
            # Update metadata with statistics
            cursor.execute("""
                UPDATE scraping_metadata 
                SET status = ?, is_changed = ?
                WHERE id = ?
            """, (
                f"Completed: {stats['new']} new, {stats['changed']} changed, {stats['duplicate']} duplicate",
                'YES' if stats['changed'] > 0 else 'NO',
                metadata_id
            ))
        
        return stats

    def get_listings_for_cleaning(self, status_filter: Optional[List[str]] = None) -> List[Dict]:
        """
        Get raw listings that need to be cleaned.
        
        Args:
            status_filter: List of statuses to filter by (e.g., ['NEW', 'CHANGED'])
        """
        if status_filter is None:
            status_filter = ['NEW', 'CHANGED']
        
        placeholders = ','.join('?' * len(status_filter))
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"""
                SELECT r.* FROM bds_raw_listings r
                LEFT JOIN cleaned_listings c ON r.listing_id = c.ID
                WHERE r.status IN ({placeholders}) AND c.ID IS NULL
                ORDER BY r.scraped_at DESC
            """, status_filter)
            
            columns = [description[0] for description in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]

    # --- OneHousing Raw Listings Operations ---
    
    def get_existing_onehousing_property_ids(self) -> set:
        """Get all existing OneHousing property IDs from the database."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT property_id FROM onehousing_raw_listings WHERE property_id IS NOT NULL")
            return {str(row[0]) for row in cursor.fetchall()}
    
    def check_onehousing_listing_status(self, url: str, content_hash: str) -> str:
        """
        Check if a OneHousing listing is NEW, DUPLICATE, or CHANGED.
        
        Returns:
            'NEW': URL doesn't exist in database
            'DUPLICATE': URL exists with same content hash
            'CHANGED': URL exists but content hash is different
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT content_hash FROM onehousing_raw_listings WHERE property_url = ? ORDER BY scraped_at DESC LIMIT 1",
                (url,)
            )
            result = cursor.fetchone()
            
            if result is None:
                return 'NEW'
            elif result[0] == content_hash:
                return 'DUPLICATE'
            else:
                return 'CHANGED'
    
    def insert_onehousing_raw_listings_batch(self, listings: List[Dict[str, Any]], metadata_id: int) -> Dict[str, int]:
        """
        Insert a batch of OneHousing raw listings into the database.
        
        Returns:
            Dictionary with counts: {'new': X, 'duplicate': Y, 'changed': Z}
        """
        stats = {'new': 0, 'duplicate': 0, 'changed': 0}
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            for listing in listings:
                content_hash = self._compute_onehousing_content_hash(listing)
                status = self.check_onehousing_listing_status(listing['property_url'], content_hash)
                
                if status == 'DUPLICATE':
                    stats['duplicate'] += 1
                    continue
                
                stats[status.lower()] += 1
                
                cursor.execute("""
                    INSERT INTO onehousing_raw_listings 
                    (property_id, property_url, listing_title, total_price, unit_price, city, district,
                     alley_width, features, property_description, image_url, content_hash, status, scraped_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    listing.get('property_id'),
                    listing.get('property_url'),
                    listing.get('listing_title'),
                    listing.get('total_price'),
                    listing.get('unit_price'),
                    listing.get('city'),
                    listing.get('district'),
                    listing.get('alley_width'),
                    listing.get('features'),
                    listing.get('property_description'),
                    listing.get('image_url'),
                    content_hash,
                    status,
                    datetime.now().isoformat()
                ))
            
            # Update metadata with statistics
            cursor.execute("""
                UPDATE scraping_metadata 
                SET status = ?, is_changed = ?
                WHERE id = ?
            """, (
                f"Completed: {stats['new']} new, {stats['changed']} changed, {stats['duplicate']} duplicate",
                'YES' if stats['changed'] > 0 else 'NO',
                metadata_id
            ))
        
        return stats
    
    def get_onehousing_listings_for_cleaning(self, status_filter: Optional[List[str]] = None) -> List[Dict]:
        """
        Get OneHousing raw listings that need to be cleaned.
        
        Args:
            status_filter: List of statuses to filter by (e.g., ['NEW', 'CHANGED'])
        """
        if status_filter is None:
            status_filter = ['NEW', 'CHANGED']
        
        placeholders = ','.join('?' * len(status_filter))
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"""
                SELECT r.* FROM onehousing_raw_listings r
                LEFT JOIN cleaned_listings c ON r.property_id = c.ID
                WHERE r.status IN ({placeholders}) AND c.ID IS NULL
                ORDER BY r.scraped_at DESC
            """, status_filter)
            
            columns = [description[0] for description in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]

    # --- Cleaned Listings Operations ---
    
    def insert_cleaned_listings_batch(self, cleaned_data: List[Dict[str, Any]]) -> int:
        """
        Insert a batch of cleaned listings into the database.
        Only inserts listings that don't already exist (based on listing_id).
        
        Returns:
            Number of records inserted
        """
        inserted = 0
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            for record in cleaned_data:
                # Check if listing already exists in cleaned table
                cursor.execute(
                    "SELECT ID FROM cleaned_listings WHERE ID = ?",
                    (record.get('ID'),)
                )
                
                if cursor.fetchone() is not None:
                    continue  # Skip duplicates
                
                # Prepare column names and values
                columns = list(record.keys())
                placeholders = ','.join('?' * len(columns))
                column_names = ','.join(f'"{col}"' for col in columns)
                
                cursor.execute(f"""
                    INSERT INTO cleaned_listings ({column_names})
                    VALUES ({placeholders})
                """, list(record.values()))
                
                inserted += 1
        
        return inserted

    # --- Metadata Operations ---
    
    def create_scraping_metadata(self, website: str) -> int:
        """
        Create a new metadata record for a scraping session.
        
        Returns:
            The ID of the created metadata record
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO scraping_metadata 
                (websites, scraped_time, status, is_changed)
                VALUES (?, ?, ?, ?)
            """, (
                website,
                datetime.now().isoformat(),
                'IN_PROGRESS',
                'UNKNOWN'
            ))
            return cursor.lastrowid

    def update_scraping_metadata(self, metadata_id: int, status: str, 
                                 error_message: Optional[str] = None):
        """Update metadata record with final status."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE scraping_metadata 
                SET status = ?, error_message = ?
                WHERE id = ?
            """, (status, error_message, metadata_id))

    def get_scraping_history(self, limit: int = 10) -> List[Dict]:
        """Get recent scraping history."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM scraping_metadata 
                ORDER BY scraped_time DESC 
                LIMIT ?
            """, (limit,))
            
            columns = [description[0] for description in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]