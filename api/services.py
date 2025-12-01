"""
Service layer for dataset management.

This module contains the business logic for creating and updating dynamic datasets
in PostgreSQL, separated from the API layer for better maintainability and testability.
"""

import logging
import re
from typing import Any, Dict, List, Set, Tuple

from django.conf import settings
from django.core.exceptions import ValidationError
from django.db import connection, transaction
from psycopg2 import sql

logger = logging.getLogger(__name__)


# Constants
TABLE_NAME_PATTERN = r'^[a-zA-Z0-9_]+$'
COLUMN_NAME_PATTERN = r'^[a-zA-Z0-9_]+$'
DEFAULT_COLUMN_TYPE = 'TEXT'
PRIMARY_KEY_COLUMN = 'id'
POSTGREST_NOTIFICATION_CHANNEL = 'pgrst'


class DatasetService:
    """
    Service class for managing dynamic dataset creation and updates.
    
    This class handles all business logic related to creating tables dynamically
    from JSON data, including validation, schema evolution, and data insertion.
    
    Responsibilities:
    - Validate table names, column names, and data structure
    - Create new tables or update existing ones
    - Handle schema evolution (adding new columns)
    - Manage database transactions atomically
    - Notify PostgREST of schema changes
    """
    
    def __init__(self, table_data: Dict[str, List[Dict[str, Any]]]):
        """
        Initialize the service with table data.
        
        Args:
            table_data: Dictionary with single key (table name) and value (list of row data).
                       Example: {"users": [{"name": "John", "age": "30"}]}
        
        Raises:
            ValidationError: If the table_data structure is invalid.
        """
        self.table_data = table_data
        self.table_name: str = ""
        self.data: List[Dict[str, Any]] = []
        self.columns: List[str] = []
        
    def create_or_update_dataset(self) -> Tuple[str, int]:
        """
        Main method to create or update a dataset.
        
        This method orchestrates the entire process:
        1. Validates the input data
        2. Creates or updates the table in an atomic transaction
        3. Inserts the data
        4. Notifies PostgREST
        
        Returns:
            Tuple of (table_name, rows_inserted)
        
        Raises:
            ValidationError: If validation fails
            Exception: If database operations fail
        """
        # Step 1: Validate all input
        self._validate_structure()
        self._validate_table_name()
        self._validate_columns()
        self._validate_data_not_empty()
        self._validate_data_consistency()
        
        logger.info(f"Creating/updating dataset '{self.table_name}' with {len(self.data)} rows")
        
        # Step 2: Execute database operations in atomic transaction
        try:
            with transaction.atomic():
                with connection.cursor() as cursor:
                    rows_inserted = self._process_table_and_data(cursor)
                    self._notify_postgrest(cursor)
            
            logger.info(f"Successfully processed {rows_inserted} rows for table '{self.table_name}'")
            return self.table_name, rows_inserted
            
        except Exception as e:
            logger.error(f"Database error while processing table '{self.table_name}': {str(e)}")
            raise
    
    def _validate_structure(self) -> None:
        """
        Validate the basic structure of the table data.
        
        Ensures:
        - Data is not empty
        - Contains exactly one table
        - Table value is a list
        
        Raises:
            ValidationError: If structure validation fails
        """
        if not self.table_data:
            raise ValidationError("Request cannot be empty")
        
        if len(self.table_data) != 1:
            raise ValidationError("Request must contain exactly one table")
        
        self.table_name = list(self.table_data.keys())[0]
        self.data = self.table_data[self.table_name]
        
        if not isinstance(self.data, list):
            raise ValidationError(f"Value for '{self.table_name}' must be an array")
    
    def _validate_table_name(self) -> None:
        """
        Validate the table name follows naming conventions.
        
        Table names must contain only alphanumeric characters and underscores
        to prevent SQL injection and ensure compatibility.
        
        Raises:
            ValidationError: If table name is invalid
        """
        if not re.match(TABLE_NAME_PATTERN, self.table_name):
            raise ValidationError(
                f"Invalid table name '{self.table_name}'. "
                "Use only alphanumeric characters and underscores."
            )
    
    def _validate_data_not_empty(self) -> None:
        """
        Validate that data array is not empty.
        
        Raises:
            ValidationError: If data array is empty
        """
        if not self.data:
            raise ValidationError(f"Data array for '{self.table_name}' cannot be empty")
    
    def _validate_columns(self) -> None:
        """
        Validate column names from the first data row.
        
        Ensures:
        - First row is a dictionary
        - All column names follow naming conventions
        
        Raises:
            ValidationError: If column validation fails
        """
        if not self.data:
            return
        
        first_row = self.data[0]
        if not isinstance(first_row, dict):
            raise ValidationError("Each data item must be an object")
        
        self.columns = list(first_row.keys())
        
        for column_name in self.columns:
            if not re.match(COLUMN_NAME_PATTERN, column_name):
                raise ValidationError(
                    f"Invalid column name '{column_name}'. "
                    "Use only alphanumeric characters and underscores."
                )
    
    def _validate_data_consistency(self) -> None:
        """
        Validate that all rows have consistent column structure.
        
        Ensures all rows have the same columns as the first row.
        
        Raises:
            ValidationError: If rows have inconsistent columns
        """
        if not self.data:
            return
        
        expected_columns = set(self.columns)
        
        for idx, row in enumerate(self.data, start=1):
            row_columns = set(row.keys())
            if row_columns != expected_columns:
                missing = expected_columns - row_columns
                extra = row_columns - expected_columns
                error_parts = []
                
                if missing:
                    error_parts.append(f"missing columns: {missing}")
                if extra:
                    error_parts.append(f"extra columns: {extra}")
                
                raise ValidationError(
                    f"Row {idx} has inconsistent columns ({', '.join(error_parts)})"
                )
    
    def _process_table_and_data(self, cursor) -> int:
        """
        Process table creation/update and data insertion.
        
        Args:
            cursor: Database cursor
        
        Returns:
            Number of rows inserted
        
        Raises:
            ValidationError: If column mismatch occurs with existing table
        """
        if self._table_exists(cursor):
            self._handle_existing_table(cursor)
        else:
            self._create_new_table(cursor)
        
        return self._insert_data(cursor)
    
    def _table_exists(self, cursor) -> bool:
        """
        Check if a table exists in the database.
        
        Args:
            cursor: Database cursor
        
        Returns:
            True if table exists, False otherwise
        """
        cursor.execute("SELECT to_regclass(%s)", [self.table_name])
        result = cursor.fetchone()[0]
        return result is not None
    
    def _get_existing_columns(self, cursor) -> Set[str]:
        """
        Get the set of existing columns for a table.
        
        Args:
            cursor: Database cursor
        
        Returns:
            Set of column names (excluding the primary key)
        """
        cursor.execute(
            """
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = %s AND column_name != %s
            """,
            [self.table_name, PRIMARY_KEY_COLUMN]
        )
        return {row[0] for row in cursor.fetchall()}
    
    def _handle_existing_table(self, cursor) -> None:
        """
        Handle operations for an existing table.
        
        Checks for column compatibility and adds missing columns if needed.
        
        Args:
            cursor: Database cursor
        
        Raises:
            ValidationError: If no columns match existing table
        """
        existing_columns = self._get_existing_columns(cursor)
        new_columns = set(self.columns)
        shared_columns = existing_columns.intersection(new_columns)
        
        if not shared_columns:
            raise ValidationError(
                f"Column mismatch. No shared columns with existing table '{self.table_name}'. "
                f"Existing columns: {sorted(existing_columns)}, "
                f"New columns: {sorted(new_columns)}"
            )
        
        missing_columns = new_columns - existing_columns
        if missing_columns:
            logger.info(
                f"Adding {len(missing_columns)} new columns to table '{self.table_name}': "
                f"{sorted(missing_columns)}"
            )
            self._add_missing_columns(cursor, missing_columns)
    
    def _add_missing_columns(self, cursor, columns: Set[str]) -> None:
        """
        Adds new columns to an existing table dynamically using ALTER TABLE.

        Iterates through the provided set of column names and executes a separate
        ALTER TABLE statement for each one. All new columns are created with the
        default type (TEXT).

        Args:
            cursor: Active database cursor to execute queries.
            columns: A set of column names (strings) that need to be added.

        Example:
            If self.table_name is "users" and columns is {"email"}, this method
            executes:
            ALTER TABLE "users" ADD COLUMN "email" TEXT
        """
        for column_name in columns:
            query = sql.SQL("ALTER TABLE {} ADD COLUMN {} {}").format(
                sql.Identifier(self.table_name),
                sql.Identifier(column_name),
                sql.SQL(DEFAULT_COLUMN_TYPE)
            )
            cursor.execute(query)
            logger.debug(f"Added column '{column_name}' to table '{self.table_name}'")
    
    def _create_new_table(self, cursor) -> None:
        """
        Creates a new dynamic table with a serial primary key and specified columns.

        Constructs a CREATE TABLE statement securely. It automatically adds a
        primary key column (defined in PRIMARY_KEY_COLUMN) as SERIAL. All user-
        defined columns are set to the default type (TEXT).

        Args:
            cursor: Active database cursor to execute queries.

        Example:
            If self.table_name is "products" and self.columns is ["name", "price"],
            this method executes:
            CREATE TABLE "products" (
                "id" SERIAL PRIMARY KEY,
                "name" TEXT,
                "price" TEXT
            )
        """
        # Build column definitions
        column_definitions = [
            sql.SQL("{} {}").format(
                sql.Identifier(col),
                sql.SQL(DEFAULT_COLUMN_TYPE)
            )
            for col in self.columns
        ]
        
        # Create table with primary key and columns
        query = sql.SQL("CREATE TABLE {} ({} SERIAL PRIMARY KEY, {})").format(
            sql.Identifier(self.table_name),
            sql.Identifier(PRIMARY_KEY_COLUMN),
            sql.SQL(", ").join(column_definitions)
        )
        
        cursor.execute(query)
        logger.info(f"Created new table '{self.table_name}' with columns: {self.columns}")
    
    def _insert_data(self, cursor) -> int:
        """
        Inserts rows into the table using secure parameterized queries.

        Constructs an INSERT statement for each row in self.data. It dynamically
        builds the list of columns and value placeholders (%s) based on the keys
        present in each row dictionary.
        
        Note: Uses sql.Placeholder() to separate the SQL structure from data,
        preventing SQL injection.

        Args:
            cursor: Active database cursor to execute queries.

        Returns:
            int: The total number of rows successfully inserted.

        Example:
            If self.table_name is "users" and a row is {"name": "Alice"},
            this method executes:
            INSERT INTO "users" ("name") VALUES ('Alice')
        """
        rows_inserted = 0
        
        for row in self.data:
            column_names = list(row.keys())
            values = [row[col] for col in column_names]
            
            # Build parameterized INSERT query
            query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                sql.Identifier(self.table_name),
                sql.SQL(", ").join(sql.Identifier(col) for col in column_names),
                sql.SQL(", ").join(sql.Placeholder() * len(values))
            )
            
            cursor.execute(query, values)
            rows_inserted += 1
        
        logger.debug(f"Inserted {rows_inserted} rows into table '{self.table_name}'")
        return rows_inserted
    
    def _notify_postgrest(self, cursor) -> None:
        """
        Sends a NOTIFY command to the database to refresh the PostgREST schema cache.

        This is required because PostgREST caches the database schema for performance.
        When we create or alter tables, we must signal PostgREST to reload its
        definitions so the new endpoints become available immediately.

        Args:
            cursor: Active database cursor to execute queries.

        Example:
            Executes the raw SQL command:
            NOTIFY "pgrst"
        """
        cursor.execute(
            sql.SQL("NOTIFY {}").format(sql.Identifier(POSTGREST_NOTIFICATION_CHANNEL))
        )
        logger.debug("Sent NOTIFY to PostgREST")
