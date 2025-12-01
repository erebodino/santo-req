"""
Comprehensive test suite for DatasetService using pytest best practices.

Tests use fixtures, parametrization, and pytest markers for clean, maintainable tests.
"""

import pytest
from django.core.exceptions import ValidationError
from django.db import connection

from api.services import DatasetService



@pytest.fixture
def valid_table_data():
    """Fixture providing valid table data."""
    return {
        "test_table": [
            {"name": "John", "age": "30"},
            {"name": "Jane", "age": "25"}
        ]
    }


@pytest.fixture
def tutifruti_data():
    """Fixture providing the challenge example data."""
    return {
        "tutifruti": [
            {"letra": "A", "comida": "Asado", "pais": "Argentina", "nombre": "Alejandro", "animal": "Ardilla"},
            {"letra": "B", "comida": "Brocoli", "pais": "Brasil", "nombre": "Bruno", "animal": "Búfalo"},
            {"letra": "C", "comida": "Canelones", "pais": "Chile", "nombre": "Carla", "animal": "Cocodrilo"}
        ]
    }


@pytest.fixture
def cleanup_test_tables():
    """Fixture to clean up test tables after each test."""
    yield
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT tablename FROM pg_tables 
            WHERE schemaname = 'public' 
            AND tablename LIKE 'test_%'
        """)
        for row in cursor.fetchall():
            cursor.execute(f'DROP TABLE IF EXISTS "{row[0]}" CASCADE')


# ===== VALIDATION TESTS =====

def test_validate_structure_success(valid_table_data):
    """Test successful structure validation with valid data."""
    service = DatasetService(valid_table_data)
    service._validate_structure()
    
    assert service.table_name == "test_table"
    assert len(service.data) == 2


def test_validate_structure_empty_data():
    """Test validation fails with empty request."""
    service = DatasetService({})
    
    with pytest.raises(ValidationError, match="Request cannot be empty"):
        service._validate_structure()


def test_validate_structure_multiple_tables():
    """Test validation fails with multiple tables."""
    data = {
        "table1": [{"col": "val"}],
        "table2": [{"col": "val"}]
    }
    service = DatasetService(data)
    
    with pytest.raises(ValidationError, match="must contain exactly one table"):
        service._validate_structure()


def test_validate_structure_not_list():
    """Test validation fails when table value is not a list."""
    data = {"test_table": "not a list"}
    service = DatasetService(data)
    
    with pytest.raises(ValidationError, match="must be an array"):
        service._validate_structure()


@pytest.mark.parametrize("table_name", [
    "users",
    "user_data",
    "Data123",
    "TABLE_1",
    "_leading_underscore",
    "MixedCase_123"
])
def test_validate_table_name_valid(table_name):
    """Test table name validation with valid names."""
    data = {table_name: [{"col": "val"}]}
    service = DatasetService(data)
    service._validate_structure()
    service._validate_table_name()  # Should not raise


@pytest.mark.parametrize("table_name,reason", [
    ("table-name", "hyphen"),
    ("table name", "space"),
    ("table.name", "dot"),
    ("table@name", "special char"),
    ("table;DROP", "SQL injection attempt"),
    ("table()", "parentheses"),
    ("table*", "asterisk"),
])
def test_validate_table_name_invalid(table_name, reason):
    """Test table name validation fails with invalid characters."""
    data = {table_name: [{"col": "val"}]}
    service = DatasetService(data)
    service._validate_structure()
    
    with pytest.raises(ValidationError, match="Invalid table name"):
        service._validate_table_name()


def test_validate_data_not_empty_success(valid_table_data):
    """Test data not empty validation with valid data."""
    service = DatasetService(valid_table_data)
    service._validate_structure()
    service._validate_data_not_empty()  # Should not raise


def test_validate_data_not_empty_fails():
    """Test validation fails with empty data array."""
    data = {"test_table": []}
    service = DatasetService(data)
    service._validate_structure()
    
    with pytest.raises(ValidationError, match="cannot be empty"):
        service._validate_data_not_empty()


@pytest.mark.parametrize("columns", [
    {"name": "John", "age_years": "30", "City123": "NYC"},
    {"col1": "val"},
    {"_underscore": "val", "num123": "val"},
    {"UPPERCASE": "val", "lowercase": "val"},
])
def test_validate_columns_valid(columns):
    """Test column validation with valid column names."""
    data = {"test_table": [columns]}
    service = DatasetService(data)
    service._validate_structure()
    service._validate_columns()  # Should not raise


@pytest.mark.parametrize("columns,reason", [
    ({"col-name": "val"}, "hyphen"),
    ({"col name": "val"}, "space"),
    ({"col.name": "val"}, "dot"),
    ({"col@name": "val"}, "special char"),
    ({"col*": "val"}, "asterisk"),
    ({"col(1)": "val"}, "parentheses"),
])
def test_validate_columns_invalid(columns, reason):
    """Test column validation fails with invalid characters."""
    data = {"test_table": [columns]}
    service = DatasetService(data)
    service._validate_structure()
    
    with pytest.raises(ValidationError, match="Invalid column name"):
        service._validate_columns()


def test_validate_columns_not_dict():
    """Test column validation fails when row is not a dictionary."""
    data = {"test_table": ["string_value"]}
    service = DatasetService(data)
    service._validate_structure()
    
    with pytest.raises(ValidationError, match="must be an object"):
        service._validate_columns()


def test_validate_data_consistency_success(valid_table_data):
    """Test data consistency validation with consistent rows."""
    service = DatasetService(valid_table_data)
    service._validate_structure()
    service._validate_columns()
    service._validate_data_consistency()  # Should not raise


def test_validate_data_consistency_missing_columns():
    """Test validation fails when rows have missing columns."""
    data = {
        "test_table": [
            {"name": "John", "age": "30"},
            {"name": "Jane"}  # Missing 'age'
        ]
    }
    service = DatasetService(data)
    service._validate_structure()
    service._validate_columns()
    
    with pytest.raises(ValidationError, match="missing columns"):
        service._validate_data_consistency()


def test_validate_data_consistency_extra_columns():
    """Test validation fails when rows have extra columns."""
    data = {
        "test_table": [
            {"name": "John", "age": "30"},
            {"name": "Jane", "age": "25", "city": "NYC"}  # Extra 'city'
        ]
    }
    service = DatasetService(data)
    service._validate_structure()
    service._validate_columns()
    
    with pytest.raises(ValidationError, match="extra columns"):
        service._validate_data_consistency()


def test_validate_data_consistency_completely_different():
    """Test validation fails when rows have completely different columns."""
    data = {
        "test_table": [
            {"name": "John", "age": "30"},
            {"city": "NYC", "country": "USA"}  # All different
        ]
    }
    service = DatasetService(data)
    service._validate_structure()
    service._validate_columns()
    
    with pytest.raises(ValidationError, match="Row 2 has inconsistent columns"):
        service._validate_data_consistency()


# ===== DATABASE OPERATION TESTS =====

@pytest.mark.django_db(transaction=True)
def test_create_new_table_success():
    """Test successful creation of a new table."""
    data = {
        "test_users": [
            {"name": "John", "email": "john@example.com"},
            {"name": "Jane", "email": "jane@example.com"}
        ]
    }
    service = DatasetService(data)
    table_name, rows_inserted = service.create_or_update_dataset()
    
    assert table_name == "test_users"
    assert rows_inserted == 2
    
    # Verify table exists and has correct data
    with connection.cursor() as cursor:
        cursor.execute("SELECT to_regclass(%s)", ["test_users"])
        assert cursor.fetchone()[0] is not None
        
        cursor.execute('SELECT name, email FROM "test_users" ORDER BY id')
        rows = cursor.fetchall()
        assert len(rows) == 2
        assert rows[0] == ("John", "john@example.com")
        assert rows[1] == ("Jane", "jane@example.com")


@pytest.mark.django_db(transaction=True)
def test_create_table_with_primary_key(cleanup_test_tables):
    """Test that created table has auto-increment primary key."""
    data = {"test_pk_table": [{"name": "Test"}]}
    service = DatasetService(data)
    service.create_or_update_dataset()
    
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'test_pk_table' AND column_name = 'id'
        """)
        result = cursor.fetchone()
        assert result is not None
        assert result[0] == "id"
        assert result[1] == "integer"


@pytest.mark.django_db(transaction=True)
def test_append_to_existing_table(cleanup_test_tables):
    """Test appending data to existing table with same columns."""
    # First insert
    data1 = {"test_append": [{"name": "John", "age": "30"}]}
    service1 = DatasetService(data1)
    service1.create_or_update_dataset()
    
    # Second insert (append)
    data2 = {"test_append": [{"name": "Jane", "age": "25"}]}
    service2 = DatasetService(data2)
    table_name, rows_inserted = service2.create_or_update_dataset()
    
    assert rows_inserted == 1
    
    # Verify both rows exist
    with connection.cursor() as cursor:
        cursor.execute('SELECT COUNT(*) FROM "test_append"')
        count = cursor.fetchone()[0]
        assert count == 2


@pytest.mark.django_db(transaction=True)
def test_schema_evolution_add_new_columns(cleanup_test_tables):
    """Test adding new columns to existing table (schema evolution)."""
    # Create table with initial columns
    data1 = {"test_evolution": [{"name": "John"}]}
    service1 = DatasetService(data1)
    service1.create_or_update_dataset()
    
    # Add data with new column
    data2 = {"test_evolution": [{"name": "Jane", "email": "jane@example.com"}]}
    service2 = DatasetService(data2)
    table_name, rows_inserted = service2.create_or_update_dataset()
    
    assert rows_inserted == 1
    
    # Verify new column was added
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT column_name FROM information_schema.columns 
            WHERE table_name = 'test_evolution' 
            AND column_name = 'email'
        """)
        assert cursor.fetchone() is not None
        
        # Verify data (old row has NULL for new column)
        cursor.execute('SELECT name, email FROM "test_evolution" ORDER BY id')
        rows = cursor.fetchall()
        assert len(rows) == 2
        assert rows[0] == ("John", None)
        assert rows[1] == ("Jane", "jane@example.com")


@pytest.mark.django_db(transaction=True)
def test_column_mismatch_no_shared_columns(cleanup_test_tables):
    """Test error when no columns match existing table."""
    # Create table
    data1 = {"test_mismatch": [{"name": "John"}]}
    service1 = DatasetService(data1)
    service1.create_or_update_dataset()
    
    # Try to insert with completely different columns
    data2 = {"test_mismatch": [{"email": "test@example.com"}]}
    service2 = DatasetService(data2)
    
    with pytest.raises(ValidationError, match="No shared columns"):
        service2.create_or_update_dataset()


@pytest.mark.django_db(transaction=True)
def test_transaction_rollback_on_error(cleanup_test_tables):
    """Test that transaction rolls back on validation error."""
    # Create a table first
    data1 = {"test_rollback": [{"name": "John"}]}
    service1 = DatasetService(data1)
    service1.create_or_update_dataset()
    
    # Try to insert invalid data (inconsistent columns)
    data2 = {
        "test_rollback": [
            {"name": "Jane"},
            {"email": "test@example.com"}  # Different columns
        ]
    }
    service2 = DatasetService(data2)
    
    with pytest.raises(ValidationError):
        service2.create_or_update_dataset()
    
    # Verify original data is unchanged
    with connection.cursor() as cursor:
        cursor.execute('SELECT COUNT(*) FROM "test_rollback"')
        count = cursor.fetchone()[0]
        assert count == 1


@pytest.mark.django_db(transaction=True)
def test_insert_multiple_rows(cleanup_test_tables):
    """Test inserting multiple rows in a single operation."""
    data = {
        "test_multi": [
            {"id_external": "1", "value": "A"},
            {"id_external": "2", "value": "B"},
            {"id_external": "3", "value": "C"},
            {"id_external": "4", "value": "D"},
            {"id_external": "5", "value": "E"},
            {"id_external": "6", "value": "F"},
            {"id_external": "7", "value": "G"},
            {"id_external": "8", "value": "H"},
            {"id_external": "9", "value": "I"},
            {"id_external": "10", "value": "J"},
        ]
    }
    service = DatasetService(data)
    table_name, rows_inserted = service.create_or_update_dataset()
    
    assert rows_inserted == 10
    
    with connection.cursor() as cursor:
        cursor.execute('SELECT COUNT(*) FROM "test_multi"')
        assert cursor.fetchone()[0] == 10


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("special_data,expected_name", [
    ({"name": "O'Brien", "description": 'Quote: "Hello"'}, "O'Brien"),
    ({"name": "Müller", "description": "UTF-8: ñ, é, ü"}, "Müller"),
    ({"name": "Test\nNewline", "description": "Tab\there"}, "Test\nNewline"),
    ({"name": "", "value": ""}, ""),  # Empty strings
])
def test_special_characters_in_data(special_data, expected_name, cleanup_test_tables):
    """Test inserting data with special characters (each parametrization is independent)."""
    data = {"test_special": [special_data]}
    service = DatasetService(data)
    table_name, rows_inserted = service.create_or_update_dataset()
    
    # Each parametrized test inserts 1 row
    assert rows_inserted == 1
    
    # Verify the specific data was inserted correctly
    with connection.cursor() as cursor:
        cursor.execute('SELECT name FROM "test_special"')
        result = cursor.fetchone()
        assert result[0] == expected_name


@pytest.mark.django_db(transaction=True)
def test_multiple_rows_with_special_characters(cleanup_test_tables):
    """Test inserting multiple rows with special characters in a single operation."""
    data = {
        "test_special_multi": [
            {"name": "O'Brien", "description": 'Quote: "Hello"'},
            {"name": "Müller", "description": "UTF-8: ñ, é, ü"},
            {"name": "Test\nNewline", "description": "Tab\here"},
            {"name": "", "description": "empty"}
        ]
    }
    service = DatasetService(data)
    table_name, rows_inserted = service.create_or_update_dataset()
    
    # Should insert all 4 rows
    assert rows_inserted == 4
    
    with connection.cursor() as cursor:
        cursor.execute('SELECT COUNT(*) FROM "test_special_multi"')
        assert cursor.fetchone()[0] == 4
        
        # Verify specific special characters were preserved
        cursor.execute('SELECT name FROM "test_special_multi" WHERE name = %s', ["O'Brien"])
        assert cursor.fetchone() is not None


# ===== INTEGRATION TESTS =====