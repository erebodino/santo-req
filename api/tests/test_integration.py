
import pytest
from django.core.exceptions import ValidationError
from django.db import connection

from api.services import DatasetService

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


@pytest.mark.django_db(transaction=True)
def test_full_workflow_new_table():
    """Test complete workflow for creating a new table."""
    data = {
        "test_workflow": [
            {"letra": "A", "comida": "Asado", "pais": "Argentina"},
            {"letra": "B", "comida": "Brocoli", "pais": "Brasil"},
            {"letra": "C", "comida": "Canelones", "pais": "Chile"}
        ]
    }
    service = DatasetService(data)
    table_name, rows_inserted = service.create_or_update_dataset()
    
    assert table_name == "test_workflow"
    assert rows_inserted == 3
    
    with connection.cursor() as cursor:
        cursor.execute('SELECT letra, comida, pais FROM "test_workflow" ORDER BY id')
        rows = cursor.fetchall()
        assert len(rows) == 3
        assert rows[0] == ("A", "Asado", "Argentina")


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("invalid_data,error_match", [
    ({"test-invalid": [{"col": "val"}]}, "Invalid table name"),
    ({"test_table": [{"col-name": "val"}]}, "Invalid column name"),
    ({"test_table": []}, "cannot be empty"),
    ({}, "Request cannot be empty"),
    ({"t1": [{"a": "1"}], "t2": [{"b": "2"}]}, "exactly one table"),
])
def test_validation_errors(invalid_data, error_match):
    """Test that various validation errors are properly raised."""
    service = DatasetService(invalid_data)
    
    with pytest.raises(ValidationError, match=error_match):
        service.create_or_update_dataset()


@pytest.mark.django_db(transaction=True)
def test_real_world_scenario_tutifruti(tutifruti_data):
    """Test with the real-world example from the challenge."""
    service = DatasetService(tutifruti_data)
    table_name, rows_inserted = service.create_or_update_dataset()
    
    assert table_name == "tutifruti"
    assert rows_inserted == 3
    
    with connection.cursor() as cursor:
        # Verify table structure
        cursor.execute("""
            SELECT column_name FROM information_schema.columns 
            WHERE table_name = 'tutifruti' 
            AND column_name != 'id'
            ORDER BY column_name
        """)
        columns = [row[0] for row in cursor.fetchall()]
        assert set(columns) == {"letra", "comida", "pais", "nombre", "animal"}
        
        # Verify data count
        cursor.execute('SELECT COUNT(*) FROM tutifruti')
        assert cursor.fetchone()[0] == 3
        
        # Verify specific row
        cursor.execute('SELECT * FROM tutifruti WHERE letra = %s', ['A'])
        row = cursor.fetchone()
        assert row[1] == 'A'  # letra (skipping id at index 0)


@pytest.mark.django_db(transaction=True)
def test_multiple_schema_evolutions():
    """Test multiple rounds of schema evolution."""
    # Round 1: Create table
    data1 = {"test_multi_evo": [{"col1": "val1"}]}
    DatasetService(data1).create_or_update_dataset()
    
    # Round 2: Add col2
    data2 = {"test_multi_evo": [{"col1": "val1", "col2": "val2"}]}
    DatasetService(data2).create_or_update_dataset()
    
    # Round 3: Add col3
    data3 = {"test_multi_evo": [{"col1": "val1", "col2": "val2", "col3": "val3"}]}
    DatasetService(data3).create_or_update_dataset()
    
    # Verify final schema
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT column_name FROM information_schema.columns 
            WHERE table_name = 'test_multi_evo' 
            AND column_name != 'id'
            ORDER BY column_name
        """)
        columns = [row[0] for row in cursor.fetchall()]
        assert set(columns) == {"col1", "col2", "col3"}
        
        # Verify we have 3 rows
        cursor.execute('SELECT COUNT(*) FROM test_multi_evo')
        assert cursor.fetchone()[0] == 3
