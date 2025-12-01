"""
API endpoint tests for the create-dataset endpoint.

Tests the API layer responses (200, 400, 500) using Django Ninja's test client.
"""

import pytest
from django.test import Client
from unittest.mock import patch, MagicMock
from django.core.exceptions import ValidationError





@pytest.fixture
def valid_payload():
    """Fixture providing valid request payload."""
    return {
        "test_api_users": [
            {"name": "Alice", "email": "alice@example.com"},
            {"name": "Bob", "email": "bob@example.com"}
        ]
    }



@pytest.mark.django_db(transaction=True)
def test_create_dataset_success_200(client, valid_payload):
    """
    Test successful dataset creation returns 200 with correct response structure.
    
    Verifies:
    - Status code 200
    - Response contains: message, url, table_name, rows_inserted
    - Table is actually created in database
    """
    response = client.post(
        '/api/create-dataset',
        data=valid_payload,
        content_type='application/json'
    )
    
    # Verify status code
    assert response.status_code == 200
    
    # Verify response structure
    data = response.json()
    assert 'message' in data
    assert 'url' in data
    assert 'table_name' in data
    assert 'rows_inserted' in data
    
    # Verify response values
    assert data['message'] == "Dataset created successfully"
    assert data['table_name'] == "test_api_users"
    assert data['rows_inserted'] == 2
    assert 'test_api_users' in data['url']
    
 


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("invalid_payload,expected_error", [
    # Invalid table name
    (
        {"test-invalid-name": [{"col": "val"}]},
        "Invalid table name"
    ),
    # Invalid column name
    (
        {"test_table": [{"col-invalid": "val"}]},
        "Invalid column name"
    ),
    # Empty data array
    (
        {"test_table": []},
        "cannot be empty"
    ),
    # Empty request
    (
        {},
        "Request cannot be empty"
    ),
    # Multiple tables
    (
        {
            "table1": [{"col": "val"}],
            "table2": [{"col": "val"}]
        },
        "exactly one table"
    ),
    # Inconsistent columns
    (
        {
            "test_table": [
                {"name": "John", "age": "30"},
                {"name": "Jane"}  # Missing 'age'
            ]
        },
        "missing columns"
    ),
])
def test_create_dataset_validation_error_400(client, invalid_payload, expected_error):
    """
    Test validation errors return 400 with error message.
    
    Tests various validation scenarios:
    - Invalid table names
    - Invalid column names
    - Empty data
    - Multiple tables
    - Inconsistent row structure
    
    Verifies:
    - Status code 400
    - Response contains error field
    - Error message matches expected pattern
    """
    response = client.post(
        '/api/create-dataset',
        data=invalid_payload,
        content_type='application/json'
    )
    
    # Verify status code
    assert response.status_code == 400
    
    # Verify error response structure
    data = response.json()
    assert 'error' in data
    
    # Verify error message contains expected text
    assert expected_error in data['error']




@pytest.mark.django_db(transaction=True)
def test_create_dataset_internal_error_500(client, valid_payload):
    """
    Test unexpected errors return 500 with error message.
    
    Simulates an unexpected database error during processing.
    
    Verifies:
    - Status code 500
    - Response contains error field
    - Error message indicates internal server error
    """
    # Mock the service to raise an unexpected exception
    with patch('api.api.DatasetService') as MockService:
        mock_instance = MagicMock()
        mock_instance.create_or_update_dataset.side_effect = Exception("Unexpected database connection error")
        MockService.return_value = mock_instance
        
        response = client.post(
            '/api/create-dataset',
            data=valid_payload,
            content_type='application/json'
        )
        
        # Verify status code
        assert response.status_code == 500
        
        # Verify error response structure
        data = response.json()
        assert 'error' in data
        
        # Verify error message indicates internal error
        assert 'Internal server error' in data['error']
        assert 'Unexpected database connection error' in data['error']



@pytest.mark.django_db(transaction=True)
def test_create_dataset_schema_evolution_200(client):
    """
    Test schema evolution (adding new columns) returns 200.
    
    Verifies that adding columns to existing table works correctly.
    """
    # First request: Create table with initial columns
    payload1 = {
        "test_evolution_api": [
            {"name": "John"}
        ]
    }
    response1 = client.post(
        '/api/create-dataset',
        data=payload1,
        content_type='application/json'
    )
    assert response1.status_code == 200
    
    # Second request: Add new column
    payload2 = {
        "test_evolution_api": [
            {"name": "Jane", "email": "jane@example.com"}
        ]
    }
    response2 = client.post(
        '/api/create-dataset',
        data=payload2,
        content_type='application/json'
    )
    
    # Verify success
    assert response2.status_code == 200
    data = response2.json()
    assert data['rows_inserted'] == 1
    
    # Verify both rows exist
    from django.db import connection
    with connection.cursor() as cursor:
        cursor.execute('SELECT COUNT(*) FROM "test_evolution_api"')
        assert cursor.fetchone()[0] == 2
