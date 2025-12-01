from ninja import NinjaAPI, Schema
from typing import List, Dict, Any
from django.conf import settings
from django.core.exceptions import ValidationError
from pydantic import Field, RootModel

from .services import DatasetService


api = NinjaAPI(
    title="Santo Request API",
    version="1.0.0",
    description="API dinámica para crear tablas en PostgreSQL y servirlas via PostgREST"
)

class CreateDatasetRequest(RootModel[Dict[str, List[Dict[str, Any]]]]):
    """
    Solicitud para crear un dataset dinámico.
    
    El JSON debe tener una única clave que será el nombre de la tabla,
    y el valor debe ser un array de objetos con los datos.
    
    ```json
    {
        "nombre_tabla": [
            {"col1": "val1", "col2": "val2"},
            {"col1": "val3", "col2": "val4"}
        ]
    }
    ```
    
    **Ejemplo:**
    ```json
    {
        "tutifruti": [
            {"letra": "A", "comida": "Asado", "pais": "Argentina", "nombre": "Alejandro", "animal": "Ardilla"},
            {"letra": "B", "comida": "Brocoli", "pais": "Brasil", "nombre": "Bruno", "animal": "Búfalo"},
            {"letra": "C", "comida": "Canelones", "pais": "Chile", "nombre": "Carla", "animal": "Cocodrilo"}
        ]
    }
    ```
    
    **La clave principal del JSON será el nombre de la tabla.**
    **Los objetos del array definen las columnas y datos.**
    
    **Validaciones:**
    - Debe contener exactamente una tabla
    - Nombre de tabla: Solo alfanumérico y guiones bajos `[a-zA-Z0-9_]`
    - Nombres de columnas: Solo alfanumérico y guiones bajos `[a-zA-Z0-9_]`
    - Los datos no pueden estar vacíos
    - Todas las filas deben tener las mismas columnas
    """
    
    model_config = {
        "json_schema_extra": {
            "example": {
                "tutifruti": [
                    {"letra": "A", "comida": "Asado", "pais": "Argentina", "nombre": "Alejandro", "animal": "Ardilla"},
                    {"letra": "B", "comida": "Brocoli", "pais": "Brasil", "nombre": "Bruno", "animal": "Búfalo"},
                    {"letra": "C", "comida": "Canelones", "pais": "Chile", "nombre": "Carla", "animal": "Cocodrilo"}
                ]
            }
        }
    }


class CreateDatasetResponse(Schema):
    """Respuesta exitosa al crear un dataset."""
    message: str = Field(..., description="Mensaje de confirmación")
    url: str = Field(
        ..., 
        description="URL del endpoint PostgREST para acceder a los datos",
        example="http://localhost:3000/tutifruti"
    )
    table_name: str = Field(..., description="Nombre de la tabla creada")
    rows_inserted: int = Field(..., description="Número de filas insertadas")


class ErrorResponse(Schema):
    """Respuesta de error."""
    error: str = Field(..., description="Descripción del error")


@api.post(
    "/create-dataset",
    response={200: CreateDatasetResponse, 400: ErrorResponse, 500: ErrorResponse},
    summary="Crear Dataset Dinámico",
    description="""
    Crea una tabla dinámica en PostgreSQL basada en la estructura del JSON recibido.
    
    Formato del Request (según Challenge):
       
    El JSON debe tener una única clave que será el nombre de la tabla, y el valor debe ser un array de objetos con los datos:
    
    ```json
    {
        "nombre_tabla": [
            {"columna1": "valor1", "columna2": "valor2"},
            {"columna1": "valor3", "columna2": "valor4"}
        ]
    }
    ```
    
    Comportamiento:
    
    - Tabla nueva: Se crea con las columnas del JSON
    - Tabla existente con mismas columnas: Los datos se agregan (append)
    - Tabla existente con columnas nuevas: Se ejecuta ALTER TABLE automáticamente
    - Sin columnas coincidentes: Se rechaza con error 400
    
    Validaciones Automáticas:
    
    - Debe contener exactamente una tabla por request
    - Nombre de tabla: Solo alfanumérico y guiones bajos [a-zA-Z0-9_]
    - Nombres de columnas: Solo alfanumérico y guiones bajos [a-zA-Z0-9_]
    - Data no vacía: Debe contener al menos un registro
    
    Resultado:
    
    Al crear la tabla, PostgREST la expone automáticamente como una API REST completa con:
    
    - GET /tabla - Listar registros
    - POST /tabla - Insertar registros
    - PATCH /tabla?id=eq.X - Actualizar
    - DELETE /tabla?id=eq.X - Eliminar
    - Filtros: ?columna=eq.valor, ?columna=like.texto*, etc.
    """,
    tags=["Dataset Management"]
)
def create_dataset(request, payload: CreateDatasetRequest):
    """
    Create or update dynamic datasets in PostgreSQL.

    Accept the format specified in the Challenge where the JSON key is
    the table name and the value is the data array.

    Delegate all business logic to DatasetService.
    """
    try:
        # Initialize service with the request data
        service = DatasetService(payload.root)
        
        # Execute business logic
        table_name, rows_inserted = service.create_or_update_dataset()
        
        # Get PostgREST URL from settings or use default
        postgrest_url = getattr(settings, 'POSTGREST_URL', 'http://localhost:3000')
        
        # Return success response
        return 200, CreateDatasetResponse(
            message="Dataset created successfully",
            url=f"{postgrest_url}/{table_name}",
            table_name=table_name,
            rows_inserted=rows_inserted
        )
    
    except ValidationError as e:
        # Handle validation errors (400 Bad Request)
        return 400, ErrorResponse(error=str(e))
    
    except Exception as e:
        # Handle unexpected errors (500 Internal Server Error)
        return 500, ErrorResponse(error=f"Internal server error: {str(e)}")

