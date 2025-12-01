FROM python:3.10-slim

# Establecer variables de entorno
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Instalar dependencias del sistema
RUN apt-get update && apt-get install -y \
    postgresql-client \
    gcc \
    python3-dev \
    musl-dev \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Instalar pipenv
RUN pip install --no-cache-dir pipenv

# Crear directorio de trabajo
WORKDIR /app

# Copiar archivos de dependencias
COPY Pipfile Pipfile.lock ./

# Instalar dependencias de Python
RUN pipenv install --system --deploy --ignore-pipfile

# Copiar el código de la aplicación
COPY . .

# Hacer ejecutable el script de entrada
RUN chmod +x /app/entrypoint.sh

# Exponer el puerto
EXPOSE 8000

# Script de entrada (ejecutar con bash para evitar problemas de permisos)
ENTRYPOINT ["bash", "/app/entrypoint.sh"]
