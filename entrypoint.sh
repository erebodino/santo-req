#!/bin/bash
set -e

echo "Esperando a que PostgreSQL esté disponible..."

# Esperar a que Postgres esté listo
until PGPASSWORD=$DB_PASSWORD psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" -c '\q' 2>/dev/null; do
  >&2 echo "Postgres no está disponible - esperando..."
  sleep 1
done

>&2 echo "PostgreSQL está disponible - ejecutando migraciones..."

# Ejecutar migraciones
python manage.py migrate --noinput

>&2 echo "Iniciando servidor Django..."

# Iniciar el servidor
exec python manage.py runserver 0.0.0.0:8000
