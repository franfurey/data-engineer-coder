# Start by pulling the official Airflow image
FROM apache/airflow:2.3.3
RUN python --version
# Establecer la variable de entorno para evitar la dependencia GPL
ENV SLUGIFY_USES_TEXT_UNIDECODE yes

# Cambia al usuario airflow para instalar las dependencias
USER airflow

# Instala las dependencias
COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install --user --no-cache-dir -r requirements.txt --verbose
