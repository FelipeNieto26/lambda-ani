# Division Archivos ANI

Sistema de división de archivos para ANI utilizando AWS Lambda y S3.

Este proyecto implementa un servicio Lambda que permite:
- Obtener archivos CSV desde un bucket S3
- Validar que el archivo tenga 1 millón de registros
- Dividir automáticamente el archivo en chunks de 10,000 registros
- Subir los archivos divididos a una carpeta con UUID único en S3

## Inicio Rápido

### Configuración de Variables de Entorno

Las siguientes variables de entorno son configurables:

**Configuración de S3:**
- `AWS_REGION`: Región de AWS (default: `us-east-1`)
- `AWS_S3_BUCKET_NAME`: Nombre del bucket S3 (default: `ani-pruebas`)
- `AWS_S3_FILE_NAME`: Nombre del archivo a procesar (default: `archivos-prueba.csv`)
- `AWS_S3_DEFAULT_PREFIX`: Directorio de entrada (default: `archivo/`)
- `AWS_S3_OUTPUT_PREFIX`: Directorio de salida (default: `output/`)

**Configuración de División:**
- `EXPECTED_RECORDS`: Número esperado de registros (default: `1000000`)
- `RECORDS_PER_CHUNK`: Registros por archivo (default: `10000`)

### Uso de la API

El handler expone un endpoint GET que busca el archivo configurado en `AWS_S3_FILE_NAME` (por defecto: `archivos-prueba.csv`).

**Uso por defecto** (busca en `s3://ani-pruebas/archivo/archivos-prueba.csv`):
```
GET /
```

**Especificar un directorio diferente**:
```
GET /?prefix=otra/ruta/
```

El nombre del archivo y el directorio por defecto son configurables mediante variables de entorno.

### Respuesta de la API

**Cuando la división es exitosa:**
```json
{
  "success": true,
  "message": "División completada exitosamente. 100 archivos generados en output/550e8400-e29b-41d4-a716-446655440000/",
  "originalFileKey": "archivo/archivos-prueba.csv",
  "totalRecords": 1000000,
  "expectedRecords": 1000000,
  "validationPassed": true,
  "outputFolder": "output/550e8400-e29b-41d4-a716-446655440000/",
  "totalChunks": 100,
  "recordsPerChunk": 10000,
  "generatedFiles": [
    "output/550e8400-e29b-41d4-a716-446655440000/chunk_0001.csv",
    "output/550e8400-e29b-41d4-a716-446655440000/chunk_0002.csv",
    "..."
  ],
  "generatedFilesUrls": [
    "s3://ani-pruebas/output/550e8400-e29b-41d4-a716-446655440000/chunk_0001.csv",
    "s3://ani-pruebas/output/550e8400-e29b-41d4-a716-446655440000/chunk_0002.csv",
    "..."
  ],
  "processingTimeMs": 45230
}
```

**Cuando la validación falla:**
```json
{
  "success": false,
  "message": "Validación fallida: Se encontraron 950000 registros, se esperaban 1000000",
  "originalFileKey": "archivo/archivos-prueba.csv",
  "totalRecords": 950000,
  "expectedRecords": 1000000,
  "validationPassed": false,
  "processingTimeMs": 5240
}
```

## Arquitectura

```
FileDivisionHandler (@Named("get"))
    ↓ @Inject
FileDivisionService (@ApplicationScoped)
    ↓ @Inject
S3Service (@ApplicationScoped)
    ↓ usa
AWS SDK S3Client
```

### Componentes Principales

- **FileDivisionHandler**: Handler Lambda que expone endpoint GET
- **FileDivisionService**: Servicio de lógica de negocio
- **S3Service**: Servicio de integración con AWS S3

## Más Información

Si quieres aprender más sobre Quarkus, visita: <https://quarkus.io/>.

## Desarrollo

### Ejecutar en modo desarrollo

```bash
mvn quarkus:dev
```

La aplicación estará disponible en http://localhost:8080

### Compilar el proyecto

```bash
mvn clean compile
```

### Build completo

```bash
mvn clean package
```

Esto genera:
- `target/division-archivos-ani-1.0-SNAPSHOT.jar` - JAR principal
- `target/division-archivos-ani-1.0-SNAPSHOT-runner.jar` - JAR ejecutable
- `target/function.zip` - Para deployment en AWS Lambda

### Build nativo

```bash
mvn clean package -Pnative
```

Requiere GraalVM instalado. Para build en contenedor:

```bash
mvn clean package -Pnative -Dquarkus.native.container-build=true
```

## Deployment en AWS

### Usando SAM CLI

```bash
# Build JVM
sam build --template target/sam.jvm.yaml

# Build Native
sam build --template target/sam.native.yaml

# Deploy
sam deploy --guided
```

### Archivos de configuración SAM

- `target/sam.jvm.yaml` - Configuración para runtime Java 17
- `target/sam.native.yaml` - Configuración para runtime nativo

## Tecnologías

- Java 21
- Quarkus 3.28.3
- AWS Lambda
- Amazon S3
- Maven
- SAM CLI

## Referencias

- [Quarkus AWS Lambda Guide](https://quarkus.io/guides/aws-lambda)
- [Quarkus Amazon Services](https://docs.quarkiverse.io/quarkus-amazon-services/)
- [AWS SAM CLI](https://docs.aws.amazon.com/serverless-application-model/)
