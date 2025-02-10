#!/bin/bash

# Activar el entorno virtual
source /home/ubuntu/myenv/bin/activate

# Ir al directorio donde está el script (opcional, si es necesario)
cd /mnt/thingsboard_data/Descargas/

# Ejecutar el script
python3 descargar_v0.py download

# Desactivar el entorno virtual
deactivate
