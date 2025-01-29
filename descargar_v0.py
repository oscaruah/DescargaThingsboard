import requests
import json
import os
import csv
import argparse
from datetime import datetime, timezone
import pytz  # Para manejo de zonas horarias
from tqdm import tqdm
import numpy as np
from scipy.interpolate import interp1d


class ThingsBoardClient:
    def __init__(self, config_file='config.json', token_file='token.json',customer_name=None):
        config_path = '/mnt/thingsboard_data/Descargas/' + config_file  #os.path.abspath(config_file)
        if os.path.exists(config_path):
            tqdm.write(f"Leyendo configuración desde: {os.path.basename(config_file)}")
            with open(config_path, 'r') as file:
                try:
                    config = json.load(file)
                    self.url = config.get('thingsboard_url')
                    self.username = config.get('username')
                    self.password = config.get('password')
                    if not self.url:
                        raise ValueError("La URL de ThingsBoard no está definida en el archivo de configuración.")
                except json.JSONDecodeError as e:
                    tqdm.write(f"Error al decodificar el archivo de configuración: {e}")
                    exit(1)
        else:
            tqdm.write(f"Archivo de configuración no encontrado: {os.path.basename(config_file)}")
            exit(1)

        self.token_file = token_file
        self.customer_name = customer_name #numero parametro para filtrar clientes
        self.token = None
        self._authenticate()

    def _authenticate(self):
        auth_url = f"{self.url}/api/auth/login"
        credentials = {
            "username": self.username,
            "password": self.password
        }
        tqdm.write(f"Autenticando con ThingsBoard...")
        response = requests.post(auth_url, json=credentials)
        if response.status_code == 200:
            self.token = response.json().get('token')
            tqdm.write("Autenticación exitosa. Token recibido.")
            with open(self.token_file, 'w') as file:
                json.dump({'token': self.token}, file)
        else:
            tqdm.write(f"Error al autenticar: {response.status_code} - {response.text}")
            exit(1)

    def _get_headers(self):
        if not self.token:
            self._authenticate()
        return {
            'Content-Type': 'application/json',
            'X-Authorization': f"Bearer {self.token}"
        }

    def get_customers(self):
        customers_url = f"{self.url}/api/customers?pageSize=1000&page=0"
        tqdm.write("Obteniendo clientes...")
        response = requests.get(customers_url, headers=self._get_headers())
        if response.status_code == 200:
            customers = response.json().get('data', [])
            if self.customer_name:
                customers = [customer for customer in customers if customer.get('title') == self.customer_name]
                tqdm.write(f"Clientes filtrados por nombre '{self.customer_name}': {len(customers)} encontrado(s).")
            else:
                tqdm.write(f"Clientes obtenidos: {len(customers)}")
            return customers
        else:
            tqdm.write(f"Error al obtener clientes: {response.status_code} - {response.text}")
            return []



    def get_gateways_for_customer(self, customer_id):
        customer_id_str = customer_id['id'] if isinstance(customer_id, dict) else customer_id
        gateways_url = f"{self.url}/api/customer/{customer_id_str}/devices?pageSize=1000&page=0"
        tqdm.write(f"Obteniendo dispositivos (gateways) para el cliente {customer_id_str}...")

        try:
            response = requests.get(gateways_url, headers=self._get_headers())
            response.raise_for_status()  # Lanza excepción si hay error
            all_devices = response.json().get('data', [])
          
            # Depuración: Mostrar todos los dispositivos obtenidos
        #    tqdm.write(f"Dispositivos obtenidos para cliente {customer_id_str}: {len(all_devices)}")
         #   for device in all_devices:
          #      tqdm.write(f"Dispositivo: {device.get('name')}, tipo: {device.get('type')}, info adicional: {device.get('additionalInfo')}")

            # Filtrar dispositivos marcados como 'gateway'
            gateways = [device for device in all_devices if device.get('additionalInfo', {}).get('gateway', False)]
            tqdm.write(f"Gateways filtrados: {len(gateways)}")
            return gateways
        except requests.exceptions.RequestException as e:
            tqdm.write(f"Error al obtener gateways para el cliente {customer_id_str}: {e}")
            return []

    def get_devices_for_gateway(self, gateway_id, customer_id):
        """
        Obtiene dispositivos asociados al gateway basado en el cliente y el atributo `lastConnectedGateway`.
        """
        customer_id_str = customer_id['id'] if isinstance(customer_id, dict) else customer_id
        devices_url = f"{self.url}/api/customer/{customer_id_str}/devices?pageSize=1000&page=0"
        tqdm.write(f"Obteniendo dispositivos para el cliente {customer_id} y el gateway {gateway_id}...")

        try:
            # Obtener todos los dispositivos asignados al cliente
            response = requests.get(devices_url, headers=self._get_headers())
            response.raise_for_status()
            customer_devices = response.json().get('data', [])

            # Filtrar dispositivos por `lastConnectedGateway`
            associated_devices = [
                device for device in customer_devices
                if device.get('additionalInfo', {}).get('lastConnectedGateway') == gateway_id
            ]
            for device in associated_devices:
                tqdm.write(f"Dispositivo asociado al gateway {gateway_id}: {device.get('name')}") 
            tqdm.write(f"Dispositivos asociados al gateway {gateway_id}: {len(associated_devices)}")
            return associated_devices
        except requests.exceptions.RequestException as e:
            tqdm.write(f"Error al obtener dispositivos para el cliente {customer_id} y gateway {gateway_id}: {e}")
            return []


    def organize_directories(self):
        print("Organizando directorios de clientes, gateways y dispositivos...")
        customers = self.get_customers()
        base_directory = 'thingsboard_data'
        if not os.path.exists(base_directory):
            os.makedirs(base_directory)

        for customer in tqdm(customers, desc="Organizando directorios de clientes"):
            customer_name = customer.get('title')
            customer_id = customer.get('id')
            print(f"Organizando directorio para el cliente: {customer_name}")
            customer_dir = os.path.join(base_directory, customer_name)
            if not os.path.exists(customer_dir):
                os.makedirs(customer_dir)

            customer_file = os.path.join(customer_dir, f"{customer_name}_customer.json")
            with open(customer_file, 'w') as file:
                json.dump(customer, file, indent=4)

            gateways = self.get_gateways_for_customer(customer_id)
            if not gateways:
                print(f"No se encontraron gateways para el cliente {customer_name} ({customer_id})")
            for gateway in gateways:
                gateway_name = gateway.get('name')
                gateway_id = gateway.get('id').get('id')
                print(f"Organizando directorio para el gateway: {gateway_name}")
                gateway_dir = os.path.join(customer_dir, gateway_name)
                if not os.path.exists(gateway_dir):
                    os.makedirs(gateway_dir)

                gateway_file = os.path.join(gateway_dir, f"{gateway_name}_gateway.json")
                with open(gateway_file, 'w') as file:
                    json.dump(gateway, file, indent=4)

                devices = self.get_devices_for_gateway(gateway_id,customer_id)
                if not devices:
                    print(f"No se encontraron dispositivos para el gateway {gateway_name} ({gateway_id})")
                for device in devices:
                    device_name = device.get('name')
                    print(f"Organizando directorio para el dispositivo: {device_name}")
                    device_dir = os.path.join(gateway_dir, device_name)
                    if not os.path.exists(device_dir):
                        os.makedirs(device_dir)
                    
                    device_file = os.path.join(device_dir, f"{device_name}_device.json")
                    with open(device_file, 'w') as file:
                        json.dump(device, file, indent=4)

    def get_telemetry_keys(self, device_id):
        url = f"{self.url}/api/plugins/telemetry/DEVICE/{device_id}/keys/timeseries"
        headers = {
            'Content-Type': 'application/json',
            'X-Authorization': f'Bearer {self.token}'
        }

        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            tqdm.write(f"Error al obtener claves de telemetría: {response.status_code} - {response.text}")
            return []

    def get_last_telemetry_timestamp(self, csv_filename):
        """
        Lee el último timestamp de telemetría de un archivo CSV, lo convierte a la zona horaria local,
        y lo retorna en formato legible.
        """
        if os.path.exists(csv_filename):
            with open(csv_filename, 'r') as csvfile:
                reader = csv.DictReader(csvfile)
                last_row = None
                for last_row in reader:
                    pass  # Leer hasta la última fila
                if last_row:
                    try:
                        # Convertir el último timestamp a datetime en UTC
                        last_timestamp_utc = datetime.strptime(last_row['timestamp'], '%Y-%m-%d %H:%M:%S')
                        last_timestamp_utc = last_timestamp_utc.replace(tzinfo=timezone.utc)

                        # Convertir a la hora local
                        local_tz = pytz.timezone('Europe/Madrid')  # Ajustar según la zona horaria deseada
                        last_timestamp_local = last_timestamp_utc.astimezone(local_tz)

                        return last_timestamp_local.strftime('%Y-%m-%d %H:%M:%S')
                    except ValueError:
                        return "Error al leer el timestamp."
        return "Sin telemetría previa."

    def count_existing_records(self, csv_filename):
        """
        Cuenta el número de registros (timestamps * claves) ya presentes en el archivo CSV.
        """
        if not os.path.exists(csv_filename):
            return 0

        record_count = 0
        with open(csv_filename, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                # Contar el número de claves no vacías por cada timestamp
                record_count += sum(1 for key, value in row.items() if key != 'timestamp' and value)
        return record_count

    def download_telemetries(self):
        tqdm.write("Descargando telemetría de dispositivos...")
        customers = self.get_customers()
        if not customers:
            tdqm.write(f"No se encontraron clientes con el nombre especificado: {self.customer_name}")
            return

        base_directory = 'thingsboard_data'
        report = []  # Lista para almacenar el reporte final de archivos descargados

        for customer in tqdm(customers, desc="Clientes"):
            customer_name = customer.get('title')
            customer_id = customer.get('id')
            customer_dir = os.path.join(base_directory, customer_name)
            tqdm.write(f"Procesando cliente: {customer_name}")

            gateways = self.get_gateways_for_customer(customer_id)
            if not gateways:
                tqdm.write(f"No se encontraron gateways para el cliente {customer_name}")
                continue

            for gateway in tqdm(gateways, desc=f"Gateways de {customer_name}", leave=False):
                gateway_name = gateway.get('name')
                gateway_id = gateway.get('id').get('id')
                gateway_dir = os.path.join(customer_dir, gateway_name)

                devices = self.get_devices_for_gateway(gateway_id,customer_id)
                if not devices:
                    tqdm.write(f"No se encontraron dispositivos para el gateway {gateway_name}")
                    continue
                for device in tqdm(devices, desc=f"Dispositivos de {gateway_name}", leave=False):
                    device_name = device.get('name')
                    device_id = device['id']['id']
                    keys = self.get_telemetry_keys(device_id)

                    device_dir = os.path.join(gateway_dir, device_name)
                    csv_filename = os.path.join(device_dir, f"{device_name}_telemetry.csv")

                    # Contar registros ya existentes en el CSV
                    existing_records = self.count_existing_records(csv_filename)

                    # Mostrar la fecha y hora de la última telemetría almacenada
                    last_telemetry_time = self.get_last_telemetry_timestamp(csv_filename)
                    tqdm.write(f"Última telemetría registrada para '{device_name}'-'{device_id}': {last_telemetry_time}")

                    start_ts = int(datetime(2024, 9, 1).timestamp() * 1000)  # Fecha de inicio predeterminada
                    end_ts = int(datetime.now().timestamp() * 1000)  # Fecha actual como límite

                    if os.path.exists(csv_filename):
                        tqdm.write(f"Archivo de telemetría encontrado: {os.path.basename(csv_filename)}. Leyendo último timestamp.")
                        with open(csv_filename, 'r') as csvfile:
                            reader = csv.DictReader(csvfile)
                            last_row = None
                            for last_row in reader:
                                pass
                            if last_row:
                                try:
                                    last_ts = int(datetime.strptime(last_row['timestamp'], '%Y-%m-%d %H:%M:%S').timestamp() * 1000)
                                    start_ts = last_ts + 1  # Evitar duplicados
                                except ValueError:
                                    tqdm.write(f"Error al leer el último timestamp. Usando fecha de inicio predeterminada.")
                    else:
                        tqdm.write(f"No se encontró archivo previo. Comenzando desde el 1 de septiembre de 2024.")

                    limit = 50000
                    total_records = 0  # Para contar el número de registros (*timestamps * claves*)

                    all_data = {}

                    while True:
                        url = f"{self.url}/api/plugins/telemetry/DEVICE/{device_id}/values/timeseries?limit={limit}&startTs={start_ts}&endTs={end_ts}&keys={','.join(keys)}"

                        try:
                            response = requests.get(url, headers=self._get_headers(), timeout=30)
                            if response.status_code == 500:
                                tqdm.write(f"Dispositivo sin telemetrias: {device_name}")
                                break
                            response.raise_for_status()
                            data = response.json()

                            if not data:
                                tqdm.write(f"No se encontraron más datos para {device_name}.")
                                break

                            for key in data:
                                for entry in data[key]:
                                    ts = entry['ts']
                                    value = entry['value']
                                    if ts not in all_data:
                                        all_data[ts] = {}
                                    all_data[ts][key] = value

                            # Calcular el número de claves (telemetrías) por cada timestamp
                            for ts in all_data.keys():
                                total_records += len(all_data[ts])

                            last_ts = max(entry['ts'] for key in data for entry in data[key])
                            start_ts = last_ts + 1

                        except requests.exceptions.RequestException as e:
                            tqdm.write(f"Error al descargar telemetría para '{device_name}': {e}")
                            break

                    if all_data:
                        if not os.path.exists(device_dir):
                            os.makedirs(device_dir)

                        with open(csv_filename, 'a', newline='') as csvfile:
                            all_keys = sorted(keys)
                            fieldnames = ['timestamp'] + all_keys
                            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

                            if os.stat(csv_filename).st_size == 0:
                                writer.writeheader()

                            for ts in sorted(all_data.keys()):
                                row = {'timestamp': datetime.fromtimestamp(ts / 1000).strftime('%Y-%m-%d %H:%M:%S')}
                                for key in all_keys:
                                    row[key] = all_data[ts].get(key, '')
                                writer.writerow(row)

                        total_records += existing_records  # Añadir los registros ya presentes

                        file_size = os.path.getsize(csv_filename) / 1024  # Tamaño en KB
                        tqdm.write(f"Telemetría guardada en '{os.path.basename(csv_filename)}' con {total_records} registros (timestamps * claves).")
                        report.append({
                            'file': os.path.basename(csv_filename),
                            'records': total_records,
                            'size_kb': file_size
                        })

                        # Llamar a la función para procesar y calibrar
                        calibration_file = os.path.join(device_dir, 'calibracion.json')
                        self.process_and_calibrate_telemetry(csv_filename, calibration_file)

                        tqdm.write(f"Borrar telemetrias de dispositivo '{device_id}'");
                        self.delete_telemetry(device_id, start_ts=None, end_ts=end_ts)
                    else:
                        tqdm.write(f"No se encontró telemetría para '{device_name}' en el rango de fechas especificado.")

        # Mostrar el reporte final
        tqdm.write("\nReporte final de archivos descargados:")
        for entry in report:
            tqdm.write(f"Archivo: {entry['file']}, Registros: {entry['records']}, Tamaño: {entry['size_kb']:.2f} KB")


    def generate_user_device_tree(self, output_file="user_device_tree.json"):
        """Genera un archivo JSON con el árbol jerárquico de clientes, gateways y dispositivos."""
        tqdm.write("Generando árbol de usuarios y dispositivos...")
        customers = self.get_customers()
        tree = []

        for customer in tqdm(customers, desc="Procesando clientes"):
            customer_data = {
            'name': customer.get('title'),
            'id': customer.get('id'),
            'gateways': []
            }

            gateways = self.get_gateways_for_customer(customer.get('id'))
            for gateway in gateways:
                gateway_data = {
                'name': gateway.get('name'),
                'id': gateway.get('id').get('id'),
                'devices': []
            }

                devices = self.get_devices_for_gateway(gateway.get('id').get('id'),customer.get('id'))
                for device in devices:
                    gateway_data['devices'].append({
                    'name': device.get('name'),
                    'id': device.get('id').get('id')
                })

                customer_data['gateways'].append(gateway_data)

            tree.append(customer_data)
        output_file = customer.get('title')+'_device_tree.json'
        # Guardar el árbol en un archivo JSON
        with open(output_file, "w") as json_file:
            json.dump(tree, json_file, indent=4)
        tqdm.write(f"Árbol de usuarios y dispositivos guardado en '{output_file}'")

    def delete_telemetry(self, device_id, start_ts=None, end_ts=None):
        """
        Elimina telemetrías de un dispositivo entre un rango de fechas.
        Si no se especifican fechas, se obtienen automáticamente del servidor.
        """
        # Obtener todas las claves de telemetría del dispositivo
        keys = self.get_telemetry_keys(device_id)
        if not keys:
            tqdm.write(f"No se encontraron claves de telemetría para el dispositivo {device_id}. No se puede eliminar.")
            return

        # Obtener rango de tiempo si no se proporciona
        if not start_ts or not end_ts:
            calculated_start_ts, calculated_end_ts = self.get_time_range(device_id, keys)
            start_ts = start_ts or calculated_start_ts
            end_ts = end_ts or calculated_end_ts

        # Validar que se hayan obtenido valores válidos
        if not start_ts or not end_ts:
            tqdm.write(f"No se pudo determinar un rango de tiempo para eliminar datos del dispositivo {device_id}.")
            return

        url = f"{self.url}/api/plugins/telemetry/DEVICE/{device_id}/timeseries/delete"
        headers = self._get_headers()

        # Parámetros de la solicitud
        params = {
            'keys': ','.join(keys),  # Claves de telemetría separadas por comas
            'startTs': start_ts,
            'endTs': end_ts
        }

        try:
            if start_ts is not None:
                start_readable = datetime.fromtimestamp(start_ts / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
            else:
                start_readable = "N/A"

            if end_ts is not None:
                end_readable = datetime.fromtimestamp(end_ts / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
            else:
                end_readable = "N/A"
 
            tqdm.write(f"Eliminando telemetrías para el dispositivo {device_id} entre {start_readable} y {end_readable} con claves: {keys}...")

            response = requests.delete(url, headers=headers, params=params)
            if response.status_code == 200:
                tqdm.write(f"Telemetrías eliminadas para el dispositivo {device_id}.")
            else:
                tqdm.write(f"Error al eliminar telemetrías: {response.status_code} - {response.text}")
        except requests.exceptions.RequestException as e:
            tqdm.write(f"Error en la solicitud de eliminación para {device_id}: {e}")


    def get_time_range_fijo(self, device_id, keys):
        """
        Obtiene el primer y último timestamp de los datos de telemetría para las claves especificadas,
        partiendo siempre de un timestamp fijo de inicio (1 de septiembre de 2024).
        """
        # Fijar el timestamp inicial al 1 de septiembre de 2024 en milisegundos
        fixed_start_ts = int(datetime(2024, 9, 1, 0, 0).timestamp() * 1000)
        start_ts = fixed_start_ts
        end_ts = None

        for key in keys:
            url = f"{self.url}/api/plugins/telemetry/DEVICE/{device_id}/values/timeseries"
            headers = self._get_headers()
            
            try:
                # Parámetros para el último dato
                params_end = {'keys': key, 'limit': 1, 'ascOrder': False}
                response_end = requests.get(url, headers=headers, params=params_end)
                response_end.raise_for_status()
                data_end = response_end.json().get(key, [])
                if data_end and 'ts' in data_end[0]:
                    key_end_ts = data_end[0]['ts']
                    end_ts = max(end_ts, key_end_ts) if end_ts else key_end_ts

            except requests.exceptions.RequestException as e:
                tqdm.write(f"Error al obtener el rango de tiempo para la clave {key}: {e}")

        # Convertir timestamps a formato legible
        start_readable = datetime.fromtimestamp(fixed_start_ts / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
        end_readable = (
            datetime.fromtimestamp(end_ts / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC') 
            if end_ts else "N/A"
        )

        # Mostrar los rangos calculados
        tqdm.write(f"Rango de tiempo calculado: Inicio fijo: {start_readable}, Fin: {end_readable}")

        return start_ts, end_ts

    def get_time_range(self, device_id, keys):
        """
        Obtiene el primer y último timestamp de los datos de telemetría para las claves especificadas
        y los imprime en formato legible.
        """
        start_ts = None
        end_ts = None

        for key in keys:
            url = f"{self.url}/api/plugins/telemetry/DEVICE/{device_id}/values/timeseries"
            headers = self._get_headers()
        
            try:
                # Parámetros para el primer dato
                params_start = {'keys': key, 'limit': 1, 'ascOrder': True}
                response_start = requests.get(url, headers=headers, params=params_start)
                response_start.raise_for_status()
                data_start = response_start.json().get(key, [])
                if data_start and 'ts' in data_start[0]:
                    key_start_ts = data_start[0]['ts']
                    start_ts = min(start_ts, key_start_ts) if start_ts else key_start_ts

                # Parámetros para el último dato
                params_end = {'keys': key, 'limit': 1, 'ascOrder': False}
                response_end = requests.get(url, headers=headers, params=params_end)
                response_end.raise_for_status()
                data_end = response_end.json().get(key, [])
                if data_end and 'ts' in data_end[0]:
                    key_end_ts = data_end[0]['ts']
                    end_ts = max(end_ts, key_end_ts) if end_ts else key_end_ts

            except requests.exceptions.RequestException as e:
                tqdm.write(f"Error al obtener el rango de tiempo para la clave {key}: {e}")

        # Convertir timestamps a formato legible
        start_readable = (
            datetime.fromtimestamp(start_ts / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC') 
            if start_ts else "N/A"
        )
        end_readable = (
            datetime.fromtimestamp(end_ts / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC') 
            if end_ts else "N/A"
        )

        # Mostrar los rangos calculados
        tqdm.write(f"Rango de tiempo calculado: Inicio: {start_readable}, Fin: {end_readable}")

        return start_ts, end_ts

    def get_time_range_old_V(self, device_id, keys):
        """
        Obtiene el primer y último timestamp de los datos de telemetría para las claves especificadas
        y los imprime en formato legible.
        """
        start_ts = None
        end_ts = None

        for key in keys:
            url = f"{self.url}/api/plugins/telemetry/DEVICE/{device_id}/values/timeseries?keys={key}&limit=1"
            headers = self._get_headers()

            try:
                # Obtener el primer dato (más antiguo)
                response_start = requests.get(url, headers=headers, params={'ascOrder': True})
                response_start.raise_for_status()
                data_start = response_start.json().get(key, [])
                if data_start:
                    key_start_ts = data_start[0]['ts']
                    if start_ts is None or key_start_ts < start_ts:
                        start_ts = key_start_ts

                # Obtener el último dato (más reciente)
                response_end = requests.get(url, headers=headers, params={'ascOrder': False})
                response_end.raise_for_status()
                data_end = response_end.json().get(key, [])
                if data_end:
                    key_end_ts = data_end[0]['ts']
                    if end_ts is None or key_end_ts > end_ts:
                        end_ts = key_end_ts

            except requests.exceptions.RequestException as e:
                tqdm.write(f"Error al obtener el rango de tiempo para la clave {key}: {e}")

        # Convertir timestamps a formato legible
        if start_ts is not None:
            start_readable = datetime.fromtimestamp(start_ts / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
        else:
            start_readable = "N/A"

        if end_ts is not None:
            end_readable = datetime.fromtimestamp(end_ts / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
        else:
            end_readable = "N/A"

        # Mostrar los rangos calculados
        tqdm.write(f"Rango de tiempo calculado: Inicio: {start_readable}, Fin: {end_readable}")

        return start_ts, end_ts




    def process_and_calibrate_telemetry(self, csv_filename, calibration_file=None):
        """
        Procesa el archivo de telemetría y crea un nuevo archivo terminado en `_cal`, recalibrando 
        toda la tabla si el archivo de calibración ha cambiado.
        """
        if not os.path.exists(csv_filename):
            tqdm.write(f"Archivo de telemetría no encontrado: {csv_filename}")
            return

        # Generar nombre del archivo calibrado
      #  base, ext = os.path.splitext(csv_filename)
      #  calibrated_csv_filename = f"{base}_cal{ext}"

        # Verificar si se debe recalibrar
        recalibrate_all = False
        if calibration_file and os.path.exists(calibration_file):
            try:
                calib_mtime = os.path.getmtime(calibration_file)  # Última modificación del archivo de calibración
                csv_mtime = os.path.getmtime(csv_filename) 

                # Si el archivo de calibración es más reciente, recalibramos
                if calib_mtime > csv_mtime:
                    tqdm.write(f"Archivo de calibración más reciente encontrado. Recalibrando toda la tabla...")
                    recalibrate_all = True
            except Exception as e:
                tqdm.write(f"Error al verificar el archivo de calibración: {e}")
                return
        else:
            tqdm.write("Archivo de calibración no encontrado. No se aplicarán ajustes.")

        # Leer el archivo CSV original
        with open(csv_filename, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            rows = list(reader)  # Leer todas las filas
            fieldnames = reader.fieldnames + ['current_cal', 'power_cal']
            fieldnames = list(dict.fromkeys(fieldnames))  # Evitar duplicados


        # Generar función de transferencia si hay un archivo de calibración
        transfer_function = None
        if calibration_file and os.path.exists(calibration_file):
            try:
                transfer_function = self.generate_transfer_function(calibration_file)
            except Exception as e:
                tqdm.write(f"Error al procesar el archivo de calibración: {e}")
                return

        # Procesar las filas y añadir las columnas calculadas
        processed_rows = []
        for row in rows:
            try:
                current = float(row.get('current', 0) or 0)
                voltage = float(row.get('voltage', 0) or 0)
            except ValueError:
                current = 0
                voltage = 0

            # Verificar si la fila ya está calibrada
            is_calibrated = (
                 'current_cal' in row and 'power_cal' in row 
                  and row['current_cal'] != '' and row['power_cal'] != ''
            )

            current_cal = None
            power_cal = None

            # Calcular `current_cal` y `power_cal` si hay función de transferencia
            if transfer_function and (recalibrate_all or not is_calibrated):
                try:
                    if current > 0:
                        current_cal = float(transfer_function(current))
                        current_cal = round(current_cal,2)
                        power_cal = round (voltage * current_cal,2)
                    else:
                        current_cal = 0
                        power_cal = 0

                except ValueError:
                    current_cal = None
                    power_cal = None

            # Asignar las columnas calculadas
            row['current_cal'] = current_cal if current_cal is not None else row.get('current_cal', '')
            row['power_cal'] = power_cal if power_cal is not None else row.get('power_cal', '')
            processed_rows.append(row)

        # Escribir el archivo actualizado con las nuevas columnas
        with open(csv_filename, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(processed_rows)

        tqdm.write(f"Archivo calibrado y guardado en: {csv_filename}")


    def generate_transfer_function(self,calibration_file):
        """
        Genera una función de transferencia a partir de un archivo de calibración.
        """
        with open(calibration_file, 'r') as f:
            calibration_data = json.load(f)

        # Extraer puntos de calibración
        sensor_readings = [point['lectura_sensor'] for point in calibration_data['puntos_calibracion']]
        real_values = [point['corriente_real'] for point in calibration_data['puntos_calibracion']]

        # Verificar que los datos sean válidos
        if len(sensor_readings) < 2 or len(real_values) < 2:
            raise ValueError("La calibración requiere al menos dos puntos.")

        # Crear la función de transferencia usando interpolación lineal
        transfer_function = interp1d(sensor_readings, real_values, fill_value="extrapolate")
        return transfer_function

    def apply_transfer_function(self, data, transfer_function):
        """
        Aplica la función de transferencia a los datos de telemetría.
        """
        calibrated_data = {}
        for ts, readings in data.items():
            calibrated_readings = {}
            for key, value in readings.items():
                try:
                    calibrated_readings[key] = transfer_function(float(value))
                except ValueError:
                    # Ignorar valores que no puedan ser convertidos
                    calibrated_readings[key] = value
            calibrated_data[ts] = calibrated_readings
        return calibrated_data







if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='ThingsBoard Data Organizer')
    parser.add_argument('command', choices=['organize', 'download','tree'], help='Command to execute')
    parser.add_argument('--customer', type=str, help='Nombre del cliente para filtrar')

    args = parser.parse_args()

    client = ThingsBoardClient(customer_name=args.customer)

    if args.command == 'organize':
        client.organize_directories()
    elif args.command == 'download':
        client.download_telemetries()
    elif args.command == 'tree':
        client.generate_user_device_tree()
