import requests
import json
import os
import csv
import argparse
from datetime import datetime
from tqdm import tqdm

class ThingsBoardClient:
    def __init__(self, config_file='config.json', token_file='token.json'):
        config_path = '/mnt/thingsboard_data/Descargas/'+ config_file  #os.path.abspath(config_file)
        if os.path.exists(config_path):
            print(f"Leyendo configuración desde: {config_path}")
            with open(config_path, 'r') as file:
                try:
                    config = json.load(file)
                    self.url = config.get('thingsboard_url')
                    self.username = config.get('username')
                    self.password = config.get('password')
                    if not self.url:
                        raise ValueError("La URL de ThingsBoard no está definida en el archivo de configuración.")
                except json.JSONDecodeError as e:
                    print(f"Error al decodificar el archivo de configuración: {e}")
                    exit(1)
        else:
            print(f"Archivo de configuración no encontrado: {config_path}")
            exit(1)

        self.token_file = token_file
        self.token = None
        self._authenticate()

    def _authenticate(self):
        auth_url = f"{self.url}/api/auth/login"
        credentials = {
            "username": self.username,
            "password": self.password
        }
        print(f"Autenticando con ThingsBoard en: {auth_url}")
        response = requests.post(auth_url, json=credentials)
        if response.status_code == 200:
            self.token = response.json().get('token')
            print("Autenticación exitosa. Token recibido.")
            with open(self.token_file, 'w') as file:
                json.dump({'token': self.token}, file)
        else:
            print(f"Error al autenticar: {response.status_code} - {response.text}")
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
        print(f"Obteniendo clientes") # desde: {customers_url}")
        response = requests.get(customers_url, headers=self._get_headers())
        if response.status_code == 200:
            customers = response.json().get('data', [])
            print(f"Clientes obtenidos: {len(customers)}")
            return customers
        else:
            print(f"Error al obtener clientes: {response.status_code} - {response.text}")
            return []

    def get_gateways_for_customer(self, customer_id):
        customer_id_str = customer_id.get('id') if isinstance(customer_id, dict) else customer_id
        gateways_url = f"{self.url}/api/customer/{customer_id_str}/devices?pageSize=1000&page=0"
        print(f"Obteniendo dispositivos para el cliente") # {customer_id_str} desde: {gateways_url}")
        response = requests.get(gateways_url, headers=self._get_headers())
        if response.status_code == 200:
            all_devices = response.json().get('data', [])
            gateways = [device for device in all_devices if device.get('additionalInfo', {}).get('gateway', False)]
            print(f"Gateways obtenidos para el cliente, {len(gateways)}") #{customer_id_str}: {len(gateways)}")
            return gateways
        else:
            print(f"Error al obtener gateways para el cliente {customer_id_str}: {response.status_code} - {response.text}")
            return []

    def get_devices_for_gateway(self, gateway_id):
        devices_url = f"{self.url}/api/tenant/devices?gatewayId={gateway_id}&pageSize=1000&page=0"
        print(f"Obteniendo dispositivos para el gateway") #{gateway_id} desde: {devices_url}")
        response = requests.get(devices_url, headers=self._get_headers())
        if response.status_code == 200:
            devices = response.json().get('data', [])
            print(f"Dispositivos obtenidos para el gateway {len(devices)}")  #{gateway_id}: {len(devices)}")
            return devices
        else:
            print(f"Error al obtener dispositivos para el gateway {gateway_id}: {response.status_code} - {response.text}")
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
                print(f"No se encontraron gateways para el cliente {customer_name}") # ({customer_id})")
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

                devices = self.get_devices_for_gateway(gateway_id)
                if not devices:
                    print(f"No se encontraron dispositivos para el gateway {gateway_name}") # ({gateway_id})")
                for device in devices:
                    device_name = device.get('name')
                    print(f"Organizando directorio para el dispositivo: {device_name}")
                    device_dir = os.path.join(gateway_dir, device_name)
                    if not os.path.exists(device_dir):
                        os.makedirs(device_dir)

                    device_file = os.path.join(device_dir, f"{device_name}_device.json")
                    with open(device_file, 'w') as file:
                        json.dump(device, file, indent=4)

    def download_telemetries(self):
        print("Descargando telemetría de dispositivos...")
        customers = self.get_customers()
        base_directory = 'thingsboard_data'

        for customer in tqdm(customers, desc="Descargando telemetría de clientes"):
            customer_name = customer.get('title')
            customer_id = customer.get('id')
            print(f"Procesando cliente: {customer_name}")
            customer_dir = os.path.join(base_directory, customer_name)

            gateways = self.get_gateways_for_customer(customer_id)
            if not gateways:
                print(f"No se encontraron gateways para el cliente {customer_name}") #({customer_id})")
                continue

            for gateway in tqdm(gateways, desc=f"Procesando gateways de {customer_name}", leave=False):
                gateway_name = gateway.get('name')
                gateway_id = gateway.get('id').get('id')
                print(f"Procesando gateway: {gateway_name}")
                gateway_dir = os.path.join(customer_dir, gateway_name)

                devices = self.get_devices_for_gateway(gateway_id)
                if not devices:
                    print(f"No se encontraron dispositivos para el gateway {gateway_name}") # ({gateway_id})")
                    continue

                for device in devices:
                    device_name = device.get('name')
                    device_id = device['id']['id']
                    keys = self.get_telemetry_keys(device_id)
                    print(f"Descargando telemetría para el dispositivo: {device_name}") # ({device_id})")

                    # Establecer el rango de tiempo para la descarga de telemetría
                    start_ts = int(datetime(2024, 9, 1).timestamp() * 1000)  # Fecha de inicio predeterminada
                    end_ts = int(datetime.now().timestamp() * 1000)  # Fecha actual como límite
                    device_dir = os.path.join(gateway_dir, device_name)
                    csv_filename = os.path.join(device_dir, f"{device_name}_telemetry.csv")

                    # Verificar si el archivo ya existe para ajustar el start_ts
                    if os.path.exists(csv_filename):
                        print(f"Archivo de telemetría encontrado: {csv_filename}. Leyendo último timestamp.")
                        with open(csv_filename, 'r') as csvfile:
                            reader = csv.DictReader(csvfile)
                            last_row = None
                            for last_row in reader:
                                pass  # Leer hasta la última fila
                            if last_row:
                                try:
                                    # Convertir el último timestamp a milisegundos
                                    last_ts = int(datetime.strptime(last_row['timestamp'], '%Y-%m-%d %H:%M:%S').timestamp() * 1000)
                                    start_ts = last_ts + 1  # Evitar duplicados al iniciar desde el siguiente timestamp
                                    #print(f"Último timestamp encontrado: {last_row['timestamp']}. Continuando desde {datetime.fromtimestamp(start_ts / 1000).strftime('%Y-%m-%d %H:%M:%S')}.")
                                except ValueError:
                                    print(f"Error al leer el último timestamp. Se usará la fecha de inicio predeterminada.")
                    else:
                        print(f"No se encontró archivo previo. Comenzando desde el 1 de septiembre de 2024.")

                    # Inicializar variables para descargar la telemetría por bloques
                    limit = 50000
                    all_data = {}

                    while True:
                        url = f"{self.url}/api/plugins/telemetry/DEVICE/{device_id}/values/timeseries?limit={limit}&startTs={start_ts}&endTs={end_ts}&keys={','.join(keys)}"
                        #print(f"Solicitando telemetría de {device_name} con URL: {url}")

                        try:
                            response = requests.get(url, headers=self._get_headers(), timeout=30)
                            response.raise_for_status()
                            data = response.json()

                            # Si no hay más datos, salir del bucle
                            if not data:
                                print(f"No se encontraron más datos para {device_name}.")
                                break

                            # Procesar los datos para organizarlos por timestamp
                            for key in data:
                                for entry in data[key]:
                                    ts = entry['ts']
                                    value = entry['value']
                                    if ts not in all_data:
                                        all_data[ts] = {}
                                    all_data[ts][key] = value

                            # Actualizar el start_ts para el siguiente bloque de datos
                            last_ts = max(entry['ts'] for key in data for entry in data[key])
                            start_ts = last_ts + 1  # Evitar duplicados
                            #print(f"Datos descargados hasta {datetime.fromtimestamp(last_ts / 1000).strftime('%Y-%m-%d %H:%M:%S')}.")

                        except requests.exceptions.RequestException as e:
                            print(f"Error al descargar telemetría para el dispositivo '{device_name}': {e}")
                            break

                    # Guardar la telemetría en un archivo CSV organizado por columnas
                    if all_data:
                        if not os.path.exists(device_dir):
                            os.makedirs(device_dir)

                        with open(csv_filename, 'a', newline='') as csvfile:
                            # Determinar todas las claves únicas
                            all_keys = sorted(keys)
                            fieldnames = ['timestamp'] + all_keys
                            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

                            # Escribir el encabezado solo si el archivo es nuevo
                            if os.stat(csv_filename).st_size == 0:
                                writer.writeheader()

                            # Escribir cada fila en el CSV
                            for ts in sorted(all_data.keys()):
                                row = {'timestamp': datetime.fromtimestamp(ts / 1000).strftime('%Y-%m-%d %H:%M:%S')}
                                for key in all_keys:
                                    row[key] = all_data[ts].get(key, '')  # Obtener el valor o un string vacío si no está presente
                                writer.writerow(row)

                        print(f"Telemetría guardada en '{csv_filename}' con un total de {len(all_data)} registros.")
                    else:
                        print(f"No se encontró telemetría para el dispositivo '{device_name}' en el rango de fechas especificado.")

    # Métodos adicionales para autenticación, obtención de clientes, gateways y dispositivos...
    # get_customers(), get_gateways_for_customer(), get_devices_for_gateway(), get_telemetry_keys()
    # ...

    def download_telemetry(self, device_id, keys, start_ts, end_ts):
        url = f"{self.url}/api/plugins/telemetry/DEVICE/{device_id}/values/timeseries"
        headers = {
            'Content-Type': 'application/json',
            'X-Authorization': f'Bearer {self.token}'
        }
        params = {
            'keys': ','.join(keys),
            'startTs': start_ts,
            'endTs': end_ts
        }

        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error al descargar telemetrías: {response.status_code} - {response.text}")
            return {}

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
            print(f"Error al obtener claves de telemetría: {response.status_code} - {response.text}")
            return []

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='ThingsBoard Data Organizer')
    parser.add_argument('command', choices=['organize', 'download'], help='Command to execute')
    args = parser.parse_args()

    client = ThingsBoardClient()

    if args.command == 'organize':
        client.organize_directories()
    elif args.command == 'download':
        client.download_telemetries()
