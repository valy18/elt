import subprocess
import time



def wait_for_postgres(host, port=5432, max_retries=5, delay_seconds=5):
    retries = 0
    while retries < max_retries:
        try:
            result = subprocess.run(
                ["pg_isready", "-h", host, "-p", str(port)], 
                check=True, 
                capture_output=True, 
                text=True
            )
            if "accepting connections" in result.stdout:
                print("Successfully connected to Postgres")
                return True
        except subprocess.CalledProcessError as e:
            print(f"Error connecting to Postgres: {e}")
            retries += 1
            print(
                f"Retrying in {delay_seconds} seconds... (Attempt {retries}/{max_retries})"
            )
            time.sleep(delay_seconds)
    print("Max retries reached. Exiting")
    return False


if not wait_for_postgres(host="source_postgres"):
    exit(1)

if not wait_for_postgres(host="destination_postgres"):
    exit(1)

print("Starting ELT Script...")

source_config = {
    'dbname': 'source_db',
    'user': 'postgres',
    'password': 'secret',
    'host': 'source_postgres'
}

destination_config = {
    'dbname': 'destination_db',
    'user': 'postgres',
    'password': 'secret',
    'host': 'destination_postgres'
}

# Dump de la base source
dump_command = [
    'pg_dump',
    '-h', source_config['host'],
    '-U', source_config['user'],
    '-d', source_config['dbname'],
    '-f', 'data_dump.sql',
    '-w'
]

subprocess_env = dict(PGPASSWORD=source_config['password'])

print(f"Running dump command: {' '.join(dump_command)}")
print(f"Using environment: {subprocess_env}")

try:
    subprocess.run(dump_command, env=subprocess_env, check=True)
    print("Dump completed successfully.")
except subprocess.CalledProcessError as e:
    print(f"Error during dump: {e}")
    exit(1)

# Chargement dans la base destination
load_command = [
    'psql',
    '-h', destination_config['host'],
    '-U', destination_config['user'],
    '-d', destination_config['dbname'],
    '-a', '-f', 'data_dump.sql'
]

subprocess_env.update(PGPASSWORD=destination_config['password'])

print(f"Running load command: {' '.join(load_command)}")
print(f"Using environment: {subprocess_env}")

try:
    subprocess.run(load_command, env=subprocess_env, check=True)
    print("Load completed successfully.")
except subprocess.CalledProcessError as e:
    print(f"Error during load: {e}")
    exit(1)

print("Ending ELT Script")