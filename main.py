import argparse
import logging
import locale
import os
import paramiko
from dotenv import load_dotenv
from scp import SCPClient
from datetime import datetime, timedelta
import time

# Configurações de Log
locale.setlocale(locale.LC_ALL, "pt_BR.UTF-8")
logging.basicConfig(
    handlers=[
        logging.FileHandler("Postgres-Backup-Manager.log", "a", "utf-8"),
        logging.StreamHandler(),
    ],
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%d/%m/%Y %H:%M:%S",
)

# Carregar variáveis de ambiente
load_dotenv()

# Credenciais do PostgreSQL e SSH
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_DB_NAME = os.getenv("PG_DB_NAME")

SSH_HOST = os.getenv("SSH_HOST")
SSH_USER = os.getenv("SSH_USER")
SSH_PASSWORD = os.getenv("SSH_PASSWORD")
BACKUP_DIR = "/var/backups/postgresql"


# Função para criar conexão SSH
def create_ssh_client(server, user, password):
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(server, username=user, password=password)
    logging.info("Connected (version 2.0, client OpenSSH_9.3)")
    logging.info("Authentication (password) successful!")
    return client


# Função para realizar backup
def perform_backup(backup_name, local_backup_subdir):
    local_backup_path = os.path.join(os.getcwd(), local_backup_subdir)
    if not os.path.exists(local_backup_path):
        os.makedirs(local_backup_path)

    try:
        ssh = create_ssh_client(SSH_HOST, SSH_USER, SSH_PASSWORD)
        backup_command = f"PGPASSWORD='{PG_PASSWORD}' pg_dump -U {PG_USER} -h {PG_HOST} -F c -b -v -f {BACKUP_DIR}/{backup_name} {PG_DB_NAME}"
        logging.info(f"Executing backup command: {backup_command}")
        stdin, stdout, stderr = ssh.exec_command(backup_command)
        exit_status = stdout.channel.recv_exit_status()

        if exit_status != 0:
            logging.error(f"pg_dump failed with exit status {exit_status}")
            logging.error(stderr.read().decode())
            return

        scp = SCPClient(ssh.get_transport())
        scp.get(f"{BACKUP_DIR}/{backup_name}", local_backup_path)
        scp.close()
        ssh.close()

        logging.info(
            f"Backup {backup_name} transferido com sucesso para {local_backup_path}"
        )
    except Exception as e:
        logging.error(f"Erro ao realizar backup: {e}")


# Função para limpar backups antigos
def clean_old_backups(local_backup_subdir, days_to_keep=4):
    local_backup_path = os.path.join(os.getcwd(), local_backup_subdir)
    if not os.path.exists(local_backup_path):
        return

    cutoff_date = datetime.now() - timedelta(days=days_to_keep)
    for filename in os.listdir(local_backup_path):
        file_path = os.path.join(local_backup_path, filename)
        if os.path.isfile(file_path):
            file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
            if file_mtime < cutoff_date:
                os.remove(file_path)
                logging.info(
                    f"Backup {filename} removido, pois é mais antigo que {days_to_keep} dias"
                )


# Funções para modos de operação
def modo_manual():
    backup_name = (
        f"backup_{PG_DB_NAME}_{PG_USER}_{datetime.now().strftime('%d%m%Y_%H%M%S')}.sql"
    )
    logging.info("Modo manual iniciado")
    perform_backup(backup_name, "backup_manual")
    clean_old_backups("backup_manual")
    logging.info("Modo manual finalizado")


def modo_diario(hora):
    now = datetime.now()
    target_time = now.replace(
        hour=int(hora.split(":")[0]),
        minute=int(hora.split(":")[1]),
        second=0,
        microsecond=0,
    )
    if now > target_time:
        target_time += timedelta(days=1)
    delay = (target_time - now).total_seconds()
    logging.info(
        f"Backup diário agendado para {target_time.strftime('%d/%m/%Y %H:%M:%S')} (em {timedelta(seconds=delay)})"
    )
    time.sleep(delay)
    while True:
        backup_name = f"backup_{PG_DB_NAME}_{PG_USER}_{datetime.now().strftime('%d%m%Y_%H%M%S')}.sql"
        logging.info("Modo diário iniciado")
        perform_backup(backup_name, "backup_diario")
        clean_old_backups("backup_diario")
        logging.info("Modo diário finalizado")
        time.sleep(24 * 3600)


def modo_por_intervalo(intervalo):
    horas, minutos = map(int, intervalo.split(":"))
    intervalo_segundos = horas * 3600 + minutos * 60
    logging.info(f"Backup agendado a cada {intervalo} horas")
    while True:
        backup_name = f"backup_{PG_DB_NAME}_{PG_USER}_{datetime.now().strftime('%d%m%Y_%H%M%S')}.sql"
        logging.info("Modo por intervalo iniciado")
        perform_backup(backup_name, "backup_por_intervalo")
        clean_old_backups("backup_por_intervalo")
        logging.info("Modo por intervalo finalizado")
        logging.info(f"Próximo backup em {timedelta(seconds=intervalo_segundos)}")
        time.sleep(intervalo_segundos)


# Argumentos do script
parser = argparse.ArgumentParser(description="Gerenciador de Backups do PostgreSQL")
parser.add_argument(
    "--modo",
    required=True,
    choices=["manual", "diario", "por_intervalo"],
    help="Modo de operação do backup",
)
parser.add_argument(
    "--tempo",
    help="Hora para o modo diário (HH:MM) ou intervalo para o modo por intervalo (HH:MM)",
)

args = parser.parse_args()

# Executar o modo apropriado
if args.modo == "manual":
    modo_manual()
elif args.modo == "diario":
    if args.tempo:
        modo_diario(args.tempo)
    else:
        logging.error("O modo diário requer a especificação do horário com --tempo")
elif args.modo == "por_intervalo":
    if args.tempo:
        modo_por_intervalo(args.tempo)
    else:
        logging.error(
            "O modo por intervalo requer a especificação do intervalo com --tempo"
        )
