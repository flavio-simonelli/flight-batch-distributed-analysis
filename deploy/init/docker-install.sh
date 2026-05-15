#!/bin/bash

LOG_FILE="/var/log/docker-install.log"
exec > >(tee -a "$LOG_FILE") 2>&1

echo "[INFO] Inizio installazione Docker e Docker Compose..."

echo "[DEBUG] Aggiorno i pacchetti di sistema"
sudo yum update -y

echo "[DEBUG] Abilito repository docker tramite amazon-linux-extras"
sudo amazon-linux-extras enable docker

echo "[DEBUG] Installo Docker"
sudo amazon-linux-extras install docker -y

echo "[DEBUG] Avvio e abilito il servizio Docker"
sudo systemctl start docker
sudo systemctl enable docker

echo "[DEBUG] Aggiungo l'utente ec2-user al gruppo docker"
sudo usermod -aG docker ec2-user

echo "[DEBUG] Scarico e installo Docker Compose"
DOCKER_COMPOSE_VERSION="v2.39.2"
sudo curl -L "https://github.com/docker/compose/releases/download/${DOCKER_COMPOSE_VERSION}/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

echo "[DEBUG] Verifico le installazioni"
docker --version
docker-compose --version

echo "[INFO] Docker e Docker Compose installati correttamente!"