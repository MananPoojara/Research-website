# Research-website

sudo apt update && sudo apt upgrade -y && sudo apt install -y ca-certificates curl gnupg git ufw docker.io docker-compose && sudo ufw allow OpenSSH && sudo ufw allow 80 && sudo ufw allow 443 && sudo ufw --force enable && sudo systemctl enable --now docker && sudo usermod -aG docker $USER && mkdir -p ~/youngturtle-server/infra/nginx ~/youngturtle-server/infra/compose ~/youngturtle-server/infra/volumes/{postgres,redis,minio} && touch ~/youngturtle-server/infra/docker-compose.yml ~/youngturtle-server/infra/nginx/default.conf


