#!/bin/bash

# Cài đặt ScyllaDB
sudo mkdir -p /etc/apt/keyrings
sudo gpg --homedir /tmp --no-default-keyring --keyring /etc/apt/keyrings/scylladb.gpg --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys a43e06657bac99e3
sudo wget -O /etc/apt/sources.list.d/scylla.list http://downloads.scylladb.com/deb/debian/scylla-6.2.list
sudo apt-get update
sudo apt-get install -y scylla

# Khóa phiên bản để ngăn việc nâng cấp tự động
sudo apt-mark hold scylla scylla-server scylla-kernel-conf scylla-conf scylla-tools-core

# Cài đặt AWS CLI
sudo apt-get update
sudo snap install aws-cli --classic
sudo apt-get install -y python3-pip
sudo apt-get update

# Cài đặt nodejs ver 22
curl -fsSL https://deb.nodesource.com/setup_22.x | sudo -E bash -
sudo apt-get install -y nodejs

# Cấu hình ScyllaDB
echo "Cấu hình ScyllaDB:"
echo "Edit file: /etc/scylla/scylla.yaml"
echo "--------------------------------"
echo "Thay đổi các giá trị sau:"
echo "listen_address: private IP của máy chủ"
echo "rpc_address: private IP của máy chủ"
echo "seeds: private IP của máy chủ"
echo "Tuỳ chọn (hiện tại đang comment):"
echo "- cluster_name: 'mumon_cluster'"
echo "--------------------------------"
echo "Khởi động ScyllaDB:"
echo "sudo systemctl start scylla-server"
echo "sudo systemctl enable scylla-server"
echo "--------------------------------"

# Cấu hình AWS CLI
echo "Cấu hình AWS CLI:"
echo "aws configure"
echo "https://us-east-1.console.aws.amazon.com/iam/home?region=ap-northeast-1#/users/details/s3?section=permissions"
echo "Default region: ap-southeast-1"
echo "Default output format: json"
echo "--------------------------------"

echo "Cấu hình ScyllaDB. Chọn Y nếu được hỏi: Measuring sequential write bandwidth:"
sudo scylla_io_setup