# Auto backup/restore ScyllaDB

### Crontab

`sudo crontab -e`

```sh
# Chạy lúc 3:00 sáng
0 3 * * * source /home/ubuntu/.env && /home/ubuntu/scylla-backup.sh > /var/log/scylla-backup-morning.log 2>&1

# Chạy lúc 15:00 chiều
0 15 * * * source /home/ubuntu/.env && /home/ubuntu/scylla-backup.sh > /var/log/scylla-backup-afternoon.log 2>&1
```

### Run manually

```bash
cd /home/ubuntu
source .env
./scylla-backup.sh
./check-backup.sh
```

# Quy trình khôi phục khẩn cấp ScyllaDB

## 1. Tạo máy chủ mới

- Name: mm-scylla-prod-restore
- OS: Ubuntu 24.04
- Architecture: 64-bit (Arm)
- Instance Type: t4g.large
- Key pair (login): mm-private-key
- Network setting: subnet-0b0a7e24f8e14e5ce - sub-dev-mm-private2-2 (Network interface: eni-003603a3873150e3a)
- Security Group: sg-0442e14cf39e218fa
- Storage: gp3 50GB

## 2. Cài đặt ScyllaDB & Tools

Chạy file `./install.sh` để cài đặt scylladb, aws-cli, python, boto3.

## 3. Cấu hình ScyllaDB

```bash
sudo nano /etc/scylla/scylla.yaml
```

Cấu hình tối thiểu:
- listen_address: private IP của máy chủ
- rpc_address: private IP của máy chủ
- seeds: private IP của máy chủ

Tuỳ chọn (hiện tại đang comment):
- cluster_name: 'mumon_cluster'

## 4. Khởi động ScyllaDB

```bash
sudo systemctl start scylla-server
sudo systemctl enable scylla-server
```

## 5. Cấu hình AWS CLI

```bash
aws configure
# Account: https://us-east-1.console.aws.amazon.com/iam/home?region=ap-northeast-1#/users/details/s3?section=permissions
# Aws Key: AKIATOCMCKJFGUD3FM5V
# Aws Secret: L/cDCJt5zrfjVVj****
# Default region: ap-southeast-1
# Default output format: json
```

Hoặc tạo Inline Policy để truy cập S3:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::mm-database-backup",
        "arn:aws:s3:::mm-database-backup/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": "kms:Decrypt",
      "Resource": "arn:aws:kms:ap-northeast-1:236382736970:key/2273e0a2-49f1-4984-9fbd-9c2d348634b4"
    }
  ]
}
```

## 6. Khôi phục từ backup

Liệt kê các backup:
```bash
aws s3 ls s3://mm-database-backup/scylladb/PROD

# 2025-10-30 02:39:02     511463 cassandra-backup-PROD-2025-10-30T02-38-57.tar.gz
# 2025-10-31 02:38:13     518141 cassandra-backup-PROD-2025-10-31T02-38-06.tar.gz
# 2025-11-01 02:39:02     523223 cassandra-backup-PROD-2025-11-01T02-38-57.tar.gz
# 2025-11-02 02:40:03     529984 cassandra-backup-PROD-2025-11-02T02-39-58.tar.gz
# 2025-11-03 02:40:25     533082 cassandra-backup-PROD-2025-11-03T02-40-17.tar.gz
# 2025-11-04 02:38:14     536866 cassandra-backup-PROD-2025-11-04T02-38-08.tar.gz
```

Chọn backup gần nhất và khôi phục:
```bash
./scylla-restore.sh cassandra-backup-PROD-2025-11-04T02-38-08
```

## 7. Kiểm tra sau khôi phục

```bash
cqlsh [private-ip]
cqlsh> DESC KEYSPACES;
cqlsh> DESCRIBE TABLES;
cqlsh> SELECT COUNT(*) FROM mumon_prod.monster_conversations;
cqlsh> SELECT * FROM mumon_prod.devices_owner LIMIT 10;
cqlsh> SELECT * FROM mumon_prod.ai_prompts LIMIT 10;
```

## 8. Cập nhật cấu hình ứng dụng

Cập nhật endpoints trong các ứng dụng để trỏ đến ScyllaDB mới.