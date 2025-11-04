#!/bin/bash

# Nạp biến môi trường từ file .env
if [ -f /home/ubuntu/.env ]; then
    source /home/ubuntu/.env
else
    echo "File /home/ubuntu/.env không tồn tại. Vui lòng tạo file theo hướng dẫn."
    exit 1
fi

# Kiểm tra các biến bắt buộc
if [ -z "$S3_BUCKET" ] || [ -z "$S3_BACKUP_PREFIX" ]; then
    echo "Thiếu biến môi trường bắt buộc. Vui lòng kiểm tra file /home/ubuntu/.env"
    exit 1
fi

# Liệt kê các backup gần đây
echo "Các backup gần đây:"
aws s3 ls "s3://${S3_BUCKET}/${S3_BACKUP_PREFIX}/" | sort | tail -n 10

# Kiểm tra backup mới nhất
latest_backup=$(aws s3 ls "s3://${S3_BUCKET}/${S3_BACKUP_PREFIX}/" | sort | tail -n 1 | awk '{print $4}')
if [ -z "$latest_backup" ]; then
    echo "Không tìm thấy backup nào!"
    exit 1
fi

# Kiểm tra thời gian backup gần nhất
backup_timestamp=$(echo $latest_backup | cut -d'.' -f1)
backup_date="${backup_timestamp:0:8}"
backup_time="${backup_timestamp:9:6}"
formatted_date="${backup_date:0:4}-${backup_date:4:2}-${backup_date:6:2} ${backup_time:0:2}:${backup_time:2:2}:${backup_time:4:2}"

echo "Backup gần nhất: $formatted_date"

# Tính thời gian từ backup gần nhất
backup_seconds=$(date -d "$formatted_date" +%s)
current_seconds=$(date +%s)
hours_diff=$(( (current_seconds - backup_seconds) / 3600 ))

echo "Backup gần nhất cách đây ${hours_diff} giờ"

if [ $hours_diff -gt 24 ]; then
    echo "CẢNH BÁO: Backup quá cũ (>24 giờ)!"
fi

exit 0
