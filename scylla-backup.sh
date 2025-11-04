#!/bin/bash

# Nạp biến môi trường từ file .env
if [ -f /home/ubuntu/.env ]; then
    source /home/ubuntu/.env
else
    echo "File /home/ubuntu/.env không tồn tại. Vui lòng tạo file theo hướng dẫn."
    exit 1
fi

# Kiểm tra các biến bắt buộc
if [ -z "$SCYLLA_HOST" ] || [ -z "$S3_BUCKET" ]; then
    echo "Thiếu biến môi trường bắt buộc. Vui lòng kiểm tra file /home/ubuntu/.env"
    exit 1
fi

# Chuyển đổi string keyspaces thành array
IFS=' ' read -r -a KEYSPACES <<< "$SCYLLA_KEYSPACES"

# Tạo thư mục sao lưu tạm thời
rm -rf $BACKUP_DIR
mkdir -p $BACKUP_DIR

# Tạo thư mục với timestamp
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
BACKUP_PATH="${BACKUP_DIR}/${TIMESTAMP}"
mkdir -p $BACKUP_PATH

# Ghi thông tin máy chủ và thời gian
echo "Backup time: $(date)" > "${BACKUP_PATH}/backup_info.txt"
echo "Server: $(hostname)" >> "${BACKUP_PATH}/backup_info.txt"
echo "ScyllaDB version: $(scylla --version)" >> "${BACKUP_PATH}/backup_info.txt"

# Sao lưu schema và dữ liệu cho mỗi keyspace
for keyspace in "${KEYSPACES[@]}"; do
    echo "Đang sao lưu schema cho $keyspace..."
    # Xuất schema
    # cqlsh 10.10.127.178 -e "DESC KEYSPACE mumon_dev;"
    cqlsh $SCYLLA_HOST -e "DESC KEYSPACE $keyspace;" > "${BACKUP_PATH}/${keyspace}_schema.cql"
    
    echo "Đang sao lưu dữ liệu cho $keyspace..."
    # Lấy danh sách bảng
    # cqlsh 10.10.127.178 -e "USE mumon_dev; DESC TABLES;" | grep -v '^$'
    tables=$(cqlsh $SCYLLA_HOST -e "USE $keyspace; DESC TABLES;" | grep -v '^$')
    
    # Tạo thư mục cho keyspace
    mkdir -p "${BACKUP_PATH}/${keyspace}"
    
    # Sao lưu từng bảng
    for table in $tables; do
        if [ "$table" = "crawled_rss_news" ]; then
            echo "Bỏ qua bảng crawled_rss_news..."
            continue
        fi
        echo "Đang sao lưu bảng $keyspace.$table..."
        cqlsh $SCYLLA_HOST -e "COPY $keyspace.$table TO '${BACKUP_PATH}/${keyspace}/${table}.csv' WITH HEADER = true AND NULL = 'NULL' AND ESCAPE='\\';"
    done
done

# Nén thư mục backup
cd $BACKUP_DIR
tar -czf "${TIMESTAMP}.tar.gz" $TIMESTAMP

# Tải lên S3
echo "Đang tải lên S3..."
aws s3 cp "${TIMESTAMP}.tar.gz" "s3://${S3_BUCKET}/${S3_BACKUP_PREFIX}/${TIMESTAMP}.tar.gz"

# Xóa backup cũ trên S3 (giữ lại trong N ngày)
if [ ! -z "$RETENTION_DAYS" ]; then
    echo "Đang xóa backup cũ trên S3..."
    aws s3 ls "s3://${S3_BUCKET}/${S3_BACKUP_PREFIX}/" | grep -v "${TIMESTAMP}" | awk '{print $4}' | while read -r old_backup; do
        backup_date=$(echo $old_backup | cut -d'_' -f1)
        backup_date_seconds=$(date -d "${backup_date:0:4}-${backup_date:4:2}-${backup_date:6:2}" +%s)
        current_date_seconds=$(date +%s)
        days_diff=$(( (current_date_seconds - backup_date_seconds) / 86400 ))
        
        if [ $days_diff -gt $RETENTION_DAYS ]; then
            echo "Xóa backup cũ: $old_backup (${days_diff} ngày trước)"
            aws s3 rm "s3://${S3_BUCKET}/${S3_BACKUP_PREFIX}/${old_backup}"
        fi
    done
fi

# Dọn dẹp
echo "Dọn dẹp..."
rm -rf $BACKUP_DIR

echo "Sao lưu hoàn thành: ${TIMESTAMP}"
