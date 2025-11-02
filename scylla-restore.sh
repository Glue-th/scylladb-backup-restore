#!/bin/bash

# Nạp biến môi trường từ file .env
if [ -f /home/ubuntu/.env ]; then
    source /home/ubuntu/.env
else
    echo "File /home/ubuntu/.env không tồn tại. Vui lòng tạo file theo hướng dẫn."
    exit 1
fi

# Kiểm tra tham số
if [ "$#" -ne 1 ]; then
    echo "Sử dụng: $0 <backup_timestamp>"
    echo "Ví dụ: $0 20251102_030000"
    exit 1
fi

# Kiểm tra các biến bắt buộc
if [ -z "$SCYLLA_HOST" ] || [ -z "$S3_BUCKET" ]; then
    echo "Thiếu biến môi trường bắt buộc. Vui lòng kiểm tra file /home/ubuntu/.env"
    exit 1
fi

# Chuyển đổi string keyspaces thành array
IFS=' ' read -r -a KEYSPACES <<< "$SCYLLA_KEYSPACES"

# Cấu hình
TIMESTAMP=$1

# Tạo thư mục khôi phục
rm -rf $RESTORE_DIR
mkdir -p $RESTORE_DIR

# Tải backup từ S3
echo "Đang tải backup từ S3..."
aws s3 cp "s3://${S3_BUCKET}/${S3_BACKUP_PREFIX}/${TIMESTAMP}.tar.gz" "${RESTORE_DIR}/${TIMESTAMP}.tar.gz"

# Kiểm tra kết quả tải xuống
if [ $? -ne 0 ]; then
    echo "Lỗi: Không thể tải file backup từ S3. Vui lòng kiểm tra lại timestamp và kết nối."
    exit 1
fi

# Giải nén
echo "Đang giải nén..."
cd $RESTORE_DIR
tar -xzf "${TIMESTAMP}.tar.gz"

# Khôi phục
echo "Đang khôi phục..."
for keyspace_dir in "${RESTORE_DIR}/${TIMESTAMP}"/*; do
    if [ -d "$keyspace_dir" ]; then
        keyspace=$(basename "$keyspace_dir")
        
        # Kiểm tra nếu keyspace nằm trong danh sách cần khôi phục
        if [[ ! " ${KEYSPACES[@]} " =~ " ${keyspace} " ]]; then
            continue
        fi
        
        echo "Đang khôi phục schema cho $keyspace..."
        # Tạo keyspace (drop trước nếu đã tồn tại)
        cqlsh $SCYLLA_HOST -e "DROP KEYSPACE IF EXISTS $keyspace;"
        cqlsh $SCYLLA_HOST -f "${RESTORE_DIR}/${TIMESTAMP}/${keyspace}_schema.cql"
        
        echo "Đang khôi phục dữ liệu cho $keyspace..."
        for table_file in "${keyspace_dir}"/*.csv; do
            table=$(basename "$table_file" .csv)
            echo "Đang khôi phục bảng $keyspace.$table..."
            cqlsh $SCYLLA_HOST -e "COPY $keyspace.$table FROM '${table_file}' WITH HEADER = true AND NULL = 'NULL' AND ESCAPE='\\';"
        done
    fi
done

# Dọn dẹp
echo "Dọn dẹp..."
rm -rf $RESTORE_DIR

echo "Khôi phục hoàn thành từ backup: ${TIMESTAMP}"
