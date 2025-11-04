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
    echo "Ví dụ: $0 cassandra-backup-PROD-2025-11-04T02-38-08"
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
echo "Tạo thư mục khôi phục: $RESTORE_DIR"
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
echo "Đang giải nén: ${TIMESTAMP}.tar.gz"
tar -xzf "${TIMESTAMP}.tar.gz"
ls -la

# Khôi phục
echo "Đang khôi phục... ${RESTORE_DIR}"

# Bước 1: Thu thập tất cả keyspaces duy nhất từ các file JSON
echo "Thu thập danh sách keyspaces cần khôi phục..."
declare -A unique_keyspaces=()
for json_file in "${RESTORE_DIR}"/*.json; do
    [ ! -f "$json_file" ] && continue
    
    filename=$(basename "$json_file")
    if [[ "$filename" == "backup-metadata.json" ]]; then
        continue
    fi
    
    keyspace=$(jq -r '.keyspace' "$json_file" 2>/dev/null)
    if [ -z "$keyspace" ] || [ "$keyspace" == "null" ]; then
        continue
    fi
    
    if [[ " ${KEYSPACES[@]} " =~ " ${keyspace} " ]]; then
        unique_keyspaces["$keyspace"]=1
    fi
done

# Hiển thị keyspaces đã tìm thấy
if [ ${#unique_keyspaces[@]} -gt 0 ]; then
    echo "Tìm thấy ${#unique_keyspaces[@]} keyspace(s) hợp lệ: ${!unique_keyspaces[@]}"
fi

if [ ${#unique_keyspaces[@]} -eq 0 ]; then
    echo "Cảnh báo: Không tìm thấy keyspace nào hợp lệ trong backup!"
    echo "Danh sách keyspaces được phép: ${KEYSPACES[@]}"
    echo "Vui lòng kiểm tra lại các file JSON trong thư mục ${RESTORE_DIR}"
    exit 1
fi

# Bước 2: Xóa và tạo lại keyspace cho mỗi keyspace duy nhất
echo "Xóa và tạo lại keyspaces..."
for keyspace in "${!unique_keyspaces[@]}"; do
    echo "Đang xử lý keyspace: $keyspace"
    
    # Xóa keyspace nếu đã tồn tại (để tránh duplicate)
    echo "Xóa keyspace $keyspace nếu đã tồn tại..."
    echo "DROP KEYSPACE IF EXISTS \"$keyspace\";" | cqlsh $SCYLLA_HOST 2>/dev/null || true
    
    # Nếu có file schema, thử đọc và tạo từ đó
    schema_file="${RESTORE_DIR}/${keyspace}_schema.cql"
    if [ -f "$schema_file" ]; then
        echo "Tìm thấy file schema cho keyspace $keyspace, đang sử dụng..."
        
        # Trích xuất lệnh CREATE KEYSPACE từ file schema
        create_keyspace_cql=$(grep -i "CREATE KEYSPACE" "$schema_file" | head -1)
        if [ -n "$create_keyspace_cql" ]; then
            # Thêm IF NOT EXISTS vào lệnh nếu chưa có
            if [[ ! "$create_keyspace_cql" =~ "IF NOT EXISTS" ]]; then
                create_keyspace_cql=$(echo "$create_keyspace_cql" | sed 's/CREATE KEYSPACE/CREATE KEYSPACE IF NOT EXISTS/i')
            fi
            echo "$create_keyspace_cql" | cqlsh $SCYLLA_HOST
            if [ $? -eq 0 ]; then
                echo "Keyspace $keyspace đã được tạo từ file schema."
                
                # Tạo UDT sau khi keyspace đã được tạo (nếu có trong schema)
                echo "Đang tạo UDT (User Defined Types) nếu có..."
                # Tìm tất cả CREATE TYPE statements (có thể có khoảng trắng ở đầu)
                udt_count=0
                while IFS= read -r create_type_cql; do
                    # Bỏ qua comment và empty lines
                    create_type_cql=$(echo "$create_type_cql" | sed 's/^[[:space:]]*//' | sed 's/--.*$//')
                    [ -z "$create_type_cql" ] && continue
                    
                    # Thêm IF NOT EXISTS nếu chưa có
                    if [[ ! "$create_type_cql" =~ "IF NOT EXISTS" ]]; then
                        create_type_cql=$(echo "$create_type_cql" | sed 's/CREATE TYPE/CREATE TYPE IF NOT EXISTS/i')
                    fi
                    echo "Tạo UDT: $create_type_cql"
                    if echo "$create_type_cql" | cqlsh $SCYLLA_HOST 2>/dev/null; then
                        udt_count=$((udt_count + 1))
                    fi
                done < <(grep -i "CREATE TYPE" "$schema_file")
                
                if [ $udt_count -gt 0 ]; then
                    echo "Đã tạo $udt_count UDT(s)"
                else
                    echo "Cảnh báo: Không tìm thấy CREATE TYPE trong file schema. Có thể cần tạo UDT thủ công."
                fi
                continue
            else
                echo "Không thể tạo keyspace từ schema, thử với replication strategy mặc định..."
            fi
        fi
    fi
    
    # Nếu không có file schema hoặc không thể tạo từ schema, tạo với replication strategy mặc định
    echo "Tạo keyspace $keyspace với replication strategy mặc định..."
    create_keyspace_cql="CREATE KEYSPACE IF NOT EXISTS \"$keyspace\" WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"
    echo "$create_keyspace_cql" | cqlsh $SCYLLA_HOST
    if [ $? -eq 0 ]; then
        echo "Keyspace $keyspace đã được tạo thành công."
        
        # Nếu có file schema, thử tạo UDT từ đó
        if [ -f "$schema_file" ]; then
            echo "Đang tạo UDT (User Defined Types) từ file schema nếu có..."
            udt_count=0
            while IFS= read -r create_type_cql; do
                # Bỏ qua comment và empty lines
                create_type_cql=$(echo "$create_type_cql" | sed 's/^[[:space:]]*//' | sed 's/--.*$//')
                [ -z "$create_type_cql" ] && continue
                
                # Thêm IF NOT EXISTS nếu chưa có
                if [[ ! "$create_type_cql" =~ "IF NOT EXISTS" ]]; then
                    create_type_cql=$(echo "$create_type_cql" | sed 's/CREATE TYPE/CREATE TYPE IF NOT EXISTS/i')
                fi
                echo "Tạo UDT: $create_type_cql"
                if echo "$create_type_cql" | cqlsh $SCYLLA_HOST 2>/dev/null; then
                    udt_count=$((udt_count + 1))
                fi
            done < <(grep -i "CREATE TYPE" "$schema_file")
            
            if [ $udt_count -gt 0 ]; then
                echo "Đã tạo $udt_count UDT(s)"
            else
                echo "Cảnh báo: Không tìm thấy CREATE TYPE trong file schema. Có thể cần tạo UDT thủ công."
            fi
        fi
    else
        echo "Lỗi: Không thể tạo keyspace $keyspace"
        exit 1
    fi
done

# Bước 3: Khôi phục dữ liệu từ các file .json
shopt -s nullglob
for json_file in "${RESTORE_DIR}"/*.json; do
    [ ! -f "$json_file" ] && continue
    
    filename=$(basename "$json_file")
    # Bỏ qua file metadata (không phải table data)
    if [[ "$filename" == "backup-metadata.json" ]]; then
        continue
    fi

    echo "Đang xử lý file $filename ..."

    # Đọc keyspace và table từ file json
    keyspace=$(jq -r '.keyspace' "$json_file")
    table=$(jq -r '.table' "$json_file")

    # Chỉ khôi phục keyspace hợp lệ (nằm trong danh sách KEYSPACES)
    if [[ ! " ${KEYSPACES[@]} " =~ " ${keyspace} " ]]; then
        echo "Bỏ qua keyspace $keyspace không nằm trong danh sách cho phép."
        continue
    fi

    # Sinh lệnh CREATE TABLE từ schema
    create_table_cql=$(python3 - <<END
import json
with open("$json_file") as f:
    obj = json.load(f)
schema = obj.get("schema", [])
cols = []
pks = []
for col in schema:
    cname = col["column_name"]
    ctype = col["type"]
    ckind = col["kind"]
    cols.append(f'"{cname}" {ctype}')
    if ckind == "partition_key":
        pks.append(f'"{cname}"')
    elif ckind == "clustering":
        pks.append(f'"{cname}"')
col_clause = ", ".join(cols)
if len(pks) == 1:
    pk_part = pks[0]
else:
    pk_part = "(" + ", ".join(pks) + ")"
cql = f'CREATE TABLE IF NOT EXISTS "{obj["keyspace"]}"."{obj["table"]}" ({col_clause}, PRIMARY KEY ({pk_part}));'
print(cql)
END
)
    echo "Tạo bảng nếu chưa có: $keyspace.$table"
    create_table_output=$(echo "$create_table_cql" | cqlsh $SCYLLA_HOST 2>&1)
    create_table_result=$?
    
    # Kiểm tra nếu lỗi do thiếu UDT
    if [ $create_table_result -ne 0 ]; then
        if echo "$create_table_output" | grep -q "Unknown type"; then
            missing_udt=$(echo "$create_table_output" | grep -o "Unknown type [^ ]*" | cut -d' ' -f3)
            echo "Cảnh báo: Không thể tạo bảng $keyspace.$table - thiếu UDT: $missing_udt"
            echo "Vui lòng tạo UDT $missing_udt thủ công hoặc kiểm tra file schema."
            echo "Bỏ qua bảng $keyspace.$table"
            continue
        else
            echo "Lỗi khi tạo bảng: $create_table_output"
            continue
        fi
    fi

    # Lấy danh sách cột theo đúng thứ tự trong schema
    columns=($(jq -r '.schema[].column_name' "$json_file"))

    # Khôi phục dữ liệu sử dụng batch INSERT (đảm bảo đúng format, nhanh hơn INSERT từng row)
    row_count=$(jq '.data | length' "$json_file")
    if (( row_count > 0 )); then
        echo "Đang khôi phục $row_count rows cho $keyspace.$table ..."
        
        # Sử dụng batch INSERT với batch size nhỏ hơn và chạy từng batch
        # Tạo và thực thi từng batch để tránh timeout
        python3 <<END
import json
import subprocess

with open("$json_file", 'r') as f:
    data = json.load(f)

columns = [col["column_name"] for col in data.get("schema", [])]
rows = data.get("data", [])

# Batch size để tăng tốc độ (200 rows mỗi batch)
batch_size = 200
host = "$SCYLLA_HOST"

for i in range(0, len(rows), batch_size):
    batch = rows[i:i+batch_size]
    batch_num = (i // batch_size) + 1
    total_batches = (len(rows) + batch_size - 1) // batch_size
    
    # Tạo batch CQL
    batch_cql = "BEGIN BATCH\\n"
    for row in batch:
        values = []
        for col in columns:
            val = row.get(col)
            if val is None:
                values.append("NULL")
            elif isinstance(val, str):
                # Escape dấu nháy đơn trong string
                escaped = val.replace("'", "''")
                values.append(f"'{escaped}'")
            elif isinstance(val, bool):
                values.append(str(val).lower())
            elif isinstance(val, (list, dict)):
                import json as json_lib
                json_str = json_lib.dumps(val, ensure_ascii=False)
                escaped = json_str.replace("'", "''")
                values.append(f"'{escaped}'")
            else:
                values.append(str(val))
        
        cols_str = ", ".join([f'"{c}"' for c in columns])
        vals_str = ", ".join(values)
        batch_cql += f'INSERT INTO "{data["keyspace"]}"."{data["table"]}" ({cols_str}) VALUES ({vals_str});\\n'
    
    batch_cql += "APPLY BATCH;"
    
    # Thực thi batch
    try:
        result = subprocess.run(
            ['cqlsh', host],
            input=batch_cql,
            text=True,
            capture_output=True,
            timeout=60
        )
        if result.returncode == 0:
            print(f"Batch {batch_num}/{total_batches} completed ({len(batch)} rows)", flush=True)
        else:
            print(f"Error in batch {batch_num}/{total_batches}: {result.stderr}", flush=True)
            break
    except subprocess.TimeoutExpired:
        print(f"Timeout in batch {batch_num}/{total_batches}", flush=True)
        break
    except Exception as e:
        print(f"Error in batch {batch_num}/{total_batches}: {e}", flush=True)
        break

print(f"Completed {len(rows)} rows")
END
        
        if [ $? -eq 0 ]; then
            echo "Khôi phục thành công $row_count rows cho $keyspace.$table"
        else
            echo "Lỗi khi khôi phục dữ liệu cho $keyspace.$table"
        fi
    else
        echo "Không có dữ liệu để khôi phục cho $keyspace.$table"
    fi
done
shopt -u nullglob

# Dọn dẹp
echo "Dọn dẹp..."
# rm -rf $RESTORE_DIR

echo "Khôi phục hoàn thành từ backup: ${TIMESTAMP}.tar.gz"
