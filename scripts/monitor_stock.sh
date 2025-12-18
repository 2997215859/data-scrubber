#!/bin/bash

# 检查是否提供了日期参数，没有则使用当前日期
if [ $# -eq 1 ]; then
    DATE_PARAM="$1"
else
    # 使用当前日期作为默认值，格式为YYYYMMDD
    DATE_PARAM=$(date +%Y%m%d)
    echo "未提供日期参数，将使用当前日期: $DATE_PARAM"
fi

# ===================== 新增：生成配置文件 config.daily.json =====================
# 配置文件存放路径（与 COMMAND 中 --config_file 对应：dest/conf/config.daily.json）
CONFIG_FILE="/home/leewind/workspace/ruiy/code/data-scrubber/dest/conf/config.daily.json"
# 源目录（与原始脚本中 DONE_FILE 的上级目录一致）
SRC_DIR="/mnt/share/tick_stock"

# 创建配置文件的父目录（确保 conf 目录存在）
mkdir -p "$(dirname "$CONFIG_FILE")"

# 生成配置文件（使用当前 DATE_PARAM 作为日期参数）
cat << EOF > "$CONFIG_FILE"
{
  "src_dir": "$SRC_DIR",
  "dst_dir": "/mnt/share/clean_stock_data",
  "date_start": "$DATE_PARAM",
  "date_end": "$DATE_PARAM",
  "data_type_list": [ "snapshot", "trade" ]
}
EOF

echo "已生成配置文件: $CONFIG_FILE（日期: $DATE_PARAM）"
# ==============================================================================

# 要监控的文件路径，使用传入的日期参数
DONE_FILE="/mnt/share/tick_stock/$DATE_PARAM/done"
# 要执行的命令（保持原样，使用上面生成的 config.daily.json）
COMMAND="cd /home/leewind/workspace/ruiy/code/data-scrubber/dest; ./data-scrubber --config_file conf/config.daily.json"
# 检查间隔（秒），5分钟=300秒
INTERVAL=300

echo "开始监控文件: $DONE_FILE"
echo "检查间隔: $INTERVAL 秒"
echo "文件存在时将执行: $COMMAND"

while true; do
    # 检查文件是否存在
    if [ -f "$DONE_FILE" ]; then
        echo "检测到文件 $DONE_FILE 存在，开始执行命令..."
        # 执行命令
        eval $COMMAND
        echo "命令执行完成，脚本退出"
        exit 0
    else
        echo "文件 $DONE_FILE 不存在，$INTERVAL 秒后再次检查..."
        sleep $INTERVAL
    fi
done
