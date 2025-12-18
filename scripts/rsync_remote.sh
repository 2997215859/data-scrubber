#!/bin/bash
set -euo pipefail

# ===================== 默认配置（可被配置文件/命令行覆盖）=====================
# 本地基础目录（源数据）
LOCAL_BASE_DIR="/mnt/local/clean_stock_data"
# 远端机器配置（默认单主机，可配置为多个，用空格分隔）
REMOTE_HOSTS=("192.168.31.97")
REMOTE_PORT="22"  # SSH端口，默认22
REMOTE_USER="root"  # 远端用户名
REMOTE_PASSWORD="Milkt@2025"  # 密码包含特殊字符，保持原样即可
REMOTE_BASE_DIR="/mnt/local/clean_stock_data"  # 远端目录
# 日期范围（YYYYMMDD格式，留空则不限制）
START_DATE="20210101"
END_DATE="20210101"
# 日志文件路径
LOG_FILE="rsync_remote.log"
# 配置文件路径（命令行参数--config指定，默认空）
CONFIG_FILE=""
# ==============================================================================

# 显示帮助信息
show_help() {
    cat << EOF
用法：$0 [选项]
选项：
  --config <file>        指定配置文件路径（优先级高于默认配置，低于命令行参数）
  --local-base <dir>     本地基础目录（源数据）
  --remote-hosts <hosts> 远端主机列表（多个用空格分隔，需加引号，如"192.168.31.97 192.168.31.98"）
  --remote-port <port>   远端SSH端口
  --remote-user <user>   远端用户名
  --remote-pass <pass>   远端密码
  --remote-base <dir>    远端基础目录
  --start-date <date>    起始日期（YYYYMMDD，留空则不限制）
  --end-date <date>      结束日期（YYYYMMDD，留空则不限制）
  --log-file <file>      日志文件路径
  -h/--help              显示帮助信息

示例：
  # 使用配置文件
  $0 --config config.ini

  # 命令行指定参数
  $0 --local-base /data/stock --remote-hosts "192.168.31.97 192.168.31.98" --start-date 20240101
EOF
}

# 解析命令行参数
parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --config)
                CONFIG_FILE="$2"
                shift 2
                ;;
            --local-base)
                LOCAL_BASE_DIR="$2"
                shift 2
                ;;
            --remote-hosts)
                REMOTE_HOSTS=($2)
                shift 2
                ;;
            --remote-port)
                REMOTE_PORT="$2"
                shift 2
                ;;
            --remote-user)
                REMOTE_USER="$2"
                shift 2
                ;;
            --remote-pass)
                REMOTE_PASSWORD="$2"
                shift 2
                ;;
            --remote-base)
                REMOTE_BASE_DIR="$2"
                shift 2
                ;;
            --start-date)
                START_DATE="$2"
                shift 2
                ;;
            --end-date)
                END_DATE="$2"
                shift 2
                ;;
            --log-file)
                LOG_FILE="$2"
                shift 2
                ;;
            -h|--help)
                show_help
                exit 0
                ;;
            *)
                log "ERROR" "未知参数：$1"
                show_help
                exit 1
                ;;
        esac
    done
}

# 加载配置文件（ini格式，键=值）
load_config_file() {
    local config_file="$1"
    if [[ -f "${config_file}" ]]; then
        log "INFO" "加载配置文件：${config_file}"
        # 读取配置文件，忽略注释和空行
        while IFS='=' read -r key value; do
            # 跳过注释和空行
            [[ -z "${key}" || "${key}" =~ ^# || -z "${value}" ]] && continue
            # 去除首尾空格
            key=$(echo "${key}" | xargs)
            value=$(echo "${value}" | xargs)
            # 赋值到对应变量
            case "${key}" in
                LOCAL_BASE_DIR)
                    LOCAL_BASE_DIR="${value}"
                    ;;
                REMOTE_HOSTS)
                    REMOTE_HOSTS=(${value})
                    ;;
                REMOTE_PORT)
                    REMOTE_PORT="${value}"
                    ;;
                REMOTE_USER)
                    REMOTE_USER="${value}"
                    ;;
                REMOTE_PASSWORD)
                    REMOTE_PASSWORD="${value}"
                    ;;
                REMOTE_BASE_DIR)
                    REMOTE_BASE_DIR="${value}"
                    ;;
                START_DATE)
                    START_DATE="${value}"
                    ;;
                END_DATE)
                    END_DATE="${value}"
                    ;;
                LOG_FILE)
                    LOG_FILE="${value}"
                    ;;
                *)
                    log "WARNING" "配置文件中未知键：${key}"
                    ;;
            esac
        done < "${config_file}"
    else
        log "ERROR" "配置文件不存在：${config_file}"
        exit 1
    fi
}

# 初始化日志
init_log() {
    > "${LOG_FILE}"  # 清空旧日志
    echo "===== 脚本开始执行：$(date +'%Y-%m-%d %H:%M:%S') =====" >> "${LOG_FILE}"
}

# 定义日志函数（同时输出到控制台和日志文件）
log() {
    local LEVEL="${1}"
    local MESSAGE="${2}"
    local TIMESTAMP=$(date +'%Y-%m-%d %H:%M:%S')
    local LOG_LINE="${TIMESTAMP} - ${LEVEL} - ${MESSAGE}"
    echo -e "${LOG_LINE}"
    echo -e "${LOG_LINE}" >> "${LOG_FILE}"
}

# 定义：执行SSH命令的函数
run_ssh_command() {
    local remote_user="${1}"
    local remote_host="${2}"
    local remote_port="${3}"
    local remote_password="${4}"
    local command="${5}"

    if [ -n "${remote_password}" ]; then
        # 密码认证
        sshpass -p "${remote_password}" ssh -p "${remote_port}" \
            -o StrictHostKeyChecking=no \
            -o ServerAliveInterval=5 \
            -o TCPKeepAlive=yes \
            "${remote_user}@${remote_host}" "${command}"
    else
        # 密钥认证
        ssh -p "${remote_port}" \
            -o StrictHostKeyChecking=no \
            -o ServerAliveInterval=5 \
            -o TCPKeepAlive=yes \
            "${remote_user}@${remote_host}" "${command}"
    fi
}

# 定义：执行远程rsync命令的函数（本地到远端）
run_remote_rsync() {
    local local_path="${1}"
    local remote_user="${2}"
    local remote_host="${3}"
    local remote_port="${4}"
    local remote_password="${5}"
    local remote_path="${6}"

    if [ -n "${remote_password}" ]; then
        sshpass -p "${remote_password}" rsync -azP --partial -W \
            -e "ssh -p ${remote_port} \
                -o StrictHostKeyChecking=no \
                -o ServerAliveInterval=5 \
                -o TCPKeepAlive=yes" \
            "${local_path}" \
            "${remote_user}@${remote_host}:${remote_path}"
    else
        rsync -azP --partial -W \
            -e "ssh -p ${remote_port} \
                -o StrictHostKeyChecking=no \
                -o ServerAliveInterval=5 \
                -o TCPKeepAlive=yes" \
            "${local_path}" \
            "${remote_user}@${remote_host}:${remote_path}"
    fi
}

# 定义日期筛选函数
filter_date_dirs() {
    local dir="${1}"
    local start="${2}"
    local end="${3}"
    local date_dirs=()

    # 遍历目录下的子目录，筛选YYYYMMDD格式的目录
    for item in "${dir}"/*; do
        if [ -d "${item}" ]; then
            date_name=$(basename "${item}")
            # 验证是否为8位数字
            if [[ ${date_name} =~ ^[0-9]{8}$ ]]; then
                # 日期范围筛选
                if [[ -n ${start} && ${date_name} < ${start} ]]; then
                    continue
                fi
                if [[ -n ${end} && ${date_name} > ${end} ]]; then
                    continue
                fi
                date_dirs+=("${item}")
            else
                log "WARNING" "跳过非日期格式的目录：${date_name}"
            fi
        fi
    done

    # 排序后输出
    printf "%s\n" "${date_dirs[@]}" | sort
}

# 本地到远端的拷贝函数（单主机）
copy_to_remote() {
    local remote_host="${1}"
    local dir_name="${2}"
    local local_dir="${LOCAL_BASE_DIR}/${dir_name}"
    local remote_dir="${REMOTE_BASE_DIR}/${dir_name}"

    # 检查本地目录是否存在
    if [ ! -d "${local_dir}" ]; then
        log "ERROR" "本地目录 ${local_dir} 不存在，跳过"
        return 1
    fi

    # 1. 创建远端目录（如果不存在）
    log "INFO" "为远端主机 ${remote_host} 创建目录 ${remote_dir}（如果不存在）"
    run_ssh_command "${REMOTE_USER}" "${remote_host}" "${REMOTE_PORT}" "${REMOTE_PASSWORD}" "mkdir -p ${remote_dir}" >> "${LOG_FILE}" 2>&1
    if [ $? -ne 0 ]; then
        log "ERROR" "为远端主机 ${remote_host} 创建目录 ${remote_dir} 失败，跳过"
        return 1
    fi

    # 2. 筛选日期目录
    log "INFO" "开始筛选 ${local_dir} 下的日期目录（范围：${START_DATE:-最早} - ${END_DATE:-最晚}）"
    date_dirs=($(filter_date_dirs "${local_dir}" "${START_DATE}" "${END_DATE}"))
    if [ ${#date_dirs[@]} -eq 0 ]; then
        log "INFO" "没有找到符合日期范围的目录，跳过"
        return 0
    fi

    log "INFO" "筛选后剩余 ${#date_dirs[@]} 个日期目录，开始向 ${remote_host} 上传"

    # 3. 逐个上传日期目录
    total=${#date_dirs[@]}
    index=1
    for local_date_dir in "${date_dirs[@]}"; do
        date_name=$(basename "${local_date_dir}")
        log "INFO" "\n===== 向 ${remote_host} 上传 [${index}/${total}]：${date_name} ====="

        # 执行rsync上传（注意路径末尾的/，保证目录结构一致）
        run_remote_rsync \
            "${local_date_dir}/" \
            "${REMOTE_USER}" \
            "${remote_host}" \
            "${REMOTE_PORT}" \
            "${REMOTE_PASSWORD}" \
            "${remote_dir}/${date_name}/" >> "${LOG_FILE}" 2>&1

        if [ $? -eq 0 ]; then
            log "INFO" "成功向 ${remote_host} 上传 ${date_name}"
        else
            log "ERROR" "向 ${remote_host} 上传 ${date_name} 失败（查看日志文件${LOG_FILE}获取详情）"
        fi

        index=$((index + 1))
    done

    log "INFO" "向 ${remote_host} 的 ${dir_name} 目录上传完成"
    return 0
}

# 主执行逻辑
main() {
    # 1. 解析命令行参数
    parse_args "$@"

    # 2. 初始化日志（需在加载配置文件前，因为配置文件可能修改LOG_FILE）
    init_log

    # 3. 加载配置文件（如果指定）
    if [ -n "${CONFIG_FILE}" ]; then
        load_config_file "${CONFIG_FILE}"
        # 重新初始化日志（如果配置文件修改了LOG_FILE）
        init_log
    fi

    # 4. 检查必要命令
    local required_cmds=("ssh" "rsync")
    if [ -n "${REMOTE_PASSWORD}" ]; then
        required_cmds+=("sshpass")
    fi

    for cmd in "${required_cmds[@]}"; do
        if ! command -v "${cmd}" &> /dev/null; then
            log "ERROR" "未找到${cmd}命令，请安装：sudo apt install ${cmd}（Debian/Ubuntu）或 sudo yum install ${cmd}（CentOS）"
            exit 1
        fi
    done

    # 5. 测试远端SSH连接（遍历所有主机）
    for remote_host in "${REMOTE_HOSTS[@]}"; do
        log "INFO" "测试与远端机器 ${remote_host}:${REMOTE_PORT} 的SSH连接"
        run_ssh_command "${REMOTE_USER}" "${remote_host}" "${REMOTE_PORT}" "${REMOTE_PASSWORD}" "echo 'SSH连接成功'" >> "${LOG_FILE}" 2>&1
        if [ $? -ne 0 ]; then
            log "ERROR" "SSH连接远端机器 ${remote_host}:${REMOTE_PORT} 失败，请检查配置（用户名/密码/端口/IP）"
            exit 1
        fi
        log "INFO" "SSH连接 ${remote_host} 测试成功！"
    done

    # 6. 同步snapshot和trade目录到远端所有主机
    for dir_name in "snapshot" "trade"; do
        for remote_host in "${REMOTE_HOSTS[@]}"; do
            copy_to_remote "${remote_host}" "${dir_name}"
        done
    done

    log "INFO" "所有任务执行完成，日志文件：${LOG_FILE}"
    exit 0
}

# 启动主函数
main "$@"