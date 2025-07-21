#!/bin/bash

basepath=$(cd `dirname $0`; pwd)
cd $basepath

# 从 appname 文件读取服务名称
if [ -f "appname" ]; then
    name=$(cat appname)
else
    echo "当前目录下未找到 appname 文件，请确保该文件存在。"
    exit 1
fi

#if [ "${runtime_env}" == "" ]; then
#    echo "please set env variable runtime_env={prod|test|dev}"
#    exit 0
#fi

# 读取 runtime.pid 文件并尝试杀死之前的进程
bash kill.sh

pid_file="$basepath/runtime.pid"

link_name=${name}_${runtime_env}

rm -fr $link_name
ln -s $name $link_name

exe=./$link_name
chmod +x $exe


# 定义更新配置文件日期的函数
update_config_dates() {
    local config_file="$1"

    if [ -z "$config_file" ]; then
        echo "错误：请提供配置文件路径作为参数"
        return 1  # 返回错误状态码
    fi

    if [ ! -f "$config_file" ]; then
        echo "错误：配置文件 $config_file 不存在"
        return 1  # 返回错误状态码
    fi

    local today=$(date +%Y%m%d)

    # 使用sed命令更新日期
    sed -i "s/\"date_start\": \".*\"/\"date_start\": \"$today\"/" "$config_file"
    if [ $? -ne 0 ]; then
        echo "错误：更新date_start失败"
        return 1  # 返回错误状态码
    fi

    sed -i "s/\"date_end\": \".*\"/\"date_end\": \"$today\"/" "$config_file"
    if [ $? -ne 0 ]; then
        echo "错误：更新date_end失败"
        return 1  # 返回错误状态码
    fi

    echo "配置文件日期已成功更新为 $today"
    return 0  # 返回成功状态码
}

# 调用函数，传入配置文件路径
update_config_dates "conf/config.daily.json"

# 检查函数返回值
if [ $? -ne 0 ]; then
    echo "脚本执行失败，已终止"
    exit 1  # 脚本以错误状态退出
fi

# 检查参数数量是否为1且参数值是否为 -d
if [ $# -eq 1 ] && [ "$1" = "-d" ]; then
    echo "即将执行: nohup $exe --config_file=conf/config.daily.json >nohup.out 2>&1 &"
    nohup $exe >nohup.out 2>&1 &
    pid=$!  # 获取后台进程的PID
    echo "$pid" > "$pid_file"
    echo "程序已在后台执行，进程ID为: $pid，已保存到 $pid_file"
else
    echo "即将执行: log_stdout=true $exe"
    log_stdout=true $exe --config_file=conf/config.daily.json
fi
