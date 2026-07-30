#!/usr/bin/env bash
#
# deploy_kafka.sh - 自动化部署 Kafka 单机版 (kafka_2.13-3.9.0, KRaft 模式)
#
# 用法:
#   ./deploy_kafka.sh [选项]
#
# 选项:
#   -i, --install-dir DIR   Kafka 安装目录   (默认: /opt/kafka)
#   -d, --data-dir DIR      Kafka 数据目录   (默认: /var/lib/kafka/data)
#   -p, --port PORT         Broker 监听端口  (默认: 9092)
#   -H, --advertised-host H 对外宣告地址(客户端连接用,默认自动探测内网 IP)
#   -f, --package FILE      本地安装包路径(离线部署,提供后跳过下载)
#   -m, --mirror URL        下载镜像地址前缀 (默认: Apache 官方归档站)
#   -S, --sasl              启用 SASL/PLAIN 认证(默认关闭,明文无认证)
#   -U, --sasl-user USER    SASL 用户名     (默认: admin,仅 --sasl 时生效)
#   -P, --sasl-password PWD SASL 密码       (默认: 随机生成并打印,仅 --sasl 时生效)
#   -h, --help              显示帮助信息
#
# 示例:
#   在线: ./deploy_kafka.sh -i /usr/local/kafka -d /data/kafka
#   离线: ./deploy_kafka.sh -f ./kafka_2.13-3.9.0.tgz -i /usr/local/kafka  -d /usr/local/kafka/data
#   认证: ./deploy_kafka.sh --sasl -U admin -P 'MyS3cret!'
#
set -euo pipefail

# ---------------------------------------------------------------------------
# 默认配置
# ---------------------------------------------------------------------------
KAFKA_VERSION="3.9.0"
SCALA_VERSION="2.13"
PKG_NAME="kafka_${SCALA_VERSION}-${KAFKA_VERSION}"
TARBALL="${PKG_NAME}.tgz"

INSTALL_DIR="/opt/kafka"
DATA_DIR="/var/lib/kafka/data"
BROKER_PORT="9092"
ADVERTISED_HOST=""   # 对外宣告地址,留空时自动探测内网 IP
PACKAGE=""   # 本地安装包路径,非空时启用离线部署
SASL_ENABLED="false"   # 是否启用 SASL/PLAIN 认证
SASL_USER="admin"      # SASL 用户名
SASL_PASSWORD=""       # SASL 密码,留空且启用 SASL 时自动生成
# 官方归档地址,可用 --mirror 替换为国内镜像(如 https://mirrors.aliyun.com/apache/kafka/${KAFKA_VERSION})
MIRROR="https://archive.apache.org/dist/kafka/${KAFKA_VERSION}"

# ---------------------------------------------------------------------------
# 工具函数
# ---------------------------------------------------------------------------
log()  { echo -e "\033[0;32m[INFO]\033[0m  $*"; }
warn() { echo -e "\033[0;33m[WARN]\033[0m  $*"; }
err()  { echo -e "\033[0;31m[ERROR]\033[0m $*" >&2; }

die() { err "$*"; exit 1; }

usage() {
    # 打印文件头部注释块(从第 2 行到 set 之前)
    sed -n '2,/^set -euo/p' "$0" | grep '^#' | sed 's/^# \{0,1\}//'
    exit 0
}

# ---------------------------------------------------------------------------
# 解析命令行参数
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        -i|--install-dir) INSTALL_DIR="$2"; shift 2 ;;
        -d|--data-dir)    DATA_DIR="$2";    shift 2 ;;
        -p|--port)            BROKER_PORT="$2";     shift 2 ;;
        -H|--advertised-host) ADVERTISED_HOST="$2"; shift 2 ;;
        -f|--package)         PACKAGE="$2";         shift 2 ;;
        -m|--mirror)      MIRROR="$2";      shift 2 ;;
        -S|--sasl)            SASL_ENABLED="true";  shift 1 ;;
        -U|--sasl-user)       SASL_USER="$2";       shift 2 ;;
        -P|--sasl-password)   SASL_PASSWORD="$2";   shift 2 ;;
        -h|--help)        usage ;;
        *) die "未知参数: $1 (使用 -h 查看帮助)" ;;
    esac
done

# 解压时剥离顶层目录,Kafka 文件直接落在 INSTALL_DIR 下
KAFKA_HOME="${INSTALL_DIR}"

# 未显式指定宣告地址时,自动探测内网 IP(失败则回退 localhost)
if [[ -z "${ADVERTISED_HOST}" ]]; then
    ADVERTISED_HOST="$(hostname -I 2>/dev/null | awk '{print $1}')"
    if [[ -z "${ADVERTISED_HOST}" ]]; then
        ADVERTISED_HOST="localhost"
        warn "未能自动探测到内网 IP,回退使用 localhost(仅本机可访问)"
    fi
fi

# 生成随机密码(避免 /dev/urandom | head 在 pipefail 下的 SIGPIPE 问题,改用 bash RANDOM)
gen_password() {
    local chars='ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnpqrstuvwxyz23456789'
    local pw="" i
    for ((i = 0; i < 20; i++)); do
        pw+="${chars:RANDOM%${#chars}:1}"
    done
    printf '%s' "${pw}"
}

# SASL 相关处理:启用时校验用户名、按需生成密码
SASL_PASSWORD_GENERATED="false"
if [[ "${SASL_ENABLED}" == "true" ]]; then
    [[ -n "${SASL_USER}" ]] || die "启用 SASL 时用户名不能为空(-U/--sasl-user)"
    if [[ -z "${SASL_PASSWORD}" ]]; then
        SASL_PASSWORD="$(gen_password)"
        SASL_PASSWORD_GENERATED="true"
    fi
fi

# ---------------------------------------------------------------------------
# 前置检查
# ---------------------------------------------------------------------------
log "开始部署 ${PKG_NAME} (KRaft 模式)"
log "安装目录: ${INSTALL_DIR}"
log "数据目录: ${DATA_DIR}"
log "监听端口: ${BROKER_PORT}"
log "宣告地址: ${ADVERTISED_HOST}:${BROKER_PORT}"
if [[ "${SASL_ENABLED}" == "true" ]]; then
    log "认证方式: SASL/PLAIN (用户: ${SASL_USER})"
else
    log "认证方式: 无 (PLAINTEXT 明文)"
fi

# 检查 root / sudo 权限(创建系统目录、注册 systemd 服务需要)
SUDO=""
if [[ "$(id -u)" -ne 0 ]]; then
    if command -v sudo >/dev/null 2>&1; then
        SUDO="sudo"
        warn "当前非 root 用户,涉及系统目录的操作将使用 sudo"
    else
        die "需要 root 权限或安装 sudo"
    fi
fi

# 检查 Java
if ! command -v java >/dev/null 2>&1; then
    die "未检测到 Java。Kafka ${KAFKA_VERSION} 需要 Java 8+ (推荐 Java 11/17),请先安装 JDK。"
fi
JAVA_VER="$(java -version 2>&1 | head -n1)"
log "检测到 Java: ${JAVA_VER}"

# 离线包预检 / 下载工具检查
if [[ -n "${PACKAGE}" ]]; then
    # 离线模式:校验本地包存在,无需下载工具
    [[ -f "${PACKAGE}" ]] || die "指定的安装包不存在: ${PACKAGE}"
    PACKAGE="$(cd "$(dirname "${PACKAGE}")" && pwd)/$(basename "${PACKAGE}")"  # 转绝对路径
    log "离线部署模式,使用本地安装包: ${PACKAGE}"
else
    # 在线模式:需要 curl 或 wget
    if command -v curl >/dev/null 2>&1; then
        DOWNLOADER="curl -fSL --retry 3 -o"
    elif command -v wget >/dev/null 2>&1; then
        DOWNLOADER="wget -O"
    else
        die "未检测到 curl 或 wget,无法下载安装包(或使用 -f 指定本地包离线部署)。"
    fi
fi

# ---------------------------------------------------------------------------
# 下载并解压
# ---------------------------------------------------------------------------
WORK_DIR="$(mktemp -d)"
trap 'rm -rf "${WORK_DIR}"' EXIT

if [[ -f "${KAFKA_HOME}/bin/kafka-server-start.sh" ]]; then
    warn "目标目录已存在 Kafka: ${KAFKA_HOME},跳过获取与解压"
else
    if [[ -n "${PACKAGE}" ]]; then
        # 离线:直接使用本地包
        SRC_TARBALL="${PACKAGE}"
    else
        # 在线:下载到临时目录
        log "下载 ${TARBALL} ..."
        if ! ${DOWNLOADER} "${WORK_DIR}/${TARBALL}" "${MIRROR}/${TARBALL}"; then
            die "下载失败: ${MIRROR}/${TARBALL}"
        fi
        SRC_TARBALL="${WORK_DIR}/${TARBALL}"
    fi

    # 一次性完整读取归档列表:既校验完整性,又取顶层目录。
    # 注意:不能用 `tar -tzf | head -n1`,head 提前关管道会让 tar 收到 SIGPIPE,
    # 在 set -o pipefail + set -e 下会导致脚本静默退出。这里整段读入再用参数展开取首行。
    if ! TARLIST="$(tar -tzf "${SRC_TARBALL}" 2>/dev/null)"; then
        die "安装包损坏或不是有效的 tgz 文件: ${SRC_TARBALL}"
    fi
    FIRST_LINE="${TARLIST%%$'\n'*}"   # 取第一行
    TOP_DIR="${FIRST_LINE%%/*}"       # 取第一行的顶层目录名
    [[ "${TOP_DIR}" == "${PKG_NAME}" ]] || \
        die "安装包顶层目录为 '${TOP_DIR}',与预期 '${PKG_NAME}' 不符,请确认是 ${PKG_NAME}.tgz"

    log "创建安装目录并解压(剥离顶层目录)..."
    ${SUDO} mkdir -p "${INSTALL_DIR}"
    # --strip-components=1 去掉顶层 ${PKG_NAME}/,内容直接落到 INSTALL_DIR
    ${SUDO} tar -xzf "${SRC_TARBALL}" -C "${INSTALL_DIR}" --strip-components=1
    [[ -f "${KAFKA_HOME}/bin/kafka-server-start.sh" ]] || \
        die "解压后未找到 ${KAFKA_HOME}/bin/kafka-server-start.sh,解压可能失败"
    log "已解压到 ${KAFKA_HOME}"
fi

# ---------------------------------------------------------------------------
# 创建数据目录
# ---------------------------------------------------------------------------
log "创建数据目录: ${DATA_DIR}"
${SUDO} mkdir -p "${DATA_DIR}"

# ---------------------------------------------------------------------------
# 配置 KRaft 单机模式
# ---------------------------------------------------------------------------
SERVER_PROPS="${KAFKA_HOME}/config/kraft/server.properties"
[[ -f "${SERVER_PROPS}" ]] || die "未找到配置文件: ${SERVER_PROPS}"

log "写入 KRaft 单机配置 ..."
# 备份原始配置
${SUDO} cp -n "${SERVER_PROPS}" "${SERVER_PROPS}.orig" 2>/dev/null || true

# 根据是否启用 SASL 决定 broker 监听器协议与认证片段。
# 说明:仅 broker 对外监听启用 SASL/PLAIN;CONTROLLER 监听为单机本地自连,保持 PLAINTEXT。
if [[ "${SASL_ENABLED}" == "true" ]]; then
    BROKER_LISTENER="SASL_PLAINTEXT"
    # JAAS 直接内联到 server.properties(无需额外的 KAFKA_OPTS / 独立 jaas 文件)。
    # username/password 为 broker 作为客户端做 inter-broker 认证时使用;
    # user_<name>=<pwd> 声明允许登录的账号(此处即 broker 自身账号)。
    SASL_CONF="
# === SASL/PLAIN 认证 (由 deploy_kafka.sh 自动生成) ===
sasl.enabled.mechanisms=PLAIN
sasl.mechanism.inter.broker.protocol=PLAIN
listener.name.sasl_plaintext.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"${SASL_USER}\" password=\"${SASL_PASSWORD}\" user_${SASL_USER}=\"${SASL_PASSWORD}\";

# === ACL 授权 (由 deploy_kafka.sh 自动生成) ===
# 仅认证(SASL)只能验明身份,不限制操作;开启 authorizer 后才真正做权限管控。
# KRaft 模式使用 StandardAuthorizer;将 SASL 账号设为超级用户(super.users),授予其全部权限。
# User:ANONYMOUS 用于 PLAINTEXT 的 CONTROLLER 监听器(单机本地自连),不加会导致控制器请求被拒、broker 无法启动。
# allow.everyone.if.no.acl.found=false:未显式授权的其他账号一律拒绝(默认拒绝)。
authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer
super.users=User:${SASL_USER};User:ANONYMOUS
allow.everyone.if.no.acl.found=false
"
else
    BROKER_LISTENER="PLAINTEXT"
    SASL_CONF=""
fi

# 使用临时文件渲染配置后再覆盖(避免 sudo 重定向问题)
TMP_CONF="${WORK_DIR}/server.properties"
cat > "${TMP_CONF}" <<EOF
# === 由 deploy_kafka.sh 自动生成 (KRaft 单机模式) ===

# 单节点同时承担 broker 和 controller 角色
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9093

# 监听配置
# CONTROLLER 仅绑定 localhost:单机 controller 本地自连,无需对外暴露。
# 这样即便启用 SASL 时把 User:ANONYMOUS 设为超级用户,该权限也只能经本机回环访问,外部够不着。
listeners=${BROKER_LISTENER}://:${BROKER_PORT},CONTROLLER://localhost:9093
inter.broker.listener.name=${BROKER_LISTENER}
advertised.listeners=${BROKER_LISTENER}://${ADVERTISED_HOST}:${BROKER_PORT}
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SSL:SSL,SASL_PLAINTEXT:SASL_PLAINTEXT,SASL_SSL:SASL_SSL
${SASL_CONF}
# 网络与 IO 线程
num.network.threads=3
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600

# 数据目录
log.dirs=${DATA_DIR}

# 单机环境分区与副本设置
num.partitions=1
num.recovery.threads.per.data.dir=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1

# 日志保留
log.retention.hours=168
log.retention.check.interval.ms=300000
log.segment.bytes=1073741824
EOF

${SUDO} cp "${TMP_CONF}" "${SERVER_PROPS}"

# 启用 SASL 时,生成客户端认证配置文件,供 kafka-*.sh 工具用 --command-config 连接
CLIENT_PROPS=""
if [[ "${SASL_ENABLED}" == "true" ]]; then
    CLIENT_PROPS="${KAFKA_HOME}/config/client-sasl.properties"
    TMP_CLIENT="${WORK_DIR}/client-sasl.properties"
    cat > "${TMP_CLIENT}" <<EOF
# === 客户端 SASL/PLAIN 连接配置 (由 deploy_kafka.sh 自动生成) ===
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="${SASL_USER}" password="${SASL_PASSWORD}";
EOF
    ${SUDO} cp "${TMP_CLIENT}" "${CLIENT_PROPS}"
    ${SUDO} chmod 600 "${CLIENT_PROPS}"
    log "已生成客户端认证配置: ${CLIENT_PROPS}"
fi

# ---------------------------------------------------------------------------
# 格式化存储目录 (KRaft 必需)
# ---------------------------------------------------------------------------
# 通过是否存在 meta.properties 判断是否已格式化,避免重复格式化导致 cluster id 改变
if [[ -f "${DATA_DIR}/meta.properties" ]] || ${SUDO} test -f "${DATA_DIR}/meta.properties"; then
    warn "数据目录已格式化,跳过 storage format"
else
    CLUSTER_ID="$(${KAFKA_HOME}/bin/kafka-storage.sh random-uuid)"
    log "生成 Cluster ID: ${CLUSTER_ID}"
    ${SUDO} "${KAFKA_HOME}/bin/kafka-storage.sh" format \
        -t "${CLUSTER_ID}" \
        -c "${SERVER_PROPS}"
    log "存储目录格式化完成"
fi

# ---------------------------------------------------------------------------
# 注册 systemd 服务
# ---------------------------------------------------------------------------
SERVICE_FILE="/etc/systemd/system/kafka.service"
if command -v systemctl >/dev/null 2>&1; then
    log "注册 systemd 服务: ${SERVICE_FILE}"
    JAVA_BIN_DIR="$(dirname "$(command -v java)")"
    TMP_SVC="${WORK_DIR}/kafka.service"
    cat > "${TMP_SVC}" <<EOF
[Unit]
Description=Apache Kafka (KRaft mode)
Documentation=https://kafka.apache.org/documentation/
After=network.target

[Service]
Type=simple
Environment="JAVA_HOME=$(dirname "${JAVA_BIN_DIR}")"
ExecStart=${KAFKA_HOME}/bin/kafka-server-start.sh ${SERVER_PROPS}
ExecStop=${KAFKA_HOME}/bin/kafka-server-stop.sh
Restart=on-failure
RestartSec=5
LimitNOFILE=100000

[Install]
WantedBy=multi-user.target
EOF
    ${SUDO} cp "${TMP_SVC}" "${SERVICE_FILE}"
    ${SUDO} systemctl daemon-reload
    ${SUDO} systemctl enable kafka >/dev/null 2>&1 || true

    log "启动 Kafka 服务 ..."
    ${SUDO} systemctl restart kafka

    sleep 3
    if ${SUDO} systemctl is-active --quiet kafka; then
        log "Kafka 服务已启动 (systemctl status kafka 查看状态)"
    else
        err "Kafka 服务启动失败,请查看: journalctl -u kafka -e"
        exit 1
    fi
else
    warn "未检测到 systemd,跳过服务注册。可手动启动:"
    echo "  ${KAFKA_HOME}/bin/kafka-server-start.sh -daemon ${SERVER_PROPS}"
fi

# ---------------------------------------------------------------------------
# 完成提示
# ---------------------------------------------------------------------------
# 验证命令中,启用 SASL 时需要附带 --command-config 客户端配置
CMD_CFG=""
if [[ "${SASL_ENABLED}" == "true" ]]; then
    CMD_CFG=" --command-config ${CLIENT_PROPS}"
fi

cat <<EOF

========================================================================
 Kafka ${KAFKA_VERSION} 单机版部署完成!
------------------------------------------------------------------------
 安装目录 : ${KAFKA_HOME}
 数据目录 : ${DATA_DIR}
 配置文件 : ${SERVER_PROPS}
 监听端口 : 0.0.0.0:${BROKER_PORT} (绑定所有网卡)
 客户端连 : ${ADVERTISED_HOST}:${BROKER_PORT} (advertised.listeners)
EOF

if [[ "${SASL_ENABLED}" == "true" ]]; then
cat <<EOF
 认证方式 : SASL_PLAINTEXT / PLAIN
   用户名 : ${SASL_USER}
   密码   : ${SASL_PASSWORD}$([[ "${SASL_PASSWORD_GENERATED}" == "true" ]] && printf '  (随机生成,请妥善保存)')
 客户端配置文件 : ${CLIENT_PROPS}
   连接工具时附加参数: --command-config ${CLIENT_PROPS}
EOF
else
cat <<EOF
 认证方式 : 无 (PLAINTEXT 明文,仅适用于可信内网)
EOF
fi

cat <<EOF

 常用命令:
   启动:  systemctl start kafka
   停止:  systemctl stop kafka
   状态:  systemctl status kafka
   日志:  journalctl -u kafka -f

 验证(创建并查看 topic):
   ${KAFKA_HOME}/bin/kafka-topics.sh --create --topic test \\
       --bootstrap-server ${ADVERTISED_HOST}:${BROKER_PORT} --partitions 1 --replication-factor 1${CMD_CFG}
   ${KAFKA_HOME}/bin/kafka-topics.sh --list --bootstrap-server ${ADVERTISED_HOST}:${BROKER_PORT}${CMD_CFG}
========================================================================
EOF
