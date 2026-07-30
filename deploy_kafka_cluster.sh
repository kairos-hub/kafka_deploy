#!/usr/bin/env bash
#
# deploy_kafka_cluster.sh - 自动化部署 Kafka 多节点集群 (kafka_2.13-3.9.0, KRaft 模式)
#
# 每台节点各执行一次本脚本,通过 --nodes 描述整个集群、--node-id 指明本机身份,
# 节点各自完成配置(无需 SSH 免密)。所有节点为 broker+controller 合体角色,
# 共同组成 controller 仲裁(quorum)。
#
# 用法:
#   ./deploy_kafka_cluster.sh -N <节点列表> -n <本机ID> [选项]
#
# 必填:
#   -N, --nodes LIST        集群所有节点 id@host,逗号分隔
#                           例: 1@10.0.0.1,2@10.0.0.2,3@10.0.0.3
#   -n, --node-id ID        本机节点 id(必须出现在 --nodes 列表中)
#
# 集群一致性(多节点必须在各节点保持一致):
#   -c, --cluster-id UUID   集群 ID(不填则自动生成并打印;第 2 台起必须用第 1 台打印出的值)
#   -P, --sasl-password PWD 管理员密码(启用 SASL 时,所有节点必须相同;不填则随机生成并打印)
#
# 目录与网络:
#   -i, --install-dir DIR   Kafka 安装目录        (默认: /opt/kafka)
#   -d, --data-dir DIR      Kafka 数据目录        (默认: /var/lib/kafka/data)
#   -p, --port PORT         broker 监听端口       (默认: 9092)
#       --controller-port P controller 监听端口   (默认: 9093)
#   -H, --advertised-host H 本机对客户端宣告地址  (默认: 取 --nodes 中本机对应 host)
#
# 安装包:
#   -f, --package FILE      本地安装包路径(离线部署,提供后跳过下载)
#   -m, --mirror URL        下载镜像地址前缀      (默认: Apache 官方归档站)
#
# 副本与分区(默认按节点数推导):
#   -r, --replication-factor N  内部/默认副本因子 (默认: min(3, 节点数))
#       --min-isr N             最小同步副本      (默认: RF>=3 ? 2 : 1)
#       --partitions N          默认分区数        (默认: 3)
#
# SASL 与授权(默认开启 SASL/PLAIN + ACL 授权):
#       --no-sasl           关闭 SASL(明文无认证,同时自动关闭授权)
#   -U, --sasl-user USER    管理员账号            (默认: admin)
#       --extra-user U:P    追加 SASL 账号(可重复,如 --extra-user app:appPwd)
#       --super-users LIST  覆盖 super.users(默认: User:<管理员账号>)
#       --no-authz          关闭 ACL 授权(仅保留 SASL 认证,不做权限管控)
#       --allow-everyone    未授权默认放行(默认: 拒绝)
#       --render-only DIR   仅把 server.properties/client 配置渲染到 DIR 后退出
#                           (不下载/不格式化/不注册服务,用于离线校验配置)
#   -h, --help              显示帮助信息
#
# 典型三节点流程:
#   # 节点1(会打印 Cluster ID)
#   ./deploy_kafka_cluster.sh -N 1@10.0.0.1,2@10.0.0.2,3@10.0.0.3 -n 1 -P 'AdminP@ss'
#   # 节点2 / 节点3(用节点1打印的 cluster-id)
#   ./deploy_kafka_cluster.sh -N 1@10.0.0.1,2@10.0.0.2,3@10.0.0.3 -n 2 -P 'AdminP@ss' -c <ClusterID>
#   ./deploy_kafka_cluster.sh -N 1@10.0.0.1,2@10.0.0.2,3@10.0.0.3 -n 3 -P 'AdminP@ss' -c <ClusterID>
#
set -euo pipefail

# ---------------------------------------------------------------------------
# 默认配置
# ---------------------------------------------------------------------------
KAFKA_VERSION="3.9.0"
SCALA_VERSION="2.13"
PKG_NAME="kafka_${SCALA_VERSION}-${KAFKA_VERSION}"
TARBALL="${PKG_NAME}.tgz"

NODES=""               # 集群节点列表 id@host,逗号分隔(必填)
NODE_ID=""             # 本机节点 id(必填)
CLUSTER_ID=""          # 集群 ID,留空自动生成
INSTALL_DIR="/opt/kafka"
DATA_DIR="/var/lib/kafka/data"
BROKER_PORT="9092"
CONTROLLER_PORT="9093"
ADVERTISED_HOST=""     # 对客户端宣告地址,留空取 --nodes 中本机 host
PACKAGE=""             # 本地安装包路径,非空启用离线部署
MIRROR="https://archive.apache.org/dist/kafka/${KAFKA_VERSION}"

REPLICATION_FACTOR=""  # 留空则按节点数推导
MIN_ISR=""             # 留空则按副本因子推导
PARTITIONS="3"

SASL_ENABLED="true"    # 集群脚本默认开启 SASL/PLAIN
SASL_USER="admin"
SASL_PASSWORD=""
declare -a EXTRA_USERS=()   # 追加账号 user:pass
SUPER_USERS=""         # 留空默认 User:<SASL_USER>
AUTHZ_ENABLED="true"   # 默认开启 ACL 授权
ALLOW_EVERYONE="false" # 未授权是否默认放行
RENDER_ONLY=""         # 非空:仅把 server.properties 渲染到该目录后退出(不下载/格式化/起服务)

# ---------------------------------------------------------------------------
# 工具函数
# ---------------------------------------------------------------------------
log()  { echo -e "\033[0;32m[INFO]\033[0m  $*"; }
warn() { echo -e "\033[0;33m[WARN]\033[0m  $*"; }
err()  { echo -e "\033[0;31m[ERROR]\033[0m $*" >&2; }
die()  { err "$*"; exit 1; }

usage() {
    sed -n '2,/^set -euo/p' "$0" | grep '^#' | sed 's/^# \{0,1\}//'
    exit 0
}

# ---------------------------------------------------------------------------
# 解析命令行参数
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        -N|--nodes)             NODES="$2";              shift 2 ;;
        -n|--node-id)           NODE_ID="$2";            shift 2 ;;
        -c|--cluster-id)        CLUSTER_ID="$2";         shift 2 ;;
        -i|--install-dir)       INSTALL_DIR="$2";        shift 2 ;;
        -d|--data-dir)          DATA_DIR="$2";           shift 2 ;;
        -p|--port)              BROKER_PORT="$2";        shift 2 ;;
        --controller-port)      CONTROLLER_PORT="$2";    shift 2 ;;
        -H|--advertised-host)   ADVERTISED_HOST="$2";    shift 2 ;;
        -f|--package)           PACKAGE="$2";            shift 2 ;;
        -m|--mirror)            MIRROR="$2";             shift 2 ;;
        -r|--replication-factor) REPLICATION_FACTOR="$2"; shift 2 ;;
        --min-isr)              MIN_ISR="$2";            shift 2 ;;
        --partitions)           PARTITIONS="$2";         shift 2 ;;
        --no-sasl)              SASL_ENABLED="false";    shift 1 ;;
        -U|--sasl-user)         SASL_USER="$2";          shift 2 ;;
        -P|--sasl-password)     SASL_PASSWORD="$2";      shift 2 ;;
        --extra-user)           EXTRA_USERS+=("$2");     shift 2 ;;
        --super-users)          SUPER_USERS="$2";        shift 2 ;;
        --no-authz)             AUTHZ_ENABLED="false";   shift 1 ;;
        --allow-everyone)       ALLOW_EVERYONE="true";   shift 1 ;;
        --render-only)          RENDER_ONLY="$2";        shift 2 ;;
        -h|--help)              usage ;;
        *) die "未知参数: $1 (使用 -h 查看帮助)" ;;
    esac
done

KAFKA_HOME="${INSTALL_DIR}"

# ---------------------------------------------------------------------------
# 校验并解析集群拓扑
# ---------------------------------------------------------------------------
[[ -n "${NODES}" ]]   || die "缺少 --nodes 集群节点列表(如 1@10.0.0.1,2@10.0.0.2,3@10.0.0.3)"
[[ -n "${NODE_ID}" ]] || die "缺少 --node-id 本机节点 id"
[[ "${NODE_ID}" =~ ^[0-9]+$ ]] || die "--node-id 必须为正整数: ${NODE_ID}"

NODES="${NODES// /}"   # 去掉所有空格,便于按逗号切分
IFS=',' read -ra NODE_ARR <<< "${NODES}"
NODE_COUNT=${#NODE_ARR[@]}
[[ "${NODE_COUNT}" -ge 2 ]] || die "多节点集群至少需要 2 个节点(当前 ${NODE_COUNT} 个);单机请用 deploy_kafka.sh"

QUORUM_VOTERS=""
THIS_HOST=""
declare -a BROKER_ENDPOINTS=()
declare -a SEEN_IDS=()
for entry in "${NODE_ARR[@]}"; do
    [[ "${entry}" == *"@"* ]] || die "节点格式错误: '${entry}'(应为 id@host)"
    nid="${entry%%@*}"
    nhost="${entry#*@}"
    [[ -n "${nid}" && -n "${nhost}" ]] || die "节点格式错误: '${entry}'(应为 id@host)"
    [[ "${nid}" =~ ^[0-9]+$ ]]         || die "节点 id 必须为正整数: '${entry}'"
    # 检查 id 重复
    for s in "${SEEN_IDS[@]:-}"; do
        [[ "${s}" == "${nid}" ]] && die "节点 id 重复: ${nid}"
    done
    SEEN_IDS+=("${nid}")
    QUORUM_VOTERS+="${nid}@${nhost}:${CONTROLLER_PORT},"
    BROKER_ENDPOINTS+=("${nhost}:${BROKER_PORT}")
    [[ "${nid}" == "${NODE_ID}" ]] && THIS_HOST="${nhost}"
done
QUORUM_VOTERS="${QUORUM_VOTERS%,}"
[[ -n "${THIS_HOST}" ]] || die "--node-id ${NODE_ID} 未出现在 --nodes 列表中"

# controller 仲裁建议奇数个节点(容忍 (N-1)/2 台故障)
if (( NODE_COUNT % 2 == 0 )); then
    warn "节点数为偶数(${NODE_COUNT}),controller 仲裁建议使用奇数(3/5),偶数不会提升容错能力"
fi

# 未显式指定宣告地址时,取本机在 --nodes 中的 host
[[ -n "${ADVERTISED_HOST}" ]] || ADVERTISED_HOST="${THIS_HOST}"

# ---------------------------------------------------------------------------
# 副本因子 / 最小同步副本推导
# ---------------------------------------------------------------------------
if [[ -z "${REPLICATION_FACTOR}" ]]; then
    REPLICATION_FACTOR=$(( NODE_COUNT < 3 ? NODE_COUNT : 3 ))
fi
[[ "${REPLICATION_FACTOR}" =~ ^[0-9]+$ && "${REPLICATION_FACTOR}" -ge 1 ]] || die "--replication-factor 非法: ${REPLICATION_FACTOR}"
[[ "${REPLICATION_FACTOR}" -le "${NODE_COUNT}" ]] || die "副本因子(${REPLICATION_FACTOR})不能大于节点数(${NODE_COUNT})"
if [[ -z "${MIN_ISR}" ]]; then
    MIN_ISR=$(( REPLICATION_FACTOR >= 3 ? 2 : 1 ))
fi
[[ "${MIN_ISR}" =~ ^[0-9]+$ && "${MIN_ISR}" -ge 1 ]] || die "--min-isr 非法: ${MIN_ISR}"
[[ "${MIN_ISR}" -le "${REPLICATION_FACTOR}" ]] || die "最小同步副本(${MIN_ISR})不能大于副本因子(${REPLICATION_FACTOR})"

# ---------------------------------------------------------------------------
# SASL / 授权 前置处理
# ---------------------------------------------------------------------------
gen_password() {
    local chars='ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnpqrstuvwxyz23456789'
    local pw="" i
    for ((i = 0; i < 20; i++)); do
        pw+="${chars:RANDOM%${#chars}:1}"
    done
    printf '%s' "${pw}"
}

SASL_PASSWORD_GENERATED="false"
if [[ "${SASL_ENABLED}" == "true" ]]; then
    [[ -n "${SASL_USER}" ]] || die "启用 SASL 时管理员账号不能为空(-U/--sasl-user)"
    if [[ -z "${SASL_PASSWORD}" ]]; then
        SASL_PASSWORD="$(gen_password)"
        SASL_PASSWORD_GENERATED="true"
        warn "未指定管理员密码,已随机生成。多节点集群要求各节点密码一致,"
        warn "请复制下面完成提示中的密码,用 -P 传给其余节点!"
    fi
    [[ -z "${SUPER_USERS}" ]] && SUPER_USERS="User:${SASL_USER}"
else
    # 无 SASL 时授权(ACL)失去意义(principal 均为 ANONYMOUS),强制关闭
    if [[ "${AUTHZ_ENABLED}" == "true" ]]; then
        warn "--no-sasl 已关闭认证,授权(ACL)缺少身份来源,自动一并关闭"
        AUTHZ_ENABLED="false"
    fi
fi

# 校验追加账号格式 user:pass
for uentry in "${EXTRA_USERS[@]:-}"; do
    [[ -z "${uentry}" ]] && continue
    [[ "${uentry}" == *":"* ]] || die "--extra-user 格式错误: '${uentry}'(应为 user:password)"
    uname="${uentry%%:*}"
    upass="${uentry#*:}"
    [[ -n "${uname}" && -n "${upass}" ]] || die "--extra-user 格式错误: '${uentry}'(应为 user:password)"
done

# ---------------------------------------------------------------------------
# 部署信息汇总
# ---------------------------------------------------------------------------
log "开始部署 Kafka 多节点集群 (KRaft 模式) - 本机节点 ${NODE_ID}"
log "集群节点数: ${NODE_COUNT}"
log "仲裁配置  : ${QUORUM_VOTERS}"
log "本机地址  : ${THIS_HOST} (宣告: ${ADVERTISED_HOST}:${BROKER_PORT})"
log "安装目录  : ${INSTALL_DIR}"
log "数据目录  : ${DATA_DIR}"
log "副本因子  : ${REPLICATION_FACTOR} (最小同步副本 ${MIN_ISR}, 默认分区 ${PARTITIONS})"
if [[ "${SASL_ENABLED}" == "true" ]]; then
    log "认证方式  : SASL_PLAINTEXT / PLAIN (管理员: ${SASL_USER})"
    if [[ "${AUTHZ_ENABLED}" == "true" ]]; then
        log "授权方式  : StandardAuthorizer (super.users=${SUPER_USERS}, 未授权默认$([[ "${ALLOW_EVERYONE}" == "true" ]] && echo 放行 || echo 拒绝))"
    else
        log "授权方式  : 关闭 (仅认证,不做权限管控)"
    fi
else
    log "认证方式  : 无 (PLAINTEXT 明文)"
fi

# 仅渲染配置模式:跳过权限/Java/下载/格式化/systemd,只把配置写到指定目录后退出。
# 用于离线校验(如 Docker 冒烟测试)本脚本生成的 server.properties 是否正确。
RENDER_MODE="false"
SUDO=""
WORK_DIR="$(mktemp -d)"
trap 'rm -rf "${WORK_DIR}"' EXIT
if [[ -n "${RENDER_ONLY}" ]]; then
    RENDER_MODE="true"
    mkdir -p "${RENDER_ONLY}"
    RENDER_ONLY="$(cd "${RENDER_ONLY}" && pwd)"
    log "仅渲染配置模式:输出到 ${RENDER_ONLY}(不下载/格式化/注册服务)"
fi

# ---------------------------------------------------------------------------
# 前置检查(权限 / Java / 下载工具)—— 渲染模式跳过
# ---------------------------------------------------------------------------
if [[ "${RENDER_MODE}" == "false" ]]; then
if [[ "$(id -u)" -ne 0 ]]; then
    if command -v sudo >/dev/null 2>&1; then
        SUDO="sudo"
        warn "当前非 root 用户,涉及系统目录的操作将使用 sudo"
    else
        die "需要 root 权限或安装 sudo"
    fi
fi

if ! command -v java >/dev/null 2>&1; then
    die "未检测到 Java。Kafka ${KAFKA_VERSION} 需要 Java 8+ (推荐 Java 11/17),请先安装 JDK。"
fi
log "检测到 Java: $(java -version 2>&1 | head -n1)"

if [[ -n "${PACKAGE}" ]]; then
    [[ -f "${PACKAGE}" ]] || die "指定的安装包不存在: ${PACKAGE}"
    PACKAGE="$(cd "$(dirname "${PACKAGE}")" && pwd)/$(basename "${PACKAGE}")"
    log "离线部署模式,使用本地安装包: ${PACKAGE}"
else
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
if [[ -f "${KAFKA_HOME}/bin/kafka-server-start.sh" ]]; then
    warn "目标目录已存在 Kafka: ${KAFKA_HOME},跳过获取与解压"
else
    if [[ -n "${PACKAGE}" ]]; then
        SRC_TARBALL="${PACKAGE}"
    else
        log "下载 ${TARBALL} ..."
        if ! ${DOWNLOADER} "${WORK_DIR}/${TARBALL}" "${MIRROR}/${TARBALL}"; then
            die "下载失败: ${MIRROR}/${TARBALL}"
        fi
        SRC_TARBALL="${WORK_DIR}/${TARBALL}"
    fi

    if ! TARLIST="$(tar -tzf "${SRC_TARBALL}" 2>/dev/null)"; then
        die "安装包损坏或不是有效的 tgz 文件: ${SRC_TARBALL}"
    fi
    FIRST_LINE="${TARLIST%%$'\n'*}"
    TOP_DIR="${FIRST_LINE%%/*}"
    [[ "${TOP_DIR}" == "${PKG_NAME}" ]] || \
        die "安装包顶层目录为 '${TOP_DIR}',与预期 '${PKG_NAME}' 不符,请确认是 ${PKG_NAME}.tgz"

    log "创建安装目录并解压(剥离顶层目录)..."
    ${SUDO} mkdir -p "${INSTALL_DIR}"
    ${SUDO} tar -xzf "${SRC_TARBALL}" -C "${INSTALL_DIR}" --strip-components=1
    [[ -f "${KAFKA_HOME}/bin/kafka-server-start.sh" ]] || \
        die "解压后未找到 ${KAFKA_HOME}/bin/kafka-server-start.sh,解压可能失败"
    log "已解压到 ${KAFKA_HOME}"
fi

log "创建数据目录: ${DATA_DIR}"
${SUDO} mkdir -p "${DATA_DIR}"
fi   # end RENDER_MODE == false 的前置/下载/数据目录部分

# ---------------------------------------------------------------------------
# 渲染 SASL / 授权 配置片段
# ---------------------------------------------------------------------------
if [[ "${RENDER_MODE}" == "true" ]]; then
    SERVER_PROPS="${RENDER_ONLY}/server.properties"
else
    SERVER_PROPS="${KAFKA_HOME}/config/kraft/server.properties"
    [[ -f "${SERVER_PROPS}" ]] || die "未找到配置文件: ${SERVER_PROPS}"
    ${SUDO} cp -n "${SERVER_PROPS}" "${SERVER_PROPS}.orig" 2>/dev/null || true
fi

if [[ "${SASL_ENABLED}" == "true" ]]; then
    BROKER_LISTENER="SASL_PLAINTEXT"
    CTRL_LISTENER_PROTOCOL="SASL_PLAINTEXT"

    # 构造 broker 监听器 JAAS 的允许账号列表(管理员 + 追加账号)
    JAAS_USER_ENTRIES="user_${SASL_USER}=\"${SASL_PASSWORD}\""
    for uentry in "${EXTRA_USERS[@]:-}"; do
        [[ -z "${uentry}" ]] && continue
        uname="${uentry%%:*}"
        upass="${uentry#*:}"
        JAAS_USER_ENTRIES+=" user_${uname}=\"${upass}\""
    done

    # 授权片段
    AUTHZ_CONF=""
    if [[ "${AUTHZ_ENABLED}" == "true" ]]; then
        AUTHZ_CONF="
# === ACL 授权 (由 deploy_kafka_cluster.sh 自动生成) ===
# 仅认证只能验明身份,开启 authorizer 才做权限管控。super.users 拥有全部权限。
authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer
super.users=${SUPER_USERS}
allow.everyone.if.no.acl.found=${ALLOW_EVERYONE}"
    fi

    # 多节点:CONTROLLER 监听器也启用 SASL,principal 为管理员(super user),
    # 避免像单机那样把 User:ANONYMOUS 设为超级用户后暴露在集群网络上。
    SASL_CONF="
# === SASL/PLAIN 认证 (由 deploy_kafka_cluster.sh 自动生成) ===
sasl.enabled.mechanisms=PLAIN
sasl.mechanism.inter.broker.protocol=PLAIN
sasl.mechanism.controller.protocol=PLAIN
# broker(数据面)监听器:接受管理员及追加账号登录
listener.name.sasl_plaintext.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"${SASL_USER}\" password=\"${SASL_PASSWORD}\" ${JAAS_USER_ENTRIES};
# controller 监听器:节点间互连,仅用管理员账号
listener.name.controller.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"${SASL_USER}\" password=\"${SASL_PASSWORD}\" user_${SASL_USER}=\"${SASL_PASSWORD}\";
${AUTHZ_CONF}
"
else
    BROKER_LISTENER="PLAINTEXT"
    CTRL_LISTENER_PROTOCOL="PLAINTEXT"
    SASL_CONF=""
fi

# ---------------------------------------------------------------------------
# 写入 server.properties
# ---------------------------------------------------------------------------
log "写入 KRaft 集群配置 (node.id=${NODE_ID}) ..."
TMP_CONF="${WORK_DIR}/server.properties"
cat > "${TMP_CONF}" <<EOF
# === 由 deploy_kafka_cluster.sh 自动生成 (KRaft 多节点, node ${NODE_ID}/${NODE_COUNT}) ===

# 每个节点同时承担 broker 和 controller 角色
process.roles=broker,controller
node.id=${NODE_ID}
controller.quorum.voters=${QUORUM_VOTERS}

# 监听配置(CONTROLLER 绑定所有网卡,供集群其它节点连接)
listeners=${BROKER_LISTENER}://:${BROKER_PORT},CONTROLLER://:${CONTROLLER_PORT}
inter.broker.listener.name=${BROKER_LISTENER}
advertised.listeners=${BROKER_LISTENER}://${ADVERTISED_HOST}:${BROKER_PORT}
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:${CTRL_LISTENER_PROTOCOL},PLAINTEXT:PLAINTEXT,SSL:SSL,SASL_PLAINTEXT:SASL_PLAINTEXT,SASL_SSL:SASL_SSL
${SASL_CONF}
# 网络与 IO 线程
num.network.threads=3
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600

# 数据目录
log.dirs=${DATA_DIR}

# 集群副本与分区设置
num.partitions=${PARTITIONS}
default.replication.factor=${REPLICATION_FACTOR}
min.insync.replicas=${MIN_ISR}
num.recovery.threads.per.data.dir=1
offsets.topic.replication.factor=${REPLICATION_FACTOR}
transaction.state.log.replication.factor=${REPLICATION_FACTOR}
transaction.state.log.min.isr=${MIN_ISR}

# 日志保留
log.retention.hours=168
log.retention.check.interval.ms=300000
log.segment.bytes=1073741824
EOF

${SUDO} cp "${TMP_CONF}" "${SERVER_PROPS}"

# 启用 SASL 时,生成客户端认证配置(供 kafka-*.sh --command-config 使用)
CLIENT_PROPS=""
if [[ "${SASL_ENABLED}" == "true" ]]; then
    if [[ "${RENDER_MODE}" == "true" ]]; then
        CLIENT_PROPS="${RENDER_ONLY}/client-sasl.properties"
    else
        CLIENT_PROPS="${KAFKA_HOME}/config/client-sasl.properties"
    fi
    TMP_CLIENT="${WORK_DIR}/client-sasl.properties"
    cat > "${TMP_CLIENT}" <<EOF
# === 客户端 SASL/PLAIN 连接配置 (由 deploy_kafka_cluster.sh 自动生成) ===
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="${SASL_USER}" password="${SASL_PASSWORD}";
EOF
    ${SUDO} cp "${TMP_CLIENT}" "${CLIENT_PROPS}"
    ${SUDO} chmod 600 "${CLIENT_PROPS}"
    log "已生成客户端认证配置: ${CLIENT_PROPS}"
fi

# 仅渲染模式:配置已写出,到此结束(不格式化、不注册 systemd)
if [[ "${RENDER_MODE}" == "true" ]]; then
    log "配置渲染完成: ${SERVER_PROPS}"
    exit 0
fi

# ---------------------------------------------------------------------------
# 格式化存储目录 (KRaft 必需,集群各节点须用同一 Cluster ID)
# ---------------------------------------------------------------------------
if [[ -f "${DATA_DIR}/meta.properties" ]] || ${SUDO} test -f "${DATA_DIR}/meta.properties"; then
    warn "数据目录已格式化,跳过 storage format"
else
    if [[ -z "${CLUSTER_ID}" ]]; then
        CLUSTER_ID="$(${KAFKA_HOME}/bin/kafka-storage.sh random-uuid)"
        CLUSTER_ID_GENERATED="true"
        log "自动生成 Cluster ID: ${CLUSTER_ID}"
        warn "其余节点必须使用该 Cluster ID: 加参数 -c ${CLUSTER_ID}"
    else
        log "使用指定 Cluster ID: ${CLUSTER_ID}"
    fi
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
Description=Apache Kafka (KRaft cluster node ${NODE_ID})
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
        log "本节点 Kafka 服务已启动"
        warn "集群需所有节点都启动后仲裁才能选主;若本节点暂时报连接错误,属正常(在等其它节点)"
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
CMD_CFG=""
[[ "${SASL_ENABLED}" == "true" ]] && CMD_CFG=" --command-config ${CLIENT_PROPS}"
# 拼接所有 broker 地址,便于客户端 --bootstrap-server
BOOTSTRAP="$(IFS=,; echo "${BROKER_ENDPOINTS[*]}")"

cat <<EOF

========================================================================
 Kafka ${KAFKA_VERSION} 集群节点 ${NODE_ID}/${NODE_COUNT} 部署完成!
------------------------------------------------------------------------
 安装目录 : ${KAFKA_HOME}
 数据目录 : ${DATA_DIR}
 配置文件 : ${SERVER_PROPS}
 本机监听 : broker ${BROKER_PORT} / controller ${CONTROLLER_PORT}
 宣告地址 : ${ADVERTISED_HOST}:${BROKER_PORT}
 仲裁成员 : ${QUORUM_VOTERS}
 Cluster ID : ${CLUSTER_ID}$([[ "${CLUSTER_ID_GENERATED:-false}" == "true" ]] && printf '  (本次自动生成,其余节点务必用 -c 传入相同值)')
 副本因子 : ${REPLICATION_FACTOR} (min.insync.replicas=${MIN_ISR})
EOF

if [[ "${SASL_ENABLED}" == "true" ]]; then
cat <<EOF
 认证方式 : SASL_PLAINTEXT / PLAIN (broker 与 controller 均认证)
   管理员 : ${SASL_USER}
   密码   : ${SASL_PASSWORD}$([[ "${SASL_PASSWORD_GENERATED}" == "true" ]] && printf '  (随机生成,所有节点须一致!请用 -P 传给其余节点)')
EOF
    if (( ${#EXTRA_USERS[@]} > 0 )); then
        printf '   追加账号 :'
        for uentry in "${EXTRA_USERS[@]}"; do printf ' %s' "${uentry%%:*}"; done
        printf '\n'
    fi
    if [[ "${AUTHZ_ENABLED}" == "true" ]]; then
cat <<EOF
 授权方式 : StandardAuthorizer (super.users=${SUPER_USERS})
   未授权账号默认: $([[ "${ALLOW_EVERYONE}" == "true" ]] && echo 放行 || echo 拒绝)
   为普通账号授权(在任一节点执行一次即可,全集群生效):
     ${KAFKA_HOME}/bin/kafka-acls.sh --bootstrap-server ${BOOTSTRAP} \\
         --command-config ${CLIENT_PROPS} \\
         --add --allow-principal User:<账号> \\
         --operation Read --operation Write --operation Describe \\
         --topic '<topic>' --group '<group>'
EOF
    else
cat <<EOF
 授权方式 : 关闭 (仅认证,未做权限管控)
EOF
    fi
    cat <<EOF
 客户端配置文件 : ${CLIENT_PROPS} (权限 600)
   连接工具附加: --command-config ${CLIENT_PROPS}
EOF
else
cat <<EOF
 认证方式 : 无 (PLAINTEXT 明文,仅适用于可信内网)
EOF
fi

cat <<EOF

 常用命令:
   启动/停止/状态: systemctl {start|stop|status} kafka
   日志:           journalctl -u kafka -f

 全部节点部署完成后,验证集群(任一节点执行):
   ${KAFKA_HOME}/bin/kafka-topics.sh --create --topic test \\
       --bootstrap-server ${BOOTSTRAP} \\
       --partitions ${PARTITIONS} --replication-factor ${REPLICATION_FACTOR}${CMD_CFG}
   ${KAFKA_HOME}/bin/kafka-topics.sh --describe --topic test \\
       --bootstrap-server ${BOOTSTRAP}${CMD_CFG}
   # 查看 controller 仲裁状态
   ${KAFKA_HOME}/bin/kafka-metadata-quorum.sh --bootstrap-server ${BOOTSTRAP}${CMD_CFG} describe --status
========================================================================
EOF
