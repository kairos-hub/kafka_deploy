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
#   -N, --nodes LIST        集群所有节点 id@host,逗号分隔(host 需为节点间互通的地址,不支持 IPv6)
#                           例: 1@10.0.0.1,2@10.0.0.2,3@10.0.0.3
#                           列表中第一个节点为"首节点",负责生成 Cluster ID 与管理员密码
#   -n, --node-id ID        本机节点 id(必须出现在 --nodes 列表中)
#
# 集群一致性(所有节点必须相同):
#   -c, --cluster-id UUID   集群 ID。首节点首次部署可不填(自动生成并打印);其余节点必填
#   -P, --sasl-password PWD 管理员密码。首节点首次部署可不填(自动生成并打印);
#                           其余节点及任何节点的重复执行必填
#   部署完成后会打印"集群参数指纹",所有节点的指纹必须一致
#
# 端口(所有节点使用相同端口):
#   -p, --port PORT         免认证 PLAINTEXT 客户端端口  (默认: 9092,始终开启)
#   -s, --sasl-port PORT    认证 SASL_PLAINTEXT 客户端端口 (默认: 9094,--no-sasl 时关闭)
#       --controller-port P controller 仲裁端口          (默认: 9093)
#       --internal-port P   broker 间复制专用端口        (默认: 9095)
#
# 目录与网络:
#   -i, --install-dir DIR   Kafka 安装目录        (默认: /opt/kafka)
#   -d, --data-dir DIR      Kafka 数据目录        (默认: /var/lib/kafka/data)
#   -H, --advertised-host H 本机对客户端宣告地址  (默认: --nodes 中本机 host)
#                           仅影响 9092/9094,broker 间复制始终走 --nodes 中的地址
#
# 运行:
#       --run-user USER     运行 Kafka 的系统用户 (默认: kafka,不存在则自动创建)
#       --heap SIZE         JVM 堆大小,如 2G      (默认: Kafka 默认 1G)
#
# 安装包:
#   -f, --package FILE      本地安装包路径(离线部署;若同目录存在 FILE.sha512 则自动校验)
#   -m, --mirror URL        下载镜像地址前缀      (默认: Apache 官方归档站)
#       --skip-checksum     在线下载时跳过 sha512 校验(不推荐)
#
# 副本与分区(默认按节点数推导):
#   -r, --replication-factor N  内部/默认副本因子 (默认: min(3, 节点数))
#       --min-isr N             最小同步副本      (默认: RF>=3 ? 2 : 1)
#       --partitions N          默认分区数        (默认: 3)
#
# SASL 与授权(默认开启 SASL/PLAIN + ACL 授权):
#       --no-sasl           关闭 SASL(不开认证端口,节点间明文无认证,同时关闭授权)
#   -U, --sasl-user USER    管理员账号            (默认: admin,用于集群内部通信)
#       --extra-user U:P    追加 SASL 账号(可重复,如 --extra-user app:appPwd)
#       --super-users LIST  追加超级用户,分号或逗号分隔(如 User:ops;User:sre)
#                           管理员账号始终自动包含
#       --plain-access MODE 免认证端口(9092)的授权策略,仅授权开启时有效:
#                             full  匿名用户(User:ANONYMOUS)为超级用户,9092 拥有全部权限(默认)
#                             acl   匿名用户与普通账号一样,需通过 ACL 单独授权
#       --no-authz          关闭 ACL 授权(仅保留 SASL 认证,不做权限管控)
#       --allow-everyone    无 ACL 的资源默认放行(默认: 拒绝)
#
# 其他:
#       --allow-two-nodes   允许 2 节点集群(任一节点故障即整体不可用,不推荐)
#       --force             重复执行时允许修改集群级参数(指纹变化),需所有节点同步修改
#       --render-only DIR   仅把 server.properties/client 配置渲染到 DIR 后退出
#                           (不下载/不格式化/不注册服务,用于离线校验配置)
#   -h, --help              显示帮助信息
#
# 典型三节点流程:
#   # 节点1(首节点,会打印 Cluster ID 与管理员密码)
#   ./deploy_kafka_cluster.sh -N 1@10.0.0.1,2@10.0.0.2,3@10.0.0.3 -n 1 -P 'AdminP@ss'
#   # 节点2 / 节点3(使用节点1打印的 Cluster ID,密码相同)
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
SCRIPT_TAG="deploy_kafka_cluster.sh"

NODES=""               # 集群节点列表 id@host,逗号分隔(必填)
NODE_ID=""             # 本机节点 id(必填)
CLUSTER_ID=""          # 集群 ID
INSTALL_DIR="/opt/kafka"
DATA_DIR="/var/lib/kafka/data"
PLAIN_PORT="9092"      # 免认证客户端端口,始终开启
SASL_PORT="9094"       # 认证客户端端口,SASL 开启时开启
SASL_PORT_SET="false"
CONTROLLER_PORT="9093" # KRaft 仲裁端口
INTERNAL_PORT="9095"   # broker 间复制专用端口
ADVERTISED_HOST=""     # 客户端宣告地址,留空取 --nodes 中本机 host
PACKAGE=""
MIRROR="https://archive.apache.org/dist/kafka/${KAFKA_VERSION}"
SKIP_CHECKSUM="false"
RUN_USER="kafka"
HEAP_SIZE=""

REPLICATION_FACTOR=""
MIN_ISR=""
PARTITIONS="3"

SASL_ENABLED="true"
SASL_USER="admin"
SASL_PASSWORD=""
declare -a EXTRA_USERS=()
SUPER_USERS_EXTRA=""
AUTHZ_ENABLED="true"
ALLOW_EVERYONE="false"
PLAIN_ACCESS="full"

ALLOW_TWO_NODES="false"
FORCE="false"
RENDER_ONLY=""

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

# 选项缺少取值时给出友好提示(避免 set -u 抛 unbound variable)
need_arg() {
    [[ $# -ge 2 && -n "$2" && "$2" != --* ]] || die "参数 $1 缺少取值 (使用 -h 查看帮助)"
}

# 端口校验:1-65535 整数
check_port() {
    local name="$1" val="$2"
    [[ "${val}" =~ ^[0-9]+$ ]] && (( 10#${val} >= 1 && 10#${val} <= 65535 )) || \
        die "${name} 端口无效: '${val}' (应为 1-65535 的整数)"
}

# 正整数校验
check_pos_int() {
    local name="$1" val="$2"
    [[ "${val}" =~ ^[0-9]+$ ]] && (( 10#${val} >= 1 )) || die "${name} 非法: '${val}' (应为正整数)"
}

# 账号名:会作为 JAAS 的 user_<name> 键,只允许安全字符
check_username() {
    local what="$1" v="$2"
    [[ "${v}" =~ ^[A-Za-z0-9._-]+$ ]] || die "${what} '${v}' 非法:仅允许字母、数字及 . _ -"
    [[ "${v}" != "ANONYMOUS" ]] || die "${what} 不能为保留名 ANONYMOUS"
}

# 密码:会写入 JAAS 双引号字符串,含 " \ 或空白会破坏语法
check_secret() {
    local what="$1" v="$2"
    [[ -n "${v}" ]] || die "${what} 不能为空"
    if [[ "${v}" =~ [\"\\[:space:]] ]]; then
        die "${what} 不能包含双引号、反斜杠或空白字符"
    fi
}

# 生成 24 位随机密码(读取定长 urandom,不会触发 pipefail 下的 SIGPIPE)
gen_password() {
    local pw
    pw="$(LC_ALL=C tr -dc 'A-HJ-NP-Za-km-z2-9' < <(head -c 1024 /dev/urandom) | cut -c1-24)"
    [[ ${#pw} -eq 24 ]] || die "随机密码生成失败"
    printf '%s' "${pw}"
}

# 校验 tgz 的 sha512。兼容 Apache 的 gpg 格式("文件名: 分组十六进制",可跨行)
# 与 sha512sum 格式("哈希  文件名")
verify_sha512() {
    local file="$1" sumfile="$2" content hex expected actual
    command -v sha512sum >/dev/null 2>&1 || die "未找到 sha512sum,无法校验安装包(可用 --skip-checksum 跳过)"
    content="$(cat "${sumfile}")"
    if [[ "${content}" == *:* ]]; then
        hex="${content#*:}"
    else
        hex="${content%%[[:space:]]*}"
    fi
    expected="$(printf '%s' "${hex}" | tr -d '[:space:]' | tr 'A-F' 'a-f')"
    [[ "${expected}" =~ ^[0-9a-f]{128}$ ]] || die "无法解析校验文件: ${sumfile}"
    actual="$(sha512sum "${file}" | awk '{print $1}')"
    [[ "${actual}" == "${expected}" ]] || die "sha512 校验失败,安装包可能被篡改或下载不完整: ${file}"
    log "sha512 校验通过"
}

# 探测 TCP 端口是否可连接
tcp_open() {
    timeout 1 bash -c "exec 3<>/dev/tcp/$1/$2" 2>/dev/null
}

# ---------------------------------------------------------------------------
# 解析命令行参数
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        -N|--nodes)              need_arg "$@"; NODES="$2";              shift 2 ;;
        -n|--node-id)            need_arg "$@"; NODE_ID="$2";            shift 2 ;;
        -c|--cluster-id)         need_arg "$@"; CLUSTER_ID="$2";         shift 2 ;;
        -i|--install-dir)        need_arg "$@"; INSTALL_DIR="$2";        shift 2 ;;
        -d|--data-dir)           need_arg "$@"; DATA_DIR="$2";           shift 2 ;;
        -p|--port)               need_arg "$@"; PLAIN_PORT="$2";         shift 2 ;;
        -s|--sasl-port)          need_arg "$@"; SASL_PORT="$2"; SASL_PORT_SET="true"; shift 2 ;;
        --controller-port)       need_arg "$@"; CONTROLLER_PORT="$2";    shift 2 ;;
        --internal-port)         need_arg "$@"; INTERNAL_PORT="$2";      shift 2 ;;
        -H|--advertised-host)    need_arg "$@"; ADVERTISED_HOST="$2";    shift 2 ;;
        -f|--package)            need_arg "$@"; PACKAGE="$2";            shift 2 ;;
        -m|--mirror)             need_arg "$@"; MIRROR="$2";             shift 2 ;;
        --skip-checksum)         SKIP_CHECKSUM="true";                   shift 1 ;;
        --run-user)              need_arg "$@"; RUN_USER="$2";           shift 2 ;;
        --heap)                  need_arg "$@"; HEAP_SIZE="$2";          shift 2 ;;
        -r|--replication-factor) need_arg "$@"; REPLICATION_FACTOR="$2"; shift 2 ;;
        --min-isr)               need_arg "$@"; MIN_ISR="$2";            shift 2 ;;
        --partitions)            need_arg "$@"; PARTITIONS="$2";         shift 2 ;;
        --no-sasl)               SASL_ENABLED="false";                   shift 1 ;;
        -U|--sasl-user)          need_arg "$@"; SASL_USER="$2";          shift 2 ;;
        -P|--sasl-password)      need_arg "$@"; SASL_PASSWORD="$2";      shift 2 ;;
        --extra-user)            need_arg "$@"; EXTRA_USERS+=("$2");     shift 2 ;;
        --super-users)           need_arg "$@"; SUPER_USERS_EXTRA="$2";  shift 2 ;;
        --plain-access)          need_arg "$@"; PLAIN_ACCESS="$2";       shift 2 ;;
        --no-authz)              AUTHZ_ENABLED="false";                  shift 1 ;;
        --allow-everyone)        ALLOW_EVERYONE="true";                  shift 1 ;;
        --allow-two-nodes)       ALLOW_TWO_NODES="true";                 shift 1 ;;
        --force)                 FORCE="true";                           shift 1 ;;
        --render-only)           need_arg "$@"; RENDER_ONLY="$2";        shift 2 ;;
        -h|--help)               usage ;;
        *) die "未知参数: $1 (使用 -h 查看帮助)" ;;
    esac
done

KAFKA_HOME="${INSTALL_DIR}"

# ---------------------------------------------------------------------------
# 基础参数校验
# ---------------------------------------------------------------------------
[[ -n "${NODES}" ]]   || die "缺少 --nodes 集群节点列表(如 1@10.0.0.1,2@10.0.0.2,3@10.0.0.3)"
[[ -n "${NODE_ID}" ]] || die "缺少 --node-id 本机节点 id"
[[ "${NODE_ID}" =~ ^[0-9]+$ ]] || die "--node-id 必须为非负整数: ${NODE_ID}"
NODE_ID=$(( 10#${NODE_ID} ))   # 规范化,01 与 1 视为同一 id

# 端口:格式合法且互不冲突
check_port "免认证(-p)" "${PLAIN_PORT}"
check_port "controller(--controller-port)" "${CONTROLLER_PORT}"
check_port "内部复制(--internal-port)" "${INTERNAL_PORT}"
PLAIN_PORT=$((10#${PLAIN_PORT})); CONTROLLER_PORT=$((10#${CONTROLLER_PORT})); INTERNAL_PORT=$((10#${INTERNAL_PORT}))
declare -a USED_PORTS=("${PLAIN_PORT}" "${CONTROLLER_PORT}" "${INTERNAL_PORT}")
if [[ "${SASL_ENABLED}" == "true" ]]; then
    check_port "认证(-s)" "${SASL_PORT}"
    SASL_PORT=$((10#${SASL_PORT}))
    USED_PORTS+=("${SASL_PORT}")
elif [[ "${SASL_PORT_SET}" == "true" ]]; then
    warn "指定了 --sasl-port 但使用了 --no-sasl,认证端口不会开启"
fi
DUP_PORT="$(printf '%s\n' "${USED_PORTS[@]}" | sort | uniq -d | head -n1 || true)"
[[ -z "${DUP_PORT}" ]] || die "端口冲突: ${DUP_PORT} 被多个监听器使用(免认证/认证/controller/内部复制端口须互不相同)"

# 目录安全:后续会 chown -R,禁止指向系统目录
for d in "${INSTALL_DIR}" "${DATA_DIR}"; do
    [[ "${d}" == /* ]] || die "目录必须为绝对路径: ${d}"
    case "${d%/}" in
        ""|/usr|/usr/local|/opt|/var|/var/lib|/etc|/home|/root|/tmp|/data)
            die "目录不能直接使用系统目录 '${d}',请指定专用子目录(如 ${d%/}/kafka)" ;;
    esac
done

[[ "${RUN_USER}" == "root" || "${RUN_USER}" =~ ^[a-z_][a-z0-9_-]*$ ]] || die "--run-user 非法: ${RUN_USER}"
[[ -z "${HEAP_SIZE}" || "${HEAP_SIZE}" =~ ^[0-9]+[mMgG]$ ]] || die "--heap 格式错误: ${HEAP_SIZE} (如 2G / 1536M)"
[[ "${PLAIN_ACCESS}" == "full" || "${PLAIN_ACCESS}" == "acl" ]] || die "--plain-access 只能为 full 或 acl"
check_pos_int "--partitions" "${PARTITIONS}"

# ---------------------------------------------------------------------------
# 解析集群拓扑
# ---------------------------------------------------------------------------
NODES="${NODES// /}"
IFS=',' read -ra NODE_ARR <<< "${NODES}"
NODE_COUNT=${#NODE_ARR[@]}
[[ "${NODE_COUNT}" -ge 2 ]] || die "多节点集群至少需要 2 个节点(当前 ${NODE_COUNT} 个);单机请用 deploy_kafka.sh"

QUORUM_VOTERS=""
THIS_HOST=""
FIRST_NODE_ID=""
NORMALIZED_NODES=""
declare -a PLAIN_ENDPOINTS=() SASL_ENDPOINTS=() SEEN_IDS=() SEEN_HOSTS=()
for entry in "${NODE_ARR[@]}"; do
    [[ "${entry}" == *"@"* ]] || die "节点格式错误: '${entry}'(应为 id@host)"
    nid="${entry%%@*}"
    nhost="${entry#*@}"
    [[ -n "${nid}" && -n "${nhost}" ]] || die "节点格式错误: '${entry}'(应为 id@host)"
    [[ "${nid}" =~ ^[0-9]+$ ]]         || die "节点 id 必须为非负整数: '${entry}'"
    [[ "${nhost}" != *:* && "${nhost}" != *@* ]] || die "节点 host 非法(不支持 IPv6 或携带端口): '${entry}'"
    nid=$(( 10#${nid} ))
    for s in "${SEEN_IDS[@]:-}"; do
        [[ "${s}" != "${nid}" ]] || die "节点 id 重复: ${nid}"
    done
    for h in "${SEEN_HOSTS[@]:-}"; do
        [[ "${h}" != "${nhost}" ]] || die "节点 host 重复: ${nhost}(所有节点端口相同,同一主机无法部署多个节点)"
    done
    SEEN_IDS+=("${nid}")
    SEEN_HOSTS+=("${nhost}")
    [[ -n "${FIRST_NODE_ID}" ]] || FIRST_NODE_ID="${nid}"
    QUORUM_VOTERS+="${nid}@${nhost}:${CONTROLLER_PORT},"
    NORMALIZED_NODES+="${nid}@${nhost},"
    PLAIN_ENDPOINTS+=("${nhost}:${PLAIN_PORT}")
    SASL_ENDPOINTS+=("${nhost}:${SASL_PORT}")
    if [[ "${nid}" == "${NODE_ID}" ]]; then THIS_HOST="${nhost}"; fi
done
QUORUM_VOTERS="${QUORUM_VOTERS%,}"
NORMALIZED_NODES="${NORMALIZED_NODES%,}"
[[ -n "${THIS_HOST}" ]] || die "--node-id ${NODE_ID} 未出现在 --nodes 列表中"
IS_FIRST_NODE="false"
[[ "${NODE_ID}" != "${FIRST_NODE_ID}" ]] || IS_FIRST_NODE="true"

# 仲裁节点数检查:2 节点时多数派=2,任一节点宕机集群整体不可用
if (( NODE_COUNT == 2 )); then
    [[ "${ALLOW_TWO_NODES}" == "true" ]] || \
        die "2 节点集群的仲裁多数派为 2,任一节点宕机即整体不可用,可用性低于单机。建议 3 节点;确需部署请加 --allow-two-nodes"
    warn "2 节点集群:任一节点宕机,元数据层即不可用"
elif (( NODE_COUNT % 2 == 0 )); then
    warn "节点数为偶数(${NODE_COUNT}),controller 仲裁建议使用奇数(3/5),偶数不会提升容错能力"
fi

[[ -n "${ADVERTISED_HOST}" ]] || ADVERTISED_HOST="${THIS_HOST}"
[[ "${ADVERTISED_HOST}" != *:* && "${ADVERTISED_HOST}" != *[[:space:]]* ]] || die "宣告地址非法: ${ADVERTISED_HOST}"

BOOTSTRAP_PLAIN="$(IFS=,; echo "${PLAIN_ENDPOINTS[*]}")"
BOOTSTRAP_SASL="$(IFS=,; echo "${SASL_ENDPOINTS[*]}")"

# ---------------------------------------------------------------------------
# 副本因子 / 最小同步副本推导
# ---------------------------------------------------------------------------
if [[ -z "${REPLICATION_FACTOR}" ]]; then
    REPLICATION_FACTOR=$(( NODE_COUNT < 3 ? NODE_COUNT : 3 ))
fi
check_pos_int "--replication-factor" "${REPLICATION_FACTOR}"
REPLICATION_FACTOR=$((10#${REPLICATION_FACTOR}))
(( REPLICATION_FACTOR <= NODE_COUNT )) || die "副本因子(${REPLICATION_FACTOR})不能大于节点数(${NODE_COUNT})"
if [[ -z "${MIN_ISR}" ]]; then
    MIN_ISR=$(( REPLICATION_FACTOR >= 3 ? 2 : 1 ))
fi
check_pos_int "--min-isr" "${MIN_ISR}"
MIN_ISR=$((10#${MIN_ISR}))
(( MIN_ISR <= REPLICATION_FACTOR )) || die "最小同步副本(${MIN_ISR})不能大于副本因子(${REPLICATION_FACTOR})"

# ---------------------------------------------------------------------------
# SASL / 授权 参数校验
# ---------------------------------------------------------------------------
SUPER_USERS=""
declare -a EXTRA_NAMES=()
if [[ "${SASL_ENABLED}" == "true" ]]; then
    check_username "管理员账号(-U)" "${SASL_USER}"
    [[ -z "${SASL_PASSWORD}" ]] || check_secret "管理员密码(-P)" "${SASL_PASSWORD}"

    for uentry in "${EXTRA_USERS[@]:-}"; do
        [[ -n "${uentry}" ]] || continue
        [[ "${uentry}" == *":"* ]] || die "--extra-user 格式错误: '${uentry}'(应为 user:password)"
        uname="${uentry%%:*}"
        upass="${uentry#*:}"
        check_username "--extra-user 账号" "${uname}"
        check_secret "--extra-user 账号 ${uname} 的密码" "${upass}"
        [[ "${uname}" != "${SASL_USER}" ]] || die "--extra-user 账号 ${uname} 与管理员账号重名"
        for n in "${EXTRA_NAMES[@]:-}"; do
            [[ "${n}" != "${uname}" ]] || die "--extra-user 账号重复: ${uname}"
        done
        EXTRA_NAMES+=("${uname}")
    done

    if [[ "${AUTHZ_ENABLED}" == "true" ]]; then
        # super.users 以分号分隔;兼容逗号输入,并强制包含管理员(集群内部通信身份),
        # 否则 broker 间复制与 controller 通信会被 ACL 拒绝,集群无法工作。
        declare -a SU_LIST=("User:${SASL_USER}")
        if [[ "${PLAIN_ACCESS}" == "full" ]]; then
            SU_LIST+=("User:ANONYMOUS")
        fi
        IFS=';' read -ra SU_IN <<< "${SUPER_USERS_EXTRA//,/;}"
        for su in "${SU_IN[@]:-}"; do
            su="${su// /}"
            [[ -n "${su}" ]] || continue
            [[ "${su}" =~ ^User:[A-Za-z0-9._-]+$ ]] || die "--super-users 条目格式错误: '${su}'(应为 User:<账号>)"
            dup="false"
            for e in "${SU_LIST[@]}"; do [[ "${e}" != "${su}" ]] || dup="true"; done
            [[ "${dup}" == "true" ]] || SU_LIST+=("${su}")
        done
        SUPER_USERS="$(IFS=';'; echo "${SU_LIST[*]}")"
    else
        [[ -z "${SUPER_USERS_EXTRA}" ]] || warn "已关闭授权(--no-authz),--super-users 不生效"
    fi
else
    if [[ "${AUTHZ_ENABLED}" == "true" ]]; then
        warn "--no-sasl 已关闭认证,授权(ACL)缺少身份来源,自动一并关闭"
        AUTHZ_ENABLED="false"
    fi
    (( ${#EXTRA_USERS[@]} == 0 )) || warn "--no-sasl 模式下 --extra-user 不生效"
fi
[[ "${AUTHZ_ENABLED}" == "true" || "${PLAIN_ACCESS}" == "full" ]] || \
    warn "未开启授权,--plain-access acl 不生效,免认证端口拥有全部权限"

# ---------------------------------------------------------------------------
# 运行模式与前置检查
# ---------------------------------------------------------------------------
RENDER_MODE="false"
SUDO=""
WORK_DIR="$(mktemp -d)"
trap 'rm -rf "${WORK_DIR}"' EXIT

if [[ -n "${RENDER_ONLY}" ]]; then
    RENDER_MODE="true"
    mkdir -p "${RENDER_ONLY}"
    RENDER_ONLY="$(cd "${RENDER_ONLY}" && pwd)"
else
    if [[ "$(id -u)" -ne 0 ]]; then
        if command -v sudo >/dev/null 2>&1; then
            SUDO="sudo"
            warn "当前非 root 用户,涉及系统目录的操作将使用 sudo"
        else
            die "需要 root 权限或安装 sudo"
        fi
    fi
    command -v java >/dev/null 2>&1 || \
        die "未检测到 Java。Kafka ${KAFKA_VERSION} 需要 Java 8+ (推荐 Java 11/17),请先安装 JDK。"
    if [[ -n "${PACKAGE}" ]]; then
        [[ -f "${PACKAGE}" ]] || die "指定的安装包不存在: ${PACKAGE}"
        PACKAGE="$(cd "$(dirname "${PACKAGE}")" && pwd)/$(basename "${PACKAGE}")"
    elif command -v curl >/dev/null 2>&1; then
        DOWNLOADER="curl -fSL --retry 3 -o"
    elif command -v wget >/dev/null 2>&1; then
        DOWNLOADER="wget -O"
    else
        die "未检测到 curl 或 wget,无法下载安装包(或使用 -f 指定本地包离线部署)。"
    fi
fi

# ---------------------------------------------------------------------------
# 已有部署状态检测:Cluster ID 与密码一致性保护
# ---------------------------------------------------------------------------
META_FILE="${DATA_DIR}/meta.properties"
EXISTING_CLUSTER_ID=""
if [[ "${RENDER_MODE}" == "false" ]] && ${SUDO} test -f "${META_FILE}"; then
    EXISTING_CLUSTER_ID="$(${SUDO} sed -n 's/^cluster\.id=//p' "${META_FILE}" | head -n1)"
fi
FRESH="true"
[[ -z "${EXISTING_CLUSTER_ID}" ]] || FRESH="false"

if [[ -n "${CLUSTER_ID}" ]]; then
    [[ "${CLUSTER_ID}" =~ ^[A-Za-z0-9_-]{22}$ ]] || die "Cluster ID 格式错误: '${CLUSTER_ID}'(应为 22 位 base64url 字符串)"
fi
if [[ "${FRESH}" == "false" ]]; then
    if [[ -n "${CLUSTER_ID}" && "${CLUSTER_ID}" != "${EXISTING_CLUSTER_ID}" ]]; then
        die "数据目录已属于集群 ${EXISTING_CLUSTER_ID},与指定的 -c ${CLUSTER_ID} 不一致。确认数据可丢弃后清空 ${DATA_DIR} 再部署"
    fi
    CLUSTER_ID="${EXISTING_CLUSTER_ID}"
elif [[ -z "${CLUSTER_ID}" && "${IS_FIRST_NODE}" == "false" && "${RENDER_MODE}" == "false" ]]; then
    die "节点 ${NODE_ID} 不是首节点(${FIRST_NODE_ID}),必须用 -c 传入首节点打印的 Cluster ID"
fi

SASL_PASSWORD_GENERATED="false"
if [[ "${SASL_ENABLED}" == "true" && -z "${SASL_PASSWORD}" ]]; then
    if [[ "${IS_FIRST_NODE}" == "false" ]]; then
        die "节点 ${NODE_ID} 不是首节点,必须用 -P 传入与首节点相同的管理员密码"
    elif [[ "${FRESH}" == "false" ]]; then
        die "本节点已部署过,重复执行必须用 -P 传入原管理员密码(重新生成会导致本节点无法与集群互认)"
    fi
    SASL_PASSWORD="$(gen_password)"
    SASL_PASSWORD_GENERATED="true"
    warn "未指定管理员密码,已随机生成。其余节点必须用 -P 传入相同密码(见完成提示)"
fi

# ---------------------------------------------------------------------------
# 部署信息汇总
# ---------------------------------------------------------------------------
log "开始部署 Kafka 多节点集群 (KRaft 模式) - 本机节点 ${NODE_ID}$([[ "${IS_FIRST_NODE}" == "true" ]] && echo ' (首节点)' || true)"
log "集群节点  : ${NORMALIZED_NODES}"
log "本机地址  : ${THIS_HOST} (客户端宣告: ${ADVERTISED_HOST})"
log "端口      : 免认证 ${PLAIN_PORT}$([[ "${SASL_ENABLED}" == "true" ]] && echo " / 认证 ${SASL_PORT}" || true) / controller ${CONTROLLER_PORT} / 内部复制 ${INTERNAL_PORT}"
log "安装目录  : ${INSTALL_DIR} (运行用户 ${RUN_USER})"
log "数据目录  : ${DATA_DIR}"
log "副本因子  : ${REPLICATION_FACTOR} (最小同步副本 ${MIN_ISR}, 默认分区 ${PARTITIONS})"
if [[ "${SASL_ENABLED}" == "true" ]]; then
    log "认证      : SASL_PLAINTEXT / PLAIN (管理员 ${SASL_USER}$( (( ${#EXTRA_NAMES[@]} > 0 )) && echo ", 追加账号 ${EXTRA_NAMES[*]}" || true))"
    if [[ "${AUTHZ_ENABLED}" == "true" ]]; then
        log "授权      : StandardAuthorizer (super.users=${SUPER_USERS})"
    else
        warn "授权      : 关闭 (认证账号与匿名连接均拥有全部权限)"
    fi
else
    warn "认证      : 无 (节点间与客户端均为明文无认证)"
fi
[[ "${RENDER_MODE}" == "false" ]] || log "仅渲染配置模式:输出到 ${RENDER_ONLY}(不下载/格式化/注册服务)"

# ---------------------------------------------------------------------------
# 获取、校验并解压安装包 —— 渲染模式跳过
# ---------------------------------------------------------------------------
if [[ "${RENDER_MODE}" == "false" ]]; then
    log "检测到 Java: $(java -version 2>&1 | head -n1)"

    if [[ -f "${KAFKA_HOME}/bin/kafka-server-start.sh" ]]; then
        warn "目标目录已存在 Kafka: ${KAFKA_HOME},跳过获取与解压"
        ls "${KAFKA_HOME}/libs/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.jar" >/dev/null 2>&1 || \
            warn "未找到 kafka_${SCALA_VERSION}-${KAFKA_VERSION}.jar,已安装版本可能不是 ${KAFKA_VERSION},请确认"
    else
        if [[ -n "${PACKAGE}" ]]; then
            SRC_TARBALL="${PACKAGE}"
            log "离线部署模式,使用本地安装包: ${PACKAGE}"
            if [[ -f "${PACKAGE}.sha512" ]]; then
                verify_sha512 "${PACKAGE}" "${PACKAGE}.sha512"
            else
                warn "未找到 ${PACKAGE}.sha512,跳过完整性校验(建议与安装包一同上传校验文件)"
            fi
        else
            log "下载 ${TARBALL} ..."
            ${DOWNLOADER} "${WORK_DIR}/${TARBALL}" "${MIRROR}/${TARBALL}" || die "下载失败: ${MIRROR}/${TARBALL}"
            SRC_TARBALL="${WORK_DIR}/${TARBALL}"
            if [[ "${SKIP_CHECKSUM}" == "true" ]]; then
                warn "已按 --skip-checksum 跳过 sha512 校验"
            else
                ${DOWNLOADER} "${WORK_DIR}/${TARBALL}.sha512" "${MIRROR}/${TARBALL}.sha512" || \
                    die "下载校验文件失败: ${MIRROR}/${TARBALL}.sha512(镜像不提供时可用 --skip-checksum 跳过)"
                verify_sha512 "${SRC_TARBALL}" "${WORK_DIR}/${TARBALL}.sha512"
            fi
        fi

        # 完整读取列表再取首行,避免 tar | head 在 pipefail 下触发 SIGPIPE
        TARLIST="$(tar -tzf "${SRC_TARBALL}" 2>/dev/null)" || die "安装包损坏或不是有效的 tgz 文件: ${SRC_TARBALL}"
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

    # 运行用户
    if [[ "${RUN_USER}" != "root" ]] && ! id "${RUN_USER}" >/dev/null 2>&1; then
        command -v useradd >/dev/null 2>&1 || die "系统无 useradd,请手动创建用户 ${RUN_USER} 或使用 --run-user root"
        NOLOGIN="$(command -v nologin 2>/dev/null || echo /bin/false)"
        ${SUDO} useradd -r -M -d "${KAFKA_HOME}" -s "${NOLOGIN}" "${RUN_USER}"
        log "已创建运行用户: ${RUN_USER}"
    fi

    # 首节点首次部署:生成 Cluster ID
    CLUSTER_ID_GENERATED="false"
    if [[ -z "${CLUSTER_ID}" ]]; then
        CLUSTER_ID="$("${KAFKA_HOME}/bin/kafka-storage.sh" random-uuid)"
        CLUSTER_ID_GENERATED="true"
        log "自动生成 Cluster ID: ${CLUSTER_ID}"
    fi
fi

# ---------------------------------------------------------------------------
# 集群参数指纹:所有节点必须一致,用于人工比对与重复执行保护
# ---------------------------------------------------------------------------
SORTED_EXTRA="$(printf '%s\n' "${EXTRA_USERS[@]:-}" | sed '/^$/d' | sort | tr '\n' ',')"
FP_SOURCE="v1|${KAFKA_VERSION}|${NORMALIZED_NODES}|cid=${CLUSTER_ID:-unset}|ports=${PLAIN_PORT},${SASL_PORT},${CONTROLLER_PORT},${INTERNAL_PORT}"
FP_SOURCE+="|sasl=${SASL_ENABLED}|authz=${AUTHZ_ENABLED}|rf=${REPLICATION_FACTOR}|isr=${MIN_ISR}|parts=${PARTITIONS}"
if [[ "${SASL_ENABLED}" == "true" ]]; then
    FP_SOURCE+="|admin=${SASL_USER}:${SASL_PASSWORD}|extra=${SORTED_EXTRA}"
fi
if [[ "${AUTHZ_ENABLED}" == "true" ]]; then
    FP_SOURCE+="|su=${SUPER_USERS}|everyone=${ALLOW_EVERYONE}|plain=${PLAIN_ACCESS}"
fi
FINGERPRINT="$(printf '%s' "${FP_SOURCE}" | sha256sum | cut -c1-16)"

if [[ "${RENDER_MODE}" == "true" ]]; then
    SERVER_PROPS="${RENDER_ONLY}/server.properties"
    CLIENT_PROPS="${RENDER_ONLY}/client-sasl.properties"
else
    SERVER_PROPS="${KAFKA_HOME}/config/kraft/server.properties"
    CLIENT_PROPS="${KAFKA_HOME}/config/client-sasl.properties"
    ${SUDO} test -f "${SERVER_PROPS}" || die "未找到配置文件: ${SERVER_PROPS}"

    # 重复执行保护:集群级参数(密码/账号/端口等)变化会导致本节点与集群不一致
    OLD_FP="$(${SUDO} sed -n 's/^# cluster-fingerprint: //p' "${SERVER_PROPS}" | head -n1)"
    if [[ -n "${OLD_FP}" && "${OLD_FP}" != "${FINGERPRINT}" ]]; then
        if [[ "${FORCE}" == "true" ]]; then
            warn "集群参数指纹变化 (${OLD_FP} -> ${FINGERPRINT}),已按 --force 继续。其余节点须同步修改!"
        else
            die "集群参数指纹变化 (${OLD_FP} -> ${FINGERPRINT}):密码、账号、端口、副本等集群级参数与上次部署不同。
        若是误操作,请使用与上次完全相同的参数;若确需修改,请加 --force 并在所有节点同步执行。"
        fi
    fi
    ${SUDO} cp -n "${SERVER_PROPS}" "${SERVER_PROPS}.orig" 2>/dev/null || true
fi

# ---------------------------------------------------------------------------
# 渲染监听器 / SASL / 授权 配置
# ---------------------------------------------------------------------------
#   PLAINTEXT      客户端免认证端口,始终开启
#   SASL_PLAINTEXT 客户端认证端口,SASL 开启时开启
#   INTERNAL       broker 间复制专用,宣告地址固定为 --nodes 中的集群内地址,不受 -H 影响
#   CONTROLLER     KRaft 仲裁,需被其它节点访问
LISTENERS="PLAINTEXT://:${PLAIN_PORT}"
ADV_LISTENERS="PLAINTEXT://${ADVERTISED_HOST}:${PLAIN_PORT}"
if [[ "${SASL_ENABLED}" == "true" ]]; then
    LISTENERS+=",SASL_PLAINTEXT://:${SASL_PORT}"
    ADV_LISTENERS+=",SASL_PLAINTEXT://${ADVERTISED_HOST}:${SASL_PORT}"
    INTERNAL_PROTO="SASL_PLAINTEXT"
else
    INTERNAL_PROTO="PLAINTEXT"
fi
LISTENERS+=",INTERNAL://:${INTERNAL_PORT},CONTROLLER://:${CONTROLLER_PORT}"
ADV_LISTENERS+=",INTERNAL://${THIS_HOST}:${INTERNAL_PORT}"
PROTO_MAP="CONTROLLER:${INTERNAL_PROTO},INTERNAL:${INTERNAL_PROTO},PLAINTEXT:PLAINTEXT"
[[ "${SASL_ENABLED}" == "false" ]] || PROTO_MAP+=",SASL_PLAINTEXT:SASL_PLAINTEXT"

SASL_CONF=""
if [[ "${SASL_ENABLED}" == "true" ]]; then
    ADMIN_LOGIN="username=\"${SASL_USER}\" password=\"${SASL_PASSWORD}\""
    JAAS_USERS="user_${SASL_USER}=\"${SASL_PASSWORD}\""
    for uentry in "${EXTRA_USERS[@]:-}"; do
        [[ -n "${uentry}" ]] || continue
        JAAS_USERS+=" user_${uentry%%:*}=\"${uentry#*:}\""
    done
    PLAIN_MODULE="org.apache.kafka.common.security.plain.PlainLoginModule required"

    AUTHZ_CONF=""
    if [[ "${AUTHZ_ENABLED}" == "true" ]]; then
        AUTHZ_CONF="
# === ACL 授权 ===
# super.users 分号分隔;User:${SASL_USER} 为集群内部通信身份,必须保留
authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer
super.users=${SUPER_USERS}
allow.everyone.if.no.acl.found=${ALLOW_EVERYONE}"
    fi

    SASL_CONF="
# === SASL/PLAIN 认证 ===
sasl.enabled.mechanisms=PLAIN
sasl.mechanism.inter.broker.protocol=PLAIN
sasl.mechanism.controller.protocol=PLAIN
# 客户端认证端口:接受管理员及追加账号
listener.name.sasl_plaintext.plain.sasl.jaas.config=${PLAIN_MODULE} ${ADMIN_LOGIN} ${JAAS_USERS};
# broker 间复制 / controller 仲裁:仅管理员账号(集群内部身份)
listener.name.internal.plain.sasl.jaas.config=${PLAIN_MODULE} ${ADMIN_LOGIN} user_${SASL_USER}=\"${SASL_PASSWORD}\";
listener.name.controller.plain.sasl.jaas.config=${PLAIN_MODULE} ${ADMIN_LOGIN} user_${SASL_USER}=\"${SASL_PASSWORD}\";
${AUTHZ_CONF}
"
fi

log "写入 KRaft 集群配置 (node.id=${NODE_ID}) ..."
TMP_CONF="${WORK_DIR}/server.properties"
cat > "${TMP_CONF}" <<EOF
# === 由 ${SCRIPT_TAG} 自动生成 (KRaft 多节点, node ${NODE_ID}/${NODE_COUNT}) ===
# cluster-fingerprint: ${FINGERPRINT}

# 每个节点同时承担 broker 和 controller 角色
process.roles=broker,controller
node.id=${NODE_ID}
controller.quorum.voters=${QUORUM_VOTERS}

# 监听配置
#   PLAINTEXT      :${PLAIN_PORT}  客户端免认证
#   SASL_PLAINTEXT :${SASL_PORT}  客户端认证(仅 SASL 开启时)
#   INTERNAL       :${INTERNAL_PORT}  broker 间复制
#   CONTROLLER     :${CONTROLLER_PORT}  KRaft 仲裁
listeners=${LISTENERS}
advertised.listeners=${ADV_LISTENERS}
inter.broker.listener.name=INTERNAL
controller.listener.names=CONTROLLER
listener.security.protocol.map=${PROTO_MAP}
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
${SUDO} chmod 600 "${SERVER_PROPS}"   # 含明文密码

# 客户端认证配置(管理员身份,供运维工具 --command-config 使用)
if [[ "${SASL_ENABLED}" == "true" ]]; then
    TMP_CLIENT="${WORK_DIR}/client-sasl.properties"
    cat > "${TMP_CLIENT}" <<EOF
# === 客户端 SASL/PLAIN 连接配置 (由 ${SCRIPT_TAG} 自动生成,管理员身份,仅供运维使用) ===
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="${SASL_USER}" password="${SASL_PASSWORD}";
EOF
    ${SUDO} cp "${TMP_CLIENT}" "${CLIENT_PROPS}"
    ${SUDO} chmod 600 "${CLIENT_PROPS}"
    log "已生成客户端认证配置: ${CLIENT_PROPS}"
fi

if [[ "${RENDER_MODE}" == "true" ]]; then
    log "配置渲染完成: ${SERVER_PROPS}"
    log "集群参数指纹: ${FINGERPRINT}$([[ -z "${CLUSTER_ID}" ]] && echo '  (未传 -c,指纹不含 Cluster ID,与实际部署会不同)' || true)"
    exit 0
fi

# ---------------------------------------------------------------------------
# 格式化存储目录 (KRaft 必需,集群各节点须用同一 Cluster ID)
# ---------------------------------------------------------------------------
if [[ "${FRESH}" == "false" ]]; then
    warn "数据目录已格式化 (Cluster ID ${CLUSTER_ID}),跳过 storage format"
else
    ${SUDO} "${KAFKA_HOME}/bin/kafka-storage.sh" format -t "${CLUSTER_ID}" -c "${SERVER_PROPS}"
    log "存储目录格式化完成 (Cluster ID ${CLUSTER_ID})"
fi

if [[ "${RUN_USER}" != "root" ]]; then
    ${SUDO} chown -R "${RUN_USER}:${RUN_USER}" "${INSTALL_DIR}" "${DATA_DIR}"
    log "已将安装与数据目录属主设为 ${RUN_USER}"
fi

# ---------------------------------------------------------------------------
# 注册 systemd 服务
# ---------------------------------------------------------------------------
SERVICE_FILE="/etc/systemd/system/kafka.service"
if command -v systemctl >/dev/null 2>&1; then
    log "注册 systemd 服务: ${SERVICE_FILE}"
    JAVA_REAL="$(readlink -f "$(command -v java)")"
    JAVA_HOME_DIR="$(dirname "$(dirname "${JAVA_REAL}")")"
    HEAP_LINE=""
    [[ -z "${HEAP_SIZE}" ]] || HEAP_LINE="Environment=\"KAFKA_HEAP_OPTS=-Xms${HEAP_SIZE} -Xmx${HEAP_SIZE}\""
    TMP_SVC="${WORK_DIR}/kafka.service"
    cat > "${TMP_SVC}" <<EOF
[Unit]
Description=Apache Kafka (KRaft cluster node ${NODE_ID})
Documentation=https://kafka.apache.org/documentation/
Wants=network-online.target
After=network-online.target

[Service]
Type=simple
User=${RUN_USER}
Group=${RUN_USER}
Environment="JAVA_HOME=${JAVA_HOME_DIR}"
${HEAP_LINE}
ExecStart=${KAFKA_HOME}/bin/kafka-server-start.sh ${SERVER_PROPS}
# 由 systemd 发送 SIGTERM 触发优雅关闭;JVM 因 SIGTERM 退出码为 143,视为正常
SuccessExitStatus=143
TimeoutStopSec=180
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

    # controller 监听在进程启动后即开放,不依赖仲裁;以此判断本节点进程是否正常
    STARTED="false"
    for ((i = 0; i < 60; i++)); do
        STATE="$(${SUDO} systemctl is-active kafka 2>/dev/null || true)"
        [[ "${STATE}" != "failed" ]] || break
        if [[ "${STATE}" == "active" ]] && tcp_open 127.0.0.1 "${CONTROLLER_PORT}"; then
            STARTED="true"; break
        fi
        sleep 1
    done
    [[ "${STARTED}" == "true" ]] || die "Kafka 进程未能正常启动(60 秒内 controller 端口未就绪),请查看: journalctl -u kafka -e"
    log "本节点 Kafka 进程已启动 (controller 端口 ${CONTROLLER_PORT} 已就绪)"

    # broker 端口需等仲裁选主、broker 注册后才开放
    BROKER_READY="false"
    for ((i = 0; i < 20; i++)); do
        if tcp_open 127.0.0.1 "${PLAIN_PORT}"; then BROKER_READY="true"; break; fi
        sleep 1
    done
    if [[ "${BROKER_READY}" == "true" ]]; then
        log "broker 端口 ${PLAIN_PORT} 已就绪,本节点已加入集群"
    else
        warn "broker 端口尚未就绪:需多数节点启动后仲裁才能选主,若其它节点尚未部署属正常"
    fi
else
    warn "未检测到 systemd,跳过服务注册。可手动启动:"
    echo "  runuser -u ${RUN_USER} -- ${KAFKA_HOME}/bin/kafka-server-start.sh -daemon ${SERVER_PROPS}"
fi

# ---------------------------------------------------------------------------
# 完成提示
# ---------------------------------------------------------------------------
cat <<EOF

========================================================================
 Kafka ${KAFKA_VERSION} 集群节点 ${NODE_ID}/${NODE_COUNT} 部署完成!
------------------------------------------------------------------------
 安装目录 : ${KAFKA_HOME} (运行用户 ${RUN_USER})
 数据目录 : ${DATA_DIR}
 配置文件 : ${SERVER_PROPS}
 仲裁成员 : ${QUORUM_VOTERS}
 副本因子 : ${REPLICATION_FACTOR} (min.insync.replicas=${MIN_ISR})

 端口(防火墙需放通,所有节点相同):
   免认证   : ${PLAIN_PORT}  客户端 → 集群      宣告 ${ADVERTISED_HOST}:${PLAIN_PORT}
EOF
if [[ "${SASL_ENABLED}" == "true" ]]; then
    echo "   认证     : ${SASL_PORT}  客户端 → 集群      宣告 ${ADVERTISED_HOST}:${SASL_PORT}"
fi
cat <<EOF
   内部复制 : ${INTERNAL_PORT}  仅节点之间          宣告 ${THIS_HOST}:${INTERNAL_PORT}
   仲裁     : ${CONTROLLER_PORT}  仅节点之间

 集群一致性(其余节点部署时必须相同):
   Cluster ID   : ${CLUSTER_ID}$([[ "${CLUSTER_ID_GENERATED:-false}" == "true" ]] && echo '  (本次生成,其余节点用 -c 传入)' || true)
   参数指纹     : ${FINGERPRINT}  (所有节点应一致)
EOF

if [[ "${SASL_ENABLED}" == "true" ]]; then
    cat <<EOF

 SASL/PLAIN 认证:
   管理员   : ${SASL_USER} (集群内部通信身份,不建议交给业务使用)
   密码     : ${SASL_PASSWORD}$([[ "${SASL_PASSWORD_GENERATED}" == "true" ]] && echo '  (随机生成,其余节点用 -P 传入)' || true)
   客户端配置 : ${CLIENT_PROPS} (管理员身份,权限 600)
EOF
    (( ${#EXTRA_NAMES[@]} == 0 )) || echo "   追加账号 : ${EXTRA_NAMES[*]}"

    if [[ "${AUTHZ_ENABLED}" == "true" ]]; then
        cat <<EOF

 ACL 授权: StandardAuthorizer
   super.users      : ${SUPER_USERS}
   无 ACL 资源默认  : $([[ "${ALLOW_EVERYONE}" == "true" ]] && echo 放行 || echo 拒绝)
EOF
        if [[ "${PLAIN_ACCESS}" == "full" ]]; then
            echo "   免认证端口 ${PLAIN_PORT}  : 匿名用户为超级用户,拥有全部权限"
        else
            echo "   免认证端口 ${PLAIN_PORT}  : 匿名用户 User:ANONYMOUS 需单独授权"
        fi
        cat <<EOF

   为账号授权(任一节点执行一次即可,全集群生效;匿名用户用 User:ANONYMOUS):
     # 生产者
     ${KAFKA_HOME}/bin/kafka-acls.sh --bootstrap-server ${BOOTSTRAP_SASL} \\
         --command-config ${CLIENT_PROPS} \\
         --add --allow-principal User:<账号> --producer --topic '<topic>'
     # 消费者
     ${KAFKA_HOME}/bin/kafka-acls.sh --bootstrap-server ${BOOTSTRAP_SASL} \\
         --command-config ${CLIENT_PROPS} \\
         --add --allow-principal User:<账号> --consumer --topic '<topic>' --group '<group>'
EOF
    else
        echo
        echo "   授权: 关闭 (认证账号与匿名连接均拥有全部权限)"
    fi
fi

if [[ "${AUTHZ_ENABLED}" == "false" || "${PLAIN_ACCESS}" == "full" ]]; then
    cat <<EOF

 [安全提醒] 免认证端口 ${PLAIN_PORT} 拥有集群全部权限,请通过防火墙/安全组
            限制访问来源,否则认证端口起不到访问控制作用。
EOF
fi

if [[ "${SASL_ENABLED}" == "true" ]]; then
    ADMIN_BOOT="${BOOTSTRAP_SASL}"
    ADMIN_CFG=" --command-config ${CLIENT_PROPS}"
else
    ADMIN_BOOT="${BOOTSTRAP_PLAIN}"
    ADMIN_CFG=""
fi
cat <<EOF

 常用命令:
   启动/停止/状态: systemctl {start|stop|status} kafka
   日志:           journalctl -u kafka -f

 全部节点部署完成后,验证集群(任一节点执行):
   # 仲裁状态
   ${KAFKA_HOME}/bin/kafka-metadata-quorum.sh --bootstrap-server ${ADMIN_BOOT}${ADMIN_CFG} describe --status
   # 创建并查看 topic
   ${KAFKA_HOME}/bin/kafka-topics.sh --create --topic test \\
       --bootstrap-server ${ADMIN_BOOT} \\
       --partitions ${PARTITIONS} --replication-factor ${REPLICATION_FACTOR}${ADMIN_CFG}
   ${KAFKA_HOME}/bin/kafka-topics.sh --describe --topic test \\
       --bootstrap-server ${ADMIN_BOOT}${ADMIN_CFG}
EOF
if [[ "${SASL_ENABLED}" == "true" ]]; then
    cat <<EOF
   # 免认证端口连通性$([[ "${AUTHZ_ENABLED}" == "true" && "${PLAIN_ACCESS}" == "acl" ]] && echo '(匿名用户需先授权)' || true)
   ${KAFKA_HOME}/bin/kafka-topics.sh --list --bootstrap-server ${BOOTSTRAP_PLAIN}
EOF
fi
echo "========================================================================"
