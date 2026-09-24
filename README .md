# Kafka 3.9.0 部署脚本（单机版 / 集群版）

本目录包含两个 **Apache Kafka 3.9.0（kafka_2.13-3.9.0）** 一键部署脚本，均采用 **KRaft 模式**（不依赖 ZooKeeper），支持在线下载与离线安装，并自动注册为 systemd 服务。

| 脚本 | 用途 |
|---|---|
| `deploy_kafka.sh` | 单机版：一台机器，单节点同时承担 broker 与 controller 角色 |
| `deploy_kafka_cluster.sh` | 集群版：多台机器各执行一次，所有节点为 broker + controller 合体角色，共同组成仲裁 |

## 目录

- [一、选型与对比](#一选型与对比)
- [二、通用说明](#二通用说明)
  - [环境要求](#环境要求)
  - [端口约定](#端口约定)
  - [安装包获取](#安装包获取)
- [三、单机版 deploy_kafka.sh](#三单机版-deploy_kafkash)
- [四、集群版 deploy_kafka_cluster.sh](#四集群版-deploy_kafka_clustersh)
- [五、客户端连接](#五客户端连接)
- [六、服务管理](#六服务管理)
- [七、安全注意事项](#七安全注意事项)
- [八、故障排查](#八故障排查)
- [九、卸载](#九卸载)
- [十、已知限制](#十已知限制)

---

## 一、选型与对比

- **开发、测试、边缘小规模业务**：用单机版，一条命令完成部署，没有副本冗余。
- **生产环境**：用集群版（至少 3 节点），具备副本冗余、节点故障容忍和 ACL 权限管控。

两个脚本的主要差异：

| 项目 | 单机版 | 集群版 |
|---|---|---|
| 节点数 | 1 | ≥ 3（2 节点需显式允许） |
| 9092 免认证端口 | 始终开启 | 始终开启 |
| 9094 认证端口 | 默认关闭，`--sasl` 开启 | 默认开启，`--no-sasl` 关闭 |
| 认证端口自定义 | `-s` | `-s` |
| 内部复制端口 | 无（走 9092） | 9095，独立监听 |
| controller 端口 9093 | 只绑定 127.0.0.1 | 对集群节点开放，SASL 认证 |
| SASL 账号 | 1 个 | 管理员 + 任意个追加账号 |
| ACL 授权 | 不支持 | 默认开启（StandardAuthorizer） |
| 副本 / 分区 | 固定为 1 | 按节点数自动推导，可自定义 |
| 运行用户 | root | 默认 `kafka`，可自定义 |
| 堆内存参数 | 无（Kafka 默认 1G） | `--heap` |
| 安装包 sha512 校验 | 无 | 在线强制校验，离线有校验文件时校验 |
| 随机密码 | 20 位，基于 bash `$RANDOM` | 24 位，基于 `/dev/urandom` |
| 重复执行保护 | 无 | 参数指纹比对，防止误改 |
| 启动检测 | 固定等待 3 秒 | 最多 60 秒，检测端口就绪 |
| 配置预渲染 | 无 | `--render-only` |

---

## 二、通用说明

### 环境要求

| 项目 | 要求 |
|---|---|
| 操作系统 | Linux，推荐使用 systemd 的发行版（CentOS 7+/RHEL/Ubuntu 18.04+ 等） |
| 权限 | root，或具备 sudo 权限的用户 |
| Java | 已安装且 `java` 在 PATH 中；脚本只检查是否存在，推荐 JDK 11 或 17 |
| 下载工具 | 在线模式需 `curl` 或 `wget`；离线模式不需要 |
| 其他命令 | 单机版：`tar`、`hostname`；集群版另需 `sha512sum`、`sha256sum`、`timeout`、`useradd` |
| 集群网络 | 集群版节点之间 9093、9095 互通，各节点地址可互相访问 |

### 端口约定

两个脚本采用统一的端口约定：

| 端口（默认） | 监听器 | 用途 | 单机版 | 集群版 |
|---|---|---|---|---|
| 9092 | PLAINTEXT | 客户端免认证 | 始终开启，`-p` 可改 | 始终开启，`-p` 可改 |
| 9094 | SASL_PLAINTEXT | 客户端 SASL/PLAIN 认证 | `--sasl` 时开启，`-s` 可改 | 默认开启，`-s` 可改 |
| 9093 | CONTROLLER | KRaft 仲裁 | 仅 127.0.0.1，不可改 | 节点间，`--controller-port` 可改 |
| 9095 | INTERNAL | broker 间复制 | 无 | 节点间，`--internal-port` 可改 |

各端口必须互不相同，脚本会做冲突检查。集群版所有节点使用相同端口。

防火墙 / 安全组放通建议：

| 端口 | 放通来源 |
|---|---|
| 9092 | 仅可信业务主机（免认证，权限说明见[安全注意事项](#七安全注意事项)） |
| 9094 | 需要访问 Kafka 的客户端 |
| 9093、9095 | 集群版：仅集群内其他节点；单机版：无需放通 |

### 安装包获取

- **在线**：默认从 `https://archive.apache.org/dist/kafka/3.9.0` 下载，可用 `-m` 换成镜像，例如 `https://mirrors.aliyun.com/apache/kafka/3.9.0`。国内镜像通常只保留较新版本，若 3.9.0 已下架请改用官方归档站或离线模式。
- **离线**：提前上传 `kafka_2.13-3.9.0.tgz`，用 `-f` 指定路径。集群版建议同时上传官方校验文件 `kafka_2.13-3.9.0.tgz.sha512` 到同一目录，脚本会自动校验。

两个脚本都会校验归档结构（顶层目录必须为 `kafka_2.13-3.9.0`），并在安装目录已有 Kafka 时跳过解压。

---

## 三、单机版 deploy_kafka.sh

### 快速开始

```bash
chmod +x deploy_kafka.sh

# 最简部署：安装到 /opt/kafka，仅开启 9092 免认证端口
sudo ./deploy_kafka.sh

# 同时开启 9094 认证端口（密码随机生成，结束时打印）
sudo ./deploy_kafka.sh --sasl
```

### 参数说明

| 短参数 | 长参数 | 说明 | 默认值 |
|---|---|---|---|
| `-i` | `--install-dir DIR` | 安装目录 | `/opt/kafka` |
| `-d` | `--data-dir DIR` | 数据目录 | `/var/lib/kafka/data` |
| `-p` | `--port PORT` | 免认证端口，始终开启 | `9092` |
| `-H` | `--advertised-host HOST` | 客户端连接用的宣告地址 | `hostname -I` 的第一个 IP，失败时为 `localhost` |
| `-f` | `--package FILE` | 本地安装包，提供后进入离线模式 | 空 |
| `-m` | `--mirror URL` | 下载地址前缀 | 官方归档站 |
| `-S` | `--sasl` | 额外开启认证端口 | 关闭 |
| `-s` | `--sasl-port PORT` | 认证端口，仅 `--sasl` 时生效 | `9094` |
| `-U` | `--sasl-user USER` | SASL 用户名，仅 `--sasl` 时生效 | `admin` |
| `-P` | `--sasl-password PWD` | SASL 密码，仅 `--sasl` 时生效 | 随机生成 |
| `-h` | `--help` | 显示帮助 | — |

参数约束：端口为 1–65535 的整数，免认证端口、认证端口和 9093 三者互不相同；用户名只允许字母、数字及 `.`、`_`、`-`；密码不能含双引号、反斜杠或空白字符；只传 `-s` 未加 `--sasl` 时会警告，认证端口不开启。

### 使用示例

```bash
sudo ./deploy_kafka.sh -i /usr/local/kafka -d /data/kafka              # 自定义目录
sudo ./deploy_kafka.sh -f ./kafka_2.13-3.9.0.tgz -i /usr/local/kafka   # 离线部署
sudo ./deploy_kafka.sh -H 192.168.10.21                                # 多网卡时显式指定宣告地址（推荐）
sudo ./deploy_kafka.sh --sasl -U admin -P 'MyS3cret!'                  # 开启认证端口
sudo ./deploy_kafka.sh --sasl -s 19094 -U app                          # 自定义认证端口
```

### 生成的监听配置

以 `--sasl -s 19094 -H 10.0.0.5` 为例：

```properties
listeners=PLAINTEXT://:9092,SASL_PLAINTEXT://:19094,CONTROLLER://127.0.0.1:9093
inter.broker.listener.name=PLAINTEXT
advertised.listeners=PLAINTEXT://10.0.0.5:9092,SASL_PLAINTEXT://10.0.0.5:19094
controller.quorum.voters=1@127.0.0.1:9093
```

单机版没有 ACL，9092 和 9094 的连接者都拥有全部权限，认证端口只负责验证身份。

### 部署流程

1. 解析并校验参数；未指定 `-H` 时自动探测 IP；开启 SASL 且未传 `-P` 时随机生成密码。
2. 检查 root/sudo、Java、下载工具或离线包。
3. 下载或使用离线包，校验归档结构后解压（已安装则跳过）。
4. 创建数据目录。
5. 首次执行时备份原始配置为 `server.properties.orig`，写入新配置；开启 SASL 时生成 `client-sasl.properties`，两者权限均为 600。
6. 数据目录未格式化时执行 `kafka-storage.sh format`。
7. 写入 systemd unit，enable 并 restart，3 秒后检查服务是否 active。
8. 输出端口、认证信息和验证命令。

### 部署产物

| 路径（默认） | 说明 |
|---|---|
| `/opt/kafka/config/kraft/server.properties` | 节点配置（开启 SASL 时权限 600） |
| `/opt/kafka/config/kraft/server.properties.orig` | 原始配置备份（仅首次） |
| `/opt/kafka/config/client-sasl.properties` | 客户端 SASL 配置（仅 `--sasl`），权限 600 |
| `/opt/kafka/logs/` | Kafka 运行日志 |
| `/var/lib/kafka/data/` | 消息数据与 KRaft 元数据 |
| `/etc/systemd/system/kafka.service` | systemd 服务单元，以 root 运行 |

主要 broker 参数：分区数、各内部 topic 副本数均为 1；消息保留 168 小时；日志段 1 GB。

### 重复执行注意事项

单机版**没有重复执行保护**：

- 配置文件每次都按本次参数重新生成并覆盖。
- 已开启 SASL 的环境重复执行时若未带 `-P`，会**重新生成随机密码**，现有客户端将无法认证。
- 若未带 `--sasl`，认证端口会被**直接移除**。
- 每次执行都会 restart 服务。
- 安装目录已有 Kafka 时跳过解压，不校验版本。

修改配置前请确认参数与首次部署一致，或改为手动编辑 `server.properties` 后执行 `systemctl restart kafka`。

---

## 四、集群版 deploy_kafka_cluster.sh

### 快速开始：三节点集群

假设三台机器为 10.0.0.1、10.0.0.2、10.0.0.3。**`--nodes` 列表中的第一个节点为首节点**，必须最先部署。

```bash
NODES=1@10.0.0.1,2@10.0.0.2,3@10.0.0.3

# 节点 1（首节点）：可不传 -P，会随机生成密码并在结束时打印
sudo ./deploy_kafka_cluster.sh -N $NODES -n 1 -P 'AdminP@ss'
# 记下输出中的 Cluster ID 和参数指纹

# 节点 2、3：必须传入相同的密码和 Cluster ID，其余集群级参数也要一致
sudo ./deploy_kafka_cluster.sh -N $NODES -n 2 -P 'AdminP@ss' -c <ClusterID>
sudo ./deploy_kafka_cluster.sh -N $NODES -n 3 -P 'AdminP@ss' -c <ClusterID>
```

完成后，核对三个节点输出的**参数指纹**完全一致，再执行[集群验证](#集群验证)。

节点 1 部署完成时，其他节点还没启动、仲裁无法选主，会提示"broker 端口尚未就绪"，属于正常现象。多数节点（3 节点集群中为 2 台）启动后即可选主。

### 参数说明

**必填**

| 参数 | 说明 |
|---|---|
| `-N`, `--nodes LIST` | 所有节点，格式 `id@host`，逗号分隔。第一个为首节点。host 必须是节点间互通的地址，不支持 IPv6，不能带端口 |
| `-n`, `--node-id ID` | 本机节点 id，必须出现在 `--nodes` 中；`01` 与 `1` 视为同一 id |

**集群一致性（所有节点必须相同）**

| 参数 | 说明 |
|---|---|
| `-c`, `--cluster-id UUID` | 集群 ID（22 位 base64url）。首节点首次部署可不填；其余节点必填 |
| `-P`, `--sasl-password PWD` | 管理员密码。首节点首次部署可不填；其余节点及任何节点的重复执行必填 |

**端口**

| 参数 | 说明 | 默认值 |
|---|---|---|
| `-p`, `--port PORT` | 免认证客户端端口 | `9092` |
| `-s`, `--sasl-port PORT` | 认证客户端端口 | `9094` |
| `--controller-port PORT` | 仲裁端口 | `9093` |
| `--internal-port PORT` | broker 间复制端口 | `9095` |

**目录、网络与运行**

| 参数 | 说明 | 默认值 |
|---|---|---|
| `-i`, `--install-dir DIR` | 安装目录（绝对路径，不能是 `/opt` 等系统目录本身） | `/opt/kafka` |
| `-d`, `--data-dir DIR` | 数据目录（同上） | `/var/lib/kafka/data` |
| `-H`, `--advertised-host HOST` | 客户端宣告地址，只影响 9092/9094，节点间复制不受影响 | `--nodes` 中本机 host |
| `--run-user USER` | 运行用户，不存在时自动创建；传 `root` 则以 root 运行 | `kafka` |
| `--heap SIZE` | JVM 堆大小，如 `2G`、`1536M` | Kafka 默认 1G |

**安装包**

| 参数 | 说明 | 默认值 |
|---|---|---|
| `-f`, `--package FILE` | 本地安装包；同目录有 `FILE.sha512` 时自动校验 | 空 |
| `-m`, `--mirror URL` | 下载地址前缀 | 官方归档站 |
| `--skip-checksum` | 在线下载时跳过 sha512 校验（不推荐） | 关闭 |

**副本与分区**

| 参数 | 说明 | 默认值 |
|---|---|---|
| `-r`, `--replication-factor N` | 默认副本因子，也用于内部 topic | min(3, 节点数) |
| `--min-isr N` | 最小同步副本 | 副本因子 ≥ 3 时为 2，否则为 1 |
| `--partitions N` | 默认分区数 | `3` |

**认证与授权**

| 参数 | 说明 | 默认值 |
|---|---|---|
| `--no-sasl` | 关闭 SASL：不开认证端口，节点间明文，同时关闭授权 | SASL 开启 |
| `-U`, `--sasl-user USER` | 管理员账号，也是集群内部通信身份 | `admin` |
| `--extra-user U:P` | 追加业务账号，可重复 | 无 |
| `--super-users LIST` | 追加超级用户，分号或逗号分隔；管理员始终自动包含 | 无 |
| `--plain-access MODE` | 9092 的授权策略：`full` 或 `acl` | `full` |
| `--no-authz` | 关闭 ACL，仅保留认证 | 授权开启 |
| `--allow-everyone` | 没有 ACL 的资源默认放行 | 拒绝 |

**其他**

| 参数 | 说明 |
|---|---|
| `--allow-two-nodes` | 允许 2 节点集群（任一节点故障即整体不可用） |
| `--force` | 重复执行时允许集群参数指纹变化 |
| `--render-only DIR` | 只渲染配置到 DIR 后退出，不下载、不格式化、不注册服务 |
| `-h`, `--help` | 显示帮助 |

参数约束：账号名只允许字母、数字及 `.`、`_`、`-`，不能用保留名 `ANONYMOUS`；追加账号不能与管理员重名或重复；密码不能含双引号、反斜杠或空白字符；同一主机不能在 `--nodes` 中出现两次；选项值不能以 `--` 开头。

### 生成的监听配置

以节点 2、`-s 19094 -H 1.2.3.4` 为例：

```properties
listeners=PLAINTEXT://:9092,SASL_PLAINTEXT://:19094,INTERNAL://:9095,CONTROLLER://:9093
advertised.listeners=PLAINTEXT://1.2.3.4:9092,SASL_PLAINTEXT://1.2.3.4:19094,INTERNAL://10.0.0.2:9095
inter.broker.listener.name=INTERNAL
controller.listener.names=CONTROLLER
controller.quorum.voters=1@10.0.0.1:9093,2@10.0.0.2:9093,3@10.0.0.3:9093
```

INTERNAL 的宣告地址始终取 `--nodes` 中的集群内地址，因此客户端走公网/NAT、节点间走内网的场景可以直接用 `-H` 实现。

### 集群一致性保障

**首节点与非首节点的规则**

| 场景 | `-c` | `-P` |
|---|---|---|
| 首节点，首次部署 | 可不填，自动生成 | 可不填，自动生成 |
| 非首节点，首次部署 | 必填 | 必填 |
| 任何节点，重复执行 | 可不填，自动读取数据目录中的值；填了必须一致 | 必填 |

**集群参数指纹**

每次部署都会打印一个 16 位参数指纹，并写入 `server.properties` 头部的 `# cluster-fingerprint:` 注释。

- **计算来源**：Kafka 版本、节点列表、Cluster ID、四个端口、SASL/授权开关、管理员账号和密码、追加账号、super.users、`--plain-access`、`--allow-everyone`、副本因子、最小同步副本、默认分区数。
- **不包含**：只影响本节点的参数（`-n`、`-H`、目录、运行用户、堆内存），这些各节点可以不同。
- **用法**：所有节点部署后比对指纹，应完全一致。
- **重复执行保护**：本次指纹与配置文件中记录的不同时拒绝执行，防止漏写 `--extra-user`、换了密码等误操作；确需修改时加 `--force`。

指纹是参数的单向哈希截断，不能反推出密码。

**部署前预渲染**

`--render-only` 不需要 root 和 Java，可在任意机器上核对各节点配置：

```bash
./deploy_kafka_cluster.sh -N $NODES -n 2 -P 'AdminP@ss' -c <ClusterID> --render-only ./render-node2
```

### 认证与授权

**身份模型**

| 连接方式 | Kafka 身份 |
|---|---|
| 连接 9092 | `User:ANONYMOUS` |
| 管理员账号连接 9094 | `User:admin`（或 `-U` 指定的账号） |
| 追加账号连接 9094 | `User:<账号>` |
| 节点之间（9095、9093） | 管理员身份，脚本自动配置 |

管理员账号是集群内部通信身份，始终在 `super.users` 中，不能移除。业务请使用 `--extra-user` 分配的独立账号。

**9092 的权限（`--plain-access`）**

| 模式 | 效果 | 适用场景 |
|---|---|---|
| `full`（默认） | 匿名用户为超级用户，9092 拥有全部权限 | 9092 只对可信内网开放，要求免认证直接可用 |
| `acl` | 匿名用户需通过 ACL 单独授权 | 希望 9092 只有部分权限，如只读 |

`--no-authz` 或 `--no-sasl` 时不做权限管控，9092 始终拥有全部权限。

**ACL 管理**

追加账号默认没有任何权限（除非加 `--allow-everyone`）。在任一节点执行一次即可，全集群生效：

```bash
KAFKA=/opt/kafka
BOOT=10.0.0.1:9094,10.0.0.2:9094,10.0.0.3:9094
CFG=$KAFKA/config/client-sasl.properties

# 生产者权限
$KAFKA/bin/kafka-acls.sh --bootstrap-server $BOOT --command-config $CFG \
    --add --allow-principal User:app --producer --topic 'orders'

# 消费者权限（topic + 消费组）
$KAFKA/bin/kafka-acls.sh --bootstrap-server $BOOT --command-config $CFG \
    --add --allow-principal User:app --consumer --topic 'orders' --group 'order-svc'

# 按前缀授权
$KAFKA/bin/kafka-acls.sh --bootstrap-server $BOOT --command-config $CFG \
    --add --allow-principal User:app --producer --topic 'orders-' --resource-pattern-type prefixed

# 查看 / 删除
$KAFKA/bin/kafka-acls.sh --bootstrap-server $BOOT --command-config $CFG --list
$KAFKA/bin/kafka-acls.sh --bootstrap-server $BOOT --command-config $CFG \
    --remove --allow-principal User:app --producer --topic 'orders'
```

`--plain-access acl` 模式下给 9092 授权时，principal 写 `User:ANONYMOUS`。新增 ACL 无需重启；**新增账号需要修改所有节点配置并滚动重启**，见[修改集群级参数](#修改集群级参数)。

### 使用示例

```bash
NODES=1@10.0.0.1,2@10.0.0.2,3@10.0.0.3

# 自定义认证端口，追加两个业务账号
sudo ./deploy_kafka_cluster.sh -N $NODES -n 1 -P 'AdminP@ss' -s 19094 \
    --extra-user app:AppPwd123 --extra-user report:RptPwd456

# 客户端通过公网/NAT 访问（节点间复制仍走内网）
sudo ./deploy_kafka_cluster.sh -N $NODES -n 2 -P 'AdminP@ss' -c <ClusterID> -H 47.100.1.2

# 9092 按 ACL 授权
sudo ./deploy_kafka_cluster.sh -N $NODES -n 1 -P 'AdminP@ss' --plain-access acl

# 追加运维超级用户
sudo ./deploy_kafka_cluster.sh -N $NODES -n 1 -P 'AdminP@ss' --extra-user ops:OpsPwd789 --super-users 'User:ops'

# 离线部署，指定堆内存和目录
sudo ./deploy_kafka_cluster.sh -N $NODES -n 1 -P 'AdminP@ss' -f /root/kafka_2.13-3.9.0.tgz \
    --heap 4G -i /data/kafka -d /data/kafka-data

# 纯明文集群（仅限完全可信的内网）
sudo ./deploy_kafka_cluster.sh -N $NODES -n 1 --no-sasl
```

同一集群的其余节点，必须使用与首节点**完全相同的集群级参数**，并额外带上 `-c`。

### 部署流程

1. **参数校验**：端口格式与冲突、目录安全、账号密码字符、super.users 格式。
2. **解析拓扑**：检查 id 和 host 是否重复，确定首节点，生成仲裁成员列表；2 节点需 `--allow-two-nodes`。
3. **推导副本配置**。
4. **前置检查**：root/sudo、Java、下载工具或离线包。
5. **一致性检查**：读取已有 Cluster ID，按规则检查 `-c`、`-P`；首节点首次部署时生成密码。
6. **获取安装包**：已安装则跳过并检查版本；否则下载或使用离线包，校验 sha512 和归档结构后解压。
7. **准备环境**：创建数据目录与运行用户；首节点首次部署时生成 Cluster ID。
8. **指纹检查**：与已有配置比对，不一致时拒绝执行（除非 `--force`）。
9. **生成配置**：首次备份原始配置，写入新配置与客户端配置，权限均为 600。
10. **存储格式化**：未格式化时执行 `kafka-storage.sh format`。
11. **修改属主**：安装目录与数据目录属主改为运行用户。
12. **注册并启动服务**：最多等 60 秒 controller 端口就绪，再检查 broker 端口。
13. **输出汇总**：端口、Cluster ID、参数指纹、账号、ACL 示例与验证命令。

### 部署产物

| 路径（默认） | 说明 |
|---|---|
| `/opt/kafka/` | 安装目录，属主为运行用户 |
| `/opt/kafka/config/kraft/server.properties` | 节点配置，含指纹和所有账号明文密码，权限 600 |
| `/opt/kafka/config/kraft/server.properties.orig` | 原始配置备份（仅首次） |
| `/opt/kafka/config/client-sasl.properties` | 管理员身份的客户端配置，仅供运维，权限 600 |
| `/opt/kafka/logs/` | Kafka 运行日志 |
| `/var/lib/kafka/data/` | 消息数据与 KRaft 元数据，`meta.properties` 记录 Cluster ID |
| `/etc/systemd/system/kafka.service` | systemd 服务单元 |

主要 broker 参数：`default.replication.factor` 与各内部 topic 副本数取副本因子；`min.insync.replicas` 与 `transaction.state.log.min.isr` 取最小同步副本；消息保留 168 小时；日志段 1 GB。

### 集群验证

所有节点部署完成后，在任一节点执行：

```bash
KAFKA=/opt/kafka
BOOT=10.0.0.1:9094,10.0.0.2:9094,10.0.0.3:9094
CFG=$KAFKA/config/client-sasl.properties

# 仲裁状态：应能看到所有节点，且有 LeaderId
$KAFKA/bin/kafka-metadata-quorum.sh --bootstrap-server $BOOT --command-config $CFG describe --status
$KAFKA/bin/kafka-metadata-quorum.sh --bootstrap-server $BOOT --command-config $CFG describe --replication

# 创建 topic 并查看副本分布
$KAFKA/bin/kafka-topics.sh --create --topic test --partitions 3 --replication-factor 3 \
    --bootstrap-server $BOOT --command-config $CFG
$KAFKA/bin/kafka-topics.sh --describe --topic test --bootstrap-server $BOOT --command-config $CFG

# 免认证端口连通性（--plain-access acl 时需先授权）
$KAFKA/bin/kafka-topics.sh --list --bootstrap-server 10.0.0.1:9092,10.0.0.2:9092,10.0.0.3:9092
```

建议再验证一次 ACL：业务账号未授权时访问应被拒绝，授权后可以正常访问。`--no-sasl` 部署时去掉 `--command-config` 并改用 9092。

### 修改配置与重复执行

**重复执行的行为**

| 步骤 | 行为 |
|---|---|
| 解压 | 已有 Kafka 时跳过；版本不是 3.9.0 会警告 |
| 管理员密码 | 必须用 `-P` 传入原密码 |
| Cluster ID | 自动从数据目录读取；传入的 `-c` 必须一致 |
| 参数指纹 | 与上次不同则拒绝执行，除非 `--force` |
| 配置文件 | 按本次参数重新生成并覆盖 |
| 存储格式化 | 已格式化时跳过 |
| 服务 | 每次都会 restart |

只修改本节点参数（如 `-H`、`--heap`）时指纹不变，直接重新执行即可。

<a id="修改集群级参数"></a>**修改集群级参数（如新增账号）**

1. 在每个节点上，用**完整的原参数**加上改动，并带 `--force` 重新执行：
   ```bash
   sudo ./deploy_kafka_cluster.sh -N $NODES -n 1 -P 'AdminP@ss' -c <ClusterID> \
       --extra-user app:AppPwd123 --extra-user report:RptPwd456 --force
   ```
2. **逐台执行**：每台执行完、确认服务正常且没有 under-replicated 分区后，再处理下一台。
3. 全部完成后，比对各节点新指纹是否一致。
4. 为新账号添加 ACL。

重复执行时漏写的 `--extra-user` 账号会被删除，指纹检查正是为了拦住这类误操作。

**修改管理员密码需格外谨慎**：节点之间用管理员身份认证，滚动过程中新旧密码的节点无法互相认证。建议在维护窗口内操作。

---

## 五、客户端连接

### 免认证端口（9092）

```bash
BOOT=<宣告地址>:9092              # 集群版填写所有节点，如 10.0.0.1:9092,10.0.0.2:9092,10.0.0.3:9092
/opt/kafka/bin/kafka-topics.sh --create --topic test --bootstrap-server $BOOT
/opt/kafka/bin/kafka-console-producer.sh --topic test --bootstrap-server $BOOT
/opt/kafka/bin/kafka-console-consumer.sh --topic test --from-beginning --bootstrap-server $BOOT
```

应用程序只需配置 `bootstrap.servers`。

### 认证端口（9094）

命令行工具需要客户端配置文件。运维可直接使用脚本生成的 `client-sasl.properties`（单机版为 `-U` 指定的账号，集群版为管理员）；集群版的业务账号请自行创建，例如 `app.properties`：

```properties
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="app" password="AppPwd123";
```

不同工具指定配置文件的参数名不同：

```bash
BOOT=<宣告地址>:9094
# 管理类工具（kafka-topics / kafka-acls / kafka-metadata-quorum 等）
/opt/kafka/bin/kafka-topics.sh --list --bootstrap-server $BOOT --command-config app.properties
# 生产者
/opt/kafka/bin/kafka-console-producer.sh --topic orders --bootstrap-server $BOOT --producer.config app.properties
# 消费者
/opt/kafka/bin/kafka-console-consumer.sh --topic orders --group order-svc --bootstrap-server $BOOT --consumer.config app.properties
```

应用程序（Java 客户端、Spring Kafka 等）使用上面相同的四项配置，`bootstrap.servers` 指向 9094。集群版建议填写所有节点地址，任一节点故障时客户端仍可连接。

---

## 六、服务管理

两个脚本都注册名为 `kafka` 的 systemd 服务，并设置开机自启：

```bash
systemctl start kafka
systemctl stop kafka
systemctl restart kafka
systemctl status kafka
journalctl -u kafka -f
```

| 项目 | 单机版 | 集群版 |
|---|---|---|
| 运行用户 | root | `kafka`（`--run-user`） |
| 停止方式 | `ExecStop=kafka-server-stop.sh`（会停掉本机所有 Kafka 进程） | systemd 发送 SIGTERM，优雅关闭，最长等待 180 秒 |
| 自动重启 | `Restart=on-failure`，5 秒 | 同左 |
| 文件句柄 | `LimitNOFILE=100000` | 同左 |
| 堆内存 | Kafka 默认 1G | `--heap` 指定，默认 1G |

单机版如需调整堆内存，在 `kafka.service` 的 `[Service]` 段加入 `Environment="KAFKA_HEAP_OPTS=-Xms2G -Xmx2G"`，再执行 `systemctl daemon-reload && systemctl restart kafka`。

**集群版重启请逐台滚动进行**：每重启一台，确认 `kafka-topics.sh --describe` 中所有分区的 Isr 与 Replicas 一致后，再处理下一台。同时停掉超过半数节点会导致仲裁不可用。

无 systemd 的环境需手动启动：

```bash
# 单机版
/opt/kafka/bin/kafka-server-start.sh -daemon /opt/kafka/config/kraft/server.properties
# 集群版
runuser -u kafka -- /opt/kafka/bin/kafka-server-start.sh -daemon /opt/kafka/config/kraft/server.properties
```

---

## 七、安全注意事项

**两个脚本共同的风险**

1. **9092 免认证端口**：单机版始终拥有全部权限；集群版默认（`--plain-access full`）也拥有全部权限。只要 9092 可被访问，认证端口就起不到访问控制作用。务必用防火墙或安全组限制 9092 的访问来源；集群版也可以改用 `--plain-access acl`。
2. **SASL_PLAINTEXT 不加密**：账号密码和消息内容明文传输，只适合可信内网。跨机房、跨 VPC 或公网访问应改用 SASL_SSL（两个脚本都不支持，需手动配置）。
3. **密码存放与显示**：
   - 密码以明文保存在 `server.properties` 和 `client-sasl.properties` 中，权限均为 600。
   - 部署结束时会在终端打印密码，注意终端录屏和日志留存。
   - `-P`、`--extra-user` 传入的密码会留在 shell 历史和进程列表中，执行后可用 `history -d` 清理。

**单机版特有**

4. 没有 ACL，所有连接者都拥有全部权限，认证端口只能验证身份。
5. 以 root 身份运行。
6. 自动生成的随机密码基于 bash `$RANDOM`，并非密码学安全的随机源，生产环境请用 `-P` 传入强密码。
7. 在线下载不校验 sha512。对安装包来源有要求时，先手工下载并校验，再用 `-f` 离线部署。

**集群版特有**

8. 管理员账号是集群内部通信身份，泄露即意味着整个集群被接管。只用于运维，不要交给业务。
9. `server.properties` 中保存了所有追加账号的明文密码，注意服务器本身的访问控制。

---

## 八、故障排查

| 现象 | 适用 | 排查方向 |
|---|---|---|
| 服务启动失败 | 通用 | 查看 `journalctl -u kafka -e` 与 `/opt/kafka/logs/server.log` |
| 端口被占用 | 通用 | `ss -lntp \| grep -E '909[2-5]'` 确认占用进程 |
| 远程客户端能连上但收发超时 | 通用 | 宣告地址客户端不可达（如单机版自动探测到 docker0 的 172.17.0.1 或回退为 localhost），用 `-H` 指定正确地址后重新执行 |
| 认证端口报 `Authentication failed` | 通用 | 核对客户端配置与 `server.properties` 中的账号密码；单机版重复执行可能已更换密码 |
| 启动报 Cluster ID 不匹配 / "数据目录已属于集群 X" | 通用 | 数据目录曾被其他集群格式化，确认数据可丢弃后清空再部署 |
| 下载失败 | 通用 | 检查网络和镜像地址，或改用 `-f` 离线部署 |
| 单机版显示成功但服务随后退出 | 单机版 | 单机版只等待 3 秒，JVM 较晚退出时检测不到，查看日志排查 |
| 报"集群参数指纹变化" | 集群版 | 本次参数与上次不同；对照上次的命令检查，确需修改见[修改集群级参数](#修改集群级参数) |
| 各节点指纹不一致 | 集群版 | 某节点的集群级参数不同，逐项比对部署命令，修正后带 `--force` 重新执行 |
| 60 秒内 controller 端口未就绪 | 集群版 | 进程启动失败，常见原因为端口占用、目录权限、Java 版本 |
| broker 端口一直未就绪 | 集群版 | 仲裁未选主：确认多数节点已启动、节点间 9093 互通、各节点密码与 Cluster ID 一致 |
| `TopicAuthorizationException` / `GroupAuthorizationException` | 集群版 | 账号缺少 ACL；`--plain-access acl` 时 9092 需给 `User:ANONYMOUS` 授权 |
| 消费者报 `COORDINATOR_NOT_AVAILABLE` | 集群版 | `__consumer_offsets` 需要足够节点在线才能创建，等所有节点启动后重试 |
| 生产者报 `NOT_ENOUGH_REPLICAS` | 集群版 | 在线副本数低于 `min.insync.replicas`，检查是否有多台节点宕机 |
| 下载校验文件失败 | 集群版 | 镜像不提供 `.sha512` 时改用官方归档站、离线部署，或加 `--skip-checksum` |

---

## 九、卸载

在每台机器上执行：

```bash
systemctl stop kafka
systemctl disable kafka
rm -f /etc/systemd/system/kafka.service
systemctl daemon-reload

rm -rf /opt/kafka               # 安装目录（以实际 -i 为准）
rm -rf /var/lib/kafka/data      # 数据目录，删除前请确认数据已不再需要
userdel kafka                   # 仅集群版：运行用户（以实际 --run-user 为准）
```

---

## 十、已知限制

**通用**

- Kafka 版本固定为 3.9.0（Scala 2.13），如需其他版本需修改脚本中的 `KAFKA_VERSION` 与 `SCALA_VERSION`。
- 认证只支持 SASL/PLAIN，不支持 SCRAM、SSL/TLS。
- 一台机器只能部署一个实例（服务名固定为 `kafka`）。

**单机版**

- 以 root 身份运行，没有专用用户。
- `ExecStop` 使用 `kafka-server-stop.sh`，会停掉本机所有 Kafka 进程。
- 只支持一个 SASL 账号，不支持 ACL。
- 没有重复执行保护，重复执行可能更换密码或移除认证端口。
- 在线下载不校验 sha512；随机密码强度不足。

**集群版**

- 使用静态仲裁（`controller.quorum.voters`），不支持通过脚本扩缩容节点，变更节点列表需按 Kafka 官方流程手动操作。
- 所有节点均为 broker + controller 合体角色，不支持独立 controller 节点。
- 新增账号或修改密码需要修改所有节点配置并滚动重启。
- 所有节点使用相同端口；节点地址不支持 IPv6。
- 首节点必须是 `--nodes` 列表中的第一个节点。
