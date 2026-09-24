# deploy_kafka_cluster.sh — Kafka 多节点集群部署脚本

自动化部署 **Apache Kafka 3.9.0（kafka_2.13-3.9.0）多节点集群**，采用 **KRaft 模式**（不依赖 ZooKeeper）。

每台节点各执行一次脚本：用 `--nodes` 描述整个集群，用 `--node-id` 指明本机身份，节点各自完成配置，无需 SSH 免密。所有节点同时承担 broker 与 controller 角色，共同组成 controller 仲裁（quorum）。

## 目录

- [功能概览](#功能概览)
- [端口与监听器](#端口与监听器)
- [环境要求](#环境要求)
- [快速开始：三节点集群](#快速开始三节点集群)
- [参数说明](#参数说明)
- [集群一致性保障](#集群一致性保障)
- [认证与授权](#认证与授权)
- [使用示例](#使用示例)
- [部署流程](#部署流程)
- [部署产物](#部署产物)
- [客户端连接](#客户端连接)
- [集群验证](#集群验证)
- [服务管理](#服务管理)
- [修改配置与重复执行](#修改配置与重复执行)
- [安全注意事项](#安全注意事项)
- [故障排查](#故障排查)
- [卸载](#卸载)
- [已知限制](#已知限制)

## 功能概览

- 多节点 KRaft 集群，节点均为 broker + controller 合体角色
- 同时提供 **9092 免认证端口**和 **9094 SASL/PLAIN 认证端口**，认证端口号可自定义
- broker 间复制使用独立的 **9095 内部端口**，不受客户端宣告地址（`-H`）影响
- 默认开启 **ACL 授权**（StandardAuthorizer），支持追加业务账号
- 副本因子、最小同步副本按节点数自动推导
- 首节点自动生成 Cluster ID 与管理员密码；其余节点强制校验一致性
- **集群参数指纹**：各节点可直接比对配置是否一致；重复执行时防止误改集群级参数
- 在线下载校验 sha512；离线安装包存在 `.sha512` 时也自动校验
- 以专用系统用户（默认 `kafka`）运行，注册 systemd 服务并开机自启
- `--render-only` 模式：只渲染配置文件，便于部署前核对

## 端口与监听器

| 端口（默认） | 监听器 | 协议 | 访问方 | 何时开启 | 修改参数 |
|---|---|---|---|---|---|
| 9092 | PLAINTEXT | 明文、免认证 | 客户端 | 始终 | `-p` |
| 9094 | SASL_PLAINTEXT | SASL/PLAIN 认证 | 客户端 | 默认开启，`--no-sasl` 时关闭 | `-s` / `--sasl-port` |
| 9095 | INTERNAL | SASL_PLAINTEXT（`--no-sasl` 时为明文） | 仅集群节点之间 | 始终 | `--internal-port` |
| 9093 | CONTROLLER | SASL_PLAINTEXT（`--no-sasl` 时为明文） | 仅集群节点之间 | 始终 | `--controller-port` |

- 四个端口必须互不相同，且**所有节点使用相同端口**。
- 9092、9094 的宣告地址默认为 `--nodes` 中本机的 host，可用 `-H` 改为客户端可达的地址（如公网 IP 或 NAT 地址）。
- 9095、9093 的地址始终取 `--nodes` 中的 host，因此 `--nodes` 必须填写**节点之间互通的地址**。

以节点 2、`-s 19094 -H 1.2.3.4` 为例，生成的监听配置为：

```properties
listeners=PLAINTEXT://:9092,SASL_PLAINTEXT://:19094,INTERNAL://:9095,CONTROLLER://:9093
advertised.listeners=PLAINTEXT://1.2.3.4:9092,SASL_PLAINTEXT://1.2.3.4:19094,INTERNAL://10.0.0.2:9095
inter.broker.listener.name=INTERNAL
controller.listener.names=CONTROLLER
controller.quorum.voters=1@10.0.0.1:9093,2@10.0.0.2:9093,3@10.0.0.3:9093
```

### 防火墙 / 安全组放通建议

| 端口 | 来源 |
|---|---|
| 9092 | 可信的业务主机（免认证，权限见[认证与授权](#认证与授权)） |
| 9094 | 需要访问 Kafka 的客户端 |
| 9095、9093 | 仅集群内其他节点 |

## 环境要求

| 项目 | 要求 |
|---|---|
| 节点数 | 至少 3 台（推荐奇数：3 或 5）；2 台需显式 `--allow-two-nodes` |
| 操作系统 | Linux，推荐使用 systemd 的发行版 |
| 权限 | root，或具备 sudo 权限的用户 |
| Java | 已安装且 `java` 在 PATH 中，推荐 JDK 11 或 17 |
| 下载工具 | 在线模式需 `curl` 或 `wget`；离线模式不需要 |
| 其他命令 | `tar`、`sha512sum`、`sha256sum`、`timeout`、`useradd`（均为常见系统自带） |
| 网络 | 节点之间 9093、9095 互通；各节点主机名或 IP 可互相解析/访问 |

## 快速开始：三节点集群

假设三台机器为 10.0.0.1、10.0.0.2、10.0.0.3。**`--nodes` 列表中的第一个节点为首节点**，必须最先部署。

```bash
NODES=1@10.0.0.1,2@10.0.0.2,3@10.0.0.3

# 节点 1（首节点）：可不传 -P，脚本会随机生成密码并在结束时打印
sudo ./deploy_kafka_cluster.sh -N $NODES -n 1 -P 'AdminP@ss'
# 记下输出中的 Cluster ID 和参数指纹

# 节点 2、3：必须传入与节点 1 相同的密码和 Cluster ID，其余参数也要一致
sudo ./deploy_kafka_cluster.sh -N $NODES -n 2 -P 'AdminP@ss' -c <ClusterID>
sudo ./deploy_kafka_cluster.sh -N $NODES -n 3 -P 'AdminP@ss' -c <ClusterID>
```

完成后：

1. 核对三个节点输出的**参数指纹**完全一致。
2. 在任一节点执行[集群验证](#集群验证)中的命令。

节点 1 部署完成时，由于其他节点还没启动、仲裁无法选主，会提示"broker 端口尚未就绪"，这属于正常现象。多数节点（3 节点集群中为 2 台）启动后，集群即可选主并对外服务。

## 参数说明

### 必填

| 参数 | 说明 |
|---|---|
| `-N`, `--nodes LIST` | 集群所有节点，格式 `id@host`，逗号分隔。第一个为首节点。host 不支持 IPv6，也不能带端口 |
| `-n`, `--node-id ID` | 本机节点 id，必须出现在 `--nodes` 中。`01` 与 `1` 视为同一 id |

### 集群一致性（所有节点必须相同）

| 参数 | 说明 |
|---|---|
| `-c`, `--cluster-id UUID` | 集群 ID（22 位 base64url 字符串）。首节点首次部署可不填，自动生成；其余节点必填 |
| `-P`, `--sasl-password PWD` | 管理员密码。首节点首次部署可不填，自动生成；**其余节点及任何节点的重复执行必填** |

### 端口

| 参数 | 说明 | 默认值 |
|---|---|---|
| `-p`, `--port PORT` | 免认证客户端端口，始终开启 | `9092` |
| `-s`, `--sasl-port PORT` | 认证客户端端口，`--no-sasl` 时关闭 | `9094` |
| `--controller-port PORT` | controller 仲裁端口 | `9093` |
| `--internal-port PORT` | broker 间复制端口 | `9095` |

### 目录与网络

| 参数 | 说明 | 默认值 |
|---|---|---|
| `-i`, `--install-dir DIR` | 安装目录（绝对路径，不能是 `/opt` 等系统目录本身） | `/opt/kafka` |
| `-d`, `--data-dir DIR` | 数据目录（同上） | `/var/lib/kafka/data` |
| `-H`, `--advertised-host HOST` | 本机对客户端宣告的地址，仅影响 9092/9094 | `--nodes` 中本机 host |

### 运行

| 参数 | 说明 | 默认值 |
|---|---|---|
| `--run-user USER` | 运行 Kafka 的系统用户，不存在时自动创建；传 `root` 则以 root 运行 | `kafka` |
| `--heap SIZE` | JVM 堆大小，如 `2G`、`1536M` | Kafka 默认 1G |

### 安装包

| 参数 | 说明 | 默认值 |
|---|---|---|
| `-f`, `--package FILE` | 本地安装包路径，提供后进入离线模式 | 空（在线下载） |
| `-m`, `--mirror URL` | 下载地址前缀 | `https://archive.apache.org/dist/kafka/3.9.0` |
| `--skip-checksum` | 在线下载时跳过 sha512 校验（不推荐） | 关闭 |

### 副本与分区

| 参数 | 说明 | 默认值 |
|---|---|---|
| `-r`, `--replication-factor N` | 默认副本因子，也用于内部 topic | min(3, 节点数) |
| `--min-isr N` | 最小同步副本数 | 副本因子 ≥ 3 时为 2，否则为 1 |
| `--partitions N` | 自动创建 topic 的默认分区数 | `3` |

### 认证与授权

| 参数 | 说明 | 默认值 |
|---|---|---|
| `--no-sasl` | 关闭 SASL：不开认证端口，节点间明文通信，同时关闭授权 | SASL 开启 |
| `-U`, `--sasl-user USER` | 管理员账号，同时是集群内部通信身份 | `admin` |
| `--extra-user U:P` | 追加业务账号，可重复 | 无 |
| `--super-users LIST` | 追加超级用户，分号或逗号分隔，如 `User:ops;User:sre`；管理员始终自动包含 | 无 |
| `--plain-access MODE` | 9092 的授权策略：`full` 或 `acl`，见[认证与授权](#认证与授权) | `full` |
| `--no-authz` | 关闭 ACL 授权，仅保留认证 | 授权开启 |
| `--allow-everyone` | 没有 ACL 的资源默认放行 | 拒绝 |

### 其他

| 参数 | 说明 |
|---|---|
| `--allow-two-nodes` | 允许部署 2 节点集群 |
| `--force` | 重复执行时允许集群参数指纹变化（需所有节点同步修改） |
| `--render-only DIR` | 只把配置渲染到 DIR 后退出，不下载、不格式化、不注册服务 |
| `-h`, `--help` | 显示帮助信息 |

### 参数约束

- 账号名只允许字母、数字及 `.`、`_`、`-`，不能使用保留名 `ANONYMOUS`；追加账号不能与管理员重名，也不能重复。
- 密码不能包含双引号 `"`、反斜杠 `\` 或空白字符（会破坏 JAAS 语法）。
- 同一主机不能出现在 `--nodes` 中两次（所有节点端口相同，同一主机无法部署多个节点）。
- 选项值不能以 `--` 开头，否则视为缺少取值。

## 集群一致性保障

多节点集群要求 Cluster ID、管理员密码、账号、端口、副本配置等在所有节点上完全一致，任何一项不一致都可能导致节点无法加入集群。脚本提供了以下几层保护。

### 首节点与非首节点

| 场景 | `-c`（Cluster ID） | `-P`（管理员密码） |
|---|---|---|
| 首节点，首次部署 | 可不填，自动生成 | 可不填，自动生成 |
| 非首节点，首次部署 | **必填**，否则报错 | **必填**，否则报错 |
| 任何节点，重复执行 | 可不填，自动读取数据目录中的值；填了则必须与之一致 | **必填**，否则报错 |

### 集群参数指纹

每次部署结束都会打印一个 16 位的**参数指纹**，并写入 `server.properties` 头部的注释 `# cluster-fingerprint:`。

- **计算来源**：Kafka 版本、节点列表、Cluster ID、四个端口、SASL/授权开关、管理员账号和密码、追加账号、super.users、`--plain-access`、`--allow-everyone`、副本因子、最小同步副本、默认分区数。
- **不包含**：只影响本节点的参数，如 `-n`、`-H`、目录、运行用户、堆内存。这些参数各节点可以不同。
- **用法**：所有节点部署完成后，比对各自输出的指纹，应完全一致。
- **重复执行保护**：如果本次计算的指纹与配置文件中记录的不同，脚本会拒绝执行，防止误操作（例如漏写 `--extra-user`、换了密码）导致本节点与集群不一致。确需修改时加 `--force`，详见[修改配置与重复执行](#修改配置与重复执行)。

指纹是参数的单向哈希截断，不能反推出密码。

### 部署前核对配置

可以先用 `--render-only` 在任意机器上渲染出各节点的配置文件核对，不需要 root 和 Java：

```bash
./deploy_kafka_cluster.sh -N $NODES -n 2 -P 'AdminP@ss' -c <ClusterID> --render-only ./render-node2
cat ./render-node2/server.properties
```

## 认证与授权

### 身份模型

| 连接方式 | Kafka 中的身份 |
|---|---|
| 连接 9092（免认证） | `User:ANONYMOUS` |
| 以管理员账号连接 9094 | `User:admin`（或 `-U` 指定的账号） |
| 以追加账号连接 9094 | `User:<账号>` |
| 节点之间（9095、9093） | 管理员身份，由脚本自动配置 |

**管理员账号是集群内部通信身份**，始终在 `super.users` 中，脚本不允许将其移除。不建议把管理员账号交给业务使用，业务请用 `--extra-user` 分配独立账号。

### 9092 免认证端口的权限（`--plain-access`）

开启 ACL 授权后，匿名用户默认没有任何权限。`--plain-access` 决定 9092 的行为：

| 模式 | 效果 | 适用场景 |
|---|---|---|
| `full`（默认） | `User:ANONYMOUS` 加入超级用户，9092 拥有集群全部权限 | 9092 只对可信内网开放，要求免认证即可直接使用 |
| `acl` | 匿名用户与普通账号一样，需要通过 ACL 单独授权 | 希望 9092 只有部分权限，例如只读某些 topic |

`--no-authz` 或 `--no-sasl` 时不做权限管控，9092 始终拥有全部权限。

### 追加账号与 ACL

追加账号默认没有任何权限（除非加 `--allow-everyone`），需要用管理员身份授权。在任一节点执行一次即可，全集群生效：

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

# 查看 / 删除 ACL
$KAFKA/bin/kafka-acls.sh --bootstrap-server $BOOT --command-config $CFG --list
$KAFKA/bin/kafka-acls.sh --bootstrap-server $BOOT --command-config $CFG \
    --remove --allow-principal User:app --producer --topic 'orders'
```

`--plain-access acl` 模式下，给 9092 授权时把 principal 写成 `User:ANONYMOUS` 即可。

ACL 存储在集群元数据中，新增 ACL 不需要重启。**新增账号则需要修改所有节点的配置并重启**，见[修改配置与重复执行](#修改配置与重复执行)。

## 使用示例

```bash
NODES=1@10.0.0.1,2@10.0.0.2,3@10.0.0.3

# 1. 自定义认证端口为 19094，追加两个业务账号
sudo ./deploy_kafka_cluster.sh -N $NODES -n 1 -P 'AdminP@ss' -s 19094 \
    --extra-user app:AppPwd123 --extra-user report:RptPwd456

# 2. 客户端通过公网/NAT 地址访问（节点间复制仍走内网）
sudo ./deploy_kafka_cluster.sh -N $NODES -n 2 -P 'AdminP@ss' -c <ClusterID> -H 47.100.1.2

# 3. 9092 不给全部权限，匿名用户按 ACL 授权
sudo ./deploy_kafka_cluster.sh -N $NODES -n 1 -P 'AdminP@ss' --plain-access acl

# 4. 追加运维超级用户
sudo ./deploy_kafka_cluster.sh -N $NODES -n 1 -P 'AdminP@ss' \
    --extra-user ops:OpsPwd789 --super-users 'User:ops'

# 5. 离线部署（安装包与 .sha512 放在同一目录）
sudo ./deploy_kafka_cluster.sh -N $NODES -n 1 -P 'AdminP@ss' -f /root/kafka_2.13-3.9.0.tgz

# 6. 指定堆内存和安装/数据目录
sudo ./deploy_kafka_cluster.sh -N $NODES -n 1 -P 'AdminP@ss' --heap 4G -i /data/kafka -d /data/kafka-data

# 7. 纯明文集群（仅限完全可信的内网）
sudo ./deploy_kafka_cluster.sh -N $NODES -n 1 --no-sasl
```

同一集群的其余节点，必须使用与示例中首节点**完全相同的集群级参数**（`-P`、`-s`、`--extra-user`、`--super-users`、`--plain-access` 等），并额外带上 `-c`。

离线部署所需的校验文件可以从 `https://archive.apache.org/dist/kafka/3.9.0/kafka_2.13-3.9.0.tgz.sha512` 获取。

## 部署流程

脚本按以下顺序执行，任一步骤失败都会立即退出（`set -euo pipefail`）：

1. **参数校验**：端口格式与冲突、目录安全、账号密码字符、super.users 格式等。
2. **解析拓扑**：解析 `--nodes`，检查 id 和 host 是否重复，确定本机是否为首节点，生成仲裁成员列表；2 节点集群需 `--allow-two-nodes`。
3. **推导副本配置**：副本因子、最小同步副本。
4. **前置检查**：root/sudo 权限、Java、下载工具或离线包。
5. **一致性检查**：读取数据目录中已有的 Cluster ID，按[首节点与非首节点](#首节点与非首节点)的规则检查 `-c`、`-P`；首节点首次部署时生成管理员密码。
6. **获取安装包**：已安装则跳过（并检查版本）；否则下载或使用离线包，校验 sha512，校验归档结构，解压到安装目录。
7. **准备环境**：创建数据目录和运行用户；首节点首次部署时生成 Cluster ID。
8. **指纹检查**：计算参数指纹，与已有配置比对，不一致时拒绝执行（除非 `--force`）。
9. **生成配置**：备份原始配置为 `server.properties.orig`（仅首次），写入新配置与客户端配置，权限均为 600。
10. **存储格式化**：数据目录未格式化时，用 Cluster ID 执行 `kafka-storage.sh format`。
11. **修改属主**：将安装目录与数据目录属主改为运行用户。
12. **注册并启动服务**：写入 systemd unit，执行 enable 与 restart；最多等待 60 秒 controller 端口就绪，再检查 broker 端口是否就绪。
13. **输出汇总**：端口、Cluster ID、参数指纹、账号信息、ACL 示例与验证命令。

## 部署产物

| 路径（默认） | 说明 |
|---|---|
| `/opt/kafka/` | 安装目录，属主为运行用户 |
| `/opt/kafka/config/kraft/server.properties` | 节点配置，含参数指纹和明文密码，权限 600 |
| `/opt/kafka/config/kraft/server.properties.orig` | 原始配置备份（仅首次执行时生成） |
| `/opt/kafka/config/client-sasl.properties` | 管理员身份的客户端配置，供运维工具使用，权限 600 |
| `/opt/kafka/logs/` | Kafka 运行日志（server.log 等） |
| `/var/lib/kafka/data/` | 消息数据与 KRaft 元数据，`meta.properties` 中记录 Cluster ID |
| `/etc/systemd/system/kafka.service` | systemd 服务单元 |

主要 broker 参数：

| 参数 | 值 |
|---|---|
| `default.replication.factor`、`offsets.topic.replication.factor`、`transaction.state.log.replication.factor` | 副本因子 |
| `min.insync.replicas`、`transaction.state.log.min.isr` | 最小同步副本 |
| `num.partitions` | 默认分区数 |
| `log.retention.hours` | 168（保留 7 天） |
| `log.segment.bytes` | 1073741824（1 GB） |

## 客户端连接

### 免认证端口（9092）

```bash
BOOT=10.0.0.1:9092,10.0.0.2:9092,10.0.0.3:9092
/opt/kafka/bin/kafka-console-producer.sh --topic test --bootstrap-server $BOOT
/opt/kafka/bin/kafka-console-consumer.sh --topic test --from-beginning --bootstrap-server $BOOT
```

应用程序只需配置 `bootstrap.servers`，无需任何认证参数。

### 认证端口（9094）

命令行工具需要一个客户端配置文件。运维可以直接使用管理员身份的 `client-sasl.properties`；业务账号请自行创建，例如 `app.properties`：

```properties
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="app" password="AppPwd123";
```

```bash
BOOT=10.0.0.1:9094,10.0.0.2:9094,10.0.0.3:9094
/opt/kafka/bin/kafka-console-producer.sh --topic orders --bootstrap-server $BOOT --producer.config app.properties
/opt/kafka/bin/kafka-console-consumer.sh --topic orders --group order-svc --bootstrap-server $BOOT --consumer.config app.properties
```

应用程序（Java 客户端、Spring Kafka 等）使用相同的四项配置，`bootstrap.servers` 指向各节点的 9094。

建议 `bootstrap.servers` 填写所有节点地址，任一节点故障时客户端仍可连接。

## 集群验证

所有节点部署完成后，在任一节点执行：

```bash
KAFKA=/opt/kafka
BOOT=10.0.0.1:9094,10.0.0.2:9094,10.0.0.3:9094
CFG=$KAFKA/config/client-sasl.properties

# 1. 仲裁状态：应能看到所有节点，且有 LeaderId
$KAFKA/bin/kafka-metadata-quorum.sh --bootstrap-server $BOOT --command-config $CFG describe --status
$KAFKA/bin/kafka-metadata-quorum.sh --bootstrap-server $BOOT --command-config $CFG describe --replication

# 2. 创建 topic 并查看副本分布
$KAFKA/bin/kafka-topics.sh --create --topic test --partitions 3 --replication-factor 3 \
    --bootstrap-server $BOOT --command-config $CFG
$KAFKA/bin/kafka-topics.sh --describe --topic test --bootstrap-server $BOOT --command-config $CFG

# 3. 免认证端口连通性（--plain-access acl 时需先授权）
$KAFKA/bin/kafka-topics.sh --list --bootstrap-server 10.0.0.1:9092,10.0.0.2:9092,10.0.0.3:9092
```

建议再验证一次 ACL：业务账号在未授权时访问应被拒绝（`TopicAuthorizationException`），授权后可以正常访问。

`--no-sasl` 部署时，以上命令去掉 `--command-config` 参数，并把端口改为 9092。

## 服务管理

```bash
systemctl start kafka      # 启动
systemctl stop kafka       # 停止（优雅关闭，最长等待 180 秒）
systemctl restart kafka    # 重启
systemctl status kafka     # 查看状态
journalctl -u kafka -f     # 实时查看日志
```

服务配置要点：以运行用户（默认 `kafka`）身份运行；`Restart=on-failure`，5 秒后重启；`LimitNOFILE=100000`；停止时由 systemd 发送 SIGTERM 触发优雅关闭，退出码 143 视为正常。

**重启集群时请逐台滚动进行**：重启一台后，确认 `kafka-topics.sh --describe` 中没有 under-replicated 分区（所有分区的 Isr 与 Replicas 一致），再重启下一台。同时停掉超过半数节点会导致仲裁不可用。

无 systemd 的环境需手动启动：

```bash
runuser -u kafka -- /opt/kafka/bin/kafka-server-start.sh -daemon /opt/kafka/config/kraft/server.properties
```

## 修改配置与重复执行

### 重复执行的行为

| 步骤 | 行为 |
|---|---|
| 解压 | 安装目录已有 Kafka 时跳过；若版本不是 3.9.0 会给出警告 |
| 管理员密码 | 必须用 `-P` 传入原密码 |
| Cluster ID | 自动从数据目录读取；传入的 `-c` 必须与之一致 |
| 参数指纹 | 与上次不同则拒绝执行，除非加 `--force` |
| 配置文件 | 按本次参数重新生成并覆盖 |
| 存储格式化 | 已格式化时跳过，数据不受影响 |
| 服务 | **每次都会 restart** |

只修改本节点参数（如 `-H`、`--heap`）时，指纹不变，直接重新执行即可。

### 修改集群级参数（如新增账号）

以新增业务账号 `report` 为例：

1. 在**每个节点**上，用**完整的原参数**加上新账号，并带 `--force` 重新执行：
   ```bash
   sudo ./deploy_kafka_cluster.sh -N $NODES -n 1 -P 'AdminP@ss' -c <ClusterID> \
       --extra-user app:AppPwd123 --extra-user report:RptPwd456 --force
   ```
2. **逐台执行**，每台执行完、确认服务正常且无 under-replicated 分区后，再处理下一台。
3. 全部完成后，比对各节点的新指纹是否一致。
4. 为新账号添加 ACL。

注意：重复执行时漏写的 `--extra-user` 账号会被删除，指纹检查正是为了防止这种误操作。

**修改管理员密码需格外谨慎**：节点之间用管理员身份认证，滚动过程中新旧密码节点无法互相认证。建议在维护窗口内操作，或先评估改用 SCRAM 等支持动态更新凭据的机制。

## 安全注意事项

1. **9092 免认证端口**：默认（`--plain-access full`）拥有集群全部权限。只要 9092 可被访问，认证端口就起不到访问控制作用。务必通过防火墙或安全组将 9092 限制在可信主机，或改用 `--plain-access acl`。
2. **SASL_PLAINTEXT 不加密**：客户端和节点之间的账号密码、消息内容均为明文传输，只适合可信内网。跨机房、跨 VPC 或公网访问应改用 SASL_SSL（本脚本不支持，需手动配置）。
3. **管理员账号**：是集群内部通信身份，泄露即意味着整个集群被接管。只用于运维，不要交给业务。
4. **密码存放与显示**：
   - 所有账号的密码以明文保存在 `server.properties` 中，管理员密码还保存在 `client-sasl.properties` 中，两者权限均为 600。
   - 部署结束时会在终端打印管理员密码，注意终端录屏和日志留存。
   - `-P`、`--extra-user` 传入的密码会留在 shell 历史和进程列表中，执行后可用 `history -d` 清理。
5. **随机密码**：由 `/dev/urandom` 生成，24 位，满足生产要求。
6. **安装包校验**：在线下载强制校验 sha512；离线部署请将官方 `.sha512` 文件与安装包放在同一目录。

## 故障排查

| 现象 | 排查方向 |
|---|---|
| 报"集群参数指纹变化" | 本次参数与上次部署不同。对照上次的命令检查 `-P`、`--extra-user`、端口等；确需修改见[修改集群级参数](#修改集群级参数如新增账号) |
| 各节点指纹不一致 | 某个节点的集群级参数不同，逐项比对各节点的部署命令，修正后带 `--force` 重新执行 |
| 报"数据目录已属于集群 X" | 数据目录曾被其他集群格式化。确认数据可丢弃后清空数据目录再部署 |
| 60 秒内 controller 端口未就绪 | 进程启动失败，查看 `journalctl -u kafka -e` 与 `/opt/kafka/logs/server.log`；常见原因为端口被占用、目录权限、Java 版本 |
| broker 端口一直未就绪 | 仲裁未能选主：确认多数节点已启动；检查节点之间 9093 是否互通；检查各节点密码与 Cluster ID 是否一致 |
| 日志中出现 `Authentication failed` | 节点之间或客户端的账号密码不一致；节点间认证失败时对照指纹排查 |
| 客户端报 `TopicAuthorizationException` / `GroupAuthorizationException` | 账号缺少 ACL，按[追加账号与 ACL](#追加账号与-acl) 授权；9092 在 `--plain-access acl` 模式下需给 `User:ANONYMOUS` 授权 |
| 消费者报 `COORDINATOR_NOT_AVAILABLE` | 内部 topic `__consumer_offsets` 需要足够的节点在线才能创建（副本因子默认 3），等所有节点启动后重试 |
| 生产者报 `NOT_ENOUGH_REPLICAS` | 在线副本数低于 `min.insync.replicas`（3 节点时为 2），检查是否有多台节点宕机 |
| 远程客户端能连上但收发超时 | 宣告地址客户端不可达，用 `-H` 指定客户端可访问的地址后重新执行 |
| 下载校验文件失败 | 镜像不提供 `.sha512` 时，改用官方归档站、离线部署，或加 `--skip-checksum`（不推荐） |

## 卸载

在每个节点执行：

```bash
systemctl stop kafka
systemctl disable kafka
rm -f /etc/systemd/system/kafka.service
systemctl daemon-reload

rm -rf /opt/kafka               # 安装目录（以实际 -i 为准）
rm -rf /var/lib/kafka/data      # 数据目录，删除前请确认数据已不再需要
userdel kafka                   # 运行用户（以实际 --run-user 为准）
```

## 已知限制

- Kafka 版本固定为 3.9.0（Scala 2.13），如需其他版本需修改脚本中的 `KAFKA_VERSION` 与 `SCALA_VERSION`。
- 使用静态仲裁（`controller.quorum.voters`），**不支持通过本脚本扩缩容节点**；变更节点列表需要按 Kafka 官方流程手动操作。
- 所有节点均为 broker + controller 合体角色，不支持独立 controller 节点。
- 认证只支持 SASL/PLAIN，不支持 SCRAM、SSL/TLS；新增账号或修改密码需要修改所有节点配置并滚动重启。
- 所有节点使用相同端口，同一主机只能部署一个节点。
- 节点地址不支持 IPv6。
- 首节点必须是 `--nodes` 列表中的第一个节点。
