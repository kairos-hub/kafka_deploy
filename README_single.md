# deploy_kafka.sh — Kafka 单机版一键部署脚本

自动化部署 **Apache Kafka 3.9.0（kafka_2.13-3.9.0）单机版**，采用 **KRaft 模式**（不依赖 ZooKeeper）。支持在线下载与离线安装两种方式，默认提供免认证端口，可选额外开启 SASL/PLAIN 认证端口，并自动注册为 systemd 服务。

## 目录

- [功能概览](#功能概览)
- [环境要求](#环境要求)
- [快速开始](#快速开始)
- [参数说明](#参数说明)
- [端口规划](#端口规划)
- [使用示例](#使用示例)
- [部署流程](#部署流程)
- [部署产物](#部署产物)
- [客户端连接](#客户端连接)
- [服务管理](#服务管理)
- [重复执行行为](#重复执行行为)
- [安全注意事项](#安全注意事项)
- [故障排查](#故障排查)
- [卸载](#卸载)
- [已知限制](#已知限制)

## 功能概览

- 单节点同时承担 broker 与 controller 角色（`process.roles=broker,controller`）
- 在线下载（支持自定义镜像）或离线安装本地 `.tgz` 包
- 安装包完整性与顶层目录名校验，解压时自动剥离顶层目录
- 默认开启 9092 免认证端口；`--sasl` 时额外开启 9094 认证端口（端口可自定义）
- 自动生成客户端 SASL 配置文件，供 `kafka-*.sh` 命令行工具使用
- 自动执行 KRaft 存储格式化，已格式化时跳过，避免 Cluster ID 变化
- 自动注册并启动 systemd 服务，开机自启
- 启动参数校验：端口合法性与冲突检查、SASL 用户名与密码字符检查

## 环境要求

| 项目 | 要求 |
|---|---|
| 操作系统 | Linux（推荐使用 systemd 的发行版，如 CentOS 7+/RHEL/Ubuntu 18.04+） |
| 权限 | root，或具备 sudo 权限的用户 |
| Java | 已安装且 `java` 在 PATH 中；脚本仅检查存在性，推荐 JDK 11 或 17 |
| 下载工具 | 在线模式需 `curl` 或 `wget`；离线模式不需要 |
| 其他命令 | `tar`、`hostname`（用于自动探测 IP） |

## 快速开始

```bash
chmod +x deploy_kafka.sh

# 最简部署：默认安装到 /opt/kafka，仅开启 9092 免认证端口
sudo ./deploy_kafka.sh

# 同时开启 9094 认证端口（密码随机生成并在结束时打印）
sudo ./deploy_kafka.sh --sasl
```

部署完成后脚本会打印端口、认证信息和验证命令。

## 参数说明

| 短参数 | 长参数 | 说明 | 默认值 |
|---|---|---|---|
| `-i` | `--install-dir DIR` | Kafka 安装目录 | `/opt/kafka` |
| `-d` | `--data-dir DIR` | Kafka 数据目录（`log.dirs`） | `/var/lib/kafka/data` |
| `-p` | `--port PORT` | 免认证（PLAINTEXT）端口，始终开启 | `9092` |
| `-H` | `--advertised-host HOST` | 对外宣告地址，客户端实际连接用 | 自动探测内网 IP |
| `-f` | `--package FILE` | 本地安装包路径，提供后进入离线模式 | 空（在线下载） |
| `-m` | `--mirror URL` | 下载地址前缀 | `https://archive.apache.org/dist/kafka/3.9.0` |
| `-S` | `--sasl` | 额外开启 SASL/PLAIN 认证端口 | 关闭 |
| `-s` | `--sasl-port PORT` | 认证（SASL_PLAINTEXT）端口，仅 `--sasl` 时生效 | `9094` |
| `-U` | `--sasl-user USER` | SASL 用户名，仅 `--sasl` 时生效 | `admin` |
| `-P` | `--sasl-password PWD` | SASL 密码，仅 `--sasl` 时生效 | 随机生成 20 位 |
| `-h` | `--help` | 显示帮助信息 | — |

参数约束：

- 端口必须是 1–65535 的整数，免认证端口、认证端口和 controller 端口（9093）三者不能相同。
- SASL 用户名只允许字母、数字及 `.`、`_`、`-`。
- SASL 密码不能包含双引号 `"`、反斜杠 `\` 或空白字符（会破坏 JAAS 配置语法）。
- 只指定 `-s` 而未加 `--sasl` 时，脚本会给出警告，认证端口不会开启。

## 端口规划

| 端口 | 监听器 | 协议 | 何时开启 | 绑定地址 | 用途 |
|---|---|---|---|---|---|
| 9092（`-p`） | PLAINTEXT | 明文、免认证 | 始终 | `0.0.0.0` | 客户端连接；broker 内部通信 |
| 9094（`-s`） | SASL_PLAINTEXT | SASL/PLAIN 认证 | `--sasl` 时 | `0.0.0.0` | 需认证的客户端连接 |
| 9093 | CONTROLLER | 明文 | 始终 | `127.0.0.1` | KRaft 控制器，仅本机使用 |

以 `--sasl -s 19094 -H 10.0.0.5` 为例，生成的监听配置为：

```properties
listeners=PLAINTEXT://:9092,SASL_PLAINTEXT://:19094,CONTROLLER://127.0.0.1:9093
inter.broker.listener.name=PLAINTEXT
advertised.listeners=PLAINTEXT://10.0.0.5:9092,SASL_PLAINTEXT://10.0.0.5:19094
controller.quorum.voters=1@127.0.0.1:9093
```

防火墙或安全组需要按需放通 9092 和（启用时）9094，9093 无需放通。

## 使用示例

```bash
# 1. 在线部署，自定义安装与数据目录
sudo ./deploy_kafka.sh -i /usr/local/kafka -d /data/kafka

# 2. 使用国内镜像加速下载
sudo ./deploy_kafka.sh -m https://mirrors.aliyun.com/apache/kafka/3.9.0

# 3. 离线部署（提前上传 kafka_2.13-3.9.0.tgz）
sudo ./deploy_kafka.sh -f ./kafka_2.13-3.9.0.tgz -i /usr/local/kafka -d /usr/local/kafka/data

# 4. 多网卡机器，显式指定客户端连接地址（推荐）
sudo ./deploy_kafka.sh -H 192.168.10.21

# 5. 开启认证端口，指定用户名和密码
sudo ./deploy_kafka.sh --sasl -U admin -P 'MyS3cret!'

# 6. 开启认证端口并自定义端口号
sudo ./deploy_kafka.sh --sasl -s 19094 -U app
```

> 国内镜像通常只保留较新版本，若 3.9.0 已下架导致下载失败，请改用默认官方归档站或离线模式。

## 部署流程

脚本按以下顺序执行，任一步骤失败都会立即退出（`set -euo pipefail`）：

1. **参数解析与校验**：解析命令行参数，未指定 `-H` 时通过 `hostname -I` 取第一个 IP（失败则回退 `localhost`），校验端口和 SASL 参数，按需生成随机密码。
2. **前置检查**：检查 root/sudo 权限、Java、下载工具或离线包是否存在。
3. **获取与解压**：若安装目录中已有 `bin/kafka-server-start.sh` 则跳过；否则下载或使用离线包，校验归档完整性和顶层目录名（必须为 `kafka_2.13-3.9.0`），然后解压到安装目录。
4. **创建数据目录**。
5. **生成配置**：首次执行时将原始配置备份为 `server.properties.orig`，然后覆盖写入新的 `config/kraft/server.properties`；启用 SASL 时同时生成客户端配置文件，两者权限均为 600。
6. **存储格式化**：若数据目录下已有 `meta.properties` 则跳过，否则生成随机 Cluster ID 并执行 `kafka-storage.sh format`。
7. **注册 systemd 服务**：写入 `/etc/systemd/system/kafka.service`，执行 `enable` 和 `restart`，3 秒后检查服务是否处于 active 状态。无 systemd 时跳过并提示手动启动命令。
8. **输出汇总信息**：端口、认证信息及验证命令。

## 部署产物

| 路径（默认） | 说明 |
|---|---|
| `/opt/kafka/` | Kafka 安装目录 |
| `/opt/kafka/config/kraft/server.properties` | 脚本生成的 broker 配置 |
| `/opt/kafka/config/kraft/server.properties.orig` | 原始配置备份（仅首次执行时生成） |
| `/opt/kafka/config/client-sasl.properties` | 客户端 SASL 配置（仅 `--sasl`），权限 600 |
| `/opt/kafka/logs/` | Kafka 运行日志（server.log 等） |
| `/var/lib/kafka/data/` | 消息数据与 KRaft 元数据 |
| `/etc/systemd/system/kafka.service` | systemd 服务单元 |

主要 broker 参数（适用于单机环境）：

| 参数 | 值 | 说明 |
|---|---|---|
| `num.partitions` | 1 | 自动创建 topic 的默认分区数 |
| `offsets.topic.replication.factor` 等 | 1 | 单机只能为 1 |
| `log.retention.hours` | 168 | 消息保留 7 天 |
| `log.segment.bytes` | 1073741824 | 日志段大小 1 GB |
| `num.network.threads` / `num.io.threads` | 3 / 8 | 网络与 IO 线程数 |

JVM 堆内存使用 Kafka 默认值 `-Xms1G -Xmx1G`，如需调整见[故障排查](#故障排查)。

## 客户端连接

### 免认证端口（9092）

```bash
/opt/kafka/bin/kafka-topics.sh --create --topic test \
    --bootstrap-server <宣告地址>:9092 --partitions 1 --replication-factor 1

/opt/kafka/bin/kafka-console-producer.sh --topic test --bootstrap-server <宣告地址>:9092
/opt/kafka/bin/kafka-console-consumer.sh --topic test --from-beginning --bootstrap-server <宣告地址>:9092
```

### 认证端口（9094）

命令行工具需要带上客户端配置文件：

```bash
/opt/kafka/bin/kafka-topics.sh --list \
    --bootstrap-server <宣告地址>:9094 \
    --command-config /opt/kafka/config/client-sasl.properties

# 生产者 / 消费者使用对应的参数名
/opt/kafka/bin/kafka-console-producer.sh --topic test --bootstrap-server <宣告地址>:9094 \
    --producer.config /opt/kafka/config/client-sasl.properties
/opt/kafka/bin/kafka-console-consumer.sh --topic test --from-beginning --bootstrap-server <宣告地址>:9094 \
    --consumer.config /opt/kafka/config/client-sasl.properties
```

应用程序（Java 客户端、Spring Kafka 等）配置示例：

```properties
bootstrap.servers=<宣告地址>:9094
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="<密码>";
```

## 服务管理

```bash
systemctl start kafka      # 启动
systemctl stop kafka       # 停止
systemctl restart kafka    # 重启
systemctl status kafka     # 查看状态
journalctl -u kafka -f     # 实时查看日志
```

服务配置了 `Restart=on-failure`（5 秒后重启）、`LimitNOFILE=100000`，并已设置开机自启。

无 systemd 的环境需手动启动：

```bash
/opt/kafka/bin/kafka-server-start.sh -daemon /opt/kafka/config/kraft/server.properties
```

## 重复执行行为

脚本可以重复执行，但需要注意以下行为：

| 步骤 | 重复执行时的行为 |
|---|---|
| 解压 | 安装目录已有 Kafka 时跳过，**不校验版本** |
| 配置文件 | **每次都会按本次参数重新生成并覆盖** |
| 原始配置备份 | 仅首次生成，之后不再覆盖 |
| 存储格式化 | 已格式化时跳过，Cluster ID 与数据不受影响 |
| 服务 | **每次都会 restart**，运行中的业务会短暂中断 |

特别注意：

- 已启用 SASL 的环境重复执行时，若未带 `-P`，会**重新生成随机密码**，现有客户端将无法认证。
- 若未带 `--sasl`，认证端口会被**直接移除**。
- 修改配置前请确认参数与首次部署一致，或改为手动编辑 `server.properties` 后执行 `systemctl restart kafka`。

## 安全注意事项

1. **免认证端口对外开放**：9092 绑定所有网卡，且未启用 ACL，连接者拥有全部权限（建删 topic、读写任意数据）。开启 `--sasl` 后，只要 9092 仍可访问，认证端口就起不到访问控制作用。请通过防火墙或安全组将 9092 的来源限制在可信业务主机。
2. **SASL_PLAINTEXT 不加密**：账号密码和消息内容在网络上明文传输，只适合可信内网；跨网段或公网访问应改用 SASL_SSL。
3. **密码存放与显示**：
   - 密码明文保存在 `server.properties` 和 `client-sasl.properties` 中（权限 600）。
   - 部署结束时会在终端打印密码，注意终端录屏和日志留存。
   - 通过 `-P` 传入的密码会留在 shell 历史和进程列表中，执行后可用 `history -d` 清理。
4. **随机密码强度**：自动生成的密码基于 bash `$RANDOM`，并非密码学安全的随机源，生产环境建议通过 `-P` 传入强密码。
5. **以 root 运行**：服务未配置专用运行用户，Kafka 进程以 root 身份运行。
6. **下载未校验哈希**：在线模式只校验归档结构，未比对官方 `.sha512`。对安装包来源有要求时，建议先手工下载并校验哈希，再使用 `-f` 离线部署。

## 故障排查

| 现象 | 排查方向 |
|---|---|
| 服务启动失败 | `journalctl -u kafka -e` 与 `/opt/kafka/logs/server.log` 查看具体报错 |
| 端口被占用 | `ss -lntp \| grep -E '9092\|9093\|9094'`，确认没有其他进程占用 |
| 远程客户端能连上但收发超时 | 多半是宣告地址不对（如取到 docker0 的 172.17.0.1 或回退为 localhost），使用 `-H` 指定正确 IP 重新部署，或修改 `advertised.listeners` 后重启 |
| 认证端口报 `Authentication failed` | 核对 `client-sasl.properties` 与 `server.properties` 中的用户名密码是否一致（重复执行可能已更换密码） |
| 启动报 Cluster ID 不匹配 | 数据目录曾被其他集群格式化过；确认数据可丢弃后清空数据目录再重新执行脚本 |
| 下载失败 | 检查网络与镜像地址，或改用 `-f` 离线部署 |
| 需要调整 JVM 堆内存 | 在 `kafka.service` 的 `[Service]` 段加入 `Environment="KAFKA_HEAP_OPTS=-Xms2G -Xmx2G"`，然后执行 `systemctl daemon-reload && systemctl restart kafka` |

注意：脚本在启动后 3 秒检查服务状态，若 JVM 较晚才因配置错误退出，脚本可能已显示部署成功。部署完成后建议用上面的验证命令实际测试一次。

## 卸载

```bash
systemctl stop kafka
systemctl disable kafka
rm -f /etc/systemd/system/kafka.service
systemctl daemon-reload

rm -rf /opt/kafka               # 安装目录（以实际 -i 为准）
rm -rf /var/lib/kafka/data      # 数据目录，删除前请确认数据已不再需要
```

## 已知限制

- Kafka 版本固定为 3.9.0（Scala 2.13），如需其他版本需修改脚本中的 `KAFKA_VERSION` 与 `SCALA_VERSION`。
- 仅支持单节点，不支持多节点集群。
- 仅支持 SASL/PLAIN 单个用户，不支持 SCRAM、SSL、ACL。
- 服务以 root 身份运行，未设置专用用户。
- systemd 单元使用 `kafka-server-stop.sh` 作为停止命令，该脚本会停止本机所有 Kafka 进程，不适合一台机器部署多个实例的场景。
