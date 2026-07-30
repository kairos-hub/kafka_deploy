# Kafka 自动化部署脚本

本仓库提供两个脚本:

| 脚本 | 用途 |
|------|------|
| `deploy_kafka_single.sh` | **单机版**部署(单节点 broker+controller)。可选 SASL/PLAIN 认证 + ACL 授权。 |
| `deploy_kafka_cluster.sh` | **多节点集群**部署(每节点 broker+controller,组成 KRaft 仲裁)。默认开启 SASL/PLAIN 认证 + ACL 授权,支持自定义账号与授权。 |

`deploy_kafka_single.sh` —— 一键部署 **Kafka 3.9.0(kafka_2.13-3.9.0,KRaft 模式)** 单机环境。
基于 KRaft(无需 ZooKeeper),单节点同时承担 broker + controller 角色,自动完成下载/解压、配置渲染、存储格式化与 systemd 服务注册。

## 特性

- ✅ **KRaft 模式**:无需额外部署 ZooKeeper
- ✅ **安装目录、数据目录可自定义**,解压内容直接落在安装目录下(不含多余的 `kafka_2.13-3.9.0/` 层)
- ✅ **在线 / 离线**两种部署方式
- ✅ **可选 SASL/PLAIN 认证**:一个 `--sasl` 开关即可开启用户名/密码认证,默认关闭
- ✅ **自动探测内网 IP** 写入 `advertised.listeners`,默认即支持远程客户端连接
- ✅ **systemd 托管**:开机自启、失败自动重启
- ✅ **幂等可重入**:已安装/已格式化会自动跳过,不会破坏已有数据
- ✅ 严格模式(`set -euo pipefail`)+ Java/权限/包完整性等前置校验

## 环境要求

| 项 | 要求 |
|----|------|
| 操作系统 | Linux(基于 systemd) |
| Java | JDK 8+,推荐 11 / 17(脚本会检测,缺失则报错) |
| 权限 | root,或具备 sudo(创建系统目录、注册服务需要) |
| 网络工具 | 在线部署需 `curl` 或 `wget`;离线部署不需要 |

## 快速开始

```bash
chmod +x deploy_kafka_single.sh
```

### 在线部署(自动从官方/镜像下载)

```bash
# 使用默认目录(/opt/kafka 与 /var/lib/kafka/data)
sudo ./deploy_kafka_single.sh

# 自定义安装目录与数据目录
sudo ./deploy_kafka_single.sh -i /usr/local/kafka -d /data/kafka

# 国内服务器走阿里云镜像加速
sudo ./deploy_kafka_single.sh -m https://mirrors.aliyun.com/apache/kafka/3.9.0
```

### 离线部署(使用本地安装包)

先把官方安装包 `kafka_2.13-3.9.0.tgz` 拷到目标机,然后:

```bash
sudo ./deploy_kafka_single.sh -f ./kafka_2.13-3.9.0.tgz \
     -i /usr/local/kafka -d /usr/local/kafka/data -H 172.21.31.29
```

## 参数说明

| 参数 | 长选项 | 说明 | 默认值 |
|------|--------|------|--------|
| `-i` | `--install-dir` | Kafka 安装目录(解压内容直接落于此) | `/opt/kafka` |
| `-d` | `--data-dir` | Kafka 数据目录(`log.dirs`) | `/var/lib/kafka/data` |
| `-p` | `--port` | Broker(PLAINTEXT)监听端口 | `9092` |
| `-H` | `--advertised-host` | 对外宣告地址,客户端用它连接 broker | 自动探测内网 IP |
| `-f` | `--package` | 本地安装包路径,提供后启用离线部署、跳过下载 | (空) |
| `-m` | `--mirror` | 下载镜像地址前缀 | Apache 官方归档站 |
| `-S` | `--sasl` | 启用 SASL/PLAIN 认证(开关,无需取值) | 关闭 |
| `-U` | `--sasl-user` | SASL 用户名(仅 `--sasl` 时生效) | `admin` |
| `-P` | `--sasl-password` | SASL 密码(仅 `--sasl` 时生效,留空则随机生成并打印) | 随机生成 |
| `-h` | `--help` | 显示帮助 | — |

> Controller 监听端口固定为 `9093`(单机自连,无需配置)。

## 部署完成后

脚本会注册并启动名为 `kafka` 的 systemd 服务:

```bash
systemctl start   kafka      # 启动
systemctl stop    kafka      # 停止
systemctl restart kafka      # 重启
systemctl status  kafka      # 查看状态
journalctl -u kafka -f       # 实时日志
```

### 验证

```bash
# 创建 topic(将 IP/端口替换为部署时的 advertised 地址)
<INSTALL_DIR>/bin/kafka-topics.sh --create --topic test \
    --bootstrap-server 172.21.31.29:9092 --partitions 1 --replication-factor 1

# 列出 topic
<INSTALL_DIR>/bin/kafka-topics.sh --list --bootstrap-server 172.21.31.29:9092
```

## SASL 认证(可选)

默认部署为 `PLAINTEXT`(无认证)。加上 `--sasl` 即可开启 **SASL_PLAINTEXT / PLAIN** 用户名密码认证:

```bash
# 指定用户名和密码
sudo ./deploy_kafka_single.sh --sasl -U admin -P 'MyS3cret!'

# 不指定密码,由脚本随机生成并在完成提示中打印
sudo ./deploy_kafka_single.sh --sasl -U admin
```

开启后脚本会:

- 将 broker 监听器改为 `SASL_PLAINTEXT`,启用 `PLAIN` 机制,并把 JAAS 配置**内联**写进 `server.properties`(无需额外的 `KAFKA_OPTS` 或独立 jaas 文件);inter-broker 通信同样走 SASL。
- **同时开启 ACL 授权**:仅认证只能验明身份、不限制操作,因此脚本还会启用 KRaft 的 `StandardAuthorizer`,把该 SASL 账号设为**超级用户**(授予全部权限),并默认拒绝其他未授权账号:

  ```properties
  authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer
  super.users=User:admin;User:ANONYMOUS
  allow.everyone.if.no.acl.found=false
  ```

  > - `User:<账号>` 即上面 `-U` 指定的 SASL 用户,作为超级用户拥有全部权限。
  > - `User:ANONYMOUS` 对应 PLAINTEXT 的 CONTROLLER 监听器(单机本地自连),不加会导致控制器请求被拒、broker 无法启动。
  > - `allow.everyone.if.no.acl.found=false`:未显式授权的其他账号一律拒绝。若日后新增普通账号,需用 `kafka-acls.sh --command-config <client-sasl.properties>` 单独授予其 Topic/Group 等权限。
- 生成客户端连接配置 `<INSTALL_DIR>/config/client-sasl.properties`(权限 `600`),内容形如:

  ```properties
  security.protocol=SASL_PLAINTEXT
  sasl.mechanism=PLAIN
  sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="MyS3cret!";
  ```

客户端 / 命令行工具连接时,附带 `--command-config` 指向该文件即可:

```bash
<INSTALL_DIR>/bin/kafka-topics.sh --list \
    --bootstrap-server 172.21.31.29:9092 \
    --command-config <INSTALL_DIR>/config/client-sasl.properties
```

> - CONTROLLER 监听器为单机本地自连,保持 `PLAINTEXT`,不参与对外认证。
> - PLAIN 机制下密码以明文存放于配置文件,仅在可信内网使用;需要更强安全性时应叠加 SSL(SASL_SSL)或改用 SCRAM。
> - 重复执行且用 `-P` 指定相同密码,结果幂等;若不指定密码,每次重跑都会生成新密码。

## 网络与监听说明

| 配置项 | 作用 | 取值 |
|--------|------|------|
| `listeners` | broker 实际绑定地址 | `0.0.0.0:9092`(所有网卡) |
| `advertised.listeners` | 返回给**客户端**用于重新建连的地址 | `<-H 指定或自动探测的 IP>:9092` |
| `controller.quorum.voters` | **controller 之间**通信 | `1@localhost:9093`(单机自连) |
| CONTROLLER 监听器绑定 | controller 端口实际绑定地址 | `localhost:9093`(仅本机回环,不对外暴露) |

- 仅本机使用:可显式 `-H localhost`。
- 远程客户端连接:`advertised.listeners` 必须是客户端可达的真实 IP/域名(默认已自动探测)。
- 多网卡/公网机器:`hostname -I` 取第一个 IP,未必符合预期,建议用 `-H` 显式指定。

## 生成的配置

脚本将 KRaft 单机配置写入 `<INSTALL_DIR>/config/kraft/server.properties`(首次会把原文件备份为 `server.properties.orig`),关键项:

- `process.roles=broker,controller`、`node.id=1`
- 单机副本因子全部为 `1`(`offsets.topic` / `transaction.state.log` 等)
- `log.dirs=<DATA_DIR>`、日志保留 168 小时

## 幂等与重复执行

- **已安装**:检测到 `<INSTALL_DIR>/bin/kafka-server-start.sh` 存在则跳过下载与解压。
- **已格式化**:检测到 `<DATA_DIR>/meta.properties` 存在则跳过存储格式化,**避免改变 Cluster ID 导致已有数据不可用**。

## 注意事项

- **数据目录不建议放在安装目录内**。若按示例放在 `<INSTALL_DIR>/kafka/data`,日后删除安装目录重装会连同数据一并删除。生产环境建议数据目录独立(如 `/data/kafka`)。
- 离线包顶层目录必须为 `kafka_2.13-3.9.0`,脚本会校验,版本/包名不符会报错。
- 默认监听为 `PLAINTEXT`(无认证、无加密),仅适用于可信内网;需要认证时加 `--sasl` 开启 SASL/PLAIN(见上文)。

## 卸载

```bash
sudo systemctl stop kafka
sudo systemctl disable kafka
sudo rm /etc/systemd/system/kafka.service
sudo systemctl daemon-reload
sudo rm -rf <INSTALL_DIR> <DATA_DIR>
```

---

# 多节点集群部署(`deploy_kafka_cluster.sh`)

在**每台**目标节点各执行一次本脚本,通过 `--nodes` 描述整个集群、`--node-id` 指明本机身份,各节点自我配置(**无需 SSH 免密**)。所有节点均为 `broker+controller` 合体角色,共同组成 KRaft controller 仲裁(quorum)。

## 与单机版的关键差异

| 方面 | 单机 `deploy_kafka_single.sh` | 集群 `deploy_kafka_cluster.sh` |
|------|------------------------|-------------------------------|
| CONTROLLER 监听器 | 绑 `localhost`,协议 `PLAINTEXT` | 绑所有网卡(供节点互连),启用 SASL 时协议为 **`SASL_PLAINTEXT`** |
| 授权超级用户 | `User:admin;User:ANONYMOUS`(ANONYMOUS 仅本机可达) | 仅 `User:admin` —— **controller 也认证**,principal 为 admin,故**无需 ANONYMOUS**,不在集群网络上暴露匿名超权 |
| 副本因子 | 固定 `1` | 默认 `min(3, 节点数)`,`min.insync.replicas` 默认 2 |
| Cluster ID / 密码 | 单机自动生成 | **所有节点必须一致**:首节点生成并打印,其余节点用 `-c` / `-P` 传入相同值 |
| SASL 默认 | 关闭(需 `--sasl` 开) | **默认开启**(可 `--no-sasl` 关) |

## 快速开始(三节点)

```bash
# 节点 1(会打印 Cluster ID,请记下)
sudo ./deploy_kafka_cluster.sh \
     -N 1@10.0.0.1,2@10.0.0.2,3@10.0.0.3 -n 1 -P 'AdminP@ss'

# 节点 2、节点 3(用节点 1 打印出的 <ClusterID>,密码保持一致)
sudo ./deploy_kafka_cluster.sh \
     -N 1@10.0.0.1,2@10.0.0.2,3@10.0.0.3 -n 2 -P 'AdminP@ss' -c <ClusterID>
sudo ./deploy_kafka_cluster.sh \
     -N 1@10.0.0.1,2@10.0.0.2,3@10.0.0.3 -n 3 -P 'AdminP@ss' -c <ClusterID>
```

> 各节点需放通端口:broker `9092`、controller `9093`(节点之间互通)。所有节点启动后仲裁才会选主。

## 参数说明

| 参数 | 长选项 | 说明 | 默认值 |
|------|--------|------|--------|
| `-N` | `--nodes` | 集群所有节点 `id@host`,逗号分隔(**必填**) | — |
| `-n` | `--node-id` | 本机节点 id,须在 `--nodes` 中(**必填**) | — |
| `-c` | `--cluster-id` | 集群 ID,所有节点须一致;不填则自动生成并打印 | 自动生成 |
| `-P` | `--sasl-password` | 管理员密码;启用 SASL 时**所有节点须相同** | 随机生成 |
| `-U` | `--sasl-user` | 管理员账号 | `admin` |
| `-i` | `--install-dir` | 安装目录 | `/opt/kafka` |
| `-d` | `--data-dir` | 数据目录 | `/var/lib/kafka/data` |
| `-p` | `--port` | broker 端口 | `9092` |
|  | `--controller-port` | controller 端口 | `9093` |
| `-H` | `--advertised-host` | 本机对客户端宣告地址 | 取 `--nodes` 中本机 host |
| `-f` | `--package` | 本地离线安装包 | (空) |
| `-m` | `--mirror` | 下载镜像前缀 | Apache 官方归档站 |
| `-r` | `--replication-factor` | 内部/默认副本因子 | `min(3, 节点数)` |
|  | `--min-isr` | 最小同步副本 | `RF>=3 ? 2 : 1` |
|  | `--partitions` | 默认分区数 | `3` |
|  | `--no-sasl` | 关闭 SASL(同时关闭授权) | (默认开启) |
|  | `--extra-user U:P` | 追加 SASL 账号(可重复) | — |
|  | `--super-users LIST` | 覆盖 `super.users` | `User:<管理员>` |
|  | `--no-authz` | 关闭 ACL 授权(仅保留认证) | (默认开启) |
|  | `--allow-everyone` | 未授权默认放行 | (默认拒绝) |

## 自定义 SASL 与授权

默认即为 **SASL_PLAINTEXT / PLAIN 认证 + StandardAuthorizer 授权**,管理员账号为超级用户,其余账号默认拒绝。可按需自定义:

```bash
# 追加两个业务账号,并把 admin 之外再指定一个超级用户
sudo ./deploy_kafka_cluster.sh -N 1@a,2@b,3@c -n 1 -P 'AdminP@ss' \
     --extra-user app:appPwd --extra-user reader:rPwd \
     --super-users 'User:admin;User:ops'
```

追加账号只是**建立了身份**,默认拒绝策略下它们还没有任何权限。用 `kafka-acls.sh` 给它们**授权**(在任一节点执行一次,全集群生效):

```bash
# 给 app 账号授予某 topic 的读写、某消费组的读权限
<INSTALL_DIR>/bin/kafka-acls.sh \
    --bootstrap-server 10.0.0.1:9092,10.0.0.2:9092,10.0.0.3:9092 \
    --command-config <INSTALL_DIR>/config/client-sasl.properties \
    --add --allow-principal User:app \
    --operation Read --operation Write --operation Describe \
    --topic orders --group order-consumers

# 查看已配置的 ACL
<INSTALL_DIR>/bin/kafka-acls.sh --bootstrap-server ... \
    --command-config <INSTALL_DIR>/config/client-sasl.properties --list
```

> - controller 监听器同样启用 SASL,节点间以管理员账号互认,因此 `super.users` **不含** `User:ANONYMOUS`,集群网络上不存在匿名超级权限。
> - PLAIN 机制密码明文存于配置文件,仅限可信内网;更强安全性应叠加 SSL(SASL_SSL)或改用 SCRAM。
> - `--no-sasl` 会一并关闭授权(无认证时 principal 均为 ANONYMOUS,ACL 失去意义)。

## 验证集群

所有节点启动后,任一节点执行:

```bash
# controller 仲裁状态(应能看到 Leader 及各节点 LogEndOffset)
<INSTALL_DIR>/bin/kafka-metadata-quorum.sh \
    --bootstrap-server 10.0.0.1:9092 \
    --command-config <INSTALL_DIR>/config/client-sasl.properties describe --status

# 创建并查看 topic(副本分布在多个 broker)
<INSTALL_DIR>/bin/kafka-topics.sh --create --topic test \
    --bootstrap-server 10.0.0.1:9092 --partitions 3 --replication-factor 3 \
    --command-config <INSTALL_DIR>/config/client-sasl.properties
```

## 幂等与重复执行

与单机版一致:已安装则跳过下载解压;`<DATA_DIR>/meta.properties` 存在则跳过格式化(**避免改变 Cluster ID**)。重跑某节点时,`-c` 需继续传入集群既有的 Cluster ID。

## 卸载(每个节点执行)

```bash
sudo systemctl stop kafka && sudo systemctl disable kafka
sudo rm /etc/systemd/system/kafka.service
sudo systemctl daemon-reload
sudo rm -rf <INSTALL_DIR> <DATA_DIR>
```
