# Mail Center - RabbitMQ Python 学习项目

通过构建一个邮件任务中心来学习 RabbitMQ 核心概念的项目。

## 项目简介

本项目是一个循序渐进的 RabbitMQ 学习教程，通过三个递进的步骤覆盖 RabbitMQ 的所有核心知识点：

| 步骤 | 知识点 | 代码量 |
|------|--------|--------|
| Step 1 | Producer, Consumer, Queue, Topic Exchange, Routing Key, Binding | ~200 行 |
| Step 2 | ACK, NACK, Durable, Persistent Message, Prefetch | 增强 |
| Step 3 | DLX (Dead Letter Exchange), TTL, Delay Queue, Retry Pattern | 完整 |

## 前置要求

- Python 3.12+
- Docker (用于运行 RabbitMQ)
- uv 包管理器

## 快速开始

### 1. 启动 RabbitMQ

```bash
docker run -d --name rabbitmq \
  -p 5672:5672 \
  -p 15672:15672 \
  rabbitmq:3-management
```

- **5672**: RabbitMQ 消息端口
- **15672**: 管理界面 (http://localhost:15672, guest/guest)

### 2. 安装依赖

```bash
# 克隆项目后
uv sync
```

### 3. 运行 Step 1 示例

**终端 1 - 启动生产者：**
```bash
uv run python scripts/step1_basic/producer.py
```

**终端 2 - 启动注册邮件消费者：**
```bash
uv run python scripts/step1_basic/worker_register.py
```

**终端 3 - 启动营销邮件消费者：**
```bash
uv run python scripts/step1_basic/worker_marketing.py
```

## 学习路径

### Step 1: 最小版本

**核心概念：**

- **Producer**: 发送消息到 Exchange
- **Exchange**: 接收消息并路由到 Queue
- **Queue**: 存储消息直到被消费
- **Routing Key**: 决定消息路由到哪个 Queue
- **Topic Exchange**: 支持通配符的路由模式
- **Binding**: 将 Queue 绑定到 Exchange

**代码文件：**

| 文件 | 说明 |
|------|------|
| [src/app/rabbitmq/connection.py](src/app/rabbitmq/connection.py) | RabbitMQ 连接管理 |
| [src/app/rabbitmq/exchange.py](src/app/rabbitmq/exchange.py) | Exchange 和 Queue 声明 |
| [src/app/rabbitmq/publisher.py](src/app/rabbitmq/publisher.py) | 消息发布者 |
| [src/app/rabbitmq/consumer.py](src/app/rabbitmq/consumer.py) | 消息消费者基类 |
| [scripts/step1_basic/producer.py](scripts/step1_basic/producer.py) | 生产者脚本 |
| [scripts/step1_basic/worker_register.py](scripts/step1_basic/worker_register.py) | 注册邮件 Worker |
| [scripts/step1_basic/worker_marketing.py](scripts/step1_basic/worker_marketing.py) | 营销邮件 Worker |

**练习：**

1. 发送 10 条消息，观察路由分发
2. 添加新的邮件类型 (如 "notification")
3. 修改 binding pattern，观察行为变化

### Step 2: 增强版

**新增概念：**

- **ACK**: 确认消息处理成功
- **NACK**: 拒绝消息，可选择重新入队
- **Durable**: Exchange/Queue 在 RabbitMQ 重启后保留
- **Persistent Message**: 消息持久化到磁盘
- **Prefetch**: 控制消费者同时处理的消息数量

**代码文件：**

| 文件 | 说明 |
|------|------|
| [scripts/step2_enhanced/producer.py](scripts/step2_enhanced/producer.py) | 持久化消息发布 |
| [scripts/step2_enhanced/worker.py](scripts/step2_enhanced/worker.py) | 带 ACK/NACK 的消费者 |

**练习：**

1. 在 Worker 处理消息时重启 RabbitMQ，观察持久化效果
2. 测试不同 prefetch 值对吞吐量的影响
3. 模拟处理失败，观察 NACK 和重新入队

### Step 3: 完整版

**新增概念：**

- **DLX (Dead Letter Exchange)**: 死信交换机，接收被拒绝或过期的消息
- **TTL (Time-To-Live)**: 消息过期时间
- **Delay Queue**: 利用 TTL + DLX 实现延迟重试
- **Retry Pattern**: 失败消息的延迟重试机制

**代码文件：**

| 文件 | 说明 |
|------|------|
| [scripts/step3_complete/setup_retry.py](scripts/step3_complete/setup_retry.py) | 设置 DLX 和延迟队列拓扑 |
| [scripts/step3_complete/producer.py](scripts/step3_complete/producer.py) | 带重试跟踪的消息发布 |
| [scripts/step3_complete/worker.py](scripts/step3_complete/worker.py) | 完整重试逻辑的消费者 |

**消息流程：**

```
Producer → Exchange → Queue → Worker
                            ↓ [失败]
                       Retry Queue (TTL 5s)
                            ↓ [过期]
                        Retry Exchange
                            ↓
                          Worker (重试)
                            ↓ [3次失败]
                           DLQ
```

**练习：**

1. 引入随机失败，观察重试行为
2. 检查 DLQ 中的失败消息
3. 调整重试次数和 TTL，优化重试策略

## FastAPI 集成

项目提供了完整的 FastAPI REST API：

### 启动 API 服务

```bash
uv run uvicorn app.main:app --reload
```

访问 http://localhost:8000/docs 查看 API 文档。

### API 端点

#### POST /api/mail/send

提交邮件任务：

```bash
curl -X POST http://localhost:8000/api/mail/send \
  -H "Content-Type: application/json" \
  -d '{
    "mail_type": "register",
    "email": "user@example.com",
    "subject": "Welcome!",
    "content": "Welcome to our service!"
  }'
```

响应：
```json
{
  "task_id": "123e4567-e89b-12d3-a456-426614174000",
  "message": "Mail task submitted successfully"
}
```

#### GET /api/mail/task/{task_id}

查询任务状态：

```bash
curl http://localhost:8000/api/mail/task/123e4567-e89b-12d3-a456-426614174000
```

响应：
```json
{
  "task_id": "123e4567-e89b-12d3-a456-426614174000",
  "status": "success",
  "mail_type": "register",
  "email": "user@example.com",
  "retry_count": 0
}
```

## 开发指南

### 项目结构

```
rabbitmq-python/
├── src/app/
│   ├── core/              # 配置和日志
│   ├── api/               # API 端点
│   ├── models/            # 数据模型
│   ├── services/          # 业务服务
│   └── rabbitmq/          # RabbitMQ 模块 (核心)
├── scripts/               # 分步骤学习脚本
│   ├── step1_basic/
│   ├── step2_enhanced/
│   └── step3_complete/
└── tests/                 # 测试文件
```

### 工具链

本项目使用完整的 Python 工具链保证代码质量：

#### 格式化 (Ruff)

```bash
# 检查格式问题
uv run ruff check .

# 自动修复
uv run ruff check . --fix

# 格式化代码
uv run ruff format .
```

#### 类型检查 (Pyright)

```bash
uv run pyright
```

#### 测试 (Pytest)

```bash
# 运行所有测试
uv run pytest

# 运行单元测试（不需要 RabbitMQ）
uv run pytest -m "not integration"

# 带覆盖率报告
uv run pytest --cov=app --cov-report=html
```

### 环境变量

复制 `.env.example` 到 `.env` 并根据需要修改：

```bash
cp .env.example .env
```

| 变量 | 默认值 | 说明 |
|------|--------|------|
| RABBITMQ_URL | amqp://guest:guest@localhost:5672/ | RabbitMQ 连接 URL |
| EXCHANGE_NAME | mail.topic | 主交换机名称 |
| PREFETCH_COUNT | 10 | 消费者预取数量 |
| MAX_RETRY_COUNT | 3 | 最大重试次数 |
| RETRY_TTL_SECONDS | 5 | 重试延迟秒数 |

## RabbitMQ 知识点映射

| RabbitMQ 概念 | 项目实现 | 文件 |
|---------------|----------|------|
| Producer | publish_mail() | publisher.py |
| Consumer | BaseConsumer | consumer.py |
| Topic Exchange | mail.topic | exchange.py |
| Routing Key | mail.register, mail.marketing, ... | exchange.py |
| Queue | register.queue, marketing.queue, ... | exchange.py |
| Binding | queue.bind(exchange, routing_key) | exchange.py |
| ACK | message.ack() | worker.py (Step 2) |
| NACK | message.nack(requeue=False) | worker.py (Step 2) |
| Durable | durable=True | producer.py (Step 2) |
| Persistent | DeliveryMode.PERSISTENT | producer.py (Step 2) |
| Prefetch | channel.set_qos(prefetch_count) | consumer.py |
| DLX | x-dead-letter-exchange | setup_retry.py (Step 3) |
| TTL | x-message-ttl | setup_retry.py (Step 3) |
| Delay Queue | retry.queue with TTL | setup_retry.py (Step 3) |

## 常见问题

### RabbitMQ 连接失败

确保 RabbitMQ 正在运行：

```bash
docker ps | grep rabbitmq
```

如果未运行，启动它：

```bash
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```

### 消息没有被消费

检查队列是否有绑定：

```bash
# 在 RabbitMQ 管理界面查看
# http://localhost:15672
```

### 测试失败

确保依赖已安装：

```bash
uv sync
```

## 下一步学习

完成本项目后，你可以：

1. **FastAPI + RabbitMQ**: 在实际 Web 应用中集成消息队列
2. **Celery**: 学习更高级的任务队列框架
3. **微服务**: 使用 RabbitMQ 作为服务间通信
4. **AI Agent 任务调度**: 异步任务处理

## 许可证

MIT License
