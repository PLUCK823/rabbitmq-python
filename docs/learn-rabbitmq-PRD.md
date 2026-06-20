# 邮件发送中心（Mail Center）

这个项目非常小。

但能覆盖：

```text
Producer
Consumer

Direct Exchange
Topic Exchange

ACK
Durable

Prefetch

DLX

TTL

Delay Queue

aio-pika
```

全部核心知识。

而且逻辑非常符合现实。

---

## RabbitMQ学习项目PRD

项目名称

Mail Center（邮件任务中心）

---

### 一、项目目标

用户提交邮件任务：

↓

RabbitMQ

↓

邮件Worker

↓

发送邮件

↓

成功/失败处理

---

最终实现：

模拟企业中的：

注册邮件

验证码邮件

营销邮件

通知邮件

定时邮件

重试机制

---

### 二、项目规模

开发时间：

1~3天

代码量：

1000行以内

技术栈：

Python

RabbitMQ

aio-pika

FastAPI（可选）

SQLite（可选）

---

### 三、核心功能

## 功能1

发送普通邮件

用户：

提交邮件

↓

RabbitMQ

↓

邮件消费者

↓

发送

---

知识点：

Producer

Consumer

Queue

ACK

---

## 功能2

不同类型邮件

邮件类型：

register

verify

marketing

system

---

Routing Key

```text
mail.register
mail.verify
mail.marketing
mail.system
```

---

Exchange

```text
mail.topic
```

---

知识点：

Topic Exchange

Routing Key

Binding

---

## 功能3

多Worker

例如：

```text
register.queue

marketing.queue

system.queue
```

---

消费者：

```text
RegisterWorker

MarketingWorker

SystemWorker
```

---

知识点：

Topic

多个Queue

多个Consumer

---

## 功能4

失败重试

模拟：

SMTP服务挂了

---

消费者：

随机失败

```python
if random.random() < 0.3:
    raise Exception()
```

---

知识点：

ACK

NACK

重新投递

---

## 功能5

死信队列

失败3次：

↓

DLX

↓

dlq.queue

---

记录失败邮件

---

知识点：

DLX

Dead Letter Queue

---

## 功能6

延迟重试

第一次失败：

5秒后重试

---

第二次失败：

10秒后重试

---

第三次失败：

DLQ

---

知识点：

TTL

Delay Queue

DLX

---

## 功能7

任务状态查询

状态：

PENDING

PROCESSING

SUCCESS

FAILED

---

知识点：

真实业务场景

---

# 四、RabbitMQ设计

Exchange

mail.topic

类型：

Topic

---

# 五、Routing Key

mail.register

mail.verify

mail.marketing

mail.system

mail.retry

mail.failed

---

# 六、Queue设计

register.queue

verify.queue

marketing.queue

system.queue

retry.queue

dlq.queue

---

# 七、业务流程

发送邮件

↓

mail.register

↓

register.queue

↓

消费者

↓

成功

↓

结束

---

失败

↓

retry.queue

↓

TTL

↓

重新投递

↓

再次消费

---

三次失败

↓

dlq.queue

---

# 八、接口设计

POST

/api/mail/send

请求

```json
{
  "type":"register",
  "email":"test@test.com",
  "subject":"欢迎注册",
  "content":"hello"
}
```

---

返回

```json
{
  "task_id":"123"
}
```

---

GET

/api/mail/task/{id}

返回

```json
{
  "status":"SUCCESS"
}
```

---

# 九、RabbitMQ知识点映射

Hello World

↓

普通发送邮件

---

Direct

↓

mail.system

---

Topic

↓

mail.*

---

ACK

↓

发送成功确认

---

Durable

↓

邮件不丢失

---

Prefetch

↓

避免Worker任务堆积

---

DLX

↓

失败邮件处理

---

TTL

↓

消息过期

---

Delay Queue

↓

延迟重试

---

aio-pika

↓

异步消费者

---

# 十、最终效果

启动：

producer.py

worker_register.py

worker_marketing.py

worker_system.py

---

发送：

100封邮件

---

观察：

消息路由

消息消费

ACK

重试

DLQ

延迟队列

全部工作

---

学完本项目后

即可进入：

FastAPI + RabbitMQ

Celery

微服务

AI Agent任务调度

对于你目前的水平，我甚至建议分两步：

### 第一步（今天）

先做最小版本

```text
Producer

↓

Topic Exchange

↓

2个Queue

↓

2个Worker
```

只需要 200 行代码。

把 Topic 真正跑通。

---

### 第二步（明天）

增加：

```text
ACK
Durable
Prefetch
```

---

### 第三步（后天）

增加：

```text
DLX
TTL
Delay Queue
```

到这里 RabbitMQ 的核心机制就全部打通了。
