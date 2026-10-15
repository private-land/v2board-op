# v2board-vo

> 后端相关运维脚本集合
>
> 服务端版本请使用 [xflash-panda/v2board](https://github.com/xflash-panda/v2board), 不要使用原版




### Durable rotation daemon (opt-in)

`lightsail_cf --daemon` enables the server's durable rotation protocol. Without
this flag, the existing batch/cron behavior is unchanged. The panel must enable
`GFW_ROTATION_ENABLED` and explicitly allow the worker's existing
`--server_query_tags`; an empty or disabled allowlist performs no cloud work.

The daemon reuses the existing regional AWS/Cloudflare configuration. Configure
the dedicated wakeup Redis connection with `ROTATION_REDIS_ADDR`,
`ROTATION_REDIS_USERNAME`, `ROTATION_REDIS_PASSWORD`, `ROTATION_REDIS_DB`, and
`ROTATION_REDIS_TLS`; it does not consume Laravel queues or cache formats.
`ROTATION_WORKER_ID` optionally sets the worker identity. Keep Redis private or
use TLS; supply secrets through the deployment's existing environment mechanism.

API tasks are authoritative. Redis reconnects trigger reconciliation, and a
300-second reconciliation repairs missing notifications. Retry availability uses
server backoff; the panel's scheduled publisher republishes eligible retries at
up to 60-second granularity. Idle workers do not initialize cloud clients.
`--concurrency` bounds running tasks and claims. The daemon holds the existing
regional/tag file lock for its lifetime, fencing legacy cron with the same tag
configuration. Stop it with SIGINT or SIGTERM before reverting to cron.

Each task is bounded to 15 minutes, and each AWS state wait to 5 minutes. Cloud
and DNS mutations are guarded by the task lease. Once a target IP is saved,
retries inspect the saved AWS instance and reconcile DNS without another IP
rotation. Ambiguous interrupted rotations or an externally changed IP require
manual inspection and stop further mutations. Mieru always uses dynamic IPs;
other server types retain the existing static/dynamic selection policy.
