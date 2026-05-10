#!/bin/sh
set -e

# 日志轮转大小，默认 100M，可通过 LOG_MAX_SIZE 环境变量覆盖
LOG_MAX_SIZE="${LOG_MAX_SIZE:-100M}"
LOG_ROTATE_INTERVAL="${LOG_ROTATE_INTERVAL:-5min}"

# 渲染 logrotate 配置模板
# sed "s|{{LOG_MAX_SIZE}}|${LOG_MAX_SIZE}|g" \
#     /etc/logrotate.d/podwatcher > /etc/logrotate.d/podwatcher.rendered
# mv /etc/logrotate.d/podwatcher.rendered /etc/logrotate.d/podwatcher

# 将间隔转换为 cron 格式
case "$LOG_ROTATE_INTERVAL" in
    *min)
        MINS="${LOG_ROTATE_INTERVAL%%min}"
        CRON_EXPR="*/${MINS} * * * *"
        ;;
    *h)
        HOURS="${LOG_ROTATE_INTERVAL%%h}"
        CRON_EXPR="0 */${HOURS} * * *"
        ;;
    *)
        CRON_EXPR="*/5 * * * *"
        ;;
esac

# 写入 crontab
echo "${CRON_EXPR} /usr/sbin/logrotate /etc/logrotate.d/podwatcher" > /etc/crontabs/root

# 启动 crond（后台）
crond

echo "logrotate configured: maxsize=${LOG_MAX_SIZE}, check every ${LOG_ROTATE_INTERVAL}"

# 启动应用
exec ./podwatcher
