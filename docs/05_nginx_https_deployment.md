# HTTPS 部署示例（Nginx 反向代理）

- 文档版本：1.0
- 修订日期：2026-03-20
- 对应任务：OPS-002

## 1. 目标

将本地 FastAPI 服务通过 Nginx 暴露为 HTTPS，满足生产环境 `SEC-002` 要求。

## 2. 前置条件

1. 应用已可通过本机访问（例如 `127.0.0.1:8000`）。
2. 已准备 TLS 证书与私钥（`fullchain.pem`、`privkey.pem`）。
3. 服务器已安装 Nginx。

## 3. 建议目录

```bash
/etc/nginx/conf.d/encrypted-data-manager.conf
/etc/ssl/private/encrypted-data-manager/privkey.pem
/etc/ssl/certs/encrypted-data-manager/fullchain.pem
```

私钥权限建议：

```bash
chown root:root /etc/ssl/private/encrypted-data-manager/privkey.pem
chmod 600 /etc/ssl/private/encrypted-data-manager/privkey.pem
```

## 4. Nginx 配置示例

```nginx
server {
    listen 80;
    server_name your-domain.example.com;
    return 301 https://$host$request_uri;
}

server {
    listen 443 ssl http2;
    server_name your-domain.example.com;

    ssl_certificate     /etc/ssl/certs/encrypted-data-manager/fullchain.pem;
    ssl_certificate_key /etc/ssl/private/encrypted-data-manager/privkey.pem;
    ssl_protocols       TLSv1.2 TLSv1.3;
    ssl_ciphers         HIGH:!aNULL:!MD5;
    ssl_prefer_server_ciphers on;

    client_max_body_size 512m;
    proxy_read_timeout 300s;

    location / {
        proxy_pass         http://127.0.0.1:8000;
        proxy_http_version 1.1;
        proxy_set_header   Host $host;
        proxy_set_header   X-Real-IP $remote_addr;
        proxy_set_header   X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header   X-Forwarded-Proto $scheme;
    }
}
```

## 5. 启用与验证

```bash
nginx -t
systemctl reload nginx
curl -I https://your-domain.example.com/healthz
```

预期：返回 `HTTP/1.1 200 OK`，并且业务接口只能通过 HTTPS 暴露。

## 6. 运维检查清单

1. 证书剩余有效期告警（建议 <30 天告警）。
2. 证书更新后执行 `nginx -t && systemctl reload nginx`。
3. 核对上传场景的 `client_max_body_size`，避免大文件导入被 413 拒绝。
4. 防火墙仅开放 443（必要时保留 80 用于跳转）。
