# Runbook vận hành

Mỗi mục dưới đây tương ứng với một alert trong `deploy/observability/alerts.yml`.
Cấu trúc mỗi mục: triệu chứng, cách xác nhận, nguyên nhân thường gặp, cách xử lý.

Quy ước lệnh: namespace `orders`, deployment `api` và `worker`.
Điều chỉnh theo môi trường của bạn.

## Cách đọc nhanh tình trạng hệ thống

```bash
kubectl -n orders get pods -o wide
kubectl -n orders logs deploy/api --since=10m | grep -E 'level=(ERROR|WARN)'
kubectl -n orders exec deploy/api -- /app/api healthcheck && echo ready      # kiểm tra chính binary
curl -s http://<pod-ip>:9090/readyz                                          # chi tiết từng dependency
```

`/readyz` trả JSON cho thấy `postgres` (bắt buộc), `redis` (không bắt buộc), `kafka` (worker, bắt buộc).
Giá trị `degraded` nghĩa là dependency không bắt buộc đang lỗi, dịch vụ vẫn phục vụ.

Mọi log có `request_id` và `trace_id`.
Từ một lỗi người dùng báo, lấy `X-Request-ID` trong response rồi tìm log theo giá trị đó, và mở trace theo `trace_id` trong Jaeger.

## High 5xx rate

Alert: `ApiHighErrorRate`.

Xác nhận:

1. Dashboard "service overview", biểu đồ 5xx theo route. Lỗi tập trung ở một route hay toàn bộ?
2. Log lỗi: `kubectl -n orders logs deploy/api --since=10m | grep 'level=ERROR'`. Tìm `msg="unhandled error"` và `msg="panic recovered"`.

Nguyên nhân thường gặp:

| Dấu hiệu | Nguyên nhân | Xử lý |
|----------|-------------|-------|
| Lỗi bắt đầu ngay sau deploy | Bản phát hành lỗi | Rollback (mục Deploy và rollback) |
| `/readyz` báo `postgres` fail | Database lỗi hoặc đầy kết nối | Xem "Database pool saturation" |
| `panic recovered` | Bug trong handler | Lấy stack từ log, rollback, sửa, thêm test |
| Chỉ `POST` với `Idempotency-Key` trả `503` | Redis chết | Khôi phục Redis. Các request không có key vẫn chạy bình thường |

## High latency

Alert: `ApiHighLatencyP99`.

Xác nhận theo thứ tự, dừng ở tầng đầu tiên cho thấy bão hòa:

1. CPU pod có chạm limit không (`kubectl top pods`)? Nếu có, HPA có đang scale không? Có thể cần nâng `maxReplicas`.
2. Pool DB: `app_db_pool_acquired_conns` so với `app_db_pool_max_conns`, và tốc độ tăng của `app_db_pool_acquire_wait_seconds_total`.
3. Tỉ lệ cache hit (biểu đồ "Cache hit ratio"). Hit ratio giảm đột ngột làm DB nhận nhiều đọc hơn.
4. Truy vấn chậm ở Postgres: `pg_stat_statements`, hoặc log `log_min_duration_statement`.
5. Mở một trace chậm trong Jaeger để thấy thời gian nằm ở đâu.
6. Lấy CPU profile (xem SCALING.md, mục profiling).

## Database pool saturation

Alert: `DbPoolSaturated`.

Triệu chứng: `acquired_conns` gần `max_conns`, `acquire_wait_seconds_total` tăng, latency tăng.

Đừng phản xạ "tăng `DB_MAX_CONNS`".
Pool đầy thường là **hệ quả** của truy vấn chậm hoặc giao dịch dài, không phải nguyên nhân.
Hãy tìm:

```sql
-- Truy vấn và giao dịch đang chạy lâu
SELECT pid, now() - xact_start AS xact_age, state, wait_event_type, wait_event, left(query, 120)
FROM pg_stat_activity WHERE datname = 'app' AND state <> 'idle' ORDER BY xact_start;

-- Khóa đang chờ
SELECT * FROM pg_locks WHERE NOT granted;
```

Tăng kết nối chỉ đúng khi Postgres còn dư CPU và tổng `số pod x DB_MAX_CONNS` vẫn nhỏ hơn `max_connections`.
Nếu không, dùng PgBouncer (SCALING.md, bậc 1).

## Outbox backlog growing

Alert: `OutboxNotDraining`. Có đơn được tạo nhưng không có event nào được publish.

Xác nhận:

```sql
SELECT count(*) AS pending, min(created_at) AS oldest FROM outbox WHERE published_at IS NULL;
```

Nếu `pending` tăng và `oldest` ngày càng cũ thì relay không xả được.

| Nguyên nhân | Dấu hiệu | Xử lý |
|-------------|----------|-------|
| Worker không chạy | `kubectl -n orders get pods -l app=worker` | Khởi động lại, xem log crash |
| Kafka không với tới được | Log worker: `outbox drain failed`, `/readyz` của worker báo `kafka` fail | Khôi phục Kafka. Outbox tự xả khi Kafka về, **không mất event** |
| Lock bị giữ bởi một session treo | Không có log lỗi nhưng không xả | Tìm session giữ advisory lock: `SELECT * FROM pg_locks WHERE locktype = 'advisory'`. Hủy session đó bằng `pg_terminate_backend(pid)` |
| Event cực lớn làm batch lỗi liên tục | Cùng một lỗi lặp trên cùng batch | Giảm `OUTBOX_BATCH_SIZE`, kiểm tra kích thước payload so với `message.max.bytes` của Kafka |

Trong lúc Kafka lỗi, API vẫn nhận ghi bình thường: đây là thiết kế có chủ đích, đừng vội dừng API.
Backlog lớn sẽ được xả theo lô liên tục ngay khi Kafka phục hồi.

## Consumer lag growing

Alert: `ConsumerNotProcessing`, hoặc lag tăng trên Kafka.

Xem lag:

```bash
kafka-consumer-groups.sh --bootstrap-server <broker> --describe --group order-notifier
```

| Nguyên nhân | Xử lý |
|-------------|-------|
| Số worker ít hơn số partition và đang bận | Tăng replicas, tối đa bằng số partition |
| Handler chậm (DB, dịch vụ ngoài) | Xem trace của `consume orders.events`, tối ưu hoặc batch ghi |
| Rebalance liên tục | Kiểm tra pod restart liên tục (OOM, liveness). Kiểm tra `session.timeout` |
| Một partition kẹt | Log có `message handling failed, will retry` lặp lại. Sau `KAFKA_CONSUMER_MAX_ATTEMPTS` lần message sẽ vào DLQ |

## Dead letter queue not empty

Alert: `DeadLetterQueueReceivingMessages`.

DLQ chứa message mà consumer không xử lý được sau số lần thử tối đa, hoặc bị đánh dấu lỗi vĩnh viễn (payload sai định dạng, thiếu header `event_id`).
Mỗi bản ghi mang các header chẩn đoán: `x-error`, `x-original-topic`, `x-original-partition`, `x-original-offset`, `x-failed-at`.

Xem nội dung:

```bash
kafka-console-consumer.sh --bootstrap-server <broker> --topic orders.events.dlq \
  --from-beginning --property print.headers=true --property print.key=true
```

Quy trình:

1. Đọc `x-error` để biết nguyên nhân. Lỗi tạm thời (dependency sập lúc đó) hay lỗi dữ liệu (message hỏng)?
2. Sửa nguyên nhân gốc trước (deploy bản sửa nếu là bug).
3. Phát lại bằng cách đưa message về topic chính. Việc phát lại **an toàn** vì consumer idempotent: message đã xử lý sẽ bị bỏ qua nhờ bảng `processed_events`.

```bash
kafka-console-consumer.sh --bootstrap-server <broker> --topic orders.events.dlq \
  --from-beginning --timeout-ms 5000 --property print.key=true --property key.separator='|' \
 | kafka-console-producer.sh --bootstrap-server <broker> --topic orders.events \
     --property parse.key=true --property key.separator='|'
```

Lưu ý: cách trên không giữ header (`event_id`).
Với message cần giữ nguyên header, dùng công cụ như `kcat` với `-H` hoặc viết một lệnh phát lại nhỏ dùng `franz-go`.
Message bị đánh dấu lỗi vĩnh viễn vì thiếu `event_id` sẽ lại vào DLQ nếu phát lại mà không có header.

4. Khi đã xử lý xong, hãy ghi lại nguyên nhân và hành động để không lặp lại.

## Redis down

Không có alert riêng vì tác động đã được thiết kế để nhỏ.

| Chức năng | Hành vi khi Redis lỗi |
|-----------|----------------------|
| Cache | Đọc thẳng DB. Latency tăng, tải DB tăng. Theo dõi pool DB |
| Rate limit | Cho qua (fail open) |
| `POST` có `Idempotency-Key` | Trả `503`. Client thử lại sau |
| `POST` không có key | Chạy bình thường |
| SSE realtime | Không nhận update mới. Kết nối hiện có vẫn mở nhưng im lặng |
| `/readyz` | `redis: degraded`, pod không bị loại |

Circuit breaker mở sau 5 lỗi liên tiếp rồi bỏ qua Redis trong 10 giây rồi thử lại, nên Redis chết không làm mỗi request chịu thêm timeout.
Khi Redis hồi phục, cache nguội: DB sẽ chịu tải đọc lớn tạm thời, `singleflight` giảm bớt đỉnh.
Nếu DB chịu không nổi, tạm giảm tải bằng cách hạ `RATE_LIMIT_RPS` rồi `kubectl rollout restart` (hoặc dùng rate limit tầng biên).

## Memory or goroutine growth

Alert: `GoroutineLeakSuspected`, hoặc pod bị OOMKilled.

```bash
kubectl -n orders port-forward deploy/api 6060:6060
curl -s 'http://localhost:6060/debug/pprof/goroutine?debug=1' | head -50   # nhóm goroutine giống nhau
go tool pprof -http=:8081 http://localhost:6060/debug/pprof/heap
```

Thủ phạm thường gặp: kết nối SSE không đóng, vòng lặp chờ channel không có ctx.Done, lời gọi ngoài thiếu timeout.
`go_goroutines` tăng đều không giảm là dấu hiệu rò rỉ.
Kết nối SSE có thời gian sống tối đa (`HTTP_SSE_MAX_CONNECTION`) nên không thể sống vô hạn.

## Deploy và rollback

Quy trình chuẩn (đã mã hóa trong `.github/workflows/release.yml`):

1. Gắn tag `vX.Y.Z`. CI build image đa kiến trúc, tạo SBOM và provenance, ký bằng cosign, đẩy lên GHCR.
2. Chạy migration (Job `migrate`) **trước** khi triển khai code mới.
3. Cập nhật image của `api` và `worker`. Rolling update với `maxUnavailable: 0` giữ nguyên năng lực.
4. Đợi `kubectl rollout status`. Theo dõi tỉ lệ 5xx và latency trong 10 tới 15 phút.
5. Production cần phê duyệt thủ công thông qua GitHub Environment.

Rollback:

```bash
kubectl -n orders rollout undo deploy/api
kubectl -n orders rollout undo deploy/worker
```

Rollback code an toàn **chỉ khi** migration tương thích ngược, xem mục dưới.
Không bao giờ chạy `migrate down` trên production như một phần của rollback thông thường.

## Migration an toàn: expand rồi contract

Trong lúc rolling update, bản cũ và bản mới cùng chạy với cùng một schema.
Vì vậy mỗi migration phải tương thích với **cả hai** phiên bản code.

| Muốn làm | Cách làm an toàn |
|----------|------------------|
| Thêm cột | Thêm cột nullable hoặc có default. Triển khai code dùng cột. Siết `NOT NULL` ở migration sau |
| Đổi tên cột | Thêm cột mới, ghi vào cả hai, backfill, chuyển đọc sang cột mới, rồi mới xóa cột cũ ở lần phát hành sau |
| Xóa cột | Triển khai code không còn dùng cột, rồi mới xóa ở lần phát hành sau |
| Thêm index trên bảng lớn | `CREATE INDEX CONCURRENTLY` (không chạy trong transaction, đặt trong một migration riêng) |
| Thêm ràng buộc | `ADD CONSTRAINT ... NOT VALID` rồi `VALIDATE CONSTRAINT` riêng để tránh khóa bảng lâu |

Đặt `lock_timeout` ngắn cho migration để nó bỏ cuộc thay vì chặn toàn bộ truy vấn khi gặp khóa.

Nếu migration bị dở dang (trạng thái `dirty`):

```bash
DB_URL=... /app/migrate version     # xem version và dirty
# Sửa tay phần schema dở dang cho khớp với một version, rồi:
DB_URL=... /app/migrate force <version>
```

## Điều chỉnh tắt êm (graceful shutdown)

Chuỗi khi pod bị thay: SIGTERM, `/readyz` chuyển `503` ngay, chờ `HTTP_SHUTDOWN_DELAY` (load balancer ngừng gửi request mới), drain request đang chạy tới `HTTP_SHUTDOWN_TIMEOUT`, rồi đóng pool.

Ràng buộc: `terminationGracePeriodSeconds` của pod phải lớn hơn `HTTP_SHUTDOWN_DELAY + HTTP_SHUTDOWN_TIMEOUT` cộng chút dư.
Manifest dùng 40 giây cho 5 giây trễ và 25 giây drain.
Nếu thấy lỗi `502` thoáng qua trong mỗi lần deploy, tăng `HTTP_SHUTDOWN_DELAY`, vì ingress của bạn cần thời gian dài hơn để cập nhật danh sách backend.

Worker có 60 giây để hoàn tất message đang xử lý và commit offset.
Nếu bị kill cứng giữa chừng, message được giao lại và `processed_events` chặn hiệu ứng trùng.

## Bảng thông số vận hành

| Thông số | Giá trị mặc định | Khi nào chỉnh |
|----------|------------------|---------------|
| `DB_MAX_CONNS` | 25 (compose), 20 (k8s) | Khi `số pod x giá trị này` tiến gần `max_connections` thì giảm hoặc dùng PgBouncer |
| `HTTP_REQUEST_TIMEOUT` | 10s | Khi có endpoint hợp lệ cần lâu hơn |
| `RATE_LIMIT_RPS` / `BURST` | 100 / 200 (50 / 100 trong k8s) | Theo năng lực đo được và đối tượng người dùng |
| `OUTBOX_BATCH_SIZE` | 500 | Giảm nếu payload lớn, tăng nếu backlog thường xuyên |
| `KAFKA_CONSUMER_MAX_ATTEMPTS` | 5 | Tăng nếu dependency hay lỗi tạm thời |
| `OTEL_TRACE_SAMPLE_RATIO` | 0.1 (0.05 trong k8s) | Giảm khi chi phí trace cao, tăng tạm thời khi điều tra |
| `LOG_LEVEL` | info | `warn` khi cần giảm khối lượng log ở tải rất cao |
