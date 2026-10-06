# Mở rộng quy mô: từ một pod tới hàng triệu người dùng

Tài liệu này trả lời ba câu hỏi:
template đã sẵn sàng chịu tải tới đâu, làm sao tính được cần bao nhiêu tài nguyên, và khi nào phải làm gì tiếp theo.

Nguyên tắc xuyên suốt: **đo trước, tối ưu sau**.
Không có con số nào trong tài liệu này thay thế được một lần load test trên hạ tầng thật của bạn.

## 1. "Triệu người dùng" nghĩa là bao nhiêu request mỗi giây?

Số người dùng không phải là đơn vị để thiết kế hệ thống, request rate mới là đơn vị đó.
Hãy đổi theo công thức sau, rồi thay giả định của bạn vào.

```
requests/ngày  = DAU x số request mỗi người mỗi ngày
RPS trung bình = requests/ngày / 86400
RPS đỉnh       = RPS trung bình x hệ số đỉnh      (thường 3 tới 10, lưu lượng dồn vào vài giờ)
RPS thiết kế   = RPS đỉnh x hệ số dự phòng        (thường 2, để chịu được lỗi một vùng và đợt tăng đột biến)
```

Ví dụ minh họa (giả định, không phải đo đạc):

| | 1 triệu DAU | 10 triệu DAU |
|---|-------------|--------------|
| Request mỗi người mỗi ngày | 30 | 30 |
| Request mỗi ngày | 30 triệu | 300 triệu |
| RPS trung bình | khoảng 350 | khoảng 3.500 |
| RPS đỉnh (hệ số 5) | khoảng 1.750 | khoảng 17.500 |
| RPS thiết kế (hệ số 2) | khoảng 3.500 | khoảng 35.000 |
| Tỉ lệ đọc:ghi 90:10, ghi ở RPS thiết kế | khoảng 350 ghi/s | khoảng 3.500 ghi/s |
| Thao tác Redis mỗi giây (2 mỗi lần đọc) | khoảng 6.300 | khoảng 63.000 |

Điều cần rút ra:
ở mức 1 triệu DAU, một PostgreSQL primary vừa phải, một Redis và vài pod API là đủ về mặt thông lượng.
Độ khó thật sự nằm ở **độ tin cậy, độ trễ đuôi (p99), hot key, tăng trưởng dữ liệu và vận hành**, không phải ở số request thô.
Ở 10 triệu DAU, lượng ghi bắt đầu chạm giới hạn của một primary đơn và cần các bước ở mục 3.

### Định luật Little: đổi RPS thành số kết nối cần có

```
số request đang xử lý đồng thời = RPS x độ trễ trung bình
```

Với 3.500 RPS và độ trễ 50ms, chỉ có khoảng 175 request đang chạy cùng lúc trên toàn bộ hệ thống.
Với 350 giao dịch ghi mỗi giây, mỗi giao dịch giữ kết nối 10ms, số kết nối DB bận trung bình chỉ khoảng `350 x 0,010 = 3,5`.

Hệ quả quan trọng: **pool kết nối nhỏ thì tốt hơn lớn**.
PostgreSQL chạy tốt nhất khi tổng số kết nối hoạt động ở cỡ vài lần số nhân CPU.
Đặt `DB_MAX_CONNS=20` cho mỗi pod là dư.
Tăng nó khi thấy nghẽn thường chỉ làm DB chậm hơn vì tranh chấp, đúng cái ngược với điều bạn muốn.
Dùng metric `app_db_pool_acquire_wait_seconds_total` để biết pool có thực sự là điểm nghẽn không.

### Số lượng pod

Đừng đoán thông lượng của một pod.
Hãy đo bằng `make load-test` trên môi trường staging, tìm RPS mà tại đó p99 bắt đầu vượt SLO, rồi lấy khoảng 60 tới 70% con số đó làm mức cho mỗi pod.
Số pod tối thiểu là `ceil(RPS thiết kế / RPS mỗi pod) + 1` (một pod dự phòng) và rải đều trên các zone.
HPA trong `deploy/k8s/api.yaml` (3 tới 50 pod, mục tiêu CPU 65%) lo phần còn lại.

Mốc tham khảo từ chính repo này (laptop, không đại diện cho server):

| Phép đo | Kết quả |
|---------|---------|
| Benchmark `GET /v1/orders/{id}` qua toàn bộ chuỗi middleware, dữ liệu trong bộ nhớ | khoảng 7 micro giây, 85 allocation mỗi request |
| Kích thước image | api 30 MB, worker 33 MB, migrate 11 MB (distroless) |
| `pgbench`, 10 client, Postgres trong Docker Desktop | khoảng 1.900 TPS, trung bình 5 ms |
| k6 400 RPS, hỗn hợp 90% đọc 10% ghi, mọi thành phần chung một laptop | Các lần chạy tốt: p95 đọc 4 tới 16 ms, p95 ghi 29 tới 31 ms, 0% lỗi. CPU của api khoảng 16% |

Về những lần chạy xấu: một số lần chạy k6 trên laptop xuất hiện đuôi trễ từ 1 tới 7 giây.
Tôi đã điều tra thay vì bỏ qua.
Log phía server xác nhận độ trễ nằm ở server, nhưng log Postgres cho thấy cả câu lệnh `-- ping` cũng mất 1,6 giây, pool DB không bão hòa, Redis không có lỗi, và tắt `synchronous_commit` hoặc tắt access log đều không làm biến mất hiện tượng.
Kết luận hợp lý nhất là nhiễu của máy ảo Docker Desktop khi k6, Kafka, Postgres và các container khác tranh nhau CPU và I/O trên một máy.
Điều đó có nghĩa là: **đừng dùng số liệu laptop để lập kế hoạch dung lượng**, hãy chạy lại trên hạ tầng riêng và nhìn vào p99.

## 2. Những gì template đã làm sẵn cho hiệu năng và độ bền

| Kỹ thuật | Ở đâu | Tác dụng |
|----------|-------|----------|
| Timeout đầy đủ trên `http.Server` | `platform/httpserver` | Chặn slowloris, rò rỉ goroutine và file descriptor |
| `context` timeout mỗi request, `statement_timeout` ở DB | router, `platform/database` | Một truy vấn kẹt không giữ kết nối mãi |
| Pool pgx có jitter vòng đời kết nối | `platform/database` | Tránh cả đội pod cùng đóng và mở lại kết nối |
| Keyset pagination | `OrderRepository.List` | Chi phí O(trang), không suy giảm khi đi sâu |
| Index khớp thứ tự sort, partial index cho outbox | `db/migrations` | Không sort thừa, index nhỏ gọn |
| UUIDv7 | `domain.NewOrder` | Chèn index gần như append-only |
| Cache-aside, `singleflight`, TTL jitter | `usecase`, `redisstore` | Chống stampede và hết hạn đồng loạt |
| Circuit breaker cho Redis | `redisstore/breaker.go` | Redis lỗi không làm mỗi request trả giá timeout |
| Rate limit phân tán, fail open | `redisstore/ratelimit.go` | Bảo vệ dung lượng chung của mọi replica |
| Batching và nén khi produce Kafka | `platform/kafkax` | Throughput cao, ít băng thông |
| Consumer song song theo partition, commit theo lô | `messaging/consumer.go` | Throughput, vẫn giữ thứ tự trong partition |
| Outbox xử lý theo lô, một relay tại một thời điểm | `messaging/relay.go` | Hàng chục nghìn event mỗi giây, thứ tự được bảo đảm |
| `ETag` và `304` | handler `get` | Tiết kiệm băng thông và CPU serialize |
| `GOMEMLIMIT` từ cgroup, `GOMAXPROCS` nhận biết container | `platform/bootstrap`, Go 1.25+ | GC hoạt động sớm hơn khi gần giới hạn bộ nhớ, tránh OOM kill và CPU throttling |
| Rolling update không giảm dung lượng | `maxUnavailable: 0` | Deploy không làm giảm năng lực |
| Drain có trễ khi tắt | `HTTP_SHUTDOWN_DELAY` | Không rớt request khi pod bị thay |

## 3. Thang mở rộng: khi nào làm gì

Đừng làm trước các bước ở dưới khi chưa có tín hiệu.
Mỗi bước thêm độ phức tạp vận hành, chỉ trả khi cần.

### Bậc 0: những gì template cung cấp

Một PostgreSQL primary, một Redis, một Kafka cluster, N pod API không trạng thái, M pod worker.
Đủ cho cỡ 1 triệu DAU với dự phòng rộng.
Việc cần làm ngay: HA cho Postgres, Redis, Kafka (xem README, mục chuẩn bị production).

### Bậc 1: PgBouncer

Dấu hiệu: `số pod x DB_MAX_CONNS` tiến gần `max_connections`, hoặc Postgres tốn nhiều CPU cho quản lý kết nối.
Hành động: đặt PgBouncer ở chế độ transaction pooling phía trước Postgres.
Lưu ý quan trọng: pgx mặc định cache prepared statement theo kết nối, không tương thích với transaction pooling.
Khi dùng PgBouncer hãy đổi `QueryExecModeCacheDescribe` hoặc `QueryExecModeExec` trong cấu hình pgx (`ConnConfig.DefaultQueryExecMode`).
Advisory lock theo transaction (`pg_try_advisory_xact_lock`) mà relay dùng vẫn hoạt động đúng với transaction pooling vì nó gắn với transaction, không gắn với session.

### Bậc 2: read replica

Dấu hiệu: CPU primary cao do truy vấn đọc (danh sách, cache miss), `app_cache_operations_total{result="miss"}` cao.
Hành động: thêm replica streaming, đưa `List` và `GetByID` miss sang replica.
Cạm bẫy: replica có độ trễ.
Sau khi ghi, đọc ngay từ replica có thể thấy dữ liệu cũ (read-your-writes).
Cách xử lý: đọc từ primary trong vài giây sau khi cùng người dùng vừa ghi, hoặc dùng cột `version` để phát hiện dữ liệu cũ.
Việc cần làm: thêm một `OrderRepository` thứ hai trỏ vào replica pool và chọn ở composition root. Usecase và domain không đổi, đó là giá trị của port.

### Bậc 3: phân vùng bảng

Dấu hiệu: bảng `orders` hàng trăm triệu dòng, vacuum và index chậm, backup lâu, truy vấn lịch sử làm bẩn cache của Postgres.
Hành động: `PARTITION BY RANGE (created_at)` theo tháng cho `orders` và `outbox`.
Lợi ích lớn nhất: xóa dữ liệu cũ bằng `DROP PARTITION` thay vì `DELETE`, hoàn toàn không gây bloat, đồng thời lưu trữ lạnh dễ dàng.
Với outbox, đây là cách thay thế cho `DeletePublishedBefore` khi khối lượng lớn.

### Bậc 4: tăng tốc độ ghi vượt một primary

Dấu hiệu: CPU hoặc I/O của primary đã chạm trần dù đã tối ưu, `commit` chậm, WAL là điểm nghẽn.
Các lựa chọn, theo thứ tự nên cân nhắc:

1. Tối ưu trước: giảm số index không cần, gộp ghi, dùng `UNLOGGED` cho dữ liệu tạm, nâng cấu hình máy chủ. Máy lớn thường rẻ hơn độ phức tạp của sharding.
2. Tách bounded context: đưa domain có tải ghi cao nhất sang cơ sở dữ liệu riêng.
3. Shard theo `customer_id` (Citus, hoặc shard ở tầng ứng dụng với bảng ánh xạ shard).
   Cùng một khách hàng luôn nằm cùng shard nên giao dịch và phân trang theo khách hàng vẫn hiệu quả.
   Outbox phải đi theo shard: mỗi shard có bảng outbox riêng và relay riêng với khóa advisory riêng, thứ tự theo aggregate vẫn giữ được vì một aggregate thuộc đúng một shard.

### Outbox ở quy mô lớn

Một relay (một instance hoạt động) xử lý theo lô 500 dòng, mỗi vòng gồm một lần SELECT, một lần produce đồng bộ và một UPDATE.
Trần thực tế phụ thuộc độ trễ tới Kafka.
Ví dụ độ trễ produce 10ms cho 500 event cho khoảng 50.000 event mỗi giây về lý thuyết, đủ cho nhu cầu ở bậc 0 tới bậc 3.
Nếu cần hơn, phân mảnh outbox theo `hash(aggregate_id) % N` và chạy N relay, mỗi relay giữ một khóa advisory khác nhau.
Thứ tự theo aggregate vẫn đúng vì một aggregate luôn rơi vào một shard.

### Bậc 5: nhiều vùng (multi-region)

Dấu hiệu: yêu cầu độ trễ toàn cầu hoặc chịu mất cả vùng.
Đây là bước tốn kém nhất: cần quyết định mô hình nhất quán (một region ghi, nhiều region đọc, hay ghi đa vùng với giải quyết xung đột), nhân bản Kafka (MirrorMaker, Cluster Linking) và định tuyến theo địa lý.
Đừng làm trước khi có yêu cầu kinh doanh rõ ràng.

## 4. Các điểm nóng thường gặp

### Hot key

Một order hoặc một khách hàng được đọc với tần suất cực cao đè lên một key Redis, một shard hay một partition.
Phòng thủ nhiều lớp, từ rẻ đến đắt:

1. `singleflight` (đã có) chặn cú sốc lên DB khi key hết hạn.
2. Thêm cache L1 trong tiến trình, TTL ngắn (một đến hai giây), đặt phía trước Redis. Một pod chỉ hỏi Redis tối đa một lần mỗi giây cho mỗi key nóng.
3. `ETag` và `Cache-Control` để client và CDN tự phục vụ.
4. Rate limit theo key thay vì chỉ theo IP.

### Khóa dòng khi nhiều người cùng ghi một aggregate

Optimistic locking (`version`) làm người thua nhận `409` thay vì chờ khóa.
Với tranh chấp thật sự cao (ví dụ bộ đếm tồn kho), thiết kế lại để giảm tranh chấp: gộp ghi, dùng cập nhật nguyên tử `SET x = x - 1 WHERE x > 0`, hoặc đưa qua hàng đợi theo key để xử lý tuần tự.

### Phân trang sâu

Đã dùng keyset nên không có vấn đề OFFSET.
Nếu cần "nhảy tới trang N" hoặc đếm tổng, hãy cân nhắc lại yêu cầu: `COUNT(*)` chính xác trên bảng lớn rất đắt, thường dùng số ước lượng.

### Số kết nối SSE

Mỗi kết nối SSE tốn một goroutine, vài KB bộ nhớ và một socket.
Hàng chục nghìn kết nối mỗi pod hoàn toàn khả thi, nhưng cần nâng `ulimit -n`.
Template đã giảm tải Redis bằng cách mỗi tiến trình chỉ giữ **một** subscription pattern rồi phân phối cục bộ trong bộ nhớ.
Khi vượt hàng trăm nghìn kết nối đồng thời, tách thành dịch vụ gateway riêng chỉ lo giữ kết nối, để nó scale độc lập với API.

## 5. Điều chỉnh Go runtime

| Hạng mục | Khuyến nghị |
|----------|-------------|
| `GOMAXPROCS` | Go 1.25 trở lên tự nhận biết giới hạn CPU của cgroup. Đặt CPU limit trong Kubernetes để giá trị này đúng |
| `GOMEMLIMIT` | `platform/bootstrap` tự đặt bằng 90% giới hạn bộ nhớ của container (thư viện `automemlimit`) |
| `GOGC` | Mặc định (100) thường đủ. Chỉ chỉnh sau khi có profile heap. Tăng GOGC đổi bộ nhớ lấy CPU |
| PGO | Thu CPU profile từ production, lưu thành `cmd/api/default.pgo`. Go tự dùng khi build và thường cho cải thiện vài phần trăm tới khoảng một chục phần trăm miễn phí |
| JSON | `encoding/json` đủ cho đa số trường hợp. Đo trước khi đổi sang thư viện khác. Nếu JSON thật sự nằm đầu profile, thử `encoding/json/v2` hoặc thư viện chuyên dụng |
| Allocation | Tìm bằng `go tool pprof -alloc_space`. Cấp phát trước slice khi biết kích thước, tránh chuyển đổi `[]byte` và `string` thừa trên đường nóng |

### Quy trình profiling

pprof nghe ở `127.0.0.1:6060` trong pod, không lộ ra ngoài.

```bash
kubectl -n orders port-forward deploy/api 6060:6060
go tool pprof -http=:8081 'http://localhost:6060/debug/pprof/profile?seconds=30'   # CPU
go tool pprof -http=:8081 http://localhost:6060/debug/pprof/heap                  # bộ nhớ đang dùng
go tool pprof http://localhost:6060/debug/pprof/goroutine                         # rò rỉ goroutine
```

Làm theo thứ tự: đo bằng metric, khoanh vùng bằng trace, rồi mới mở pprof để tìm dòng code.

## 6. Kafka

| Quyết định | Khuyến nghị | Lý do |
|------------|-------------|-------|
| Số partition | Chọn theo **mức song song consumer tối đa** bạn cần trong 2 tới 3 năm tới, không theo throughput | Thông lượng một partition rất cao. Giảm partition là không thể, tăng partition làm đổi ánh xạ key sang partition cho message mới (thứ tự theo key bị ngắt tại thời điểm đổi) |
| Replication | `replication.factor=3`, `min.insync.replicas=2`, producer `acks=all` (đã đặt) | Chịu mất một broker mà không mất dữ liệu |
| Producer | Idempotent, nén zstd, `linger` vài ms (đã đặt) | Throughput cao, không trùng khi retry mạng |
| Consumer | Commit thủ công theo lô, xử lý song song theo partition (đã làm) | At-least-once, thứ tự theo partition |
| Rebalance | franz-go mặc định dùng cooperative-sticky | Rebalance không dừng toàn bộ group |
| Scale worker | KEDA theo consumer lag. HPA theo CPU chỉ là phương án dự phòng | Lag là tín hiệu đúng của hàng đợi |
| Retention | Đặt theo thời gian bạn cần để phát lại sau sự cố | Replay là điểm mạnh lớn nhất của Kafka |

Cách xử lý khi consumer chậm hơn producer: tăng số worker (tối đa bằng số partition), tối ưu handler (batch ghi DB), và đảm bảo handler không gọi dịch vụ chậm theo kiểu tuần tự mà không có timeout.

## 7. PostgreSQL

| Hạng mục | Khuyến nghị |
|----------|-------------|
| Autovacuum cho `outbox` | Bảng có tỉ lệ ghi và xóa rất cao. Hạ `autovacuum_vacuum_scale_factor` (ví dụ 0,01) và tăng tần suất cho riêng bảng này |
| Index | Mỗi truy vấn trên đường nóng phải có index khớp. Kiểm tra bằng `EXPLAIN (ANALYZE, BUFFERS)` |
| N+1 | Dùng sqlc với truy vấn gộp (`WHERE id = ANY($1)`) thay vì lặp từng dòng |
| Giao dịch | Ngắn nhất có thể, không gọi IO ngoài (HTTP, Kafka) khi đang giữ giao dịch. Relay là ngoại lệ có chủ đích: nó cần giữ lock trong lúc produce một lô |
| Theo dõi | `pg_stat_statements`, truy vấn chậm (`log_min_duration_statement`), bloat, độ trễ replica |
| Migration | Xem RUNBOOK. Tạo index bằng `CREATE INDEX CONCURRENTLY` trên bảng lớn |

## 8. Bảo vệ khi quá tải (backpressure)

Hệ thống chịu tải tốt không phải là hệ thống không bao giờ quá tải, mà là hệ thống **hỏng một cách có kiểm soát**.

| Cơ chế | Có sẵn | Ghi chú |
|--------|--------|---------|
| Rate limit theo IP, phân tán | Có | Trả `429` kèm `Retry-After` |
| Timeout mọi lời gọi ra ngoài | Có | DB, Redis, Kafka, request |
| Giới hạn kích thước body | Có | Trả `413` |
| Pool DB có giới hạn | Có | Khi hết kết nối, request chờ rồi hết hạn theo context thay vì tạo thêm kết nối |
| Circuit breaker cho Redis | Có | Bỏ qua dependency đang lỗi |
| Hàng đợi bất đồng bộ cho việc nặng | Có (outbox + Kafka) | API chỉ ghi nhận, việc nặng làm ở worker |
| Giới hạn số request đồng thời (load shedding) | Chưa | Thêm middleware giới hạn in-flight nếu cần từ chối sớm khi đã bão hòa |
| Giới hạn goroutine cho tác vụ song song | Khi cần | Dùng `errgroup.SetLimit` thay vì sinh goroutine không giới hạn |

## 9. Phương pháp load test

Kịch bản `test/load/orders.js` dùng **mô hình mở** (`ramping-arrival-rate`): request đến theo tốc độ cố định bất kể server chậm hay nhanh.
Đó là cách người dùng thật hành xử.
Mô hình đóng (số VU cố định) tự giảm tải khi server chậm nên che giấu quá tải.

Các loại test nên chạy, theo thứ tự:

| Loại | Mục đích | Cách làm |
|------|----------|----------|
| Smoke | Kịch bản chạy đúng | Tải rất thấp, vài phút |
| Load | Có đạt SLO ở tải dự kiến không | RPS thiết kế giữ trong 15 tới 30 phút |
| Stress / breakpoint | Giới hạn nằm ở đâu và hỏng như thế nào | Tăng dần tới khi vỡ, quan sát thành phần nào vỡ trước |
| Spike | Phản ứng với tăng đột biến | Nhảy vọt lên 5 tới 10 lần trong vài giây, xem HPA và rate limit |
| Soak | Rò rỉ bộ nhớ, goroutine, kết nối | Tải vừa phải trong nhiều giờ, theo dõi `go_goroutines` và heap |

Quy tắc đọc kết quả:

1. Nhìn p95 và p99, không nhìn trung bình. Trung bình che giấu đuôi.
2. Tìm thành phần bão hòa đầu tiên theo phương pháp USE (Utilization, Saturation, Errors): CPU pod, pool DB (`app_db_pool_*`), CPU và I/O Postgres, Redis, consumer lag.
3. Dùng dữ liệu có kích thước giống production. Bảng 1.000 dòng cho kết quả khác hẳn bảng 100 triệu dòng.
4. Chạy từ máy riêng, không chung máy với hệ thống đang đo (bài học từ chính các lần đo laptop ở trên).
5. Thay đổi một biến mỗi lần.

Ngưỡng trong script cấu hình được qua biến môi trường để siết lại cho staging:

```bash
k6 run -e BASE_URL=https://staging.example.com -e RATE=2000 -e DURATION=10m \
  -e READ_P95_MS=50 -e WRITE_P95_MS=150 test/load/orders.js
```

## 10. Danh sách kiểm tra "sẵn sàng cho triệu người dùng"

- [ ] Đã load test trên hạ tầng riêng với RPS thiết kế và có SLO được ghi lại (p99, tỉ lệ lỗi).
- [ ] Postgres có HA, backup liên tục, đã thử restore và có kế hoạch failover được diễn tập.
- [ ] Tổng kết nối DB của mọi replica nhỏ hơn `max_connections` với dư địa, hoặc đã có PgBouncer.
- [ ] Kafka RF 3, `min.insync.replicas=2`, topic tạo bằng IaC, số partition được chọn có tính toán.
- [ ] Redis có HA. Đã diễn tập tình huống Redis chết: API vẫn phục vụ được (cache, rate limit fail open).
- [ ] Đã diễn tập tình huống Kafka chết: API vẫn nhận ghi, outbox tự xả khi Kafka về.
- [ ] Alert đã nối tới kênh trực và mỗi alert có runbook (`docs/RUNBOOK.md`).
- [ ] Có xác thực và phân quyền, rate limit tầng biên, TLS.
- [ ] Có kế hoạch tăng trưởng dữ liệu: phân vùng, lưu trữ lạnh, retention cho outbox và Kafka.
- [ ] Đã thử rolling update khi có tải: không rớt request, không giảm dung lượng.
- [ ] Đã chạy soak test nhiều giờ: không rò rỉ goroutine hay bộ nhớ.
