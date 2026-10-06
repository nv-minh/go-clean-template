# ADR 0002: Transactional Outbox và Kafka cho sự kiện

Trạng thái: Accepted

## Bối cảnh

Khi tạo hoặc hủy order, các hệ thống khác cần được thông báo.
Ghi DB rồi publish Kafka (dual write) có cửa sổ lỗi làm mất event hoặc phát event cho giao dịch đã rollback.

## Quyết định

Ghi event vào bảng `outbox` trong cùng transaction với dữ liệu nghiệp vụ.
Một relay (tiến trình `worker`) đọc theo lô và publish lên Kafka với `acks=all` và producer idempotent.
Chỉ một relay hoạt động tại một thời điểm (`pg_try_advisory_xact_lock`) để giữ thứ tự theo aggregate.
Consumer commit offset thủ công, ghi `(consumer, event_id)` vào `processed_events` trong cùng transaction với hiệu ứng, và chuyển message lỗi sang DLQ.

## Các lựa chọn đã cân nhắc

| Lựa chọn | Đánh giá |
|----------|----------|
| Dual write | Không đảm bảo, loại |
| Kafka transaction (exactly-once) cho cả DB | Không thể bao gồm Postgres trong transaction Kafka |
| CDC (Debezium) đọc WAL | Mạnh và độ trễ thấp, nhưng thêm hạ tầng vận hành (Kafka Connect, Debezium). Là bước nâng cấp hợp lý ở quy mô rất lớn |
| Outbox polling (chọn) | Đơn giản, chạy trên hạ tầng đã có, đủ cho hàng chục nghìn event mỗi giây |
| NATS JetStream, RabbitMQ thay Kafka | NATS nhẹ và đơn giản hơn. RabbitMQ hợp task queue. Kafka được chọn vì throughput, giữ thứ tự theo key và khả năng replay. Phần `messaging` đứng sau interface `Publisher` nên có thể thay |

## Hệ quả

- Đảm bảo at-least-once tới Kafka, consumer phải idempotent (đã làm bằng `processed_events`).
- Có thêm một bảng ghi nhiều và cần dọn dẹp (retention, hoặc phân vùng ở quy mô lớn).
- Độ trễ thêm cỡ khoảng `OUTBOX_POLL_INTERVAL` ở trường hợp rảnh. Khi có backlog thì xả liên tục.
- Một relay hoạt động là trần thông lượng. Cách phân mảnh nằm ở SCALING.md.
