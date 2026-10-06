# ADR 0005: Cách dùng Redis và chính sách khi Redis lỗi

Trạng thái: Accepted

## Bối cảnh

Redis phục vụ nhiều mục đích: cache, rate limit, idempotency, pub/sub realtime.
Nếu mọi thứ phụ thuộc cứng vào Redis, một sự cố Redis trở thành sự cố toàn hệ thống.

## Quyết định

Phân loại theo mục đích và đặt chính sách lỗi rõ ràng cho từng loại:

| Mục đích | Khi Redis lỗi | Lý do |
|----------|---------------|-------|
| Cache | Fail open, đọc DB | Chỉ là tối ưu |
| Rate limit | Fail open, cho qua | Bảo vệ dung lượng, không được gây sập |
| Idempotency | Fail closed (`503`) cho request có key | Phục vụ lời hứa không trùng của client |
| Readiness | Chỉ báo `degraded`, không loại pod | Tránh loại cả đội pod vì một dependency mềm |

Mọi lời gọi Redis trên đường request có timeout ngắn (300ms) và cache, rate limit được bọc circuit breaker.
Pub/sub dùng một subscription mẫu cho mỗi tiến trình và phân phối cục bộ.
Redis pub/sub chỉ dùng cho thông báo "đẩy trạng thái mới nhất" (có thể mất), còn việc phải bảo đảm đi qua Kafka.

## Hệ quả

- Redis sập làm giảm hiệu năng nhưng không làm sập dịch vụ.
- Có một hành vi cố ý "không đối xứng" (idempotency fail closed). Điều này được ghi rõ và có test.
- Khóa cache có phiên bản (`order:v1:`) để đổi định dạng mà không cần xóa cache.
