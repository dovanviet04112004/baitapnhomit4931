# Hướng Dẫn Đóng Góp (Contributing Guide)

Cảm ơn bạn đã quan tâm đến việc đóng góp cho dự án **Crypto Analytics Pipeline**! Tài liệu này sẽ hướng dẫn bạn cách tham gia phát triển dự án.

---

## Mục Lục

- [Quy Tắc Ứng Xử](#quy-tắc-ứng-xử)
- [Cách Đóng Góp](#cách-đóng-góp)
- [Quy Trình Pull Request](#quy-trình-pull-request)
- [Coding Standards](#coding-standards)
- [Commit Message Convention](#commit-message-convention)
- [Cấu Trúc Dự Án](#cấu-trúc-dự-án)

---

## Quy Tắc Ứng Xử

- Tôn trọng tất cả thành viên trong team
- Sử dụng ngôn ngữ chuyên nghiệp trong communication
- Chấp nhận feedback mang tính xây dựng
- Tập trung vào việc cải thiện dự án

---

## Cách Đóng Góp

### 1. Fork Repository

```bash
git clone https://github.com/your-username/crypto-analytics-pipeline.git
cd crypto-analytics-pipeline
```

### 2. Tạo Branch Mới

```bash
git checkout -b feature/ten-tinh-nang
# hoặc
git checkout -b fix/ten-bug
```

### 3. Thực Hiện Thay Đổi

- Viết code theo coding standards
- Thêm unit tests nếu cần
- Cập nhật documentation

### 4. Commit và Push

```bash
git add .
git commit -m "feat: mô tả ngắn gọn thay đổi"
git push origin feature/ten-tinh-nang
```

---

## Quy Trình Pull Request

1. **Tạo Pull Request** từ branch của bạn vào `main`
2. **Điền đầy đủ thông tin** trong PR template
3. **Chờ review** từ maintainers
4. **Sửa đổi** nếu có feedback
5. **Merge** sau khi được approve

### PR Checklist

- [ ] Code đã được test locally
- [ ] Không có lỗi lint
- [ ] Documentation đã được cập nhật
- [ ] Commit messages tuân theo convention

---

## Coding Standards

### Python (crawl/, spark/, elasticsearch/)

```python
# Sử dụng snake_case cho biến và hàm
def calculate_daily_metrics(data):
    pass

# Sử dụng PascalCase cho class
class KafkaProducer:
    pass

# Docstrings cho functions
def process_data(raw_data: dict) -> dict:
    """
    Xử lý dữ liệu raw từ Kafka.

    Args:
        raw_data: Dictionary chứa dữ liệu crypto

    Returns:
        Dictionary đã được clean
    """
    pass
```

### JavaScript (webapp/js/)

```javascript
// Sử dụng camelCase cho biến và hàm
function fetchCryptoData() {}

// Sử dụng PascalCase cho class/component
class ChartComponent {}

// Sử dụng UPPER_SNAKE_CASE cho constants
const API_BASE_URL = '/api';
```

### YAML (k8s/)

```yaml
# Sử dụng 2 spaces cho indentation
apiVersion: apps/v1
kind: Deployment
metadata:
  name: service-name
  labels:
    app: service-name
```

---

## Commit Message Convention

Sử dụng [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<scope>): <description>

[optional body]

[optional footer]
```

### Types

| Type | Mô tả |
|------|-------|
| `feat` | Thêm tính năng mới |
| `fix` | Sửa bug |
| `docs` | Cập nhật documentation |
| `style` | Format code (không thay đổi logic) |
| `refactor` | Refactor code |
| `test` | Thêm/sửa tests |
| `chore` | Maintenance tasks |

### Ví dụ

```bash
feat(crawler): thêm retry mechanism cho API calls
fix(spark): sửa lỗi null pointer trong daily_aggregation
docs(readme): cập nhật hướng dẫn cài đặt
refactor(webapp): tách components thành modules riêng
```

---

## Cấu Trúc Dự Án

```
crypto-analytics-pipeline/
├── crawl/              # Data collection từ CoinGecko
├── kafka/              # Kafka cluster configuration
├── hdfs/               # HDFS consumers
├── spark/              # Spark batch processing jobs
├── elasticsearch/      # ES queries và API
├── webapp/             # Frontend dashboard
├── k8s/                # Kubernetes manifests
├── docs/               # Documentation
└── docker-compose.yml  # Local development setup
```

### Khi đóng góp vào từng module

| Module | Yêu cầu |
|--------|---------|
| `crawl/` | Test với fake data trước khi dùng real API |
| `spark/` | Đảm bảo jobs chạy được với sample data |
| `webapp/` | Test trên cả dark và light theme |
| `k8s/` | Validate YAML trước khi commit |

---

## Liên Hệ

Nếu có thắc mắc, vui lòng:
- Tạo Issue trên GitHub
- Liên hệ team lead qua email

---

**Happy Contributing!**
