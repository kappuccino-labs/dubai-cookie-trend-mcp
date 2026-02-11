# Dubai Cookie Trend MCP
 
두바이 쫀득 쿠키 및 유행 디저트/음식/카페 트렌드 데이터를 수집하고 분석하는 MCP(Model Context Protocol) + Kafka 파이프라인 프로젝트입니다.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Dubai Cookie Trend MCP                       │
├─────────────────┬──────────────────────┬────────────────────────┤
│ confluent-kafka  │ naver-food-trend     │ google-youtube-trend   │
│ (MCP Server)     │ (Data Collector)     │ (Data Collector)       │
│                  │                      │                        │
│ - Kafka 관리     │ - 네이버 검색 API    │ - Google Custom Search │
│ - 네이버 검색    │ - 트렌드 수집        │ - YouTube Data API     │
│ - 데이터랩 연동  │ - 재료 가격 수집     │ - 네이버 쇼핑 가격    │
│ - 토픽 CRUD      │ - Kafka Producer     │ - Kafka Producer       │
│ - Consumer/      │ - Consumer 요약      │ - Consumer 요약        │
│   Producer       │                      │                        │
└────────┬────────┴──────────┬───────────┴────────────┬───────────┘
         │                   │                        │
         └───────────────────┴────────────────────────┘
                             │
                    ┌────────▼────────┐
                    │  Confluent Cloud │
                    │  Apache Kafka    │
                    │  (ap-northeast-2)│
                    └─────────────────┘
```

## Projects

### 1. `confluent-kafka/` - MCP Server
Confluent Cloud Kafka와 네이버 API를 연동하는 MCP 서버 (13개 도구 제공)

**MCP Tools:**
| Tool | Description |
|------|-------------|
| `list_topics` | Kafka 토픽 목록 조회 |
| `describe_topic` | 토픽 상세 정보 (파티션, 오프셋, 설정) |
| `produce_message` | 메시지 전송 |
| `consume_messages` | 메시지 소비 |
| `create_topic` | 새 토픽 생성 |
| `list_consumer_groups` | 컨슈머 그룹 목록 |
| `describe_consumer_group` | 컨슈머 그룹 상세 |
| `naver_datalab_trend` | 네이버 데이터랩 검색 트렌드 |
| `naver_datalab_shopping` | 네이버 데이터랩 쇼핑 인사이트 |
| `consume_naver_trends` | 트렌드 데이터 소비 |
| `naver_search` | 네이버 검색 (뉴스/블로그/쇼핑/이미지/웹/카페) |
| `naver_search_stream` | 대량 검색 수집 + Kafka 저장 |
| `consume_naver_search` | 검색 데이터 소비 |

### 2. `naver-food-trend-collector/` - Naver Data Pipeline
네이버 검색 API로 두바이 쿠키, 유행 디저트, 음식, 카페 트렌드를 수집하고 Kafka에 저장

**수집 카테고리:**
- 두바이쿠키 (두바이 초콜릿 쿠키, 두바이 쫀득 쿠키, 두바이 쿠키 맛집)
- 유행디저트 (크럼블쿠키, 약과, 소금빵, 휘낭시에, 크루아상, 마카롱)
- 유행음식 (마라탕, 로제떡볶이, 제로음료, 수비드 스테이크, 오마카세)
- 유행카페 (성수, 을지로, 대형카페, 뷰맛집, 브런치)

**재료 가격 수집 (두바이 쿠키):**
피스타치오, 카다이프, 다크초콜릿, 타히니, 버터, 설탕, 밀가루, 달걀, 바닐라, 완제품

### 3. `google-youtube-trend-collector/` - Google & YouTube Data Pipeline
Google Custom Search + YouTube Data API로 트렌드 및 재료 가격 정보 수집

## Quick Start

### 1. 환경변수 설정
각 프로젝트 디렉토리에서 `.env.example`을 `.env`로 복사하고 API 키를 입력합니다.

```bash
# Naver Food Trend Collector
cd naver-food-trend-collector
cp .env.example .env
# .env 파일에 API 키 입력

# 의존성 설치
npm install
```

### 2. API 키 발급

| API | 발급 링크 |
|-----|-----------|
| Naver 검색 API | https://developers.naver.com/apps |
| Google Custom Search | https://console.cloud.google.com |
| YouTube Data API v3 | https://console.cloud.google.com |
| Confluent Cloud Kafka | https://confluent.cloud |

### 3. 데이터 수집 실행

```bash
# 네이버 트렌드 수집 -> Kafka
cd naver-food-trend-collector
npm run collect:trends

# 두바이 쿠키 재료 가격 수집 -> Kafka
npm run collect:ingredients

# Kafka 데이터 요약 보기
npm run summary
```

### 4. MCP 서버 실행

```bash
cd confluent-kafka
npm install
npm run dev
```

Claude Desktop `claude_desktop_config.json` 설정:
```json
{
  "mcpServers": {
    "confluent-kafka": {
      "command": "node",
      "args": ["path/to/confluent-kafka/dist/index.js"],
      "env": {
        "KAFKA_BOOTSTRAP_SERVERS": "your-server:9092",
        "KAFKA_API_KEY": "your-key",
        "KAFKA_API_SECRET": "your-secret",
        "NAVER_CLIENT_ID": "your-id",
        "NAVER_CLIENT_SECRET": "your-secret"
      }
    }
  }
}
```

## Collection Results (2026-02-11)

| Pipeline | Messages | Topic |
|----------|----------|-------|
| Naver Trends (뉴스/블로그/쇼핑) | 5,101건 | `naver-search-results` |
| Naver Ingredients (재료 가격) | 1,957건 | `naver-search-results` |
| Kafka Total Consumed | 14,165건 | - |

### Dubai Cookie Ingredient Prices (Kafka Summary)

| Ingredient | Min Price | Avg Price | Max Price | Items |
|-----------|-----------|-----------|-----------|-------|
| Pistachio (분태/페이스트) | 12,470원 | 170,185원 | 1,295,900원 | 300 |
| Kadaif (카다이프면) | 8,500원 | 301,767원 | 11,322,500원 | 300 |
| Dark Chocolate (커버춰) | 6,690원 | 104,154원 | 2,805,900원 | 300 |
| Tahini (타히니) | 10,830원 | 996,224원 | 56,067,380원 | 200 |
| Butter (무염버터) | 1,360원 | 75,960원 | 653,400원 | 400 |
| Vanilla | 5,200원 | 64,433원 | 551,700원 | 200 |
| Sugar (설탕) | 2,800원 | 31,815원 | 170,110원 | 200 |
| Flour (밀가루) | 1,990원 | 28,341원 | 119,130원 | 200 |
| Eggs (달걀) | 2,190원 | 27,798원 | 792,000원 | 200 |
| Finished Product | 4,000원 | 34,841원 | 149,800원 | 157 |

## Tech Stack

- **Runtime**: Node.js + TypeScript
- **MCP SDK**: `@modelcontextprotocol/sdk`
- **Message Broker**: Apache Kafka (Confluent Cloud)
- **APIs**: Naver Search, Naver DataLab, Google Custom Search, YouTube Data API v3
- **Libraries**: KafkaJS, Zod, dotenv

## License

ISC
