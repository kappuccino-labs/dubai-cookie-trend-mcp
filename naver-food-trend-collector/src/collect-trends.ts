import { kafka, searchNaver, CompressionTypes, TOPIC, sleep } from "./lib.js";

interface QueryGroup {
  category: string;
  keywords: string[];
}

const queries: QueryGroup[] = [
  {
    category: "두바이쿠키",
    keywords: ["두바이 초콜릿 쿠키", "두바이 쫀득 쿠키", "두바이 쿠키 맛집"],
  },
  {
    category: "유행디저트",
    keywords: [
      "2025 유행 디저트",
      "크럼블쿠키",
      "약과 디저트",
      "소금빵",
      "휘낭시에",
      "크루아상 맛집",
      "마카롱 신메뉴",
    ],
  },
  {
    category: "유행음식",
    keywords: [
      "2025 유행 음식",
      "마라탕 맛집",
      "로제떡볶이",
      "제로음료 트렌드",
      "수비드 스테이크",
      "오마카세 맛집",
      "주먹밥 맛집",
    ],
  },
  {
    category: "유행카페",
    keywords: [
      "2025 핫플 카페",
      "성수 카페 추천",
      "을지로 카페",
      "카페 디저트 맛집",
      "대형카페 추천",
      "뷰맛집 카페",
      "브런치 카페",
    ],
  },
];

const SHOP_KEYWORDS = new Set([
  "두바이 초콜릿 쿠키",
  "두바이 쫀득 쿠키",
  "크럼블쿠키",
  "약과 디저트",
  "소금빵",
  "휘낭시에",
  "마카롱 신메뉴",
]);

type SearchType = "news" | "blog" | "shop";

async function main() {
  const producer = kafka.producer();
  await producer.connect();

  const searchTypes: SearchType[] = ["news", "blog", "shop"];
  let totalMessages = 0;
  let totalItems = 0;

  for (const { category, keywords } of queries) {
    console.log(`\n${"=".repeat(60)}`);
    console.log(`📂 카테고리: ${category}`);
    console.log("=".repeat(60));

    for (const keyword of keywords) {
      for (const type of searchTypes) {
        if (type === "shop" && !SHOP_KEYWORDS.has(keyword)) continue;

        try {
          const data = await searchNaver(type, keyword, type === "shop" ? 50 : 100);
          const items = data.items || [];

          if (items.length === 0) {
            console.log(`  ⏭️  [${type}] "${keyword}" - 결과 없음`);
            continue;
          }

          const messages = items.map((item, idx) => ({
            key: `${category}:${type}:${keyword}:${idx}`,
            value: JSON.stringify({
              type: `search_${type}`,
              category,
              keyword,
              searchType: type,
              requestedAt: new Date().toISOString(),
              totalAvailable: data.total,
              item,
            }),
            headers: {
              source: "naver-trend-collector",
              category,
              searchType: type,
              query: keyword,
            },
          }));

          await producer.send({
            topic: TOPIC,
            compression: CompressionTypes.GZIP,
            messages,
          });

          totalMessages += messages.length;
          totalItems += items.length;
          console.log(
            `  ✅ [${type}] "${keyword}" - ${items.length}건 수집 (전체 ${data.total}건)`,
          );
          await sleep(100);
        } catch (e) {
          console.log(`  ❌ [${type}] "${keyword}" - 오류: ${(e as Error).message}`);
        }
      }
    }
  }

  await producer.disconnect();

  console.log(`\n${"=".repeat(60)}`);
  console.log("📊 수집 완료 요약");
  console.log("=".repeat(60));
  console.log(`  총 Kafka 메시지: ${totalMessages}건`);
  console.log(`  총 수집 아이템: ${totalItems}건`);
  console.log(`  저장 토픽: ${TOPIC}`);
  console.log("=".repeat(60));
}

main().catch((e) => {
  console.error("치명적 오류:", (e as Error).message);
  process.exit(1);
});
