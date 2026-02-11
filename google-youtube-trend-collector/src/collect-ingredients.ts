import {
  kafka,
  searchYouTube,
  getYouTubeVideoStats,
  CompressionTypes,
  YOUTUBE_INGREDIENTS_TOPIC,
  sleep,
  oneWeekAgo,
} from "./lib.js";

interface IngredientQuery {
  ingredient: string;
  keywords: string[];
}

const ingredients: IngredientQuery[] = [
  {
    ingredient: "피스타치오",
    keywords: ["피스타치오 가격", "피스타치오 구매 추천", "피스타치오 베이킹 재료"],
  },
  {
    ingredient: "카다이프(카타이피)",
    keywords: ["카다이프 가격", "카다이프 구매", "카타이피 면 베이킹"],
  },
  {
    ingredient: "다크초콜릿",
    keywords: ["다크초콜릿 가격 비교", "베이킹 초콜릿 추천", "커버춰 초콜릿 가격"],
  },
  {
    ingredient: "타히니(참깨페이스트)",
    keywords: ["타히니 가격", "타히니 만들기", "참깨 페이스트 구매"],
  },
  {
    ingredient: "버터",
    keywords: ["버터 가격 비교 2025", "베이킹 버터 추천", "무염버터 가격"],
  },
  {
    ingredient: "설탕",
    keywords: ["설탕 가격 2025", "베이킹 설탕 종류", "비정제 설탕 가격"],
  },
  {
    ingredient: "밀가루",
    keywords: ["밀가루 가격 비교", "박력분 가격", "베이킹 밀가루 추천"],
  },
  {
    ingredient: "달걀",
    keywords: ["달걀 가격 시세", "계란 가격 2025", "달걀 가격 추이"],
  },
  {
    ingredient: "바닐라",
    keywords: ["바닐라 에센스 가격", "바닐라빈 가격", "바닐라 익스트랙트 추천"],
  },
  {
    ingredient: "두바이쿠키 완제품",
    keywords: ["두바이 쫀득 쿠키 가격", "두쫀쿠 가격 비교", "두바이 쿠키 편의점 가격"],
  },
];

async function main() {
  // 토픽 생성
  const admin = kafka.admin();
  await admin.connect();
  const created = await admin.createTopics({
    topics: [{ topic: YOUTUBE_INGREDIENTS_TOPIC, numPartitions: 6, replicationFactor: 3 }],
  });
  console.log(`토픽 '${YOUTUBE_INGREDIENTS_TOPIC}':`, created ? "새로 생성" : "이미 존재");
  await admin.disconnect();

  const producer = kafka.producer();
  await producer.connect();

  let totalMessages = 0;
  const publishedAfter = oneWeekAgo();

  console.log(`\n${"=".repeat(60)}`);
  console.log("🍪 두바이 쫀득 쿠키 재료 가격 수집 (YouTube)");
  console.log(`수집 기간: ${publishedAfter.split("T")[0]} ~ 현재`);
  console.log("=".repeat(60));

  for (const { ingredient, keywords } of ingredients) {
    console.log(`\n── 🧂 ${ingredient} ──`);

    for (const keyword of keywords) {
      try {
        // 최근 1주일, 관련도순 15건
        const data = await searchYouTube(keyword, 15, "relevance", publishedAfter);
        const items = data.items || [];

        if (items.length === 0) {
          // 기간 제한 없이 재시도
          const dataAll = await searchYouTube(keyword, 15, "relevance");
          const itemsAll = dataAll.items || [];

          if (itemsAll.length === 0) {
            console.log(`  ⏭️  "${keyword}" - 결과 없음`);
            continue;
          }

          // 통계 가져오기
          const videoIds = itemsAll
            .map((i) => i.id.videoId)
            .filter((id): id is string => !!id);

          let statsMap: Record<string, { viewCount: string; likeCount: string; commentCount: string }> = {};
          if (videoIds.length > 0) {
            try {
              const statsResult = await getYouTubeVideoStats(videoIds);
              for (const v of statsResult.items) {
                statsMap[v.id] = v.statistics;
              }
            } catch {
              // 통계 조회 실패 시 무시
            }
          }

          const messages = itemsAll.map((item, idx) => {
            const videoId = item.id.videoId ?? "";
            const stats = statsMap[videoId];
            return {
              key: `ingredient:${ingredient}:${keyword}:${idx}`,
              value: JSON.stringify({
                type: "ingredient_price",
                ingredient,
                keyword,
                requestedAt: new Date().toISOString(),
                period: "all",
                totalResults: dataAll.pageInfo.totalResults,
                video: {
                  videoId,
                  title: item.snippet.title,
                  description: item.snippet.description,
                  channelTitle: item.snippet.channelTitle,
                  publishedAt: item.snippet.publishedAt,
                  thumbnail: item.snippet.thumbnails?.medium?.url ?? "",
                  url: videoId ? `https://www.youtube.com/watch?v=${videoId}` : "",
                },
                statistics: stats
                  ? {
                      viewCount: parseInt(stats.viewCount || "0"),
                      likeCount: parseInt(stats.likeCount || "0"),
                      commentCount: parseInt(stats.commentCount || "0"),
                    }
                  : null,
              }),
              headers: {
                source: "youtube-ingredient-collector",
                ingredient,
                query: keyword,
                videoId,
              },
            };
          });

          await producer.send({
            topic: YOUTUBE_INGREDIENTS_TOPIC,
            compression: CompressionTypes.GZIP,
            messages,
          });

          totalMessages += messages.length;
          console.log(
            `  ✅ "${keyword}" - ${itemsAll.length}건 (전체기간, 총 ${dataAll.pageInfo.totalResults}건)`,
          );
          await sleep(200);
          continue;
        }

        // 통계 가져오기
        const videoIds = items
          .map((i) => i.id.videoId)
          .filter((id): id is string => !!id);

        let statsMap: Record<string, { viewCount: string; likeCount: string; commentCount: string }> = {};
        if (videoIds.length > 0) {
          try {
            const statsResult = await getYouTubeVideoStats(videoIds);
            for (const v of statsResult.items) {
              statsMap[v.id] = v.statistics;
            }
          } catch {
            // 통계 조회 실패 시 무시
          }
        }

        const messages = items.map((item, idx) => {
          const videoId = item.id.videoId ?? "";
          const stats = statsMap[videoId];
          return {
            key: `ingredient:${ingredient}:${keyword}:${idx}`,
            value: JSON.stringify({
              type: "ingredient_price",
              ingredient,
              keyword,
              requestedAt: new Date().toISOString(),
              period: "1week",
              totalResults: data.pageInfo.totalResults,
              video: {
                videoId,
                title: item.snippet.title,
                description: item.snippet.description,
                channelTitle: item.snippet.channelTitle,
                publishedAt: item.snippet.publishedAt,
                thumbnail: item.snippet.thumbnails?.medium?.url ?? "",
                url: videoId ? `https://www.youtube.com/watch?v=${videoId}` : "",
              },
              statistics: stats
                ? {
                    viewCount: parseInt(stats.viewCount || "0"),
                    likeCount: parseInt(stats.likeCount || "0"),
                    commentCount: parseInt(stats.commentCount || "0"),
                  }
                : null,
            }),
            headers: {
              source: "youtube-ingredient-collector",
              ingredient,
              query: keyword,
              videoId,
            },
          };
        });

        await producer.send({
          topic: YOUTUBE_INGREDIENTS_TOPIC,
          compression: CompressionTypes.GZIP,
          messages,
        });

        totalMessages += messages.length;

        // 조회수 상위 2개 표시
        const topViewed = items
          .filter((i) => i.id.videoId && statsMap[i.id.videoId!])
          .sort((a, b) => {
            const aViews = parseInt(statsMap[a.id.videoId!]?.viewCount || "0");
            const bViews = parseInt(statsMap[b.id.videoId!]?.viewCount || "0");
            return bViews - aViews;
          })
          .slice(0, 2);

        console.log(
          `  ✅ "${keyword}" - ${items.length}건 (1주일, 총 ${data.pageInfo.totalResults}건)`,
        );
        for (const v of topViewed) {
          const stats = statsMap[v.id.videoId!];
          console.log(
            `     🎬 ${v.snippet.title.substring(0, 50)}... (조회 ${parseInt(stats.viewCount).toLocaleString()})`,
          );
        }

        await sleep(200);
      } catch (e) {
        console.log(`  ❌ "${keyword}" - ${(e as Error).message}`);
      }
    }
  }

  await producer.disconnect();

  console.log(`\n${"=".repeat(60)}`);
  console.log("📊 재료 가격 수집 완료");
  console.log("=".repeat(60));
  console.log(`  총 Kafka 메시지: ${totalMessages}건`);
  console.log(`  저장 토픽: ${YOUTUBE_INGREDIENTS_TOPIC}`);
  console.log(`  재료 종류: ${ingredients.length}종`);
  console.log("=".repeat(60));
}

main().catch((e) => {
  console.error("치명적 오류:", (e as Error).message);
  process.exit(1);
});
