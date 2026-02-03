import { ImageResponse } from '@vercel/og';

export const config = {
  runtime: 'edge',
};

const DIFFICULTY_COLORS = {
  beginner: '#22c55e',
  intermediate: '#f59e0b',
  advanced: '#ef4444',
};

export default async function handler(req) {
  const { searchParams } = new URL(req.url);
  const title = searchParams.get('title') || 'PySpark Koan';
  const category = searchParams.get('category') || 'General';
  const difficulty = searchParams.get('difficulty') || 'beginner';

  const diffColor = DIFFICULTY_COLORS[difficulty] || DIFFICULTY_COLORS.beginner;
  const diffLabel = difficulty.charAt(0).toUpperCase() + difficulty.slice(1);

  return new ImageResponse(
    (
      <div
        style={{
          width: 1200,
          height: 630,
          background: '#0f172a',
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'center',
          justifyContent: 'center',
          fontFamily: 'sans-serif',
          color: '#f1f5f9',
        }}
      >
        <div style={{ fontSize: 26, color: '#f97316', fontWeight: 600, marginBottom: 48 }}>
          PySpark Koans
        </div>

        <div style={{ fontSize: 52, fontWeight: 700, textAlign: 'center', lineHeight: 1.3, maxWidth: 900, marginBottom: 48 }}>
          {title}
        </div>

        <div style={{ display: 'flex', gap: 16 }}>
          <div
            style={{
              background: '#1e293b',
              border: '1px solid #f97316',
              color: '#f97316',
              padding: '10px 24px',
              borderRadius: 8,
              fontSize: 22,
              fontWeight: 500,
            }}
          >
            {category}
          </div>
          <div
            style={{
              background: '#1e293b',
              border: `1px solid ${diffColor}`,
              color: diffColor,
              padding: '10px 24px',
              borderRadius: 8,
              fontSize: 22,
              fontWeight: 500,
            }}
          >
            {diffLabel}
          </div>
        </div>
      </div>
    ),
    { width: 1200, height: 630 }
  );
}
