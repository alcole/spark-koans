import { ImageResponse } from '@vercel/og';

export const config = {
  runtime: 'edge',
};

export default async function handler() {
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
        <div style={{ fontSize: 28, color: '#f97316', fontWeight: 600, marginBottom: 24, letterSpacing: 2 }}>
          PYSPARK KOANS
        </div>

        <div style={{ fontSize: 64, fontWeight: 700, textAlign: 'center', lineHeight: 1.2, marginBottom: 32 }}>
          Master PySpark
        </div>
        <div style={{ fontSize: 64, fontWeight: 700, textAlign: 'center', lineHeight: 1.2, color: '#f97316', marginBottom: 48 }}>
          Through Practice
        </div>

        <div style={{ display: 'flex', gap: 24 }}>
          <div
            style={{
              background: '#1e293b',
              border: '1px solid #f97316',
              color: '#f97316',
              padding: '12px 28px',
              borderRadius: 8,
              fontSize: 20,
              fontWeight: 500,
            }}
          >
            Interactive Exercises
          </div>
          <div
            style={{
              background: '#1e293b',
              border: '1px solid #f97316',
              color: '#f97316',
              padding: '12px 28px',
              borderRadius: 8,
              fontSize: 20,
              fontWeight: 500,
            }}
          >
            Delta Lake
          </div>
          <div
            style={{
              background: '#1e293b',
              border: '1px solid #f97316',
              color: '#f97316',
              padding: '12px 28px',
              borderRadius: 8,
              fontSize: 20,
              fontWeight: 500,
            }}
          >
            Achievement Badges
          </div>
        </div>

        <div style={{ fontSize: 18, color: '#64748b', marginTop: 56 }}>
          spark-koans.com
        </div>
      </div>
    ),
    { width: 1200, height: 630 }
  );
}
