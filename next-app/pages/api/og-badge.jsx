import { ImageResponse } from '@vercel/og';

export const config = {
  runtime: 'edge',
};

export default async function handler(req) {
  try {
    const { searchParams } = new URL(req.url);
    const koans = searchParams.get('koans') || '39';

    return new ImageResponse(
      (
        <div
          style={{
            height: '100%',
            width: '100%',
            display: 'flex',
            flexDirection: 'column',
            alignItems: 'center',
            justifyContent: 'center',
            backgroundColor: '#111827',
            padding: '40px',
          }}
        >
          <div
            style={{
              display: 'flex',
              flexDirection: 'column',
              alignItems: 'center',
              justifyContent: 'center',
              backgroundColor: '#1f2937',
              border: '8px solid #f97316',
              borderRadius: '20px',
              padding: '60px',
              width: '100%',
              maxWidth: '1000px',
            }}
          >
            {/* Badge Circle */}
            <div
              style={{
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                width: '150px',
                height: '150px',
                background: 'linear-gradient(135deg, #f97316 0%, #ea580c 100%)',
                borderRadius: '75px',
                marginBottom: '30px',
                boxShadow: '0 10px 40px rgba(249, 115, 22, 0.3)',
              }}
            >
              <div style={{ fontSize: '80px', display: 'flex' }}>🎓</div>
            </div>

            {/* Title */}
            <div
              style={{
                fontSize: '56px',
                fontWeight: 'bold',
                color: '#f97316',
                marginBottom: '20px',
                display: 'flex',
              }}
            >
              Achievement Unlocked!
            </div>

            {/* Divider */}
            <div
              style={{
                width: '150px',
                height: '4px',
                backgroundColor: '#f97316',
                marginBottom: '30px',
                display: 'flex',
              }}
            />

            {/* Achievement Title */}
            <div
              style={{
                fontSize: '44px',
                fontWeight: 'bold',
                color: '#ffffff',
                marginBottom: '20px',
                display: 'flex',
              }}
            >
              PySpark Koans Master
            </div>

            {/* Description */}
            <div
              style={{
                fontSize: '24px',
                color: '#9ca3af',
                marginBottom: '40px',
                display: 'flex',
                textAlign: 'center',
              }}
            >
              Successfully completed all {koans} PySpark and Delta Lake exercises
            </div>

            {/* Stats */}
            <div
              style={{
                display: 'flex',
                gap: '60px',
                marginTop: '20px',
              }}
            >
              <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
                <div style={{ fontSize: '48px', fontWeight: 'bold', color: '#f97316', display: 'flex' }}>
                  {koans}
                </div>
                <div style={{ fontSize: '18px', color: '#6b7280', display: 'flex' }}>
                  Koans Completed
                </div>
              </div>
              <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
                <div style={{ fontSize: '48px', fontWeight: 'bold', color: '#f97316', display: 'flex' }}>
                  9
                </div>
                <div style={{ fontSize: '18px', color: '#6b7280', display: 'flex' }}>
                  Categories Mastered
                </div>
              </div>
            </div>
          </div>
        </div>
      ),
      {
        width: 1200,
        height: 630,
      },
    );
  } catch (e) {
    console.error('OG Image Error:', e);
    return new Response(`Failed to generate image: ${e.message}`, { status: 500 });
  }
}
