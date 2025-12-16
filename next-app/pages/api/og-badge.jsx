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
            alignItems: 'center',
            justifyContent: 'center',
            background: 'linear-gradient(135deg, #1e293b 0%, #0f172a 100%)',
          }}
        >
          {/* Badge Container - More compact, badge-like proportions */}
          <div
            style={{
              display: 'flex',
              flexDirection: 'column',
              alignItems: 'center',
              justifyContent: 'center',
              background: 'linear-gradient(135deg, #1f2937 0%, #111827 100%)',
              border: '12px solid #f97316',
              borderRadius: '30px',
              padding: '80px 100px',
              boxShadow: '0 25px 50px rgba(0, 0, 0, 0.5)',
              position: 'relative',
            }}
          >
            {/* Top Icon Circle */}
            <div
              style={{
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                width: '160px',
                height: '160px',
                background: 'linear-gradient(135deg, #f97316 0%, #ea580c 100%)',
                borderRadius: '80px',
                marginBottom: '40px',
                border: '6px solid #1f2937',
                boxShadow: '0 15px 30px rgba(249, 115, 22, 0.4)',
              }}
            >
              <div style={{ fontSize: '90px', display: 'flex' }}>🎓</div>
            </div>

            {/* Main Title */}
            <div
              style={{
                fontSize: '68px',
                fontWeight: 'bold',
                color: '#ffffff',
                marginBottom: '15px',
                display: 'flex',
                textAlign: 'center',
                lineHeight: 1.1,
              }}
            >
              PYSPARK KOANS
            </div>

            {/* Subtitle */}
            <div
              style={{
                fontSize: '42px',
                fontWeight: '600',
                color: '#10b981',
                marginBottom: '40px',
                display: 'flex',
                textAlign: 'center',
                textTransform: 'uppercase',
                letterSpacing: '2px',
              }}
            >
              Achievement Program
            </div>

            {/* Divider Line */}
            <div
              style={{
                width: '300px',
                height: '3px',
                background: 'linear-gradient(90deg, transparent 0%, #f97316 50%, transparent 100%)',
                marginBottom: '35px',
                display: 'flex',
              }}
            />

            {/* Completion Stats */}
            <div
              style={{
                display: 'flex',
                flexDirection: 'column',
                alignItems: 'center',
                gap: '15px',
              }}
            >
              <div
                style={{
                  fontSize: '32px',
                  color: '#9ca3af',
                  display: 'flex',
                  textAlign: 'center',
                }}
              >
                All {koans} Exercises Completed
              </div>
              <div
                style={{
                  fontSize: '26px',
                  color: '#6b7280',
                  display: 'flex',
                  textAlign: 'center',
                }}
              >
                PySpark & Delta Lake Mastery
              </div>
            </div>

            {/* Bottom accent */}
            <div
              style={{
                position: 'absolute',
                bottom: '0',
                left: '0',
                right: '0',
                height: '8px',
                background: 'linear-gradient(90deg, #ea580c 0%, #f97316 50%, #ea580c 100%)',
                borderRadius: '0 0 18px 18px',
                display: 'flex',
              }}
            />
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
