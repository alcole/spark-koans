/**
 * Next.js App Wrapper
 * This wraps all pages
 */

import '../styles/globals.css';
import { Analytics } from '@vercel/analytics/react';
import Script from 'next/script';

export default function App({ Component, pageProps }) {
  return (
    <>
      <Component {...pageProps} />
      <Analytics />
      <Script
        src="https://static.cloudflareinsights.com/beacon.min.js"
        data-cf-beacon='{"token": "4dcca25d30504b899a50f0b5dde26810"}'
        strategy="afterInteractive"
      />
    </>
  );
}
